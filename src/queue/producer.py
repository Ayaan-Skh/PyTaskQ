# This is where the core logic happens. The producer takes task request and puts it into Redis

# The producer is responsible for:
# 1. Generating a unique id
# 2. Seralizing the task in JSON
# 3. Writing to the correct redis stream based on priority
# 4. Stroing metadata in redis for task lookups
# 5. Handling idempotency (don't re-enqueue duplicate tasks)

# Producer creates work, Consumer executes it

import json 
import uuid
from datetime import datetime, timezone
from typing import Optional,Any

import structlog

from src.config import settings
from src.queue.broker import redis_client
from src.api.models import Priority, TaskStatus, TaskStatusResponse, TaskSubmitRequest, TaskSubmitResponse
from src.tasks.registry import registry


logger=structlog.get_logger(__name__)


# ----------------- Queue name mapping --------------------------
# Maps priority to actual Redis stream names.
# Centralizing this means u need to renames just in one place, not scattered through out the code.

PRIORITY_TO_QUEUE:dict[Priority:str]={
    Priority.HIGH:settings.QUEUE_HIGH,
    Priority.MEDIUM:settings.QUEUE_MEDIUM,
    Priority.LOW:settings.QUEUE_LOW,
}
 
def _serialize_task(task_id:str,request:TaskSubmitRequest)->dict[str,str]:
    """
    Convert a TaskSubmitRequest into a flat dict of stringd for Redis
    why flat dict of strings?
    Redis Streams store data as field valus pairs, Where both mst be strings.
    You can't store nested dicts or integers with json.model_dumps()
    
    Why not pickle?
    1. pickle is Python-specific — if you ever add a Go/Node worker, it can't read it
    2. pickle can execute arbitrary code when deserializing — MAJOR security risk
       if task data comes from untrusted sources
    3. JSON is human-readable — you can inspect tasks directly in Redis CLI
    
    Real-life analogy: Think of this like packing a suitcase.
    You can't put a whole dresser in a suitcase.
    You fold each item flat (serialize to string) so it fits.
    On the other side, the worker unpacks (deserializes) each item.
    """
    now =datetime.now(timezone.utc).isoformat()
    return{
        "task_id":task_id,
        "task_type":request.task_type,
        "args":json.dumps(request.args),
        # json.dumps() coverts {"..":".."} to '{"..":".."}'
        # json.loads does the reverse of it
    
        "priority":request.priority.value,
        # .value gets the string "high" from Priority.HIGH
        "status":TaskStatus.PENDING.value,
        "retry_count":"0",
        # Redis stores strings not integers so "0" not 0
        
        "max_retries":str(request.max_retries or settings.MAX_RETRIES),
        "timeout":str(request.timeout or settings.TASK_TIMEOUT),
        "created_at":now,
        "started_at":"", # Empty string = null for Redis
        "completed_at":"",
        "result":"",
        "error":"",
        "worker_id":""  # Which worker claimed this task
    }
    
async def enqueue_task(request:TaskSubmitRequest)->TaskSubmitResponse:
    """
    Main entry point: takes a task request, puts it in the right redis queue
    This function is handled by FastAPI route handler
    Its async becuz its Redis opertaions I/O bound
    """
    # --------- STEP 1: Validate task type -------------------
    if request.task_type not in registry:
        # Fail fast: if it dosen't exist reject immediately
        # Don't enqueue unkonwn tasks
        raise ValueError(
            f"Unknown task type:'{request.task_type}"
            f"Registered tasks: {registry.list_tasks()}"
        ) 
    
    # ----------- STEP 2: Handle idempotency ---------------
    if request.idempotency_key:
        existing_task_id=await _check_idempotency(request.idempotency_key)
        if existing_task_id:
            # This task was already submitted. Return the original
            # This is safe to call multiple time - runs only once
            logger.info(
                "idempotent_task_returned",
                idempotency_key=request.idempotency_key,
                existing_task_id=existing_task_id,
            )
            return await _get_task_as_response(existing_task_id)
        
    # ── Step 3: Generate unique task ID ───────────────────────────────────────
    task_id=str(uuid.uuid4())
    
    # ── Step 4: Determine target queue ────────────────────────────────────────
    queue_name=PRIORITY_TO_QUEUE[request.priority]    # O(1) dict lookup. Clean and fast.
    
    # ── Step 5: Serialize task data ───────────────────────────────────────────    
    task_data=_serialize_task(task_id,request)
    
    # ── Step 6: Write to Redis Stream (the actual enqueue) ────────────────────    
    redis=redis_client.client
    stream_id=await redis.xadd(
        queue_name,
        # XADD <stream_name> <id> <field> <value> [<field> <value> ...]
        # Appends a new entry to the stream.
        task_data,
        # The dict of field-value pairs. Redis stores these as the message body.
        
        
        id="*",
        # id="*" means "auto-generate an ID".
        # Redis Stream IDs are: <millisecond_timestamp>-<sequence_number>l        
        maxlen=10000
        # Trim the stream to at most 10,000 entries.
        # Without this, a queue backlog would grow unboundedly and eat all memory.
        # 10,000 is conservative for dev. Production might be 100,000+.
        # approximate=True (default) means Redis trims "approximately" — 
        # it won't trim mid-batch for performance. Acceptable for our use case.
    )    
    # stream_id is the Redis-generated message ID (e.g., "1699900000000-0")
    # We don't need to store it — we use task_id for lookups.
    
    # ── Step 7: Store task metadata for status lookups ────────────────────────
    task_key=f'dtq:task:{task_id}'
    # Key naming convention: app_prefix:entity_type:entity_id
    # "dtq" = distributed task queue (our app namespace)
    # "task" = entity type
    # task_id = the specific task        
    
    await redis.hset(task_key,mapping=task_data)
    # HSET stores a hash (dict) at a key.
    # This lets us do HGET dtq:task:{id} field to get individual fields,
    # and HGETALL dtq:task:{id} to get all fields at once.
    
    
    await redis.expire(task_key,86400*7)
    # Expire task metadata after 7 days (86400 seconds/day × 7).
    # Tasks are transient — you don't need them forever.
    
    # ── Step 8: Store idempotency key mapping ─────────────────────────────────
    if request.idempotency_key:
        idempotency_key=f"dqt:idempotenncy:{request.idempotency_key}"
        await redis.set(idempotency_key,task_id,ex=86400)
        # Map the idempotency key → task_id for 24 hours.
        # After 24h, re-submission creates a new task (reasonable deduplication window).    
    logger.info(
        "task Enqueued",
        task_id=task_id,
        task_type=request.task_type,
        priority=request.priority.value,
        queue=queue_name,
        stream_id=stream_id
    )
    
    return TaskSubmitResponse(
        task_id=task_id,
        status=TaskStatus.PENDING,
        priority=request.priority,
        queue=queue_name,
        created_at=datetime.now(timezone.utc)
        
    )

async def get_task_status(task_id:str)->Optional[dict]:
    """
    Retrive current task stats from Redis
    """
    redis=redis_client.client
    task_key=f"dqt:task:{task_id}"
    
    task_data=await redis.hgetall(task_key)
    if not task_data:
        return None
    
    return task_data
        
async def _get_task_as_response(task_id: str) -> TaskSubmitResponse:
    """Convert stored task data back into a TaskSubmitResponse."""
    task_data = await get_task_status(task_id)
    return TaskSubmitResponse(
        task_id=task_id,
        status=TaskStatus(task_data["status"]),
        priority=Priority(task_data["priority"]),
        queue=PRIORITY_TO_QUEUE[Priority(task_data["priority"])],
        created_at=datetime.fromisoformat(task_data["created_at"]),
        message="Task already submitted (idempotent return)",
    )    


async def _check_idempotency(idempotency_key: str) -> Optional[str]:
    """Check if a task with this idempotency key was already submitted."""
    redis = redis_client.client
    key = f"dtq:idempotency:{idempotency_key}"
    return await redis.get(key)
    # Returns task_id string if found, None if not found.

async def get_queue_depths()->dict[str,int]:
    """
    Returns the nubmer of tasks in the queue
    Required for the monitoring endpoint 
    """
    redis=redis_client.client
    depths={}
    
    for priority,queue_name in PRIORITY_TO_QUEUE.items():
        length=redis.xlen(queue_name)
        
        depths[priority.value]=length
    
    return depths    
        
    
            