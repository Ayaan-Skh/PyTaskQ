# This is responsible for:
#     1. generating a unique task ID
#     2. seralizing the task to JSON
#     3. Writing to the correct Redis stream based on priority
#     4. Storing task metadata in redis for status lookups
#     5. Hiding idempotency (dont reengue duplicate task)
# 
# 
# "Producer" comes from the Producer-Consumer pattern:
# Producer creates work, Consumer executes it.
# They're decoupled — producer doesn't know or care who picks up the task.

import json
import uuid
from datetime import datetime, timezone
from typing import Optional

import structlog

from src.config import settings
from src.queue.broker import redis_client
from src.api.models import Priority, TaskStatus, TaskSubmitRequest, TaskSubmitResponse
from src.tasks.registry import registry

logger = structlog.get_logger(__name__)


# ── Queue name mapping ────────────────────────────────────────────────────────
# Maps Priority enum values to actual Redis Stream names.
# Centralizing this mapping means if you rename a queue,
# you change it in ONE place, not scattered throughout the code.
PRIORITY_TO_QUEUE: dict[Priority, str] = {
    Priority.HIGH:   settings.QUEUE_HIGH,
    Priority.MEDIUM: settings.QUEUE_MEDIUM,
    Priority.LOW:    settings.QUEUE_LOW,
}


def _serialize_task(task_id: str, request: TaskSubmitRequest) -> dict[str, str]:
    """
    Convert a TaskSubmitRequest into a flat dict of strings for Redis.
    
    Why flat dict of strings?
    Redis Streams store data as field-value pairs, where both must be strings.
    You can't store nested dicts or integers directly.
    Solution: serialize complex values with json.dumps().
    
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
    now = datetime.now(timezone.utc).isoformat()
    # Always use UTC for timestamps in distributed systems.
    # Workers might run in different timezones.
    # UTC is the universal reference point — like GMT for aviation.
    
    return {
        "task_id":        task_id,
        "task_type":      request.task_type,
        "args":           json.dumps(request.args),
        # json.dumps converts {"to": "a@b.com"} → '{"to": "a@b.com"}'
        # json.loads does the reverse when the worker reads it
        
        "priority":       request.priority.value,
        # .value gets the string "high" from Priority.HIGH
        
        "status":         TaskStatus.PENDING.value,
        "retry_count":    "0",
        # Redis stores strings, not integers. "0" not 0.
        # Workers increment this with Redis HINCRBY for atomicity.
        
        "max_retries":    str(request.max_retries or settings.MAX_RETRIES),
        "timeout":        str(request.timeout or settings.TASK_TIMEOUT),
        "created_at":     now,
        "started_at":     "",   # Empty string = null for Redis
        "completed_at":   "",
        "result":         "",
        "error":          "",
        "worker_id":      "",   # Which worker claimed this task
    }


async def enqueue_task(request: TaskSubmitRequest) -> TaskSubmitResponse:
    """
    Main entry point: takes a task request, puts it in the right Redis queue.
    
    This function is called by the FastAPI route handler.
    It's async because Redis operations are I/O bound —
    while waiting for Redis to respond, asyncio can handle other requests.
    """
    
    # ── Step 1: Validate task type exists ─────────────────────────────────────
    if request.task_type not in registry:
        # Fail fast: if the task type doesn't exist, reject immediately.
        # Don't enqueue unknown tasks — they'd sit in the queue forever,
        # fail on every worker, and fill up the dead letter queue.
        raise ValueError(
            f"Unknown task type: '{request.task_type}'. "
            f"Registered tasks: {registry.list_tasks()}"
        )
    
    # ── Step 2: Handle idempotency ────────────────────────────────────────────
    if request.idempotency_key:
        existing_task_id = await _check_idempotency(request.idempotency_key)
        if existing_task_id:
            # This exact task was already submitted. Return the original.
            # The caller gets the same response as if they submitted it fresh.
            # This is safe to call multiple times — only runs once.
            logger.info(
                "idempotent_task_returned",
                idempotency_key=request.idempotency_key,
                existing_task_id=existing_task_id,
            )
            return await _get_task_as_response(existing_task_id)
    
    # ── Step 3: Generate unique task ID ───────────────────────────────────────
    task_id = str(uuid.uuid4())
    # UUID v4 is randomly generated — no coordination needed between workers/servers.
    # Probability of collision: 1 in 2^122. Effectively impossible.
    # Alternative: sequential IDs from PostgreSQL — globally unique but requires
    # a DB roundtrip just to generate an ID, creating a bottleneck.
    # UUID is the right choice here.
    
    # ── Step 4: Determine target queue ────────────────────────────────────────
    queue_name = PRIORITY_TO_QUEUE[request.priority]
    # O(1) dict lookup. Clean and fast.
    
    # ── Step 5: Serialize task data ───────────────────────────────────────────
    task_data = _serialize_task(task_id, request)
    
    # ── Step 6: Write to Redis Stream (the actual enqueue) ────────────────────
    redis = redis_client.client
    
    stream_id = await redis.xadd(
        queue_name,
        # XADD <stream_name> <id> <field> <value> [<field> <value> ...]
        # Appends a new entry to the stream.
        
        task_data,
        # The dict of field-value pairs. Redis stores these as the message body.
        
        id="*",
        # id="*" means "auto-generate an ID".
        # Redis Stream IDs are: <millisecond_timestamp>-<sequence_number>
        # Example: "1699900000000-0", "1699900000000-1"
        # This gives us natural time ordering for free —
        # messages created earlier have lower IDs.
        # We could use task_id as the stream ID but that loses time ordering.
        
        maxlen=10_000,
        # Trim the stream to at most 10,000 entries.
        # Without this, a queue backlog would grow unboundedly and eat all memory.
        # 10,000 is conservative for dev. Production might be 100,000+.
        # approximate=True (default) means Redis trims "approximately" — 
        # it won't trim mid-batch for performance. Acceptable for our use case.
    )
    # stream_id is the Redis-generated message ID (e.g., "1699900000000-0")
    # We don't need to store it — we use task_id for lookups.
    
    # ── Step 7: Store task metadata for status lookups ────────────────────────
    task_key = f"dtq:task:{task_id}"
    # Key naming convention: app_prefix:entity_type:entity_id
    # "dtq" = distributed task queue (our app namespace)
    # "task" = entity type
    # task_id = the specific task
    
    await redis.hset(task_key, mapping=task_data)
    # HSET stores a hash (dict) at a key.
    # This lets us do HGET dtq:task:{id} field to get individual fields,
    # and HGETALL dtq:task:{id} to get all fields at once.
    # Why store separately from the stream?
    # Streams are for worker consumption (ordered, claimable).
    # Hashes are for fast lookups by ID (O(1) access by task_id).
    # Two different access patterns → two different data structures.
    
    await redis.expire(task_key, 86400 * 7)
    # Expire task metadata after 7 days (86400 seconds/day × 7).
    # Tasks are transient — you don't need them forever.
    # Without TTL, your Redis would eventually run out of memory.
    # This is a key production habit: everything in Redis should have a TTL.
    
    # ── Step 8: Store idempotency key mapping ─────────────────────────────────
    if request.idempotency_key:
        idempotency_key = f"dtq:idempotency:{request.idempotency_key}"
        await redis.set(idempotency_key, task_id, ex=86400)
        # Map the idempotency key → task_id for 24 hours.
        # After 24h, re-submission creates a new task (reasonable deduplication window).
    
    logger.info(
        "task_enqueued",
        task_id=task_id,
        task_type=request.task_type,
        priority=request.priority.value,
        queue=queue_name,
        stream_id=stream_id,
    )
    
    return TaskSubmitResponse(
        task_id=task_id,
        status=TaskStatus.PENDING,
        priority=request.priority,
        queue=queue_name,
        created_at=datetime.now(timezone.utc),
    )


async def get_task_status(task_id: str) -> Optional[dict]:
    """
    Retrieve current task state from Redis.
    Returns None if task not found (404 case).
    """
    redis = redis_client.client
    task_key = f"dtq:task:{task_id}"
    
    task_data = await redis.hgetall(task_key)
    # HGETALL returns all fields of a hash as a dict.
    # Returns {} (empty dict) if key doesn't exist.
    
    if not task_data:
        return None
    
    return task_data


async def _check_idempotency(idempotency_key: str) -> Optional[str]:
    """Check if a task with this idempotency key was already submitted."""
    redis = redis_client.client
    key = f"dtq:idempotency:{idempotency_key}"
    return await redis.get(key)
    # Returns task_id string if found, None if not found.


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


async def get_queue_depths() -> dict[str, int]:
    """
    Returns the number of pending tasks in each queue.
    Used by the monitoring endpoint.
    """
    redis = redis_client.client
    depths = {}
    
    for priority, queue_name in PRIORITY_TO_QUEUE.items():
        length = await redis.xlen(queue_name)
        # XLEN returns the number of entries in a stream.
        depths[priority.value] = length
    
    return depths