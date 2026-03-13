# The consumer bridges Redis and the executor.
# It speaks "Redis language" (XREADGROUP, HSET, XACK)
# so the executor doesn't have to.
#
# Responsibility boundary:
# Consumer: "get task from Redis, write result back to Redis"
# Executor: "run the Python function, return what happened"

import asyncio
import json
import socket
from datetime import datetime, timezone
from typing import AsyncIterator

import structlog

from src.config import settings
from src.queue.broker import redis_client
from src.api.models import TaskStatus
from src.worker.executor import execute_task, TaskExecutionResult

logger = structlog.get_logger(__name__)

# Consumer group name — must match what we created in main.py
CONSUMER_GROUP = "workers"


class TaskConsumer:
    """
    Reads tasks from Redis Streams and coordinates execution.
    
    One TaskConsumer instance runs per worker coroutine.
    If you run 4 concurrent workers, you have 4 TaskConsumer instances,
    each with a unique consumer_name so Redis tracks them separately.
    
    Think of a TaskConsumer like a single restaurant waiter:
    - They watch the order queue (Redis Stream)
    - They pick up one order at a time (XREADGROUP)
    - They hand it to the kitchen (executor.py)
    - They deliver the result to the customer (update Redis hash)
    - They mark the order as done (XACK)
    """
    
    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        
        # Unique name for this consumer within the group.
        # Redis tracks pending messages per consumer by this name.
        # If this consumer crashes, its pending messages can be reclaimed
        # by another consumer using XAUTOCLAIM (Week 2).
        hostname = socket.gethostname()
        # Include hostname so in a multi-server setup you can tell
        # which physical machine the worker is on.
        self.consumer_name = f"worker-{hostname}-{worker_id}"
        
        self._running = False
        # Flag to control the run loop.
        # When False, the worker finishes its current task and stops.
        # This is how we implement graceful shutdown.

        logger.info("consumer_initialized", consumer_name=self.consumer_name)
    
    async def start(self):
        """
        Main worker loop. Runs forever until self._running = False.
        
        The loop structure:
        1. Try to read a task from any queue (high → medium → low priority)
        2. If got a task: execute it, store result, acknowledge
        3. If no task: wait briefly, try again
        4. Repeat
        
        Why check queues in priority order?
        High priority tasks must execute before medium tasks.
        If we read from all queues simultaneously, a medium task
        might execute before a high task that arrived milliseconds later.
        Sequential priority checking ensures strict ordering.
        """
        self._running = True
        logger.info("worker_started", consumer_name=self.consumer_name)
        
        while self._running:
            try:
                # Try queues in priority order: high → medium → low
                task_claimed = False
                
                for queue_name in [
                    settings.QUEUE_HIGH,
                    settings.QUEUE_MEDIUM,
                    settings.QUEUE_LOW,
                ]:
                    task_data = await self._claim_task(queue_name)
                    
                    if task_data:
                        task_claimed = True
                        await self._process_task(queue_name, task_data)
                        break
                        # Break after processing ONE task, then restart the
                        # priority loop from the top.
                        # Why? A high-priority task may have arrived while we
                        # were processing a medium task. We should check high
                        # priority first on the next iteration.
                
                if not task_claimed:
                    # No tasks in any queue. Sleep briefly to avoid
                    # hammering Redis with empty reads.
                    # This is "polling" — we'll discuss long-polling optimization
                    # and blocking reads below.
                    await asyncio.sleep(0.5)
                    # 0.5 seconds: low enough that tasks are picked up quickly,
                    # high enough that we don't do 2000 Redis reads/second per worker.
                    # Production tuning: use XREAD with blocking (COUNT 1, BLOCK 1000)
                    # instead of polling — we'll add this as an improvement.
            
            except asyncio.CancelledError:
                # CancelledError means the event loop is shutting down.
                # This is not an error — it's the signal to stop cleanly.
                logger.info("worker_cancelled", consumer_name=self.consumer_name)
                break
            
            except Exception as e:
                # Unexpected error in the worker loop itself (not in task execution).
                # We log it and continue — a worker loop crash would lose this worker
                # permanently. Better to keep running with logged errors.
                logger.error(
                    "worker_loop_error",
                    consumer_name=self.consumer_name,
                    error=str(e),
                    exc_info=True,
                )
                await asyncio.sleep(1)
                # Wait a bit before retrying to avoid tight error loops.
        
        logger.info("worker_stopped", consumer_name=self.consumer_name)
    
    def stop(self):
        """Signal the worker to stop after finishing its current task."""
        self._running = False
        logger.info("worker_stop_requested", consumer_name=self.consumer_name)
    
    async def _claim_task(self, queue_name: str) -> dict | None:
        """
        Attempt to claim ONE task from a specific queue.
        Returns the task data dict if successful, None if queue is empty.
        
        XREADGROUP is the atomic operation that:
        1. Reads the next undelivered message from the stream
        2. Assigns it to this consumer (moves to Pending Entries List)
        3. Returns the message data
        
        All in one atomic operation — no race condition possible.
        Two workers calling XREADGROUP simultaneously will each get
        a DIFFERENT message. Redis guarantees this.
        """
        redis = redis_client.client
        
        try:
            messages = await redis.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=self.consumer_name,
                streams={queue_name: ">"},
                # ">" is a special ID meaning "give me only NEW messages
                # (messages not yet delivered to any consumer in this group)"
                # "0" would mean "give me my pending (unacknowledged) messages"
                # We use ">" for normal operation.
                
                count=1,
                # Only claim ONE task at a time.
                # Why not batch (count=10)?
                # If we claim 10 tasks and then crash, all 10 are stuck
                # in our PEL until timeout. Claiming one at a time
                # minimizes the blast radius of a worker crash.
                # Trade-off: slightly less throughput, much better reliability.
                
                block=0,
                # block=0 means "don't block, return immediately if empty"
                # block=1000 would mean "wait up to 1000ms for a message"
                # We use 0 here because we check multiple queues in sequence.
                # If we blocked on the high queue, we'd never check medium/low
                # while high queue is empty.
            )
            
            # messages format from Redis:
            # [[b"stream_name", [(b"message_id", {b"field": b"value", ...})]]]
            # After decode_responses=True: strings instead of bytes
            # We need to extract the actual task data from this nested structure.
            
            if not messages:
                return None
            
            # messages = [["dtq:queue:high", [("1699900000-0", {...task_data...})]]]
            stream_name, entries = messages[0]
            # stream_name = "dtq:queue:high" (we already knew this)
            # entries = list of (message_id, data_dict) tuples
            
            if not entries:
                return None
            
            message_id, task_data = entries[0]
            # message_id = "1699900000-0" (Redis Stream entry ID)
            # task_data = {"task_id": "...", "task_type": "...", ...}
            
            # Attach the stream message_id to the task data.
            # We need this later to XACK the message.
            task_data["_stream_message_id"] = message_id
            task_data["_queue_name"] = queue_name
            
            logger.debug(
                "task_claimed",
                consumer_name=self.consumer_name,
                task_id=task_data.get("task_id"),
                queue=queue_name,
                message_id=message_id,
            )
            
            return task_data
        
        except Exception as e:
            logger.error(
                "task_claim_failed",
                consumer_name=self.consumer_name,
                queue=queue_name,
                error=str(e),
            )
            return None
    
    async def _process_task(self, queue_name: str, task_data: dict):
        """
        Full lifecycle of one task:
        1. Update status → running
        2. Execute
        3. Update status → completed/failed
        4. Acknowledge (XACK)
        """
        task_id = task_data["task_id"]
        message_id = task_data["_stream_message_id"]
        
        # ── Phase 1: Mark task as running ─────────────
        await self._update_task_status(
            task_id=task_id,
            status=TaskStatus.RUNNING,
            extra_fields={
                "worker_id": self.consumer_name,
                "started_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        
        # ── Phase 2: Execute the task ─────────
        result: TaskExecutionResult = await execute_task(task_data)
        
        # ── Phase 3: Update status based on result ─────────────────────────
        if result.success:
            await self._handle_success(task_id, result)
        else:
            await self._handle_failure(task_id, task_data, result)
        
        # ── Phase 4: Acknowledge the message ──────────────────────────────
        # XACK tells Redis: "I'm done with this message, remove it from PEL"
        # This happens AFTER we've written the result to Redis.
        # Order matters: if we XACK before writing the result and then crash,
        # the result is lost but the task won't be retried (already ACK'd).
        # Write result FIRST, then ACK.
        await self._acknowledge_message(queue_name, message_id)
    
    async def _handle_success(self, task_id: str, result: TaskExecutionResult):
        """Store successful result and mark task completed."""
        await self._update_task_status(
            task_id=task_id,
            status=TaskStatus.COMPLETED,
            extra_fields={
                "result": json.dumps(result.result),
                # Serialize result back to JSON string for Redis storage.
                # result.result could be any JSON-serializable value.
                "completed_at": result.completed_at.isoformat(),
            }
        )
        
        logger.info(
            "task_completed",
            task_id=task_id,
            duration_seconds=round(result.duration_seconds, 3),
        )
    
    async def _handle_failure(
        self,
        task_id: str,
        task_data: dict,
        result: TaskExecutionResult,
    ):
        """
        Handle a failed task. Either retry it or send to dead letter queue.
        
        The retry decision:
        - retry_count < max_retries → re-enqueue with incremented retry count
        - retry_count >= max_retries → send to dead letter queue
        
        This implements "at-least-once" delivery with a retry cap.
        """
        retry_count = int(task_data.get("retry_count", 0))
        max_retries = int(task_data.get("max_retries", settings.MAX_RETRIES))
        
        if retry_count < max_retries:
            # ── Retry path ────────
            new_retry_count = retry_count + 1
            
            # Exponential backoff delay before re-enqueueing.
            # We don't block the worker — we just note when the task
            # should next be attempted. Full backoff scheduling is Week 3.
            # For now: log it and re-enqueue immediately (simplified).
            backoff_seconds = settings.RETRY_BACKOFF_BASE ** new_retry_count
            # Attempt 1: 2^1 = 2s, Attempt 2: 2^2 = 4s, Attempt 3: 2^3 = 8s
            
            logger.warning(
                "task_failed_will_retry",
                task_id=task_id,
                error=result.error,
                retry_count=new_retry_count,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )
            
            # Update retry count and re-enqueue to the SAME priority queue
            await self._update_task_status(
                task_id=task_id,
                status=TaskStatus.FAILED,
                extra_fields={
                    "retry_count": str(new_retry_count),
                    "error": result.error or "",
                }
            )
            
            # Re-enqueue to the same queue
            # In Week 3 we'll add proper delay scheduling
            await self._reenqueue_task(task_data, new_retry_count)
        
        else:
            # ── Dead letter queue path ────────
            # Task has exhausted all retries. It goes to the DLQ
            # where a human or monitoring system can investigate.
            logger.error(
                "task_dead",
                task_id=task_id,
                error=result.error,
                retry_count=retry_count,
                max_retries=max_retries,
            )
            
            await self._update_task_status(
                task_id=task_id,
                status=TaskStatus.DEAD,
                extra_fields={
                    "error": result.error or "",
                    "error_traceback": result.error_traceback or "",
                    "completed_at": result.completed_at.isoformat(),
                }
            )
            
            # Write to dead letter queue for analysis
            await self._send_to_dead_letter_queue(task_id, task_data, result)
    
    async def _reenqueue_task(self, task_data: dict, new_retry_count: int):
        """Re-enqueue a failed task for retry."""
        redis = redis_client.client
        queue_name = task_data["_queue_name"]
        
        # Build updated task data for re-enqueue
        updated_task = {
            k: v for k, v in task_data.items()
            if not k.startswith("_")
            # Strip internal fields (_stream_message_id, _queue_name)
            # These are not valid Redis Stream field values
        }
        updated_task["retry_count"] = str(new_retry_count)
        updated_task["status"] = TaskStatus.PENDING.value
        updated_task["started_at"] = ""
        updated_task["worker_id"] = ""
        
        await redis.xadd(queue_name, updated_task, maxlen=10_000)
        logger.info("task_requeued", task_id=task_data["task_id"], retry=new_retry_count)
    
    async def _send_to_dead_letter_queue(
        self,
        task_id: str,
        task_data: dict,
        result: TaskExecutionResult,
    ):
        """Write failed task to the dead letter queue for analysis."""
        redis = redis_client.client
        
        dlq_entry = {
            k: v for k, v in task_data.items()
            if not k.startswith("_")
        }
        dlq_entry.update({
            "final_error": result.error or "",
            "failed_at": result.completed_at.isoformat(),
        })
        
        await redis.xadd(settings.DEAD_LETTER_QUEUE, dlq_entry)
        logger.info("task_sent_to_dlq", task_id=task_id)
    
    async def _update_task_status(
        self,
        task_id: str,
        status: TaskStatus,
        extra_fields: dict | None = None,
    ):
        """
        Update task metadata in Redis.
        
        Why HSET and not just SET?
        HSET updates individual fields in a hash WITHOUT overwriting the whole thing.
        If we used SET, we'd have to read → modify → write the entire task dict
        (read-modify-write cycle — has race conditions under concurrent access).
        HSET is atomic: only the specified fields change.
        """
        redis = redis_client.client
        task_key = f"dtq:task:{task_id}"
        
        fields = {"status": status.value}
        if extra_fields:
            fields.update(extra_fields)
        
        await redis.hset(task_key, mapping=fields)
    
    async def _acknowledge_message(self, queue_name: str, message_id: str):
        """
        XACK tells Redis this message has been fully processed.
        Removes it from the Pending Entries List (PEL).
        
        This is the "commit" in our transaction-like flow.
        Before XACK: message is in PEL, considered "in-flight"
        After XACK: message is done, PEL entry removed
        """
        redis = redis_client.client
        await redis.xack(queue_name, CONSUMER_GROUP, message_id)
        logger.debug(
            "message_acknowledged",
            queue=queue_name,
            message_id=message_id,
        )