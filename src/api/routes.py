# This file defines the HTTP API.
# It's intentionally thin — routes should only:
# 1. Validate input (Pydantic handles this automatically)
# 2. Call the appropriate service function (producer, etc.)
# 3. Return the response

# Business logic belongs in producer.py, NOT here.
# Why? So you could theoretically replace FastAPI with a CLI or gRPC
# and the business logic stays the same.

from fastapi import APIRouter, HTTPException, status
# APIRouter: a way to group related routes. We'll mount this on the main app.
# HTTPException: raise this to return HTTP error responses with proper status codes.
# status: constants like status.HTTP_404_NOT_FOUND (cleaner than raw 404)

from datetime import datetime, timezone
import structlog

from src.config import settings
from src.api.models import (
    TaskSubmitRequest,
    TaskSubmitResponse,
    TaskStatusResponse,
    TaskStatus,
    Priority,
    HealthResponse,
)
from src.queue.producer import enqueue_task, get_task_status, get_queue_depths
from src.queue.broker import redis_client
from src.tasks.registry import registry

logger = structlog.get_logger(__name__)

# APIRouter with prefix means all routes here start with /api/v1
# Versioning (/v1/) is critical for APIs:
# When you make breaking changes, you add /v2/ without breaking existing clients.
router = APIRouter(prefix="/api/v1", tags=["tasks"])
# tags=["tasks"] groups these endpoints together in the Swagger UI at /docs


@router.post(
    "/tasks",
    response_model=TaskSubmitResponse,
    status_code=status.HTTP_202_ACCEPTED,
    # 202 Accepted (not 200 OK or 201 Created) is the semantically correct
    # response code for async task submission.
    # 200 OK = "I did the thing you asked"
    # 201 Created = "I created the resource synchronously"
    # 202 Accepted = "I received your request and will process it asynchronously"
    # Using the right status code tells clients they should poll for the result.
    summary="Submit a task for async execution",
    description="""
    Enqueues a task for asynchronous execution by a worker.
    Returns immediately with a task_id you can use to check status.
    
    The task will be executed based on priority:
    - high: Executed before medium and low priority tasks
    - medium: Default priority
    - low: Executed after high and medium tasks are cleared
    """,
)
async def submit_task(request: TaskSubmitRequest) -> TaskSubmitResponse:
    """
    FastAPI automatically:
    1. Parses the JSON request body into a TaskSubmitRequest
    2. Validates all fields (required fields present, formats correct, etc.)
    3. Returns 422 Unprocessable Entity if validation fails (before this runs)
    4. Serializes our return value using TaskSubmitResponse schema
    """
    try:
        response = await enqueue_task(request)
        return response
    
    except ValueError as e:
        # ValueError from our code = client sent bad data (unknown task type, etc.)
        # 400 Bad Request = "your request is malformed or invalid"
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    
    except Exception as e:
        # Unexpected errors = our fault, not the client's
        # Log the full error internally, but don't expose internals to client.
        # Leaking stack traces to clients is a security risk.
        logger.error("task_submission_failed", error=str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to enqueue task. Please try again.",
        )


@router.get(
    "/tasks/{task_id}",
    response_model=TaskStatusResponse,
    summary="Get task status and result",
)
async def get_task(task_id: str) -> TaskStatusResponse:
    """
    Poll this endpoint to check if your task is done.
    Returns current status and result (if completed).
    
    {task_id} is a path parameter — FastAPI extracts it from the URL.
    GET /api/v1/tasks/abc-123 → task_id = "abc-123"
    """
    task_data = await get_task_status(task_id)
    
    if task_data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task '{task_id}' not found. "
                   f"It may have expired (tasks expire after 7 days).",
        )
    
    # Reconstruct the response from stored Redis hash data.
    # All values came back as strings from Redis — deserialize as needed.
    import json
    
    return TaskStatusResponse(
        task_id=task_data["task_id"],
        task_type=task_data["task_type"],
        status=TaskStatus(task_data["status"]),
        priority=Priority(task_data["priority"]),
        args=json.loads(task_data["args"]),
        # json.loads converts '{"to": "a@b.com"}' back to {"to": "a@b.com"}
        
        result=json.loads(task_data["result"]) if task_data.get("result") else None,
        error=task_data.get("error") or None,
        retry_count=int(task_data.get("retry_count", 0)),
        max_retries=int(task_data.get("max_retries", settings.MAX_RETRIES)),
        created_at=datetime.fromisoformat(task_data["created_at"]),
        started_at=datetime.fromisoformat(task_data["started_at"]) if task_data.get("started_at") else None,
        completed_at=datetime.fromisoformat(task_data["completed_at"]) if task_data.get("completed_at") else None,
    )


@router.get(
    "/tasks",
    summary="List queue depths across all priority levels",
)
async def list_queue_stats() -> dict:
    """Returns how many tasks are waiting in each priority queue."""
    depths = await get_queue_depths()
    return {
        "queue_depths": depths,
        "total_pending": sum(depths.values()),
        "registered_task_types": registry.list_tasks(),
    }


@router.delete(
    "/tasks/{task_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Cancel a pending task",
)
async def cancel_task(task_id: str):
    """
    Marks a task as cancelled if it hasn't started yet.
    Cannot cancel a running task (would need to send SIGTERM to the worker — Week 2).
    
    204 No Content = success with no response body.
    Standard HTTP convention for DELETE operations.
    """
    task_data = await get_task_status(task_id)
    
    if task_data is None:
        raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found")
    
    if task_data["status"] == TaskStatus.RUNNING.value:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            # 409 Conflict = "your request conflicts with current state"
            detail="Cannot cancel a running task. "
                   "Task cancellation for running tasks will be added in Week 2.",
        )
    
    if task_data["status"] in [TaskStatus.COMPLETED.value, TaskStatus.DEAD.value]:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Task is already in terminal state: {task_data['status']}",
        )
    
    # Mark as cancelled in Redis
    redis = redis_client.client
    task_key = f"dtq:task:{task_id}"
    await redis.hset(task_key, "status", TaskStatus.CANCELLED.value)
    # Workers will check this flag before executing — if cancelled, skip it.


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Check system health",
    tags=["monitoring"],
)
async def health_check() -> HealthResponse:
    """
    Used by:
    - Docker health checks
    - Kubernetes liveness/readiness probes  
    - Load balancers to decide whether to route traffic here
    - Monitoring systems (PagerDuty, etc.)
    
    Returns 200 if healthy, still returns 200 even if degraded
    (so the process stays up), but the body indicates which components are down.
    Some teams return 503 if any component is down — both approaches are valid.
    """
    redis_healthy = await redis_client.health_check()
    
    # We'll add PostgreSQL health check in Day 4 when we add DB integration
    postgres_healthy = True  # Placeholder
    
    overall = "healthy" if (redis_healthy and postgres_healthy) else "degraded"
    
    return HealthResponse(
        status=overall,
        redis=redis_healthy,
        postgres=postgres_healthy,
    )

from src.config import settings