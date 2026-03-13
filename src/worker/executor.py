# This is the engine room of the worker
# It receives a task dict, finds the right fumction,calls it, returns result.
# It knows about python functions(running, timing, error catching)

import asyncio
import time
import traceback
from datetime import datetime,timezone
from typing import Optional,Any
import json

import structlog
from src.config import settings
from src.tasks.registry import registry

logger=structlog.get_logger(__name__)

class TaskExecutionResult:
    """
        A structured container for the outcome of the executing task
        
        Why class instead of dict?
        Type safety. when consumer.py recives this object, it knows exactly what files exist. With dict there is room for typos as it gets none instead of errors.The class makes a complie-time AttributeError
    """
    
    def __init__(self,
                 task_id:str,
                 success:bool,
                 result:Any=None,
                 error:str |None=None,
                 error_traceback:str|None = None,
                 duration_seconds:float=0.0
                 ):
        self.task_id=task_id
        self.success=success
        self.result=result
        self.error=error
        self.error_traceback=error_traceback
        self.duration_seconds=duration_seconds
        self.completed_at=datetime.now(timezone.utc)

        
    def __repr__(self):
        status="✓" if self.success else "✕"    
        return(
            f"TaskExecutionResult({status} task_id={self.task_id}),"
            f"duration={self.duration_seconds:.3f}s"
        )
async def execute_task(task_data:dict)->TaskExecutionResult:
    """
        Core execution function. Takes raw task data from Redis,
        runs the registered function, returns a structured output.
        
        This is async because:
        1. The tasks themselves are async (send_email, process_image, etc.)
        2. asyncio.wait_for() needs an async context for timeout enforcement
        3. Multiple tasks can run "concurrently" via the event loop
        (though for CPU-bound tasks, real parallelism needs multiprocessing)
    """
    
    task_id=task_data['task_id']
    task_type=task_data['task_type']
    
    try:
        args = json.loads(task_data['args'])
        
        
        
    except(json.JSONDecodeError,KeyError) as e:
        # If we can't even parse the args, the task is malformed.
        # This should never happen if producer.py works correctly,
        # but defensive programming means handling it anyway.
        return TaskExecutionResult(
            task_id=task_id,
            success=False,
            error=f"Failed to deserialize task args: {e}",
        )
    timeout = int(task_data.get("timeout", settings.TASK_TIMEOUT))
    logger.info(
        "task_execution_started",
        task_id=task_id,
        task_type=task_type,
        args=args,
        timeout=timeout
    )    
    
    start_time=time.monotonic()
    
    try:
        # Look up the task function
        task_func= registry.get(task_type)
        # Raises KeyError if task_type not registered
        # This is programming error so we let it propogate it as task failure
        
        result= await asyncio.wait_for(
            task_func(**args),
            timeout=float(timeout)            
            )
        duration=time.monotonic()-start_time
        
        logger.info(
            "Task Execution Completed",
            task_id=task_id,
            task_type=task_type,
            duration_seconds=round(duration,3),
            result=result
            )
        return TaskExecutionResult(
            task_id=task_id,
            success=True,
            result=result,
            duration_seconds=duration
        )
    except asyncio.TimeoutError:
        # Task took longer than its timeout.
        # This is NOT a bug in our worker — it's expected behavior.
        # The task itself misbehaved (infinite loop, hung network call, etc.)
        duration = time.monotonic() - start_time
        error_msg = f"Task timed out after {timeout} seconds"
        
        logger.warning(
            "task_execution_timeout",
            task_id=task_id,
            task_type=task_type,
            timeout=timeout,
            duration_seconds=round(duration, 3),
        )
        
        return TaskExecutionResult(
            task_id=task_id,
            success=False,
            error=error_msg,
            duration_seconds=duration,
        )
    
    except KeyError as e:
        # Task type not found in registry.
        # Production implication: this means a task was enqueued on one
        # codebase version but the worker is running an older version
        # that doesn't have that task registered yet.
        # Solution: always deploy workers BEFORE deploying API servers
        duration = time.monotonic() - start_time
        error_msg = f"Task type '{task_type}' not registered on this worker: {e}"
        
        logger.error(
            "task_type_not_registered",
            task_id=task_id,
            task_type=task_type,
        )
        
        return TaskExecutionResult(
            task_id=task_id,
            success=False,
            error=error_msg,
            duration_seconds=duration,
        )
    
    except Exception as e:
        # The task function itself raised an exception.
        # This is the most common failure case: bad args, external service down,
        duration = time.monotonic() - start_time
        
        # Capture full traceback for internal debugging
        full_traceback = traceback.format_exc()
        # format_exc() returns the current exception's traceback as a string.        
        # Only expose the error message externally (not the traceback)
        error_msg = f"{type(e).__name__}: {str(e)}"
        # type(e).__name__ gives "ValueError", "ConnectionError", etc.
        
        logger.error(
            "task_execution_failed",
            task_id=task_id,
            task_type=task_type,
            error=error_msg,
            duration_seconds=round(duration, 3),
            exc_info=True, 
        )
        
        return TaskExecutionResult(
            task_id=task_id,
            success=False,
            error=error_msg,
            error_traceback=full_traceback,
            duration_seconds=duration,
        )
            
        
        
        
        
        
        

