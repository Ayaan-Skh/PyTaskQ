# It answer the question given a task type which function should I call to execute the task. It also proivdes a way to register new task types and their corresponding functions.

# Given a task_type string like 'send_email', which Python function should I call?
from typing import Callable, Any
import structlog

logger=structlog.get_logger(__name__)
    
class TaskRegistry:
    """
    Maps task types to actual python functions
    
    why not just import functions directly?
    Because worker recives type_task as STRING form Redis(eg:send_mail)
    They need to look which function it corrresponds to. the registry acts as lookup table.
    """
    def __init__(self):
        self._tasks:dict[str,Callable]={}
        
    def register(self,name:str |None=None):
        """
        Decorator that register a function as task
        
        Usage:
        @registry.register
        async def send_mail(self,....)
        """
        def decorator(func:Callable):
            task_name=name or func.__name__
            
            if task_name in self._tasks:
                raise KeyError(
                    f"Task {task_name} already registered"
                    f"Use different name or chech duplicate registrations"
                )
            self._tasks[task_name]=func
            logger.debug("Registered Task ",task_name=task_name,func=func.__qualname__)  
            return func
            # We return the original function unchanged
            # The decorator just adds it to our registry as side effect
             
    
        return decorator
    
    
    def get(self, task_type:str)->Callable:
        """Look up task function by name. Raise if not found"""
        if  task_type not in self._tasks:
            available=list(self._tasks.keys())
            return ValueError(
                f'Unknown task type: {task_type}'
                f'Available tasks: {available}'
                f'Did you forget to register with @registry.register() ?'
            )
        return self._tasks[task_type]
    
    def list_tasks(self)->list[str]:
        """List al registered task names"""
        return list(self._tasks.keys())
    
    def __contains__(self, task_type)->bool:
        return task_type in self._tasks
            
# Module level singleton - shared across entire app  
registry=TaskRegistry()            
      
            
# ─────────────────────────────────────────────────────────────
# BUILT-IN EXAMPLE TASKS
# These are demo tasks for testing. In a real app, you'd define
# tasks in their own modules and import them.
# ─────────────────────────────────────────────────────────────

import asyncio
import time

@registry.register()
async def send_email(to: str, subject: str, body: str = "") -> dict:
    """
    Simulated email sending task.
    In production, this would call SendGrid/SES/Mailgun API.
    We simulate with a sleep to mimic network I/O.
    """
    logger.info("sending_email", to=to, subject=subject)
    await asyncio.sleep(0.1)  # Simulate API call latency
    return {"status": "sent", "to": to, "subject": subject}


@registry.register()
async def process_image(image_url: str, operations: list = None) -> dict:
    """Simulated image processing task. CPU-bound in reality."""
    logger.info("processing_image", url=image_url)
    await asyncio.sleep(0.5)  # Simulate processing time
    return {"status": "processed", "url": image_url, "operations_applied": operations or []}


@registry.register()
async def generate_report(report_type: str, params: dict = None) -> dict:
    """Simulated report generation. I/O + CPU bound in reality."""
    logger.info("generating_report", type=report_type)
    await asyncio.sleep(1.0)
    return {"status": "generated", "type": report_type, "rows": 1500}


@registry.register()
async def failing_task(should_fail: bool = True) -> dict:
    """
    A task that intentionally fails. 
    Critical for testing retry logic — you need a reliable way to 
    trigger failures in development.
    """
    if should_fail:
        raise ValueError("This task always fails. Used for testing retry logic.")
    return {"status": "succeeded"}


@registry.register()
async def slow_task(duration: int = 10) -> dict:
    """
    A task that sleeps for `duration` seconds.
    Used to test timeout handling — set task timeout < duration to trigger it.
    """
    await asyncio.sleep(duration)
    return {"status": "completed", "slept_for": duration}            
    

