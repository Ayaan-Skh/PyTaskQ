# The registry answers one question: "Given a task_type string like 'send_email', which python function should I call"

# This is the Registry pattern — a central lookup table mapping
# names to implementations. It's how Celery's @app.task decorator works
# under the hood.

from typing import Callable, Any
import structlog

logger=structlog.get_logger(__name__)

class TaskRegistry:
    """
    Maps task type names (strings) to actual Python functions (callables).
    
        Why not just import functions directly?
        Because workers receive task_type as a STRING from Redis 
        (e.g., "send_email"). They need to look up which function 
        that string corresponds to. The registry is that lookup table.
        
        Think of it like a phone book:
        - Name (task_type) → Phone number (function reference)
        - You look up the name, get the number, make the call
    """
    def __init__(self):
        self._tasks:dict[str,Callable]={}
        # Maps "send_email" → <function send_email at 0x...>

    def register(self, name:str | None=None):
        """
        Decorator that registers the function as task
        
        Usage:
            @registory.register()
            async def send_mail(to:str, subject:str):
            ....
            
            # OR with explicit name:
            @registry.register(name="my_custom_name")
            async def send_email(...):
                ...
        
        A decorator is a function that takes a function and returns a function.
        @registry.register() is "syntactic sugar" for:
            send_email = registry.register()(send_email)
        """
        def decorator(func: Callable) -> Callable:
            task_name = name or func.__name__
            # Use provided name, or fall back to the function's actual name
            
            if task_name in self._tasks:
                # Prevent accidental overwrites.
                # If you register "send_email" twice, the second silently
                raise ValueError(
                    f"Task '{task_name}' is already registered. "
                    f"Use a different name or check for duplicate registrations."
                )
            
            self._tasks[task_name] = func
            logger.debug("task_registered", task_name=task_name, func=func.__qualname__)
            return func
            # We return the original function unchanged.
            # The decorator just adds it to our registry as a side effect.
        
        return decorator
    
    def get(self, task_type:str) -> Callable:
        """
        Look up a task function by name. Raises if not found
        """
        if task_type is not self._tasks:
            available=list(self._tasks.keys())
            raise KeyError(
                f"Unknown task type: '{task_type}'. "
                f"Available tasks: {available}. "
                f"Did you forget to register the task with @registry.register()?"
            )
        return self._tasks[task_type]    
    
    def list_tasks(self)->list[str]:
        """Return all registered task names. Used by api for validation"""
        return list(self._tasks.keys())
    
    def __contains__(self, task_type:str)->bool:
        """Allows: if 'send_email' in registory:..."""
        return task_type in self._tasks

# Module level singleton - share across the entire app    
registory=TaskRegistry()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    