from pydantic import BaseModel, Field, field_validator

from typing import Optional, Any
from datetime import datetime
from enum import Enum
# Enum: a fixed set of allowed values

class Priority(str, Enum):
    """
    Task priority levels. Inheriting from both str and Enum means:
    - It behaves like an Enum (fixed values, comparison safety)
    - It serializes to a plain string in JSON ("high", not "Priority.high")
    """
    HIGH="high"
    MEDIUM="medium"
    LOW="low"

class TaskStatus(str,Enum):
    """
    All possible states a task can be in.
    This is a state machine — tasks move through these states in order.
    
    State transitions:
    PENDING → RUNNING → COMPLETED
    PENDING → RUNNING → FAILED → PENDING (retry)
    PENDING → RUNNING → FAILED → DEAD (max retries exceeded)
    PENDING → CANCELLED (user cancelled before execution)
    """    
    PENDING="pending"
    COMPLETED="completed"
    FAILED="failed"
    RUNNING="running"
    DEAD="dead"
    CANCELLED="cancelled"

class TaskSubmitRequest(BaseModel):
    """TaskSubmitRequest Model
    A Pydantic BaseModel that defines the schema for task submission requests from clients.
    This model validates and structures all input data required to submit a task to the queue system.
    Attributes:
        task_type (str): 
            The identifier/name of the task function to execute.
            Must be a non-empty string between 1-100 characters.
            Determines which task handler will process this request.
        args (dict[str, Any]): 
            Keyword arguments to pass to the task function upon execution.
            Defaults to an empty dictionary if not provided.
            Allows flexible parameter passing to task handlers.
        priority (Priority): 
            Execution priority level for task scheduling.
            Defaults to Priority.MEDIUM if not specified.
            Higher priority tasks are executed before lower priority ones in the queue.
            Example: High-priority payment tasks execute before medium-priority batch jobs.
        max_retries (Optional[int]): 
            Maximum number of times to retry the task if it fails.
            Defaults to None (uses system-wide configuration setting).
            Valid range: 0-10 retries per task.
            Allows per-task override for flexibility:
                - Critical tasks (e.g., payments) can request max_retries=10
                - Non-critical tasks (e.g., analytics) can request max_retries=0
        timeout (Optional[str]): 
            Task execution timeout in seconds before termination.
            Defaults to None (uses system-wide configuration setting).
            Valid range: 1-3600 seconds (maximum 1 hour).
            Override the system default for specific long/short-running tasks.
            If a task requires > 1 hour, consider redesigning it for better performance.
        idempotency_key (Optional[str]): 
            Unique identifier for ensuring idempotent task submissions.
            Defaults to None (no idempotency guarantee).
            Maximum length: 255 characters.
            When provided, prevents duplicate task execution:
                - If a request with the same key is resubmitted, returns the original task instead
                - Enables safe client-side retries without creating duplicate tasks
            Recommended for critical operations like payment processing."""
    """
    What a client sends when submitting a task
    This is the INPUT schema
    """    

    task_type:str=Field(
        ...,
        default_factory=dict,
        description="Arguments to pass to the task function",
        min_length=1,
        max_length=100,
    )
    
    args:dict[str,Any]=Field(
        default_factory=dict,
        description="Arguments to pass to  the task function"
    )
    
    priority:Priority=Field(
        default=Priority.MEDIUM,
        description="Execution priority. High tasks execute before medium/low.",
    )
    
    max_retries:Optional[int]=Field(
        default=None,
        # None means "use the system default from config"
        # Allowing per-task override gives callers flexibility.
        # A critical payment task might want max_retries=10.
        # A non-critical analytics task might want max_retries=0.
        ge=0,   # ge = "greater than or equal to" — can't have negative retries
        le=10,  # le = "less than or equal to" — sanity cap at 10
    )
    
    timeout:Optional[str]=Field(
        default=None,
        description=(
            "Task timeout in seconds. Overrides system default."
        ),
        ge=1,
        le=3600, # Max 1 hour. If ur task takes > 1 hour, rethink ur design.
    )
    
    idempotency_key:Optional[str]=Field(
        default=None,
        description=(
            "If provided, duplicate submissions with the same key"
            "Return the original task instead of new one"
            "Use this for safe retries from client side"
        ),
        max_length=255,
    )
    
    @field_validator('task_type')
    @classmethod
    def task_type_must_be_valid(cls,v:str)->str:
        """
            Task types become function names internally
            Prevents inkection attacks or lookup errors by enforcing pythons field validators
        """
        if not v.replace("_","").replace(".","").isalnum():
            raise ValueError(
                f"task_type must contail only letters, numbers, undeerscores and dots. Got:{v!r}"
            )
        return v.lower()
    
class TaskSubmitResponse(BaseModel):
    """
        This is what we return to client after submitting the task
        This is the Output schema for POST/tasks.
        
        Design principle: return enough info for the client to:
    1. Track the task (task_id)
    2. Know where it is in the system (status, queue)
    3. Know where to check back (a hint about the status endpoint)
    """
    task_id:str
    status:TaskStatus
    priority:Priority
    queue:str
    created_at:datetime
    message:str="Task submitted successfully"
    
class TaskStatusResponse(BaseModel):
    """
    Whats we return for GET/tasks/{task_id}
    More detaieled than submission response
    """    
    task_id:str
    task_type:str
    status:TaskStatus
    priority:Priority
    args:dict[str,Any]
    result:Optional[Any]=None   # Result is Null until task is complete. Then it holds the return value
    error:Optional[str]=None# Error is null unless a task is failed. Then it holds the exception message
    retry_count:int=0
    max_count:int=3
    started_at:Optional[datetime]=None
    completed_at:Optional[datetime]=None
    
    @property
    def duration_seconds(self):
        """To calculate the time taken to do the task"""
        if self.started_at and self.completed_at:
            return (self.started_at - self.completed_at).total_seconds()
        return None
            
class HealthResponse(BaseModel):
    """Response for GET/health - shows system component status"""
    status:str
    redis:bool
    postgres:bool
    version:str="1.0.0"        

