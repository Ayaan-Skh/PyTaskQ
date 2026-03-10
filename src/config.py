
from pydantic_settings import BaseSettings
# Pydantic v2's way to load settings from enviornment variables. 
# Better than os.getenv() because:
# 1. Gives type validation
# 2. All config in one place
# 3. IDE autocomplete works
# 4. Ful config surface area visible at a glance 

from pydantic import Field
# Lets us add metadata to settings

class Settings(BaseSettings):
    
    # ========Application settings===========
    APP_NAME:str="PytaskQ"
    DEBUG:bool=False
    
    
    # ======= Redis settings ==========
    REDIS_HOST:str="localhost"
    REDIS_PORT:int=6379
    REDIS_DB:int=0
    # Redis has 16 databases (0-15) by default.
    # Using DB 0 for everything is fine for now.
    # In production, you'd separate: DB 0 for queues, DB 1 for cache, etc.
    # to avoid key collisions and allow separate eviction policies.
    
    REDIS_PASSWORD:str | None = None
    
    # --- Queue Names ---
    # These are the actual Redis Stream names we'll create
    QUEUE_HIGH:str="dtq:queue:high"
    QUEUE_MEDIUM:str="dtq:queue:medium"
    QUEUE_LOW:str="dtq:queue:low"
    QUEUE_DEAD:str="dtq:queue:dead"
    # "dtq:" prefix is a namespace. 
    # Real-life analogy: like a folder structure for Redis keys.
    # Without namespacing, if two apps use the same Redis instance,
    # key "queue:high" might collide. "dtq:queue:high" is unique to our app.
    # Convention: use your app's abbreviation as prefix.
    
    # ======= Workers settings =======
    WORKER_CONCURRENCY:int=4
    # How many tasks a single worker process can handle simultaneously.
    # For I/O-bound tasks (HTTP calls, DB queries): set this high (20-100)
    # For CPU-bound tasks (ML inference, image processing): set this to CPU count
    # Why? CPU-bound tasks can't actually parallelize beyond CPU cores due to GIL.
    # We'll discuss this deeply when we build the worker.
    
    WORKER_HEARTBEAT_INTERVAL: int = 10  # seconds
    # How often workers announce "I'm alive" to Redis.
    # If a worker's heartbeat stops for 3x this interval (30s),
    # we consider it dead and reassign its tasks.
    # Too low: lots of Redis traffic. Too high: slow failure detection.
    # 10 seconds is a common production value.
    
    MAX_RETRIES: int = 3
    # How many times to retry a failed task before giving up.
    # After MAX_RETRIES failures, task goes to Dead Letter Queue.
    
    RETRY_BACKOFF_BASE: float = 2.0
    # For exponential backoff: wait = RETRY_BACKOFF_BASE ^ attempt_number
    # Attempt 1: wait 2^1 = 2 seconds
    # Attempt 2: wait 2^2 = 4 seconds  
    # Attempt 3: wait 2^3 = 8 seconds
    # This prevents a failing task from hammering a flaky external service.
    
    TASK_TIMEOUT: int = 300  # seconds (5 minutes)
    # If a task runs longer than this, kill it.
    # Without timeouts, a task with an infinite loop would block a worker forever.
    # Production reality: always set timeouts. Always.
    
    # --- PostgreSQL Settings ---
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "taskqueue"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    
    
    @property # makes this callable as settings.DATABASE_URL
    def DATABASE_URL(self)->str:
        """
        @property makes this callable as settings.DATABASE_URL (no parentheses)
        It's computed from other settings, not stored separately.
        asyncpg uses "postgresql+asyncpg://" for async connections.
        """
        return(
            f"postgressql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )
        
    @property
    def REDIS_URL(self)->str:
        # Build redis connection URL from components
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://:{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
    
    class Config:
        """Pydantic settings configuration"""
        env_file=".env"
        #  Read from env file if exists
        case_sensitive=False
        # REDIS_HOST and redis_host both map to the same setting
        # Less footgun-prone for dev environments


settings=Settings()         
# This creates a single global settings instance
# All modules imports this not the class
# This is a singleton pattern ensures one consistent config across the app.