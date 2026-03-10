import asyncio
import structlog
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import router
from src.queue.broker import redis_client
from src.config import settings


structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.dev.ConsoleRenderer(),
        # ConsoleRenderer = preety coloured output in development
        # In production: use structlog.processors.JSONRender()
    ]
)

logger=structlog.get_logger(__name__)

@asynccontextmanager
async def lifespan(app:FastAPI):
    """
    FastAPI's lifespan context manager
    code before yeild runs on startup
    code after yeild runs before shutdown
    Why is startup/shutdown management important?
    
    Without it:
    - App starts before Redis is ready → first requests fail
    - App shuts down before connections close → resource leaks
    - Graceful shutdown becomes impossible
    """
    # Startup
    logger.info("App Starting", app_name=settings.APP_NAME,debug=settings.DEBUG)
    
    # Connect to redis
    await redis_client.connect()
    logger.info("redis_ready!")
    
    await _initialize_queues()
    logger.info("app_ready!")
    
    yield
    
    # Shutdown
    logger.info("app_shutting_down")
    await redis_client.disconnect()
    logger.info("App_stopped")

async def _initialize_queues():
    """
    Create Redis Stream consumer groups if they don't exist.
    
    What is a consumer group?
    A Redis Streams concept. It lets multiple workers (consumers) 
    read from the same stream WITHOUT each getting every message.
    Instead, each message goes to exactly ONE worker.
    
    Without consumer groups: all 5 workers would read every task → 
    every task executes 5 times. With consumer groups: each task 
    goes to exactly one worker.
    
    We're creating the group infrastructure here. Workers will JOIN 
    these groups in Day 3.
    """
    redis = redis_client.client
    from src.config import settings
    
    queues = [settings.QUEUE_HIGH, settings.QUEUE_MEDIUM, settings.QUEUE_LOW]
    group_name = "workers"
    # All workers belong to the "workers" consumer group.
    # This means they compete for tasks (one task → one worker).
    
    for queue in queues:
        try:
            await redis.xgroup_create(
                queue,
                group_name,
                id="$",
                # id="$" means "only deliver messages that arrive AFTER this group was created"
                # id="0" would mean "deliver ALL historical messages too"
                # For a task queue, we want "$" — don't replay old tasks on restart.
                mkstream=True,
                # mkstream=True: create the stream if it doesn't exist.
                # Without this, XGROUP_CREATE fails on non-existent streams.
            )
            logger.info("consumer_group_created", queue=queue, group=group_name)
        
        except Exception as e:
            if "BUSYGROUP" in str(e):
                # "BUSYGROUP Consumer Group name already exists"
                # This is fine — group already exists from a previous startup.
                # Not an error, just skip.
                logger.debug("consumer_group_exists", queue=queue)
            else:
                # A real error — re-raise it
                raise


# Create the FastAPI application
app = FastAPI(
    title="Distributed Task Queue",
    description="""
    A production-grade asynchronous task processing system built from scratch.
    
    ## How it works
    1. Submit a task via POST /api/v1/tasks
    2. Get back a task_id immediately
    3. Poll GET /api/v1/tasks/{task_id} for status and result
    
    ## Priority levels
    - **high**: Executed first, for time-sensitive tasks
    - **medium**: Default, for normal tasks  
    - **low**: Background tasks, executed when queue is clear
    """,
    version="1.0.0",
    lifespan=lifespan,
    # Tell FastAPI to use our lifespan context manager
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register our routes
app.include_router(router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",   
        port=8000,
        reload=settings.DEBUG,
    )



