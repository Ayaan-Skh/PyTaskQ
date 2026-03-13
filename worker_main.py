# worker_main.py
# Entry point for the worker process.
# In production: API servers and workers run in SEPARATE containers.
# This separation means you can scale them independently:
# - 2 API servers (handles HTTP traffic)
# - 10 worker processes (handles task backlog)
#
# Run with: python worker_main.py

import asyncio
import structlog

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.dev.ConsoleRenderer(),
    ]
)

logger = structlog.get_logger(__name__)


async def main():
    from src.queue.broker import redis_client
    from src.worker.pool import WorkerPool
    from src.config import settings

    logger.info(
        "worker_process_starting",
        app=settings.APP_NAME,
        concurrency=settings.WORKER_CONCURRENCY,
    )

    # Connect to Redis before starting the pool
    await redis_client.connect()

    # Create and start the worker pool
    # This blocks until SIGTERM/SIGINT received
    pool = WorkerPool(concurrency=settings.WORKER_CONCURRENCY)
    await pool.start()


if __name__ == "__main__":
    asyncio.run(main())