# Manages multiple TaskConsumer instances running as concurrent coroutines.
#
# "Pool" in the name comes from the Thread Pool / Process Pool pattern.
# Instead of threads or processes, we use asyncio Tasks —
# lightweight coroutines that share a single thread's event loop.
#
# Real-life analogy: a call center with N agents (workers).
# The pool manager is the supervisor who starts all agents,
# keeps track of who's working, and handles agents who quit unexpectedly.

import asyncio
import signal
import structlog

from src.config import settings
from src.queue.consumer import TaskConsumer
from src.queue.broker import redis_client

logger = structlog.get_logger(__name__)


class WorkerPool:
    """
    Manages a pool of concurrent TaskConsumer coroutines.
    
    Key responsibilities:
    1. Start N workers as asyncio Tasks
    2. Handle OS signals (SIGTERM, SIGINT) for graceful shutdown
    3. Monitor workers and restart if they die unexpectedly
    4. Coordinate clean shutdown (finish in-progress tasks, then stop)
    """
    
    def __init__(self, concurrency: int | None = None):
        self.concurrency = concurrency or settings.WORKER_CONCURRENCY
        # How many workers to run simultaneously
        
        self._consumers: list[TaskConsumer] = []
        # Track consumer instances so we can call .stop() on them
        
        self._tasks: list[asyncio.Task] = []
        # Track asyncio Tasks (the running coroutines) for cancellation
        
        self._shutdown_event = asyncio.Event()
        # An asyncio Event is a synchronization primitive.
        # .set() marks it as "happened"
        # .wait() blocks until it's set
        # We use it to signal all workers to stop simultaneously.
    
    async def start(self):
        """
        Start the worker pool and block until shutdown signal.
        This is the main entry point for the worker process.
        """
        # ── Setup signal handlers ──────────────────────────────────────────
        self._setup_signal_handlers()
        # SIGTERM: sent by 'docker stop', 'kill <pid>', Kubernetes pod termination
        # SIGINT: sent by Ctrl+C
        # Without signal handlers, these immediately kill the process,
        # potentially mid-task. Our handlers trigger graceful shutdown instead.
        
        logger.info(
            "worker_pool_starting",
            concurrency=self.concurrency,
        )
        
        # ── Start all worker coroutines ────────────────────────────────────
        for i in range(self.concurrency):
            consumer = TaskConsumer(worker_id=i)
            self._consumers.append(consumer)
            
            # asyncio.create_task() schedules the coroutine to run
            # "concurrently" on the event loop.
            # It returns immediately — the coroutine runs in the background.
            task = asyncio.create_task(
                consumer.start(),
                name=f"worker-{i}",
                # Naming tasks makes debugging easier.
                # asyncio.all_tasks() shows names in its output.
            )
            self._tasks.append(task)
            
            # Add a callback for if the task exits unexpectedly
            task.add_done_callback(
                lambda t: self._on_worker_done(t)
            )
        
        logger.info(
            "worker_pool_started",
            workers=self.concurrency,
            worker_names=[c.consumer_name for c in self._consumers],
        )
        
        # ── Block until shutdown signal ────────────────────────────────────
        await self._shutdown_event.wait()
        # This await yields control until _shutdown_event.set() is called.
        # The workers are running concurrently while we wait here.
        
        # ── Graceful shutdown sequence ─────────────────────────────────────
        await self._shutdown()
    
    def _setup_signal_handlers(self):
        """Register OS signal handlers for graceful shutdown."""
        loop = asyncio.get_event_loop()
        
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(self._handle_signal(s))
                # lambda s=sig captures the current value of sig.
                # Without s=sig, all lambdas would reference the final value
                # of sig after the loop ends — a classic Python closure gotcha.
            )
    
    async def _handle_signal(self, sig: signal.Signals):
        """Called when SIGTERM or SIGINT received."""
        logger.info("shutdown_signal_received", signal=sig.name)
        self._shutdown_event.set()
        # Set the event → the await in start() unblocks → shutdown sequence begins
    
    def _on_worker_done(self, task: asyncio.Task):
        """
        Callback when a worker task finishes.
        
        Workers should run forever until stop() is called.
        If a worker exits on its own, that's unexpected — log it.
        """
        if task.cancelled():
            # Expected during shutdown — we cancelled it
            return
        
        exc = task.exception()
        if exc:
            logger.error(
                "worker_died_unexpectedly",
                worker=task.get_name(),
                error=str(exc),
                exc_info=exc,
            )
            # In production, you'd restart the worker here.
            # For now, we just log — the pool continues with N-1 workers.
    
    async def _shutdown(self):
        """
        Graceful shutdown: let in-progress tasks finish, then stop.
        
        Shutdown sequence:
        1. Signal all consumers to stop accepting new tasks
        2. Wait for currently running tasks to complete (up to 30s)
        3. Force-cancel any remaining tasks
        4. Clean up connections
        """
        logger.info("graceful_shutdown_starting", workers=len(self._consumers))
        
        # Step 1: Tell all consumers to stop after their current task
        for consumer in self._consumers:
            consumer.stop()
            # Sets consumer._running = False.
            # Each consumer finishes its current task, then exits its loop.
        
        # Step 2: Wait for all worker tasks to finish (with timeout)
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._tasks, return_exceptions=True),
                # gather() runs all tasks concurrently and waits for all to finish.
                # return_exceptions=True: don't raise if a task raises an exception,
                # just include it in the results list.
                timeout=30.0,
                # Give workers 30 seconds to finish in-progress tasks.
                # This is your "graceful shutdown window."
                # Kubernetes default terminationGracePeriodSeconds is also 30s.
            )
            logger.info("all_workers_stopped_cleanly")
        
        except asyncio.TimeoutError:
            # Workers didn't finish within 30s — force cancel them
            logger.warning(
                "shutdown_timeout_forcing_cancel",
                message="Some tasks may not have completed cleanly",
            )
            for task in self._tasks:
                task.cancel()
            
            # Wait for cancellations to complete
            await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Step 3: Disconnect from Redis
        await redis_client.disconnect()
        logger.info("worker_pool_shutdown_complete")