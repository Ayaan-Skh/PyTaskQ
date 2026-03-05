#  Module responsible for one thing: Managing Redis connection
import asyncio
from typing import Optional
import redis.asyncio as aioredis
# redis.asyncio is just async-await version of the redis python library
# So while waiting for redis response other works can be done
from src.config import settings
import structlog # For structured logging

logger=structlog.get_logger(__name__)

class RedisClient:
    """
    A wrapper the redis connection that provides:
    1. Lazy initialization(connect only when first needed)
    2. Connection pooling (Reuse connections)
    3. Health check
    4. Clean shutdown
    
    Better than directly calling redis.from_url() as every time it will create new connections and might exhaust connection limit
    
    """
    def __init__(self):
        self._client=Optional[aioredis.Redis]=None #The redis client connection instance
    
    async def connect(self)->None:
        """Initialize Redis connection pool"""
        if self._client is not None:
            # Already initialized connection
            # This guard prevents double initialization
            return
        self._client=aioredis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_response=True,
            max_conncetions=20,
            # Connection pool size.
            # A pool keeps N connections open, reuses them for requests.
            # Real-life analogy: like a pool of taxi cabs.
            # Instead of calling a new cab every time (slow), 
            # you have 20 cabs waiting. You take one, use it, return it.
            # 20 is fine for development. Production might need 50-200.
            
            socket_keepalive=True,
            # TCP keepalive: send periodic pings on idle connections.
            # Prevents firewalls from dropping idle connections after timeout.
            # Without this, a connection unused for 15 minutes might be
            # silently dropped by your cloud provider's NAT, causing 
            # mysterious "connection reset" errors.
            
            socke_connect_timeout=5,
            # If Redis doesn't respond to a connection attempt within 5s, fail fast.
            # Without this, you might wait forever on a dead Redis.
            
            retry_on_timeout=True
            # If a command times out, automatically retry once.
            # Handles transient network blips gracefully.
        )   
        
        # Verify the connection actually works right now.
        # from_url() doesn't actually connect — it just stores config.
        # The first command will trigger the actual TCP connection. 
        await self._client.ping()
        logger.info(f"Redis connected",url=settings.REDIS_URL)
        # structlog: instead of logger.info("Connected to Redis at %s", url),
        # we pass key=value pairs. These become JSON fields in production.
        # Makes logs machine-parseable: {"event": "redis_connected", "url": "..."}
        
        
    async def disconnect(self)->None:
        """Close all the connection cleanly"""
        if self._client:
            await self._client.aclose()
            # aclose() = async close. Waits for in-flight commands to finish.
            # Never use .close() on async clients — it's synchronous and may
            # cut off in-progress operations.
            self._client=None
            logger.info("Redis disconnected")
    
    @property
    def client(self)->aioredis.Redis:
        if self._client is None:
            raise RuntimeError(
                "Redis client not initialized",
                "Call await redis_client.connect() first"
            )     
        return self._client
    
    async def health_check(self) -> bool:
        """Returns True if Redis is reachable, False otherwise."""
        try:
            await self._client.ping()
            return True
        except Exception as e:
            logger.error("redis_health_check_failed", error=str(e))
            return False

# This is the module-level singleton instance.
# Every module that does `from src.queue.broker import redis_client`
# gets THIS same object — not a new one each time.
# This is how we share a single connection pool across the whole app.
redis_client = RedisClient()    