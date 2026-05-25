import abc
import asyncio
import json
from typing import Optional, Dict, Any
from app.config import settings
from app.core.logger import logger

class BaseMarketQueue(abc.ABC):
    """
    Abstract Base Class for the Market Data Message Queue.
    Decouples raw tick ingestion from processing.
    """
    @abc.abstractmethod
    async def put(self, item: Dict[str, Any]) -> None:
        """Pushes an item into the queue."""
        pass

    @abc.abstractmethod
    async def get(self) -> Dict[str, Any]:
        """Pulls an item from the queue. Blocks until an item is available."""
        pass

    @abc.abstractmethod
    async def size(self) -> int:
        """Returns the current size of the queue."""
        pass

class InMemoryMarketQueue(BaseMarketQueue):
    """
    Pure Python asyncio.Queue implementation.
    Ideal for local development, fallback mode, or unit tests.
    """
    def __init__(self, maxsize: int = 10000):
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        logger.info("Initialized In-Memory Market Queue.")

    async def put(self, item: Dict[str, Any]) -> None:
        try:
            await self._queue.put(item)
        except asyncio.QueueFull:
            logger.warning("In-Memory Queue is full! Dropping item to prevent memory leak.")

    async def get(self) -> Dict[str, Any]:
        return await self._queue.get()

    async def size(self) -> int:
        return self._queue.qsize()

class RedisMarketQueue(BaseMarketQueue):
    """
    Production-grade Redis-backed queue utilizing List LPUSH/RPOP.
    Provides persistence, durability, and allows distributed consumer workers.
    """
    def __init__(self, key: str = "market_data_ticks"):
        self.key = key
        # Import redis dynamically to handle cases where it isn't installed or needed
        import redis.asyncio as aioredis
        self.redis_client = aioredis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            decode_responses=True,
            socket_timeout=5.0
        )
        logger.info(f"Initialized Redis Queue connection to {settings.REDIS_HOST}:{settings.REDIS_PORT}")

    async def ping(self) -> bool:
        """Checks if Redis is alive."""
        try:
            return await self.redis_client.ping()
        except Exception:
            return False

    async def put(self, item: Dict[str, Any]) -> None:
        payload = json.dumps(item)
        await self.redis_client.lpush(self.key, payload)

    async def get(self) -> Dict[str, Any]:
        # BRPOP blocks until an item is available. Returns (key, value)
        result = await self.redis_client.brpop(self.key, timeout=0)
        if result:
            _, payload = result
            return json.loads(payload)
        return {}

    async def size(self) -> int:
        return await self.redis_client.llen(self.key)

async def get_market_queue() -> BaseMarketQueue:
    """
    Factory function to retrieve the configured queue backplane.
    Automatically falls back to InMemoryMarketQueue if Redis is configured
    but unreachable or missing.
    """
    if settings.ENVIRONMENT == "test":
        logger.info("Test environment detected. Returning In-Memory Queue.")
        return InMemoryMarketQueue()

    try:
        redis_queue = RedisMarketQueue()
        if await redis_queue.ping():
            return redis_queue
        else:
            raise ConnectionError("Redis ping failed.")
    except Exception as e:
        if settings.REDIS_USE_FALLBACK:
            logger.warning(
                f"Redis connection failed ({e}). Falling back to InMemoryMarketQueue as configured."
            )
            return InMemoryMarketQueue()
        else:
            logger.error(f"Failed to connect to Redis: {e}. Fallback is disabled.")
            raise e
