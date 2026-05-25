import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Callable, Awaitable
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import async_session_maker
from app.core.logger import logger
from app.core.queue import BaseMarketQueue
from app.models.market import MarketTick, Candle1Min, SymbolMetrics
from app.processing.engine import MarketAnalyticsEngine

class EventBroadcaster:
    """
    Publisher-Subscriber event broadcaster.
    Allows WebSocket handlers to register callbacks and receive real-time ticks & analytics.
    """
    def __init__(self):
        self._subscribers: List[Callable[[Dict[str, Any]], Awaitable[None]]] = []

    def subscribe(self, callback: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
        if callback not in self._subscribers:
            self._subscribers.append(callback)

    def unsubscribe(self, callback: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
        if callback in self._subscribers:
            self._subscribers.remove(callback)

    async def broadcast(self, data: Dict[str, Any]) -> None:
        tasks = []
        for cb in self._subscribers:
            tasks.append(asyncio.create_task(self._safe_call(cb, data)))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_call(self, cb: Callable[[Dict[str, Any]], Awaitable[None]], data: Dict[str, Any]) -> None:
        try:
            await cb(data)
        except Exception as e:
            logger.warning(f"Error executing broadcaster subscriber callback: {e}")

# Global broadcaster singleton
broadcaster = EventBroadcaster()

class MarketDataPipeline:
    """
    Coordinates real-time streaming consumption, analytics calculations,
    PostgreSQL transactional storage, and real-time WebSocket broadcasts.
    """
    def __init__(self, queue: BaseMarketQueue):
        self.queue = queue
        self.engine = MarketAnalyticsEngine()
        self.is_running = False
        self._consume_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self.is_running = True
        self._consume_task = asyncio.create_task(self._processing_loop())
        logger.info("Real-Time Processing Pipeline coordinator started.")

    async def stop(self) -> None:
        self.is_running = False
        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
        logger.info("Real-Time Processing Pipeline coordinator stopped.")

    async def _processing_loop(self) -> None:
        while self.is_running:
            try:
                # Dequeue raw tick
                tick_data = await self.queue.get()
                if not tick_data:
                    continue

                # Process the tick
                await self._process_tick(tick_data)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in pipelines processing loop: {e}")
                await asyncio.sleep(1.0) # Small safety sleep to prevent tight-loop CPU spinning

    async def _process_tick(self, tick_data: Dict[str, Any]) -> None:
        """
        Coordinates database writing, candle rolling, metrics generation,
        and real-time socket broadcasting for a single tick payload.
        """
        symbol = tick_data["symbol"]
        price = float(tick_data["price"])
        volume = float(tick_data["volume"])
        provider = tick_data.get("provider", "simulator")

        # Parse timestamp
        timestamp_str = tick_data.get("timestamp")
        if isinstance(timestamp_str, str):
            ts = datetime.fromisoformat(timestamp_str.rstrip("Z"))
        else:
            ts = datetime.utcnow()

        # 1. Update rolling window technical analytics
        enriched_metrics = self.engine.add_tick(tick_data)

        # 2. Database transaction saving (using single session)
        async with async_session_maker() as session:
            try:
                # A. Write MarketTick
                db_tick = MarketTick(
                    symbol=symbol,
                    price=price,
                    volume=volume,
                    timestamp=ts,
                    provider=provider
                )
                session.add(db_tick)

                # B. Write calculated metrics
                if enriched_metrics:
                    session.add(enriched_metrics)

                # C. Aggregation: Roll 1-minute OHLCV candle
                await self._roll_candle(session, symbol, price, volume, ts)

                # Commit database writes
                await session.commit()

            except Exception as e:
                await session.rollback()
                logger.error(f"Failed to persist tick/candle data to PostgreSQL: {e}")
                # We do not crash the pipeline, we keep processing subsequent events

        # 3. Broadcast enriched packet to active WebSocket streams
        broadcast_packet = {
            "tick": {
                "symbol": symbol,
                "price": price,
                "volume": volume,
                "timestamp": ts.isoformat(),
                "provider": provider
            },
            "metrics": {
                "moving_average_5m": enriched_metrics.moving_average_5m if enriched_metrics else None,
                "moving_average_15m": enriched_metrics.moving_average_15m if enriched_metrics else None,
                "volatility_5m": enriched_metrics.volatility_5m if enriched_metrics else None,
                "trend_signal": enriched_metrics.trend_signal if enriched_metrics else "NEUTRAL"
            }
        }
        await broadcaster.broadcast(broadcast_packet)

    async def _roll_candle(
        self,
        session: AsyncSession,
        symbol: str,
        price: float,
        volume: float,
        ts: datetime
    ) -> None:
        """
        Calculates or updates a 1-minute candle based on the incoming tick.
        """
        # Determine 1-minute bucket start
        candle_start = ts.replace(second=0, microsecond=0)
        candle_end = candle_start + timedelta(minutes=1)

        stmt = select(Candle1Min).where(
            Candle1Min.symbol == symbol,
            Candle1Min.start_time == candle_start
        )

        import inspect
        if inspect.iscoroutinefunction(session.execute):
            result = await session.execute(stmt)
        else:
            result = session.execute(stmt)
        candle = result.scalars().first()

        if candle:
            # Update existing candle
            candle.high = max(candle.high, price)
            candle.low = min(candle.low, price)
            candle.close = price
            candle.volume += volume
            session.add(candle)
        else:
            # Create new candle
            new_candle = Candle1Min(
                symbol=symbol,
                start_time=candle_start,
                end_time=candle_end,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=volume
            )
            session.add(new_candle)
