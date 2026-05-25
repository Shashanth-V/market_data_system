import pytest
from datetime import datetime
from sqlmodel import select
from app.models.market import Candle1Min, MarketTick
from app.processing.pipeline import MarketDataPipeline
from app.core.queue import InMemoryMarketQueue

@pytest.mark.asyncio
async def test_pipeline_candle_rolling(db_session):
    """
    Verifies that the candle rolling algorithm accurately creates and updates
    1-minute OHLCV candle aggregations in database transactions.
    """
    # Instantiate pipeline with a dummy queue
    queue = InMemoryMarketQueue()
    pipeline = MarketDataPipeline(queue)

    ts = datetime(2026, 5, 25, 12, 30, 45) # 12:30:45
    symbol = "BTC-USD"

    # Step 1: Roll first price tick
    await pipeline._roll_candle(db_session, symbol, price=100.0, volume=1.5, ts=ts)
    db_session.commit()

    # Verify candle creation
    stmt = select(Candle1Min).where(Candle1Min.symbol == symbol)
    candle = db_session.execute(stmt).scalars().first()

    assert candle is not None
    assert candle.start_time == datetime(2026, 5, 25, 12, 30, 0)
    assert candle.open == 100.0
    assert candle.high == 100.0
    assert candle.low == 100.0
    assert candle.close == 100.0
    assert candle.volume == 1.5

    # Step 2: Roll second tick within same minute (12:30:55) with higher price
    ts_later = datetime(2026, 5, 25, 12, 30, 55)
    await pipeline._roll_candle(db_session, symbol, price=105.0, volume=2.5, ts=ts_later)
    db_session.commit()

    # Re-query
    db_session.expire_all()
    candle_updated = db_session.execute(stmt).scalars().first()

    assert candle_updated is not None
    assert candle_updated.high == 105.0
    assert candle_updated.low == 100.0
    assert candle_updated.close == 105.0
    assert candle_updated.volume == 4.0 # 1.5 + 2.5

    # Step 3: Roll third tick with a lower price (12:30:58)
    ts_third = datetime(2026, 5, 25, 12, 30, 58)
    await pipeline._roll_candle(db_session, symbol, price=95.0, volume=1.0, ts=ts_third)
    db_session.commit()

    db_session.expire_all()
    candle_final = db_session.execute(stmt).scalars().first()

    assert candle_final.high == 105.0
    assert candle_final.low == 95.0 # Updated low
    assert candle_final.close == 95.0
    assert candle_final.volume == 5.0
