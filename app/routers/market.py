from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_async_session
from app.config import settings
from app.models.market import MarketTick, Candle1Min, SymbolMetrics

router = APIRouter(prefix="/api/v1/market", tags=["market-data"])

@router.get("/symbols", response_model=List[str])
async def get_symbols():
    """
    Returns the list of active trading symbols configured in the system.
    """
    return settings.symbols_list

@router.get("/latest-ticks", response_model=List[MarketTick])
async def get_latest_ticks(
    symbol: Optional[str] = Query(None, description="Filter ticks by symbol"),
    limit: int = Query(50, ge=1, le=500, description="Max number of ticks to return"),
    session: AsyncSession = Depends(get_async_session)
):
    """
    Fetches the latest raw ticks from the database.
    """
    stmt = select(MarketTick).order_by(desc(MarketTick.timestamp))
    if symbol:
        stmt = stmt.where(MarketTick.symbol == symbol)
    
    stmt = stmt.limit(limit)
    result = await session.execute(stmt)
    return list(result.scalars().all())

@router.get("/candles", response_model=List[Candle1Min])
async def get_candles(
    symbol: str = Query(..., description="The symbol to fetch candles for"),
    limit: int = Query(100, ge=1, le=1000, description="Number of candles to return"),
    session: AsyncSession = Depends(get_async_session)
):
    """
    Fetches 1-minute historical OHLCV candlesticks for charts and trends.
    """
    if symbol not in settings.symbols_list:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not tracked by system.")
        
    stmt = (
        select(Candle1Min)
        .where(Candle1Min.symbol == symbol)
        .order_by(desc(Candle1Min.start_time))
        .limit(limit)
    )
    result = await session.execute(stmt)
    candles = list(result.scalars().all())
    
    # Return in chronological order (oldest to newest) for chart plotting
    candles.reverse()
    return candles

@router.get("/stats")
async def get_market_stats(
    symbol: str = Query(..., description="Symbol to query stats for"),
    session: AsyncSession = Depends(get_async_session)
):
    """
    Computes real-time statistics cards for dashboard rendering.
    Calculates 24-candle high, low, close, and trend details.
    """
    if symbol not in settings.symbols_list:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found.")

    # 1. Fetch latest tick
    tick_stmt = select(MarketTick).where(MarketTick.symbol == symbol).order_by(desc(MarketTick.timestamp)).limit(1)
    tick_res = await session.execute(tick_stmt)
    latest_tick = tick_res.scalars().first()

    if not latest_tick:
        return {
            "symbol": symbol,
            "message": "No trade data available yet.",
            "latest_price": None,
            "high_24h": None,
            "low_24h": None,
            "volume_24h": 0.0,
            "trend_signal": "NEUTRAL"
        }

    # 2. Fetch latest computed analytics metrics
    metrics_stmt = select(SymbolMetrics).where(SymbolMetrics.symbol == symbol).order_by(desc(SymbolMetrics.timestamp)).limit(1)
    metrics_res = await session.execute(metrics_stmt)
    latest_metrics = metrics_res.scalars().first()

    # 3. Calculate 24-minute window aggregates from candles (simulated 24-hour block)
    # We look at the last 24 1-min candles.
    candle_stmt = (
        select(
            func.max(Candle1Min.high).label("high"),
            func.min(Candle1Min.low).label("low"),
            func.sum(Candle1Min.volume).label("volume")
        )
        .where(Candle1Min.symbol == symbol)
        .order_by(desc(Candle1Min.start_time))
        .limit(24)
    )
    candle_res = await session.execute(candle_stmt)
    aggregates = candle_res.first()

    high_val = float(aggregates.high) if aggregates and aggregates.high is not None else latest_tick.price
    low_val = float(aggregates.low) if aggregates and aggregates.low is not None else latest_tick.price
    vol_val = float(aggregates.volume) if aggregates and aggregates.volume is not None else latest_tick.volume

    return {
        "symbol": symbol,
        "latest_price": latest_tick.price,
        "latest_volume": latest_tick.volume,
        "timestamp": latest_tick.timestamp.isoformat(),
        "high_24h": round(high_val, 4),
        "low_24h": round(low_val, 4),
        "volume_24h": round(vol_val, 4),
        "moving_average_5m": latest_metrics.moving_average_5m if latest_metrics else None,
        "moving_average_15m": latest_metrics.moving_average_15m if latest_metrics else None,
        "volatility_5m": latest_metrics.volatility_5m if latest_metrics else None,
        "trend_signal": latest_metrics.trend_signal if latest_metrics else "NEUTRAL"
    }

@router.get("/health")
async def health_check(session: AsyncSession = Depends(get_async_session)):
    """
    Confirms backend health by testing database query latency.
    """
    try:
        start_time = datetime.utcnow()
        await session.execute(select(1))
        db_latency_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        return {
            "status": "healthy",
            "database_connected": True,
            "database_latency_ms": round(db_latency_ms, 2)
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database connection degraded: {str(e)}"
        )
