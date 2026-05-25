import pytest
from app.processing.engine import MarketAnalyticsEngine
from app.models.market import SymbolMetrics

def test_engine_single_tick(analytics_engine):
    """Verifies analytics behavior with a single tick."""
    tick = {
        "symbol": "BTC-USD",
        "price": 100.0,
        "volume": 1.5,
        "timestamp": "2026-05-25T12:00:00Z",
        "provider": "simulator"
    }
    
    metrics = analytics_engine.add_tick(tick)
    
    assert metrics is not None
    assert metrics.symbol == "BTC-USD"
    assert metrics.moving_average_5m == 100.0
    assert metrics.moving_average_15m == 100.0
    assert metrics.volatility_5m == 0.0
    assert metrics.trend_signal == "NEUTRAL"

def test_engine_moving_averages(analytics_engine):
    """Tests if simple moving averages and volatility calculate correctly over multiple ticks."""
    prices = [100.0, 102.0, 104.0, 106.0, 108.0]
    metrics = None
    
    for p in prices:
        tick = {
            "symbol": "BTC-USD",
            "price": p,
            "volume": 1.0,
            "timestamp": "2026-05-25T12:00:00Z"
        }
        metrics = analytics_engine.add_tick(tick)
        
    assert metrics is not None
    # SMA 5 should be mean of [100, 102, 104, 106, 108] = 104.0
    assert metrics.moving_average_5m == 104.0
    # Volatility should be std dev of [100, 102, 104, 106, 108] = 3.162277
    assert metrics.volatility_5m > 0
    # Trend requires 5 ticks and SMA 5 == SMA 15 (as there are only 5 ticks total)
    assert metrics.trend_signal == "NEUTRAL"

def test_engine_bullish_bearish_trends(analytics_engine):
    """Tests trend crossover detection (BULLISH/BEARISH)."""
    # Feed 15 ticks to establish historical SMA(15) base
    for _ in range(15):
        analytics_engine.add_tick({"symbol": "BTC-USD", "price": 100.0})
        
    # Prices rising dramatically (short term SMA 5 will exceed long term SMA 15)
    for p in [110.0, 115.0, 120.0, 125.0, 130.0]:
        metrics = analytics_engine.add_tick({"symbol": "BTC-USD", "price": p})
        
    assert metrics.trend_signal == "BULLISH"

    # Prices falling dramatically (short term SMA 5 will drop below long term SMA 15)
    for p in [90.0, 85.0, 80.0, 75.0, 70.0]:
        metrics = analytics_engine.add_tick({"symbol": "BTC-USD", "price": p})
        
    assert metrics.trend_signal == "BEARISH"
