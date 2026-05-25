import pytest
from app.ingestion.stream_simulator import MarketDataSimulator
from app.ingestion.collector import MarketDataCollector
from app.core.queue import InMemoryMarketQueue

def test_simulator_tick_generation():
    """Verifies that the stream simulator generates correct tick schema and reasonable prices."""
    simulator = MarketDataSimulator()
    tick = simulator.generate_tick("BTC-USD")
    
    assert tick["symbol"] == "BTC-USD"
    assert "price" in tick
    assert "volume" in tick
    assert "timestamp" in tick
    assert tick["provider"] == "simulator"
    assert tick["price"] > 0
    assert tick["volume"] > 0

def test_collector_normalization_valid_tick(market_queue):
    """Verifies valid Binance REST ticks are normalized correctly."""
    collector = MarketDataCollector(market_queue)
    
    raw_tick = {
        "symbol": "BTCUSDT",
        "price": "63250.50"
    }
    
    normalized = collector.validate_and_normalize(raw_tick)
    
    assert normalized is not None
    assert normalized["symbol"] == "BTC-USD"
    assert normalized["price"] == 63250.50
    assert normalized["provider"] == "binance"
    assert normalized["volume"] > 0
    assert "timestamp" in normalized

def test_collector_normalization_invalid_ticks(market_queue):
    """Verifies corrupted or untracked ticks are rejected gracefully."""
    collector = MarketDataCollector(market_queue)
    
    # Untracked symbol
    untracked = collector.validate_and_normalize({"symbol": "DOGEUSDT-CORRUPTED", "price": "10.0"})
    assert untracked is None
    
    # Non-numeric price
    corrupted_price = collector.validate_and_normalize({"symbol": "BTCUSDT", "price": "not-a-number"})
    assert corrupted_price is None

    # Negative price
    negative_price = collector.validate_and_normalize({"symbol": "BTCUSDT", "price": "-500.00"})
    assert negative_price is None

def test_collector_failure_state_trigger(market_queue):
    """Verifies consecutive failures increment correctly."""
    collector = MarketDataCollector(market_queue)
    assert collector.consecutive_failures == 0
    
    # Trigger an exception mock simulation
    with pytest.raises(Exception):
        # Trigger failure fetch block by giving completely invalid symbol
        import asyncio
        asyncio.run(collector.fetch_binance_ticker("INVALID_EXCHANGE_TICKER"))
        
    assert collector.consecutive_failures > 0
