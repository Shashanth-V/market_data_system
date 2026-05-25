import sys
import os
from pathlib import Path
import pytest
import asyncio
from typing import AsyncGenerator, Generator
from sqlmodel import SQLModel, create_engine, Session

# Add project root to sys.path to resolve imports correctly in test environment
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import settings
from app.core.queue import InMemoryMarketQueue
from app.processing.engine import MarketAnalyticsEngine
from app.models.market import MarketTick, Candle1Min, SymbolMetrics

# Override configurations for tests
settings.ENVIRONMENT = "test"
settings.DEFAULT_SYMBOLS = "BTC-USD,ETH-USD"

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(name="db_session")
def db_session_fixture() -> Generator[Session, None, None]:
    """
    Creates an in-memory SQLite database and yields a synchronous database session.
    Perfect for unit and integration testing without running external services.
    """
    # Override settings sync database URL to memory SQLite for tests
    test_engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    
    # Create all tables in sqlite
    SQLModel.metadata.create_all(test_engine)
    
    with Session(test_engine) as session:
        yield session
        
    SQLModel.metadata.drop_all(test_engine)

@pytest.fixture(name="market_queue")
def market_queue_fixture() -> InMemoryMarketQueue:
    """Provides an isolated in-memory market queue for trade ingestion."""
    return InMemoryMarketQueue(maxsize=100)

@pytest.fixture(name="analytics_engine")
def analytics_engine_fixture() -> MarketAnalyticsEngine:
    """Provides a fresh, clean analytical calculation engine."""
    engine = MarketAnalyticsEngine(max_buffer_size=100)
    engine.clear()
    return engine
