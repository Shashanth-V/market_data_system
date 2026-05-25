import uuid
from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel

class MarketTick(SQLModel, table=True):
    """
    Represents a single raw tick / trade from an exchange or source.
    """
    __tablename__ = "market_tick"

    id: uuid.UUID = Field(
        default_factory=uuid.uuid4,
        primary_key=True,
        index=True
    )
    symbol: str = Field(index=True, max_length=20)
    price: float = Field(nullable=False)
    volume: float = Field(nullable=False)
    timestamp: datetime = Field(default_factory=datetime.utcnow, index=True)
    provider: str = Field(default="simulator", max_length=50)

class Candle1Min(SQLModel, table=True):
    """
    Represents a 1-minute aggregated OHLCV candle for charts and history.
    """
    __tablename__ = "candle_1min"

    id: uuid.UUID = Field(
        default_factory=uuid.uuid4,
        primary_key=True,
        index=True
    )
    symbol: str = Field(index=True, max_length=20)
    start_time: datetime = Field(nullable=False, index=True)
    end_time: datetime = Field(nullable=False)
    open: float = Field(nullable=False)
    high: float = Field(nullable=False)
    low: float = Field(nullable=False)
    close: float = Field(nullable=False)
    volume: float = Field(default=0.0)

class SymbolMetrics(SQLModel, table=True):
    """
    Calculated sliding window indicators and active trend signal.
    """
    __tablename__ = "symbol_metrics"

    id: uuid.UUID = Field(
        default_factory=uuid.uuid4,
        primary_key=True,
        index=True
    )
    symbol: str = Field(index=True, max_length=20)
    timestamp: datetime = Field(default_factory=datetime.utcnow, index=True)
    moving_average_5m: Optional[float] = Field(default=None, nullable=True)
    moving_average_15m: Optional[float] = Field(default=None, nullable=True)
    volatility_5m: Optional[float] = Field(default=None, nullable=True)
    trend_signal: str = Field(default="NEUTRAL", max_length=15) # BULLISH, BEARISH, NEUTRAL
