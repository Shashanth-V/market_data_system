from collections import defaultdict
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from app.core.logger import logger
from app.models.market import MarketTick, SymbolMetrics

class MarketAnalyticsEngine:
    """
    Stateful real-time analytical engine.
    Maintains a rolling buffer of ticks per symbol and utilizes Pandas to
    asynchronously calculate moving averages, historical volatility, and technical signals.
    """
    def __init__(self, max_buffer_size: int = 100):
        self.max_buffer_size = max_buffer_size
        # Rolling storage for raw prices and timestamps per symbol
        self._buffers: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    def clear(self) -> None:
        """Clears all buffers. Useful for unit testing."""
        self._buffers.clear()

    def add_tick(self, tick: Dict[str, Any]) -> Optional[SymbolMetrics]:
        """
        Registers a new tick in the sliding window buffer, recalculates
        indicators using Pandas, and returns enriched metrics.
        """
        symbol = tick["symbol"]
        price = float(tick["price"])
        timestamp = tick.get("timestamp")
        
        # If timestamp is a string, parse it
        if isinstance(timestamp, str):
            try:
                # Strip Z and parse
                ts_str = timestamp.rstrip("Z")
                dt = datetime.fromisoformat(ts_str)
            except Exception:
                dt = datetime.utcnow()
        elif isinstance(timestamp, datetime):
            dt = timestamp
        else:
            dt = datetime.utcnow()

        # Add to window buffer
        self._buffers[symbol].append({
            "price": price,
            "timestamp": dt
        })

        # Prune older ticks to keep buffer constant size
        if len(self._buffers[symbol]) > self.max_buffer_size:
            self._buffers[symbol].pop(0)

        # Calculate analytics
        buffer_ticks = self._buffers[symbol]
        ticks_count = len(buffer_ticks)
        
        if ticks_count == 0:
            return None

        # Convert buffer to DataFrame for robust vectorized mathematical calculations
        df = pd.DataFrame(buffer_ticks)

        # 5-tick Simple Moving Average (SMA)
        sma_5 = float(df["price"].tail(5).mean())

        # 15-tick Simple Moving Average (SMA)
        sma_15 = float(df["price"].tail(15).mean()) if ticks_count >= 5 else sma_5

        # 5-tick rolling volatility (Standard Deviation)
        if ticks_count >= 2:
            vol_5 = float(df["price"].tail(5).std())
            # Replace NaN with 0.0 (e.g. if all prices are identical)
            if np.isnan(vol_5):
                vol_5 = 0.0
        else:
            vol_5 = 0.0

        # Technical Trend Indicator (MA Crossover)
        # Bullish if short-term SMA is above long-term SMA, Bearish if below.
        # Requires at least 5 ticks for long-term SMA stability.
        if ticks_count < 5:
            trend = "NEUTRAL"
        elif sma_5 > sma_15 * 1.0001:  # 0.01% threshold to avoid noise flapping
            trend = "BULLISH"
        elif sma_5 < sma_15 * 0.9999:
            trend = "BEARISH"
        else:
            trend = "NEUTRAL"

        logger.debug(
            f"Calculated for {symbol}: SMA(5)={sma_5:.4f}, SMA(15)={sma_15:.4f}, Vol(5)={vol_5:.6f}, Trend={trend}"
        )

        return SymbolMetrics(
            symbol=symbol,
            timestamp=dt,
            moving_average_5m=round(sma_5, 4),
            moving_average_15m=round(sma_15, 4),
            volatility_5m=round(vol_5, 6),
            trend_signal=trend
        )
