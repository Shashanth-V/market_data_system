import asyncio
import random
from datetime import datetime
from typing import Dict, Any, List, Optional
import httpx
from app.config import settings
from app.core.logger import logger
from app.core.queue import BaseMarketQueue
from app.ingestion.stream_simulator import MarketDataSimulator

# Map internal normalized symbols (e.g., BTC-USD) to public API equivalents (e.g., Binance BTCUSDT)
SYMBOL_MAPPING = {
    "BTC-USD": "BTCUSDT",
    "ETH-USD": "ETHUSDT",
    "SOL-USD": "SOLUSDT",
    "ADA-USD": "ADAUSDT",
    "DOGE-USD": "DOGEUSDT"
}

REVERSE_SYMBOL_MAPPING = {v: k for k, v in SYMBOL_MAPPING.items()}

class MarketDataCollector:
    """
    Ingestion collector that queries live cryptocurrency public APIs
    and pipes data into the message queue.
    Features:
    - Automatic retries with exponential backoff & jitter.
    - JSON validation and error logging.
    - Failover fallback to MarketDataSimulator when APIs are offline or rate-limited.
    """
    def __init__(self, queue: BaseMarketQueue):
        self.queue = queue
        self.simulator = MarketDataSimulator()
        self.client = httpx.AsyncClient(timeout=5.0)
        self.is_running = False
        self.consecutive_failures = 0
        self.max_consecutive_failures_before_fallback = 5

    async def fetch_binance_ticker(self, binance_symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetches price tick for a single symbol from Binance REST API with retries and backoff.
        """
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={binance_symbol}"
        base_delay = 1.0
        max_retries = 3

        for attempt in range(max_retries):
            try:
                response = await self.client.get(url)
                if response.status_code == 429:
                    # Rate limited
                    retry_after = float(response.headers.get("Retry-After", 2.0))
                    logger.warning(f"Rate limited by Binance (429). Sleeping for {retry_after} seconds.")
                    await asyncio.sleep(retry_after)
                    continue

                response.raise_for_status()
                data = response.json()
                
                # Basic validation
                if "symbol" not in data or "price" not in data:
                    raise ValueError(f"Corrupted or invalid JSON response: {data}")
                
                # Check for numeric price
                float(data["price"])
                
                self.consecutive_failures = max(0, self.consecutive_failures - 1)
                return data

            except (httpx.HTTPError, ValueError, TypeError) as e:
                logger.warning(
                    f"Attempt {attempt + 1}/{max_retries} failed to fetch {binance_symbol}: {e}"
                )
                if attempt == max_retries - 1:
                    self.consecutive_failures += 1
                    raise e
                
                # Exponential backoff with jitter
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                await asyncio.sleep(delay)
        
        return None

    def validate_and_normalize(self, raw_tick: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Validates the incoming raw tick data and transforms it into our internal normal form.
        Filters out corrupted or incomplete payloads.
        """
        try:
            raw_symbol = raw_tick["symbol"]
            normalized_symbol = REVERSE_SYMBOL_MAPPING.get(raw_symbol)
            if not normalized_symbol:
                logger.warning(f"Skipping tick: Unknown symbol mapping for {raw_symbol}")
                return None

            price = float(raw_tick["price"])
            if price <= 0:
                raise ValueError("Price must be strictly positive")

            # Binance ticker/price doesn't return volume directly, so we mock a realistic volume
            volume = abs(random.normalvariate(1.2, 0.5))
            if volume < 0.01:
                volume = 0.01

            return {
                "symbol": normalized_symbol,
                "price": round(price, 4),
                "volume": round(volume, 4),
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "provider": "binance"
            }
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Failed to normalize corrupted/invalid payload: {raw_tick}. Error: {e}")
            return None

    async def collect_live_ticks(self) -> List[Dict[str, Any]]:
        """
        Polls the public APIs for all active symbols and returns normalized ticks.
        """
        ticks = []
        for norm_symbol, binance_symbol in SYMBOL_MAPPING.items():
            try:
                raw_tick = await self.fetch_binance_ticker(binance_symbol)
                if raw_tick:
                    normalized = self.validate_and_normalize(raw_tick)
                    if normalized:
                        ticks.append(normalized)
            except Exception as e:
                logger.error(f"Final retry attempt failed for {binance_symbol}: {e}")
        return ticks

    async def run(self) -> None:
        """
        Main execution loop. Periodically collects ticks and pushes them to the queue.
        Enforces automatic fallback to simulated data if live API fails consistently.
        """
        self.is_running = True
        logger.info("Starting Market Data Collector ingestion engine...")
        
        while self.is_running:
            start_time = asyncio.get_event_loop().time()
            ticks = []

            # Check if we should use fallback simulator
            use_fallback = self.consecutive_failures >= self.max_consecutive_failures_before_fallback
            if use_fallback:
                logger.warning(
                    f"Ingestion fallback active (failures: {self.consecutive_failures}). Emulating real-time market data."
                )
                # Generate random ticks for active symbols
                for symbol in settings.symbols_list:
                    ticks.append(self.simulator.generate_tick(symbol))
            else:
                try:
                    ticks = await self.collect_live_ticks()
                except Exception as e:
                    logger.error(f"Unhandled exception in collection run: {e}")
                    self.consecutive_failures += 1

            # Push all ticks to the queue
            for tick in ticks:
                await self.queue.put(tick)

            # Sleep to maintain regular interval, accounting for execution drift
            elapsed = asyncio.get_event_loop().time() - start_time
            sleep_time = max(0.1, settings.API_POLL_INTERVAL_SECONDS - elapsed)
            await asyncio.sleep(sleep_time)

    def stop(self) -> None:
        """Stops the collection loop."""
        self.is_running = False
        logger.info("Stopping Market Data Collector ingestion engine...")
