import random
import uuid
from datetime import datetime
from typing import Dict, Any, Generator

class MarketDataSimulator:
    """
    Simulates high-fidelity real-time cryptocurrency trade tick data.
    Uses a random-walk algorithm starting from standard baseline values.
    """
    def __init__(self):
        # Base prices for configured symbols
        self.prices = {
            "BTC-USD": 65000.0,
            "ETH-USD": 35000.0 / 10.0, # 3500.0
            "SOL-USD": 145.0,
            "ADA-USD": 0.45,
            "DOGE-USD": 0.15
        }
        # Volatility index for symbols
        self.volatility = {
            "BTC-USD": 0.0005,
            "ETH-USD": 0.0008,
            "SOL-USD": 0.0015,
            "ADA-USD": 0.002,
            "DOGE-USD": 0.003
        }

    def generate_tick(self, symbol: str) -> Dict[str, Any]:
        """
        Generates a single randomized trade tick for the requested symbol.
        Simulates geometric Brownian motion step.
        """
        if symbol not in self.prices:
            self.prices[symbol] = 100.0
            self.volatility[symbol] = 0.001

        current_price = self.prices[symbol]
        vol = self.volatility[symbol]
        
        # Random step: percentage change
        change_pct = random.normalvariate(0, vol)
        # Slight upward drift
        drift = 0.00002
        new_price = current_price * (1 + change_pct + drift)
        
        # Ensure price is strictly positive
        if new_price <= 0:
            new_price = 0.01

        self.prices[symbol] = new_price
        
        # Random volume
        volume = abs(random.normalvariate(1.5, 1.0))
        if volume < 0.01:
            volume = 0.01

        return {
            "id": str(uuid.uuid4()),
            "symbol": symbol,
            "price": round(new_price, 4),
            "volume": round(volume, 4),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "provider": "simulator"
        }

    def stream_ticks(self) -> Generator[Dict[str, Any], None, None]:
        """
        Infinite generator yielding simulated tick data.
        """
        symbols = list(self.prices.keys())
        while True:
            # Pick a random symbol to tick
            symbol = random.choice(symbols)
            yield self.generate_tick(symbol)
