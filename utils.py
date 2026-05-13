"""
ARUNABHA ELITE SCALPER v3.0
FILE 16/18: utils.py
Shared utilities — formatting, math, retry decorator, rate limiter
"""

import asyncio
import functools
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import numpy as np

import config

log = logging.getLogger("elite.utils")


# ═══════════════════════════════════════════════
# FORMATTING
# ═══════════════════════════════════════════════

def fmt_price(price: float, symbol: str = "") -> str:
    """Format price with appropriate decimal places."""
    if price >= 10000:
        return f"{price:,.2f}"
    elif price >= 100:
        return f"{price:,.3f}"
    elif price >= 1:
        return f"{price:,.4f}"
    else:
        return f"{price:.6f}"


def fmt_pct(value: float, sign: bool = True) -> str:
    """Format percentage with optional + sign."""
    pct = value * 100
    prefix = "+" if sign and pct > 0 else ""
    return f"{prefix}{pct:.2f}%"


def fmt_usdt(value: float) -> str:
    """Format USDT value."""
    if abs(value) >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    elif abs(value) >= 1000:
        return f"${value/1000:.1f}K"
    else:
        return f"${value:.2f}"


def fmt_duration(seconds: float) -> str:
    """Format duration in human-readable form."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.0f}m"
    elif seconds < 86400:
        return f"{seconds/3600:.1f}h"
    else:
        return f"{seconds/86400:.1f}d"


def to_ist(ts: Optional[float] = None) -> str:
    """Convert UTC timestamp to IST string."""
    if ts is None:
        ts = time.time()
    utc_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    ist_h = (utc_dt.hour + 5) % 24
    ist_m = (utc_dt.minute + 30) % 60
    ist_h += (utc_dt.minute + 30) // 60
    ist_h = ist_h % 24
    return f"{ist_h:02d}:{ist_m:02d} IST"


# ═══════════════════════════════════════════════
# MATH HELPERS
# ═══════════════════════════════════════════════

def pct_change(a: float, b: float) -> float:
    """Percentage change from a to b."""
    if a == 0:
        return 0.0
    return (b - a) / a


def clamp(value: float, min_val: float, max_val: float) -> float:
    return max(min_val, min(max_val, value))


def rolling_std(values: list, window: int) -> float:
    if len(values) < window:
        return float(np.std(values)) if values else 0.0
    return float(np.std(values[-window:]))


def percentile_rank(values: list, current: float) -> float:
    """Where is current in the distribution of values (0-100)."""
    if not values:
        return 50.0
    arr = np.array(values)
    return float(np.sum(arr <= current) / len(arr) * 100)


def kelly_fraction(win_rate: float, avg_win: float, avg_loss: float) -> float:
    """Kelly criterion position sizing fraction."""
    if avg_loss <= 0 or win_rate <= 0:
        return 0.0
    b = avg_win / avg_loss
    q = 1 - win_rate
    kelly = (b * win_rate - q) / b
    # Half-Kelly for safety
    return max(0.0, min(kelly * 0.5, 0.25))


def sharpe_ratio(returns: list, periods_per_year: int = 365 * 96) -> float:
    """Annualized Sharpe ratio from list of period returns."""
    if len(returns) < 2:
        return 0.0
    arr = np.array(returns)
    mean = np.mean(arr)
    std = np.std(arr)
    if std <= 0:
        return 0.0
    return float(mean / std * np.sqrt(periods_per_year))


def max_drawdown(equity_curve: list) -> float:
    """Maximum drawdown fraction from equity curve."""
    if len(equity_curve) < 2:
        return 0.0
    eq = np.array(equity_curve, dtype=float)
    peak = np.maximum.accumulate(eq)
    dd = (peak - eq) / np.where(peak > 0, peak, 1)
    return float(np.max(dd))


# ═══════════════════════════════════════════════
# ASYNC RETRY DECORATOR
# ═══════════════════════════════════════════════

def async_retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
):
    """Decorator: retry async function on exception with exponential backoff."""
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except asyncio.CancelledError:
                    raise
                except exceptions as e:
                    if attempt == max_attempts - 1:
                        raise
                    log.debug(f"{func.__name__} attempt {attempt+1} failed: {e}, retry in {current_delay:.1f}s")
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator


# ═══════════════════════════════════════════════
# RATE LIMITER
# ═══════════════════════════════════════════════

class RateLimiter:
    """Token bucket rate limiter for API calls."""

    def __init__(self, max_calls: int, period: float):
        self.max_calls = max_calls
        self.period = period
        self._calls: list = []

    async def acquire(self):
        now = time.time()
        # Remove calls outside window
        self._calls = [t for t in self._calls if now - t < self.period]
        if len(self._calls) >= self.max_calls:
            sleep_time = self.period - (now - self._calls[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        self._calls.append(time.time())

    def remaining(self) -> int:
        now = time.time()
        recent = [t for t in self._calls if now - t < self.period]
        return max(0, self.max_calls - len(recent))


# ═══════════════════════════════════════════════
# BINANCE WEIGHT TRACKER
# ═══════════════════════════════════════════════

class BinanceWeightTracker:
    """Track Binance API request weight to avoid rate limits."""

    LIMIT = config.BINANCE_RATE_LIMIT_WEIGHT
    WINDOW = 60  # seconds

    def __init__(self):
        self._usage: list = []  # (timestamp, weight)

    def record(self, weight: int):
        self._usage.append((time.time(), weight))

    def current_weight(self) -> int:
        cutoff = time.time() - self.WINDOW
        self._usage = [(t, w) for t, w in self._usage if t > cutoff]
        return sum(w for _, w in self._usage)

    def can_use(self, weight: int) -> bool:
        return self.current_weight() + weight <= self.LIMIT * 0.80  # 80% safety margin

    async def wait_if_needed(self, weight: int):
        while not self.can_use(weight):
            log.warning(f"Rate limit near ({self.current_weight()}/{self.LIMIT}) — waiting 5s")
            await asyncio.sleep(5)
        self.record(weight)


# ═══════════════════════════════════════════════
# SIGNAL DEDUPLICATOR
# ═══════════════════════════════════════════════

class SignalDeduplicator:
    """Prevent duplicate signals for same symbol within cooldown period."""

    def __init__(self, cooldown_seconds: int = config.ALERT_COOLDOWN_SECONDS):
        self._sent: dict = {}  # symbol → (direction, timestamp)
        self._cooldown = cooldown_seconds

    def is_duplicate(self, symbol: str, direction: str) -> bool:
        entry = self._sent.get(symbol)
        if not entry:
            return False
        prev_direction, prev_ts = entry
        if time.time() - prev_ts < self._cooldown:
            return True  # same symbol within cooldown
        return False

    def record(self, symbol: str, direction: str):
        self._sent[symbol] = (direction, time.time())

    def clear_expired(self):
        now = time.time()
        self._sent = {
            sym: (d, t)
            for sym, (d, t) in self._sent.items()
            if now - t < self._cooldown
        }
