"""
ARUNABHA ELITE SCALPER v3.0
NEW FILE: btc_dominance.py
BTC Dominance tracker — proxy from Binance USDT-margined OI
Rising BTC.D + BTC bullish = alt season ending → reduce alt longs
Falling BTC.D + BTC bullish = alt season → boost alt longs
"""

import asyncio
import logging
import time
from collections import deque
from typing import Optional

import aiohttp

import config

log = logging.getLogger("elite.btcdom")


class BTCDominanceTracker:
    """
    BTC Dominance proxy:
    True BTC.D (from CoinGecko/TradingView) requires paid API.
    We approximate using:
      BTC_OI / TOTAL_OI across tracked symbols.
    This is a reliable proxy: if BTC captures more OI share, it's dominant.

    Additionally we track BTC price vs alt basket performance.
    """

    def __init__(self):
        self._dominance_history: deque = deque(maxlen=288)  # 24h at 5min intervals
        self._current_dominance: float = 0.50   # 50% default
        self._trend: str = "NEUTRAL"            # RISING / FALLING / NEUTRAL
        self._btc_oi: float = 0.0
        self._total_oi: float = 0.0
        self._last_fetch: float = 0.0
        self._session: Optional[aiohttp.ClientSession] = None
        self._shutdown = asyncio.Event()

        # Alt performance relative to BTC (rolling 4h)
        self._alt_vs_btc: float = 0.0
        self._btc_price_4h_ago: float = 0.0
        self._alt_basket_4h_ago: dict = {}

    # ═══════════════════════════════════════════
    # MAIN LOOP
    # ═══════════════════════════════════════════

    async def run(self):
        log.info("BTC Dominance tracker started")
        while not self._shutdown.is_set():
            try:
                await self.fetch()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug(f"BTC Dom fetch error: {e}")
            await asyncio.sleep(config.BTC_DOM_FETCH_INTERVAL)

    # ═══════════════════════════════════════════
    # FETCH
    # ═══════════════════════════════════════════

    async def fetch(self):
        """Fetch OI for all symbols and compute BTC dominance proxy."""
        now = time.time()
        if now - self._last_fetch < config.BTC_DOM_FETCH_INTERVAL - 10:
            return

        if not self._session:
            self._session = aiohttp.ClientSession()

        try:
            url = f"{config.BINANCE_BASE_URL}/fapi/v1/openInterest"
            btc_oi = 0.0
            total_oi = 0.0
            btc_price = 0.0

            for sym in config.SYMBOLS:
                try:
                    params = {"symbol": sym}
                    async with self._session.get(
                        url, params=params, timeout=aiohttp.ClientTimeout(total=3)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            oi = float(data.get("openInterest", 0))
                            # Get price to convert to USDT
                            price_url = f"{config.BINANCE_BASE_URL}/fapi/v1/ticker/price"
                            async with self._session.get(
                                price_url, params={"symbol": sym},
                                timeout=aiohttp.ClientTimeout(total=3)
                            ) as pr:
                                if pr.status == 200:
                                    pd = await pr.json()
                                    price = float(pd.get("price", 0))
                                    oi_usdt = oi * price
                                    total_oi += oi_usdt
                                    if sym == "BTCUSDT":
                                        btc_oi = oi_usdt
                                        btc_price = price
                    await asyncio.sleep(0.05)
                except Exception:
                    continue

            if total_oi > 0:
                new_dom = btc_oi / total_oi
                self._dominance_history.append((now, new_dom))
                self._current_dominance = new_dom
                self._btc_oi = btc_oi
                self._total_oi = total_oi
                self._update_trend()
                self._last_fetch = now
                log.debug(f"BTC.D proxy: {new_dom:.1%} | trend: {self._trend}")

        except Exception as e:
            log.debug(f"BTC Dom fetch error: {e}")

    # ═══════════════════════════════════════════
    # TREND CALCULATION
    # ═══════════════════════════════════════════

    def _update_trend(self):
        """Detect trend from dominance history over last 24h."""
        if len(self._dominance_history) < 6:
            self._trend = "NEUTRAL"
            return

        history = list(self._dominance_history)
        # Compare last 1h vs 4h ago
        recent = [d for ts, d in history if time.time() - ts < 3600]
        older = [d for ts, d in history if 3600 <= time.time() - ts < 14400]

        if not recent or not older:
            self._trend = "NEUTRAL"
            return

        recent_avg = sum(recent) / len(recent)
        older_avg = sum(older) / len(older)
        change = recent_avg - older_avg

        if change > config.BTC_DOM_CHANGE_THRESHOLD:
            self._trend = "RISING"
        elif change < -config.BTC_DOM_CHANGE_THRESHOLD:
            self._trend = "FALLING"
        else:
            self._trend = "NEUTRAL"

    # ═══════════════════════════════════════════
    # GETTERS
    # ═══════════════════════════════════════════

    def get_trend(self) -> str:
        """RISING / FALLING / NEUTRAL"""
        return self._trend

    def get_dominance(self) -> float:
        """Current BTC dominance proxy (0-1)."""
        return self._current_dominance

    def get_alt_size_multiplier(self, direction: str) -> float:
        """
        For alt coins only (not BTCUSDT):
        - BTC.D RISING + direction LONG  → reduce (0.75x)
        - BTC.D FALLING + direction LONG → boost (1.20x) — alt season
        - BTC.D RISING + direction SHORT → normal (bearish alts anyway)
        - BTC.D FALLING + direction SHORT→ reduce (alts might pump)
        """
        if self._trend == "RISING":
            if direction == "LONG":
                return config.BTC_DOM_RISING_ALT_LONG_MULT
            else:
                return 1.0
        elif self._trend == "FALLING":
            if direction == "LONG":
                return config.BTC_DOM_FALLING_ALT_LONG_MULT
            else:
                return 0.85  # alts may pump so shorting is riskier
        return 1.0

    def get_status(self) -> dict:
        return {
            "dominance_proxy": round(self._current_dominance, 4),
            "trend": self._trend,
            "btc_oi_usdt": round(self._btc_oi, 0),
            "total_oi_usdt": round(self._total_oi, 0),
            "history_points": len(self._dominance_history),
            "last_fetch_ago_s": round(time.time() - self._last_fetch),
        }

    async def close(self):
        self._shutdown.set()
        if self._session and not self._session.closed:
            await self._session.close()
