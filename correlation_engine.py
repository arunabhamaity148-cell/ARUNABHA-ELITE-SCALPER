"""
ARUNABHA ELITE SCALPER v3.0
NEW FILE: correlation_engine.py
Real-time rolling correlation matrix across tracked symbols.
- If all alts correlated >0.85 with BTC = no diversification benefit
- Portfolio correlation check before each signal
- Auto-reduce size if adding a correlated position
"""

import asyncio
import logging
import time
from collections import deque
from typing import Dict, Optional, Tuple

import numpy as np

import config
from data_processor import DataProcessor

log = logging.getLogger("elite.corr")


class CorrelationEngine:
    def __init__(self, data_processor: DataProcessor):
        self.dp = data_processor
        # {symbol: deque of recent closes (15m)}
        self._price_buffers: Dict[str, deque] = {
            sym: deque(maxlen=config.CORRELATION_LOOKBACK + 5)
            for sym in config.SYMBOLS
        }
        # Cached correlation matrix: {(sym_a, sym_b): corr_value}
        self._matrix: Dict[Tuple[str, str], float] = {}
        self._last_update: float = 0.0
        self._update_interval: int = 60   # rebuild every 60s

        # Funding rate trend tracking: {symbol: deque of (ts, rate)}
        self._funding_history: Dict[str, deque] = {
            sym: deque(maxlen=config.FUNDING_TREND_LOOKBACK * 3)
            for sym in config.SYMBOLS
        }

    # ═══════════════════════════════════════════
    # PRICE INGESTION
    # ═══════════════════════════════════════════

    def update_prices(self):
        """Pull latest prices from data_processor into buffers."""
        for sym in config.SYMBOLS:
            price = self.dp.get_price(sym)
            if price > 0:
                self._price_buffers[sym].append(price)

    def update_funding(self, symbol: str, rate: float, ts: float):
        """Record a funding rate observation for trend tracking."""
        self._funding_history[symbol].append((ts, rate))

    # ═══════════════════════════════════════════
    # CORRELATION MATRIX
    # ═══════════════════════════════════════════

    def rebuild_matrix(self):
        """Recompute rolling correlation matrix for all symbol pairs."""
        now = time.time()
        if now - self._last_update < self._update_interval:
            return

        self._update_prices_from_dp()
        symbols = config.SYMBOLS
        self._matrix.clear()

        for i in range(len(symbols)):
            for j in range(i + 1, len(symbols)):
                sym_a = symbols[i]
                sym_b = symbols[j]
                corr = self._compute_correlation(sym_a, sym_b)
                self._matrix[(sym_a, sym_b)] = corr
                self._matrix[(sym_b, sym_a)] = corr

        self._last_update = now

    def _update_prices_from_dp(self):
        """Sample current price into rolling buffer."""
        for sym in config.SYMBOLS:
            candles = self.dp.get_candles(sym, "15m", n=config.CORRELATION_LOOKBACK + 2)
            if len(candles) >= config.CORRELATION_LOOKBACK:
                closes = [c.c for c in candles[-config.CORRELATION_LOOKBACK:]]
                self._price_buffers[sym] = deque(closes, maxlen=config.CORRELATION_LOOKBACK + 5)

    def _compute_correlation(self, sym_a: str, sym_b: str) -> float:
        """Pearson correlation of returns over CORRELATION_LOOKBACK periods."""
        buf_a = list(self._price_buffers[sym_a])
        buf_b = list(self._price_buffers[sym_b])

        n = min(len(buf_a), len(buf_b), config.CORRELATION_LOOKBACK)
        if n < 5:
            return 0.0

        a = np.array(buf_a[-n:])
        b = np.array(buf_b[-n:])

        # Use returns (% change) for better stationarity
        ret_a = np.diff(a) / a[:-1]
        ret_b = np.diff(b) / b[:-1]

        if len(ret_a) < 4 or np.std(ret_a) == 0 or np.std(ret_b) == 0:
            return 0.0

        corr = float(np.corrcoef(ret_a, ret_b)[0, 1])
        return round(corr if not np.isnan(corr) else 0.0, 4)

    # ═══════════════════════════════════════════
    # PORTFOLIO CORRELATION CHECK
    # ═══════════════════════════════════════════

    def get_correlation_with_btc(self, symbol: str) -> float:
        """Return correlation of symbol with BTCUSDT."""
        if symbol == "BTCUSDT":
            return 1.0
        self.rebuild_matrix()
        return self._matrix.get(("BTCUSDT", symbol), 0.0)

    def get_portfolio_correlation(self, active_symbols: list, new_symbol: str) -> float:
        """
        Average correlation of new_symbol with all currently active positions.
        Returns 0.0 if no active positions.
        """
        self.rebuild_matrix()
        if not active_symbols:
            return 0.0

        corrs = []
        for sym in active_symbols:
            if sym == new_symbol:
                continue
            pair = (sym, new_symbol)
            pair_rev = (new_symbol, sym)
            c = self._matrix.get(pair, self._matrix.get(pair_rev, 0.0))
            corrs.append(abs(c))

        return float(np.mean(corrs)) if corrs else 0.0

    def all_alts_correlated_with_btc(self, threshold: float = None) -> bool:
        """
        Returns True if >70% of alts are correlated above threshold with BTC.
        This means: market is moving as one unit, no diversification.
        """
        self.rebuild_matrix()
        threshold = threshold or config.CORRELATION_HIGH
        alts = [s for s in config.SYMBOLS if s != "BTCUSDT"]
        if not alts:
            return False
        high_corr_count = sum(
            1 for sym in alts
            if abs(self._matrix.get(("BTCUSDT", sym), 0.0)) >= threshold
        )
        return high_corr_count / len(alts) >= 0.70

    def get_size_multiplier(self, new_symbol: str, direction: str) -> float:
        """
        Returns position size multiplier based on portfolio correlation.
        1.0 = no reduction
        0.5 = high correlation penalty
        """
        from state_manager import StateManager
        # Get active symbols from state (passed indirectly via dp)
        # We just use correlation with BTC as proxy here
        btc_corr = abs(self.get_correlation_with_btc(new_symbol))

        if btc_corr >= config.CORRELATION_HIGH:
            # All moving with BTC — no diversification
            return config.CORRELATION_SIZE_REDUCTION
        elif btc_corr >= config.CORRELATION_PORTFOLIO_LIMIT:
            return 0.75
        return 1.0

    # ═══════════════════════════════════════════
    # FUNDING TREND ANALYSIS
    # ═══════════════════════════════════════════

    def get_funding_trend(self, symbol: str) -> str:
        """
        RISING / FALLING / NEUTRAL based on 24h funding history.
        Rising funding = increasing leverage = caution (reduce longs).
        Falling funding = deleveraging = opportunity.
        """
        history = list(self._funding_history.get(symbol, []))
        if len(history) < 4:
            return "NEUTRAL"

        # Compare last 8h vs previous 16h
        now = time.time()
        recent = [r for ts, r in history if now - ts < 8 * 3600]
        older = [r for ts, r in history if 8 * 3600 <= now - ts < 24 * 3600]

        if not recent or not older:
            return "NEUTRAL"

        recent_avg = sum(recent) / len(recent)
        older_avg = sum(older) / len(older)
        change = recent_avg - older_avg

        if change > config.FUNDING_RISING_THRESHOLD:
            return "RISING"
        elif change < config.FUNDING_FALLING_THRESHOLD:
            return "FALLING"
        return "NEUTRAL"

    def get_funding_trend_multiplier(self, symbol: str, direction: str) -> float:
        """
        Size multiplier based on funding trend.
        Rising funding + LONG = crowding risk → 0.75x
        Falling funding + LONG = deleveraging → 1.10x (opportunity)
        """
        trend = self.get_funding_trend(symbol)
        if trend == "RISING" and direction == "LONG":
            return 0.75
        elif trend == "FALLING" and direction == "LONG":
            return 1.10
        elif trend == "RISING" and direction == "SHORT":
            return 1.05   # short squeeze risk rising
        elif trend == "FALLING" and direction == "SHORT":
            return 0.90
        return 1.0

    # ═══════════════════════════════════════════
    # STATUS
    # ═══════════════════════════════════════════

    def get_status(self) -> dict:
        self.rebuild_matrix()
        btc_corrs = {
            sym: round(self._matrix.get(("BTCUSDT", sym), 0.0), 3)
            for sym in config.SYMBOLS if sym != "BTCUSDT"
        }
        return {
            "btc_correlations": btc_corrs,
            "all_correlated": self.all_alts_correlated_with_btc(),
            "matrix_age_s": round(time.time() - self._last_update),
        }

    def format_for_telegram(self) -> str:
        self.rebuild_matrix()
        lines = ["📊 <b>Correlations (BTC)</b>"]
        for sym in config.SYMBOLS:
            if sym == "BTCUSDT":
                continue
            c = self._matrix.get(("BTCUSDT", sym), 0.0)
            bar = "🔴" if abs(c) >= config.CORRELATION_HIGH else "🟡" if abs(c) >= 0.60 else "🟢"
            lines.append(f"{bar} {sym}: <code>{c:+.2f}</code>")
        return "\n".join(lines)
