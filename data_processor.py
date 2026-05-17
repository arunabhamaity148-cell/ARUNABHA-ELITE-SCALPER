"""
ARUNABHA ELITE SCALPER v3.0
FILE 4/18: data_processor.py
Candle aggregation, indicator calculation (numpy), orderbook, REST polling
"""

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import numpy as np

import config

log = logging.getLogger("elite.data")


# ═══════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════

@dataclass
class Candle:
    ts: int             # open time ms
    o: float
    h: float
    l: float
    c: float
    v: float
    closed: bool = True

    def is_valid(self) -> bool:
        return (
            self.h >= self.l
            and self.o > 0
            and self.h > 0
            and self.l > 0
            and self.c > 0
            and self.v >= 0
            and self.h >= self.o
            and self.h >= self.c
            and self.l <= self.o
            and self.l <= self.c
        )


@dataclass
class Indicators:
    # EMAs
    ema9: float = 0.0
    ema21: float = 0.0
    ema50: float = 0.0
    ema200: float = 0.0
    # SMAs
    sma20: float = 0.0
    sma50: float = 0.0
    sma200: float = 0.0
    # RSI
    rsi: float = 50.0
    # MACD
    macd: float = 0.0
    macd_signal: float = 0.0
    macd_hist: float = 0.0
    # Bollinger
    bb_upper: float = 0.0
    bb_mid: float = 0.0
    bb_lower: float = 0.0
    bb_bandwidth: float = 0.0
    bb_pct_b: float = 0.5
    # ATR
    atr: float = 0.0
    atr_pct: float = 0.0
    # ADX
    adx: float = 0.0
    plus_di: float = 0.0
    minus_di: float = 0.0
    # Volume
    vol_sma20: float = 0.0
    vol_ratio: float = 1.0
    # Stochastic
    stoch_k: float = 50.0
    stoch_d: float = 50.0
    # VWAP
    vwap: float = 0.0
    # ATR percentile
    atr_percentile: float = 50.0
    # BB avg bandwidth
    bb_bandwidth_avg: float = 0.0


@dataclass
class OrderbookState:
    bids: List[List[float]] = field(default_factory=list)   # [[price, size], ...]
    asks: List[List[float]] = field(default_factory=list)
    spread: float = 0.0
    mid: float = 0.0
    imbalance: float = 0.0     # -1 to +1, positive = buy pressure
    wap: float = 0.0           # weighted average price
    walls_bid: List[float] = field(default_factory=list)
    walls_ask: List[float] = field(default_factory=list)
    liquidity_depth_bid: float = 0.0
    liquidity_depth_ask: float = 0.0
    updated_at: float = 0.0


@dataclass
class FundingData:
    rate: float = 0.0
    next_time: int = 0
    updated_at: float = 0.0


@dataclass
class OIData:
    open_interest: float = 0.0
    oi_change_1h: float = 0.0
    oi_change_4h: float = 0.0
    updated_at: float = 0.0


# ═══════════════════════════════════════════════
# INDICATOR CALCULATOR
# ═══════════════════════════════════════════════

class IndicatorCalc:
    """Pure numpy indicator calculations — no pandas dependency for speed."""

    @staticmethod
    def ema(prices: np.ndarray, period: int) -> np.ndarray:
        if len(prices) < period:
            return np.full(len(prices), np.nan)
        alpha = 2.0 / (period + 1)
        result = np.full(len(prices), np.nan)
        result[period - 1] = np.mean(prices[:period])
        for i in range(period, len(prices)):
            result[i] = prices[i] * alpha + result[i - 1] * (1 - alpha)
        return result

    @staticmethod
    def sma(prices: np.ndarray, period: int) -> np.ndarray:
        if len(prices) < period:
            return np.full(len(prices), np.nan)
        result = np.full(len(prices), np.nan)
        for i in range(period - 1, len(prices)):
            result[i] = np.mean(prices[i - period + 1: i + 1])
        return result

    @staticmethod
    def rsi(closes: np.ndarray, period: int = 14) -> np.ndarray:
        if len(closes) < period + 1:
            return np.full(len(closes), 50.0)
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)
        result = np.full(len(closes), 50.0)

        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])

        for i in range(period, len(deltas)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            if avg_loss == 0:
                result[i + 1] = 100.0
            else:
                rs = avg_gain / avg_loss
                result[i + 1] = 100 - (100 / (1 + rs))

        return result

    @staticmethod
    def macd(closes: np.ndarray, fast=12, slow=26, signal=9) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        if len(closes) < slow + signal:
            z = np.zeros(len(closes))
            return z, z, z
        ema_fast = IndicatorCalc.ema(closes, fast)
        ema_slow = IndicatorCalc.ema(closes, slow)
        macd_line = ema_fast - ema_slow
        valid = ~np.isnan(macd_line)
        signal_line = np.full(len(closes), np.nan)
        if valid.sum() >= signal:
            valid_idx = np.where(valid)[0]
            sig = IndicatorCalc.ema(macd_line[valid], signal)
            signal_line[valid_idx] = sig
        hist = macd_line - signal_line
        return (
            np.nan_to_num(macd_line),
            np.nan_to_num(signal_line),
            np.nan_to_num(hist),
        )

    @staticmethod
    def bollinger(closes: np.ndarray, period=20, std_mult=2.0) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        mid = IndicatorCalc.sma(closes, period)
        result_upper = np.full(len(closes), np.nan)
        result_lower = np.full(len(closes), np.nan)
        for i in range(period - 1, len(closes)):
            std = np.std(closes[i - period + 1: i + 1], ddof=0)
            result_upper[i] = mid[i] + std_mult * std
            result_lower[i] = mid[i] - std_mult * std
        return (
            np.nan_to_num(result_upper),
            np.nan_to_num(mid),
            np.nan_to_num(result_lower),
        )

    @staticmethod
    def atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period=14) -> np.ndarray:
        if len(closes) < 2:
            return np.zeros(len(closes))
        tr = np.zeros(len(closes))
        tr[0] = highs[0] - lows[0]
        for i in range(1, len(closes)):
            tr[i] = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
        atr_arr = np.full(len(closes), np.nan)
        atr_arr[period - 1] = np.mean(tr[:period])
        for i in range(period, len(closes)):
            atr_arr[i] = (atr_arr[i - 1] * (period - 1) + tr[i]) / period
        return np.nan_to_num(atr_arr)

    @staticmethod
    def adx(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period=14) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        if len(closes) < period * 2:
            z = np.full(len(closes), 20.0)
            return z, z, z
        n = len(closes)
        plus_dm = np.zeros(n)
        minus_dm = np.zeros(n)
        tr = np.zeros(n)

        for i in range(1, n):
            h_diff = highs[i] - highs[i - 1]
            l_diff = lows[i - 1] - lows[i]
            plus_dm[i] = h_diff if h_diff > l_diff and h_diff > 0 else 0.0
            minus_dm[i] = l_diff if l_diff > h_diff and l_diff > 0 else 0.0
            tr[i] = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )

        smoothed_tr = np.zeros(n)
        smoothed_plus = np.zeros(n)
        smoothed_minus = np.zeros(n)
        smoothed_tr[period] = np.sum(tr[1: period + 1])
        smoothed_plus[period] = np.sum(plus_dm[1: period + 1])
        smoothed_minus[period] = np.sum(minus_dm[1: period + 1])

        for i in range(period + 1, n):
            smoothed_tr[i] = smoothed_tr[i - 1] - smoothed_tr[i - 1] / period + tr[i]
            smoothed_plus[i] = smoothed_plus[i - 1] - smoothed_plus[i - 1] / period + plus_dm[i]
            smoothed_minus[i] = smoothed_minus[i - 1] - smoothed_minus[i - 1] / period + minus_dm[i]

        plus_di = np.where(smoothed_tr > 0, 100.0 * smoothed_plus / np.where(smoothed_tr > 0, smoothed_tr, 1.0), 0.0)
        minus_di = np.where(smoothed_tr > 0, 100.0 * smoothed_minus / np.where(smoothed_tr > 0, smoothed_tr, 1.0), 0.0)
        di_sum = plus_di + minus_di
        dx = np.where(
            di_sum > 0,
            100.0 * np.abs(plus_di - minus_di) / np.where(di_sum > 0, di_sum, 1.0),
            0.0,
        )
        adx_arr = np.full(n, 20.0)
        adx_arr[period * 2] = np.mean(dx[period: period * 2])
        for i in range(period * 2 + 1, n):
            adx_arr[i] = (adx_arr[i - 1] * (period - 1) + dx[i]) / period

        return adx_arr, plus_di, minus_di

    @staticmethod
    def stochastic(highs, lows, closes, k=14, d=3, smooth=3) -> Tuple[np.ndarray, np.ndarray]:
        n = len(closes)
        stoch_k = np.full(n, 50.0)
        for i in range(k - 1, n):
            h = np.max(highs[i - k + 1: i + 1])
            l = np.min(lows[i - k + 1: i + 1])
            stoch_k[i] = 100 * (closes[i] - l) / (h - l) if h != l else 50.0
        # Smooth K
        smooth_k = np.full(n, 50.0)
        for i in range(smooth - 1, n):
            smooth_k[i] = np.mean(stoch_k[i - smooth + 1: i + 1])
        # D = SMA of smoothed K
        stoch_d = np.full(n, 50.0)
        for i in range(d - 1, n):
            stoch_d[i] = np.mean(smooth_k[i - d + 1: i + 1])
        return smooth_k, stoch_d

    @staticmethod
    def vwap(highs, lows, closes, volumes) -> np.ndarray:
        typical = (highs + lows + closes) / 3
        cumvol = np.cumsum(volumes)
        cumtvol = np.cumsum(typical * volumes)
        return np.where(cumvol > 0, cumtvol / cumvol, closes)


# ═══════════════════════════════════════════════
# DATA PROCESSOR
# ═══════════════════════════════════════════════

class DataProcessor:
    def __init__(self):
        # {symbol: {tf: deque of Candle}}
        self._candles: Dict[str, Dict[str, deque]] = {}
        # {symbol: {tf: Indicators}}
        self._indicators: Dict[str, Dict[str, Indicators]] = {}
        # {symbol: OrderbookState}
        self._orderbooks: Dict[str, OrderbookState] = {}
        # {symbol: FundingData}
        self._funding: Dict[str, FundingData] = {}
        # {symbol: OIData}
        self._oi: Dict[str, OIData] = {}
        # {symbol: float} current price
        self._prices: Dict[str, float] = {}
        # REST poll timestamps
        self._last_funding_poll: float = 0.0
        self._last_oi_poll: float = 0.0
        self._cache: Dict[str, Any] = {}
        self._calc = IndicatorCalc()
        self._http_session: Optional[aiohttp.ClientSession] = None
        self._rest_task: Optional[asyncio.Task] = None

        self._init_buffers()

    def _init_buffers(self):
        for sym in config.SYMBOLS:
            self._candles[sym] = {tf: deque(maxlen=config.CANDLE_BUFFER) for tf in config.TIMEFRAMES}
            self._indicators[sym] = {tf: Indicators() for tf in config.TIMEFRAMES}
            self._orderbooks[sym] = OrderbookState()
            self._funding[sym] = FundingData()
            self._oi[sym] = OIData()
            self._prices[sym] = 0.0

    # ───────────────────────────────────────────
    # WS MESSAGE HANDLERS
    # ───────────────────────────────────────────

    async def on_kline(self, symbol: str, data: dict):
        k = data.get("k", {})
        tf_map = {"5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h"}
        interval = k.get("i", "")
        tf = tf_map.get(interval)
        if not tf:
            return

        candle = Candle(
            ts=int(k["t"]),
            o=float(k["o"]),
            h=float(k["h"]),
            l=float(k["l"]),
            c=float(k["c"]),
            v=float(k["v"]),
            closed=bool(k.get("x", False)),
        )

        if not self._validate_candle(symbol, candle):
            return

        self._prices[symbol] = candle.c

        buf = self._candles[symbol][tf]
        # Update or append
        if buf and buf[-1].ts == candle.ts:
            buf[-1] = candle
        else:
            buf.append(candle)

        # Recalculate indicators if candle closed
        if candle.closed and len(buf) >= config.CANDLE_BUFFER_MIN:
            self._recalculate(symbol, tf)

    async def on_depth(self, symbol: str, data: dict):
        ob = self._orderbooks[symbol]
        bids_raw = data.get("b", [])
        asks_raw = data.get("a", [])

        # Update bid/ask levels
        for price_s, size_s in bids_raw:
            price, size = float(price_s), float(size_s)
            ob.bids = [b for b in ob.bids if b[0] != price]
            if size > 0:
                ob.bids.append([price, size])
        for price_s, size_s in asks_raw:
            price, size = float(price_s), float(size_s)
            ob.asks = [a for a in ob.asks if a[0] != price]
            if size > 0:
                ob.asks.append([price, size])

        # Sort and trim
        ob.bids = sorted(ob.bids, key=lambda x: -x[0])[:config.OB_LEVELS]
        ob.asks = sorted(ob.asks, key=lambda x: x[0])[:config.OB_LEVELS]

        self._compute_ob_metrics(symbol)
        ob.updated_at = time.time()

    async def on_agg_trade(self, symbol: str, data: dict):
        # Price update from trades
        price = float(data.get("p", 0))
        if price > 0:
            self._prices[symbol] = price

    async def on_rest_kline(self, symbol: str, tf: str, data: list):
        """Handle REST kline fallback data."""
        for k in data:
            candle = Candle(
                ts=int(k[0]), o=float(k[1]), h=float(k[2]),
                l=float(k[3]), c=float(k[4]), v=float(k[5]),
                closed=True,
            )
            if self._validate_candle(symbol, candle):
                buf = self._candles[symbol][tf]
                if not buf or buf[-1].ts != candle.ts:
                    buf.append(candle)

    # ───────────────────────────────────────────
    # VALIDATION
    # ───────────────────────────────────────────

    def _validate_candle(self, symbol: str, c: Candle) -> bool:
        if not c.is_valid():
            log.debug(f"Invalid candle {symbol}: {c}")
            return False
        # Price within 20% of last
        last = self._prices.get(symbol, 0)
        if last > 0 and abs(c.c - last) / last > 0.20:
            log.warning(f"Price outlier {symbol}: {c.c} vs {last}")
            return False
        return True

    # ───────────────────────────────────────────
    # INDICATOR CALCULATION
    # ───────────────────────────────────────────

    def _recalculate(self, symbol: str, tf: str):
        buf = list(self._candles[symbol][tf])
        if len(buf) < 50:
            return

        closes = np.array([c.c for c in buf])
        highs = np.array([c.h for c in buf])
        lows = np.array([c.l for c in buf])
        vols = np.array([c.v for c in buf])

        ind = Indicators()

        # EMAs
        ind.ema9 = self._calc.ema(closes, 9)[-1]
        ind.ema21 = self._calc.ema(closes, 21)[-1]
        ind.ema50 = self._calc.ema(closes, 50)[-1] if len(closes) >= 50 else closes[-1]
        ind.ema200 = self._calc.ema(closes, 200)[-1] if len(closes) >= 200 else closes[-1]

        # SMAs
        ind.sma20 = self._calc.sma(closes, 20)[-1]
        ind.sma50 = self._calc.sma(closes, 50)[-1] if len(closes) >= 50 else closes[-1]
        ind.sma200 = self._calc.sma(closes, 200)[-1] if len(closes) >= 200 else closes[-1]

        # RSI
        ind.rsi = self._calc.rsi(closes, 14)[-1]

        # MACD
        m, ms, mh = self._calc.macd(closes)
        ind.macd, ind.macd_signal, ind.macd_hist = m[-1], ms[-1], mh[-1]

        # Bollinger
        bu, bm, bl = self._calc.bollinger(closes)
        ind.bb_upper, ind.bb_mid, ind.bb_lower = bu[-1], bm[-1], bl[-1]
        if bm[-1] > 0:
            ind.bb_bandwidth = (bu[-1] - bl[-1]) / bm[-1]
            ind.bb_pct_b = (closes[-1] - bl[-1]) / (bu[-1] - bl[-1]) if bu[-1] != bl[-1] else 0.5

        # ATR
        atr_arr = self._calc.atr(highs, lows, closes)
        ind.atr = atr_arr[-1]
        ind.atr_pct = ind.atr / closes[-1] if closes[-1] > 0 else 0
        # ATR percentile
        atr_window = atr_arr[-50:]
        atr_window = atr_window[atr_window > 0]
        if len(atr_window) > 5:
            ind.atr_percentile = float(np.percentile(atr_window, 50) / ind.atr * 50) if ind.atr > 0 else 50.0
            ind.atr_percentile = min(100, max(0, (ind.atr - np.min(atr_window)) / (np.max(atr_window) - np.min(atr_window) + 1e-9) * 100))

        # BB bandwidth average
        bw_series = np.where(bm > 0, (bu - bl) / bm, 0)
        ind.bb_bandwidth_avg = float(np.mean(bw_series[-20:])) if len(bw_series) >= 20 else ind.bb_bandwidth

        # ADX
        adx_arr, pdi, mdi = self._calc.adx(highs, lows, closes)
        ind.adx, ind.plus_di, ind.minus_di = adx_arr[-1], pdi[-1], mdi[-1]

        # Volume
        vol_sma = self._calc.sma(vols, 20)[-1]
        ind.vol_sma20 = vol_sma
        ind.vol_ratio = vols[-1] / vol_sma if vol_sma > 0 else 1.0

        # Stochastic
        sk, sd = self._calc.stochastic(highs, lows, closes)
        ind.stoch_k, ind.stoch_d = sk[-1], sd[-1]

        # VWAP
        ind.vwap = self._calc.vwap(highs, lows, closes, vols)[-1]

        self._indicators[symbol][tf] = ind

    # ───────────────────────────────────────────
    # ORDERBOOK METRICS
    # ───────────────────────────────────────────

    def _compute_ob_metrics(self, symbol: str):
        ob = self._orderbooks[symbol]
        if not ob.bids or not ob.asks:
            return

        best_bid = ob.bids[0][0]
        best_ask = ob.asks[0][0]
        ob.spread = best_ask - best_bid
        ob.mid = (best_bid + best_ask) / 2

        # Imbalance (top 5 levels)
        bid_vol = sum(b[1] for b in ob.bids[:5])
        ask_vol = sum(a[1] for a in ob.asks[:5])
        total = bid_vol + ask_vol
        ob.imbalance = (bid_vol - ask_vol) / total if total > 0 else 0.0

        # Weighted average price
        all_levels = [[b[0], b[1]] for b in ob.bids[:10]] + [[a[0], a[1]] for a in ob.asks[:10]]
        total_vol = sum(x[1] for x in all_levels)
        ob.wap = sum(x[0] * x[1] for x in all_levels) / total_vol if total_vol > 0 else ob.mid

        # Wall detection
        avg_bid_size = np.mean([b[1] for b in ob.bids]) if ob.bids else 0
        avg_ask_size = np.mean([a[1] for a in ob.asks]) if ob.asks else 0
        ob.walls_bid = [b[0] for b in ob.bids if avg_bid_size > 0 and b[1] > avg_bid_size * config.OB_WALL_MULTIPLIER_3X]
        ob.walls_ask = [a[0] for a in ob.asks if avg_ask_size > 0 and a[1] > avg_ask_size * config.OB_WALL_MULTIPLIER_3X]

        # Liquidity depth within 1%
        depth_pct = config.OB_LIQUIDITY_DEPTH_PCT
        ob.liquidity_depth_bid = sum(b[1] * b[0] for b in ob.bids if b[0] >= ob.mid * (1 - depth_pct))
        ob.liquidity_depth_ask = sum(a[1] * a[0] for a in ob.asks if a[0] <= ob.mid * (1 + depth_pct))

    # ───────────────────────────────────────────
    # REST POLLING
    # ───────────────────────────────────────────

    async def start_rest_polling(self):
        """Run REST polling for funding, OI, 24h stats in background."""
        if not self._http_session:
            self._http_session = aiohttp.ClientSession()
        while True:
            try:
                now = time.time()
                if now - self._last_funding_poll > 60:
                    await self._poll_funding()
                    self._last_funding_poll = now
                if now - self._last_oi_poll > 60:
                    await self._poll_oi()
                    self._last_oi_poll = now
            except Exception as e:
                log.debug(f"REST poll error: {e}")
            await asyncio.sleep(30)

    async def _poll_funding(self):
        if not self._http_session:
            return
        try:
            url = f"{config.BINANCE_BASE_URL}/fapi/v1/premiumIndex"
            async with self._http_session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data:
                        sym = item.get("symbol", "")
                        if sym in self._funding:
                            self._funding[sym].rate = float(item.get("lastFundingRate", 0))
                            self._funding[sym].updated_at = time.time()
        except Exception as e:
            log.debug(f"Funding poll error: {e}")

    async def _poll_oi(self):
        if not self._http_session:
            return
        for sym in config.SYMBOLS:
            try:
                url = f"{config.BINANCE_BASE_URL}/fapi/v1/openInterest"
                params = {"symbol": sym}
                async with self._http_session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self._oi[sym].open_interest = float(data.get("openInterest", 0))
                        self._oi[sym].updated_at = time.time()
                await asyncio.sleep(0.1)  # rate limit
            except Exception as e:
                log.debug(f"OI poll {sym}: {e}")

    # ───────────────────────────────────────────
    # GETTERS
    # ───────────────────────────────────────────

    def get_candles(self, symbol: str, tf: str, n: int = 100) -> List[Candle]:
        buf = self._candles.get(symbol, {}).get(tf, deque())
        candles = list(buf)
        return candles[-n:] if n else candles

    def get_indicators(self, symbol: str, tf: str) -> Optional[Indicators]:
        return self._indicators.get(symbol, {}).get(tf)

    def get_orderbook(self, symbol: str) -> OrderbookState:
        return self._orderbooks.get(symbol, OrderbookState())

    def get_funding(self, symbol: str) -> FundingData:
        return self._funding.get(symbol, FundingData())

    def get_oi(self, symbol: str) -> OIData:
        return self._oi.get(symbol, OIData())

    def get_price(self, symbol: str) -> float:
        return self._prices.get(symbol, 0.0)

    def has_data(self, symbol: str, tf: str, min_candles: int = 100) -> bool:
        buf = self._candles.get(symbol, {}).get(tf, deque())
        return len(buf) >= min_candles

    def evict_old_candles(self):
        """Memory pressure relief — trim all buffers to minimum."""
        for sym in self._candles:
            for tf in self._candles[sym]:
                buf = self._candles[sym][tf]
                while len(buf) > config.CANDLE_BUFFER_MIN:
                    buf.popleft()
        log.info("Evicted old candles (memory relief)")

    def get_all_prices(self) -> Dict[str, float]:
        return dict(self._prices)
