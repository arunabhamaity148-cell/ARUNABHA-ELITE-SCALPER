"""
ARUNABHA ELITE SCALPER v3.0
FILE 5/18: orderflow_engine.py
CVD, delta analysis, whale detection, tape reading, volume profile
Data source: aggTrade stream
"""

import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np

import config

log = logging.getLogger("elite.orderflow")


# ═══════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════

@dataclass
class Trade:
    ts: float          # unix seconds
    price: float
    qty: float
    usdt_value: float
    is_buyer_maker: bool  # True = seller is aggressor (taker sell)

    @property
    def is_taker_buy(self) -> bool:
        return not self.is_buyer_maker

    @property
    def is_taker_sell(self) -> bool:
        return self.is_buyer_maker


@dataclass
class CandleDelta:
    ts: int            # candle open time
    buy_vol: float = 0.0
    sell_vol: float = 0.0
    delta: float = 0.0    # buy - sell
    delta_pct: float = 0.0
    cvd: float = 0.0


@dataclass
class OrderflowSnapshot:
    symbol: str
    # CVD
    cvd: float = 0.0
    cvd_change_1m: float = 0.0
    cvd_divergence: bool = False   # price up + CVD down = weakness
    # Delta
    delta_1m: float = 0.0
    delta_5m: float = 0.0
    delta_bullish: bool = True
    delta_extreme: bool = False    # > 3 std dev
    # Pressure
    buy_pressure_pct: float = 0.5  # 0-1
    trade_speed: float = 0.0       # trades/sec
    # Tape signals
    absorption_detected: bool = False
    exhaustion_detected: bool = False
    iceberg_detected: bool = False
    # Dominant side
    bid_dominant: bool = False
    ask_dominant: bool = False
    # POC
    poc_price: float = 0.0
    value_area_high: float = 0.0
    value_area_low: float = 0.0
    # Timestamp
    updated_at: float = 0.0


@dataclass
class WhaleAlert:
    symbol: str
    ts: float
    price: float
    usdt_value: float
    direction: str        # "BUY" or "SELL"
    tier: str             # "SMALL", "MEDIUM", "LARGE", "HUGE"
    cluster: bool = False


# ═══════════════════════════════════════════════
# ORDERFLOW ENGINE
# ═══════════════════════════════════════════════

class OrderflowEngine:
    def __init__(self, data_processor, telegram):
        self.dp = data_processor
        self.telegram = telegram

        # Per-symbol trade history (rolling 5 minutes)
        self._trades: Dict[str, deque] = {
            sym: deque(maxlen=5000) for sym in config.SYMBOLS
        }
        # CVD running total per symbol
        self._cvd: Dict[str, float] = {sym: 0.0 for sym in config.SYMBOLS}
        # Candle deltas per symbol per TF
        self._candle_deltas: Dict[str, Dict[str, deque]] = {
            sym: {"5m": deque(maxlen=200), "15m": deque(maxlen=200)}
            for sym in config.SYMBOLS
        }
        # Snapshots
        self._snapshots: Dict[str, OrderflowSnapshot] = {
            sym: OrderflowSnapshot(symbol=sym) for sym in config.SYMBOLS
        }
        # Recent whale alerts (for dedup)
        self._recent_whales: deque = deque(maxlen=100)
        # Volume profile (intraday): {symbol: {price_level: volume}}
        self._volume_profile: Dict[str, Dict[float, float]] = {
            sym: defaultdict(float) for sym in config.SYMBOLS
        }
        # Delta history for std dev calculation
        self._delta_history: Dict[str, deque] = {
            sym: deque(maxlen=100) for sym in config.SYMBOLS
        }
        self._shutdown = asyncio.Event()

    # ───────────────────────────────────────────
    # MAIN LOOP
    # ───────────────────────────────────────────

    async def run(self):
        """Background processing loop — update snapshots every second."""
        log.info("Orderflow engine started")
        while not self._shutdown.is_set():
            try:
                for sym in config.SYMBOLS:
                    self._update_snapshot(sym)
            except Exception as e:
                log.debug(f"Orderflow loop error: {e}")
            await asyncio.sleep(1.0)

    # ───────────────────────────────────────────
    # TRADE INGESTION (called by data_processor)
    # ───────────────────────────────────────────

    async def on_trade(self, symbol: str, data: dict):
        """Process aggTrade message from WS."""
        try:
            price = float(data["p"])
            qty = float(data["q"])
            is_buyer_maker = bool(data["m"])
            ts = float(data["T"]) / 1000

            trade = Trade(
                ts=ts,
                price=price,
                qty=qty,
                usdt_value=price * qty,
                is_buyer_maker=is_buyer_maker,
            )

            self._trades[symbol].append(trade)

            # Update CVD
            delta = trade.qty if trade.is_taker_buy else -trade.qty
            self._cvd[symbol] += delta

            # Volume profile (round to nearest 0.1%)
            level = self._round_to_vp_level(price)
            self._volume_profile[symbol][level] += trade.usdt_value

            # Whale detection
            if trade.usdt_value >= config.WHALE_THRESHOLD_SMALL:
                await self._handle_whale(symbol, trade)

        except Exception as e:
            log.debug(f"Trade parse error {symbol}: {e}")

    # ───────────────────────────────────────────
    # SNAPSHOT UPDATE
    # ───────────────────────────────────────────

    def _update_snapshot(self, symbol: str):
        snap = self._snapshots[symbol]
        trades = list(self._trades[symbol])
        now = time.time()

        if not trades:
            return

        # Last 60s trades
        t60 = [t for t in trades if now - t.ts <= 60]
        # Last 300s trades
        t300 = [t for t in trades if now - t.ts <= 300]

        # CVD
        snap.cvd = self._cvd[symbol]
        if t60:
            delta_60 = sum(t.qty if t.is_taker_buy else -t.qty for t in t60)
            snap.cvd_change_1m = delta_60

        # Delta 1m and 5m
        if t60:
            buy_60 = sum(t.qty for t in t60 if t.is_taker_buy)
            sell_60 = sum(t.qty for t in t60 if t.is_taker_sell)
            snap.delta_1m = buy_60 - sell_60
            snap.buy_pressure_pct = buy_60 / (buy_60 + sell_60) if (buy_60 + sell_60) > 0 else 0.5

        if t300:
            buy_300 = sum(t.qty for t in t300 if t.is_taker_buy)
            sell_300 = sum(t.qty for t in t300 if t.is_taker_sell)
            snap.delta_5m = buy_300 - sell_300
            snap.delta_bullish = snap.delta_5m > 0

        # Delta extreme detection
        self._delta_history[symbol].append(snap.delta_1m)
        if len(self._delta_history[symbol]) >= 20:
            hist = np.array(list(self._delta_history[symbol]))
            std = np.std(hist)
            snap.delta_extreme = std > 0 and abs(snap.delta_1m) > 3 * std

        # Trade speed
        snap.trade_speed = len(t60) / 60.0 if t60 else 0.0

        # CVD divergence: need price data
        price_now = self.dp.get_price(symbol)
        if price_now > 0 and t300:
            price_start = t300[0].price
            price_up = price_now > price_start * 1.001
            cvd_down = snap.delta_5m < 0
            snap.cvd_divergence = price_up and cvd_down

        # Tape signals
        snap.absorption_detected = self._detect_absorption(symbol, t60)
        snap.exhaustion_detected = self._detect_exhaustion(symbol, t60)
        snap.iceberg_detected = self._detect_iceberg(t60)
        snap.bid_dominant, snap.ask_dominant = self._detect_dominance(t60)

        # Volume profile
        self._update_poc(symbol)

        snap.updated_at = now

    # ───────────────────────────────────────────
    # TAPE READING
    # ───────────────────────────────────────────

    def _detect_absorption(self, symbol: str, trades: List[Trade]) -> bool:
        """Heavy selling at a level + price holds = absorption (bullish)."""
        if len(trades) < 10:
            return False
        sells = [t for t in trades if t.is_taker_sell]
        if not sells:
            return False
        sell_vol = sum(t.usdt_value for t in sells)
        # Heavy sell pressure
        if sell_vol < config.WHALE_THRESHOLD_MEDIUM:
            return False
        # Price should be flat or rising (absorbed)
        prices = [t.price for t in trades]
        price_change_pct = (prices[-1] - prices[0]) / prices[0] if prices[0] > 0 else 0
        return price_change_pct > -0.001  # price flat or up despite selling

    def _detect_exhaustion(self, symbol: str, trades: List[Trade]) -> bool:
        """Heavy buying at level + price stalls = exhaustion (bearish)."""
        if len(trades) < 10:
            return False
        buys = [t for t in trades if t.is_taker_buy]
        if not buys:
            return False
        buy_vol = sum(t.usdt_value for t in buys)
        if buy_vol < config.WHALE_THRESHOLD_MEDIUM:
            return False
        prices = [t.price for t in trades]
        price_change_pct = (prices[-1] - prices[0]) / prices[0] if prices[0] > 0 else 0
        return price_change_pct < 0.001  # price flat or down despite buying

    def _detect_iceberg(self, trades: List[Trade]) -> bool:
        """Repeated same size trades = iceberg / hidden order."""
        if len(trades) < 20:
            return False
        sizes = [round(t.qty, 2) for t in trades]
        from collections import Counter
        counts = Counter(sizes)
        most_common_count = counts.most_common(1)[0][1]
        # If same size appears >5 times in last 60s = likely iceberg
        return most_common_count >= 5

    def _detect_dominance(self, trades: List[Trade]) -> Tuple[bool, bool]:
        """Check if bid or ask side is being repeatedly hit."""
        if len(trades) < 10:
            return False, False
        buy_count = sum(1 for t in trades if t.is_taker_buy)
        sell_count = len(trades) - buy_count
        total = len(trades)
        bid_dom = buy_count / total > 0.70
        ask_dom = sell_count / total > 0.70
        return bid_dom, ask_dom

    # ───────────────────────────────────────────
    # VOLUME PROFILE
    # ───────────────────────────────────────────

    def _round_to_vp_level(self, price: float) -> float:
        """Round price to nearest 0.1% level for volume profile."""
        if price <= 0:
            return 0.0
        step = price * 0.001
        if step < 0.01:
            step = 0.01
        return round(price / step) * step

    def _update_poc(self, symbol: str):
        """Calculate Point of Control and Value Area."""
        profile = self._volume_profile[symbol]
        if not profile:
            return
        snap = self._snapshots[symbol]

        sorted_levels = sorted(profile.items(), key=lambda x: -x[1])
        if not sorted_levels:
            return

        snap.poc_price = sorted_levels[0][0]

        # Value Area = 70% of total volume
        total = sum(v for _, v in sorted_levels)
        target = total * 0.70
        accumulated = 0.0
        va_levels = []
        for price, vol in sorted_levels:
            accumulated += vol
            va_levels.append(price)
            if accumulated >= target:
                break

        if va_levels:
            snap.value_area_high = max(va_levels)
            snap.value_area_low = min(va_levels)

    def reset_daily_profile(self, symbol: str):
        """Reset volume profile at start of new day."""
        self._volume_profile[symbol].clear()
        self._cvd[symbol] = 0.0
        log.debug(f"VP reset for {symbol}")

    # ───────────────────────────────────────────
    # WHALE DETECTION
    # ───────────────────────────────────────────

    async def _handle_whale(self, symbol: str, trade: Trade):
        direction = "BUY" if trade.is_taker_buy else "SELL"

        # Determine tier
        if trade.usdt_value >= config.WHALE_THRESHOLD_HUGE:
            tier = "HUGE"
        elif trade.usdt_value >= config.WHALE_THRESHOLD_LARGE:
            tier = "LARGE"
        elif trade.usdt_value >= config.WHALE_THRESHOLD_MEDIUM:
            tier = "MEDIUM"
        else:
            tier = "SMALL"

        # Dedup: same symbol + direction + tier within 10s
        key = f"{symbol}:{direction}:{tier}"
        now = time.time()
        recent_keys = [(a, t) for a, t in self._recent_whales]
        duplicate = any(a == key and now - t < 10 for a, t in recent_keys)

        if duplicate:
            return

        self._recent_whales.append((key, now))

        # Check for cluster (multiple large trades in 10s)
        recent_large = [
            t for t in self._trades[symbol]
            if now - t.ts <= config.WHALE_CLUSTER_SECONDS
            and t.usdt_value >= config.WHALE_THRESHOLD_MEDIUM
        ]
        cluster = len(recent_large) >= 3

        alert = WhaleAlert(
            symbol=symbol,
            ts=now,
            price=trade.price,
            usdt_value=trade.usdt_value,
            direction=direction,
            tier=tier,
            cluster=cluster,
        )

        # Only alert MEDIUM+ and clusters
        if tier in ("LARGE", "HUGE") or cluster:
            await self._send_whale_alert(alert)

    async def _send_whale_alert(self, alert: WhaleAlert):
        emoji = "🐋" if alert.tier == "HUGE" else "🦈" if alert.tier == "LARGE" else "🐟"
        cluster_tag = " [CLUSTER]" if alert.cluster else ""
        dir_emoji = "🟢" if alert.direction == "BUY" else "🔴"

        msg = (
            f"{emoji} <b>WHALE ALERT{cluster_tag}</b>\n"
            f"Symbol: <b>{alert.symbol}</b>\n"
            f"Direction: {dir_emoji} <b>{alert.direction}</b>\n"
            f"Size: <b>${alert.usdt_value:,.0f}</b> ({alert.tier})\n"
            f"Price: {alert.price:,.4f}"
        )
        try:
            await self.telegram.send_alert(msg, priority="whale")
        except Exception as e:
            log.debug(f"Whale alert send error: {e}")

    # ───────────────────────────────────────────
    # GETTERS
    # ───────────────────────────────────────────

    def get_snapshot(self, symbol: str) -> OrderflowSnapshot:
        return self._snapshots.get(symbol, OrderflowSnapshot(symbol=symbol))

    def get_cvd(self, symbol: str) -> float:
        return self._cvd.get(symbol, 0.0)

    def cvd_aligned_with_direction(self, symbol: str, direction: str) -> bool:
        """Check if CVD momentum aligns with proposed trade direction."""
        snap = self._snapshots.get(symbol)
        if not snap:
            return True  # default allow
        if direction == "LONG":
            return snap.delta_5m > 0
        else:
            return snap.delta_5m < 0

    def get_buy_pressure(self, symbol: str) -> float:
        """0-1, where >0.6 = buy pressure, <0.4 = sell pressure."""
        snap = self._snapshots.get(symbol)
        return snap.buy_pressure_pct if snap else 0.5

    async def close(self):
        self._shutdown.set()
