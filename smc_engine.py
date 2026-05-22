"""
ARUNABHA ELITE SCALPER v3.0
FILE: smc_engine.py
Smart Money Concepts Engine — 5 new features:
  1. Smart Money Orderblock (Bullish/Bearish)
  2. Fair Value Gap (FVG)
  3. Breaker Block
  4. Long/Short Ratio Trap
  5. Asian Range Breakout
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import aiohttp
import numpy as np

import config

log = logging.getLogger("elite.smc")


# ═══════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════

@dataclass
class Orderblock:
    price_high: float
    price_low: float
    direction: str          # "BULLISH" (demand zone) or "BEARISH" (supply zone)
    tf: str                 # timeframe where it was formed
    ts: int                 # candle timestamp
    broken: bool = False    # True = became a Breaker Block
    mitigated: bool = False # True = price already traded through it
    strength: float = 0.0   # 0-1, based on move size after OB formed

    @property
    def mid(self) -> float:
        return (self.price_high + self.price_low) / 2

    def contains(self, price: float) -> bool:
        return self.price_low <= price <= self.price_high

    def near(self, price: float, pct: float = 0.005) -> bool:
        """True if price is within pct% of the orderblock."""
        margin = self.mid * pct
        return abs(price - self.mid) <= margin + (self.price_high - self.price_low) / 2


@dataclass
class FairValueGap:
    price_high: float       # top of gap
    price_low: float        # bottom of gap
    direction: str          # "BULLISH" (gap up) or "BEARISH" (gap down)
    ts: int                 # timestamp of the 2nd candle (middle of 3-candle pattern)
    filled: bool = False    # True = price traded back through gap
    fill_pct: float = 0.0   # how much of gap is filled (0-1)

    @property
    def size(self) -> float:
        return self.price_high - self.price_low

    @property
    def mid(self) -> float:
        return (self.price_high + self.price_low) / 2

    def contains(self, price: float) -> bool:
        return self.price_low <= price <= self.price_high


@dataclass
class BreakerBlock:
    """Failed Orderblock that reversed — now acts as opposite zone."""
    price_high: float
    price_low: float
    original_direction: str  # was "BULLISH" OB, now acts as resistance
    breaker_direction: str   # "BEARISH" (former demand = now supply)
    ts: int
    strength: float = 0.0

    @property
    def mid(self) -> float:
        return (self.price_high + self.price_low) / 2

    def near(self, price: float, pct: float = 0.006) -> bool:
        margin = self.mid * pct
        return abs(price - self.mid) <= margin + (self.price_high - self.price_low) / 2


@dataclass
class LongShortData:
    symbol: str
    long_pct: float = 0.5      # 0-1
    short_pct: float = 0.5
    ls_ratio: float = 1.0      # long/short
    is_crowded_long: bool = False   # >70% long = trap signal
    is_crowded_short: bool = False  # <30% long = trap signal
    updated_at: float = 0.0


@dataclass
class AsianRangeData:
    symbol: str
    session_high: float = 0.0
    session_low: float = 0.0
    range_size: float = 0.0
    range_pct: float = 0.0      # range / price
    breakout_direction: Optional[str] = None   # "LONG" or "SHORT" or None
    breakout_confirmed: bool = False
    in_asian_session: bool = False
    updated_at: float = 0.0


@dataclass
class SMCSnapshot:
    symbol: str
    orderblocks: List[Orderblock] = field(default_factory=list)
    fvgs: List[FairValueGap] = field(default_factory=list)
    breaker_blocks: List[BreakerBlock] = field(default_factory=list)
    # Nearest relevant zones to current price
    nearest_ob_bull: Optional[Orderblock] = None    # nearest demand zone below price
    nearest_ob_bear: Optional[Orderblock] = None    # nearest supply zone above price
    nearest_fvg_bull: Optional[FairValueGap] = None
    nearest_fvg_bear: Optional[FairValueGap] = None
    nearest_breaker: Optional[BreakerBlock] = None
    # Scores
    ob_score: float = 0.0       # 0-10: price at OB
    fvg_score: float = 0.0      # 0-8: FVG present or acting as magnet
    breaker_score: float = 0.0  # 0-7: breaker block context
    ls_score: float = 0.0       # 0-8: long/short ratio
    asian_score: float = 0.0    # 0-7: asian range breakout
    total_score: float = 0.0    # sum, max 40
    updated_at: float = 0.0


# ═══════════════════════════════════════════════
# SMC ENGINE
# ═══════════════════════════════════════════════

class SMCEngine:
    """
    Smart Money Concepts Engine.
    Analyzes orderblocks, FVGs, breaker blocks,
    long/short ratio, and asian range breakout.
    """

    # Scoring caps per feature (total max = 40 points added to signal score)
    OB_MAX = 10
    FVG_MAX = 8
    BREAKER_MAX = 7
    LS_MAX = 8
    ASIAN_MAX = 7

    # OB detection params
    OB_LOOKBACK = 30          # candles to look back
    OB_MIN_MOVE_PCT = 0.008   # body must be at least 0.8% (filters noise)
    OB_MAX_STALE_BARS = 20    # OB older than this = stale, skip

    # FVG detection params
    FVG_MIN_SIZE_PCT = 0.002  # gap must be ≥0.2% of price
    FVG_MAX_STALE_BARS = 15

    # Long/Short thresholds
    LS_CROWDED_LONG = 0.68    # ≥68% long = crowd trap
    LS_CROWDED_SHORT = 0.32   # ≤32% long = crowd trap

    # Asian session UTC hours
    ASIAN_START_UTC = 0
    ASIAN_END_UTC = 8

    def __init__(self, data_processor):
        self.dp = data_processor
        self._snapshots: Dict[str, SMCSnapshot] = {
            sym: SMCSnapshot(symbol=sym) for sym in config.SYMBOLS
        }
        self._ls_data: Dict[str, LongShortData] = {
            sym: LongShortData(symbol=sym) for sym in config.SYMBOLS
        }
        self._asian_data: Dict[str, AsianRangeData] = {
            sym: AsianRangeData(symbol=sym) for sym in config.SYMBOLS
        }
        self._http_session: Optional[aiohttp.ClientSession] = None
        self._last_ls_poll: float = 0.0
        self._last_asian_update: Dict[str, float] = {sym: 0.0 for sym in config.SYMBOLS}

    # ───────────────────────────────────────────
    # PUBLIC API
    # ───────────────────────────────────────────

    async def update(self, symbol: str):
        """Full SMC analysis. Call before signal scoring."""
        try:
            price = self.dp.get_price(symbol)
            if price <= 0:
                return

            candles_15m = self.dp.get_candles(symbol, "15m", n=50)
            candles_1h = self.dp.get_candles(symbol, "1h", n=50)

            if len(candles_15m) < 10:
                return

            snap = self._snapshots[symbol]
            snap.symbol = symbol

            # 1. Detect orderblocks (1h for institutional quality)
            obs = self._detect_orderblocks(candles_1h, tf="1h")
            # Also 15m OBs for precision entry
            obs_15m = self._detect_orderblocks(candles_15m, tf="15m")
            snap.orderblocks = obs + obs_15m

            # 2. Detect FVGs on 15m
            snap.fvgs = self._detect_fvgs(candles_15m)

            # 3. Derive breaker blocks from broken OBs
            snap.breaker_blocks = self._derive_breakers(snap.orderblocks, price)

            # 4. Mark mitigated zones
            self._update_mitigation(snap, price)

            # 5. Find nearest zones to price
            self._find_nearest_zones(snap, price)

            # 6. Compute sub-scores
            snap.ob_score = self._score_ob(snap, price)
            snap.fvg_score = self._score_fvg(snap, price)
            snap.breaker_score = self._score_breaker(snap, price)

            # Long/Short and Asian range scored separately (async API data)
            snap.total_score = (
                snap.ob_score
                + snap.fvg_score
                + snap.breaker_score
                + snap.ls_score      # updated by poll_long_short_ratio()
                + snap.asian_score   # updated by update_asian_range()
            )
            snap.updated_at = time.time()

        except Exception as e:
            log.debug(f"SMC update error {symbol}: {e}")

    def get_snapshot(self, symbol: str) -> SMCSnapshot:
        return self._snapshots.get(symbol, SMCSnapshot(symbol=symbol))

    def get_ls_data(self, symbol: str) -> LongShortData:
        return self._ls_data.get(symbol, LongShortData(symbol=symbol))

    def get_asian_data(self, symbol: str) -> AsianRangeData:
        return self._asian_data.get(symbol, AsianRangeData(symbol=symbol))

    # ───────────────────────────────────────────
    # 1. ORDERBLOCK DETECTION
    # ───────────────────────────────────────────

    def _detect_orderblocks(self, candles: list, tf: str) -> List[Orderblock]:
        """
        Bullish OB: Last bearish candle before a strong bullish move.
        Bearish OB: Last bullish candle before a strong bearish move.
        """
        obs = []
        if len(candles) < 5:
            return obs

        for i in range(2, len(candles) - 2):
            c = candles[i]
            body = abs(c.c - c.o)
            price_ref = c.c if c.c > 0 else 1.0

            # Skip doji / tiny candles
            if body / price_ref < 0.001:
                continue

            # Check move AFTER this candle
            move_forward = self._measure_forward_move(candles, i)

            # ── BULLISH OB: bearish candle before strong bullish impulse ──
            if c.c < c.o:  # bearish candle
                if move_forward > self.OB_MIN_MOVE_PCT:
                    strength = min(move_forward / 0.03, 1.0)
                    ob = Orderblock(
                        price_high=c.o,   # top of bearish candle body
                        price_low=c.l,    # low wick included for full zone
                        direction="BULLISH",
                        tf=tf,
                        ts=c.ts,
                        strength=strength,
                    )
                    obs.append(ob)

            # ── BEARISH OB: bullish candle before strong bearish impulse ──
            elif c.c > c.o:  # bullish candle
                if move_forward < -self.OB_MIN_MOVE_PCT:
                    strength = min(abs(move_forward) / 0.03, 1.0)
                    ob = Orderblock(
                        price_high=c.h,   # high wick for full zone
                        price_low=c.o,    # bottom of bullish candle body
                        direction="BEARISH",
                        tf=tf,
                        ts=c.ts,
                        strength=strength,
                    )
                    obs.append(ob)

        # Keep only recent ones (not stale)
        if candles:
            cutoff_ts = candles[-1].ts - (self.OB_MAX_STALE_BARS * self._tf_ms(tf))
            obs = [ob for ob in obs if ob.ts >= cutoff_ts]

        return obs

    def _measure_forward_move(self, candles: list, idx: int, bars: int = 3) -> float:
        """
        Measure how much price moved after candle[idx].
        Positive = bullish move, Negative = bearish move.
        Returns move as fraction of price.
        """
        if idx + bars >= len(candles):
            bars = len(candles) - idx - 1
        if bars <= 0:
            return 0.0

        base_price = candles[idx].c
        if base_price <= 0:
            return 0.0

        future_high = max(c.h for c in candles[idx + 1:idx + 1 + bars])
        future_low = min(c.l for c in candles[idx + 1:idx + 1 + bars])

        bull_move = (future_high - base_price) / base_price
        bear_move = (future_low - base_price) / base_price

        # Return the dominant direction
        if abs(bull_move) >= abs(bear_move):
            return bull_move
        return bear_move

    def _tf_ms(self, tf: str) -> int:
        """Timeframe to milliseconds."""
        mapping = {"1m": 60000, "5m": 300000, "15m": 900000,
                   "1h": 3600000, "4h": 14400000, "1d": 86400000}
        return mapping.get(tf, 900000)

    # ───────────────────────────────────────────
    # 2. FAIR VALUE GAP DETECTION
    # ───────────────────────────────────────────

    def _detect_fvgs(self, candles: list) -> List[FairValueGap]:
        """
        3-candle FVG pattern:
        Bullish FVG: candle[i].low > candle[i-2].high → gap between them
        Bearish FVG: candle[i].high < candle[i-2].low → gap between them
        """
        fvgs = []
        if len(candles) < 3:
            return fvgs

        price_ref = candles[-1].c if candles[-1].c > 0 else 1.0
        cutoff_ts = candles[-1].ts - (self.FVG_MAX_STALE_BARS * self._tf_ms("15m"))

        for i in range(2, len(candles)):
            c0 = candles[i - 2]   # first candle
            c2 = candles[i]       # third candle

            if c0.ts < cutoff_ts:
                continue

            # Bullish FVG: gap between c0.high and c2.low (price moved up fast)
            if c2.l > c0.h:
                gap_size = c2.l - c0.h
                if gap_size / price_ref >= self.FVG_MIN_SIZE_PCT:
                    fvgs.append(FairValueGap(
                        price_high=c2.l,
                        price_low=c0.h,
                        direction="BULLISH",
                        ts=candles[i - 1].ts,  # middle candle timestamp
                    ))

            # Bearish FVG: gap between c2.high and c0.low (price moved down fast)
            elif c2.h < c0.l:
                gap_size = c0.l - c2.h
                if gap_size / price_ref >= self.FVG_MIN_SIZE_PCT:
                    fvgs.append(FairValueGap(
                        price_high=c0.l,
                        price_low=c2.h,
                        direction="BEARISH",
                        ts=candles[i - 1].ts,
                    ))

        return fvgs

    # ───────────────────────────────────────────
    # 3. BREAKER BLOCK DERIVATION
    # ───────────────────────────────────────────

    def _derive_breakers(self, orderblocks: List[Orderblock], price: float) -> List[BreakerBlock]:
        """
        A Bullish OB that price has traded through (broken) becomes a
        Bearish Breaker Block (old demand = now resistance).
        Vice versa for Bearish OB.
        """
        breakers = []

        for ob in orderblocks:
            if ob.direction == "BULLISH":
                # Bullish OB is broken when price closes below the OB low
                if price < ob.price_low * 0.999:
                    ob.broken = True
                    breakers.append(BreakerBlock(
                        price_high=ob.price_high,
                        price_low=ob.price_low,
                        original_direction="BULLISH",
                        breaker_direction="BEARISH",
                        ts=ob.ts,
                        strength=ob.strength,
                    ))

            elif ob.direction == "BEARISH":
                # Bearish OB is broken when price closes above the OB high
                if price > ob.price_high * 1.001:
                    ob.broken = True
                    breakers.append(BreakerBlock(
                        price_high=ob.price_high,
                        price_low=ob.price_low,
                        original_direction="BEARISH",
                        breaker_direction="BULLISH",
                        ts=ob.ts,
                        strength=ob.strength,
                    ))

        return breakers

    # ───────────────────────────────────────────
    # MITIGATION & NEAREST ZONES
    # ───────────────────────────────────────────

    def _update_mitigation(self, snap: SMCSnapshot, price: float):
        """Mark OBs/FVGs that price has already traded through."""
        for ob in snap.orderblocks:
            if ob.broken:
                ob.mitigated = True
                continue
            if ob.direction == "BULLISH" and price < ob.price_low:
                ob.mitigated = True
            elif ob.direction == "BEARISH" and price > ob.price_high:
                ob.mitigated = True

        for fvg in snap.fvgs:
            if fvg.direction == "BULLISH" and price < fvg.price_low:
                fvg.filled = True
                fvg.fill_pct = 1.0
            elif fvg.direction == "BEARISH" and price > fvg.price_high:
                fvg.filled = True
                fvg.fill_pct = 1.0
            elif fvg.contains(price):
                # Partially filled
                if fvg.direction == "BULLISH":
                    fvg.fill_pct = (price - fvg.price_low) / max(fvg.size, 0.0001)
                else:
                    fvg.fill_pct = (fvg.price_high - price) / max(fvg.size, 0.0001)

    def _find_nearest_zones(self, snap: SMCSnapshot, price: float):
        """Find nearest valid (not broken/mitigated) zones."""
        # Bullish OBs below price (demand zones)
        bull_obs = [
            ob for ob in snap.orderblocks
            if ob.direction == "BULLISH"
            and not ob.mitigated
            and not ob.broken
            and ob.price_high <= price * 1.003  # at or slightly above price is fine
        ]
        if bull_obs:
            snap.nearest_ob_bull = max(bull_obs, key=lambda x: x.price_high)

        # Bearish OBs above price (supply zones)
        bear_obs = [
            ob for ob in snap.orderblocks
            if ob.direction == "BEARISH"
            and not ob.mitigated
            and not ob.broken
            and ob.price_low >= price * 0.997
        ]
        if bear_obs:
            snap.nearest_ob_bear = min(bear_obs, key=lambda x: x.price_low)

        # FVGs
        bull_fvgs = [
            fvg for fvg in snap.fvgs
            if fvg.direction == "BULLISH"
            and not fvg.filled
            and fvg.price_high <= price * 1.003
        ]
        if bull_fvgs:
            snap.nearest_fvg_bull = max(bull_fvgs, key=lambda x: x.price_high)

        bear_fvgs = [
            fvg for fvg in snap.fvgs
            if fvg.direction == "BEARISH"
            and not fvg.filled
            and fvg.price_low >= price * 0.997
        ]
        if bear_fvgs:
            snap.nearest_fvg_bear = min(bear_fvgs, key=lambda x: x.price_low)

        # Nearest breaker
        all_breakers = [b for b in snap.breaker_blocks if b.near(price, pct=0.02)]
        if all_breakers:
            snap.nearest_breaker = min(all_breakers, key=lambda x: abs(x.mid - price))

    # ───────────────────────────────────────────
    # SCORING FUNCTIONS
    # ───────────────────────────────────────────

    def _score_ob(self, snap: SMCSnapshot, price: float) -> float:
        """
        Score based on price proximity to a valid orderblock.
        LONG: price at/near bullish OB (demand zone)
        SHORT: price at/near bearish OB (supply zone)
        Max = OB_MAX (10)
        """
        score = 0.0

        if snap.nearest_ob_bull:
            ob = snap.nearest_ob_bull
            dist_pct = abs(price - ob.mid) / price
            if ob.contains(price):
                score = self.OB_MAX * ob.strength  # inside zone = full score
            elif dist_pct < 0.005:
                score = self.OB_MAX * ob.strength * 0.7
            elif dist_pct < 0.01:
                score = self.OB_MAX * ob.strength * 0.4

        if snap.nearest_ob_bear:
            ob = snap.nearest_ob_bear
            dist_pct = abs(price - ob.mid) / price
            bear_score = 0.0
            if ob.contains(price):
                bear_score = self.OB_MAX * ob.strength
            elif dist_pct < 0.005:
                bear_score = self.OB_MAX * ob.strength * 0.7
            elif dist_pct < 0.01:
                bear_score = self.OB_MAX * ob.strength * 0.4
            score = max(score, bear_score)

        return round(min(score, self.OB_MAX), 2)

    def _score_fvg(self, snap: SMCSnapshot, price: float) -> float:
        """
        Score based on FVG presence.
        - Price at unfilled FVG edge = high score (fill expected)
        - FVG above/below acting as magnet = medium score
        Max = FVG_MAX (8)
        """
        score = 0.0

        # Bullish FVG below price = support, price may bounce
        if snap.nearest_fvg_bull and not snap.nearest_fvg_bull.filled:
            fvg = snap.nearest_fvg_bull
            dist_pct = abs(price - fvg.price_high) / price
            if fvg.contains(price):
                score = self.FVG_MAX  # inside FVG
            elif dist_pct < 0.003:
                score = self.FVG_MAX * 0.8
            elif dist_pct < 0.008:
                score = self.FVG_MAX * 0.5

        # Bearish FVG above price = resistance
        if snap.nearest_fvg_bear and not snap.nearest_fvg_bear.filled:
            fvg = snap.nearest_fvg_bear
            dist_pct = abs(price - fvg.price_low) / price
            bear_score = 0.0
            if fvg.contains(price):
                bear_score = self.FVG_MAX
            elif dist_pct < 0.003:
                bear_score = self.FVG_MAX * 0.8
            elif dist_pct < 0.008:
                bear_score = self.FVG_MAX * 0.5
            score = max(score, bear_score)

        return round(min(score, self.FVG_MAX), 2)

    def _score_breaker(self, snap: SMCSnapshot, price: float) -> float:
        """
        Score based on breaker block context.
        If price is retesting a breaker block from the correct side,
        it's a high-conviction entry point.
        Max = BREAKER_MAX (7)
        """
        score = 0.0

        if not snap.nearest_breaker:
            return score

        bb = snap.nearest_breaker
        dist_pct = abs(price - bb.mid) / price

        # Retesting breaker from correct side
        if bb.near(price, pct=0.008):
            score = self.BREAKER_MAX * bb.strength
        elif dist_pct < 0.015:
            score = self.BREAKER_MAX * bb.strength * 0.5

        return round(min(score, self.BREAKER_MAX), 2)

    # ───────────────────────────────────────────
    # 4. LONG/SHORT RATIO — REST POLLING
    # ───────────────────────────────────────────

    async def poll_long_short_ratio(self):
        """
        Fetch Binance global long/short account ratio.
        Endpoint: GET /futures/data/globalLongShortAccountRatio
        Poll every 5 minutes (data refreshes every 5 min on Binance).
        """
        now = time.time()
        if now - self._last_ls_poll < 300:  # 5 min cooldown
            return

        if not self._http_session:
            self._http_session = aiohttp.ClientSession()

        self._last_ls_poll = now

        for sym in config.SYMBOLS:
            try:
                url = f"{config.BINANCE_BASE_URL}/futures/data/globalLongShortAccountRatio"
                params = {"symbol": sym, "period": "5m", "limit": 1}
                async with self._http_session.get(
                    url, params=params, timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data and len(data) > 0:
                            row = data[0]
                            long_pct = float(row.get("longAccount", 0.5))
                            short_pct = float(row.get("shortAccount", 0.5))
                            ls_ratio = float(row.get("longShortRatio", 1.0))

                            ls = self._ls_data[sym]
                            ls.long_pct = long_pct
                            ls.short_pct = short_pct
                            ls.ls_ratio = ls_ratio
                            ls.is_crowded_long = long_pct >= self.LS_CROWDED_LONG
                            ls.is_crowded_short = long_pct <= self.LS_CROWDED_SHORT
                            ls.updated_at = now

                            # Update LS score in snapshot
                            snap = self._snapshots[sym]
                            snap.ls_score = self._score_ls(ls)
                            snap.total_score = (
                                snap.ob_score + snap.fvg_score + snap.breaker_score
                                + snap.ls_score + snap.asian_score
                            )

                await asyncio.sleep(0.15)  # rate limit

            except Exception as e:
                log.debug(f"LS ratio poll {sym}: {e}")

    def _score_ls(self, ls: LongShortData) -> float:
        """
        Score long/short ratio for contrarian signal quality.
        Crowded long (>68%) = good for SHORT
        Crowded short (<32%) = good for LONG
        Neutral = low score
        Max = LS_MAX (8)
        """
        if ls.updated_at == 0:
            return 0.0

        score = 0.0

        # Extreme crowd positioning = contrarian opportunity
        if ls.is_crowded_long:
            # 68-75% long → moderate score, >75% → full score
            intensity = (ls.long_pct - self.LS_CROWDED_LONG) / (0.80 - self.LS_CROWDED_LONG)
            score = self.LS_MAX * min(intensity + 0.5, 1.0)

        elif ls.is_crowded_short:
            intensity = (self.LS_CROWDED_SHORT - ls.long_pct) / self.LS_CROWDED_SHORT
            score = self.LS_MAX * min(intensity + 0.5, 1.0)

        # Moderate positioning still gets partial score
        else:
            deviation = abs(ls.long_pct - 0.5)  # 0-0.18 range for non-extreme
            score = self.LS_MAX * 0.3 * (deviation / 0.18)

        return round(min(score, self.LS_MAX), 2)

    def get_ls_bias(self, symbol: str) -> Optional[str]:
        """
        Returns direction bias from L/S ratio:
        'SHORT' if crowded long (fade longs)
        'LONG' if crowded short (fade shorts)
        None if neutral
        """
        ls = self._ls_data.get(symbol)
        if not ls or ls.updated_at == 0:
            return None
        if ls.is_crowded_long:
            return "SHORT"
        if ls.is_crowded_short:
            return "LONG"
        return None

    # ───────────────────────────────────────────
    # 5. ASIAN RANGE BREAKOUT
    # ───────────────────────────────────────────

    def update_asian_range(self, symbol: str):
        """
        Track Asian session (00:00–08:00 UTC) high/low.
        Detect breakout of that range in London/NY session.
        """
        import datetime

        try:
            price = self.dp.get_price(symbol)
            if price <= 0:
                return

            now_utc = datetime.datetime.utcnow()
            current_hour = now_utc.hour

            ar = self._asian_data[symbol]
            ar.symbol = symbol
            ar.in_asian_session = self.ASIAN_START_UTC <= current_hour < self.ASIAN_END_UTC

            # During Asian session: track range
            if ar.in_asian_session:
                if ar.session_high == 0.0:
                    ar.session_high = price
                    ar.session_low = price
                else:
                    ar.session_high = max(ar.session_high, price)
                    ar.session_low = min(ar.session_low, price)
                ar.range_size = ar.session_high - ar.session_low
                ar.range_pct = ar.range_size / price if price > 0 else 0.0
                ar.breakout_direction = None
                ar.breakout_confirmed = False
                ar.updated_at = time.time()
                return

            # Reset at start of each Asian session
            if current_hour == self.ASIAN_END_UTC:
                # Store last range before clearing (keep it for London session use)
                pass

            # Outside Asian session: check for breakout
            if ar.session_high > 0 and ar.session_low > 0 and ar.range_size > 0:
                # Require minimum range (avoids noise on flat sessions)
                min_range_pct = 0.003  # 0.3% minimum session range
                if ar.range_pct < min_range_pct:
                    ar.updated_at = time.time()
                    self._update_asian_score(symbol, ar)
                    return

                # Breakout detection: close above/below session range
                breakout_margin = ar.range_size * 0.1  # 10% of range as buffer

                if price > ar.session_high + breakout_margin:
                    ar.breakout_direction = "LONG"
                    ar.breakout_confirmed = True
                elif price < ar.session_low - breakout_margin:
                    ar.breakout_direction = "SHORT"
                    ar.breakout_confirmed = True
                else:
                    ar.breakout_direction = None
                    ar.breakout_confirmed = False

                ar.updated_at = time.time()
                self._update_asian_score(symbol, ar)

        except Exception as e:
            log.debug(f"Asian range update {symbol}: {e}")

    def reset_asian_session(self, symbol: str):
        """Call this at 00:00 UTC to reset the session high/low."""
        ar = self._asian_data[symbol]
        ar.session_high = 0.0
        ar.session_low = 0.0
        ar.range_size = 0.0
        ar.range_pct = 0.0
        ar.breakout_direction = None
        ar.breakout_confirmed = False

    def _update_asian_score(self, symbol: str, ar: AsianRangeData):
        """Update asian_score in snapshot."""
        score = 0.0

        if ar.breakout_confirmed and ar.breakout_direction:
            # Clean breakout confirmed
            score = self.ASIAN_MAX

            # Bonus for larger session range (more compressed = stronger break)
            if ar.range_pct > 0.01:  # >1% range = meaningful compression
                score = min(score + 1, self.ASIAN_MAX)

        elif ar.breakout_direction and not ar.breakout_confirmed:
            score = self.ASIAN_MAX * 0.4  # unconfirmed, partial

        snap = self._snapshots[symbol]
        snap.asian_score = round(score, 2)
        snap.total_score = (
            snap.ob_score + snap.fvg_score + snap.breaker_score
            + snap.ls_score + snap.asian_score
        )

    def get_asian_bias(self, symbol: str) -> Optional[str]:
        """Returns 'LONG', 'SHORT', or None based on Asian range breakout."""
        ar = self._asian_data.get(symbol)
        if not ar or not ar.breakout_confirmed:
            return None
        return ar.breakout_direction

    # ───────────────────────────────────────────
    # DIRECTION HELPERS (for signal_engine use)
    # ───────────────────────────────────────────

    def get_smc_direction_bias(self, symbol: str, proposed_direction: str) -> Tuple[bool, str]:
        """
        Returns (aligned: bool, reason: str).
        Checks if proposed_direction is aligned with SMC context.
        Used by signal_engine to confirm or block signals.
        """
        snap = self._snapshots.get(symbol)
        if not snap or snap.updated_at == 0:
            return True, "smc_no_data"

        reasons = []
        conflicts = 0
        confirmations = 0

        # OB alignment check
        if proposed_direction == "LONG":
            if snap.nearest_ob_bull and not snap.nearest_ob_bull.mitigated:
                confirmations += 1
                reasons.append("at_bullish_ob")
            if snap.nearest_ob_bear:
                ob = snap.nearest_ob_bear
                price = self.dp.get_price(symbol)
                if ob.near(price, pct=0.005):
                    conflicts += 1
                    reasons.append("near_bearish_ob")
        else:  # SHORT
            if snap.nearest_ob_bear and not snap.nearest_ob_bear.mitigated:
                confirmations += 1
                reasons.append("at_bearish_ob")
            if snap.nearest_ob_bull:
                ob = snap.nearest_ob_bull
                price = self.dp.get_price(symbol)
                if ob.near(price, pct=0.005):
                    conflicts += 1
                    reasons.append("near_bullish_ob")

        # L/S bias check
        ls_bias = self.get_ls_bias(symbol)
        if ls_bias == proposed_direction:
            confirmations += 1
            reasons.append("ls_aligned")
        elif ls_bias and ls_bias != proposed_direction:
            # L/S bias opposes — soft warning, not hard block
            reasons.append("ls_opposing")

        # Asian range breakout check
        asian_bias = self.get_asian_bias(symbol)
        if asian_bias == proposed_direction:
            confirmations += 1
            reasons.append("asian_breakout_aligned")
        elif asian_bias and asian_bias != proposed_direction:
            conflicts += 1
            reasons.append("asian_breakout_opposes")

        # Hard block: only if OB directly opposing AND no confirmations
        if conflicts >= 2 and confirmations == 0:
            return False, "|".join(reasons)

        return True, "|".join(reasons)

    async def start_polling(self):
        """Background polling for L/S ratio. Start as asyncio task."""
        while True:
            try:
                await self.poll_long_short_ratio()
            except Exception as e:
                log.debug(f"SMC poll loop error: {e}")
            await asyncio.sleep(300)  # 5 min

    async def close(self):
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
