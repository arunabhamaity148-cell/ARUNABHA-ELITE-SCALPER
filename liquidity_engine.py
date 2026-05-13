"""
ARUNABHA ELITE SCALPER v3.0
FILE 6/18: liquidity_engine.py
Swing level detection, liquidity sweep, liquidation estimation,
equal highs/lows, previous period levels, round numbers
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np

import config
from data_processor import Candle, DataProcessor

log = logging.getLogger("elite.liquidity")


# ═══════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════

@dataclass
class SwingLevel:
    price: float
    level_type: str      # "HIGH" or "LOW"
    ts: int              # candle timestamp
    touches: int = 1
    broken: bool = False
    equal_count: int = 0  # number of equal highs/lows at this price


@dataclass
class LiquidityZone:
    price: float
    zone_type: str        # "POOL_HIGH", "POOL_LOW", "PREV_DAY_H", "PREV_DAY_L", "ROUND", etc.
    strength: int = 1     # 1-5, based on touches
    swept: bool = False
    sweep_ts: float = 0.0


@dataclass
class LiquiditySweep:
    symbol: str
    direction: str        # "UP_SWEEP" (swept highs) or "DOWN_SWEEP" (swept lows)
    swept_price: float
    close_price: float
    wick_ratio: float     # wick size / total candle
    volume_spike: bool
    funding_extreme: bool
    oi_dropped: bool
    confidence: float     # 0-1
    ts: float
    signal_direction: str  # "SHORT" for up sweep, "LONG" for down sweep


@dataclass
class LiquidationCluster:
    price: float
    direction: str     # "LONG_LIQ" or "SHORT_LIQ"
    estimated_size_usdt: float
    cascade_risk: float   # 0-1


@dataclass
class LiquiditySnapshot:
    symbol: str
    swing_highs: List[SwingLevel] = field(default_factory=list)
    swing_lows: List[SwingLevel] = field(default_factory=list)
    equal_highs: List[float] = field(default_factory=list)
    equal_lows: List[float] = field(default_factory=list)
    liquidity_zones: List[LiquidityZone] = field(default_factory=list)
    recent_sweeps: List[LiquiditySweep] = field(default_factory=list)
    liquidation_clusters: List[LiquidationCluster] = field(default_factory=list)
    prev_day_high: float = 0.0
    prev_day_low: float = 0.0
    prev_week_high: float = 0.0
    prev_week_low: float = 0.0
    nearest_round_above: float = 0.0
    nearest_round_below: float = 0.0
    updated_at: float = 0.0


# ═══════════════════════════════════════════════
# LIQUIDITY ENGINE
# ═══════════════════════════════════════════════

class LiquidityEngine:
    def __init__(self, data_processor: DataProcessor):
        self.dp = data_processor
        self._snapshots: Dict[str, LiquiditySnapshot] = {
            sym: LiquiditySnapshot(symbol=sym) for sym in config.SYMBOLS
        }
        self._last_update: Dict[str, float] = {sym: 0.0 for sym in config.SYMBOLS}

    async def update(self, symbol: str):
        """Full liquidity analysis for a symbol. Called before signal generation."""
        now = time.time()
        # Cache for 30s
        if now - self._last_update.get(symbol, 0) < 30:
            return

        try:
            snap = LiquiditySnapshot(symbol=symbol, updated_at=now)

            # Use 15m candles as primary, 1h for HTF levels
            candles_15m = self.dp.get_candles(symbol, "15m", n=200)
            candles_1h = self.dp.get_candles(symbol, "1h", n=100)

            if len(candles_15m) < 30:
                return

            # Swing levels
            snap.swing_highs, snap.swing_lows = self._detect_swings(candles_15m)

            # Equal highs/lows (liquidity pools)
            snap.equal_highs = self._find_equal_levels(snap.swing_highs)
            snap.equal_lows = self._find_equal_levels(snap.swing_lows)

            # Liquidity zones (consolidate all levels)
            snap.liquidity_zones = self._build_zones(snap, candles_1h)

            # Previous period levels
            snap.prev_day_high, snap.prev_day_low = self._prev_period_levels(candles_1h, 24)
            snap.prev_week_high, snap.prev_week_low = self._prev_period_levels(candles_1h, 168)

            # Round numbers
            price = self.dp.get_price(symbol)
            if price > 0:
                snap.nearest_round_above, snap.nearest_round_below = self._round_numbers(price)

            # Sweep detection (last 10 candles)
            recent_candles = candles_15m[-10:]
            sweep = self._detect_sweep(symbol, recent_candles, snap)
            if sweep:
                snap.recent_sweeps = [sweep]

            # Liquidation clusters (estimated from OI)
            oi_data = self.dp.get_oi(symbol)
            funding = self.dp.get_funding(symbol)
            if oi_data.open_interest > 0 and price > 0:
                snap.liquidation_clusters = self._estimate_liquidations(
                    price, oi_data.open_interest, funding.rate
                )

            self._snapshots[symbol] = snap
            self._last_update[symbol] = now

        except Exception as e:
            log.debug(f"Liquidity update error {symbol}: {e}")

    # ───────────────────────────────────────────
    # SWING DETECTION
    # ───────────────────────────────────────────

    def _detect_swings(self, candles: List[Candle]) -> Tuple[List[SwingLevel], List[SwingLevel]]:
        """Detect swing highs and lows using fractal method (N-period)."""
        n = config.SWING_LOOKBACK
        highs = [c.h for c in candles]
        lows = [c.l for c in candles]
        timestamps = [c.ts for c in candles]

        swing_highs = []
        swing_lows = []

        for i in range(n, len(candles) - n):
            # Swing high: highest in window
            window_h = highs[i - n: i + n + 1]
            if highs[i] == max(window_h):
                # Count touches (nearby levels within tolerance)
                price = highs[i]
                touches = sum(
                    1 for h in highs
                    if abs(h - price) / price <= config.EQUAL_LEVEL_TOLERANCE * 2
                )
                swing_highs.append(SwingLevel(
                    price=price,
                    level_type="HIGH",
                    ts=timestamps[i],
                    touches=touches,
                ))

            # Swing low: lowest in window
            window_l = lows[i - n: i + n + 1]
            if lows[i] == min(window_l):
                price = lows[i]
                touches = sum(
                    1 for l in lows
                    if abs(l - price) / price <= config.EQUAL_LEVEL_TOLERANCE * 2
                )
                swing_lows.append(SwingLevel(
                    price=price,
                    level_type="LOW",
                    ts=timestamps[i],
                    touches=touches,
                ))

        # Deduplicate nearby levels (within 0.3%)
        swing_highs = self._dedup_levels(swing_highs)
        swing_lows = self._dedup_levels(swing_lows)

        return swing_highs[-10:], swing_lows[-10:]  # Keep last 10

    def _dedup_levels(self, levels: List[SwingLevel]) -> List[SwingLevel]:
        if not levels:
            return []
        result = [levels[0]]
        for lvl in levels[1:]:
            # Check if too close to an existing level
            too_close = any(
                abs(lvl.price - r.price) / r.price <= config.EQUAL_LEVEL_TOLERANCE * 3
                for r in result
            )
            if not too_close:
                result.append(lvl)
        return result

    # ───────────────────────────────────────────
    # EQUAL HIGHS/LOWS (LIQUIDITY POOLS)
    # ───────────────────────────────────────────

    def _find_equal_levels(self, levels: List[SwingLevel]) -> List[float]:
        """Detect equal highs/lows — price levels with multiple touches."""
        pools = []
        for lvl in levels:
            if lvl.touches >= config.STRUCTURE_MIN_TOUCHES:
                pools.append(lvl.price)
        return pools

    # ───────────────────────────────────────────
    # PREVIOUS PERIOD LEVELS
    # ───────────────────────────────────────────

    def _prev_period_levels(self, candles_1h: List[Candle], period_hours: int) -> Tuple[float, float]:
        """Get high/low from N hours ago."""
        if len(candles_1h) <= period_hours:
            return 0.0, 0.0
        period_candles = candles_1h[-period_hours - period_hours:-period_hours]
        if not period_candles:
            return 0.0, 0.0
        return (
            max(c.h for c in period_candles),
            min(c.l for c in period_candles),
        )

    # ───────────────────────────────────────────
    # ROUND NUMBERS
    # ───────────────────────────────════════════

    def _round_numbers(self, price: float) -> Tuple[float, float]:
        """Find nearest psychological round numbers above/below current price."""
        if price <= 0:
            return 0.0, 0.0

        # Determine step based on price magnitude
        if price >= 10000:
            step = 1000.0
        elif price >= 1000:
            step = 100.0
        elif price >= 100:
            step = 10.0
        elif price >= 10:
            step = 1.0
        elif price >= 1:
            step = 0.1
        else:
            step = 0.01

        above = np.ceil(price / step) * step
        below = np.floor(price / step) * step

        if above == price:
            above += step
        if below == price:
            below -= step

        return float(above), float(below)

    # ───────────────────────────────────────────
    # LIQUIDITY ZONES
    # ───────────────────────────────────────────

    def _build_zones(self, snap: LiquiditySnapshot, candles_1h: List[Candle]) -> List[LiquidityZone]:
        zones = []

        for sh in snap.swing_highs:
            zones.append(LiquidityZone(
                price=sh.price,
                zone_type="SWING_HIGH",
                strength=min(5, sh.touches),
            ))

        for sl in snap.swing_lows:
            zones.append(LiquidityZone(
                price=sl.price,
                zone_type="SWING_LOW",
                strength=min(5, sl.touches),
            ))

        for p in snap.equal_highs:
            zones.append(LiquidityZone(price=p, zone_type="POOL_HIGH", strength=3))

        for p in snap.equal_lows:
            zones.append(LiquidityZone(price=p, zone_type="POOL_LOW", strength=3))

        if snap.prev_day_high > 0:
            zones.append(LiquidityZone(price=snap.prev_day_high, zone_type="PREV_DAY_H", strength=4))
        if snap.prev_day_low > 0:
            zones.append(LiquidityZone(price=snap.prev_day_low, zone_type="PREV_DAY_L", strength=4))
        if snap.prev_week_high > 0:
            zones.append(LiquidityZone(price=snap.prev_week_high, zone_type="PREV_WEEK_H", strength=5))
        if snap.prev_week_low > 0:
            zones.append(LiquidityZone(price=snap.prev_week_low, zone_type="PREV_WEEK_L", strength=5))

        if snap.nearest_round_above > 0:
            zones.append(LiquidityZone(price=snap.nearest_round_above, zone_type="ROUND", strength=2))
        if snap.nearest_round_below > 0:
            zones.append(LiquidityZone(price=snap.nearest_round_below, zone_type="ROUND", strength=2))

        return sorted(zones, key=lambda z: z.price)

    # ───────────────────────────────────────────
    # LIQUIDITY SWEEP DETECTION
    # ───────────────────────────────────────────

    def _detect_sweep(
        self, symbol: str, recent_candles: List[Candle], snap: LiquiditySnapshot
    ) -> Optional[LiquiditySweep]:
        """
        Sweep = wick beyond a known level + close back inside.
        Returns sweep signal to fade (opposite direction).
        """
        if len(recent_candles) < 3:
            return None

        last = recent_candles[-1]
        prev = recent_candles[-2]
        candle_range = last.h - last.l
        if candle_range < 1e-10:
            return None

        wick_up = last.h - max(last.o, last.c)
        wick_down = min(last.o, last.c) - last.l
        wick_ratio_up = wick_up / candle_range
        wick_ratio_down = wick_down / candle_range

        # Funding/OI context
        funding = self.dp.get_funding(symbol)
        oi = self.dp.get_oi(symbol)
        funding_extreme_long = funding.rate >= config.FUNDING_EXTREME_LONG
        funding_extreme_short = funding.rate <= config.FUNDING_EXTREME_SHORT

        # Volume spike
        candles_all = self.dp.get_candles(symbol, "15m", n=20)
        avg_vol = np.mean([c.v for c in candles_all[:-1]]) if len(candles_all) > 1 else 1.0
        vol_spike = last.v > avg_vol * 1.5

        # Check sweep of swing highs (up sweep → signal SHORT)
        if wick_ratio_up >= config.SWEEP_WICK_RATIO:
            for zone in snap.liquidity_zones:
                if zone.zone_type in ("SWING_HIGH", "POOL_HIGH", "PREV_DAY_H", "ROUND"):
                    if last.h >= zone.price and last.c < zone.price:
                        # Wick swept the level, close is back below
                        oi_dropped = oi.oi_change_1h < config.OI_DROP_THRESHOLD
                        confidence = self._sweep_confidence(
                            wick_ratio_up, vol_spike, funding_extreme_long, oi_dropped, zone.strength
                        )
                        if confidence >= 0.50:
                            return LiquiditySweep(
                                symbol=symbol,
                                direction="UP_SWEEP",
                                swept_price=zone.price,
                                close_price=last.c,
                                wick_ratio=wick_ratio_up,
                                volume_spike=vol_spike,
                                funding_extreme=funding_extreme_long,
                                oi_dropped=oi_dropped,
                                confidence=confidence,
                                ts=time.time(),
                                signal_direction="SHORT",
                            )

        # Check sweep of swing lows (down sweep → signal LONG)
        if wick_ratio_down >= config.SWEEP_WICK_RATIO:
            for zone in snap.liquidity_zones:
                if zone.zone_type in ("SWING_LOW", "POOL_LOW", "PREV_DAY_L", "ROUND"):
                    if last.l <= zone.price and last.c > zone.price:
                        oi_dropped = oi.oi_change_1h < config.OI_DROP_THRESHOLD
                        confidence = self._sweep_confidence(
                            wick_ratio_down, vol_spike, funding_extreme_short, oi_dropped, zone.strength
                        )
                        if confidence >= 0.50:
                            return LiquiditySweep(
                                symbol=symbol,
                                direction="DOWN_SWEEP",
                                swept_price=zone.price,
                                close_price=last.c,
                                wick_ratio=wick_ratio_down,
                                volume_spike=vol_spike,
                                funding_extreme=funding_extreme_short,
                                oi_dropped=oi_dropped,
                                confidence=confidence,
                                ts=time.time(),
                                signal_direction="LONG",
                            )

        return None

    def _sweep_confidence(
        self,
        wick_ratio: float,
        vol_spike: bool,
        funding_extreme: bool,
        oi_dropped: bool,
        zone_strength: int,
    ) -> float:
        """Score sweep quality 0-1."""
        score = 0.0
        score += min(wick_ratio, 1.0) * 0.30       # wick quality
        score += 0.20 if vol_spike else 0.0         # volume confirmation
        score += 0.20 if funding_extreme else 0.0   # crowded in sweep direction
        score += 0.15 if oi_dropped else 0.0        # stops hit
        score += (zone_strength / 5.0) * 0.15       # level quality
        return round(min(score, 1.0), 3)

    # ───────────────────────────────────────────
    # LIQUIDATION ESTIMATION
    # ───────────────────────────────────────────

    def _estimate_liquidations(
        self, price: float, open_interest: float, funding_rate: float
    ) -> List[LiquidationCluster]:
        """
        Estimate liquidation clusters from OI + assumed leverage.
        This is a heuristic — real liquidation engines require full position data.
        """
        clusters = []
        lev = config.LIQ_LEVERAGE_ASSUMPTION

        # Long liquidation: price drops by 1/leverage
        long_liq_price = price * (1 - 1 / lev * 0.85)  # 85% of margin = margin call
        short_liq_price = price * (1 + 1 / lev * 0.85)

        # Estimate size: if funding is positive (longs dominant), more long OI
        long_bias = 0.6 if funding_rate > 0 else 0.4
        long_oi = open_interest * long_bias * price
        short_oi = open_interest * (1 - long_bias) * price

        cascade_long = min(1.0, long_oi / (open_interest * price + 1) * 2)
        cascade_short = min(1.0, short_oi / (open_interest * price + 1) * 2)

        if long_liq_price > 0:
            clusters.append(LiquidationCluster(
                price=long_liq_price,
                direction="LONG_LIQ",
                estimated_size_usdt=long_oi,
                cascade_risk=cascade_long,
            ))

        if short_liq_price > 0:
            clusters.append(LiquidationCluster(
                price=short_liq_price,
                direction="SHORT_LIQ",
                estimated_size_usdt=short_oi,
                cascade_risk=cascade_short,
            ))

        return clusters

    # ───────────────────────────────────────────
    # GETTERS
    # ───────────────────────────────────────────

    def get_snapshot(self, symbol: str) -> LiquiditySnapshot:
        return self._snapshots.get(symbol, LiquiditySnapshot(symbol=symbol))

    def nearest_zone(self, symbol: str, price: float, zone_types: Optional[List[str]] = None) -> Optional[LiquidityZone]:
        """Find nearest liquidity zone to current price."""
        snap = self._snapshots.get(symbol)
        if not snap or not snap.liquidity_zones:
            return None
        zones = snap.liquidity_zones
        if zone_types:
            zones = [z for z in zones if z.zone_type in zone_types]
        if not zones:
            return None
        return min(zones, key=lambda z: abs(z.price - price))

    def is_near_liquidity(self, symbol: str, price: float, pct_threshold: float = 0.005) -> Tuple[bool, str]:
        """Check if price is near a significant liquidity zone."""
        zone = self.nearest_zone(symbol, price)
        if not zone:
            return False, ""
        distance_pct = abs(zone.price - price) / price
        return distance_pct <= pct_threshold, zone.zone_type

    def get_recent_sweep(self, symbol: str) -> Optional[LiquiditySweep]:
        """Get most recent sweep if within last 5 candles time."""
        snap = self._snapshots.get(symbol)
        if not snap or not snap.recent_sweeps:
            return None
        sweep = snap.recent_sweeps[-1]
        # Only return if fresh (within 15 minutes)
        if time.time() - sweep.ts > 900:
            return None
        return sweep

    def is_entry_near_wall(self, symbol: str, entry_price: float, direction: str) -> bool:
        """Check if entry is walking into an orderbook wall."""
        ob = self.dp.get_orderbook(symbol)
        if direction == "LONG":
            for wall_price in ob.walls_ask:
                if 0 < wall_price - entry_price < entry_price * 0.005:
                    return True
        else:
            for wall_price in ob.walls_bid:
                if 0 < entry_price - wall_price < entry_price * 0.005:
                    return True
        return False

    def structure_score(self, symbol: str, direction: str, entry_price: float) -> int:
        """0-15 points for structure quality around entry price."""
        snap = self._snapshots.get(symbol)
        if not snap:
            return 0

        score = 0
        # Clean S/R level nearby
        zone = self.nearest_zone(symbol, entry_price)
        if zone and abs(zone.price - entry_price) / entry_price <= 0.01:
            score += 5
            # Multiple touches
            if zone.strength >= 3:
                score += 5
        # Recent sweep confirmation
        sweep = self.get_recent_sweep(symbol)
        if sweep and sweep.signal_direction == direction:
            score += 5

        return min(score, 15)