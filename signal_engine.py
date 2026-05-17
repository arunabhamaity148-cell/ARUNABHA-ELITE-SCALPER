"""
ARUNABHA ELITE SCALPER v3.0
FILE 8/18: signal_engine.py
THE CORE — Multi-timeframe confluence signal generation
10-step validation pipeline, 6 signal types, anti-chop, fakeout detection
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np

import config
from data_processor import DataProcessor, Indicators
from liquidity_engine import LiquidityEngine, LiquiditySnapshot
from market_regime_engine import MarketRegimeEngine, RegimeResult, RegimeType
from ml_engine import MLEngine
from orderflow_engine import OrderflowEngine, OrderflowSnapshot
from risk_engine import RiskEngine
from state_manager import StateManager
from telegram_bot import TelegramBot

log = logging.getLogger("elite.signal")


# ═══════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════

@dataclass
class SignalResult:
    symbol: str
    direction: str          # "LONG" or "SHORT"
    signal_type: str        # e.g. "TREND_PULLBACK"
    grade: str              # "ELITE" / "TIER1" / "TIER2" / "TIER3"
    score: float            # 0-100
    entry_price: float
    sl_price: float
    tp1_price: float
    tp2_price: float
    tp3_price: float
    risk_pct: float
    risk_usdt: float
    size_usdt: float
    rr_ratio: float
    # Breakdown
    score_breakdown: Dict[str, float] = field(default_factory=dict)
    # Context
    regime: str = ""
    regime_confidence: float = 0.0
    volatility_regime: str = ""
    volume_regime: str = ""
    atr: float = 0.0
    funding_rate: float = 0.0
    # Validity
    generated_at: float = 0.0
    expires_at: float = 0.0
    invalidated: bool = False
    invalidation_reason: str = ""
    # Market breadth & new context fields
    breadth_score: float = 0.5
    extreme_funding_pct: float = 0.0
    btc_dom_trend: str = ""
    session: str = ""
    session_mult: float = 1.0
    funding_trend: str = ""
    btc_correlation: float = 0.0


@dataclass
class BlockResult:
    blocked: bool
    reason: str


# ═══════════════════════════════════════════════
# SIGNAL ENGINE
# ═══════════════════════════════════════════════

class SignalEngine:
    def __init__(
        self,
        data_processor: DataProcessor,
        orderflow: OrderflowEngine,
        liquidity: LiquidityEngine,
        regime: MarketRegimeEngine,
        risk_engine: RiskEngine,
        ml_engine: MLEngine,
        telegram: TelegramBot,
        state: StateManager,
        btc_dominance=None,
        correlation_engine=None,
        session_tracker=None,
    ):
        self.dp = data_processor
        self.orderflow = orderflow
        self.liquidity = liquidity
        self.regime = regime
        self.risk = risk_engine
        self.ml = ml_engine
        self.telegram = telegram
        self.state = state
        self.btc_dom = btc_dominance
        self.corr = correlation_engine
        self.session = session_tracker
        self._last_signal: Dict[str, float] = {}  # symbol → timestamp
        # Trailing stop state: symbol → {sl, tp1_hit, tp2_hit, direction, atr}
        self._trailing_state: Dict[str, dict] = {}

    # ═══════════════════════════════════════════
    # MAIN ENTRY POINT
    # ═══════════════════════════════════════════

    async def generate(self, symbol: str, regime_result: Optional[RegimeResult]) -> Optional[SignalResult]:
        """
        10-step validation pipeline.
        Returns SignalResult or None.
        """
        try:
            # Pre-flight: data readiness
            if not self._has_sufficient_data(symbol):
                return None

            # Get all context
            ind_15m = self.dp.get_indicators(symbol, "15m")
            ind_1h = self.dp.get_indicators(symbol, "1h")
            ind_4h = self.dp.get_indicators(symbol, "4h")
            ind_5m = self.dp.get_indicators(symbol, "5m")
            price = self.dp.get_price(symbol)
            candles_15m = self.dp.get_candles(symbol, "15m", n=50)
            of_snap = self.orderflow.get_snapshot(symbol)
            await self.liquidity.update(symbol)
            liq_snap = self.liquidity.get_snapshot(symbol)
            funding = self.dp.get_funding(symbol)
            ob = self.dp.get_orderbook(symbol)

            if not ind_15m or not ind_1h or price <= 0:
                return None

            # ── STEP 1: Generate raw signal candidates ──
            direction, signal_type = self._detect_raw_signal(
                symbol, ind_15m, ind_1h, ind_4h, ind_5m,
                candles_15m, of_snap, liq_snap, price, funding,
            )
            if not direction:
                return None

            # ── STEP 2: Regime check ──
            block = self._check_regime(symbol, direction, regime_result)
            if block.blocked:
                log.debug(f"[{symbol}] BLOCKED regime: {block.reason}")
                return None

            # ── STEP 3: Anti-chop check ──
            block = self._check_anti_chop(symbol, ind_15m, regime_result)
            if block.blocked:
                log.debug(f"[{symbol}] BLOCKED chop: {block.reason}")
                return None

            # ── STEP 4: Fakeout detection ──
            block = self._check_fakeout(symbol, direction, ind_15m, of_snap, candles_15m, funding)
            if block.blocked:
                log.debug(f"[{symbol}] BLOCKED fakeout: {block.reason}")
                return None

            # ── STEP 5: Liquidity check ──
            entry = self._compute_entry(price, direction, ind_15m, liq_snap)
            block = self._check_liquidity(symbol, entry, direction, liq_snap, ob)
            if block.blocked:
                log.debug(f"[{symbol}] BLOCKED liquidity: {block.reason}")
                return None

            # ── STEP 6: Correlation check ──
            block = await self._check_correlation(symbol, direction)
            if block.blocked:
                log.debug(f"[{symbol}] BLOCKED correlation: {block.reason}")
                return None

            # ── STEP 7: Risk check ──
            block = self.risk.pre_signal_check(symbol, direction)
            if block:
                log.debug(f"[{symbol}] BLOCKED risk: {block}")
                return None

            # ── STEP 8: Confluence scoring ──
            score, breakdown = self._compute_score(
                symbol, direction, signal_type,
                ind_15m, ind_1h, ind_4h, ind_5m,
                of_snap, liq_snap, regime_result,
                funding, ob, price, candles_15m,
            )

            if score < config.SCORE_MINIMUM:
                log.debug(f"[{symbol}] Score too low: {score:.1f}")
                return None

            # ── STEP 9: ML quality check ──
            features = self._build_ml_features(ind_15m, ind_1h, of_snap, funding, score)
            win_prob = self.ml.predict_win_probability(features)
            if win_prob < config.ML_MIN_WIN_PROBABILITY:
                log.debug(f"[{symbol}] ML blocked: win_prob={win_prob:.2f}")
                return None

            # ── STEP 10: Cross-exchange validation ──
            # Price divergence validated at WS layer (websocket_engine.validate_price)
            # No additional check needed here — keep pipeline clean

            # ── GRADE & BUILD SIGNAL ──
            grade = self._grade(score)
            sl, tp1, tp2, tp3 = self._compute_levels(entry, direction, ind_15m, liq_snap, price)
            risk_pct = config.MAX_RISK_TIER[grade]
            size_info = self.risk.compute_size(
                symbol, entry, sl, risk_pct, regime_result
            )

            # ── Collect extra context fields ──
            breadth = regime_result.breadth_score if regime_result else 0.5
            # % of symbols with extreme funding
            all_fundings = [self.dp.get_funding(s).rate for s in config.SYMBOLS]
            extreme_funding_pct = sum(
                1 for r in all_fundings if abs(r) >= config.FUNDING_EXTREME_LONG
            ) / len(all_fundings) if all_fundings else 0.0
            btc_dom_trend = self.btc_dom.get_trend() if self.btc_dom else ""
            session_name = self.session.get_current_session() if self.session else ""
            session_mult = self.session.get_size_multiplier() if self.session else 1.0
            funding_trend = self.corr.get_funding_trend(symbol) if self.corr else "NEUTRAL"
            btc_corr = self.corr.get_correlation_with_btc(symbol) if self.corr else 0.0

            signal = SignalResult(
                symbol=symbol,
                direction=direction,
                signal_type=signal_type,
                grade=grade,
                score=score,
                entry_price=entry,
                sl_price=sl,
                tp1_price=tp1,
                tp2_price=tp2,
                tp3_price=tp3,
                risk_pct=risk_pct,
                risk_usdt=size_info["risk_usdt"],
                size_usdt=size_info["size_usdt"],
                rr_ratio=size_info.get("rr", 0.0),
                score_breakdown=breakdown,
                regime=regime_result.regime.value if regime_result else "UNKNOWN",
                regime_confidence=regime_result.confidence if regime_result else 0.0,
                volatility_regime=regime_result.volatility_regime if regime_result else "",
                volume_regime=regime_result.volume_regime if regime_result else "",
                atr=ind_15m.atr,
                funding_rate=funding.rate,
                generated_at=time.time(),
                expires_at=time.time() + config.SIGNAL_EXPIRY_MINUTES * 60,
                breadth_score=breadth,
                extreme_funding_pct=extreme_funding_pct,
                btc_dom_trend=btc_dom_trend,
                session=session_name,
                session_mult=session_mult,
                funding_trend=funding_trend,
                btc_correlation=btc_corr,
            )

            self._last_signal[symbol] = time.time()
            return signal

        except Exception as e:
            log.error(f"Signal error {symbol}: {e}", exc_info=True)
            return None

    # ═══════════════════════════════════════════
    # STEP 1: RAW SIGNAL DETECTION
    # ═══════════════════════════════════════════

    def _detect_raw_signal(
        self, symbol, ind_15m, ind_1h, ind_4h, ind_5m,
        candles, of_snap, liq_snap, price, funding,
    ) -> Tuple[Optional[str], str]:
        """
        Detect which signal type is active.
        Returns (direction, signal_type) or (None, "")
        """
        # 1. Liquidity sweep (highest priority)
        sweep = self.liquidity.get_recent_sweep(symbol)
        if sweep and sweep.confidence >= 0.65:
            return sweep.signal_direction, "LIQUIDITY_SWEEP"

        if not ind_15m or not ind_1h:
            return None, ""

        # 2. Trend pullback
        direction = self._check_trend_pullback(ind_15m, ind_1h, ind_4h, price, candles)
        if direction:
            return direction, "TREND_PULLBACK"

        # 3. Breakout
        direction = self._check_breakout(ind_15m, of_snap, candles, funding)
        if direction:
            return direction, "BREAKOUT"

        # 4. Mean reversion
        direction = self._check_mean_reversion(ind_15m, of_snap, candles, price)
        if direction:
            return direction, "MEAN_REVERSION"

        # 5. Trend continuation (structure break + retest)
        direction = self._check_trend_continuation(ind_15m, ind_1h, candles, price)
        if direction:
            return direction, "TREND_CONTINUATION"

        # 6. Fade crowd
        direction = self._check_fade_crowd(ind_15m, of_snap, funding, price)
        if direction:
            return direction, "FADE_CROWD"

        return None, ""

    def _check_trend_pullback(self, ind_15m, ind_1h, ind_4h, price, candles) -> Optional[str]:
        """HTF trend + MTF pullback to EMA + bullish structure."""
        if not ind_1h:
            return None

        # HTF: bullish (EMA9 > EMA21 > EMA50 on 1h)
        htf_bull = ind_1h.ema9 > ind_1h.ema21 > ind_1h.ema50
        htf_bear = ind_1h.ema9 < ind_1h.ema21 < ind_1h.ema50

        if htf_bull:
            # MTF: price pulled back to EMA21 or EMA50 (15m)
            near_ema21 = abs(price - ind_15m.ema21) / price < 0.008
            near_ema50 = abs(price - ind_15m.ema50) / price < 0.012
            # LTF: RSI not overbought, MACD hist turning positive
            if (near_ema21 or near_ema50) and ind_15m.rsi < 65 and ind_15m.macd_hist > 0:
                return "LONG"

        elif htf_bear:
            near_ema21 = abs(price - ind_15m.ema21) / price < 0.008
            near_ema50 = abs(price - ind_15m.ema50) / price < 0.012
            if (near_ema21 or near_ema50) and ind_15m.rsi > 35 and ind_15m.macd_hist < 0:
                return "SHORT"

        return None

    def _check_breakout(self, ind_15m, of_snap, candles, funding) -> Optional[str]:
        """HTF consolidation break + volume spike + close beyond."""
        if len(candles) < 20:
            return None

        highs = [c.h for c in candles[-20:]]
        lows = [c.l for c in candles[-20:]]
        recent_high = max(highs[:-2])
        recent_low = min(lows[:-2])
        last_close = candles[-1].c

        vol_confirms = ind_15m.vol_ratio >= 1.3  # elevated volume on break

        if last_close > recent_high and vol_confirms:
            # Check funding not crowded in long direction
            if funding.rate < config.FUNDING_CROWDED:
                return "LONG"

        elif last_close < recent_low and vol_confirms:
            if funding.rate > -config.FUNDING_CROWDED:
                return "SHORT"

        return None

    def _check_mean_reversion(self, ind_15m, of_snap, candles, price) -> Optional[str]:
        """Extreme RSI + structure hold + divergence."""
        if ind_15m.rsi <= 25 and ind_15m.bb_pct_b <= 0.05:
            # Extreme oversold + at lower BB
            if of_snap and of_snap.cvd_divergence:  # price down, CVD turning
                return "LONG"

        if ind_15m.rsi >= 75 and ind_15m.bb_pct_b >= 0.95:
            if of_snap and of_snap.cvd_divergence:
                return "SHORT"

        return None

    def _check_trend_continuation(self, ind_15m, ind_1h, candles, price) -> Optional[str]:
        """Structure break (CHoCH) + retest + LTF entry."""
        if len(candles) < 10:
            return None

        closes = [c.c for c in candles]
        recent_close = closes[-1]
        prev_high = max(closes[-10:-3])
        prev_low = min(closes[-10:-3])

        # CHoCH: broke above resistance, now retesting as support
        if recent_close > prev_high * 1.002:
            if ind_15m.macd_hist > 0 and ind_1h.ema9 > ind_1h.ema21:
                return "LONG"

        # CHoCH: broke below support, retesting as resistance
        if recent_close < prev_low * 0.998:
            if ind_15m.macd_hist < 0 and ind_1h.ema9 < ind_1h.ema21:
                return "SHORT"

        return None

    def _check_fade_crowd(self, ind_15m, of_snap, funding, price) -> Optional[str]:
        """Extreme funding + OI divergence + reversal signal."""
        if abs(funding.rate) < config.FUNDING_EXTREME_LONG:
            return None

        # Extreme long funding = market too long = fade with SHORT
        if funding.rate >= config.FUNDING_CROWDED and ind_15m.rsi >= 65:
            if of_snap and of_snap.cvd_divergence:
                return "SHORT"

        # Extreme short funding = market too short = fade with LONG
        if funding.rate <= -config.FUNDING_CROWDED and ind_15m.rsi <= 35:
            if of_snap and not of_snap.delta_bullish:
                return "LONG"

        return None

    # ═══════════════════════════════════════════
    # STEPS 2-7: VALIDATION FILTERS
    # ═══════════════════════════════════════════

    def _check_regime(self, symbol, direction, regime) -> BlockResult:
        if not regime:
            return BlockResult(False, "")

        if regime.recommendation == "NO_TRADE":
            return BlockResult(True, f"Regime NO_TRADE: {regime.regime.value}")

        # Don't go long in strong downtrend
        if direction == "LONG" and regime.regime == RegimeType.STRONG_TREND_DOWN:
            return BlockResult(True, "LONG blocked in STRONG_TREND_DOWN")

        # Don't go short in strong uptrend
        if direction == "SHORT" and regime.regime == RegimeType.STRONG_TREND_UP:
            return BlockResult(True, "SHORT blocked in STRONG_TREND_UP")

        return BlockResult(False, "")

    def _check_anti_chop(self, symbol, ind, regime) -> BlockResult:
        """All anti-chop filters must pass."""
        if not ind:
            return BlockResult(True, "No indicators")

        # Skip chop filters if strong trend regime
        if regime and regime.regime in (RegimeType.STRONG_TREND_UP, RegimeType.STRONG_TREND_DOWN):
            return BlockResult(False, "")

        # ADX filter
        if ind.adx < config.ADX_CHOP:
            return BlockResult(True, f"ADX too low: {ind.adx:.1f}")

        # EMA distance filter
        ema_dist = abs(ind.ema9 - ind.ema21) / ind.ema21 if ind.ema21 > 0 else 0
        if ema_dist < config.MIN_EMA_DISTANCE_PCT:
            return BlockResult(True, f"EMA gap too small: {ema_dist:.4f}")

        # RSI chop: stuck between 45-55
        if config.RSI_CHOP_LOW <= ind.rsi <= config.RSI_CHOP_HIGH:
            return BlockResult(True, f"RSI in chop zone: {ind.rsi:.1f}")

        # ATR percentile
        if ind.atr_percentile < config.MIN_ATR_PERCENTILE:
            return BlockResult(True, f"ATR too compressed: {ind.atr_percentile:.0f}th pct")

        # BB bandwidth
        if ind.bb_bandwidth_avg > 0 and ind.bb_bandwidth < config.MIN_BB_BANDWIDTH_RATIO * ind.bb_bandwidth_avg:
            return BlockResult(True, "BB squeeze")

        # Volume
        if ind.vol_ratio < config.MIN_VOLUME_RATIO:
            return BlockResult(True, f"Volume dry: {ind.vol_ratio:.2f}x")

        return BlockResult(False, "")

    def _check_fakeout(self, symbol, direction, ind, of_snap, candles, funding) -> BlockResult:
        if not candles or len(candles) < 3:
            return BlockResult(False, "")

        last = candles[-1]
        candle_range = last.h - last.l
        if candle_range <= 0:
            return BlockResult(False, "")

        # Breakout with no volume
        if direction == "LONG" and last.c > last.o:
            if ind.vol_ratio < config.FAKEOUT_VOLUME_THRESHOLD:
                return BlockResult(True, "Breakout with no volume")

        # Close back inside level
        if len(candles) >= 6:
            prev_high = max(c.h for c in candles[-6:-2])
            if direction == "LONG" and last.c < prev_high * 0.998:
                return BlockResult(True, "Close failed above breakout level")

        # Funding crowded in breakout direction
        if direction == "LONG" and funding.rate >= config.FUNDING_CROWDED:
            return BlockResult(True, f"Funding crowded long: {funding.rate:.5f}")
        if direction == "SHORT" and funding.rate <= -config.FUNDING_CROWDED:
            return BlockResult(True, f"Funding crowded short: {funding.rate:.5f}")

        return BlockResult(False, "")

    def _count_level_taps(self, candles: list, level: float, direction: str) -> int:
        """
        3-tap rule: count how many times price tested a level before breaking.
        More taps = stronger breakout (not a fakeout).
        direction LONG = count tests of resistance level
        direction SHORT = count tests of support level
        """
        taps = 0
        tol = config.THREE_TAP_TOLERANCE
        lookback = candles[-config.THREE_TAP_LOOKBACK:] if len(candles) >= config.THREE_TAP_LOOKBACK else candles
        for c in lookback[:-1]:  # exclude last candle (the breakout candle itself)
            if direction == "LONG":
                # High touched the level but closed below (each test)
                if abs(c.h - level) / level <= tol and c.c < level:
                    taps += 1
            else:
                # Low touched the level but closed above
                if abs(c.l - level) / level <= tol and c.c > level:
                    taps += 1
        return taps

    def _is_low_volume_node_breakout(self, symbol: str, price: float) -> bool:
        """
        Check if price is breaking out of a low-volume node in the VP.
        Low volume nodes = price moves fast through them = stronger breakout.
        """
        of_snap = self.orderflow.get_snapshot(symbol)
        if not of_snap or of_snap.poc_price == 0:
            return False
        # If price is between VAL and VAH but NOT near POC = mid-range, not a VP breakout
        if of_snap.value_area_low < price < of_snap.value_area_high:
            # Inside value area — check distance from POC
            poc_dist = abs(price - of_snap.poc_price) / of_snap.poc_price
            return poc_dist > 0.01  # breaking away from POC zone
        # Outside value area = breaking out of single print (low vol node)
        return True

    def _check_liquidity(self, symbol, entry, direction, liq_snap, ob) -> BlockResult:
        # Don't enter into a wall
        if self.liquidity.is_entry_near_wall(symbol, entry, direction):
            return BlockResult(True, "Entry into orderbook wall")

        # Check if there's a nearby liquidity pool (good entry)
        near, zone_type = self.liquidity.is_near_liquidity(symbol, entry, pct_threshold=0.008)
        if not near:
            # Not near any known level — random entry, no edge
            pass  # Allow but lower score

        return BlockResult(False, "")

    async def _check_correlation(self, symbol, direction) -> BlockResult:
        """Don't take correlated positions in same direction."""
        active_signals = self.state.get_active_signals()
        correlated_count = 0
        for sig in active_signals:
            if sig["symbol"] != symbol and sig["direction"] == direction:
                correlated_count += 1
        if correlated_count >= config.MAX_POSITIONS - 1:
            return BlockResult(True, f"Too many correlated {direction} signals")
        return BlockResult(False, "")

    # ═══════════════════════════════════════════
    # STEP 8: CONFLUENCE SCORING
    # ═══════════════════════════════════════════

    def _compute_score(
        self, symbol, direction, signal_type,
        ind_15m, ind_1h, ind_4h, ind_5m,
        of_snap, liq_snap, regime, funding, ob, price, candles,
    ) -> Tuple[float, Dict[str, float]]:
        breakdown = {}

        # ── HTF Trend Alignment (0-20) ──
        trend_score = 0.0
        if ind_1h:
            # EMA200 slope
            if direction == "LONG" and ind_1h.ema200 > 0 and price > ind_1h.ema200:
                trend_score += 5
            elif direction == "SHORT" and price < ind_1h.ema200:
                trend_score += 5
            # Price vs EMA200
            if direction == "LONG" and ind_1h.ema9 > ind_1h.ema21:
                trend_score += 5
            elif direction == "SHORT" and ind_1h.ema9 < ind_1h.ema21:
                trend_score += 5
            # Structure
            if direction == "LONG" and ind_1h.adx > config.ADX_TREND:
                trend_score += 5
            elif direction == "SHORT" and ind_1h.adx > config.ADX_TREND:
                trend_score += 5
            # HTF MACD
            if direction == "LONG" and ind_1h.macd_hist > 0:
                trend_score += 5
            elif direction == "SHORT" and ind_1h.macd_hist < 0:
                trend_score += 5
        breakdown["trend_alignment"] = trend_score

        # ── Momentum (0-15) ──
        mom_score = 0.0
        if direction == "LONG":
            if 40 <= ind_15m.rsi <= 65:
                mom_score += 5
            if ind_15m.macd_hist > 0:
                mom_score += 5
            if ind_15m.stoch_k > ind_15m.stoch_d and ind_15m.stoch_k < 80:
                mom_score += 5
        else:
            if 35 <= ind_15m.rsi <= 60:
                mom_score += 5
            if ind_15m.macd_hist < 0:
                mom_score += 5
            if ind_15m.stoch_k < ind_15m.stoch_d and ind_15m.stoch_k > 20:
                mom_score += 5
        breakdown["momentum"] = mom_score

        # ── Volume (0-15) ──
        vol_score = 0.0
        if ind_15m.vol_ratio >= 1.3:
            vol_score += 5
        elif ind_15m.vol_ratio >= 1.0:
            vol_score += 3
        if ind_15m.vol_ratio > 0.8:
            vol_score += 5  # rising
        if of_snap:
            cvd_aligned = (direction == "LONG" and of_snap.delta_5m > 0) or \
                          (direction == "SHORT" and of_snap.delta_5m < 0)
            if cvd_aligned:
                vol_score += 5
        breakdown["volume"] = vol_score

        # ── Structure Quality (0-15) ──
        struct_score = float(self.liquidity.structure_score(symbol, direction, price))
        breakdown["structure"] = struct_score

        # ── Orderbook (0-10) ──
        ob_score = 0.0
        if ob.imbalance != 0:
            if direction == "LONG" and ob.imbalance > 0.2:
                ob_score += 5
            elif direction == "SHORT" and ob.imbalance < -0.2:
                ob_score += 5
        if direction == "LONG" and ob.walls_bid:
            ob_score += 5
        elif direction == "SHORT" and ob.walls_ask:
            ob_score += 5
        breakdown["orderbook"] = ob_score

        # ── Funding Edge (0-10) ──
        fund_score = 0.0
        if direction == "LONG" and funding.rate <= config.FUNDING_EXTREME_SHORT:
            fund_score += 5  # extreme short funding = good for LONG
        elif direction == "SHORT" and funding.rate >= config.FUNDING_EXTREME_LONG:
            fund_score += 5
        if direction == "LONG" and funding.rate < 0:
            fund_score += 5
        elif direction == "SHORT" and funding.rate > 0:
            fund_score += 5
        breakdown["funding"] = fund_score

        # ── Volatility Fit (0-10) ──
        vf_score = 0.0
        if regime:
            if regime.volatility_regime == "MEDIUM":
                vf_score += 5
            elif regime.volatility_regime == "LOW" and signal_type == "MEAN_REVERSION":
                vf_score += 5
            elif regime.volatility_regime == "HIGH" and signal_type == "TREND_PULLBACK":
                vf_score += 3
        atr_pct = ind_15m.atr / price if price > 0 else 0
        if 0.003 <= atr_pct <= config.MAX_SL_DISTANCE:
            vf_score += 5
        breakdown["volatility_fit"] = vf_score

        # ── BTC Context + Dominance (0-5) ──
        btc_score = 0.0
        btc_bias, btc_breadth = self.regime.get_btc_context()
        if symbol != "BTCUSDT":
            if direction == "LONG" and btc_bias == "BULLISH":
                btc_score += 3
            elif direction == "SHORT" and btc_bias == "BEARISH":
                btc_score += 3
            if btc_breadth >= 0.65 and direction == "LONG":
                btc_score += 2
            elif btc_breadth <= 0.35 and direction == "SHORT":
                btc_score += 2

            # BTC Dominance adjustment (bonus/penalty, not a hard block)
            if self.btc_dom:
                dom_trend = self.btc_dom.get_trend()
                if direction == "LONG" and dom_trend == "FALLING":
                    # Alt season — slightly boost alt longs
                    btc_score = min(btc_score + 2, 5)
                elif direction == "LONG" and dom_trend == "RISING":
                    # BTC dominance rising = alt season ending
                    btc_score = max(btc_score - 2, 0)
        breakdown["btc_context"] = btc_score

        # ── Session Filter bonus (not in 0-100 score, applied to size_adj later) ──
        session_mult = 1.0
        if self.session:
            session_mult = self.session.get_size_multiplier()
        breakdown["_session_mult"] = session_mult  # prefixed _ = meta, not scored

        # ── Correlation penalty ──
        corr_mult = 1.0
        if self.corr:
            corr_mult = self.corr.get_size_multiplier(symbol, direction)
        breakdown["_corr_mult"] = corr_mult

        # ── 3-tap bonus on BREAKOUT signals ──
        if signal_type == "BREAKOUT" and len(candles) >= 6:
            level = max(c.h for c in candles[-6:-2]) if direction == "LONG" else min(c.l for c in candles[-6:-2])
            taps = self._count_level_taps(candles, level, direction)
            if taps >= 3:
                # 3+ taps = stronger breakout = add to structure score
                breakdown["structure"] = min(breakdown.get("structure", 0) + 5, 15)

        # ── Low Volume Node breakout bonus ──
        if signal_type == "BREAKOUT":
            if self._is_low_volume_node_breakout(symbol, price):
                breakdown["volume"] = min(breakdown.get("volume", 0) + config.VP_LOW_VOLUME_BREAKOUT_BONUS, 15)

        # Total (exclude _ meta keys)
        total = sum(v for k, v in breakdown.items() if not k.startswith("_"))
        return round(min(total, 100), 1), breakdown

    # ═══════════════════════════════════════════
    # ENTRY / LEVELS COMPUTATION
    # ═══════════════════════════════════════════

    def _compute_entry(self, price, direction, ind, liq_snap) -> float:
        """Entry price = current price ± small buffer."""
        if direction == "LONG":
            return round(price * 1.0001, 6)
        else:
            return round(price * 0.9999, 6)

    def _compute_levels(self, entry, direction, ind, liq_snap, price) -> Tuple[float, float, float, float]:
        """Compute SL and TP levels."""
        atr = ind.atr if ind.atr > 0 else entry * 0.005
        sl_buffer = atr * config.SL_BUFFER_ATR

        if direction == "LONG":
            sl = entry - atr * 2 - sl_buffer
            sl = max(sl, entry * (1 - config.MAX_SL_DISTANCE))
            risk = entry - sl
        else:
            sl = entry + atr * 2 + sl_buffer
            sl = min(sl, entry * (1 + config.MAX_SL_DISTANCE))
            risk = sl - entry

        if direction == "LONG":
            tp1 = entry + risk * config.TP1_RR
            tp2 = entry + risk * config.TP2_RR
            tp3 = entry + risk * config.TP3_RR
        else:
            tp1 = entry - risk * config.TP1_RR
            tp2 = entry - risk * config.TP2_RR
            tp3 = entry - risk * config.TP3_RR

        return (
            round(sl, 6),
            round(tp1, 6),
            round(tp2, 6),
            round(tp3, 6),
        )

    # ═══════════════════════════════════════════
    # TRAILING STOP MANAGEMENT
    # ═══════════════════════════════════════════

    async def update_trailing_stops(self):
        """
        Called every scan. Updates SL on active signals:
        - After TP1: move SL to breakeven + 0.5 ATR
        - After TP2: trail with EMA(9)
        Sends Telegram update if SL moves.
        """
        active = self.state.get_active_signals()
        for sig_dict in active:
            symbol = sig_dict["symbol"]
            try:
                await self._update_signal_trail(symbol, sig_dict)
            except Exception as e:
                log.debug(f"Trail update error {symbol}: {e}")

    async def _update_signal_trail(self, symbol: str, sig_dict: dict):
        price = self.dp.get_price(symbol)
        if price <= 0:
            return

        direction = sig_dict["direction"]
        entry = sig_dict["entry_price"]
        state_key = symbol

        if state_key not in self._trailing_state:
            # Initialise from active signal record
            ind = self.dp.get_indicators(symbol, "15m")
            atr = ind.atr if ind else entry * 0.005
            self._trailing_state[state_key] = {
                "direction": direction,
                "entry": entry,
                "current_sl": sig_dict.get("sl_price", entry),
                "tp1": sig_dict.get("tp1_price", 0),
                "tp2": sig_dict.get("tp2_price", 0),
                "tp1_hit": False,
                "tp2_hit": False,
                "atr": atr,
            }

        ts = self._trailing_state[state_key]
        ind = self.dp.get_indicators(symbol, "15m")
        atr = ind.atr if ind and ind.atr > 0 else ts["atr"]
        old_sl = ts["current_sl"]
        new_sl = old_sl

        if direction == "LONG":
            # TP1 hit?
            if not ts["tp1_hit"] and ts["tp1"] > 0 and price >= ts["tp1"]:
                ts["tp1_hit"] = True
                # Move SL to breakeven + 0.5 ATR
                new_sl = max(old_sl, entry + atr * config.TRAIL_SL_AFTER_TP1_ATR_MULT)
                log.info(f"[{symbol}] TP1 hit — SL moved to {new_sl:.4f}")

            # TP2 hit?
            if ts["tp1_hit"] and not ts["tp2_hit"] and ts["tp2"] > 0 and price >= ts["tp2"]:
                ts["tp2_hit"] = True
                log.info(f"[{symbol}] TP2 hit — trailing with EMA9")

            # Trail with EMA9 after TP2
            if ts["tp2_hit"] and config.TRAIL_SL_AFTER_TP2_USE_EMA9 and ind:
                ema9_sl = ind.ema9 - atr * 0.3
                new_sl = max(old_sl, ema9_sl)

        else:  # SHORT
            if not ts["tp1_hit"] and ts["tp1"] > 0 and price <= ts["tp1"]:
                ts["tp1_hit"] = True
                new_sl = min(old_sl, entry - atr * config.TRAIL_SL_AFTER_TP1_ATR_MULT)
                log.info(f"[{symbol}] TP1 hit — SL moved to {new_sl:.4f}")

            if ts["tp1_hit"] and not ts["tp2_hit"] and ts["tp2"] > 0 and price <= ts["tp2"]:
                ts["tp2_hit"] = True
                log.info(f"[{symbol}] TP2 hit — trailing with EMA9")

            if ts["tp2_hit"] and config.TRAIL_SL_AFTER_TP2_USE_EMA9 and ind:
                ema9_sl = ind.ema9 + atr * 0.3
                new_sl = min(old_sl, ema9_sl)

        # If SL moved, send update
        sl_moved = abs(new_sl - old_sl) > entry * 0.0001
        if sl_moved:
            ts["current_sl"] = new_sl
            await self._send_trail_update(symbol, direction, new_sl, ts)

    async def _send_trail_update(self, symbol: str, direction: str, new_sl: float, ts: dict):
        stage = "TP2 trail (EMA9)" if ts["tp2_hit"] else "TP1 hit (breakeven)"
        msg = (
            f"🔄 <b>TRAIL UPDATE — {symbol}</b>\n"
            f"Stage: <code>{stage}</code>\n"
            f"Direction: <b>{direction}</b>\n"
            f"New SL: <code>{new_sl:.4f}</code>\n"
            f"Entry: <code>{ts['entry']:.4f}</code>\n"
            f"TP1 hit: {'✅' if ts['tp1_hit'] else '❌'} | "
            f"TP2 hit: {'✅' if ts['tp2_hit'] else '❌'}"
        )
        try:
            await self.telegram.send_alert(msg, priority="normal")
        except Exception as e:
            log.debug(f"Trail alert error: {e}")

    def clear_trailing_state(self, symbol: str):
        """Call when signal closes (SL/TP hit)."""
        self._trailing_state.pop(symbol, None)

    # ═══════════════════════════════════════════
    # ML FEATURES
    # ═══════════════════════════════════════════

    def _build_ml_features(self, ind_15m, ind_1h, of_snap, funding, score) -> list:
        return [
            ind_15m.rsi / 100,
            ind_15m.adx / 100,
            ind_15m.macd_hist,
            ind_15m.vol_ratio,
            ind_15m.bb_pct_b,
            ind_15m.atr_percentile / 100,
            ind_1h.rsi / 100 if ind_1h else 0.5,
            ind_1h.adx / 100 if ind_1h else 0.3,
            of_snap.buy_pressure_pct if of_snap else 0.5,
            of_snap.delta_5m if of_snap else 0.0,
            funding.rate * 1000,
            score / 100,
            float(of_snap.absorption_detected) if of_snap else 0.0,
            float(of_snap.exhaustion_detected) if of_snap else 0.0,
            ind_15m.stoch_k / 100,
            ind_15m.stoch_d / 100,
            ind_15m.ema9 / ind_15m.ema21 - 1 if ind_15m.ema21 > 0 else 0,
            ind_15m.ema21 / ind_15m.ema50 - 1 if ind_15m.ema50 > 0 else 0,
            ind_15m.bb_bandwidth,
            ind_15m.macd / (ind_15m.atr + 1e-9),
            0.0, 0.0, 0.0, 0.0,  # pad to 24 features
        ]

    # ═══════════════════════════════════════════
    # HELPERS
    # ═══════════════════════════════════════════

    def _grade(self, score: float) -> str:
        if score >= config.SCORE_ELITE:
            return "ELITE"
        elif score >= config.SCORE_TIER1:
            return "TIER1"
        elif score >= config.SCORE_TIER2:
            return "TIER2"
        else:
            return "TIER3"

    def _has_sufficient_data(self, symbol: str) -> bool:
        return (
            self.dp.has_data(symbol, "15m", min_candles=100)
            and self.dp.has_data(symbol, "1h", min_candles=50)
            and self.dp.get_price(symbol) > 0
        )
