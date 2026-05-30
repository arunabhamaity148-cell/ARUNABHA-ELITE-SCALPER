"""
ARUNABHA MANUAL SCALPER v4.0
FILE: signal_engine.py  (MAJOR REWRITE from v3)

LAYER B — Execution Filter Engine
Runs ONLY on pairs that passed Layer A (attention threshold).
7 attention-aware scalp signal types.
Fast timeframe framework: 1m/3m trigger + 15m structure + 1h bias.
Expected hold time: 8-45 minutes.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Tuple

import config

log = logging.getLogger("scalper.signal")

# Lazy imports to avoid circular deps — loaded on first use
_exhaustion_filter = None
_disagreement_engine = None


def _get_exhaustion_filter():
    global _exhaustion_filter
    if _exhaustion_filter is None:
        from exhaustion_filter import ExhaustionFilter
        _exhaustion_filter = ExhaustionFilter()
    return _exhaustion_filter


def _get_disagreement_engine():
    global _disagreement_engine
    if _disagreement_engine is None:
        from exhaustion_filter import DisagreementEngine
        _disagreement_engine = DisagreementEngine()
    return _disagreement_engine


# ═══════════════════════════════════════════════
# SIGNAL TYPES
# ═══════════════════════════════════════════════

class ScalpSignalType(Enum):
    HYPE_CONTINUATION_SCALP = "HYPE_CONTINUATION_SCALP"
    LIQUIDITY_SWEEP_REVERSAL = "LIQUIDITY_SWEEP_REVERSAL"
    OI_EXPANSION_BREAKOUT = "OI_EXPANSION_BREAKOUT"
    NARRATIVE_MOMENTUM_PULLBACK = "NARRATIVE_MOMENTUM_PULLBACK"
    SMC_IMBALANCE_RECLAIM = "SMC_IMBALANCE_RECLAIM"
    FUNDING_TRAP_FADE = "FUNDING_TRAP_FADE"
    LIQUIDATION_CASCADE_SCALP = "LIQUIDATION_CASCADE_SCALP"


class SignalGrade(Enum):
    ELITE = "ELITE"
    TIER1 = "TIER1"
    TIER2 = "TIER2"
    TIER3 = "TIER3"


class SignalStatus(Enum):
    ACTIVE = "ACTIVE"
    TRIGGERED = "TRIGGERED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"
    HIT_TP1 = "HIT_TP1"
    HIT_TP2 = "HIT_TP2"
    HIT_TP3 = "HIT_TP3"
    HIT_SL = "HIT_SL"


@dataclass
class ScalpSignal:
    # Identity
    symbol: str
    direction: str                    # LONG / SHORT
    signal_type: ScalpSignalType
    grade: SignalGrade
    status: SignalStatus = SignalStatus.ACTIVE

    # Prices
    entry_low: float = 0.0           # entry zone bottom
    entry_high: float = 0.0          # entry zone top
    entry_ideal: float = 0.0         # ideal entry (midpoint or limit)
    sl_price: float = 0.0
    tp1_price: float = 0.0
    tp2_price: float = 0.0
    tp3_price: float = 0.0
    max_chase_price: float = 0.0     # cancel if price past this

    # Risk
    risk_pct: float = 0.0
    rr_ratio: float = 0.0
    atr: float = 0.0

    # Timing
    generated_at: float = field(default_factory=time.time)
    expires_at: float = 0.0
    trigger_timeout_at: float = 0.0  # cancel if not triggered by this time
    expected_hold_minutes: int = 20

    # Scoring
    confluence_score: float = 0.0    # 0-100
    attention_score: float = 0.0     # from Layer A
    derivatives_score: float = 0.0   # from Layer A
    news_risk_score: float = 0.0     # 0=clear, 1=dangerous

    # Context
    narrative: str = ""
    regime: str = ""
    session: str = ""
    oi_change_pct: float = 0.0
    funding_rate: float = 0.0
    ls_ratio: float = 0.5
    volume_vs_avg: float = 1.0
    spread_pct: float = 0.0

    # Invalidation rules
    thesis_invalidation_price: float = 0.0
    volatility_invalidation_atr_mult: float = 3.0

    # Plain-language context
    why_this_pair: str = ""
    why_this_setup: str = ""
    trade_thesis: str = ""           # one-line
    execution_notes: str = ""

    # Internal
    ml_features: List[float] = field(default_factory=list)
    signal_id: str = ""

    def __post_init__(self):
        if not self.signal_id:
            self.signal_id = f"{self.symbol}_{self.signal_type.value}_{int(self.generated_at)}"

    @property
    def is_expired(self) -> bool:
        return time.time() > self.expires_at

    @property
    def trigger_timed_out(self) -> bool:
        return time.time() > self.trigger_timeout_at

    @property
    def entry_zone_str(self) -> str:
        return f"{self.entry_low:.4f} – {self.entry_high:.4f}"


# ═══════════════════════════════════════════════
# SIGNAL ENGINE
# ═══════════════════════════════════════════════

class SignalEngine:
    """
    LAYER B: Execution Filter Engine.
    Only called for pairs that passed Layer A attention threshold.
    Implements 7 scalp signal types.
    """

    def __init__(
        self,
        data_processor,
        orderflow,
        liquidity,
        regime,
        risk_engine,
        ml_engine,
        telegram,
        state,
        btc_dominance,
        correlation_engine,
        session_tracker,
        smc_engine,
        attention_engine=None,
        news_guard=None,
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
        self.correlation = correlation_engine
        self.sessions = session_tracker
        self.smc = smc_engine
        self.attention = attention_engine
        self.news_guard = news_guard
        self._active_signals: Dict[str, ScalpSignal] = {}

    # ═══════════════════════════════════════════
    # MAIN ENTRY POINT
    # ═══════════════════════════════════════════

    async def generate(
        self, symbol: str, regime_snapshot=None
    ) -> Optional[ScalpSignal]:
        """
        Attempt to generate a scalp signal for symbol.
        Returns ScalpSignal or None.
        """
        try:
            # Gate 1: News guard (Layer 4 of attention)
            if self.news_guard:
                guard = self.news_guard.check(symbol)
                if not guard.can_trade:
                    log.debug(f"{symbol}: blocked by news guard — {guard.reason}")
                    return None

            # Gate 2: Existing signal for this symbol
            if symbol in self._active_signals:
                existing = self._active_signals[symbol]
                if not existing.is_expired:
                    return None
                else:
                    del self._active_signals[symbol]

            # Gate 3: Attention score minimum
            attention_snap = None
            if self.attention:
                attention_snap = self.attention.get_attention(symbol)
                if not attention_snap:
                    return None
                if attention_snap.combined_score < config.ATTENTION_MIN_SCORE:
                    log.debug(
                        f"{symbol}: attention score {attention_snap.combined_score:.1f} "
                        f"< {config.ATTENTION_MIN_SCORE} — skip"
                    )
                    return None

            # Gate 4: Regime check
            if regime_snapshot and regime_snapshot.recommendation == "NO_TRADE":
                return None

            # Get candle data for multiple TFs
            candles_trigger = self.dp.get_candles(symbol, config.TRIGGER_TF)
            candles_primary = self.dp.get_candles(symbol, config.PRIMARY_TF)
            candles_bias = self.dp.get_candles(symbol, config.BIAS_TF)

            if not candles_trigger or len(candles_trigger) < 30:
                return None
            if not candles_primary or len(candles_primary) < 50:
                return None

            # Get indicators
            ind_trigger = self.dp.get_indicators(symbol, config.TRIGGER_TF)
            ind_primary = self.dp.get_indicators(symbol, config.PRIMARY_TF)
            ind_bias = self.dp.get_indicators(symbol, config.BIAS_TF) if candles_bias else None

            if not ind_trigger or not ind_primary:
                return None

            # Get supporting data
            smc_snap = self.smc.get_snapshot(symbol) if self.smc else None
            of_snap = self.orderflow.get_snapshot(symbol) if self.orderflow else None
            liq_snap = self.liquidity.get_snapshot(symbol) if self.liquidity else None
            deriv_snap = (
                self.attention.get_derivatives(symbol)
                if self.attention else None
            )

            # ── Try each signal type ──
            signal = await self._try_all_signal_types(
                symbol=symbol,
                ind_trigger=ind_trigger,
                ind_primary=ind_primary,
                ind_bias=ind_bias,
                regime=regime_snapshot,
                smc=smc_snap,
                of_snap=of_snap,
                liq_snap=liq_snap,
                attention=attention_snap,
                deriv=deriv_snap,
            )

            if not signal:
                return None

            # ── Exhaustion Filter (anti-overfitting gate) ──
            # Checks if move is already extended BEFORE we commit to signal
            try:
                exh = _get_exhaustion_filter().check(
                    symbol=symbol,
                    direction=signal.direction,
                    ind_trigger=ind_trigger,
                    ind_primary=ind_primary,
                    ind_bias=ind_bias,
                    atr=signal.atr,
                    signal_type=signal.signal_type.value,
                )
                if exh.should_block:
                    log.debug(f"{symbol}: exhaustion HARD block — {exh.reason}")
                    return None
                if exh.should_reduce_size:
                    # Don't block, but note it — size reduced in manual_execution_assistant
                    signal.execution_notes = f"⚠️ Mild exhaustion: {exh.reason} — reduce size 30%"
                    log.debug(f"{symbol}: exhaustion SOFT — {exh.reason}")
            except Exception as e:
                log.debug(f"Exhaustion filter error {symbol}: {e}")

            # ── Disagreement Engine (data integrity gate) ──
            # Checks if attention + derivatives are telling opposite stories
            try:
                dis = _get_disagreement_engine().check(
                    direction=signal.direction,
                    attention_snap=attention_snap,
                    deriv_snap=deriv_snap,
                    of_snap=of_snap,
                )
                if dis.is_blocking:
                    log.debug(
                        f"{symbol}: disagreement block ({dis.disagreement_type}, "
                        f"severity={dis.severity:.2f}) — {dis.advice}"
                    )
                    return None
                if dis.has_disagreement and dis.severity >= 0.40:
                    # Add context note — don't block
                    existing_note = signal.execution_notes or ""
                    signal.execution_notes = (
                        f"{existing_note}\n⚡ {dis.advice}".strip()
                    )
            except Exception as e:
                log.debug(f"Disagreement engine error {symbol}: {e}")

            # ── ML filter ──
            if self.ml and self.ml.is_ready():
                features = self._build_ml_features(
                    signal, ind_trigger, ind_primary, deriv_snap
                )
                win_prob = self.ml.predict(features)
                if win_prob < config.ML_MIN_WIN_PROBABILITY:
                    log.debug(
                        f"{symbol}: ML filtered out — win_prob={win_prob:.2f}"
                    )
                    return None
                signal.ml_features = features

            # ── News risk ──
            if self.news_guard:
                guard = self.news_guard.check(symbol)
                signal.news_risk_score = 1.0 - guard.size_multiplier

            # ── Set timing ──
            expiry_min = config.SIGNAL_EXPIRY_MINUTES.get(
                signal.signal_type.value,
                config.SIGNAL_DEFAULT_EXPIRY_MINUTES,
            )
            signal.expires_at = time.time() + expiry_min * 60
            signal.trigger_timeout_at = (
                time.time() + config.SIGNAL_TRIGGER_TIMEOUT_MINUTES * 60
            )

            # ── Compute max chase price ──
            if signal.direction == "LONG":
                signal.max_chase_price = signal.entry_high * (
                    1 + config.SIGNAL_MAX_CHASE_PCT
                )
            else:
                signal.max_chase_price = signal.entry_low * (
                    1 - config.SIGNAL_MAX_CHASE_PCT
                )

            # ── Correlation check ──
            if self.correlation:
                corr_block = self.correlation.check_new_signal(
                    symbol, signal.direction
                )
                if corr_block:
                    log.debug(f"{symbol}: correlation blocked — {corr_block}")
                    return None

            # ── Session context ──
            session_name = "NY"
            if self.sessions:
                session_name = self.sessions.current_session()
            signal.session = session_name

            self._active_signals[symbol] = signal
            log.info(
                f"SIGNAL: {symbol} {signal.direction} {signal.signal_type.value} "
                f"[{signal.grade.value}] score={signal.confluence_score:.0f}"
                + (f" NOTE:{signal.execution_notes[:40]}" if signal.execution_notes else "")
            )
            return signal

        except Exception as e:
            log.error(f"Signal generation error {symbol}: {e}", exc_info=True)
            return None

    # ═══════════════════════════════════════════
    # SIGNAL TYPE DISPATCHER
    # ═══════════════════════════════════════════

    async def _try_all_signal_types(
        self,
        symbol, ind_trigger, ind_primary, ind_bias,
        regime, smc, of_snap, liq_snap, attention, deriv,
    ) -> Optional[ScalpSignal]:
        """
        Try signal types in priority order.
        Returns first valid signal, or None.
        """
        price = ind_trigger.close if ind_trigger else 0
        atr = ind_primary.atr if ind_primary else 0

        if not price or not atr:
            return None

        # Priority order based on strength of current setup
        type_order = self._determine_type_priority(attention, deriv, regime)

        for stype in type_order:
            signal = None
            try:
                if stype == ScalpSignalType.FUNDING_TRAP_FADE:
                    signal = self._check_funding_trap_fade(
                        symbol, price, atr, ind_trigger, ind_primary, ind_bias,
                        attention, deriv, smc
                    )
                elif stype == ScalpSignalType.LIQUIDITY_SWEEP_REVERSAL:
                    signal = self._check_liquidity_sweep_reversal(
                        symbol, price, atr, ind_trigger, ind_primary,
                        liq_snap, smc, of_snap, attention
                    )
                elif stype == ScalpSignalType.OI_EXPANSION_BREAKOUT:
                    signal = self._check_oi_expansion_breakout(
                        symbol, price, atr, ind_trigger, ind_primary, ind_bias,
                        deriv, of_snap, attention
                    )
                elif stype == ScalpSignalType.HYPE_CONTINUATION_SCALP:
                    signal = self._check_hype_continuation(
                        symbol, price, atr, ind_trigger, ind_primary, ind_bias,
                        attention, deriv, regime
                    )
                elif stype == ScalpSignalType.NARRATIVE_MOMENTUM_PULLBACK:
                    signal = self._check_narrative_pullback(
                        symbol, price, atr, ind_trigger, ind_primary, ind_bias,
                        attention, deriv
                    )
                elif stype == ScalpSignalType.SMC_IMBALANCE_RECLAIM:
                    signal = self._check_smc_imbalance_reclaim(
                        symbol, price, atr, ind_trigger, ind_primary,
                        smc, of_snap, attention
                    )
                elif stype == ScalpSignalType.LIQUIDATION_CASCADE_SCALP:
                    signal = self._check_liquidation_cascade(
                        symbol, price, atr, ind_trigger, ind_primary,
                        deriv, liq_snap, of_snap, attention
                    )
            except Exception as e:
                log.debug(f"{symbol} {stype.value} check error: {e}")
                continue

            if signal and signal.confluence_score >= config.SCORE_MINIMUM:
                signal = self._grade_signal(signal)
                return signal

        return None

    def _determine_type_priority(self, attention, deriv, regime) -> List[ScalpSignalType]:
        """
        Order signal types by what fits current context best.
        Attention and derivatives context drives priority.
        """
        priority = []

        if not attention and not deriv:
            return list(ScalpSignalType)

        # Funding trap is highest priority when crowded
        if attention and attention.is_funding_trap_setup:
            priority.append(ScalpSignalType.FUNDING_TRAP_FADE)

        # Squeeze setup = liquidation cascade
        if attention and attention.is_squeeze_setup:
            priority.append(ScalpSignalType.LIQUIDATION_CASCADE_SCALP)

        # OI expansion = breakout attempt
        if deriv and deriv.oi_expanding:
            priority.append(ScalpSignalType.OI_EXPANSION_BREAKOUT)

        # Momentum = hype continuation or pullback
        if attention and attention.is_momentum_setup:
            priority.append(ScalpSignalType.HYPE_CONTINUATION_SCALP)
            priority.append(ScalpSignalType.NARRATIVE_MOMENTUM_PULLBACK)

        # Always check sweep reversal and SMC
        priority.extend([
            ScalpSignalType.LIQUIDITY_SWEEP_REVERSAL,
            ScalpSignalType.SMC_IMBALANCE_RECLAIM,
        ])

        # Fill remaining types not yet in list
        for t in ScalpSignalType:
            if t not in priority:
                priority.append(t)

        return priority

    # ═══════════════════════════════════════════
    # SIGNAL TYPE 1: FUNDING TRAP FADE
    # ═══════════════════════════════════════════

    def _check_funding_trap_fade(
        self, symbol, price, atr, ind_t, ind_p, ind_b,
        attention, deriv, smc
    ) -> Optional[ScalpSignal]:
        """
        Setup: Funding rate extreme + price rejecting + OI collapsing.
        Edge: Crowded side is wrong → squeeze the losers.
        Direction: Fade the crowded side.
        """
        if not deriv:
            return None

        funding = deriv.funding_rate
        abs_funding = abs(funding)

        # Need extreme funding
        if abs_funding < config.FUNDING_EXTREME_LONG:
            return None

        # Determine fade direction
        if funding > config.FUNDING_EXTREME_LONG:
            # Too many longs → fade longs → go SHORT
            direction = "SHORT"
            crowded_side = "LONG"
        elif funding < config.FUNDING_EXTREME_SHORT:
            # Too many shorts → fade shorts → go LONG
            direction = "LONG"
            crowded_side = "SHORT"
        else:
            return None

        # Price confirmation: should be rejecting — but use LOOSE thresholds
        # Old: SHORT needs RSI >= 60, LONG needs RSI <= 40 (too tight, kills signals)
        # New: SHORT needs RSI >= 55, LONG needs RSI <= 45 (more realistic)
        rsi = ind_p.rsi if ind_p else 50
        if direction == "SHORT" and rsi < 55:
            return None  # longs crowded but price definitely not stretched up
        if direction == "LONG" and rsi > 45:
            return None  # shorts crowded but price definitely not stretched down

        # OI should be collapsing (longs/shorts getting wiped)
        if not deriv.oi_collapsing and abs_funding < config.FUNDING_USEFUL_FADE_LONG:
            return None

        # Build signal
        score = 45.0  # base for having extreme funding

        # Score contributions
        if abs_funding > config.FUNDING_CROWDED:
            score += 15   # very extreme funding
        if deriv.oi_collapsing:
            score += 15
        if deriv.ls_crowded_long and direction == "SHORT":
            score += 10
        if deriv.ls_crowded_short and direction == "LONG":
            score += 10
        if attention and attention.hype_velocity_score > 60:
            score += 5   # hype adding to the trap

        # Entry zone: current price ± small buffer
        if direction == "SHORT":
            entry_high = price * 1.001
            entry_low = price * 0.999
            sl = price + atr * 1.5
            tp1 = price - atr * 1.5
            tp2 = price - atr * 2.5
            tp3 = price - atr * 3.5
        else:
            entry_low = price * 0.999
            entry_high = price * 1.001
            sl = price - atr * 1.5
            tp1 = price + atr * 1.5
            tp2 = price + atr * 2.5
            tp3 = price + atr * 3.5

        why_pair = f"Funding rate at {funding:.4%} — {crowded_side}s are trapped"
        why_setup = f"Extreme funding + {'OI collapsing' if deriv.oi_collapsing else 'price rejecting'} = trap squeeze"
        thesis = f"Fade crowded {crowded_side}s as funding extreme reverses — scalp {direction} {symbol}"

        sig = ScalpSignal(
            symbol=symbol,
            direction=direction,
            signal_type=ScalpSignalType.FUNDING_TRAP_FADE,
            grade=SignalGrade.TIER1,
            entry_low=entry_low,
            entry_high=entry_high,
            entry_ideal=(entry_low + entry_high) / 2,
            sl_price=sl,
            tp1_price=tp1,
            tp2_price=tp2,
            tp3_price=tp3,
            atr=atr,
            confluence_score=score,
            attention_score=attention.combined_score if attention else 0,
            derivatives_score=attention.derivatives_score if attention else 0,
            oi_change_pct=deriv.oi_change_pct if deriv else 0,
            funding_rate=funding,
            ls_ratio=deriv.ls_ratio if deriv else 0.5,
            expected_hold_minutes=20,
            why_this_pair=why_pair,
            why_this_setup=why_setup,
            trade_thesis=thesis,
        )
        return sig

    # ═══════════════════════════════════════════
    # SIGNAL TYPE 2: LIQUIDITY SWEEP REVERSAL
    # ═══════════════════════════════════════════

    def _check_liquidity_sweep_reversal(
        self, symbol, price, atr, ind_t, ind_p,
        liq_snap, smc, of_snap, attention
    ) -> Optional[ScalpSignal]:
        """
        Setup: Price sweeps a key high/low, wicks strongly, closes back.
        Edge: Stop hunts are predictable. Post-sweep = fast reversal.
        """
        # Need SMC data for sweep detection
        if not smc:
            return None

        sweep_data = smc.get_recent_sweep(symbol) if hasattr(smc, 'get_recent_sweep') else None

        # Fallback: use wick ratio on latest candles
        candles = self.dp.get_candles(symbol, config.TRIGGER_TF) if self.dp else []
        if not candles or len(candles) < 3:
            return None

        last = candles[-1]
        prev = candles[-2]

        candle_range = last.high - last.low
        if candle_range < atr * 0.3:
            return None  # tiny candle, not a sweep

        # Bullish sweep: wicked below prev low but closed above it
        upper_wick = last.high - max(last.open, last.close)
        lower_wick = min(last.open, last.close) - last.low
        body = abs(last.close - last.open)

        bullish_sweep = (
            last.low < prev.low                    # swept below
            and last.close > prev.low               # reclaimed
            and lower_wick > body * 1.5             # big wick = sweep
            and lower_wick / candle_range > config.SWEEP_WICK_RATIO
        )
        bearish_sweep = (
            last.high > prev.high                   # swept above
            and last.close < prev.high              # reclaimed back below
            and upper_wick > body * 1.5             # big wick
            and upper_wick / candle_range > config.SWEEP_WICK_RATIO
        )

        if not bullish_sweep and not bearish_sweep:
            return None

        direction = "LONG" if bullish_sweep else "SHORT"

        # RSI should not be extreme in wrong direction
        rsi = ind_p.rsi if ind_p else 50
        if direction == "LONG" and rsi > 70:
            return None
        if direction == "SHORT" and rsi < 30:
            return None

        score = 50.0  # base for confirmed sweep

        # Volume confirmation
        vol_ratio = ind_t.vol_ratio if ind_t else 1.0
        if vol_ratio >= 1.5:
            score += 15
        elif vol_ratio >= 1.2:
            score += 8

        # Orderflow confirmation
        if of_snap:
            cvd_positive = of_snap.cvd_delta > 0
            if (direction == "LONG" and cvd_positive) or (direction == "SHORT" and not cvd_positive):
                score += 10

        # SMC orderblock nearby
        if smc:
            ob_nearby = getattr(smc, 'has_nearby_ob', lambda s, p, d: False)(symbol, price, direction)
            if ob_nearby:
                score += 10

        # Attention bonus
        if attention and attention.combined_score >= config.ATTENTION_HIGH_SCORE:
            score += 8

        # Entry: just above/below the sweep close
        if direction == "LONG":
            entry_low = last.close
            entry_high = last.close * 1.002
            sl = last.low - atr * 0.3   # just below the sweep wick
            tp1 = last.close + atr * 1.5
            tp2 = last.close + atr * 2.5
            tp3 = last.close + atr * 3.5
            thesis_invalidation = last.low - atr * 0.5
        else:
            entry_high = last.close
            entry_low = last.close * 0.998
            sl = last.high + atr * 0.3
            tp1 = last.close - atr * 1.5
            tp2 = last.close - atr * 2.5
            tp3 = last.close - atr * 3.5
            thesis_invalidation = last.high + atr * 0.5

        why_pair = f"Liquidity sweep detected on {config.TRIGGER_TF} — stop hunt complete"
        why_setup = f"Wick ratio {lower_wick/candle_range if bullish_sweep else upper_wick/candle_range:.0%} with close reclaim — institutional sweep"
        thesis = f"Post-sweep reversal {direction} {symbol} — stops cleared, smart money reversing"

        sig = ScalpSignal(
            symbol=symbol,
            direction=direction,
            signal_type=ScalpSignalType.LIQUIDITY_SWEEP_REVERSAL,
            grade=SignalGrade.TIER1,
            entry_low=entry_low,
            entry_high=entry_high,
            entry_ideal=(entry_low + entry_high) / 2,
            sl_price=sl,
            tp1_price=tp1,
            tp2_price=tp2,
            tp3_price=tp3,
            atr=atr,
            confluence_score=score,
            attention_score=attention.combined_score if attention else 0,
            expected_hold_minutes=18,
            thesis_invalidation_price=thesis_invalidation,
            why_this_pair=why_pair,
            why_this_setup=why_setup,
            trade_thesis=thesis,
        )
        return sig

    # ═══════════════════════════════════════════
    # SIGNAL TYPE 3: OI EXPANSION BREAKOUT
    # ═══════════════════════════════════════════

    def _check_oi_expansion_breakout(
        self, symbol, price, atr, ind_t, ind_p, ind_b,
        deriv, of_snap, attention
    ) -> Optional[ScalpSignal]:
        """
        Setup: OI expanding + price breaking structure + volume.
        Edge: New money entering = directional conviction breakout.
        """
        if not deriv or not deriv.oi_expanding:
            return None

        # Price must be breaking a structure level
        candles = self.dp.get_candles(symbol, config.PRIMARY_TF) if self.dp else []
        if not candles or len(candles) < 20:
            return None

        # Find recent high/low (simple swing detection)
        recent = candles[-20:]
        period_high = max(c.high for c in recent[:-1])
        period_low = min(c.low for c in recent[:-1])
        current_close = candles[-1].close

        breaking_up = current_close > period_high * 0.998   # within 0.2% of breakout
        breaking_down = current_close < period_low * 1.002

        if not breaking_up and not breaking_down:
            return None

        direction = "LONG" if breaking_up else "SHORT"

        # Volume must confirm (not a fake breakout)
        vol_ratio = ind_p.vol_ratio if ind_p else 1.0
        if vol_ratio < 1.3:
            return None  # breakout without volume = likely fake

        score = 45.0

        # OI expansion strength
        if deriv.oi_change_pct >= 0.06:   # 6%+ OI expansion
            score += 20
        elif deriv.oi_change_pct >= 0.03:  # 3%+ OI expansion
            score += 12

        # Volume confirmation
        if vol_ratio >= 2.0:
            score += 15
        elif vol_ratio >= 1.5:
            score += 8

        # Funding aligned (not too extreme — extreme = trap, not breakout)
        funding = deriv.funding_rate
        if direction == "LONG" and 0 < funding < config.FUNDING_EXTREME_LONG:
            score += 8  # moderate positive funding = longs entering
        elif direction == "SHORT" and config.FUNDING_EXTREME_SHORT < funding < 0:
            score += 8

        # Not already crowded
        if deriv.ls_crowded_long and direction == "LONG":
            score -= 10   # already too crowded
        if deriv.ls_crowded_short and direction == "SHORT":
            score -= 10

        # Attention bonus
        if attention and attention.combined_score >= 60:
            score += 8

        # Entry: at or slightly above/below the breakout level
        if direction == "LONG":
            entry_low = period_high * 0.999
            entry_high = period_high * 1.003
            sl = period_high - atr * 1.0   # back inside range = invalid
            tp1 = entry_high + atr * 1.5
            tp2 = entry_high + atr * 2.5
            tp3 = entry_high + atr * 3.5
        else:
            entry_high = period_low * 1.001
            entry_low = period_low * 0.997
            sl = period_low + atr * 1.0
            tp1 = entry_low - atr * 1.5
            tp2 = entry_low - atr * 2.5
            tp3 = entry_low - atr * 3.5

        why_pair = f"OI expanding +{deriv.oi_change_pct:.1%} (4h) — new money entering"
        why_setup = f"Structural breakout + OI expansion + {vol_ratio:.1f}x volume = conviction move"
        thesis = f"OI-backed breakout {direction} {symbol} — not a fakeout, new participants entering"

        sig = ScalpSignal(
            symbol=symbol,
            direction=direction,
            signal_type=ScalpSignalType.OI_EXPANSION_BREAKOUT,
            grade=SignalGrade.TIER1,
            entry_low=entry_low,
            entry_high=entry_high,
            entry_ideal=(entry_low + entry_high) / 2,
            sl_price=sl,
            tp1_price=tp1,
            tp2_price=tp2,
            tp3_price=tp3,
            atr=atr,
            confluence_score=score,
            attention_score=attention.combined_score if attention else 0,
            oi_change_pct=deriv.oi_change_pct,
            funding_rate=funding,
            expected_hold_minutes=30,
            why_this_pair=why_pair,
            why_this_setup=why_setup,
            trade_thesis=thesis,
        )
        return sig

    # ═══════════════════════════════════════════
    # SIGNAL TYPE 4: HYPE CONTINUATION SCALP
    # ═══════════════════════════════════════════

    def _check_hype_continuation(
        self, symbol, price, atr, ind_t, ind_p, ind_b,
        attention, deriv, regime
    ) -> Optional[ScalpSignal]:
        """
        Setup: Trending hype pair pulling back to EMA on trigger TF.
        Edge: Strong narrative momentum — pullback buyers = fast resumption.
        Bias = trend direction from 1h, entry = 3m/5m pullback.
        """
        if not attention:
            return None

        # Need strong attention + active narrative
        if attention.combined_score < 55:
            return None
        if not attention.active_narrative:
            return None

        # Determine bias from 1h
        if not ind_b:
            return None

        price_vs_ema50 = price / ind_b.ema_slow if ind_b.ema_slow else 1.0
        if price_vs_ema50 > 1.005:
            direction = "LONG"   # price above 1h EMA50 = bullish bias
        elif price_vs_ema50 < 0.995:
            direction = "SHORT"
        else:
            return None  # no clear bias

        # Pullback on trigger TF: price near EMA9 or EMA21
        ema9_t = ind_t.ema_fast if ind_t else price
        ema21_t = ind_t.ema_mid if ind_t else price

        at_pullback_zone = (
            abs(price - ema9_t) / price < 0.005
            or abs(price - ema21_t) / price < 0.008
        )
        if not at_pullback_zone:
            return None

        # RSI not extreme (pullback should be moderate — loose thresholds)
        # Old: LONG needs RSI 40-65, SHORT needs RSI 35-60 (too narrow)
        # New: LONG needs RSI 38-68, SHORT needs RSI 32-62
        rsi = ind_p.rsi if ind_p else 50
        if direction == "LONG" and (rsi < 38 or rsi > 68):
            return None
        if direction == "SHORT" and (rsi > 62 or rsi < 32):
            return None

        score = 40.0  # base

        # Attention quality
        score += min(attention.combined_score * 0.25, 20)

        # Narrative strength
        narrative_bonus = {
            "AI": 10, "MEME": 8, "GAMING": 6, "LAUNCHPAD": 8,
            "LAYER2": 5, "LAYER1": 5,
        }
        score += narrative_bonus.get(attention.active_narrative, 3)

        # Volume on pullback should be lower (healthy)
        vol_ratio = ind_t.vol_ratio if ind_t else 1.0
        if vol_ratio < 0.80:
            score += 8   # low volume pullback = clean
        elif vol_ratio > 1.5:
            score -= 5   # high volume pullback = might be reversal

        # Derivatives support
        if deriv and deriv.oi_expanding:
            score += 8

        if direction == "LONG":
            entry_low = ema9_t * 0.999
            entry_high = ema9_t * 1.003
            sl = ema21_t - atr * 0.5
            tp1 = price + atr * 1.5
            tp2 = price + atr * 2.5
            tp3 = price + atr * 3.5
        else:
            entry_high = ema9_t * 1.001
            entry_low = ema9_t * 0.997
            sl = ema21_t + atr * 0.5
            tp1 = price - atr * 1.5
            tp2 = price - atr * 2.5
            tp3 = price - atr * 3.5

        why_pair = f"{attention.active_narrative} narrative active — attention score {attention.combined_score:.0f}"
        why_setup = f"Healthy pullback to EMA9 on {config.TRIGGER_TF} while 1h trend intact"
        thesis = f"Hype continuation {direction} {symbol} — {attention.active_narrative} narrative driving"

        sig = ScalpSignal(
            symbol=symbol,
            direction=direction,
            signal_type=ScalpSignalType.HYPE_CONTINUATION_SCALP,
            grade=SignalGrade.TIER2,
            entry_low=entry_low,
            entry_high=entry_high,
            entry_ideal=(entry_low + entry_high) / 2,
            sl_price=sl,
            tp1_price=tp1,
            tp2_price=tp2,
            tp3_price=tp3,
            atr=atr,
            confluence_score=score,
            attention_score=attention.combined_score,
            narrative=attention.active_narrative,
            expected_hold_minutes=25,
            why_this_pair=why_pair,
            why_this_setup=why_setup,
            trade_thesis=thesis,
        )
        return sig

    # ═══════════════════════════════════════════
    # SIGNAL TYPE 5: NARRATIVE MOMENTUM PULLBACK
    # ═══════════════════════════════════════════

    def _check_narrative_pullback(
        self, symbol, price, atr, ind_t, ind_p, ind_b,
        attention, deriv
    ) -> Optional[ScalpSignal]:
        """
        Similar to hype continuation but triggered on primary TF (15m).
        For slightly slower-moving narrative moves.
        Uses 15m EMA stack as context, entry on dip to VWAP.
        """
        if not attention or attention.combined_score < 45:
            return None

        if not ind_p or not ind_b:
            return None

        # 15m EMA stack
        ema_stack_bull = (
            ind_p.ema_fast > ind_p.ema_mid > ind_p.ema_slow
            and price > ind_p.ema_fast
        )
        ema_stack_bear = (
            ind_p.ema_fast < ind_p.ema_mid < ind_p.ema_slow
            and price < ind_p.ema_fast
        )

        if not ema_stack_bull and not ema_stack_bear:
            return None

        direction = "LONG" if ema_stack_bull else "SHORT"

        # VWAP as pullback magnet
        vwap = ind_p.vwap if hasattr(ind_p, 'vwap') else ind_p.ema_mid
        near_vwap = abs(price - vwap) / price < 0.006

        if not near_vwap:
            return None

        # MACD: must not be diverging against direction
        macd_ok = True
        if ind_p.macd_hist:
            if direction == "LONG" and ind_p.macd_hist < -0.001 * price:
                macd_ok = False
            if direction == "SHORT" and ind_p.macd_hist > 0.001 * price:
                macd_ok = False
        if not macd_ok:
            return None

        score = 38.0

        # Narrative quality
        score += min(attention.narrative_score * 0.20, 15)

        # Trend strength from 1h
        if ind_b.adx and ind_b.adx >= config.ADX_TREND:
            score += 12

        # Derivatives light confirmation
        if deriv and not deriv.ls_crowded_long and direction == "LONG":
            score += 6
        if deriv and not deriv.ls_crowded_short and direction == "SHORT":
            score += 6

        if direction == "LONG":
            entry_low = vwap * 0.999
            entry_high = vwap * 1.003
            sl = vwap - atr * 1.2
            tp1 = price + atr * 1.5
            tp2 = price + atr * 2.5
            tp3 = price + atr * 3.5
        else:
            entry_high = vwap * 1.001
            entry_low = vwap * 0.997
            sl = vwap + atr * 1.2
            tp1 = price - atr * 1.5
            tp2 = price - atr * 2.5
            tp3 = price - atr * 3.5

        why_pair = f"{attention.active_narrative or 'active'} pair pulling back to VWAP"
        why_setup = f"15m EMA stack intact + VWAP pullback + {'MACD positive' if direction == 'LONG' else 'MACD negative'}"
        thesis = f"Narrative pullback {direction} {symbol} — VWAP bounce with trend intact"

        sig = ScalpSignal(
            symbol=symbol,
            direction=direction,
            signal_type=ScalpSignalType.NARRATIVE_MOMENTUM_PULLBACK,
            grade=SignalGrade.TIER2,
            entry_low=entry_low,
            entry_high=entry_high,
            entry_ideal=vwap,
            sl_price=sl,
            tp1_price=tp1,
            tp2_price=tp2,
            tp3_price=tp3,
            atr=atr,
            confluence_score=score,
            attention_score=attention.combined_score,
            narrative=attention.active_narrative or "",
            expected_hold_minutes=20,
            why_this_pair=why_pair,
            why_this_setup=why_setup,
            trade_thesis=thesis,
        )
        return sig

    # ═══════════════════════════════════════════
    # SIGNAL TYPE 6: SMC IMBALANCE RECLAIM
    # ═══════════════════════════════════════════

    def _check_smc_imbalance_reclaim(
        self, symbol, price, atr, ind_t, ind_p,
        smc, of_snap, attention
    ) -> Optional[ScalpSignal]:
        """
        Setup: Price returns to unfilled FVG or orderblock.
        Edge: Institutional zones where original orders still sit.
        """
        if not smc:
            return None

        # Get nearest FVG
        fvg = smc.get_nearest_fvg(symbol, price) if hasattr(smc, 'get_nearest_fvg') else None
        ob = smc.get_nearest_orderblock(symbol, price) if hasattr(smc, 'get_nearest_orderblock') else None

        zone = fvg or ob
        if not zone:
            return None

        # Determine direction
        zone_mid = (zone.high + zone.low) / 2 if hasattr(zone, 'high') else price
        zone_type = getattr(zone, 'zone_type', 'BULLISH')
        direction = "LONG" if "BULL" in str(zone_type).upper() else "SHORT"

        # Price must be AT the zone (within 0.5 ATR)
        if abs(price - zone_mid) > atr * 0.8:
            return None

        # RSI confirmation
        rsi = ind_p.rsi if ind_p else 50
        if direction == "LONG" and rsi > 60:
            return None  # too hot to buy at zone
        if direction == "SHORT" and rsi < 40:
            return None

        score = 50.0  # FVG/OB touch is strong base

        # Is it an orderblock (stronger than FVG)
        if ob and not fvg:
            score += 10

        # Zone freshness (untouched = stronger)
        zone_touches = getattr(zone, 'touch_count', 0)
        if zone_touches == 0:
            score += 12
        elif zone_touches == 1:
            score += 6

        # CVD confirmation
        if of_snap:
            if direction == "LONG" and of_snap.cvd_delta > 0:
                score += 8
            elif direction == "SHORT" and of_snap.cvd_delta < 0:
                score += 8

        # Attention bonus
        if attention and attention.combined_score >= 50:
            score += 6

        zone_low = getattr(zone, 'low', price - atr)
        zone_high = getattr(zone, 'high', price + atr)

        if direction == "LONG":
            entry_low = zone_low
            entry_high = zone_high
            sl = zone_low - atr * 0.5
            tp1 = price + atr * 1.5
            tp2 = price + atr * 2.5
            tp3 = price + atr * 3.5
        else:
            entry_low = zone_low
            entry_high = zone_high
            sl = zone_high + atr * 0.5
            tp1 = price - atr * 1.5
            tp2 = price - atr * 2.5
            tp3 = price - atr * 3.5

        zone_label = "FVG" if fvg else "Orderblock"
        why_pair = f"Returned to {zone_label} zone — institutional demand/supply level"
        why_setup = f"First touch of {zone_label} with {'CVD positive' if direction == 'LONG' else 'CVD negative'} confirmation"
        thesis = f"SMC {zone_label} reclaim {direction} {symbol} — untouched zone with orderflow confirmation"

        sig = ScalpSignal(
            symbol=symbol,
            direction=direction,
            signal_type=ScalpSignalType.SMC_IMBALANCE_RECLAIM,
            grade=SignalGrade.TIER1,
            entry_low=entry_low,
            entry_high=entry_high,
            entry_ideal=zone_mid,
            sl_price=sl,
            tp1_price=tp1,
            tp2_price=tp2,
            tp3_price=tp3,
            atr=atr,
            confluence_score=score,
            attention_score=attention.combined_score if attention else 0,
            expected_hold_minutes=25,
            why_this_pair=why_pair,
            why_this_setup=why_setup,
            trade_thesis=thesis,
        )
        return sig

    # ═══════════════════════════════════════════
    # SIGNAL TYPE 7: LIQUIDATION CASCADE SCALP
    # ═══════════════════════════════════════════

    def _check_liquidation_cascade(
        self, symbol, price, atr, ind_t, ind_p,
        deriv, liq_snap, of_snap, attention
    ) -> Optional[ScalpSignal]:
        """
        Setup: Liquidation cluster detected ahead + price approaching.
        Edge: Liquidations = forced selling/buying = predictable momentum.
        Direction: Follow the cascade direction.
        """
        if not deriv:
            return None

        # Need liquidation imbalance on one side
        if deriv.liq_imbalance_pct < config.LIQ_IMBALANCE_THRESHOLD:
            if deriv.liq_imbalance_pct > (1 - config.LIQ_IMBALANCE_THRESHOLD):
                pass  # short-side heavy
            else:
                return None

        # Crowded side exists
        if not deriv.ls_crowded_long and not deriv.ls_crowded_short:
            return None

        # Price momentum must be moving toward liquidation cluster
        if deriv.ls_crowded_long:
            # Longs crowded → price dropping toward their stops = cascade DOWN
            if ind_p and ind_p.close > ind_p.ema_mid:
                return None  # price still above EMA, cascade not started
            direction = "SHORT"
        else:
            # Shorts crowded → price rising toward their stops = squeeze UP
            if ind_p and ind_p.close < ind_p.ema_mid:
                return None
            direction = "LONG"

        score = 45.0

        # Imbalance strength
        imbalance = max(deriv.liq_imbalance_pct, 1 - deriv.liq_imbalance_pct)
        if imbalance >= 0.75:
            score += 20
        elif imbalance >= 0.65:
            score += 12

        # OI still high (liq hasn't happened yet = more fuel)
        if deriv.oi_now > 0 and not deriv.oi_collapsing:
            score += 10

        # Velocity: price should be moving with momentum
        change_1h = getattr(attention, 'price_change_1h', 0) if attention else 0
        if direction == "SHORT" and change_1h < -1.0:
            score += 8
        elif direction == "LONG" and change_1h > 1.0:
            score += 8

        # Attention adds confirmation
        if attention and attention.combined_score >= 55:
            score += 8

        if direction == "SHORT":
            entry_low = price * 0.999
            entry_high = price * 1.001
            sl = price + atr * 1.2
            tp1 = price - atr * 1.5
            tp2 = price - atr * 2.0
            tp3 = price - atr * 2.8
        else:
            entry_low = price * 0.999
            entry_high = price * 1.001
            sl = price - atr * 1.2
            tp1 = price + atr * 1.5
            tp2 = price + atr * 2.0
            tp3 = price + atr * 2.8

        side = "long" if direction == "SHORT" else "short"
        why_pair = f"Liquidation imbalance: {imbalance:.0%} of liq on {side} side"
        why_setup = f"Crowded {side}s approaching forced close zone — cascade momentum"
        thesis = f"Liquidation cascade {direction} {symbol} — follow forced closes as stops hit"

        sig = ScalpSignal(
            symbol=symbol,
            direction=direction,
            signal_type=ScalpSignalType.LIQUIDATION_CASCADE_SCALP,
            grade=SignalGrade.TIER2,
            entry_low=entry_low,
            entry_high=entry_high,
            entry_ideal=(entry_low + entry_high) / 2,
            sl_price=sl,
            tp1_price=tp1,
            tp2_price=tp2,
            tp3_price=tp3,
            atr=atr,
            confluence_score=score,
            attention_score=attention.combined_score if attention else 0,
            derivatives_score=attention.derivatives_score if attention else 0,
            ls_ratio=deriv.ls_ratio,
            expected_hold_minutes=15,  # shortest — cascades are fast
            why_this_pair=why_pair,
            why_this_setup=why_setup,
            trade_thesis=thesis,
        )
        return sig

    # ═══════════════════════════════════════════
    # GRADING + ML FEATURES
    # ═══════════════════════════════════════════

    def _grade_signal(self, signal: ScalpSignal) -> ScalpSignal:
        score = signal.confluence_score
        if score >= config.SCORE_ELITE:
            signal.grade = SignalGrade.ELITE
        elif score >= config.SCORE_TIER1:
            signal.grade = SignalGrade.TIER1
        elif score >= config.SCORE_TIER2:
            signal.grade = SignalGrade.TIER2
        else:
            signal.grade = SignalGrade.TIER3
        return signal

    def _build_ml_features(
        self, signal, ind_t, ind_p, deriv
    ) -> List[float]:
        """Build 20 real features (no zero-padding — fixed from v3)."""
        def safe(val, default=0.0):
            try:
                return float(val) if val is not None else default
            except Exception:
                return default

        return [
            # Price structure (5)
            safe(ind_p.rsi if ind_p else None, 50) / 100,
            safe(ind_p.adx if ind_p else None) / 100,
            safe(ind_p.ema_fast / ind_p.ema_mid if ind_p and ind_p.ema_mid else None, 1.0) - 1,
            safe(ind_p.bb_pct if ind_p else None, 0.5),
            safe(ind_p.vol_ratio if ind_p else None, 1.0) / 3,

            # Trigger TF (4)
            safe(ind_t.rsi if ind_t else None, 50) / 100,
            safe(ind_t.vol_ratio if ind_t else None, 1.0) / 3,
            safe(ind_t.ema_fast / ind_t.ema_mid if ind_t and ind_t.ema_mid else None, 1.0) - 1,
            safe(ind_t.macd_hist if ind_t else None) / (signal.atr + 1e-10),

            # Derivatives (4)
            safe(deriv.funding_rate if deriv else None) * 1000,
            safe(deriv.oi_change_pct if deriv else None),
            safe(deriv.ls_ratio if deriv else None, 0.5),
            safe(deriv.liq_imbalance_pct if deriv else None, 0.5),

            # Signal context (4)
            signal.confluence_score / 100,
            signal.attention_score / 100,
            signal.derivatives_score / 100,
            float(list(ScalpSignalType).index(signal.signal_type)) / 7,

            # Risk context (3)
            signal.news_risk_score,
            float(signal.direction == "LONG"),
            safe(signal.atr / (signal.entry_ideal + 1e-10)) * 100,
        ]

    # ═══════════════════════════════════════════
    # SIGNAL EXPIRY MANAGEMENT
    # ═══════════════════════════════════════════

    async def check_and_expire_signals(self) -> List[Tuple[str, str]]:
        """
        Check all active signals for expiry.
        Returns list of (symbol, reason) for expired signals.
        """
        expired = []
        now = time.time()

        for symbol, signal in list(self._active_signals.items()):
            reason = None

            if signal.is_expired:
                reason = f"time_expired_{config.SIGNAL_EXPIRY_MINUTES.get(signal.signal_type.value, 20)}min"
            elif signal.trigger_timed_out:
                reason = "trigger_timeout_not_hit"
            elif self.news_guard:
                guard = self.news_guard.check(symbol)
                if not guard.can_trade:
                    reason = f"news_invalidated_{guard.reason[:30]}"

            if reason:
                signal.status = SignalStatus.EXPIRED
                expired.append((symbol, reason))
                del self._active_signals[symbol]

        return expired

    def get_active_signals(self) -> Dict[str, ScalpSignal]:
        return dict(self._active_signals)
