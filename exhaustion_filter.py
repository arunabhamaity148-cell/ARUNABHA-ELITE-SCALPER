"""
ARUNABHA MANUAL SCALPER v4.0
FILE: exhaustion_filter.py  [NEW]

Micro Trend Exhaustion Filter (#9)
+ Attention/Derivatives Disagreement Engine (#20)
+ Market Energy Index (#17)

PURPOSE — stop 3 specific boring/losing scenarios:
  A) Signal fires on an ALREADY extended move → immediate SL
  B) Attention says HOT but derivatives say DUMP → conflicting signal
  C) Market is dead/sleeping → no edge, skip

Design philosophy:
  → These are VETO filters, not score adders
  → Binary: pass or block
  → Low false-positive rate (loose thresholds by design)
  → Better to miss 1 good trade than take 3 exhausted ones
"""

import logging
import time
from dataclasses import dataclass
from typing import Optional, Tuple

import config

log = logging.getLogger("scalper.exhaustion")


# ═══════════════════════════════════════════════
# RESULT DATACLASSES
# ═══════════════════════════════════════════════

@dataclass
class ExhaustionResult:
    is_exhausted: bool
    reason: str
    severity: str = "NONE"    # NONE / SOFT / HARD
    detail: str = ""

    @property
    def should_block(self) -> bool:
        return self.severity == "HARD"

    @property
    def should_reduce_size(self) -> bool:
        return self.severity == "SOFT"


@dataclass
class DisagreementResult:
    has_disagreement: bool
    disagreement_type: str = ""   # ATTENTION_VS_DERIV / OI_VS_PRICE / FUNDING_VS_FLOW
    severity: float = 0.0         # 0.0 to 1.0
    advice: str = ""

    @property
    def is_blocking(self) -> bool:
        # Only block on strong disagreement
        return self.severity >= 0.70


@dataclass
class MarketEnergyResult:
    energy_score: float           # 0-100
    label: str                    # DEAD / LOW / NORMAL / HIGH / EXTREME
    is_tradeable: bool
    reason: str = ""

    @property
    def size_multiplier(self) -> float:
        if self.energy_score >= 70:
            return 1.0   # HIGH / EXTREME → full size
        elif self.energy_score >= 45:
            return 0.85  # NORMAL → slight reduction
        elif self.energy_score >= 25:
            return 0.60  # LOW → reduced
        else:
            return 0.0   # DEAD → no trade


# ═══════════════════════════════════════════════
# EXHAUSTION FILTER
# ═══════════════════════════════════════════════

class ExhaustionFilter:
    """
    Detects if price has moved too far already before a signal fires.
    Goal: Avoid entering at the end of a move, not the beginning.

    Not a precision system — uses loose thresholds intentionally.
    If uncertain, passes (better to take a slightly extended trade
    than to miss every setup that has moved at all).
    """

    def check(
        self,
        symbol: str,
        direction: str,
        ind_trigger,     # 3m indicators
        ind_primary,     # 15m indicators
        ind_bias,        # 1h indicators
        atr: float,
        signal_type: str,
    ) -> ExhaustionResult:
        """
        Run exhaustion check. Returns ExhaustionResult.
        Call this BEFORE generating entry prices.
        """
        checks = []

        # Check 1: RSI exhaustion on primary TF
        rsi_result = self._check_rsi_exhaustion(
            ind_primary, direction, signal_type
        )
        checks.append(rsi_result)

        # Check 2: Consecutive same-direction candles (momentum fatigue)
        candle_result = self._check_candle_fatigue(
            ind_trigger, direction
        )
        checks.append(candle_result)

        # Check 3: Price vs VWAP stretch (how far from fair value?)
        vwap_result = self._check_vwap_stretch(
            ind_primary, direction, atr
        )
        checks.append(vwap_result)

        # Check 4: ATR expansion (volatility spike = climax move)
        atr_result = self._check_atr_climax(ind_primary, atr)
        checks.append(atr_result)

        # Aggregate: need 2+ HARD checks to block
        # 1 HARD = SOFT (size reduction only)
        hard_count = sum(1 for r in checks if r.severity == "HARD")
        soft_count = sum(1 for r in checks if r.severity == "SOFT")

        hard_reasons = [r.reason for r in checks if r.severity == "HARD"]
        soft_reasons = [r.reason for r in checks if r.severity == "SOFT"]

        if hard_count >= 2:
            return ExhaustionResult(
                is_exhausted=True,
                severity="HARD",
                reason=f"Multiple exhaustion signals: {' + '.join(hard_reasons[:2])}",
                detail=f"Hard:{hard_count} Soft:{soft_count}",
            )
        elif hard_count == 1 or soft_count >= 2:
            return ExhaustionResult(
                is_exhausted=True,
                severity="SOFT",
                reason=f"Mild exhaustion: {(hard_reasons + soft_reasons)[0]}",
                detail=f"Reduce size 30%",
            )
        else:
            return ExhaustionResult(
                is_exhausted=False,
                severity="NONE",
                reason="no exhaustion detected",
            )

    # ── Sub-checks ──────────────────────────────

    def _check_rsi_exhaustion(
        self, ind_p, direction: str, signal_type: str
    ) -> ExhaustionResult:
        """RSI in extreme zone = move likely extended."""
        if not ind_p or not ind_p.rsi:
            return ExhaustionResult(False, "no_rsi_data")

        rsi = ind_p.rsi

        # Exception: for FUNDING_TRAP_FADE, extreme RSI is actually the setup
        # Don't penalize it
        if signal_type in ("FUNDING_TRAP_FADE", "LIQUIDATION_CASCADE_SCALP"):
            return ExhaustionResult(False, "rsi_exempt_for_type")

        # Hard exhaustion: RSI in extreme territory
        if direction == "LONG" and rsi >= 80:
            return ExhaustionResult(
                True, f"RSI_overbought_{rsi:.0f}", severity="HARD"
            )
        if direction == "SHORT" and rsi <= 20:
            return ExhaustionResult(
                True, f"RSI_oversold_{rsi:.0f}", severity="HARD"
            )

        # Soft: getting stretched (not extreme yet)
        if direction == "LONG" and rsi >= 72:
            return ExhaustionResult(
                True, f"RSI_stretched_{rsi:.0f}", severity="SOFT"
            )
        if direction == "SHORT" and rsi <= 28:
            return ExhaustionResult(
                True, f"RSI_stretched_{rsi:.0f}", severity="SOFT"
            )

        return ExhaustionResult(False, f"RSI_ok_{rsi:.0f}")

    def _check_candle_fatigue(
        self, ind_t, direction: str
    ) -> ExhaustionResult:
        """
        4+ consecutive same-direction candles = momentum fatigue.
        Not blocking on its own, but combined with others = exhaustion.
        """
        if not ind_t or not hasattr(ind_t, 'consecutive_same_direction'):
            return ExhaustionResult(False, "no_candle_data")

        count = getattr(ind_t, 'consecutive_same_direction', 0)

        if count >= 6:
            return ExhaustionResult(
                True,
                f"{count}_consecutive_{direction}_candles",
                severity="SOFT",
            )
        return ExhaustionResult(False, f"candle_count_ok_{count}")

    def _check_vwap_stretch(
        self, ind_p, direction: str, atr: float
    ) -> ExhaustionResult:
        """
        Price too far from VWAP = stretched.
        Ideal entry is NEAR vwap, not already extended.
        Exception: breakout signals expect price ABOVE vwap.
        """
        if not ind_p or not hasattr(ind_p, 'vwap') or not ind_p.vwap:
            return ExhaustionResult(False, "no_vwap_data")
        if not ind_p.close or ind_p.close <= 0:
            return ExhaustionResult(False, "no_price_data")

        vwap = ind_p.vwap
        price = ind_p.close
        stretch_pct = (price - vwap) / vwap   # positive = above vwap

        # 2.5+ ATR from VWAP = seriously stretched
        atr_from_vwap = abs(price - vwap) / atr if atr > 0 else 0

        if direction == "LONG" and atr_from_vwap >= 3.0:
            return ExhaustionResult(
                True,
                f"LONG_{atr_from_vwap:.1f}x_ATR_above_VWAP",
                severity="HARD",
            )
        if direction == "SHORT" and atr_from_vwap >= 3.0:
            return ExhaustionResult(
                True,
                f"SHORT_{atr_from_vwap:.1f}x_ATR_below_VWAP",
                severity="HARD",
            )
        if atr_from_vwap >= 2.0:
            return ExhaustionResult(
                True,
                f"vwap_stretch_{atr_from_vwap:.1f}x_ATR",
                severity="SOFT",
            )

        return ExhaustionResult(False, f"vwap_stretch_ok_{atr_from_vwap:.1f}x")

    def _check_atr_climax(
        self, ind_p, current_atr: float
    ) -> ExhaustionResult:
        """
        If current candle range >> ATR = climax / capitulation candle.
        These are often reversal signals, not continuation entries.
        Exception: liquidation cascade specifically needs this.
        """
        if not ind_p or not hasattr(ind_p, 'last_candle_range'):
            return ExhaustionResult(False, "no_range_data")

        last_range = getattr(ind_p, 'last_candle_range', 0)
        if not last_range or current_atr <= 0:
            return ExhaustionResult(False, "no_range_data")

        ratio = last_range / current_atr

        if ratio >= 3.5:
            return ExhaustionResult(
                True,
                f"climax_candle_{ratio:.1f}x_ATR",
                severity="HARD",
            )
        if ratio >= 2.5:
            return ExhaustionResult(
                True,
                f"large_candle_{ratio:.1f}x_ATR",
                severity="SOFT",
            )

        return ExhaustionResult(False, f"candle_size_ok_{ratio:.1f}x")


# ═══════════════════════════════════════════════
# DISAGREEMENT ENGINE
# ═══════════════════════════════════════════════

class DisagreementEngine:
    """
    Detects when attention data and derivatives data tell opposite stories.

    Example contradictions:
    - Attention says LONG (trending up, hype) but OI collapsing (unwinding)
    - High social buzz but funding already extreme in that direction
    - Volume spike but CVD negative (buying looks fake)
    - Price moving up but OI not expanding (no new conviction)

    These are NOT automatic blocks — they add context and reduce size.
    Only very strong disagreement (0.70+) blocks a signal.
    """

    def check(
        self,
        direction: str,
        attention_snap,     # AttentionSnapshot
        deriv_snap,         # DerivativesSnapshot
        of_snap=None,       # OrderflowSnapshot (optional)
    ) -> DisagreementResult:
        """
        Check for meaningful contradictions between data layers.
        Returns DisagreementResult.
        """
        if not attention_snap or not deriv_snap:
            return DisagreementResult(False, "no_data")

        disagreements = []

        # ── Check A: Attention says HOT but OI collapsing ──
        attention_bullish = (
            attention_snap.hype_velocity_score > 55
            and attention_snap.trend_search_score > 40
        )
        oi_collapsing = deriv_snap.oi_collapsing

        if direction == "LONG" and attention_bullish and oi_collapsing:
            disagreements.append(("ATTENTION_VS_OI", 0.55,
                "Hype high but OI unwinding — smart money leaving?"))

        if direction == "SHORT" and attention_bullish and not oi_collapsing:
            disagreements.append(("ATTENTION_VS_OI", 0.40,
                "Social hype increasing while going SHORT — contrarian risk"))

        # ── Check B: Volume spike but wrong CVD direction ──
        if of_snap and attention_snap.volume_vs_avg >= 1.5:
            cvd_agrees = (
                (direction == "LONG" and of_snap.cvd_delta > 0) or
                (direction == "SHORT" and of_snap.cvd_delta < 0)
            )
            if not cvd_agrees:
                # Volume up but buyers/sellers wrong direction
                disagreements.append(("VOLUME_VS_CVD", 0.50,
                    f"Volume {attention_snap.volume_vs_avg:.1f}x but CVD disagrees"))

        # ── Check C: Funding direction vs trade direction ──
        funding = deriv_snap.funding_rate
        if direction == "LONG" and funding > config.FUNDING_CROWDED:
            # Going LONG into extremely crowded longs = dangerous
            # (unless it's specifically a squeeze setup)
            disagreements.append(("DIRECTION_VS_FUNDING", 0.60,
                f"LONG into {funding:.4%} funding — joining crowded longs"))

        if direction == "SHORT" and funding < -config.FUNDING_CROWDED:
            disagreements.append(("DIRECTION_VS_FUNDING", 0.60,
                f"SHORT into {abs(funding):.4%} negative funding — joining crowded shorts"))

        # ── Check D: Narrative momentum but price rejecting ──
        # High attention velocity (fast hype) but price going opposite direction
        if direction == "LONG" and attention_snap.hype_velocity_score > 70:
            # Hype accelerating but OI says selling
            if deriv_snap.ls_crowded_long and oi_collapsing:
                disagreements.append(("HYPE_VS_STRUCTURE", 0.65,
                    "Hype accelerating but longs trapped — fakeout risk"))

        # ── Aggregate ──
        if not disagreements:
            return DisagreementResult(False, "no_disagreement", severity=0.0)

        # Use highest severity disagreement
        worst = max(disagreements, key=lambda x: x[1])
        d_type, severity, advice = worst

        # Average severity if multiple
        if len(disagreements) > 1:
            avg_severity = sum(d[1] for d in disagreements) / len(disagreements)
            severity = min(0.95, (severity + avg_severity) / 2)

        return DisagreementResult(
            has_disagreement=True,
            disagreement_type=d_type,
            severity=severity,
            advice=advice,
        )


# ═══════════════════════════════════════════════
# MARKET ENERGY INDEX
# ═══════════════════════════════════════════════

class MarketEnergyIndex:
    """
    Answers: "Is the market awake right now?"

    Dead market = wide spreads, thin books, no follow-through
    → signals work less → reduce frequency + size

    Uses:
    - Aggregate volume across universe (vs 24h avg)
    - Session timing
    - BTC volatility proxy
    - Number of pairs showing meaningful movement
    """

    def compute(
        self,
        universe_candidates,     # List[PairCandidate]
        session: str,            # ASIAN / LONDON / NY
        btc_change_1h: float,    # BTC 1h % change
    ) -> MarketEnergyResult:
        """
        Compute market energy index (0-100).
        """
        score = 0.0
        reasons = []

        if not universe_candidates:
            return MarketEnergyResult(
                energy_score=30.0,
                label="UNKNOWN",
                is_tradeable=True,
                reason="no_universe_data",
            )

        # ── Factor 1: How many pairs are "moving" (>2% in 24h) ──
        moving_pairs = [
            c for c in universe_candidates
            if abs(c.price_change_24h_pct) >= 2.0
        ]
        moving_ratio = len(moving_pairs) / max(len(universe_candidates), 1)

        if moving_ratio >= 0.70:
            score += 30
            reasons.append(f"{moving_ratio:.0%} pairs moving")
        elif moving_ratio >= 0.40:
            score += 18
        elif moving_ratio >= 0.20:
            score += 8
        else:
            reasons.append("few pairs moving")

        # ── Factor 2: Volume vs average across universe ──
        vol_ratios = [
            c.volume_vs_avg_ratio for c in universe_candidates
            if c.volume_vs_avg_ratio > 0
        ]
        if vol_ratios:
            avg_vol = sum(vol_ratios) / len(vol_ratios)
            if avg_vol >= 1.5:
                score += 25
                reasons.append(f"avg_vol_{avg_vol:.1f}x")
            elif avg_vol >= 1.1:
                score += 15
            elif avg_vol < 0.70:
                score -= 10
                reasons.append("volume_below_average")

        # ── Factor 3: Session energy ──
        session_scores = {
            "NY":     25,   # Most active, best for scalping
            "LONDON": 22,   # Good liquidity
            "ASIAN":  10,   # Lower activity
        }
        score += session_scores.get(session, 15)

        # ── Factor 4: BTC volatility proxy ──
        btc_abs = abs(btc_change_1h)
        if 0.3 <= btc_abs <= 2.5:
            # Moving but not crashing = good energy
            score += 20
            reasons.append(f"BTC_{btc_change_1h:+.1f}%_1h")
        elif btc_abs < 0.1:
            # BTC frozen = market sleeping
            score += 3
            reasons.append("BTC_frozen")
        elif btc_abs > 4.0:
            # BTC crashing/pumping hard = too volatile, risk off
            score += 8
            reasons.append(f"BTC_extreme_{btc_change_1h:+.1f}%")

        score = max(0.0, min(100.0, score))

        # Label
        if score >= 75:
            label = "HIGH"
        elif score >= 50:
            label = "NORMAL"
        elif score >= 28:
            label = "LOW"
        else:
            label = "DEAD"

        is_tradeable = score >= 25   # DEAD = no trades
        reason_str = " | ".join(reasons[:3]) if reasons else "calculated"

        return MarketEnergyResult(
            energy_score=score,
            label=label,
            is_tradeable=is_tradeable,
            reason=reason_str,
        )
