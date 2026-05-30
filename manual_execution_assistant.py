"""
ARUNABHA MANUAL SCALPER v4.0
FILE: manual_execution_assistant.py

Transforms a ScalpSignal into a complete manual-trader instruction card.
Determines execution quality tag (A+/A/B/SKIP).
Generates Telegram-ready formatted alert text.
Handles signal expiry/cancellation alerts.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import config
from signal_engine import ScalpSignal, ScalpSignalType, SignalGrade, SignalStatus

log = logging.getLogger("scalper.mexec")


# Execution quality tags
QUALITY_A_PLUS = "A+"
QUALITY_A = "A"
QUALITY_B = "B"
QUALITY_SKIP = "SKIP-LATE"


class ManualExecutionAssistant:
    """
    Converts ScalpSignal → manual trader instruction card.
    Single responsibility: formatting and execution guidance.
    Does NOT generate signals — receives them from SignalEngine.
    """

    # ═══════════════════════════════════════════
    # EXECUTION QUALITY ASSESSMENT
    # ═══════════════════════════════════════════

    def assess_execution_quality(
        self, signal: ScalpSignal, current_price: float
    ) -> str:
        """
        Determine how clean entry is RIGHT NOW.
        Returns quality tag.
        """
        entry_low = signal.entry_low
        entry_high = signal.entry_high

        # Is price still in entry zone?
        in_zone = entry_low <= current_price <= entry_high

        # How much has price moved from ideal entry?
        ideal = signal.entry_ideal
        drift_pct = abs(current_price - ideal) / ideal if ideal > 0 else 0

        # How much time has elapsed since signal?
        elapsed_min = (time.time() - signal.generated_at) / 60

        # Has price already moved toward TP1? (chased too far)
        if signal.direction == "LONG":
            toward_tp = current_price > signal.entry_high * (1 + config.SIGNAL_MAX_CHASE_PCT)
        else:
            toward_tp = current_price < signal.entry_low * (1 - config.SIGNAL_MAX_CHASE_PCT)

        if toward_tp:
            return QUALITY_SKIP   # price already ran — don't chase

        if elapsed_min > config.SIGNAL_TRIGGER_TIMEOUT_MINUTES:
            return QUALITY_SKIP   # too late — trigger window closed

        if in_zone and elapsed_min < 3:
            return QUALITY_A_PLUS  # perfect timing

        if in_zone or drift_pct < 0.002:
            if elapsed_min < 6:
                return QUALITY_A   # still reasonable
            else:
                return QUALITY_B   # doable but not ideal

        if drift_pct < config.SIGNAL_MAX_CHASE_PCT:
            return QUALITY_B      # slightly outside zone but still valid

        return QUALITY_SKIP       # too far from entry

    # ═══════════════════════════════════════════
    # MAIN ALERT FORMATTER
    # ═══════════════════════════════════════════

    def format_signal_alert(
        self,
        signal: ScalpSignal,
        current_price: float,
        quality: str,
        news_guard_summary: str = "CLEAR",
        account_balance: float = 1000.0,
    ) -> str:
        """
        Formats the complete Telegram alert for a new signal.
        Returns HTML-formatted string.
        """
        e = config.EMOJI
        now_utc = datetime.now(timezone.utc)
        ist_hour = (now_utc.hour + 5) % 24
        ist_min = (now_utc.minute + 30) % 60
        ist_str = f"{ist_hour:02d}:{ist_min:02d} IST"

        # Quality emoji
        quality_emoji = {
            QUALITY_A_PLUS: f"{e['QUALITY_A+']} A+ CLEAN",
            QUALITY_A:      f"{e['QUALITY_A']} A TACTICAL",
            QUALITY_B:      f"{e['QUALITY_B']} B AGGRESSIVE",
            QUALITY_SKIP:   f"{e['SKIP_LATE']} SKIP-LATE — DO NOT ENTER",
        }.get(quality, "❓ UNKNOWN")

        # Grade header
        grade_emoji = {
            SignalGrade.ELITE: f"{e['ELITE']} ELITE",
            SignalGrade.TIER1: f"{e['TIER1']} TIER 1",
            SignalGrade.TIER2: f"{e['TIER2']} TIER 2",
            SignalGrade.TIER3: f"{e['CHART']} TIER 3",
        }.get(signal.grade, "TIER 2")

        direction_emoji = e["LONG"] if signal.direction == "LONG" else e["SHORT"]
        signal_label = signal.signal_type.value.replace("_", " ")

        # Price formatting (auto-detect decimal places)
        def fp(price: float) -> str:
            if price >= 1000:
                return f"{price:.2f}"
            elif price >= 10:
                return f"{price:.3f}"
            elif price >= 0.10:
                return f"{price:.4f}"
            else:
                return f"{price:.6f}"

        # Risk/position info
        rp = config.get_risk_profile()
        risk_pct_map = {
            SignalGrade.ELITE: rp["max_risk_elite"],
            SignalGrade.TIER1: rp["max_risk_tier1"],
            SignalGrade.TIER2: rp["max_risk_tier2"],
            SignalGrade.TIER3: rp["max_risk_tier3"],
        }
        risk_pct = risk_pct_map.get(signal.grade, rp["max_risk_tier2"])
        risk_usdt = account_balance * risk_pct

        # SL distance for position size
        sl_dist_pct = abs(signal.entry_ideal - signal.sl_price) / signal.entry_ideal
        pos_size_usdt = risk_usdt / max(sl_dist_pct, 0.001)
        pos_size_usdt = min(pos_size_usdt, account_balance * 5)  # max 5x leverage cap

        # Expiry time
        expiry_min = config.SIGNAL_EXPIRY_MINUTES.get(
            signal.signal_type.value,
            config.SIGNAL_DEFAULT_EXPIRY_MINUTES
        )
        trigger_timeout_min = config.SIGNAL_TRIGGER_TIMEOUT_MINUTES

        # Spread check
        max_spread = config.MAX_SPREAD_PCT
        spread_note = f"Spread must be < {max_spread*100:.2f}%"

        # Derivatives context string
        deriv_str = self._format_derivatives_line(signal)

        # News status emoji
        news_emoji = "🟢" if "CLEAR" in news_guard_summary.upper() else "🟡"

        # Session
        session_mult = {
            "ASIAN": f"{e['CLOCK']} Asian session (0.7x size)",
            "LONDON": f"🇬🇧 London session",
            "NY": f"🇺🇸 NY session",
        }.get(signal.session, signal.session)

        # ── BUILD ALERT ──
        lines = []

        # Header
        if quality == QUALITY_SKIP:
            lines.append(f"{'─'*30}")
            lines.append(f"{e['SKIP_LATE']} <b>SKIP — ENTRY WINDOW CLOSED</b>")
            lines.append(f"<b>{signal.symbol}</b> {signal.direction} was valid but price moved too far")
            lines.append(f"{'─'*30}")
            return "\n".join(lines)

        lines.append(f"{'━'*32}")
        lines.append(f"{direction_emoji} <b>{signal.symbol} {signal.direction}</b>  {grade_emoji}")
        lines.append(f"<b>📋 {signal_label}</b>")
        lines.append(f"🏷 Quality: <b>{quality_emoji}</b>")
        lines.append(f"{'─'*32}")

        # WHY THIS PAIR
        lines.append(f"\n{e['ATTENTION']} <b>WHY THIS PAIR</b>")
        lines.append(f"  {signal.why_this_pair}")

        # WHY THIS SETUP
        lines.append(f"\n{e['CHART']} <b>WHY THIS SETUP</b>")
        lines.append(f"  {signal.why_this_setup}")

        # ONE LINE THESIS
        lines.append(f"\n<i>📖 Thesis: {signal.trade_thesis}</i>")

        lines.append(f"\n{'─'*32}")

        # ENTRY ZONE
        lines.append(f"\n🎯 <b>ENTRY ZONE</b>")
        lines.append(f"  Zone:     <code>{fp(signal.entry_low)}</code> – <code>{fp(signal.entry_high)}</code>")
        lines.append(f"  Ideal:    <code>{fp(signal.entry_ideal)}</code>")
        lines.append(f"  Now:      <code>{fp(current_price)}</code>")
        lines.append(f"  Max Chase: <code>{fp(signal.max_chase_price)}</code>  (do NOT enter past this)")

        # SL / TP
        lines.append(f"\n🛑 <b>STOP LOSS</b>:  <code>{fp(signal.sl_price)}</code>")
        lines.append(f"  ({sl_dist_pct*100:.2f}% from entry | ATR-based)")

        lines.append(f"\n✅ <b>TAKE PROFITS</b>")
        lines.append(f"  TP1 (50%): <code>{fp(signal.tp1_price)}</code>  → move SL to breakeven")
        lines.append(f"  TP2 (30%): <code>{fp(signal.tp2_price)}</code>  → trail with EMA9")
        lines.append(f"  TP3 (20%): <code>{fp(signal.tp3_price)}</code>  → runner close")

        lines.append(f"\n{'─'*32}")

        # TIMING
        lines.append(f"\n{e['CLOCK']} <b>TIMING</b>")
        lines.append(f"  Signal at:    {ist_str}")
        lines.append(f"  Cancel if not triggered in: <b>{trigger_timeout_min} min</b>")
        lines.append(f"  Expected hold: <b>{signal.expected_hold_minutes} min</b>")
        lines.append(f"  Signal expires: <b>{expiry_min} min</b>")

        # INVALIDATION
        if signal.thesis_invalidation_price > 0:
            lines.append(f"\n🚫 <b>INVALIDATION</b>")
            lines.append(f"  Thesis broken if price reaches: <code>{fp(signal.thesis_invalidation_price)}</code>")
            lines.append(f"  → Exit immediately regardless of SL")

        lines.append(f"\n{'─'*32}")

        # EXECUTION RULES
        lines.append(f"\n⚙️ <b>EXECUTION RULES</b>")
        lines.append(f"  ❌ Do NOT enter if spread > <b>{max_spread*100:.2f}%</b>")
        lines.append(f"  ❌ Do NOT enter if price > <code>{fp(signal.max_chase_price)}</code>")
        lines.append(f"  ❌ Do NOT enter if > <b>{trigger_timeout_min}min</b> since alert")
        lines.append(f"  ✅ Use limit order, not market (avoid slippage)")

        # POSITION SIZING GUIDANCE
        lines.append(f"\n💰 <b>SIZING GUIDE</b> ({config.RISK_PROFILE})")
        lines.append(f"  Risk: {risk_pct*100:.1f}% = <b>~${risk_usdt:.1f} USDT</b>")
        lines.append(f"  Position size: ~<b>${pos_size_usdt:.0f} notional</b>")
        lines.append(f"  (at {pos_size_usdt/account_balance:.1f}x on ${account_balance:.0f} account)")

        lines.append(f"\n{'─'*32}")

        # SCORES & CONTEXT
        lines.append(f"\n📊 <b>CONTEXT SCORES</b>")
        lines.append(f"  Confluence:  {signal.confluence_score:.0f}/100")
        lines.append(f"  Attention:   {signal.attention_score:.0f}/100  {e['ATTENTION']}")
        lines.append(f"  Derivatives: {signal.derivatives_score:.0f}/100  {e['DERIV']}")
        lines.append(f"  News risk:   {signal.news_risk_score*100:.0f}/100  {news_emoji} {news_guard_summary[:40]}")

        # Derivatives detail
        lines.append(f"\n{e['DERIV']} <b>DERIVATIVES</b>")
        lines.append(f"  {deriv_str}")

        # Session
        lines.append(f"\n{session_mult}")
        if signal.narrative:
            lines.append(f"{e['NARRATIVE']} Narrative: <b>{signal.narrative}</b>")
        if signal.regime:
            lines.append(f"{e['REGIME']} Regime: {signal.regime}")

        lines.append(f"\n{'━'*32}")
        lines.append(f"<code>ID: {signal.signal_id[-16:]}</code>")

        # ── Execution notes (exhaustion/disagreement warnings) ──
        if signal.execution_notes and signal.execution_notes.strip():
            lines.append(f"\n💬 <b>NOTES</b>\n{signal.execution_notes.strip()}")

        return "\n".join(lines)

    # ═══════════════════════════════════════════
    # EXPIRY / CANCELLATION ALERT
    # ═══════════════════════════════════════════

    def format_expiry_alert(self, signal: ScalpSignal, reason: str) -> str:
        """Format a signal cancellation/expiry notification."""
        e = config.EMOJI
        clean_reason = reason.replace("_", " ").title()

        return (
            f"{e['EXPIRY']} <b>SIGNAL EXPIRED</b>\n"
            f"Symbol: <b>{signal.symbol}</b> {signal.direction}\n"
            f"Type: {signal.signal_type.value.replace('_', ' ')}\n"
            f"Reason: {clean_reason}\n"
            f"<i>Do not enter this trade.</i>"
        )

    # ═══════════════════════════════════════════
    # UPDATE ALERT (e.g. post-event reaction mode)
    # ═══════════════════════════════════════════

    def format_update_alert(
        self, signal: ScalpSignal, update_type: str, message: str
    ) -> str:
        """Format an update to an existing signal."""
        e = config.EMOJI
        return (
            f"{e['INFO']} <b>SIGNAL UPDATE — {signal.symbol}</b>\n"
            f"Type: {update_type}\n"
            f"{message}\n"
            f"<code>ID: {signal.signal_id[-16:]}</code>"
        )

    # ═══════════════════════════════════════════
    # UNIVERSE ROTATION ALERT
    # ═══════════════════════════════════════════

    def format_universe_update(
        self, new_pairs: list, removed_pairs: list, top_pairs: list
    ) -> str:
        """
        Periodic alert showing current watchlist rotation.
        Sent every UNIVERSE_REFRESH_MINUTES.
        """
        e = config.EMOJI
        now_ist = datetime.now(timezone.utc)

        lines = [
            f"{e['ATTENTION']} <b>WATCHLIST UPDATE</b>",
            f"🕐 {now_ist.strftime('%H:%M')} UTC",
            "",
        ]

        if top_pairs:
            lines.append(f"<b>👀 Scanning ({len(top_pairs)}):</b>")
            for i, sym in enumerate(top_pairs[:8], 1):
                lines.append(f"  {i}. {sym}")

        if new_pairs:
            lines.append(f"\n{e['HYPE']} <b>New additions:</b> {', '.join(new_pairs[:5])}")

        if removed_pairs:
            lines.append(f"\n⬇️ <b>Rotated out:</b> {', '.join(removed_pairs[:5])}")

        return "\n".join(lines)

    # ═══════════════════════════════════════════
    # HELPERS
    # ═══════════════════════════════════════════

    def _format_derivatives_line(self, signal: ScalpSignal) -> str:
        """One-line derivatives summary for the alert."""
        parts = []

        if signal.funding_rate != 0:
            f_pct = signal.funding_rate * 100
            f_dir = "📈" if f_pct > 0 else "📉"
            parts.append(f"Funding: {f_dir} {f_pct:.4f}%")

        if signal.oi_change_pct != 0:
            oi_dir = "↑" if signal.oi_change_pct > 0 else "↓"
            parts.append(f"OI (4h): {oi_dir} {abs(signal.oi_change_pct)*100:.1f}%")

        if signal.ls_ratio != 0.5:
            long_pct = signal.ls_ratio * 100
            parts.append(f"L/S: {long_pct:.0f}% longs")

        if signal.volume_vs_avg != 1.0:
            parts.append(f"Vol: {signal.volume_vs_avg:.1f}x avg")

        return "  |  ".join(parts) if parts else "Derivatives data not available"

    def format_daily_summary(
        self,
        signals_today: int,
        wins: int,
        losses: int,
        pnl_pct: float,
        top_narratives: list,
    ) -> str:
        """End-of-day performance summary."""
        e = config.EMOJI
        win_rate = wins / max(signals_today, 1) * 100
        pnl_emoji = e["PROFIT"] if pnl_pct >= 0 else e["LOSS"]

        lines = [
            f"{'━'*30}",
            f"📅 <b>DAILY SUMMARY</b>",
            f"{'─'*30}",
            f"Signals today: {signals_today}",
            f"✅ Wins: {wins}  ❌ Losses: {losses}",
            f"Win rate: {win_rate:.0f}%",
            f"{pnl_emoji} PnL: {pnl_pct:+.2f}%",
        ]

        if top_narratives:
            lines.append(f"\n{e['NARRATIVE']} Active narratives: {', '.join(top_narratives[:3])}")

        lines.append(f"{'━'*30}")
        return "\n".join(lines)
