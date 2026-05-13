"""
ARUNABHA ELITE SCALPER v3.0
FILE 9/18: risk_engine.py
THE GUARDIAN — Position sizing, drawdown limits, cooldowns, kill switch
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

import config
from state_manager import StateManager

log = logging.getLogger("elite.risk")


@dataclass
class SizeInfo:
    size_usdt: float
    risk_usdt: float
    risk_pct: float
    leverage: float
    rr: float
    size_reduction_reason: str = ""


class RiskEngine:
    def __init__(self, state: StateManager):
        self.state = state

    # ═══════════════════════════════════════════
    # CAN WE SCAN AT ALL?
    # ═══════════════════════════════════════════

    def can_scan(self, now: datetime) -> bool:
        """Gate: should the main loop even bother scanning?"""

        # Daily loss limit
        if self.state.daily_loss_pct >= config.MAX_DAILY_LOSS:
            log.warning(f"Daily loss limit hit: {self.state.daily_loss_pct:.1%}")
            return False

        # Weekly loss limit
        if self.state.weekly_loss_pct >= config.MAX_WEEKLY_LOSS:
            log.warning(f"Weekly loss limit hit: {self.state.weekly_loss_pct:.1%}")
            return False

        # Emergency drawdown
        if self.state.drawdown_pct >= config.DRAWDOWN_EMERGENCY:
            log.critical(f"EMERGENCY drawdown: {self.state.drawdown_pct:.1%}")
            return False

        # Operator kill switch
        if self.state.kill_switch_active:
            log.warning("Kill switch active — no scanning")
            return False

        # Time restriction
        if now.hour >= config.NO_TRADE_AFTER_UTC:
            log.debug(f"Past trading hours: UTC {now.hour}:00")
            return False

        # Cooldown
        cooldown_remaining = self._cooldown_remaining()
        if cooldown_remaining > 0:
            log.debug(f"Cooldown: {cooldown_remaining:.0f}s remaining")
            return False

        return True

    # ═══════════════════════════════════════════
    # PRE-SIGNAL CHECK (symbol-level)
    # ═══════════════════════════════════════════

    def pre_signal_check(self, symbol: str, direction: str) -> Optional[str]:
        """Returns block reason string, or None if OK."""

        # Max positions
        active = self.state.count_active_positions()
        if active >= config.MAX_POSITIONS:
            return f"Max positions ({config.MAX_POSITIONS}) reached"

        # Already have position in this symbol
        if self.state.has_active_signal(symbol):
            return f"Already have active signal for {symbol}"

        # Monthly loss
        if self.state.monthly_loss_pct >= config.MAX_MONTHLY_LOSS:
            return "Monthly loss limit hit"

        # Account minimum
        if self.state.current_balance_usdt < config.ACCOUNT_BALANCE_USDT * config.MIN_ACCOUNT_PCT:
            return "Account below minimum threshold"

        return None

    # ═══════════════════════════════════════════
    # POSITION SIZING
    # ═══════════════════════════════════════════

    def compute_size(
        self,
        symbol: str,
        entry: float,
        sl: float,
        base_risk_pct: float,
        regime=None,
    ) -> dict:
        """
        Dynamic position sizing with multi-factor reductions.
        Returns dict with size_usdt, risk_usdt, rr, etc.
        """
        balance = self.state.current_balance_usdt
        if balance <= 0 or entry <= 0 or sl <= 0:
            return {"size_usdt": 0, "risk_usdt": 0, "rr": 0}

        sl_distance = abs(entry - sl)
        if sl_distance <= 0:
            return {"size_usdt": 0, "risk_usdt": 0, "rr": 0}

        sl_pct = sl_distance / entry
        if sl_pct > config.MAX_SL_DISTANCE:
            sl_pct = config.MAX_SL_DISTANCE
            # Adjust SL to max allowed
            if entry > sl:  # LONG
                sl = entry * (1 - config.MAX_SL_DISTANCE)
            else:
                sl = entry * (1 + config.MAX_SL_DISTANCE)
            sl_distance = abs(entry - sl)

        # ── Risk percent adjustments ──
        risk_pct = base_risk_pct
        reason_parts = []

        # 1. Consecutive loss reduction
        consec = self.state.consecutive_losses
        if consec >= 4:
            risk_pct *= config.SIZE_REDUCTION_4
            reason_parts.append("4+ losses → 0%")
        elif consec >= 3:
            risk_pct *= config.SIZE_REDUCTION_3
            reason_parts.append("3 losses → 25%")
        elif consec >= 2:
            risk_pct *= config.SIZE_REDUCTION_2
            reason_parts.append("2 losses → 50%")

        # 2. Drawdown reduction
        dd = self.state.drawdown_pct
        if dd >= config.DRAWDOWN_CRITICAL:
            risk_pct *= 0.25
            reason_parts.append("Critical DD → 25%")
        elif dd >= config.DRAWDOWN_SERIOUS:
            risk_pct *= 0.50
            reason_parts.append("Serious DD → 50%")
        elif dd >= config.DRAWDOWN_WARNING:
            risk_pct *= 0.75
            reason_parts.append("Warning DD → 75%")

        # 3. Volatility regime adjustment
        if regime:
            vol_regime = regime.volatility_regime
            if vol_regime == "EXTREME":
                risk_pct *= config.VOL_SIZE_EXTREME
                reason_parts.append("Extreme vol → 25%")
            elif vol_regime == "HIGH":
                risk_pct *= config.VOL_SIZE_HIGH
                reason_parts.append("High vol → 50%")
            elif vol_regime == "MEDIUM":
                risk_pct *= config.VOL_SIZE_MEDIUM
                reason_parts.append("Med vol → 75%")

        # 4. Weekend reduction
        now = datetime.now(timezone.utc)
        if now.weekday() >= 5:  # Saturday/Sunday
            risk_pct *= config.WEEKEND_SIZE_REDUCTION
            reason_parts.append("Weekend → 75%")

        # 5. Regime size adjustment
        if regime and hasattr(regime, "size_adjustment"):
            risk_pct *= regime.size_adjustment

        # Cap at absolute max
        risk_pct = min(risk_pct, config.MAX_RISK_PER_TRADE)
        if risk_pct <= 0:
            return {"size_usdt": 0, "risk_usdt": 0, "rr": 0, "blocked": True, "reason": "Size reduced to 0"}

        # ── Dollar risk ──
        risk_usdt = balance * risk_pct

        # ── Position size ──
        # size_usdt = risk_usdt / sl_pct (capital at risk = size × sl_pct)
        size_usdt = risk_usdt / sl_pct

        # Leverage check
        leverage = size_usdt / balance
        if leverage > config.MAX_LEVERAGE:
            size_usdt = balance * config.MAX_LEVERAGE
            risk_usdt = size_usdt * sl_pct
            reason_parts.append(f"Leverage capped at {config.MAX_LEVERAGE}x")

        # ── RR ratio ──
        tp1_dist = sl_distance * config.TP1_RR
        rr = config.TP1_RR  # base

        return {
            "size_usdt": round(size_usdt, 2),
            "risk_usdt": round(risk_usdt, 2),
            "risk_pct": round(risk_pct, 4),
            "leverage": round(leverage, 2),
            "rr": rr,
            "reduction_reason": " | ".join(reason_parts) if reason_parts else "None",
        }

    # ═══════════════════════════════════════════
    # SIGNAL EVALUATION (final approval)
    # ═══════════════════════════════════════════

    async def evaluate(self, signal) -> bool:
        """Final gate before Telegram delivery."""
        # Re-check all limits (state may have changed during scan)
        now = datetime.now(timezone.utc)
        if not self.can_scan(now):
            return False

        # SL distance sanity
        sl_dist_pct = abs(signal.entry_price - signal.sl_price) / signal.entry_price
        if sl_dist_pct > config.MAX_SL_DISTANCE:
            log.warning(f"SL too wide {signal.symbol}: {sl_dist_pct:.2%}")
            return False

        # Size sanity
        if signal.size_usdt <= 0:
            return False

        # Duplicate signal cooldown
        if signal.symbol in self._recent_alerts():
            return False

        return True

    # ═══════════════════════════════════════════
    # DRAWDOWN MONITORING
    # ═══════════════════════════════════════════

    def get_drawdown_action(self) -> str:
        """What action to take at current drawdown level."""
        dd = self.state.drawdown_pct
        if dd >= config.DRAWDOWN_NUCLEAR:
            return "NUCLEAR_STOP"
        elif dd >= config.DRAWDOWN_EMERGENCY:
            return "EMERGENCY_STOP"
        elif dd >= config.DRAWDOWN_CRITICAL:
            return "REDUCE_75PCT"
        elif dd >= config.DRAWDOWN_SERIOUS:
            return "REDUCE_50PCT"
        elif dd >= config.DRAWDOWN_WARNING:
            return "REDUCE_25PCT"
        return "NORMAL"

    # ═══════════════════════════════════════════
    # COOLDOWN
    # ═══════════════════════════════════════════

    def _cooldown_remaining(self) -> float:
        """Seconds remaining in cooldown. 0 = no cooldown."""
        consec = self.state.consecutive_losses
        last_loss_ts = self.state.last_loss_ts

        if consec < 2 or last_loss_ts == 0:
            return 0.0

        if consec >= 5:
            cooldown = config.COOLDOWN_5_LOSS * 60
        elif consec >= 4:
            cooldown = config.COOLDOWN_4_LOSS * 60
        elif consec >= 3:
            cooldown = config.COOLDOWN_3_LOSS * 60
        else:
            cooldown = config.COOLDOWN_2_LOSS * 60

        elapsed = time.time() - last_loss_ts
        return max(0, cooldown - elapsed)

    def _recent_alerts(self) -> set:
        """Symbols that had an alert within ALERT_COOLDOWN seconds."""
        cutoff = time.time() - config.ALERT_COOLDOWN_SECONDS
        return {
            sig["symbol"]
            for sig in self.state.get_active_signals()
            if sig.get("generated_at", 0) > cutoff
        }

    # ═══════════════════════════════════════════
    # RISK STATUS SUMMARY
    # ═══════════════════════════════════════════

    def get_status(self) -> dict:
        return {
            "balance": self.state.current_balance_usdt,
            "drawdown_pct": f"{self.state.drawdown_pct:.1%}",
            "daily_loss_pct": f"{self.state.daily_loss_pct:.1%}",
            "weekly_loss_pct": f"{self.state.weekly_loss_pct:.1%}",
            "consecutive_losses": self.state.consecutive_losses,
            "active_positions": self.state.count_active_positions(),
            "cooldown_remaining_s": round(self._cooldown_remaining()),
            "kill_switch": self.state.kill_switch_active,
            "action": self.get_drawdown_action(),
        }
