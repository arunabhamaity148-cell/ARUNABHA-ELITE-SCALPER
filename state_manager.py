"""
ARUNABHA ELITE SCALPER v3.0
FILE 12/18: state_manager.py
Persistent runtime state — balance, drawdown, P&L, active signals, scan metrics
Saves to JSON (Railway ephemeral FS acceptable; Redis optional)
"""

import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import config

log = logging.getLogger("elite.state")

STATE_FILE = "bot_state.json"


@dataclass
class SignalRecord:
    symbol: str
    direction: str
    grade: str
    signal_type: str
    entry_price: float
    sl_price: float
    tp1_price: float
    risk_pct: float
    risk_usdt: float
    size_usdt: float
    generated_at: float
    expires_at: float
    status: str = "ACTIVE"          # ACTIVE / HIT_TP1 / HIT_TP2 / HIT_TP3 / HIT_SL / EXPIRED
    closed_at: float = 0.0
    pnl_usdt: float = 0.0
    ml_features: List[float] = field(default_factory=list)


class StateManager:
    def __init__(self):
        # Account
        self.current_balance_usdt: float = config.ACCOUNT_BALANCE_USDT
        self.peak_balance_usdt: float = config.ACCOUNT_BALANCE_USDT
        self.drawdown_pct: float = 0.0

        # Daily / weekly / monthly P&L (as fraction)
        self.daily_loss_pct: float = 0.0
        self.weekly_loss_pct: float = 0.0
        self.monthly_loss_pct: float = 0.0
        self._daily_start_balance: float = config.ACCOUNT_BALANCE_USDT
        self._weekly_start_balance: float = config.ACCOUNT_BALANCE_USDT
        self._monthly_start_balance: float = config.ACCOUNT_BALANCE_USDT
        self._day_start_ts: float = 0.0
        self._week_start_ts: float = 0.0
        self._month_start_ts: float = 0.0

        # Loss streak
        self.consecutive_losses: int = 0
        self.last_loss_ts: float = 0.0

        # Kill switch
        self.kill_switch_active: bool = False

        # Signals
        self._active_signals: Dict[str, SignalRecord] = {}
        self._signal_history: List[SignalRecord] = []

        # Stats
        self.total_signals: int = 0
        self.total_wins: int = 0
        self.total_losses: int = 0
        self.scan_count: int = 0
        self._started_at: float = time.time()

    # ═══════════════════════════════════════════
    # LOAD / SAVE
    # ═══════════════════════════════════════════

    async def load(self):
        if not os.path.exists(STATE_FILE):
            log.info("No saved state — starting fresh")
            self._reset_period_timestamps()
            return

        try:
            with open(STATE_FILE, "r") as f:
                data = json.load(f)

            self.current_balance_usdt = data.get("current_balance_usdt", config.ACCOUNT_BALANCE_USDT)
            self.peak_balance_usdt = data.get("peak_balance_usdt", self.current_balance_usdt)
            self.consecutive_losses = data.get("consecutive_losses", 0)
            self.last_loss_ts = data.get("last_loss_ts", 0.0)
            self.kill_switch_active = data.get("kill_switch_active", False)
            self.total_signals = data.get("total_signals", 0)
            self.total_wins = data.get("total_wins", 0)
            self.total_losses = data.get("total_losses", 0)
            self._day_start_ts = data.get("day_start_ts", 0.0)
            self._week_start_ts = data.get("week_start_ts", 0.0)
            self._month_start_ts = data.get("month_start_ts", 0.0)
            self._daily_start_balance = data.get("daily_start_balance", self.current_balance_usdt)
            self._weekly_start_balance = data.get("weekly_start_balance", self.current_balance_usdt)
            self._monthly_start_balance = data.get("monthly_start_balance", self.current_balance_usdt)

            # Restore active signals
            for sym, rec_data in data.get("active_signals", {}).items():
                try:
                    rec = SignalRecord(**rec_data)
                    if rec.expires_at > time.time():  # not yet expired
                        self._active_signals[sym] = rec
                except Exception:
                    pass

            self._update_drawdown()
            self._check_period_reset()
            log.info(f"State loaded: balance=${self.current_balance_usdt:.2f}, dd={self.drawdown_pct:.1%}")

        except Exception as e:
            log.error(f"State load error: {e} — starting fresh")
            self._reset_period_timestamps()

    async def save(self):
        try:
            active = {}
            for sym, rec in self._active_signals.items():
                try:
                    active[sym] = asdict(rec)
                except Exception:
                    pass

            data = {
                "current_balance_usdt": self.current_balance_usdt,
                "peak_balance_usdt": self.peak_balance_usdt,
                "consecutive_losses": self.consecutive_losses,
                "last_loss_ts": self.last_loss_ts,
                "kill_switch_active": self.kill_switch_active,
                "total_signals": self.total_signals,
                "total_wins": self.total_wins,
                "total_losses": self.total_losses,
                "day_start_ts": self._day_start_ts,
                "week_start_ts": self._week_start_ts,
                "month_start_ts": self._month_start_ts,
                "daily_start_balance": self._daily_start_balance,
                "weekly_start_balance": self._weekly_start_balance,
                "monthly_start_balance": self._monthly_start_balance,
                "active_signals": active,
                "saved_at": time.time(),
            }
            with open(STATE_FILE, "w") as f:
                json.dump(data, f, indent=2)
            log.debug("State saved")
        except Exception as e:
            log.error(f"State save error: {e}")

    # ═══════════════════════════════════════════
    # SIGNAL MANAGEMENT
    # ═══════════════════════════════════════════

    def record_signal(self, signal):
        """Register a newly generated signal."""
        rec = SignalRecord(
            symbol=signal.symbol,
            direction=signal.direction,
            grade=signal.grade,
            signal_type=signal.signal_type,
            entry_price=signal.entry_price,
            sl_price=signal.sl_price,
            tp1_price=signal.tp1_price,
            risk_pct=signal.risk_pct,
            risk_usdt=signal.risk_usdt,
            size_usdt=signal.size_usdt,
            generated_at=signal.generated_at,
            expires_at=signal.expires_at,
        )
        self._active_signals[signal.symbol] = rec
        self.total_signals += 1
        log.info(f"Signal recorded: {signal.symbol} {signal.direction} {signal.grade}")

    def close_signal(self, symbol: str, outcome: str, pnl_usdt: float):
        """
        Close a signal with outcome.
        outcome: 'WIN' or 'LOSS'
        """
        rec = self._active_signals.pop(symbol, None)
        if not rec:
            return

        rec.status = "HIT_TP1" if outcome == "WIN" else "HIT_SL"
        rec.closed_at = time.time()
        rec.pnl_usdt = pnl_usdt
        self._signal_history.append(rec)

        # Update balance
        self.current_balance_usdt += pnl_usdt
        self._update_drawdown()
        self._update_period_pnl()

        if outcome == "WIN":
            self.total_wins += 1
            self.consecutive_losses = 0
        else:
            self.total_losses += 1
            self.consecutive_losses += 1
            self.last_loss_ts = time.time()

        log.info(f"Signal closed: {symbol} {outcome} PnL=${pnl_usdt:.2f}")

    def expire_old_signals(self):
        """Remove signals past their expiry time."""
        now = time.time()
        expired = [sym for sym, rec in self._active_signals.items() if rec.expires_at < now]
        for sym in expired:
            rec = self._active_signals.pop(sym)
            rec.status = "EXPIRED"
            rec.closed_at = now
            self._signal_history.append(rec)
            log.debug(f"Signal expired: {sym}")

    def get_active_signals(self) -> List[dict]:
        self.expire_old_signals()
        result = []
        for sym, rec in self._active_signals.items():
            result.append({
                "symbol": rec.symbol,
                "direction": rec.direction,
                "grade": rec.grade,
                "entry_price": rec.entry_price,
                "generated_at": rec.generated_at,
            })
        return result

    def has_active_signal(self, symbol: str) -> bool:
        self.expire_old_signals()
        return symbol in self._active_signals

    def count_active_positions(self) -> int:
        self.expire_old_signals()
        return len(self._active_signals)

    # ═══════════════════════════════════════════
    # BALANCE & DRAWDOWN
    # ═══════════════════════════════════════════

    def update_balance(self, new_balance: float):
        self.current_balance_usdt = new_balance
        self._update_drawdown()
        self._update_period_pnl()

    def _update_drawdown(self):
        if self.current_balance_usdt > self.peak_balance_usdt:
            self.peak_balance_usdt = self.current_balance_usdt
        if self.peak_balance_usdt > 0:
            self.drawdown_pct = max(
                0, (self.peak_balance_usdt - self.current_balance_usdt) / self.peak_balance_usdt
            )

    def _update_period_pnl(self):
        if self._daily_start_balance > 0:
            self.daily_loss_pct = max(
                0, (self._daily_start_balance - self.current_balance_usdt) / self._daily_start_balance
            )
        if self._weekly_start_balance > 0:
            self.weekly_loss_pct = max(
                0, (self._weekly_start_balance - self.current_balance_usdt) / self._weekly_start_balance
            )
        if self._monthly_start_balance > 0:
            self.monthly_loss_pct = max(
                0, (self._monthly_start_balance - self.current_balance_usdt) / self._monthly_start_balance
            )

    # ═══════════════════════════════════════════
    # PERIOD RESET
    # ═══════════════════════════════════════════

    def _reset_period_timestamps(self):
        now = time.time()
        self._day_start_ts = now
        self._week_start_ts = now
        self._month_start_ts = now
        self._daily_start_balance = self.current_balance_usdt
        self._weekly_start_balance = self.current_balance_usdt
        self._monthly_start_balance = self.current_balance_usdt

    def _check_period_reset(self):
        now = time.time()
        nowdt = datetime.fromtimestamp(now, tz=timezone.utc)

        # Day reset
        if now - self._day_start_ts >= 86400:
            self._day_start_ts = now
            self._daily_start_balance = self.current_balance_usdt
            self.daily_loss_pct = 0.0
            log.info("Daily P&L reset")

        # Week reset (Monday UTC)
        if nowdt.weekday() == 0 and now - self._week_start_ts >= 86400 * 6:
            self._week_start_ts = now
            self._weekly_start_balance = self.current_balance_usdt
            self.weekly_loss_pct = 0.0
            log.info("Weekly P&L reset")

        # Month reset
        if nowdt.day == 1 and now - self._month_start_ts >= 86400 * 27:
            self._month_start_ts = now
            self._monthly_start_balance = self.current_balance_usdt
            self.monthly_loss_pct = 0.0
            log.info("Monthly P&L reset")

    # ═══════════════════════════════════════════
    # SCAN TRACKING
    # ═══════════════════════════════════════════

    def update_scan(self, scan_count: int, signals_found: int):
        self.scan_count = scan_count
        self._check_period_reset()

    # ═══════════════════════════════════════════
    # STATS
    # ═══════════════════════════════════════════

    def get_stats(self) -> dict:
        total_closed = self.total_wins + self.total_losses
        win_rate = self.total_wins / total_closed if total_closed > 0 else 0.0
        uptime_h = (time.time() - self._started_at) / 3600

        return {
            "balance": round(self.current_balance_usdt, 2),
            "peak": round(self.peak_balance_usdt, 2),
            "drawdown": f"{self.drawdown_pct:.1%}",
            "daily_loss": f"{self.daily_loss_pct:.1%}",
            "weekly_loss": f"{self.weekly_loss_pct:.1%}",
            "total_signals": self.total_signals,
            "wins": self.total_wins,
            "losses": self.total_losses,
            "win_rate": f"{win_rate:.1%}",
            "consecutive_losses": self.consecutive_losses,
            "active": self.count_active_positions(),
            "scan_count": self.scan_count,
            "uptime_h": round(uptime_h, 1),
            "kill_switch": self.kill_switch_active,
        }
