"""
ARUNABHA MANUAL SCALPER v4.0
FILE: signal_health_engine.py  [NEW]

Real-Time Signal Health Engine (#8)
+ Post-Liquidation Reclaim Setup (#7)
+ Scalp Lifecycle Engine (#19 — partial)

PURPOSE:
  After a signal is generated, the market keeps moving.
  A signal that was valid 5 minutes ago may now be:
    - Still perfect (A+)
    - Getting stale (downgrade to B)
    - Completely invalidated (cancel it)

  This engine monitors ALL active signals continuously
  and sends updates when signal health changes.

POST-LIQUIDATION RECLAIM:
  After a large liquidation event, price often:
  1. Drops/spikes fast (cascade)
  2. Stabilizes at a level (absorption)
  3. Reclaims back through the trigger level
  This pattern is detectable and tradeable.
"""

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple

import config
from signal_engine import ScalpSignal, ScalpSignalType, SignalGrade, SignalStatus

log = logging.getLogger("scalper.health")


class HealthStatus(Enum):
    PERFECT = "PERFECT"       # Setup improving since generation
    GOOD = "GOOD"             # Setup intact
    DEGRADED = "DEGRADED"     # Partially invalidated, still tradeable
    STALE = "STALE"           # Price drifted, entry no longer clean
    CANCEL = "CANCEL"         # Setup broken, send cancellation


@dataclass
class SignalHealthReport:
    signal_id: str
    symbol: str
    status: HealthStatus
    current_price: float
    entry_drift_pct: float       # how far price moved from ideal entry
    time_elapsed_min: float
    quality_tag: str             # A+ / A / B / SKIP
    change_from_last: str        # IMPROVED / UNCHANGED / DEGRADED / CANCELLED
    reason: str
    send_update: bool            # should we send Telegram update?
    size_multiplier: float = 1.0


class SignalHealthEngine:
    """
    Monitors all active signals every 30 seconds.
    Downgrades or cancels signals based on live price movement.
    Also detects Post-Liquidation Reclaim setups.
    """

    def __init__(self, data_processor, telegram_bot, manual_exec_assistant):
        self.dp = data_processor
        self.telegram = telegram_bot
        self.mexec = manual_exec_assistant
        self._last_health: Dict[str, HealthStatus] = {}
        self._post_liq_tracking: Dict[str, dict] = {}   # sym → liq event data

    # ═══════════════════════════════════════════
    # MAIN CHECK — called every 30s from main.py
    # ═══════════════════════════════════════════

    def check_all(
        self, active_signals: Dict[str, ScalpSignal]
    ) -> List[SignalHealthReport]:
        """
        Check health of all active signals.
        Returns list of reports — caller sends Telegram alerts as needed.
        """
        reports = []
        for symbol, signal in list(active_signals.items()):
            try:
                report = self._check_signal(signal)
                reports.append(report)
            except Exception as e:
                log.debug(f"Health check error {symbol}: {e}")
        return reports

    def _check_signal(self, signal: ScalpSignal) -> SignalHealthReport:
        """Run all health checks on one signal."""
        current_price = self.dp.get_latest_price(signal.symbol)
        elapsed_min = (time.time() - signal.generated_at) / 60
        ideal = signal.entry_ideal

        # ── Drift from ideal entry ──
        if ideal > 0:
            drift_pct = abs(current_price - ideal) / ideal
        else:
            drift_pct = 0.0

        # ── Direction of drift (good or bad?) ──
        # "bad drift" = price moving away from our entry toward TP (we missed)
        if signal.direction == "LONG":
            moved_toward_tp = current_price > signal.entry_high
            moved_toward_sl = current_price < signal.sl_price * 1.01
        else:
            moved_toward_tp = current_price < signal.entry_low
            moved_toward_sl = current_price > signal.sl_price * 0.99

        # ── Hard cancels ──
        if moved_toward_sl:
            return self._make_report(
                signal, current_price, elapsed_min, drift_pct,
                HealthStatus.CANCEL,
                "Price hit SL zone before entry — thesis broken",
                send_update=True, size_mult=0.0,
            )

        if moved_toward_tp and drift_pct > config.SIGNAL_MAX_CHASE_PCT:
            return self._make_report(
                signal, current_price, elapsed_min, drift_pct,
                HealthStatus.CANCEL,
                "Price ran to TP zone — entry window closed",
                send_update=True, size_mult=0.0,
            )

        # ── Entry drift degradation ──
        if drift_pct > config.SIGNAL_ENTRY_DRIFT_PCT:
            # 0.5%+ drift = stale
            return self._make_report(
                signal, current_price, elapsed_min, drift_pct,
                HealthStatus.STALE,
                f"Entry drifted {drift_pct*100:.2f}% from ideal — still possible but imperfect",
                send_update=self._status_changed(signal.signal_id, HealthStatus.STALE),
                size_mult=0.70,
            )

        if drift_pct > config.SIGNAL_MAX_CHASE_PCT:
            # Beyond max chase = CANCEL
            return self._make_report(
                signal, current_price, elapsed_min, drift_pct,
                HealthStatus.CANCEL,
                f"Price drifted {drift_pct*100:.2f}% — beyond chase limit",
                send_update=True, size_mult=0.0,
            )

        # ── Time degradation ──
        if elapsed_min > config.SIGNAL_TRIGGER_TIMEOUT_MINUTES * 0.75:
            # 75% of timeout elapsed = warn
            return self._make_report(
                signal, current_price, elapsed_min, drift_pct,
                HealthStatus.DEGRADED,
                f"{elapsed_min:.0f}min elapsed — {config.SIGNAL_TRIGGER_TIMEOUT_MINUTES - elapsed_min:.0f}min left before cancel",
                send_update=self._status_changed(signal.signal_id, HealthStatus.DEGRADED),
                size_mult=0.85,
            )

        # ── Signal improving? (price moving INTO entry zone) ──
        in_zone = signal.entry_low <= current_price <= signal.entry_high
        improving = (
            signal.direction == "LONG" and current_price <= signal.entry_high
            and current_price >= signal.entry_low * 0.998
        ) or (
            signal.direction == "SHORT" and current_price >= signal.entry_low
            and current_price <= signal.entry_high * 1.002
        )

        if in_zone and elapsed_min < 5:
            status = HealthStatus.PERFECT
        elif improving:
            status = HealthStatus.GOOD
        else:
            status = HealthStatus.GOOD

        changed = self._status_changed(signal.signal_id, status)
        return self._make_report(
            signal, current_price, elapsed_min, drift_pct,
            status, "Signal intact",
            send_update=(changed and status == HealthStatus.PERFECT),
            size_mult=1.0,
        )

    def _make_report(
        self, signal, price, elapsed, drift,
        status, reason, send_update, size_mult,
    ) -> SignalHealthReport:
        last = self._last_health.get(signal.signal_id)
        if last == status:
            change_str = "UNCHANGED"
        elif status.value > (last.value if last else ""):
            change_str = "IMPROVED"
        else:
            change_str = "DEGRADED"

        self._last_health[signal.signal_id] = status

        quality_map = {
            HealthStatus.PERFECT:  "A+",
            HealthStatus.GOOD:     "A",
            HealthStatus.DEGRADED: "B",
            HealthStatus.STALE:    "B",
            HealthStatus.CANCEL:   "SKIP-LATE",
        }

        return SignalHealthReport(
            signal_id=signal.signal_id,
            symbol=signal.symbol,
            status=status,
            current_price=price,
            entry_drift_pct=drift,
            time_elapsed_min=elapsed,
            quality_tag=quality_map[status],
            change_from_last=change_str,
            reason=reason,
            send_update=send_update,
            size_multiplier=size_mult,
        )

    def _status_changed(self, signal_id: str, new_status: HealthStatus) -> bool:
        return self._last_health.get(signal_id) != new_status

    # ═══════════════════════════════════════════
    # FORMAT UPDATE MESSAGE
    # ═══════════════════════════════════════════

    def format_health_update(
        self, signal: ScalpSignal, report: SignalHealthReport
    ) -> str:
        """Format a health update Telegram message."""
        e = config.EMOJI

        if report.status == HealthStatus.CANCEL:
            return (
                f"⏰ <b>SIGNAL CANCELLED — {signal.symbol}</b>\n"
                f"{signal.direction} {signal.signal_type.value.replace('_', ' ')}\n"
                f"Reason: {report.reason}\n"
                f"<i>Do not enter. Setup broken.</i>"
            )

        if report.status == HealthStatus.PERFECT:
            return (
                f"🎯 <b>SIGNAL PERFECT NOW — {signal.symbol}</b>\n"
                f"Price <code>{report.current_price:.4f}</code> is in entry zone\n"
                f"⚡ {report.time_elapsed_min:.0f}min since signal — entry window still clean\n"
                f"Quality: <b>{report.quality_tag}</b>"
            )

        if report.status == HealthStatus.DEGRADED:
            return (
                f"⚠️ <b>SIGNAL DEGRADING — {signal.symbol}</b>\n"
                f"Time elapsed: {report.time_elapsed_min:.0f}min\n"
                f"{report.reason}\n"
                f"Quality now: <b>{report.quality_tag}</b>"
            )

        return ""

    # ═══════════════════════════════════════════
    # POST-LIQUIDATION RECLAIM DETECTOR
    # ═══════════════════════════════════════════

    def detect_post_liq_reclaim(
        self,
        symbol: str,
        deriv_snap,      # DerivativesSnapshot
        ind_trigger,     # 3m indicators
        ind_primary,     # 15m indicators
        attention_snap,  # AttentionSnapshot
    ) -> Optional[dict]:
        """
        Detect Post-Liquidation Reclaim setup (#7).

        Pattern:
        1. Large OI collapse (liquidation event happened)
        2. Price stabilizes for 2-5 candles (absorption)
        3. Price starts reclaiming key level
        → This is a high-quality fast setup

        Returns setup dict if detected, None otherwise.
        """
        if not deriv_snap or not ind_trigger or not ind_primary:
            return None

        sym_data = self._post_liq_tracking.get(symbol, {})

        # Phase 1: Detect liquidation event
        if deriv_snap.oi_collapsing and abs(deriv_snap.oi_change_pct) >= 0.05:
            # 5%+ OI drop = significant liquidation
            if "liq_event_time" not in sym_data:
                sym_data = {
                    "liq_event_time": time.time(),
                    "liq_price": ind_trigger.close,
                    "liq_direction": (
                        "LONG_LIQ" if deriv_snap.ls_crowded_long else "SHORT_LIQ"
                    ),
                    "oi_at_liq": deriv_snap.oi_now,
                    "phase": "LIQUIDATING",
                }
                self._post_liq_tracking[symbol] = sym_data
                log.info(f"Post-liq tracking started: {symbol} "
                         f"{sym_data['liq_direction']} OI:{deriv_snap.oi_change_pct:.1%}")

        if "liq_event_time" not in sym_data:
            return None

        elapsed_since_liq = (time.time() - sym_data["liq_event_time"]) / 60

        # Liquidation event too old (> 30 min) — clear tracking
        if elapsed_since_liq > 30:
            self._post_liq_tracking.pop(symbol, None)
            return None

        # Phase 2: Stabilization (2-8 min after liquidation)
        if sym_data["phase"] == "LIQUIDATING" and 2 <= elapsed_since_liq <= 8:
            if not deriv_snap.oi_collapsing:
                # OI stabilizing = liquidation done
                sym_data["phase"] = "STABILIZING"
                sym_data["stable_price"] = ind_trigger.close
                self._post_liq_tracking[symbol] = sym_data

        # Phase 3: Reclaim (price moves back through trigger level)
        if sym_data["phase"] == "STABILIZING" and elapsed_since_liq >= 3:
            liq_price = sym_data["liq_price"]
            current_price = ind_trigger.close
            liq_direction = sym_data["liq_direction"]

            # For LONG liquidation: price dropped → now reclaiming UP
            if (
                liq_direction == "LONG_LIQ"
                and current_price > liq_price * 0.998   # back near liq level
                and ind_primary.rsi and 35 <= ind_primary.rsi <= 60  # not extreme
            ):
                sym_data["phase"] = "RECLAIMING"
                self._post_liq_tracking[symbol] = sym_data

                atr = ind_primary.atr if ind_primary.atr else current_price * 0.005
                return {
                    "symbol": symbol,
                    "direction": "LONG",
                    "signal_type": "POST_LIQ_RECLAIM",
                    "setup_quality": "HIGH",
                    "liq_event_elapsed_min": elapsed_since_liq,
                    "entry_low": current_price * 0.999,
                    "entry_high": current_price * 1.002,
                    "entry_ideal": current_price,
                    "sl": current_price - atr * 1.2,
                    "tp1": current_price + atr * 1.5,
                    "tp2": current_price + atr * 2.5,
                    "why": (
                        f"Long liquidation cascade ended {elapsed_since_liq:.0f}min ago — "
                        f"price reclaiming liquidation level"
                    ),
                    "thesis": (
                        f"Post-liq reclaim LONG {symbol} — forced sellers done, "
                        f"buyers stepping in at re-test"
                    ),
                    "expected_hold_min": 20,
                    "base_score": 65.0,
                }

            # For SHORT liquidation: price pumped → now reclaiming DOWN
            if (
                liq_direction == "SHORT_LIQ"
                and current_price < liq_price * 1.002
                and ind_primary.rsi and 40 <= ind_primary.rsi <= 65
            ):
                sym_data["phase"] = "RECLAIMING"
                self._post_liq_tracking[symbol] = sym_data

                atr = ind_primary.atr if ind_primary.atr else current_price * 0.005
                return {
                    "symbol": symbol,
                    "direction": "SHORT",
                    "signal_type": "POST_LIQ_RECLAIM",
                    "setup_quality": "HIGH",
                    "liq_event_elapsed_min": elapsed_since_liq,
                    "entry_low": current_price * 0.998,
                    "entry_high": current_price * 1.001,
                    "entry_ideal": current_price,
                    "sl": current_price + atr * 1.2,
                    "tp1": current_price - atr * 1.5,
                    "tp2": current_price - atr * 2.5,
                    "why": (
                        f"Short squeeze ended {elapsed_since_liq:.0f}min ago — "
                        f"price reclaiming pump level"
                    ),
                    "thesis": (
                        f"Post-liq reclaim SHORT {symbol} — squeezed shorts done, "
                        f"sellers returning at re-test"
                    ),
                    "expected_hold_min": 20,
                    "base_score": 65.0,
                }

        return None

    def clear_post_liq(self, symbol: str):
        """Clear post-liquidation tracking for symbol."""
        self._post_liq_tracking.pop(symbol, None)
