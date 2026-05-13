"""
ARUNABHA ELITE SCALPER v3.0
FILE 13/18: monitoring.py
System health monitoring — memory, CPU, WS latency, data freshness
Periodic health reports to Telegram
"""

import asyncio
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Dict, Optional

import psutil

import config

log = logging.getLogger("elite.monitor")


class MonitoringEngine:
    def __init__(self, state, telegram, ws_engine):
        self.state = state
        self.telegram = telegram
        self.ws = ws_engine

        self._shutdown = asyncio.Event()
        self._error_timestamps: deque = deque(maxlen=1000)
        self._api_latencies: deque = deque(maxlen=100)
        self._ws_latencies: deque = deque(maxlen=100)
        self._last_health_report: float = 0.0
        self._health_report_interval: int = 3600  # hourly
        self._start_time: float = time.time()
        self._scan_times: deque = deque(maxlen=100)
        self._consecutive_errors: int = 0

    # ═══════════════════════════════════════════
    # MAIN LOOP
    # ═══════════════════════════════════════════

    async def run(self):
        log.info("Monitoring engine started")
        while not self._shutdown.is_set():
            try:
                await self._check_health()
                await self._check_data_freshness()
                await self._check_drawdown_alerts()
                await self._maybe_send_health_report()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug(f"Monitor loop error: {e}")
            await asyncio.sleep(config.HEALTH_CHECK_INTERVAL)

    # ═══════════════════════════════════════════
    # HEALTH CHECKS
    # ═══════════════════════════════════════════

    async def _check_health(self):
        issues = []

        # Memory
        mem_pct = self._get_memory_pct()
        if mem_pct > config.MEMORY_ALERT_PCT:
            issues.append(f"⚠️ Memory HIGH: {mem_pct:.0%}")

        # CPU
        try:
            cpu = psutil.cpu_percent(interval=1) / 100
            if cpu > config.CPU_TARGET_PCT:
                issues.append(f"⚠️ CPU HIGH: {cpu:.0%}")
        except Exception:
            pass

        # WS state
        if self.ws:
            ws_stats = self.ws.get_ws_stats()
            state_val = ws_stats.get("state", "UNKNOWN")
            if state_val not in ("OPEN", "CONNECTING"):
                issues.append(f"🔴 WS state: {state_val}")

            # WS latency
            lat = ws_stats.get("latency_ms", 0)
            if lat > config.WS_LATENCY_TARGET_MS * 2:
                issues.append(f"⚠️ WS latency: {lat:.0f}ms")

            # Stale data
            age = ws_stats.get("last_msg_age_s", -1)
            if age > 30:
                issues.append(f"🔴 WS stale: {age:.0f}s")

        # Error rate
        recent_errors = self._count_recent_errors(60)
        if recent_errors > 10:
            issues.append(f"⚠️ Error rate: {recent_errors}/min")

        if issues:
            self._consecutive_errors += 1
            if self._consecutive_errors >= 3:
                msg = "🚨 <b>HEALTH ALERT</b>\n" + "\n".join(issues)
                await self.telegram.send_alert(msg, priority="high")
                self._consecutive_errors = 0
        else:
            self._consecutive_errors = 0

    async def _check_data_freshness(self):
        """Verify data is actually flowing in from WS."""
        if not self.ws:
            return
        ws_stats = self.ws.get_ws_stats()
        age = ws_stats.get("last_msg_age_s", -1)
        if 0 < age > config.DATA_FRESHNESS_SECONDS * 3:
            log.warning(f"Data stale: {age:.0f}s since last WS message")

    async def _check_drawdown_alerts(self):
        """Send alerts at drawdown thresholds."""
        dd = self.state.drawdown_pct
        s = self.state

        # Daily loss
        if s.daily_loss_pct >= config.MAX_DAILY_LOSS * 0.9:
            msg = f"⚠️ Daily loss near limit: {s.daily_loss_pct:.1%} / {config.MAX_DAILY_LOSS:.0%}"
            await self.telegram.send_alert(msg, priority="high")

        # Drawdown thresholds (alert once at each level)
        if not hasattr(self, "_dd_alerted"):
            self._dd_alerted = set()

        for threshold, label in [
            (config.DRAWDOWN_WARNING, "WARNING"),
            (config.DRAWDOWN_SERIOUS, "SERIOUS"),
            (config.DRAWDOWN_CRITICAL, "CRITICAL"),
            (config.DRAWDOWN_EMERGENCY, "EMERGENCY 🚨"),
        ]:
            key = f"dd_{threshold}"
            if dd >= threshold and key not in self._dd_alerted:
                self._dd_alerted.add(key)
                msg = (
                    f"🔴 <b>DRAWDOWN {label}</b>\n"
                    f"Current: <code>{dd:.1%}</code>\n"
                    f"Threshold: <code>{threshold:.0%}</code>\n"
                    f"Balance: <code>${s.current_balance_usdt:.2f}</code>"
                )
                await self.telegram.send_alert(msg, priority="high")
            elif dd < threshold * 0.8 and key in self._dd_alerted:
                self._dd_alerted.discard(key)  # reset if recovered

    # ═══════════════════════════════════════════
    # HEALTH REPORT
    # ═══════════════════════════════════════════

    async def _maybe_send_health_report(self):
        now = time.time()
        if now - self._last_health_report < self._health_report_interval:
            return

        self._last_health_report = now
        report = await self._build_health_report()
        await self.telegram.send_alert(report, priority="normal")

    async def _build_health_report(self) -> str:
        uptime_h = (time.time() - self._start_time) / 3600
        mem_pct = self._get_memory_pct()
        ws_stats = self.ws.get_ws_stats() if self.ws else {}
        s = self.state
        stats = s.get_stats()

        now_ist = datetime.now(timezone.utc)
        ist_h = (now_ist.hour + 5) % 24
        ist_m = (now_ist.minute + 30) % 60
        ist_h += (now_ist.minute + 30) // 60

        return (
            f"💚 <b>HEALTH REPORT</b>\n"
            f"🕐 {ist_h:02d}:{ist_m:02d} IST | Up {uptime_h:.1f}h\n\n"
            f"<b>Account</b>\n"
            f"Balance: <code>${stats['balance']}</code>\n"
            f"Drawdown: <code>{stats['drawdown']}</code>\n"
            f"Daily P&L: <code>{stats['daily_loss']}</code>\n\n"
            f"<b>Signals</b>\n"
            f"Total: <code>{stats['total_signals']}</code>\n"
            f"Win/Loss: <code>{stats['wins']}/{stats['losses']}</code>\n"
            f"Win Rate: <code>{stats['win_rate']}</code>\n"
            f"Active: <code>{stats['active']}</code>\n\n"
            f"<b>System</b>\n"
            f"Memory: <code>{mem_pct:.0%}</code>\n"
            f"WS State: <code>{ws_stats.get('state', 'N/A')}</code>\n"
            f"WS Latency: <code>{ws_stats.get('latency_ms', 0):.0f}ms</code>\n"
            f"Scans: <code>{stats['scan_count']}</code>\n"
            f"Errors/min: <code>{self._count_recent_errors(60)}</code>"
        )

    # ═══════════════════════════════════════════
    # ERROR TRACKING
    # ═══════════════════════════════════════════

    def record_error(self, source: str = ""):
        self._error_timestamps.append(time.time())

    def _count_recent_errors(self, window_seconds: int) -> int:
        cutoff = time.time() - window_seconds
        return sum(1 for ts in self._error_timestamps if ts > cutoff)

    def record_api_latency(self, ms: float):
        self._api_latencies.append(ms)

    def record_ws_latency(self, ms: float):
        self._ws_latencies.append(ms)

    # ═══════════════════════════════════════════
    # SYSTEM METRICS
    # ═══════════════════════════════════════════

    def _get_memory_pct(self) -> float:
        try:
            proc = psutil.Process(os.getpid())
            rss = proc.memory_info().rss
            total = psutil.virtual_memory().total
            return rss / total
        except Exception:
            return 0.0

    def get_metrics(self) -> dict:
        import numpy as np
        mem = self._get_memory_pct()
        ws_stats = self.ws.get_ws_stats() if self.ws else {}
        api_lats = list(self._api_latencies)
        ws_lats = list(self._ws_latencies)

        return {
            "memory_pct": round(mem, 3),
            "uptime_s": round(time.time() - self._start_time),
            "errors_1min": self._count_recent_errors(60),
            "errors_5min": self._count_recent_errors(300),
            "api_latency_avg_ms": round(float(np.mean(api_lats)), 1) if api_lats else 0,
            "ws_latency_avg_ms": round(float(np.mean(ws_lats)), 1) if ws_lats else 0,
            "ws_state": ws_stats.get("state", "N/A"),
            "ws_reconnects": ws_stats.get("reconnects", 0),
            "ws_queue_depth": ws_stats.get("queue_depth", 0),
        }

    async def close(self):
        self._shutdown.set()
