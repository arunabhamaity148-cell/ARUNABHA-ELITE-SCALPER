"""
ARUNABHA ELITE SCALPER v3.0
NEW FILE: session_tracker.py
Trading session tracker — Asian / London / NY session detection.
Tracks win rate per session and auto-adjusts position size.
Asian: 00:00-08:00 UTC → often chop → reduce 25%
London: 08:00-16:00 UTC → high volume → normal
NY: 16:00-00:00 UTC → high volume → normal
"""

import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import config

log = logging.getLogger("elite.session")

SESSION_FILE = "session_stats.json"


@dataclass
class SessionStats:
    trades: int = 0
    wins: int = 0
    losses: int = 0
    total_r: float = 0.0

    @property
    def win_rate(self) -> float:
        return self.wins / self.trades if self.trades > 0 else 0.5

    @property
    def avg_r(self) -> float:
        return self.total_r / self.trades if self.trades > 0 else 0.0


class SessionTracker:
    """
    Identifies current trading session and applies size multiplier.
    Learns from historical win rates per session.
    """

    SESSIONS = {
        "ASIAN": (config.SESSION_ASIAN_START_UTC, config.SESSION_ASIAN_END_UTC),
        "LONDON": (config.SESSION_LONDON_START_UTC, config.SESSION_LONDON_END_UTC),
        "NY": (config.SESSION_NY_START_UTC, config.SESSION_NY_END_UTC),
    }

    BASE_MULTIPLIERS = {
        "ASIAN": config.SESSION_ASIAN_SIZE_MULT,
        "LONDON": config.SESSION_LONDON_SIZE_MULT,
        "NY": config.SESSION_NY_SIZE_MULT,
    }

    def __init__(self):
        self._stats: Dict[str, SessionStats] = {
            s: SessionStats() for s in self.SESSIONS
        }
        self._current_session: str = "ASIAN"
        self._session_signals: deque = deque(maxlen=200)   # rolling signal log
        self._shutdown = asyncio.Event()
        self._load()

    # ═══════════════════════════════════════════
    # MAIN LOOP
    # ═══════════════════════════════════════════

    async def run(self):
        log.info("Session tracker started")
        while not self._shutdown.is_set():
            try:
                self._current_session = self.get_current_session()
                await self._maybe_log_session_change()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug(f"Session tracker error: {e}")
            await asyncio.sleep(60)

    # ═══════════════════════════════════════════
    # SESSION DETECTION
    # ═══════════════════════════════════════════

    def get_current_session(self) -> str:
        """Returns 'ASIAN', 'LONDON', or 'NY'."""
        utc_hour = datetime.now(timezone.utc).hour
        for name, (start, end) in self.SESSIONS.items():
            if start <= utc_hour < end:
                return name
        return "NY"  # fallback (covers 00:00 edge if needed)

    def get_session_for_hour(self, utc_hour: int) -> str:
        for name, (start, end) in self.SESSIONS.items():
            if start <= utc_hour < end:
                return name
        return "NY"

    # ═══════════════════════════════════════════
    # SIZE MULTIPLIER
    # ═══════════════════════════════════════════

    def get_size_multiplier(self) -> float:
        """
        Returns size multiplier for current session.
        Base multiplier adjusted by learned win rate vs global average.
        """
        session = self.get_current_session()
        base = self.BASE_MULTIPLIERS[session]

        # Only adjust if we have enough data
        stats = self._stats[session]
        if stats.trades < config.SESSION_MIN_TRADES_FOR_STATS:
            return base

        # Compare session win rate vs global win rate
        global_trades = sum(s.trades for s in self._stats.values())
        global_wins = sum(s.wins for s in self._stats.values())
        global_wr = global_wins / global_trades if global_trades > 0 else 0.5

        session_wr = stats.win_rate
        wr_diff = session_wr - global_wr

        # Adjust multiplier proportionally (±20% max adjustment)
        adjustment = max(-0.20, min(0.20, wr_diff * 2))
        adjusted = base + adjustment

        return round(max(0.25, min(1.50, adjusted)), 3)

    # ═══════════════════════════════════════════
    # OUTCOME RECORDING
    # ═══════════════════════════════════════════

    def record_signal(self, symbol: str, direction: str):
        """Record that a signal was generated in current session."""
        session = self.get_current_session()
        self._session_signals.append({
            "session": session,
            "symbol": symbol,
            "direction": direction,
            "ts": time.time(),
        })

    def record_outcome(self, symbol: str, won: bool, pnl_r: float):
        """Record trade outcome for session stats learning."""
        session = self.get_current_session()
        stats = self._stats[session]
        stats.trades += 1
        if won:
            stats.wins += 1
        else:
            stats.losses += 1
        stats.total_r += pnl_r
        self._save()
        log.debug(f"Session {session}: WR={stats.win_rate:.1%} AvgR={stats.avg_r:.2f} ({stats.trades} trades)")

    # ═══════════════════════════════════════════
    # SESSION OVERLAP DETECTION
    # ═══════════════════════════════════════════

    def is_session_overlap(self) -> bool:
        """
        London/NY overlap: 16:00-17:00 UTC → highest volume.
        London/Asia overlap: 07:00-09:00 UTC → moderate volume.
        Returns True if currently in an overlap period.
        """
        utc_hour = datetime.now(timezone.utc).hour
        london_ny_overlap = (utc_hour == 16 or utc_hour == 17)
        london_asia_overlap = (utc_hour == 7 or utc_hour == 8)
        return london_ny_overlap or london_asia_overlap

    def get_overlap_bonus(self) -> float:
        """Return size bonus during session overlaps (high liquidity)."""
        return 1.15 if self.is_session_overlap() else 1.0

    # ═══════════════════════════════════════════
    # LOGGING
    # ═══════════════════════════════════════════

    async def _maybe_log_session_change(self):
        """Log when session changes."""
        if not hasattr(self, "_prev_session"):
            self._prev_session = self._current_session
            return
        if self._current_session != self._prev_session:
            log.info(f"Session change: {self._prev_session} → {self._current_session}")
            self._prev_session = self._current_session

    # ═══════════════════════════════════════════
    # PERSISTENCE
    # ═══════════════════════════════════════════

    def _save(self):
        try:
            data = {s: asdict(stats) for s, stats in self._stats.items()}
            with open(SESSION_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            log.debug(f"Session save error: {e}")

    def _load(self):
        if not os.path.exists(SESSION_FILE):
            return
        try:
            with open(SESSION_FILE, "r") as f:
                data = json.load(f)
            for session, vals in data.items():
                if session in self._stats:
                    self._stats[session] = SessionStats(**vals)
            log.info(f"Session stats loaded: {[(s, self._stats[s].trades) for s in self._stats]}")
        except Exception as e:
            log.debug(f"Session load error: {e}")

    # ═══════════════════════════════════════════
    # STATUS
    # ═══════════════════════════════════════════

    def get_status(self) -> dict:
        current = self.get_current_session()
        return {
            "current_session": current,
            "multiplier": self.get_size_multiplier(),
            "is_overlap": self.is_session_overlap(),
            "stats": {
                s: {
                    "trades": st.trades,
                    "win_rate": round(st.win_rate, 3),
                    "avg_r": round(st.avg_r, 3),
                }
                for s, st in self._stats.items()
            },
        }

    def format_for_telegram(self) -> str:
        current = self.get_current_session()
        lines = [f"🕐 <b>Session: {current}</b>"]
        if self.is_session_overlap():
            lines.append("⚡ Session overlap — high liquidity")
        lines.append(f"Size multiplier: <code>{self.get_size_multiplier():.2f}x</code>")
        lines.append("")
        lines.append("<b>Session Stats</b>")
        for s, stats in self._stats.items():
            marker = "▶" if s == current else "  "
            if stats.trades > 0:
                lines.append(
                    f"{marker} {s}: {stats.trades} trades | WR {stats.win_rate:.0%} | "
                    f"AvgR {stats.avg_r:.2f}"
                )
            else:
                lines.append(f"{marker} {s}: no data yet")
        return "\n".join(lines)

    async def close(self):
        self._shutdown.set()
        self._save()
