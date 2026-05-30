"""
ARUNABHA MANUAL SCALPER v4.0
FILE: state_manager.py  (UPGRADED from v3)

Critical changes vs v3:
- Redis persistence for risk state (survives Railway restarts)
- Pair-level cooldowns after failed trades
- Narrative-level cooldowns after repeated failures
- Rotation statistics tracking
- Signal outcome logging for ML training
"""

import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import config

log = logging.getLogger("scalper.state")


@dataclass
class TradeOutcome:
    signal_id: str
    symbol: str
    direction: str
    signal_type: str
    grade: str
    entry_price: float
    sl_price: float
    tp1_price: float
    exit_price: float
    outcome: str          # TP1 / TP2 / TP3 / SL / MANUAL
    pnl_pct: float
    pnl_r: float          # R multiple
    hold_minutes: float
    attention_score: float
    narrative: str
    session: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class PairCooldown:
    symbol: str
    reason: str           # SL_HIT / NARRATIVE_FAIL / MANUAL
    cooldown_until: float
    loss_count: int = 1


@dataclass
class NarrativeCooldown:
    narrative: str
    failure_count: int
    cooldown_until: float


class StateManager:
    """
    Tracks all runtime state.
    Critical state is persisted to Redis every 60s and on each trade close.
    Non-critical state (websocket stats, etc.) is in-memory only.
    """

    def __init__(self):
        self._redis = None

        # ── Risk state (MUST be persisted) ──
        self.daily_pnl: float = 0.0
        self.daily_loss_pct: float = 0.0
        self.weekly_loss_pct: float = 0.0
        self.monthly_loss_pct: float = 0.0
        self.consecutive_losses: int = 0
        self.consecutive_wins: int = 0
        self.daily_trades: int = 0
        self.drawdown_peak: float = 0.0
        self.current_drawdown: float = 0.0

        # Cooldowns
        self._trading_pause_until: float = 0.0
        self._pair_cooldowns: Dict[str, PairCooldown] = {}
        self._narrative_cooldowns: Dict[str, NarrativeCooldown] = {}

        # Active signals (non-persisted — rebuilt from Telegram history if needed)
        self._active_signals: Dict[str, object] = {}

        # Trade history (persisted, rolling window)
        self._closed_trades: List[TradeOutcome] = []
        self._MAX_CLOSED_TRADES: int = 500

        # Rotation stats (non-critical)
        self._universe_history: List[List[str]] = []
        self._attention_history: Dict[str, List[float]] = {}

        # Day tracking
        self._day_start_ts: float = self._get_day_start()
        self._week_start_ts: float = self._get_week_start()

        # Persistence
        self._last_persist: float = 0.0
        self._persist_interval: float = 60.0
        self._shutdown = asyncio.Event()
        self._lock = asyncio.Lock()

    # ═══════════════════════════════════════════
    # REDIS SETUP
    # ═══════════════════════════════════════════

    async def connect_redis(self, redis_url: str):
        if not redis_url:
            log.warning("No REDIS_URL — state will NOT persist across restarts")
            return
        try:
            import aioredis
            self._redis = await aioredis.from_url(redis_url, decode_responses=True)
            await self._redis.ping()
            log.info("Redis connected — state persistence enabled")
        except ImportError:
            log.warning("aioredis not installed — no persistence")
        except Exception as e:
            log.warning(f"Redis connection failed: {e} — running without persistence")

    # ═══════════════════════════════════════════
    # STARTUP STATE RESTORE
    # ═══════════════════════════════════════════

    async def restore_on_startup(self):
        """
        Restore critical risk state from Redis after restart.
        Only restores if state is fresh (< 1 hour old).
        """
        if not self._redis:
            return

        try:
            raw = await self._redis.get("scalper_v4:state")
            if not raw:
                log.info("No saved state found — starting fresh")
                return

            data = json.loads(raw)
            saved_at = data.get("saved_at", 0)
            age_hours = (time.time() - saved_at) / 3600

            if age_hours > 2.0:
                log.info(f"Saved state is {age_hours:.1f}h old — too stale, starting fresh")
                return

            # Restore risk counters
            self.daily_pnl = data.get("daily_pnl", 0.0)
            self.daily_loss_pct = data.get("daily_loss_pct", 0.0)
            self.weekly_loss_pct = data.get("weekly_loss_pct", 0.0)
            self.monthly_loss_pct = data.get("monthly_loss_pct", 0.0)
            self.consecutive_losses = data.get("consecutive_losses", 0)
            self.consecutive_wins = data.get("consecutive_wins", 0)
            self.daily_trades = data.get("daily_trades", 0)
            self.drawdown_peak = data.get("drawdown_peak", 0.0)
            self.current_drawdown = data.get("current_drawdown", 0.0)
            self._trading_pause_until = data.get("trading_pause_until", 0.0)

            # Restore pair cooldowns
            for sym, cd_data in data.get("pair_cooldowns", {}).items():
                if cd_data["cooldown_until"] > time.time():
                    self._pair_cooldowns[sym] = PairCooldown(**cd_data)

            # Restore narrative cooldowns
            for narr, cd_data in data.get("narrative_cooldowns", {}).items():
                if cd_data["cooldown_until"] > time.time():
                    self._narrative_cooldowns[narr] = NarrativeCooldown(**cd_data)

            # Restore recent trades (for ML training)
            for t in data.get("recent_trades", []):
                try:
                    self._closed_trades.append(TradeOutcome(**t))
                except Exception:
                    pass

            log.info(
                f"State restored: daily_pnl={self.daily_pnl:.2f}, "
                f"cons_losses={self.consecutive_losses}, "
                f"pair_cooldowns={len(self._pair_cooldowns)}, "
                f"state_age={age_hours:.1f}h"
            )

        except Exception as e:
            log.warning(f"State restore error: {e} — starting fresh")

    # ═══════════════════════════════════════════
    # STATE PERSISTENCE
    # ═══════════════════════════════════════════

    async def _persist(self):
        """Save critical state to Redis."""
        if not self._redis:
            return
        try:
            data = {
                "saved_at": time.time(),
                "daily_pnl": self.daily_pnl,
                "daily_loss_pct": self.daily_loss_pct,
                "weekly_loss_pct": self.weekly_loss_pct,
                "monthly_loss_pct": self.monthly_loss_pct,
                "consecutive_losses": self.consecutive_losses,
                "consecutive_wins": self.consecutive_wins,
                "daily_trades": self.daily_trades,
                "drawdown_peak": self.drawdown_peak,
                "current_drawdown": self.current_drawdown,
                "trading_pause_until": self._trading_pause_until,
                "pair_cooldowns": {
                    sym: asdict(cd)
                    for sym, cd in self._pair_cooldowns.items()
                    if cd.cooldown_until > time.time()
                },
                "narrative_cooldowns": {
                    narr: asdict(cd)
                    for narr, cd in self._narrative_cooldowns.items()
                    if cd.cooldown_until > time.time()
                },
                "recent_trades": [
                    asdict(t) for t in self._closed_trades[-50:]
                ],
            }
            await self._redis.set(
                "scalper_v4:state",
                json.dumps(data),
                ex=86400,  # 24h TTL
            )
            self._last_persist = time.time()
        except Exception as e:
            log.warning(f"State persist error: {e}")

    async def run(self):
        """Background persistence loop."""
        while not self._shutdown.is_set():
            try:
                if time.time() - self._last_persist >= self._persist_interval:
                    await self._persist()
                self._check_day_reset()
            except Exception as e:
                log.warning(f"State run error: {e}")
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass

    # ═══════════════════════════════════════════
    # TRADE RECORDING
    # ═══════════════════════════════════════════

    async def record_trade_close(self, outcome: TradeOutcome):
        """Record closed trade. Updates all risk counters. Persists immediately."""
        async with self._lock:
            self._closed_trades.append(outcome)
            if len(self._closed_trades) > self._MAX_CLOSED_TRADES:
                self._closed_trades = self._closed_trades[-self._MAX_CLOSED_TRADES:]

            # Update PnL
            self.daily_pnl += outcome.pnl_pct
            self.daily_trades += 1

            if outcome.pnl_pct < 0:
                loss_abs = abs(outcome.pnl_pct)
                self.daily_loss_pct += loss_abs
                self.weekly_loss_pct += loss_abs
                self.monthly_loss_pct += loss_abs
                self.consecutive_losses += 1
                self.consecutive_wins = 0

                # Pair cooldown on SL hit
                if outcome.outcome == "SL":
                    await self._apply_pair_cooldown(
                        outcome.symbol, "SL_HIT", outcome.narrative
                    )
            else:
                self.consecutive_wins += 1
                self.consecutive_losses = 0

            # Drawdown tracking
            if self.daily_pnl < 0:
                self.current_drawdown = abs(self.daily_pnl)
                if self.current_drawdown > self.drawdown_peak:
                    self.drawdown_peak = self.current_drawdown

            # Apply trading pause based on consecutive losses
            rp = config.get_risk_profile()
            if self.consecutive_losses >= 4:
                pause = rp["cooldown_4loss_min"] * 60
                self._trading_pause_until = time.time() + pause
                log.warning(
                    f"4 consecutive losses — pausing trading for {rp['cooldown_4loss_min']}min"
                )
            elif self.consecutive_losses == 3:
                pause = rp["cooldown_3loss_min"] * 60
                self._trading_pause_until = time.time() + pause
            elif self.consecutive_losses == 2:
                pause = rp["cooldown_2loss_min"] * 60
                self._trading_pause_until = time.time() + pause

        # Persist immediately after trade close (critical)
        await self._persist()

    async def _apply_pair_cooldown(
        self, symbol: str, reason: str, narrative: str = ""
    ):
        """Apply cooldown to specific pair after SL hit."""
        rp = config.get_risk_profile()
        existing = self._pair_cooldowns.get(symbol)
        loss_count = (existing.loss_count + 1) if existing else 1

        # Longer cooldown for repeated failures
        if loss_count >= 3:
            cooldown_min = 120
        elif loss_count >= 2:
            cooldown_min = 60
        else:
            cooldown_min = 30

        self._pair_cooldowns[symbol] = PairCooldown(
            symbol=symbol,
            reason=reason,
            cooldown_until=time.time() + cooldown_min * 60,
            loss_count=loss_count,
        )
        log.info(f"Pair cooldown: {symbol} for {cooldown_min}min (loss #{loss_count})")

        # Narrative cooldown if repeated failures in same category
        if narrative:
            narr_cd = self._narrative_cooldowns.get(narrative)
            fails = (narr_cd.failure_count + 1) if narr_cd else 1
            if fails >= 2:
                narr_min = rp["narrative_cooldown_min"]
                self._narrative_cooldowns[narrative] = NarrativeCooldown(
                    narrative=narrative,
                    failure_count=fails,
                    cooldown_until=time.time() + narr_min * 60,
                )
                log.info(
                    f"Narrative cooldown: {narrative} for {narr_min}min "
                    f"({fails} failures)"
                )

    # ═══════════════════════════════════════════
    # RISK CHECKS
    # ═══════════════════════════════════════════

    def can_trade(
        self, symbol: str = None, narrative: str = None
    ) -> Tuple[bool, str]:
        """
        Primary risk gate. Returns (can_trade, reason).
        Called before any signal is generated.
        """
        now = time.time()
        rp = config.get_risk_profile()

        # Global pause
        if self._trading_pause_until > now:
            remaining = (self._trading_pause_until - now) / 60
            return False, f"trading_paused_{remaining:.0f}min"

        # Daily loss limit
        if self.daily_loss_pct >= rp["max_daily_loss"]:
            return False, f"daily_loss_limit_{self.daily_loss_pct*100:.1f}%"

        # Weekly loss limit
        if self.weekly_loss_pct >= rp["max_weekly_loss"]:
            return False, f"weekly_loss_limit_{self.weekly_loss_pct*100:.1f}%"

        # Monthly absolute limit
        if self.monthly_loss_pct >= config.ABS_MAX_MONTHLY_LOSS:
            return False, "monthly_absolute_limit"

        # Drawdown emergency
        if self.current_drawdown >= config.DRAWDOWN_EMERGENCY:
            return False, f"drawdown_emergency_{self.current_drawdown*100:.1f}%"

        # Pair cooldown
        if symbol:
            cd = self._pair_cooldowns.get(symbol)
            if cd and cd.cooldown_until > now:
                remaining = (cd.cooldown_until - now) / 60
                return False, f"pair_cooldown_{symbol}_{remaining:.0f}min"

        # Narrative cooldown
        if narrative:
            cd = self._narrative_cooldowns.get(narrative)
            if cd and cd.cooldown_until > now:
                remaining = (cd.cooldown_until - now) / 60
                return False, f"narrative_cooldown_{narrative}_{remaining:.0f}min"

        return True, "ok"

    def get_size_multiplier(self) -> float:
        """Returns current position size multiplier based on drawdown state."""
        if self.consecutive_losses >= 3:
            return config.SIZE_REDUCTION_3
        elif self.consecutive_losses >= 2:
            return config.SIZE_REDUCTION_2

        if self.current_drawdown >= config.DRAWDOWN_CRITICAL:
            return 0.25
        elif self.current_drawdown >= config.DRAWDOWN_SERIOUS:
            return 0.50
        elif self.current_drawdown >= config.DRAWDOWN_WARNING:
            return 0.75

        return 1.0

    def get_active_positions_count(self) -> int:
        return len(self._active_signals)

    def add_active_signal(self, symbol: str, signal):
        self._active_signals[symbol] = signal

    def remove_active_signal(self, symbol: str):
        self._active_signals.pop(symbol, None)

    def has_active_signal(self, symbol: str) -> bool:
        return symbol in self._active_signals

    # ═══════════════════════════════════════════
    # ML DATA ACCESS
    # ═══════════════════════════════════════════

    def get_last_n_closed(self, n: int = 50) -> List[TradeOutcome]:
        return list(self._closed_trades[-n:])

    def get_win_rate(self, n: int = 50) -> float:
        trades = self.get_last_n_closed(n)
        if not trades:
            return 0.5
        wins = sum(1 for t in trades if t.pnl_pct > 0)
        return wins / len(trades)

    def get_avg_rr(self, n: int = 50) -> float:
        trades = self.get_last_n_closed(n)
        if not trades:
            return 1.0
        return sum(t.pnl_r for t in trades) / len(trades)

    # ═══════════════════════════════════════════
    # STATUS SUMMARY
    # ═══════════════════════════════════════════

    def get_status_summary(self) -> dict:
        rp = config.get_risk_profile()
        return {
            "daily_pnl": f"{self.daily_pnl*100:+.2f}%",
            "daily_loss": f"{self.daily_loss_pct*100:.2f}% / {rp['max_daily_loss']*100:.0f}%",
            "weekly_loss": f"{self.weekly_loss_pct*100:.2f}% / {rp['max_weekly_loss']*100:.0f}%",
            "consecutive_losses": self.consecutive_losses,
            "active_signals": len(self._active_signals),
            "size_mult": f"{self.get_size_multiplier():.0%}",
            "pair_cooldowns": len([
                c for c in self._pair_cooldowns.values()
                if c.cooldown_until > time.time()
            ]),
            "narrative_cooldowns": len([
                c for c in self._narrative_cooldowns.values()
                if c.cooldown_until > time.time()
            ]),
            "trading_paused": self._trading_pause_until > time.time(),
            "drawdown": f"{self.current_drawdown*100:.2f}%",
        }

    # ═══════════════════════════════════════════
    # DAY RESET
    # ═══════════════════════════════════════════

    def _check_day_reset(self):
        now = time.time()
        day_start = self._get_day_start()
        if day_start > self._day_start_ts:
            log.info("New UTC day — resetting daily counters")
            self.daily_pnl = 0.0
            self.daily_loss_pct = 0.0
            self.daily_trades = 0
            self.consecutive_losses = 0
            self.consecutive_wins = 0
            self.current_drawdown = 0.0
            self._day_start_ts = day_start

        week_start = self._get_week_start()
        if week_start > self._week_start_ts:
            log.info("New UTC week — resetting weekly counters")
            self.weekly_loss_pct = 0.0
            self._week_start_ts = week_start

    @staticmethod
    def _get_day_start() -> float:
        now = datetime.now(timezone.utc)
        return now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

    @staticmethod
    def _get_week_start() -> float:
        now = datetime.now(timezone.utc)
        days_since_monday = now.weekday()
        monday = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return (monday.timestamp() - days_since_monday * 86400)

    async def close(self):
        self._shutdown.set()
        await self._persist()  # Final save on shutdown
