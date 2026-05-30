"""
ARUNABHA MANUAL SCALPER v4.0
FILE: news_guard_engine.py
4-Layer News Guard — replaces events_calendar.py

Layer 1: Macro guard (FOMC, CPI, NFP, Powell, etc.)
Layer 2: Crypto event guard (token unlock, listing, fork, hack)
Layer 3: Pair-specific catalyst guard
Layer 4: Exchange outage / abnormal event guard

States: HARD_BLOCK | SOFT_PENALTY | COOLDOWN | REACTION_MODE | CLEAR
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Tuple

import aiohttp

import config

log = logging.getLogger("scalper.newsguard")


class GuardState(Enum):
    CLEAR = "CLEAR"
    SOFT_PENALTY = "SOFT_PENALTY"    # 50% size, can trade
    HARD_BLOCK = "HARD_BLOCK"        # no new trades
    COOLDOWN = "COOLDOWN"            # post-event, no trades yet
    REACTION_MODE = "REACTION_MODE"  # post-cooldown, trade reactions only
    UNCERTAINTY = "UNCERTAINTY"      # unclear macro state, reduce size


@dataclass
class NewsEvent:
    title: str
    layer: int                    # 1=macro, 2=crypto, 3=pair, 4=exchange
    impact: str                   # HIGH / MEDIUM / LOW
    event_time_utc: float
    currency: str = "USD"
    affected_pairs: List[str] = field(default_factory=list)
    source: str = ""
    actual: Optional[str] = None
    forecast: Optional[str] = None
    is_past: bool = False


@dataclass
class GuardResult:
    state: GuardState
    reason: str
    event: Optional[NewsEvent] = None
    size_multiplier: float = 1.0    # 1.0=normal, 0.5=reduced, 0.0=blocked
    minutes_until_clear: float = 0.0
    pair_specific: bool = False
    allow_reaction_trades: bool = False

    @property
    def can_trade(self) -> bool:
        return self.state not in (GuardState.HARD_BLOCK, GuardState.COOLDOWN)

    @property
    def is_fully_clear(self) -> bool:
        return self.state == GuardState.CLEAR


class NewsGuardEngine:
    """
    4-layer news guard. Checks every trade before generation.
    Uses CoinMarketCal (free key) + hardcoded known schedule
    + live scraping fallbacks.
    """

    def __init__(self):
        self._events: List[NewsEvent] = []
        self._pair_events: Dict[str, List[NewsEvent]] = {}   # sym → events
        self._last_macro_fetch: float = 0.0
        self._last_crypto_fetch: float = 0.0
        self._global_state: GuardState = GuardState.CLEAR
        self._global_event: Optional[NewsEvent] = None
        self._post_event_ts: float = 0.0   # when last event ended
        self._session: Optional[aiohttp.ClientSession] = None
        self._shutdown = asyncio.Event()

    # ═══════════════════════════════════════════
    # PUBLIC API — MAIN CHECK
    # ═══════════════════════════════════════════

    def check(self, symbol: Optional[str] = None) -> GuardResult:
        """
        Primary gate — call before generating any signal.
        Returns GuardResult with state, size_multiplier, reason.
        """
        now = time.time()

        # Layer 1: Macro check (global — affects all pairs)
        macro_result = self._check_macro(now)
        if macro_result.state == GuardState.HARD_BLOCK:
            return macro_result

        # Layer 2: Crypto-specific events (global)
        crypto_result = self._check_crypto_events(now)
        if crypto_result.state == GuardState.HARD_BLOCK:
            return crypto_result

        # Layer 3: Pair-specific check
        if symbol:
            pair_result = self._check_pair_specific(symbol, now)
            if pair_result.state == GuardState.HARD_BLOCK:
                return pair_result
            if pair_result.state == GuardState.SOFT_PENALTY:
                # Combine with macro soft penalty if any
                combined_mult = pair_result.size_multiplier
                if macro_result.state == GuardState.SOFT_PENALTY:
                    combined_mult *= macro_result.size_multiplier
                return GuardResult(
                    state=GuardState.SOFT_PENALTY,
                    reason=pair_result.reason,
                    size_multiplier=combined_mult,
                    pair_specific=True,
                    event=pair_result.event,
                )

        # Layer 4: Post-event cooldown / reaction mode
        if self._post_event_ts > 0:
            elapsed_since_event = now - self._post_event_ts
            cooldown_secs = config.CRYPTO_EVENT_COOLDOWN_MIN * 60
            reaction_secs = cooldown_secs + config.POST_EVENT_REACTION_WINDOW_MIN * 60

            if elapsed_since_event < cooldown_secs:
                remaining = (cooldown_secs - elapsed_since_event) / 60
                return GuardResult(
                    state=GuardState.COOLDOWN,
                    reason=f"Post-event cooldown: {remaining:.0f}min remaining",
                    size_multiplier=0.0,
                    minutes_until_clear=remaining,
                )
            elif elapsed_since_event < reaction_secs:
                return GuardResult(
                    state=GuardState.REACTION_MODE,
                    reason="Post-event reaction window — reduced size",
                    size_multiplier=0.70,
                    allow_reaction_trades=True,
                    minutes_until_clear=0.0,
                )

        # If macro soft penalty
        if macro_result.state == GuardState.SOFT_PENALTY:
            return macro_result

        return GuardResult(
            state=GuardState.CLEAR,
            reason="clear",
            size_multiplier=1.0,
        )

    def mark_event_occurred(self, event: NewsEvent):
        """Call when a high-impact event has just happened."""
        self._post_event_ts = time.time()
        log.info(f"NewsGuard: post-event cooldown started for: {event.title}")

    def get_next_high_impact_event(self) -> Optional[NewsEvent]:
        """Returns the next upcoming HIGH impact event."""
        now = time.time()
        upcoming = [
            e for e in self._events
            if e.impact == "HIGH" and e.event_time_utc > now
        ]
        if not upcoming:
            return None
        return min(upcoming, key=lambda e: e.event_time_utc)

    def get_pair_event_risk(self, symbol: str) -> float:
        """Returns event risk score 0.0-1.0 for a specific pair."""
        now = time.time()
        events = self._pair_events.get(symbol, [])
        risk = 0.0
        for e in events:
            time_to_event = e.event_time_utc - now
            window = config.CRYPTO_EVENT_HARD_BLOCK_MIN * 60
            if -300 <= time_to_event <= window * 3:
                if e.impact == "HIGH":
                    risk = max(risk, 0.9)
                elif e.impact == "MEDIUM":
                    risk = max(risk, 0.5)
        return risk

    # ═══════════════════════════════════════════
    # LAYER 1: MACRO CHECK
    # ═══════════════════════════════════════════

    def _check_macro(self, now: float) -> GuardResult:
        for event in self._events:
            if event.layer != 1 or event.impact != "HIGH":
                continue

            time_to_event = event.event_time_utc - now
            hard_block_secs = config.MACRO_HARD_BLOCK_BEFORE_MIN * 60
            soft_secs = config.MACRO_SOFT_PENALTY_BEFORE_MIN * 60
            after_secs = config.MACRO_HARD_BLOCK_AFTER_MIN * 60

            # Hard block window: X min before to Y min after
            if -after_secs <= time_to_event <= hard_block_secs:
                mins_left = time_to_event / 60
                direction = "before" if time_to_event > 0 else "after"
                return GuardResult(
                    state=GuardState.HARD_BLOCK,
                    reason=f"MACRO HARD BLOCK: {event.title} in {abs(mins_left):.0f}min ({direction})",
                    size_multiplier=0.0,
                    event=event,
                    minutes_until_clear=(
                        (time_to_event + after_secs) / 60
                        if time_to_event > 0
                        else (after_secs + time_to_event) / 60
                    ),
                )

            # Soft penalty window: before hard block
            if hard_block_secs < time_to_event <= soft_secs:
                return GuardResult(
                    state=GuardState.SOFT_PENALTY,
                    reason=f"Macro soft window: {event.title} in {time_to_event/60:.0f}min",
                    size_multiplier=0.60,
                    event=event,
                )

        return GuardResult(state=GuardState.CLEAR, reason="macro_clear", size_multiplier=1.0)

    # ═══════════════════════════════════════════
    # LAYER 2: CRYPTO EVENT CHECK
    # ═══════════════════════════════════════════

    def _check_crypto_events(self, now: float) -> GuardResult:
        for event in self._events:
            if event.layer != 2 or event.impact not in ("HIGH", "MEDIUM"):
                continue

            time_to_event = event.event_time_utc - now
            block_secs = config.CRYPTO_EVENT_HARD_BLOCK_MIN * 60

            if -300 <= time_to_event <= block_secs:
                return GuardResult(
                    state=GuardState.HARD_BLOCK,
                    reason=f"CRYPTO EVENT BLOCK: {event.title}",
                    size_multiplier=0.0,
                    event=event,
                    minutes_until_clear=max(0, time_to_event / 60 + 5),
                )

        return GuardResult(state=GuardState.CLEAR, reason="crypto_clear", size_multiplier=1.0)

    # ═══════════════════════════════════════════
    # LAYER 3: PAIR-SPECIFIC CHECK
    # ═══════════════════════════════════════════

    def _check_pair_specific(self, symbol: str, now: float) -> GuardResult:
        events = self._pair_events.get(symbol, [])
        for event in events:
            time_to_event = event.event_time_utc - now
            block_secs = config.CRYPTO_EVENT_HARD_BLOCK_MIN * 60

            if -600 <= time_to_event <= block_secs:
                severity = "BLOCK" if event.impact == "HIGH" else "CAUTION"
                mult = 0.0 if event.impact == "HIGH" else 0.50
                state = (
                    GuardState.HARD_BLOCK
                    if event.impact == "HIGH"
                    else GuardState.SOFT_PENALTY
                )
                return GuardResult(
                    state=state,
                    reason=f"PAIR EVENT {severity}: {event.title} for {symbol}",
                    size_multiplier=mult,
                    event=event,
                    pair_specific=True,
                )

        return GuardResult(state=GuardState.CLEAR, reason="pair_clear", size_multiplier=1.0)

    # ═══════════════════════════════════════════
    # DATA FETCHING
    # ═══════════════════════════════════════════

    async def refresh(self):
        """Full refresh of all event sources."""
        now = time.time()
        try:
            session = await self._get_session()

            # Macro events (refresh every 2h)
            if now - self._last_macro_fetch > 7200:
                macro_events = await self._fetch_macro_events(session)
                # Remove old macro events and add new
                self._events = [e for e in self._events if e.layer != 1]
                self._events.extend(macro_events)
                self._last_macro_fetch = now
                log.info(f"NewsGuard: fetched {len(macro_events)} macro events")

            # Crypto events (refresh every 1h)
            if now - self._last_crypto_fetch > 3600:
                crypto_events = await self._fetch_crypto_events(session)
                self._events = [e for e in self._events if e.layer != 2]
                self._events.extend(crypto_events)
                self._last_crypto_fetch = now
                log.info(f"NewsGuard: fetched {len(crypto_events)} crypto events")

            # Clean up past events (older than 2h)
            cutoff = now - 7200
            self._events = [e for e in self._events if e.event_time_utc > cutoff]

        except Exception as e:
            log.warning(f"NewsGuard refresh error: {e}")

    async def _fetch_macro_events(
        self, session: aiohttp.ClientSession
    ) -> List[NewsEvent]:
        """
        Fetch macro economic events.
        Uses hardcoded known schedule + optional TradingEconomics.
        Free fallback: hardcoded FOMC/CPI schedule.
        """
        events = []

        # Try CoinMarketCal for macro-tagged crypto events
        if config.COINMARKETCAL_API_KEY:
            try:
                url = f"{config.COINMARKETCAL_BASE}/events"
                now_dt = datetime.now(timezone.utc)
                params = {
                    "max": 20,
                    "dateRangeStart": now_dt.strftime("%Y-%m-%d"),
                    "dateRangeEnd": now_dt.strftime("%Y-%m-%d"),
                    "significance": "3",
                }
                headers = {"x-api-key": config.COINMARKETCAL_API_KEY}
                async with session.get(
                    url, params=params, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        for item in data.get("body", []):
                            title = item.get("title", {}).get("en", "")
                            if any(
                                kw.lower() in title.lower()
                                for kw in config.HIGH_IMPACT_MACRO_EVENTS
                            ):
                                # Parse date
                                date_str = item.get("date_event", "")
                                try:
                                    from datetime import datetime as dt
                                    evt_ts = dt.fromisoformat(
                                        date_str.replace("Z", "+00:00")
                                    ).timestamp()
                                except Exception:
                                    continue

                                events.append(NewsEvent(
                                    title=title,
                                    layer=1,
                                    impact="HIGH",
                                    event_time_utc=evt_ts,
                                    source="CoinMarketCal",
                                ))
            except Exception as e:
                log.debug(f"CoinMarketCal macro fetch error: {e}")

        # Inject known FOMC dates (hardcoded 2025-2026 schedule)
        # These are fixed Fed announcement dates — always block
        known_fomc = self._get_known_fomc_dates()
        events.extend(known_fomc)

        return events

    async def _fetch_crypto_events(
        self, session: aiohttp.ClientSession
    ) -> List[NewsEvent]:
        """
        Fetch crypto-specific events from CoinMarketCal.
        Includes token unlocks, listings, forks, mainnet launches.
        """
        events = []
        self._pair_events = {}  # Reset pair events

        if not config.COINMARKETCAL_API_KEY:
            log.debug("No CoinMarketCal key — skipping crypto event fetch")
            return events

        try:
            url = f"{config.COINMARKETCAL_BASE}/events"
            now_dt = datetime.now(timezone.utc)
            params = {
                "max": 50,
                "dateRangeStart": now_dt.strftime("%Y-%m-%d"),
                "dateRangeEnd": now_dt.strftime("%Y-%m-%d"),
            }
            headers = {"x-api-key": config.COINMARKETCAL_API_KEY}

            async with session.get(
                url, params=params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status != 200:
                    return events
                data = await r.json()

                for item in data.get("body", []):
                    title = item.get("title", {}).get("en", "")
                    coins = item.get("coins", [])
                    date_str = item.get("date_event", "")
                    is_verified = item.get("percentage", 0) >= 70

                    # Only high-confidence events
                    if not is_verified:
                        continue

                    # Check if it's a high-impact crypto event
                    is_high = any(
                        kw.lower() in title.lower()
                        for kw in config.HIGH_IMPACT_CRYPTO_EVENTS
                    )
                    if not is_high:
                        continue

                    # Determine impact
                    impact = "HIGH" if any(
                        kw in title.lower()
                        for kw in ["unlock", "listing", "delisting", "hack", "fork", "mainnet"]
                    ) else "MEDIUM"

                    try:
                        from datetime import datetime as dt
                        evt_ts = dt.fromisoformat(
                            date_str.replace("Z", "+00:00")
                        ).timestamp()
                    except Exception:
                        continue

                    # Get affected pairs
                    affected = []
                    for coin in coins:
                        sym_raw = coin.get("symbol", "").upper()
                        binance_sym = f"{sym_raw}USDT"
                        affected.append(binance_sym)

                    event = NewsEvent(
                        title=title,
                        layer=2,
                        impact=impact,
                        event_time_utc=evt_ts,
                        affected_pairs=affected,
                        source="CoinMarketCal",
                    )
                    events.append(event)

                    # Index into pair-specific events
                    for sym in affected:
                        if sym not in self._pair_events:
                            self._pair_events[sym] = []
                        pair_evt = NewsEvent(
                            title=title,
                            layer=3,
                            impact=impact,
                            event_time_utc=evt_ts,
                            affected_pairs=affected,
                            source="CoinMarketCal",
                        )
                        self._pair_events[sym].append(pair_evt)

        except Exception as e:
            log.debug(f"CoinMarketCal crypto events error: {e}")

        return events

    def _get_known_fomc_dates(self) -> List[NewsEvent]:
        """
        Known FOMC meeting dates 2025-2026 (hardcoded).
        These are fixed Fed announcement dates — always layer 1 HIGH.
        Update annually.
        """
        import calendar
        # Known FOMC announcement dates (UTC 18:00 = statement release)
        fomc_dates_utc = [
            "2025-09-17 18:00",
            "2025-11-07 18:00",
            "2025-12-17 18:00",
            "2026-01-28 18:00",
            "2026-03-18 18:00",
            "2026-04-29 18:00",
            "2026-06-10 18:00",
            "2026-07-29 18:00",
            "2026-09-16 18:00",
            "2026-11-04 18:00",
            "2026-12-16 18:00",
        ]
        events = []
        now = time.time()
        for date_str in fomc_dates_utc:
            try:
                from datetime import datetime as dt
                evt_ts = dt.strptime(date_str, "%Y-%m-%d %H:%M").replace(
                    tzinfo=timezone.utc
                ).timestamp()
                # Only include upcoming (or last 2h)
                if evt_ts > now - 7200:
                    events.append(NewsEvent(
                        title="FOMC Rate Decision",
                        layer=1,
                        impact="HIGH",
                        event_time_utc=evt_ts,
                        currency="USD",
                        source="hardcoded_schedule",
                    ))