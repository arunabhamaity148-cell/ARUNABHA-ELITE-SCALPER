"""
ARUNABHA ELITE SCALPER v3.0
FILE 17/18: events_calendar.py
Economic calendar filter — detect high-impact events (FOMC, CPI, NFP)
Reduce position size PRE_EVENT_MINUTES before major events
Uses free investing.com / forexfactory scraping (no API key needed)
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

import aiohttp

import config

log = logging.getLogger("elite.events")

KNOWN_HIGH_IMPACT: List[str] = [
    "FOMC", "Federal Reserve", "Interest Rate", "CPI", "Inflation",
    "NFP", "Non-Farm", "GDP", "Unemployment", "PPI",
    "Powell", "Yellen", "ECB", "Bank of England", "BOJ",
    "Flash Crash", "Circuit Breaker",
]


@dataclass
class EconomicEvent:
    title: str
    impact: str           # "HIGH", "MEDIUM", "LOW"
    event_time_utc: float
    currency: str = "USD"
    actual: Optional[str] = None
    forecast: Optional[str] = None


class EventsCalendar:
    def __init__(self):
        self._events: List[EconomicEvent] = []
        self._last_fetch: float = 0.0
        self._fetch_interval: int = 3600  # refresh hourly
        self._session: Optional[aiohttp.ClientSession] = None

    # ═══════════════════════════════════════════
    # MAIN CHECK
    # ═══════════════════════════════════════════

    def is_pre_event(self) -> tuple[bool, Optional[EconomicEvent]]:
        """
        Returns (is_pre_event, event) if a high-impact event is within
        PRE_EVENT_MINUTES in either direction.
        """
        now = time.time()
        window = config.PRE_EVENT_MINUTES * 60

        for event in self._events:
            if event.impact != "HIGH":
                continue
            time_to_event = event.event_time_utc - now
            # Pre-event window: [now, now + window]
            # Post-event window: [now - 5min, now] (still volatile)
            if -300 <= time_to_event <= window:
                return True, event

        return False, None

    def get_size_adjustment(self) -> float:
        """Return size multiplier (1.0 = normal, 0.5 = pre-event reduced)."""
        pre_event, event = self.is_pre_event()
        if pre_event:
            log.info(f"Pre-event size reduction: {event.title if event else 'unknown'}")
            return config.PRE_EVENT_REDUCTION
        return 1.0

    # ═══════════════════════════════════════════
    # EVENT FETCHING
    # ═══════════════════════════════════════════

    async def refresh(self):
        """Fetch upcoming events. Called periodically from monitoring."""
        now = time.time()
        if now - self._last_fetch < self._fetch_interval:
            return

        try:
            await self._fetch_events()
            self._last_fetch = now
        except Exception as e:
            log.debug(f"Events fetch error: {e}")

    async def _fetch_events(self):
        """
        Fetch from a free economic calendar.
        Primary: investing.com (scrape-friendly endpoint)
        Fallback: hardcoded known schedule
        """
        try:
            if not self._session:
                self._session = aiohttp.ClientSession(
                    headers={"User-Agent": "Mozilla/5.0 (compatible; bot/1.0)"}
                )
            # Try to get events from a simple JSON endpoint
            url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    self._parse_forex_factory(data)
                    log.info(f"Fetched {len(self._events)} economic events")
                    return
        except Exception as e:
            log.debug(f"FF calendar fetch: {e}")

        # Fallback: inject known recurring events (approximate times)
        self._inject_known_events()

    def _parse_forex_factory(self, data: list):
        """Parse ForexFactory JSON calendar format."""
        events = []
        now = time.time()

        for item in data:
            try:
                impact = item.get("impact", "").upper()
                if impact not in ("HIGH", "MEDIUM"):
                    continue

                title = item.get("title", "")
                currency = item.get("country", "USD")
                date_str = item.get("date", "")
                time_str = item.get("time", "12:00am")

                # Parse datetime (FF uses format like "Jan 15 2025" and "8:30am")
                try:
                    dt_str = f"{date_str} {time_str}"
                    dt = datetime.strptime(dt_str, "%b %d %Y %I:%M%p")
                    dt = dt.replace(tzinfo=timezone.utc)
                    event_ts = dt.timestamp()
                except ValueError:
                    continue

                # Only future events (within 7 days)
                if event_ts < now - 3600 or event_ts > now + 86400 * 7:
                    continue

                events.append(EconomicEvent(
                    title=title,
                    impact=impact,
                    event_time_utc=event_ts,
                    currency=currency,
                ))
            except Exception:
                continue

        if events:
            self._events = sorted(events, key=lambda e: e.event_time_utc)

    def _inject_known_events(self):
        """
        Inject known recurring high-impact events for current week.
        Approximate times — better than nothing when API fails.
        """
        now = time.time()
        nowdt = datetime.fromtimestamp(now, tz=timezone.utc)
        week_day = nowdt.weekday()  # 0=Monday

        # FOMC (8 per year, Wednesday at 14:00 UTC, approximate)
        # NFP (First Friday of month at 12:30 UTC)
        # CPI (second Tuesday of month at 12:30 UTC)

        events = []

        # Check for Friday NFP (rough: any Friday around 12:30 UTC)
        days_to_friday = (4 - week_day) % 7
        if days_to_friday == 0:
            # Today is Friday
            target_ts = nowdt.replace(hour=12, minute=30, second=0, microsecond=0).timestamp()
            if target_ts > now:
                events.append(EconomicEvent(
                    title="Potential NFP / US Jobs Data",
                    impact="HIGH",
                    event_time_utc=target_ts,
                    currency="USD",
                ))

        self._events.extend(events)

    # ═══════════════════════════════════════════
    # KEYWORD DETECTION
    # ═══════════════════════════════════════════

    def has_high_impact_keyword(self, title: str) -> bool:
        title_upper = title.upper()
        return any(kw.upper() in title_upper for kw in KNOWN_HIGH_IMPACT)

    # ═══════════════════════════════════════════
    # STATUS
    # ═══════════════════════════════════════════

    def get_upcoming(self, hours: int = 24) -> List[EconomicEvent]:
        now = time.time()
        cutoff = now + hours * 3600
        return [e for e in self._events if now <= e.event_time_utc <= cutoff]

    def format_upcoming_text(self) -> str:
        upcoming = self.get_upcoming(hours=12)
        if not upcoming:
            return "No high-impact events in next 12h"

        lines = ["📅 <b>Upcoming Events (12h)</b>"]
        for ev in upcoming[:5]:
            dt = datetime.fromtimestamp(ev.event_time_utc, tz=timezone.utc)
            ist_h = (dt.hour + 5) % 24
            ist_m = (dt.minute + 30) % 60
            ist_h += (dt.minute + 30) // 60
            impact_emoji = "🔴" if ev.impact == "HIGH" else "🟡"
            lines.append(
                f"{impact_emoji} {ev.title} — {ist_h:02d}:{ist_m:02d} IST ({ev.currency})"
            )
        return "\n".join(lines)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
