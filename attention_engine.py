"""
ARUNABHA MANUAL SCALPER v4.0
FILE: attention_engine.py
LAYER A — Attention & Derivatives Context Scoring

Scores each candidate pair on:
- Search/trending attention (CoinGecko)
- Narrative/category heat
- Hype velocity (acceleration)
- Derivatives interest (OI delta, funding context)
- Volume spike quality

Output: pair_attention_score (0-100)
Pairs with score < ATTENTION_MIN_SCORE are skipped entirely.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import aiohttp

import config
from pair_universe_engine import PairCandidate

log = logging.getLogger("scalper.attention")


@dataclass
class AttentionSnapshot:
    symbol: str
    attention_score: float = 0.0
    derivatives_score: float = 0.0
    combined_score: float = 0.0

    # Sub-scores (0-100 each, then weighted)
    trend_search_score: float = 0.0
    narrative_score: float = 0.0
    hype_velocity_score: float = 0.0
    deriv_interest_score: float = 0.0
    volume_spike_score: float = 0.0
    liquidity_quality_score: float = 0.0

    # Penalties (applied to total)
    crowding_penalty: float = 0.0
    event_risk_penalty: float = 0.0

    # Context
    oi_change_4h_pct: float = 0.0
    funding_rate: float = 0.0
    ls_ratio: float = 0.5           # long/short ratio (0-1, 0.5 = neutral)
    liq_imbalance_pct: float = 0.0  # % of liquidations on long side
    volume_vs_avg: float = 1.0

    # Narrative
    active_narrative: str = ""
    narrative_age_hours: float = 0.0  # 0 = fresh, higher = aging

    # Flags
    is_funding_trap_setup: bool = False   # crowded + price rejection
    is_squeeze_setup: bool = False        # crowded shorts + price holding
    is_momentum_setup: bool = False       # OI expanding + price accepting

    scored_at: float = 0.0


@dataclass
class DerivativesSnapshot:
    symbol: str
    oi_now: float = 0.0
    oi_4h_ago: float = 0.0
    oi_change_pct: float = 0.0
    oi_expanding: bool = False
    oi_collapsing: bool = False
    funding_rate: float = 0.0
    funding_8h_avg: float = 0.0
    funding_trend: str = "NEUTRAL"   # RISING / FALLING / NEUTRAL
    ls_ratio: float = 0.5
    ls_crowded_long: bool = False
    ls_crowded_short: bool = False
    liq_24h_long_usdt: float = 0.0
    liq_24h_short_usdt: float = 0.0
    liq_imbalance_pct: float = 0.0   # % of liq on long side
    updated_at: float = 0.0


class AttentionEngine:
    """
    Scores candidate pairs on attention + derivatives dimensions.
    Determines which pairs go to Layer B for technical scanning.
    """

    def __init__(self, pair_universe_engine):
        self._universe = pair_universe_engine
        self._session: Optional[aiohttp.ClientSession] = None
        self._attention_scores: Dict[str, AttentionSnapshot] = {}
        self._derivatives: Dict[str, DerivativesSnapshot] = {}
        self._oi_history: Dict[str, List[Tuple[float, float]]] = {}  # sym → [(ts, oi), ...]
        self._funding_history: Dict[str, List[Tuple[float, float]]] = {}
        self._shutdown = asyncio.Event()
        self._lock = asyncio.Lock()

    # ═══════════════════════════════════════════
    # PUBLIC API
    # ═══════════════════════════════════════════

    def get_attention(self, symbol: str) -> Optional[AttentionSnapshot]:
        return self._attention_scores.get(symbol)

    def get_derivatives(self, symbol: str) -> Optional[DerivativesSnapshot]:
        return self._derivatives.get(symbol)

    def get_qualified_pairs(self) -> List[str]:
        """Return pairs that pass minimum attention threshold, ranked."""
        qualified = [
            (sym, snap)
            for sym, snap in self._attention_scores.items()
            if snap.combined_score >= config.ATTENTION_MIN_SCORE
        ]
        qualified.sort(key=lambda x: x[1].combined_score, reverse=True)
        return [sym for sym, _ in qualified]

    def get_high_priority_pairs(self) -> List[str]:
        """Return pairs with attention_score >= HIGH threshold."""
        return [
            sym
            for sym, snap in self._attention_scores.items()
            if snap.combined_score >= config.ATTENTION_HIGH_SCORE
        ]

    # ═══════════════════════════════════════════
    # BACKGROUND LOOP
    # ═══════════════════════════════════════════

    async def run(self):
        log.info("AttentionEngine started")
        while not self._shutdown.is_set():
            try:
                symbols = self._universe.get_universe()
                if symbols:
                    await self._score_all(symbols)
                    qualified = len(self.get_qualified_pairs())
                    log.info(
                        f"Attention scored {len(symbols)} pairs | "
                        f"Qualified: {qualified} | "
                        f"High priority: {len(self.get_high_priority_pairs())}"
                    )
            except Exception as e:
                log.warning(f"Attention engine cycle error: {e}")

            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=120)  # every 2min
            except asyncio.TimeoutError:
                pass

    # ═══════════════════════════════════════════
    # SCORING PIPELINE
    # ═══════════════════════════════════════════

    async def _score_all(self, symbols: List[str]):
        """Score all symbols in parallel (within rate limits)."""
        session = await self._get_session()

        # Fetch derivatives data in bulk (more efficient)
        await self._bulk_fetch_derivatives(session, symbols)

        # Score each symbol
        tasks = [self._score_symbol(symbol) for symbol in symbols]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _score_symbol(self, symbol: str):
        """Compute full attention + derivatives score for one symbol."""
        candidate = self._universe.get_candidate(symbol)
        if not candidate:
            return

        snap = AttentionSnapshot(symbol=symbol)
        deriv = self._derivatives.get(symbol, DerivativesSnapshot(symbol=symbol))

        # ── Trend/Search Score (from candidate discovery data) ──
        snap.trend_search_score = self._score_trend_search(candidate)

        # ── Narrative Score ──
        snap.narrative_score, snap.active_narrative = self._score_narrative(candidate)

        # ── Hype Velocity Score ──
        snap.hype_velocity_score = self._score_hype_velocity(candidate)

        # ── Derivatives Interest Score ──
        snap.deriv_interest_score, deriv_signals = self._score_derivatives(deriv)

        # ── Volume Spike Score ──
        snap.volume_spike_score = self._score_volume_spike(candidate)

        # ── Liquidity Quality Score ──
        snap.liquidity_quality_score = self._score_liquidity_quality(candidate)

        # ── Compute Weighted Attention Score ──
        raw_attention = (
            snap.trend_search_score * config.ATTN_WEIGHT_TREND_SEARCH / 100
            + snap.narrative_score * config.ATTN_WEIGHT_NARRATIVE / 100
            + snap.hype_velocity_score * config.ATTN_WEIGHT_HYPE_VELOCITY / 100
            + snap.volume_spike_score * config.ATTN_WEIGHT_VOLUME_SPIKE / 100
            + snap.liquidity_quality_score * config.ATTN_WEIGHT_LIQUIDITY_QUALITY / 100
        )
        snap.attention_score = min(raw_attention, 100.0)

        # ── Derivatives Score (separate dimension) ──
        snap.derivatives_score = snap.deriv_interest_score * config.ATTN_WEIGHT_DERIV_INTEREST / 100

        # ── Penalties ──
        # Crowded funding penalty
        if (
            deriv.funding_rate > config.FUNDING_CROWDED
            or deriv.funding_rate < -config.FUNDING_CROWDED
        ):
            snap.crowding_penalty = config.ATTN_PENALTY_CROWDING

        # Populate derivatives context
        snap.oi_change_4h_pct = deriv.oi_change_pct
        snap.funding_rate = deriv.funding_rate
        snap.ls_ratio = deriv.ls_ratio
        snap.liq_imbalance_pct = deriv.liq_imbalance_pct
        snap.volume_vs_avg = candidate.volume_vs_avg_ratio

        # Detect signal setups
        snap.is_funding_trap_setup = (
            deriv.ls_crowded_long
            and candidate.price_change_1h_pct < -1.0  # price rejecting
            and deriv.oi_collapsing
        )
        snap.is_squeeze_setup = (
            deriv.ls_crowded_short
            and candidate.price_change_1h_pct > 0.5   # price holding/rising
            and deriv.oi_expanding
        )
        snap.is_momentum_setup = (
            deriv.oi_expanding
            and abs(candidate.price_change_1h_pct) > 1.5  # price moving
            and not deriv.ls_crowded_long and not deriv.ls_crowded_short
        )

        # ── Combined Score ──
        snap.combined_score = max(
            0.0,
            snap.attention_score + snap.derivatives_score
            - snap.crowding_penalty - snap.event_risk_penalty,
        )
        snap.scored_at = time.time()

        self._attention_scores[symbol] = snap

    # ═══════════════════════════════════════════
    # SUB-SCORERS
    # ═══════════════════════════════════════════

    def _score_trend_search(self, cand: PairCandidate) -> float:
        """CoinGecko trending rank → score 0-100."""
        if cand.cg_trending_rank <= 3:
            return 100.0
        elif cand.cg_trending_rank <= 7:
            return 80.0
        elif cand.cg_trending_rank <= 15:
            return 60.0
        elif cand.cmc_trending_rank <= 5:
            return 70.0
        elif cand.cmc_trending_rank <= 10:
            return 50.0
        return 20.0  # base score for being on Binance Futures at all

    def _score_narrative(self, cand: PairCandidate) -> Tuple[float, str]:
        """Category heat / narrative activity score."""
        if not cand.category or cand.category in ("UNKNOWN", "OTHER"):
            return 10.0, ""

        # HOT narratives right now (would be dynamic in production,
        # but we use scoring heuristics from candidate data)
        hot_categories = {"AI", "MEME", "GAMING", "LAUNCHPAD"}
        warm_categories = {"LAYER2", "LAYER1", "DEFI"}

        if cand.category in hot_categories:
            base = 80.0
        elif cand.category in warm_categories:
            base = 60.0
        else:
            base = 40.0

        # Boost if pair is in a trending move
        if abs(cand.price_change_24h_pct) > 10:
            base = min(base + 15, 100)

        return base, cand.category

    def _score_hype_velocity(self, cand: PairCandidate) -> float:
        """Rate of change of attention — acceleration matters more than level."""
        # Use 1h vs 24h price change ratio as proxy for velocity
        if cand.price_change_24h_pct == 0:
            return 20.0

        # If 1h change is in same direction and > 30% of 24h move
        # → move is accelerating (fresh)
        if cand.price_change_1h_pct != 0 and cand.price_change_24h_pct != 0:
            same_direction = (
                cand.price_change_1h_pct > 0 and cand.price_change_24h_pct > 0
            ) or (
                cand.price_change_1h_pct < 0 and cand.price_change_24h_pct < 0
            )
            if same_direction:
                velocity_ratio = abs(cand.price_change_1h_pct) / max(
                    abs(cand.price_change_24h_pct), 0.1
                )
                if velocity_ratio >= 0.4:   # 40%+ of daily move in last hour
                    return 90.0
                elif velocity_ratio >= 0.2:
                    return 65.0

        # Fallback: use absolute 24h change as proxy
        abs_24h = abs(cand.price_change_24h_pct)
        if abs_24h >= 20:
            return 75.0
        elif abs_24h >= 10:
            return 55.0
        elif abs_24h >= 5:
            return 35.0
        return 15.0

    def _score_derivatives(
        self, deriv: DerivativesSnapshot
    ) -> Tuple[float, Dict]:
        """OI + funding context → derivatives interest score 0-100."""
        score = 30.0   # baseline
        signals = {}

        # OI expansion (price + OI = strong direction)
        if deriv.oi_expanding:
            score += 25
            signals["oi"] = "expanding"
        elif deriv.oi_collapsing:
            score += 15   # collapse = potential reversal setup
            signals["oi"] = "collapsing"

        # Funding useful context (not just extreme = avoid)
        abs_funding = abs(deriv.funding_rate)
        if 0.0001 < abs_funding < config.FUNDING_EXTREME_LONG:
            # Moderate funding — useful for directional bias
            score += 15
            signals["funding"] = "moderate_bias"
        elif abs_funding >= config.FUNDING_EXTREME_LONG:
            # Extreme → fade setup
            score += 20
            signals["funding"] = "extreme_fade_setup"
        else:
            signals["funding"] = "neutral"

        # Liquidation imbalance = potential squeeze/cascade
        if deriv.liq_imbalance_pct >= config.LIQ_IMBALANCE_THRESHOLD:
            score += 20
            signals["liq"] = f"imbalanced_{deriv.liq_imbalance_pct:.0%}"
        elif deriv.liq_imbalance_pct <= (1 - config.LIQ_IMBALANCE_THRESHOLD):
            score += 15
            signals["liq"] = f"short_side_heavy_{deriv.liq_imbalance_pct:.0%}"

        # Long/short crowding — crowded = trap setup value
        if deriv.ls_crowded_long or deriv.ls_crowded_short:
            score += 10
            signals["ls"] = "crowded"

        return min(score, 100.0), signals

    def _score_volume_spike(self, cand: PairCandidate) -> float:
        """Volume vs average — spike = attention real."""
        ratio = cand.volume_vs_avg_ratio
        if ratio >= 3.0:
            return 100.0
        elif ratio >= 2.0:
            return 80.0
        elif ratio >= 1.5:
            return 60.0
        elif ratio >= 1.2:
            return 40.0
        return 20.0

    def _score_liquidity_quality(self, cand: PairCandidate) -> float:
        """Higher volume + lower spread + better depth = higher quality."""
        score = 50.0

        if cand.volume_24h_usdt >= 200_000_000:   # >$200M
            score = 90.0
        elif cand.volume_24h_usdt >= 100_000_000:  # >$100M
            score = 75.0
        elif cand.volume_24h_usdt >= 50_000_000:   # >$50M
            score = 60.0
        elif cand.volume_24h_usdt >= 30_000_000:   # >$30M (min)
            score = 40.0
        else:
            score = 0.0   # below min threshold

        return score

    # ═══════════════════════════════════════════
    # BULK DERIVATIVES FETCH (Binance Futures)
    # ═══════════════════════════════════════════

    async def _bulk_fetch_derivatives(
        self, session: aiohttp.ClientSession, symbols: List[str]
    ):
        """Fetch funding rates + OI + premium index in parallel."""
        tasks = [self._fetch_symbol_derivatives(session, sym) for sym in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for sym, result in zip(symbols, results):
            if isinstance(result, DerivativesSnapshot):
                self._derivatives[sym] = result
                # Update OI history
                if sym not in self._oi_history:
                    self._oi_history[sym] = []
                self._oi_history[sym].append((time.time(), result.oi_now))
                # Keep last 48 entries (4h if every 5min)
                if len(self._oi_history[sym]) > 48:
                    self._oi_history[sym] = self._oi_history[sym][-48:]

    async def _fetch_symbol_derivatives(
        self, session: aiohttp.ClientSession, symbol: str
    ) -> DerivativesSnapshot:
        snap = DerivativesSnapshot(symbol=symbol, updated_at=time.time())

        try:
            # Funding rate
            url = f"{config.BINANCE_BASE_URL}/fapi/v1/premiumIndex"
            async with session.get(
                url, params={"symbol": symbol},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    snap.funding_rate = float(data.get("lastFundingRate", 0))
        except Exception:
            pass

        try:
            # Open Interest
            url = f"{config.BINANCE_BASE_URL}/fapi/v1/openInterest"
            async with session.get(
                url, params={"symbol": symbol},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    snap.oi_now = float(data.get("openInterest", 0))
        except Exception:
            pass

        # Compute OI change vs 4h ago
        history = self._oi_history.get(symbol, [])
        if len(history) >= 2:
            cutoff = time.time() - 4 * 3600
            old_entries = [h for h in history if h[0] <= cutoff]
            if old_entries:
                old_oi = old_entries[-1][1]
                if old_oi > 0:
                    snap.oi_4h_ago = old_oi
                    snap.oi_change_pct = (snap.oi_now - old_oi) / old_oi
                    snap.oi_expanding = (
                        snap.oi_change_pct >= config.OI_EXPANSION_THRESHOLD_PCT
                    )
                    snap.oi_collapsing = (
                        snap.oi_change_pct <= config.OI_COLLAPSE_THRESHOLD_PCT
                    )

        # Long/short crowding assessment
        snap.ls_crowded_long = (
            snap.ls_ratio >= config.OI_EXTREME_LONG_CROWDING_PCT
        )
        snap.ls_crowded_short = (
            snap.ls_ratio <= config.OI_EXTREME_SHORT_CROWDING_PCT
        )

        # Funding trend
        funding_history = self._funding_history.get(symbol, [])
        if len(funding_history) >= 3:
            recent = [h[1] for h in funding_history[-3:]]
            if all(recent[i] < recent[i + 1] for i in range(len(recent) - 1)):
                snap.funding_trend = "RISING"
            elif all(recent[i] > recent[i + 1] for i in range(len(recent) - 1)):
                snap.funding_trend = "FALLING"
            else:
                snap.funding_trend = "NEUTRAL"

        # Store funding history
        if symbol not in self._funding_history:
            self._funding_history[symbol] = []
        self._funding_history[symbol].append((time.time(), snap.funding_rate))
        if len(self._funding_history[symbol]) > 24:
            self._funding_history[symbol] = self._funding_history[symbol][-24:]

        return snap

    # ═══════════════════════════════════════════
    # SESSION
    # ═══════════════════════════════════════════

    async def _get_session(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        self._shutdown.set()
        if self._session and not self._session.closed:
            await self._session.close()
