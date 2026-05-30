"""
ARUNABHA MANUAL SCALPER v4.0
FILE: pair_universe_engine.py
LAYER A — Pair Universe Builder

Discovers rotating universe of uncommon but liquid tradable pairs.
Sources: CoinGecko (free), CMC (free key), Binance Futures perp list.
Refreshes every UNIVERSE_REFRESH_MINUTES.
Outputs: ranked list of symbols ready for attention scoring.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import aiohttp

import config

log = logging.getLogger("scalper.universe")


@dataclass
class PairCandidate:
    symbol: str                      # Binance Futures symbol e.g. INJUSDT
    coingecko_id: str = ""
    display_name: str = ""
    category: str = "UNKNOWN"

    # Tradability metrics
    volume_24h_usdt: float = 0.0
    oi_usdt: float = 0.0
    spread_pct: float = 0.002
    depth_usdt: float = 0.0

    # Discovery signals
    cg_trending_rank: int = 999       # 1 = top trending on CoinGecko
    cmc_trending_rank: int = 999      # 1 = top trending on CMC
    volume_vs_avg_ratio: float = 1.0  # >1.5 = volume spike
    price_change_1h_pct: float = 0.0
    price_change_24h_pct: float = 0.0
    narrative_active: bool = False

    # Tradability pass
    tradable: bool = True
    reject_reason: str = ""

    # Scoring
    candidate_score: float = 0.0
    scored_at: float = 0.0


class PairUniverseEngine:
    """
    Builds rotating universe of uncommon but liquid tradeable pairs.
    Does NOT include static fixed lists — discovery is dynamic.
    Refreshes every UNIVERSE_REFRESH_MINUTES.
    """

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._universe: List[str] = []                    # final ranked symbol list
        self._candidates: Dict[str, PairCandidate] = {}   # all discovered candidates
        self._binance_perps: Set[str] = set()             # what's actually tradeable
        self._last_refresh: float = 0.0
        self._refresh_interval: float = config.UNIVERSE_REFRESH_MINUTES * 60
        self._shutdown = asyncio.Event()
        self._lock = asyncio.Lock()

    # ═══════════════════════════════════════════
    # PUBLIC API
    # ═══════════════════════════════════════════

    def get_universe(self) -> List[str]:
        """Returns current ranked symbol list for scanning."""
        return list(self._universe)

    def get_candidate(self, symbol: str) -> Optional[PairCandidate]:
        return self._candidates.get(symbol)

    def get_all_candidates(self) -> List[PairCandidate]:
        return list(self._candidates.values())

    def is_fresh(self) -> bool:
        return time.time() - self._last_refresh < self._refresh_interval * 2

    # ═══════════════════════════════════════════
    # BACKGROUND LOOP
    # ═══════════════════════════════════════════

    async def run(self):
        """Background task — refreshes universe periodically."""
        log.info("PairUniverseEngine started")
        while not self._shutdown.is_set():
            try:
                await self._refresh()
                log.info(
                    f"Universe refreshed: {len(self._universe)} pairs | "
                    f"Top 5: {self._universe[:5]}"
                )
            except Exception as e:
                log.warning(f"Universe refresh error: {e}")
                # On failure: keep existing universe if we have one
                # If first-run failure, fetch dynamic fallback (NOT hardcoded list)
                if not self._universe:
                    log.warning("Discovery failed on first run — fetching dynamic fallback")
                    try:
                        session = await self._get_session()
                        dynamic_fallback = await self._fetch_dynamic_fallback(session)
                        if dynamic_fallback:
                            self._universe = dynamic_fallback
                            log.info(f"Dynamic fallback: {self._universe}")
                        else:
                            # Only use hardcoded as absolute last resort
                            log.warning("Dynamic fallback also failed — using static fallback")
                            self._universe = list(config.FALLBACK_SYMBOLS)
                    except Exception as fe:
                        log.warning(f"Fallback fetch error: {fe}")
                        self._universe = list(config.FALLBACK_SYMBOLS)

            try:
                await asyncio.wait_for(
                    self._shutdown.wait(),
                    timeout=self._refresh_interval,
                )
            except asyncio.TimeoutError:
                pass

    async def refresh_now(self):
        """Force immediate refresh (called at startup)."""
        await self._refresh()

    # ═══════════════════════════════════════════
    # CORE REFRESH PIPELINE
    # ═══════════════════════════════════════════

    async def _refresh(self):
        async with self._lock:
            session = await self._get_session()

            # Step 1: Get tradeable Binance Futures perps
            await self._fetch_binance_perps(session)

            # Step 2: Gather candidates from free sources
            candidates: Dict[str, PairCandidate] = {}

            cg_trending = await self._fetch_coingecko_trending(session)
            for c in cg_trending:
                if c.symbol in candidates:
                    candidates[c.symbol].cg_trending_rank = c.cg_trending_rank
                else:
                    candidates[c.symbol] = c

            cg_gainers = await self._fetch_coingecko_top_gainers(session)
            for c in cg_gainers:
                if c.symbol in candidates:
                    candidates[c.symbol].price_change_24h_pct = c.price_change_24h_pct
                else:
                    candidates[c.symbol] = c

            # Step 3: Enrich with Binance Futures data (OI, volume, spread)
            await self._enrich_with_binance(session, candidates)

            # Step 4: Apply tradability filter
            for sym, cand in candidates.items():
                self._check_tradability(cand)

            # Step 5: Score candidates
            for sym, cand in candidates.items():
                cand.candidate_score = self._score_candidate(cand)
                cand.scored_at = time.time()

            # Step 6: Filter + rank
            tradable = [c for c in candidates.values() if c.tradable and c.candidate_score > 0]
            tradable.sort(key=lambda x: x.candidate_score, reverse=True)

            # Step 7: Apply universe size limits
            final = tradable[:config.UNIVERSE_MAX_SIZE]
            if len(final) < config.UNIVERSE_MIN_SIZE:
                # Not enough discovered pairs.
                # Pad with top-volume Binance pairs (dynamic, not hardcoded list)
                existing_syms = {c.symbol for c in final}
                top_volume = await self._fetch_top_volume_pairs(
                    session,
                    exclude=existing_syms | set(config.CROWDED_MAJORS),
                    limit=config.UNIVERSE_MIN_SIZE - len(final) + 3,
                )
                for sym in top_volume:
                    if sym not in existing_syms and len(final) < config.UNIVERSE_MIN_SIZE:
                        pad_cand = PairCandidate(symbol=sym, category="TOP_VOLUME")
                        pad_cand.candidate_score = 12.0  # low but present
                        final.append(pad_cand)
                        existing_syms.add(sym)

                # If still not enough (all APIs down), use static fallback
                if len(final) < 3:
                    for fb in config.FALLBACK_SYMBOLS:
                        if fb not in existing_syms and len(final) < config.UNIVERSE_MIN_SIZE:
                            fb_cand = PairCandidate(symbol=fb, category="FALLBACK")
                            fb_cand.candidate_score = 8.0
                            final.append(fb_cand)

            self._candidates = {c.symbol: c for c in final}
            self._universe = [c.symbol for c in final]
            self._last_refresh = time.time()

    # ═══════════════════════════════════════════
    # STEP 1: BINANCE FUTURES PERP LIST
    # ═══════════════════════════════════════════

    async def _fetch_binance_perps(self, session: aiohttp.ClientSession):
        """Get all USDT-margined perpetuals from Binance Futures."""
        try:
            url = f"{config.BINANCE_BASE_URL}/fapi/v1/exchangeInfo"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return
                data = await r.json()
                self._binance_perps = {
                    s["symbol"]
                    for s in data.get("symbols", [])
                    if s.get("status") == "TRADING"
                    and s.get("contractType") == "PERPETUAL"
                    and s["symbol"].endswith("USDT")
                }
                log.debug(f"Binance perps: {len(self._binance_perps)} symbols")
        except Exception as e:
            log.debug(f"Binance perp list error: {e}")

    # ═══════════════════════════════════════════
    # STEP 2A: COINGECKO TRENDING (FREE, NO KEY)
    # ═══════════════════════════════════════════

    async def _fetch_coingecko_trending(
        self, session: aiohttp.ClientSession
    ) -> List[PairCandidate]:
        """Fetch CoinGecko trending coins — no API key required."""
        candidates = []
        try:
            url = f"{config.COINGECKO_BASE}/search/trending"
            headers = {"Accept": "application/json"}
            async with session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                if r.status != 200:
                    return candidates
                data = await r.json()

                for rank, item in enumerate(data.get("coins", [])[:15], 1):
                    coin = item.get("item", {})
                    name = coin.get("name", "")
                    symbol_raw = coin.get("symbol", "").upper()
                    cg_id = coin.get("id", "")

                    binance_sym = f"{symbol_raw}USDT"
                    if binance_sym not in self._binance_perps:
                        # Try common variants
                        for variant in [f"{symbol_raw}USDT", f"1000{symbol_raw}USDT"]:
                            if variant in self._binance_perps:
                                binance_sym = variant
                                break
                        else:
                            continue  # Not on Binance Futures

                    category = self._detect_category(symbol_raw, name)

                    cand = PairCandidate(
                        symbol=binance_sym,
                        coingecko_id=cg_id,
                        display_name=name,
                        category=category,
                        cg_trending_rank=rank,
                    )
                    candidates.append(cand)

        except Exception as e:
            log.debug(f"CoinGecko trending error: {e}")

        return candidates

    # ═══════════════════════════════════════════
    # STEP 2B: COINGECKO TOP GAINERS (FREE)
    # ═══════════════════════════════════════════

    async def _fetch_coingecko_top_gainers(
        self, session: aiohttp.ClientSession
    ) -> List[PairCandidate]:
        """Fetch top gainers/movers in last 24h."""
        candidates = []
        try:
            url = f"{config.COINGECKO_BASE}/coins/markets"
            params = {
                "vs_currency": "usd",
                "order": "percent_change_24h_desc",
                "per_page": 50,
                "page": 1,
                "sparkline": "false",
            }
            headers = {"Accept": "application/json"}
            async with session.get(
                url, params=params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status != 200:
                    return candidates
                coins = await r.json()

                for coin in coins:
                    symbol_raw = coin.get("symbol", "").upper()
                    name = coin.get("name", "")
                    change_24h = coin.get("price_change_percentage_24h", 0.0) or 0.0

                    # Only consider coins moving significantly
                    if abs(change_24h) < 5.0:
                        continue

                    binance_sym = f"{symbol_raw}USDT"
                    if binance_sym not in self._binance_perps:
                        continue

                    # Skip crowded majors unless exceptional
                    if (
                        binance_sym in config.CROWDED_MAJORS
                        and abs(change_24h) < 10.0
                    ):
                        continue

                    cand = PairCandidate(
                        symbol=binance_sym,
                        coingecko_id=coin.get("id", ""),
                        display_name=name,
                        category=self._detect_category(symbol_raw, name),
                        price_change_24h_pct=change_24h,
                    )
                    candidates.append(cand)

        except Exception as e:
            log.debug(f"CoinGecko top gainers error: {e}")

        return candidates

    # ═══════════════════════════════════════════
    # STEP 3: ENRICH WITH BINANCE FUTURES DATA
    # ═══════════════════════════════════════════

    async def _enrich_with_binance(
        self,
        session: aiohttp.ClientSession,
        candidates: Dict[str, PairCandidate],
    ):
        """Fetch OI, volume, ticker data for all candidates."""
        if not candidates:
            return

        # Batch fetch 24h ticker stats
        try:
            url = f"{config.BINANCE_BASE_URL}/fapi/v1/ticker/24hr"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    tickers = await r.json()
                    ticker_map = {t["symbol"]: t for t in tickers}

                    for sym, cand in candidates.items():
                        t = ticker_map.get(sym)
                        if not t:
                            continue
                        cand.volume_24h_usdt = float(t.get("quoteVolume", 0))
                        cand.price_change_1h_pct = 0.0  # not in 24h ticker
                        if not cand.price_change_24h_pct:
                            cand.price_change_24h_pct = float(
                                t.get("priceChangePercent", 0)
                            )
        except Exception as e:
            log.debug(f"Binance ticker enrich error: {e}")

        # Fetch OI for candidates
        try:
            url = f"{config.BINANCE_BASE_URL}/fapi/v1/openInterest"
            tasks = []
            syms = list(candidates.keys())[:20]  # limit API calls
            for sym in syms:
                tasks.append(self._fetch_oi(session, sym))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            for sym, result in zip(syms, results):
                if isinstance(result, float) and result > 0:
                    candidates[sym].oi_usdt = result
        except Exception as e:
            log.debug(f"OI enrich error: {e}")

    async def _fetch_oi(
        self, session: aiohttp.ClientSession, symbol: str
    ) -> float:
        try:
            url = f"{config.BINANCE_BASE_URL}/fapi/v1/openInterest"
            params = {"symbol": symbol}
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    # OI in contracts * price ≈ USDT value
                    # Rough estimate without price: use notional
                    return float(data.get("openInterest", 0))
        except Exception:
            pass
        return 0.0

    # ═══════════════════════════════════════════
    # STEP 4: TRADABILITY CHECK
    # ═══════════════════════════════════════════

    def _check_tradability(self, cand: PairCandidate):
        """Hard filters — must pass all to be tradeable."""
        if cand.symbol not in self._binance_perps:
            cand.tradable = False
            cand.reject_reason = "not_on_binance_futures"
            return

        if cand.volume_24h_usdt < config.MIN_24H_VOLUME_USDT:
            cand.tradable = False
            cand.reject_reason = f"volume_too_low_{cand.volume_24h_usdt/1e6:.1f}M"
            return

        # Detect category
        if not cand.category or cand.category == "UNKNOWN":
            cand.category = self._detect_category(
                cand.symbol.replace("USDT", ""), cand.display_name
            )

        cand.tradable = True

    # ═══════════════════════════════════════════
    # STEP 5: CANDIDATE SCORING
    # ═══════════════════════════════════════════

    def _score_candidate(self, cand: PairCandidate) -> float:
        """
        Score = discovery signal strength.
        This is NOT signal quality — that comes in Layer B.
        This just determines which pairs deserve ATTENTION.
        """
        if not cand.tradable:
            return 0.0

        score = 0.0

        # Trending rank score (CoinGecko)
        if cand.cg_trending_rank <= 5:
            score += 25
        elif cand.cg_trending_rank <= 10:
            score += 18
        elif cand.cg_trending_rank <= 20:
            score += 10

        # CMC trending
        if cand.cmc_trending_rank <= 5:
            score += 20
        elif cand.cmc_trending_rank <= 10:
            score += 12

        # Price movement (hype signal)
        abs_change = abs(cand.price_change_24h_pct)
        if abs_change >= 15:
            score += 20
        elif abs_change >= 8:
            score += 12
        elif abs_change >= 4:
            score += 6

        # Volume spike
        if cand.volume_vs_avg_ratio >= 2.5:
            score += 15
        elif cand.volume_vs_avg_ratio >= 1.5:
            score += 8

        # Narrative active
        if cand.narrative_active:
            score += 10

        # OI present (liquidity quality)
        if cand.oi_usdt > 50_000_000:     # >$50M OI
            score += 10
        elif cand.oi_usdt > 10_000_000:   # >$10M OI
            score += 5

        # Penalty for being a crowded major (unless very high attention)
        if cand.symbol in config.CROWDED_MAJORS and score < 50:
            score *= 0.4

        return round(score, 1)

    # ═══════════════════════════════════════════
    # HELPERS
    # ═══════════════════════════════════════════

    def _detect_category(self, symbol: str, name: str) -> str:
        """
        Detect narrative category from symbol/name.

        TWO-PASS approach:
        Pass 1: Keyword matching in name/description (catches new coins too)
        Pass 2: Static list lookup in config (backup)

        This way a brand-new AI coin not in the config list
        still gets detected as "AI" if its name contains "AI" keywords.
        """
        combined = (symbol + " " + name).upper()

        # Pass 1: Keyword-based detection (works for ANY new coin)
        keyword_map = {
            "AI":       ["ARTIFICIAL", "AI ", " AI", "INTELLIGENCE", "NEURAL",
                         "MACHINE LEARN", "GPT", "AGENT", "INFERENCE", "LLM",
                         "COMPUTE", "RENDER", "FETCH", "BITTENSOR", "OCEAN"],
            "MEME":     ["MEME", "DOGE", "SHIB", "PEPE", "BONK", "FLOKI",
                         "WIF", "HAMSTER", "CAT", "DOG", "FROG", "WOJAK",
                         "MOON", "BASED", "CHAD", "INU"],
            "GAMING":   ["GAME", "GAMING", "PLAY", "METAVERSE", "NFT",
                         "SANDBOX", "AXIE", "GALA", "ILLUVIUM", "IMMUTABLE",
                         "PIXEL", "GUILD", "QUEST"],
            "DEFI":     ["DEFI", "DEX", "SWAP", "LIQUIDITY", "YIELD",
                         "LENDING", "BORROW", "CURVE", "AAVE", "UNISWAP",
                         "COMPOUND", "MAKER", "PROTOCOL", "LIDO"],
            "LAYER2":   ["LAYER 2", "L2", "ROLLUP", "ARBITRUM", "OPTIMISM",
                         "POLYGON", "STARKNET", "ZKSYNC", "BASE", "SCROLL"],
            "LAYER1":   ["LAYER 1", "L1", "BLOCKCHAIN", "CONSENSUS",
                         "AVALANCHE", "NEAR", "APTOS", "SUI", "SEI", "MONAD"],
            "LAUNCHPAD":["LAUNCHPAD", "IDO", "IEO", "LAUNCH", "PAD"],
            "INFRA":    ["ORACLE", "CHAINLINK", "STORAGE", "FILECOIN",
                         "BANDWIDTH", "COMPUTE", "INFRASTRUCTURE", "NODE",
                         "VALIDATOR", "INDEXER"],
            "PAYMENTS": ["PAYMENT", "TRANSFER", "REMITTANCE", "CROSS.BORDER",
                         "RIPPLE", "STELLAR", "LITECOIN"],
            "RWA":      ["REAL WORLD", "RWA", "TOKENIZE", "ASSET", "ESTATE"],
            "DEPIN":    ["DEPIN", "PHYSICAL", "IOT", "NETWORK", "HELIUM",
                         "HIVEMAPPER", "GEODNET"],
            "SOCIAL":   ["SOCIAL", "FRIEND", "LENS", "FARCASTER", "CREATOR",
                         "CONTENT", "COMMUNITY"],
        }

        for category, keywords in keyword_map.items():
            if any(kw in combined for kw in keywords):
                return category

        # Pass 2: Static config list lookup (fallback)
        for cat, symbols in config.NARRATIVE_CATEGORIES.items():
            syms_clean = [s.replace("USDT", "") for s in symbols]
            if symbol in syms_clean:
                return cat

        return "OTHER"

    async def _fetch_top_volume_pairs(
        self,
        session: aiohttp.ClientSession,
        exclude: set,
        limit: int = 8,
    ) -> List[str]:
        """
        Fetch top-volume USDT perps from Binance (dynamic, not hardcoded).
        Used for padding when discovery yields too few candidates.
        Excludes crowded majors and already-found pairs.
        """
        try:
            url = f"{config.BINANCE_BASE_URL}/fapi/v1/ticker/24hr"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200:
                    return []
                tickers = await r.json()

                # Filter: USDT perp, in tradeable set, not excluded, min volume
                eligible = []
                for t in tickers:
                    sym = t.get("symbol", "")
                    if not sym.endswith("USDT"):
                        continue
                    if sym not in self._binance_perps:
                        continue
                    if sym in exclude:
                        continue
                    if sym in config.CROWDED_MAJORS:
                        continue
                    try:
                        vol = float(t.get("quoteVolume", 0))
                        change = abs(float(t.get("priceChangePercent", 0)))
                    except (ValueError, TypeError):
                        continue
                    if vol < config.MIN_24H_VOLUME_USDT:
                        continue
                    # Prefer pairs that are actually moving today
                    eligible.append((sym, vol, change))

                # Sort: weight by movement + volume (not just volume)
                # This favors active pairs over just big stale ones
                eligible.sort(key=lambda x: x[1] * (1 + x[2] / 100), reverse=True)
                return [sym for sym, _, _ in eligible[:limit]]

        except Exception as e:
            log.debug(f"Top volume fetch error: {e}")
            return []

    async def _fetch_dynamic_fallback(
        self, session: aiohttp.ClientSession
    ) -> List[str]:
        """
        Emergency fallback: fetch top 10 moving + liquid USDT perps.
        Called only when CoinGecko completely fails on startup.
        Better than a static list because it reflects current market.
        """
        await self._fetch_binance_perps(session)
        if not self._binance_perps:
            return []
        return await self._fetch_top_volume_pairs(
            session,
            exclude=set(config.CROWDED_MAJORS),
            limit=10,
        )
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; CryptoBot/4.0)",
                    "Accept": "application/json",
                }
            )
        return self._session

    async def close(self):
        self._shutdown.set()
        if self._session and not self._session.closed:
            await self._session.close()
