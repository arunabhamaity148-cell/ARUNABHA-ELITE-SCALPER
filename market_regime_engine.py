"""
ARUNABHA ELITE SCALPER v3.0
FILE 7/18: market_regime_engine.py
Multi-factor regime classification — trend, ranging, volatile, accumulation, distribution
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

import numpy as np

import config
from data_processor import DataProcessor, Indicators

log = logging.getLogger("elite.regime")


class RegimeType(Enum):
    STRONG_TREND_UP = "STRONG_TREND_UP"
    WEAK_TREND_UP = "WEAK_TREND_UP"
    RANGING = "RANGING"
    WEAK_TREND_DOWN = "WEAK_TREND_DOWN"
    STRONG_TREND_DOWN = "STRONG_TREND_DOWN"
    VOLATILE = "VOLATILE"
    ACCUMULATION = "ACCUMULATION"
    DISTRIBUTION = "DISTRIBUTION"
    TRANSITION = "TRANSITION"


@dataclass
class RegimeResult:
    symbol: str
    regime: RegimeType
    confidence: float           # 0-100
    factors: Dict[str, float]
    recommendation: str         # TRADE / CAUTION / NO_TRADE
    size_adjustment: float      # 0.0 to 1.0
    duration_candles: int       # how long in current regime
    volatility_regime: str      # LOW / MEDIUM / HIGH / EXTREME
    volume_regime: str          # DRY / NORMAL / ELEVATED / EXTREME
    trend_bias: str             # BULLISH / BEARISH / NEUTRAL
    breadth_score: float        # 0-1 market breadth
    updated_at: float = 0.0


@dataclass
class RegimeHistory:
    regime: RegimeType
    since_candle: int
    started_at: float


class MarketRegimeEngine:
    def __init__(self, data_processor: DataProcessor):
        self.dp = data_processor
        self._history: Dict[str, RegimeHistory] = {}
        self._results: Dict[str, RegimeResult] = {}
        self._last_update: Dict[str, float] = {}
        self._candle_index: Dict[str, int] = {sym: 0 for sym in config.SYMBOLS}

    # ═══════════════════════════════════════════
    # MAIN CLASSIFICATION
    # ═══════════════════════════════════════════

    async def classify(self, symbol: str) -> RegimeResult:
        """Full regime classification. Cached for 30s."""
        now = time.time()
        if now - self._last_update.get(symbol, 0) < 30:
            if symbol in self._results:
                return self._results[symbol]

        try:
            result = await self._classify(symbol)
            self._results[symbol] = result
            self._last_update[symbol] = now
            return result
        except Exception as e:
            log.debug(f"Regime error {symbol}: {e}")
            return self._default_result(symbol)

    async def _classify(self, symbol: str) -> RegimeResult:
        # Get indicators for all timeframes
        ind_15m = self.dp.get_indicators(symbol, "15m")
        ind_1h = self.dp.get_indicators(symbol, "1h")
        ind_4h = self.dp.get_indicators(symbol, "4h")
        ind_5m = self.dp.get_indicators(symbol, "5m")

        if not ind_15m or not ind_1h:
            return self._default_result(symbol)

        candles_15m = self.dp.get_candles(symbol, "15m", n=100)
        price = self.dp.get_price(symbol)
        funding = self.dp.get_funding(symbol)
        oi = self.dp.get_oi(symbol)

        factors = {}

        # ── 1. TREND ANALYSIS ──
        ema_score = self._score_ema_alignment(ind_15m, ind_1h, ind_4h)
        factors["ema_alignment"] = ema_score  # -4 to +4

        adx_score = self._score_adx(ind_15m)
        factors["adx"] = adx_score  # 0-100

        structure_score, hh_hl, lh_ll = self._score_structure(candles_15m)
        factors["structure"] = structure_score  # -1 to +1

        # ── 2. VOLATILITY ──
        vol_regime = self._classify_volatility(ind_15m, candles_15m)
        factors["vol_regime"] = vol_regime  # 0=LOW, 1=MED, 2=HIGH, 3=EXTREME

        atr_spike = ind_15m.atr_percentile > 80
        factors["atr_spike"] = float(atr_spike)

        # ── 3. VOLUME ──
        vol_ratio = ind_15m.vol_ratio
        volume_regime = self._classify_volume(vol_ratio)
        factors["vol_ratio"] = vol_ratio

        # ── 4. BOLLINGER ──
        bb_squeeze = ind_15m.bb_bandwidth < config.BB_SQUEEZE * ind_15m.bb_bandwidth_avg if ind_15m.bb_bandwidth_avg > 0 else False
        factors["bb_squeeze"] = float(bb_squeeze)

        # ── 5. FUNDING & OI ──
        funding_extreme = abs(funding.rate) > config.FUNDING_EXTREME_LONG
        oi_direction = self._classify_oi(oi, price)
        factors["funding_rate"] = funding.rate
        factors["oi_signal"] = oi_direction  # -1 to +1

        # ── 6. MARKET BREADTH ──
        breadth = self._compute_breadth()
        factors["breadth"] = breadth  # 0-1

        # ── REGIME DETERMINATION ──
        regime = self._determine_regime(
            ema_score, adx_score, structure_score,
            vol_regime, atr_spike, vol_ratio, bb_squeeze,
            hh_hl, lh_ll, oi_direction, breadth,
        )

        # ── DURATION ──
        duration = self._update_duration(symbol, regime)
        factors["duration_candles"] = float(duration)

        # ── CONFIDENCE ──
        confidence = self._compute_confidence(regime, factors)

        # ── RECOMMENDATION ──
        recommendation, size_adj = self._get_recommendation(
            regime, confidence, duration, vol_regime, bb_squeeze, funding_extreme
        )

        # ── TREND BIAS ──
        if ema_score >= 2:
            trend_bias = "BULLISH"
        elif ema_score <= -2:
            trend_bias = "BEARISH"
        else:
            trend_bias = "NEUTRAL"

        return RegimeResult(
            symbol=symbol,
            regime=regime,
            confidence=confidence,
            factors=factors,
            recommendation=recommendation,
            size_adjustment=size_adj,
            duration_candles=duration,
            volatility_regime=["LOW", "MEDIUM", "HIGH", "EXTREME"][int(vol_regime)],
            volume_regime=volume_regime,
            trend_bias=trend_bias,
            breadth_score=breadth,
            updated_at=time.time(),
        )

    # ═══════════════════════════════════════════
    # FACTOR SCORING
    # ═══════════════════════════════════════════

    def _score_ema_alignment(
        self,
        ind_15m: Indicators,
        ind_1h: Indicators,
        ind_4h: Optional[Indicators],
    ) -> float:
        """
        EMA alignment score: -4 to +4
        +1 per TF where 9>21>50>200 (bullish)
        -1 per TF where 9<21<50<200 (bearish)
        """
        score = 0.0
        for ind in [ind_15m, ind_1h, ind_4h]:
            if not ind:
                continue
            if ind.ema9 > ind.ema21 > ind.ema50:
                score += 1.0
            elif ind.ema9 < ind.ema21 < ind.ema50:
                score -= 1.0
            # EMA200 bonus
            if ind.ema9 > ind.ema200:
                score += 0.3
            elif ind.ema9 < ind.ema200:
                score -= 0.3
        return round(score, 2)

    def _score_adx(self, ind: Indicators) -> float:
        return ind.adx

    def _score_structure(self, candles: list) -> Tuple[float, bool, bool]:
        """
        Detect HH/HL (uptrend) or LH/LL (downtrend).
        Returns (score -1 to +1, hh_hl bool, lh_ll bool)
        """
        if len(candles) < 6:
            return 0.0, False, False

        highs = [c.h for c in candles[-6:]]
        lows = [c.l for c in candles[-6:]]

        # Check HH and HL (bullish structure)
        hh = highs[-1] > highs[-3] > highs[-5]
        hl = lows[-1] > lows[-3] > lows[-5]

        # Check LH and LL (bearish structure)
        lh = highs[-1] < highs[-3] < highs[-5]
        ll = lows[-1] < lows[-3] < lows[-5]

        hh_hl = hh and hl
        lh_ll = lh and ll

        if hh_hl:
            return 1.0, True, False
        elif lh_ll:
            return -1.0, False, True
        elif hh or hl:
            return 0.5, False, False
        elif lh or ll:
            return -0.5, False, False
        return 0.0, False, False

    def _classify_volatility(self, ind: Indicators, candles: list) -> int:
        """0=LOW, 1=MEDIUM, 2=HIGH, 3=EXTREME"""
        pct = ind.atr_percentile
        if pct < 30:
            return 0
        elif pct < 70:
            return 1
        elif pct < 90:
            return 2
        else:
            return 3

    def _classify_volume(self, vol_ratio: float) -> str:
        if vol_ratio < config.VOLUME_DRY:
            return "DRY"
        elif vol_ratio < config.VOLUME_ELEVATED:
            return "NORMAL"
        elif vol_ratio < config.VOLUME_EXTREME:
            return "ELEVATED"
        else:
            return "EXTREME"

    def _classify_oi(self, oi, price: float) -> float:
        """
        OI interpretation:
        Rising OI + rising price = +1 (conviction long)
        Rising OI + falling price = -1 (conviction short)
        Falling OI = 0 (neutral)
        """
        if oi.oi_change_1h > 0.02 and price > 0:
            return 0.5
        elif oi.oi_change_1h < -0.02:
            return -0.3
        return 0.0

    def _compute_breadth(self) -> float:
        """% of all tracked symbols with bullish EMA alignment."""
        bullish = 0
        total = 0
        for sym in config.SYMBOLS:
            ind = self.dp.get_indicators(sym, "15m")
            if ind and ind.ema9 > 0 and ind.ema21 > 0:
                total += 1
                if ind.ema9 > ind.ema21 > ind.ema50:
                    bullish += 1
        return bullish / total if total > 0 else 0.5

    # ═══════════════════════════════════════════
    # REGIME DETERMINATION
    # ═══════════════════════════════════════════

    def _determine_regime(
        self,
        ema_score: float,
        adx: float,
        structure: float,
        vol_regime: int,
        atr_spike: bool,
        vol_ratio: float,
        bb_squeeze: bool,
        hh_hl: bool,
        lh_ll: bool,
        oi_signal: float,
        breadth: float,
    ) -> RegimeType:

        # VOLATILE: massive ATR spike
        if vol_regime == 3 and atr_spike and vol_ratio > config.VOLUME_EXTREME:
            return RegimeType.VOLATILE

        # STRONG TREND UP
        if adx >= 30 and ema_score >= 2.5 and hh_hl and breadth >= 0.65:
            return RegimeType.STRONG_TREND_UP

        # WEAK TREND UP
        if adx >= config.ADX_TREND and ema_score >= 1.0 and structure >= 0:
            return RegimeType.WEAK_TREND_UP

        # STRONG TREND DOWN
        if adx >= 30 and ema_score <= -2.5 and lh_ll and breadth <= 0.35:
            return RegimeType.STRONG_TREND_DOWN

        # WEAK TREND DOWN
        if adx >= config.ADX_TREND and ema_score <= -1.0 and structure <= 0:
            return RegimeType.WEAK_TREND_DOWN

        # RANGING
        if adx <= config.ADX_CHOP:
            return RegimeType.RANGING

        # ACCUMULATION: flat price + rising OI + low vol
        if abs(ema_score) <= 0.5 and oi_signal > 0 and vol_regime <= 1 and vol_ratio > 1.0:
            return RegimeType.ACCUMULATION

        # DISTRIBUTION: slow rise + falling OI
        if ema_score >= 0.5 and oi_signal < 0 and vol_ratio > 1.0:
            return RegimeType.DISTRIBUTION

        # TRANSITION (default uncertain)
        return RegimeType.TRANSITION

    # ═══════════════════════════════════════════
    # CONFIDENCE & RECOMMENDATION
    # ═══════════════════════════════════════════

    def _compute_confidence(self, regime: RegimeType, factors: dict) -> float:
        """0-100 confidence in regime classification."""
        base = 50.0
        adx = factors.get("adx", 20)
        ema = abs(factors.get("ema_alignment", 0))
        structure = abs(factors.get("structure", 0))
        duration = factors.get("duration_candles", 0)

        # Strong ADX = more confident
        if adx > 35:
            base += 20
        elif adx > 25:
            base += 10

        # EMA alignment
        base += ema * 5

        # Structure confirmation
        base += structure * 10

        # Duration: longer = more confident
        if duration > config.REGIME_ESTABLISHED_CANDLES:
            base += 10
        elif duration < config.REGIME_NEW_CANDLES:
            base -= 15

        return round(min(max(base, 10), 100), 1)

    def _get_recommendation(
        self,
        regime: RegimeType,
        confidence: float,
        duration: int,
        vol_regime: int,
        bb_squeeze: bool,
        funding_extreme: bool,
    ) -> Tuple[str, float]:
        """
        Returns (recommendation, size_adjustment).
        recommendation: TRADE / CAUTION / NO_TRADE
        """
        # No trade regimes
        if regime == RegimeType.RANGING:
            return "CAUTION", 0.50

        if regime == RegimeType.VOLATILE:
            if vol_regime == 3:
                return "NO_TRADE", 0.0
            return "CAUTION", 0.25

        # New regime: be careful
        if duration < config.REGIME_NEW_CANDLES:
            return "CAUTION", config.REGIME_SIZE_NEW

        # BB squeeze: big move coming but direction unknown
        if bb_squeeze:
            return "CAUTION", 0.50

        # Extreme funding = crowded = caution
        if funding_extreme:
            return "CAUTION", 0.75

        # Transition: mixed signals
        if regime == RegimeType.TRANSITION:
            return "CAUTION", 0.50

        # Low confidence
        if confidence < 50:
            return "CAUTION", 0.50

        # High vol
        if vol_regime == 2:
            return "TRADE", 0.50
        elif vol_regime == 3:
            return "CAUTION", 0.25

        # Normal trade
        size = config.REGIME_SIZE_ESTABLISHED if duration >= config.REGIME_ESTABLISHED_CANDLES else 0.75
        return "TRADE", size

    # ═══════════════════════════════════════════
    # DURATION TRACKING
    # ═══════════════════════════════════════════

    def _update_duration(self, symbol: str, regime: RegimeType) -> int:
        self._candle_index[symbol] = self._candle_index.get(symbol, 0) + 1
        current_idx = self._candle_index[symbol]

        if symbol not in self._history or self._history[symbol].regime != regime:
            self._history[symbol] = RegimeHistory(
                regime=regime,
                since_candle=current_idx,
                started_at=time.time(),
            )
            return 0

        return current_idx - self._history[symbol].since_candle

    # ═══════════════════════════════════════════
    # UTILITIES
    # ═══════════════════════════════════════════

    def _default_result(self, symbol: str) -> RegimeResult:
        return RegimeResult(
            symbol=symbol,
            regime=RegimeType.TRANSITION,
            confidence=30.0,
            factors={},
            recommendation="CAUTION",
            size_adjustment=0.50,
            duration_candles=0,
            volatility_regime="MEDIUM",
            volume_regime="NORMAL",
            trend_bias="NEUTRAL",
            breadth_score=0.5,
            updated_at=time.time(),
        )

    def get_result(self, symbol: str) -> Optional[RegimeResult]:
        return self._results.get(symbol)

    def is_bullish_regime(self, symbol: str) -> bool:
        r = self._results.get(symbol)
        if not r:
            return False
        return r.regime in (RegimeType.STRONG_TREND_UP, RegimeType.WEAK_TREND_UP, RegimeType.ACCUMULATION)

    def is_bearish_regime(self, symbol: str) -> bool:
        r = self._results.get(symbol)
        if not r:
            return False
        return r.regime in (RegimeType.STRONG_TREND_DOWN, RegimeType.WEAK_TREND_DOWN, RegimeType.DISTRIBUTION)

    def get_btc_context(self) -> Tuple[str, float]:
        """BTC regime as context for alts."""
        result = self._results.get("BTCUSDT")
        if not result:
            return "NEUTRAL", 0.5
        return result.trend_bias, result.breadth_score

    def regime_allows_long(self, symbol: str) -> bool:
        r = self._results.get(symbol)
        if not r:
            return True
        return r.regime not in (RegimeType.STRONG_TREND_DOWN, RegimeType.VOLATILE)

    def regime_allows_short(self, symbol: str) -> bool:
        r = self._results.get(symbol)
        if not r:
            return True
        return r.regime not in (RegimeType.STRONG_TREND_UP, RegimeType.VOLATILE)
