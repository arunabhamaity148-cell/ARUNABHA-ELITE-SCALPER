"""
ARUNABHA MANUAL SCALPER v4.0
FILE: config.py
Complete config — Railway env vars + all v4 constants
Two-layer architecture: ATTENTION (discovery) + EXECUTION (signal)
"""

import os
from typing import Dict, List

# ═══════════════════════════════════════════════
# ENVIRONMENT VARIABLES
# ═══════════════════════════════════════════════
BINANCE_API_KEY: str = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET: str = os.getenv("BINANCE_API_SECRET", "")
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
REDIS_URL: str = os.getenv("REDIS_URL", "")
COINMARKETCAL_API_KEY: str = os.getenv("COINMARKETCAL_API_KEY", "")  # free tier
_bal_raw = os.getenv("ACCOUNT_BALANCE_USDT", "").strip()
ACCOUNT_BALANCE_USDT: float = float(_bal_raw) if _bal_raw else 1000.0

# ═══════════════════════════════════════════════
# SYSTEM INFO
# ═══════════════════════════════════════════════
BOT_NAME: str = "Arunabha Manual Scalper"
BOT_VERSION: str = "4.0.0"
DEPLOYMENT: str = "Railway"
REGION: str = os.getenv("RAILWAY_REGION", "asia-southeast1")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT: str = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
LOG_FILE: str = "scalper_v4.log"
LOG_ROTATION_MB: int = 10

# ═══════════════════════════════════════════════
# LAYER A — PAIR UNIVERSE CONFIG
# Pairs are NOT fixed. This section controls discovery.
# ═══════════════════════════════════════════════

# Majors to EXCLUDE from default scan (too crowded for edge)
# Only included if attention score is exceptionally high
CROWDED_MAJORS: List[str] = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
]
# Hard minimum liquidity filter
MIN_24H_VOLUME_USDT: float = 30_000_000    # $30M minimum
MIN_OI_USDT: float = 5_000_000            # $5M OI minimum
MIN_QUOTE_DEPTH_USDT: float = 50_000      # $50K depth within 0.5%
MAX_SPREAD_PCT: float = 0.0015            # 0.15% max spread

# Universe size limits
UNIVERSE_MIN_SIZE: int = 6
UNIVERSE_MAX_SIZE: int = 15
UNIVERSE_REFRESH_MINUTES: int = 20        # rebuild candidate list every 20min

# Fallback pairs if ALL APIs fail completely (last resort only)
FALLBACK_SYMBOLS: List[str] = [
    "LINKUSDT", "AVAXUSDT", "INJUSDT", "TIAUSDT", "SUIUSDT",
    "ARBUSDT", "OPUSDT", "NEARUSDT", "APTUSDT", "STXUSDT",
]

# Category definitions for NARRATIVE LABEL SCORING only.
# ⚠️  These lists do NOT control which pairs are discovered.
#     Discovery comes from CoinGecko trending + Binance top movers.
#     These lists are used as BACKUP label assignment in _detect_category()
#     when keyword matching fails to classify a coin.
#
# Primary category detection uses keyword matching in pair_universe_engine.py
# which works for ANY coin including brand new ones not in these lists.
#
# FALLBACK_SYMBOLS are used only when ALL external APIs fail completely.
# In normal operation, pairs come from live market data.
NARRATIVE_CATEGORIES: Dict[str, List[str]] = {
    "AI":         ["FETUSDT", "RENDERUSDT", "WLDUSDT", "AGIXUSDT", "OCEANUSDT", "TAOUSDT"],
    "MEME":       ["DOGEUSDT", "SHIBUSDT", "PEPEUSDT", "BONKUSDT", "FLOKIUSDT", "WIFUSDT"],
    "GAMING":     ["AXSUSDT", "SANDUSDT", "MANAUSDT", "IMXUSDT", "GALAUSDT", "NOTUSDT"],
    "DEFI":       ["UNIUSDT", "AAVEUSDT", "CRVUSDT", "MKRUSDT", "COMPUSDT", "LDOUSDT"],
    "LAUNCHPAD":  ["BNBUSDT", "INJUSDT", "TIAUSDT", "SEIUSDT", "PYTHUSDT"],
    "LAYER2":     ["ARBUSDT", "OPUSDT", "MATICUSDT", "STRKUSDT", "ZKUSDT"],
    "LAYER1":     ["SOLUSDT", "AVAXUSDT", "NEARUSDT", "APTUSDT", "SUIUSDT"],
    "INFRA":      ["LINKUSDT", "FILUSDT", "ARUSDT", "STXUSDT", "IOTAUSDT"],
    "EXCHANGE":   ["BNBUSDT", "OKBUSDT", "GTUSDT"],
    "PAYMENTS":   ["XRPUSDT", "XLMUSDT", "LTCUSDT", "BCHUSDT"],
    "RWA":        ["ONDO", "POLIXUSDT", "CFGUSDT"],
    "DEPIN":      ["HNTUSDT", "IOTAUSDT", "RENDERUSDT"],
    "SOCIAL":     ["FRIENDUSDT", "DYMOUSDT"],
}

# ═══════════════════════════════════════════════
# LAYER A — ATTENTION SCORING THRESHOLDS
# ═══════════════════════════════════════════════
ATTENTION_MIN_SCORE: float = 35.0         # below this → skip entirely
ATTENTION_HIGH_SCORE: float = 65.0        # above this → priority scan

# Weights for pair_attention_score (sum = 100)
ATTN_WEIGHT_TREND_SEARCH: float = 20.0    # CoinGecko trending rank
ATTN_WEIGHT_NARRATIVE: float = 20.0       # category heat / narrative activity
ATTN_WEIGHT_HYPE_VELOCITY: float = 15.0   # acceleration of attention
ATTN_WEIGHT_DERIV_INTEREST: float = 20.0  # OI change + funding context
ATTN_WEIGHT_VOLUME_SPIKE: float = 15.0    # volume vs 24h avg
ATTN_WEIGHT_LIQUIDITY_QUALITY: float = 10.0  # spread + depth quality

# Penalties
ATTN_PENALTY_CROWDING: float = 15.0       # if funding already extreme
ATTN_PENALTY_EVENT_RISK: float = 10.0     # if near scheduled event for pair
ATTN_PENALTY_CORRELATED: float = 10.0     # if already have correlated position

# Decay
ATTENTION_DECAY_HALFLIFE_HOURS: float = 4.0  # attention score halves every 4h
HYPE_VELOCITY_LOOKBACK_HOURS: int = 2        # rate of change window

# ═══════════════════════════════════════════════
# LAYER A — DERIVATIVES CONTEXT THRESHOLDS
# ═══════════════════════════════════════════════
OI_EXPANSION_THRESHOLD_PCT: float = 0.03   # +3% OI = expansion signal
OI_COLLAPSE_THRESHOLD_PCT: float = -0.03   # -3% OI = collapse signal
OI_LOOKBACK_HOURS: int = 4                 # compare OI now vs 4h ago
OI_EXTREME_LONG_CROWDING_PCT: float = 0.70 # 70%+ longs = crowded long
OI_EXTREME_SHORT_CROWDING_PCT: float = 0.30 # 30% or less longs = crowded short

FUNDING_EXTREME_LONG: float = 0.0005       # above this = crowded longs
FUNDING_EXTREME_SHORT: float = -0.0005     # below this = crowded shorts
FUNDING_CROWDED: float = 0.001             # above = very crowded, reduce size
FUNDING_USEFUL_FADE_LONG: float = 0.0008   # funding > 0.08% → fade longs
FUNDING_USEFUL_FADE_SHORT: float = -0.0008

LIQ_CLUSTER_DETECT_ATR_MULT: float = 2.0  # liq cluster within 2 ATR = setup
LIQ_IMBALANCE_THRESHOLD: float = 0.60     # 60%+ liq on one side = imbalance

# ═══════════════════════════════════════════════
# LAYER B — SIGNAL ENGINE CONFIG
# ═══════════════════════════════════════════════

# Timeframes for scalping
TRIGGER_TF: str = "3m"        # entry trigger
PRIMARY_TF: str = "15m"       # primary structure
BIAS_TF: str = "1h"           # direction bias
CONFIRM_TFS: List[str] = ["1m", "3m", "5m", "15m", "1h"]

# Scan
SCAN_INTERVAL: int = 45        # seconds (slower = less noise)
CANDLE_BUFFER: int = 300       # reduced from 500
CANDLE_BUFFER_MIN: int = 100

# Signal grading (REALISTIC thresholds — not cherry-picked 95)
SCORE_ELITE: int = 88
SCORE_TIER1: int = 78
SCORE_TIER2: int = 68
SCORE_MINIMUM: int = 60        # below this = no signal

# ═══════════════════════════════════════════════
# 7 SCALP SIGNAL TYPES — expiry per type
# ═══════════════════════════════════════════════
SIGNAL_EXPIRY_MINUTES: Dict[str, int] = {
    "HYPE_CONTINUATION_SCALP":      25,
    "LIQUIDITY_SWEEP_REVERSAL":     20,
    "OI_EXPANSION_BREAKOUT":        30,
    "NARRATIVE_MOMENTUM_PULLBACK":  20,
    "SMC_IMBALANCE_RECLAIM":        25,
    "FUNDING_TRAP_FADE":            20,
    "LIQUIDATION_CASCADE_SCALP":    15,
}
SIGNAL_DEFAULT_EXPIRY_MINUTES: int = 20

# Trigger window — if price doesn't hit entry within X min, cancel
SIGNAL_TRIGGER_TIMEOUT_MINUTES: int = 8

# Max chase % — don't enter if price moved more than this past entry
SIGNAL_MAX_CHASE_PCT: float = 0.003        # 0.3%

# Entry drift invalidation (price moves away too far)
SIGNAL_ENTRY_DRIFT_PCT: float = 0.005      # 0.5% drift = stale

# ═══════════════════════════════════════════════
# REGIME THRESHOLDS (improved from v3)
# ═══════════════════════════════════════════════
ADX_TREND: float = 28.0        # was 25 — raised for crypto
ADX_CHOP: float = 22.0         # was 18 — raised for crypto
ADX_TRANSITION: float = 24.0
BB_SQUEEZE: float = 0.10
VOL_LOW: float = 0.30
VOL_MEDIUM: float = 0.60
VOL_HIGH: float = 1.00
CORRELATION_LIMIT: float = 0.70

# ═══════════════════════════════════════════════
# TP / SL
# ═══════════════════════════════════════════════
TP1_RR: float = 1.5
TP2_RR: float = 2.5
TP3_RR: float = 3.5
TP1_SIZE_PCT: float = 0.50
TP2_SIZE_PCT: float = 0.30
TP3_SIZE_PCT: float = 0.20
SL_BUFFER_ATR: float = 0.5
MAX_SL_DISTANCE: float = 0.025    # tighter for scalps: 2.5% max

# Trailing
TRAIL_SL_AFTER_TP1_ATR_MULT: float = 0.5
TRAIL_SL_AFTER_TP2_USE_EMA9: bool = True

# ═══════════════════════════════════════════════
# RISK PROFILES
# ═══════════════════════════════════════════════
# Two selectable profiles. Default: CONTROLLED_SCALP
# Change via env var: RISK_PROFILE=AGGRESSIVE_SCALP
RISK_PROFILE: str = os.getenv("RISK_PROFILE", "CONTROLLED_SCALP")

RISK_PROFILES: Dict[str, Dict] = {
    "CONTROLLED_SCALP": {
        "max_risk_elite":    0.015,   # 1.5% per trade
        "max_risk_tier1":    0.012,
        "max_risk_tier2":    0.008,
        "max_risk_tier3":    0.005,
        "max_daily_loss":    0.04,    # 4%
        "max_weekly_loss":   0.08,    # 8%
        "max_positions":     2,
        "max_corr_exposure": 0.60,
        "cooldown_2loss_min": 45,
        "cooldown_3loss_min": 90,
        "cooldown_4loss_min": 180,
        "narrative_cooldown_min": 120,
    },
    "AGGRESSIVE_SCALP": {
        "max_risk_elite":    0.025,   # 2.5% per trade
        "max_risk_tier1":    0.020,
        "max_risk_tier2":    0.015,
        "max_risk_tier3":    0.010,
        "max_daily_loss":    0.06,    # 6%
        "max_weekly_loss":   0.12,    # 12%
        "max_positions":     3,
        "max_corr_exposure": 0.70,
        "cooldown_2loss_min": 30,
        "cooldown_3loss_min": 60,
        "cooldown_4loss_min": 120,
        "narrative_cooldown_min": 60,
    },
}

# Absolute hard limits (cannot be overridden by profile)
ABS_MAX_DAILY_LOSS: float = 0.07
ABS_MAX_WEEKLY_LOSS: float = 0.15
ABS_MAX_MONTHLY_LOSS: float = 0.20
ABS_MAX_POSITIONS: int = 3
ABS_MAX_SL_DISTANCE: float = 0.03
MIN_ACCOUNT_PCT: float = 0.10

# Drawdown ladder
DRAWDOWN_WARNING: float = 0.05
DRAWDOWN_SERIOUS: float = 0.10
DRAWDOWN_CRITICAL: float = 0.15
DRAWDOWN_EMERGENCY: float = 0.20
DRAWDOWN_NUCLEAR: float = 0.25

# Size reductions
SIZE_REDUCTION_2: float = 0.50
SIZE_REDUCTION_3: float = 0.25
SIZE_REDUCTION_4: float = 0.00

# Uncommon pair size guard
ILLIQUID_PAIR_SIZE_CAP_PCT: float = 0.01
ILLIQUID_PAIR_VOLUME_THRESHOLD: float = 50_000_000

# ═══════════════════════════════════════════════
# NEWS GUARD (4-layer)
# ═══════════════════════════════════════════════
NEWS_GUARD_ENABLED: bool = True

MACRO_HARD_BLOCK_BEFORE_MIN: int = 45
MACRO_HARD_BLOCK_AFTER_MIN: int = 15
MACRO_SOFT_PENALTY_BEFORE_MIN: int = 90

CRYPTO_EVENT_HARD_BLOCK_MIN: int = 30
CRYPTO_EVENT_COOLDOWN_MIN: int = 60

POST_EVENT_REACTION_WINDOW_MIN: int = 30

UNCERTAINTY_MODE_ENABLED: bool = True
UNCERTAINTY_SCORE_THRESHOLD: float = 0.6

HIGH_IMPACT_MACRO_EVENTS: List[str] = [
    "FOMC", "Federal Reserve", "Interest Rate Decision",
    "CPI", "Core CPI", "Inflation Rate",
    "NFP", "Non-Farm Payrolls", "Unemployment Rate",
    "PPI", "Producer Price", "GDP", "Retail Sales",
    "Powell", "Yellen", "ECB Rate", "BOE Rate", "BOJ",
    "Flash Crash", "Circuit Breaker",
    "JOLTS", "ISM Manufacturing", "Core PCE",
]

HIGH_IMPACT_CRYPTO_EVENTS: List[str] = [
    "token unlock", "cliff unlock", "vesting",
    "exchange listing", "exchange delisting",
    "hard fork", "mainnet launch", "mainnet upgrade",
    "protocol migration", "smart contract upgrade",
    "regulatory", "SEC", "CFTC", "ban", "lawsuit",
    "ETF approval", "ETF rejection", "futures listing",
    "hack", "exploit", "rug pull", "exit scam",
    "exchange outage", "withdrawal halt", "KYC enforcement",
    "airdrop", "snapshot", "snapshot date",
]

# ═══════════════════════════════════════════════
# INDICATOR PARAMETERS (optimized for scalp)
# ═══════════════════════════════════════════════
EMA_FAST: int = 9
EMA_MID: int = 21
EMA_SLOW: int = 50
EMA_TREND: int = 200
RSI_PERIOD: int = 14
RSI_OVERBOUGHT: float = 70.0
RSI_OVERSOLD: float = 30.0
MACD_FAST: int = 12
MACD_SLOW: int = 26
MACD_SIGNAL: int = 9
BB_PERIOD: int = 20
BB_STD: float = 2.0
ATR_PERIOD: int = 14
ADX_PERIOD: int = 14
STOCH_K: int = 14
STOCH_D: int = 3
STOCH_SMOOTH: int = 3
VOLUME_SMA: int = 20
VOLUME_ELEVATED: float = 1.30
VOLUME_EXTREME: float = 2.00
VWAP_RESET: str = "daily"

# Anti-chop filters
MIN_EMA_DISTANCE_PCT: float = 0.003
MIN_ATR_PERCENTILE: int = 20
MIN_BB_BANDWIDTH_RATIO: float = 0.10
MIN_VOLUME_RATIO: float = 0.80

# Fakeout detection
FAKEOUT_VOLUME_THRESHOLD: float = 0.80
SWEEP_WICK_RATIO: float = 0.60
OI_DROP_THRESHOLD: float = -0.02
CVD_DIVERGENCE_CANDLES: int = 3

# ═══════════════════════════════════════════════
# EXCHANGE ENDPOINTS
# ═══════════════════════════════════════════════
BINANCE_BASE_URL: str = "https://fapi.binance.com"
BINANCE_WS_BASE: str = "wss://fstream.binance.com/stream?streams="
BINANCE_WS_COMBINED: str = "wss://fstream.binance.com/ws"
BINANCE_RATE_LIMIT_WEIGHT: int = 2400
BINANCE_WS_MAX_STREAMS: int = 200

COINGECKO_BASE: str = "https://api.coingecko.com/api/v3"
CMC_BASE: str = "https://pro-api.coinmarketcap.com/v1"
CMC_API_KEY: str = os.getenv("CMC_API_KEY", "")
COINALYZE_BASE: str = "https://api.coinalyze.net/v1"
COINALYZE_API_KEY: str = os.getenv("COINALYZE_API_KEY", "")
COINMARKETCAL_BASE: str = "https://developers.coinmarketcal.com/v1"

# ═══════════════════════════════════════════════
# SESSION TRACKING (NO 22:00 UTC HARD BLOCK)
# ═══════════════════════════════════════════════
SESSION_ASIAN_START_UTC: int = 0
SESSION_ASIAN_END_UTC: int = 8
SESSION_LONDON_START_UTC: int = 8
SESSION_LONDON_END_UTC: int = 16
SESSION_NY_START_UTC: int = 16
SESSION_NY_END_UTC: int = 24

SESSION_ASIAN_SIZE_MULT: float = 0.70
SESSION_LONDON_SIZE_MULT: float = 1.00
SESSION_NY_SIZE_MULT: float = 1.00
SESSION_ASIAN_SIGNAL_TYPES: List[str] = [
    "LIQUIDITY_SWEEP_REVERSAL",
    "SMC_IMBALANCE_RECLAIM",
    "FUNDING_TRAP_FADE",
]

# ═══════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════
ALERT_COOLDOWN_SECONDS: int = 30
TELEGRAM_MAX_RETRIES: int = 3
TELEGRAM_RETRY_DELAY: float = 2.0
TELEGRAM_HTML_MODE: bool = True
TELEGRAM_TIMEOUT: float = 10.0

# ═══════════════════════════════════════════════
# MONITORING & INFRASTRUCTURE
# ═══════════════════════════════════════════════
HEALTH_CHECK_INTERVAL: int = 30
WS_HEARTBEAT_INTERVAL: int = 20
WS_PONG_TIMEOUT: int = 10
WS_RECONNECT_DELAYS: List[int] = [1, 2, 5, 10, 30, 60]
WS_MAX_RECONNECT: int = 10
WS_QUEUE_MAX: int = 1000
WS_TIMESTAMP_TOLERANCE: int = 5
WS_LATENCY_TARGET_MS: int = 200
DATA_FRESHNESS_SECONDS: int = 10
MEMORY_TARGET_PCT: float = 0.70
MEMORY_ALERT_PCT: float = 0.85
MEMORY_KILL_PCT: float = 0.95
CPU_TARGET_PCT: float = 0.80
CACHE_TTL: int = 60
CACHE_FUNDING_TTL: int = 30
CACHE_OI_TTL: int = 30
MAX_CACHE_SIZE: int = 10_000

# ═══════════════════════════════════════════════
# ML (FIXED from v3)
# ═══════════════════════════════════════════════
ML_MIN_WIN_PROBABILITY: float = 0.55
ML_MIN_SAMPLES_TO_TRAIN: int = 200     # was 30
ML_RETRAIN_EVERY: int = 50            # was 20
ML_LOOKBACK_CANDLES: int = 100
ML_FEATURES: int = 20                 # was 24 with zero-pads

# ═══════════════════════════════════════════════
# SWING / STRUCTURE
# ═══════════════════════════════════════════════
SWING_LOOKBACK: int = 20
EQUAL_LEVEL_TOLERANCE: float = 0.001
STRUCTURE_MIN_TOUCHES: int = 2
REGIME_NEW_CANDLES: int = 10
REGIME_ESTABLISHED_CANDLES: int = 50
REGIME_SIZE_NEW: float = 0.50
REGIME_SIZE_ESTABLISHED: float = 1.00
BREADTH_BULL_THRESHOLD: float = 0.70
BREADTH_BEAR_THRESHOLD: float = 0.30
LIQ_LEVERAGE_ASSUMPTION: float = 10.0
LIQ_CASCADE_OI_PCT: float = 0.05

# ═══════════════════════════════════════════════
# WHALE / ORDERFLOW
# ═══════════════════════════════════════════════
WHALE_THRESHOLD_SMALL: float = 100_000
WHALE_THRESHOLD_MEDIUM: float = 500_000
WHALE_THRESHOLD_LARGE: float = 1_000_000
WHALE_THRESHOLD_HUGE: float = 5_000_000
WHALE_CLUSTER_SECONDS: int = 10
OB_LEVELS: int = 20
OB_WALL_MULTIPLIER_2X: float = 2.0
OB_WALL_MULTIPLIER_3X: float = 3.0

# ═══════════════════════════════════════════════
# CORRELATION
# ═══════════════════════════════════════════════
CORRELATION_LOOKBACK: int = 20
CORRELATION_HIGH: float = 0.85
CORRELATION_PORTFOLIO_LIMIT: float = 0.70
CORRELATION_SIZE_REDUCTION: float = 0.50

# ═══════════════════════════════════════════════
# TIMEZONE
# ═══════════════════════════════════════════════
IST_OFFSET_HOURS: float = 5.5

# ═══════════════════════════════════════════════
# EMOJI MAP
# ═══════════════════════════════════════════════
EMOJI: Dict[str, str] = {
    "LONG": "🟢", "SHORT": "🔴", "ELITE": "⭐", "TIER1": "🔥",
    "TIER2": "✅", "TIER3": "📊", "WARNING": "⚠️", "DANGER": "🚨",
    "INFO": "ℹ️", "PROFIT": "💰", "LOSS": "💸", "WHALE": "🐋",
    "REGIME": "🌊", "HEALTH": "💚", "DEAD": "💀", "BULL": "🐂",
    "BEAR": "🐻", "CHOP": "🔄", "SWEEP": "🎯", "BLOCK": "🚫",
    "CLOCK": "🕐", "CHART": "📈", "FIRE": "🔥", "HYPE": "🚀",
    "NEWS": "📰", "DERIV": "📊", "SKIP": "⏭️", "EXPIRY": "⏰",
    "ATTENTION": "👀", "NARRATIVE": "📖", "OI": "📉", "FUNDING": "💫",
    "QUALITY_A+": "🏆", "QUALITY_A": "🥇", "QUALITY_B": "🥈",
    "SKIP_LATE": "🚫",
}

def get_risk_profile() -> Dict:
    return RISK_PROFILES.get(RISK_PROFILE, RISK_PROFILES["CONTROLLED_SCALP"])
