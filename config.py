"""
ARUNABHA ELITE SCALPER v3.0
FILE 1/18: config.py
All configuration — Railway env vars + hardcoded constants
NO external files. NO YAML. Everything here.
"""

import os
from typing import Dict, List

# ═══════════════════════════════════════════════
# ENVIRONMENT VARIABLES (Railway secrets)
# ═══════════════════════════════════════════════
BINANCE_API_KEY: str = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET: str = os.getenv("BINANCE_API_SECRET", "")
BYBIT_API_KEY: str = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET: str = os.getenv("BYBIT_API_SECRET", "")
OKX_API_KEY: str = os.getenv("OKX_API_KEY", "")
OKX_API_SECRET: str = os.getenv("OKX_API_SECRET", "")
OKX_PASSPHRASE: str = os.getenv("OKX_PASSPHRASE", "")
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
ADMIN_PASSWORD: str = os.getenv("ADMIN_PASSWORD", "elite2024")
REDIS_URL: str = os.getenv("REDIS_URL", "")
ACCOUNT_BALANCE_USDT: float = float(os.getenv("ACCOUNT_BALANCE_USDT", "1000"))

# ═══════════════════════════════════════════════
# SYMBOLS & TIMEFRAMES
# ═══════════════════════════════════════════════
SYMBOLS: List[str] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "POLUSDT",
]
TIMEFRAMES: List[str] = ["5m", "15m", "1h", "4h"]
PRIMARY_TF: str = "15m"
HTF_LIST: List[str] = ["1h", "4h"]
LTF: str = "5m"
SCAN_INTERVAL: int = 30          # seconds between scans
CANDLE_BUFFER: int = 500         # candles per symbol per TF
CANDLE_BUFFER_MIN: int = 300     # minimum before analysis

# ═══════════════════════════════════════════════
# EXCHANGE ENDPOINTS
# ═══════════════════════════════════════════════
BINANCE_BASE_URL: str = "https://fapi.binance.com"
BINANCE_WS_BASE: str = "wss://fstream.binance.com/stream?streams="
BINANCE_WS_COMBINED: str = "wss://fstream.binance.com/ws"
BINANCE_RATE_LIMIT_WEIGHT: int = 2400       # per minute
BINANCE_RATE_LIMIT_ORDERS: int = 1200
BINANCE_WS_MAX_STREAMS: int = 200

BYBIT_BASE_URL: str = "https://api.bybit.com"
BYBIT_WS_URL: str = "wss://stream.bybit.com/v5/public/linear"

OKX_BASE_URL: str = "https://www.okx.com"
OKX_WS_URL: str = "wss://ws.okx.com:8443/ws/v5/public"

# ═══════════════════════════════════════════════
# RISK PARAMETERS
# ═══════════════════════════════════════════════
MAX_LEVERAGE: int = 5
MAX_RISK_PER_TRADE: float = 0.02            # 2% absolute cap
MAX_POSITIONS: int = 3
MAX_DAILY_LOSS: float = 0.05                # 5%
MAX_WEEKLY_LOSS: float = 0.10               # 10%
MAX_MONTHLY_LOSS: float = 0.15              # 15%
MAX_SL_DISTANCE: float = 0.03              # 3% from entry
MIN_ACCOUNT_PCT: float = 0.10              # stop if balance < 10% of start

MAX_RISK_TIER: Dict[str, float] = {
    "ELITE": 0.020,
    "TIER1": 0.015,
    "TIER2": 0.010,
    "TIER3": 0.005,
}

# Drawdown levels → actions
DRAWDOWN_WARNING: float = 0.05
DRAWDOWN_SERIOUS: float = 0.10
DRAWDOWN_CRITICAL: float = 0.15
DRAWDOWN_EMERGENCY: float = 0.20
DRAWDOWN_NUCLEAR: float = 0.25

# Cooldown after consecutive losses (minutes)
COOLDOWN_2_LOSS: int = 30
COOLDOWN_3_LOSS: int = 60
COOLDOWN_4_LOSS: int = 120
COOLDOWN_5_LOSS: int = 1440        # 24h

# Size reductions after N consecutive losses
SIZE_REDUCTION_2: float = 0.50
SIZE_REDUCTION_3: float = 0.25
SIZE_REDUCTION_4: float = 0.00    # no new trades

# Time-based restrictions
NO_TRADE_AFTER_UTC: int = 22      # no new trades after 22:00 UTC
WEEKEND_SIZE_REDUCTION: float = 0.75
PRE_EVENT_REDUCTION: float = 0.50
PRE_EVENT_MINUTES: int = 30       # minutes before high-impact event

# ═══════════════════════════════════════════════
# CONFLUENCE SCORING WEIGHTS (sum = 100)
# ═══════════════════════════════════════════════
WEIGHT_TREND_ALIGNMENT: int = 20
WEIGHT_MOMENTUM: int = 15
WEIGHT_VOLUME: int = 15
WEIGHT_STRUCTURE: int = 15
WEIGHT_ORDERBOOK: int = 10
WEIGHT_FUNDING: int = 10
WEIGHT_VOLATILITY_FIT: int = 10
WEIGHT_BTC_CONTEXT: int = 5

# Score thresholds → signal grade
SCORE_ELITE: int = 95
SCORE_TIER1: int = 85
SCORE_TIER2: int = 75
SCORE_TIER3: int = 65
SCORE_MINIMUM: int = 65

# ═══════════════════════════════════════════════
# REGIME THRESHOLDS
# ═══════════════════════════════════════════════
ADX_TREND: float = 25.0
ADX_CHOP: float = 18.0
ADX_TRANSITION: float = 20.0
BB_SQUEEZE: float = 0.10
VOL_LOW: float = 0.30
VOL_MEDIUM: float = 0.60
VOL_HIGH: float = 1.00
FUNDING_EXTREME_LONG: float = 0.0005
FUNDING_EXTREME_SHORT: float = -0.0005
FUNDING_CROWDED: float = 0.001
CORRELATION_LIMIT: float = 0.70

# ═══════════════════════════════════════════════
# VOLATILITY REGIME SIZE MULTIPLIERS
# ═══════════════════════════════════════════════
VOL_SIZE_LOW: float = 1.00
VOL_SIZE_MEDIUM: float = 0.75
VOL_SIZE_HIGH: float = 0.50
VOL_SIZE_EXTREME: float = 0.25

# ═══════════════════════════════════════════════
# INDICATOR PARAMETERS
# ═══════════════════════════════════════════════
EMA_FAST: int = 9
EMA_MID: int = 21
EMA_SLOW: int = 50
EMA_TREND: int = 200
SMA_SHORT: int = 20
SMA_MID: int = 50
SMA_LONG: int = 200
RSI_PERIOD: int = 14
RSI_OVERBOUGHT: float = 70.0
RSI_OVERSOLD: float = 30.0
RSI_CHOP_HIGH: float = 55.0
RSI_CHOP_LOW: float = 45.0
RSI_CHOP_CANDLES: int = 10
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
VOLUME_DRY: float = 0.70
VOLUME_ELEVATED: float = 1.30
VOLUME_EXTREME: float = 2.00
VWAP_RESET: str = "daily"

# ═══════════════════════════════════════════════
# ANTI-CHOP FILTERS
# ═══════════════════════════════════════════════
MIN_EMA_DISTANCE_PCT: float = 0.003     # 0.3% between EMA9 and EMA21
MIN_ATR_PERCENTILE: int = 20
MIN_BB_BANDWIDTH_RATIO: float = 0.10
MIN_VOLUME_RATIO: float = 0.80

# ═══════════════════════════════════════════════
# FAKEOUT / LIQUIDITY SWEEP DETECTION
# ═══════════════════════════════════════════════
FAKEOUT_VOLUME_THRESHOLD: float = 0.80
SWEEP_WICK_RATIO: float = 0.60         # wick > 60% of candle = sweep
OI_DROP_THRESHOLD: float = -0.02       # -2% OI on breakout = no conviction
CVD_DIVERGENCE_CANDLES: int = 3

# ═══════════════════════════════════════════════
# ORDERBOOK
# ═══════════════════════════════════════════════
OB_LEVELS: int = 20
OB_WALL_MULTIPLIER_2X: float = 2.0
OB_WALL_MULTIPLIER_3X: float = 3.0
OB_WALL_MULTIPLIER_5X: float = 5.0
OB_LIQUIDITY_DEPTH_PCT: float = 0.01   # 1% from mid

# ═══════════════════════════════════════════════
# LARGE TRADE DETECTION
# ═══════════════════════════════════════════════
WHALE_THRESHOLD_SMALL: float = 100_000     # 100K USDT
WHALE_THRESHOLD_MEDIUM: float = 500_000
WHALE_THRESHOLD_LARGE: float = 1_000_000
WHALE_THRESHOLD_HUGE: float = 5_000_000
WHALE_CLUSTER_SECONDS: int = 10

# ═══════════════════════════════════════════════
# SIGNAL LIFECYCLE
# ═══════════════════════════════════════════════
SIGNAL_EXPIRY_MINUTES: int = 15
SIGNAL_TRIGGER_TIMEOUT_MINUTES: int = 5
SIGNAL_ENTRY_TOLERANCE_PCT: float = 0.002   # 0.2% from entry price

# ═══════════════════════════════════════════════
# TAKE PROFIT / STOP LOSS
# ═══════════════════════════════════════════════
TP1_RR: float = 1.5    # 1.5R
TP2_RR: float = 2.5    # 2.5R
TP3_RR: float = 4.0    # 4R (if Elite/Tier1)
TP1_SIZE_PCT: float = 0.50   # close 50% at TP1
TP2_SIZE_PCT: float = 0.30   # close 30% at TP2
TP3_SIZE_PCT: float = 0.20   # trail remaining 20%
SL_BUFFER_ATR: float = 0.5   # SL = structure ± 0.5 ATR

# ═══════════════════════════════════════════════
# CROSS-EXCHANGE VALIDATION
# ═══════════════════════════════════════════════
PRICE_DIVERGENCE_LIMIT: float = 0.003   # 0.3% max between exchanges
VALIDATION_TIMEOUT: float = 2.0         # seconds

# ═══════════════════════════════════════════════
# ML / QUALITY CHECK
# ═══════════════════════════════════════════════
ML_MIN_WIN_PROBABILITY: float = 0.55    # block if predicted win < 55%
ML_LOOKBACK_CANDLES: int = 100
ML_FEATURES: int = 24

# ═══════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════
ALERT_COOLDOWN_SECONDS: int = 30
TELEGRAM_MAX_RETRIES: int = 3
TELEGRAM_RETRY_DELAY: float = 2.0
TELEGRAM_HTML_MODE: bool = True
TELEGRAM_TIMEOUT: float = 10.0

# ═══════════════════════════════════════════════
# MONITORING & HEALTH
# ═══════════════════════════════════════════════
HEALTH_CHECK_INTERVAL: int = 30         # seconds
WS_LATENCY_TARGET_MS: int = 200
API_LATENCY_TARGET_MS: int = 500
DATA_FRESHNESS_SECONDS: int = 10
MEMORY_TARGET_PCT: float = 0.70
MEMORY_ALERT_PCT: float = 0.85
MEMORY_KILL_PCT: float = 0.95
CPU_TARGET_PCT: float = 0.80
ERROR_RATE_TARGET: float = 0.05
LOG_ROTATION_MB: int = 10
WS_HEARTBEAT_INTERVAL: int = 20
WS_PONG_TIMEOUT: int = 10
WS_RECONNECT_DELAYS: List[int] = [1, 2, 5, 10, 30, 60]
WS_MAX_RECONNECT: int = 10
WS_QUEUE_MAX: int = 1000
WS_TIMESTAMP_TOLERANCE: int = 5        # reject msgs older than 5s

# ═══════════════════════════════════════════════
# CACHE
# ═══════════════════════════════════════════════
CACHE_TTL: int = 60
CACHE_FUNDING_TTL: int = 30
CACHE_OI_TTL: int = 30
CACHE_STATS_TTL: int = 300
MAX_CACHE_SIZE: int = 10_000

# ═══════════════════════════════════════════════
# SWING DETECTION
# ═══════════════════════════════════════════════
SWING_LOOKBACK: int = 20
EQUAL_LEVEL_TOLERANCE: float = 0.001    # 0.1% for equal highs/lows
STRUCTURE_MIN_TOUCHES: int = 2

# ═══════════════════════════════════════════════
# REGIME PERSISTENCE
# ═══════════════════════════════════════════════
REGIME_NEW_CANDLES: int = 10       # regime age < 10 = uncertain
REGIME_ESTABLISHED_CANDLES: int = 50
REGIME_SIZE_NEW: float = 0.50      # 50% size in new regime
REGIME_SIZE_ESTABLISHED: float = 1.00

# ═══════════════════════════════════════════════
# MARKET BREADTH
# ═══════════════════════════════════════════════
BREADTH_BULL_THRESHOLD: float = 0.70   # 70% symbols above EMA50 = broad bull
BREADTH_BEAR_THRESHOLD: float = 0.30

# ═══════════════════════════════════════════════
# LIQUIDATION ESTIMATION
# ═══════════════════════════════════════════════
LIQ_LEVERAGE_ASSUMPTION: float = 10.0  # assume avg retail leverage
LIQ_CASCADE_OI_PCT: float = 0.05       # 5% OI at level = cascade risk

# ═══════════════════════════════════════════════
# TIMEZONE
# ═══════════════════════════════════════════════
IST_OFFSET_HOURS: float = 5.5          # UTC+5:30

# ═══════════════════════════════════════════════
# EMOJI / COLOR MAP FOR TELEGRAM
# ═══════════════════════════════════════════════
EMOJI: Dict[str, str] = {
    "LONG": "🟢",
    "SHORT": "🔴",
    "ELITE": "⭐",
    "TIER1": "🔥",
    "TIER2": "✅",
    "TIER3": "📊",
    "WARNING": "⚠️",
    "DANGER": "🚨",
    "INFO": "ℹ️",
    "PROFIT": "💰",
    "LOSS": "💸",
    "WHALE": "🐋",
    "REGIME": "🌊",
    "HEALTH": "💚",
    "DEAD": "💀",
    "BULL": "🐂",
    "BEAR": "🐻",
    "CHOP": "🔄",
    "SWEEP": "🎯",
    "BLOCK": "🚫",
    "CLOCK": "🕐",
    "CHART": "📈",
    "FIRE": "🔥",
}

SIGNAL_COLORS: Dict[str, str] = {
    "ELITE": "#FFD700",
    "TIER1": "#FF6B35",
    "TIER2": "#4CAF50",
    "TIER3": "#2196F3",
    "BLOCKED": "#9E9E9E",
}

# ═══════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT: str = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
LOG_FILE: str = "elite_scalper.log"

# ═══════════════════════════════════════════════
# SYSTEM INFO
# ═══════════════════════════════════════════════
BOT_NAME: str = "Arunabha Elite Scalper"
BOT_VERSION: str = "3.0.0"
DEPLOYMENT: str = "Railway"
REGION: str = os.getenv("RAILWAY_REGION", "asia-southeast1")

