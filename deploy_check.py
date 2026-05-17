"""
ARUNABHA ELITE SCALPER v3.0
FILE 18/18: deploy_check.py
Pre-deployment validation — run this BEFORE deploying to Railway
Checks: env vars, Binance connectivity, Telegram, indicator math
"""

import asyncio
import logging
import os
import sys
import time
from typing import List, Tuple

import aiohttp
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("deploy_check")


REQUIRED_ENV = [
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "BINANCE_API_KEY",
    "ACCOUNT_BALANCE_USDT",
]

OPTIONAL_ENV = [
    "BINANCE_API_SECRET",
    "BYBIT_API_KEY",
    "OKX_API_KEY",
    "ADMIN_PASSWORD",
    "REDIS_URL",
    "LOG_LEVEL",
]

PASS = "✅"
FAIL = "❌"
WARN = "⚠️"


class DeployCheck:
    def __init__(self):
        self._results: List[Tuple[str, bool, str]] = []

    def _record(self, name: str, passed: bool, detail: str = ""):
        emoji = PASS if passed else FAIL
        self._results.append((name, passed, detail))
        log.info(f"{emoji} {name}" + (f" — {detail}" if detail else ""))

    # ═══════════════════════════════════════════
    # CHECKS
    # ═══════════════════════════════════════════

    def check_env_vars(self):
        log.info("\n── ENV VARS ──")
        for var in REQUIRED_ENV:
            val = os.getenv(var, "")
            self._record(f"ENV: {var}", bool(val), "set" if val else "MISSING")

        for var in OPTIONAL_ENV:
            val = os.getenv(var, "")
            emoji = WARN if not val else PASS
            log.info(f"{emoji} OPTIONAL: {var} — {'set' if val else 'not set'}")

    async def check_binance(self, session: aiohttp.ClientSession):
        log.info("\n── BINANCE CONNECTIVITY ──")
        base = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com")

        # Ping
        try:
            async with session.get(f"{base}/fapi/v1/ping", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                self._record("Binance Futures ping", resp.status == 200, f"HTTP {resp.status}")
        except Exception as e:
            self._record("Binance Futures ping", False, str(e))

        # Server time
        try:
            async with session.get(f"{base}/fapi/v1/time", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    server_ts = data["serverTime"] / 1000
                    local_ts = time.time()
                    diff_ms = abs(server_ts - local_ts) * 1000
                    self._record("Binance time sync", diff_ms < 1000, f"Δ{diff_ms:.0f}ms")
                else:
                    self._record("Binance time sync", False, f"HTTP {resp.status}")
        except Exception as e:
            self._record("Binance time sync", False, str(e))

        # Kline fetch
        try:
            params = {"symbol": "BTCUSDT", "interval": "15m", "limit": "5"}
            async with session.get(f"{base}/fapi/v1/klines", params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self._record("Binance klines fetch", len(data) > 0, f"{len(data)} candles")
                else:
                    self._record("Binance klines fetch", False, f"HTTP {resp.status}")
        except Exception as e:
            self._record("Binance klines fetch", False, str(e))

        # Funding rate
        try:
            async with session.get(f"{base}/fapi/v1/premiumIndex", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                self._record("Binance funding rates", resp.status == 200, f"HTTP {resp.status}")
        except Exception as e:
            self._record("Binance funding rates", False, str(e))

    async def check_telegram(self, session: aiohttp.ClientSession):
        log.info("\n── TELEGRAM ──")
        token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

        if not token or not chat_id:
            self._record("Telegram config", False, "Token or chat_id missing")
            return

        try:
            url = f"https://api.telegram.org/bot{token}/getMe"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    bot_name = data["result"].get("username", "unknown")
                    self._record("Telegram bot auth", True, f"@{bot_name}")
                else:
                    self._record("Telegram bot auth", False, f"HTTP {resp.status}")
        except Exception as e:
            self._record("Telegram bot auth", False, str(e))

        # Send test message
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": "✅ <b>Deploy Check</b> — Arunabha Elite Scalper v3.0 connectivity test passed",
                "parse_mode": "HTML",
            }
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                self._record("Telegram send message", resp.status == 200, f"HTTP {resp.status}")
        except Exception as e:
            self._record("Telegram send message", False, str(e))

    def check_indicator_math(self):
        log.info("\n── INDICATOR MATH ──")
        try:
            sys.path.insert(0, os.path.dirname(__file__))
            from data_processor import IndicatorCalc
            calc = IndicatorCalc()

            # Generate synthetic price data
            np.random.seed(42)
            prices = 50000 + np.cumsum(np.random.randn(300) * 100)
            highs = prices + np.abs(np.random.randn(300) * 50)
            lows = prices - np.abs(np.random.randn(300) * 50)
            vols = np.abs(np.random.randn(300) * 1000) + 500

            # EMA
            ema9 = calc.ema(prices, 9)
            self._record("EMA calculation", not np.isnan(ema9[-1]), f"EMA9={ema9[-1]:.2f}")

            # RSI
            rsi = calc.rsi(prices, 14)
            valid_rsi = 0 <= rsi[-1] <= 100
            self._record("RSI calculation", valid_rsi, f"RSI={rsi[-1]:.2f}")

            # ATR
            atr = calc.atr(highs, lows, prices)
            self._record("ATR calculation", atr[-1] > 0, f"ATR={atr[-1]:.2f}")

            # ADX
            adx_arr, pdi, mdi = calc.adx(highs, lows, prices)
            self._record("ADX calculation", adx_arr[-1] > 0, f"ADX={adx_arr[-1]:.2f}")

            # MACD
            m, ms, mh = calc.macd(prices)
            self._record("MACD calculation", not np.isnan(m[-1]), f"MACD={m[-1]:.4f}")

            # Bollinger
            bu, bm, bl = calc.bollinger(prices)
            bb_valid = bu[-1] > bm[-1] > bl[-1]
            self._record("Bollinger Bands", bb_valid, f"Upper={bu[-1]:.2f}")

        except ImportError:
            self._record("Indicator math", False, "data_processor.py not found in path")
        except Exception as e:
            self._record("Indicator math", False, str(e))

    def check_config(self):
        log.info("\n── CONFIG VALIDATION ──")
        try:
            import config as cfg

            # Score weights sum
            weight_sum = (
                cfg.WEIGHT_TREND_ALIGNMENT + cfg.WEIGHT_MOMENTUM + cfg.WEIGHT_VOLUME +
                cfg.WEIGHT_STRUCTURE + cfg.WEIGHT_ORDERBOOK + cfg.WEIGHT_FUNDING +
                cfg.WEIGHT_VOLATILITY_FIT + cfg.WEIGHT_BTC_CONTEXT
            )
            self._record("Score weights sum to 100", weight_sum == 100, f"Sum={weight_sum}")

            # Risk sanity
            self._record("MAX_RISK sane", 0 < cfg.MAX_RISK_PER_TRADE <= 0.05, f"{cfg.MAX_RISK_PER_TRADE:.0%}")
            self._record("MAX_LEVERAGE sane", 1 <= cfg.MAX_LEVERAGE <= 10, f"{cfg.MAX_LEVERAGE}x")
            self._record("Symbols not empty", len(cfg.SYMBOLS) > 0, f"{len(cfg.SYMBOLS)} symbols")
            self._record("POLUSDT replaces MATICUSDT",
                         "POLUSDT" in cfg.SYMBOLS and "MATICUSDT" not in cfg.SYMBOLS,
                         "Symbol list correct")

        except ImportError:
            self._record("Config import", False, "config.py not found")
        except Exception as e:
            self._record("Config validation", False, str(e))

    def check_config(self):
        log.info("\n── CONFIG VALIDATION ──")
        try:
            import config as cfg

            # Score weights sum
            weight_sum = (
                cfg.WEIGHT_TREND_ALIGNMENT + cfg.WEIGHT_MOMENTUM + cfg.WEIGHT_VOLUME +
                cfg.WEIGHT_STRUCTURE + cfg.WEIGHT_ORDERBOOK + cfg.WEIGHT_FUNDING +
                cfg.WEIGHT_VOLATILITY_FIT + cfg.WEIGHT_BTC_CONTEXT
            )
            self._record("Score weights sum to 100", weight_sum == 100, f"Sum={weight_sum}")
            self._record("MAX_RISK sane", 0 < cfg.MAX_RISK_PER_TRADE <= 0.05, f"{cfg.MAX_RISK_PER_TRADE:.0%}")
            self._record("MAX_LEVERAGE sane", 1 <= cfg.MAX_LEVERAGE <= 10, f"{cfg.MAX_LEVERAGE}x")
            self._record("Symbols not empty", len(cfg.SYMBOLS) > 0, f"{len(cfg.SYMBOLS)} symbols")
            self._record("POLUSDT replaces MATICUSDT",
                         "POLUSDT" in cfg.SYMBOLS and "MATICUSDT" not in cfg.SYMBOLS,
                         "Symbol list correct")
            self._record("ACCOUNT_BALANCE_USDT non-zero",
                         cfg.ACCOUNT_BALANCE_USDT > 0, f"${cfg.ACCOUNT_BALANCE_USDT:.2f}")
            self._record("Session constants present",
                         hasattr(cfg, "SESSION_ASIAN_SIZE_MULT"), "ok")
            self._record("BTC DOM constants present",
                         hasattr(cfg, "BTC_DOM_RISING_ALT_LONG_MULT"), "ok")
            self._record("Correlation constants present",
                         hasattr(cfg, "CORRELATION_HIGH"), "ok")
            self._record("Trailing stop constants present",
                         hasattr(cfg, "TRAIL_SL_AFTER_TP1_ATR_MULT"), "ok")
            self._record("3-tap constants present",
                         hasattr(cfg, "THREE_TAP_LOOKBACK"), "ok")
            self._record("Funding trend constants present",
                         hasattr(cfg, "FUNDING_TREND_LOOKBACK"), "ok")

        except ImportError:
            self._record("Config import", False, "config.py not found")
        except Exception as e:
            self._record("Config validation", False, str(e))

    def check_new_files(self):
        log.info("\n── NEW MODULE FILES ──")
        required_files = [
            "btc_dominance.py",
            "correlation_engine.py",
            "session_tracker.py",
            "events_calendar.py",
            "ml_engine.py",
            "state_manager.py",
            "monitoring.py",
            "cache_manager.py",
            "utils.py",
            "backtest_engine.py",
            "liquidity_engine.py",
            "market_regime_engine.py",
            "orderflow_engine.py",
            "websocket_engine.py",
            "data_processor.py",
            "signal_engine.py",
            "risk_engine.py",
            "telegram_bot.py",
            "main.py",
            "config.py",
        ]
        for fname in required_files:
            exists = os.path.exists(fname)
            self._record(f"file: {fname}", exists, "present" if exists else "MISSING")
        log.info("\n── PYTHON DEPENDENCIES ──")
        deps = [
            ("aiohttp", "aiohttp"),
            ("websockets", "websockets"),
            ("numpy", "numpy"),
            ("psutil", "psutil"),
        ]
        optional_deps = [
            ("sklearn", "scikit-learn"),
            ("pandas", "pandas"),
        ]
        for module, name in deps:
            try:
                __import__(module)
                import importlib.metadata
                ver = importlib.metadata.version(name)
                self._record(f"dep: {name}", True, f"v{ver}")
            except Exception as e:
                self._record(f"dep: {name}", False, str(e))

        for module, name in optional_deps:
            try:
                __import__(module)
                self._record(f"optional: {name}", True, "available")
            except ImportError:
                log.info(f"{WARN} optional: {name} — not installed (ML in fallback mode)")

    # ═══════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════

    def print_summary(self):
        log.info("\n══════════════════════════════")
        log.info("DEPLOY CHECK SUMMARY")
        log.info("══════════════════════════════")

        passed = [r for r in self._results if r[1]]
        failed = [r for r in self._results if not r[1]]

        log.info(f"{PASS} Passed: {len(passed)}")
        log.info(f"{FAIL} Failed: {len(failed)}")

        if failed:
            log.info("\nFailed checks:")
            for name, _, detail in failed:
                log.info(f"  {FAIL} {name}: {detail}")
            log.info("\n🚫 FIX FAILURES BEFORE DEPLOYING")
            return False
        else:
            log.info("\n🚀 ALL CHECKS PASSED — READY TO DEPLOY")
            return True


async def main():
    checker = DeployCheck()

    checker.check_env_vars()
    checker.check_config()
    checker.check_new_files()
    checker.check_dependencies()
    checker.check_indicator_math()

    async with aiohttp.ClientSession() as session:
        await checker.check_binance(session)
        await checker.check_telegram(session)

    success = checker.print_summary()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())
