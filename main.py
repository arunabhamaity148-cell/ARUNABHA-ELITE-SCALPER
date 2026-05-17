"""
ARUNABHA ELITE SCALPER v3.0
FILE 2/18: main.py
Asyncio entry point — startup, main loop, graceful shutdown
Railway-compatible: single process, env vars, health endpoint
"""

import asyncio
import logging
import logging.handlers
import os
import signal
import sys
import traceback
from datetime import datetime, timezone
from typing import Optional

import psutil

import config
from btc_dominance import BTCDominanceTracker
from correlation_engine import CorrelationEngine
from data_processor import DataProcessor
from events_calendar import EventsCalendar
from liquidity_engine import LiquidityEngine
from market_regime_engine import MarketRegimeEngine
from ml_engine import MLEngine
from monitoring import MonitoringEngine
from orderflow_engine import OrderflowEngine
from risk_engine import RiskEngine
from session_tracker import SessionTracker
from signal_engine import SignalEngine
from state_manager import StateManager
from telegram_bot import TelegramBot
from websocket_engine import WebsocketEngine

# ═══════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("elite")
    logger.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))

    formatter = logging.Formatter(config.LOG_FORMAT)

    # Console handler (Railway captures stdout)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Rotating file handler
    try:
        fh = logging.handlers.RotatingFileHandler(
            config.LOG_FILE,
            maxBytes=config.LOG_ROTATION_MB * 1024 * 1024,
            backupCount=3,
        )
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    except Exception:
        pass  # File logging optional on Railway

    return logger


# ═══════════════════════════════════════════════
# MAIN APPLICATION
# ═══════════════════════════════════════════════

class EliteScalper:
    def __init__(self):
        self.logger = setup_logging()
        self.log = self.logger
        self._shutdown_event = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

        # Core components (initialized in startup)
        self.state: Optional[StateManager] = None
        self.telegram: Optional[TelegramBot] = None
        self.data_processor: Optional[DataProcessor] = None
        self.ws_engine: Optional[WebsocketEngine] = None
        self.orderflow: Optional[OrderflowEngine] = None
        self.liquidity: Optional[LiquidityEngine] = None
        self.regime: Optional[MarketRegimeEngine] = None
        self.signal_engine: Optional[SignalEngine] = None
        self.risk_engine: Optional[RiskEngine] = None
        self.ml_engine: Optional[MLEngine] = None
        self.monitor: Optional[MonitoringEngine] = None
        self.events_calendar: Optional[EventsCalendar] = None
        self.btc_dominance: Optional[BTCDominanceTracker] = None
        self.correlation_engine: Optional[CorrelationEngine] = None
        self.session_tracker: Optional[SessionTracker] = None

    # ───────────────────────────────────────────
    # STARTUP
    # ───────────────────────────────────────────

    async def startup(self) -> bool:
        self.log.info(f"═══ {config.BOT_NAME} v{config.BOT_VERSION} STARTING ═══")
        self.log.info(f"Region: {config.REGION} | PID: {os.getpid()}")

        try:
            # 1. Validate environment
            if not self._validate_env():
                return False

            # 2. State manager
            self.state = StateManager()
            await self.state.load()
            self.log.info("✅ State manager ready")

            # 3. Telegram (early — for startup alerts)
            self.telegram = TelegramBot(self.state)
            await self.telegram.test_connection()
            self.log.info("✅ Telegram connected")

            # 4. Data processor
            self.data_processor = DataProcessor()
            self.log.info("✅ Data processor ready")

            # 5. WebSocket engine
            self.ws_engine = WebsocketEngine(self.data_processor)
            self.log.info("✅ WebSocket engine ready")

            # 6. Orderflow engine
            self.orderflow = OrderflowEngine(self.data_processor, self.telegram)
            self.log.info("✅ Orderflow engine ready")

            # 7. Liquidity engine
            self.liquidity = LiquidityEngine(self.data_processor)
            self.log.info("✅ Liquidity engine ready")

            # 8. Regime engine
            self.regime = MarketRegimeEngine(self.data_processor)
            self.log.info("✅ Regime engine ready")

            # 9. ML engine
            self.ml_engine = MLEngine()
            await self.ml_engine.initialize()
            self.log.info("✅ ML engine ready")

            # 10. Risk engine
            self.risk_engine = RiskEngine(self.state)
            self.log.info("✅ Risk engine ready")

            # 10a. Events Calendar → inject into risk engine
            self.events_calendar = EventsCalendar()
            await self.events_calendar.refresh()
            self.risk_engine.set_events_calendar(self.events_calendar)
            self.log.info("✅ Events calendar ready")

            # 10b. Session tracker
            self.session_tracker = SessionTracker()
            self.log.info("✅ Session tracker ready")

            # 10c. BTC Dominance tracker
            self.btc_dominance = BTCDominanceTracker()
            await self.btc_dominance.fetch()
            self.log.info("✅ BTC Dominance tracker ready")

            # 10d. Correlation engine
            self.correlation_engine = CorrelationEngine(self.data_processor)
            self.log.info("✅ Correlation engine ready")

            # 11. Signal engine
            self.signal_engine = SignalEngine(
                data_processor=self.data_processor,
                orderflow=self.orderflow,
                liquidity=self.liquidity,
                regime=self.regime,
                risk_engine=self.risk_engine,
                ml_engine=self.ml_engine,
                telegram=self.telegram,
                state=self.state,
                btc_dominance=self.btc_dominance,
                correlation_engine=self.correlation_engine,
                session_tracker=self.session_tracker,
            )
            self.log.info("✅ Signal engine ready")

            # 12. Monitoring
            self.monitor = MonitoringEngine(
                state=self.state,
                telegram=self.telegram,
                ws_engine=self.ws_engine,
            )
            self.log.info("✅ Monitoring engine ready")

            await self.telegram.send_startup_message()
            return True

        except Exception as e:
            self.log.critical(f"STARTUP FAILED: {e}\n{traceback.format_exc()}")
            return False

    def _validate_env(self) -> bool:
        required = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "BINANCE_API_KEY"]
        missing = [v for v in required if not os.getenv(v)]
        if missing:
            self.log.critical(f"Missing env vars: {missing}")
            return False
        self.log.info(f"Symbols: {config.SYMBOLS}")
        self.log.info(f"Account balance: ${config.ACCOUNT_BALANCE_USDT:.2f}")
        return True

    # ───────────────────────────────────────────
    # MAIN LOOP
    # ───────────────────────────────────────────

    async def main_loop(self):
        self.log.info("🚀 Main loop started")
        scan_count = 0

        while not self._shutdown_event.is_set():
            loop_start = asyncio.get_event_loop().time()
            scan_count += 1

            try:
                await self._run_scan(scan_count)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Scan #{scan_count} error: {e}\n{traceback.format_exc()}")
                # Don't crash — log and continue
                try:
                    await self.telegram.send_alert(
                        f"⚠️ Scan error (non-fatal): {str(e)[:200]}", priority="low"
                    )
                except Exception:
                    pass

            # Sleep until next scan
            elapsed = asyncio.get_event_loop().time() - loop_start
            sleep_time = max(0, config.SCAN_INTERVAL - elapsed)
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=sleep_time
                )
            except asyncio.TimeoutError:
                pass

    async def _run_scan(self, scan_count: int):
        now = datetime.now(timezone.utc)

        # 1. Check risk limits before scanning
        if not self.risk_engine.can_scan(now):
            return

        # 1b. Update trailing stops on active signals
        try:
            await self.signal_engine.update_trailing_stops()
        except Exception as e:
            self.log.debug(f"Trailing stop update error: {e}")

        # 2. Update regime (once per scan, all symbols)
        regimes = {}
        for symbol in config.SYMBOLS:
            try:
                regime = await self.regime.classify(symbol)
                regimes[symbol] = regime
            except Exception as e:
                self.log.debug(f"Regime error {symbol}: {e}")

        # 3. Generate signals for each symbol
        signals = []
        for symbol in config.SYMBOLS:
            try:
                regime = regimes.get(symbol)
                if regime and regime.recommendation == "NO_TRADE":
                    continue
                signal = await self.signal_engine.generate(symbol, regime)
                if signal:
                    signals.append(signal)
            except Exception as e:
                self.log.debug(f"Signal error {symbol}: {e}")

        # 4. Risk filter → deliver
        for signal in signals:
            try:
                approved = await self.risk_engine.evaluate(signal)
                if approved:
                    await self.telegram.send_signal(signal)
                    self.state.record_signal(signal)
            except Exception as e:
                self.log.error(f"Signal delivery error: {e}")

        # 5. Update state
        self.state.update_scan(scan_count, len(signals))

        # 6. Feed funding rates into correlation engine for trend tracking
        if self.correlation_engine:
            try:
                now_ts = datetime.now(timezone.utc).timestamp()
                for sym in config.SYMBOLS:
                    fd = self.data_processor.get_funding(sym)
                    if fd.rate != 0.0:
                        self.correlation_engine.update_funding(sym, fd.rate, now_ts)
                self.correlation_engine.rebuild_matrix()
            except Exception as e:
                self.log.debug(f"Correlation update error: {e}")

        if scan_count % 10 == 0:
            self.log.info(
                f"Scan #{scan_count} | Signals: {len(signals)} | "
                f"RAM: {self._get_memory_pct():.0%}"
            )

    # ───────────────────────────────────────────
    # BACKGROUND TASKS
    # ───────────────────────────────────────────

    async def _start_background_tasks(self):
        tasks = [
            ("ws_engine", self.ws_engine.run()),
            ("orderflow", self.orderflow.run()),
            ("monitor", self.monitor.run()),
            ("rest_polling", self.data_processor.start_rest_polling()),
            ("telegram_polling", self.telegram.start_polling()),
            ("btc_dominance", self.btc_dominance.run()),
            ("events_calendar", self._events_refresh_loop()),
            ("session_tracker", self.session_tracker.run()),
            ("memory_watchdog", self._memory_watchdog()),
            ("main_loop", self.main_loop()),
        ]
        for name, coro in tasks:
            task = asyncio.create_task(coro, name=name)
            self._tasks.append(task)
            self.log.info(f"Started task: {name}")

    async def _events_refresh_loop(self):
        """Refresh economic calendar every hour."""
        while not self._shutdown_event.is_set():
            try:
                await self.events_calendar.refresh()
            except Exception as e:
                self.log.debug(f"Events refresh error: {e}")
            await asyncio.sleep(3600)

    # ───────────────────────────────────────────
    # MEMORY WATCHDOG
    # ───────────────────────────────────────────

    async def _memory_watchdog(self):
        while not self._shutdown_event.is_set():
            try:
                pct = self._get_memory_pct()
                if pct > config.MEMORY_KILL_PCT:
                    self.log.critical(f"MEMORY CRITICAL {pct:.0%} — initiating restart")
                    await self.telegram.send_alert(
                        f"🚨 MEMORY {pct:.0%} — Emergency restart", priority="high"
                    )
                    self._shutdown_event.set()
                elif pct > config.MEMORY_ALERT_PCT:
                    self.log.warning(f"Memory high: {pct:.0%}")
                    self.data_processor.evict_old_candles()
                    await self.telegram.send_alert(
                        f"⚠️ Memory {pct:.0%} — Buffer trimmed", priority="low"
                    )
            except Exception as e:
                self.log.debug(f"Memory watchdog error: {e}")
            await asyncio.sleep(30)

    def _get_memory_pct(self) -> float:
        try:
            proc = psutil.Process(os.getpid())
            mem = proc.memory_info().rss
            total = psutil.virtual_memory().total
            return mem / total
        except Exception:
            return 0.0

    # ───────────────────────────────────────────
    # SHUTDOWN
    # ───────────────────────────────────────────

    async def shutdown(self, reason: str = "Signal received"):
        self.log.info(f"═══ SHUTDOWN: {reason} ═══")
        self._shutdown_event.set()

        # Cancel all tasks gracefully
        for task in self._tasks:
            if not task.done():
                task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

        # Close WS connections
        if self.ws_engine:
            await self.ws_engine.close()

        # Save state
        if self.state:
            await self.state.save()

        # Final alert
        if self.telegram:
            try:
                await self.telegram.send_alert(
                    f"🔴 Bot shutdown: {reason}", priority="high"
                )
            except Exception:
                pass

        self.log.info("Shutdown complete")

    # ───────────────────────────────────────────
    # SIGNAL HANDLERS
    # ───────────────────────────────────────────

    def _install_signal_handlers(self, loop: asyncio.AbstractEventLoop):
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(
                        self.shutdown(f"OS signal {s.name}")
                    ),
                )
            except NotImplementedError:
                # Windows doesn't support add_signal_handler
                signal.signal(sig, lambda s, f: asyncio.create_task(
                    self.shutdown("OS signal")
                ))

    # ───────────────────────────────────────────
    # HEALTH CHECK ENDPOINT (Railway)
    # ───────────────────────────────────────────

    async def _health_server(self):
        """Minimal HTTP health endpoint for Railway healthchecks."""
        import aiohttp
        from aiohttp import web

        async def health_handler(request):
            if self._shutdown_event.is_set():
                return web.Response(status=503, text="SHUTTING DOWN")
            mem = self._get_memory_pct()
            status = {
                "status": "ok",
                "version": config.BOT_VERSION,
                "memory_pct": round(mem, 3),
                "uptime_s": int(asyncio.get_event_loop().time()),
            }
            return web.json_response(status)

        app = web.Application()
        app.router.add_get("/health", health_handler)
        app.router.add_get("/", health_handler)

        port = int(os.getenv("PORT", "8080"))
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        self.log.info(f"Health endpoint: http://0.0.0.0:{port}/health")

        while not self._shutdown_event.is_set():
            await asyncio.sleep(5)
        await runner.cleanup()

    # ───────────────────────────────────────────
    # RUN
    # ───────────────────────────────────────────

    async def run(self):
        loop = asyncio.get_running_loop()
        self._install_signal_handlers(loop)

        ok = await self.startup()
        if not ok:
            self.log.critical("Startup failed — exiting")
            sys.exit(1)

        # Add health server to tasks
        self._tasks.append(
            asyncio.create_task(self._health_server(), name="health_server")
        )

        await self._start_background_tasks()

        # Wait for shutdown
        await self._shutdown_event.wait()
        await self.shutdown("Event loop done")


# ═══════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════

def main():
    bot = EliteScalper()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.critical(f"Fatal error: {e}\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
