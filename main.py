"""
ARUNABHA MANUAL SCALPER v4.0
FILE: main.py

Two-layer architecture orchestrator:
LAYER A: PairUniverseEngine + AttentionEngine + NewsGuardEngine
LAYER B: SignalEngine (per qualified pair) + ManualExecutionAssistant

Scan loop:
1. Every UNIVERSE_REFRESH_MINUTES: rebuild pair universe
2. Every 2min: score attention for all candidates
3. Every SCAN_INTERVAL (45s): run Layer B on qualified pairs only
4. On signal: format + send Telegram alert via ManualExecutionAssistant
5. Every 30s: check signal expiry, send cancellation if needed
"""

import asyncio
import logging
import logging.handlers
import os
import signal
import sys
import time
from typing import List, Optional, Set

import config

# ── Logging setup ──
log_handler = logging.handlers.RotatingFileHandler(
    config.LOG_FILE, maxBytes=config.LOG_ROTATION_MB * 1024 * 1024, backupCount=3
)
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format=config.LOG_FORMAT,
    handlers=[log_handler, logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("scalper.main")


class ScalperV4:
    """Main orchestrator — wires all modules and runs event loop."""

    def __init__(self):
        # ── Layer A modules ──
        from pair_universe_engine import PairUniverseEngine
        from attention_engine import AttentionEngine
        from news_guard_engine import NewsGuardEngine

        self.universe = PairUniverseEngine()
        self.attention = AttentionEngine(self.universe)
        self.news_guard = NewsGuardEngine()

        # ── Shared state ──
        from state_manager import StateManager
        self.state = StateManager()

        # ── Data infrastructure ──
        from websocket_engine import WebsocketEngine
        from data_processor import DataProcessor
        from cache_manager import CacheManager

        self.cache = CacheManager()
        self.dp = DataProcessor(self.cache)
        self.ws = WebsocketEngine(self.dp, self.cache)

        # ── Market analysis modules (preserved from v3) ──
        from market_regime_engine import MarketRegimeEngine
        from orderflow_engine import OrderflowEngine
        from liquidity_engine import LiquidityEngine
        from smc_engine import SMCEngine
        from correlation_engine import CorrelationEngine
        from session_tracker import SessionTracker
        from btc_dominance import BTCDominanceTracker
        from ml_engine import MLEngine

        self.regime = MarketRegimeEngine(self.dp)
        self.orderflow = OrderflowEngine(self.dp)
        self.liquidity = LiquidityEngine(self.dp)
        self.smc = SMCEngine(self.dp)
        self.correlation = CorrelationEngine(self.dp)
        self.sessions = SessionTracker()
        self.btc_dom = BTCDominanceTracker()
        self.ml = MLEngine(self.state)

        # ── Telegram ──
        from telegram_bot import TelegramBot
        self.telegram = TelegramBot()

        # ── Layer B: Signal Engine ──
        from signal_engine import SignalEngine
        self.signal_engine = SignalEngine(
            data_processor=self.dp,
            orderflow=self.orderflow,
            liquidity=self.liquidity,
            regime=self.regime,
            risk_engine=None,           # risk checked in state
            ml_engine=self.ml,
            telegram=self.telegram,
            state=self.state,
            btc_dominance=self.btc_dom,
            correlation_engine=self.correlation,
            session_tracker=self.sessions,
            smc_engine=self.smc,
            attention_engine=self.attention,
            news_guard=self.news_guard,
        )

        # ── Manual execution assistant ──
        from manual_execution_assistant import ManualExecutionAssistant
        self.mexec = ManualExecutionAssistant()

        # ── Signal Health Engine (NEW) ──
        from signal_health_engine import SignalHealthEngine
        self.health_engine = SignalHealthEngine(self.dp, self.telegram, self.mexec)

        # ── Market Energy Index (NEW) ──
        from exhaustion_filter import MarketEnergyIndex
        self.energy_index = MarketEnergyIndex()

        # ── Monitoring ──
        from monitoring import MonitoringEngine
        self.monitor = MonitoringEngine()

        # Runtime
        self._running = False
        self._shutdown = asyncio.Event()
        self._previous_universe: Set[str] = set()
        self._tasks: List[asyncio.Task] = []

    # ═══════════════════════════════════════════
    # STARTUP
    # ═══════════════════════════════════════════

    async def start(self):
        log.info(f"═══ {config.BOT_NAME} v{config.BOT_VERSION} ═══")
        log.info(f"Risk profile: {config.RISK_PROFILE}")
        log.info(f"Deployment: {config.DEPLOYMENT} / {config.REGION}")

        # Validate required env vars
        if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
            log.error("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID — abort")
            sys.exit(1)

        if not config.BINANCE_API_KEY:
            log.warning("No BINANCE_API_KEY — using public endpoints only")

        # Connect Redis
        await self.state.connect_redis(config.REDIS_URL)
        await self.state.restore_on_startup()

        # Initial news guard refresh
        await self.news_guard.refresh()

        # Discover initial pair universe (blocking — need pairs before WS)
        log.info("Building initial pair universe...")
        await self.universe.refresh_now()
        universe = self.universe.get_universe()
        log.info(f"Initial universe: {universe}")

        # Score initial attention
        if universe:
            await self.attention._score_all(universe)

        # Connect WebSocket for initial universe
        await self.ws.connect(universe)

        # Send startup alert
        await self.telegram.send_message(
            f"🚀 <b>{config.BOT_NAME} v{config.BOT_VERSION} ONLINE</b>\n"
            f"Profile: <b>{config.RISK_PROFILE}</b>\n"
            f"Scanning: <b>{len(universe)}</b> pairs\n"
            f"Pairs: <code>{', '.join(universe[:6])}</code>"
            f"{'...' if len(universe) > 6 else ''}\n"
            f"News guard: {self.news_guard.summary()}"
        )

        self._running = True
        log.info("Startup complete — beginning main loop")

    # ═══════════════════════════════════════════
    # MAIN LOOP TASKS
    # ═══════════════════════════════════════════

    async def run(self):
        """Launch all background tasks."""
        await self.start()

        self._tasks = [
            asyncio.create_task(self._universe_loop(), name="universe"),
            asyncio.create_task(self.attention.run(), name="attention"),
            asyncio.create_task(self.news_guard.run(), name="newsguard"),
            asyncio.create_task(self._scan_loop(), name="scan"),
            asyncio.create_task(self._expiry_loop(), name="expiry"),
            asyncio.create_task(self.state.run(), name="state"),
            asyncio.create_task(self.ws.run(), name="websocket"),
            asyncio.create_task(self.monitor.run(), name="monitor"),
            asyncio.create_task(self.btc_dom.run(), name="btcdom"),
        ]

        try:
            await self._shutdown.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown_all()

    # ═══════════════════════════════════════════
    # UNIVERSE REFRESH LOOP
    # ═══════════════════════════════════════════

    async def _universe_loop(self):
        """
        Refreshes pair universe every UNIVERSE_REFRESH_MINUTES.
        Sends rotation alert to Telegram when universe changes.
        Updates WebSocket subscriptions.
        """
        log.info("Universe loop started")
        while not self._shutdown.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown.wait(),
                    timeout=config.UNIVERSE_REFRESH_MINUTES * 60,
                )
            except asyncio.TimeoutError:
                pass

            if self._shutdown.is_set():
                break

            try:
                old_universe = set(self.universe.get_universe())
                await self.universe.refresh_now()
                new_universe = set(self.universe.get_universe())

                added = list(new_universe - old_universe)
                removed = list(old_universe - new_universe)

                if added or removed:
                    log.info(f"Universe rotation: +{added} -{removed}")

                    # Update WebSocket subscriptions
                    if added:
                        await self.ws.add_symbols(added)
                    if removed:
                        await self.ws.remove_symbols(removed)

                    # Send rotation alert
                    msg = self.mexec.format_universe_update(
                        new_pairs=added,
                        removed_pairs=removed,
                        top_pairs=list(new_universe)[:8],
                    )
                    await self.telegram.send_message(msg)

                self._previous_universe = new_universe

            except Exception as e:
                log.error(f"Universe loop error: {e}", exc_info=True)

    # ═══════════════════════════════════════════
    # SCAN LOOP (LAYER B)
    # ═══════════════════════════════════════════

    async def _scan_loop(self):
        """
        Main scan loop — runs Layer B signal generation.
        Only scans pairs that passed Layer A attention threshold.
        """
        log.info("Scan loop started")
        while not self._shutdown.is_set():
            try:
                await self._scan_cycle()
            except Exception as e:
                log.error(f"Scan cycle error: {e}", exc_info=True)

            try:
                await asyncio.wait_for(
                    self._shutdown.wait(),
                    timeout=config.SCAN_INTERVAL,
                )
            except asyncio.TimeoutError:
                pass

    async def _scan_cycle(self):
        """One scan cycle — process all qualified pairs."""
        # Get pairs that passed attention threshold
        qualified = self.attention.get_qualified_pairs()

        if not qualified:
            log.debug("No pairs passed attention threshold this cycle")
            return

        # ── Market Energy Check (NEW) ──
        # Dead market = no trading. Skip entire cycle.
        try:
            candidates = self.universe.get_all_candidates()
            btc_change = 0.0
            btc_data = self.dp.get_indicators("BTCUSDT", config.TRIGGER_TF)
            if btc_data and hasattr(btc_data, 'price_change_1h_pct'):
                btc_change = btc_data.price_change_1h_pct
            session = self.sessions.current_session() if self.sessions else "NY"
            energy = self.energy_index.compute(candidates, session, btc_change)

            if not energy.is_tradeable:
                log.debug(f"Market DEAD (energy={energy.energy_score:.0f}) — skipping scan cycle")
                return
        except Exception as e:
            log.debug(f"Energy check error: {e}")
            # Don't block on energy check failure — continue scanning

        # Risk check: can we trade at all?
        can_trade, reason = self.state.can_trade()
        if not can_trade:
            log.debug(f"Trading blocked globally: {reason}")
            return

        # Check active position limit
        rp = config.get_risk_profile()
        if self.state.get_active_positions_count() >= rp["max_positions"]:
            log.debug("Max positions reached — skipping scan")
            return

        log.debug(f"Scanning {len(qualified)} attention-qualified pairs")

        for symbol in qualified:
            try:
                # Per-pair risk check (cooldowns)
                attention_snap = self.attention.get_attention(symbol)
                narrative = attention_snap.active_narrative if attention_snap else ""

                can_trade_pair, reason = self.state.can_trade(symbol, narrative)
                if not can_trade_pair:
                    log.debug(f"{symbol}: blocked — {reason}")
                    continue

                # Already have active signal for this symbol?
                if self.state.has_active_signal(symbol):
                    continue

                # Get regime snapshot for this symbol
                regime_snap = self.regime.get_snapshot(symbol)

                # Layer B: Generate signal
                signal = await self.signal_engine.generate(symbol, regime_snap)

                if signal:
                    # Register in state
                    self.state.add_active_signal(symbol, signal)

                    # Assess execution quality vs current price
                    current_price = self.dp.get_latest_price(symbol)
                    quality = self.mexec.assess_execution_quality(signal, current_price)

                    # Get news guard summary
                    guard = self.news_guard.check(symbol)
                    news_summary = "CLEAR" if guard.is_fully_clear else guard.reason

                    # Format alert
                    alert = self.mexec.format_signal_alert(
                        signal=signal,
                        current_price=current_price,
                        quality=quality,
                        news_guard_summary=news_summary,
                        account_balance=config.ACCOUNT_BALANCE_USDT,
                    )

                    # Send Telegram alert
                    await self.telegram.send_message(alert)

                    # Apply news guard size modifier to sizing guidance
                    if guard.size_multiplier < 1.0:
                        await self.telegram.send_message(
                            f"⚠️ <b>SIZE REDUCED</b> to {guard.size_multiplier*100:.0f}% "
                            f"due to: {guard.reason}"
                        )

                    log.info(
                        f"SENT: {symbol} {signal.direction} "
                        f"{signal.signal_type.value} [{signal.grade.value}] "
                        f"score={signal.confluence_score:.0f} "
                        f"quality={quality}"
                    )

                    # Log to ML training data
                    if self.ml:
                        self.ml.log_signal(signal)

            except Exception as e:
                log.error(f"Scan error for {symbol}: {e}", exc_info=True)

    # ═══════════════════════════════════════════
    # EXPIRY LOOP
    # ═══════════════════════════════════════════

    async def _expiry_loop(self):
        """
        Checks active signals every 30s for:
        1. Expiry (time-based)
        2. Health degradation (price-based)
        3. Post-liquidation reclaim setups (new opportunity)
        Sends cancellation/update alert when status changes.
        """
        log.info("Expiry loop started")
        while not self._shutdown.is_set():
            try:
                # ── Standard expiry check ──
                expired = await self.signal_engine.check_and_expire_signals()
                for symbol, reason in expired:
                    signal_obj = self.state._active_signals.get(symbol)
                    self.state.remove_active_signal(symbol)
                    if signal_obj:
                        cancel_msg = self.mexec.format_expiry_alert(signal_obj, reason)
                        await self.telegram.send_message(cancel_msg)
                        self.health_engine.clear_post_liq(symbol)
                        log.info(f"Signal expired: {symbol} — {reason}")

                # ── Health engine check (NEW) ──
                active = self.signal_engine.get_active_signals()
                if active:
                    reports = self.health_engine.check_all(active)
                    for report in reports:
                        if report.send_update:
                            signal_obj = active.get(report.symbol)
                            if signal_obj:
                                if report.status.value == "CANCEL":
                                    # Remove from active
                                    self.state.remove_active_signal(report.symbol)
                                    del self.signal_engine._active_signals[report.symbol]
                                msg = self.health_engine.format_health_update(
                                    signal_obj, report
                                )
                                if msg:
                                    await self.telegram.send_message(msg)

                # ── Post-Liquidation Reclaim Detection (NEW) ──
                # Check universe pairs for post-liq setups (not just active signals)
                qualified = self.attention.get_qualified_pairs()
                for symbol in qualified[:6]:   # top 6 only to avoid overload
                    try:
                        if self.state.has_active_signal(symbol):
                            continue
                        can_trade, _ = self.state.can_trade(symbol)
                        if not can_trade:
                            continue
                        deriv = self.attention.get_derivatives(symbol)
                        ind_t = self.dp.get_indicators(symbol, config.TRIGGER_TF)
                        ind_p = self.dp.get_indicators(symbol, config.PRIMARY_TF)
                        attn = self.attention.get_attention(symbol)
                        reclaim = self.health_engine.detect_post_liq_reclaim(
                            symbol, deriv, ind_t, ind_p, attn
                        )
                        if reclaim:
                            log.info(f"Post-liq reclaim detected: {symbol} {reclaim['direction']}")
                            # Format as signal and send
                            msg = self._format_post_liq_alert(reclaim)
                            await self.telegram.send_message(msg)
                    except Exception as e:
                        log.debug(f"Post-liq check error {symbol}: {e}")

            except Exception as e:
                log.error(f"Expiry loop error: {e}", exc_info=True)

            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass

    def _format_post_liq_alert(self, reclaim: dict) -> str:
        """Format a post-liquidation reclaim signal for Telegram."""
        e = config.EMOJI
        d = reclaim
        direction_emoji = e["LONG"] if d["direction"] == "LONG" else e["SHORT"]

        def fp(p):
            if p >= 1000: return f"{p:.2f}"
            elif p >= 1: return f"{p:.4f}"
            return f"{p:.6f}"

        return (
            f"{'━'*30}\n"
            f"{direction_emoji} <b>{d['symbol']} {d['direction']}</b>  🔥 TIER 1\n"
            f"📋 POST-LIQUIDATION RECLAIM\n"
            f"🏷 Quality: 🥇 A TACTICAL\n"
            f"{'─'*30}\n\n"
            f"👀 <b>WHY THIS PAIR</b>\n"
            f"  {d['why']}\n\n"
            f"📖 <i>Thesis: {d['thesis']}</i>\n\n"
            f"{'─'*30}\n\n"
            f"🎯 <b>ENTRY ZONE</b>\n"
            f"  Zone:   <code>{fp(d['entry_low'])}</code> – <code>{fp(d['entry_high'])}</code>\n"
            f"  Ideal:  <code>{fp(d['entry_ideal'])}</code>\n\n"
            f"🛑 <b>SL:</b>  <code>{fp(d['sl'])}</code>\n"
            f"✅ <b>TP1:</b> <code>{fp(d['tp1'])}</code>  |  "
            f"<b>TP2:</b> <code>{fp(d['tp2'])}</code>\n\n"
            f"🕐 <b>TIMING</b>\n"
            f"  Cancel if not triggered in: <b>8 min</b>\n"
            f"  Expected hold: <b>{d['expected_hold_min']} min</b>\n\n"
            f"⚙️ Limit order only | No chase past TP1 zone\n"
            f"{'━'*30}"
        )

    # ═══════════════════════════════════════════
    # SHUTDOWN
    # ═══════════════════════════════════════════

    async def _shutdown_all(self):
        log.info("Shutting down...")
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

        # Close connections
        await self.universe.close()
        await self.attention.close()
        await self.news_guard.close()
        await self.ws.close()
        await self.state.close()

        # Final Telegram alert
        try:
            await self.telegram.send_message(
                f"🛑 <b>{config.BOT_NAME} OFFLINE</b> — graceful shutdown"
            )
        except Exception:
            pass

        log.info("Shutdown complete")


# ═══════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════

def main():
    bot = ScalperV4()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def handle_signal(sig, frame):
        log.info(f"Signal {sig} received — initiating shutdown")
        bot._shutdown.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        loop.run_until_complete(bot.run())
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt — shutting down")
    finally:
        loop.close()


if __name__ == "__main__":
    main()
