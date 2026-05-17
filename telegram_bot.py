"""
ARUNABHA ELITE SCALPER v3.0
FILE 11/18: telegram_bot.py
Telegram signal delivery, admin commands, formatted alerts
HTML mode, retry logic, cooldown per symbol
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import aiohttp

import config

log = logging.getLogger("elite.telegram")


class TelegramBot:
    def __init__(self, state):
        self.state = state
        self._base_url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_alert: dict = {}   # symbol → timestamp (cooldown)
        self._last_general: float = 0.0
        self._offset: int = 0
        self._polling_task: Optional[asyncio.Task] = None
        self._admin_handlers = self._build_admin_handlers()

    # ═══════════════════════════════════════════
    # SESSION MANAGEMENT
    # ═══════════════════════════════════════════

    async def _get_session(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=config.TELEGRAM_TIMEOUT)
            )
        return self._session

    # ═══════════════════════════════════════════
    # CORE SEND
    # ═══════════════════════════════════════════

    async def _send(self, text: str, parse_mode: str = "HTML") -> bool:
        for attempt in range(config.TELEGRAM_MAX_RETRIES):
            try:
                session = await self._get_session()
                url = f"{self._base_url}/sendMessage"
                payload = {
                    "chat_id": config.TELEGRAM_CHAT_ID,
                    "text": text,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": True,
                }
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        return True
                    elif resp.status == 429:
                        # Rate limited
                        data = await resp.json()
                        retry_after = data.get("parameters", {}).get("retry_after", 5)
                        await asyncio.sleep(retry_after)
                    else:
                        body = await resp.text()
                        log.warning(f"Telegram send failed {resp.status}: {body[:100]}")
                        await asyncio.sleep(config.TELEGRAM_RETRY_DELAY)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error(f"Telegram send error (attempt {attempt+1}): {e}")
                await asyncio.sleep(config.TELEGRAM_RETRY_DELAY * (attempt + 1))
        return False

    # ═══════════════════════════════════════════
    # SIGNAL ALERT
    # ═══════════════════════════════════════════

    async def send_signal(self, signal) -> bool:
        """Format and send a trading signal."""
        # Per-symbol cooldown
        last = self._last_alert.get(signal.symbol, 0)
        if time.time() - last < config.ALERT_COOLDOWN_SECONDS:
            return False

        text = self._format_signal(signal)
        ok = await self._send(text)
        if ok:
            self._last_alert[signal.symbol] = time.time()
        return ok

    def _format_signal(self, signal) -> str:
        e = config.EMOJI
        grade_emoji = e.get(signal.grade, "📊")
        dir_emoji = e["LONG"] if signal.direction == "LONG" else e["SHORT"]

        # IST time
        now_ist = datetime.now(timezone.utc)
        ist_hour = (now_ist.hour + 5) % 24
        ist_min = (now_ist.minute + 30) % 60
        ist_hour += (now_ist.minute + 30) // 60
        ist_str = f"{ist_hour:02d}:{ist_min:02d} IST"

        # RR
        rr_str = f"{signal.rr_ratio:.1f}R"

        # Score bar
        score_bar = self._score_bar(signal.score)

        lines = [
            f"{grade_emoji} <b>{signal.grade} SIGNAL</b> {grade_emoji}",
            f"",
            f"{dir_emoji} <b>{signal.symbol}</b> — <b>{signal.direction}</b>",
            f"📋 Type: <code>{signal.signal_type}</code>",
            f"🎯 Score: <b>{signal.score:.1f}/100</b> {score_bar}",
            f"",
            f"<b>📌 ENTRY:</b>  <code>{signal.entry_price:.4f}</code>",
            f"<b>🛑 SL:</b>     <code>{signal.sl_price:.4f}</code>  ({abs(signal.entry_price - signal.sl_price)/signal.entry_price*100:.2f}%)",
            f"<b>🎯 TP1:</b>    <code>{signal.tp1_price:.4f}</code>  (1.5R)",
            f"<b>🎯 TP2:</b>    <code>{signal.tp2_price:.4f}</code>  (2.5R)",
            f"<b>🎯 TP3:</b>    <code>{signal.tp3_price:.4f}</code>  (4.0R)",
            f"",
            f"💰 Risk: <b>{signal.risk_pct*100:.1f}%</b> = ${signal.risk_usdt:.1f}",
            f"📦 Size: <b>${signal.size_usdt:.0f}</b> | RR: <b>{rr_str}</b>",
            f"",
            f"<b>📊 Context</b>",
            f"🌊 Regime: <code>{signal.regime}</code> ({signal.regime_confidence:.0f}%)",
            f"⚡ Volatility: <code>{signal.volatility_regime}</code>",
            f"📈 Volume: <code>{signal.volume_regime}</code>",
            f"💸 Funding: <code>{signal.funding_rate*100:.4f}%</code>",
            f"",
        ]

        # ── Market Breadth block ──
        breadth_pct = getattr(signal, "breadth_score", None)
        if breadth_pct is not None:
            bull_pct = int(breadth_pct * 100)
            breadth_bar = "█" * (bull_pct // 10) + "░" * (10 - bull_pct // 10)
            breadth_emoji = "🐂" if bull_pct >= 60 else "🐻" if bull_pct <= 40 else "🔄"
            lines.append(f"{breadth_emoji} Breadth: <b>{bull_pct}% bullish</b> {breadth_bar}")

        extreme_funding_pct = getattr(signal, "extreme_funding_pct", None)
        if extreme_funding_pct is not None:
            lines.append(f"💸 Extreme funding: <b>{int(extreme_funding_pct*100)}% symbols</b>")

        btc_dom_trend = getattr(signal, "btc_dom_trend", "")
        if btc_dom_trend:
            dom_emoji = "📈" if btc_dom_trend == "RISING" else "📉" if btc_dom_trend == "FALLING" else "➡️"
            lines.append(f"{dom_emoji} BTC.D trend: <code>{btc_dom_trend}</code>")

        session_name = getattr(signal, "session", "")
        session_mult = getattr(signal, "session_mult", 1.0)
        if session_name:
            lines.append(f"🕐 Session: <code>{session_name}</code> ({session_mult:.2f}x size)")

        funding_trend = getattr(signal, "funding_trend", "")
        if funding_trend and funding_trend != "NEUTRAL":
            ft_emoji = "⬆️" if funding_trend == "RISING" else "⬇️"
            lines.append(f"{ft_emoji} Funding trend: <code>{funding_trend}</code>")

        corr_with_btc = getattr(signal, "btc_correlation", None)
        if corr_with_btc is not None:
            corr_emoji = "🔴" if abs(corr_with_btc) >= 0.85 else "🟡" if abs(corr_with_btc) >= 0.60 else "🟢"
            lines.append(f"{corr_emoji} BTC correlation: <code>{corr_with_btc:+.2f}</code>")

        lines.extend([
            f"",
            f"<b>🔢 Score Breakdown</b>",
        ])

        # Score breakdown (skip meta keys prefixed with _)
        for k, v in signal.score_breakdown.items():
            if k.startswith("_"):
                continue
            label = k.replace("_", " ").title()
            max_v = 20 if k == "trend_alignment" else 15 if k in ("momentum", "volume", "structure") else 10 if k in ("orderbook", "funding", "volatility_fit") else 5
            filled = min(int(v / max_v * 4), 4)
            bar = "█" * filled + "░" * (4 - filled)
            lines.append(f"  {label}: <code>{v:.0f}</code> {bar}")

        lines.extend([
            f"",
            f"⏰ {ist_str} | Expires in {config.SIGNAL_EXPIRY_MINUTES}min",
            f"⚠️ <i>Signal only. Manual execution on Delta Exchange.</i>",
        ])

        return "\n".join(lines)

    def _score_bar(self, score: float) -> str:
        filled = int(score / 10)
        empty = 10 - filled
        return "█" * filled + "░" * empty

    # ═══════════════════════════════════════════
    # GENERIC ALERTS
    # ═══════════════════════════════════════════

    async def send_alert(self, text: str, priority: str = "normal") -> bool:
        """General-purpose alert with cooldown for low-priority."""
        if priority == "low":
            if time.time() - self._last_general < 60:
                return False
        if priority not in ("high", "whale"):
            self._last_general = time.time()
        return await self._send(text)

    async def send_startup_message(self):
        msg = (
            f"🚀 <b>{config.BOT_NAME} v{config.BOT_VERSION} ONLINE</b>\n"
            f"\n"
            f"📍 Region: <code>{config.REGION}</code>\n"
            f"💰 Balance: <code>${config.ACCOUNT_BALANCE_USDT:.2f}</code>\n"
            f"🎯 Symbols: <code>{len(config.SYMBOLS)}</code>\n"
            f"⏱ Scan: every <code>{config.SCAN_INTERVAL}s</code>\n"
            f"📊 Mode: SIGNAL ONLY (no auto-trade)\n"
            f"\n"
            f"Symbols: {', '.join(config.SYMBOLS)}\n"
            f"\n"
            f"Send /status for live status\n"
            f"Send /help for all commands"
        )
        await self._send(msg)

    async def test_connection(self):
        """Verify bot token and chat ID are valid."""
        session = await self._get_session()
        url = f"{self._base_url}/getMe"
        async with session.get(url) as resp:
            if resp.status != 200:
                raise ConnectionError(f"Telegram getMe failed: {resp.status}")
            data = await resp.json()
            bot_name = data["result"].get("username", "unknown")
            log.info(f"Telegram connected: @{bot_name}")

    # ═══════════════════════════════════════════
    # ADMIN COMMAND POLLING
    # ═══════════════════════════════════════════

    async def start_polling(self):
        """Poll for admin commands in background."""
        log.info("Telegram command polling started")
        while True:
            try:
                await self._poll_updates()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug(f"Poll error: {e}")
            await asyncio.sleep(2)

    async def _poll_updates(self):
        session = await self._get_session()
        url = f"{self._base_url}/getUpdates"
        params = {"offset": self._offset, "timeout": 5, "allowed_updates": ["message"]}
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                return
            data = await resp.json()
            for update in data.get("result", []):
                self._offset = update["update_id"] + 1
                await self._handle_update(update)

    async def _handle_update(self, update: dict):
        msg = update.get("message", {})
        text = msg.get("text", "").strip()
        chat_id = str(msg.get("chat", {}).get("id", ""))

        if chat_id != config.TELEGRAM_CHAT_ID:
            return  # ignore messages from other chats

        if not text.startswith("/"):
            # Check admin password for kill switch
            if text == config.ADMIN_PASSWORD:
                self.state.kill_switch_active = not self.state.kill_switch_active
                status = "ACTIVATED 🚨" if self.state.kill_switch_active else "DEACTIVATED ✅"
                await self._send(f"Kill switch {status}")
            return

        cmd = text.split()[0].lower()
        handler = self._admin_handlers.get(cmd)
        if handler:
            await handler()

    def _build_admin_handlers(self) -> dict:
        return {
            "/status": self._cmd_status,
            "/risk": self._cmd_risk,
            "/kill": self._cmd_kill,
            "/resume": self._cmd_resume,
            "/signals": self._cmd_signals,
            "/help": self._cmd_help,
            "/balance": self._cmd_balance,
        }

    async def _cmd_status(self):
        s = self.state
        dd_emoji = "🟢" if s.drawdown_pct < 0.05 else "🟡" if s.drawdown_pct < 0.10 else "🔴"
        msg = (
            f"📊 <b>SYSTEM STATUS</b>\n\n"
            f"💰 Balance: <code>${s.current_balance_usdt:.2f}</code>\n"
            f"{dd_emoji} Drawdown: <code>{s.drawdown_pct:.1%}</code>\n"
            f"📉 Daily P&L: <code>{s.daily_loss_pct:.1%}</code>\n"
            f"📅 Weekly P&L: <code>{s.weekly_loss_pct:.1%}</code>\n"
            f"❌ Consec Losses: <code>{s.consecutive_losses}</code>\n"
            f"📌 Active Signals: <code>{s.count_active_positions()}</code>\n"
            f"🔢 Total Signals: <code>{s.total_signals}</code>\n"
            f"🔴 Kill Switch: <code>{'ON' if s.kill_switch_active else 'OFF'}</code>\n"
            f"🔄 Scans: <code>{s.scan_count}</code>"
        )
        await self._send(msg)

    async def _cmd_risk(self):
        from risk_engine import RiskEngine
        # Build a quick status without full engine
        s = self.state
        action_map = {
            "NORMAL": "🟢 Normal trading",
            "REDUCE_25PCT": "🟡 Size -25%",
            "REDUCE_50PCT": "🟠 Size -50%",
            "REDUCE_75PCT": "🔴 Size -75%",
            "EMERGENCY_STOP": "🚨 EMERGENCY STOP",
            "NUCLEAR_STOP": "💀 NUCLEAR STOP",
        }
        dd = s.drawdown_pct
        if dd >= config.DRAWDOWN_NUCLEAR:
            action = "NUCLEAR_STOP"
        elif dd >= config.DRAWDOWN_EMERGENCY:
            action = "EMERGENCY_STOP"
        elif dd >= config.DRAWDOWN_CRITICAL:
            action = "REDUCE_75PCT"
        elif dd >= config.DRAWDOWN_SERIOUS:
            action = "REDUCE_50PCT"
        elif dd >= config.DRAWDOWN_WARNING:
            action = "REDUCE_25PCT"
        else:
            action = "NORMAL"

        msg = (
            f"⚙️ <b>RISK STATUS</b>\n\n"
            f"Action: {action_map.get(action, action)}\n"
            f"Drawdown: <code>{dd:.1%}</code>\n"
            f"Max Daily: <code>{config.MAX_DAILY_LOSS:.0%}</code>\n"
            f"Max Weekly: <code>{config.MAX_WEEKLY_LOSS:.0%}</code>\n"
            f"Max Positions: <code>{config.MAX_POSITIONS}</code>"
        )
        await self._send(msg)

    async def _cmd_kill(self):
        self.state.kill_switch_active = True
        await self._send("🚨 <b>KILL SWITCH ACTIVATED</b>\nNo new signals will be generated.\nSend /resume to re-enable.")

    async def _cmd_resume(self):
        self.state.kill_switch_active = False
        await self._send("✅ <b>Kill switch deactivated.</b>\nBot resuming normal operation.")

    async def _cmd_signals(self):
        active = self.state.get_active_signals()
        if not active:
            await self._send("No active signals.")
            return
        lines = ["📋 <b>ACTIVE SIGNALS</b>\n"]
        for sig in active:
            age_min = (time.time() - sig.get("generated_at", time.time())) / 60
            lines.append(
                f"• {sig['symbol']} {sig['direction']} "
                f"@ {sig.get('entry_price', 0):.4f} "
                f"[{sig.get('grade', '?')}] {age_min:.0f}min ago"
            )
        await self._send("\n".join(lines))

    async def _cmd_balance(self):
        msg = (
            f"💰 <b>BALANCE</b>\n"
            f"Current: <code>${self.state.current_balance_usdt:.2f}</code>\n"
            f"Start: <code>${config.ACCOUNT_BALANCE_USDT:.2f}</code>\n"
            f"PnL: <code>${self.state.current_balance_usdt - config.ACCOUNT_BALANCE_USDT:.2f}</code>"
        )
        await self._send(msg)

    async def _cmd_help(self):
        msg = (
            f"🤖 <b>{config.BOT_NAME} Commands</b>\n\n"
            f"/status — System status\n"
            f"/risk — Risk engine status\n"
            f"/signals — Active signals\n"
            f"/balance — Account balance\n"
            f"/kill — Activate kill switch\n"
            f"/resume — Deactivate kill switch\n"
            f"/help — This message\n\n"
            f"<i>To toggle kill switch: send admin password</i>"
        )
        await self._send(msg)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
