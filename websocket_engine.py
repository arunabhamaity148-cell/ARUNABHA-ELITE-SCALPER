"""
ARUNABHA ELITE SCALPER v3.0
FILE 3/18: websocket_engine.py
Binance Futures combined stream manager + Bybit/OKX validation
Auto-reconnect, heartbeat, dedup, fallback to REST
"""

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Dict, List, Optional, Set

import aiohttp
import websockets

import config

log = logging.getLogger("elite.ws")


class ConnState(Enum):
    CONNECTING = "CONNECTING"
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"


@dataclass
class WSStats:
    connected_at: float = 0.0
    messages_total: int = 0
    messages_per_sec: float = 0.0
    latency_ms_avg: float = 0.0
    reconnect_count: int = 0
    last_message_at: float = 0.0
    queue_depth: int = 0
    errors: int = 0


class BinanceFuturesWS:
    """
    Manages a single combined WebSocket connection to Binance Futures.
    Streams: kline_5m, kline_15m, kline_1h, depth@100ms, aggTrade per symbol.
    """

    def __init__(self, data_processor, symbols: List[str]):
        self.dp = data_processor
        self.symbols = symbols
        self.state = ConnState.CLOSED
        self.stats = WSStats()
        self._ws = None
        self._shutdown = asyncio.Event()
        self._seen_ids: deque = deque(maxlen=10000)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=config.WS_QUEUE_MAX)
        self._last_ping = 0.0
        self._last_pong = 0.0
        self._reconnect_delay_idx = 0

    def _build_streams(self) -> List[str]:
        streams = []
        for sym in self.symbols:
            s = sym.lower()
            streams.extend([
                f"{s}@kline_5m",
                f"{s}@kline_15m",
                f"{s}@kline_1h",
                f"{s}@depth@100ms",
                f"{s}@aggTrade",
            ])
        return streams

    def _build_url(self) -> str:
        streams = self._build_streams()
        # Split into chunks of 200 (Binance limit)
        chunk = streams[:200]
        return config.BINANCE_WS_BASE + "/".join(chunk)

    async def run(self):
        while not self._shutdown.is_set():
            try:
                await self._connect()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Binance WS error: {e}")
                self.stats.errors += 1
                await self._backoff()

    async def _connect(self):
        url = self._build_url()
        self.state = ConnState.CONNECTING
        log.info(f"Connecting Binance WS ({len(self.symbols)} symbols)...")

        async with websockets.connect(
            url,
            ping_interval=None,   # manual heartbeat
            ping_timeout=None,
            close_timeout=5,
            max_size=10 * 1024 * 1024,  # 10MB
        ) as ws:
            self._ws = ws
            self.state = ConnState.OPEN
            self.stats.connected_at = time.time()
            self.stats.reconnect_count += 1 if self.stats.reconnect_count > 0 else 0
            self._reconnect_delay_idx = 0
            log.info("✅ Binance WS connected")

            # Start consumer and heartbeat concurrently
            await asyncio.gather(
                self._receive_loop(ws),
                self._heartbeat_loop(ws),
                self._process_queue(),
            )

    async def _receive_loop(self, ws):
        async for raw in ws:
            recv_ts = time.time()
            self.stats.messages_total += 1
            self.stats.last_message_at = recv_ts

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            # Handle pong
            if isinstance(msg, dict) and msg.get("result") is None and "id" not in msg:
                pass

            # Timestamp sanity
            event_ts = msg.get("E", msg.get("data", {}).get("E", 0)) if isinstance(msg, dict) else 0
            if event_ts:
                age = recv_ts - (event_ts / 1000)
                if age > config.WS_TIMESTAMP_TOLERANCE:
                    log.debug(f"Stale message: {age:.1f}s old")
                    continue
                self.stats.latency_ms_avg = (
                    self.stats.latency_ms_avg * 0.95 + (age * 1000) * 0.05
                )

            # Deduplication via sequence-like key
            msg_key = self._make_key(msg)
            if msg_key and msg_key in self._seen_ids:
                continue
            if msg_key:
                self._seen_ids.append(msg_key)

            # Backpressure: if queue full, drop oldest non-critical
            if self._queue.full():
                try:
                    self._queue.get_nowait()
                    log.debug("Queue full — dropped oldest message")
                except asyncio.QueueEmpty:
                    pass

            await self._queue.put(msg)
            self.stats.queue_depth = self._queue.qsize()

    async def _process_queue(self):
        while True:
            try:
                msg = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self._route(msg)
                self._queue.task_done()
            except asyncio.TimeoutError:
                if self._shutdown.is_set():
                    break
            except Exception as e:
                log.debug(f"Queue process error: {e}")

    async def _route(self, msg: dict):
        if not isinstance(msg, dict):
            return

        # Combined stream wraps data in {"stream": ..., "data": {...}}
        data = msg.get("data", msg)
        event = data.get("e", "")
        symbol = data.get("s", "").upper()

        if not symbol or symbol not in config.SYMBOLS:
            return

        try:
            if event == "kline":
                await self.dp.on_kline(symbol, data)
            elif event == "depthUpdate":
                await self.dp.on_depth(symbol, data)
            elif event == "aggTrade":
                await self.dp.on_agg_trade(symbol, data)
        except Exception as e:
            log.debug(f"Route error {event} {symbol}: {e}")

    async def _heartbeat_loop(self, ws):
        while True:
            await asyncio.sleep(config.WS_HEARTBEAT_INTERVAL)
            try:
                self._last_ping = time.time()
                await ws.ping()
                # Check pong received
                await asyncio.sleep(config.WS_PONG_TIMEOUT)
                if self._last_ping > self._last_pong:
                    log.warning("No pong received — reconnecting")
                    await ws.close()
                    return
            except Exception:
                return

    def _make_key(self, msg: dict) -> Optional[str]:
        data = msg.get("data", msg)
        t = data.get("t") or data.get("T") or data.get("E")
        s = data.get("s", "")
        e = data.get("e", "")
        if t and s:
            return f"{s}:{e}:{t}"
        return None

    async def _backoff(self):
        idx = min(self._reconnect_delay_idx, len(config.WS_RECONNECT_DELAYS) - 1)
        delay = config.WS_RECONNECT_DELAYS[idx]
        self._reconnect_delay_idx = min(idx + 1, len(config.WS_RECONNECT_DELAYS) - 1)
        log.info(f"Reconnecting in {delay}s...")
        await asyncio.sleep(delay)

    async def close(self):
        self._shutdown.set()
        self.state = ConnState.CLOSING
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        self.state = ConnState.CLOSED

    def get_stats(self) -> WSStats:
        return self.stats


class BybitValidationWS:
    """
    Lightweight Bybit ticker stream for price validation only.
    """

    def __init__(self, price_callback: Callable):
        self._callback = price_callback
        self._shutdown = asyncio.Event()
        self.prices: Dict[str, float] = {}

    async def run(self):
        while not self._shutdown.is_set():
            try:
                await self._connect()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug(f"Bybit WS error: {e}")
                await asyncio.sleep(10)

    async def _connect(self):
        async with websockets.connect(config.BYBIT_WS_URL, ping_interval=20) as ws:
            # Subscribe to tickers
            topics = [f"tickers.{s}" for s in config.SYMBOLS]
            await ws.send(json.dumps({"op": "subscribe", "args": topics}))
            log.info("Bybit validation WS connected")

            async for raw in ws:
                if self._shutdown.is_set():
                    break
                try:
                    msg = json.loads(raw)
                    data = msg.get("data", {})
                    symbol = data.get("symbol", "")
                    price = float(data.get("lastPrice", 0))
                    if symbol and price:
                        self.prices[symbol] = price
                        await self._callback(symbol, price, "bybit")
                except Exception:
                    pass

    async def close(self):
        self._shutdown.set()


class OKXValidationWS:
    """
    Lightweight OKX ticker stream for price validation only.
    OKX uses INST-ID format: BTC-USDT-SWAP
    """

    def __init__(self, price_callback: Callable):
        self._callback = price_callback
        self._shutdown = asyncio.Event()
        self.prices: Dict[str, float] = {}

    def _to_okx_id(self, symbol: str) -> str:
        base = symbol.replace("USDT", "")
        return f"{base}-USDT-SWAP"

    def _from_okx_id(self, inst_id: str) -> str:
        parts = inst_id.split("-")
        return f"{parts[0]}USDT" if len(parts) >= 2 else inst_id

    async def run(self):
        while not self._shutdown.is_set():
            try:
                await self._connect()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug(f"OKX WS error: {e}")
                await asyncio.sleep(10)

    async def _connect(self):
        async with websockets.connect(config.OKX_WS_URL, ping_interval=20) as ws:
            args = [{"channel": "tickers", "instId": self._to_okx_id(s)} for s in config.SYMBOLS]
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            log.info("OKX validation WS connected")

            async for raw in ws:
                if self._shutdown.is_set():
                    break
                try:
                    msg = json.loads(raw)
                    for item in msg.get("data", []):
                        inst_id = item.get("instId", "")
                        price = float(item.get("last", 0))
                        symbol = self._from_okx_id(inst_id)
                        if symbol and price:
                            self.prices[symbol] = price
                            await self._callback(symbol, price, "okx")
                except Exception:
                    pass

    async def close(self):
        self._shutdown.set()


class WebsocketEngine:
    """
    Orchestrates all WS connections.
    Provides price validation: Binance vs Bybit vs OKX.
    Falls back to REST polling if WS down.
    """

    def __init__(self, data_processor):
        self.dp = data_processor
        self._validation_prices: Dict[str, Dict[str, float]] = {}
        self._shutdown = asyncio.Event()
        self._fallback_active: Dict[str, bool] = {}

        self.binance_ws = BinanceFuturesWS(data_processor, config.SYMBOLS)
        self.bybit_ws = BybitValidationWS(self._on_validation_price)
        self.okx_ws = OKXValidationWS(self._on_validation_price)

    async def run(self):
        await asyncio.gather(
            self.binance_ws.run(),
            self.bybit_ws.run(),
            self.okx_ws.run(),
            self._fallback_watchdog(),
        )

    async def _on_validation_price(self, symbol: str, price: float, exchange: str):
        if symbol not in self._validation_prices:
            self._validation_prices[symbol] = {}
        self._validation_prices[symbol][exchange] = price

    def validate_price(self, symbol: str, binance_price: float) -> tuple[bool, float]:
        """
        Compare Binance price against Bybit/OKX.
        Returns (valid, max_divergence).
        """
        others = self._validation_prices.get(symbol, {})
        if not others:
            return True, 0.0  # Can't validate yet

        max_div = 0.0
        for exchange, price in others.items():
            if price <= 0:
                continue
            div = abs(binance_price - price) / price
            max_div = max(max_div, div)

        valid = max_div <= config.PRICE_DIVERGENCE_LIMIT
        if not valid:
            log.warning(f"Price divergence {symbol}: Binance={binance_price:.4f}, others={others}, div={max_div:.4f}")

        return valid, max_div

    async def _fallback_watchdog(self):
        """Switch to REST polling if primary WS is stale."""
        while not self._shutdown.is_set():
            await asyncio.sleep(30)
            try:
                stats = self.binance_ws.get_stats()
                if stats.last_message_at == 0:
                    continue
                age = time.time() - stats.last_message_at
                if age > 30 and self.binance_ws.state != ConnState.CONNECTING:
                    log.warning(f"WS stale ({age:.0f}s) — activating REST fallback")
                    await self._rest_fallback_scan()
            except Exception as e:
                log.debug(f"Fallback watchdog error: {e}")

    async def _rest_fallback_scan(self):
        """Poll Binance REST for klines when WS is down."""
        async with aiohttp.ClientSession() as session:
            for symbol in config.SYMBOLS[:3]:  # Limited fallback
                try:
                    url = f"{config.BINANCE_BASE_URL}/fapi/v1/klines"
                    params = {"symbol": symbol, "interval": "5m", "limit": 2}
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            await self.dp.on_rest_kline(symbol, "5m", data)
                except Exception as e:
                    log.debug(f"REST fallback error {symbol}: {e}")

    def get_ws_stats(self) -> dict:
        s = self.binance_ws.get_stats()
        return {
            "state": self.binance_ws.state.value,
            "messages_total": s.messages_total,
            "latency_ms": round(s.latency_ms_avg, 1),
            "reconnects": s.reconnect_count,
            "queue_depth": s.queue_depth,
            "errors": s.errors,
            "last_msg_age_s": round(time.time() - s.last_message_at, 1) if s.last_message_at else -1,
        }

    async def close(self):
        self._shutdown.set()
        await asyncio.gather(
            self.binance_ws.close(),
            self.bybit_ws.close(),
            self.okx_ws.close(),
            return_exceptions=True,
        )
