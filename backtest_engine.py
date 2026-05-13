"""
ARUNABHA ELITE SCALPER v3.0
FILE 14/18: backtest_engine.py
Walk-forward backtesting — replay historical candles through signal engine
Outputs: win rate, avg RR, max DD, Sharpe, per-signal stats
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import aiohttp
import numpy as np

import config

log = logging.getLogger("elite.backtest")


@dataclass
class BacktestTrade:
    symbol: str
    direction: str
    grade: str
    signal_type: str
    score: float
    entry: float
    sl: float
    tp1: float
    tp2: float
    outcome: str      # "TP1", "TP2", "TP3", "SL", "EXPIRED"
    pnl_r: float      # in R units
    candle_index: int
    timestamp: int


@dataclass
class BacktestResult:
    symbol: str
    total_trades: int
    wins: int
    losses: int
    win_rate: float
    avg_rr: float
    total_r: float
    max_drawdown: float
    sharpe: float
    by_grade: Dict[str, dict] = field(default_factory=dict)
    by_type: Dict[str, dict] = field(default_factory=dict)
    trades: List[BacktestTrade] = field(default_factory=list)
    equity_curve: List[float] = field(default_factory=list)


class BacktestEngine:
    """
    Offline walk-forward backtester.
    Fetches historical klines from Binance REST API.
    Replays candles through indicator + signal logic.
    Does NOT use live WS — pure historical data.
    """

    LOOKBACK_CANDLES = 1000    # candles of history to fetch
    WARMUP_CANDLES = 200       # candles needed before signals start

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None

    async def run(
        self,
        symbols: List[str] = None,
        tf: str = "15m",
        limit: int = 500,
    ) -> Dict[str, BacktestResult]:
        if symbols is None:
            symbols = config.SYMBOLS[:5]  # limit for speed

        self._session = aiohttp.ClientSession()
        results = {}

        try:
            for symbol in symbols:
                log.info(f"Backtesting {symbol} {tf}...")
                try:
                    result = await self._backtest_symbol(symbol, tf, limit)
                    results[symbol] = result
                    self._print_result(result)
                except Exception as e:
                    log.error(f"Backtest error {symbol}: {e}")
        finally:
            await self._session.close()

        # Aggregate
        self._print_aggregate(results)
        self._save_results(results)
        return results

    async def _backtest_symbol(self, symbol: str, tf: str, limit: int) -> BacktestResult:
        candles = await self._fetch_candles(symbol, tf, self.LOOKBACK_CANDLES + limit)
        if len(candles) < self.WARMUP_CANDLES + 50:
            raise ValueError(f"Insufficient data: {len(candles)} candles")

        trades = []
        equity = [1.0]
        current_equity = 1.0

        from data_processor import IndicatorCalc
        calc = IndicatorCalc()

        for i in range(self.WARMUP_CANDLES, len(candles) - 5):
            window = candles[:i + 1]
            closes = np.array([c["c"] for c in window])
            highs = np.array([c["h"] for c in window])
            lows = np.array([c["l"] for c in window])
            vols = np.array([c["v"] for c in window])

            # Compute indicators
            ema9 = calc.ema(closes, 9)[-1]
            ema21 = calc.ema(closes, 21)[-1]
            ema50 = calc.ema(closes, 50)[-1] if len(closes) >= 50 else closes[-1]
            rsi = calc.rsi(closes)[-1]
            adx_arr, pdi, mdi = calc.adx(highs, lows, closes)
            adx = adx_arr[-1]
            atr = calc.atr(highs, lows, closes)[-1]
            macd, macd_sig, macd_hist = calc.macd(closes)
            vol_sma = calc.sma(vols, 20)[-1]
            vol_ratio = vols[-1] / vol_sma if vol_sma > 0 else 1.0

            price = closes[-1]

            # Simple signal detection (mirrors signal_engine logic)
            direction = None
            signal_type = ""

            # Anti-chop
            if adx < config.ADX_CHOP:
                continue
            if vol_ratio < config.MIN_VOLUME_RATIO:
                continue

            # Trend pullback
            if ema9 > ema21 > ema50 and rsi < 65 and macd_hist[-1] > 0:
                near_ema21 = abs(price - ema21) / price < 0.008
                near_ema50 = abs(price - ema50) / price < 0.012
                if near_ema21 or near_ema50:
                    direction = "LONG"
                    signal_type = "TREND_PULLBACK"

            elif ema9 < ema21 < ema50 and rsi > 35 and macd_hist[-1] < 0:
                near_ema21 = abs(price - ema21) / price < 0.008
                if near_ema21:
                    direction = "SHORT"
                    signal_type = "TREND_PULLBACK"

            if not direction:
                continue

            # Levels
            sl_dist = atr * 2
            sl_dist = min(sl_dist, price * config.MAX_SL_DISTANCE)
            if direction == "LONG":
                entry = price
                sl = entry - sl_dist
                tp1 = entry + sl_dist * config.TP1_RR
                tp2 = entry + sl_dist * config.TP2_RR
            else:
                entry = price
                sl = entry + sl_dist
                tp1 = entry - sl_dist * config.TP1_RR
                tp2 = entry - sl_dist * config.TP2_RR

            # Simulate outcome on next N candles
            outcome, pnl_r = self._simulate_outcome(
                candles[i + 1:i + 20], direction, entry, sl, tp1, tp2
            )

            score = 75.0  # simplified score for backtest

            trade = BacktestTrade(
                symbol=symbol,
                direction=direction,
                grade="TIER2",
                signal_type=signal_type,
                score=score,
                entry=entry,
                sl=sl,
                tp1=tp1,
                tp2=tp2,
                outcome=outcome,
                pnl_r=pnl_r,
                candle_index=i,
                timestamp=candles[i]["ts"],
            )
            trades.append(trade)

            # Equity (2% risk per trade, so 2% × pnl_r change)
            current_equity *= (1 + 0.02 * pnl_r)
            equity.append(current_equity)

        return self._compute_stats(symbol, trades, equity)

    def _simulate_outcome(
        self,
        future_candles: list,
        direction: str,
        entry: float,
        sl: float,
        tp1: float,
        tp2: float,
    ) -> Tuple[str, float]:
        """Simulate which level is hit first."""
        for c in future_candles:
            h, l = c["h"], c["l"]
            if direction == "LONG":
                if l <= sl:
                    return "SL", -1.0
                if h >= tp2:
                    return "TP2", config.TP2_RR
                if h >= tp1:
                    return "TP1", config.TP1_RR
            else:
                if h >= sl:
                    return "SL", -1.0
                if l <= tp2:
                    return "TP2", config.TP2_RR
                if l <= tp1:
                    return "TP1", config.TP1_RR
        return "EXPIRED", 0.0

    def _compute_stats(self, symbol: str, trades: List[BacktestTrade], equity: List[float]) -> BacktestResult:
        if not trades:
            return BacktestResult(symbol=symbol, total_trades=0, wins=0, losses=0,
                                  win_rate=0, avg_rr=0, total_r=0,
                                  max_drawdown=0, sharpe=0, equity_curve=equity)

        wins = sum(1 for t in trades if t.pnl_r > 0)
        losses = len(trades) - wins
        win_rate = wins / len(trades)
        total_r = sum(t.pnl_r for t in trades)
        avg_rr = total_r / len(trades)

        # Max drawdown on equity curve
        eq = np.array(equity)
        peak = np.maximum.accumulate(eq)
        dd = (peak - eq) / peak
        max_dd = float(np.max(dd))

        # Sharpe (annualized, assuming 15m candles)
        returns = np.diff(eq) / eq[:-1]
        candles_per_year = 365 * 24 * 4  # 15m
        if len(returns) > 1 and np.std(returns) > 0:
            sharpe = float(np.mean(returns) / np.std(returns) * np.sqrt(candles_per_year))
        else:
            sharpe = 0.0

        # By grade / type
        by_type: dict = {}
        for t in trades:
            if t.signal_type not in by_type:
                by_type[t.signal_type] = {"count": 0, "wins": 0, "total_r": 0.0}
            by_type[t.signal_type]["count"] += 1
            if t.pnl_r > 0:
                by_type[t.signal_type]["wins"] += 1
            by_type[t.signal_type]["total_r"] += t.pnl_r

        return BacktestResult(
            symbol=symbol,
            total_trades=len(trades),
            wins=wins,
            losses=losses,
            win_rate=win_rate,
            avg_rr=avg_rr,
            total_r=total_r,
            max_drawdown=max_dd,
            sharpe=sharpe,
            by_type=by_type,
            trades=trades,
            equity_curve=equity,
        )

    # ═══════════════════════════════════════════
    # DATA FETCHING
    # ═══════════════════════════════════════════

    async def _fetch_candles(self, symbol: str, tf: str, limit: int) -> list:
        candles = []
        remaining = min(limit, 1500)
        end_time = None

        while remaining > 0:
            fetch = min(remaining, 1000)
            params = {"symbol": symbol, "interval": tf, "limit": fetch}
            if end_time:
                params["endTime"] = end_time

            url = f"{config.BINANCE_BASE_URL}/fapi/v1/klines"
            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    break
                data = await resp.json()
                if not data:
                    break
                chunk = [
                    {
                        "ts": int(d[0]),
                        "o": float(d[1]),
                        "h": float(d[2]),
                        "l": float(d[3]),
                        "c": float(d[4]),
                        "v": float(d[5]),
                    }
                    for d in data
                ]
                candles = chunk + candles
                end_time = data[0][0] - 1
                remaining -= len(data)
                if len(data) < fetch:
                    break
                await asyncio.sleep(0.2)  # rate limit

        return sorted(candles, key=lambda x: x["ts"])

    # ═══════════════════════════════════════════
    # OUTPUT
    # ═══════════════════════════════════════════

    def _print_result(self, r: BacktestResult):
        log.info(
            f"[{r.symbol}] Trades={r.total_trades} WR={r.win_rate:.1%} "
            f"AvgRR={r.avg_rr:.2f} TotalR={r.total_r:.1f} "
            f"MaxDD={r.max_drawdown:.1%} Sharpe={r.sharpe:.2f}"
        )

    def _print_aggregate(self, results: Dict[str, BacktestResult]):
        if not results:
            return
        all_trades = sum(r.total_trades for r in results.values())
        all_wins = sum(r.wins for r in results.values())
        all_r = sum(r.total_r for r in results.values())
        avg_sharpe = np.mean([r.sharpe for r in results.values()])

        log.info("═══ AGGREGATE BACKTEST RESULTS ═══")
        log.info(f"Symbols: {len(results)} | Trades: {all_trades}")
        log.info(f"Win Rate: {all_wins/all_trades:.1%} | Total R: {all_r:.1f}")
        log.info(f"Avg Sharpe: {avg_sharpe:.2f}")

    def _save_results(self, results: Dict[str, BacktestResult]):
        try:
            output = {}
            for sym, r in results.items():
                output[sym] = {
                    "total_trades": r.total_trades,
                    "win_rate": round(r.win_rate, 4),
                    "avg_rr": round(r.avg_rr, 4),
                    "total_r": round(r.total_r, 2),
                    "max_drawdown": round(r.max_drawdown, 4),
                    "sharpe": round(r.sharpe, 3),
                    "by_type": r.by_type,
                }
            with open("backtest_results.json", "w") as f:
                json.dump(output, f, indent=2)
            log.info("Backtest results saved to backtest_results.json")
        except Exception as e:
            log.error(f"Save error: {e}")


# ═══════════════════════════════════════════════
# STANDALONE RUN
# ═══════════════════════════════════════════════

async def run_backtest():
    logging.basicConfig(level=logging.INFO, format=config.LOG_FORMAT)
    engine = BacktestEngine()
    results = await engine.run(
        symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT", "LINKUSDT"],
        tf="15m",
        limit=500,
    )
    return results


if __name__ == "__main__":
    asyncio.run(run_backtest())
