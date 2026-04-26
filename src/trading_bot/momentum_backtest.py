from __future__ import annotations

import csv
import json
import logging
import math
import time
from dataclasses import dataclass, asdict
from decimal import Decimal
from pathlib import Path
from random import random

import httpx

from trading_bot.config import get_settings
from trading_bot.logging_utils import configure_logging
from trading_bot.momentum_profiles import ALT_USDT_PROFILE, discover_profile_universe

LOGGER = logging.getLogger(__name__)

DEFAULT_LIMIT = 1200
DEFAULT_INTERVAL = "1m"
DEFAULT_TIMEFRAMES = ("1m", "5m", "15m")
DEFAULT_LOOKBACK = 40
DEFAULT_VOLUME_LOOKBACK = 30
DEFAULT_HOLD_BARS = 30
DEFAULT_TP = Decimal("0.0090")
DEFAULT_SL = Decimal("0.0045")
FEE_RATE = Decimal("0.0008")


@dataclass(slots=True)
class BacktestTrade:
    symbol: str
    side: str
    entry_index: int
    exit_index: int
    entry_price: str
    exit_price: str
    pnl_pct: str
    reason: str


@dataclass(slots=True)
class BacktestResult:
    profile: str
    family: str
    interval: str
    lookback: int
    hold_bars: int
    tp: str
    sl: str
    min_volume_ratio: str
    min_atr_pct: str
    max_atr_pct: str
    symbols: list[str]
    trades: int
    wins: int
    losses: int
    win_rate: float
    gross_return_pct: float
    avg_return_pct: float
    max_drawdown_pct: float
    sharpe_like: float
    train_return_pct: float
    test_return_pct: float
    train_trades: int
    test_trades: int
    trades_detail: list[BacktestTrade]


def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, log_dir=settings.log_dir_path)
    out_dir = settings.state_dir_path / "backtests"
    out_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.info("开始离线回测：profile=%s，输出目录=%s。", ALT_USDT_PROFILE, out_dir)

    with httpx.Client(timeout=20.0) as client:
        universe = discover_profile_universe(_BacktestClient(client, settings), ALT_USDT_PROFILE)

    symbols = list(universe[:10])
    if not symbols:
        raise RuntimeError("没有发现可回测的交易对")

    grid = []
    for lookback, hold_bars, tp, sl, volume_ratio, min_atr, max_atr in [
        (20, 18, "0.004", "0.0030", "1.5", "0.0015", "0.0300"),
        (40, 30, "0.006", "0.0045", "1.35", "0.0018", "0.0300"),
        (60, 30, "0.009", "0.0055", "1.5", "0.0020", "0.0300"),
        (80, 36, "0.012", "0.0065", "1.6", "0.0020", "0.0350"),
    ]:
        grid.append(
            {
                "family": "breakout",
                "lookback": lookback,
                "hold_bars": hold_bars,
                "tp": Decimal(tp),
                "sl": Decimal(sl),
                "min_volume_ratio": Decimal(volume_ratio),
                "min_atr_pct": Decimal(min_atr),
                "max_atr_pct": Decimal(max_atr),
            }
        )

    for lookback, hold_bars, tp, sl, volume_ratio, min_atr, max_atr in [
        (30, 24, "0.005", "0.0040", "1.4", "0.0015", "0.0280"),
        (50, 30, "0.007", "0.0050", "1.6", "0.0020", "0.0320"),
        (80, 36, "0.010", "0.0065", "1.7", "0.0020", "0.0350"),
    ]:
        grid.append(
            {
                "family": "pullback",
                "lookback": lookback,
                "hold_bars": hold_bars,
                "tp": Decimal(tp),
                "sl": Decimal(sl),
                "min_volume_ratio": Decimal(volume_ratio),
                "min_atr_pct": Decimal(min_atr),
                "max_atr_pct": Decimal(max_atr),
            }
        )

    for lookback, hold_bars, tp, sl, volume_ratio, min_atr, max_atr in [
        (30, 18, "0.0035", "0.0028", "1.5", "0.0015", "0.0280"),
        (45, 24, "0.0045", "0.0035", "1.5", "0.0018", "0.0300"),
        (60, 24, "0.0060", "0.0045", "1.6", "0.0020", "0.0320"),
    ]:
        grid.append(
            {
                "family": "mean_reversion",
                "lookback": lookback,
                "hold_bars": hold_bars,
                "tp": Decimal(tp),
                "sl": Decimal(sl),
                "min_volume_ratio": Decimal(volume_ratio),
                "min_atr_pct": Decimal(min_atr),
                "max_atr_pct": Decimal(max_atr),
            }
        )

    for lookback, hold_bars, tp, sl, volume_ratio, min_atr, max_atr in [
        (40, 24, "0.006", "0.0045", "1.4", "0.0018", "0.0300"),
        (60, 30, "0.008", "0.0050", "1.5", "0.0020", "0.0320"),
    ]:
        grid.append(
            {
                "family": "trend_pullback",
                "lookback": lookback,
                "hold_bars": hold_bars,
                "tp": Decimal(tp),
                "sl": Decimal(sl),
                "min_volume_ratio": Decimal(volume_ratio),
                "min_atr_pct": Decimal(min_atr),
                "max_atr_pct": Decimal(max_atr),
            }
        )

    for lookback, hold_bars, tp, sl, volume_ratio, min_atr, max_atr in [
        (30, 24, "0.006", "0.0035", "1.4", "0.0015", "0.0280"),
        (45, 30, "0.009", "0.0050", "1.5", "0.0018", "0.0300"),
        (60, 36, "0.012", "0.0065", "1.6", "0.0020", "0.0320"),
    ]:
        grid.append(
            {
                "family": "long_only_trend",
                "lookback": lookback,
                "hold_bars": hold_bars,
                "tp": Decimal(tp),
                "sl": Decimal(sl),
                "min_volume_ratio": Decimal(volume_ratio),
                "min_atr_pct": Decimal(min_atr),
                "max_atr_pct": Decimal(max_atr),
            }
        )

    benchmark_symbols = ["BTCUSDT", "ETHUSDT"]
    candle_cache = {
        interval: {symbol: _load_candles(symbol, interval=interval, limit=DEFAULT_LIMIT) for symbol in symbols + benchmark_symbols}
        for interval in DEFAULT_TIMEFRAMES
    }
    benchmark_regime = {interval: _market_regime(candle_cache[interval]) for interval in DEFAULT_TIMEFRAMES}
    LOGGER.info("当前样本风向：%s。", benchmark_regime)
    reports: list[BacktestResult] = []
    for interval in DEFAULT_TIMEFRAMES:
        for params in grid:
            LOGGER.info("开始回测：interval=%s params=%s", interval, params)
            result = _run_grid(candle_cache[interval], params, benchmark_regime=benchmark_regime[interval], interval=interval)
            reports.append(result)
            LOGGER.info(
                "回测完成：interval=%s family=%s trades=%s win_rate=%.3f net_return=%.2f%% train=%.2f%% test=%.2f%% avg=%.4f%% dd=%.2f%%",
                result.interval,
                result.family,
                result.trades,
                result.win_rate,
                result.gross_return_pct,
                result.train_return_pct,
                result.test_return_pct,
                result.avg_return_pct,
                result.max_drawdown_pct,
            )

    reports.sort(key=lambda item: item.gross_return_pct, reverse=True)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    json_path = out_dir / f"momentum_backtest_{timestamp}.json"
    csv_path = out_dir / f"momentum_backtest_{timestamp}.csv"
    json_path.write_text(json.dumps([asdict(item) for item in reports], ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(csv_path, reports)

    best = reports[0] if reports else None
    if best:
        LOGGER.info(
            "最佳结果：interval=%s family=%s lookback=%s hold=%s tp=%s sl=%s trades=%s win_rate=%.3f net_return=%.2f%% train=%.2f%% test=%.2f%% dd=%.2f%%。",
            best.interval,
            best.family,
            best.lookback,
            best.hold_bars,
            best.tp,
            best.sl,
            best.trades,
            best.win_rate,
            best.gross_return_pct,
            best.train_return_pct,
            best.test_return_pct,
            best.max_drawdown_pct,
        )
        print(json.dumps(asdict(best), ensure_ascii=False, indent=2))
    print(json.dumps({"json_path": str(json_path), "csv_path": str(csv_path), "count": len(reports)}, ensure_ascii=False))
    LOGGER.info("回测完成，结果已写入：%s 和 %s。", json_path, csv_path)


class _BacktestClient:
    def __init__(self, client: httpx.Client, settings) -> None:
        self.client = client
        self.settings = settings

    def open_orders(self, symbol: str | None = None) -> list[dict]:
        return []

    def position_risk(self, symbol: str | None = None) -> list[dict]:
        return []

    def symbol_rules(self, symbol: str):
        response = self.client.get(self.settings.binance_futures_base_url.rstrip("/") + "/fapi/v1/exchangeInfo", params={"symbol": symbol})
        response.raise_for_status()
        payload = response.json()
        symbol_config = next(item for item in payload.get("symbols", []) if item.get("symbol") == symbol)
        filters = {item["filterType"]: item for item in symbol_config.get("filters", [])}
        price_filter = filters["PRICE_FILTER"]
        lot_size = filters["LOT_SIZE"]
        market_lot_size = filters.get("MARKET_LOT_SIZE", lot_size)
        min_notional = filters.get("MIN_NOTIONAL", {"notional": "0"})
        from trading_bot.models import SymbolRules

        return SymbolRules(
            symbol=symbol,
            tick_size=Decimal(price_filter["tickSize"]),
            min_price=Decimal(price_filter["minPrice"]),
            min_qty=Decimal(lot_size["minQty"]),
            qty_step=Decimal(lot_size["stepSize"]),
            market_min_qty=Decimal(market_lot_size["minQty"]),
            market_qty_step=Decimal(market_lot_size["stepSize"]),
            min_notional=Decimal(min_notional["notional"]),
        )

    def mark_price(self, symbol: str) -> dict:
        response = self.client.get(self.settings.binance_futures_base_url.rstrip("/") + "/fapi/v1/premiumIndex", params={"symbol": symbol})
        response.raise_for_status()
        return response.json()

    def exchange_info_all(self) -> dict:
        response = self.client.get(self.settings.binance_futures_base_url.rstrip("/") + "/fapi/v1/exchangeInfo")
        response.raise_for_status()
        return response.json()

    def ticker_24hr_all(self) -> list[dict]:
        response = self.client.get(self.settings.binance_futures_base_url.rstrip("/") + "/fapi/v1/ticker/24hr")
        response.raise_for_status()
        return response.json()


def _run_grid(
    data: dict[str, list[dict[str, Decimal]]],
    params: dict,
    benchmark_regime: dict[str, bool],
    interval: str,
) -> BacktestResult:
    trades: list[BacktestTrade] = []
    equity_curve: list[float] = [1.0]
    total_return = 0.0
    wins = losses = 0
    train_return = 0.0
    test_return = 0.0
    train_trades = 0
    test_trades = 0
    symbols = [symbol for symbol in data if symbol not in {"BTCUSDT", "ETHUSDT"}]
    for symbol in symbols:
        candles = data[symbol]
        symbol_trades = _simulate_symbol(symbol, candles, params, benchmark_regime=benchmark_regime)
        for trade in symbol_trades:
            trades.append(trade)
            pnl = float(trade.pnl_pct)
            total_return += pnl
            if trade.entry_index < len(candles) // 2:
                train_return += pnl
                train_trades += 1
            else:
                test_return += pnl
                test_trades += 1
            if pnl > 0:
                wins += 1
            else:
                losses += 1
            equity_curve.append(1.0 + total_return)
    win_rate = wins / max(1, wins + losses)
    avg_return = total_return / max(1, len(trades))
    max_drawdown = _max_drawdown(equity_curve)
    sharpe_like = _sharpe_like([float(t.pnl_pct) for t in trades])
    return BacktestResult(
        profile=ALT_USDT_PROFILE,
        family=params["family"],
        interval=interval,
        lookback=params["lookback"],
        hold_bars=params["hold_bars"],
        tp=str(params["tp"]),
        sl=str(params["sl"]),
        min_volume_ratio=str(params["min_volume_ratio"]),
        min_atr_pct=str(params["min_atr_pct"]),
        max_atr_pct=str(params["max_atr_pct"]),
        symbols=symbols,
        trades=len(trades),
        wins=wins,
        losses=losses,
        win_rate=win_rate,
        gross_return_pct=total_return * 100.0,
        avg_return_pct=avg_return * 100.0,
        max_drawdown_pct=max_drawdown * 100.0,
        sharpe_like=sharpe_like,
        train_return_pct=train_return * 100.0,
        test_return_pct=test_return * 100.0,
        train_trades=train_trades,
        test_trades=test_trades,
        trades_detail=trades,
    )


def _simulate_symbol(
    symbol: str,
    candles: list[dict[str, Decimal]],
    params: dict,
    benchmark_regime: dict[str, bool],
) -> list[BacktestTrade]:
    trades: list[BacktestTrade] = []
    if len(candles) < params["lookback"] + params["hold_bars"] + 50:
        return trades
    i = params["lookback"] + 40
    while i < len(candles) - params["hold_bars"] - 1:
        window = candles[: i + 1]
        close = window[-1]["close"]
        highs = [c["high"] for c in window]
        lows = [c["low"] for c in window]
        vols = [c["volume"] for c in window]
        prev_high = max(highs[-params["lookback"] - 1 : -1])
        prev_low = min(lows[-params["lookback"] - 1 : -1])
        avg_vol = sum(vols[-31:-1], Decimal("0")) / Decimal(30)
        vol_ratio = vols[-1] / avg_vol if avg_vol > 0 else Decimal("0")
        atr_pct = _atr_pct(window[-21:], close)
        body_pct = abs(close - window[-2]["close"]) / window[-2]["close"] if window[-2]["close"] > 0 else Decimal("0")
        if vol_ratio < params["min_volume_ratio"] or atr_pct < params["min_atr_pct"] or atr_pct > params["max_atr_pct"]:
            i += 1
            continue

        side = None
        if params["family"] == "breakout":
            if close > prev_high and close / prev_high - Decimal("1") <= Decimal("0.015"):
                side = "LONG"
            elif close < prev_low and prev_low / close - Decimal("1") <= Decimal("0.015"):
                side = "SHORT"
        elif params["family"] == "pullback":
            ema_fast = _ema([c["close"] for c in window[-21:]], 9)
            ema_slow = _ema([c["close"] for c in window[-21:]], 21)
            if ema_fast > ema_slow and body_pct >= Decimal("0.001"):
                retrace = (prev_high - close) / prev_high if prev_high > 0 else Decimal("0")
                if Decimal("0.002") <= retrace <= Decimal("0.010"):
                    side = "LONG"
            elif ema_fast < ema_slow and body_pct >= Decimal("0.001"):
                retrace = (close - prev_low) / prev_low if prev_low > 0 else Decimal("0")
                if Decimal("0.002") <= retrace <= Decimal("0.010"):
                    side = "SHORT"
        elif params["family"] == "mean_reversion":
            midpoint = (prev_high + prev_low) / Decimal("2")
            band = (prev_high - prev_low) / midpoint if midpoint > 0 else Decimal("0")
            distance = (close - midpoint) / midpoint if midpoint > 0 else Decimal("0")
            if band >= Decimal("0.008"):
                if distance <= Decimal("-0.004"):
                    side = "LONG"
                elif distance >= Decimal("0.004"):
                    side = "SHORT"
        elif params["family"] == "trend_pullback":
            ema_fast = _ema([c["close"] for c in window[-21:]], 9)
            ema_slow = _ema([c["close"] for c in window[-21:]], 21)
            if ema_fast > ema_slow and close > ema_fast:
                pullback = (ema_fast - close) / ema_fast if ema_fast > 0 else Decimal("0")
                if Decimal("0.0008") <= pullback <= Decimal("0.0045"):
                    side = "LONG"
            elif ema_fast < ema_slow and close < ema_fast:
                pullback = (close - ema_fast) / ema_fast if ema_fast > 0 else Decimal("0")
                if Decimal("0.0008") <= pullback <= Decimal("0.0045"):
                    side = "SHORT"
        elif params["family"] == "long_only_trend":
            ema_fast = _ema([c["close"] for c in window[-21:]], 9)
            ema_slow = _ema([c["close"] for c in window[-21:]], 21)
            rising = close > ema_fast > ema_slow
            breakout = close >= prev_high * Decimal("0.998")
            if rising and breakout and benchmark_regime.get("risk_on", False):
                side = "LONG"
        elif params["family"] == "regime_breakout":
            ema_fast = _ema([c["close"] for c in window[-21:]], 9)
            ema_slow = _ema([c["close"] for c in window[-21:]], 21)
            if benchmark_regime.get("risk_on", False) and ema_fast > ema_slow and close > prev_high:
                side = "LONG"
            elif benchmark_regime.get("risk_off", False) and ema_fast < ema_slow and close < prev_low:
                side = "SHORT"
        if side is None:
            i += 1
            continue

        if params["family"] == "long_only_trend" and side != "LONG":
            i += 1
            continue

        entry = candles[i + 1]["open"]
        if side == "LONG":
            target = entry * (Decimal("1") + params["tp"])
            stop = entry * (Decimal("1") - params["sl"])
        else:
            target = entry * (Decimal("1") - params["tp"])
            stop = entry * (Decimal("1") + params["sl"])

        exit_price = candles[min(i + params["hold_bars"], len(candles) - 1)]["close"]
        reason = "TIME"
        for j in range(i + 1, min(i + params["hold_bars"] + 1, len(candles))):
            high = candles[j]["high"]
            low = candles[j]["low"]
            if side == "LONG":
                if low <= stop:
                    exit_price = stop
                    reason = "STOP"
                    break
                if high >= target:
                    exit_price = target
                    reason = "TP"
                    break
            else:
                if high >= stop:
                    exit_price = stop
                    reason = "STOP"
                    break
                if low <= target:
                    exit_price = target
                    reason = "TP"
                    break

        pnl = (exit_price / entry - Decimal("1")) if side == "LONG" else (entry / exit_price - Decimal("1"))
        pnl -= FEE_RATE * Decimal("2")
        trades.append(
            BacktestTrade(
                symbol=symbol,
                side=side,
                entry_index=i + 1,
                exit_index=min(i + params["hold_bars"], len(candles) - 1),
                entry_price=str(entry),
                exit_price=str(exit_price),
                pnl_pct=str(pnl),
                reason=reason,
            )
        )
        i += max(params["hold_bars"], 10)
    return trades


def _load_candles(symbol: str, *, interval: str, limit: int) -> list[dict[str, Decimal]]:
    last_exc: Exception | None = None
    with httpx.Client(timeout=20.0) as client:
        for attempt in range(1, 4):
            try:
                response = client.get(
                    "https://fapi.binance.com/fapi/v1/klines",
                    params={"symbol": symbol, "interval": interval, "limit": limit},
                )
                response.raise_for_status()
                return [
                    {
                        "open": Decimal(row[1]),
                        "high": Decimal(row[2]),
                        "low": Decimal(row[3]),
                        "close": Decimal(row[4]),
                        "volume": Decimal(row[5]),
                    }
                    for row in response.json()
                ]
            except Exception as exc:  # noqa: BLE001 - backtest needs resilience to flaky network.
                last_exc = exc
                LOGGER.warning("%s 历史K线抓取失败（第%s次）：%s", symbol, attempt, exc)
                if attempt < 3:
                    time.sleep(1.5 * attempt + random())
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"{symbol} 历史K线抓取失败")


def _market_regime(data: dict[str, list[dict[str, Decimal]]]) -> dict[str, bool]:
    btc = data.get("BTCUSDT", [])
    eth = data.get("ETHUSDT", [])
    if len(btc) < 50 or len(eth) < 50:
        return {"risk_on": False, "risk_off": False}
    btc_close = [row["close"] for row in btc[-60:]]
    eth_close = [row["close"] for row in eth[-60:]]
    btc_fast = _ema(btc_close[-21:], 9)
    btc_slow = _ema(btc_close[-21:], 21)
    eth_fast = _ema(eth_close[-21:], 9)
    eth_slow = _ema(eth_close[-21:], 21)
    btc_ret = (btc_close[-1] / btc_close[0] - Decimal("1")) if btc_close[0] > 0 else Decimal("0")
    eth_ret = (eth_close[-1] / eth_close[0] - Decimal("1")) if eth_close[0] > 0 else Decimal("0")
    risk_on = btc_fast > btc_slow and eth_fast > eth_slow and btc_ret > Decimal("0") and eth_ret > Decimal("0")
    risk_off = btc_fast < btc_slow and eth_fast < eth_slow and btc_ret < Decimal("0") and eth_ret < Decimal("0")
    return {"risk_on": risk_on, "risk_off": risk_off}


def _ema(values: list[Decimal], span: int) -> Decimal:
    if not values:
        return Decimal("0")
    alpha = Decimal("2") / Decimal(span + 1)
    ema = values[0]
    for value in values[1:]:
        ema = value * alpha + ema * (Decimal("1") - alpha)
    return ema


def _atr_pct(candles: list[dict[str, Decimal]], last_close: Decimal) -> Decimal:
    ranges: list[Decimal] = []
    previous_close = candles[0]["close"]
    for candle in candles[1:]:
        ranges.append(
            max(
                candle["high"] - candle["low"],
                abs(candle["high"] - previous_close),
                abs(candle["low"] - previous_close),
            )
        )
        previous_close = candle["close"]
    if not ranges or last_close <= 0:
        return Decimal("0")
    return sum(ranges, Decimal("0")) / Decimal(len(ranges)) / last_close


def _max_drawdown(equity: list[float]) -> float:
    peak = -math.inf
    max_dd = 0.0
    for value in equity:
        peak = max(peak, value)
        if peak > 0:
            max_dd = max(max_dd, (peak - value) / peak)
    return max_dd


def _sharpe_like(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    if variance <= 0:
        return 0.0
    return mean / variance**0.5


def _write_csv(path: Path, reports: list[BacktestResult]) -> None:
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([
            "family",
            "interval",
            "lookback",
            "hold_bars",
            "tp",
            "sl",
            "trades",
            "train_trades",
            "test_trades",
            "wins",
            "win_rate",
            "gross_return_pct",
            "train_return_pct",
            "test_return_pct",
            "avg_return_pct",
            "max_drawdown_pct",
            "sharpe_like",
        ])
        for report in reports:
            writer.writerow([
                report.family,
                report.interval,
                report.lookback,
                report.hold_bars,
                report.tp,
                report.sl,
                report.trades,
                report.train_trades,
                report.test_trades,
                report.wins,
                f"{report.win_rate:.4f}",
                f"{report.gross_return_pct:.4f}",
                f"{report.train_return_pct:.4f}",
                f"{report.test_return_pct:.4f}",
                f"{report.avg_return_pct:.4f}",
                f"{report.max_drawdown_pct:.4f}",
                f"{report.sharpe_like:.4f}",
            ])


def main() -> None:
    run()


if __name__ == "__main__":
    main()
