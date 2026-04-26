"""
高级回测系统
用于测试多种策略的表现
"""
from __future__ import annotations

import csv
import json
import logging
import math
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import httpx

from trading_bot.advanced_strategy import (
    BollingerRSIStrategy,
    Candle,
    MeanReversionStrategy,
    Strategy,
    TrendFollowingStrategy,
)
from trading_bot.config import get_settings
from trading_bot.logging_utils import configure_logging


class PositionStatus(Enum):
    NONE = "NONE"
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass
class Trade:
    symbol: str
    entry_index: int
    exit_index: int
    entry_price: Decimal
    exit_price: Decimal
    side: str
    pnl: Decimal
    pnl_pct: Decimal
    reason: str


@dataclass
class BacktestResult:
    strategy_name: str
    symbol: str
    interval: str
    total_trades: int
    wins: int
    losses: int
    win_rate: float
    total_pnl_pct: float
    avg_pnl_pct: float
    max_drawdown_pct: float
    sharpe_ratio: float
    best_trade_pct: float
    worst_trade_pct: float
    trades: list[Trade]


LOGGER = logging.getLogger(__name__)
DEFAULT_INTERVAL = "1h"
DEFAULT_LIMIT = 1000
FEE_RATE = Decimal("0.001")
DEFAULT_INITIAL_CAPITAL = Decimal("10000")


def fetch_candles(
    symbol: str,
    interval: str = DEFAULT_INTERVAL,
    limit: int = DEFAULT_LIMIT,
    proxy_url: str | None = None,
) -> list[Candle]:
    """从 Binance 获取 K 线数据"""
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    
    client_kwargs: dict[str, Any] = {"timeout": 30.0}
    if proxy_url:
        client_kwargs["proxy"] = proxy_url
    
    with httpx.Client(**client_kwargs) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
    
    candles = []
    for row in data:
        candles.append(
            Candle(
                open=Decimal(str(row[1])),
                high=Decimal(str(row[2])),
                low=Decimal(str(row[3])),
                close=Decimal(str(row[4])),
                volume=Decimal(str(row[5])),
            )
        )
    return candles


def calculate_max_drawdown(equity_curve: list[Decimal]) -> float:
    """计算最大回撤"""
    peak = Decimal("-inf")
    max_drawdown = Decimal("0")
    
    for equity in equity_curve:
        if equity > peak:
            peak = equity
        if peak > 0:
            drawdown = (peak - equity) / peak
            if drawdown > max_drawdown:
                max_drawdown = drawdown
    
    return float(max_drawdown)


def calculate_sharpe_ratio(returns: list[Decimal], risk_free_rate: float = 0.0) -> float:
    """计算夏普比率（简化版）"""
    if len(returns) < 2:
        return 0.0
    
    mean_return = sum(returns) / Decimal(len(returns))
    variance = sum((r - mean_return) ** 2 for r in returns) / Decimal(len(returns) - 1)
    std_dev = variance.sqrt() if variance > 0 else Decimal(0)
    
    if std_dev == 0:
        return 0.0
    
    sharpe = (mean_return - Decimal(risk_free_rate)) / std_dev
    return float(sharpe)


def backtest_strategy(
    strategy: Strategy,
    candles: list[Candle],
    symbol: str,
    take_profit_pct: Decimal = Decimal("0.01"),
    stop_loss_pct: Decimal = Decimal("0.005"),
    max_hold_bars: int = 48,
) -> BacktestResult:
    """运行单个策略的回测"""
    trades: list[Trade] = []
    equity_curve: list[Decimal] = [DEFAULT_INITIAL_CAPITAL]
    current_capital = DEFAULT_INITIAL_CAPITAL
    
    position_status = PositionStatus.NONE
    entry_price = Decimal("0")
    entry_index = -1
    position_size = Decimal("0")
    
    for i in range(50, len(candles)):
        candles_up_to_now = candles[: i + 1]
        signal = strategy.generate_signal(candles_up_to_now, symbol)
        
        # 如果有持仓，先检查止盈止损和时间退出
        if position_status != PositionStatus.NONE:
            current_candle = candles[i]
            should_exit = False
            exit_reason = ""
            exit_price = Decimal("0")
            
            # 止盈
            if position_status == PositionStatus.LONG:
                if current_candle.high >= entry_price * (Decimal("1") + take_profit_pct):
                    exit_price = entry_price * (Decimal("1") + take_profit_pct)
                    exit_reason = "TP"
                    should_exit = True
                elif current_candle.low <= entry_price * (Decimal("1") - stop_loss_pct):
                    exit_price = entry_price * (Decimal("1") - stop_loss_pct)
                    exit_reason = "SL"
                    should_exit = True
            elif position_status == PositionStatus.SHORT:
                if current_candle.low <= entry_price * (Decimal("1") - take_profit_pct):
                    exit_price = entry_price * (Decimal("1") - take_profit_pct)
                    exit_reason = "TP"
                    should_exit = True
                elif current_candle.high >= entry_price * (Decimal("1") + stop_loss_pct):
                    exit_price = entry_price * (Decimal("1") + stop_loss_pct)
                    exit_reason = "SL"
                    should_exit = True
            
            # 时间退出
            if not should_exit and (i - entry_index) >= max_hold_bars:
                exit_price = current_candle.close
                exit_reason = "TIME"
                should_exit = True
            
            if should_exit:
                if position_status == PositionStatus.LONG:
                    pnl = (exit_price - entry_price) / entry_price - FEE_RATE * Decimal("2")
                else:
                    pnl = (entry_price - exit_price) / entry_price - FEE_RATE * Decimal("2")
                
                current_capital = current_capital * (Decimal("1") + pnl)
                equity_curve.append(current_capital)
                
                trades.append(
                    Trade(
                        symbol=symbol,
                        entry_index=entry_index,
                        exit_index=i,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        side=position_status.value,
                        pnl=pnl,
                        pnl_pct=pnl * Decimal("100"),
                        reason=exit_reason,
                    )
                )
                
                position_status = PositionStatus.NONE
                continue
        
        # 没有持仓，检查入场信号
        if signal and position_status == PositionStatus.NONE:
            if i + 1 >= len(candles):
                continue
            
            entry_candle = candles[i + 1]
            entry_price = entry_candle.open
            entry_index = i + 1
            
            if "LONG" in signal.reason:
                position_status = PositionStatus.LONG
            else:
                position_status = PositionStatus.SHORT
    
    # 强制关闭未平仓
    if position_status != PositionStatus.NONE and entry_index > 0:
        exit_price = candles[-1].close
        if position_status == PositionStatus.LONG:
            pnl = (exit_price - entry_price) / entry_price - FEE_RATE * Decimal("2")
        else:
            pnl = (entry_price - exit_price) / entry_price - FEE_RATE * Decimal("2")
        
        current_capital = current_capital * (Decimal("1") + pnl)
        equity_curve.append(current_capital)
        
        trades.append(
            Trade(
                symbol=symbol,
                entry_index=entry_index,
                exit_index=len(candles) - 1,
                entry_price=entry_price,
                exit_price=exit_price,
                side=position_status.value,
                pnl=pnl,
                pnl_pct=pnl * Decimal("100"),
                reason="FORCED_CLOSE",
            )
        )
    
    # 计算统计数据
    wins = sum(1 for t in trades if t.pnl > 0)
    losses = len(trades) - wins
    win_rate = wins / len(trades) if trades else 0.0
    
    total_pnl_pct = float((current_capital - DEFAULT_INITIAL_CAPITAL) / DEFAULT_INITIAL_CAPITAL * Decimal("100"))
    avg_pnl_pct = float(sum(t.pnl_pct for t in trades) / Decimal(len(trades))) if trades else 0.0
    
    max_drawdown_pct = calculate_max_drawdown(equity_curve) * 100
    
    returns = [(equity_curve[i] - equity_curve[i - 1]) / equity_curve[i - 1] for i in range(1, len(equity_curve))]
    sharpe_ratio = calculate_sharpe_ratio(returns)
    
    best_trade_pct = float(max(t.pnl_pct for t in trades)) if trades else 0.0
    worst_trade_pct = float(min(t.pnl_pct for t in trades)) if trades else 0.0
    
    return BacktestResult(
        strategy_name=strategy.name,
        symbol=symbol,
        interval=DEFAULT_INTERVAL,
        total_trades=len(trades),
        wins=wins,
        losses=losses,
        win_rate=win_rate,
        total_pnl_pct=total_pnl_pct,
        avg_pnl_pct=avg_pnl_pct,
        max_drawdown_pct=max_drawdown_pct,
        sharpe_ratio=sharpe_ratio,
        best_trade_pct=best_trade_pct,
        worst_trade_pct=worst_trade_pct,
        trades=trades,
    )


def get_test_symbols() -> list[str]:
    """获取测试用的交易对列表"""
    return [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "BNBUSDT",
        "XRPUSDT",
        "DOGEUSDT",
        "AVAXUSDT",
        "LINKUSDT",
        "MATICUSDT",
        "DOTUSDT",
    ]


def run_backtest_suite() -> None:
    """运行完整的回测套件"""
    settings = get_settings()
    configure_logging(settings.log_level, log_dir=settings.log_dir_path)
    
    output_dir = Path(settings.state_dir_path) / "backtests_advanced"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    LOGGER.info("开始高级策略回测套件")
    
    # 创建策略列表
    strategies: list[Strategy] = [
        BollingerRSIStrategy(
            bb_period=20,
            bb_std_dev=2.0,
            rsi_period=14,
            rsi_overbought=70,
            rsi_oversold=30,
            volume_ratio_threshold=1.3,
            min_bandwidth=0.008,
        ),
        TrendFollowingStrategy(
            ema_fast=12,
            ema_slow=26,
            adx_period=14,
            adx_threshold=25,
            volume_ratio_threshold=1.2,
        ),
        MeanReversionStrategy(
            lookback_period=40,
            min_atr_pct=0.002,
            max_atr_pct=0.03,
            volume_ratio_threshold=1.3,
            pullback_threshold=0.004,
        ),
    ]
    
    # 参数网格搜索
    param_grids = {
        "take_profit_pct": [Decimal("0.008"), Decimal("0.012"), Decimal("0.018")],
        "stop_loss_pct": [Decimal("0.004"), Decimal("0.006"), Decimal("0.009")],
        "max_hold_bars": [24, 36, 48],
    }
    
    symbols = get_test_symbols()
    all_results: list[BacktestResult] = []
    
    proxy_url = getattr(settings, "binance_proxy_url", None)
    
    for symbol in symbols:
        LOGGER.info(f"正在下载 {symbol} 的历史数据...")
        try:
            candles = fetch_candles(symbol, proxy_url=proxy_url)
            LOGGER.info(f"已获取 {len(candles)} 根 K 线")
        except Exception as e:
            LOGGER.error(f"获取 {symbol} 数据失败: {e}")
            continue
        
        for strategy in strategies:
            for tp in param_grids["take_profit_pct"]:
                for sl in param_grids["stop_loss_pct"]:
                    for hold in param_grids["max_hold_bars"]:
                        result = backtest_strategy(
                            strategy=strategy,
                            candles=candles,
                            symbol=symbol,
                            take_profit_pct=tp,
                            stop_loss_pct=sl,
                            max_hold_bars=hold,
                        )
                        
                        if result.total_trades > 0:
                            all_results.append(result)
                            LOGGER.info(
                                f"{symbol} | {strategy.name} | TP={tp} SL={sl} HOLD={hold} | "
                                f"Trades={result.total_trades} WinRate={result.win_rate:.1%} "
                                f"PnL={result.total_pnl_pct:.2f}% Sharpe={result.sharpe_ratio:.2f}"
                            )
    
    # 保存结果
    save_results(all_results, output_dir)


def save_results(results: list[BacktestResult], output_dir: Path) -> None:
    """保存回测结果"""
    if not results:
        LOGGER.warning("没有回测结果可保存")
        return
    
    # 按总盈亏排序
    results_sorted = sorted(results, key=lambda r: r.total_pnl_pct, reverse=True)
    
    # 保存为 JSON
    json_path = output_dir / "backtest_results.json"
    results_dict = []
    for r in results_sorted:
        results_dict.append({
            "strategy_name": r.strategy_name,
            "symbol": r.symbol,
            "interval": r.interval,
            "total_trades": r.total_trades,
            "wins": r.wins,
            "losses": r.losses,
            "win_rate": r.win_rate,
            "total_pnl_pct": r.total_pnl_pct,
            "avg_pnl_pct": r.avg_pnl_pct,
            "max_drawdown_pct": r.max_drawdown_pct,
            "sharpe_ratio": r.sharpe_ratio,
            "best_trade_pct": r.best_trade_pct,
            "worst_trade_pct": r.worst_trade_pct,
            "trades": [
                {
                    "symbol": t.symbol,
                    "entry_index": t.entry_index,
                    "exit_index": t.exit_index,
                    "entry_price": float(t.entry_price),
                    "exit_price": float(t.exit_price),
                    "side": t.side,
                    "pnl_pct": float(t.pnl_pct),
                    "reason": t.reason,
                }
                for t in r.trades
            ],
        })
    
    json_path.write_text(json.dumps(results_dict, ensure_ascii=False, indent=2), encoding="utf-8")
    
    # 保存为 CSV（摘要）
    csv_path = output_dir / "backtest_summary.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Strategy",
            "Symbol",
            "Trades",
            "Wins",
            "Losses",
            "WinRate",
            "TotalPnL%",
            "AvgPnL%",
            "MaxDD%",
            "Sharpe",
            "Best%",
            "Worst%",
        ])
        
        for r in results_sorted:
            writer.writerow([
                r.strategy_name,
                r.symbol,
                r.total_trades,
                r.wins,
                r.losses,
                f"{r.win_rate:.4f}",
                f"{r.total_pnl_pct:.4f}",
                f"{r.avg_pnl_pct:.4f}",
                f"{r.max_drawdown_pct:.4f}",
                f"{r.sharpe_ratio:.4f}",
                f"{r.best_trade_pct:.4f}",
                f"{r.worst_trade_pct:.4f}",
            ])
    
    # 打印最佳结果
    LOGGER.info("\n" + "=" * 80)
    LOGGER.info("TOP 10 最佳策略表现")
    LOGGER.info("=" * 80)
    
    for i, r in enumerate(results_sorted[:10]):
        LOGGER.info(
            f"{i+1:2d}. {r.strategy_name:25s} | {r.symbol:10s} | "
            f"PnL={r.total_pnl_pct:8.2f}% | WinRate={r.win_rate:6.1%} | "
            f"Trades={r.total_trades:3d} | Sharpe={r.sharpe_ratio:6.2f}"
        )
    
    LOGGER.info(f"\n结果已保存至: {output_dir}")


def main() -> None:
    run_backtest_suite()


if __name__ == "__main__":
    main()
