
"""
高级策略执行器 - 用于实盘和纸面交易
"""
from __future__ import annotations

import time
from decimal import Decimal
from typing import Optional
from dataclasses import dataclass

from trading_bot.config import get_settings
from trading_bot.binance_client import BinanceFuturesClient
from trading_bot.advanced_strategy import (
    Candle,
    Strategy,
    BollingerRSIStrategy,
    TrendFollowingStrategy,
    MeanReversionStrategy,
)
from trading_bot.logging_utils import configure_logging
import logging

logger = logging.getLogger(__name__)


@dataclass
class Position:
    symbol: str
    side: str  # 'LONG' or 'SHORT'
    entry_price: Decimal
    size: Decimal
    take_profit: Decimal
    stop_loss: Decimal


class AdvancedExecutor:
    def __init__(
        self,
        strategy: Strategy,
        symbol: str,
        quote_size: Decimal = Decimal("25"),
        take_profit_pct: Decimal = Decimal("0.012"),
        stop_loss_pct: Decimal = Decimal("0.006"),
        max_hold_hours: int = 36,
    ):
        self.strategy = strategy
        self.symbol = symbol
        self.quote_size = quote_size
        self.take_profit_pct = take_profit_pct
        self.stop_loss_pct = stop_loss_pct
        self.max_hold_hours = max_hold_hours
        
        self.settings = get_settings()
        self.client = BinanceFuturesClient(self.settings)
        
        self.position: Optional[Position] = None
        self.entry_time: float = 0
        
    def fetch_candles(self, interval: str = "1h", limit: int = 100) -> list[Candle]:
        """从交易所获取 K 线"""
        response = self.client.client.get(
            self.settings.binance_futures_base_url.rstrip("/") + "/fapi/v1/klines",
            params={"symbol": self.symbol, "interval": interval, "limit": limit},
        )
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
    
    def get_mark_price(self) -> Decimal:
        """获取标记价格"""
        data = self.client.mark_price(self.symbol)
        return Decimal(data["markPrice"])
    
    def calculate_position_size(self, entry_price: Decimal) -> Decimal:
        """计算仓位大小"""
        rules = self.client.symbol_rules(self.symbol)
        notional_value = self.quote_size
        size = notional_value / entry_price
        # 对齐精度
        size = (size / rules.market_qty_step).quantize(Decimal("1")) * rules.market_qty_step
        # 确保满足最小要求
        if size < rules.market_min_qty:
            size = rules.market_min_qty
        return size
    
    def enter_position(self, side: str):
        """开仓"""
        if self.position:
            logger.warning("已有仓位，跳过开仓")
            return
        
        entry_price = self.get_mark_price()
        size = self.calculate_position_size(entry_price)
        
        if side == "LONG":
            take_profit = entry_price * (Decimal("1") + self.take_profit_pct)
            stop_loss = entry_price * (Decimal("1") - self.stop_loss_pct)
        else:
            take_profit = entry_price * (Decimal("1") - self.take_profit_pct)
            stop_loss = entry_price * (Decimal("1") + self.stop_loss_pct)
        
        logger.info(f"开 {side} 仓: {self.symbol} @ {entry_price}, 数量: {size}")
        
        if not self.settings.paper_trading and not self.settings.dry_run:
            # 实盘开仓
            self.client.create_order(
                symbol=self.symbol,
                side="BUY" if side == "LONG" else "SELL",
                order_type="MARKET",
                quantity=float(size),
                position_side=side,
            )
        
        self.position = Position(
            symbol=self.symbol,
            side=side,
            entry_price=entry_price,
            size=size,
            take_profit=take_profit,
            stop_loss=stop_loss,
        )
        self.entry_time = time.time()
    
    def check_exit_conditions(self) -> bool:
        """检查是否应该平仓"""
        if not self.position:
            return False
        
        current_price = self.get_mark_price()
        side = self.position.side
        
        # 止盈检查
        if side == "LONG" and current_price >= self.position.take_profit:
            logger.info(f"止盈触发: {current_price} >= {self.position.take_profit}")
            return True
        if side == "SHORT" and current_price <= self.position.take_profit:
            logger.info(f"止盈触发: {current_price} <= {self.position.take_profit}")
            return True
        
        # 止损检查
        if side == "LONG" and current_price <= self.position.stop_loss:
            logger.info(f"止损触发: {current_price} <= {self.position.stop_loss}")
            return True
        if side == "SHORT" and current_price >= self.position.stop_loss:
            logger.info(f"止损触发: {current_price} >= {self.position.stop_loss}")
            return True
        
        # 时间退出检查
        hours_held = (time.time() - self.entry_time) / 3600
        if hours_held >= self.max_hold_hours:
            logger.info(f"时间到退出: 已持仓 {hours_held:.1f} 小时")
            return True
        
        return False
    
    def exit_position(self):
        """平仓"""
        if not self.position:
            return
        
        side = self.position.side
        size = self.position.size
        
        logger.info(f"平 {side} 仓: {self.symbol}, 数量: {size}")
        
        if not self.settings.paper_trading and not self.settings.dry_run:
            # 实盘平仓
            self.client.create_order(
                symbol=self.symbol,
                side="SELL" if side == "LONG" else "BUY",
                order_type="MARKET",
                quantity=float(size),
                position_side=side,
                reduce_only=True,
            )
        
        self.position = None
        self.entry_time = 0
    
    def run_cycle(self):
        """运行一个交易周期"""
        logger.info(f"开始运行 {self.strategy.name} 策略，币种: {self.symbol}")
        
        try:
            # 获取数据
            candles = self.fetch_candles()
            if len(candles) < 50:
                logger.warning("K 线数据不足")
                return
            
            # 检查是否需要平仓
            if self.position and self.check_exit_conditions():
                self.exit_position()
            
            # 检查是否需要开仓
            if not self.position:
                signal = self.strategy.generate_signal(candles, self.symbol)
                if signal:
                    if "LONG" in signal.reason:
                        self.enter_position("LONG")
                    elif "SHORT" in signal.reason:
                        self.enter_position("SHORT")
        
        except Exception as e:
            logger.error(f"运行出错: {e}", exc_info=True)
    
    def run_continuous(self, poll_interval: int = 3600):
        """持续运行策略"""
        logger.info(f"开始持续运行，检查间隔: {poll_interval} 秒")
        
        try:
            while True:
                self.run_cycle()
                logger.info(f"等待 {poll_interval} 秒...")
                time.sleep(poll_interval)
        except KeyboardInterrupt:
            logger.info("收到停止信号")
            if self.position:
                logger.info("平仓退出...")
                self.exit_position()
        except Exception as e:
            logger.error(f"运行异常: {e}", exc_info=True)


def main():
    """简单演示"""
    settings = get_settings()
    configure_logging(settings.log_level, log_dir=settings.log_dir_path)
    
    # 创建策略
    strategy = TrendFollowingStrategy(adx_threshold=22, volume_ratio_threshold=1.1)
    
    # 创建执行器
    executor = AdvancedExecutor(
        strategy=strategy,
        symbol="BTCUSDT",
        quote_size=Decimal("25"),
        take_profit_pct=Decimal("0.012"),
        stop_loss_pct=Decimal("0.006"),
        max_hold_hours=36,
    )
    
    # 运行
    executor.run_continuous(poll_interval=3600)


if __name__ == "__main__":
    main()
