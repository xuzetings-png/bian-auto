#!/usr/bin/env python3
"""
实盘交易测试脚本
使用 TrendFollowingStrategy 在 ETHUSDT 上进行测试
"""

import sys
import time
from decimal import Decimal

# 添加项目路径
sys.path.insert(0, '/Users/lulu/Documents/Codex/2026-04-25/100u-polymarket/src')

from trading_bot.config import get_settings
from trading_bot.advanced_strategy import TrendFollowingStrategy
from trading_bot.advanced_executor import AdvancedExecutor
from trading_bot.logging_utils import configure_logging

def test_live_trading():
    """测试实盘交易"""
    settings = get_settings()
    configure_logging(settings.log_level, log_dir=settings.log_dir_path)
    
    print("=" * 60)
    print("实盘交易测试")
    print("=" * 60)
    print(f"安全模式: {settings.dry_run}")
    print(f"纸面交易: {settings.paper_trading}")
    print(f"API 密钥配置: {settings.has_api_credentials}")
    print("=" * 60)
    
    # 创建策略（使用回测中表现最好的参数）
    strategy = TrendFollowingStrategy(
        adx_threshold=22,
        volume_ratio_threshold=1.1
    )
    
    # 创建执行器
    executor = AdvancedExecutor(
        strategy=strategy,
        symbol="ETHUSDT",  # 回测中表现最好的币种
        quote_size=Decimal("25"),  # 每笔交易 25 USDT
        take_profit_pct=Decimal("0.008"),  # 0.8% 止盈
        stop_loss_pct=Decimal("0.009"),  # 0.9% 止损
        max_hold_hours=24,  # 最大持仓 24 小时
    )
    
    print(f"策略: {strategy.name}")
    print(f"币种: {executor.symbol}")
    print(f"每笔交易金额: {executor.quote_size} USDT")
    print(f"止盈: {executor.take_profit_pct * 100}%")
    print(f"止损: {executor.stop_loss_pct * 100}%")
    print(f"最大持仓时间: {executor.max_hold_hours} 小时")
    print("=" * 60)
    
    # 运行几个周期进行测试
    print("开始运行交易周期...")
    for i in range(3):
        print(f"\n周期 {i+1}/3")
        print("-" * 40)
        executor.run_cycle()
        print(f"等待 10 秒...")
        time.sleep(10)
    
    print("\n" + "=" * 60)
    print("实盘交易测试完成")
    print("=" * 60)
    
    # 如果有仓位，平仓
    if executor.position:
        print("平仓退出...")
        executor.exit_position()

if __name__ == "__main__":
    test_live_trading()