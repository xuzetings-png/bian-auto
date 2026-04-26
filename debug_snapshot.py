#!/usr/bin/env python3
"""
调试 _snapshot_symbol 函数
"""

import sys
import logging

# 添加项目路径
sys.path.insert(0, '/Users/lulu/Documents/Codex/2026-04-25/100u-polymarket/src')

from trading_bot.config import get_settings
from trading_bot.binance_client import BinanceFuturesClient
from trading_bot.momentum_profiles import discover_profile_universe
from trading_bot.momentum_cycle import _tuning, _snapshot_symbol

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def debug_snapshot():
    """调试 _snapshot_symbol 函数"""
    settings = get_settings()
    client = BinanceFuturesClient(settings)
    
    try:
        # 获取交易池
        universe = discover_profile_universe(client, "alt_usdt")
        print(f"交易池币种: {universe}")
        
        # 获取策略参数
        tuning = _tuning("alt_usdt")
        print(f"\n策略参数:")
        print(f"  min_score: {tuning.min_score}")
        print(f"  min_pullback_pct: {tuning.min_pullback_pct}")
        print(f"  max_entry_chase_pct: {tuning.max_entry_chase_pct}")
        
        # 测试每个币种
        print(f"\n测试 _snapshot_symbol 函数:")
        for symbol in universe[:5]:  # 测试前5个币种
            print(f"\n测试 {symbol}:")
            try:
                rules = client.symbol_rules(symbol)
                snapshot = _snapshot_symbol(client, symbol, rules, tuning, force_direction=False)
                if snapshot:
                    print(f"  ✅ 触发信号: {snapshot.side}，分数: {snapshot.score}")
                    print(f"  参考价: {snapshot.anchor_price}，数量: {snapshot.qty}")
                else:
                    print(f"  ❌ 未触发信号")
            except Exception as e:
                print(f"  ❌ 测试失败: {e}")
        
    finally:
        client.close()

if __name__ == "__main__":
    debug_snapshot()