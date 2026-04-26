#!/usr/bin/env python3
"""
调试策略，查看具体哪个条件没有满足
"""

import sys

# 添加项目路径
sys.path.insert(0, '/Users/lulu/Documents/Codex/2026-04-25/100u-polymarket/src')

from trading_bot.config import get_settings
from trading_bot.binance_client import BinanceFuturesClient
from trading_bot.momentum_profiles import discover_profile_universe
from trading_bot.momentum_cycle import _tuning

def debug_strategy():
    """调试策略"""
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
        print(f"\n详细调试各币种:")
        for symbol in universe[:3]:  # 测试前3个币种
            print(f"\n=== 调试 {symbol} ===")
            try:
                # 获取K线数据
                klines = client.client.get("/fapi/v1/klines", params={"symbol": symbol, "interval": "1m", "limit": 90})
                klines.raise_for_status()
                
                # 计算指标
                closes_1m = [float(row[4]) for row in klines.json()]
                highs_1m = [float(row[2]) for row in klines.json()]
                lows_1m = [float(row[3]) for row in klines.json()]
                volumes_1m = [float(row[5]) for row in klines.json()]
                
                # 计算基本指标
                last = closes_1m[-1]
                ret_5m = last / closes_1m[-6] - 1
                ret_15m = last / closes_1m[-16] - 1
                
                # 计算EMA
                def ema(values, span):
                    multiplier = 2 / (span + 1)
                    ema_value = values[0]
                    for value in values[1:]:
                        ema_value = value * multiplier + ema_value * (1 - multiplier)
                    return ema_value
                
                ema_fast = ema(closes_1m, 12)
                ema_slow = ema(closes_1m, 36)
                ema_bias = (ema_fast - ema_slow) / last
                
                # 计算波动率
                def atr_pct(candles, last_close):
                    if len(candles) < 2 or last_close <= 0:
                        return 0
                    ranges = []
                    previous_close = candles[0][4]
                    for candle in candles[1:]:
                        high = candle[2]
                        low = candle[3]
                        close = candle[4]
                        true_range = max(high - low, abs(high - previous_close), abs(low - previous_close))
                        ranges.append(true_range)
                        previous_close = close
                    return (sum(ranges) / len(ranges)) / last_close
                
                atr_pct_val = atr_pct(klines.json(), last)
                
                # 计算成交量
                average_volume = sum(volumes_1m[-31:-1]) / 30 if len(volumes_1m) >= 31 else 0
                volume_ratio = volumes_1m[-1] / average_volume if average_volume > 0 else 0
                
                # 计算突破
                BREAKOUT_LOOKBACK = 40
                previous_high = max(highs_1m[-BREAKOUT_LOOKBACK-1:-1])
                previous_low = min(lows_1m[-BREAKOUT_LOOKBACK-1:-1])
                long_breakout = last > previous_high and (last / previous_high - 1) <= 0.02
                short_breakout = last < previous_low and (previous_low / last - 1) <= 0.02
                
                # 计算分数
                score = ret_15m * 0.55 + ret_5m * 0.25 + ema_bias * 0.10 + (volume_ratio - 1) * 0.002
                
                # 打印详细信息
                print(f"  价格: {last:.4f}")
                print(f"  5分钟收益: {ret_5m:.4f}")
                print(f"  15分钟收益: {ret_15m:.4f}")
                print(f"  EMA快线: {ema_fast:.4f}")
                print(f"  EMA慢线: {ema_slow:.4f}")
                print(f"  EMA偏差: {ema_bias:.4f}")
                print(f"  成交量比率: {volume_ratio:.2f}")
                print(f"  ATR%: {atr_pct_val:.4f}")
                print(f"  突破: 长={long_breakout}, 短={short_breakout}")
                print(f"  分数: {score:.4f}")
                
                # 检查条件
                print(f"  条件检查:")
                print(f"    长突破: {long_breakout}")
                print(f"    分数>=0.003: {score >= 0.003}")
                print(f"    5分钟>0: {ret_5m > 0}")
                print(f"    15分钟>0: {ret_15m > 0}")
                print(f"    EMA快>慢: {ema_fast > ema_slow}")
                print(f"    趋势策略: {score >= 0.0024 and ret_5m > 0 and ret_15m > 0 and ema_fast > ema_slow and volume_ratio >= 1.0}")
                
            except Exception as e:
                print(f"  测试失败: {e}")
        
    finally:
        client.close()

if __name__ == "__main__":
    debug_strategy()