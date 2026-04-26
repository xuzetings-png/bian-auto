
from trading_bot.advanced_backtest import fetch_candles, backtest_strategy
from trading_bot.advanced_strategy import BollingerRSIStrategy, TrendFollowingStrategy, MeanReversionStrategy
from decimal import Decimal

# 获取 BTCUSDT 数据
candles = fetch_candles('BTCUSDT', interval='1h', limit=500)
print(f'已获取 {len(candles)} 根 K 线\n')

# 定义参数
tp = Decimal('0.012')
sl = Decimal('0.006')
hold = 36

# 测试所有策略
strategies = [
    ('BollingerRSI', BollingerRSIStrategy(volume_ratio_threshold=1.2, min_bandwidth=0.007)),
    ('TrendFollowing', TrendFollowingStrategy(adx_threshold=22, volume_ratio_threshold=1.1)),
    ('MeanReversion', MeanReversionStrategy(lookback_period=35, pullback_threshold=0.0035)),
]

print('=' * 80)
print(f'{"Strategy":20s} | {"Trades":6s} | {"WinRate":10s} | {"TotalPnL":12s} | {"Sharpe":8s} | {"MaxDD":10s}')
print('=' * 80)

for name, strategy in strategies:
    result = backtest_strategy(strategy, candles, 'BTCUSDT', tp, sl, hold)
    print(f'{name:20s} | {result.total_trades:6d} | {result.win_rate:10.1%} | {result.total_pnl_pct:10.2f}% | {result.sharpe_ratio:8.2f} | {result.max_drawdown_pct:8.2f}%')

print('=' * 80)
