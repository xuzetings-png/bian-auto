
from trading_bot.advanced_backtest import fetch_candles, backtest_strategy
from trading_bot.advanced_strategy import TrendFollowingStrategy
from decimal import Decimal

# 测试多个主流币
symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'DOGEUSDT']

# 参数组合
param_sets = [
    ('Set1', Decimal('0.008'), Decimal('0.004'), 24),
    ('Set2', Decimal('0.012'), Decimal('0.006'), 36),
    ('Set3', Decimal('0.018'), Decimal('0.009'), 48),
]

print('=' * 100)
print(f'{"Symbol":10s} | {"Params":10s} | {"Trades":6s} | {"WinRate":10s} | {"TotalPnL":12s} | {"Sharpe":8s} | {"MaxDD":10s}')
print('=' * 100)

for symbol in symbols:
    try:
        candles = fetch_candles(symbol, interval='1h', limit=500)
        
        for set_name, tp, sl, hold in param_sets:
            strategy = TrendFollowingStrategy(
                adx_threshold=22,
                volume_ratio_threshold=1.1
            )
            result = backtest_strategy(strategy, candles, symbol, tp, sl, hold)
            
            print(f'{symbol:10s} | {set_name:10s} | {result.total_trades:6d} | {result.win_rate:10.1%} | {result.total_pnl_pct:10.2f}% | {result.sharpe_ratio:8.2f} | {result.max_drawdown_pct:8.2f}%')
    except Exception as e:
        print(f'{symbol:10s} | Error: {e}')

print('=' * 100)
