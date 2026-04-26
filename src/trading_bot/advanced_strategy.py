"""
高级量化策略模块
包含多种经过研究验证的策略
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from typing import NamedTuple, Optional

from trading_bot.config import Settings
from trading_bot.models import OrderType, Side, Signal, SignalAction


@dataclass
class Candle:
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


class BollingerBands(NamedTuple):
    middle: Decimal
    upper: Decimal
    lower: Decimal
    bandwidth: Decimal


class Strategy:
    """策略基类"""

    def generate_signal(self, candles: list[Candle], symbol: str) -> Signal | None:
        raise NotImplementedError

    @property
    def name(self) -> str:
        return self.__class__.__name__


class BollingerRSIStrategy(Strategy):
    """
    布林带 + RSI 反转策略
    
    逻辑:
    - 价格突破布林带下轨且 RSI 超卖 (< 30) → 做多
    - 价格突破布林带上轨且 RSI 超买 (> 70) → 做空
    - 需要成交量确认
    """

    def __init__(
        self,
        bb_period: int = 20,
        bb_std_dev: float = 2.0,
        rsi_period: int = 14,
        rsi_overbought: float = 70.0,
        rsi_oversold: float = 30.0,
        volume_ratio_threshold: float = 1.5,
        min_bandwidth: float = 0.01,
    ):
        self.bb_period = bb_period
        self.bb_std_dev = bb_std_dev
        self.rsi_period = rsi_period
        self.rsi_overbought = rsi_overbought
        self.rsi_oversold = rsi_oversold
        self.volume_ratio_threshold = volume_ratio_threshold
        self.min_bandwidth = min_bandwidth

    def calculate_bollinger_bands(self, candles: list[Candle]) -> BollingerBands:
        closes = [c.close for c in candles[-self.bb_period:]]
        mean = sum(closes) / Decimal(len(closes))
        variance = sum((c - mean) ** 2 for c in closes) / Decimal(len(closes))
        std_dev = variance.sqrt() if variance > 0 else Decimal(0)
        upper = mean + Decimal(self.bb_std_dev) * std_dev
        lower = mean - Decimal(self.bb_std_dev) * std_dev
        bandwidth = (upper - lower) / mean if mean > 0 else Decimal(0)
        return BollingerBands(middle=mean, upper=upper, lower=lower, bandwidth=bandwidth)

    def calculate_rsi(self, candles: list[Candle]) -> Decimal:
        closes = [c.close for c in candles[-self.rsi_period - 1:]]
        gains = []
        losses = []
        for i in range(1, len(closes)):
            change = closes[i] - closes[i - 1]
            if change > 0:
                gains.append(change)
                losses.append(Decimal(0))
            else:
                gains.append(Decimal(0))
                losses.append(-change)
        
        avg_gain = sum(gains[-self.rsi_period:]) / Decimal(self.rsi_period) if gains else Decimal(0)
        avg_loss = sum(losses[-self.rsi_period:]) / Decimal(self.rsi_period) if losses else Decimal(0)
        
        if avg_loss == 0:
            return Decimal(100) if avg_gain > 0 else Decimal(50)
        
        rs = avg_gain / avg_loss
        rsi = Decimal(100) - Decimal(100) / (Decimal(1) + rs)
        return rsi

    def calculate_volume_ratio(self, candles: list[Candle]) -> Decimal:
        recent_volumes = [c.volume for c in candles[-30:-1]]
        avg_volume = sum(recent_volumes) / Decimal(len(recent_volumes)) if recent_volumes else Decimal(0)
        current_volume = candles[-1].volume if candles else Decimal(0)
        return current_volume / avg_volume if avg_volume > 0 else Decimal(0)

    def generate_signal(self, candles: list[Candle], symbol: str) -> Signal | None:
        if len(candles) < max(self.bb_period, self.rsi_period) + 10:
            return None
        
        bb = self.calculate_bollinger_bands(candles)
        rsi = self.calculate_rsi(candles)
        volume_ratio = self.calculate_volume_ratio(candles)
        current_close = candles[-1].close
        current_low = candles[-1].low
        current_high = candles[-1].high
        
        if bb.bandwidth < Decimal(self.min_bandwidth):
            return None
        
        # 做多条件：价格触及下轨 + RSI 超卖 + 放量
        if (
            current_low <= bb.lower
            and rsi <= Decimal(self.rsi_oversold)
            and volume_ratio >= Decimal(self.volume_ratio_threshold)
        ):
            return Signal(
                symbol=symbol,
                side=Side.BUY,
                quantity=Decimal(0),
                order_type=OrderType.MARKET,
                reason=f"BOLL_RSI_LONG bb={bb.lower:.4f} rsi={rsi:.2f} vol={volume_ratio:.2f}",
                action=SignalAction.OPEN_LONG,
            )
        
        # 做空条件：价格触及上轨 + RSI 超买 + 放量
        if (
            current_high >= bb.upper
            and rsi >= Decimal(self.rsi_overbought)
            and volume_ratio >= Decimal(self.volume_ratio_threshold)
        ):
            return Signal(
                symbol=symbol,
                side=Side.SELL,
                quantity=Decimal(0),
                order_type=OrderType.MARKET,
                reason=f"BOLL_RSI_SHORT bb={bb.upper:.4f} rsi={rsi:.2f} vol={volume_ratio:.2f}",
                action=SignalAction.OPEN_SHORT,
            )
        
        return None


class TrendFollowingStrategy(Strategy):
    """
    趋势跟踪策略
    
    逻辑:
    - 短期 EMA 上穿长期 EMA（金叉）且 ADX > 25 → 做多
    - 短期 EMA 下穿长期 EMA（死叉）且 ADX > 25 → 做空
    - 结合成交量确认
    """

    def __init__(
        self,
        ema_fast: int = 12,
        ema_slow: int = 26,
        adx_period: int = 14,
        adx_threshold: float = 25.0,
        volume_ratio_threshold: float = 1.2,
    ):
        self.ema_fast = ema_fast
        self.ema_slow = ema_slow
        self.adx_period = adx_period
        self.adx_threshold = adx_threshold
        self.volume_ratio_threshold = volume_ratio_threshold

    def calculate_ema(self, values: list[Decimal], period: int) -> Decimal:
        if not values:
            return Decimal(0)
        alpha = Decimal(2) / Decimal(period + 1)
        ema = values[0]
        for val in values[1:]:
            ema = val * alpha + ema * (Decimal(1) - alpha)
        return ema

    def calculate_adx(self, candles: list[Candle]) -> Decimal:
        if len(candles) < self.adx_period + 1:
            return Decimal(0)
        
        tr_list = []
        plus_dm = []
        minus_dm = []
        
        for i in range(1, len(candles)):
            high = candles[i].high
            low = candles[i].low
            prev_close = candles[i - 1].close
            
            true_range = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )
            tr_list.append(true_range)
            
            up_move = high - candles[i - 1].high
            down_move = candles[i - 1].low - low
            
            if up_move > down_move and up_move > 0:
                plus_dm.append(up_move)
            else:
                plus_dm.append(Decimal(0))
            
            if down_move > up_move and down_move > 0:
                minus_dm.append(down_move)
            else:
                minus_dm.append(Decimal(0))
        
        atr = sum(tr_list[-self.adx_period:]) / Decimal(self.adx_period) if tr_list else Decimal(0)
        
        if atr == 0:
            return Decimal(0)
        
        plus_di = (sum(plus_dm[-self.adx_period:]) / Decimal(self.adx_period)) / atr * Decimal(100)
        minus_di = (sum(minus_dm[-self.adx_period:]) / Decimal(self.adx_period)) / atr * Decimal(100)
        
        dx = abs(plus_di - minus_di) / (plus_di + minus_di) * Decimal(100) if (plus_di + minus_di) > 0 else Decimal(0)
        return dx

    def calculate_volume_ratio(self, candles: list[Candle]) -> Decimal:
        recent_volumes = [c.volume for c in candles[-20:-1]]
        avg_volume = sum(recent_volumes) / Decimal(len(recent_volumes)) if recent_volumes else Decimal(0)
        current_volume = candles[-1].volume if candles else Decimal(0)
        return current_volume / avg_volume if avg_volume > 0 else Decimal(0)

    def generate_signal(self, candles: list[Candle], symbol: str) -> Signal | None:
        required_length = max(self.ema_slow, self.adx_period) + 5
        if len(candles) < required_length:
            return None
        
        closes = [c.close for c in candles]
        ema_fast_current = self.calculate_ema(closes[-self.ema_fast:], self.ema_fast)
        ema_fast_prev = self.calculate_ema(closes[-self.ema_fast - 1:-1], self.ema_fast)
        ema_slow_current = self.calculate_ema(closes[-self.ema_slow:], self.ema_slow)
        ema_slow_prev = self.calculate_ema(closes[-self.ema_slow - 1:-1], self.ema_slow)
        adx = self.calculate_adx(candles)
        volume_ratio = self.calculate_volume_ratio(candles)
        
        if adx < Decimal(self.adx_threshold):
            return None
        
        # 金叉：快线上穿慢线
        if (
            ema_fast_prev <= ema_slow_prev
            and ema_fast_current > ema_slow_current
            and volume_ratio >= Decimal(self.volume_ratio_threshold)
        ):
            return Signal(
                symbol=symbol,
                side=Side.BUY,
                quantity=Decimal(0),
                order_type=OrderType.MARKET,
                reason=f"TREND_LONG ema_fast={ema_fast_current:.4f} ema_slow={ema_slow_current:.4f} adx={adx:.2f}",
                action=SignalAction.OPEN_LONG,
            )
        
        # 死叉：快线下穿慢线
        if (
            ema_fast_prev >= ema_slow_prev
            and ema_fast_current < ema_slow_current
            and volume_ratio >= Decimal(self.volume_ratio_threshold)
        ):
            return Signal(
                symbol=symbol,
                side=Side.SELL,
                quantity=Decimal(0),
                order_type=OrderType.MARKET,
                reason=f"TREND_SHORT ema_fast={ema_fast_current:.4f} ema_slow={ema_slow_current:.4f} adx={adx:.2f}",
                action=SignalAction.OPEN_SHORT,
            )
        
        return None


class MeanReversionStrategy(Strategy):
    """
    均值回归策略（改进版）
    
    逻辑:
    - 计算过去 N 个周期的高低点
    - 当价格接近低点且有反弹迹象 → 做多
    - 当价格接近高点且有回落迹象 → 做空
    - 结合 ATR 和成交量过滤
    """

    def __init__(
        self,
        lookback_period: int = 40,
        min_atr_pct: float = 0.002,
        max_atr_pct: float = 0.03,
        volume_ratio_threshold: float = 1.3,
        pullback_threshold: float = 0.005,
    ):
        self.lookback_period = lookback_period
        self.min_atr_pct = min_atr_pct
        self.max_atr_pct = max_atr_pct
        self.volume_ratio_threshold = volume_ratio_threshold
        self.pullback_threshold = pullback_threshold

    def calculate_atr_pct(self, candles: list[Candle]) -> Decimal:
        period = min(20, len(candles) - 1)
        if period < 2:
            return Decimal(0)
        
        tr_list = []
        for i in range(1, period + 1):
            idx = len(candles) - 1 - i
            tr = max(
                candles[idx].high - candles[idx].low,
                abs(candles[idx].high - candles[idx - 1].close),
                abs(candles[idx].low - candles[idx - 1].close)
            )
            tr_list.append(tr)
        
        avg_tr = sum(tr_list) / Decimal(len(tr_list))
        return avg_tr / candles[-1].close if candles[-1].close > 0 else Decimal(0)

    def calculate_volume_ratio(self, candles: list[Candle]) -> Decimal:
        recent_volumes = [c.volume for c in candles[-30:-1]]
        avg_volume = sum(recent_volumes) / Decimal(len(recent_volumes)) if recent_volumes else Decimal(0)
        current_volume = candles[-1].volume if candles else Decimal(0)
        return current_volume / avg_volume if avg_volume > 0 else Decimal(0)

    def generate_signal(self, candles: list[Candle], symbol: str) -> Signal | None:
        if len(candles) < self.lookback_period + 10:
            return None
        
        atr_pct = self.calculate_atr_pct(candles)
        if atr_pct < Decimal(self.min_atr_pct) or atr_pct > Decimal(self.max_atr_pct):
            return None
        
        lookback = candles[-self.lookback_period - 1:-1]
        prev_high = max(c.high for c in lookback)
        prev_low = min(c.low for c in lookback)
        midpoint = (prev_high + prev_low) / Decimal(2)
        band_width = (prev_high - prev_low) / midpoint if midpoint > 0 else Decimal(0)
        
        if band_width < Decimal(0.008):
            return None
        
        current_close = candles[-1].close
        current_open = candles[-1].open
        volume_ratio = self.calculate_volume_ratio(candles)
        
        # 做多条件：接近低点 + 阳线 + 放量
        dist_from_low = (current_close - prev_low) / prev_low if prev_low > 0 else Decimal(0)
        body_pct = (current_close - current_open) / current_open if current_open > 0 else Decimal(0)
        
        if (
            dist_from_low <= Decimal(0.01)
            and dist_from_low >= Decimal(-0.01)
            and body_pct >= Decimal(self.pullback_threshold)
            and volume_ratio >= Decimal(self.volume_ratio_threshold)
        ):
            return Signal(
                symbol=symbol,
                side=Side.BUY,
                quantity=Decimal(0),
                order_type=OrderType.MARKET,
                reason=f"MEAN_REV_LONG dist={dist_from_low:.4f} body={body_pct:.4f} vol={volume_ratio:.2f}",
                action=SignalAction.OPEN_LONG,
            )
        
        # 做空条件：接近高点 + 阴线 + 放量
        dist_from_high = (prev_high - current_close) / prev_high if prev_high > 0 else Decimal(0)
        
        if (
            dist_from_high <= Decimal(0.01)
            and dist_from_high >= Decimal(-0.01)
            and body_pct <= -Decimal(self.pullback_threshold)
            and volume_ratio >= Decimal(self.volume_ratio_threshold)
        ):
            return Signal(
                symbol=symbol,
                side=Side.SELL,
                quantity=Decimal(0),
                order_type=OrderType.MARKET,
                reason=f"MEAN_REV_SHORT dist={dist_from_high:.4f} body={body_pct:.4f} vol={volume_ratio:.2f}",
                action=SignalAction.OPEN_SHORT,
            )
        
        return None
