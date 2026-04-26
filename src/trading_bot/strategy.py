from __future__ import annotations

from dataclasses import dataclass

from trading_bot.config import Settings
from trading_bot.models import OrderType, Side, Signal, SignalAction


@dataclass(slots=True)
class MarketSnapshot:
    symbol: str
    mark_price: float


class Strategy:
    def generate_signal(self, snapshot: MarketSnapshot) -> Signal | None:
        raise NotImplementedError


class NoopStrategy(Strategy):
    def generate_signal(self, snapshot: MarketSnapshot) -> Signal | None:
        return None


class DemoFlipStrategy(Strategy):
    def __init__(self, interval_ticks: int) -> None:
        self.interval_ticks = max(1, interval_ticks)
        self.tick_count = 0

    def generate_signal(self, snapshot: MarketSnapshot) -> Signal | None:
        self.tick_count += 1
        if self.tick_count % self.interval_ticks != 0:
            return None

        phase = (self.tick_count // self.interval_ticks) % 4
        quantity = 0.001
        if phase == 1:
            return Signal(
                symbol=snapshot.symbol,
                side=Side.BUY,
                quantity=quantity,
                order_type=OrderType.MARKET,
                reason="demo strategy open long",
                action=SignalAction.OPEN_LONG,
            )
        if phase == 2:
            return Signal(
                symbol=snapshot.symbol,
                side=Side.SELL,
                quantity=quantity,
                order_type=OrderType.MARKET,
                reason="demo strategy close long",
                reduce_only=True,
                action=SignalAction.CLOSE_LONG,
            )
        if phase == 3:
            return Signal(
                symbol=snapshot.symbol,
                side=Side.SELL,
                quantity=quantity,
                order_type=OrderType.MARKET,
                reason="demo strategy open short",
                action=SignalAction.OPEN_SHORT,
            )
        return Signal(
            symbol=snapshot.symbol,
            side=Side.BUY,
            quantity=quantity,
            order_type=OrderType.MARKET,
            reason="demo strategy close short",
            reduce_only=True,
            action=SignalAction.CLOSE_SHORT,
        )


def build_strategy(name: str, settings: Settings) -> Strategy:
    if name == "noop":
        return NoopStrategy()
    if name == "demo_flip":
        return DemoFlipStrategy(settings.demo_strategy_interval_ticks)

    raise ValueError(f"Unsupported strategy: {name}")
