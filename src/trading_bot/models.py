from dataclasses import dataclass
from decimal import Decimal
from enum import Enum


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class SignalAction(str, Enum):
    OPEN_LONG = "OPEN_LONG"
    CLOSE_LONG = "CLOSE_LONG"
    OPEN_SHORT = "OPEN_SHORT"
    CLOSE_SHORT = "CLOSE_SHORT"


@dataclass(slots=True)
class SymbolRules:
    symbol: str
    tick_size: Decimal
    min_price: Decimal
    min_qty: Decimal
    qty_step: Decimal
    market_min_qty: Decimal
    market_qty_step: Decimal
    min_notional: Decimal


@dataclass(slots=True)
class Signal:
    symbol: str
    side: Side
    quantity: float
    order_type: OrderType = OrderType.MARKET
    price: float | None = None
    reduce_only: bool = False
    reason: str = "unspecified"
    action: SignalAction | None = None
