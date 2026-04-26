from __future__ import annotations

from decimal import Decimal, ROUND_DOWN

from trading_bot.models import OrderType, Signal, SymbolRules


def validate_signal_against_rules(signal: Signal, rules: SymbolRules, *, mark_price: float) -> None:
    quantity = Decimal(str(signal.quantity))
    if signal.order_type == OrderType.MARKET:
        min_qty = rules.market_min_qty
        step = rules.market_qty_step
        compare_price = Decimal(str(mark_price))
    else:
        min_qty = rules.min_qty
        step = rules.qty_step
        if signal.price is None:
            raise ValueError("Limit order requires a price")
        compare_price = Decimal(str(signal.price))
        _ensure_step(compare_price, rules.tick_size, "price")
        if compare_price < rules.min_price:
            raise ValueError(
                f"Limit price {compare_price} lower than Binance minimum price {rules.min_price}"
            )

    _ensure_step(quantity, step, "quantity")
    if quantity < min_qty:
        raise ValueError(f"Quantity {quantity} lower than Binance minimum quantity {min_qty}")

    notional = quantity * compare_price
    if notional < rules.min_notional:
        raise ValueError(f"Order notional {notional} lower than Binance minimum notional {rules.min_notional}")


def normalize_order_values(signal: Signal, rules: SymbolRules) -> Signal:
    quantity = Decimal(str(signal.quantity))
    step = rules.market_qty_step if signal.order_type == OrderType.MARKET else rules.qty_step
    normalized_qty = _round_to_step(quantity, step)
    price = signal.price
    if price is not None:
        normalized_price = _round_to_step(Decimal(str(price)), rules.tick_size)
        price = float(normalized_price)

    signal.quantity = float(normalized_qty)
    signal.price = price
    return signal


def _ensure_step(value: Decimal, step: Decimal, field_name: str) -> None:
    if step == 0:
        return
    normalized = _round_to_step(value, step)
    if normalized != value:
        raise ValueError(f"{field_name} {value} does not align with step {step}")


def _round_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step == 0:
        return value
    units = (value / step).quantize(Decimal("1"), rounding=ROUND_DOWN)
    return units * step
