from __future__ import annotations

import json
import logging
from decimal import Decimal, ROUND_DOWN
from uuid import uuid4

from trading_bot.binance_client import BinanceFuturesClient
from trading_bot.config import get_settings
from trading_bot.execution_rules import validate_signal_against_rules
from trading_bot.logging_utils import configure_logging
from trading_bot.models import OrderType, Side, Signal, SignalAction

LOGGER = logging.getLogger(__name__)

DEFAULT_SYMBOL = "ETHUSDT"
DEFAULT_NOTIONAL = Decimal("55")


def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, log_dir=settings.log_dir_path)

    if settings.paper_trading or settings.dry_run or settings.emergency_stop:
        raise RuntimeError(
            "Live once mode requires PAPER_TRADING=false, DRY_RUN=false, and EMERGENCY_STOP=false."
        )

    client = BinanceFuturesClient(settings)
    try:
        symbol = DEFAULT_SYMBOL
        rules = client.symbol_rules(symbol)
        mark_price = Decimal(client.mark_price(symbol)["markPrice"])
        quantity = _floor_to_step(DEFAULT_NOTIONAL / mark_price, rules.market_qty_step)
        signal = Signal(
            symbol=symbol,
            side=Side.BUY,
            quantity=float(quantity),
            order_type=OrderType.MARKET,
            reason="one-shot live test order",
            action=SignalAction.OPEN_LONG,
        )

        validate_signal_against_rules(signal, rules, mark_price=float(mark_price))
        _ensure_symbol_flat(client, symbol)

        client_order_id = f"live_once_{uuid4().hex[:20]}"
        create_response = client.create_order(
            symbol=symbol,
            side=signal.side.value,
            quantity=signal.quantity,
            order_type=signal.order_type.value,
            client_order_id=client_order_id,
            position_side="LONG",
        )
        order = client.query_order(
            symbol=symbol,
            order_id=create_response.get("orderId"),
            client_order_id=client_order_id,
        )

        payload = {
            "symbol": symbol,
            "side": signal.side.value,
            "requested_notional_usdt": str(DEFAULT_NOTIONAL),
            "mark_price": str(mark_price),
            "quantity": signal.quantity,
            "client_order_id": client_order_id,
            "create_order_response": {
                "orderId": create_response.get("orderId"),
                "status": create_response.get("status"),
                "executedQty": create_response.get("executedQty"),
                "avgPrice": create_response.get("avgPrice"),
            },
            "query_order_response": {
                "orderId": order.get("orderId"),
                "status": order.get("status"),
                "executedQty": order.get("executedQty"),
                "avgPrice": order.get("avgPrice"),
                "cumQuote": order.get("cumQuote"),
            },
        }
        print(json.dumps(payload, indent=2))
    finally:
        client.close()


def _ensure_symbol_flat(client: BinanceFuturesClient, symbol: str) -> None:
    rows = client.position_risk(symbol)
    position_qty = sum(float(item.get("positionAmt", 0.0)) for item in rows)
    if abs(position_qty) > 1e-9:
        raise RuntimeError(
            f"Refusing one-shot live test because {symbol} already has an open position: {position_qty}"
        )


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step == 0:
        return value
    units = (value / step).quantize(Decimal("1"), rounding=ROUND_DOWN)
    return units * step
