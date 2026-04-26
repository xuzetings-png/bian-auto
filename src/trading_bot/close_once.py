from __future__ import annotations

import json
from uuid import uuid4

from trading_bot.binance_client import BinanceFuturesClient
from trading_bot.config import get_settings
from trading_bot.logging_utils import configure_logging

DEFAULT_SYMBOL = "ETHUSDT"


def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, log_dir=settings.log_dir_path)

    if settings.paper_trading or settings.dry_run or settings.emergency_stop:
        raise RuntimeError(
            "Close once mode requires PAPER_TRADING=false, DRY_RUN=false, and EMERGENCY_STOP=false."
        )

    client = BinanceFuturesClient(settings)
    try:
        symbol = DEFAULT_SYMBOL
        long_qty = _current_long_qty(client, symbol)
        if long_qty <= 0:
            raise RuntimeError(f"No {symbol} LONG position to close.")

        client_order_id = f"close_once_{uuid4().hex[:20]}"
        create_response = client.create_order(
            symbol=symbol,
            side="SELL",
            quantity=long_qty,
            order_type="MARKET",
            client_order_id=client_order_id,
            position_side="LONG",
        )
        order = client.query_order(
            symbol=symbol,
            order_id=create_response.get("orderId"),
            client_order_id=client_order_id,
        )
        final_rows = client.position_risk(symbol)

        payload = {
            "symbol": symbol,
            "side": "SELL",
            "position_side": "LONG",
            "quantity": long_qty,
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
            "final_positions": [
                {
                    "positionSide": item.get("positionSide"),
                    "positionAmt": item.get("positionAmt"),
                    "entryPrice": item.get("entryPrice"),
                    "unRealizedProfit": item.get("unRealizedProfit"),
                }
                for item in final_rows
            ],
        }
        print(json.dumps(payload, indent=2))
    finally:
        client.close()


def _current_long_qty(client: BinanceFuturesClient, symbol: str) -> float:
    rows = client.position_risk(symbol)
    for item in rows:
        if item.get("positionSide") == "LONG":
            return float(item.get("positionAmt", 0.0))
    return 0.0
