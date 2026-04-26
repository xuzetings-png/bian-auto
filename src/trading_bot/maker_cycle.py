from __future__ import annotations

import json
import time
from collections import deque
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from uuid import uuid4

from trading_bot.binance_client import BinanceFuturesClient
from trading_bot.config import get_settings
from trading_bot.logging_utils import configure_logging

SYMBOL = "ETHUSDC"
POSITION_SIDE = "LONG"
NOTIONAL_USDC = Decimal("55")
TARGET_USDC = Decimal("0.20")
STOP_LOSS_USDC = Decimal("0.40")
MAX_ENTRY_CHASE_USDC = Decimal("0.20")
ENTRY_PULLBACK_USDC = Decimal("0.20")
ENTRY_REBOUND_LIMIT_USDC = Decimal("0.05")
ENTRY_SETUP_SAMPLES = 18
ENTRY_SETUP_MAX_LOOPS = 50
ENTRY_ATTEMPTS = 12
EXIT_ATTEMPTS = 60
SLEEP_SECONDS = 10


def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, log_dir=settings.log_dir_path)
    if settings.paper_trading or settings.dry_run or settings.emergency_stop:
        raise RuntimeError("Maker cycle requires PAPER_TRADING=false, DRY_RUN=false, and EMERGENCY_STOP=false.")

    client = BinanceFuturesClient(settings)
    try:
        rules = client.symbol_rules(SYMBOL)
        _ensure_clean_start(client)
        qty = _entry_qty(client, rules)
        entry = _chase_entry(client, rules, qty)
        if entry is None:
            print(json.dumps({"status": "NO_ENTRY_FILL", "symbol": SYMBOL}, indent=2))
            return

        filled_qty = Decimal(entry["executedQty"])
        avg_price = Decimal(entry["avgPrice"])
        exit_order = _chase_exit(client, rules, filled_qty, avg_price)
        print(json.dumps({
            "status": "DONE" if exit_order else "ENTRY_FILLED_EXIT_RESTING_OR_TIMEOUT",
            "entry": _compact_order(entry),
            "exit": _compact_order(exit_order) if exit_order else None,
            "final_positions": _compact_positions(client.position_risk(SYMBOL)),
        }, indent=2))
    finally:
        client.close()


def _ensure_clean_start(client: BinanceFuturesClient) -> None:
    open_orders = client.open_orders(SYMBOL)
    if open_orders:
        raise RuntimeError(f"Refusing maker cycle because {SYMBOL} has open orders: {len(open_orders)}")
    long_qty = _long_qty(client)
    if long_qty != Decimal("0"):
        raise RuntimeError(f"Refusing maker cycle because {SYMBOL} LONG is not flat: {long_qty}")


def _entry_qty(client: BinanceFuturesClient, rules) -> Decimal:
    mark = Decimal(client.mark_price(SYMBOL)["markPrice"])
    qty = _floor_to_step(NOTIONAL_USDC / mark, rules.market_qty_step)
    if qty * mark < rules.min_notional:
        qty += rules.market_qty_step
    return qty


def _chase_entry(client: BinanceFuturesClient, rules, qty: Decimal) -> dict | None:
    anchor_bid = _wait_for_entry_setup(client)
    if anchor_bid is None:
        return None
    client_order_id = ""
    for _ in range(ENTRY_ATTEMPTS):
        if client_order_id:
            _cancel_quietly(client, client_order_id)
        book = _book(client)
        bid = Decimal(book["bidPrice"])
        if bid > anchor_bid + MAX_ENTRY_CHASE_USDC:
            if client_order_id:
                _cancel_quietly(client, client_order_id)
            return None
        price = _floor_to_step(bid, rules.tick_size)
        client_order_id = f"mk_entry_{uuid4().hex[:20]}"
        order = client.create_order(
            symbol=SYMBOL,
            side="BUY",
            quantity=float(qty),
            order_type="LIMIT",
            price=float(price),
            client_order_id=client_order_id,
            position_side=POSITION_SIDE,
            time_in_force="GTX",
        )
        time.sleep(SLEEP_SECONDS)
        checked = client.query_order(symbol=SYMBOL, order_id=order.get("orderId"), client_order_id=client_order_id)
        if checked.get("status") == "FILLED":
            return checked
    if client_order_id:
        _cancel_quietly(client, client_order_id)
    return None


def _wait_for_entry_setup(client: BinanceFuturesClient) -> Decimal | None:
    mids: deque[Decimal] = deque(maxlen=ENTRY_SETUP_SAMPLES)
    for _ in range(ENTRY_SETUP_MAX_LOOPS):
        book = _book(client)
        bid = Decimal(book["bidPrice"])
        ask = Decimal(book["askPrice"])
        mid = (bid + ask) / Decimal("2")
        mids.append(mid)
        if len(mids) < ENTRY_SETUP_SAMPLES:
            time.sleep(SLEEP_SECONDS)
            continue
        recent_high = max(mids)
        recent_low = min(mids)
        pulled_back = mid <= recent_high - ENTRY_PULLBACK_USDC
        not_breaking_down = mid >= recent_low + ENTRY_REBOUND_LIMIT_USDC
        if pulled_back and not_breaking_down:
            return bid
        time.sleep(SLEEP_SECONDS)
    return None


def _chase_exit(client: BinanceFuturesClient, rules, qty: Decimal, entry_price: Decimal) -> dict | None:
    target_price = entry_price + TARGET_USDC
    stop_price = entry_price - STOP_LOSS_USDC
    client_order_id = ""
    for _ in range(EXIT_ATTEMPTS):
        if client_order_id:
            _cancel_quietly(client, client_order_id)
        if _long_qty(client) <= Decimal("0"):
            return None
        book = _book(client)
        best_bid = Decimal(book["bidPrice"])
        best_ask = Decimal(book["askPrice"])
        if best_bid <= stop_price:
            return _market_stop_close(client, qty)
        passive_ask = _ceil_to_step(Decimal(book["askPrice"]), rules.tick_size)
        price = max(_ceil_to_step(target_price, rules.tick_size), passive_ask)
        client_order_id = f"mk_exit_{uuid4().hex[:20]}"
        order = client.create_order(
            symbol=SYMBOL,
            side="SELL",
            quantity=float(qty),
            order_type="LIMIT",
            price=float(price),
            client_order_id=client_order_id,
            position_side=POSITION_SIDE,
            time_in_force="GTX",
        )
        time.sleep(SLEEP_SECONDS)
        checked = client.query_order(symbol=SYMBOL, order_id=order.get("orderId"), client_order_id=client_order_id)
        if checked.get("status") == "FILLED":
            return checked
        if best_ask > target_price + Decimal("0.30"):
            # When price has already moved through our small target, keep the quote near the touch.
            target_price = _ceil_to_step(best_ask, rules.tick_size)
    if client_order_id:
        _cancel_quietly(client, client_order_id)
    return None


def _book(client: BinanceFuturesClient) -> dict:
    response = client.client.get("/fapi/v1/ticker/bookTicker", params={"symbol": SYMBOL})
    response.raise_for_status()
    return response.json()


def _long_qty(client: BinanceFuturesClient) -> Decimal:
    for item in client.position_risk(SYMBOL):
        if item.get("positionSide") == POSITION_SIDE:
            return Decimal(item.get("positionAmt", "0"))
    return Decimal("0")


def _cancel_quietly(client: BinanceFuturesClient, client_order_id: str) -> None:
    try:
        client.cancel_order(symbol=SYMBOL, client_order_id=client_order_id)
    except Exception:
        pass


def _market_stop_close(client: BinanceFuturesClient, qty: Decimal) -> dict:
    client_order_id = f"mk_stop_{uuid4().hex[:20]}"
    create = client.create_order(
        symbol=SYMBOL,
        side="SELL",
        quantity=float(qty),
        order_type="MARKET",
        client_order_id=client_order_id,
        position_side=POSITION_SIDE,
    )
    return client.query_order(
        symbol=SYMBOL,
        order_id=create.get("orderId"),
        client_order_id=client_order_id,
    )


def _compact_order(order: dict | None) -> dict | None:
    if order is None:
        return None
    return {
        "orderId": order.get("orderId"),
        "status": order.get("status"),
        "side": order.get("side"),
        "price": order.get("price"),
        "executedQty": order.get("executedQty"),
        "avgPrice": order.get("avgPrice"),
        "cumQuote": order.get("cumQuote"),
    }


def _compact_positions(rows: list[dict]) -> list[dict]:
    return [
        {
            "positionSide": item.get("positionSide"),
            "positionAmt": item.get("positionAmt"),
            "entryPrice": item.get("entryPrice"),
            "unRealizedProfit": item.get("unRealizedProfit"),
        }
        for item in rows
    ]


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).quantize(Decimal("1"), rounding=ROUND_FLOOR) * step


def _ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).quantize(Decimal("1"), rounding=ROUND_CEILING) * step
