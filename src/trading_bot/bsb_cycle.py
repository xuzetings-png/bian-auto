from __future__ import annotations

import json
import time
from collections import deque
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from uuid import uuid4

import httpx
from tenacity import RetryError

from trading_bot.binance_client import BinanceFuturesClient
from trading_bot.config import get_settings
from trading_bot.logging_utils import configure_logging

SYMBOL = "BSBUSDT"
LONG_SIDE = "LONG"
SHORT_SIDE = "SHORT"
ENTRY_NOTIONAL_PER_SIDE = Decimal("5.1")
TAKE_PROFIT_PCT = Decimal("0.001")
STOP_LOSS_PCT = Decimal("0.005")
MAX_ENTRY_CHASE = Decimal("0.00080")
ENTRY_PULLBACK = Decimal("0.00016")
ENTRY_REBOUND = Decimal("0.00005")
ENTRY_TREND_BIAS = Decimal("0.00005")
MIN_SETUP_RANGE = Decimal("0.00018")
ENTRY_SETUP_SAMPLES = 16
ENTRY_SETUP_MAX_LOOPS = 60
ENTRY_ATTEMPTS = 10
EXIT_POLL_SECONDS = 10
MAX_ENTRY_WAIT_SECONDS = 180
MAX_MANAGE_SECONDS = 600
MIN_ENTRY_SPREAD_TICKS = Decimal("3")
TARGET_LEVERAGE = 5
MAX_ENTRY_PRICE = Decimal("0.745")


def run() -> None:
    print(json.dumps(run_cycle(), indent=2))


def run_cycle() -> dict:
    settings = get_settings()
    configure_logging(settings.log_level, log_dir=settings.log_dir_path)
    if settings.paper_trading or settings.dry_run or settings.emergency_stop:
        raise RuntimeError("BSB cycle requires PAPER_TRADING=false, DRY_RUN=false, and EMERGENCY_STOP=false.")

    client = BinanceFuturesClient(settings)
    try:
        _ensure_leverage(client)
        rules = client.symbol_rules(SYMBOL)
        _ensure_clean_start(client)
        qty = _entry_qty(client, rules)
        entry_price = _find_entry_price(client, rules)
        if entry_price is None:
            return {"status": "NO_ENTRY_FILL", "symbol": SYMBOL}

        entry_result = _open_pair(client, rules, qty, entry_price)
        if entry_result is None:
            return {"status": "NO_ENTRY_FILL", "symbol": SYMBOL}

        long_entry, short_entry = entry_result
        exit_result = _manage_pair(client, rules, qty, long_entry, short_entry)
        return {
            "status": "DONE" if exit_result["flat"] else "OPEN_PAIR_TIMEOUT",
            "entry_price": str(entry_price),
            "long_entry": _compact_order(long_entry),
            "short_entry": _compact_order(short_entry),
            "exit": exit_result,
            "final_positions": _compact_positions(client.position_risk(SYMBOL)),
        }
    finally:
        client.close()


def _ensure_clean_start(client: BinanceFuturesClient) -> None:
    if client.open_orders(SYMBOL):
        raise RuntimeError(f"Refusing bsb cycle because {SYMBOL} has open orders")
    rows = client.position_risk(SYMBOL)
    if _side_qty(rows, LONG_SIDE) != Decimal("0") or _side_qty(rows, SHORT_SIDE) != Decimal("0"):
        raise RuntimeError(f"Refusing bsb cycle because {SYMBOL} is not flat")


def _ensure_leverage(client: BinanceFuturesClient) -> None:
    current = _current_leverage(client)
    if current == TARGET_LEVERAGE:
        return
    response = client.change_leverage(SYMBOL, TARGET_LEVERAGE)
    if int(response.get("leverage", TARGET_LEVERAGE)) != TARGET_LEVERAGE:
        raise RuntimeError(f"Unable to set {SYMBOL} leverage to {TARGET_LEVERAGE}: {response}")


def _current_leverage(client: BinanceFuturesClient) -> int:
    rows = client.position_risk(SYMBOL)
    for item in rows:
        leverage = item.get("leverage")
        if leverage is not None:
            return int(leverage)
    return TARGET_LEVERAGE


def _entry_qty(client: BinanceFuturesClient, rules) -> Decimal:
    mark = Decimal(client.mark_price(SYMBOL)["markPrice"])
    qty = _floor_to_step(ENTRY_NOTIONAL_PER_SIDE / mark, rules.market_qty_step)
    if qty * mark < rules.min_notional:
        qty += rules.market_qty_step
    return max(qty, rules.market_min_qty)


def _find_entry_price(client: BinanceFuturesClient, rules) -> Decimal | None:
    mids: deque[Decimal] = deque(maxlen=ENTRY_SETUP_SAMPLES)
    for _ in range(ENTRY_SETUP_MAX_LOOPS):
        book = _book(client)
        bid = Decimal(book["bidPrice"])
        ask = Decimal(book["askPrice"])
        mid = (bid + ask) / Decimal("2")
        if mid > MAX_ENTRY_PRICE:
            time.sleep(EXIT_POLL_SECONDS)
            continue
        mids.append(mid)
        if len(mids) < ENTRY_SETUP_SAMPLES:
            time.sleep(EXIT_POLL_SECONDS)
            continue
        recent_high = max(mids)
        recent_low = min(mids)
        recent_range = recent_high - recent_low
        inside_spread = _snap_inside_spread(mid, bid, ask, rules.tick_size, MIN_ENTRY_SPREAD_TICKS)
        if recent_range >= MIN_SETUP_RANGE and inside_spread is not None:
            return inside_spread
        time.sleep(EXIT_POLL_SECONDS)
    return None


def _open_pair(client: BinanceFuturesClient, rules, qty: Decimal, entry_price: Decimal) -> tuple[dict, dict] | None:
    long_id = f"bsb_long_{uuid4().hex[:20]}"
    short_id = f"bsb_short_{uuid4().hex[:20]}"
    for _ in range(ENTRY_ATTEMPTS):
        book = _book(client)
        bid = Decimal(book["bidPrice"])
        ask = Decimal(book["askPrice"])
        current_entry = _snap_inside_spread((bid + ask) / Decimal("2"), bid, ask, rules.tick_size, MIN_ENTRY_SPREAD_TICKS)
        if current_entry is None or current_entry > MAX_ENTRY_PRICE or abs(((bid + ask) / Decimal("2")) - entry_price) > MAX_ENTRY_CHASE:
            time.sleep(1)
            continue
        try:
            long_order = client.create_order(
                symbol=SYMBOL,
                side="BUY",
                quantity=float(qty),
                order_type="LIMIT",
                price=float(current_entry),
                client_order_id=long_id,
                position_side=LONG_SIDE,
                time_in_force="GTX",
            )
        except (httpx.HTTPStatusError, RetryError):
            _cancel_quietly(client, long_id)
            _cancel_quietly(client, short_id)
            time.sleep(1)
            continue
        except Exception:
            _cancel_quietly(client, long_id)
            _cancel_quietly(client, short_id)
            time.sleep(1)
            continue
        try:
            short_order = client.create_order(
                symbol=SYMBOL,
                side="SELL",
                quantity=float(qty),
                order_type="LIMIT",
                price=float(current_entry),
                client_order_id=short_id,
                position_side=SHORT_SIDE,
                time_in_force="GTX",
            )
        except (httpx.HTTPStatusError, RetryError):
            _cancel_quietly(client, long_id)
            _cancel_quietly(client, short_id)
            time.sleep(1)
            continue
        except Exception:
            _cancel_quietly(client, long_id)
            _cancel_quietly(client, short_id)
            time.sleep(1)
            continue
        if long_order and short_order:
            break

    else:
        _cancel_quietly(client, long_id)
        _cancel_quietly(client, short_id)
        return None

    deadline = time.time() + MAX_ENTRY_WAIT_SECONDS
    while time.time() < deadline:
        long_pos = _side_qty(client.position_risk(SYMBOL), LONG_SIDE)
        short_pos = _side_qty(client.position_risk(SYMBOL), SHORT_SIDE)
        if long_pos > Decimal("0") and short_pos > Decimal("0"):
            return client.query_order(symbol=SYMBOL, client_order_id=long_id), client.query_order(symbol=SYMBOL, client_order_id=short_id)

        if (long_pos > Decimal("0")) != (short_pos > Decimal("0")):
            _cancel_quietly(client, long_id)
            _cancel_quietly(client, short_id)
            _flatten_uneven(client, long_pos, short_pos)
            return None

        long_status = client.query_order(symbol=SYMBOL, client_order_id=long_id)
        short_status = client.query_order(symbol=SYMBOL, client_order_id=short_id)
        if long_status.get("status") == "FILLED" and short_status.get("status") == "FILLED":
            return long_status, short_status
        time.sleep(EXIT_POLL_SECONDS)

    _cancel_quietly(client, long_id)
    _cancel_quietly(client, short_id)
    return None


def _manage_pair(client: BinanceFuturesClient, rules, qty: Decimal, long_entry: dict, short_entry: dict) -> dict:
    long_tp_id = f"bsb_long_tp_{uuid4().hex[:20]}"
    long_sl_id = f"bsb_long_sl_{uuid4().hex[:20]}"
    short_tp_id = f"bsb_short_tp_{uuid4().hex[:20]}"
    short_sl_id = f"bsb_short_sl_{uuid4().hex[:20]}"
    long_avg = Decimal(long_entry["avgPrice"])
    short_avg = Decimal(short_entry["avgPrice"])
    long_tp = _ceil_to_step(long_avg * (Decimal("1") + TAKE_PROFIT_PCT), rules.tick_size)
    long_sl = _floor_to_step(long_avg * (Decimal("1") - STOP_LOSS_PCT), rules.tick_size)
    short_tp = _floor_to_step(short_avg * (Decimal("1") - TAKE_PROFIT_PCT), rules.tick_size)
    short_sl = _ceil_to_step(short_avg * (Decimal("1") + STOP_LOSS_PCT), rules.tick_size)

    long_tp_order = _place_exit_limit(client, qty, "SELL", LONG_SIDE, long_tp_id, long_tp, rules.tick_size)
    short_tp_order = _place_exit_limit(client, qty, "BUY", SHORT_SIDE, short_tp_id, short_tp, rules.tick_size)
    long_sl_order = _place_stop_market(client, qty, "SELL", LONG_SIDE, long_sl_id, long_sl, rules.tick_size)
    short_sl_order = _place_stop_market(client, qty, "BUY", SHORT_SIDE, short_sl_id, short_sl, rules.tick_size)

    result = {
        "long_tp": _compact_order(long_tp_order),
        "long_sl": _compact_order(long_sl_order),
        "short_tp": _compact_order(short_tp_order),
        "short_sl": _compact_order(short_sl_order),
        "flat": False,
    }

    deadline = time.time() + MAX_MANAGE_SECONDS
    while True:
        long_pos = _side_qty(client.position_risk(SYMBOL), LONG_SIDE)
        short_pos = _side_qty(client.position_risk(SYMBOL), SHORT_SIDE)
        if long_pos <= Decimal("0"):
            _cancel_quietly(client, long_tp_id)
            _cancel_quietly(client, long_sl_id)
        if short_pos <= Decimal("0"):
            _cancel_quietly(client, short_tp_id)
            _cancel_quietly(client, short_sl_id)
        if long_pos <= Decimal("0") and short_pos <= Decimal("0"):
            result["flat"] = True
            result["final_positions"] = _compact_positions(client.position_risk(SYMBOL))
            return result
        if long_pos > Decimal("0") and Decimal(client.mark_price(SYMBOL)["markPrice"]) <= long_sl:
            _market_close(client, long_pos, "SELL", LONG_SIDE, "bsb_long_stop")
        if short_pos > Decimal("0") and Decimal(client.mark_price(SYMBOL)["markPrice"]) >= short_sl:
            _market_close(client, short_pos, "BUY", SHORT_SIDE, "bsb_short_stop")
        if time.time() >= deadline:
            _cancel_quietly(client, long_tp_id)
            _cancel_quietly(client, long_sl_id)
            _cancel_quietly(client, short_tp_id)
            _cancel_quietly(client, short_sl_id)
            if long_pos > Decimal("0"):
                _market_close(client, long_pos, "SELL", LONG_SIDE, "bsb_long_timeout")
            if short_pos > Decimal("0"):
                _market_close(client, short_pos, "BUY", SHORT_SIDE, "bsb_short_timeout")
            result["timeout"] = True
            result["final_positions"] = _compact_positions(client.position_risk(SYMBOL))
            return result
        time.sleep(EXIT_POLL_SECONDS)


def _place_exit_limit(client: BinanceFuturesClient, qty: Decimal, side: str, position_side: str, client_order_id: str, price: Decimal, tick_size: Decimal) -> dict:
    try:
        return client.create_order(
            symbol=SYMBOL,
            side=side,
            quantity=float(qty),
            order_type="LIMIT",
            price=float(_snap_to_tick(price, tick_size)),
            client_order_id=client_order_id,
            position_side=position_side,
            time_in_force="GTX",
        )
    except Exception:
        return {}


def _place_stop_market(client: BinanceFuturesClient, qty: Decimal, side: str, position_side: str, client_order_id: str, stop_price: Decimal, tick_size: Decimal) -> dict:
    try:
        return client.create_order(
            symbol=SYMBOL,
            side=side,
            quantity=float(qty),
            order_type="STOP_MARKET",
            client_order_id=client_order_id,
            position_side=position_side,
            extra_params={
                "stopPrice": _snap_to_tick(stop_price, tick_size),
                "workingType": "MARK_PRICE",
            },
        )
    except Exception:
        return {}


def _market_close(client: BinanceFuturesClient, qty: Decimal, side: str, position_side: str, prefix: str) -> dict:
    client_order_id = f"{prefix}_{uuid4().hex[:20]}"
    create = client.create_order(
        symbol=SYMBOL,
        side=side,
        quantity=float(qty),
        order_type="MARKET",
        client_order_id=client_order_id,
        position_side=position_side,
    )
    return client.query_order(symbol=SYMBOL, order_id=create.get("orderId"), client_order_id=client_order_id)


def _flatten_uneven(client: BinanceFuturesClient, long_pos: Decimal, short_pos: Decimal) -> None:
    if long_pos > Decimal("0"):
        _market_close(client, long_pos, "SELL", LONG_SIDE, "bsb_flat_long")
    if short_pos > Decimal("0"):
        _market_close(client, short_pos, "BUY", SHORT_SIDE, "bsb_flat_short")


def _book(client: BinanceFuturesClient) -> dict:
    response = client.client.get("/fapi/v1/ticker/bookTicker", params={"symbol": SYMBOL})
    response.raise_for_status()
    return response.json()


def _side_qty(rows: list[dict], side: str) -> Decimal:
    for item in rows:
        if item.get("positionSide") == side:
            return Decimal(item.get("positionAmt", "0"))
    return Decimal("0")


def _cancel_quietly(client: BinanceFuturesClient, client_order_id: str) -> None:
    try:
        client.cancel_order(symbol=SYMBOL, client_order_id=client_order_id)
    except Exception:
        pass


def _compact_order(order: dict | None) -> dict | None:
    if not order:
        return None
    return {
        "orderId": order.get("orderId"),
        "status": order.get("status"),
        "side": order.get("side"),
        "positionSide": order.get("positionSide"),
        "price": order.get("price"),
        "stopPrice": order.get("stopPrice"),
        "executedQty": order.get("executedQty"),
        "avgPrice": order.get("avgPrice"),
        "clientOrderId": order.get("clientOrderId"),
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


def _snap_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).quantize(Decimal("1")) * tick


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).quantize(Decimal("1"), rounding=ROUND_FLOOR) * step


def _ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).quantize(Decimal("1"), rounding=ROUND_CEILING) * step


def _snap_inside_spread(mid: Decimal, bid: Decimal, ask: Decimal, tick: Decimal, min_spread_ticks: Decimal) -> Decimal | None:
    if (ask - bid) < tick * min_spread_ticks:
        return None
    price = _snap_to_tick(mid, tick)
    if price <= bid:
        price = bid + tick
    if price >= ask:
        price = ask - tick
    if price <= bid or price >= ask:
        return None
    return price
