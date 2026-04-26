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

SYMBOL = "HYPERUSDT"
POSITION_SIDE = "LONG"
NOTIONAL_USDT = Decimal("25")
TARGET_PRICE_DELTA = Decimal("0.00032")
STOP_LOSS_DELTA = Decimal("0.00040")
MAX_ENTRY_CHASE_DELTA = Decimal("0.00020")
ENTRY_PULLBACK_DELTA = Decimal("0.00035")
ENTRY_REBOUND_DELTA = Decimal("0.00008")
ENTRY_TREND_BIAS = Decimal("0.00010")
MIN_SETUP_RANGE = Decimal("0.00045")
ENTRY_SETUP_SAMPLES = 24
ENTRY_SETUP_MAX_LOOPS = 60
ENTRY_ATTEMPTS = 14
EXIT_ATTEMPTS = 75
SLEEP_SECONDS = 10


def run() -> None:
    print(json.dumps(run_cycle(), indent=2))


def run_cycle() -> dict:
    settings = get_settings()
    configure_logging(settings.log_level, log_dir=settings.log_dir_path)
    if settings.paper_trading or settings.dry_run or settings.emergency_stop:
        raise RuntimeError("Hyper cycle requires PAPER_TRADING=false, DRY_RUN=false, and EMERGENCY_STOP=false.")

    client = BinanceFuturesClient(settings)
    try:
        rules = client.symbol_rules(SYMBOL)
        _ensure_clean_start(client)
        qty = _entry_qty(client, rules)
        entry = _chase_entry(client, rules, qty)
        if entry is None:
            return {"status": "NO_ENTRY_FILL", "symbol": SYMBOL}

        filled_qty = Decimal(entry["executedQty"])
        avg_price = Decimal(entry["avgPrice"])
        exit_order = _chase_exit(client, rules, filled_qty, avg_price)
        return {
            "status": "DONE" if exit_order else "ENTRY_FILLED_EXIT_TIMEOUT",
            "entry": _compact_order(entry),
            "exit": _compact_order(exit_order) if exit_order else None,
            "final_positions": _compact_positions(client.position_risk(SYMBOL)),
        }
    finally:
        client.close()


def _ensure_clean_start(client: BinanceFuturesClient) -> None:
    open_orders = client.open_orders(SYMBOL)
    if open_orders:
        raise RuntimeError(f"Refusing hyper cycle because {SYMBOL} has open orders: {len(open_orders)}")
    long_qty = _long_qty(client)
    if long_qty != Decimal("0"):
        raise RuntimeError(f"Refusing hyper cycle because {SYMBOL} LONG is not flat: {long_qty}")


def _entry_qty(client: BinanceFuturesClient, rules) -> Decimal:
    mark = Decimal(client.mark_price(SYMBOL)["markPrice"])
    qty = _floor_to_step(NOTIONAL_USDT / mark, rules.market_qty_step)
    if qty * mark < rules.min_notional:
        qty += rules.market_qty_step
    return max(qty, rules.market_min_qty)


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
        if bid > anchor_bid + MAX_ENTRY_CHASE_DELTA:
            if client_order_id:
                _cancel_quietly(client, client_order_id)
            return None
        price = _floor_to_step(bid, rules.tick_size)
        client_order_id = f"hyper_entry_{uuid4().hex[:20]}"
        try:
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
        except (httpx.HTTPStatusError, RetryError):
            time.sleep(1)
            continue
        except Exception:
            time.sleep(1)
            continue
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
        if not _higher_timeframe_allows_long(client):
            time.sleep(SLEEP_SECONDS)
            continue
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
        recent_range = recent_high - recent_low
        trend_up = mids[-1] >= mids[0] + ENTRY_TREND_BIAS
        pulled_back = mid <= recent_high - ENTRY_PULLBACK_DELTA
        not_breaking_down = mid >= recent_low + ENTRY_REBOUND_DELTA
        if recent_range >= MIN_SETUP_RANGE and trend_up and pulled_back and not_breaking_down:
            return bid
        time.sleep(SLEEP_SECONDS)
    return None


def _higher_timeframe_allows_long(client: BinanceFuturesClient) -> bool:
    response = client.client.get("/fapi/v1/klines", params={"symbol": SYMBOL, "interval": "1m", "limit": 8})
    response.raise_for_status()
    candles = response.json()
    if len(candles) < 8:
        return False

    closes = [Decimal(item[4]) for item in candles]
    lows = [Decimal(item[3]) for item in candles]
    highs = [Decimal(item[2]) for item in candles]
    recent_range = max(highs[-5:]) - min(lows[-5:])
    if recent_range <= Decimal("0"):
        return False

    not_sliding = closes[-1] >= closes[-4] - Decimal("0.00015")
    away_from_recent_low = closes[-1] >= min(lows[-5:]) + recent_range * Decimal("0.35")
    last_two_stable = closes[-1] >= closes[-2] - Decimal("0.00008")
    return not_sliding and away_from_recent_low and last_two_stable


def _chase_exit(client: BinanceFuturesClient, rules, qty: Decimal, entry_price: Decimal) -> dict | None:
    target_price = entry_price + TARGET_PRICE_DELTA
    stop_price = entry_price - STOP_LOSS_DELTA
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
            return _market_close(client, qty, prefix="hyper_stop")
        passive_ask = _ceil_to_step(best_ask, rules.tick_size)
        price = max(_ceil_to_step(target_price, rules.tick_size), passive_ask)
        client_order_id = f"hyper_exit_{uuid4().hex[:20]}"
        try:
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
        except (httpx.HTTPStatusError, RetryError):
            time.sleep(1)
            continue
        except Exception:
            time.sleep(1)
            continue
        time.sleep(SLEEP_SECONDS)
        checked = client.query_order(symbol=SYMBOL, order_id=order.get("orderId"), client_order_id=client_order_id)
        if checked.get("status") == "FILLED":
            return checked
        if _long_qty(client) <= Decimal("0"):
            return checked
        if best_ask > target_price + TARGET_PRICE_DELTA:
            target_price = _ceil_to_step(best_ask, rules.tick_size)
    if client_order_id:
        _cancel_quietly(client, client_order_id)
    if _long_qty(client) > Decimal("0"):
        return _market_close(client, qty, prefix="hyper_timeout")
    if client_order_id:
        return client.query_order(symbol=SYMBOL, client_order_id=client_order_id)
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


def _market_close(client: BinanceFuturesClient, qty: Decimal, *, prefix: str) -> dict:
    client_order_id = f"{prefix}_{uuid4().hex[:20]}"
    create = client.create_order(
        symbol=SYMBOL,
        side="SELL",
        quantity=float(qty),
        order_type="MARKET",
        client_order_id=client_order_id,
        position_side=POSITION_SIDE,
    )
    return client.query_order(symbol=SYMBOL, order_id=create.get("orderId"), client_order_id=client_order_id)


def _compact_order(order: dict | None) -> dict | None:
    if order is None:
        return None
    return {
        "orderId": order.get("orderId"),
        "status": order.get("status"),
        "side": order.get("side"),
        "price": order.get("price"),
        "clientOrderId": order.get("clientOrderId"),
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
