from __future__ import annotations

import json
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from uuid import uuid4

import fcntl
import httpx
from tenacity import RetryError

from trading_bot.binance_client import BinanceFuturesClient
from trading_bot.config import get_settings
from trading_bot.logging_utils import configure_logging
from trading_bot.models import SymbolRules

LOGGER = logging.getLogger(__name__)

LONG_SIDE = "LONG"
SHORT_SIDE = "SHORT"
DEFAULT_ATTEMPTS = 120
DEFAULT_WAIT_SECONDS = 3
DEFAULT_MAKER_SECONDS = 30


@dataclass(slots=True)
class CloseTarget:
    symbol: str
    position_side: str
    qty: Decimal


def run() -> None:
    payload = close_all_positions_maker()
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def close_all_positions_maker(
    *,
    attempts: int = DEFAULT_ATTEMPTS,
    wait_seconds: int = DEFAULT_WAIT_SECONDS,
    maker_seconds: int = DEFAULT_MAKER_SECONDS,
) -> dict:
    settings = get_settings()
    configure_logging(settings.log_level, log_dir=settings.log_dir_path)

    if settings.paper_trading or settings.dry_run or settings.emergency_stop:
        message = (
            "清仓跳过：当前仍在安全模式。需要 PAPER_TRADING=false、DRY_RUN=false、"
            "EMERGENCY_STOP=false 才会真实挂 maker 平仓单。"
        )
        LOGGER.warning(message)
        return {"status": "SKIPPED_SAFE_MODE", "message": message}

    with _close_lock(settings.state_dir_path) as acquired:
        if not acquired:
            message = "清仓已经在执行中：本次停止请求不会再启动第二路清仓，避免重复挂单。"
            LOGGER.warning(message)
            return {"status": "CLOSE_ALREADY_RUNNING", "message": message}
        return _close_all_positions_locked(settings, attempts=attempts, wait_seconds=wait_seconds, maker_seconds=maker_seconds)


def _close_all_positions_locked(
    settings,
    *,
    attempts: int,
    wait_seconds: int,
    maker_seconds: int,
) -> dict:
    client = BinanceFuturesClient(settings)
    try:
        LOGGER.info("开始停止后的清仓检查：先撤掉未成交挂单，再逐个仓位 maker 追单平仓。")
        cancelled = _cancel_all_open_orders(client)
        targets = _open_position_targets(client)
        if not targets:
            LOGGER.info("清仓检查完成：当前没有需要处理的持仓。")
            return {"status": "NO_POSITION", "cancelled_orders": cancelled, "closed": []}

        closed: list[dict] = []
        for target in targets:
            LOGGER.info(
                "发现待平仓仓位：%s %s 数量=%s，将使用 post-only 限价单追单平仓。",
                target.symbol,
                target.position_side,
                target.qty,
            )
            closed.append(_close_target(client, target, attempts=attempts, wait_seconds=wait_seconds, maker_seconds=maker_seconds))

        cancelled.extend(_cancel_all_open_orders(client))
        still_open = [item for item in _open_position_targets(client) if item.qty != Decimal("0")]
        status = "FLAT" if not still_open else "OPEN_POSITION_REMAINS"
        if still_open:
            LOGGER.error("清仓未完全完成：仍有 %s 个仓位未平。", len(still_open))
        else:
            LOGGER.info("清仓完成：所有可见持仓已经归零。")
        return {
            "status": status,
            "cancelled_orders": cancelled,
            "closed": closed,
            "remaining_positions": [_target_payload(item) for item in still_open],
        }
    finally:
        client.close()


@contextmanager
def _close_lock(state_dir: Path):
    state_dir.mkdir(parents=True, exist_ok=True)
    lock_path = state_dir / "position_closer.lock"
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(f"pid={os.getpid()} started_at={int(time.time())}\n")
        lock_file.flush()
        try:
            yield True
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _cancel_all_open_orders(client: BinanceFuturesClient) -> list[dict]:
    cancelled: list[dict] = []
    orders = client.open_orders()
    if not orders:
        LOGGER.info("没有未成交挂单需要撤销。")
        return cancelled

    LOGGER.info("发现 %s 个未成交挂单，开始逐个撤销。", len(orders))
    for order in orders:
        symbol = order.get("symbol")
        client_order_id = order.get("clientOrderId")
        order_id = order.get("orderId")
        try:
            result = client.cancel_order(symbol=symbol, order_id=order_id, client_order_id=client_order_id)
            cancelled.append({"symbol": symbol, "orderId": order_id, "status": result.get("status")})
            LOGGER.info("已撤销挂单：%s orderId=%s。", symbol, order_id)
        except Exception as exc:  # noqa: BLE001 - best effort cancel before closing positions.
            cancelled.append({"symbol": symbol, "orderId": order_id, "error": str(exc)})
            LOGGER.warning("撤单失败：%s orderId=%s，原因=%s。", symbol, order_id, exc)
    return cancelled


def _open_position_targets(client: BinanceFuturesClient) -> list[CloseTarget]:
    targets: list[CloseTarget] = []
    for row in client.position_risk():
        symbol = str(row.get("symbol", ""))
        position_side = str(row.get("positionSide", "BOTH"))
        qty = Decimal(str(row.get("positionAmt", "0")))
        if qty == Decimal("0") or not symbol:
            continue
        if position_side == "BOTH":
            position_side = LONG_SIDE if qty > 0 else SHORT_SIDE
        targets.append(CloseTarget(symbol=symbol, position_side=position_side, qty=abs(qty)))
    return targets


def _close_target(
    client: BinanceFuturesClient,
    target: CloseTarget,
    *,
    attempts: int,
    wait_seconds: int,
    maker_seconds: int,
) -> dict:
    rules = client.symbol_rules(target.symbol)
    side = "SELL" if target.position_side == LONG_SIDE else "BUY"
    last_order: dict | None = None
    deadline = time.time() + maker_seconds
    LOGGER.info("%s %s 开始 maker 平仓追单，最多 %s 秒，超时后市价兜底。", target.symbol, target.position_side, maker_seconds)

    for attempt in range(1, attempts + 1):
        if time.time() >= deadline:
            break
        _cancel_symbol_open_orders(client, target.symbol)
        remaining = _current_side_qty(client, target.symbol, target.position_side)
        if remaining <= Decimal("0"):
            LOGGER.info("%s %s 已平仓。", target.symbol, target.position_side)
            return {"symbol": target.symbol, "position_side": target.position_side, "status": "FILLED"}

        qty = _floor_to_step(remaining, rules.qty_step)
        if qty <= Decimal("0"):
            return {
                "symbol": target.symbol,
                "position_side": target.position_side,
                "status": "QTY_TOO_SMALL",
                "remaining_qty": str(remaining),
            }

        price = _maker_close_price(client, target.symbol, side, rules)
        client_order_id = f"flat_{target.symbol[:3].lower()}_{uuid4().hex[:10]}"
        LOGGER.info(
            "第 %s 次平仓追单：%s %s 数量=%s 价格=%s。",
            attempt,
            target.symbol,
            side,
            qty,
            price,
        )
        try:
            created = client.create_order(
                symbol=target.symbol,
                side=side,
                quantity=float(qty),
                order_type="LIMIT",
                price=float(price),
                client_order_id=client_order_id,
                position_side=target.position_side,
                time_in_force="GTX",
            )
            time.sleep(wait_seconds)
            last_order = client.query_order(
                symbol=target.symbol,
                order_id=created.get("orderId"),
                client_order_id=client_order_id,
            )
            if last_order.get("status") == "FILLED":
                LOGGER.info(
                    "%s %s 平仓成交：数量=%s 均价=%s。",
                    target.symbol,
                    target.position_side,
                    last_order.get("executedQty"),
                    last_order.get("avgPrice"),
                )
                continue
            _cancel_quietly(client, target.symbol, client_order_id)
        except (httpx.HTTPStatusError, RetryError) as exc:
            LOGGER.warning("%s 平仓挂单被拒或查询失败：%s。稍后继续追单。", target.symbol, exc)
            time.sleep(wait_seconds)
        except Exception as exc:  # noqa: BLE001 - keep chasing unless attempts are exhausted.
            LOGGER.warning("%s 平仓遇到异常：%s。稍后继续追单。", target.symbol, exc)
            time.sleep(wait_seconds)

    remaining = _current_side_qty(client, target.symbol, target.position_side)
    if remaining <= Decimal("0"):
        return {"symbol": target.symbol, "position_side": target.position_side, "status": "FILLED"}

    LOGGER.warning(
        "%s %s maker 平仓超过 %s 秒仍未完全成交，执行市价兜底平仓，剩余数量=%s。",
        target.symbol,
        target.position_side,
        maker_seconds,
        remaining,
    )
    return _market_close_target(client, target, remaining, side, rules, last_order)


def _market_close_target(
    client: BinanceFuturesClient,
    target: CloseTarget,
    remaining: Decimal,
    side: str,
    rules: SymbolRules,
    last_order: dict | None,
) -> dict:
    _cancel_symbol_open_orders(client, target.symbol)
    qty = _floor_to_step(remaining, rules.market_qty_step)
    if qty <= Decimal("0"):
        qty = _floor_to_step(remaining, rules.qty_step)
    if qty <= Decimal("0"):
        return {
            "symbol": target.symbol,
            "position_side": target.position_side,
            "status": "QTY_TOO_SMALL",
            "remaining_qty": str(remaining),
            "last_order": _compact_order(last_order),
        }

    client_order_id = f"flatm_{target.symbol[:3].lower()}_{uuid4().hex[:10]}"
    try:
        created = client.create_order(
            symbol=target.symbol,
            side=side,
            quantity=float(qty),
            order_type="MARKET",
            client_order_id=client_order_id,
            position_side=target.position_side,
        )
        order = client.query_order(
            symbol=target.symbol,
            order_id=created.get("orderId"),
            client_order_id=client_order_id,
        )
        LOGGER.info(
            "%s %s 市价兜底平仓完成：数量=%s，均价=%s。",
            target.symbol,
            target.position_side,
            order.get("executedQty"),
            order.get("avgPrice"),
        )
        return {"symbol": target.symbol, "position_side": target.position_side, "status": "FILLED", "order": _compact_order(order)}
    except Exception as exc:  # noqa: BLE001 - final safety report for the dashboard.
        remaining_after = _current_side_qty(client, target.symbol, target.position_side)
        if remaining_after <= Decimal("0"):
            LOGGER.info("%s %s 市价兜底查询前已归零。", target.symbol, target.position_side)
            return {"symbol": target.symbol, "position_side": target.position_side, "status": "FILLED"}
        LOGGER.error("%s %s 市价兜底平仓失败：%s。", target.symbol, target.position_side, exc)
        return {
            "symbol": target.symbol,
            "position_side": target.position_side,
            "status": "MARKET_CLOSE_FAILED",
            "remaining_qty": str(remaining_after),
            "error": str(exc),
            "last_order": _compact_order(last_order),
        }


def _maker_close_price(client: BinanceFuturesClient, symbol: str, side: str, rules: SymbolRules) -> Decimal:
    book = _book(client, symbol)
    bid = Decimal(book["bidPrice"])
    ask = Decimal(book["askPrice"])
    if side == "SELL":
        if ask - bid > rules.tick_size:
            return _ceil_to_step(bid + rules.tick_size, rules.tick_size)
        return _ceil_to_step(ask, rules.tick_size)
    if ask - bid > rules.tick_size:
        return _floor_to_step(ask - rules.tick_size, rules.tick_size)
    return _floor_to_step(bid, rules.tick_size)


def _current_side_qty(client: BinanceFuturesClient, symbol: str, position_side: str) -> Decimal:
    rows = client.position_risk(symbol)
    if position_side == LONG_SIDE:
        return abs(_side_qty(rows, LONG_SIDE))
    if position_side == SHORT_SIDE:
        return abs(_side_qty(rows, SHORT_SIDE))
    return sum((abs(Decimal(str(row.get("positionAmt", "0")))) for row in rows), Decimal("0"))


def _side_qty(rows: list[dict], side: str) -> Decimal:
    for row in rows:
        if row.get("positionSide") == side:
            return Decimal(str(row.get("positionAmt", "0")))
    return Decimal("0")


def _book(client: BinanceFuturesClient, symbol: str) -> dict:
    response = client.client.get("/fapi/v1/ticker/bookTicker", params={"symbol": symbol})
    response.raise_for_status()
    return response.json()


def _cancel_quietly(client: BinanceFuturesClient, symbol: str, client_order_id: str) -> None:
    try:
        client.cancel_order(symbol=symbol, client_order_id=client_order_id)
    except Exception:
        pass


def _cancel_symbol_open_orders(client: BinanceFuturesClient, symbol: str) -> list[dict]:
    cancelled: list[dict] = []
    try:
        orders = client.open_orders(symbol)
    except Exception as exc:  # noqa: BLE001 - closing can still continue after a read failure.
        LOGGER.warning("%s 查询未成交挂单失败：%s。", symbol, exc)
        return cancelled
    for order in orders:
        order_id = order.get("orderId")
        client_order_id = order.get("clientOrderId")
        try:
            result = client.cancel_order(symbol=symbol, order_id=order_id, client_order_id=client_order_id)
            cancelled.append({"symbol": symbol, "orderId": order_id, "status": result.get("status")})
            LOGGER.info("%s 已撤销残留挂单：orderId=%s。", symbol, order_id)
        except Exception as exc:  # noqa: BLE001 - another close path may have filled/cancelled it.
            LOGGER.info("%s 残留挂单撤销未完成：orderId=%s，原因=%s。", symbol, order_id, exc)
    return cancelled


def _target_payload(target: CloseTarget) -> dict:
    return {"symbol": target.symbol, "position_side": target.position_side, "qty": str(target.qty)}


def _compact_order(order: dict | None) -> dict | None:
    if not order:
        return None
    return {
        "orderId": order.get("orderId"),
        "status": order.get("status"),
        "side": order.get("side"),
        "price": order.get("price"),
        "executedQty": order.get("executedQty"),
        "avgPrice": order.get("avgPrice"),
    }


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).quantize(Decimal("1"), rounding=ROUND_FLOOR) * step


def _ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).quantize(Decimal("1"), rounding=ROUND_CEILING) * step
