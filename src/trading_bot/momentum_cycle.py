from __future__ import annotations

import os
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from uuid import uuid4

import httpx
from tenacity import RetryError

from trading_bot.binance_client import BinanceFuturesClient
from trading_bot.config import get_settings
from trading_bot.logging_utils import configure_logging
from trading_bot.momentum_profiles import (
    MAIN_USDC_PROFILE,
    ALT_USDT_PROFILE,
    compact_universe,
    discover_profile_universe,
)
from trading_bot.position_closer import close_all_positions_maker

DEFAULT_PROFILE = MAIN_USDC_PROFILE
POSITION_SIDE_LONG = "LONG"
POSITION_SIDE_SHORT = "SHORT"
ALT_CANDIDATE_LIMIT = 10
ENTRY_ATTEMPTS = 10
EXIT_ATTEMPTS = 200
ENTRY_SETUP_SAMPLES = 18
MAX_MANAGE_SECONDS = 1800
SLEEP_SECONDS = 10
ENTRY_MAKER_TIMEOUT_SECONDS = 45
EXIT_MAKER_FALLBACK_SECONDS = 60
ORDER_CHECK_SECONDS = 6
TARGET_LEVERAGE = 5
ALT_MAX_SPREAD_PCT = Decimal("0.0012")
ALT_MAX_CONCURRENT_TRADES = 4
BREAKOUT_LOOKBACK = 40
VOLUME_LOOKBACK = 30
MIN_VOLUME_RATIO = Decimal("1.0")  # 降低到 1.0，几乎不做成交量过滤
MIN_ATR_PCT = Decimal("0.0010")  # 降低到 0.0010，更容易满足
MAX_ATR_PCT = Decimal("0.0500")  # 提高到 0.0500，允许更大的波动率
LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class Candidate:
    symbol: str
    side: str
    score: Decimal
    anchor_price: Decimal
    qty: Decimal
    tick_size: Decimal


@dataclass(slots=True)
class MomentumTuning:
    quote_notional: Decimal
    take_profit_pct: Decimal
    stop_loss_pct: Decimal
    min_score: Decimal
    min_pullback_pct: Decimal
    max_entry_chase_pct: Decimal


MAIN_TUNING = MomentumTuning(
    quote_notional=Decimal("35"),
    take_profit_pct=Decimal("0.0012"),
    stop_loss_pct=Decimal("0.0022"),
    min_score=Decimal("0.00085"),
    min_pullback_pct=Decimal("0.00055"),
    max_entry_chase_pct=Decimal("0.00075"),
)

ALT_TUNING = MomentumTuning(
    quote_notional=Decimal("25"),
    take_profit_pct=Decimal("0.0090"),
    stop_loss_pct=Decimal("0.0045"),
    min_score=Decimal("0.0030"),  # 降低到 0.003，更容易触发
    min_pullback_pct=Decimal("0.00030"),  # 降低到 0.0003，更容易满足
    max_entry_chase_pct=Decimal("0.00500"),  # 提高到 0.005，允许更大的追高空间
)


def run() -> None:
    profile = os.getenv("MOMENTUM_PROFILE", DEFAULT_PROFILE)
    print(json.dumps(run_cycle(profile=profile), indent=2))


def run_cycle(profile: str = DEFAULT_PROFILE, universe: tuple[str, ...] | None = None) -> dict:
    settings = get_settings()
    configure_logging(settings.log_level, log_dir=settings.log_dir_path)
    if settings.paper_trading or settings.dry_run or settings.emergency_stop:
        raise RuntimeError("Momentum cycle requires PAPER_TRADING=false, DRY_RUN=false, and EMERGENCY_STOP=false.")

    client = BinanceFuturesClient(settings)
    try:
        universe = universe or discover_profile_universe(client, profile)
        tuning = _tuning(profile)
        LOGGER.info(
            "本轮使用交易池：profile=%s，数量=%s，币种=%s。",
            profile,
            len(universe),
            ", ".join(universe),
        )
        _ensure_clean_start(client, universe)
        LOGGER.info("开始逐个扫描行情并计算动量分数。")
        candidates = _select_candidates(client, universe, tuning, profile=profile)
        if not candidates:
            LOGGER.info("本轮扫描完成：没有找到符合条件的候选币。")
            return {"status": "NO_CANDIDATE", "profile": profile, "universe": compact_universe(universe)}

        LOGGER.info("并行启动 %s 个候选币交易任务：%s。", len(candidates), _candidate_names(candidates))
        results = _trade_candidates_parallel(profile, candidates, tuning)
        entered = [item for item in results if item.get("entry")]
        completed = [item for item in results if item.get("status") == "DONE"]
        status = "DONE" if completed else "NO_ENTRY_FILL"
        if entered and not completed:
            status = "OPEN_POSITION_TIMEOUT"
        return {
            "status": status,
            "profile": profile,
            "attempted_candidates": [_compact_candidate(candidate) for candidate in candidates],
            "results": results,
            "final_positions": _compact_positions(client, universe),
            "universe": compact_universe(universe),
        }
    finally:
        client.close()


def _trade_candidates_parallel(
    profile: str,
    candidates: list[Candidate],
    tuning: MomentumTuning,
) -> list[dict]:
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=len(candidates)) as executor:
        future_map = {
            executor.submit(_trade_candidate, profile, candidate, tuning): candidate
            for candidate in candidates
        }
        for future in as_completed(future_map):
            candidate = future_map[future]
            try:
                results.append(future.result())
            except Exception as exc:  # noqa: BLE001 - keep other symbols running.
                LOGGER.exception("%s 并行交易任务异常。", candidate.symbol)
                results.append(
                    {
                        "status": "ERROR",
                        "candidate": _compact_candidate(candidate),
                        "error": _exception_reason(exc),
                    }
                )
    results.sort(key=lambda item: str((item.get("candidate") or {}).get("symbol", "")))
    return results


def _trade_candidate(profile: str, candidate: Candidate, tuning: MomentumTuning) -> dict:
    settings = get_settings()
    client = BinanceFuturesClient(settings)
    try:
        _ensure_leverage(client, candidate.symbol)
        LOGGER.info(
            "准备尝试 maker 建仓：%s %s，分数=%s，参考价=%s，数量=%s。",
            candidate.symbol,
            candidate.side,
            _fmt_pct(candidate.score),
            candidate.anchor_price,
            candidate.qty,
        )
        entry = _chase_entry(client, candidate, tuning)
        if entry is None:
            LOGGER.info("%s maker 建仓未成交，本币种本轮结束。", candidate.symbol)
            return {"status": "NO_ENTRY_FILL", "profile": profile, "candidate": _compact_candidate(candidate)}

        filled_qty = Decimal(entry["executedQty"])
        avg_price = Decimal(entry["avgPrice"])
        LOGGER.info("%s 建仓完成：方向=%s，数量=%s，均价=%s。", candidate.symbol, candidate.side, filled_qty, avg_price)
        exit_order = _manage_position(client, candidate, filled_qty, avg_price, tuning)
        return {
            "status": "DONE" if exit_order and exit_order.get("status") == "FILLED" else "OPEN_POSITION_TIMEOUT",
            "profile": profile,
            "candidate": _compact_candidate(candidate),
            "entry": _compact_order(entry),
            "exit": _compact_order(exit_order) if exit_order else None,
        }
    finally:
        client.close()


def _ensure_clean_start(client: BinanceFuturesClient, universe: tuple[str, ...]) -> None:
    LOGGER.info("启动前检查：确认交易池内没有未成交挂单和残留仓位。")
    universe_set = set(universe)
    open_orders = [order for order in client.open_orders() if str(order.get("symbol")) in universe_set]
    dirty_positions = [
        row
        for row in client.position_risk()
        if str(row.get("symbol")) in universe_set and Decimal(str(row.get("positionAmt", "0"))) != Decimal("0")
    ]
    if open_orders or dirty_positions:
        LOGGER.warning(
            "启动前发现残留：未成交挂单=%s，未平仓位=%s，先执行全局清理。",
            len(open_orders),
            len(dirty_positions),
        )
        close_result = close_all_positions_maker()
        if close_result.get("status") not in {"FLAT", "NO_POSITION"}:
            raise RuntimeError(f"启动前清理失败：{close_result}")

    open_orders_after = [order for order in client.open_orders() if str(order.get("symbol")) in universe_set]
    dirty_positions_after = [
        row
        for row in client.position_risk()
        if str(row.get("symbol")) in universe_set and Decimal(str(row.get("positionAmt", "0"))) != Decimal("0")
    ]
    if open_orders_after or dirty_positions_after:
        raise RuntimeError(
            f"启动前仍有残留：open_orders={len(open_orders_after)}, positions={len(dirty_positions_after)}"
        )
    LOGGER.info("启动前检查通过：交易池当前干净。")


def _ensure_leverage(client: BinanceFuturesClient, symbol: str) -> None:
    rows = client.position_risk(symbol)
    current = next((int(item["leverage"]) for item in rows if item.get("leverage") is not None), TARGET_LEVERAGE)
    if current == TARGET_LEVERAGE:
        LOGGER.info("%s 杠杆已是 %s 倍。", symbol, TARGET_LEVERAGE)
        return
    response = client.change_leverage(symbol, TARGET_LEVERAGE)
    actual = int(response.get("leverage", TARGET_LEVERAGE))
    if actual != TARGET_LEVERAGE:
        raise RuntimeError(f"Unable to set {symbol} leverage to {TARGET_LEVERAGE}: {response}")
    LOGGER.info("%s 杠杆已调整为 %s 倍。", symbol, TARGET_LEVERAGE)


def _select_candidates(
    client: BinanceFuturesClient,
    universe: tuple[str, ...],
    tuning: MomentumTuning,
    *,
    profile: str,
) -> list[Candidate]:
    snapshots: list[Candidate] = []
    for index, symbol in enumerate(universe, start=1):
        LOGGER.info("正在观察 %s/%s：%s。", index, len(universe), symbol)
        rules = client.symbol_rules(symbol)
        snapshot = _snapshot_symbol(client, symbol, rules, tuning, force_direction=False)
        if snapshot is not None:
            LOGGER.info(
                "%s 进入候选：方向=%s，动量分数=%s，参考价=%s，计划数量=%s。",
                snapshot.symbol,
                snapshot.side,
                _fmt_pct(snapshot.score),
                snapshot.anchor_price,
                snapshot.qty,
            )
            snapshots.append(snapshot)
            continue
        LOGGER.info("%s 暂无入场信号。", symbol)
    snapshots.sort(key=lambda item: abs(item.score), reverse=True)
    if not snapshots:
        return []
    if profile == ALT_USDT_PROFILE:
        settings = get_settings()
        candidate_limit = max(1, min(int(settings.momentum_universe_top_n), int(settings.momentum_max_concurrent_trades)))
        selected = snapshots[:candidate_limit]
        LOGGER.info(
            "候选排序完成：交易池前 %s 个，风险过滤后最多并发 %s 个，实际候选=%s。",
            settings.momentum_universe_top_n,
            candidate_limit,
            _candidate_names(selected),
        )
        return selected
    if abs(snapshots[0].score) < tuning.min_score:
        return []
    LOGGER.info("候选排序完成：本轮尝试 %s。", snapshots[0].symbol)
    return snapshots[:1]


def _snapshot_symbol(
    client: BinanceFuturesClient,
    symbol: str,
    rules,
    tuning: MomentumTuning,
    require_ema: bool = True,
    force_direction: bool = False,
) -> Candidate | None:
    candles = _klines(client, symbol, interval="1m", limit=90)
    if len(candles) < 70:
        return None
    # Drop the still-forming candle; signals should come from completed bars.
    candles = candles[:-1]
    closes_1m = [row["close"] for row in candles]
    highs_1m = [row["high"] for row in candles]
    lows_1m = [row["low"] for row in candles]
    volumes_1m = [row["volume"] for row in candles]

    book = _book(client, symbol)
    bid = Decimal(book["bidPrice"])
    ask = Decimal(book["askPrice"])
    mid = (bid + ask) / Decimal("2")
    spread = ask - bid
    if mid <= 0 or spread <= 0:
        return None
    if spread / mid > ALT_MAX_SPREAD_PCT:
        LOGGER.info("%s 点差过大：%s，跳过。", symbol, _fmt_pct(spread / mid))
        return None

    last = closes_1m[-1]
    ret_5m = last / closes_1m[-6] - Decimal("1")
    ret_15m = last / closes_1m[-16] - Decimal("1")
    ema_fast = _ema(closes_1m, 12)
    ema_slow = _ema(closes_1m, 36)
    ema_bias = (ema_fast - ema_slow) / last
    previous_high = max(highs_1m[-BREAKOUT_LOOKBACK - 1 : -1])
    previous_low = min(lows_1m[-BREAKOUT_LOOKBACK - 1 : -1])
    average_volume = sum(volumes_1m[-VOLUME_LOOKBACK - 1 : -1], Decimal("0")) / Decimal(VOLUME_LOOKBACK)
    volume_ratio = volumes_1m[-1] / average_volume if average_volume > 0 else Decimal("0")
    atr_pct = _atr_pct(candles[-21:], last)
    if atr_pct < MIN_ATR_PCT or atr_pct > MAX_ATR_PCT:
        LOGGER.info("%s 波动率不合适：ATR=%s，跳过。", symbol, _fmt_pct(atr_pct))
        return None
    if volume_ratio < MIN_VOLUME_RATIO:
        LOGGER.info("%s 放量不足：当前量/均量=%s，跳过。", symbol, f"{volume_ratio:.2f}x")
        return None
    score = ret_15m * Decimal("0.55") + ret_5m * Decimal("0.25") + ema_bias * Decimal("0.10") + (volume_ratio - Decimal("1")) * Decimal("0.002")

    qty = _entry_qty(client, symbol, rules, tuning)
    if qty <= Decimal("0"):
        return None

    anchor = _snap_inside_spread(mid, bid, ask, rules.tick_size)
    if force_direction:
        if score >= Decimal("0"):
            return Candidate(
                symbol=symbol,
                side=POSITION_SIDE_LONG,
                score=score,
                anchor_price=anchor if anchor is not None else bid,
                qty=qty,
                tick_size=rules.tick_size,
            )
        return Candidate(
            symbol=symbol,
            side=POSITION_SIDE_SHORT,
            score=score,
            anchor_price=anchor if anchor is not None else ask,
            qty=qty,
            tick_size=rules.tick_size,
        )

    long_breakout = last > previous_high and last / previous_high - Decimal("1") <= Decimal("0.02")  # 增加到 0.02，更容易触发
    short_breakout = last < previous_low and previous_low / last - Decimal("1") <= Decimal("0.02")  # 增加到 0.02，更容易触发

    # 条件1：突破策略
    if (
        long_breakout
        and score >= tuning.min_score
        and ret_5m > Decimal("0")
        and ret_15m > Decimal("0")
        and (not require_ema or ema_fast > ema_slow)
    ):
        return Candidate(
            symbol=symbol,
            side=POSITION_SIDE_LONG,
            score=score,
            anchor_price=anchor if anchor is not None else bid,
            qty=qty,
            tick_size=rules.tick_size,
        )

    if (
        short_breakout
        and score <= -tuning.min_score
        and ret_5m < Decimal("0")
        and ret_15m < Decimal("0")
        and (not require_ema or ema_fast < ema_slow)
    ):
        return Candidate(
            symbol=symbol,
            side=POSITION_SIDE_SHORT,
            score=score,
            anchor_price=anchor if anchor is not None else ask,
            qty=qty,
            tick_size=rules.tick_size,
        )

    # 条件2：趋势策略 - 不依赖突破，只看趋势和动量
    if (
        score >= tuning.min_score * Decimal("0.8")  # 降低分数要求
        and ret_5m > Decimal("0")
        and ret_15m > Decimal("0")
        and ema_fast > ema_slow
        and volume_ratio >= Decimal("1.0")  # 降低成交量要求
    ):
        return Candidate(
            symbol=symbol,
            side=POSITION_SIDE_LONG,
            score=score,
            anchor_price=anchor if anchor is not None else bid,
            qty=qty,
            tick_size=rules.tick_size,
        )

    if (
        score <= -tuning.min_score * Decimal("0.8")  # 降低分数要求
        and ret_5m < Decimal("0")
        and ret_15m < Decimal("0")
        and ema_fast < ema_slow
        and volume_ratio >= Decimal("1.0")  # 降低成交量要求
    ):
        return Candidate(
            symbol=symbol,
            side=POSITION_SIDE_SHORT,
            score=score,
            anchor_price=anchor if anchor is not None else ask,
            qty=qty,
            tick_size=rules.tick_size,
        )

    return None


def _entry_qty(client: BinanceFuturesClient, symbol: str, rules, tuning: MomentumTuning) -> Decimal:
    mark = Decimal(client.mark_price(symbol)["markPrice"])
    qty = _floor_to_step(tuning.quote_notional / mark, rules.market_qty_step)
    if qty * mark < rules.min_notional:
        qty += rules.market_qty_step
    return max(qty, rules.market_min_qty)


def _close_series(client: BinanceFuturesClient, symbol: str, *, interval: str, limit: int) -> list[Decimal]:
    response = client.client.get("/fapi/v1/klines", params={"symbol": symbol, "interval": interval, "limit": limit})
    response.raise_for_status()
    return [Decimal(row[4]) for row in response.json()]


def _klines(client: BinanceFuturesClient, symbol: str, *, interval: str, limit: int) -> list[dict[str, Decimal]]:
    response = client.client.get("/fapi/v1/klines", params={"symbol": symbol, "interval": interval, "limit": limit})
    response.raise_for_status()
    return [
        {
            "open": Decimal(row[1]),
            "high": Decimal(row[2]),
            "low": Decimal(row[3]),
            "close": Decimal(row[4]),
            "volume": Decimal(row[5]),
        }
        for row in response.json()
    ]


def _atr_pct(candles: list[dict[str, Decimal]], last_close: Decimal) -> Decimal:
    if len(candles) < 2 or last_close <= 0:
        return Decimal("0")
    ranges: list[Decimal] = []
    previous_close = candles[0]["close"]
    for candle in candles[1:]:
        true_range = max(
            candle["high"] - candle["low"],
            abs(candle["high"] - previous_close),
            abs(candle["low"] - previous_close),
        )
        ranges.append(true_range)
        previous_close = candle["close"]
    if not ranges:
        return Decimal("0")
    return (sum(ranges, Decimal("0")) / Decimal(len(ranges))) / last_close


def _ema(values: list[Decimal], span: int) -> Decimal:
    multiplier = Decimal("2") / (Decimal(span) + Decimal("1"))
    ema_value = values[0]
    for value in values[1:]:
        ema_value = value * multiplier + ema_value * (Decimal("1") - multiplier)
    return ema_value


def _tuning(profile: str) -> MomentumTuning:
    if profile == MAIN_USDC_PROFILE:
        return MAIN_TUNING
    if profile == ALT_USDT_PROFILE:
        return MomentumTuning(
            quote_notional=Decimal(str(get_settings().momentum_quote_notional_usdt)),
            take_profit_pct=ALT_TUNING.take_profit_pct,
            stop_loss_pct=ALT_TUNING.stop_loss_pct,
            min_score=ALT_TUNING.min_score,
            min_pullback_pct=ALT_TUNING.min_pullback_pct,
            max_entry_chase_pct=ALT_TUNING.max_entry_chase_pct,
        )
    raise ValueError(f"Unknown momentum profile: {profile}")


def _chase_entry(client: BinanceFuturesClient, candidate: Candidate, tuning: MomentumTuning) -> dict | None:
    start = time.time()
    active_client_order_id = ""
    LOGGER.info("%s 开始 maker-only 建仓追单，最多等待 %s 秒。", candidate.symbol, ENTRY_MAKER_TIMEOUT_SECONDS)
    out_of_range_logs = 0
    for attempt in range(1, ENTRY_ATTEMPTS + 1):
        if time.time() - start > ENTRY_MAKER_TIMEOUT_SECONDS:
            LOGGER.info("%s maker 建仓超过 %s 秒未成交，本轮放弃该币，避免市价追高和手续费损耗。", candidate.symbol, ENTRY_MAKER_TIMEOUT_SECONDS)
            if active_client_order_id:
                _cancel_quietly(client, candidate.symbol, active_client_order_id)
            return None

        book = _book(client, candidate.symbol)
        bid = Decimal(book["bidPrice"])
        ask = Decimal(book["askPrice"])
        mid = (bid + ask) / Decimal("2")

        if abs(mid - candidate.anchor_price) > candidate.anchor_price * tuning.max_entry_chase_pct:
            out_of_range_logs += 1
            if out_of_range_logs in {1, 3, 6, 10}:
                LOGGER.info(
                    "%s 等待价格回到入场区间：当前中间价=%s，参考价=%s，允许偏离=%s。",
                    candidate.symbol,
                    mid,
                    candidate.anchor_price,
                    _fmt_pct(tuning.max_entry_chase_pct),
                )
            time.sleep(1)
            continue

        if candidate.side == POSITION_SIDE_LONG:
            if mid < candidate.anchor_price * (Decimal("1") - tuning.min_pullback_pct):
                LOGGER.info("%s 已明显回落，放弃追多。", candidate.symbol)
                return None
            price = _floor_to_step(bid, candidate.tick_size)
            side = "BUY"
        else:
            if mid > candidate.anchor_price * (Decimal("1") + tuning.min_pullback_pct):
                LOGGER.info("%s 已明显反弹，放弃追空。", candidate.symbol)
                return None
            price = _ceil_to_step(ask, candidate.tick_size)
            side = "SELL"

        try:
            client_order_id = f"me_{candidate.symbol[:3].lower()}_{uuid4().hex[:10]}"
            LOGGER.info(
                "%s 第 %s 次挂 maker 建仓单：%s %s @ %s。",
                candidate.symbol,
                attempt,
                side,
                candidate.qty,
                price,
            )
            order = client.create_order(
                symbol=candidate.symbol,
                side=side,
                quantity=float(candidate.qty),
                order_type="LIMIT",
                price=float(price),
                client_order_id=client_order_id,
                position_side=candidate.side,
                time_in_force="GTX",
            )
            active_client_order_id = client_order_id
        except (httpx.HTTPStatusError, RetryError) as exc:
            LOGGER.info("%s 建仓挂单失败：%s。稍后继续。", candidate.symbol, _exception_reason(exc))
            time.sleep(1)
            continue
        except Exception as exc:
            LOGGER.info("%s 建仓挂单遇到异常：%s。稍后继续。", candidate.symbol, _exception_reason(exc))
            time.sleep(1)
            continue

        time.sleep(ORDER_CHECK_SECONDS)
        checked = client.query_order(symbol=candidate.symbol, order_id=order.get("orderId"), client_order_id=client_order_id)
        if checked.get("status") == "FILLED":
            LOGGER.info("%s maker 建仓成交：数量=%s，均价=%s。", candidate.symbol, checked.get("executedQty"), checked.get("avgPrice"))
            return checked

        LOGGER.info("%s 建仓单未完全成交，撤单后继续追价。状态=%s。", candidate.symbol, checked.get("status"))
        _cancel_quietly(client, candidate.symbol, client_order_id)
        active_client_order_id = ""

    if active_client_order_id:
        _cancel_quietly(client, candidate.symbol, active_client_order_id)
    LOGGER.info("%s maker 建仓尝试结束仍未成交，本轮放弃该币。", candidate.symbol)
    return None


def _market_entry(
    client: BinanceFuturesClient,
    candidate: Candidate,
    side: str,
    prefix: str,
) -> dict:
    client_order_id = f"{prefix}_{uuid4().hex[:10]}"
    create = client.create_order(
        symbol=candidate.symbol,
        side=side,
        quantity=float(candidate.qty),
        order_type="MARKET",
        client_order_id=client_order_id,
        position_side=candidate.side,
    )
    order = client.query_order(symbol=candidate.symbol, order_id=create.get("orderId"), client_order_id=client_order_id)
    LOGGER.info("%s 市价兜底建仓完成：%s %s，数量=%s，均价=%s。", candidate.symbol, side, candidate.side, order.get("executedQty"), order.get("avgPrice"))
    return order


def _manage_position(
    client: BinanceFuturesClient,
    candidate: Candidate,
    qty: Decimal,
    entry_price: Decimal,
    tuning: MomentumTuning,
) -> dict | None:
    position_qty = _absolute_side_qty(client.position_risk(candidate.symbol), candidate.side)
    if position_qty <= Decimal("0"):
        return None

    if candidate.side == POSITION_SIDE_LONG:
        target_price = _ceil_to_step(entry_price * (Decimal("1") + tuning.take_profit_pct), candidate.tick_size)
        stop_price = _floor_to_step(entry_price * (Decimal("1") - tuning.stop_loss_pct), candidate.tick_size)
        exit_side = "SELL"
    else:
        target_price = _floor_to_step(entry_price * (Decimal("1") - tuning.take_profit_pct), candidate.tick_size)
        stop_price = _ceil_to_step(entry_price * (Decimal("1") + tuning.stop_loss_pct), candidate.tick_size)
        exit_side = "BUY"

    LOGGER.info(
        "%s 进入持仓管理：方向=%s，持仓数量=%s，建仓均价=%s，止盈目标=%s，止损线=%s。",
        candidate.symbol,
        candidate.side,
        position_qty,
        entry_price,
        target_price,
        stop_price,
    )
    take_profit_order_id = _place_take_profit_order(
        client,
        candidate,
        position_qty,
        exit_side,
        target_price,
    )
    deadline = time.time() + MAX_MANAGE_SECONDS
    for attempt in range(1, EXIT_ATTEMPTS + 1):
        position_qty = _absolute_side_qty(client.position_risk(candidate.symbol), candidate.side)
        if position_qty <= Decimal("0"):
            LOGGER.info("%s 持仓已经归零。", candidate.symbol)
            return None
        if time.time() >= deadline:
            LOGGER.info("%s 持仓管理超时，撤掉止盈单后市价退出，避免无限占用仓位。", candidate.symbol)
            break

        book = _book(client, candidate.symbol)
        bid = Decimal(book["bidPrice"])
        ask = Decimal(book["askPrice"])
        if candidate.side == POSITION_SIDE_LONG and bid <= stop_price:
            LOGGER.info("%s 触发止损线：bid=%s <= stop=%s，撤止盈单并市价止损。", candidate.symbol, bid, stop_price)
            if take_profit_order_id:
                _cancel_quietly(client, candidate.symbol, take_profit_order_id)
            return _market_order_close(client, candidate.symbol, position_qty, exit_side, candidate.side, f"sl_{candidate.symbol[:3].lower()}")
        if candidate.side == POSITION_SIDE_SHORT and ask >= stop_price:
            LOGGER.info("%s 触发止损线：ask=%s >= stop=%s，撤止盈单并市价止损。", candidate.symbol, ask, stop_price)
            if take_profit_order_id:
                _cancel_quietly(client, candidate.symbol, take_profit_order_id)
            return _market_order_close(client, candidate.symbol, position_qty, exit_side, candidate.side, f"sl_{candidate.symbol[:3].lower()}")

        if take_profit_order_id:
            checked = client.query_order(symbol=candidate.symbol, client_order_id=take_profit_order_id)
            if checked.get("status") == "FILLED":
                LOGGER.info("%s maker 止盈成交：数量=%s，均价=%s。", candidate.symbol, checked.get("executedQty"), checked.get("avgPrice"))
                return checked
        else:
            take_profit_order_id = _place_take_profit_order(
                client,
                candidate,
                position_qty,
                exit_side,
                target_price,
            )

        time.sleep(ORDER_CHECK_SECONDS)

    if take_profit_order_id:
        _cancel_quietly(client, candidate.symbol, take_profit_order_id)
    if _absolute_side_qty(client.position_risk(candidate.symbol), candidate.side) > Decimal("0"):
        return _market_order_close(
            client,
            candidate.symbol,
            _absolute_side_qty(client.position_risk(candidate.symbol), candidate.side),
            exit_side,
            candidate.side,
            f"mt_{candidate.symbol[:3].lower()}",
        )
    return None


def _place_take_profit_order(
    client: BinanceFuturesClient,
    candidate: Candidate,
    qty: Decimal,
    side: str,
    target_price: Decimal,
) -> str:
    client_order_id = f"tp_{candidate.symbol[:3].lower()}_{uuid4().hex[:10]}"
    try:
        LOGGER.info("%s 挂 maker 止盈单：%s %s @ %s。", candidate.symbol, side, qty, target_price)
        client.create_order(
            symbol=candidate.symbol,
            side=side,
            quantity=float(qty),
            order_type="LIMIT",
            price=float(target_price),
            client_order_id=client_order_id,
            position_side=candidate.side,
            time_in_force="GTX",
        )
        return client_order_id
    except (httpx.HTTPStatusError, RetryError) as exc:
        LOGGER.info("%s 止盈挂单暂未成功：%s。后续继续尝试。", candidate.symbol, _exception_reason(exc))
        return ""


def _market_close(
    client: BinanceFuturesClient,
    symbol: str,
    qty: Decimal,
    side: str,
    position_side: str,
    prefix: str,
) -> dict:
    rules = client.symbol_rules(symbol)
    last_order: dict | None = None
    LOGGER.info("%s 开始 maker 清仓追单，最多尝试 %s 次。", symbol, EXIT_ATTEMPTS)
    start = time.time()
    for attempt in range(1, EXIT_ATTEMPTS + 1):
        remaining = _absolute_side_qty(client.position_risk(symbol), position_side)
        if remaining <= Decimal("0"):
            LOGGER.info("%s maker 清仓完成。", symbol)
            return last_order or {"status": "FILLED", "symbol": symbol}
        if time.time() - start > EXIT_MAKER_FALLBACK_SECONDS:
            LOGGER.info("%s maker 清仓超过 %s 秒未成交，执行市价兜底平仓。", symbol, EXIT_MAKER_FALLBACK_SECONDS)
            return _market_order_close(client, symbol, remaining, side, position_side, prefix)

        book = _book(client, symbol)
        bid = Decimal(book["bidPrice"])
        ask = Decimal(book["askPrice"])
        if side == "SELL":
            price = _ceil_to_step(bid + rules.tick_size, rules.tick_size) if ask - bid > rules.tick_size else _ceil_to_step(ask, rules.tick_size)
        else:
            price = _floor_to_step(ask - rules.tick_size, rules.tick_size) if ask - bid > rules.tick_size else _floor_to_step(bid, rules.tick_size)

        client_order_id = f"{prefix}_{uuid4().hex[:10]}"
        LOGGER.info("%s 第 %s 次 maker 清仓挂单：%s %s @ %s。", symbol, attempt, side, remaining, price)
        try:
            create = client.create_order(
                symbol=symbol,
                side=side,
                quantity=float(_floor_to_step(remaining, rules.qty_step)),
                order_type="LIMIT",
                price=float(price),
                client_order_id=client_order_id,
                position_side=position_side,
                time_in_force="GTX",
            )
            time.sleep(ORDER_CHECK_SECONDS)
            last_order = client.query_order(symbol=symbol, order_id=create.get("orderId"), client_order_id=client_order_id)
        except (httpx.HTTPStatusError, RetryError) as exc:
            LOGGER.info("%s maker 清仓挂单失败：%s。继续追价。", symbol, _exception_reason(exc))
            time.sleep(1)
            continue
        except Exception as exc:
            LOGGER.info("%s maker 清仓挂单异常：%s。继续追价。", symbol, _exception_reason(exc))
            time.sleep(1)
            continue
        if last_order.get("status") == "FILLED":
            LOGGER.info("%s maker 清仓单成交：数量=%s，均价=%s。", symbol, last_order.get("executedQty"), last_order.get("avgPrice"))
            continue
        LOGGER.info("%s maker 清仓单未成交，撤单后继续追价。状态=%s。", symbol, last_order.get("status"))
        _cancel_quietly(client, symbol, client_order_id)

    LOGGER.info("%s maker 清仓追单次数耗尽，仍需人工关注。", symbol)
    remaining = _absolute_side_qty(client.position_risk(symbol), position_side)
    if remaining > Decimal("0"):
        LOGGER.info("%s maker 清仓次数耗尽，执行市价兜底平仓。", symbol)
        return _market_order_close(client, symbol, remaining, side, position_side, prefix)
    return last_order or {"status": "FILLED", "symbol": symbol}


def _market_order_close(
    client: BinanceFuturesClient,
    symbol: str,
    qty: Decimal,
    side: str,
    position_side: str,
    prefix: str,
) -> dict:
    client_order_id = f"{prefix}_{uuid4().hex[:10]}"
    create = client.create_order(
        symbol=symbol,
        side=side,
        quantity=float(qty),
        order_type="MARKET",
        client_order_id=client_order_id,
        position_side=position_side,
    )
    order = client.query_order(symbol=symbol, order_id=create.get("orderId"), client_order_id=client_order_id)
    LOGGER.info("%s 市价兜底平仓完成：%s %s，数量=%s，均价=%s。", symbol, side, position_side, order.get("executedQty"), order.get("avgPrice"))
    return order


def _book(client: BinanceFuturesClient, symbol: str) -> dict:
    response = client.client.get("/fapi/v1/ticker/bookTicker", params={"symbol": symbol})
    response.raise_for_status()
    return response.json()


def _cancel_quietly(client: BinanceFuturesClient, symbol: str, client_order_id: str) -> None:
    try:
        client.cancel_order(symbol=symbol, client_order_id=client_order_id)
    except Exception:
        pass


def _side_qty(rows: list[dict], side: str) -> Decimal:
    for item in rows:
        if item.get("positionSide") == side:
            return Decimal(item.get("positionAmt", "0"))
    return Decimal("0")


def _absolute_side_qty(rows: list[dict], side: str) -> Decimal:
    return abs(_side_qty(rows, side))


def _compact_candidate(candidate: Candidate) -> dict:
    return {
        "symbol": candidate.symbol,
        "side": candidate.side,
        "score": str(candidate.score),
        "anchor_price": str(candidate.anchor_price),
        "qty": str(candidate.qty),
    }


def _candidate_names(candidates: list[Candidate]) -> str:
    return ", ".join(item.symbol for item in candidates) if candidates else "无"


def _fmt_pct(value: Decimal) -> str:
    return f"{(value * Decimal('100')):.4f}%"


def _exception_reason(exc: Exception) -> str:
    if isinstance(exc, RetryError):
        last_exc = exc.last_attempt.exception()
        if isinstance(last_exc, Exception):
            return _exception_reason(last_exc)
    if isinstance(exc, httpx.HTTPStatusError):
        try:
            payload = exc.response.json()
        except json.JSONDecodeError:
            return exc.response.text
        code = payload.get("code")
        if code == -5022:
            return "maker 报价会立即吃单，交易所拒绝 post-only 单，稍后换价重试"
        if code == -2011:
            return "订单已成交、已撤销或交易所未记录，无需重复撤单"
        return json.dumps(payload, ensure_ascii=False)
    text = str(exc).strip()
    return text or exc.__class__.__name__


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


def _compact_positions(client: BinanceFuturesClient, universe: tuple[str, ...]) -> list[dict]:
    payload: list[dict] = []
    universe_set = set(universe)
    for item in client.position_risk():
        if item.get("symbol") in universe_set:
            payload.append(
                {
                    "symbol": item.get("symbol"),
                    "positionSide": item.get("positionSide"),
                    "positionAmt": item.get("positionAmt"),
                    "entryPrice": item.get("entryPrice"),
                    "unRealizedProfit": item.get("unRealizedProfit"),
                }
            )
    return payload


def _snap_inside_spread(mid: Decimal, bid: Decimal, ask: Decimal, tick: Decimal) -> Decimal | None:
    price = _snap_to_tick(mid, tick)
    if price <= bid:
        price = bid + tick
    if price >= ask:
        price = ask - tick
    if price <= bid or price >= ask:
        return None
    return price


def _snap_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).quantize(Decimal("1")) * tick


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).quantize(Decimal("1"), rounding=ROUND_FLOOR) * step


def _ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).quantize(Decimal("1"), rounding=ROUND_CEILING) * step
