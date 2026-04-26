from __future__ import annotations

import json
import logging
import time
from decimal import Decimal

import httpx

from trading_bot.binance_client import BinanceFuturesClient
from trading_bot.config import get_settings
from trading_bot.logging_utils import configure_logging
from trading_bot.momentum_cycle import DEFAULT_PROFILE, run_cycle
from trading_bot.momentum_profiles import compact_universe
from trading_bot.position_closer import close_all_positions_maker

DEFAULT_ROUNDS = 8
ROUND_PAUSE_SECONDS = 10
QUERY_RETRIES = 5
ROUND_TAKE_PROFIT_USDT = Decimal("1")
ROUND_STOP_LOSS_USDT = Decimal("-1.5")
NET_INCOME_TYPES = {"REALIZED_PNL", "COMMISSION", "FUNDING_FEE"}
LOGGER = logging.getLogger(__name__)


def run() -> None:
    run_batch()


def run_batch(profile: str = DEFAULT_PROFILE) -> None:
    settings = get_settings()
    configure_logging(settings.log_level, log_dir=settings.log_dir_path)
    if settings.paper_trading or settings.dry_run or settings.emergency_stop:
        raise RuntimeError("Momentum runner requires PAPER_TRADING=false, DRY_RUN=false, and EMERGENCY_STOP=false.")

    rounds = settings.momentum_runner_rounds or DEFAULT_ROUNDS
    take_profit = Decimal(str(settings.momentum_round_take_profit_usdt))
    stop_loss = Decimal(str(settings.momentum_round_stop_loss_usdt))
    max_cycles_per_round = settings.momentum_round_max_cycles
    LOGGER.info(
        "策略启动：profile=%s，计划运行 %s 轮；每轮目标盈利=%sU，单轮止损=%sU。",
        profile,
        rounds,
        take_profit,
        stop_loss,
    )
    baseline = _realized_pnl()
    LOGGER.info("启动时净收益基准：%s。", baseline)
    results: list[dict] = []
    for index in range(1, rounds + 1):
        round_baseline = _realized_pnl()
        cycle_index = 0
        round_results: list[dict] = []
        LOGGER.info("开始第 %s/%s 轮；本轮会反复重新扫描市场，直到达到盈亏阈值。", index, rounds)
        while True:
            cycle_index += 1
            current_round_pnl = _realized_pnl() - round_baseline
            current_batch_pnl = _realized_pnl() - baseline
            LOGGER.info(
                "收益更新：本轮收益=%sU，本批累计收益=%sU；目标=%sU，止损=%sU。",
                current_round_pnl,
                current_batch_pnl,
                take_profit,
                stop_loss,
            )
            LOGGER.info("第 %s 轮第 %s 次全市场扫描。", index, cycle_index)
            result = run_cycle(profile=profile, universe=None)
            result["round"] = index
            result["round_cycle"] = cycle_index
            result["batch_realized_pnl"] = str(_realized_pnl() - baseline)
            result["round_realized_pnl"] = str(_realized_pnl() - round_baseline)
            round_results.append(result)
            results.append(result)
            LOGGER.info(
                "第 %s 轮第 %s 次扫描结束：status=%s，本轮净收益=%sU，本批净收益=%sU。",
                index,
                cycle_index,
                result.get("status"),
                result["round_realized_pnl"],
                result["batch_realized_pnl"],
            )
            print(json.dumps(result, indent=2), flush=True)

            universe = tuple(result.get("universe", []))
            if universe:
                _ensure_clean_end(universe)

            round_pnl = Decimal(result["round_realized_pnl"])
            if round_pnl >= take_profit:
                LOGGER.info("第 %s 轮达到盈利目标：%sU >= %sU，进入下一轮。", index, round_pnl, take_profit)
                break
            if round_pnl <= stop_loss:
                LOGGER.info("第 %s 轮触发止损：%sU <= %sU，进入下一轮。", index, round_pnl, stop_loss)
                break
            if max_cycles_per_round > 0 and cycle_index >= max_cycles_per_round:
                LOGGER.info("第 %s 轮达到最大扫描次数 %s，进入下一轮。", index, max_cycles_per_round)
                break

            LOGGER.info("第 %s 轮尚未达到盈亏阈值，等待 %s 秒后重新扫描。", index, ROUND_PAUSE_SECONDS)
            time.sleep(ROUND_PAUSE_SECONDS)

        LOGGER.info("第 %s 轮完成，共执行 %s 次扫描。", index, len(round_results))
        if index < rounds:
            LOGGER.info("等待 %s 秒后进入下一轮。", ROUND_PAUSE_SECONDS)
            time.sleep(ROUND_PAUSE_SECONDS)

    LOGGER.info("策略批次完成。")
    print(json.dumps(_summary(results, _realized_pnl() - baseline, profile=profile), indent=2))


def _realized_pnl() -> Decimal:
    for attempt in range(1, QUERY_RETRIES + 1):
        settings = get_settings()
        client = BinanceFuturesClient(settings)
        try:
            response = client._signed_request(
                "GET",
                "/fapi/v1/income",
                params={"limit": 1000},
            )
            response.raise_for_status()
            return sum(
                (
                    Decimal(item["income"])
                    for item in response.json()
                    if item.get("incomeType") in NET_INCOME_TYPES
                ),
                Decimal("0"),
            )
        except httpx.HTTPError:
            if attempt == QUERY_RETRIES:
                raise
            time.sleep(attempt)
        finally:
            client.close()
    raise RuntimeError("unreachable")


def _ensure_clean_end(universe: tuple[str, ...]) -> None:
    for attempt in range(1, QUERY_RETRIES + 1):
        settings = get_settings()
        client = BinanceFuturesClient(settings)
        try:
            universe_set = set(universe)
            open_orders = [order for order in client.open_orders() if str(order.get("symbol")) in universe_set]
            positions = []
            for row in client.position_risk():
                symbol = str(row.get("symbol", ""))
                if symbol not in universe_set:
                    continue
                qty = Decimal(str(row.get("positionAmt", "0")))
                if qty != Decimal("0"):
                    positions.append(row)
            if not open_orders and not positions:
                return

            LOGGER.info(
                "本轮结束发现残留：未成交挂单=%s，未平仓位=%s，交给全局清仓器统一处理。",
                len(open_orders),
                len(positions),
            )
            close_result = close_all_positions_maker()
            close_status = close_result.get("status")
            if close_status in {"FLAT", "NO_POSITION"}:
                return
            if close_status == "CLOSE_ALREADY_RUNNING":
                LOGGER.info("全局清仓器已经在运行，等待它完成后复查。")

            if attempt < QUERY_RETRIES:
                LOGGER.info("残留处理后等待复查 (%s/%s)：清仓状态=%s。", attempt, QUERY_RETRIES, close_status)
                time.sleep(5)
                continue
            raise RuntimeError(f"本轮结束仍未清干净：清仓状态={close_status}")
        except httpx.HTTPError:
            if attempt == QUERY_RETRIES:
                raise
            time.sleep(attempt)
        finally:
            client.close()


def _summary(results: list[dict], batch_pnl: Decimal, *, profile: str) -> dict:
    entered = [
        child
        for item in results
        for child in item.get("results", [])
        if child.get("entry")
    ]
    completed = [
        child
        for item in results
        for child in item.get("results", [])
        if child.get("status") == "DONE"
    ]
    skipped = [item for item in results if item.get("status") in {"NO_CANDIDATE", "NO_ENTRY_FILL"}]
    return {
        "status": "SUMMARY",
        "profile": profile,
        "universe": compact_universe(tuple(results[0].get("universe", []))) if results else [],
        "rounds": len(results),
        "entered": len(entered),
        "completed": len(completed),
        "skipped": len(skipped),
        "batch_realized_pnl": str(batch_pnl),
    }
