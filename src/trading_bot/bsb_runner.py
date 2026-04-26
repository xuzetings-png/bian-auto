from __future__ import annotations

import json
import os
import time
from decimal import Decimal

import httpx

from trading_bot.binance_client import BinanceFuturesClient
from trading_bot.bsb_cycle import SYMBOL, run_cycle
from trading_bot.config import get_settings
from trading_bot.logging_utils import configure_logging

DEFAULT_ROUNDS = 10
ROUND_PAUSE_SECONDS = 10
QUERY_RETRIES = 5


def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, log_dir=settings.log_dir_path)
    if settings.paper_trading or settings.dry_run or settings.emergency_stop:
        raise RuntimeError("BSB runner requires PAPER_TRADING=false, DRY_RUN=false, and EMERGENCY_STOP=false.")

    rounds = int(os.getenv("BSB_RUNNER_ROUNDS", str(DEFAULT_ROUNDS)))
    baseline = _realized_pnl()
    results: list[dict] = []

    for index in range(1, rounds + 1):
        result = run_cycle()
        result["round"] = index
        result["batch_realized_pnl"] = str(_realized_pnl() - baseline)
        results.append(result)
        print(json.dumps(result, indent=2), flush=True)
        _ensure_clean_end()
        if index < rounds:
            time.sleep(ROUND_PAUSE_SECONDS)

    print(json.dumps(_summary(results, _realized_pnl() - baseline), indent=2))


def _realized_pnl() -> Decimal:
    for attempt in range(1, QUERY_RETRIES + 1):
        settings = get_settings()
        client = BinanceFuturesClient(settings)
        try:
            response = client._signed_request(
                "GET",
                "/fapi/v1/income",
                params={"symbol": SYMBOL, "incomeType": "REALIZED_PNL", "limit": 100},
            )
            response.raise_for_status()
            return sum((Decimal(item["income"]) for item in response.json()), Decimal("0"))
        except httpx.HTTPError:
            if attempt == QUERY_RETRIES:
                raise
            time.sleep(attempt)
        finally:
            client.close()
    raise RuntimeError("unreachable")


def _ensure_clean_end() -> None:
    for attempt in range(1, QUERY_RETRIES + 1):
        settings = get_settings()
        client = BinanceFuturesClient(settings)
        try:
            if client.open_orders(SYMBOL):
                raise RuntimeError(f"{SYMBOL} ended dirty: open orders remain")
            positions = client.position_risk(SYMBOL)
            long_qty = next((Decimal(item["positionAmt"]) for item in positions if item["positionSide"] == "LONG"), Decimal("0"))
            short_qty = next((Decimal(item["positionAmt"]) for item in positions if item["positionSide"] == "SHORT"), Decimal("0"))
            if long_qty != Decimal("0") or short_qty != Decimal("0"):
                raise RuntimeError(f"{SYMBOL} ended dirty: long={long_qty}, short={short_qty}")
            return
        except httpx.HTTPError:
            if attempt == QUERY_RETRIES:
                raise
            time.sleep(attempt)
        finally:
            client.close()


def _summary(results: list[dict], batch_pnl: Decimal) -> dict:
    enters = [item for item in results if item.get("entry_price")]
    done = [item for item in results if item.get("status") == "DONE"]
    skipped = [item for item in results if item.get("status") == "NO_ENTRY_FILL"]
    return {
        "status": "SUMMARY",
        "symbol": SYMBOL,
        "rounds": len(results),
        "entered": len(enters),
        "completed": len(done),
        "skipped": len(skipped),
        "batch_realized_pnl": str(batch_pnl),
    }
