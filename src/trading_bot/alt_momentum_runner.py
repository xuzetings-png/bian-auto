from __future__ import annotations

from trading_bot.momentum_runner import run_batch
from trading_bot.momentum_profiles import ALT_USDT_PROFILE


def run() -> None:
    run_batch(profile=ALT_USDT_PROFILE)
