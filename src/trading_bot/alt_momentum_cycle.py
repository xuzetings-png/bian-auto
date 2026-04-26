from __future__ import annotations

import json

from trading_bot.momentum_cycle import run_cycle
from trading_bot.momentum_profiles import ALT_USDT_PROFILE


def run() -> None:
    print(json.dumps(run_cycle(profile=ALT_USDT_PROFILE), indent=2))
