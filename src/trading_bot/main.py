from __future__ import annotations

import logging

from trading_bot.binance_client import BinanceFuturesClient
from trading_bot.config import get_settings
from trading_bot.engine import RecoveryFreeze, RiskError, TradingEngine
from trading_bot.logging_utils import configure_logging

LOGGER = logging.getLogger(__name__)


def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, log_dir=settings.log_dir_path)
    client = BinanceFuturesClient(settings)
    engine = TradingEngine(settings, client)

    try:
        engine.run_forever()
    except KeyboardInterrupt:
        LOGGER.info("Shutdown requested by user")
    except RecoveryFreeze as exc:
        LOGGER.error("Execution frozen for recovery: %s", exc)
    except RiskError as exc:
        LOGGER.error("Risk guard stopped execution: %s", exc)
        raise
    finally:
        client.close()
