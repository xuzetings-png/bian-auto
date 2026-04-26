from __future__ import annotations

import json

from trading_bot.binance_client import BinanceAuthError, BinanceFuturesClient
from trading_bot.config import get_settings
from trading_bot.logging_utils import configure_logging


def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, log_dir=settings.log_dir_path)
    client = BinanceFuturesClient(settings)

    try:
        server_time = client.server_time()
        try:
            account = client.account_info()
        except BinanceAuthError as exc:
            payload = {
                "status": "AUTH_FAILED",
                "message": str(exc),
                "server_time": server_time.get("serverTime"),
                "hint": "请在 Binance API 管理页面检查 key/secret、USD-M Futures 权限和 IP 白名单。",
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            raise SystemExit(2) from exc
        payload = {
            "app_env": settings.app_env,
            "dry_run": settings.dry_run,
            "paper_trading": settings.paper_trading,
            "emergency_stop": settings.emergency_stop,
            "single_position_mode": settings.single_position_mode,
            "signal_dedup_seconds": settings.signal_dedup_seconds,
            "startup_position_mode": settings.startup_position_mode,
            "enforce_exchange_rules": settings.enforce_exchange_rules,
            "auto_freeze_on_recovery_error": settings.auto_freeze_on_recovery_error,
            "symbol": settings.default_symbol,
            "server_time": server_time.get("serverTime"),
            "available_balance": account.get("availableBalance"),
            "total_wallet_balance": account.get("totalWalletBalance"),
            "assets_count": len(account.get("assets", [])),
            "positions_count": len(account.get("positions", [])),
            "runtime_recovery_frozen": False,
            "runtime_recovery_reason": "",
        }
        from trading_bot.runtime_state import RuntimeStateStore

        runtime_state = RuntimeStateStore(settings.runtime_state_path).load()
        payload["runtime_recovery_frozen"] = runtime_state.recovery_frozen
        payload["runtime_recovery_reason"] = runtime_state.recovery_reason
        payload["runtime_last_client_order_id"] = runtime_state.last_client_order_id
        payload["runtime_last_order_status"] = runtime_state.last_order_status
        print(json.dumps(payload, indent=2))
    finally:
        client.close()
