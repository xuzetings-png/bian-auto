from __future__ import annotations

import logging
import time
from uuid import uuid4

from trading_bot.binance_client import BinanceFuturesClient
from trading_bot.config import Settings
from trading_bot.execution_rules import normalize_order_values, validate_signal_against_rules
from trading_bot.models import OrderType, Signal, SignalAction
from trading_bot.paper_executor import PaperExecutor
from trading_bot.paper_store import PaperStateStore
from trading_bot.runtime_state import RuntimeStateStore
from trading_bot.strategy import MarketSnapshot, build_strategy

LOGGER = logging.getLogger(__name__)


class RiskError(Exception):
    pass


class SignalBlocked(Exception):
    pass


class RecoveryFreeze(Exception):
    pass


class TradingEngine:
    def __init__(self, settings: Settings, client: BinanceFuturesClient) -> None:
        self.settings = settings
        self.client = client
        self.strategy = build_strategy(settings.strategy_name, settings)
        self.last_healthcheck_at = 0.0
        self.paper_executor = PaperExecutor(
            PaperStateStore(settings.paper_state_path),
            symbol=settings.default_symbol,
        )
        self.runtime_state_store = RuntimeStateStore(settings.runtime_state_path)
        self.runtime_state = self.runtime_state_store.load()
        self.symbol_rules = self.client.symbol_rules(settings.default_symbol)

    def run_forever(self) -> None:
        LOGGER.info(
            "Starting engine app_env=%s dry_run=%s paper_trading=%s strategy=%s symbol=%s startup_position_mode=%s",
            self.settings.app_env,
            self.settings.dry_run,
            self.settings.paper_trading,
            self.settings.strategy_name,
            self.settings.default_symbol,
            self.settings.startup_position_mode,
        )
        self._reconcile_startup_position()

        cycle = 0
        while True:
            self._tick()
            cycle += 1
            if self.settings.max_cycles > 0 and cycle >= self.settings.max_cycles:
                LOGGER.info("Reached MAX_CYCLES=%s, stopping", self.settings.max_cycles)
                return
            time.sleep(self.settings.poll_interval_seconds)

    def _tick(self) -> None:
        self._ensure_not_recovery_frozen()
        self._maybe_healthcheck()
        snapshot = self._load_market_snapshot()
        signal = self.strategy.generate_signal(snapshot)

        if signal is None:
            LOGGER.info(
                "No signal symbol=%s mark_price=%s",
                snapshot.symbol,
                snapshot.mark_price,
            )
            return

        try:
            self._execute_signal(signal, mark_price=snapshot.mark_price)
        except SignalBlocked as exc:
            LOGGER.warning("Signal blocked: %s", exc)

    def _maybe_healthcheck(self) -> None:
        now = time.time()
        if now - self.last_healthcheck_at < self.settings.healthcheck_interval_seconds:
            return

        self.client.ping()
        server_time = self.client.server_time()
        LOGGER.info("Healthcheck ok server_time=%s", server_time.get("serverTime"))

        if self.settings.has_api_credentials:
            account = self.client.account_info()
            LOGGER.info(
                "Account connected available_balance=%s total_wallet_balance=%s",
                account.get("availableBalance"),
                account.get("totalWalletBalance"),
            )
        else:
            LOGGER.info("API credentials missing, signed account checks skipped")

        self.last_healthcheck_at = now

    def _load_market_snapshot(self) -> MarketSnapshot:
        payload = self.client.mark_price(self.settings.default_symbol)
        snapshot = MarketSnapshot(
            symbol=payload["symbol"],
            mark_price=float(payload["markPrice"]),
        )
        self.paper_executor.sync_mark_price(snapshot.mark_price)
        return snapshot

    def _execute_signal(self, signal: Signal, *, mark_price: float) -> None:
        current_position_qty = self._current_position_qty()
        self._validate_position_sync(current_position_qty)
        self._validate_duplicate_signal(signal)
        self._validate_single_position(signal, current_position_qty)
        self._validate_reduce_only(signal, current_position_qty)
        signal = self._prepare_signal(signal, mark_price=mark_price)
        self._validate_risk(signal, mark_price)

        if self.settings.paper_trading:
            order = self.paper_executor.execute(signal, mark_price=mark_price)
            new_qty = self.paper_executor.position_qty()
            self._record_signal(signal, new_position_qty=new_qty)
            LOGGER.info("Paper state summary=%s last_order_id=%s", self.paper_executor.summary(), order.order_id)
            return

        if self.settings.emergency_stop:
            LOGGER.warning(
                "Emergency stop is enabled. Signal blocked symbol=%s side=%s reason=%s",
                signal.symbol,
                signal.side.value,
                signal.reason,
            )
            return

        if self.settings.dry_run:
            LOGGER.info(
                "DRY RUN order symbol=%s side=%s quantity=%s type=%s reason=%s",
                signal.symbol,
                signal.side.value,
                signal.quantity,
                signal.order_type.value,
                signal.reason,
            )
            return

        result = self.client.create_order(
            symbol=signal.symbol,
            side=signal.side.value,
            quantity=signal.quantity,
            order_type=signal.order_type.value,
            price=signal.price if signal.order_type == OrderType.LIMIT else None,
            reduce_only=signal.reduce_only,
            client_order_id=self._client_order_id(signal),
        )
        reconciled = self._reconcile_submitted_order(signal.symbol, result)
        next_qty = self._apply_signal_to_position(current_position_qty, signal)
        self._record_signal(signal, new_position_qty=next_qty)
        LOGGER.info("Live order submitted response=%s reconciled=%s", result, reconciled)

    def _validate_risk(self, signal: Signal, mark_price: float) -> None:
        notional = signal.quantity * mark_price
        if notional > self.settings.max_notional_usdt:
            raise SignalBlocked(
                f"Signal blocked. notional={notional:.2f} exceeds "
                f"MAX_NOTIONAL_USDT={self.settings.max_notional_usdt:.2f}"
            )

        if not self.settings.paper_trading and not self.settings.dry_run and not self.settings.has_api_credentials:
            raise RiskError("Live trading requires Binance API credentials.")

    def _prepare_signal(self, signal: Signal, *, mark_price: float) -> Signal:
        if not self.settings.enforce_exchange_rules:
            return signal

        signal = normalize_order_values(signal, self.symbol_rules)
        try:
            validate_signal_against_rules(signal, self.symbol_rules, mark_price=mark_price)
        except ValueError as exc:
            raise SignalBlocked(f"Exchange rule check failed: {exc}") from exc
        return signal

    def _current_position_qty(self) -> float:
        if self.settings.paper_trading:
            return self.paper_executor.position_qty()

        positions = self.client.position_risk(self.settings.default_symbol)
        position_qty = 0.0
        for item in positions:
            amount = float(item.get("positionAmt", 0.0))
            if amount != 0.0:
                position_qty += amount

        self.runtime_state.last_exchange_position_qty = position_qty
        self.runtime_state_store.save(self.runtime_state)
        return position_qty

    def _validate_position_sync(self, current_position_qty: float) -> None:
        if self.runtime_state.last_signal_ts == 0:
            self.runtime_state.expected_position_qty = current_position_qty
            self.runtime_state.last_exchange_position_qty = current_position_qty
            self.runtime_state_store.save(self.runtime_state)
            return

        expected = self.runtime_state.expected_position_qty
        tolerance = self.settings.position_sync_tolerance
        if abs(current_position_qty - expected) > tolerance:
            self._freeze_recovery(
                "Position sync check failed. "
                f"expected_position_qty={expected}, current_position_qty={current_position_qty}"
            )

    def _validate_duplicate_signal(self, signal: Signal) -> None:
        signal_key = self._signal_key(signal)
        now = time.time()
        if (
            signal_key == self.runtime_state.last_signal_key
            and now - self.runtime_state.last_signal_ts < self.settings.signal_dedup_seconds
        ):
            raise SignalBlocked(
                "Duplicate signal blocked within dedup window. "
                f"signal_key={signal_key}"
            )

    def _validate_single_position(self, signal: Signal, current_position_qty: float) -> None:
        if not self.settings.single_position_mode:
            return

        tolerance = self.settings.position_sync_tolerance
        action = signal.action
        has_long = current_position_qty > tolerance
        has_short = current_position_qty < -tolerance
        flat = abs(current_position_qty) <= tolerance

        if action == SignalAction.OPEN_LONG and not flat:
            raise SignalBlocked(f"Single-position guard blocked OPEN_LONG while position_qty={current_position_qty}")
        if action == SignalAction.OPEN_SHORT and not flat:
            raise SignalBlocked(f"Single-position guard blocked OPEN_SHORT while position_qty={current_position_qty}")
        if action == SignalAction.CLOSE_LONG and not has_long:
            raise SignalBlocked(f"Single-position guard blocked CLOSE_LONG while position_qty={current_position_qty}")
        if action == SignalAction.CLOSE_SHORT and not has_short:
            raise SignalBlocked(f"Single-position guard blocked CLOSE_SHORT while position_qty={current_position_qty}")

    def _validate_reduce_only(self, signal: Signal, current_position_qty: float) -> None:
        if not signal.reduce_only:
            return

        tolerance = self.settings.position_sync_tolerance
        if abs(current_position_qty) <= tolerance:
            raise SignalBlocked("Reduce-only order blocked because current position is flat")

        if signal.side.value == "SELL" and current_position_qty <= tolerance:
            raise SignalBlocked(
                f"Reduce-only SELL blocked because current position is not long: {current_position_qty}"
            )
        if signal.side.value == "BUY" and current_position_qty >= -tolerance:
            raise SignalBlocked(
                f"Reduce-only BUY blocked because current position is not short: {current_position_qty}"
            )

    def _record_signal(self, signal: Signal, *, new_position_qty: float) -> None:
        self.runtime_state.last_signal_key = self._signal_key(signal)
        self.runtime_state.last_signal_ts = time.time()
        self.runtime_state.expected_position_qty = new_position_qty
        self.runtime_state_store.save(self.runtime_state)

    def _signal_key(self, signal: Signal) -> str:
        action = signal.action.value if signal.action else "NONE"
        return "|".join(
            [
                signal.symbol,
                action,
                signal.side.value,
                signal.order_type.value,
                f"{signal.quantity:.8f}",
                "reduce" if signal.reduce_only else "open",
            ]
        )

    def _apply_signal_to_position(self, current_position_qty: float, signal: Signal) -> float:
        signed_qty = signal.quantity if signal.side.value == "BUY" else -signal.quantity
        return current_position_qty + signed_qty

    def _client_order_id(self, signal: Signal) -> str:
        action = signal.action.value.lower() if signal.action else "none"
        return f"bot_{action}_{uuid4().hex[:20]}"

    def _reconcile_startup_position(self) -> None:
        current_position_qty = self._current_position_qty()
        tolerance = self.settings.position_sync_tolerance
        mode = self.settings.startup_position_mode.strip().lower()

        if self.runtime_state.startup_reconciled and self.runtime_state.startup_mode == mode:
            return

        if abs(current_position_qty) <= tolerance:
            self.runtime_state.expected_position_qty = 0.0
            self.runtime_state.startup_reconciled = True
            self.runtime_state.startup_mode = mode
            self.runtime_state_store.save(self.runtime_state)
            LOGGER.info("Startup position reconciliation complete: flat position")
            return

        if mode == "adopt":
            self.runtime_state.expected_position_qty = current_position_qty
            self.runtime_state.last_exchange_position_qty = current_position_qty
            self.runtime_state.startup_reconciled = True
            self.runtime_state.startup_mode = mode
            self.runtime_state_store.save(self.runtime_state)
            LOGGER.warning(
                "Startup reconciliation adopted existing position_qty=%s into runtime state",
                current_position_qty,
            )
            return

        if mode == "freeze":
            raise RiskError(
                "Startup position reconciliation froze execution because an existing position was found. "
                f"current_position_qty={current_position_qty}"
            )

        raise RiskError(f"Unsupported STARTUP_POSITION_MODE={self.settings.startup_position_mode}")

    def _reconcile_submitted_order(self, symbol: str, create_response: dict) -> dict:
        client_order_id = create_response.get("clientOrderId", "")
        order_id = create_response.get("orderId", "")
        self.runtime_state.last_client_order_id = str(client_order_id)
        self.runtime_state.last_order_id = str(order_id)
        self.runtime_state_store.save(self.runtime_state)

        try:
            order = self.client.query_order(
                symbol=symbol,
                order_id=int(order_id) if order_id not in ("", None) else None,
                client_order_id=client_order_id or None,
            )
        except Exception as exc:
            self._freeze_recovery(f"Order reconciliation query failed: {exc}")

        status = order.get("status", "")
        self.runtime_state.last_order_status = status
        self.runtime_state_store.save(self.runtime_state)
        if status in {"REJECTED", "EXPIRED"}:
            self._freeze_recovery(f"Order reconciliation returned terminal failure status={status}")
        return order

    def _freeze_recovery(self, reason: str) -> None:
        if not self.settings.auto_freeze_on_recovery_error:
            raise RiskError(reason)

        self.runtime_state.recovery_frozen = True
        self.runtime_state.recovery_reason = reason
        self.runtime_state_store.save(self.runtime_state)
        raise RecoveryFreeze(reason)

    def _ensure_not_recovery_frozen(self) -> None:
        if self.runtime_state.recovery_frozen:
            raise RecoveryFreeze(
                "Runtime is frozen for recovery. "
                f"reason={self.runtime_state.recovery_reason}"
            )
