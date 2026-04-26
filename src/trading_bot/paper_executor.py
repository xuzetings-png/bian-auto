from __future__ import annotations

import logging

from trading_bot.models import Side, Signal
from trading_bot.paper_store import PaperOrder, PaperState, PaperStateStore

LOGGER = logging.getLogger(__name__)


class PaperExecutor:
    def __init__(self, store: PaperStateStore, *, symbol: str) -> None:
        self.store = store
        self.state = store.load(symbol=symbol)

    def sync_mark_price(self, mark_price: float) -> None:
        self.state.last_mark_price = mark_price
        self.store.save(self.state)

    def execute(self, signal: Signal, *, mark_price: float) -> PaperOrder:
        fill_price = signal.price or mark_price
        self.state.order_seq += 1
        self.state.last_mark_price = mark_price

        quantity = signal.quantity
        signed_qty = quantity if signal.side == Side.BUY else -quantity
        current_qty = self.state.position_qty
        next_qty = current_qty + signed_qty

        if current_qty == 0 or (current_qty > 0 and signed_qty > 0) or (
            current_qty < 0 and signed_qty < 0
        ):
            self._extend_position(signed_qty, fill_price)
        else:
            self._reduce_or_flip_position(signed_qty, fill_price)

        order = PaperOrder(
            order_id=self.state.order_seq,
            symbol=signal.symbol,
            side=signal.side.value,
            quantity=quantity,
            fill_price=fill_price,
            reason=signal.reason,
            status="FILLED",
        )
        self.state.orders.append(order)
        self.state.orders = self.state.orders[-50:]
        self.store.save(self.state)

        unrealized_pnl = self.unrealized_pnl()
        LOGGER.info(
            "Paper order filled id=%s side=%s qty=%s price=%s position_qty=%s avg_entry=%s realized_pnl=%.4f unrealized_pnl=%.4f",
            order.order_id,
            order.side,
            order.quantity,
            order.fill_price,
            self.state.position_qty,
            self.state.average_entry_price,
            self.state.realized_pnl,
            unrealized_pnl,
        )
        return order

    def summary(self) -> dict:
        return {
            "symbol": self.state.symbol,
            "cash_balance": round(self.state.cash_balance, 4),
            "position_qty": round(self.state.position_qty, 6),
            "average_entry_price": round(self.state.average_entry_price, 4),
            "realized_pnl": round(self.state.realized_pnl, 4),
            "unrealized_pnl": round(self.unrealized_pnl(), 4),
            "last_mark_price": round(self.state.last_mark_price, 4),
            "orders_count": len(self.state.orders),
        }

    def position_qty(self) -> float:
        return self.state.position_qty

    def unrealized_pnl(self) -> float:
        qty = self.state.position_qty
        if qty == 0 or self.state.last_mark_price == 0:
            return 0.0

        if qty > 0:
            return (self.state.last_mark_price - self.state.average_entry_price) * qty
        return (self.state.average_entry_price - self.state.last_mark_price) * abs(qty)

    def _extend_position(self, signed_qty: float, fill_price: float) -> None:
        current_qty = self.state.position_qty
        next_qty = current_qty + signed_qty
        if current_qty == 0:
            self.state.average_entry_price = fill_price
            self.state.position_qty = next_qty
            return

        total_cost = abs(current_qty) * self.state.average_entry_price + abs(signed_qty) * fill_price
        self.state.position_qty = next_qty
        self.state.average_entry_price = total_cost / abs(next_qty)

    def _reduce_or_flip_position(self, signed_qty: float, fill_price: float) -> None:
        current_qty = self.state.position_qty
        close_qty = min(abs(current_qty), abs(signed_qty))
        self.state.realized_pnl += self._realized_pnl_for_close(
            current_qty=current_qty,
            close_qty=close_qty,
            fill_price=fill_price,
        )

        next_qty = current_qty + signed_qty
        self.state.position_qty = next_qty
        if next_qty == 0:
            self.state.average_entry_price = 0.0
            return

        if (current_qty > 0 > next_qty) or (current_qty < 0 < next_qty):
            self.state.average_entry_price = fill_price

    def _realized_pnl_for_close(
        self,
        *,
        current_qty: float,
        close_qty: float,
        fill_price: float,
    ) -> float:
        if current_qty > 0:
            return (fill_price - self.state.average_entry_price) * close_qty
        return (self.state.average_entry_price - fill_price) * close_qty
