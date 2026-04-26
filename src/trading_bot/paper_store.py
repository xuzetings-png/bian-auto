from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass(slots=True)
class PaperOrder:
    order_id: int
    symbol: str
    side: str
    quantity: float
    fill_price: float
    reason: str
    status: str


@dataclass(slots=True)
class PaperState:
    symbol: str
    cash_balance: float = 10000.0
    position_qty: float = 0.0
    average_entry_price: float = 0.0
    realized_pnl: float = 0.0
    last_mark_price: float = 0.0
    order_seq: int = 0
    orders: list[PaperOrder] = field(default_factory=list)


class PaperStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self, *, symbol: str) -> PaperState:
        if not self.path.exists():
            state = PaperState(symbol=symbol)
            self.save(state)
            return state

        data = json.loads(self.path.read_text(encoding="utf-8"))
        orders = [PaperOrder(**item) for item in data.get("orders", [])]
        return PaperState(
            symbol=data.get("symbol", symbol),
            cash_balance=data.get("cash_balance", 10000.0),
            position_qty=data.get("position_qty", 0.0),
            average_entry_price=data.get("average_entry_price", 0.0),
            realized_pnl=data.get("realized_pnl", 0.0),
            last_mark_price=data.get("last_mark_price", 0.0),
            order_seq=data.get("order_seq", 0),
            orders=orders,
        )

    def save(self, state: PaperState) -> None:
        payload = asdict(state)
        self.path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
