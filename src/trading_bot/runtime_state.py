from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(slots=True)
class RuntimeState:
    last_signal_key: str = ""
    last_signal_ts: float = 0.0
    expected_position_qty: float = 0.0
    last_exchange_position_qty: float = 0.0
    startup_reconciled: bool = False
    startup_mode: str = ""
    recovery_frozen: bool = False
    recovery_reason: str = ""
    last_client_order_id: str = ""
    last_order_status: str = ""
    last_order_id: str = ""


class RuntimeStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> RuntimeState:
        if not self.path.exists():
            state = RuntimeState()
            self.save(state)
            return state

        data = json.loads(self.path.read_text(encoding="utf-8"))
        return RuntimeState(
            last_signal_key=data.get("last_signal_key", ""),
            last_signal_ts=data.get("last_signal_ts", 0.0),
            expected_position_qty=data.get("expected_position_qty", 0.0),
            last_exchange_position_qty=data.get("last_exchange_position_qty", 0.0),
            startup_reconciled=data.get("startup_reconciled", False),
            startup_mode=data.get("startup_mode", ""),
            recovery_frozen=data.get("recovery_frozen", False),
            recovery_reason=data.get("recovery_reason", ""),
            last_client_order_id=data.get("last_client_order_id", ""),
            last_order_status=data.get("last_order_status", ""),
            last_order_id=data.get("last_order_id", ""),
        )

    def save(self, state: RuntimeState) -> None:
        self.path.write_text(
            json.dumps(asdict(state), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
