from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from trading_bot.config import get_settings
from trading_bot.position_closer import close_all_positions_maker


ROOT_DIR = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT_DIR / "logs"
DATA_DIR = ROOT_DIR / "data"
DASHBOARD_LOG = LOG_DIR / "dashboard-run.log"
BOT_LOG = LOG_DIR / "bot.log"
RUNTIME_STATE = DATA_DIR / "runtime_state.json"

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765

SIGNATURE_RE = re.compile(r"([?&]signature=)[^&\s\"]+")

COMMANDS: dict[str, dict[str, str]] = {
    "bot-alt-momentum-runner": {
        "label": "自动策略",
        "description": "运行 USDT 山寨币动量扫描，使用 maker 建仓和平仓追单。",
    },
}


@dataclass
class ManagedProcess:
    command: str
    process: subprocess.Popen[bytes]
    started_at: float


class ProcessManager:
    def __init__(self) -> None:
        self.current: ManagedProcess | None = None
        self._lock = threading.Lock()
        self._stopping = False

    def status(self) -> dict[str, object]:
        current = self.current
        if current is None:
            return {"running": False}

        returncode = current.process.poll()
        running = returncode is None
        return {
            "running": running,
            "command": current.command,
            "pid": current.process.pid,
            "started_at": current.started_at,
            "uptime_seconds": max(0, int(time.time() - current.started_at)),
            "returncode": returncode,
        }

    def start(self, command: str) -> dict[str, object]:
        if command not in COMMANDS:
            raise ValueError(f"Unknown command: {command}")

        if self.current is not None and self.current.process.poll() is None:
            raise RuntimeError(f"{self.current.command} is already running")

        blockers = _live_mode_blockers()
        if blockers:
            payload = {
                "status": "START_BLOCKED_SAFE_MODE",
                "message": "策略没有启动：当前仍是安全模式。",
                "blockers": blockers,
            }
            _append_dashboard_event("START_BLOCKED", payload)
            return {"running": False, "blocked": True, "blockers": blockers}

        LOG_DIR.mkdir(parents=True, exist_ok=True)
        executable = _command_executable(command)
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        with DASHBOARD_LOG.open("ab") as log_file:
            log_file.write(_log_marker(f"START {command}").encode("utf-8"))
            process = subprocess.Popen(
                [str(executable)],
                cwd=ROOT_DIR,
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )

        self.current = ManagedProcess(command=command, process=process, started_at=time.time())
        return self.status()

    def stop(self) -> dict[str, object]:
        with self._lock:
            if self._stopping:
                return {"running": bool(self.current and self.current.process.poll() is None), "message": "正在停止并清仓，请等待当前流程完成。"}
            self._stopping = True

        try:
            return self._stop_once()
        finally:
            with self._lock:
                self._stopping = False

    def _stop_once(self) -> dict[str, object]:
        if self.current is None:
            close_result = close_all_positions_maker()
            _append_dashboard_event("CLOSE_RESULT", close_result)
            return {"running": False, "message": "No process to stop", "close_result": close_result}

        process = self.current.process
        if process.poll() is not None:
            status = self.status()
            status["close_result"] = close_all_positions_maker()
            _append_dashboard_event("CLOSE_RESULT", status["close_result"])
            return status

        with DASHBOARD_LOG.open("ab") as log_file:
            log_file.write(_log_marker(f"STOP {self.current.command}").encode("utf-8"))

        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
        status = self.status()
        close_result = close_all_positions_maker()
        status["close_result"] = close_result
        _append_dashboard_event("CLOSE_RESULT", close_result)
        return status

    def shutdown(self) -> None:
        current = self.current
        if current is not None and current.process.poll() is None:
            self.stop()


MANAGER = ProcessManager()


def _command_executable(command: str) -> Path:
    executable = ROOT_DIR / ".venv" / "bin" / command
    if executable.exists():
        return executable
    fallback = Path(sys.executable).resolve().parent / command
    if fallback.exists():
        return fallback
    raise RuntimeError(f"Command executable not found: {command}. Run pip install -e . first.")


def _strategy_command() -> str:
    command = os.getenv("DASHBOARD_STRATEGY_COMMAND", "bot-alt-momentum-runner")
    if command not in COMMANDS:
        raise RuntimeError(f"Unsupported DASHBOARD_STRATEGY_COMMAND: {command}")
    return command


def _log_marker(message: str) -> str:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    return f"\n[{timestamp}] dashboard | {message}\n".encode("utf-8").decode("utf-8")


def _append_dashboard_event(label: str, payload: object) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with DASHBOARD_LOG.open("ab") as log_file:
        log_file.write(_log_marker(label).encode("utf-8"))
        log_file.write(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        log_file.write(b"\n")


def _live_mode_blockers() -> list[str]:
    get_settings.cache_clear()
    settings = get_settings()
    blockers: list[str] = []
    if settings.paper_trading:
        blockers.append("PAPER_TRADING 当前是 true，需要改成 false")
    if settings.dry_run:
        blockers.append("DRY_RUN 当前是 true，需要改成 false")
    if settings.emergency_stop:
        blockers.append("EMERGENCY_STOP 当前是 true，需要改成 false")
    if not settings.has_api_credentials:
        blockers.append("没有读取到 Binance API Key/Secret")
    return blockers


def _runtime_mode_summary() -> dict[str, object]:
    settings = get_settings()
    blockers = _live_mode_blockers()
    safety_on = settings.paper_trading or settings.dry_run or settings.emergency_stop
    return {
        "ready_for_live": not blockers,
        "safety_on": safety_on,
        "blockers": blockers,
        "effective": {
            "PAPER_TRADING": str(settings.paper_trading).lower(),
            "DRY_RUN": str(settings.dry_run).lower(),
            "EMERGENCY_STOP": str(settings.emergency_stop).lower(),
            "DEFAULT_SYMBOL": settings.default_symbol,
            "MAX_NOTIONAL_USDT": str(settings.max_notional_usdt),
            "MAX_POSITION_NOTIONAL_USDT": str(settings.max_position_notional_usdt),
            "MOMENTUM_UNIVERSE_TOP_N": str(settings.momentum_universe_top_n),
            "MOMENTUM_QUOTE_NOTIONAL_USDT": str(settings.momentum_quote_notional_usdt),
            "MOMENTUM_MAX_CONCURRENT_TRADES": str(settings.momentum_max_concurrent_trades),
            "MOMENTUM_ROUND_TAKE_PROFIT_USDT": str(settings.momentum_round_take_profit_usdt),
            "MOMENTUM_ROUND_STOP_LOSS_USDT": str(settings.momentum_round_stop_loss_usdt),
        },
    }


def _set_safety_mode(enabled: bool) -> dict[str, object]:
    updates = {
        "PAPER_TRADING": "true" if enabled else "false",
        "DRY_RUN": "true" if enabled else "false",
        "EMERGENCY_STOP": "true" if enabled else "false",
    }
    _write_env_values(updates)
    get_settings.cache_clear()
    payload = {
        "status": "SAFETY_MODE_ON" if enabled else "SAFETY_MODE_OFF",
        "message": "安全模式已开启：不会真实下单。" if enabled else "安全模式已关闭：允许实盘策略发单。",
        "updates": updates,
    }
    _append_dashboard_event("SAFETY_MODE", payload)
    return payload


def _set_strategy_config(payload: dict[str, object]) -> dict[str, object]:
    top_n = int(payload.get("top_n", 10))
    quote_notional = float(payload.get("quote_notional", 25))
    if not 1 <= top_n <= 30:
        raise ValueError("扫描数量需要在 1 到 30 之间。")
    if not 5 <= quote_notional <= 200:
        raise ValueError("单币投入金额需要在 5U 到 200U 之间。")

    updates = {
        "MOMENTUM_UNIVERSE_TOP_N": str(top_n),
        "MOMENTUM_QUOTE_NOTIONAL_USDT": _format_config_number(quote_notional),
    }
    _write_env_values(updates)
    get_settings.cache_clear()
    result = {
        "status": "STRATEGY_CONFIG_UPDATED",
        "message": f"策略配置已更新：动态取前 {top_n} 个币，单币投入 {updates['MOMENTUM_QUOTE_NOTIONAL_USDT']}U。",
        "updates": updates,
    }
    _append_dashboard_event("STRATEGY_CONFIG", result)
    return result


def _format_config_number(value: float) -> str:
    text = f"{value:.8f}".rstrip("0").rstrip(".")
    return text or "0"


def _write_env_values(updates: dict[str, str]) -> None:
    env_path = ROOT_DIR / ".env"
    lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    seen: set[str] = set()
    output: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            output.append(line)
            continue
        key, _value = line.split("=", 1)
        key = key.strip()
        if key in updates:
            output.append(f"{key}={updates[key]}")
            seen.add(key)
        else:
            output.append(line)
    if output and output[-1].strip():
        output.append("")
    for key, value in updates.items():
        if key not in seen:
            output.append(f"{key}={value}")
    env_path.write_text("\n".join(output).rstrip() + "\n", encoding="utf-8")


def _read_tail(path: Path, max_bytes: int, *, offset: int | None = None) -> tuple[str, int]:
    if not path.exists():
        return "", 0
    with path.open("rb") as file:
        file.seek(0, os.SEEK_END)
        size = file.tell()
        start = offset if offset is not None else max(0, size - max_bytes)
        file.seek(min(max(0, start), size), os.SEEK_SET)
        data = file.read().decode("utf-8", errors="replace")
    if len(data) > 0 and not data.startswith("\n"):
        first_newline = data.find("\n")
        if first_newline != -1:
            data = data[first_newline + 1 :]
    return _mask_sensitive(data), size


def _humanize_logs(raw_text: str) -> str:
    events: list[str] = []
    json_buffer: list[str] = []
    json_depth = 0

    for line in raw_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if "HTTP Request:" in stripped or "signature=" in stripped:
            continue

        if json_depth > 0 or stripped.startswith("{"):
            json_buffer.append(stripped)
            json_depth += stripped.count("{") + stripped.count("[") - stripped.count("}") - stripped.count("]")
            if json_depth <= 0:
                events.extend(_json_to_events("\n".join(json_buffer)))
                json_buffer = []
                json_depth = 0
            continue

        event = _line_to_event(stripped)
        if event and (not events or events[-1] != event):
            events.append(event)

    if json_buffer:
        events.append("策略输出了一段未完成的结果，等待下一轮日志补全。")

    return "\n".join(events[-300:])


def _line_to_event(line: str) -> str | None:
    message = line
    if " | " in line:
        message = line.split(" | ", 3)[-1]
    if "dashboard | START_BLOCKED" in line:
        return None
    if "dashboard | SAFETY_MODE" in line:
        return None
    if "dashboard | STRATEGY_CONFIG" in line:
        return None
    if "dashboard | START" in line:
        command = line.rsplit("START", 1)[-1].strip()
        return f"已启动策略进程：{command}。"
    if "dashboard | STOP" in line:
        command = line.rsplit("STOP", 1)[-1].strip()
        return f"收到停止请求，正在停止监控进程：{command}。"
    if "dashboard | CLOSE_RESULT" in line:
        return None
    if "Binance order rejected:" in message:
        reason = message.split("Binance order rejected:", 1)[-1].strip()
        return f"交易所拒绝订单：{reason}"
    if "清仓" in message or "平仓" in message or "挂单" in message or "仓位" in message:
        return message
    if "No signal" in message:
        return "本轮没有出现交易信号，继续观察市场。"
    if "Healthcheck ok" in message:
        return "交易所连接正常。"
    if "Account connected" in message:
        return "账户连接正常，余额信息已更新。"
    if "Starting engine" in message:
        return "基础策略引擎已经启动。"
    if "Risk guard stopped" in message or "Execution frozen" in message:
        return f"风控已拦截：{message}"
    if "RuntimeError" in message:
        if "PAPER_TRADING=false, DRY_RUN=false, and EMERGENCY_STOP=false" in message:
            return "策略没有启动：当前仍是安全模式。实盘运行需要 PAPER_TRADING=false、DRY_RUN=false、EMERGENCY_STOP=false。"
        return f"策略运行失败：{message}"
    if "Traceback" in message:
        return None
    if (
        message.startswith("File ")
        or message.startswith("sys.exit")
        or "run_batch(" in message
        or set(message) <= {"^", "~", " "}
    ):
        return None
    return message if not _looks_like_noise(message) else None


def _json_to_events(text: str) -> list[str]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return [text]
    if not isinstance(payload, dict):
        return [json.dumps(payload, ensure_ascii=False)]

    status = payload.get("status")
    events: list[str] = []
    if status == "NO_CANDIDATE":
        events.append("本轮扫描完成：没有找到符合条件的币种。")
    elif status == "NO_ENTRY_FILL":
        attempted = payload.get("attempted_candidates") or []
        symbols = "、".join(str(item.get("symbol")) for item in attempted if isinstance(item, dict))
        events.append(f"本轮尝试建仓但 maker 单未成交。候选：{symbols or '无'}。")
    elif status == "DONE":
        results = payload.get("results") or []
        if isinstance(results, list) and results:
            done = [item for item in results if isinstance(item, dict) and item.get("status") == "DONE"]
            events.append(f"本次并发交易完成：成交平仓 {len(done)}/{len(results)} 个币。")
        else:
            candidate = payload.get("candidate") or {}
            entry = payload.get("entry") or {}
            exit_order = payload.get("exit") or {}
            events.append(
                "交易完成："
                f"{candidate.get('symbol', '-') } {candidate.get('side', '-') }，"
                f"建仓均价 {entry.get('avgPrice', '-')}，"
                f"平仓均价 {exit_order.get('avgPrice', '-')}。"
            )
        if payload.get("round_realized_pnl") is not None or payload.get("batch_realized_pnl") is not None:
            events.append(
                f"收益更新：本轮收益={payload.get('round_realized_pnl', '-')}U，"
                f"本批累计收益={payload.get('batch_realized_pnl', '-')}U。"
            )
    elif status == "OPEN_POSITION_TIMEOUT":
        events.append("仓位管理超时：有币种仍需要关注清仓结果。")
        if payload.get("round_realized_pnl") is not None or payload.get("batch_realized_pnl") is not None:
            events.append(
                f"收益更新：本轮收益={payload.get('round_realized_pnl', '-')}U，"
                f"本批累计收益={payload.get('batch_realized_pnl', '-')}U。"
            )
    elif status == "SUMMARY":
        events.append(
            "批次完成："
            f"共 {payload.get('rounds', '-')} 轮，"
            f"成交 {payload.get('completed', '-')} 轮，"
            f"跳过 {payload.get('skipped', '-')} 轮，"
            f"本批已实现盈亏 {payload.get('batch_realized_pnl', '-')}。"
        )
    elif status == "SKIPPED_SAFE_MODE":
        events.append(str(payload.get("message", "清仓跳过：当前是安全模式。")))
    elif status == "NO_POSITION":
        events.append("停止后的清仓检查完成：没有持仓需要处理。")
    elif status == "FLAT":
        events.append("停止后的 maker 清仓完成：所有持仓已经归零。")
    elif status == "START_BLOCKED_SAFE_MODE":
        blockers = payload.get("blockers") or []
        if isinstance(blockers, list) and blockers:
            events.append("策略没有启动：安全开关仍然打开。需要处理：" + "；".join(str(item) for item in blockers) + "。")
        else:
            events.append("策略没有启动：安全开关仍然打开。")
    elif status == "SAFETY_MODE_ON":
        events.append("安全模式已开启：不会真实下单。")
    elif status == "SAFETY_MODE_OFF":
        events.append("安全模式已关闭：允许实盘策略发单。")
    elif status == "STRATEGY_CONFIG_UPDATED":
        events.append(str(payload.get("message", "策略配置已更新。")))
    elif status:
        events.append(f"策略状态：{status}。")
    elif "available_balance" in payload:
        events.append(
            "账户诊断完成："
            f"可用余额 {payload.get('available_balance', '-')}，"
            f"钱包余额 {payload.get('total_wallet_balance', '-')}，"
            f"风控冻结={payload.get('runtime_recovery_frozen', '-')}。"
        )
    else:
        events.append(json.dumps(payload, ensure_ascii=False))
    return events


def _looks_like_noise(message: str) -> bool:
    noise_markers = ("GET /", "POST /", "DELETE /", "recvWindow=", "orderId=", "newClientOrderId=")
    return any(marker in message for marker in noise_markers)


def _mask_sensitive(text: str) -> str:
    return SIGNATURE_RE.sub(r"\1***", text)


def _read_json_file(path: Path) -> object:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {"error": f"Invalid JSON: {exc}"}


def _read_env_summary() -> dict[str, str]:
    allowed = {
        "APP_ENV",
        "LOG_LEVEL",
        "DRY_RUN",
        "PAPER_TRADING",
        "EMERGENCY_STOP",
        "STRATEGY_NAME",
        "DEFAULT_SYMBOL",
        "POLL_INTERVAL_SECONDS",
        "MAX_NOTIONAL_USDT",
        "MAX_POSITION_NOTIONAL_USDT",
        "MOMENTUM_RUNNER_ROUNDS",
        "MOMENTUM_UNIVERSE_TOP_N",
        "MOMENTUM_QUOTE_NOTIONAL_USDT",
        "MOMENTUM_MAX_CONCURRENT_TRADES",
        "MOMENTUM_ROUND_TAKE_PROFIT_USDT",
        "MOMENTUM_ROUND_STOP_LOSS_USDT",
    }
    env_path = ROOT_DIR / ".env"
    result: dict[str, str] = {}
    if not env_path.exists():
        return _runtime_mode_summary()["effective"]  # type: ignore[return-value]
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key in allowed:
            result[key] = value.strip().strip("\"'")
    for key, value in _runtime_mode_summary()["effective"].items():
        result.setdefault(key, str(value))
    return result


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "TradingBotDashboard/0.1"

    def do_HEAD(self) -> None:
        if urlparse(self.path).path == "/":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send_html(INDEX_HTML)
            return
        if parsed.path == "/api/status":
            command = _strategy_command()
            self._send_json(
                {
                    "process": MANAGER.status(),
                    "strategy": {"command": command, **COMMANDS[command]},
                    "mode": _runtime_mode_summary(),
                    "env": _read_env_summary(),
                    "runtime_state": _read_json_file(RUNTIME_STATE),
                }
            )
            return
        if parsed.path == "/api/logs":
            query = parse_qs(parsed.query)
            source = query.get("source", ["events"])[0]
            max_bytes = int(query.get("bytes", ["80000"])[0])
            offset = int(query["offset"][0]) if "offset" in query else None
            path = DASHBOARD_LOG if source in {"events", "dashboard"} else BOT_LOG
            text, size = _read_tail(path, max_bytes, offset=offset)
            if source == "events":
                text = _humanize_logs(text)
            self._send_json({"source": source, "path": str(path), "text": text, "size": size})
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/api/start":
                self._send_json(MANAGER.start(_strategy_command()))
                return
            if parsed.path == "/api/stop":
                self._send_json(MANAGER.stop())
                return
            if parsed.path == "/api/safety-mode":
                if MANAGER.status().get("running"):
                    raise RuntimeError("策略运行中不能切换安全模式，请先停止策略。")
                payload = self._read_payload()
                self._send_json(_set_safety_mode(bool(payload.get("enabled", True))))
                return
            if parsed.path == "/api/strategy-config":
                if MANAGER.status().get("running"):
                    raise RuntimeError("策略运行中不能修改配置，请先停止策略。")
                self._send_json(_set_strategy_config(self._read_payload()))
                return
            self.send_error(HTTPStatus.NOT_FOUND)
        except Exception as exc:  # noqa: BLE001 - return operational errors to the local dashboard.
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

    def log_message(self, format: str, *args: object) -> None:
        return

    def _read_payload(self) -> dict[str, object]:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        return json.loads(raw)

    def _send_json(self, payload: object, *, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Trading Bot 控制台</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f6f8;
      --panel: #ffffff;
      --panel-soft: #eef2f5;
      --ink: #17212b;
      --muted: #697782;
      --line: #d8dee5;
      --green: #137a52;
      --red: #b42318;
      --amber: #b7791f;
      --blue: #2463a6;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 18px 24px;
      border-bottom: 1px solid var(--line);
      background: var(--panel);
    }
    h1 {
      margin: 0;
      font-size: 20px;
      line-height: 1.2;
    }
    main {
      display: grid;
      grid-template-columns: 340px minmax(0, 1fr);
      gap: 16px;
      padding: 16px;
      max-width: 1440px;
      margin: 0 auto;
    }
    section, aside {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }
    aside {
      padding: 16px;
      align-self: start;
    }
    .stack { display: grid; gap: 14px; }
    .status {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--panel-soft);
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
    }
    .dot {
      width: 9px;
      height: 9px;
      border-radius: 50%;
      background: var(--muted);
    }
    .running .dot { background: var(--green); }
    .stopped .dot { background: var(--red); }
    label {
      display: block;
      margin-bottom: 6px;
      color: var(--muted);
      font-size: 13px;
    }
    select, input, button {
      width: 100%;
      height: 38px;
      border: 1px solid var(--line);
      border-radius: 7px;
      background: #fff;
      color: var(--ink);
      font: inherit;
    }
    button {
      cursor: pointer;
      font-weight: 650;
    }
    button.primary {
      border-color: var(--blue);
      background: var(--blue);
      color: #fff;
    }
    button.danger {
      border-color: var(--red);
      background: var(--red);
      color: #fff;
    }
    button:disabled {
      opacity: .48;
      cursor: not-allowed;
    }
    .grid2 {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .config-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .config-grid label {
      margin-bottom: 5px;
    }
    .meta {
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
    }
    .kv {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      padding: 9px 10px;
      background: var(--panel-soft);
      border-radius: 7px;
      font-size: 13px;
    }
    .kv span:first-child { color: var(--muted); }
    .kv span:last-child {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      text-align: right;
      overflow-wrap: anywhere;
    }
    .viewer {
      min-height: calc(100vh - 112px);
      display: grid;
      grid-template-rows: auto minmax(0, 1fr) auto;
    }
    .toolbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      padding: 12px;
      border-bottom: 1px solid var(--line);
    }
    .toolbar-title {
      font-size: 14px;
      font-weight: 700;
    }
    .small {
      width: auto;
      padding: 0 12px;
      background: var(--panel-soft);
    }
    .log-view {
      margin: 0;
      padding: 14px;
      overflow: auto;
      background: #101418;
      color: #d7e1ea;
      font-size: 12px;
      line-height: 1.55;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .log-line { min-height: 18px; }
    .log-entry { color: #ff6b6b; font-weight: 700; }
    .log-exit { color: #45d483; font-weight: 700; }
    .log-warn { color: #ffd166; }
    .log-muted { color: #8ea0ad; }
    .log-profit { color: #7cff9b; font-weight: 800; background: rgba(69, 212, 131, .12); padding: 1px 4px; border-radius: 4px; }
    .log-loss { color: #ff7b7b; font-weight: 800; background: rgba(255, 107, 107, .12); padding: 1px 4px; border-radius: 4px; }
    .footer {
      padding: 9px 12px;
      color: var(--muted);
      border-top: 1px solid var(--line);
      font-size: 12px;
    }
    .message {
      min-height: 20px;
      color: var(--amber);
      font-size: 13px;
    }
    .switch-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 12px;
      background: var(--panel-soft);
      border-radius: 8px;
    }
    .switch-copy {
      display: grid;
      gap: 2px;
      min-width: 0;
    }
    .switch-title {
      font-size: 14px;
      font-weight: 700;
    }
    .switch-note {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
    }
    .switch {
      position: relative;
      width: 54px;
      height: 30px;
      flex: 0 0 auto;
    }
    .switch input {
      position: absolute;
      inset: 0;
      opacity: 0;
    }
    .slider {
      position: absolute;
      inset: 0;
      border-radius: 999px;
      background: var(--red);
      cursor: pointer;
      transition: background .16s ease;
    }
    .slider::before {
      content: "";
      position: absolute;
      width: 24px;
      height: 24px;
      left: 3px;
      top: 3px;
      border-radius: 50%;
      background: #fff;
      transition: transform .16s ease;
      box-shadow: 0 1px 4px rgba(0,0,0,.22);
    }
    .switch input:checked + .slider {
      background: var(--green);
    }
    .switch input:checked + .slider::before {
      transform: translateX(24px);
    }
    .switch input:disabled + .slider {
      opacity: .5;
      cursor: not-allowed;
    }
    @media (max-width: 900px) {
      header { align-items: flex-start; flex-direction: column; }
      main { grid-template-columns: 1fr; padding: 12px; }
      .viewer { min-height: 70vh; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Trading Bot 控制台</h1>
    <div id="statusBadge" class="status stopped"><span class="dot"></span><span>未运行</span></div>
  </header>
  <main>
    <aside class="stack">
      <div>
        <label>当前策略</label>
        <div class="kv"><span>入口</span><span id="strategyName">加载中</span></div>
      </div>
      <div id="strategyDescription" class="message"></div>
      <div class="switch-row">
        <div class="switch-copy">
          <div class="switch-title">安全模式</div>
          <div id="safetyModeText" class="switch-note">加载中</div>
        </div>
        <label class="switch" title="切换 PAPER_TRADING / DRY_RUN / EMERGENCY_STOP">
          <input id="safetyModeToggle" type="checkbox">
          <span class="slider"></span>
        </label>
      </div>
      <div>
        <label>策略参数</label>
        <div class="config-grid">
          <div>
            <label for="topNInput">动态取前 N</label>
            <input id="topNInput" type="number" min="1" max="30" step="1">
          </div>
          <div>
            <label for="quoteInput">单币投入 U</label>
            <input id="quoteInput" type="number" min="5" max="200" step="1">
          </div>
        </div>
        <button id="saveConfigBtn" class="small" style="width:100%; margin-top:10px;">保存策略参数</button>
      </div>
      <div class="grid2">
        <button id="startBtn" class="primary">启动策略</button>
        <button id="stopBtn" class="danger">停止并清仓</button>
      </div>
      <div id="message" class="message"></div>
      <div class="meta" id="processMeta"></div>
      <div>
        <label>关键配置</label>
        <div class="meta" id="envMeta"></div>
      </div>
      <div>
        <label>运行状态文件</label>
        <div class="meta" id="runtimeMeta"></div>
      </div>
    </aside>
    <section class="viewer">
      <div class="toolbar">
        <div class="toolbar-title">事件日志</div>
        <button id="clearLogBtn" class="small">清空当前显示</button>
      </div>
      <div id="logView" class="log-view">加载中...</div>
      <div class="footer" id="logFooter"></div>
    </section>
  </main>
  <script>
    const strategyName = document.querySelector("#strategyName");
    const strategyDescription = document.querySelector("#strategyDescription");
    const startBtn = document.querySelector("#startBtn");
    const stopBtn = document.querySelector("#stopBtn");
    const clearLogBtn = document.querySelector("#clearLogBtn");
    const safetyModeToggle = document.querySelector("#safetyModeToggle");
    const safetyModeText = document.querySelector("#safetyModeText");
    const topNInput = document.querySelector("#topNInput");
    const quoteInput = document.querySelector("#quoteInput");
    const saveConfigBtn = document.querySelector("#saveConfigBtn");
    const message = document.querySelector("#message");
    const statusBadge = document.querySelector("#statusBadge");
    const processMeta = document.querySelector("#processMeta");
    const envMeta = document.querySelector("#envMeta");
    const runtimeMeta = document.querySelector("#runtimeMeta");
    const logView = document.querySelector("#logView");
    const logFooter = document.querySelector("#logFooter");
    let clearedOffset = 0;
    let latestLogSize = 0;
    let topNInputFocused = false;
    let quoteInputFocused = false;

    topNInput.addEventListener("focus", () => { topNInputFocused = true; });
    topNInput.addEventListener("blur", () => { topNInputFocused = false; });
    quoteInput.addEventListener("focus", () => { quoteInputFocused = true; });
    quoteInput.addEventListener("blur", () => { quoteInputFocused = false; });

    function kv(parent, key, value) {
      const row = document.createElement("div");
      row.className = "kv";
      const k = document.createElement("span");
      const v = document.createElement("span");
      k.textContent = key;
      v.textContent = value ?? "-";
      row.append(k, v);
      parent.append(row);
    }

    function renderStatus(data) {
      const strategy = data.strategy || {};
      const mode = data.mode || {};
      strategyName.textContent = strategy.command || "-";
      strategyDescription.textContent = mode.ready_for_live
        ? strategy.description || ""
        : `当前不会实盘启动：${(mode.blockers || []).join("；")}`;
      const proc = data.process || {};
      safetyModeToggle.checked = !!mode.safety_on;
      safetyModeToggle.disabled = !!proc.running;
      safetyModeText.textContent = mode.safety_on
        ? "已开启：禁止真实下单"
        : "已关闭：允许实盘发单";
      statusBadge.className = `status ${proc.running ? "running" : "stopped"}`;
      statusBadge.lastElementChild.textContent = proc.running
        ? `运行中: ${proc.command} #${proc.pid}`
        : proc.returncode === undefined || proc.returncode === null ? "未运行" : `已退出: ${proc.returncode}`;
      startBtn.disabled = !!proc.running;
      stopBtn.disabled = false;
      saveConfigBtn.disabled = !!proc.running;
      topNInput.disabled = !!proc.running;
      quoteInput.disabled = !!proc.running;
      const env = data.env || {};
      if (!topNInputFocused) topNInput.value = env.MOMENTUM_UNIVERSE_TOP_N || "10";
      if (!quoteInputFocused) quoteInput.value = env.MOMENTUM_QUOTE_NOTIONAL_USDT || "25";

      processMeta.innerHTML = "";
      kv(processMeta, "运行入口", proc.command || strategy.command || "-");
      kv(processMeta, "PID", proc.pid || "-");
      kv(processMeta, "运行秒数", proc.uptime_seconds ?? "-");
      kv(processMeta, "退出码", proc.returncode ?? "-");

      envMeta.innerHTML = "";
      Object.entries(env).forEach(([key, value]) => kv(envMeta, key, value));
      if (!envMeta.children.length) kv(envMeta, ".env", "未读取到关键项");

      runtimeMeta.innerHTML = "";
      const runtime = data.runtime_state || {};
      ["expected_position_qty", "last_exchange_position_qty", "startup_reconciled", "startup_mode", "last_signal_key"].forEach((key) => {
        kv(runtimeMeta, key, runtime[key]);
      });
    }

    async function refreshStatus() {
      const res = await fetch("/api/status");
      renderStatus(await res.json());
    }

    async function refreshLogs() {
      const params = new URLSearchParams({source: "events", bytes: "120000"});
      if (clearedOffset > 0) params.set("offset", String(clearedOffset));
      const res = await fetch(`/api/logs?${params.toString()}`);
      const data = await res.json();
      latestLogSize = data.size || latestLogSize;
      const nearBottom = logView.scrollTop + logView.clientHeight >= logView.scrollHeight - 80;
      renderLogs(data.text || "暂无日志");
      logFooter.textContent = clearedOffset > 0 ? "当前只显示清空之后的新事件" : "已隐藏接口请求，只显示策略事件和清仓进度";
      if (nearBottom) logView.scrollTop = logView.scrollHeight;
    }

    function renderLogs(text) {
      logView.innerHTML = text.split("\n").map((line) => {
        const cls = logClass(line);
        return `<div class="log-line ${cls}">${escapeHtml(line)}</div>`;
      }).join("");
    }

    function logClass(line) {
      if (line.includes("触发止损") || /盈亏=-/.test(line) || /收益=-/.test(line)) return "log-loss";
      if (line.includes("盈利目标") || line.includes("本轮已实现盈亏") || line.includes("本批已实现盈亏") || line.includes("本轮净收益") || line.includes("本批净收益") || line.includes("本轮收益") || line.includes("本批累计收益")) return "log-profit";
      if (line.includes("建仓")) return "log-entry";
      if (line.includes("平仓") || line.includes("清仓")) return "log-exit";
      if (line.includes("失败") || line.includes("拒绝") || line.includes("风控") || line.includes("没有启动")) return "log-warn";
      if (line.includes("暂无") || line.includes("等待价格回到入场区间")) return "log-muted";
      return "";
    }

    function escapeHtml(text) {
      return text
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    }

    async function postJson(url, payload = {}) {
      message.textContent = "";
      const res = await fetch(url, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!res.ok || data.error) message.textContent = data.error || "请求失败";
      await refreshStatus();
      await refreshLogs();
    }

    startBtn.addEventListener("click", () => postJson("/api/start"));
    stopBtn.addEventListener("click", () => postJson("/api/stop"));
    safetyModeToggle.addEventListener("change", () => {
      postJson("/api/safety-mode", {enabled: safetyModeToggle.checked});
    });
    saveConfigBtn.addEventListener("click", () => {
      postJson("/api/strategy-config", {
        top_n: Number(topNInput.value || 10),
        quote_notional: Number(quoteInput.value || 25),
      });
    });
    clearLogBtn.addEventListener("click", () => {
      clearedOffset = latestLogSize;
      logView.innerHTML = "";
      logFooter.textContent = "当前显示已清空，后续只显示新事件";
    });

    refreshStatus();
    refreshLogs();
    setInterval(refreshStatus, 2000);
    setInterval(refreshLogs, 2000);
  </script>
</body>
</html>
"""


def run() -> None:
    host = os.getenv("DASHBOARD_HOST", DEFAULT_HOST)
    port = int(os.getenv("DASHBOARD_PORT", str(DEFAULT_PORT)))
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"Trading Bot 控制台已启动: http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Trading Bot 控制台正在关闭", flush=True)
    finally:
        MANAGER.shutdown()
        server.server_close()


if __name__ == "__main__":
    run()
