from __future__ import annotations

import hashlib
import hmac
import logging
import time
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_fixed

from trading_bot.config import Settings
from trading_bot.models import SymbolRules

LOGGER = logging.getLogger(__name__)


class BinanceAuthError(RuntimeError):
    """Raised when Binance rejects the configured API credentials."""


def _is_retryable_exception(exc: BaseException) -> bool:
    if isinstance(exc, BinanceAuthError):
        return False
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in {401, 403}:
        return False
    return True


SIGNED_RETRY = retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    retry=retry_if_exception(_is_retryable_exception),
)


class BinanceFuturesClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = settings.binance_futures_base_url.rstrip("/")
        self.client = httpx.Client(
            base_url=self.base_url,
            timeout=10.0,
            proxy=settings.binance_proxy_url or None,
            headers={
                "X-MBX-APIKEY": settings.binance_api_key or "",
                "User-Agent": settings.app_name,
            },
        )

    def close(self) -> None:
        self.client.close()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def ping(self) -> dict:
        return self.client.get("/fapi/v1/ping").json()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def server_time(self) -> dict:
        response = self.client.get("/fapi/v1/time")
        response.raise_for_status()
        return response.json()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def mark_price(self, symbol: str) -> dict:
        response = self.client.get("/fapi/v1/premiumIndex", params={"symbol": symbol})
        response.raise_for_status()
        return response.json()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def exchange_info(self, symbol: str) -> dict:
        response = self.client.get("/fapi/v1/exchangeInfo", params={"symbol": symbol})
        response.raise_for_status()
        return response.json()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def exchange_info_all(self) -> dict:
        response = self.client.get("/fapi/v1/exchangeInfo")
        response.raise_for_status()
        return response.json()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def ticker_24hr_all(self) -> list[dict]:
        response = self.client.get("/fapi/v1/ticker/24hr")
        response.raise_for_status()
        return response.json()

    def symbol_rules(self, symbol: str) -> SymbolRules:
        payload = self.exchange_info(symbol)
        symbols = payload.get("symbols", [])
        symbol_config = next((item for item in symbols if item.get("symbol") == symbol), None)
        if symbol_config is None:
            raise RuntimeError(f"Binance exchangeInfo returned no symbol config for {symbol}")

        filters = {item["filterType"]: item for item in symbol_config.get("filters", [])}
        price_filter = filters["PRICE_FILTER"]
        lot_size = filters["LOT_SIZE"]
        market_lot_size = filters.get("MARKET_LOT_SIZE", lot_size)
        min_notional = filters.get("MIN_NOTIONAL", {"notional": "0"})
        return SymbolRules(
            symbol=symbol,
            tick_size=Decimal(price_filter["tickSize"]),
            min_price=Decimal(price_filter["minPrice"]),
            min_qty=Decimal(lot_size["minQty"]),
            qty_step=Decimal(lot_size["stepSize"]),
            market_min_qty=Decimal(market_lot_size["minQty"]),
            market_qty_step=Decimal(market_lot_size["stepSize"]),
            min_notional=Decimal(min_notional["notional"]),
        )

    def account_info(self) -> dict:
        response = self._signed_request("GET", "/fapi/v2/account")
        self._raise_for_status(response, "读取账户信息")
        return response.json()

    @SIGNED_RETRY
    def position_risk(self, symbol: str | None = None) -> list[dict]:
        params = {"symbol": symbol} if symbol else None
        response = self._signed_request("GET", "/fapi/v2/positionRisk", params=params)
        self._raise_for_status(response, "读取持仓")
        return response.json()

    @SIGNED_RETRY
    def query_order(
        self,
        *,
        symbol: str,
        order_id: int | None = None,
        client_order_id: str | None = None,
    ) -> dict:
        params: dict[str, str | int] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        if client_order_id is not None:
            params["origClientOrderId"] = client_order_id
        response = self._signed_request("GET", "/fapi/v1/order", params=params)
        self._raise_for_status(response, "查询订单")
        return response.json()

    @SIGNED_RETRY
    def open_orders(self, symbol: str | None = None) -> list[dict]:
        params = {"symbol": symbol} if symbol else None
        response = self._signed_request("GET", "/fapi/v1/openOrders", params=params)
        self._raise_for_status(response, "读取未成交挂单")
        return response.json()

    @SIGNED_RETRY
    def change_leverage(self, symbol: str, leverage: int) -> dict:
        response = self._signed_request(
            "POST",
            "/fapi/v1/leverage",
            params={"symbol": symbol, "leverage": leverage},
        )
        try:
            self._raise_for_status(response, "调整杠杆")
        except httpx.HTTPStatusError:
            LOGGER.warning("Binance leverage change rejected: %s", response.text)
            raise
        return response.json()

    def cancel_order(
        self,
        *,
        symbol: str,
        order_id: int | None = None,
        client_order_id: str | None = None,
    ) -> dict:
        params: dict[str, str | int] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        if client_order_id is not None:
            params["origClientOrderId"] = client_order_id
        response = self._signed_request("DELETE", "/fapi/v1/order", params=params)
        self._raise_for_status(response, "撤销订单")
        return response.json()

    def create_order(
        self,
        *,
        symbol: str,
        side: str,
        quantity: float,
        order_type: str,
        price: float | None = None,
        reduce_only: bool | None = None,
        client_order_id: str | None = None,
        position_side: str | None = None,
        time_in_force: str | None = None,
        extra_params: dict | None = None,
    ) -> dict:
        payload: dict[str, str | float] = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": self._format_decimal(quantity),
        }
        if reduce_only is not None:
            payload["reduceOnly"] = str(reduce_only).lower()
        if client_order_id:
            payload["newClientOrderId"] = client_order_id
        if position_side:
            payload["positionSide"] = position_side
        if price is not None:
            payload["price"] = self._format_decimal(price)
            payload["timeInForce"] = time_in_force or "GTC"
        if extra_params:
            payload.update(extra_params)

        response = self._signed_request("POST", "/fapi/v1/order", params=payload)
        try:
            self._raise_for_status(response, "创建订单")
        except httpx.HTTPStatusError:
            if '"code":-5022' in response.text:
                LOGGER.info("Binance post-only order skipped: %s", response.text)
            else:
                LOGGER.warning("Binance order rejected: %s", response.text)
            raise
        return response.json()

    def _signed_request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
    ) -> httpx.Response:
        if not self.settings.has_api_credentials:
            raise RuntimeError("Signed Binance request requires API key and secret.")

        payload = dict(params or {})
        payload["timestamp"] = int(time.time() * 1000)
        payload["recvWindow"] = 5000

        query_string = urlencode(payload, doseq=True)
        signature = hmac.new(
            self.settings.binance_api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        payload["signature"] = signature

        LOGGER.debug("Signed request %s %s", method, path)
        return self.client.request(method, path, params=payload)

    def _raise_for_status(self, response: httpx.Response, action: str) -> None:
        if response.status_code in {401, 403}:
            detail = _binance_error_text(response)
            raise BinanceAuthError(
                f"{action}失败：Binance 拒绝当前 API 凭证（HTTP {response.status_code}）。"
                f"请检查 API key/secret 是否匹配、是否开启 USD-M Futures 权限、IP 白名单是否允许当前网络。"
                f"交易所返回：{detail}"
            )
        response.raise_for_status()

    @staticmethod
    def _format_decimal(value: float) -> str:
        text = f"{value:.8f}"
        return text.rstrip("0").rstrip(".")


def _binance_error_text(response: httpx.Response) -> str:
    try:
        payload: Any = response.json()
    except ValueError:
        return response.text[:300]
    if isinstance(payload, dict):
        code = payload.get("code", "")
        msg = payload.get("msg", "")
        return f"code={code}, msg={msg}"
    return str(payload)[:300]
