from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import logging
import re
import time

import httpx

MAIN_USDC_PROFILE = "main_usdc"
ALT_USDT_PROFILE = "alt_usdt"
ALT_USDT_TOP_N = 10
ALT_USDT_MIN_ABS_CHANGE_PCT = 0.005
MARKET_CAP_TOP_N = 500
COINGECKO_PAGE_SIZE = 250
COINGECKO_CACHE_DURATION = 3600
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]+(USDC|USDT)$")
LOGGER = logging.getLogger(__name__)

_cached_market_cap_assets: tuple[frozenset[str], float] | None = None

MAJOR_USDC_SYMBOLS: tuple[str, ...] = (
    "BTCUSDC",
    "ETHUSDC",
    "SOLUSDC",
    "BNBUSDC",
    "XRPUSDC",
    "DOGEUSDC",
)

MAJOR_USDT_SYMBOLS: tuple[str, ...] = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "DOGEUSDT",
)


@dataclass(frozen=True, slots=True)
class MomentumProfileSpec:
    profile: str
    quote_asset: str
    major_symbols: tuple[str, ...]


PROFILE_SPECS: dict[str, MomentumProfileSpec] = {
    MAIN_USDC_PROFILE: MomentumProfileSpec(
        profile=MAIN_USDC_PROFILE,
        quote_asset="USDC",
        major_symbols=MAJOR_USDC_SYMBOLS,
    ),
    ALT_USDT_PROFILE: MomentumProfileSpec(
        profile=ALT_USDT_PROFILE,
        quote_asset="USDT",
        major_symbols=MAJOR_USDT_SYMBOLS,
    ),
}


def profile_spec(profile: str) -> MomentumProfileSpec:
    try:
        return PROFILE_SPECS[profile]
    except KeyError as exc:
        raise ValueError(f"Unknown momentum profile: {profile}") from exc


def discover_profile_universe(client, profile: str) -> tuple[str, ...]:
    spec = profile_spec(profile)
    response = client.exchange_info_all()
    symbols = response.get("symbols", [])
    available = {
        item["symbol"]
        for item in symbols
        if item.get("quoteAsset") == spec.quote_asset
        and item.get("contractType") == "PERPETUAL"
        and item.get("status") == "TRADING"
        and SYMBOL_PATTERN.fullmatch(item.get("symbol", "")) is not None
    }

    if profile == MAIN_USDC_PROFILE:
        return tuple(symbol for symbol in spec.major_symbols if symbol in available)

    if profile == ALT_USDT_PROFILE:
        major_set = frozenset(spec.major_symbols)
        top_market_cap_assets = _top_market_cap_base_assets()
        tickers = client.ticker_24hr_all()
        ranked = sorted(
            (
                item
                for item in tickers
                if item.get("symbol") in available and item.get("symbol") not in major_set
                and (
                    top_market_cap_assets is None
                    or _base_asset(item.get("symbol", ""), spec.quote_asset) in top_market_cap_assets
                )
                and abs(float(item.get("priceChangePercent", 0.0))) >= ALT_USDT_MIN_ABS_CHANGE_PCT
            ),
            key=lambda item: (
                abs(float(item.get("priceChangePercent", 0.0))),
                float(item.get("quoteVolume", 0.0)),
            ),
            reverse=True,
        )
        if not ranked:
            ranked = sorted(
                (
                    item
                    for item in tickers
                    if item.get("symbol") in available and item.get("symbol") not in major_set
                    and (
                        top_market_cap_assets is None
                        or _base_asset(item.get("symbol", ""), spec.quote_asset) in top_market_cap_assets
                    )
                ),
                key=lambda item: float(item.get("quoteVolume", 0.0)),
                reverse=True,
            )
        top_n = max(1, int(getattr(client.settings, "momentum_universe_top_n", ALT_USDT_TOP_N)))
        return tuple(item["symbol"] for item in ranked[:top_n])

    raise ValueError(f"Unknown momentum profile: {profile}")


def compact_universe(universe: Sequence[str]) -> list[str]:
    return list(universe)


def _base_asset(symbol: str, quote_asset: str) -> str:
    if symbol.endswith(quote_asset):
        return symbol[: -len(quote_asset)]
    return symbol


def _top_market_cap_base_assets() -> frozenset[str] | None:
    global _cached_market_cap_assets

    current_time = time.time()
    if _cached_market_cap_assets is not None:
        cached_assets, cached_time = _cached_market_cap_assets
        if current_time - cached_time < COINGECKO_CACHE_DURATION:
            LOGGER.info("使用缓存的市值排名数据，资产数=%s。", len(cached_assets))
            return cached_assets

    try:
        assets: set[str] = set()
        pages = (MARKET_CAP_TOP_N + COINGECKO_PAGE_SIZE - 1) // COINGECKO_PAGE_SIZE
        with httpx.Client(timeout=10.0) as http_client:
            for page in range(1, pages + 1):
                response = http_client.get(
                    "https://api.coingecko.com/api/v3/coins/markets",
                    params={
                        "vs_currency": "usd",
                        "order": "market_cap_desc",
                        "per_page": COINGECKO_PAGE_SIZE,
                        "page": page,
                        "sparkline": "false",
                    },
                )
                if response.status_code == 429:
                    LOGGER.warning("CoinGecko API 请求被限流，使用 Binance 活跃度排序。")
                    if _cached_market_cap_assets is not None:
                        LOGGER.info("返回过期的缓存数据，资产数=%s。", len(_cached_market_cap_assets[0]))
                        return _cached_market_cap_assets[0]
                    return None
                response.raise_for_status()
                for item in response.json():
                    rank = item.get("market_cap_rank")
                    symbol = str(item.get("symbol", "")).upper()
                    if symbol and isinstance(rank, int) and rank <= MARKET_CAP_TOP_N:
                        assets.add(symbol)
                time.sleep(1.2)
        if not assets:
            raise RuntimeError("CoinGecko returned empty market-cap universe")
        LOGGER.info("已按流通市值前 %s 过滤山寨币，资产数=%s。", MARKET_CAP_TOP_N, len(assets))
        _cached_market_cap_assets = (frozenset(assets), current_time)
        return frozenset(assets)
    except Exception as exc:
        LOGGER.warning("市值前 %s 过滤数据获取失败，将退回 Binance 活跃度排序：%s", MARKET_CAP_TOP_N, exc)
        if _cached_market_cap_assets is not None:
            LOGGER.info("返回过期的缓存数据，资产数=%s。", len(_cached_market_cap_assets[0]))
            return _cached_market_cap_assets[0]
        return None
