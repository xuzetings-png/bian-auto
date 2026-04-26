from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    app_name: str = Field(default="binance-auto-bot", alias="APP_NAME")
    app_env: str = Field(default="local", alias="APP_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    dry_run: bool = Field(default=True, alias="DRY_RUN")
    paper_trading: bool = Field(default=True, alias="PAPER_TRADING")
    log_dir: str = Field(default="logs", alias="LOG_DIR")
    state_dir: str = Field(default="data", alias="STATE_DIR")
    max_cycles: int = Field(default=0, alias="MAX_CYCLES")

    binance_futures_base_url: str = Field(
        default="https://fapi.binance.com",
        alias="BINANCE_FUTURES_BASE_URL",
    )
    binance_proxy_url: str | None = Field(default=None, alias="BINANCE_PROXY_URL")
    binance_api_key: str | None = Field(default=None, alias="BINANCE_API_KEY")
    binance_api_secret: str | None = Field(default=None, alias="BINANCE_API_SECRET")

    default_symbol: str = Field(default="BTCUSDT", alias="DEFAULT_SYMBOL")
    poll_interval_seconds: int = Field(default=10, alias="POLL_INTERVAL_SECONDS")
    healthcheck_interval_seconds: int = Field(
        default=60,
        alias="HEALTHCHECK_INTERVAL_SECONDS",
    )

    strategy_name: str = Field(default="noop", alias="STRATEGY_NAME")
    demo_strategy_interval_ticks: int = Field(
        default=6,
        alias="DEMO_STRATEGY_INTERVAL_TICKS",
    )
    max_notional_usdt: float = Field(default=50.0, alias="MAX_NOTIONAL_USDT")
    max_position_notional_usdt: float = Field(
        default=100.0,
        alias="MAX_POSITION_NOTIONAL_USDT",
    )
    momentum_universe_top_n: int = Field(default=10, alias="MOMENTUM_UNIVERSE_TOP_N")
    momentum_quote_notional_usdt: float = Field(default=25.0, alias="MOMENTUM_QUOTE_NOTIONAL_USDT")
    momentum_max_concurrent_trades: int = Field(default=4, alias="MOMENTUM_MAX_CONCURRENT_TRADES")
    momentum_runner_rounds: int = Field(default=8, alias="MOMENTUM_RUNNER_ROUNDS")
    momentum_round_max_cycles: int = Field(default=0, alias="MOMENTUM_ROUND_MAX_CYCLES")
    momentum_round_take_profit_usdt: float = Field(default=1.0, alias="MOMENTUM_ROUND_TAKE_PROFIT_USDT")
    momentum_round_stop_loss_usdt: float = Field(default=-1.5, alias="MOMENTUM_ROUND_STOP_LOSS_USDT")
    single_position_mode: bool = Field(
        default=True,
        alias="SINGLE_POSITION_MODE",
    )
    signal_dedup_seconds: int = Field(default=30, alias="SIGNAL_DEDUP_SECONDS")
    position_sync_tolerance: float = Field(
        default=0.000001,
        alias="POSITION_SYNC_TOLERANCE",
    )
    startup_position_mode: str = Field(
        default="adopt",
        alias="STARTUP_POSITION_MODE",
    )
    enforce_exchange_rules: bool = Field(
        default=True,
        alias="ENFORCE_EXCHANGE_RULES",
    )
    auto_freeze_on_recovery_error: bool = Field(
        default=True,
        alias="AUTO_FREEZE_ON_RECOVERY_ERROR",
    )
    emergency_stop: bool = Field(default=True, alias="EMERGENCY_STOP")

    @property
    def has_api_credentials(self) -> bool:
        return bool(self.binance_api_key and self.binance_api_secret)

    @property
    def log_dir_path(self) -> Path:
        return Path(self.log_dir)

    @property
    def state_dir_path(self) -> Path:
        return Path(self.state_dir)

    @property
    def paper_state_path(self) -> Path:
        return self.state_dir_path / "paper_state.json"

    @property
    def runtime_state_path(self) -> Path:
        return self.state_dir_path / "runtime_state.json"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
