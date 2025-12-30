"""Configuration management for the Polymarket bot."""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    """Bot configuration loaded from environment variables."""

    # Polygon/Wallet
    polygon_private_key: str = os.getenv("POLYGON_PRIVATE_KEY", "")
    polygon_wallet_address: str = os.getenv("POLYGON_WALLET_ADDRESS", "")

    # Database
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/polybot"
    )
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379")

    # Bot settings
    mode: str = os.getenv("MODE", "paper")
    min_profit_pct: float = float(os.getenv("MIN_PROFIT_PCT", "0.025"))
    max_position_usd: float = float(os.getenv("MAX_POSITION_USD", "50"))
    max_daily_loss: float = float(os.getenv("MAX_DAILY_LOSS", "100"))
    scan_interval_ms: int = int(os.getenv("SCAN_INTERVAL_MS", "500"))

    # Notifications
    discord_webhook_url: str = os.getenv("DISCORD_WEBHOOK_URL", "")
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    # Polymarket API
    clob_host: str = "https://clob.polymarket.com"
    gamma_host: str = "https://gamma-api.polymarket.com"
    ws_host: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    chain_id: int = 137  # Polygon mainnet

    @property
    def is_live(self) -> bool:
        return self.mode.lower() == "live"

    @property
    def has_wallet(self) -> bool:
        return bool(self.polygon_private_key and self.polygon_wallet_address)

    def validate(self) -> list[str]:
        """Validate configuration and return list of errors."""
        errors = []

        if self.is_live and not self.has_wallet:
            errors.append("Live mode requires POLYGON_PRIVATE_KEY and POLYGON_WALLET_ADDRESS")

        if self.min_profit_pct < 0.01:
            errors.append("MIN_PROFIT_PCT should be at least 0.01 (1%)")

        if self.max_position_usd < 1:
            errors.append("MAX_POSITION_USD should be at least $1")

        return errors


# Global config instance
config = Config()
