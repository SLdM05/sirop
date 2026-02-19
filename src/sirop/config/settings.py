"""
Application settings loaded from the .env file via pydantic-settings.

Import get_settings() anywhere you need configuration. Never call
os.getenv() directly in other modules.

Usage
-----
from sirop.config.settings import get_settings

settings = get_settings()
db_path = settings.data_dir / "my2025tax.sirop"
"""

from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All configurable values resolved from environment / .env file.

    Every field has a safe default so the tool works out-of-the-box
    without a .env file (useful for CI and first-run).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Storage ───────────────────────────────────────────────────────────────

    data_dir: Path = Path("./data")
    output_dir: Path = Path("./output")

    # ── Bitcoin node ──────────────────────────────────────────────────────────

    btc_node_backend: Literal["rpc", "mempool"] = "mempool"
    btc_rpc_url: str = "http://127.0.0.1:8332"
    btc_rpc_user: str = ""
    btc_rpc_password: str = ""
    btc_mempool_url: str = "http://localhost:3006/api"


def get_settings() -> Settings:
    """Return a Settings instance loaded from the environment / .env file."""
    return Settings()
