"""
config/settings.py
==================
Configuración tipada y segura del bot usando pydantic-settings.

Correcciones integradas:
- SecretStr para la clave privada (nunca expuesta en logs/repr)
- Validación estricta: ENV=mainnet requiere MAINNET_CONFIRMATION
- boot_epoch guardado al inicio para idempotencia de cloid
"""
from __future__ import annotations

import os
import time
from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        # Jamás incluir secrets en el repr
        secrets_dir=None,
    )

    # ── Entorno ────────────────────────────────────────────────────────────────
    env: Literal["testnet", "mainnet"] = Field(default="testnet")
    mainnet_confirmation: str | None = Field(default=None)

    # ── Credenciales (NUNCA expuestas) ─────────────────────────────────────────
    hl_agent_private_key: SecretStr = Field(...)
    hl_main_address: str = Field(...)

    # ── Telegram ───────────────────────────────────────────────────────────────
    telegram_bot_token: SecretStr = Field(...)
    telegram_chat_id: int = Field(...)

    # ── Trading ────────────────────────────────────────────────────────────────
    trading_symbol: str = Field(default="BTC")
    risk_per_trade_pct: float = Field(default=0.005, gt=0.0, le=0.02)
    max_leverage: float = Field(default=3.0, gt=0.0, le=5.0)
    daily_loss_limit_pct: float = Field(default=0.03, gt=0.0, le=0.10)
    max_drawdown_pct: float = Field(default=0.10, gt=0.0, le=0.25)
    shadow_mode: bool = Field(default=True)

    # ── URLs por entorno ───────────────────────────────────────────────────────
    @property
    def api_url(self) -> str:
        return (
            "https://api.hyperliquid-testnet.xyz"
            if self.env == "testnet"
            else "https://api.hyperliquid.xyz"
        )

    @property
    def ws_url(self) -> str:
        return (
            "wss://api.hyperliquid-testnet.xyz/ws"
            if self.env == "testnet"
            else "wss://api.hyperliquid.xyz/ws"
        )

    # ── Validaciones ───────────────────────────────────────────────────────────
    @model_validator(mode="after")
    def enforce_mainnet_confirmation(self) -> "Settings":
        """
        REGLA DE SEGURIDAD #3 (RULES.md):
        Si ENV=mainnet, exige confirmación explícita.
        """
        if self.env == "mainnet":
            if self.mainnet_confirmation != "I_ACCEPT_THE_RISK":
                raise RuntimeError(
                    "⛔ ENV=mainnet requiere MAINNET_CONFIRMATION=I_ACCEPT_THE_RISK "
                    "en el archivo .env. Esta es una protección deliberada contra "
                    "ejecución accidental con dinero real."
                )
        return self

    @field_validator("hl_agent_private_key", mode="before")
    @classmethod
    def key_must_not_be_placeholder(cls, v: str) -> str:
        if "your_" in str(v).lower() or str(v) == "":
            raise ValueError(
                "HL_AGENT_PRIVATE_KEY no puede ser el valor placeholder del .env.example"
            )
        return v

    # ── Seguridad: ocultar clave en repr ───────────────────────────────────────
    def __repr__(self) -> str:
        return (
            f"Settings(env={self.env!r}, symbol={self.trading_symbol!r}, "
            f"shadow_mode={self.shadow_mode}, "
            f"hl_agent_private_key=**REDACTED**)"
        )

    def __str__(self) -> str:
        return self.__repr__()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Singleton de configuración. Cacheado para evitar re-lectura del .env.
    Usar siempre esta función, nunca instanciar Settings() directamente.
    """
    return Settings()  # type: ignore[call-arg]


def get_boot_epoch() -> float:
    """
    Retorna el timestamp de inicio del proceso (boot epoch).

    CORRECCIÓN #2 (race condition de cloid):
    Este valor se guarda en SQLite al arrancar y se usa como semilla
    para generar cloids deterministas únicos incluso tras reinicios.
    El valor se lee de la BD si ya existe (reconexión) o se crea nuevo
    (primer arranque).
    """
    boot_epoch_path = os.path.join(os.path.dirname(__file__), "..", ".boot_epoch")
    boot_epoch_path = os.path.abspath(boot_epoch_path)

    if os.path.exists(boot_epoch_path):
        with open(boot_epoch_path) as f:
            return float(f.read().strip())

    epoch = time.time()
    with open(boot_epoch_path, "w") as f:
        f.write(str(epoch))
    return epoch
