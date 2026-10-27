"""
data/external_signals.py
========================
Señales externas gratuitas que complementan los indicadores internos del bot.

Fuentes:
- Fear & Greed Index: alternative.me (sin API key)
- Binance Funding Rate: fapi.binance.com (sin API key)

Actualización: cada 15 minutos.
Uso: como filtro/gate antes de aprobar señales del engine interno.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


class SentimentLevel(str, Enum):
    EXTREME_FEAR  = "Extreme Fear"
    FEAR          = "Fear"
    NEUTRAL       = "Neutral"
    GREED         = "Greed"
    EXTREME_GREED = "Extreme Greed"


@dataclass
class ExternalSnapshot:
    """Último snapshot de señales externas."""
    # Fear & Greed
    fng_value: int = 50                          # 0-100
    fng_label: str = "Neutral"
    fng_updated_at: float = 0.0

    # Binance Funding
    binance_funding_8h_pct: float = 0.0          # % por 8h
    binance_funding_annual_pct: float = 0.0      # % anual equiv.
    binance_mark_price: float = 0.0
    binance_updated_at: float = 0.0

    # Flags calculados
    is_extreme_greed: bool = False               # FnG > 75
    is_extreme_fear: bool = False                # FnG < 25
    is_overleveraged_longs: bool = False         # funding > 0.05%/8h
    is_overleveraged_shorts: bool = False        # funding < -0.02%/8h

    @property
    def age_seconds(self) -> float:
        oldest = min(self.fng_updated_at, self.binance_updated_at)
        if oldest == 0.0:
            return float("inf")
        return time.time() - oldest

    @property
    def is_stale(self) -> bool:
        return self.age_seconds > 1800  # > 30 min = stale

    def should_block_long(self) -> tuple[bool, str]:
        """Retorna (bloqueado, motivo) para entradas LONG."""
        if self.is_extreme_greed:
            return True, f"Extreme Greed ({self.fng_value}) — riesgo de corrección"
        if self.is_overleveraged_longs:
            return True, f"Funding {self.binance_funding_8h_pct:.4f}%/8h — longs sobreapalancados"
        return False, ""

    def should_block_short(self) -> tuple[bool, str]:
        """Retorna (bloqueado, motivo) para entradas SHORT."""
        if self.is_extreme_fear:
            return True, f"Extreme Fear ({self.fng_value}) — riesgo de rebote violento"
        if self.is_overleveraged_shorts:
            return True, f"Funding {self.binance_funding_8h_pct:.4f}%/8h — shorts sobreapalancados"
        return False, ""

    def size_multiplier(self) -> float:
        """
        Ajusta el tamaño de la posición según el contexto externo.
        - Neutral / sin extremos → 1.0 (tamaño normal)
        - Fear/Greed moderado   → 0.8 (reducir 20%)
        - Extremo               → 0.5 (reducir 50%)
        """
        if self.is_extreme_greed or self.is_extreme_fear:
            return 0.5
        if self.fng_value < 35 or self.fng_value > 65:
            return 0.8
        return 1.0

    def summary(self) -> str:
        return (
            f"F&G={self.fng_value}({self.fng_label}) | "
            f"BinanceFunding={self.binance_funding_8h_pct:+.4f}%/8h"
        )


class ExternalSignalsFetcher:
    """
    Tarea de fondo que actualiza ExternalSnapshot cada UPDATE_INTERVAL_S.
    Usa httpx.AsyncClient para no bloquear el event loop.
    """

    UPDATE_INTERVAL_S = 900.0   # 15 minutos
    FNG_URL     = "https://api.alternative.me/fng/?limit=1"
    BINANCE_URL = "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT"

    def __init__(self) -> None:
        self.snapshot = ExternalSnapshot()
        self._running = True

    def stop(self) -> None:
        self._running = False

    async def run(self) -> None:
        """Bucle infinito — actualiza snapshot cada 15 minutos."""
        logger.info("ExternalSignalsFetcher started (interval=%ds)", self.UPDATE_INTERVAL_S)
        # Primera actualización inmediata al arrancar
        await self._update()
        while self._running:
            await asyncio.sleep(self.UPDATE_INTERVAL_S)
            await self._update()

    async def _update(self) -> None:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                await asyncio.gather(
                    self._fetch_fng(client),
                    self._fetch_binance_funding(client),
                )
            self._recalculate_flags()
            logger.info(
                "External signals updated: %s",
                self.snapshot.summary(),
            )
        except Exception:
            logger.exception("Failed to update external signals — keeping last values")

    async def _fetch_fng(self, client: httpx.AsyncClient) -> None:
        resp = await client.get(self.FNG_URL)
        resp.raise_for_status()
        data = resp.json()["data"][0]
        self.snapshot.fng_value = int(data["value"])
        self.snapshot.fng_label = data["value_classification"]
        self.snapshot.fng_updated_at = time.time()

    async def _fetch_binance_funding(self, client: httpx.AsyncClient) -> None:
        resp = await client.get(self.BINANCE_URL)
        resp.raise_for_status()
        data = resp.json()
        funding_8h = float(data["lastFundingRate"]) * 100  # a porcentaje
        self.snapshot.binance_funding_8h_pct = funding_8h
        self.snapshot.binance_funding_annual_pct = funding_8h * 3 * 365
        self.snapshot.binance_mark_price = float(data["markPrice"])
        self.snapshot.binance_updated_at = time.time()

    def _recalculate_flags(self) -> None:
        fng = self.snapshot.fng_value
        funding = self.snapshot.binance_funding_8h_pct

        self.snapshot.is_extreme_greed         = fng > 75
        self.snapshot.is_extreme_fear          = fng < 25
        self.snapshot.is_overleveraged_longs   = funding > 0.05   # > 0.05%/8h = ~55%/yr
        self.snapshot.is_overleveraged_shorts  = funding < -0.02  # shorts pagando mucho
