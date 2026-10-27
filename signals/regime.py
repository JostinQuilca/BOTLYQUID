"""
signals/regime.py
=================
Clasificador de Régimen de Mercado con histéresis.

Correcciones integradas:
- GAP #2: Histéresis: requiere N confirmaciones consecutivas para cambiar régimen.
  Sin esto, en la frontera RANGO/TENDENCIA el régimen oscila cada 5 min.
- Calendario de eventos FOMC/IPC en YAML: bloqueo 30 min antes, 60 min después.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

import yaml

from data.state import MarketState
from microstructure.metrics import MicrostructureSnapshot
from signals.schemas import MarketRegime

logger = logging.getLogger(__name__)

EVENTS_CALENDAR_PATH = Path("config/events_calendar.yaml")

# ── Clasificador ───────────────────────────────────────────────────────────────

class RegimeClassifier:
    """
    Evalúa el régimen de mercado cada 5 minutos.

    GAP #2 — HISTÉRESIS:
    Un cambio de régimen solo se confirma si N evaluaciones consecutivas
    coinciden en el mismo régimen propuesto. Esto evita oscillaciones
    en la frontera entre dos regímenes.
    """

    EVAL_INTERVAL_S = 300.0     # 5 minutos
    CONFIRM_COUNT = 2            # Evaluaciones consecutivas requeridas para cambiar régimen
    EVENT_PRE_BLOCK_MIN = 30     # Bloqueo antes del evento
    EVENT_POST_BLOCK_MIN = 60    # Bloqueo después del evento

    def __init__(self) -> None:
        self._current_regime: MarketRegime = MarketRegime.RANGO
        self._pending_regime: MarketRegime | None = None
        self._pending_count: int = 0
        self._last_eval_at: float = 0.0
        self._events: list[dict[str, Any]] = self._load_events()

    def _load_events(self) -> list[dict[str, Any]]:
        """Carga el calendario de eventos de alto impacto desde YAML."""
        if not EVENTS_CALENDAR_PATH.exists():
            logger.warning("Events calendar not found at %s", EVENTS_CALENDAR_PATH)
            return []
        try:
            with open(EVENTS_CALENDAR_PATH) as f:
                data = yaml.safe_load(f)
            return data.get("events", [])
        except Exception:
            logger.exception("Failed to load events calendar")
            return []

    @property
    def current_regime(self) -> MarketRegime:
        return self._current_regime

    def is_event_blackout(self) -> tuple[bool, str]:
        """
        Retorna (True, motivo) si estamos dentro de la ventana de bloqueo
        de algún evento de alto impacto del calendario.
        """
        now = datetime.now(timezone.utc)
        for event in self._events:
            try:
                event_dt = datetime.fromisoformat(event["datetime_utc"])
                if event_dt.tzinfo is None:
                    event_dt = event_dt.replace(tzinfo=timezone.utc)

                delta_min = (now - event_dt).total_seconds() / 60

                # Ventana de bloqueo: 30 min antes hasta 60 min después
                if -self.EVENT_PRE_BLOCK_MIN <= delta_min <= self.EVENT_POST_BLOCK_MIN:
                    return True, (
                        f"Event blackout: {event.get('name', 'Unknown')} "
                        f"({event_dt.strftime('%Y-%m-%d %H:%M UTC')})"
                    )
            except (KeyError, ValueError):
                continue

        return False, ""

    def evaluate(
        self,
        state: MarketState,
        snapshot: MicrostructureSnapshot,
    ) -> MarketRegime:
        """
        Evalúa y potencialmente actualiza el régimen.
        Llamar periódicamente (cada 5 min), no en cada tick.

        GAP #2 HISTÉRESIS: Solo cambia el régimen si el mismo resultado
        aparece CONFIRM_COUNT veces seguidas.
        """
        now = time.monotonic()

        # Throttle: evaluar máximo cada 5 minutos
        if now - self._last_eval_at < self.EVAL_INTERVAL_S:
            return self._current_regime

        self._last_eval_at = now

        # Chequear si estamos en blackout de evento
        in_blackout, reason = self.is_event_blackout()
        if in_blackout:
            logger.info("Regime → MODO_EVENTO (%s)", reason)
            self._force_regime(MarketRegime.MODO_EVENTO)
            return self._current_regime

        # Clasificar régimen propuesto
        proposed = self._classify(state, snapshot)

        # HISTÉRESIS: acumular confirmaciones
        if proposed == self._pending_regime:
            self._pending_count += 1
        else:
            self._pending_regime = proposed
            self._pending_count = 1

        if self._pending_count >= self.CONFIRM_COUNT:
            if proposed != self._current_regime:
                logger.info(
                    "Regime change: %s → %s (confirmed %dx)",
                    self._current_regime.value,
                    proposed.value,
                    self._pending_count,
                )
                self._current_regime = proposed
            self._pending_regime = None
            self._pending_count = 0
        else:
            logger.debug(
                "Regime pending: %s (%d/%d confirmations)",
                proposed.value,
                self._pending_count,
                self.CONFIRM_COUNT,
            )

        return self._current_regime

    def _force_regime(self, regime: MarketRegime) -> None:
        """Fuerza un cambio de régimen inmediato (usado para eventos)."""
        self._current_regime = regime
        self._pending_regime = None
        self._pending_count = 0

    def _classify(
        self,
        state: MarketState,
        snapshot: MicrostructureSnapshot,
    ) -> MarketRegime:
        """
        Lógica de clasificación determinista basada en métricas.
        """
        vol = snapshot.volatility
        basis_oi = snapshot.basis_oi

        if not state.candles_1m or vol.atr_1h is None:
            return MarketRegime.RANGO   # Default seguro sin suficientes datos

        # Rango de las últimas 4 horas (240 velas de 1m)
        recent_candles = list(state.candles_1m)[-240:]
        highs = [c.high for c in recent_candles]
        lows = [c.low for c in recent_candles]
        price_range = max(highs) - min(lows) if highs else 0
        mid_price = state.l2_book.mid_price or state.asset_ctx.mark_price

        if mid_price <= 0:
            return MarketRegime.RANGO

        range_pct = price_range / mid_price * 100

        # Compresión: rango < 0.5% en 4h y ATR bajo
        if range_pct < 0.5 and vol.is_compressed:
            return MarketRegime.COMPRESION

        # Tendencia: precio fuera de rango 4h Y OI expandiéndose
        oi_expanding = (
            basis_oi.oi_change_5m_pct is not None
            and abs(basis_oi.oi_change_5m_pct) > 0.5
        )
        if range_pct > 2.0 and oi_expanding:
            return MarketRegime.TENDENCIA

        # Default: Rango
        return MarketRegime.RANGO
