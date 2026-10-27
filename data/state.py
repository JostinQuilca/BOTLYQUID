"""
data/state.py
=============
Estado global del mercado en memoria — 100% Hot Path.

Correcciones integradas:
- Flag is_warm para CVD y OBI (Corrección #9)
- Ring buffers (deque) sin bloqueo de disco
- clock_skew_ms y last_tick_age_ms en todo momento
- thread-safe para asyncio (solo un hilo de evento, no se necesita Lock aquí)
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import NamedTuple


# ── Tipos básicos ──────────────────────────────────────────────────────────────

class L2Level(NamedTuple):
    price: float
    size: float


class Trade(NamedTuple):
    timestamp_ms: int    # Timestamp del servidor Hyperliquid
    price: float
    size: float
    side: str            # "B" = compra agresora, "A" = venta agresora


class Candle(NamedTuple):
    timestamp_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    interval: str        # "1m", "5m", etc.


@dataclass
class L2Book:
    """Snapshot actual del libro de órdenes."""
    bids: list[L2Level] = field(default_factory=list)   # Ordenado desc por precio
    asks: list[L2Level] = field(default_factory=list)   # Ordenado asc por precio
    timestamp_ms: int = 0

    @property
    def mid_price(self) -> float | None:
        if self.bids and self.asks:
            return (self.bids[0].price + self.asks[0].price) / 2
        return None

    @property
    def spread_bps(self) -> float | None:
        if self.bids and self.asks and self.bids[0].price > 0:
            return ((self.asks[0].price - self.bids[0].price) / self.bids[0].price) * 10_000
        return None


@dataclass
class AssetContext:
    """Datos de activeAssetCtx de Hyperliquid."""
    mark_price: float = 0.0
    oracle_price: float = 0.0
    funding_rate: float = 0.0       # Por hora (no anualizado)
    open_interest: float = 0.0
    timestamp_ms: int = 0

    @property
    def basis_bps(self) -> float:
        """Prima Mark sobre Oracle en puntos básicos."""
        if self.oracle_price > 0:
            return ((self.mark_price - self.oracle_price) / self.oracle_price) * 10_000
        return 0.0

    @property
    def funding_annualized_pct(self) -> float:
        """Funding anualizado en porcentaje (8760h/año)."""
        return self.funding_rate * 8760 * 100


@dataclass
class AccountState:
    """Estado de la cuenta del usuario."""
    equity: float = 0.0
    available_margin: float = 0.0
    position_size: float = 0.0      # En BTC, positivo=long, negativo=short
    position_entry_price: float = 0.0
    unrealized_pnl: float = 0.0
    margin_used: float = 0.0
    timestamp_ms: int = 0

    @property
    def has_position(self) -> bool:
        return abs(self.position_size) > 1e-8

    @property
    def effective_leverage(self) -> float:
        if self.equity > 0 and self.position_entry_price > 0:
            notional = abs(self.position_size) * self.position_entry_price
            return notional / self.equity
        return 0.0


# ── Estado global del mercado ──────────────────────────────────────────────────

class MarketState:
    """
    Contenedor central del Hot Path.
    Todos los campos en RAM, acceso en microsegundos.
    Los ring buffers usan deque con maxlen para eficiencia O(1).
    """

    MIN_TRADES_FOR_SIGNAL = 50      # Warmup: mínimo de trades para señales válidas
    MIN_CANDLES_FOR_SIGNAL = 20     # Warmup: mínimo de velas de 1m para ATR válido

    def __init__(self) -> None:
        # ── Libro de órdenes ──────────────────────────────────────────────────
        self.l2_book: L2Book = L2Book()

        # ── Contexto del activo ───────────────────────────────────────────────
        self.asset_ctx: AssetContext = AssetContext()

        # ── Ring buffers de trades (últimos 1,000) ────────────────────────────
        self.trades: deque[Trade] = deque(maxlen=1_000)

        # ── Ring buffers de velas 1m (últimas 240 = 4 horas) ─────────────────
        self.candles_1m: deque[Candle] = deque(maxlen=240)

        # ── Velas 5m (últimas 288 = 24 horas) ────────────────────────────────
        self.candles_5m: deque[Candle] = deque(maxlen=288)

        # ── Estado de la cuenta ───────────────────────────────────────────────
        self.account: AccountState = AccountState()

        # ── Métricas de latencia ──────────────────────────────────────────────
        self._last_ws_message_at: float = 0.0     # time.monotonic()
        self._server_ts_last: int = 0             # timestamp del servidor (ms)
        self._local_ts_at_server: float = 0.0     # time.time() cuando llegó ese msg

    # ── Métricas de latencia ───────────────────────────────────────────────────

    def record_message(self, server_timestamp_ms: int) -> None:
        """Llamar en cada mensaje WebSocket recibido."""
        self._last_ws_message_at = time.monotonic()
        self._server_ts_last = server_timestamp_ms
        self._local_ts_at_server = time.time()

    @property
    def last_tick_age_ms(self) -> float:
        """Milisegundos desde el último mensaje WebSocket."""
        if self._last_ws_message_at == 0.0:
            return float("inf")
        return (time.monotonic() - self._last_ws_message_at) * 1000

    @property
    def clock_skew_ms(self) -> float:
        """
        Desfase estimado entre el reloj del servidor y el reloj local.
        Positivo = servidor adelantado respecto al local.
        """
        if self._server_ts_last == 0:
            return 0.0
        local_ms = self._local_ts_at_server * 1000
        return self._server_ts_last - local_ms

    # ── CORRECCIÓN #9: Flag is_warm ───────────────────────────────────────────

    @property
    def is_warm(self) -> bool:
        """
        True cuando hay suficientes datos históricos para que los indicadores
        (CVD, OBI, ATR) sean estadísticamente válidos.
        Bloquea señales durante el período de calentamiento al arrancar.
        """
        return (
            len(self.trades) >= self.MIN_TRADES_FOR_SIGNAL
            and len(self.candles_1m) >= self.MIN_CANDLES_FOR_SIGNAL
        )

    @property
    def warmup_progress(self) -> dict[str, str]:
        """Estado del calentamiento para logs y dashboard."""
        return {
            "trades": f"{len(self.trades)}/{self.MIN_TRADES_FOR_SIGNAL}",
            "candles_1m": f"{len(self.candles_1m)}/{self.MIN_CANDLES_FOR_SIGNAL}",
            "is_warm": str(self.is_warm),
        }

    # ── Acceso rápido a trades por ventana temporal ────────────────────────────

    def trades_last_n_seconds(self, seconds: float) -> list[Trade]:
        """Retorna trades de los últimos N segundos desde el tick más reciente."""
        if not self.trades:
            return []
        cutoff_ms = self.trades[-1].timestamp_ms - int(seconds * 1000)
        return [t for t in self.trades if t.timestamp_ms >= cutoff_ms]

    def trades_last_n_minutes(self, minutes: float) -> list[Trade]:
        return self.trades_last_n_seconds(minutes * 60)

    # ── OI histórico para variación ────────────────────────────────────────────

    def __post_init_oi_history(self) -> None:
        """Historial de OI para calcular variación."""
        self._oi_history: deque[tuple[int, float]] = deque(maxlen=360)  # 1h a 1/tick

    def record_oi_snapshot(self) -> None:
        """Guarda snapshot de OI con timestamp."""
        if not hasattr(self, "_oi_history"):
            self._oi_history: deque[tuple[int, float]] = deque(maxlen=360)
        self._oi_history.append((
            self.asset_ctx.timestamp_ms,
            self.asset_ctx.open_interest,
        ))

    def oi_change_pct(self, window_minutes: float = 5.0) -> float | None:
        """Variación porcentual de OI en los últimos N minutos."""
        if not hasattr(self, "_oi_history") or len(self._oi_history) < 2:
            return None
        now_ms = self.asset_ctx.timestamp_ms
        cutoff_ms = now_ms - int(window_minutes * 60 * 1000)
        past = [oi for ts, oi in self._oi_history if ts <= cutoff_ms]
        if not past:
            return None
        past_oi = past[-1]
        current_oi = self.asset_ctx.open_interest
        if past_oi == 0:
            return None
        return ((current_oi - past_oi) / past_oi) * 100


# Instancia global del Hot Path (singleton de proceso)
market_state = MarketState()
