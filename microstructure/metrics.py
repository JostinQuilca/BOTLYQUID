"""
microstructure/metrics.py
=========================
Calculadores de microestructura en tiempo real.
Todos operan sobre MarketState en RAM. < 2ms por cálculo.

Correcciones integradas:
- CORRECCIÓN #4: OBI por rango de bps fijo (no por niveles)
  Razón: Los primeros N niveles representan rangos de precio muy distintos
  durante alta vs. baja volatilidad. El rango fijo en bps es estable.
- Flag is_warm verificado antes de retornar señales válidas
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import NamedTuple

from data.state import L2Book, MarketState, Trade


# ── Resultados tipados (inmutables) ───────────────────────────────────────────

@dataclass(frozen=True)
class OBIResult:
    value: float                    # Rango [-1.0, +1.0]
    bid_volume: float
    ask_volume: float
    bps_range_used: float
    levels_captured: int
    mid_price: float
    computed_at: float = 0.0        # time.monotonic()

    @property
    def is_bullish(self) -> bool:
        return self.value > 0.35

    @property
    def is_bearish(self) -> bool:
        return self.value < -0.35


@dataclass(frozen=True)
class CVDResult:
    cvd_1m: float
    cvd_5m: float
    cvd_15m: float
    price_change_1m_pct: float
    divergence_1m: bool             # CVD sube pero precio baja (o viceversa)
    divergence_5m: bool
    trades_counted_1m: int
    computed_at: float = 0.0


@dataclass(frozen=True)
class BasisOIResult:
    basis_bps: float                # (mark - oracle) / oracle * 10000
    oi_change_5m_pct: float | None
    oi_change_1h_pct: float | None
    oi_trend: str                   # "UP", "DOWN", "FLAT", "UNKNOWN"
    price_oi_regime: str            # "TREND_LONG", "SHORT_SQUEEZE", "TREND_SHORT", "DISTRIBUTION", "FLAT"
    mark_price: float
    oracle_price: float
    computed_at: float = 0.0


@dataclass(frozen=True)
class VolatilityResult:
    atr_5m: float | None
    atr_1h: float | None
    session_vwap: float | None
    distance_to_vwap_pct: float | None
    current_price: float
    computed_at: float = 0.0

    @property
    def is_compressed(self) -> bool:
        """Volatilidad comprimida: ATR bajo respecto a niveles históricos."""
        if self.atr_5m is None:
            return False
        return self.atr_5m < (self.current_price * 0.0008)  # < 8 bps


# ── Calculadores ───────────────────────────────────────────────────────────────

class OrderBookImbalance:
    """
    CORRECCIÓN #4: OBI calculado por rango de precio fijo en bps.

    Por qué NO usar "primeros N niveles":
    - En baja volatilidad (spread 2 bps), 5 niveles cubre 10 bps del libro.
    - En alta volatilidad (spread 50 bps), 5 niveles cubre 250 bps.
    - El OBI sería incomparable entre regímenes de volatilidad.

    Por qué SÍ usar rango fijo en bps:
    - Siempre evalúa la misma "zona de liquidez" relativa al mid price.
    - Es consistente durante eventos de volatilidad (FOMC, etc.).
    - El umbral de señal (±0.35) tiene el mismo significado en todos los contextos.
    """

    DEFAULT_BPS_RANGE = 15.0   # Zona de ±15 bps alrededor del mid price

    def calculate(
        self,
        book: L2Book,
        bps_range: float = DEFAULT_BPS_RANGE,
    ) -> OBIResult | None:
        """
        Calcula OBI dentro de un rango fijo de bps alrededor del mid price.

        Returns None si el libro está vacío o el mid price es inválido.
        """
        mid = book.mid_price
        if mid is None or mid <= 0:
            return None

        threshold = mid * (bps_range / 10_000)
        lower_bid = mid - threshold    # Límite inferior de la zona bid
        upper_ask = mid + threshold    # Límite superior de la zona ask

        bid_vol = 0.0
        levels_captured = 0

        for price, size in book.bids:
            if price < lower_bid:
                break  # El libro está ordenado desc, ya no hay más en rango
            bid_vol += size
            levels_captured += 1

        ask_vol = 0.0
        for price, size in book.asks:
            if price > upper_ask:
                break  # El libro está ordenado asc
            ask_vol += size
            levels_captured += 1

        total = bid_vol + ask_vol
        if total < 1e-10:
            return None

        obi = (bid_vol - ask_vol) / total

        return OBIResult(
            value=obi,
            bid_volume=bid_vol,
            ask_volume=ask_vol,
            bps_range_used=bps_range,
            levels_captured=levels_captured,
            mid_price=mid,
            computed_at=time.monotonic(),
        )


class RealTimeCVD:
    """
    Cumulative Volume Delta en ventanas rodantes de 1m, 5m y 15m.
    Detecta divergencias: CVD ↑ con precio ↓ = absorción de vendedores.
    """

    DIVERGENCE_THRESHOLD = 0.001   # 0.1% de cambio de precio vs dirección CVD

    def calculate(self, state: MarketState) -> CVDResult | None:
        """Calcula CVD en las tres ventanas temporales."""
        if not state.is_warm:
            return None

        trades_1m = state.trades_last_n_minutes(1.0)
        trades_5m = state.trades_last_n_minutes(5.0)
        trades_15m = state.trades_last_n_minutes(15.0)

        if not trades_1m:
            return None

        cvd_1m = self._cvd(trades_1m)
        cvd_5m = self._cvd(trades_5m)
        cvd_15m = self._cvd(trades_15m)

        # Variación de precio en 1m
        price_now = trades_1m[-1].price
        price_1m_ago = trades_1m[0].price
        price_change_1m_pct = (
            (price_now - price_1m_ago) / price_1m_ago * 100
            if price_1m_ago > 0
            else 0.0
        )

        # Divergencias: precio y CVD van en sentidos opuestos
        divergence_1m = self._detect_divergence(cvd_1m, price_change_1m_pct)

        # Divergencia 5m
        price_5m_ago = trades_5m[0].price if trades_5m else price_now
        price_change_5m_pct = (
            (price_now - price_5m_ago) / price_5m_ago * 100
            if price_5m_ago > 0
            else 0.0
        )
        divergence_5m = self._detect_divergence(cvd_5m, price_change_5m_pct)

        return CVDResult(
            cvd_1m=cvd_1m,
            cvd_5m=cvd_5m,
            cvd_15m=cvd_15m,
            price_change_1m_pct=price_change_1m_pct,
            divergence_1m=divergence_1m,
            divergence_5m=divergence_5m,
            trades_counted_1m=len(trades_1m),
            computed_at=time.monotonic(),
        )

    @staticmethod
    def _cvd(trades: list[Trade]) -> float:
        """
        Suma firmada: compras agresoras (side="B") - ventas agresoras (side="A").
        """
        total = 0.0
        for t in trades:
            if t.side == "B":
                total += t.size
            elif t.side == "A":
                total -= t.size
        return total

    def _detect_divergence(self, cvd: float, price_change_pct: float) -> bool:
        """
        Divergencia alcista: precio baja pero CVD sube (absorción compradora).
        Divergencia bajista: precio sube pero CVD baja (distribución).
        Solo señala si el cambio de precio es significativo.
        """
        if abs(price_change_pct) < self.DIVERGENCE_THRESHOLD * 100:
            return False
        price_bullish = price_change_pct > 0
        cvd_bullish = cvd > 0
        return price_bullish != cvd_bullish


class BasisAndOITracker:
    """
    Diferencial Mark-Oracle (Basis) y tasa de cambio de Open Interest.
    Detecta el régimen precio-OI para clasificar el tipo de movimiento.
    """

    OI_CHANGE_THRESHOLD_PCT = 0.5   # 0.5% en 5m = movimiento significativo

    def calculate(self, state: MarketState) -> BasisOIResult:
        ctx = state.asset_ctx
        oi_5m = state.oi_change_pct(5.0)
        oi_1h = state.oi_change_pct(60.0)

        oi_trend = self._oi_trend(oi_5m)

        # Régimen precio-OI (tabla del plan original)
        price_change = 0.0
        if state.candles_1m:
            last_c = state.candles_1m[-1]
            first_c = state.candles_1m[0]
            if first_c.open > 0:
                price_change = (last_c.close - first_c.open) / first_c.open * 100

        regime = self._price_oi_regime(price_change, oi_5m)

        return BasisOIResult(
            basis_bps=ctx.basis_bps,
            oi_change_5m_pct=oi_5m,
            oi_change_1h_pct=oi_1h,
            oi_trend=oi_trend,
            price_oi_regime=regime,
            mark_price=ctx.mark_price,
            oracle_price=ctx.oracle_price,
            computed_at=time.monotonic(),
        )

    def _oi_trend(self, oi_change_5m: float | None) -> str:
        if oi_change_5m is None:
            return "UNKNOWN"
        if oi_change_5m > self.OI_CHANGE_THRESHOLD_PCT:
            return "UP"
        if oi_change_5m < -self.OI_CHANGE_THRESHOLD_PCT:
            return "DOWN"
        return "FLAT"

    def _price_oi_regime(
        self,
        price_change_pct: float,
        oi_change_5m: float | None,
    ) -> str:
        if oi_change_5m is None:
            return "FLAT"

        price_up = price_change_pct > 0.1
        price_down = price_change_pct < -0.1
        oi_up = oi_change_5m > self.OI_CHANGE_THRESHOLD_PCT
        oi_down = oi_change_5m < -self.OI_CHANGE_THRESHOLD_PCT

        if price_up and oi_up:
            return "TREND_LONG"        # Nuevos largos: tendencia fuerte
        if price_up and oi_down:
            return "SHORT_SQUEEZE"     # Cortos cerrando: agotamiento potencial
        if price_down and oi_up:
            return "TREND_SHORT"       # Nuevos cortos: tendencia bajista
        if price_down and oi_down:
            return "DISTRIBUTION"      # Largos cerrando: debilidad
        return "FLAT"


class VolatilityTracker:
    """
    ATR de 5m y 1h, VWAP de sesión UTC, distancia porcentual al VWAP.
    """

    def calculate(self, state: MarketState) -> VolatilityResult:
        price = state.l2_book.mid_price or state.asset_ctx.mark_price

        atr_5m = self._atr(list(state.candles_1m)[-5:]) if len(state.candles_1m) >= 5 else None
        atr_1h = self._atr(list(state.candles_1m)[-60:]) if len(state.candles_1m) >= 60 else None

        vwap = self._session_vwap(state) if state.candles_1m else None
        dist = None
        if vwap and price and vwap > 0:
            dist = (price - vwap) / vwap * 100

        return VolatilityResult(
            atr_5m=atr_5m,
            atr_1h=atr_1h,
            session_vwap=vwap,
            distance_to_vwap_pct=dist,
            current_price=price or 0.0,
            computed_at=time.monotonic(),
        )

    @staticmethod
    def _atr(candles: list) -> float | None:
        """ATR = media de True Range de N velas."""
        if len(candles) < 2:
            return None
        trs = []
        for i in range(1, len(candles)):
            c = candles[i]
            prev_close = candles[i - 1].close
            tr = max(
                c.high - c.low,
                abs(c.high - prev_close),
                abs(c.low - prev_close),
            )
            trs.append(tr)
        return sum(trs) / len(trs) if trs else None

    @staticmethod
    def _session_vwap(state: MarketState) -> float | None:
        """VWAP de sesión UTC (desde las 00:00 UTC)."""
        import time as _time

        now_s = _time.time()
        # Inicio de sesión UTC (00:00:00)
        from datetime import datetime, timezone
        today_utc = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        session_start_ms = int(today_utc.timestamp() * 1000)

        session_candles = [
            c for c in state.candles_1m
            if c.timestamp_ms >= session_start_ms
        ]

        if not session_candles:
            return None

        sum_pv = sum(
            ((c.high + c.low + c.close) / 3) * c.volume
            for c in session_candles
        )
        sum_v = sum(c.volume for c in session_candles)

        return sum_pv / sum_v if sum_v > 0 else None


# ── Fachada: Snapshot completo de microestructura ─────────────────────────────

@dataclass(frozen=True)
class MicrostructureSnapshot:
    obi: OBIResult | None
    cvd: CVDResult | None
    basis_oi: BasisOIResult
    volatility: VolatilityResult
    is_warm: bool
    computed_at: float


def compute_snapshot(state: MarketState) -> MicrostructureSnapshot:
    """
    Calcula todas las métricas en un solo snapshot.
    Punto de entrada para el Motor de Decisiones.
    Tiempo objetivo: < 2ms.
    """
    obi_calc = OrderBookImbalance()
    cvd_calc = RealTimeCVD()
    basis_calc = BasisAndOITracker()
    vol_calc = VolatilityTracker()

    return MicrostructureSnapshot(
        obi=obi_calc.calculate(state.l2_book),
        cvd=cvd_calc.calculate(state),
        basis_oi=basis_calc.calculate(state),
        volatility=vol_calc.calculate(state),
        is_warm=state.is_warm,
        computed_at=time.monotonic(),
    )
