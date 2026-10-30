"""
signals/engine.py
=================
Motor de Decisiones Híbrido v3 — Research-driven improvements.

Mejoras v3 basadas en investigación de microestructura HL (2024-2026):

1. FILTRO DE FUNDING RATE EXTREMO
   - Si funding horario > 0.04%: bloquea longs (mercado sobrecalentado)
   - Si funding horario < -0.02%: bloquea shorts (mercado sobre-shorteado)
   - Señal de convicción adicional cuando funding confirma la dirección

2. FILTRO OI DIVERGENCE TRAP
   - Precio estancado + OI spike agresivo + funding extremo → trampa
   - Bloquea entradas en la dirección de la trampa

3. STOP HUNT REVERSAL (nueva estrategia)
   - Detecta barrido de swing highs/lows seguido de absorción
   - Wick-to-body ratio > 2.0 + CVD invirtiendo = señal de alta precisión
   - R/R objetivo: 2:1

4. BB + RSI optimizado (parámetros 2024)
   - BB: 20 períodos, 1.5σ (más sensible que el estándar 2.0σ)
   - RSI: período 7 (más reactivo para scalping en 1m)

5. SISTEMA DE PUNTUACIÓN (scoring) en lugar de AND gates puros
6. TrendMomentum reactivado con filtros de funding
"""
from __future__ import annotations

import logging
import time
from typing import Callable

from data.state import MarketState
from microstructure.metrics import MicrostructureSnapshot, compute_snapshot
from signals.regime import RegimeClassifier
from signals.schemas import (
    MarketRegime,
    OrderType,
    SignalProposal,
    SignalSide,
)

logger = logging.getLogger(__name__)


# ── Helpers técnicos ───────────────────────────────────────────────────────────

def _ema(prices: list[float], period: int) -> float | None:
    """Calcula EMA simple sobre lista de precios de cierre."""
    if len(prices) < period:
        return None
    k = 2.0 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = price * k + ema * (1 - k)
    return ema


def _rsi(prices: list[float], period: int = 14) -> float | None:
    """RSI Wilder. Retorna valor 0-100.
    FIX #5: Incluye deltas=0 en el promedio para que la división sea siempre
    sobre `period` elementos y no sobre el conteo filtrado.
    """
    if len(prices) < period + 1:
        return None
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    # Usa max() para preservar los ceros y mantener el promedio correcto
    gains = [max(d, 0.0) for d in deltas[-period:]]
    losses = [max(-d, 0.0) for d in deltas[-period:]]
    if not any(gains) and not any(losses):
        return 50.0
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1 + rs))


def _get_closes(state: MarketState, n: int = 60) -> list[float]:
    """Extrae los últimos N precios de cierre de velas 1m."""
    candles = list(state.candles_1m)[-n:]
    return [c.close for c in candles]


def _bollinger_bands(prices: list[float], period: int = 20, num_std: float = 1.5) -> tuple[float, float, float] | None:
    """
    Calcula Bollinger Bands.
    Retorna (lower, middle, upper) o None si no hay datos suficientes.
    Usa 1.5σ por defecto (más sensible que el estándar 2.0σ para scalping en 1m).
    """
    if len(prices) < period:
        return None
    window = prices[-period:]
    middle = sum(window) / period
    variance = sum((p - middle) ** 2 for p in window) / period
    std = variance ** 0.5
    return (middle - num_std * std, middle, middle + num_std * std)


def _is_funding_extreme(funding_rate: float) -> tuple[bool, bool]:
    """
    Clasifica si el funding rate horario es extremo.

    Thresholds HL BTC (funding horario):
    - Muy positivo (FOMO retail/longs sobrecalentados): > 0.04%/h
    - Muy negativo (short squeeze inminente):           < -0.02%/h

    Retorna (is_extreme_positive, is_extreme_negative)
    """
    return (funding_rate > 0.0004, funding_rate < -0.0002)


def _is_oi_divergence_trap(
    price_change_pct: float,
    oi_change_pct: float | None,
    funding_rate: float,
    direction: str,  # "LONG" o "SHORT"
) -> bool:
    """
    Detecta trampa de OI divergence (Trap Divergence).

    Señal de alta precisión:
    - Precio estancado (< 0.1% de movimiento en 5m)
    - OI spike agresivo (> 3% en 5m) → traders atrapados
    - Funding extremo en misma dirección

    Si se detecta trampa LONG: bloquear entradas long.
    Si se detecta trampa SHORT: bloquear entradas short.
    """
    if oi_change_pct is None:
        return False
    price_flat = abs(price_change_pct) < 0.001
    oi_spiked = abs(oi_change_pct) > 0.03
    if not (price_flat and oi_spiked):
        return False
    # Trampa alcista: precio plano + OI spike + funding muy positivo
    if direction == "LONG" and oi_change_pct > 0.03 and funding_rate > 0.0003:
        return True
    # Trampa bajista: precio plano + OI spike + funding muy negativo
    if direction == "SHORT" and oi_change_pct > 0.03 and funding_rate < -0.0001:
        return True
    return False


# ── Estrategia 1: Reversión a la media por absorción ──────────────────────────

class AbsorptionMeanReversion:
    """
    MEJORADA v2: Sistema de puntuación en lugar de AND gates.

    Puntuación máxima: 100 pts. Señal dispara con >= 55 pts.
    - OBI en dirección:      30 pts
    - CVD divergente:        25 pts
    - Precio en extremo:     25 pts
    - OI caída/subida:       20 pts (opcional, no bloquea)

    Régimen: RANGO o COMPRESION
    """

    SCORE_THRESHOLD = 55        # antes era AND de 4 condiciones
    OBI_THRESHOLD = 0.15        # reducido de 0.25 → más señales
    VWAP_SIGMA = 1.2            # reducido de 1.5 → más señales
    OI_DROP_PCT = -0.2          # reducido de -0.3

    def evaluate(
        self,
        state: MarketState,
        snap: MicrostructureSnapshot,
        regime: MarketRegime,
    ) -> SignalProposal | None:
        if regime not in (MarketRegime.RANGO, MarketRegime.COMPRESION):
            return None
        if snap.obi is None or snap.cvd is None:
            return None

        vol = snap.volatility
        obi = snap.obi
        cvd = snap.cvd

        if vol.session_vwap is None or vol.atr_1h is None:
            return None

        mid = obi.mid_price
        vwap = vol.session_vwap
        atr = vol.atr_1h

        # ── Filtro de micro-tendencia (evitar comprar cuchillos cayendo) ───────────
        closes = _get_closes(state, 55)
        ema_fast = _ema(closes, 20)
        ema_slow = _ema(closes, 50)
        rsi = _rsi(closes, 14)

        is_downtrend = (ema_fast is not None and ema_slow is not None and ema_fast < ema_slow and mid < ema_slow)
        is_uptrend = (ema_fast is not None and ema_slow is not None and ema_fast > ema_slow and mid > ema_slow)

        # ── Filtros v3: Funding Rate + OI Divergence Trap ───────────────────────────
        funding = state.asset_ctx.funding_rate if state.asset_ctx else 0.0
        funding_extreme_pos, funding_extreme_neg = _is_funding_extreme(funding)

        oi_change_pct = snap.basis_oi.oi_change_5m_pct
        price_change_5m = 0.0
        if len(closes) >= 6 and closes[-6] != 0:
            price_change_5m = (closes[-1] - closes[-6]) / closes[-6]

        # Distancia de Stop Loss saludable (mínimo 0.8% del precio para evitar ruido)
        min_sl_dist = mid * 0.008   # ~ $640 en BTC a $80k
        sl_dist = max(1.8 * atr, min_sl_dist)
        sl_dist = min(sl_dist, mid * 0.015)  # tope de 1.5%
        tp_dist = sl_dist * 1.6              # R:R de 1.6:1

        # ── Señal LONG ─────────────────────────────────────────────────────────
        # v3: bloquear si funding extremo positivo (mercado sobrecalentado)
        if funding_extreme_pos:
            logger.debug("AbsorptionMR: LONG bloqueado por funding extremo+ve (%.5f%%/h)", funding * 100)
            score_long = 0.0
        elif _is_oi_divergence_trap(price_change_5m, oi_change_pct, funding, "LONG"):
            logger.debug("AbsorptionMR: LONG bloqueado por OI Divergence Trap")
            score_long = 0.0
        elif is_downtrend and (rsi is None or rsi > 35 or not cvd.divergence_1m):
            score_long = 0.0
        else:
            score_long = 0.0
            if obi.value > self.OBI_THRESHOLD:
                score_long += 30
            if cvd.divergence_1m and cvd.cvd_1m > 0:
                score_long += 25
            if mid < (vwap - self.VWAP_SIGMA * atr):
                score_long += 25
            if oi_change_pct is not None and oi_change_pct < self.OI_DROP_PCT:
                score_long += 20
            # Bonus: funding negativo confirma long (cortos pagando)
            if funding_extreme_neg:
                score_long += 10

        if score_long >= self.SCORE_THRESHOLD:
            confidence = round(score_long / 100.0, 3)
            return SignalProposal(
                strategy="AbsorptionMeanReversion",
                side=SignalSide.BUY,
                order_type=OrderType.ALO,
                proposed_price=mid,
                stop_loss=round(mid - sl_dist, 2),
                take_profit=round(mid + tp_dist, 2),
                regime=regime,
                obi=obi.value,
                cvd_1m=cvd.cvd_1m,
                cvd_5m=cvd.cvd_5m,
                basis_bps=snap.basis_oi.basis_bps,
                oi_change_5m=oi_change_pct,
                estimated_hold_minutes=20.0,
                expected_r=1.6,
                confidence=confidence,
            )

        # ── Señal SHORT ────────────────────────────────────────────────────────
        # v3: bloquear si funding extremo negativo (short squeeze inminente)
        if funding_extreme_neg:
            logger.debug("AbsorptionMR: SHORT bloqueado por funding extremo-ve (%.5f%%/h)", funding * 100)
            score_short = 0.0
        elif _is_oi_divergence_trap(price_change_5m, oi_change_pct, funding, "SHORT"):
            logger.debug("AbsorptionMR: SHORT bloqueado por OI Divergence Trap")
            score_short = 0.0
        elif is_uptrend and (rsi is None or rsi < 65 or not cvd.divergence_1m):
            score_short = 0.0
        else:
            score_short = 0.0
            if obi.value < -self.OBI_THRESHOLD:
                score_short += 30
            if cvd.divergence_1m and cvd.cvd_1m < 0:
                score_short += 25
            if mid > (vwap + self.VWAP_SIGMA * atr):
                score_short += 25
            if oi_change_pct is not None and oi_change_pct < self.OI_DROP_PCT:
                score_short += 20
            # Bonus: funding positivo confirma short (longs pagando mucho)
            if funding_extreme_pos:
                score_short += 10

        if score_short >= self.SCORE_THRESHOLD:
            confidence = round(score_short / 100.0, 3)
            return SignalProposal(
                strategy="AbsorptionMeanReversion",
                side=SignalSide.SELL,
                order_type=OrderType.ALO,
                proposed_price=mid,
                stop_loss=round(mid + sl_dist, 2),
                take_profit=round(mid - tp_dist, 2),
                regime=regime,
                obi=obi.value,
                cvd_1m=cvd.cvd_1m,
                cvd_5m=cvd.cvd_5m,
                basis_bps=snap.basis_oi.basis_bps,
                oi_change_5m=oi_change_pct,
                estimated_hold_minutes=20.0,
                expected_r=1.6,
                confidence=confidence,
            )

        return None


# ── Estrategia 2: Ruptura por volatilidad ─────────────────────────────────────

class VolatilityBreakout:
    """
    MEJORADA v2: Umbral de volumen reducido + scoring.

    Cambios:
    - Spike multiplier: 3.0x → 2.0x (más señales en breakouts reales)
    - OI expansion: 0.5% → 0.3%
    - Añadido: RSI confirma la dirección del breakout
    """

    VOLUME_SPIKE_MULTIPLIER = 2.0   # reducido de 3.0
    OI_EXPANSION_PCT = 0.3          # reducido de 0.5
    MIN_CANDLES = 20

    def evaluate(
        self,
        state: MarketState,
        snap: MicrostructureSnapshot,
        regime: MarketRegime,
    ) -> SignalProposal | None:
        if regime != MarketRegime.TENDENCIA:
            return None
        if snap.obi is None or snap.cvd is None:
            return None

        vol = snap.volatility
        basis_oi = snap.basis_oi

        if vol.atr_5m is None or vol.atr_1h is None:
            return None
        if len(state.candles_1m) < self.MIN_CANDLES:
            return None

        oi_expanding = (
            basis_oi.oi_change_5m_pct is not None
            and basis_oi.oi_change_5m_pct > self.OI_EXPANSION_PCT
        )

        recent_vol = state.candles_1m[-1].volume
        avg_vol = sum(c.volume for c in list(state.candles_1m)[-20:-1]) / 19
        volume_spike = avg_vol > 0 and (recent_vol / avg_vol) > self.VOLUME_SPIKE_MULTIPLIER

        # Con scoring: necesita volumen O OI (no ambos obligatoriamente)
        breakout_score = 0
        if volume_spike:
            breakout_score += 50
        if oi_expanding:
            breakout_score += 50

        if breakout_score < 50:
            return None

        # RSI para confirmar dirección
        closes = _get_closes(state, 20)
        rsi = _rsi(closes)

        mid = snap.obi.mid_price
        atr = vol.atr_5m
        cvd = snap.cvd
        price_oi_regime = basis_oi.price_oi_regime

        min_sl_dist = mid * 0.008
        sl_dist = max(1.5 * atr, min_sl_dist)
        sl_dist = min(sl_dist, mid * 0.015)
        tp_dist = sl_dist * 1.8

        if (price_oi_regime == "TREND_LONG"
                and cvd.cvd_1m > 0
                and (rsi is None or rsi < 70)):   # no sobrecomprado
            return SignalProposal(
                strategy="VolatilityBreakout",
                side=SignalSide.BUY,
                order_type=OrderType.IOC,
                proposed_price=mid,
                stop_loss=round(mid - sl_dist, 2),
                take_profit=round(mid + tp_dist, 2),
                regime=regime,
                obi=snap.obi.value,
                cvd_1m=cvd.cvd_1m,
                cvd_5m=cvd.cvd_5m,
                basis_bps=basis_oi.basis_bps,
                oi_change_5m=basis_oi.oi_change_5m_pct,
                estimated_hold_minutes=45.0,
                expected_r=1.8,
                confidence=min(0.9, 0.6 + breakout_score / 200),
            )

        if (price_oi_regime == "TREND_SHORT"
                and cvd.cvd_1m < 0
                and (rsi is None or rsi > 30)):   # no sobrevendido
            return SignalProposal(
                strategy="VolatilityBreakout",
                side=SignalSide.SELL,
                order_type=OrderType.IOC,
                proposed_price=mid,
                stop_loss=round(mid + sl_dist, 2),
                take_profit=round(mid - tp_dist, 2),
                regime=regime,
                obi=snap.obi.value,
                cvd_1m=cvd.cvd_1m,
                cvd_5m=cvd.cvd_5m,
                basis_bps=basis_oi.basis_bps,
                oi_change_5m=basis_oi.oi_change_5m_pct,
                estimated_hold_minutes=45.0,
                expected_r=1.8,
                confidence=min(0.9, 0.6 + breakout_score / 200),
            )

        return None


# ── Estrategia 3: TrendMomentum (NUEVA) ───────────────────────────────────────

class TrendMomentum:
    """
    NUEVA — La estrategia más probada en bots rentables 2024-2025.

    Basada en:
    - NostalgiaForInfinity (Freqtrade top community strategy)
    - HyperLiquidAlgoBot (Bollinger + RSI + ADX)

    Lógica:
    - EMA20 > EMA50 = tendencia alcista → buscar LONG
    - EMA20 < EMA50 = tendencia bajista → buscar SHORT
    - RSI entre 40-60: zona óptima (momentum sin extremos)
    - CVD confirma la dirección (institucionales acumulando)
    - OBI > 0.1 confirma presión en el libro

    Funciona en CUALQUIER régimen (es tendencia-following).
    """

    EMA_FAST = 20
    EMA_SLOW = 50
    RSI_PERIOD = 14
    RSI_LONG_MIN = 40   # RSI mínimo para largo (no sobrevendido extremo)
    RSI_LONG_MAX = 65   # RSI máximo para largo (no sobrecomprado)
    RSI_SHORT_MIN = 35  # RSI mínimo para corto (no sobrevendido)
    RSI_SHORT_MAX = 60  # RSI máximo para corto (no sobrecomprado extremo)
    MIN_CANDLES = 55    # necesita al menos 55 velas para EMA50

    def evaluate(
        self,
        state: MarketState,
        snap: MicrostructureSnapshot,
        regime: MarketRegime,
    ) -> SignalProposal | None:
        if snap.obi is None or snap.cvd is None:
            return None
        if len(state.candles_1m) < self.MIN_CANDLES:
            return None

        closes = _get_closes(state, self.MIN_CANDLES + 5)
        ema_fast = _ema(closes, self.EMA_FAST)
        ema_slow = _ema(closes, self.EMA_SLOW)
        rsi = _rsi(closes, self.RSI_PERIOD)

        if ema_fast is None or ema_slow is None or rsi is None:
            return None

        mid = snap.obi.mid_price
        obi = snap.obi.value
        cvd = snap.cvd

        if snap.volatility.atr_1h is None:
            return None
        atr = snap.volatility.atr_1h

        min_sl_dist = mid * 0.008
        sl_dist = max(1.5 * atr, min_sl_dist)
        sl_dist = min(sl_dist, mid * 0.015)
        tp_dist = sl_dist * 1.8

        funding = state.asset_ctx.funding_rate if state.asset_ctx else 0.0
        funding_extreme_pos, funding_extreme_neg = _is_funding_extreme(funding)

        # ── LONG: EMA fast > slow + RSI en zona + CVD positivo ────────────────
        if (ema_fast > ema_slow                          # tendencia alcista
                and self.RSI_LONG_MIN <= rsi <= self.RSI_LONG_MAX   # momentum óptimo
                and cvd.cvd_1m > 0                       # presión compradora
                and obi > 0.10                           # libro inclinado al alza
                and not funding_extreme_pos):            # no long en FOMO extremo

            confidence = self._confidence(ema_fast, ema_slow, rsi, obi, "long")
            return SignalProposal(
                strategy="TrendMomentum",
                side=SignalSide.BUY,
                order_type=OrderType.ALO,
                proposed_price=mid,
                stop_loss=round(mid - sl_dist, 2),
                take_profit=round(mid + tp_dist, 2),
                regime=regime,
                obi=obi,
                cvd_1m=cvd.cvd_1m,
                cvd_5m=cvd.cvd_5m,
                basis_bps=snap.basis_oi.basis_bps,
                oi_change_5m=snap.basis_oi.oi_change_5m_pct,
                estimated_hold_minutes=30.0,
                expected_r=1.8,
                confidence=confidence,
            )

        # ── SHORT: EMA fast < slow + RSI en zona + CVD negativo ───────────────
        if (ema_fast < ema_slow                           # tendencia bajista
                and self.RSI_SHORT_MIN <= rsi <= self.RSI_SHORT_MAX  # momentum bajista
                and cvd.cvd_1m < 0                        # presión vendedora
                and obi < -0.10                           # libro inclinado a la baja
                and not funding_extreme_neg):             # no short en short-squeeze extremo

            confidence = self._confidence(ema_slow, ema_fast, rsi, abs(obi), "short")
            return SignalProposal(
                strategy="TrendMomentum",
                side=SignalSide.SELL,
                order_type=OrderType.ALO,
                proposed_price=mid,
                stop_loss=round(mid + sl_dist, 2),
                take_profit=round(mid - tp_dist, 2),
                regime=regime,
                obi=obi,
                cvd_1m=cvd.cvd_1m,
                cvd_5m=cvd.cvd_5m,
                basis_bps=snap.basis_oi.basis_bps,
                oi_change_5m=snap.basis_oi.oi_change_5m_pct,
                estimated_hold_minutes=30.0,
                expected_r=1.8,
                confidence=confidence,
            )

        return None

    def _confidence(
        self,
        ema_fast: float,
        ema_slow: float,
        rsi: float,
        obi: float,
        direction: str,
    ) -> float:
        """Calcula confianza 0-1 basada en fuerza de cada señal."""
        # Separación EMA (mayor separación = tendencia más fuerte)
        ema_spread_pct = abs(ema_fast - ema_slow) / ema_slow * 100
        ema_score = min(ema_spread_pct / 0.5, 1.0) * 0.35

        # RSI en zona ideal (45-55 es más neutral, más seguro)
        rsi_center = 52 if direction == "long" else 48
        rsi_score = max(0, 1.0 - abs(rsi - rsi_center) / 15.0) * 0.35

        # OBI fuerte
        obi_score = min(obi / 0.4, 1.0) * 0.30

        return round(ema_score + rsi_score + obi_score, 3)


# ── Estrategia 4: Stop Hunt Reversal (NUEVA - v3) ────────────────────────────

class StopHuntReversal:
    """
    Detecta barridos de stops (stop hunts) seguidos de absorción y reversión.

    Lógica (investigación de microestructura 2024):
    1. Precio barre el swing high/low reciente (ventana de 20 velas 1m)
    2. Vela imprime wick largo (> 2x el cuerpo) = señal de barrido
    3. Precio CIERRA de vuelta dentro del rango previo
    4. CVD invierte en contra de la dirección del barrido (absorción)

    Sesiones óptimas: NY Open (14:30-16:00 UTC), London Open (08:00-10:00 UTC)

    R:R objetivo: 2:1 (stop más allá del extremo del wick)
    Win rate reportado en literatura 2024: 42-50% con R:R 2:1
    """

    PIVOT_WINDOW = 20       # velas 1m para calcular swing high/low
    MIN_WICK_RATIO = 2.0    # wick debe ser > 2x el cuerpo
    MIN_CANDLES = 25        # mínimo de velas para operar

    def evaluate(
        self,
        state: MarketState,
        snap: MicrostructureSnapshot,
        regime: MarketRegime,
    ) -> SignalProposal | None:
        if snap.obi is None or snap.cvd is None:
            return None
        if len(state.candles_1m) < self.MIN_CANDLES:
            return None

        candles = list(state.candles_1m)
        last = candles[-1]        # Vela más reciente (posible barrido)
        prev = candles[-2]        # Vela anterior (referencia)

        # Necesitamos ATR para el stop
        if snap.volatility.atr_1h is None:
            return None

        atr = snap.volatility.atr_1h
        mid = snap.obi.mid_price
        cvd = snap.cvd
        funding = state.asset_ctx.funding_rate if state.asset_ctx else 0.0
        funding_extreme_pos, funding_extreme_neg = _is_funding_extreme(funding)

        # Swing high/low en ventana de PIVOT_WINDOW velas (excluye la última)
        window = candles[-(self.PIVOT_WINDOW + 1):-1]
        if len(window) < 10:
            return None
        swing_high = max(c.high for c in window)
        swing_low = min(c.low for c in window)

        # ── Detectar barrido de HIGHS (precio rompe el swing high y regresa) ─────
        # Condiciones:
        # 1. La vela actual superó el swing high
        # 2. La vela actual CERRÓ por DEBAJO del swing high (regreso dentro del rango)
        # 3. Wick superior es > 2x el cuerpo de la vela (señal de rechazo)
        # 4. CVD negativo (vendedores absorbiendo el barrido)
        # 5. No hay funding extremo positivo (evitar short contra tendencia alcista fuerte)
        upper_body = abs(last.close - last.open)
        upper_wick = last.high - max(last.close, last.open)
        lower_wick = min(last.close, last.open) - last.low
        lower_body = upper_body  # mismo cuerpo, alias

        if (last.high > swing_high                          # baró el high
                and last.close < swing_high                 # cerró dentro
                and upper_body > 0                          # vela no es doji puro
                and upper_wick > self.MIN_WICK_RATIO * upper_body  # wick largo
                and cvd.cvd_1m < 0                         # vendedores dominando
                and not funding_extreme_neg):               # no bloquear short en short-squeeze

            # Stop: más allá del extremo del wick + buffer 0.15%
            sl_price = round(last.high * 1.0015, 2)
            sl_dist = sl_price - mid
            if sl_dist <= 0:
                return None
            tp_dist = sl_dist * 2.0  # R:R 2:1
            tp_price = round(mid - tp_dist, 2)

            # Filtro: sl_dist mínimo de 0.5% para evitar ruido
            if sl_dist / mid < 0.005:
                return None

            confidence = min(0.75, 0.5 + (upper_wick / (upper_body + 0.001)) * 0.05)
            return SignalProposal(
                strategy="StopHuntReversal",
                side=SignalSide.SELL,
                order_type=OrderType.ALO,
                proposed_price=mid,
                stop_loss=sl_price,
                take_profit=tp_price,
                regime=regime,
                obi=snap.obi.value,
                cvd_1m=cvd.cvd_1m,
                cvd_5m=cvd.cvd_5m,
                basis_bps=snap.basis_oi.basis_bps,
                oi_change_5m=snap.basis_oi.oi_change_5m_pct,
                estimated_hold_minutes=15.0,
                expected_r=2.0,
                confidence=round(confidence, 3),
            )

        # ── Detectar barrido de LOWS (precio rompe el swing low y regresa) ──────
        if (last.low < swing_low                            # baró el low
                and last.close > swing_low                  # cerró dentro
                and lower_body > 0                          # vela no es doji puro
                and lower_wick > self.MIN_WICK_RATIO * lower_body  # wick largo
                and cvd.cvd_1m > 0                         # compradores absorbiendo
                and not funding_extreme_pos):               # no bloquear long en FOMO

            # Stop: más allá del extremo del wick - buffer 0.15%
            sl_price = round(last.low * 0.9985, 2)
            sl_dist = mid - sl_price
            if sl_dist <= 0:
                return None
            tp_dist = sl_dist * 2.0  # R:R 2:1
            tp_price = round(mid + tp_dist, 2)

            # Filtro: sl_dist mínimo de 0.5%
            if sl_dist / mid < 0.005:
                return None

            confidence = min(0.75, 0.5 + (lower_wick / (lower_body + 0.001)) * 0.05)
            return SignalProposal(
                strategy="StopHuntReversal",
                side=SignalSide.BUY,
                order_type=OrderType.ALO,
                proposed_price=mid,
                stop_loss=sl_price,
                take_profit=tp_price,
                regime=regime,
                obi=snap.obi.value,
                cvd_1m=cvd.cvd_1m,
                cvd_5m=cvd.cvd_5m,
                basis_bps=snap.basis_oi.basis_bps,
                oi_change_5m=snap.basis_oi.oi_change_5m_pct,
                estimated_hold_minutes=15.0,
                expected_r=2.0,
                confidence=round(confidence, 3),
            )

        return None



# ── Motor de Decisiones Principal v2 ──────────────────────────────────────────

class DecisionEngine:
    """
    v3: Combina 4 estrategias con sistema de scoring y filtros de microestructura.

    Prioridad (primera en disparar gana):
    1. TrendMomentum (reactivado, con filtros de funding rate v3)
    2. AbsorptionMeanReversion (con filtros de funding + OI Trap v3)
    3. VolatilityBreakout (umbral reducido)
    4. StopHuntReversal (NUEVA: barridos de stops con absorción, R:R 2:1)
    """

    def __init__(self, shadow_mode: bool = True) -> None:
        self._shadow_mode = shadow_mode
        self._regime_classifier = RegimeClassifier()
        self._strategies = [
            # TrendMomentum reactivado con filtros de funding v3
            TrendMomentum(),
            AbsorptionMeanReversion(),
            VolatilityBreakout(),
            StopHuntReversal(),
        ]
        self._on_signal_callbacks: list[Callable[[SignalProposal, bool], None]] = []
        self._last_signal_at: float = 0.0
        self._min_signal_gap_s: float = 600.0  # máximo 1 señal cada 10 min

    def register_signal_callback(
        self,
        callback: Callable[[SignalProposal, bool], None],
    ) -> None:
        self._on_signal_callbacks.append(callback)

    def evaluate(self, state: MarketState) -> SignalProposal | None:
        if not state.is_warm:
            logger.debug("Not warm yet: %s", state.warmup_progress)
            return None

        # Rate limit: no más de 1 señal cada 5 minutos
        now = time.monotonic()
        if now - self._last_signal_at < self._min_signal_gap_s:
            return None

        snap = compute_snapshot(state)
        regime = self._regime_classifier.evaluate(state, snap)

        in_blackout, reason = self._regime_classifier.is_event_blackout()
        if in_blackout:
            # FIX #6: En shadow mode NO bloqueamos la señal durante eventos (FOMC);
            # solo la registramos para capturar datos del evento en modo simulación.
            if self._shadow_mode:
                logger.info("Shadow mode: blackout detectado (%s) — señal permitida para captura de datos", reason)
            else:
                logger.info("Signal blocked: %s", reason)
                return None

        for strategy in self._strategies:
            signal = strategy.evaluate(state, snap, regime)
            if signal is not None:
                self._last_signal_at = now
                logger.info(
                    "Signal generated: %s %s @ %.2f (shadow=%s, confidence=%.2f, regime=%s)",
                    signal.strategy,
                    signal.side.value,
                    signal.proposed_price,
                    self._shadow_mode,
                    signal.confidence,
                    regime.value,
                )
                for cb in self._on_signal_callbacks:
                    try:
                        cb(signal, self._shadow_mode)
                    except Exception:
                        logger.exception("Signal callback error")
                return signal

        return None

    @property
    def current_regime(self) -> MarketRegime:
        return self._regime_classifier.current_regime
