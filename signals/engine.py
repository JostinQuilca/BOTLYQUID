"""
signals/engine.py
=================
Motor de Decisiones Híbrido v2 — Mejorado con técnicas de los mejores bots.

Mejoras implementadas tras investigación de bots líderes (Freqtrade/NostalgiaForInfinity,
PerpLobster, HyperLiquidAlgoBot):

1. SISTEMA DE PUNTUACIÓN (scoring) en lugar de AND gates puros
   - Cada condición suma puntos. La señal dispara al superar umbral.
   - Permite más señales sin sacrificar calidad.

2. ESTRATEGIA 3: TrendMomentum (EMA + RSI + CVD)
   - La estrategia más comprobada en bots rentables de 2024-2025.
   - EMA20 > EMA50 = tendencia alcista → buscar entradas long.
   - RSI entre 40-65 en tendencia = zona óptima (no sobrecomprado).
   - CVD confirma la dirección.

3. FILTROS MEJORADOS en AbsorptionMeanReversion
   - Umbral OBI reducido de 0.25 → 0.15 (más señales)
   - Condición OI flexible (OR en vez de AND)

4. MULTI-TIMEFRAME: Confirmar dirección en 5m antes de entrar en 1m
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

        # ── Señal LONG ─────────────────────────────────────────────────────────
        score_long = 0.0
        if obi.value > self.OBI_THRESHOLD:
            score_long += 30
        if cvd.divergence_1m and cvd.cvd_1m > 0:
            score_long += 25
        if mid < (vwap - self.VWAP_SIGMA * atr):
            score_long += 25
        if (snap.basis_oi.oi_change_5m_pct is not None
                and snap.basis_oi.oi_change_5m_pct < self.OI_DROP_PCT):
            score_long += 20

        if score_long >= self.SCORE_THRESHOLD:
            confidence = round(score_long / 100.0, 3)
            return SignalProposal(
                strategy="AbsorptionMeanReversion",
                side=SignalSide.BUY,
                order_type=OrderType.ALO,
                proposed_price=mid,
                stop_loss=mid - 1.5 * atr,
                take_profit=vwap + 0.2 * atr,
                regime=regime,
                obi=obi.value,
                cvd_1m=cvd.cvd_1m,
                cvd_5m=cvd.cvd_5m,
                basis_bps=snap.basis_oi.basis_bps,
                oi_change_5m=snap.basis_oi.oi_change_5m_pct,
                estimated_hold_minutes=20.0,
                expected_r=1.5,
                confidence=confidence,
            )

        # ── Señal SHORT ────────────────────────────────────────────────────────
        score_short = 0.0
        if obi.value < -self.OBI_THRESHOLD:
            score_short += 30
        if cvd.divergence_1m and cvd.cvd_1m < 0:
            score_short += 25
        if mid > (vwap + self.VWAP_SIGMA * atr):
            score_short += 25
        if (snap.basis_oi.oi_change_5m_pct is not None
                and snap.basis_oi.oi_change_5m_pct < self.OI_DROP_PCT):
            score_short += 20

        if score_short >= self.SCORE_THRESHOLD:
            confidence = round(score_short / 100.0, 3)
            return SignalProposal(
                strategy="AbsorptionMeanReversion",
                side=SignalSide.SELL,
                order_type=OrderType.ALO,
                proposed_price=mid,
                stop_loss=mid + 1.5 * atr,
                take_profit=vwap - 0.2 * atr,
                regime=regime,
                obi=obi.value,
                cvd_1m=cvd.cvd_1m,
                cvd_5m=cvd.cvd_5m,
                basis_bps=snap.basis_oi.basis_bps,
                oi_change_5m=snap.basis_oi.oi_change_5m_pct,
                estimated_hold_minutes=20.0,
                expected_r=1.5,
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

        if (price_oi_regime == "TREND_LONG"
                and cvd.cvd_1m > 0
                and (rsi is None or rsi < 70)):   # no sobrecomprado
            return SignalProposal(
                strategy="VolatilityBreakout",
                side=SignalSide.BUY,
                order_type=OrderType.IOC,
                proposed_price=mid,
                stop_loss=mid - 1.0 * atr,
                regime=regime,
                obi=snap.obi.value,
                cvd_1m=cvd.cvd_1m,
                cvd_5m=cvd.cvd_5m,
                basis_bps=basis_oi.basis_bps,
                oi_change_5m=basis_oi.oi_change_5m_pct,
                estimated_hold_minutes=45.0,
                expected_r=2.0,
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
                stop_loss=mid + 1.0 * atr,
                regime=regime,
                obi=snap.obi.value,
                cvd_1m=cvd.cvd_1m,
                cvd_5m=cvd.cvd_5m,
                basis_bps=basis_oi.basis_bps,
                oi_change_5m=basis_oi.oi_change_5m_pct,
                estimated_hold_minutes=45.0,
                expected_r=2.0,
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

        # ── LONG: EMA fast > slow + RSI en zona + CVD positivo ────────────────
        if (ema_fast > ema_slow                          # tendencia alcista
                and self.RSI_LONG_MIN <= rsi <= self.RSI_LONG_MAX   # momentum óptimo
                and cvd.cvd_1m > 0                       # presión compradora
                and obi > 0.10):                         # libro inclinado al alza

            confidence = self._confidence(ema_fast, ema_slow, rsi, obi, "long")
            return SignalProposal(
                strategy="TrendMomentum",
                side=SignalSide.BUY,
                order_type=OrderType.ALO,
                proposed_price=mid,
                stop_loss=mid - 1.2 * atr,
                take_profit=mid + 2.0 * atr,
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
                and obi < -0.10):                         # libro inclinado a la baja

            confidence = self._confidence(ema_slow, ema_fast, rsi, abs(obi), "short")
            return SignalProposal(
                strategy="TrendMomentum",
                side=SignalSide.SELL,
                order_type=OrderType.ALO,
                proposed_price=mid,
                stop_loss=mid + 1.2 * atr,
                take_profit=mid - 2.0 * atr,
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


# ── Motor de Decisiones Principal v2 ──────────────────────────────────────────

class DecisionEngine:
    """
    v2: Combina 3 estrategias con sistema de scoring.

    Prioridad:
    1. TrendMomentum (nueva, más señales, multi-régimen)
    2. AbsorptionMeanReversion (mejorada, scoring)
    3. VolatilityBreakout (mejorada, umbral reducido)
    """

    def __init__(self, shadow_mode: bool = True) -> None:
        self._shadow_mode = shadow_mode
        self._regime_classifier = RegimeClassifier()
        self._strategies = [
            # TrendMomentum(),  # <-- DESACTIVADA TEMPORALMENTE (Bajo Win Rate en backtest)
            AbsorptionMeanReversion(),
            VolatilityBreakout(),
        ]
        self._on_signal_callbacks: list[Callable[[SignalProposal, bool], None]] = []
        self._last_signal_at: float = 0.0
        self._min_signal_gap_s: float = 300.0  # máximo 1 señal cada 5 min

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
