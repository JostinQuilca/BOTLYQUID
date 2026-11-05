"""
signals/engine.py
=================
Motor de Decisiones v5 — SmartMoneyFVGReversal + TrendMomentumV4 (modo conservador).

DIAGNÓSTICO DE PÉRDIDAS (oct 2026):
- Win rate real: 28.6% (necesitamos 45%+ para ser rentables con R:R 2:1)
- Demasiadas operaciones (14 en 24h) → fees destruyen el capital
- Señales débiles (umbral de scoring muy bajo, 55 pts sobre 100)
- Error Binance 451 (señales externas ciegas)

CAMBIOS v4 (modo conservador):
1. SOLO UNA ESTRATEGIA ACTIVA: TrendMomentum con filtros extremos
   → Las otras 3 estrategias quedan desactivadas hasta backtest confirme
2. UMBRAL DE CONFIANZA: 0.70+ (antes cualquier señal pasaba)
3. TRIPLE CONFIRMACIÓN OBLIGATORIA:
   - EMA alineada (fast > slow en tendencia alcista, viceversa en bajista)
   - RSI en zona óptima (45-58 para longs, 42-55 para shorts)
   - CVD Y OBI confirman juntos (antes era OR)
   - Precio por encima/debajo de EMA lenta (tendencia clara)
4. MÁXIMO 2 TRADES POR DÍA (antes era ilimitado)
5. GAP MÍNIMO ENTRE SEÑALES: 90 minutos (antes eran 10 min)
6. EMA MULTI-TIMEFRAME: Usar EMA 20 y EMA 100 (antes 20 y 50)
   → Tendencia más sólida, menos falsas señales
7. VOLUMEN DE CONFIRMACIÓN: El volumen de la vela actual debe ser > promedio
8. R:R MÍNIMO: 2.0:1 (antes era 1.6:1)
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
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


# ── Constantes de control global ──────────────────────────────────────────────

# Máximo trades por día (seguridad extra: el risk validator también controla)
MAX_TRADES_PER_DAY = 2

# Gap mínimo entre señales: 90 minutos
MIN_SIGNAL_GAP_SECONDS = 5400.0

# Confianza mínima para que una señal pase al ejecutor
MIN_CONFIDENCE = 0.68


# ── Helpers técnicos ───────────────────────────────────────────────────────────

def _ema(prices: list[float], period: int) -> float | None:
    """Calcula EMA exponencial sobre lista de precios de cierre."""
    if len(prices) < period:
        return None
    k = 2.0 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = price * k + ema * (1 - k)
    return ema


def _rsi(prices: list[float], period: int = 14) -> float | None:
    """RSI Wilder. Retorna valor 0-100."""
    if len(prices) < period + 1:
        return None
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
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


def _get_closes(state: MarketState, n: int = 110) -> list[float]:
    """Extrae los últimos N precios de cierre de velas 1m."""
    candles = list(state.candles_1m)[-n:]
    return [c.close for c in candles]


def _atr(candles: list, period: int = 14) -> float | None:
    """ATR real (True Range promedio)."""
    if len(candles) < period + 1:
        return None
    true_ranges = []
    for i in range(1, len(candles)):
        high = candles[i].high
        low = candles[i].low
        prev_close = candles[i - 1].close
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)
    return sum(true_ranges[-period:]) / period


def _is_funding_extreme(funding_rate: float) -> tuple[bool, bool]:
    """
    Clasifica si el funding rate horario es extremo.
    Thresholds HL BTC (funding horario):
    - Muy positivo (FOMO retail): > 0.03%/h → bloquear longs
    - Muy negativo (short squeeze): < -0.015%/h → bloquear shorts
    """
    return (funding_rate > 0.0003, funding_rate < -0.00015)



def _avg_volume(candles: list, lookback: int = 20) -> float:
    """Volumen promedio de las últimas `lookback` velas (excluyendo la actual)."""
    if len(candles) < lookback + 1:
        return 0.0
    vols = [c.volume for c in candles[-(lookback + 1):-1]]
    return sum(vols) / len(vols) if vols else 0.0


# ── Estrategia SmartMoneyFVGReversal (v5) ─────────────────────────────────────

class SmartMoneyFVGReversal:
    """
    SmartMoneyFVGReversal — Estrategia institucional basada en:

    1. Fair Value Gap (FVG / Imbalance ICT): zona de liquidez dejada por
       movimiento institucional rápido (vela 1 y vela 3 no se solapan).
    2. Vela Envolvente (Engulfing) como confirmación de absorción.
    3. Validación de estructura (precio respetando swing high/low previo).
    4. Orderflow en tiempo real: CVD + OBI confirman la dirección.
    5. Bonus de Killzone NY (13:30-17:00 UTC): R:R elevado a 2.5:1.

    HOT PATH SAFE: 100% cálculo en memoria, sin I/O, < 1ms.
    Compatible con RiskValidator v2 (proposed_price = mid_price).

    Win rate esperado (literatura ICT 2024): 42-55% con R:R 2.2-2.5:1
    → Esperanza matemática positiva a partir del 32% WR con R:R 2.2:1.
    """

    MIN_CANDLES = 35
    FVG_SEARCH_WINDOW = 10          # últimas N velas para buscar FVGs
    MIN_FVG_ATR_RATIO = 0.15        # gap debe ser >= 15% del ATR_5m
    OBI_MIN_THRESHOLD = 0.08        # umbral mínimo de OBI

    def evaluate(
        self,
        state: MarketState,
        snap: MicrostructureSnapshot,
        regime: MarketRegime,
    ) -> SignalProposal | None:

        # ── B. Validaciones iniciales ─────────────────────────────────────────
        if snap.obi is None or snap.cvd is None:
            return None
        if len(state.candles_1m) < self.MIN_CANDLES:
            return None
        if snap.volatility.atr_5m is None or snap.volatility.atr_1h is None:
            return None

        atr_5m = snap.volatility.atr_5m
        mid = snap.obi.mid_price
        obi_val = snap.obi.value
        cvd_1m = snap.cvd.cvd_1m

        # ── C.1. Velas recientes ──────────────────────────────────────────────
        candles = list(state.candles_1m)
        c_now = candles[-1]
        c_prev = candles[-2]

        # ── C.2. Estructura previa (swing high/low) ───────────────────────────
        struct_candles = candles[-self.MIN_CANDLES:-self.FVG_SEARCH_WINDOW]
        if not struct_candles:
            return None
        swing_high = max(c.high for c in struct_candles)
        swing_low = min(c.low for c in struct_candles)

        # ── C.3. Escanear FVGs en la ventana reciente ─────────────────────────
        # Ventana: las últimas FVG_SEARCH_WINDOW+2 velas (excluida la actual)
        recent = candles[-(self.FVG_SEARCH_WINDOW + 2):-1]

        bullish_fvg: tuple[float, float] | None = None   # (bottom=c0.high, top=c2.low)
        bearish_fvg: tuple[float, float] | None = None   # (bottom=c2.high, top=c0.low)
        min_gap = atr_5m * self.MIN_FVG_ATR_RATIO

        for i in range(2, len(recent)):
            c0 = recent[i - 2]
            c2 = recent[i]

            # Bullish FVG: c2.low > c0.high (gap hacia arriba)
            gap_bull = c2.low - c0.high
            if gap_bull >= min_gap:
                bullish_fvg = (c0.high, c2.low)   # guardar el último encontrado

            # Bearish FVG: c0.low > c2.high (gap hacia abajo)
            gap_bear = c0.low - c2.high
            if gap_bear >= min_gap:
                bearish_fvg = (c2.high, c0.low)   # guardar el último encontrado

        # ── C.4. Vela Envolvente ──────────────────────────────────────────────
        c_now_body = c_now.close - c_now.open
        c_prev_body = c_prev.close - c_prev.open

        is_bullish_engulfing = (
            c_now.close > c_now.open                            # vela actual verde
            and c_prev.close < c_prev.open                     # vela anterior roja
            and c_now.close >= c_prev.open                     # cierra encima del open anterior
            and c_now_body > abs(c_prev_body)                  # cuerpo mayor
        )
        is_bearish_engulfing = (
            c_now.close < c_now.open                           # vela actual roja
            and c_prev.close > c_prev.open                     # vela anterior verde
            and c_now.close <= c_prev.open                     # cierra debajo del open anterior
            and abs(c_now_body) > c_prev_body                  # cuerpo mayor
        )

        # ── C.5. Killzone Nueva York (13:30 – 17:00 UTC) ─────────────────────
        now_utc = datetime.now(timezone.utc)
        hour_utc = now_utc.hour + now_utc.minute / 60.0
        in_ny_killzone = 13.5 <= hour_utc < 17.0

        expected_r = 2.5 if in_ny_killzone else 2.2
        kz_confidence_bonus = 0.08 if in_ny_killzone else 0.0

        # ── D. Señal LONG ─────────────────────────────────────────────────────
        if bullish_fvg is not None and is_bullish_engulfing:
            fvg_bottom, fvg_top = bullish_fvg

            # Precio tocó/mitigó la zona FVG
            price_in_fvg = c_now.low <= fvg_top and c_now.close >= fvg_bottom

            # Confirmación de estructura: precio respeta el swing high
            recent_10 = candles[-10:]
            structure_ok = max(c.high for c in recent_10) >= swing_high * 0.999

            # Orderflow
            orderflow_ok = cvd_1m > 0 and obi_val > self.OBI_MIN_THRESHOLD

            if price_in_fvg and structure_ok and orderflow_ok:
                # Stop Loss
                sl_price = min(c_now.low, fvg_bottom) - (0.5 * atr_5m)
                if abs(mid - sl_price) < (0.8 * atr_5m):
                    sl_price = mid - (0.8 * atr_5m)
                sl_dist = abs(mid - sl_price)

                # Take Profit
                tp_price = mid + (sl_dist * expected_r)

                confidence = round(
                    min(0.72 + kz_confidence_bonus + min(obi_val / 0.5, 0.15), 0.95),
                    3,
                )

                logger.info(
                    "SmartMoneyFVG LONG: FVG(%.1f-%.1f) engulf=True mid=%.1f "
                    "SL=%.1f TP=%.1f R:R=%.1f KZ=%s conf=%.2f",
                    fvg_bottom, fvg_top, mid, sl_price, tp_price, expected_r,
                    in_ny_killzone, confidence,
                )
                return SignalProposal(
                    strategy="SmartMoneyFVG",
                    side=SignalSide.BUY,
                    order_type=OrderType.ALO,
                    proposed_price=mid,
                    stop_loss=round(sl_price, 2),
                    take_profit=round(tp_price, 2),
                    regime=regime,
                    obi=obi_val,
                    cvd_1m=cvd_1m,
                    cvd_5m=snap.cvd.cvd_5m,
                    basis_bps=snap.basis_oi.basis_bps,
                    oi_change_5m=snap.basis_oi.oi_change_5m_pct,
                    estimated_hold_minutes=25.0,
                    expected_r=expected_r,
                    confidence=confidence,
                )

        # ── D. Señal SHORT ────────────────────────────────────────────────────
        if bearish_fvg is not None and is_bearish_engulfing:
            fvg_bottom, fvg_top = bearish_fvg

            # Precio tocó/mitigó la zona FVG
            price_in_fvg = c_now.high >= fvg_bottom and c_now.close <= fvg_top

            # Confirmación de estructura: precio respeta el swing low
            recent_10 = candles[-10:]
            structure_ok = min(c.low for c in recent_10) <= swing_low * 1.001

            # Orderflow
            orderflow_ok = cvd_1m < 0 and obi_val < -self.OBI_MIN_THRESHOLD

            if price_in_fvg and structure_ok and orderflow_ok:
                # Stop Loss
                sl_price = max(c_now.high, fvg_top) + (0.5 * atr_5m)
                if abs(sl_price - mid) < (0.8 * atr_5m):
                    sl_price = mid + (0.8 * atr_5m)
                sl_dist = abs(sl_price - mid)

                # Take Profit
                tp_price = mid - (sl_dist * expected_r)

                confidence = round(
                    min(0.72 + kz_confidence_bonus + min(abs(obi_val) / 0.5, 0.15), 0.95),
                    3,
                )

                logger.info(
                    "SmartMoneyFVG SHORT: FVG(%.1f-%.1f) engulf=True mid=%.1f "
                    "SL=%.1f TP=%.1f R:R=%.1f KZ=%s conf=%.2f",
                    fvg_bottom, fvg_top, mid, sl_price, tp_price, expected_r,
                    in_ny_killzone, confidence,
                )
                return SignalProposal(
                    strategy="SmartMoneyFVG",
                    side=SignalSide.SELL,
                    order_type=OrderType.ALO,
                    proposed_price=mid,
                    stop_loss=round(sl_price, 2),
                    take_profit=round(tp_price, 2),
                    regime=regime,
                    obi=obi_val,
                    cvd_1m=cvd_1m,
                    cvd_5m=snap.cvd.cvd_5m,
                    basis_bps=snap.basis_oi.basis_bps,
                    oi_change_5m=snap.basis_oi.oi_change_5m_pct,
                    estimated_hold_minutes=25.0,
                    expected_r=expected_r,
                    confidence=confidence,
                )

        return None


# ── Estrategia principal: TrendMomentum v4 (MODO CONSERVADOR) ─────────────────

class TrendMomentumV4:
    """
    Única estrategia activa en v4. Basada en confluencia de múltiples señales.

    CRITERIOS DE ENTRADA (TODOS obligatorios para LONG):
    1. EMA20 > EMA100 (tendencia alcista confirmada en múltiple TF)
    2. Precio actual > EMA100 (no operar en pullbacks profundos)
    3. RSI entre 45-58 (momentum positivo, no sobrecomprado)
    4. CVD positivo (compradores netos en trades recientes)
    5. OBI > 0.15 (libro de órdenes inclinado al alza, con margen)
    6. Volumen actual > 1.2x promedio (confirmación de momentum)
    7. Funding rate no extremo positivo
    8. Confianza calculada > 0.68

    CRITERIOS INVERSOS PARA SHORT.

    R:R objetivo: 2.0:1 (TP = 2x SL)
    SL: 1.2x ATR (14 periodos), mínimo 0.8% del precio
    """

    # Parámetros de tendencia
    EMA_FAST = 20
    EMA_SLOW = 100          # antes era 50, ahora 100 para tendencias más sólidas

    # RSI
    RSI_PERIOD = 14
    RSI_LONG_MIN = 45.0     # no entrar en RSI bajo (sin momentum)
    RSI_LONG_MAX = 58.0     # no entrar sobrecomprado
    RSI_SHORT_MIN = 42.0
    RSI_SHORT_MAX = 55.0

    # OBI (Order Book Imbalance) — umbral más alto que antes
    OBI_THRESHOLD = 0.15    # antes era 0.10

    # Volumen
    VOLUME_MULTIPLIER = 1.2 # volumen debe ser > 1.2x el promedio

    # R:R
    RR_RATIO = 2.0          # antes era 1.8

    # Mínimo de velas para operar
    MIN_CANDLES = 105       # necesita EMA100 + buffer

    def evaluate(
        self,
        state: MarketState,
        snap: MicrostructureSnapshot,
        regime: MarketRegime,
    ) -> SignalProposal | None:
        if snap.obi is None or snap.cvd is None:
            return None
        if len(state.candles_1m) < self.MIN_CANDLES:
            logger.debug(
                "TrendMomentumV4: insuficientes velas %d/%d",
                len(state.candles_1m), self.MIN_CANDLES,
            )
            return None

        # ── Datos básicos ────────────────────────────────────────────────────
        candles = list(state.candles_1m)
        closes = _get_closes(state, self.MIN_CANDLES + 5)

        ema_fast = _ema(closes, self.EMA_FAST)
        ema_slow = _ema(closes, self.EMA_SLOW)
        rsi = _rsi(closes, self.RSI_PERIOD)

        if ema_fast is None or ema_slow is None or rsi is None:
            return None

        mid = snap.obi.mid_price
        obi = snap.obi.value
        cvd = snap.cvd

        # ATR para SL
        atr = _atr(candles, 14) or snap.volatility.atr_1h
        if atr is None:
            return None

        # SL: 1.2x ATR, mínimo 0.8% del precio, máximo 1.5%
        min_sl_dist = mid * 0.008
        sl_dist = max(1.2 * atr, min_sl_dist)
        sl_dist = min(sl_dist, mid * 0.015)
        tp_dist = sl_dist * self.RR_RATIO   # R:R 2:1

        # Volumen actual vs promedio
        recent_volume = candles[-1].volume
        avg_vol = _avg_volume(candles, 20)
        volume_ok = avg_vol > 0 and recent_volume > (self.VOLUME_MULTIPLIER * avg_vol)

        # Funding
        funding = state.asset_ctx.funding_rate if state.asset_ctx else 0.0
        funding_extreme_pos, funding_extreme_neg = _is_funding_extreme(funding)

        # ── LONG: TODOS los criterios deben cumplirse ─────────────────────
        long_conditions = {
            "ema_trend_up":    ema_fast > ema_slow,
            "price_above_ema": mid > ema_slow,
            "rsi_zone":        self.RSI_LONG_MIN <= rsi <= self.RSI_LONG_MAX,
            "cvd_positive":    cvd.cvd_1m > 0,
            "obi_bullish":     obi > self.OBI_THRESHOLD,
            "volume_ok":       volume_ok,
            "funding_ok":      not funding_extreme_pos,
        }
        long_met = all(long_conditions.values())

        if long_met:
            confidence = self._confidence(ema_fast, ema_slow, rsi, obi, cvd.cvd_1m, "long")
            if confidence >= MIN_CONFIDENCE:
                logger.info(
                    "TrendMomentumV4 LONG: EMA(%.1f>%.1f) RSI(%.1f) OBI(%.2f) CVD(%.0f) conf=%.2f",
                    ema_fast, ema_slow, rsi, obi, cvd.cvd_1m, confidence,
                )
                return SignalProposal(
                    strategy="TrendMomentumV4",
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
                    estimated_hold_minutes=45.0,
                    expected_r=self.RR_RATIO,
                    confidence=confidence,
                )
            else:
                logger.debug(
                    "TrendMomentumV4 LONG bloqueado: confianza %.2f < %.2f",
                    confidence, MIN_CONFIDENCE,
                )

        # ── SHORT: TODOS los criterios deben cumplirse ────────────────────
        short_conditions = {
            "ema_trend_down":   ema_fast < ema_slow,
            "price_below_ema":  mid < ema_slow,
            "rsi_zone":         self.RSI_SHORT_MIN <= rsi <= self.RSI_SHORT_MAX,
            "cvd_negative":     cvd.cvd_1m < 0,
            "obi_bearish":      obi < -self.OBI_THRESHOLD,
            "volume_ok":        volume_ok,
            "funding_ok":       not funding_extreme_neg,
        }
        short_met = all(short_conditions.values())

        if short_met:
            confidence = self._confidence(ema_slow, ema_fast, rsi, abs(obi), abs(cvd.cvd_1m), "short")
            if confidence >= MIN_CONFIDENCE:
                logger.info(
                    "TrendMomentumV4 SHORT: EMA(%.1f<%.1f) RSI(%.1f) OBI(%.2f) CVD(%.0f) conf=%.2f",
                    ema_fast, ema_slow, rsi, obi, cvd.cvd_1m, confidence,
                )
                return SignalProposal(
                    strategy="TrendMomentumV4",
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
                    estimated_hold_minutes=45.0,
                    expected_r=self.RR_RATIO,
                    confidence=confidence,
                )
            else:
                logger.debug(
                    "TrendMomentumV4 SHORT bloqueado: confianza %.2f < %.2f",
                    confidence, MIN_CONFIDENCE,
                )

        # Debug: log qué condiciones fallaron
        if not long_met and not short_met:
            long_failed = {k: v for k, v in long_conditions.items() if not v}
            short_failed = {k: v for k, v in short_conditions.items() if not v}
            logger.debug(
                "TrendMomentumV4: sin señal | LONG falló=%s | SHORT falló=%s",
                list(long_failed.keys()), list(short_failed.keys()),
            )

        return None

    def _confidence(
        self,
        ema_fast: float,
        ema_slow: float,
        rsi: float,
        obi: float,
        cvd_strength: float,
        direction: str,
    ) -> float:
        """
        Calcula confianza 0-1.
        Pesos: EMA spread 30%, RSI zona 30%, OBI 25%, CVD 15%.
        """
        # Separación EMA: más spread = tendencia más fuerte
        ema_spread_pct = abs(ema_fast - ema_slow) / max(ema_slow, 1.0) * 100
        ema_score = min(ema_spread_pct / 0.3, 1.0) * 0.30  # 0.3% spread es máximo

        # RSI: más centrado en la zona ideal = mejor
        rsi_ideal = 52.0 if direction == "long" else 48.0
        rsi_score = max(0.0, 1.0 - abs(rsi - rsi_ideal) / 8.0) * 0.30

        # OBI: más fuerte = mejor señal
        obi_score = min(abs(obi) / 0.40, 1.0) * 0.25

        # CVD: normalizado (en BTC, valores típicos 1-50 BTC)
        cvd_score = min(abs(cvd_strength) / 20.0, 1.0) * 0.15

        return round(ema_score + rsi_score + obi_score + cvd_score, 3)


# ── Estrategias desactivadas (PENDIENTES DE BACKTEST) ─────────────────────────
# AbsorptionMeanReversion → win rate demasiado bajo en live
# VolatilityBreakout → muchos falsos breakouts en rango lateral
# StopHuntReversal → pocos datos live para validar


# ── Motor de Decisiones Principal v4 ──────────────────────────────────────────

class DecisionEngine:
    """
    v4 (MODO CONSERVADOR):
    - Solo TrendMomentumV4 activa
    - Máximo 2 trades/día
    - Gap mínimo de 90 min entre señales
    - Umbral de confianza 0.68+

    Las estrategias AbsorptionMR, VolatilityBreakout y StopHuntReversal
    están desactivadas hasta que el backtest confirme que son rentables
    en condiciones de mercado reales.
    """

    def __init__(self, shadow_mode: bool = True) -> None:
        self._shadow_mode = shadow_mode
        self._regime_classifier = RegimeClassifier()
        self._strategies = [
            SmartMoneyFVGReversal(),   # v5: primera en evaluar — FVG institucional
            TrendMomentumV4(),
            # AbsorptionMeanReversion(),  # DESACTIVADO - win rate <30%
            # VolatilityBreakout(),       # DESACTIVADO - muchos falsos breakouts
            # StopHuntReversal(),         # DESACTIVADO - sin datos de backtest
        ]
        self._on_signal_callbacks: list[Callable[[SignalProposal, bool], None]] = []
        self._last_signal_at: float = 0.0
        self._min_signal_gap_s: float = MIN_SIGNAL_GAP_SECONDS  # 90 min

        # Contador de trades del día actual
        self._trades_today: int = 0
        self._trades_date: str = ""

    def register_signal_callback(
        self,
        callback: Callable[[SignalProposal, bool], None],
    ) -> None:
        self._on_signal_callbacks.append(callback)

    def _check_daily_trade_limit(self) -> bool:
        """Retorna True si aún podemos operar hoy."""
        from datetime import date
        today = date.today().isoformat()
        if self._trades_date != today:
            # Nuevo día: reset contador
            self._trades_date = today
            self._trades_today = 0
        return self._trades_today < MAX_TRADES_PER_DAY

    def register_trade_executed(self) -> None:
        """Llamar después de cada trade ejecutado para incrementar el contador diario."""
        self._trades_today += 1
        logger.info(
            "Trade diario registrado: %d/%d permitidos hoy",
            self._trades_today, MAX_TRADES_PER_DAY,
        )

    def evaluate(self, state: MarketState) -> SignalProposal | None:
        if not state.is_warm:
            logger.debug("Not warm yet: %s", state.warmup_progress)
            return None

        # Rate limit: gap mínimo de 90 minutos entre señales
        now = time.monotonic()
        gap_remaining = self._min_signal_gap_s - (now - self._last_signal_at)
        if gap_remaining > 0:
            logger.debug(
                "Rate limit: próxima señal en %.0f min",
                gap_remaining / 60,
            )
            return None

        # Límite diario de trades
        if not self._shadow_mode and not self._check_daily_trade_limit():
            logger.info(
                "Límite diario alcanzado: %d/%d trades ejecutados hoy. Reactivando mañana.",
                self._trades_today, MAX_TRADES_PER_DAY,
            )
            return None

        snap = compute_snapshot(state)
        regime = self._regime_classifier.evaluate(state, snap)

        in_blackout, reason = self._regime_classifier.is_event_blackout()
        if in_blackout:
            if self._shadow_mode:
                logger.info("Shadow mode: blackout (%s) — señal permitida para datos", reason)
            else:
                logger.info("Signal blocked: %s", reason)
                return None

        for strategy in self._strategies:
            signal = strategy.evaluate(state, snap, regime)
            if signal is not None:
                # Verificar confianza mínima (doble verificación)
                if not self._shadow_mode and signal.confidence < MIN_CONFIDENCE:
                    logger.info(
                        "Señal rechazada por confianza baja: %.2f < %.2f",
                        signal.confidence, MIN_CONFIDENCE,
                    )
                    continue

                self._last_signal_at = now
                logger.info(
                    "SEÑAL GENERADA: %s %s @ %.2f | conf=%.2f | SL=%.2f | TP=%.2f | régimen=%s | shadow=%s",
                    signal.strategy,
                    signal.side.value,
                    signal.proposed_price,
                    signal.confidence,
                    signal.stop_loss,
                    signal.take_profit,
                    regime.value,
                    self._shadow_mode,
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
