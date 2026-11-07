"""
tests/test_smart_money_fvg.py
==============================
Tests de SmartMoneyFVGReversal — estrategia institucional basada en FVG + Engulfing.

Cobertura:
1. Señal LONG válida: FVG alcista + engulfing + estructura + orderflow → SignalProposal
2. Señal SHORT válida: FVG bajista + engulfing + estructura + orderflow → SignalProposal
3. Sin FVG → None
4. FVG presente pero sin engulfing → None
5. FVG presente + engulfing pero orderflow en contra → None
6. FVG demasiado pequeño (< MIN_FVG_ATR_RATIO * atr_5m) → None
7. Integración con RiskValidator en modo live → ApprovedOrder sin VetoException
"""
from __future__ import annotations

import time
from collections import deque
from unittest.mock import MagicMock

import aiosqlite
import pytest

from data.state import (
    AccountState,
    AssetContext,
    Candle,
    L2Book,
    L2Level,
    MarketState,
    Trade,
)
from microstructure.metrics import (
    BasisOIResult,
    CVDResult,
    MicrostructureSnapshot,
    OBIResult,
    VolatilityResult,
)
from risk.validator import RiskValidator, VetoException
from signals.engine import SmartMoneyFVGReversal
from signals.schemas import MarketRegime, OrderType, SignalSide


# ── Helpers de construcción ──────────────────────────────────────────────────

BASE_PRICE = 80_000.0   # BTC a $80k para todos los tests
ATR_5M = 120.0          # $120 de ATR en 5m (realista para BTC)
ATR_1H = 400.0


def _make_candle(
    ts_ms: int,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float = 10.0,
) -> Candle:
    return Candle(
        timestamp_ms=ts_ms,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        interval="1m",
    )


def _make_snap(
    mid: float = BASE_PRICE,
    obi: float = 0.25,
    cvd_1m: float = 15.0,
    cvd_5m: float = 40.0,
    atr_5m: float = ATR_5M,
    atr_1h: float = ATR_1H,
) -> MicrostructureSnapshot:
    """Construye un MicrostructureSnapshot completo con valores realistas."""
    obi_result = OBIResult(
        value=obi,
        bid_volume=1000.0 if obi > 0 else 400.0,
        ask_volume=400.0 if obi > 0 else 1000.0,
        bps_range_used=15.0,
        levels_captured=5,
        mid_price=mid,
    )
    cvd_result = CVDResult(
        cvd_1m=cvd_1m,
        cvd_5m=cvd_5m,
        cvd_15m=cvd_5m * 2,
        price_change_1m_pct=0.05 if cvd_1m > 0 else -0.05,
        divergence_1m=False,
        divergence_5m=False,
        trades_counted_1m=50,
    )
    vol_result = VolatilityResult(
        atr_5m=atr_5m,
        atr_1h=atr_1h,
        session_vwap=mid,
        distance_to_vwap_pct=0.1,
        current_price=mid,
    )
    basis_result = BasisOIResult(
        basis_bps=2.0,
        oi_change_5m_pct=0.5,
        oi_change_1h_pct=1.5,
        oi_trend="UP",
        price_oi_regime="TREND_LONG",
        mark_price=mid,
        oracle_price=mid * 0.9998,
    )
    return MicrostructureSnapshot(
        obi=obi_result,
        cvd=cvd_result,
        volatility=vol_result,
        basis_oi=basis_result,
        is_warm=True,
        computed_at=time.monotonic(),
    )


def _make_state_with_candles(candles: list[Candle], equity: float = 50.0) -> MarketState:
    """
    Construye un MarketState completo con las velas dadas,
    libro L2, contexto del activo y cuenta.
    """
    state = MarketState()
    state.candles_1m = deque(candles, maxlen=500)

    mid = BASE_PRICE
    state.l2_book = L2Book(
        bids=[L2Level(mid - 2.0, 5.0), L2Level(mid - 5.0, 8.0)],
        asks=[L2Level(mid + 2.0, 2.0), L2Level(mid + 5.0, 3.0)],
        timestamp_ms=int(time.time() * 1000),
    )
    state.asset_ctx = AssetContext(
        mark_price=mid,
        oracle_price=mid * 0.9999,
        funding_rate=0.000005,  # neutral, no extremo
        open_interest=45_000.0,
        timestamp_ms=int(time.time() * 1000),
    )
    state.account = AccountState(
        equity=equity,
        available_margin=equity * 0.95,
        position_size=0.0,
        position_entry_price=0.0,
        unrealized_pnl=0.0,
    )
    state._last_ws_message_at = time.monotonic()
    return state


def _build_bullish_fvg_scenario() -> tuple[list[Candle], MicrostructureSnapshot]:
    """
    Construye 40 velas donde:
    - velas 0-24 (struct_candles=candles[-35:-10]): estructura con swing high claro
    - velas 25-27 (dentro de FVG_SEARCH_WINDOW=candles[-12:-1]): se forma un Bullish FVG
    - velas 28-37: retroceso al FVG
    - vela 38 (c_prev): vela roja que entra en el FVG
    - vela 39 (c_now): vela verde envolvente alcista que sale del FVG

    Con 40 velas:
      - struct_candles = candles[-35:-10] = candles[5:30] → incluye velas 5-29
      - FVG window = candles[-(10+2):-1] = candles[-12:-1] = candles[28:39]
      - c_prev = candles[-2] = candle[38], c_now = candles[-1] = candle[39]

    swing_high se calcula de candles[5:30], que incluye vela 20 con high = p+300.
    La confirmación de estructura max(candles[-10:]) = max(candles[30:40]).
    Ponemos una vela en pos 32 con high = p+299 para pasar el check >= swing_high*0.999.
    """
    candles: list[Candle] = []
    t = 1_700_000_000_000  # ms timestamp base
    step = 60_000           # 1 minuto en ms

    p = BASE_PRICE  # precio actual

    # --- Velas 0-4: relleno neutro ---
    for i in range(5):
        candles.append(_make_candle(t + i * step, p, p + 10, p - 10, p + 2))

    # --- Velas 5-29: estructura base con swing high visible ---
    # swing_high = max high de candles[5:30]
    # Ponemos swing_high claro en vela 20: high = p + 300
    for i in range(5, 30):
        if i == 20:
            hi = p + 300.0   # <--- swing_high = p + 300
            lo = p - 20.0
            o = p - 10.0
            c = p + 250.0
        else:
            hi = p + 30.0
            lo = p - 20.0
            o = p + 5.0
            c = p + 10.0
        candles.append(_make_candle(t + i * step, o, hi, lo, c))

    # --- Velas 28-29 están dentro de range 5-30 → ya añadidas arriba ---
    # Ahora necesitamos el FVG en la ventana candles[-12:-1] = candles[28:39]
    # Pero candles[28] y [29] ya fueron añadidos en el loop anterior.
    # El FVG debe estar en i = 2..len(recent)-1 donde recent = candles[28:39]
    # Vamos a poner el FVG en posiciones 30, 31, 32 (c0=30, c1=31, c2=32)

    # --- Vela 30 (c0 del FVG): impulso alcista fuerte ---
    c0_high = p + 60.0
    c0_low = p - 10.0
    c0_open = p - 5.0
    c0_close = p + 55.0
    candles.append(_make_candle(t + 30 * step, c0_open, c0_high, c0_low, c0_close, volume=20.0))

    # --- Vela 31 (c1 del FVG): vela de en medio ---
    candles.append(_make_candle(t + 31 * step, c0_close, c0_close + 30, c0_close - 5, c0_close + 20.0))

    # --- Vela 32 (c2 del FVG): impulso alcista que deja el gap ---
    # FVG bullish: c2.low - c0.high >= ATR_5M * 0.15 → gap mínimo = 0.15 * 120 = 18
    # Usamos 0.25 * ATR = 30 para holgura
    c2_low = c0_high + ATR_5M * 0.25   # p + 60 + 30 = p + 90 → gap = 30 >= 18 OK
    c2_open = c2_low + 5.0
    c2_high = c2_low + 80.0
    c2_close = c2_low + 60.0
    # IMPORTANTE: c2.high también debe alcanzar swing_high * 0.999 para la confirmación de estructura
    # swing_high = p + 300. Necesitamos max(candles[-10:]) >= p + 300 * 0.999 = p + 299.7
    # Ponemos el high de c2 en p + 300 para satisfacer la condición
    c2_high = p + 300.0  # coincide con swing_high → estructura OK
    candles.append(_make_candle(t + 32 * step, c2_open, c2_high, c2_low, c2_close, volume=25.0))

    # --- Velas 33-37: retroceso al FVG ---
    fvg_bottom = c0_high       # p + 60
    fvg_top = c2_low           # p + 90
    for i in range(33, 38):
        pullback_close = fvg_bottom + (fvg_top - fvg_bottom) * 0.5  # mitad del FVG
        candles.append(_make_candle(
            t + i * step,
            pullback_close + 10,
            pullback_close + 20,
            pullback_close - 10,
            pullback_close,
        ))

    # --- Vela 38 (c_prev): vela ROJA que entra en el FVG ---
    # c_prev: open > close, toca fondo del FVG
    c_prev_open = fvg_top + 5.0        # abre en techo del FVG
    c_prev_close = fvg_bottom + 2.0    # cierra cerca del fondo del FVG (roja)
    c_prev_high = c_prev_open + 5.0
    c_prev_low = fvg_bottom - 5.0
    candles.append(_make_candle(
        t + 38 * step,
        c_prev_open,
        c_prev_high,
        c_prev_low,
        c_prev_close,
        volume=15.0,
    ))

    # --- Vela 39 (c_now): vela VERDE ENVOLVENTE alcista ---
    # Condiciones engulfing:
    #   c_now.close > c_now.open  (verde)
    #   c_prev.close < c_prev.open  (roja, ya cumplida)
    #   c_now.close >= c_prev.open  (envuelve por arriba)
    #   (c_now.close - c_now.open) > abs(c_prev.close - c_prev.open)  (cuerpo mayor)
    c_prev_body = abs(c_prev_close - c_prev_open)   # = fvg_top + 5 - (fvg_bottom + 2) = 93-62 = ~33
    c_now_open = c_prev_close - 2.0                 # abre debajo del close de la anterior
    c_now_close = c_prev_open + c_prev_body * 1.5   # cierra bien encima del open anterior → envuelve
    c_now_high = c_now_close + 15.0
    c_now_low = c_now_open - 10.0
    # Verificación: c_now.low <= fvg_top (toca/mitiga el FVG) AND c_now.close >= fvg_bottom
    # c_now_low = c_now_open - 10 = (c_prev_close - 2) - 10 = fvg_bottom + 2 - 2 - 10 = fvg_bottom - 10
    # → c_now.low < fvg_top  OK
    # c_now_close >> fvg_bottom  OK
    candles.append(_make_candle(
        t + 39 * step,
        c_now_open,
        c_now_high,
        c_now_low,
        c_now_close,
        volume=30.0,  # volumen alto = confirmación
    ))

    # Snap con OBI positivo, CVD positivo, mid = BASE_PRICE
    snap = _make_snap(
        mid=BASE_PRICE,
        obi=0.25,
        cvd_1m=18.0,
    )
    return candles, snap


def _build_bearish_fvg_scenario() -> tuple[list[Candle], MicrostructureSnapshot]:
    """
    Construye 40 velas con un Bearish FVG y vela envolvente bajista.

    Con 40 velas:
      struct_candles = candles[-35:-10] = candles[5:30]
      swing_low = min(c.low for c in struct_candles) = p - 300 (vela 20)
      FVG window = candles[-12:-1] = candles[28:39]
      El FVG se forma en velas 30(c0), 31(c1), 32(c2).
      La confirmación de estructura: min(candles[-10:]) = min(candles[30:40]) <= swing_low * 1.001
    """
    candles: list[Candle] = []
    t = 1_700_000_000_000
    step = 60_000
    p = BASE_PRICE

    # --- Velas 0-4: relleno neutro ---
    for i in range(5):
        candles.append(_make_candle(t + i * step, p, p + 10, p - 10, p - 2))

    # --- Velas 5-29: estructura con swing LOW claro en vela 20 ---
    for i in range(5, 30):
        if i == 20:
            hi = p + 20.0
            lo = p - 300.0  # <--- swing_low = p - 300
            o = p + 10.0
            c = p - 250.0
        else:
            hi = p + 20.0
            lo = p - 30.0
            o = p - 5.0
            c = p - 10.0
        candles.append(_make_candle(t + i * step, o, hi, lo, c))

    # --- Velas 30-32: Bearish FVG ---
    # c0.low - c2.high >= ATR_5M * 0.15 → gap hacia abajo
    c0_high = p + 15.0
    c0_low = p - 60.0
    c0_open = p + 10.0
    c0_close = p - 55.0  # vela bajista fuerte
    candles.append(_make_candle(t + 30 * step, c0_open, c0_high, c0_low, c0_close, volume=20.0))

    # c1 (vela de en medio)
    candles.append(_make_candle(t + 31 * step, c0_close, c0_close + 5, c0_close - 30, c0_close - 20))

    # c2: c0.low - c2.high >= ATR*0.25 (holgura)
    # FVG bearish = (c2.high, c0.low) → bottom=c2.high, top=c0.low
    c2_high = c0_low - ATR_5M * 0.25   # p - 60 - 30 = p - 90
    c2_open = c2_high - 5.0
    # c2.low debe alcanzar swing_low * 1.001 para confirmación de estructura
    # swing_low = p - 300. Necesitamos min(candles[-10:]) <= (p-300)*1.001
    # Ponemos c2.low = p - 300 exacto
    c2_low = p - 300.0
    c2_close = c2_high - 60.0
    candles.append(_make_candle(t + 32 * step, c2_open, c2_high, c2_low, c2_close, volume=25.0))

    # --- Velas 33-37: retroceso al FVG (precio sube de vuelta) ---
    fvg_bottom = c2_high    # límite inferior del gap = c2.high
    fvg_top = c0_low        # límite superior del gap = c0.low
    for i in range(33, 38):
        bounce_close = fvg_bottom + (fvg_top - fvg_bottom) * 0.5
        candles.append(_make_candle(
            t + i * step,
            bounce_close - 10,
            bounce_close + 10,
            bounce_close - 20,
            bounce_close,
        ))

    # --- Vela 38 (c_prev): vela VERDE que entra en el FVG ---
    # c_prev debe ser alcista (open < close) y tocar el FVG
    c_prev_open = fvg_bottom - 10.0
    c_prev_close = fvg_top - 2.0   # cierra cerca del techo del FVG (verde)
    c_prev_high = c_prev_close + 5.0
    c_prev_low = c_prev_open - 5.0
    candles.append(_make_candle(
        t + 38 * step,
        c_prev_open,
        c_prev_high,
        c_prev_low,
        c_prev_close,
        volume=15.0,
    ))

    # --- Vela 39 (c_now): vela ROJA ENVOLVENTE bajista ---
    # Condiciones engulfing bearish:
    #   c_now.close < c_now.open  (roja)
    #   c_prev.close > c_prev.open  (verde, ya cumplida)
    #   c_now.close <= c_prev.open  (envuelve por abajo)
    #   abs(c_now.close - c_now.open) > (c_prev.close - c_prev.open)
    c_prev_body = abs(c_prev_close - c_prev_open)
    c_now_open = c_prev_close + 2.0              # abre encima del close anterior
    c_now_close = c_prev_open - c_prev_body * 1.5  # cierra debajo del open anterior
    c_now_high = c_now_open + 10.0
    c_now_low = c_now_close - 15.0
    # Verificación FVG touch: c_now.high >= fvg_bottom AND c_now.close <= fvg_top
    # c_now_high = c_now_open + 10 = c_prev_close + 2 + 10 ≈ fvg_top + 10 >= fvg_bottom OK
    # c_now_close << fvg_top  OK
    candles.append(_make_candle(
        t + 39 * step,
        c_now_open,
        c_now_high,
        c_now_low,
        c_now_close,
        volume=30.0,
    ))

    snap = _make_snap(
        mid=BASE_PRICE,
        obi=-0.25,
        cvd_1m=-18.0,
    )
    return candles, snap


def make_test_settings(equity: float = 50.0):
    s = MagicMock()
    s.env = "mainnet"
    s.trading_symbol = "BTC"
    s.risk_per_trade_pct = 0.015         # 1.5% de riesgo → $0.75 por trade
    s.max_leverage = 3.0
    s.daily_loss_limit_pct = 0.02
    s.max_drawdown_pct = 0.08
    s.shadow_mode = False
    s.telegram_chat_id = 123456789
    s.hl_main_address = "0x0000000000000000000000000000000000000000"
    return s


@pytest.fixture
async def mock_db():
    """BD en memoria con esquema completo."""
    db = await aiosqlite.connect(":memory:")
    db.row_factory = aiosqlite.Row
    await db.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS daily_risk (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_utc TEXT NOT NULL UNIQUE,
            equity_open REAL NOT NULL,
            equity_peak REAL NOT NULL,
            realized_pnl REAL NOT NULL DEFAULT 0.0,
            unrealized_pnl REAL NOT NULL DEFAULT 0.0,
            paused_until REAL,
            kill_switch INTEGER NOT NULL DEFAULT 0,
            updated_at REAL NOT NULL DEFAULT (unixepoch('now', 'subsec'))
        );
        CREATE TABLE IF NOT EXISTS live_fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            oid INTEGER,
            coin TEXT,
            dir TEXT,
            px REAL,
            sz REAL,
            closed_pnl REAL DEFAULT 0.0,
            fee REAL DEFAULT 0.0,
            ts_ms INTEGER,
            created_at REAL DEFAULT (unixepoch('now', 'subsec'))
        );
    """)
    await db.commit()
    yield db
    await db.close()


# ── Diagnóstico inline (descomenta para debuggear) ──────────────────────────

def _debug_bullish_scenario():
    """Función de diagnóstico para verificar las condiciones del scenario."""
    candles, snap = _build_bullish_fvg_scenario()
    strat = SmartMoneyFVGReversal()

    n = len(candles)
    atr_5m = snap.volatility.atr_5m
    struct_candles = candles[-strat.MIN_CANDLES:-strat.FVG_SEARCH_WINDOW]
    swing_high = max(c.high for c in struct_candles)

    recent = candles[-(strat.FVG_SEARCH_WINDOW + 2):-1]
    bullish_fvg = None
    for i in range(2, len(recent)):
        c0 = recent[i - 2]
        c2 = recent[i]
        gap = c2.low - c0.high
        if gap >= atr_5m * strat.MIN_FVG_ATR_RATIO:
            bullish_fvg = (c0.high, c2.low)

    c_now = candles[-1]
    c_prev = candles[-2]

    is_bullish_engulfing = (
        c_now.close > c_now.open
        and c_prev.close < c_prev.open
        and c_now.close >= c_prev.open
        and (c_now.close - c_now.open) > abs(c_prev.close - c_prev.open)
    )

    max_last10 = max(c.high for c in candles[-10:])
    struct_ok = max_last10 >= swing_high * 0.999

    obi_ok = snap.obi.value > strat.OBI_MIN_THRESHOLD
    cvd_ok = snap.cvd.cvd_1m > 0

    fvg_touch = False
    if bullish_fvg:
        fvg_bottom, fvg_top = bullish_fvg
        fvg_touch = c_now.low <= fvg_top and c_now.close >= fvg_bottom

    print(f"n_candles={n}, swing_high={swing_high:.1f}")
    print(f"bullish_fvg={bullish_fvg}")
    print(f"is_bullish_engulfing={is_bullish_engulfing}")
    print(f"struct_ok={struct_ok} (max_last10={max_last10:.1f} >= {swing_high*0.999:.1f})")
    print(f"fvg_touch={fvg_touch}")
    print(f"obi_ok={obi_ok}, cvd_ok={cvd_ok}")
    print(f"c_now: o={c_now.open:.1f} h={c_now.high:.1f} l={c_now.low:.1f} c={c_now.close:.1f}")
    print(f"c_prev: o={c_prev.open:.1f} h={c_prev.high:.1f} l={c_prev.low:.1f} c={c_prev.close:.1f}")
    return bullish_fvg, is_bullish_engulfing, struct_ok, fvg_touch


# ── Tests ────────────────────────────────────────────────────────────────────

class TestSmartMoneyFVGLong:

    def test_bullish_fvg_returns_valid_signal(self):
        """FVG alcista + engulfing + estructura + orderflow → SignalProposal BUY."""
        strat = SmartMoneyFVGReversal()
        candles, snap = _build_bullish_fvg_scenario()
        state = _make_state_with_candles(candles)

        result = strat.evaluate(state, snap, MarketRegime.RANGO)

        assert result is not None, "Debe generar señal con FVG alcista válido"
        assert result.strategy == "SmartMoneyFVG"
        assert result.side == SignalSide.BUY
        assert result.order_type == OrderType.IOC, "SmartMoneyFVG debe usar IOC para ejecución inmediata"
        assert result.stop_loss < result.proposed_price, "SL debe estar debajo del precio"
        assert result.take_profit > result.proposed_price, "TP debe estar encima del precio"
        assert result.expected_r >= 2.2, "R:R mínimo 2.2:1"
        assert result.confidence > 0.0

    def test_bullish_sl_distance_floor(self):
        """El SL distance debe ser al menos 0.8 * atr_5m."""
        strat = SmartMoneyFVGReversal()
        candles, snap = _build_bullish_fvg_scenario()
        state = _make_state_with_candles(candles)

        result = strat.evaluate(state, snap, MarketRegime.RANGO)
        assert result is not None

        sl_dist = abs(result.proposed_price - result.stop_loss)
        assert sl_dist >= 0.8 * ATR_5M, f"SL dist {sl_dist:.2f} < piso {0.8*ATR_5M:.2f}"

    def test_bullish_rr_ratio(self):
        """R:R efectivo debe ser >= expected_r."""
        strat = SmartMoneyFVGReversal()
        candles, snap = _build_bullish_fvg_scenario()
        state = _make_state_with_candles(candles)

        result = strat.evaluate(state, snap, MarketRegime.RANGO)
        assert result is not None

        sl_dist = abs(result.proposed_price - result.stop_loss)
        tp_dist = abs(result.take_profit - result.proposed_price)
        rr = tp_dist / sl_dist
        assert abs(rr - result.expected_r) < 0.01, f"R:R {rr:.2f} != expected_r {result.expected_r}"

    def test_proposed_price_equals_mid(self):
        """proposed_price debe ser mid_price (evitar veto RISK_PRICE_DEVIATION)."""
        strat = SmartMoneyFVGReversal()
        candles, snap = _build_bullish_fvg_scenario()
        state = _make_state_with_candles(candles)

        result = strat.evaluate(state, snap, MarketRegime.RANGO)
        assert result is not None
        assert result.proposed_price == snap.obi.mid_price


class TestSmartMoneyFVGShort:

    def test_bearish_fvg_returns_valid_signal(self):
        """FVG bajista + engulfing + estructura + orderflow → SignalProposal SELL."""
        strat = SmartMoneyFVGReversal()
        candles, snap = _build_bearish_fvg_scenario()
        state = _make_state_with_candles(candles)

        result = strat.evaluate(state, snap, MarketRegime.RANGO)

        assert result is not None, "Debe generar señal con FVG bajista válido"
        assert result.strategy == "SmartMoneyFVG"
        assert result.side == SignalSide.SELL
        assert result.order_type == OrderType.IOC, "SmartMoneyFVG debe usar IOC para ejecución inmediata"
        assert result.stop_loss > result.proposed_price, "SL debe estar encima del precio"
        assert result.take_profit < result.proposed_price, "TP debe estar debajo del precio"
        assert result.expected_r >= 2.2

    def test_bearish_sl_distance_floor(self):
        """El SL distance debe ser al menos 0.8 * atr_5m (SHORT)."""
        strat = SmartMoneyFVGReversal()
        candles, snap = _build_bearish_fvg_scenario()
        state = _make_state_with_candles(candles)

        result = strat.evaluate(state, snap, MarketRegime.RANGO)
        assert result is not None

        sl_dist = abs(result.stop_loss - result.proposed_price)
        assert sl_dist >= 0.8 * ATR_5M


class TestSmartMoneyFVGFilters:

    def test_no_fvg_returns_none(self):
        """Sin FVG → None. Velas laterales sin gap institucional."""
        strat = SmartMoneyFVGReversal()

        # Construir velas laterales sin gap significativo
        t = 1_700_000_000_000
        step = 60_000
        candles = []
        p = BASE_PRICE
        for i in range(40):
            # Velas pequeñas, sin gap
            candles.append(_make_candle(
                t + i * step,
                p + (i % 3) * 5,
                p + (i % 3) * 5 + 10,
                p + (i % 3) * 5 - 10,
                p + (i % 3) * 5 + 3,
            ))

        snap = _make_snap(obi=0.20, cvd_1m=10.0)
        state = _make_state_with_candles(candles)

        result = strat.evaluate(state, snap, MarketRegime.RANGO)
        assert result is None, "Sin FVG real, no debe generar señal"

    def test_fvg_without_engulfing_returns_none(self):
        """FVG presente pero sin vela envolvente → None."""
        strat = SmartMoneyFVGReversal()
        candles, snap = _build_bullish_fvg_scenario()

        # Reemplazar la última vela (c_now) por una doji no envolvente
        t = candles[-1].timestamp_ms
        p = BASE_PRICE
        # Vela pequeña verde que NO envuelve a la anterior
        doji = _make_candle(t, p - 2, p + 5, p - 8, p + 1)
        candles[-1] = doji

        state = _make_state_with_candles(candles)
        result = strat.evaluate(state, snap, MarketRegime.RANGO)
        assert result is None, "Sin engulfing no debe generar señal"

    def test_fvg_engulfing_but_negative_obi_returns_none(self):
        """FVG alcista + engulfing pero OBI negativo (orderflow en contra) → None."""
        strat = SmartMoneyFVGReversal()
        candles, _ = _build_bullish_fvg_scenario()

        # OBI negativo = presión vendedora
        snap_bearish = _make_snap(obi=-0.30, cvd_1m=-10.0)
        state = _make_state_with_candles(candles)

        result = strat.evaluate(state, snap_bearish, MarketRegime.RANGO)
        assert result is None, "OBI bearish debe bloquear señal LONG"

    def test_fvg_too_small_returns_none(self):
        """FVG con gap < MIN_FVG_ATR_RATIO * atr_5m → None."""
        strat = SmartMoneyFVGReversal()

        # ATR_5m muy grande para que el gap de las velas sea insignificante
        snap_big_atr = _make_snap(atr_5m=5000.0)  # gap << 15% de 5000

        candles, _ = _build_bullish_fvg_scenario()
        state = _make_state_with_candles(candles)

        result = strat.evaluate(state, snap_big_atr, MarketRegime.RANGO)
        assert result is None, "FVG demasiado pequeño relativo al ATR no debe disparar"

    def test_insufficient_candles_returns_none(self):
        """Menos de MIN_CANDLES velas → None."""
        strat = SmartMoneyFVGReversal()
        t = 1_700_000_000_000
        # Solo 10 velas
        candles = [_make_candle(t + i * 60_000, BASE_PRICE, BASE_PRICE + 10, BASE_PRICE - 10, BASE_PRICE + 5)
                   for i in range(10)]
        state = _make_state_with_candles(candles)
        snap = _make_snap()

        result = strat.evaluate(state, snap, MarketRegime.RANGO)
        assert result is None

    def test_missing_atr_returns_none(self):
        """snap.volatility.atr_5m = None → None."""
        strat = SmartMoneyFVGReversal()
        candles, _ = _build_bullish_fvg_scenario()
        state = _make_state_with_candles(candles)

        snap_no_atr = _make_snap(atr_5m=None)  # type: ignore[arg-type]
        result = strat.evaluate(state, snap_no_atr, MarketRegime.RANGO)
        assert result is None

    def test_dead_market_filter_blocks_signal(self):
        """atr_5m < mid * 0.0008 (mercado muerto / baja volatilidad) → None."""
        strat = SmartMoneyFVGReversal()
        candles, _ = _build_bullish_fvg_scenario()
        state = _make_state_with_candles(candles)

        # mid = 80000 -> 80000 * 0.0008 = 64.0; pasar atr_5m = 40.0
        snap_low_vol = _make_snap(atr_5m=40.0)
        result = strat.evaluate(state, snap_low_vol, MarketRegime.RANGO)
        assert result is None, "Debe bloquear señales cuando atr_5m < mid * 0.0008"

    def test_sl_floor_satisfies_min_035_pct(self):
        """El Stop Loss debe tener un piso de al menos 0.35% del precio (mid * 0.0035)."""
        strat = SmartMoneyFVGReversal()
        candles, snap = _build_bullish_fvg_scenario()
        state = _make_state_with_candles(candles)

        result = strat.evaluate(state, snap, MarketRegime.RANGO)
        assert result is not None
        sl_dist = abs(result.proposed_price - result.stop_loss)
        min_allowed = result.proposed_price * 0.0035
        assert sl_dist >= min_allowed, f"SL dist {sl_dist:.2f} < piso {min_allowed:.2f}"


# ── Test de integración con RiskValidator ────────────────────────────────────

class TestSmartMoneyFVGRiskIntegration:

    @pytest.mark.asyncio
    async def test_valid_signal_approved_by_risk_validator(self, mock_db):
        """
        Señal SmartMoneyFVG válida pasa el RiskValidator en modo live
        (shadow_mode=False) sin lanzar VetoException.
        """
        strat = SmartMoneyFVGReversal()
        candles, snap = _build_bullish_fvg_scenario()
        state = _make_state_with_candles(candles, equity=50.0)

        signal = strat.evaluate(state, snap, MarketRegime.RANGO)
        assert signal is not None, "El setup debe generar una señal válida"
        assert signal.strategy == "SmartMoneyFVG"
        assert signal.side == SignalSide.BUY

        # Configurar validador en modo LIVE (shadow_mode=False)
        validator = RiskValidator(
            mock_db,
            sz_decimals=4,
            min_notional_usd=10.0,
            settings=make_test_settings(equity=50.0),
        )

        # NO debe lanzar VetoException
        try:
            approved = await validator.validate(signal, state, shadow_mode=False)
        except VetoException as e:
            pytest.fail(f"RiskValidator vetó la señal inesperadamente: {e}")

        # Verificaciones del ApprovedOrder
        assert approved is not None
        assert approved.approved_size > 0, "El tamaño aprobado debe ser positivo"
        assert approved.approved_stop_loss == signal.stop_loss
        assert approved.approved_take_profit == signal.take_profit
        assert approved.effective_leverage <= 3.0, "No debe exceder max_leverage 3x"
        assert approved.approved_size * approved.approved_price >= 10.0, "Nocional >= $10"

    @pytest.mark.asyncio
    async def test_signal_sl_dist_satisfies_min_margin_ratio(self, mock_db):
        """
        La distancia SL/liquidación debe satisfacer MIN_MARGIN_TO_SL_RATIO = 2.5x.
        Con leverage <= 3x y SL >= 0.8*ATR esto siempre se cumple.
        """
        strat = SmartMoneyFVGReversal()
        candles, snap = _build_bullish_fvg_scenario()
        state = _make_state_with_candles(candles, equity=50.0)

        signal = strat.evaluate(state, snap, MarketRegime.RANGO)
        assert signal is not None

        validator = RiskValidator(
            mock_db,
            sz_decimals=4,
            min_notional_usd=10.0,
            settings=make_test_settings(),
        )

        # No debe lanzar RISK_MARGIN_DISTANCE
        approved = await validator.validate(signal, state, shadow_mode=False)
        assert approved is not None
