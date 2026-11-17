"""
tests/test_kronos_strategy.py
=============================
Tests unitarios para KronosFoundationStrategy y su integración en DecisionEngine.
Verifica:
1. len(state.candles_5m) < 257 -> None
2. Predicción alcista válida -> SignalProposal válido sin ValidationError
3. Predicción bajista válida -> SignalProposal válido sin ValidationError
4. Filtro de libro L2 (OBI adverso) veta la señal
5. DecisionEngine inicializa exclusivamente Kronos cuando USE_KRONOS=true
"""

import os
import time
from unittest.mock import MagicMock
import pandas as pd
import pytest

from data.state import Candle, L2Book, L2Level, MarketState
from microstructure.metrics import (
    BasisOIResult,
    CVDResult,
    MicrostructureSnapshot,
    OBIResult,
    VolatilityResult,
)
from signals.engine import DecisionEngine, KronosFoundationStrategy
from signals.schemas import MarketRegime, OrderType, SignalProposal, SignalSide


def _make_snapshot(obi_val: float = 0.1) -> MicrostructureSnapshot:
    return MicrostructureSnapshot(
        obi=OBIResult(
            value=obi_val,
            bid_volume=100.0,
            ask_volume=90.0,
            bps_range_used=10.0,
            levels_captured=5,
            mid_price=70000.0,
            computed_at=time.monotonic(),
        ),
        cvd=CVDResult(
            cvd_1m=5.0,
            cvd_5m=12.0,
            cvd_15m=25.0,
            price_change_1m_pct=0.05,
            divergence_1m=False,
            divergence_5m=False,
            trades_counted_1m=50,
            computed_at=time.monotonic(),
        ),
        basis_oi=BasisOIResult(
            basis_bps=1.5,
            oi_change_5m_pct=0.2,
            oi_change_1h_pct=0.5,
            oi_trend="UP",
            price_oi_regime="TREND_LONG",
            mark_price=70000.0,
            oracle_price=69990.0,
            computed_at=time.monotonic(),
        ),
        volatility=VolatilityResult(
            atr_5m=100.0,
            atr_1h=300.0,
            session_vwap=70000.0,
            distance_to_vwap_pct=0.1,
            current_price=70000.0,
        ),
        is_warm=True,
        computed_at=time.monotonic(),
    )


def _make_state_with_5m_candles(n: int = 260) -> MarketState:
    state = MarketState()
    base_ts = 1700000000000
    for i in range(n):
        ts = base_ts + i * 300000
        c = Candle(
            timestamp_ms=ts,
            open=70000.0 + i,
            high=70050.0 + i,
            low=69950.0 + i,
            close=70020.0 + i,
            volume=10.0,
            interval="5m",
        )
        state.candles_5m.append(c)

    state.l2_book = L2Book(
        bids=[L2Level(price=70019.0, size=2.0)],
        asks=[L2Level(price=70021.0, size=2.0)],
        timestamp_ms=base_ts + n * 300000,
    )
    return state


def test_kronos_insufficient_candles():
    strat = KronosFoundationStrategy()
    state = _make_state_with_5m_candles(n=100)  # Menos de 257
    snap = _make_snapshot()
    sig = strat.evaluate(state, snap, MarketRegime.RANGO)
    assert sig is None


def test_kronos_valid_long_proposal():
    strat = KronosFoundationStrategy()
    state = _make_state_with_5m_candles(n=260)
    snap = _make_snapshot(obi_val=0.2)

    # Mock del predictor
    mock_pred = MagicMock()
    mock_pred.predict_ensemble.return_value = {
        "mean_df": pd.DataFrame({"close": [70400.0, 70500.0, 70600.0, 70700.0, 70800.0, 71000.0]}),
        "bull_ratio": 1.0,
        "bear_ratio": 0.0,
    }
    strat._predictor = mock_pred

    sig = strat.evaluate(state, snap, MarketRegime.RANGO)
    assert sig is not None
    assert isinstance(sig, SignalProposal)
    assert sig.strategy == "KronosFoundationStrategy"
    assert sig.side == SignalSide.BUY
    assert sig.order_type == OrderType.IOC
    assert sig.regime == MarketRegime.RANGO
    assert sig.stop_loss < sig.proposed_price
    assert sig.take_profit > sig.proposed_price
    assert sig.confidence == 1.0


def test_kronos_valid_short_proposal():
    strat = KronosFoundationStrategy()
    state = _make_state_with_5m_candles(n=260)
    snap = _make_snapshot(obi_val=-0.2)

    mock_pred = MagicMock()
    mock_pred.predict_ensemble.return_value = {
        "mean_df": pd.DataFrame({"close": [69900.0, 69850.0, 69800.0, 69750.0, 69700.0, 69600.0]}),
        "bull_ratio": 0.0,
        "bear_ratio": 1.0,
    }
    strat._predictor = mock_pred

    sig = strat.evaluate(state, snap, MarketRegime.RANGO)
    assert sig is not None
    assert isinstance(sig, SignalProposal)
    assert sig.strategy == "KronosFoundationStrategy"
    assert sig.side == SignalSide.SELL
    assert sig.order_type == OrderType.IOC
    assert sig.regime == MarketRegime.RANGO
    assert sig.stop_loss > sig.proposed_price
    assert sig.take_profit < sig.proposed_price
    assert sig.confidence == 1.0


def test_kronos_obi_filter_blocks_long():
    strat = KronosFoundationStrategy()
    state = _make_state_with_5m_candles(n=260)
    # Muro institucional de venta (OBI < -0.65)
    snap = _make_snapshot(obi_val=-0.75)

    mock_pred = MagicMock()
    mock_pred.predict_ensemble.return_value = {
        "mean_df": pd.DataFrame({"close": [70400.0, 70500.0, 70600.0, 70700.0, 70800.0, 71000.0]}),
        "bull_ratio": 1.0,
        "bear_ratio": 0.0,
    }
    strat._predictor = mock_pred

    sig = strat.evaluate(state, snap, MarketRegime.RANGO)
    assert sig is None


def test_decision_engine_exclusive_when_use_kronos(monkeypatch):
    # Por argumento explícito
    engine_arg = DecisionEngine(shadow_mode=True, use_kronos=True)
    assert len(engine_arg._strategies) == 1
    assert isinstance(engine_arg._strategies[0], KronosFoundationStrategy)

    engine_arg_off = DecisionEngine(shadow_mode=True, use_kronos=False)
    assert len(engine_arg_off._strategies) == 2

    # Por variable de entorno
    monkeypatch.setenv("USE_KRONOS", "true")
    engine = DecisionEngine(shadow_mode=True)
    assert len(engine._strategies) == 1
    assert isinstance(engine._strategies[0], KronosFoundationStrategy)

    monkeypatch.setenv("USE_KRONOS", "false")
    engine_old = DecisionEngine(shadow_mode=True)
    assert len(engine_old._strategies) == 2
    strat_names = [type(s).__name__ for s in engine_old._strategies]
    assert "SmartMoneyFVGReversal" in strat_names
    assert "TrendMomentumV4" in strat_names
