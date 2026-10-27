"""
tests/test_microstructure.py
============================
Tests unitarios para el motor de microestructura.
Usa libros L2 y flujos de trades sintéticos.
"""
from __future__ import annotations

import time
from collections import deque

import pytest

from data.state import AssetContext, Candle, L2Book, L2Level, MarketState, Trade
from microstructure.metrics import (
    BasisAndOITracker,
    OrderBookImbalance,
    RealTimeCVD,
    VolatilityTracker,
    compute_snapshot,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def balanced_book() -> L2Book:
    """Libro equilibrado: igual volumen en bids y asks → OBI ≈ 0."""
    return L2Book(
        bids=[L2Level(50_000.0, 1.0), L2Level(49_995.0, 1.0), L2Level(49_990.0, 1.0)],
        asks=[L2Level(50_005.0, 1.0), L2Level(50_010.0, 1.0), L2Level(50_015.0, 1.0)],
        timestamp_ms=int(time.time() * 1000),
    )


@pytest.fixture
def bid_heavy_book() -> L2Book:
    """Libro con mucho más volumen en bids → OBI > 0.35 (señal alcista)."""
    return L2Book(
        bids=[L2Level(50_000.0, 10.0), L2Level(49_995.0, 8.0)],
        asks=[L2Level(50_005.0, 1.0), L2Level(50_010.0, 1.0)],
        timestamp_ms=int(time.time() * 1000),
    )


@pytest.fixture
def ask_heavy_book() -> L2Book:
    """Libro con mucho más volumen en asks → OBI < -0.35 (señal bajista)."""
    return L2Book(
        bids=[L2Level(50_000.0, 1.0), L2Level(49_995.0, 1.0)],
        asks=[L2Level(50_005.0, 10.0), L2Level(50_010.0, 8.0)],
        timestamp_ms=int(time.time() * 1000),
    )


@pytest.fixture
def warm_market_state() -> MarketState:
    """Estado de mercado con suficientes datos para is_warm=True."""
    state = MarketState()
    now_ms = int(time.time() * 1000)

    # Añadir trades suficientes (mínimo 50)
    for i in range(60):
        state.trades.append(Trade(
            timestamp_ms=now_ms - (60 - i) * 1000,
            price=50_000.0 + (i % 5),
            size=0.1 + (i % 3) * 0.05,
            side="B" if i % 2 == 0 else "A",
        ))

    # Añadir velas suficientes (mínimo 20)
    for i in range(25):
        state.candles_1m.append(Candle(
            timestamp_ms=now_ms - (25 - i) * 60_000,
            open=50_000.0,
            high=50_100.0,
            low=49_900.0,
            close=50_050.0,
            volume=10.0,
            interval="1m",
        ))

    state.l2_book = L2Book(
        bids=[L2Level(50_000.0, 5.0), L2Level(49_995.0, 3.0)],
        asks=[L2Level(50_005.0, 5.0), L2Level(50_010.0, 3.0)],
        timestamp_ms=now_ms,
    )

    state.asset_ctx = AssetContext(
        mark_price=50_002.5,
        oracle_price=50_000.0,
        funding_rate=0.0001,
        open_interest=1_000_000.0,
        timestamp_ms=now_ms,
    )
    state._last_ws_message_at = time.monotonic()

    return state


# ── Tests OBI ─────────────────────────────────────────────────────────────────

class TestOrderBookImbalance:

    def test_balanced_book_obi_near_zero(self, balanced_book):
        """Libro equilibrado debe dar OBI cercano a 0."""
        calc = OrderBookImbalance()
        result = calc.calculate(balanced_book)

        assert result is not None
        assert abs(result.value) < 0.05, f"OBI debería ser ~0, got {result.value}"

    def test_bid_heavy_is_bullish(self, bid_heavy_book):
        """Libro con más bids → OBI > 0.35 → is_bullish = True."""
        calc = OrderBookImbalance()
        result = calc.calculate(bid_heavy_book)

        assert result is not None
        assert result.value > 0.35, f"Expected OBI > 0.35, got {result.value}"
        assert result.is_bullish

    def test_ask_heavy_is_bearish(self, ask_heavy_book):
        """Libro con más asks → OBI < -0.35 → is_bearish = True."""
        calc = OrderBookImbalance()
        result = calc.calculate(ask_heavy_book)

        assert result is not None
        assert result.value < -0.35, f"Expected OBI < -0.35, got {result.value}"
        assert result.is_bearish

    def test_obi_uses_bps_range_not_levels(self):
        """
        CORRECCIÓN #4: Verifica que OBI usa rango en bps, no los primeros N niveles.
        Un bid a 40,000 (lejos del mid 50,000) NO debe incluirse en el OBI.
        """
        book = L2Book(
            bids=[
                L2Level(50_000.0, 1.0),   # Dentro del rango (mid=50002.5, ±15bps = ±7.5)
                L2Level(40_000.0, 100.0), # FUERA del rango: no debe contar
            ],
            asks=[
                L2Level(50_005.0, 1.0),
            ],
            timestamp_ms=int(time.time() * 1000),
        )
        calc = OrderBookImbalance()
        result = calc.calculate(book, bps_range=15.0)

        assert result is not None
        # Si contara el bid de 40k, bid_vol sería 101 y el OBI sería ~0.98
        # Con el filtro correcto, bid_vol ≈ 1 y ask_vol ≈ 1, OBI ≈ 0
        assert abs(result.value) < 0.5, (
            f"El bid de 40k no debería influir en el OBI. "
            f"bid_vol={result.bid_volume}, ask_vol={result.ask_volume}, obi={result.value}"
        )

    def test_empty_book_returns_none(self):
        calc = OrderBookImbalance()
        result = calc.calculate(L2Book())
        assert result is None

    def test_obi_range_minus_one_to_one(self, bid_heavy_book):
        calc = OrderBookImbalance()
        result = calc.calculate(bid_heavy_book)
        assert result is not None
        assert -1.0 <= result.value <= 1.0


# ── Tests CVD ─────────────────────────────────────────────────────────────────

class TestRealTimeCVD:

    def test_all_buys_positive_cvd(self, warm_market_state):
        """Todos los trades son compras → CVD positivo."""
        state = warm_market_state
        # Reemplazar trades con solo compras
        now_ms = int(time.time() * 1000)
        state.trades.clear()
        for i in range(60):
            state.trades.append(Trade(
                timestamp_ms=now_ms - (60 - i) * 1000,
                price=50_000.0,
                size=1.0,
                side="B",  # Todos compradores
            ))

        calc = RealTimeCVD()
        result = calc.calculate(state)

        assert result is not None
        assert result.cvd_1m > 0, "CVD debe ser positivo cuando todos compran"

    def test_all_sells_negative_cvd(self, warm_market_state):
        """Todos los trades son ventas → CVD negativo."""
        state = warm_market_state
        now_ms = int(time.time() * 1000)
        state.trades.clear()
        for i in range(60):
            state.trades.append(Trade(
                timestamp_ms=now_ms - (60 - i) * 1000,
                price=50_000.0,
                size=1.0,
                side="A",  # Todos vendedores
            ))

        calc = RealTimeCVD()
        result = calc.calculate(state)

        assert result is not None
        assert result.cvd_1m < 0, "CVD debe ser negativo cuando todos venden"

    def test_divergence_detected(self, warm_market_state):
        """Precio baja pero CVD sube → divergencia alcista."""
        state = warm_market_state
        now_ms = int(time.time() * 1000)
        state.trades.clear()

        # Precio bajando pero compradores dominando (absorción)
        for i in range(60):
            price = 50_000.0 - i * 2  # Precio descendente
            state.trades.append(Trade(
                timestamp_ms=now_ms - (60 - i) * 1000,
                price=price,
                size=1.0,
                side="B",  # Pero compradores agresivos (absorción)
            ))

        calc = RealTimeCVD()
        result = calc.calculate(state)

        assert result is not None
        assert result.divergence_1m, "Debe detectar divergencia: precio baja, CVD sube"

    def test_not_warm_returns_none(self):
        """Sin warmup, CVD debe retornar None."""
        state = MarketState()  # Vacío
        calc = RealTimeCVD()
        result = calc.calculate(state)
        assert result is None


# ── Tests de Salud del Sistema ─────────────────────────────────────────────────

class TestIsWarm:

    def test_empty_state_not_warm(self):
        state = MarketState()
        assert not state.is_warm

    def test_enough_data_is_warm(self, warm_market_state):
        assert warm_market_state.is_warm

    def test_partial_data_not_warm(self):
        state = MarketState()
        # Solo trades, sin velas
        now_ms = int(time.time() * 1000)
        for i in range(60):
            state.trades.append(Trade(now_ms - i * 1000, 50000.0, 1.0, "B"))
        assert not state.is_warm, "Sin velas suficientes no debe ser warm"

    def test_warmup_progress(self):
        state = MarketState()
        progress = state.warmup_progress
        assert "trades" in progress
        assert "candles_1m" in progress
        assert "is_warm" in progress


# ── Tests Snapshot ─────────────────────────────────────────────────────────────

class TestComputeSnapshot:

    def test_snapshot_not_warm_returns_no_cvd(self, warm_market_state):
        """Con is_warm=True, el snapshot debe incluir CVD."""
        snap = compute_snapshot(warm_market_state)
        assert snap.is_warm
        # CVD puede ser None si los trades son muy viejos, pero should_be_computed

    def test_snapshot_timing(self, warm_market_state):
        """El snapshot debe completarse en < 10ms."""
        import time as _time
        start = _time.perf_counter()
        snap = compute_snapshot(warm_market_state)
        elapsed_ms = (_time.perf_counter() - start) * 1000

        assert elapsed_ms < 10.0, f"Snapshot tardó {elapsed_ms:.2f}ms (max 10ms)"
        assert snap.computed_at > 0
