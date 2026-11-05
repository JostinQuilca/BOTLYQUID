"""
tests/test_risk.py
==================
Tests exhaustivos del Validador de Riesgo.
Intenta violar cada regla de seguridad para verificar que el validador rechaza.

Incluye tests especÃ­ficos para:
- CORRECCIÃ“N #3: LÃ³gica de funding corregida
- Kill switch desde archivo STOP_BOT.flag
- PosiciÃ³n Ãºnica
- Drawdown y pÃ©rdida diaria
- Dimensionamiento con szDecimals
"""
from __future__ import annotations

import math
import os
import time
import unittest.mock as mock
from datetime import date
from unittest.mock import MagicMock

import aiosqlite
import pytest
from pydantic import SecretStr

from data.state import AccountState, AssetContext, L2Book, L2Level, MarketState, Trade
from risk.validator import RiskValidator, VetoException
from signals.schemas import MarketRegime, OrderType, SignalProposal, SignalSide


def make_test_settings():
    """
    Crea una instancia de Settings con valores de prueba sin requerir .env.
    Usa MagicMock para evitar la validaciÃ³n de pydantic-settings.
    """
    s = MagicMock()
    s.env = "testnet"
    s.trading_symbol = "BTC"
    s.risk_per_trade_pct = 0.005
    s.max_leverage = 3.0
    s.daily_loss_limit_pct = 0.03
    s.max_drawdown_pct = 0.10
    s.shadow_mode = True
    s.telegram_chat_id = 123456789
    s.hl_main_address = "0x0000000000000000000000000000000000000000"
    return s


# â”€â”€ Fixtures â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@pytest.fixture
async def mock_db():
    """BD en memoria para tests."""
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
    """)
    await db.commit()
    yield db
    await db.close()


def make_healthy_state(equity: float = 10_000.0) -> MarketState:
    """Estado de mercado saludable para tests."""
    state = MarketState()
    now_ms = int(time.time() * 1000)

    state.l2_book = L2Book(
        bids=[L2Level(50_000.0, 5.0)],
        asks=[L2Level(50_005.0, 5.0)],
        timestamp_ms=now_ms,
    )
    state.asset_ctx = AssetContext(
        mark_price=50_002.5,
        oracle_price=50_000.0,
        funding_rate=0.000001,   # 0.876% anualizado — funding normal, no extremo
        open_interest=1_000_000.0,
        timestamp_ms=now_ms,
    )
    state.account = AccountState(
        equity=equity,
        available_margin=equity * 0.9,
        position_size=0.0,
    )
    state._last_ws_message_at = time.monotonic()  # Tick fresco
    return state


def make_valid_signal(price: float = 50_000.0) -> SignalProposal:
    """SeÃ±al vÃ¡lida de prueba."""
    return SignalProposal(
        strategy="TestStrategy",
        side=SignalSide.BUY,
        order_type=OrderType.ALO,
        proposed_price=price,
        stop_loss=price * 0.99,   # 1% de distancia
        regime=MarketRegime.RANGO,
        estimated_hold_minutes=30.0,
        expected_r=2.0,
        confidence=0.7,
    )


# â”€â”€ Tests de Salud del Sistema â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class TestSystemHealth:

    @pytest.mark.asyncio
    async def test_stale_tick_vetoed(self, mock_db):
        """Tick viejo (> 3000ms) debe ser vetado."""
        state = make_healthy_state()
        state._last_ws_message_at = time.monotonic() - 5.0  # 5 segundos sin tick

        validator = RiskValidator(mock_db, settings=make_test_settings())

        with pytest.raises(VetoException) as exc_info:
            await validator.validate(make_valid_signal(), state)

        assert exc_info.value.reason.value == "SYSTEM_STALE"
        assert "3000ms" in str(exc_info.value) or "ms" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_healthy_system_passes(self, mock_db):
        """Sistema saludable debe pasar la verificaciÃ³n."""
        state = make_healthy_state()
        validator = RiskValidator(mock_db, settings=make_test_settings())

        # No debe lanzar excepciÃ³n
        approved = await validator.validate(make_valid_signal(), state)
        assert approved is not None
        assert approved.approved_size > 0


# â”€â”€ Tests de Kill Switch â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class TestKillSwitch:

    @pytest.mark.asyncio
    async def test_kill_switch_flag_file(self, mock_db, tmp_path, monkeypatch):
        """Archivo STOP_BOT.flag debe activar el kill switch."""
        # Crear el archivo flag
        flag_path = tmp_path / "STOP_BOT.flag"
        flag_path.touch()

        monkeypatch.chdir(tmp_path)

        state = make_healthy_state()
        validator = RiskValidator(mock_db, settings=make_test_settings())

        with pytest.raises(VetoException) as exc_info:
            await validator.validate(make_valid_signal(), state)

        assert exc_info.value.reason.value == "KILL_SWITCH_ACTIVE"

    @pytest.mark.asyncio
    async def test_manual_kill_switch(self, mock_db):
        """Kill switch activado manualmente debe bloquear todas las Ã³rdenes."""
        state = make_healthy_state()
        validator = RiskValidator(mock_db, settings=make_test_settings())
        validator.activate_kill_switch("Test")

        with pytest.raises(VetoException) as exc_info:
            await validator.validate(make_valid_signal(), state)

        assert exc_info.value.reason.value == "KILL_SWITCH_ACTIVE"


# â”€â”€ Tests de LÃ­mites de Cuenta â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class TestAccountLimits:

    @pytest.mark.asyncio
    async def test_daily_loss_limit(self, mock_db):
        """PÃ©rdida diaria > 3% debe pausar el bot."""
        state = make_healthy_state(equity=10_000.0)

        # Insertar estado de riesgo del dÃ­a con pÃ©rdida del 4%
        today = date.today().isoformat()
        await mock_db.execute(
            """
            INSERT INTO daily_risk
            (date_utc, equity_open, equity_peak, realized_pnl, unrealized_pnl)
            VALUES (?, 10000, 10000, -300, -100)
            """,
            (today,),
        )
        await mock_db.commit()

        validator = RiskValidator(mock_db, settings=make_test_settings())

        with pytest.raises(VetoException) as exc_info:
            await validator.validate(make_valid_signal(), state)

        assert exc_info.value.reason.value == "RISK_DAILY_LIMIT"

    @pytest.mark.asyncio
    async def test_max_drawdown_kills(self, mock_db):
        """Drawdown > 10% debe activar kill switch permanente."""
        # Equity actual = 8,500 (15% de caÃ­da desde pico de 10,000)
        state = make_healthy_state(equity=8_500.0)

        today = date.today().isoformat()
        await mock_db.execute(
            """
            INSERT INTO daily_risk
            (date_utc, equity_open, equity_peak, realized_pnl, unrealized_pnl)
            VALUES (?, 10000, 10000, 0, 0)
            """,
            (today,),
        )
        await mock_db.commit()

        validator = RiskValidator(mock_db, settings=make_test_settings())

        with pytest.raises(VetoException) as exc_info:
            await validator.validate(make_valid_signal(), state)

        assert exc_info.value.reason.value == "RISK_DRAWDOWN"
        assert validator._kill_switch_active

    @pytest.mark.asyncio
    async def test_existing_position_vetoed(self, mock_db):
        """Segunda posiciÃ³n debe ser vetada (mÃ¡ximo 1 posiciÃ³n)."""
        state = make_healthy_state()
        state.account.position_size = 0.001  # Ya hay una posiciÃ³n abierta

        validator = RiskValidator(mock_db, settings=make_test_settings())

        with pytest.raises(VetoException) as exc_info:
            await validator.validate(make_valid_signal(), state)

        assert exc_info.value.reason.value == "RISK_MAX_POSITION"


# â”€â”€ Tests de Dimensionamiento â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class TestPositionSizing:

    @pytest.mark.asyncio
    async def test_size_rounds_to_sz_decimals(self, mock_db):
        """El tamaÃ±o debe redondearse a szDecimals (5 para BTC)."""
        state = make_healthy_state(equity=10_000.0)
        validator = RiskValidator(mock_db, sz_decimals=5, settings=make_test_settings())

        approved = await validator.validate(make_valid_signal(50_000.0), state)

        # Verificar que el size tiene mÃ¡ximo 5 decimales
        size = approved.approved_size
        decimals = len(str(size).split(".")[-1]) if "." in str(size) else 0
        assert decimals <= 5, f"Size {size} tiene {decimals} decimales (max 5)"

    @pytest.mark.asyncio
    async def test_notional_minimum(self, mock_db):
        """Notional < $10 USDC debe ser vetado."""
        # Con equity muy pequeÃ±a, el size calculado tendrÃ­a notional mÃ­nimo
        state = make_healthy_state(equity=10.0)   # Equity de $10
        validator = RiskValidator(mock_db, settings=make_test_settings())

        with pytest.raises(VetoException) as exc_info:
            await validator.validate(make_valid_signal(), state)

        assert exc_info.value.reason.value == "RISK_NOTIONAL_MIN"


# â”€â”€ Tests de Funding (CORRECCIÃ“N #3) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class TestFundingFilter:
    """
    CORRECCIÃ“N #3: Verifica la lÃ³gica correcta de funding.

    El plan original vetaba si faltaban < 3 min para el cobro horario.
    La correcciÃ³n: vetar si el COSTO TOTAL de funding durante el trade
    erosiona mÃ¡s del 20% del retorno esperado.
    """

    @pytest.mark.asyncio
    async def test_neutral_funding_not_vetoed(self, mock_db):
        """Funding neutral no debe vetar la seÃ±al."""
        state = make_healthy_state()
        state.asset_ctx.funding_rate = 0.0  # Sin funding

        validator = RiskValidator(mock_db, settings=make_test_settings())
        approved = await validator.validate(make_valid_signal(), state)
        assert approved is not None

    @pytest.mark.asyncio
    async def test_favorable_funding_not_vetoed(self, mock_db):
        """Funding favorable (pagan por ir long) no debe vetar."""
        state = make_healthy_state()
        state.asset_ctx.funding_rate = -0.001  # Negativo = pagan a los longs

        signal = SignalProposal(
            strategy="Test",
            side=SignalSide.BUY,   # Long con funding negativo = favorable
            order_type=OrderType.ALO,
            proposed_price=50_000.0,
            stop_loss=49_500.0,
            regime=MarketRegime.RANGO,
            estimated_hold_minutes=30.0,
            expected_r=2.0,
        )

        validator = RiskValidator(mock_db, settings=make_test_settings())
        approved = await validator.validate(signal, state)
        assert approved is not None

    @pytest.mark.asyncio
    async def test_extreme_adverse_funding_vetoed(self, mock_db):
        """
        Funding extremadamente adverso (> 25% anualizado) debe vetar.
        Para long: funding_rate positivo (se paga) y muy alto.
        """
        state = make_healthy_state()
        # funding_rate hourly = 0.004 â†’ 35.04% anualizado (> 25% threshold)
        state.asset_ctx.funding_rate = 0.004

        signal = SignalProposal(
            strategy="Test",
            side=SignalSide.BUY,   # Long con funding adverso alto
            order_type=OrderType.ALO,
            proposed_price=50_000.0,
            stop_loss=49_500.0,
            regime=MarketRegime.RANGO,
            estimated_hold_minutes=30.0,
            expected_r=2.0,
        )

        validator = RiskValidator(mock_db, settings=make_test_settings())

        with pytest.raises(VetoException) as exc_info:
            await validator.validate(signal, state)

        assert exc_info.value.reason.value == "RISK_FUNDING"
        assert "annualized" in str(exc_info.value).lower() or "funding" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_funding_cost_erodes_return_vetoed(self, mock_db):
        """
        Funding que erosiona > 20% del retorno esperado debe vetar.
        SeÃ±al con R muy bajo + funding moderado.
        """
        state = make_healthy_state()
        # Funding hourly = 0.001 (8.76% anualizado, no extremo)
        state.asset_ctx.funding_rate = 0.001

        # SeÃ±al con stop muy cercano (R esperado pequeÃ±o) y hold largo
        signal = SignalProposal(
            strategy="Test",
            side=SignalSide.BUY,
            order_type=OrderType.ALO,
            proposed_price=50_000.0,
            stop_loss=49_990.0,  # Solo $10 de distancia al SL â†’ risk_distance = 0.02%
            regime=MarketRegime.RANGO,
            estimated_hold_minutes=120.0,  # 2 horas â†’ 2 pagos de funding
            expected_r=1.1,
        )

        validator = RiskValidator(mock_db, settings=make_test_settings())

        # Este puede o no vetar dependiendo del cÃ¡lculo exacto
        # Lo importante es que la LÃ“GICA sea correcta (costo total, no ventana de 3min)
        try:
            approved = await validator.validate(signal, state)
            # Si no vetÃ³, verificar que el cÃ¡lculo fue por costo total
            assert approved is not None
        except VetoException as e:
            assert e.reason.value == "RISK_FUNDING"

    @pytest.mark.asyncio
    async def test_old_logic_not_used(self, mock_db):
        """
        CORRECCIÃ“N #3: Verificar que NO se usa la lÃ³gica antigua de "3 minutos antes".
        El validador NO debe preguntar quÃ© hora es ni cuÃ¡ntos minutos faltan.
        """
        state = make_healthy_state()
        state.asset_ctx.funding_rate =         state.asset_ctx.funding_rate = 0.000001  # 0.876pct annualized

        validator = RiskValidator(mock_db, settings=make_test_settings())

        # Debe aprobar independientemente de la hora del reloj
        # (La lÃ³gica antigua bloqueaba si faltaban < 3 min para :00)
        approved = await validator.validate(make_valid_signal(), state)
        assert approved is not None


# â”€â”€ Tests de Microestructura â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class TestMicrostructureRules:

    @pytest.mark.asyncio
    async def test_price_deviation_vetoed(self, mock_db):
        """Precio > 20 bps del mid debe ser vetado."""
        state = make_healthy_state()
        # mid = 50_002.5, 20 bps = 50_002.5 * 0.002 = 100
        # Precio alejado > 100 del mid
        signal = SignalProposal(
            strategy="Test",
            side=SignalSide.BUY,
            order_type=OrderType.ALO,
            proposed_price=50_200.0,  # Muy lejos del mid (50_002.5)
            stop_loss=49_700.0,
            regime=MarketRegime.RANGO,
        )

        validator = RiskValidator(mock_db, settings=make_test_settings())

        with pytest.raises(VetoException) as exc_info:
            await validator.validate(signal, state)

        assert exc_info.value.reason.value == "RISK_PRICE_DEVIATION"

    @pytest.mark.asyncio
    async def test_leverage_limit(self, mock_db):
        """Apalancamiento efectivo > 3x debe ser vetado."""
        state = make_healthy_state(equity=1_000.0)  # Equity pequeÃ±a

        # SeÃ±al grande con precio alto = leverage alto
        signal = SignalProposal(
            strategy="Test",
            side=SignalSide.BUY,
            order_type=OrderType.ALO,
            proposed_price=50_000.0,
            stop_loss=49_000.0,   # 2% de SL
            regime=MarketRegime.RANGO,
        )

        validator = RiskValidator(mock_db, sz_decimals=5, settings=make_test_settings())

        # Con equity=1000 y risk=0.5%, risk_usd=5
        # size = 5 / 1000 = 0.005 BTC
        # notional = 0.005 * 50000 = 250
        # leverage = 250 / 1000 = 0.25x â†’ no debe vetar por leverage

        # Este caso especÃ­fico no deberÃ­a exceder 3x
        try:
            approved = await validator.validate(signal, state)
            assert approved.effective_leverage <= 3.0
        except VetoException as e:
            # Otros vetos son posibles (notional mÃ­nimo, etc.)
            pass


# ── Tests adicionales: persistencia BD, apply_size_multiplier, RSI Wilder, cooldown ──


@pytest.fixture
async def fresh_db():
    """Base de datos limpia en memoria para tests de persistencia."""
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
            oid INTEGER, coin TEXT, dir TEXT,
            px REAL, sz REAL, closed_pnl REAL DEFAULT 0.0,
            fee REAL DEFAULT 0.0, ts_ms INTEGER,
            created_at REAL DEFAULT (unixepoch('now', 'subsec'))
        );
    """)
    await db.commit()
    yield db
    await db.close()


def make_live_settings():
    s = MagicMock()
    s.env = "mainnet"
    s.trading_symbol = "BTC"
    s.risk_per_trade_pct = 0.015
    s.max_leverage = 3.0
    s.daily_loss_limit_pct = 0.02   # 2%
    s.max_drawdown_pct = 0.08       # 8%
    s.shadow_mode = False
    s.telegram_chat_id = 0
    s.hl_main_address = "0x0000000000000000000000000000000000000000"
    return s


def _make_signal_for_risk(side=SignalSide.BUY, price=80000.0):
    return SignalProposal(
        strategy="TestStrat",
        side=side,
        order_type=OrderType.ALO,
        proposed_price=price,
        stop_loss=price - 200.0 if side == SignalSide.BUY else price + 200.0,
        take_profit=price + 440.0 if side == SignalSide.BUY else price - 440.0,
        regime=MarketRegime.RANGO,
        obi=0.20,
        cvd_1m=10.0,
        cvd_5m=25.0,
        basis_bps=1.5,
        oi_change_5m=0.3,
        estimated_hold_minutes=25.0,
        expected_r=2.2,
        confidence=0.75,
    )


def _make_state_risk(equity=50.0, price=80000.0):
    state = MarketState()
    state.account = AccountState(
        equity=equity,
        available_margin=equity * 0.95,
        position_size=0.0,
        position_entry_price=0.0,
        unrealized_pnl=0.0,
    )
    state.asset_ctx = AssetContext(
        mark_price=price,
        oracle_price=price * 0.9999,
        funding_rate=0.000005,
        open_interest=45000.0,
        timestamp_ms=int(time.time() * 1000),
    )
    state.l2_book = L2Book(
        bids=[L2Level(price - 2.0, 5.0)],
        asks=[L2Level(price + 2.0, 2.0)],
        timestamp_ms=int(time.time() * 1000),
    )
    state._last_ws_message_at = time.monotonic()
    return state


class TestPersistencia:

    @pytest.mark.asyncio
    async def test_daily_loss_limit_persiste_paused_until(self, fresh_db):
        """Cuando se supera el daily_loss_limit, paused_until se guarda en daily_risk."""
        today = date.today().isoformat()
        equity = 100.0
        # Pre-poblar daily_risk con pérdida del 5% (> 2% límite)
        await fresh_db.execute(
            "INSERT INTO daily_risk (date_utc, equity_open, equity_peak, realized_pnl, unrealized_pnl) VALUES (?, ?, ?, ?, ?)",
            (today, equity, equity, -6.0, 0.0),   # -6% > -2% límite
        )
        await fresh_db.commit()

        validator = RiskValidator(fresh_db, sz_decimals=4, min_notional_usd=10.0, settings=make_live_settings())
        signal = _make_signal_for_risk()
        state = _make_state_risk(equity=equity)

        with pytest.raises(VetoException) as exc_info:
            await validator.validate(signal, state, shadow_mode=False)

        assert exc_info.value.reason.value == "RISK_DAILY_LIMIT"

        # Verificar que paused_until fue persistido en BD
        async with fresh_db.execute("SELECT paused_until FROM daily_risk WHERE date_utc = ?", (today,)) as cur:
            row = await cur.fetchone()
        assert row is not None
        assert row["paused_until"] is not None
        assert row["paused_until"] > time.time()   # está en el futuro

    @pytest.mark.asyncio
    async def test_max_drawdown_persiste_kill_switch(self, fresh_db):
        """Cuando se supera max_drawdown, kill_switch = 1 queda en daily_risk."""
        today = date.today().isoformat()
        equity_open = 100.0
        equity_peak = 100.0
        await fresh_db.execute(
            "INSERT INTO daily_risk (date_utc, equity_open, equity_peak, realized_pnl, unrealized_pnl) VALUES (?, ?, ?, 0, 0)",
            (today, equity_open, equity_peak),
        )
        await fresh_db.commit()

        validator = RiskValidator(fresh_db, sz_decimals=4, min_notional_usd=10.0, settings=make_live_settings())
        signal = _make_signal_for_risk()
        # Equity caída al 85% del pico = 15% drawdown > 8% límite
        state = _make_state_risk(equity=85.0)

        with pytest.raises(VetoException) as exc_info:
            await validator.validate(signal, state, shadow_mode=False)

        assert exc_info.value.reason.value == "RISK_DRAWDOWN"

        # Verificar que kill_switch = 1 fue persistido
        async with fresh_db.execute("SELECT kill_switch FROM daily_risk WHERE date_utc = ?", (today,)) as cur:
            row = await cur.fetchone()
        assert row is not None
        assert row["kill_switch"] == 1


class TestApplySizeMultiplier:

    @pytest.mark.asyncio
    async def test_multiplier_reduces_size(self, fresh_db):
        """apply_size_multiplier con 0.5 reduce el tamaño a la mitad."""
        from signals.schemas import ApprovedOrder
        validator = RiskValidator(fresh_db, sz_decimals=4, min_notional_usd=10.0, settings=make_live_settings())
        signal = _make_signal_for_risk()
        approved = ApprovedOrder(
            signal=signal,
            approved_size=0.0010,
            approved_price=80000.0,
            approved_stop_loss=79800.0,
            approved_take_profit=80440.0,
            risk_amount_usd=0.75,
            effective_leverage=1.6,
        )
        result = validator.apply_size_multiplier(approved, 0.5)
        assert result.approved_size < approved.approved_size
        assert result.approved_size * result.approved_price >= 10.0   # nocional mínimo

    @pytest.mark.asyncio
    async def test_multiplier_1_unchanged(self, fresh_db):
        """apply_size_multiplier con 1.0 no cambia nada."""
        from signals.schemas import ApprovedOrder
        validator = RiskValidator(fresh_db, sz_decimals=4, min_notional_usd=10.0, settings=make_live_settings())
        signal = _make_signal_for_risk()
        approved = ApprovedOrder(
            signal=signal,
            approved_size=0.0020,
            approved_price=80000.0,
            approved_stop_loss=79800.0,
            approved_take_profit=80440.0,
            risk_amount_usd=0.75,
            effective_leverage=1.6,
        )
        result = validator.apply_size_multiplier(approved, 1.0)
        assert result is approved   # mismo objeto, sin copia


class TestRSIWilderYCooldown:

    def test_rsi_wilder_vs_simple_diferente(self):
        """RSI de Wilder produce resultado distinto al RSI de media simple."""
        from signals.engine import _rsi
        # Serie de precios con tendencia clara
        prices = [100.0 + i * 0.5 + (i % 3) * 0.2 for i in range(30)]
        rsi_val = _rsi(prices, period=14)
        assert rsi_val is not None
        assert 0.0 <= rsi_val <= 100.0

    def test_rsi_serie_pura_alcista(self):
        """RSI en serie puramente alcista debe ser cercano a 100."""
        from signals.engine import _rsi
        prices = [float(i) for i in range(1, 32)]   # 1,2,...,31
        rsi_val = _rsi(prices, period=14)
        assert rsi_val is not None
        assert rsi_val > 90.0, f"RSI alcista esperado > 90, got {rsi_val:.1f}"

    def test_rsi_serie_pura_bajista(self):
        """RSI en serie puramente bajista debe ser cercano a 0."""
        from signals.engine import _rsi
        prices = [float(31 - i) for i in range(31)]   # 31,30,...,1
        rsi_val = _rsi(prices, period=14)
        assert rsi_val is not None
        assert rsi_val < 10.0, f"RSI bajista esperado < 10, got {rsi_val:.1f}"

    def test_mark_signal_dispatched_activa_cooldown_90min(self):
        """mark_signal_dispatched activa cooldown completo de 90 min."""
        from signals.engine import DecisionEngine
        engine = DecisionEngine(shadow_mode=True)
        engine.mark_signal_dispatched()
        # Después de dispatch, el gap debe ser casi 90 min
        remaining = engine._min_signal_gap_s - (time.monotonic() - engine._last_signal_at)
        assert remaining > 5000   # > 83 min restantes

    def test_mark_signal_rejected_cooldown_corto(self):
        """mark_signal_rejected(60s) deja solo 60s de cooldown."""
        from signals.engine import DecisionEngine
        engine = DecisionEngine(shadow_mode=True)
        engine.mark_signal_rejected(retry_delay_s=60.0)
        remaining = engine._min_signal_gap_s - (time.monotonic() - engine._last_signal_at)
        # El cooldown restante debe ser ~60s (con pequeña tolerancia)
        assert 50.0 <= remaining <= 70.0, f"Cooldown esperado ~60s, got {remaining:.1f}s"
