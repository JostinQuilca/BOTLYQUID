"""
tests/test_executor_execution.py
================================
Tests unitarios para HyperliquidExecutor:
- market_open en _execute_ioc con px y slippage
- self.last_order_filled en __init__, execute() y on_fill()
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock
import pytest

from execution.executor import HyperliquidExecutor
from execution.state_machine import ExecutorState, ExecutorStateMachine
from signals.schemas import ApprovedOrder, MarketRegime, OrderType, SignalProposal, SignalSide


def _make_sample_approved(order_type: OrderType = OrderType.IOC) -> ApprovedOrder:
    signal = SignalProposal(
        strategy="SmartMoneyFVG",
        side=SignalSide.BUY,
        order_type=order_type,
        proposed_price=76000.0,
        stop_loss=75800.0,
        take_profit=76440.0,
        regime=MarketRegime.RANGO,
        obi=0.20,
        cvd_1m=15.0,
        cvd_5m=30.0,
        basis_bps=2.0,
        oi_change_5m=0.5,
        estimated_hold_minutes=25.0,
        expected_r=2.2,
        confidence=0.85,
    )
    return ApprovedOrder(
        signal=signal,
        approved_size=0.0015,
        approved_price=76000.0,
        approved_stop_loss=75800.0,
        approved_take_profit=76440.0,
        risk_amount_usd=0.75,
        effective_leverage=2.28,
    )


class TestExecutorExecution:

    def test_initial_last_order_filled_is_false(self):
        sm = ExecutorStateMachine()
        mock_exchange = MagicMock()
        executor = HyperliquidExecutor(mock_exchange, sm, symbol="BTC")
        assert executor.last_order_filled is False

    @pytest.mark.asyncio
    async def test_execute_ioc_calls_market_open_and_sets_last_order_filled(self):
        sm = ExecutorStateMachine()
        mock_exchange = MagicMock()
        # Simular retorno exitoso de market_open con fill
        mock_exchange.market_open.return_value = {
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [
                        {
                            "filled": {
                                "oid": 123456,
                                "avgPx": 76001.0,
                                "totalSz": 0.0015,
                            }
                        }
                    ]
                },
            },
        }
        # Simular orden TP/SL
        mock_exchange.order.return_value = {
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [
                        {"resting": {"oid": 99999}}
                    ]
                },
            },
        }

        executor = HyperliquidExecutor(mock_exchange, sm, symbol="BTC")
        approved = _make_sample_approved(OrderType.IOC)

        await executor.execute(approved)

        # Verificar que market_open fue llamado con px y slippage=0.001
        mock_exchange.market_open.assert_called_once()
        call_kwargs = mock_exchange.market_open.call_args.kwargs
        assert call_kwargs["px"] == 76000.0
        assert call_kwargs["slippage"] == 0.001

        # Verificar que last_order_filled es True tras el fill
        assert executor.last_order_filled is True
        assert sm.state == ExecutorState.OPEN_PROTECTED

    @pytest.mark.asyncio
    async def test_execute_ioc_unfilled_keeps_last_order_filled_false(self):
        sm = ExecutorStateMachine()
        mock_exchange = MagicMock()
        # Simular retorno sin fill
        mock_exchange.market_open.return_value = {
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": []
                },
            },
        }

        executor = HyperliquidExecutor(mock_exchange, sm, symbol="BTC")
        approved = _make_sample_approved(OrderType.IOC)

        await executor.execute(approved)

        assert executor.last_order_filled is False
        assert sm.state == ExecutorState.FLAT

    @pytest.mark.asyncio
    async def test_on_fill_close_in_open_protected_calls_on_tp_sl_trigger(self):
        sm = ExecutorStateMachine()
        mock_exchange = MagicMock()
        mock_exchange.cancel.return_value = {"status": "ok"}
        mock_exchange.bulk_cancel.return_value = {"status": "ok"}
        mock_exchange.info.open_orders.return_value = [{"coin": "BTC", "oid": 555}]

        executor = HyperliquidExecutor(mock_exchange, sm, symbol="BTC")
        # Simular que ya está en OPEN_PROTECTED
        await sm.transition(ExecutorState.ENTRY_PENDING, "test")
        await sm.transition(ExecutorState.OPEN_PROTECTED, "test")
        executor._active_sl_oid = 111
        executor._active_tp_oid = 222
        executor._filled_size = 0.00136

        # Recibir fill de cierre
        close_fill_data = {
            "dir": "Close Short",
            "closedPnl": 0.10064,
            "px": 76197.0,
            "sz": 0.00136,
        }
        await executor.on_fill(None, 76197.0, 0.00136, 999, fill_data=close_fill_data)

        # Debe transicionar a FLAT y cancelar órdenes de protección residuales
        assert sm.state == ExecutorState.FLAT
        mock_exchange.cancel.assert_any_call("BTC", 111)
        mock_exchange.cancel.assert_any_call("BTC", 222)
        mock_exchange.bulk_cancel.assert_called_once_with([{"coin": "BTC", "oid": 555}])
        # _filled_size debe haberse reiniciado a 0, NO sumado
        assert executor._filled_size == 0.0

    @pytest.mark.asyncio
    async def test_reconcile_state_cleans_up_when_position_zero(self):
        sm = ExecutorStateMachine()
        mock_exchange = MagicMock()
        mock_exchange.cancel.return_value = {"status": "ok"}
        mock_exchange.bulk_cancel.return_value = {"status": "ok"}
        mock_exchange.info.open_orders.return_value = [{"coin": "BTC", "oid": 666}]

        executor = HyperliquidExecutor(mock_exchange, sm, symbol="BTC")
        # Poner en OPEN_PROTECTED
        await sm.transition(ExecutorState.ENTRY_PENDING, "test")
        await sm.transition(ExecutorState.OPEN_PROTECTED, "test")
        executor._active_sl_oid = 333

        # Reconciliación detecta que en el exchange la posición es 0.0
        await executor.reconcile_state(0.0)

        assert sm.state == ExecutorState.FLAT
        mock_exchange.cancel.assert_called_once_with("BTC", 333)
        mock_exchange.bulk_cancel.assert_called_once_with([{"coin": "BTC", "oid": 666}])

    @pytest.mark.asyncio
    async def test_on_fill_deduplicates_entry_fill_from_ws(self):
        sm = ExecutorStateMachine()
        mock_exchange = MagicMock()
        mock_exchange.order.return_value = {
            "status": "ok",
            "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 111}}, {"resting": {"oid": 222}}]}},
        }

        executor = HyperliquidExecutor(mock_exchange, sm, symbol="BTC")
        approved = _make_sample_approved(OrderType.IOC)
        executor._active_order = approved

        await sm.transition(ExecutorState.ENTRY_PENDING, "test")

        # 1. Primer fill (REST response)
        entry_fill = {"dir": "Open Long", "closedPnl": 0.0, "avgPx": 76000.0, "totalSz": 0.001}
        await executor.on_fill("0x123", 76000.0, 0.001, 8888, fill_data=entry_fill)

        assert sm.state == ExecutorState.OPEN_PROTECTED
        assert executor._filled_size == 0.001

        # 2. Segundo fill idéntico desde WebSocket userEvents
        await executor.on_fill("0x123", 76000.0, 0.001, 8888, fill_data=entry_fill)

        # Debe ignorarse: _filled_size NO debe duplicarse a 0.002
        assert executor._filled_size == 0.001

    @pytest.mark.asyncio
    async def test_manage_open_position_break_even_covers_fees(self):
        sm = ExecutorStateMachine()
        mock_exchange = MagicMock()
        mock_exchange.order.return_value = {
            "status": "ok",
            "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 999}}]}},
        }
        mock_exchange.cancel.return_value = {"status": "ok"}

        executor = HyperliquidExecutor(mock_exchange, sm, symbol="BTC")
        approved = _make_sample_approved(OrderType.IOC)  # BUY @ 76000, SL=75500 (dist=500)
        executor._active_order = approved
        executor._filled_size = 0.001
        executor._active_sl_oid = 123
        await sm.transition(ExecutorState.ENTRY_PENDING, "test")
        await sm.transition(ExecutorState.OPEN_PROTECTED, "test")

        # Mock state con precio a +1.0R (mark_price = 76000 + 500 = 76500)
        state = MagicMock()
        state.account.position_entry_price = 76000.0
        state.asset_ctx.mark_price = 76500.0

        await executor.manage_open_position(state)

        assert executor._moved_to_be is True
        # be_price para LONG debe ser entry_price * 1.0010 = 76000 * 1.0010 = 76076.0
        mock_exchange.order.assert_called_once()
        args, kwargs = mock_exchange.order.call_args
        placed_price = args[3]
        assert placed_price == 76076.0

    @pytest.mark.asyncio
    async def test_adopt_existing_position_transitions_to_open_protected(self):
        sm = ExecutorStateMachine()
        mock_exchange = MagicMock()
        executor = HyperliquidExecutor(mock_exchange, sm, symbol="BTC")

        assert sm.state == ExecutorState.FLAT
        await executor.adopt_existing_position(
            signed_size=-0.00028,
            entry_price=75626.0,
            active_tp_px=74422.0,
            active_sl_px=76238.0,
            tp_oid=598086002267,
            sl_oid=598085996887,
        )

        assert sm.state == ExecutorState.OPEN_PROTECTED
        assert executor._filled_size == 0.00028
        assert executor._active_tp_oid == 598086002267
        assert executor._active_sl_oid == 598085996887

        # Test manage_open_position without _active_order using state.account
        state = MagicMock()
        state.account.has_position = True
        state.account.position_size = -0.00028
        state.account.position_entry_price = 75626.0
        state.account.active_sl_px = 76238.0
        # Mark price moved 0.20% in favor (SHORT -> price down to 75450)
        state.asset_ctx.mark_price = 75450.0

        mock_exchange.order.return_value = {
            "status": "ok",
            "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 888}}]}},
        }
        mock_exchange.cancel.return_value = {"status": "ok"}

        await executor.manage_open_position(state)
        assert executor._moved_to_be is True
        mock_exchange.order.assert_called_once()
        args, kwargs = mock_exchange.order.call_args
        assert args[3] == float(round(75626.0 * 0.9990))
