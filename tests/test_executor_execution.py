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
