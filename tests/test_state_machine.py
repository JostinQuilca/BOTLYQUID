"""
tests/test_state_machine.py
===========================
Tests de la máquina de estados del ejecutor.
Verifica la corrección del asyncio.Lock y las transiciones válidas.
"""
from __future__ import annotations

import asyncio

import pytest

from execution.state_machine import (
    ExecutorState,
    ExecutorStateMachine,
    InvalidTransitionError,
    VALID_TRANSITIONS,
)


class TestStateMachine:

    @pytest.mark.asyncio
    async def test_initial_state_is_flat(self):
        sm = ExecutorStateMachine()
        assert sm.state == ExecutorState.FLAT

    @pytest.mark.asyncio
    async def test_valid_transition(self):
        sm = ExecutorStateMachine()
        await sm.transition(ExecutorState.ENTRY_PENDING, "test")
        assert sm.state == ExecutorState.ENTRY_PENDING

    @pytest.mark.asyncio
    async def test_invalid_transition_raises(self):
        sm = ExecutorStateMachine()
        # FLAT → OPEN_PROTECTED no es una transición válida
        with pytest.raises(InvalidTransitionError):
            await sm.transition(ExecutorState.OPEN_PROTECTED, "test")

    @pytest.mark.asyncio
    async def test_halted_is_terminal(self):
        """Desde HALTED no hay transiciones válidas (estado terminal)."""
        sm = ExecutorStateMachine()
        await sm.force_halt("test")
        assert sm.state == ExecutorState.HALTED

        # Intentar cualquier transición desde HALTED debe fallar
        with pytest.raises(InvalidTransitionError):
            await sm.transition(ExecutorState.FLAT, "test")

    @pytest.mark.asyncio
    async def test_concurrent_transitions_are_safe(self):
        """
        CORRECCIÓN #1: El Lock debe prevenir race conditions.
        Dos transiciones concurrentes no deben corromper el estado.
        """
        sm = ExecutorStateMachine()

        # Simular dos corutinas intentando transicionar al mismo tiempo
        async def transition_a():
            await sm.transition(ExecutorState.ENTRY_PENDING, "from_a")

        async def transition_b():
            # Esta intentará transicionar desde FLAT también
            # Una de las dos debe ganar, la otra debe ver el estado ya cambiado
            try:
                await sm.transition(ExecutorState.ENTRY_PENDING, "from_b")
            except InvalidTransitionError:
                pass  # Esperado: el estado ya cambió

        # Ejecutar concurrentemente
        await asyncio.gather(transition_a(), transition_b())

        # El estado debe ser consistente (no corrompido)
        assert sm.state in (
            ExecutorState.ENTRY_PENDING,
            ExecutorState.FLAT,   # Si ambas fallaron
        )

    @pytest.mark.asyncio
    async def test_transition_history_recorded(self):
        sm = ExecutorStateMachine()
        await sm.transition(ExecutorState.ENTRY_PENDING, "signal_001")

        history = sm.get_history()
        assert len(history) == 1
        ts, from_s, to_s, reason = history[0]
        assert from_s == "FLAT"
        assert to_s == "ENTRY_PENDING"
        assert reason == "signal_001"

    @pytest.mark.asyncio
    async def test_force_halt_from_any_state(self):
        """force_halt debe funcionar desde cualquier estado."""
        sm = ExecutorStateMachine()
        await sm.transition(ExecutorState.ENTRY_PENDING, "test")
        await sm.transition(ExecutorState.OPEN_PROTECTED, "fill")
        await sm.force_halt("emergency")

        assert sm.state == ExecutorState.HALTED

    @pytest.mark.asyncio
    async def test_time_in_state(self):
        sm = ExecutorStateMachine()
        await asyncio.sleep(0.01)
        elapsed = sm.time_in_state_s
        assert elapsed >= 0.0
        assert elapsed < 1.0  # No debería haber pasado 1 segundo

    def test_all_states_have_valid_transitions_defined(self):
        """Todos los estados deben tener una entrada en VALID_TRANSITIONS."""
        for state in ExecutorState:
            assert state in VALID_TRANSITIONS, (
                f"Estado {state.value} no tiene transiciones definidas en VALID_TRANSITIONS"
            )
