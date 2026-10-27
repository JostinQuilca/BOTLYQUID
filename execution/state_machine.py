"""
execution/state_machine.py
===========================
Máquina de estados del ejecutor de órdenes.

Correcciones integradas:
- CORRECCIÓN #1: asyncio.Lock en todas las transiciones de estado.
  Sin el lock, el evento fill del WebSocket y el ejecutor pueden
  modificar el estado simultáneamente causando corrupción silenciosa.
- Transiciones válidas explícitas (no cualquier → cualquier)
- Log de toda transición para auditabilidad
"""
from __future__ import annotations

import asyncio
import logging
import time
from enum import Enum
from typing import Callable

logger = logging.getLogger(__name__)


class ExecutorState(str, Enum):
    FLAT = "FLAT"                         # Sin posición, sin órdenes
    ENTRY_PENDING = "ENTRY_PENDING"       # Orden de entrada enviada, no llenada
    OPEN_PROTECTED = "OPEN_PROTECTED"     # Posición abierta + SL confirmado
    OPEN_UNPROTECTED = "OPEN_UNPROTECTED" # Posición abierta, SL NO confirmado (EMERGENCIA)
    CLOSING = "CLOSING"                   # Cerrando posición
    HALTED = "HALTED"                     # Kill switch activo, sin operaciones


# Grafo de transiciones permitidas
VALID_TRANSITIONS: dict[ExecutorState, set[ExecutorState]] = {
    ExecutorState.FLAT: {ExecutorState.ENTRY_PENDING, ExecutorState.HALTED},
    ExecutorState.ENTRY_PENDING: {
        ExecutorState.OPEN_PROTECTED,
        ExecutorState.OPEN_UNPROTECTED,
        ExecutorState.FLAT,               # Orden cancelada/expirada
        ExecutorState.HALTED,
    },
    ExecutorState.OPEN_PROTECTED: {
        ExecutorState.CLOSING,
        ExecutorState.OPEN_UNPROTECTED,   # SL fue cancelado externamente
        ExecutorState.FLAT,               # SL ejecutado
        ExecutorState.HALTED,
    },
    ExecutorState.OPEN_UNPROTECTED: {
        ExecutorState.OPEN_PROTECTED,     # SL recolocado exitosamente
        ExecutorState.CLOSING,            # Cierre de emergencia
        ExecutorState.FLAT,
        ExecutorState.HALTED,
    },
    ExecutorState.CLOSING: {
        ExecutorState.FLAT,
        ExecutorState.HALTED,
    },
    ExecutorState.HALTED: set(),          # Estado terminal: no hay transiciones automáticas
}


class InvalidTransitionError(Exception):
    pass


class ExecutorStateMachine:
    """
    Máquina de estados thread-safe para el ejecutor.

    CORRECCIÓN #1 (Race Condition):
    Usa asyncio.Lock para garantizar que solo una corutina modifica
    el estado a la vez.

    Escenario del bug sin lock:
    - Tarea A: _place_stop_loss() → verifica estado == ENTRY_PENDING
    - Tarea B: _on_fill() llega del WebSocket → modifica estado a OPEN_UNPROTECTED
    - Tarea A: (sin lock) → transiciona a OPEN_PROTECTED aunque el SL no está puesto
    - Resultado: Posición REAL sin SL, bot cree que está protegido → PÉRDIDA CATASTRÓFICA
    """

    def __init__(self) -> None:
        self._state: ExecutorState = ExecutorState.FLAT
        self._lock: asyncio.Lock = asyncio.Lock()   # ← CORRECCIÓN #1
        self._state_entered_at: float = time.monotonic()
        self._transition_callbacks: list[Callable[[ExecutorState, ExecutorState, str], None]] = []
        self._history: list[tuple[float, ExecutorState, ExecutorState, str]] = []

    @property
    def state(self) -> ExecutorState:
        return self._state

    @property
    def time_in_state_s(self) -> float:
        return time.monotonic() - self._state_entered_at

    def register_callback(
        self,
        callback: Callable[[ExecutorState, ExecutorState, str], None],
    ) -> None:
        """Registra un callback (from_state, to_state, reason) para cada transición."""
        self._transition_callbacks.append(callback)

    async def transition(
        self,
        new_state: ExecutorState,
        reason: str = "",
    ) -> None:
        """
        Transiciona al nuevo estado de forma atómica.

        CORRECCIÓN #1: async with self._lock garantiza exclusión mutua.
        Si dos corutinas llaman a transition() simultáneamente, una espera
        a que la otra termine antes de verificar y mutar el estado.
        """
        async with self._lock:
            from_state = self._state

            # Verificar transición permitida
            if new_state not in VALID_TRANSITIONS.get(from_state, set()):
                raise InvalidTransitionError(
                    f"Invalid state transition: {from_state.value} → {new_state.value} "
                    f"(reason: {reason}). "
                    f"Valid from {from_state.value}: "
                    f"{[s.value for s in VALID_TRANSITIONS.get(from_state, set())]}"
                )

            self._state = new_state
            self._state_entered_at = time.monotonic()

            # Log de auditoría
            log_msg = (
                f"State: {from_state.value} → {new_state.value}"
                + (f" [{reason}]" if reason else "")
            )
            if new_state in (ExecutorState.OPEN_UNPROTECTED, ExecutorState.HALTED):
                logger.critical(log_msg)
            else:
                logger.info(log_msg)

            # Historial (últimas 50 transiciones)
            self._history.append((time.monotonic(), from_state, new_state, reason))
            if len(self._history) > 50:
                self._history.pop(0)

        # Callbacks fuera del lock (evitar deadlocks si el callback llama a transition)
        for cb in self._transition_callbacks:
            try:
                cb(from_state, new_state, reason)
            except Exception:
                logger.exception("State transition callback error")

    async def force_halt(self, reason: str) -> None:
        """
        Fuerza la transición a HALTED desde cualquier estado.
        Usado por el kill switch.
        """
        async with self._lock:
            from_state = self._state
            self._state = ExecutorState.HALTED
            self._state_entered_at = time.monotonic()
            logger.critical("FORCE HALT: %s → HALTED [%s]", from_state.value, reason)
            self._history.append((time.monotonic(), from_state, ExecutorState.HALTED, reason))

    def get_history(self) -> list[tuple[float, str, str, str]]:
        return [
            (ts, f.value, t.value, r)
            for ts, f, t, r in self._history
        ]
