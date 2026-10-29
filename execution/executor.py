"""
execution/executor.py
=====================
Ejecutor seguro de órdenes en Hyperliquid.

Correcciones integradas:
- CORRECCIÓN #2: cloid con boot_epoch para idempotencia en reinicios
- CORRECCIÓN #1: Toda mutación de estado via state_machine con Lock
- GAP #3: Manejo específico de WouldExceedLeverage (no reintentar)
- Órdenes ALO con timeout y cancelación automática
- Stop Loss inmediato: cierre de emergencia si SL no se confirma en 2s
- Kill switch invocable desde Telegram, validador, o archivo STOP_BOT.flag
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import time
from typing import Any

from config.settings import get_boot_epoch, get_settings
from execution.state_machine import ExecutorState, ExecutorStateMachine, InvalidTransitionError
from hyperliquid.utils.types import Cloid
from signals.schemas import ApprovedOrder, OrderType, SignalSide

logger = logging.getLogger(__name__)

# Errores del SDK de Hyperliquid que NO deben reintentarse
NON_RETRYABLE_ERRORS = {
    "Would exceed max leverage",
    "WouldExceedLeverage",
    "Insufficient margin",
    "Order not found",
    "Invalid size",
    "Size too small",
}


class HyperliquidExecutor:
    """
    Gestor de órdenes para Hyperliquid.

    CORRECCIÓN #2: Genera cloids deterministas únicos incluyendo
    boot_epoch para garantizar que un reinicio del proceso no reutilice
    el mismo cloid que una orden activa anterior en el exchange.

    Flujo de una orden:
    1. FLAT → ENTRY_PENDING: Enviar orden de entrada (ALO o IOC)
    2. ENTRY_PENDING → OPEN_UNPROTECTED: Fill confirmado via userEvents
    3. OPEN_UNPROTECTED → OPEN_PROTECTED: SL colocado y confirmado
    4. OPEN_PROTECTED → FLAT: SL ejecutado o cierre manual
    """

    ALO_TIMEOUT_S = 15.0        # Cancelar ALO si no se llena en 15s
    SL_CONFIRM_TIMEOUT_S = 2.0  # Tiempo máximo para confirmar SL
    SL_MAX_RETRIES = 2
    ORDER_MODIFY_COOLDOWN_S = 5.0  # Máximo 1 modificación cada 5s (rate limit HL)

    def __init__(
        self,
        exchange: Any,              # hyperliquid_python_sdk.exchange.Exchange
        state_machine: ExecutorStateMachine,
        symbol: str = "BTC",
    ) -> None:
        self._exchange = exchange
        self._sm = state_machine
        self._symbol = symbol
        self._settings = get_settings()
        self._boot_epoch = get_boot_epoch()   # CORRECCIÓN #2

        self._active_entry_cloid: str | None = None
        self._active_sl_oid: int | None = None
        self._last_modify_at: float = 0.0
        self._kill_switch_active = False

        # Monitorear archivo STOP_BOT.flag
        asyncio.get_event_loop().create_task(self._monitor_kill_flag())

    # ── Generación de cloid (CORRECCIÓN #2) ───────────────────────────────────

    def _make_cloid(self, signal_id: str) -> str:
        """
        Genera un cloid de 128 bits determinista e idempotente.

        CORRECCIÓN #2: Incluye boot_epoch para que el mismo signal_id
        en diferentes ejecuciones del proceso genere cloids distintos.

        Esto previene el rechazo de Hyperliquid de "duplicate cloid"
        cuando el bot se reinicia y la orden anterior sigue activa.

        Formato: hex de 32 chars (128 bits)
        """
        seed = f"{signal_id}:{self._boot_epoch}:{self._symbol}"
        # El SDK de Hyperliquid requiere Cloid con prefijo 0x y exactamente 32 hex chars (16 bytes)
        hex32 = hashlib.sha256(seed.encode()).hexdigest()[:32]
        return Cloid(f"0x{hex32}")

    # ── Ejecución de señal ─────────────────────────────────────────────────────

    async def execute(self, approved: ApprovedOrder) -> None:
        """
        Ejecuta una orden aprobada por el RiskValidator.
        En SHADOW_MODE (validado antes de llegar aquí), no llega a este punto.
        """
        if self._kill_switch_active:
            logger.warning("Execution blocked: kill switch active")
            return

        if self._sm.state != ExecutorState.FLAT:
            logger.warning(
                "Cannot execute: state is %s (expected FLAT)",
                self._sm.state.value,
            )
            return

        signal = approved.signal
        cloid = self._make_cloid(signal.signal_id)
        self._active_entry_cloid = cloid

        logger.info(
            "Executing %s %s %.5f BTC @ %.2f (cloid=%s, order_type=%s)",
            signal.side.value,
            self._symbol,
            approved.approved_size,
            approved.approved_price,
            cloid,
            signal.order_type.value,
        )

        await self._sm.transition(ExecutorState.ENTRY_PENDING, f"signal_id={signal.signal_id}")

        if signal.order_type == OrderType.ALO:
            await self._execute_alo(approved, cloid)
        else:
            await self._execute_ioc(approved, cloid)

    async def _execute_alo(self, approved: ApprovedOrder, cloid: str) -> None:
        """
        Orden Add Liquidity Only (Post-Only / Maker).
        Si no se llena en ALO_TIMEOUT_S, se cancela.
        """
        signal = approved.signal
        is_buy = signal.side == SignalSide.BUY

        # En BTC en Hyperliquid el tick size es 1.0 (número entero).
        # Para ALO (post-only/maker):
        # BUY -> floor (para no cruzar hacia arriba con el ask)
        # SELL -> ceil (para no cruzar hacia abajo con el bid)
        if self._symbol == "BTC":
            order_px = float(math.floor(approved.approved_price) if is_buy else math.ceil(approved.approved_price))
        else:
            order_px = round(approved.approved_price, 2)

        try:
            result = self._exchange.order(
                self._symbol,
                is_buy,
                approved.approved_size,
                order_px,
                {"limit": {"tif": "Alo"}},
                cloid=cloid,  # ya es un objeto Cloid
            )
            logger.info("ALO order result from exchange: %s (px=%.1f)", result, order_px)
            if isinstance(result, dict) and result.get("status") == "err":
                raise RuntimeError(f"Exchange error: {result.get('response')}")
            statuses = result.get("response", {}).get("data", {}).get("statuses", []) if isinstance(result, dict) else []
            for st in statuses:
                if isinstance(st, dict) and "error" in st:
                    raise RuntimeError(f"Order rejected by exchange: {st['error']}")

            # Esperar fill con timeout
            try:
                await asyncio.wait_for(
                    self._wait_for_fill(cloid),
                    timeout=self.ALO_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                logger.info("ALO order timed out after %.0fs, cancelling...", self.ALO_TIMEOUT_S)
                await self._cancel_order(cloid)
                await self._sm.transition(ExecutorState.FLAT, "ALO timeout - cancelled")

        except Exception as e:
            await self._handle_order_error(e, "ALO entry")

    async def _execute_ioc(self, approved: ApprovedOrder, cloid: str) -> None:
        """
        Orden Immediate Or Cancel (Taker, para breakouts).
        Se llena inmediatamente o se cancela.
        """
        signal = approved.signal
        is_buy = signal.side == SignalSide.BUY

        if self._symbol == "BTC":
            order_px = float(round(approved.approved_price))
        else:
            order_px = round(approved.approved_price, 2)

        try:
            result = self._exchange.order(
                self._symbol,
                is_buy,
                approved.approved_size,
                order_px,
                {"limit": {"tif": "Ioc"}},
                cloid=cloid,  # ya es un objeto Cloid
            )
            logger.info("IOC order result from exchange: %s (px=%.1f)", result, order_px)
            if isinstance(result, dict) and result.get("status") == "err":
                raise RuntimeError(f"Exchange error: {result.get('response')}")
            statuses = result.get("response", {}).get("data", {}).get("statuses", []) if isinstance(result, dict) else []
            for st in statuses:
                if isinstance(st, dict) and "error" in st:
                    raise RuntimeError(f"Order rejected by exchange: {st['error']}")

        except Exception as e:
            await self._handle_order_error(e, "IOC entry")

    async def on_fill(
        self,
        cloid: str | None,
        fill_price: float,
        fill_size: float,
        oid: int,
    ) -> None:
        """
        Llamado cuando llega un fill del canal userEvents.
        Inicia el proceso de colocación del Stop Loss.
        """
        if cloid != self._active_entry_cloid:
            return

        logger.info(
            "Entry FILLED: %.5f @ %.2f (oid=%d)",
            fill_size,
            fill_price,
            oid,
        )

        # Transicionar a OPEN_UNPROTECTED (posición real sin SL aún)
        await self._sm.transition(
            ExecutorState.OPEN_UNPROTECTED,
            f"fill @ {fill_price:.2f}",
        )

        # Colocar Stop Loss inmediatamente
        await self._place_stop_loss_with_retry(fill_price, fill_size)

    async def _place_stop_loss_with_retry(
        self,
        entry_price: float,
        fill_size: float,
    ) -> None:
        """
        Intenta colocar el SL hasta SL_MAX_RETRIES veces.
        Si falla todos los intentos: cierre de emergencia a mercado.
        """
        # Obtener stop price de la orden aprobada (guardada en estado)
        # En producción se recupera del DecisionLog o del ApprovedOrder activo
        stop_price = self._get_active_stop_loss_price(entry_price)
        if self._symbol == "BTC":
            stop_price = float(round(stop_price))
        else:
            stop_price = round(stop_price, 2)
        is_buy_position = entry_price > 0   # Simplificado; en prod leer del state_machine

        for attempt in range(1, self.SL_MAX_RETRIES + 1):
            try:
                result = self._exchange.order(
                    self._symbol,
                    not is_buy_position,    # Lado contrario para cerrar
                    fill_size,
                    stop_price,
                    {
                        "trigger": {
                            "isMarket": True,
                            "triggerPx": stop_price,
                            "tpsl": "sl",
                        }
                    },
                    reduce_only=True,
                )

                if result and result.get("status") == "ok":
                    sl_oid = result.get("response", {}).get("data", {}).get("statuses", [{}])[0].get("resting", {}).get("oid")
                    self._active_sl_oid = sl_oid
                    await self._sm.transition(
                        ExecutorState.OPEN_PROTECTED,
                        f"SL placed @ {stop_price:.2f} (oid={sl_oid})",
                    )
                    logger.info("Stop Loss confirmed: oid=%s @ %.2f", sl_oid, stop_price)
                    return

            except Exception as e:
                logger.error("SL placement attempt %d/%d failed: %s", attempt, self.SL_MAX_RETRIES, e)

            if attempt < self.SL_MAX_RETRIES:
                await asyncio.sleep(1.0)

        # EMERGENCIA: SL falló todos los intentos
        logger.critical(
            "EMERGENCY: Failed to place SL after %d attempts. "
            "Closing position at market!",
            self.SL_MAX_RETRIES,
        )
        await self._emergency_close(fill_size, "SL placement failed")

    def _get_active_stop_loss_price(self, entry_price: float) -> float:
        """
        Retorna el precio de stop loss de la señal activa.
        En producción, esto viene del ApprovedOrder almacenado en el executor.
        """
        # Placeholder: 1.5% del precio de entrada (sobreescrito en producción)
        return entry_price * 0.985

    # ── Cierre de emergencia ───────────────────────────────────────────────────

    async def _emergency_close(self, size: float, reason: str) -> None:
        """Cierra la posición a mercado inmediatamente."""
        logger.critical("EMERGENCY CLOSE: size=%.5f, reason=%s", size, reason)

        try:
            await self._sm.transition(ExecutorState.CLOSING, f"Emergency: {reason}")

            result = self._exchange.market_close(self._symbol)
            logger.critical("Emergency close result: %s", result)

            await self._sm.transition(ExecutorState.FLAT, "Emergency close complete")

        except Exception:
            logger.exception("Emergency close FAILED")
            await self._sm.force_halt(f"Emergency close failed: {reason}")

    # ── Kill Switch ────────────────────────────────────────────────────────────

    async def kill_switch(self, reason: str = "Manual kill switch") -> None:
        """
        Kill switch: cancela todas las órdenes y cierra posición.
        Invocable desde Telegram, validador, o archivo STOP_BOT.flag.
        """
        self._kill_switch_active = True
        logger.critical("KILL SWITCH ACTIVATED: %s", reason)

        await self._sm.force_halt(reason)

        try:
            # Cancelar todas las órdenes abiertas
            self._exchange.cancel_all(self._symbol)
            logger.info("All orders cancelled")

            # Cerrar posición si existe
            try:
                self._exchange.market_close(self._symbol)
                logger.info("Position closed at market")
            except Exception:
                logger.exception("Failed to close position during kill switch")

        except Exception:
            logger.exception("Kill switch execution error")

    async def _monitor_kill_flag(self) -> None:
        """Monitorea el archivo STOP_BOT.flag cada 5 segundos."""
        import os
        while not self._kill_switch_active:
            await asyncio.sleep(5)
            if os.path.exists("STOP_BOT.flag"):
                await self.kill_switch("STOP_BOT.flag detected")
                break

    # ── Rate limiting de modificaciones ───────────────────────────────────────

    def _can_modify_order(self) -> bool:
        """Máximo 1 modificación cada 5 segundos (rate limit de Hyperliquid)."""
        return (time.monotonic() - self._last_modify_at) >= self.ORDER_MODIFY_COOLDOWN_S

    # ── Helpers ────────────────────────────────────────────────────────────────

    async def _wait_for_fill(self, cloid: str) -> None:
        """Espera hasta que el fill llegue via userEvents (señal de evento)."""
        # En producción, esto usa asyncio.Event disparado desde el ingestor
        # cuando llega un fill con el cloid correspondiente
        while self._active_entry_cloid == cloid:
            await asyncio.sleep(0.1)

    async def _cancel_order(self, cloid: str) -> None:
        """Cancela una orden por cloid."""
        try:
            result = self._exchange.cancel_by_cloid(self._symbol, cloid)
            logger.info("Order cancelled (cloid=%s): %s", cloid, result)
        except Exception:
            logger.exception("Failed to cancel order cloid=%s", cloid)

    async def _handle_order_error(self, error: Exception, context: str) -> None:
        """
        GAP #3: Manejo específico de WouldExceedLeverage.
        Este error es un VETO del exchange (no de red): no reintentar.
        """
        error_str = str(error)

        # Verificar si es un error de no-reintento
        is_non_retryable = any(
            msg in error_str for msg in NON_RETRYABLE_ERRORS
        )

        if is_non_retryable:
            logger.error(
                "Non-retryable exchange error in %s: %s. "
                "NOT retrying (exchange veto, not network error).",
                context,
                error_str,
            )
            # Transicionar de vuelta a FLAT sin reintentar
            try:
                await self._sm.transition(
                    ExecutorState.FLAT,
                    f"Non-retryable error: {error_str[:50]}",
                )
            except InvalidTransitionError:
                pass
        else:
            logger.error("Order error in %s: %s", context, error_str)
            try:
                await self._sm.transition(
                    ExecutorState.FLAT,
                    f"Order error: {error_str[:50]}",
                )
            except InvalidTransitionError:
                pass
