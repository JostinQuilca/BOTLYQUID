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
        alerter: Any = None,
    ) -> None:
        self._exchange = exchange
        self._sm = state_machine
        self._symbol = symbol
        self._alerter = alerter
        self._settings = get_settings()
        self._boot_epoch = get_boot_epoch()   # CORRECCIÓN #2

        self._active_order: ApprovedOrder | None = None
        self._active_entry_cloid: str | None = None
        self._active_sl_oid: int | None = None
        self._active_tp_oid: int | None = None
        self._fill_event = asyncio.Event()
        self._last_modify_at: float = 0.0
        self._kill_switch_active = False
        self._protect_lock = asyncio.Lock()  # FIX: evita colocar SL/TP en paralelo
                                              # si on_fill() y ensure_protected()
                                              # (reconciliador) se disparan a la vez

        # Monitorear archivo STOP_BOT.flag
        asyncio.get_event_loop().create_task(self._monitor_kill_flag())

    def set_alerter(self, alerter: Any) -> None:
        """Asigna o actualiza el alerter de Telegram."""
        self._alerter = alerter

    # ── Generación de cloid (CORRECCIÓN #2) ───────────────────────────────────

    def _make_cloid(self, signal_id: str) -> str:
        """
        Genera un cloid de 128 bits determinista e idempotente.

        CORRECCIÓN #2: Incluye boot_epoch para que el mismo signal_id
        en diferentes ejecuciones del proceso genere cloids distintos.

        Formato: hex de 32 chars (128 bits) con prefijo 0x para el SDK
        """
        seed = f"{signal_id}:{self._boot_epoch}:{self._symbol}"
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
        self._active_order = approved
        self._active_entry_cloid = cloid
        self._fill_event.clear()

        logger.info(
            "Executing %s %s %.5f BTC @ %.2f (cloid=%s, order_type=%s, TP=%.2f, SL=%.2f)",
            signal.side.value,
            self._symbol,
            approved.approved_size,
            approved.approved_price,
            cloid,
            signal.order_type.value,
            approved.approved_take_profit,
            approved.approved_stop_loss,
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
                cloid=cloid,
            )
            logger.info("ALO order result from exchange: %s (px=%.1f)", result, order_px)
            if isinstance(result, dict) and result.get("status") == "err":
                raise RuntimeError(f"Exchange error: {result.get('response')}")

            statuses = result.get("response", {}).get("data", {}).get("statuses", []) if isinstance(result, dict) else []
            for st in statuses:
                if isinstance(st, dict):
                    if "error" in st:
                        raise RuntimeError(f"Order rejected by exchange: {st['error']}")
                    if "filled" in st:
                        filled_data = st["filled"]
                        fill_px = float(filled_data.get("avgPx", order_px))
                        fill_sz = float(filled_data.get("totalSz", approved.approved_size))
                        oid = int(filled_data.get("oid", 0))
                        await self.on_fill(cloid, fill_px, fill_sz, oid)
                        return

            # Esperar fill con timeout via asyncio.Event
            try:
                await asyncio.wait_for(
                    self._fill_event.wait(),
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
                cloid=cloid,
            )
            logger.info("IOC order result from exchange: %s (px=%.1f)", result, order_px)
            if isinstance(result, dict) and result.get("status") == "err":
                raise RuntimeError(f"Exchange error: {result.get('response')}")

            statuses = result.get("response", {}).get("data", {}).get("statuses", []) if isinstance(result, dict) else []
            filled = False
            for st in statuses:
                if isinstance(st, dict):
                    if "error" in st:
                        raise RuntimeError(f"Order rejected by exchange: {st['error']}")
                    if "filled" in st:
                        filled = True
                        filled_data = st["filled"]
                        fill_px = float(filled_data.get("avgPx", order_px))
                        fill_sz = float(filled_data.get("totalSz", approved.approved_size))
                        oid = int(filled_data.get("oid", 0))
                        await self.on_fill(cloid, fill_px, fill_sz, oid)
                        return

            if not filled:
                logger.info("IOC order did not fill immediately — returning to FLAT")
                await self._sm.transition(ExecutorState.FLAT, "IOC cancelled (no fill)")

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
        Llamado cuando llega un fill (del REST o del canal userEvents).
        Inicia la colocación automática de Stop Loss y Take Profit.
        """
        if self._sm.state in (ExecutorState.OPEN_PROTECTED, ExecutorState.OPEN_UNPROTECTED):
            logger.debug("on_fill ignored: already in state %s", self._sm.state.value)
            return

        logger.info(
            "Entry FILLED: %.5f @ %.2f (oid=%d, cloid=%s)",
            fill_size,
            fill_price,
            oid,
            cloid,
        )

        self._fill_event.set()

        # FIX #A: el fill puede llegar cuando el estado YA NO es ENTRY_PENDING
        # (ej.: el timeout de 15s de la ALO ya hizo ENTRY_PENDING -> FLAT justo
        # antes de que llegara este evento tardío del WebSocket). Antes, la
        # transición directa a OPEN_UNPROTECTED fallaba con InvalidTransitionError,
        # esa excepción se perdía en el logger.exception() de _message_loop(), y
        # _place_tp_sl_with_retry() NUNCA se ejecutaba: la posición quedaba real
        # y abierta en el exchange, pero completamente desprotegida.
        try:
            if self._sm.state == ExecutorState.FLAT:
                logger.warning(
                    "Fill llegó con estado FLAT (probable timeout/race de la ALO). "
                    "Recuperando estado antes de proteger la posición."
                )
                await self._sm.transition(
                    ExecutorState.ENTRY_PENDING,
                    "late fill recovery",
                )
            await self._sm.transition(
                ExecutorState.OPEN_UNPROTECTED,
                f"fill @ {fill_price:.2f}",
            )
        except InvalidTransitionError as e:
            # No debería pasar nunca, pero si pasa: la posición YA existe en el
            # exchange. Proteger la posición real importa más que la limpieza
            # del estado interno, así que seguimos adelante igual.
            logger.critical(
                "Estado inconsistente tras un fill real (%s). "
                "Colocando SL/TP de todas formas para no dejar la posición desnuda.",
                e,
            )
            if self._alerter:
                await self._alerter.send(
                    f"⚠️ *ESTADO INCONSISTENTE TRAS FILL*\n"
                    f"Fill real: `{fill_size:.5f} BTC @ ${fill_price:,.2f}`\n"
                    f"Estado interno: `{self._sm.state.value}`\n"
                    f"Colocando SL/TP de todos modos...",
                    alert_type="CRITICAL",
                )

        # Colocar Stop Loss y Take Profit automáticamente
        async with self._protect_lock:
            await self._place_tp_sl_with_retry(fill_price, fill_size)

    # ── Auto-sanación (llamado por el reconciliador) ──────────────────────────

    async def ensure_protected(self, entry_price: float, signed_size: float) -> None:
        """
        Fuerza la colocación de SL y TP cuando el reconciliador detecta
        que faltan en el exchange, incluso si el estado decía OPEN_PROTECTED.
        """
        if self._kill_switch_active:
            logger.info("ensure_protected: kill switch activo, no se protege")
            return

        size = abs(signed_size)
        is_long_fallback = signed_size > 0

        logger.critical(
            "RECONCILE: posición real de %.6f %s SIN protección completa detectada "
            "(entry=%.2f). Forzando colocación de SL/TP.",
            signed_size, self._symbol, entry_price,
        )

        try:
            # Si creía estar protegido pero el exchange no tiene las órdenes, regresar a OPEN_UNPROTECTED
            if self._sm.state == ExecutorState.OPEN_PROTECTED:
                await self._sm.transition(
                    ExecutorState.OPEN_UNPROTECTED, "reconcile: missing TP/SL on exchange"
                )
            elif self._sm.state == ExecutorState.FLAT:
                await self._sm.transition(
                    ExecutorState.ENTRY_PENDING, "reconcile: unprotected position found"
                )
                await self._sm.transition(
                    ExecutorState.OPEN_UNPROTECTED, "reconcile: unprotected position found"
                )
            elif self._sm.state == ExecutorState.ENTRY_PENDING:
                await self._sm.transition(
                    ExecutorState.OPEN_UNPROTECTED, "reconcile: unprotected position found"
                )
        except InvalidTransitionError as e:
            logger.critical(
                "No se pudo preparar la máquina de estados (%s). Se intenta igual colocar SL/TP.", e
            )

        async with self._protect_lock:
            # Limpiar órdenes huérfanas previas antes de recolocar el par TP/SL limpio
            try:
                self._exchange.cancel_all(self._symbol)
            except Exception:
                pass
            await self._place_tp_sl_with_retry(entry_price, size, is_long_fallback=is_long_fallback)

    async def _place_tp_sl_with_retry(
        self,
        entry_price: float,
        fill_size: float,
        is_long_fallback: bool = True,
    ) -> None:
        """
        Coloca AUTOMÁTICAMENTE el Stop Loss y el Take Profit en Hyperliquid
        y verifica que no exista rechazo dentro de 'statuses'.
        """
        if self._sm.state == ExecutorState.OPEN_PROTECTED:
            logger.debug("_place_tp_sl_with_retry: ya protegida, nada que hacer")
            return

        if self._active_order is None:
            is_buy_position = is_long_fallback
            if is_buy_position:
                stop_price = entry_price * 0.985
                tp_price = entry_price * 1.02
            else:
                stop_price = entry_price * 1.015
                tp_price = entry_price * 0.98
        else:
            is_buy_position = (self._active_order.signal.side == SignalSide.BUY)
            stop_price = self._active_order.approved_stop_loss
            tp_price = self._active_order.approved_take_profit

        if self._symbol == "BTC":
            stop_price = float(round(stop_price))
            tp_price = float(round(tp_price))
        else:
            stop_price = round(stop_price, 2)
            tp_price = round(tp_price, 2)

        sl_side = not is_buy_position
        tp_side = not is_buy_position

        for attempt in range(1, self.SL_MAX_RETRIES + 1):
            try:
                # 1. Colocar Stop Loss
                sl_res = self._exchange.order(
                    self._symbol,
                    sl_side,
                    fill_size,
                    stop_price,
                    {"trigger": {"isMarket": True, "triggerPx": stop_price, "tpsl": "sl"}},
                    reduce_only=True,
                )
                logger.info("SL order result (attempt %d): %s @ %.1f", attempt, sl_res, stop_price)

                # 2. Colocar Take Profit
                tp_res = self._exchange.order(
                    self._symbol,
                    tp_side,
                    fill_size,
                    tp_price,
                    {"trigger": {"isMarket": True, "triggerPx": tp_price, "tpsl": "tp"}},
                    reduce_only=True,
                )
                logger.info("TP order result (attempt %d): %s @ %.1f", attempt, tp_res, tp_price)

                # Verificar que AMBAS respuestas sean 'ok' y NO contengan 'error' en statuses
                if not (isinstance(sl_res, dict) and sl_res.get("status") == "ok"):
                    raise RuntimeError(f"Exchange SL error: {sl_res}")
                if not (isinstance(tp_res, dict) and tp_res.get("status") == "ok"):
                    raise RuntimeError(f"Exchange TP error: {tp_res}")

                sl_data = sl_res.get("response", {}) if isinstance(sl_res.get("response"), dict) else {}
                tp_data = tp_res.get("response", {}) if isinstance(tp_res.get("response"), dict) else {}

                sl_statuses = sl_data.get("data", {}).get("statuses", [{}])
                tp_statuses = tp_data.get("data", {}).get("statuses", [{}])

                sl_err = sl_statuses[0].get("error") if sl_statuses and isinstance(sl_statuses[0], dict) else "No status"
                tp_err = tp_statuses[0].get("error") if tp_statuses and isinstance(tp_statuses[0], dict) else "No status"

                if sl_err or tp_err:
                    raise RuntimeError(f"Rechazo en trigger orders -> SL_err={sl_err}, TP_err={tp_err}")

                self._active_sl_oid = sl_statuses[0].get("resting", {}).get("oid") if sl_statuses else None
                self._active_tp_oid = tp_statuses[0].get("resting", {}).get("oid") if tp_statuses else None

                await self._sm.transition(
                    ExecutorState.OPEN_PROTECTED,
                    f"TP/SL placed: TP={tp_price:.1f}, SL={stop_price:.1f}",
                )
                logger.info(
                    "Position PROTECTED: TP=%.1f (oid=%s), SL=%.1f (oid=%s)",
                    tp_price, self._active_tp_oid,
                    stop_price, self._active_sl_oid,
                )

                if self._alerter:
                    side_lbl = "LONG 🟢" if is_buy_position else "SHORT 🔴"
                    notional = fill_size * entry_price
                    await self._alerter.send(
                        f"✅ *POSICIÓN ABIERTA Y PROTEGIDA*\n"
                        f"Lado: {side_lbl}\n"
                        f"Tamaño: `{fill_size:.5f} BTC` (~${notional:.2f} USDC)\n"
                        f"Entrada: `${entry_price:,.2f}`\n"
                        f"🎯 Take Profit: `${tp_price:,.2f}`\n"
                        f"🛑 Stop Loss: `${stop_price:,.2f}`",
                        alert_type="FILL",
                    )
                return

            except Exception as e:
                logger.error("TP/SL placement attempt %d/%d failed: %s", attempt, self.SL_MAX_RETRIES, e)

            if attempt < self.SL_MAX_RETRIES:
                await asyncio.sleep(1.0)

        # EMERGENCIA: SL falló todos los intentos
        logger.critical(
            "EMERGENCY: Failed to place SL/TP after %d attempts. Closing position at market!",
            self.SL_MAX_RETRIES,
        )
        await self._emergency_close(fill_size, "TP/SL placement failed")

    async def on_position_closed(self, reason: str = "TP/SL hit") -> None:
        """
        Llamado cuando la posición se cierra en el exchange (por TP, SL o cierre manual).
        Cancela cualquier orden residual y regresa la máquina de estados a FLAT.
        """
        logger.info("Position closed (%s). Cleaning up residual orders...", reason)
        try:
            self._exchange.cancel_all(self._symbol)
            logger.info("Residual orders cancelled for %s", self._symbol)
        except Exception:
            logger.exception("Failed to cancel residual orders on position close")

        self._active_order = None
        self._active_entry_cloid = None
        self._active_sl_oid = None
        self._active_tp_oid = None

        if self._sm.state not in (ExecutorState.FLAT, ExecutorState.HALTED):
            try:
                await self._sm.transition(ExecutorState.FLAT, reason)
            except InvalidTransitionError:
                pass

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
