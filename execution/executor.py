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
        self._filled_size: float = 0.0       # Acumulador de partial fills
        self._moved_to_be: bool = False       # Flag de break-even ya aplicado
        self._entry_time: float = 0.0         # Tiempo de apertura de posición
        self.last_order_filled: bool = False  # Flag indicando si la última orden se llenó
        self._processed_fill_keys: set[str] = set()  # v7: dedup fills REST vs WS

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
        self.last_order_filled = False
        self._processed_fill_keys.clear()  # v7: limpiar dedup al inicio de cada trade

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
        Ancla el precio al l2_book actual para garantizar Post-Only.
        """
        signal = approved.signal
        is_buy = signal.side == SignalSide.BUY

        # FIX precio ALO: anclar al l2_book para nunca cruzar como Taker
        from data.state import market_state
        best_bid = market_state.l2_book.bids[0].price if market_state.l2_book and market_state.l2_book.bids else approved.approved_price
        best_ask = market_state.l2_book.asks[0].price if market_state.l2_book and market_state.l2_book.asks else approved.approved_price

        if is_buy:
            # Post-Only BUY: nunca >= best_ask
            raw_px = min(approved.approved_price, best_bid, best_ask - (1.0 if self._symbol == "BTC" else 0.01))
            order_px = float(math.floor(raw_px)) if self._symbol == "BTC" else round(raw_px, 2)
        else:
            # Post-Only SELL: nunca <= best_bid
            raw_px = max(approved.approved_price, best_ask, best_bid + (1.0 if self._symbol == "BTC" else 0.01))
            order_px = float(math.ceil(raw_px)) if self._symbol == "BTC" else round(raw_px, 2)

        try:
            result = await asyncio.to_thread(
                self._exchange.order,
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
                        await self.on_fill(cloid, fill_px, fill_sz, oid, fill_data=filled_data)
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
                if self._alerter:
                    await self._alerter.send(
                        f"⏱ *ORDEN ALO CANCELADA*\n"
                        f"Timeout de {self.ALO_TIMEOUT_S:.0f}s sin fill en Hyperliquid.\n"
                        f"Capital intacto. Buscando nueva señal...",
                        alert_type="INFO",
                    )

        except Exception as e:
            await self._handle_order_error(e, "ALO entry")

    async def _execute_ioc(self, approved: ApprovedOrder, cloid: str) -> None:
        """
        Orden Immediate Or Cancel (Taker con market_open nativo).
        Se llena inmediatamente o se cancela.
        """
        signal = approved.signal
        is_buy = signal.side == SignalSide.BUY

        try:
            result = await asyncio.to_thread(
                self._exchange.market_open,
                self._symbol,
                is_buy,
                approved.approved_size,
                px=approved.approved_price,
                slippage=0.001,
                cloid=cloid,
            )
            logger.info("IOC/Market order result from exchange: %s (px=%.1f)", result, approved.approved_price)
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
                        fill_px = float(filled_data.get("avgPx", approved.approved_price))
                        fill_sz = float(filled_data.get("totalSz", approved.approved_size))
                        oid = int(filled_data.get("oid", 0))
                        await self.on_fill(cloid, fill_px, fill_sz, oid, fill_data=filled_data)
                        return

            if not filled:
                logger.info("IOC order did not fill immediately — returning to FLAT")
                await self._sm.transition(ExecutorState.FLAT, "IOC cancelled (no fill)")
                if self._alerter:
                    await self._alerter.send(
                        f"⚠️ *ORDEN NO LLENADA*\n"
                        f"La orden IOC para {self._symbol} no se completó en el exchange.\n"
                        f"Capital intacto. Buscando nueva oportunidad...",
                        alert_type="WARNING",
                    )

        except Exception as e:
            await self._handle_order_error(e, "IOC entry")

    async def on_fill(
        self,
        cloid: str | None,
        fill_price: float,
        fill_size: float,
        oid: int,
        fill_data: dict[str, Any] | None = None,
    ) -> None:
        """
        Llamado cuando llega un fill (del REST o del canal userEvents).
        Inicia la colocación automática de Stop Loss y Take Profit.
        """
        fill_data = fill_data or {}
        dir_str = str(fill_data.get("dir", ""))
        closed_pnl = float(fill_data.get("closedPnl", 0.0) or 0.0)
        is_closing_fill = dir_str.startswith("Close") or closed_pnl != 0.0

        # v7: deduplicar fills de entrada (REST response + WS userEvents envían el mismo fill)
        fill_key = f"{oid}:{fill_size:.5f}:{'close' if is_closing_fill else 'open'}"
        if not is_closing_fill and oid > 0 and fill_key in self._processed_fill_keys:
            logger.debug("Ignorando fill de entrada duplicado (REST+WS): %s", fill_key)
            return
        if oid > 0:
            self._processed_fill_keys.add(fill_key)

        if self._sm.state == ExecutorState.OPEN_PROTECTED and is_closing_fill:
            logger.info(
                "Closing fill detected in OPEN_PROTECTED (dir='%s', closedPnl=%.4f). Calling on_tp_sl_trigger...",
                dir_str,
                closed_pnl,
            )
            await self.on_tp_sl_trigger("TP_OR_SL", fill_price)
            return

        if self._sm.state in (ExecutorState.OPEN_PROTECTED, ExecutorState.OPEN_UNPROTECTED):
            # Partial fill adicional: acumular tamaño y re-proteger
            self._filled_size = getattr(self, "_filled_size", 0.0) + fill_size
            logger.warning(
                "Additional partial fill received (+%.5f BTC, total=%.5f BTC). Updating TP/SL...",
                fill_size, self._filled_size,
            )
            if self._active_order:
                signed = self._filled_size if self._active_order.signal.side == SignalSide.BUY else -self._filled_size
                await self.ensure_protected(fill_price, signed)
            return

        logger.info(
            "Entry FILLED: %.5f @ %.2f (oid=%d, cloid=%s)",
            fill_size,
            fill_price,
            oid,
            cloid,
        )

        self._fill_event.set()
        self._filled_size = fill_size
        self._entry_time = time.monotonic()
        self._moved_to_be = False
        self.last_order_filled = True

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
            await self._cancel_all_symbol_orders()
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
                sl_res = await asyncio.to_thread(
                    self._exchange.order,
                    self._symbol,
                    sl_side,
                    fill_size,
                    stop_price,
                    {"trigger": {"isMarket": True, "triggerPx": stop_price, "tpsl": "sl"}},
                    reduce_only=True,
                )
                logger.info("SL order result (attempt %d): %s @ %.1f", attempt, sl_res, stop_price)

                # 2. Colocar Take Profit
                tp_res = await asyncio.to_thread(
                    self._exchange.order,
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

    async def on_tp_sl_trigger(self, trigger_type: str = "TP_OR_SL", fill_price: float = 0.0) -> None:
        """
        Llamado cuando se ejecuta una orden TP o SL (o fill de cierre).
        Cancela cualquier orden residual de protección en el exchange y
        transiciona la máquina de estados a FLAT (pasando por CLOSING si aplica).
        """
        logger.info(
            "on_tp_sl_trigger: %s @ %.2f. Cancelando órdenes residuales...",
            trigger_type,
            fill_price,
        )
        try:
            await self._cancel_all_symbol_orders()
            logger.info("Residual protection orders cancelled for %s", self._symbol)
        except Exception:
            logger.exception("Failed to cancel residual protection orders on TP/SL trigger")

        self._active_order = None
        self._active_entry_cloid = None
        self._active_sl_oid = None
        self._active_tp_oid = None
        self._filled_size = 0.0

        if self._sm.state not in (ExecutorState.FLAT, ExecutorState.HALTED):
            try:
                if self._sm.state == ExecutorState.OPEN_PROTECTED:
                    try:
                        await self._sm.transition(ExecutorState.CLOSING, f"{trigger_type} hit")
                    except InvalidTransitionError:
                        pass
                await self._sm.transition(ExecutorState.FLAT, f"{trigger_type} completed")
            except InvalidTransitionError:
                pass

    # Alias en caso de ser invocado con mayúscula según convención
    on_tp_sl_Trigger = on_tp_sl_trigger

    async def on_position_closed(self, reason: str = "TP/SL hit") -> None:
        """
        Llamado cuando la posición se cierra en el exchange (por TP, SL o cierre manual).
        Cancela cualquier orden residual y regresa la máquina de estados a FLAT.
        """
        await self.on_tp_sl_trigger(reason, 0.0)

    async def reconcile_state(self, position_size: float) -> None:
        """
        Reconcilia el estado del ejecutor con la posición real del exchange.
        Si la posición en el exchange es 0 y el estado es OPEN_PROTECTED/OPEN_UNPROTECTED/CLOSING,
        limpia órdenes residuales y regresa a FLAT.
        """
        if abs(position_size) < 1e-6:
            if self._sm.state in (
                ExecutorState.OPEN_PROTECTED,
                ExecutorState.OPEN_UNPROTECTED,
                ExecutorState.CLOSING,
            ):
                logger.info(
                    "reconcile_state: position is 0 on exchange while state=%s. Transitioning to FLAT",
                    self._sm.state.value,
                )
                await self.on_tp_sl_trigger("Reconcile (pos=0)", 0.0)
        else:
            if self._sm.state == ExecutorState.FLAT:
                logger.warning(
                    "reconcile_state: position %.5f exists on exchange while FLAT. Transitioning to OPEN_UNPROTECTED",
                    position_size,
                )
                try:
                    await self._sm.transition(
                        ExecutorState.ENTRY_PENDING, "reconcile: external position found"
                    )
                    await self._sm.transition(
                        ExecutorState.OPEN_UNPROTECTED, "reconcile: external position found"
                    )
                except InvalidTransitionError:
                    pass

    # ── Cierre de emergencia ───────────────────────────────────────────────────

    async def _emergency_close(self, size: float, reason: str) -> None:
        """Cierra la posición a mercado inmediatamente."""
        logger.critical("EMERGENCY CLOSE: size=%.5f, reason=%s", size, reason)

        try:
            await self._sm.transition(ExecutorState.CLOSING, f"Emergency: {reason}")

            result = await asyncio.to_thread(self._exchange.market_close, self._symbol)
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
            await self._cancel_all_symbol_orders()
            logger.info("All orders cancelled")

            # Cerrar posición si existe
            try:
                await asyncio.to_thread(self._exchange.market_close, self._symbol)
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

    # ── Gestión activa: Break-Even y Time-Stop ────────────────────────────────

    async def manage_open_position(self, state: Any) -> None:
        """
        Llamar cada 5s desde main.py mientras hay posición abierta.
        1. Break-Even automático en +1.0R: mueve SL a precio de entrada.
        2. Time-Stop: cierra si la posición lleva > 2.5x estimated_hold_minutes
           y el PnL flotante está estancado (entre -0.3R y +0.5R).
        """
        if self._sm.state != ExecutorState.OPEN_PROTECTED:
            return
        if self._active_order is None or self._kill_switch_active:
            return

        signal = self._active_order.signal
        entry_price = state.account.position_entry_price or self._active_order.approved_price
        mark_price = state.asset_ctx.mark_price if state.asset_ctx else 0.0
        if mark_price <= 0 or entry_price <= 0:
            return

        initial_sl = self._active_order.approved_stop_loss
        sl_dist = abs(entry_price - initial_sl)
        if sl_dist <= 0:
            return

        is_long = signal.side == SignalSide.BUY
        pnl_r = ((mark_price - entry_price) / sl_dist) if is_long else ((entry_price - mark_price) / sl_dist)

        # ── Break-Even en +1.0R (offset 10 bps para cubrir comisiones Taker) ──
        if not self._moved_to_be and pnl_r >= 1.0 and self._can_modify_order():
            be_price = entry_price * 1.0010 if is_long else entry_price * 0.9990
            be_price = float(round(be_price)) if self._symbol == "BTC" else round(be_price, 2)
            logger.info(
                "Break-Even activado: posición en +%.1fR. Moviendo SL a %.2f (entry=%.2f)",
                pnl_r, be_price, entry_price,
            )
            try:
                # Cancelar SL anterior y colocar nuevo SL en break-even
                if self._active_sl_oid:
                    await asyncio.to_thread(
                        self._exchange.cancel, self._symbol, int(self._active_sl_oid)
                    )
                sl_side = not is_long
                sl_res = await asyncio.to_thread(
                    self._exchange.order,
                    self._symbol,
                    sl_side,
                    self._filled_size,
                    be_price,
                    {"trigger": {"isMarket": True, "triggerPx": be_price, "tpsl": "sl"}},
                    reduce_only=True,
                )
                self._active_sl_oid = None
                statuses = sl_res.get("response", {}).get("data", {}).get("statuses", [{}]) if isinstance(sl_res, dict) else [{}]
                if statuses and isinstance(statuses[0], dict):
                    self._active_sl_oid = statuses[0].get("resting", {}).get("oid")
                self._moved_to_be = True
                self._last_modify_at = time.monotonic()
                if self._alerter:
                    await self._alerter.send(
                        f"🔒 *BREAK-EVEN ACTIVADO*\n"
                        f"Posición en `+{pnl_r:.1f}R`\n"
                        f"SL movido a entrada: `${be_price:,.2f}`\n"
                        f"Riesgo = 0 desde ahora.",
                        alert_type="INFO",
                    )
            except Exception:
                logger.exception("Error aplicando break-even")

        # ── Time-Stop ─────────────────────────────────────────────────────────
        hold_minutes = signal.estimated_hold_minutes or 25.0
        elapsed_minutes = (time.monotonic() - self._entry_time) / 60.0
        if elapsed_minutes >= 2.5 * hold_minutes and -0.3 <= pnl_r <= 0.5:
            logger.warning(
                "Time-Stop activado: %.0f min elapsed (> 2.5x %.0f min hold), PnL=%.2fR. Cerrando.",
                elapsed_minutes, hold_minutes, pnl_r,
            )
            if self._alerter:
                await self._alerter.send(
                    f"⏱ *TIME-STOP EJECUTADO*\n"
                    f"Posición abierta `{elapsed_minutes:.0f}min` (límite: `{2.5*hold_minutes:.0f}min`)\n"
                    f"PnL flotante: `{pnl_r:+.2f}R` — operación estancada.\n"
                    f"Cerrando a mercado...",
                    alert_type="WARNING",
                )
            await self._emergency_close(self._filled_size, "Time-Stop: posición estancada")

    # ── Helpers ────────────────────────────────────────────────────────────────

    async def _wait_for_fill(self, cloid: str) -> None:
        """Espera hasta que el fill llegue via userEvents (señal de evento)."""
        # En producción, esto usa asyncio.Event disparado desde el ingestor
        # cuando llega un fill con el cloid correspondiente
        while self._active_entry_cloid == cloid:
            await asyncio.sleep(0.1)

    async def _cancel_all_symbol_orders(self) -> None:
        """
        Cancela todas las órdenes del símbolo actual de manera segura:
        1. Cancela las órdenes activas conocidas (SL y TP) vía self._exchange.cancel(self._symbol, int(oid)).
        2. Consulta órdenes abiertas en el exchange vía self._exchange.info.open_orders(self._settings.hl_main_address)
           y ejecuta self._exchange.bulk_cancel([{'coin': self._symbol, 'oid': int(o['oid'])} for o in open_orders if o.get('coin') == self._symbol]).
        """
        # 1. Cancelar órdenes activas conocidas (SL y TP)
        for oid in (self._active_sl_oid, self._active_tp_oid):
            if oid is not None:
                try:
                    await asyncio.to_thread(self._exchange.cancel, self._symbol, int(oid))
                    logger.info("Cancelled active order %s (oid=%s)", self._symbol, oid)
                except Exception as e:
                    logger.debug("Error cancelling active order oid=%s: %s", oid, e)

        # 2. Consultar open_orders y ejecutar bulk_cancel
        try:
            info_client = getattr(self._exchange, "info", None)
            if info_client is None:
                try:
                    from hyperliquid.info import Info
                    info_client = Info(self._settings.api_url, skip_ws=True)
                except Exception:
                    info_client = None

            if info_client and hasattr(info_client, "open_orders"):
                main_addr = self._settings.hl_main_address
                open_orders = await asyncio.to_thread(info_client.open_orders, main_addr)
                symbol_cancels = [
                    {"coin": self._symbol, "oid": int(o["oid"])}
                    for o in open_orders
                    if isinstance(o, dict) and o.get("coin") == self._symbol and "oid" in o
                ]
                if symbol_cancels and hasattr(self._exchange, "bulk_cancel"):
                    res = await asyncio.to_thread(self._exchange.bulk_cancel, symbol_cancels)
                    logger.info("Bulk cancelled %d residual orders for %s: %s", len(symbol_cancels), self._symbol, res)
        except Exception as e:
            logger.debug("Error in bulk_cancel for %s: %s", self._symbol, e)

    async def _cancel_order(self, cloid: str) -> None:
        """Cancela una orden por cloid."""
        try:
            result = await asyncio.to_thread(self._exchange.cancel_by_cloid, self._symbol, cloid)
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
