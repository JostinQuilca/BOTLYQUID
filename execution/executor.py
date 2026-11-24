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
        # El monitor de STOP_BOT.flag se arranca con start_kill_flag_monitor() desde
        # main() (requiere un event loop activo). Antes se creaba en __init__ con
        # get_event_loop(), dejando la task sin referencia (riesgo de GC) y generando
        # warnings cuando no había loop corriendo.
        self._kill_flag_task: asyncio.Task[None] | None = None

    def set_alerter(self, alerter: Any) -> None:
        """Asigna o actualiza el alerter de Telegram."""
        self._alerter = alerter

    def _clear_active_order(self) -> None:
        """Limpia la orden de entrada activa en caminos SIN fill (timeout / no-fill /
        error), para que un fill tardío o la reconciliación no reutilicen un
        SL/TP/lado obsoletos de una orden ya abandonada."""
        self._active_order = None
        self._active_entry_cloid = None

    def start_kill_flag_monitor(self) -> None:
        """Arranca (una sola vez) el monitor del archivo STOP_BOT.flag.

        Debe llamarse con un event loop activo (desde main()). Conserva la
        referencia a la task para que no la recoja el recolector de basura.
        """
        if self._kill_flag_task is not None and not self._kill_flag_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.warning("start_kill_flag_monitor() sin event loop activo; no se arrancó")
            return
        self._kill_flag_task = loop.create_task(self._monitor_kill_flag())
        logger.info("Monitor de STOP_BOT.flag arrancado")

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
                self._clear_active_order()
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
                self._clear_active_order()
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

        # Dedup de fills (entrada Y cierre): la respuesta REST y el canal WS
        # userEvents pueden enviar el mismo fill. Se prefiere el tid (único por
        # trade); si no hay, se cae a oid:tamaño:tipo.
        tid = fill_data.get("tid")
        fill_key = (
            f"tid:{tid}" if tid is not None
            else f"{oid}:{fill_size:.5f}:{'close' if is_closing_fill else 'open'}"
        )
        if fill_key in self._processed_fill_keys:
            logger.debug("Ignorando fill duplicado (REST+WS): %s", fill_key)
            return
        if tid is not None or oid > 0:
            self._processed_fill_keys.add(fill_key)

        # CORRECCIÓN (Track B): un fill de CIERRE se maneja en CUALQUIER estado y
        # NUNCA por el camino de entrada. Antes, un fill de cierre fuera de
        # OPEN_PROTECTED se trataba como entrada (colocaba SL/TP reduce-only contra
        # una cuenta que se cerraba, o adoptaba un fill ajeno como entrada propia).
        if is_closing_fill:
            await self._handle_closing_fill(dir_str, fill_price, fill_size, closed_pnl)
            return

        if self._sm.state in (ExecutorState.OPEN_PROTECTED, ExecutorState.OPEN_UNPROTECTED):
            # Partial fill adicional de ENTRADA: acumular tamaño y re-proteger
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

    async def _handle_closing_fill(
        self,
        dir_str: str,
        fill_price: float,
        fill_size: float,
        closed_pnl: float,
    ) -> None:
        """Maneja un fill de CIERRE (parcial o total) en cualquier estado.

        Nunca lo trata como entrada. Reduce el tamaño conocido y, si la posición
        quedó plana (verificado por REST, con la estimación local como respaldo),
        cancela residuales y pasa a FLAT. Si es un cierre PARCIAL, mantiene la
        protección del tamaño restante.
        """
        self._filled_size = max(0.0, self._filled_size - fill_size)
        logger.info(
            "Closing fill: dir='%s' sz=%.5f @ %.2f closedPnl=%.4f (restante estimado=%.5f)",
            dir_str, fill_size, fill_price, closed_pnl, self._filled_size,
        )

        flat = await self._verify_flat_via_rest()
        fully_closed = flat is True or (flat is None and self._filled_size <= 1e-6)

        if fully_closed:
            await self.on_tp_sl_trigger("CLOSE_FILL", fill_price)
            return

        # Cierre PARCIAL: la posición sigue abierta → mantener/ajustar la protección.
        logger.warning(
            "Cierre parcial: la posición sigue abierta (~%.5f BTC). Reprotegiendo el resto.",
            self._filled_size,
        )
        if self._active_order is not None and self._filled_size > 1e-6:
            is_long = self._active_order.signal.side == SignalSide.BUY
            signed = self._filled_size if is_long else -self._filled_size
            await self.ensure_protected(fill_price, signed)

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

        # cloids deterministas para SL/TP: un reintento tras timeout no duplica la
        # orden (idempotencia) y permite emparejar fills con su orden.
        base_id = self._active_order.signal.signal_id if self._active_order else f"reconcile:{entry_price:.0f}"
        sl_cloid = self._make_cloid(f"{base_id}:sl")
        tp_cloid = self._make_cloid(f"{base_id}:tp")

        # ── Prioridad ABSOLUTA: el Stop Loss, con deadline real (SL_CONFIRM_TIMEOUT_S) ──
        sl_placed = False
        for attempt in range(1, self.SL_MAX_RETRIES + 1):
            try:
                sl_res = await asyncio.wait_for(
                    asyncio.to_thread(
                        self._exchange.order,
                        self._symbol,
                        sl_side,
                        fill_size,
                        stop_price,
                        {"trigger": {"isMarket": True, "triggerPx": stop_price, "tpsl": "sl"}},
                        reduce_only=True,
                        cloid=sl_cloid,
                    ),
                    timeout=self.SL_CONFIRM_TIMEOUT_S,
                )
                if not (isinstance(sl_res, dict) and sl_res.get("status") == "ok"):
                    raise RuntimeError(f"Exchange SL error: {sl_res}")
                sl_statuses = sl_res.get("response", {}).get("data", {}).get("statuses", [{}])
                sl_err = sl_statuses[0].get("error") if sl_statuses and isinstance(sl_statuses[0], dict) else None
                if sl_err:
                    raise RuntimeError(f"SL rechazado por el exchange: {sl_err}")
                self._active_sl_oid = (
                    sl_statuses[0].get("resting", {}).get("oid") if sl_statuses else None
                )
                sl_placed = True
                logger.info("SL colocado (intento %d): %.1f (oid=%s)", attempt, stop_price, self._active_sl_oid)
                break
            except Exception as e:
                logger.error("Colocación de SL intento %d/%d falló: %s", attempt, self.SL_MAX_RETRIES, e)
                if attempt < self.SL_MAX_RETRIES:
                    await asyncio.sleep(0.3)

        if not sl_placed:
            logger.critical(
                "EMERGENCY: no se pudo colocar el SL tras %d intentos. Cierre a mercado.",
                self.SL_MAX_RETRIES,
            )
            await self._emergency_close(fill_size, "SL placement failed")
            return

        # ── Take Profit: deseable pero NO crítico (la posición YA tiene SL) ──
        try:
            tp_res = await asyncio.wait_for(
                asyncio.to_thread(
                    self._exchange.order,
                    self._symbol,
                    tp_side,
                    fill_size,
                    tp_price,
                    {"trigger": {"isMarket": True, "triggerPx": tp_price, "tpsl": "tp"}},
                    reduce_only=True,
                    cloid=tp_cloid,
                ),
                timeout=self.SL_CONFIRM_TIMEOUT_S,
            )
            if isinstance(tp_res, dict) and tp_res.get("status") == "ok":
                tp_statuses = tp_res.get("response", {}).get("data", {}).get("statuses", [{}])
                tp_err = tp_statuses[0].get("error") if tp_statuses and isinstance(tp_statuses[0], dict) else None
                if not tp_err:
                    self._active_tp_oid = (
                        tp_statuses[0].get("resting", {}).get("oid") if tp_statuses else None
                    )
                    logger.info("TP colocado: %.1f (oid=%s)", tp_price, self._active_tp_oid)
                else:
                    logger.warning("TP rechazado (no crítico, la posición ya tiene SL): %s", tp_err)
            else:
                logger.warning("TP no colocado (no crítico): %s", tp_res)
        except Exception as e:
            logger.warning("TP no colocado (no crítico): %s", e)

        # Transición y alerta FUERA del bucle de reintentos: evita que un error de
        # formateo/log tras el éxito re-dispare la colocación o un cierre espurio.
        await self._sm.transition(
            ExecutorState.OPEN_PROTECTED,
            f"SL={stop_price:.1f}, TP={tp_price:.1f}",
        )
        logger.info(
            "Position PROTECTED: SL=%.1f (oid=%s), TP=%.1f (oid=%s)",
            stop_price, self._active_sl_oid, tp_price, self._active_tp_oid,
        )
        if self._alerter:
            try:
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
            except Exception:
                logger.exception("No se pudo enviar la alerta de posición protegida")

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

    async def adopt_existing_position(
        self,
        signed_size: float,
        entry_price: float,
        active_tp_px: float,
        active_sl_px: float,
        tp_oid: int | None = None,
        sl_oid: int | None = None,
    ) -> None:
        """
        Sincroniza el ejecutor cuando existe una posición abierta y protegida
        en el exchange (ej. tras un reinicio del contenedor en Fly.io).
        """
        self._filled_size = abs(signed_size)
        if self._entry_time <= 0.0:
            self._entry_time = time.monotonic()
        if tp_oid:
            self._active_tp_oid = tp_oid
        if sl_oid:
            self._active_sl_oid = sl_oid

        if active_tp_px > 0 and active_sl_px > 0:
            try:
                if self._sm.state == ExecutorState.FLAT:
                    await self._sm.transition(ExecutorState.ENTRY_PENDING, "adopt existing position")
                    await self._sm.transition(ExecutorState.OPEN_UNPROTECTED, "adopt existing position")
                if self._sm.state == ExecutorState.OPEN_UNPROTECTED:
                    await self._sm.transition(
                        ExecutorState.OPEN_PROTECTED,
                        f"Adopted protected position: TP={active_tp_px:.1f}, SL={active_sl_px:.1f}",
                    )
            except InvalidTransitionError:
                pass

    # ── Cierre de emergencia ───────────────────────────────────────────────────

    async def _emergency_close(self, size: float, reason: str) -> None:
        """Cierra la posición a mercado inmediatamente.

        CORRECCIÓN (Track B): el cierre a mercado se intenta PRIMERO (con
        reintentos y verificación por REST), y la transición de estado va DESPUÉS
        en su propio try. Antes, un error de la máquina de estados (p.ej. intentar
        CLOSING desde HALTED/FLAT) abortaba el bloque entero y market_close NUNCA
        se ejecutaba, dejando la posición abierta y desprotegida.
        """
        logger.critical("EMERGENCY CLOSE: size=%.5f, reason=%s", size, reason)

        # Transición a CLOSING: útil para la auditoría, NO crítica. Si falla, se
        # cierra igual.
        try:
            await self._sm.transition(ExecutorState.CLOSING, f"Emergency: {reason}")
        except InvalidTransitionError as e:
            logger.warning("No se pudo transicionar a CLOSING (%s); se cierra igual.", e)

        closed = await self._close_at_market_with_retries(reason)

        if closed:
            try:
                await self._sm.transition(ExecutorState.FLAT, "Emergency close complete")
            except InvalidTransitionError:
                await self._sm.force_halt("Emergency close OK pero estado inconsistente")
        else:
            logger.critical("EMERGENCY CLOSE NO CONFIRMADO tras reintentos — HALT")
            await self._sm.force_halt(f"Emergency close failed: {reason}")
            await self._alert_critical(
                f"🚨 *CIERRE DE EMERGENCIA FALLÓ*\n"
                f"Motivo: `{reason}`\n"
                f"La posición puede seguir ABIERTA. Revisa el exchange de inmediato."
            )

    async def _close_at_market_with_retries(self, reason: str, retries: int = 3) -> bool:
        """Cierra a mercado con reintentos y verifica por REST que la posición sea 0.

        Retorna True solo si el cierre quedó confirmado: posición plana por REST, o
        bien market_close no reportó error cuando no se pudo verificar por REST.
        """
        for attempt in range(1, retries + 1):
            close_errored = False
            try:
                result = await asyncio.to_thread(self._exchange.market_close, self._symbol)
                logger.critical("market_close intento %d/%d: %s", attempt, retries, result)
                if isinstance(result, dict) and result.get("status") == "err":
                    close_errored = True
                    logger.error("market_close devolvió error: %s", result.get("response"))
            except Exception:
                close_errored = True
                logger.exception("market_close intento %d lanzó excepción", attempt)

            flat = await self._verify_flat_via_rest()
            if flat is True:
                return True
            if flat is None and not close_errored:
                # No se pudo verificar por REST, pero el cierre no reportó error:
                # se acepta como mejor esfuerzo.
                return True
            if attempt < retries:
                await asyncio.sleep(0.5)

        return (await self._verify_flat_via_rest()) is True

    async def _verify_flat_via_rest(self) -> bool | None:
        """True si la posición del símbolo es ~0 según el exchange, False si hay
        posición, None si no se pudo verificar (conservador: no asumir nada)."""
        try:
            info_client = getattr(self._exchange, "info", None)
            if info_client is None or not hasattr(info_client, "user_state"):
                return None
            st = await asyncio.to_thread(info_client.user_state, self._settings.hl_main_address)
            if not isinstance(st, dict):
                return None
            for ap in st.get("assetPositions", []):
                pos = ap.get("position", {}) if isinstance(ap, dict) else {}
                if pos.get("coin") == self._symbol:
                    return abs(float(pos.get("szi", 0.0) or 0.0)) < 1e-6
            return True
        except Exception:
            logger.exception("No se pudo verificar la posición por REST")
            return None

    async def _alert_critical(self, msg: str) -> None:
        """Envía una alerta crítica a Telegram si hay alerter (best-effort)."""
        if self._alerter:
            try:
                await self._alerter.send(msg, alert_type="CRITICAL")
            except Exception:
                logger.exception("No se pudo enviar alerta crítica")

    # ── Kill Switch ────────────────────────────────────────────────────────────

    async def kill_switch(self, reason: str = "Manual kill switch") -> None:
        """
        Kill switch: cancela todas las órdenes y cierra posición.
        Invocable desde Telegram, validador, o archivo STOP_BOT.flag.
        """
        self._kill_switch_active = True
        logger.critical("KILL SWITCH ACTIVATED: %s", reason)

        await self._sm.force_halt(reason)

        # CORRECCIÓN (Track B): cerrar PRIMERO (con reintentos + verificación) y
        # cancelar el resto de órdenes SOLO si el cierre quedó confirmado. Antes se
        # cancelaba el SL antes de cerrar: si el cierre fallaba, la posición quedaba
        # abierta y SIN stop.
        closed = await self._close_at_market_with_retries(reason)

        if closed:
            try:
                await self._cancel_all_symbol_orders()
                logger.info("Órdenes canceladas tras cierre confirmado")
            except Exception:
                logger.exception("Error cancelando órdenes tras kill switch")
        else:
            logger.critical(
                "KILL SWITCH: cierre NO confirmado — se mantiene el SL y se alerta"
            )
            await self._alert_critical(
                f"🚨 *KILL SWITCH: EL CIERRE FALLÓ*\n"
                f"Motivo: `{reason}`\n"
                f"Mantengo el SL puesto. La posición puede seguir ABIERTA. Revisa el exchange YA."
            )

    async def _monitor_kill_flag(self) -> None:
        """Monitorea el archivo STOP_BOT.flag cada 5 segundos."""
        import os
        from config.settings import stop_flag_path
        flag_abs = stop_flag_path()
        while not self._kill_switch_active:
            await asyncio.sleep(5)
            # Ruta absoluta (raíz del proyecto) + fallback relativo al CWD por compat.
            if os.path.exists(flag_abs) or os.path.exists("STOP_BOT.flag"):
                await self.kill_switch("STOP_BOT.flag detected")
                break

    # ── Rate limiting de modificaciones ───────────────────────────────────────

    def _can_modify_order(self) -> bool:
        """Máximo 1 modificación cada 5 segundos (rate limit de Hyperliquid)."""
        return (time.monotonic() - self._last_modify_at) >= self.ORDER_MODIFY_COOLDOWN_S

    # ── Gestión activa: Break-Even y Time-Stop ────────────────────────────────

    async def manage_open_position(self, state: Any) -> None:
        """
        Gestión activa cada 5s (v8 Scalp Blindado):
        1. Break-Even temprano: apenas avanza +0.16% (~$120 en BTC) o +0.55R,
           mueve el SL a +0.10% (cubre 100% de comisiones Taker y asegura ganancia).
        2. Cierre en verde a los 8+ min (réplica del horizonte 5m-15m de Shadow Mode):
           si lleva >= 8 min abierta y avanza >= +0.15% a favor, cobra ganancia a mercado.
        3. Time-Stop a los 20 min si la operación queda estancada.
        """
        if self._sm.state != ExecutorState.OPEN_PROTECTED or self._kill_switch_active:
            return

        if self._active_order is not None:
            signal = self._active_order.signal
            is_long = signal.side == SignalSide.BUY
            entry_price = state.account.position_entry_price or self._active_order.approved_price
            initial_sl = self._active_order.approved_stop_loss
        else:
            # Fallback tras reinicio del contenedor con posición adoptada desde Hyperliquid
            if not state.account or not state.account.has_position:
                return
            is_long = state.account.position_size > 0
            entry_price = state.account.position_entry_price
            initial_sl = state.account.active_sl_px
            if self._filled_size <= 0:
                self._filled_size = abs(state.account.position_size)
            if self._entry_time <= 0:
                self._entry_time = time.monotonic()

        mark_price = state.asset_ctx.mark_price if state.asset_ctx else 0.0
        if mark_price <= 0 or entry_price <= 0:
            return

        sl_dist = abs(entry_price - initial_sl) if initial_sl > 0 else (entry_price * 0.0030)
        if sl_dist <= 0:
            return

        move_pct = ((mark_price - entry_price) / entry_price) if is_long else ((entry_price - mark_price) / entry_price)
        pnl_r = ((mark_price - entry_price) / sl_dist) if is_long else ((entry_price - mark_price) / sl_dist)
        elapsed_minutes = (time.monotonic() - self._entry_time) / 60.0

        strategy_name = self._active_order.signal.strategy if self._active_order else ""
        hold_limit_min = self._active_order.signal.estimated_hold_minutes if self._active_order else 20.0

        if strategy_name == "KronosFoundationStrategy":
            be_trigger = 0.0028 if hold_limit_min >= 60.0 else 0.0020
            be_offset = 0.0015 if hold_limit_min >= 60.0 else 0.0014
        else:
            be_trigger = 0.0016
            be_offset = 0.0010

        # ── 1. Break-Even Temprano ────────────────────────────────────────────
        if not self._moved_to_be and (move_pct >= be_trigger or pnl_r >= 0.65) and self._can_modify_order():
            be_price = entry_price * (1.0 + be_offset) if is_long else entry_price * (1.0 - be_offset)
            be_price = float(round(be_price)) if self._symbol == "BTC" else round(be_price, 2)
            logger.info(
                "Break-Even temprano activado: move=%+.3f%% (+%.2fR). Moviendo SL a %.2f (entry=%.2f)",
                move_pct * 100, pnl_r, be_price, entry_price,
            )
            try:
                sl_side = not is_long
                old_sl_oid = self._active_sl_oid
                # CORRECCIÓN (Track B): colocar el NUEVO SL primero y solo DESPUÉS
                # cancelar el viejo. Antes se cancelaba primero → ventana sin stop, y
                # si la colocación fallaba la posición quedaba sin SL con _moved_to_be
                # marcado en True.
                sl_res = await asyncio.to_thread(
                    self._exchange.order,
                    self._symbol,
                    sl_side,
                    self._filled_size,
                    be_price,
                    {"trigger": {"isMarket": True, "triggerPx": be_price, "tpsl": "sl"}},
                    reduce_only=True,
                )
                if not (isinstance(sl_res, dict) and sl_res.get("status") == "ok"):
                    raise RuntimeError(f"BE SL rechazado: {sl_res}")
                statuses = sl_res.get("response", {}).get("data", {}).get("statuses", [{}])
                be_err = statuses[0].get("error") if statuses and isinstance(statuses[0], dict) else None
                if be_err:
                    raise RuntimeError(f"BE SL error: {be_err}")
                new_sl_oid = statuses[0].get("resting", {}).get("oid") if statuses and isinstance(statuses[0], dict) else None
                # Nuevo SL confirmado → ahora sí cancelar el viejo.
                if old_sl_oid:
                    try:
                        await asyncio.to_thread(self._exchange.cancel, self._symbol, int(old_sl_oid))
                    except Exception:
                        logger.exception("No se pudo cancelar el SL viejo tras colocar el nuevo (no crítico)")
                self._active_sl_oid = new_sl_oid
                self._moved_to_be = True
                self._last_modify_at = time.monotonic()
                if self._alerter:
                    await self._alerter.send(
                        f"🔒 *OPERACIÓN BLINDADA EN VERDE*\n"
                        f"Avance: `{move_pct*100:+.2f}%` (`+{pnl_r:.2f}R`)\n"
                        f"SL subido a ganancia: `${be_price:,.2f}`\n"
                        f"Comisiones cubiertas — imposible perder en este trade.",
                        alert_type="INFO",
                    )
            except Exception:
                logger.exception("Error aplicando break-even temprano (se mantiene el SL actual)")

        # ── 2. Cobro Rápido en Verde (solo para estrategias rápidas de 1m, NO para Kronos 30m) ──
        if strategy_name != "KronosFoundationStrategy" and elapsed_minutes >= 8.0 and move_pct >= 0.0015:
            logger.info(
                "Scalp Profit Exit: %.1f min abierta con ganancia %+.3f%%. Cerrando en verde a mercado.",
                elapsed_minutes, move_pct * 100,
            )
            if self._alerter:
                await self._alerter.send(
                    f"🎯 *GANANCIA RÁPIDA ASEGURADA (SCALP EXIT)*\n"
                    f"Tiempo: `{elapsed_minutes:.1f} min` | Avance: `{move_pct*100:+.2f}%`\n"
                    f"Cerrando a mercado en `${mark_price:,.2f}` para asegurar beneficio neto.",
                    alert_type="FILL",
                )
            await self._cancel_all_symbol_orders()
            await self._emergency_close(self._filled_size, "Scalp Profit Exit in green")
            return

        # ── 3. Time-Stop (según horizonte de la estrategia: 30m en Kronos, 20m en resto) ──
        if elapsed_minutes >= hold_limit_min and -0.25 <= pnl_r <= 0.40:
            logger.warning(
                "Time-Stop activado (%.0fm): %.0f min elapsed, PnL=%.2fR. Cerrando.",
                hold_limit_min, elapsed_minutes, pnl_r,
            )
            if self._alerter:
                await self._alerter.send(
                    f"⏱ *TIME-STOP EJECUTADO ({hold_limit_min:.0f}m)*\n"
                    f"Posición sin impulso (`{pnl_r:+.2f}R`). Cerrando a mercado...",
                    alert_type="WARNING",
                )
            await self._cancel_all_symbol_orders()
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

        # Ningún fill se completó por este camino: limpiar la orden de entrada activa.
        self._clear_active_order()
