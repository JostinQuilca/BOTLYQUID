"""
data/ingestor.py
================
Ingestor asíncrono de baja latencia para Hyperliquid WebSocket.

Suscripciones: l2Book, trades, activeAssetCtx, candle(1m), userEvents
Correcciones integradas:
- Reconexión exponencial con jitter
- Reconciliador REST cada 30s
- Volcado Parquet cada 5min (sin bloquear el loop)
- clock_skew_ms y last_tick_age_ms en MarketState
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from config.settings import get_settings
from data.database import BotProcessState, DBEvent, DatabaseWorker, dump_trades_to_parquet, set_bot_state
from data.state import AssetContext, Candle, L2Book, L2Level, MarketState, Trade, market_state

logger = logging.getLogger(__name__)


class HyperliquidIngestor:
    """
    Gestiona la conexión WebSocket a Hyperliquid y actualiza MarketState.

    Flujo:
    1. Escribe estado STARTING en BD
    2. Conecta WebSocket
    3. Suscribe a los 5 canales
    4. Despacha mensajes → actualiza MarketState (Hot Path)
    5. Encola eventos de BD → DatabaseWorker (Cold Path)
    6. Ejecuta reconciliación REST cada 30s
    7. Vuelca Parquet cada 5 min
    """

    RECONNECT_BASE_S = 1.0
    RECONNECT_MAX_S = 60.0
    RECONCILE_INTERVAL_S = 30.0
    PARQUET_DUMP_INTERVAL_S = 300.0   # 5 minutos

    def __init__(
        self,
        state: MarketState,
        db_worker: DatabaseWorker,
        symbol: str = "BTC",
        executor: Any = None,
        alerter: Any = None,
    ) -> None:
        self._state = state
        self._db_worker = db_worker
        self._symbol = symbol
        self._executor = executor
        self._alerter = alerter
        self._settings = get_settings()
        self._running = False
        self._reconnect_attempts = 0
        self._trades_buffer: list[dict[str, Any]] = []
        self._last_parquet_dump = time.monotonic()

    def set_dependencies(self, executor: Any, alerter: Any) -> None:
        self._executor = executor
        self._alerter = alerter

    async def run(self, db_conn: Any) -> None:
        """Bucle principal de reconexión con backoff exponencial + jitter."""
        self._running = True
        self._db_conn = db_conn

        while self._running:
            try:
                await self._connect_and_ingest()
            except asyncio.CancelledError:
                logger.info("Ingestor cancelled")
                break
            except Exception as e:
                if not self._running:
                    break
                wait = min(
                    self.RECONNECT_BASE_S * (2 ** self._reconnect_attempts),
                    self.RECONNECT_MAX_S,
                )
                jitter = random.uniform(0, wait * 0.3)
                wait += jitter
                self._reconnect_attempts += 1
                logger.warning(
                    "WebSocket disconnected (%s). Reconnecting in %.1fs (attempt %d)...",
                    e,
                    wait,
                    self._reconnect_attempts,
                )
                await asyncio.sleep(wait)

    async def _connect_and_ingest(self) -> None:
        logger.info("Connecting to %s", self._settings.ws_url)

        async with websockets.connect(
            self._settings.ws_url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=10,
            max_size=10 * 1024 * 1024,   # 10 MB para libros grandes
        ) as ws:
            self._reconnect_attempts = 0
            logger.info("WebSocket connected. Subscribing to channels...")
            await set_bot_state(self._db_conn, BotProcessState.RUNNING)

            await self._subscribe(ws)

            # Lanzar tareas concurrentes (gather en lugar de TaskGroup para evitar
            # cancelación silenciosa si una sub-tarea falla)
            await asyncio.gather(
                self._message_loop(ws),
                self._reconcile_loop(),
                self._parquet_dump_loop(),
            )

    async def _subscribe(self, ws: Any) -> None:
        """Suscribe a los 5 canales de Hyperliquid."""
        subscriptions = [
            {"method": "subscribe", "subscription": {"type": "l2Book", "coin": self._symbol}},
            {"method": "subscribe", "subscription": {"type": "trades", "coin": self._symbol}},
            {"method": "subscribe", "subscription": {
                "type": "activeAssetCtx", "coin": self._symbol
            }},
            {"method": "subscribe", "subscription": {
                "type": "candle", "coin": self._symbol, "interval": "1m"
            }},
            {"method": "subscribe", "subscription": {
                "type": "userEvents",
                "user": self._settings.hl_main_address,
            }},
        ]
        for sub in subscriptions:
            await ws.send(json.dumps(sub))
            logger.debug("Subscribed: %s", sub["subscription"]["type"])

    async def _message_loop(self, ws: Any) -> None:
        """Bucle de recepción y despacho de mensajes."""
        async for raw in ws:
            if not self._running:
                break
            try:
                msg = json.loads(raw)
                await self._dispatch(msg)
            except json.JSONDecodeError:
                logger.warning("Invalid JSON from WS: %s", raw[:200])
            except Exception:
                logger.exception("Error dispatching message")

    async def _dispatch(self, msg: dict[str, Any]) -> None:
        """Enruta mensajes al handler correcto."""
        channel = msg.get("channel", "")
        data = msg.get("data", {})

        # Extraer timestamp del servidor cuando está disponible
        server_ts = msg.get("time") or (data.get("time") if isinstance(data, dict) else 0) or 0
        if server_ts:
            self._state.record_message(int(server_ts))

        if channel == "l2Book":
            self._handle_l2book(data)
        elif channel == "trades":
            self._handle_trades(data)
        elif channel == "activeAssetCtx":
            self._handle_asset_ctx(data)
        elif channel == "candle":
            self._handle_candle(data)
        elif channel == "userEvents":
            await self._handle_user_events(data)

    # ── Handlers de mensajes ───────────────────────────────────────────────────

    def _handle_l2book(self, data: dict[str, Any]) -> None:
        levels = data.get("levels", [[], []])
        if len(levels) < 2:
            return

        bids = [L2Level(float(b["px"]), float(b["sz"])) for b in levels[0]]
        asks = [L2Level(float(a["px"]), float(a["sz"])) for a in levels[1]]

        self._state.l2_book = L2Book(
            bids=bids,
            asks=asks,
            timestamp_ms=int(data.get("time", time.time() * 1000)),
        )

    def _handle_trades(self, data: list[dict[str, Any]]) -> None:
        if not isinstance(data, list):
            return

        for t in data:
            trade = Trade(
                timestamp_ms=int(t.get("time", 0)),
                price=float(t.get("px", 0)),
                size=float(t.get("sz", 0)),
                side=t.get("side", ""),   # "B" o "A"
            )
            self._state.trades.append(trade)
            self._trades_buffer.append({
                "timestamp_ms": trade.timestamp_ms,
                "price": trade.price,
                "size": trade.size,
                "side": trade.side,
            })

    def _handle_asset_ctx(self, data: dict[str, Any]) -> None:
        ctx_data = data.get("ctx", data)
        self._state.asset_ctx = AssetContext(
            mark_price=float(ctx_data.get("markPx", 0)),
            oracle_price=float(ctx_data.get("oraclePx", 0)),
            funding_rate=float(ctx_data.get("funding", 0)),
            open_interest=float(ctx_data.get("openInterest", 0)),
            timestamp_ms=int(time.time() * 1000),
        )
        self._state.record_oi_snapshot()

    def _handle_candle(self, data: dict[str, Any]) -> None:
        candle = Candle(
            timestamp_ms=int(data.get("t", 0)),
            open=float(data.get("o", 0)),
            high=float(data.get("h", 0)),
            low=float(data.get("l", 0)),
            close=float(data.get("c", 0)),
            volume=float(data.get("v", 0)),
            interval=data.get("i", "1m"),
        )
        # FIX #2: Deduplicar velas del mismo minuto (WS env\u00eda muchos updates por minuto)
        if self._state.candles_1m and self._state.candles_1m[-1].timestamp_ms == candle.timestamp_ms:
            self._state.candles_1m[-1] = candle   # reemplazar en lugar de duplicar
        else:
            self._state.candles_1m.append(candle)


        # Persistir en BD via Cola (Cold Path)
        self._db_worker._queue.put_nowait(DBEvent(
            table="candles_1m",
            data={
                "symbol": self._symbol,
                "ts_ms": candle.timestamp_ms,
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
            },
            event_type="upsert",
        ))

    async def _handle_user_events(self, data: Any) -> None:
        """
        Procesa fills, cancelaciones y liquidaciones de la cuenta.
        Guarda en live_fills, notifica al ejecutor y alerta a Telegram.
        """
        from datetime import date

        if isinstance(data, dict) and "fills" in data:
            for fill in data["fills"]:
                tid = int(fill.get("tid", int(time.time() * 1000)))
                oid = int(fill.get("oid", 0))
                px = float(fill.get("px", 0.0))
                sz = float(fill.get("sz", 0.0))
                side = str(fill.get("side", ""))
                dir_str = str(fill.get("dir", ""))
                closed_pnl = float(fill.get("closedPnl", 0.0))
                fee = float(fill.get("fee", 0.0))
                ts_ms = int(fill.get("time", time.time() * 1000))

                logger.info(
                    "FILL: %s %s @ %s (dir=%s, oid=%s, closedPnl=%s)",
                    side, sz, px, dir_str, oid, closed_pnl,
                )

                # Guardar en live_fills
                self._db_worker.enqueue(
                    DBEvent(
                        table="live_fills",
                        event_type="insert",
                        data={
                            "tid": tid,
                            "oid": oid,
                            "symbol": self._symbol,
                            "dir": dir_str,
                            "side": side,
                            "px": px,
                            "sz": sz,
                            "closed_pnl": closed_pnl,
                            "fee": fee,
                            "ts_ms": ts_ms,
                        },
                    )
                )

                # Si es apertura, notificar a executor
                if "Open" in dir_str and self._executor:
                    active_cloid = getattr(self._executor, "_active_entry_cloid", None)
                    await self._executor.on_fill(active_cloid, px, sz, oid)

                # Si es cierre o tiene PnL cerrado
                if "Close" in dir_str or abs(closed_pnl) > 0:
                    net_pnl = closed_pnl - fee
                    today = date.today().isoformat()
                    if hasattr(self, "_db_conn") and self._db_conn:
                        try:
                            await self._db_conn.execute(
                                "UPDATE daily_risk SET realized_pnl = realized_pnl + ?, updated_at = unixepoch('now', 'subsec') WHERE date_utc = ?",
                                (net_pnl, today),
                            )
                            await self._db_conn.commit()
                        except Exception:
                            logger.exception("Failed to update daily_risk realized_pnl")

                    if self._alerter:
                        emoji = "💰 GANANCIA" if net_pnl >= 0 else "📉 PÉRDIDA"
                        await self._alerter.send(
                            f"{emoji} — *OPERACIÓN CERRADA*\n"
                            f"Acción: `{dir_str}`\n"
                            f"Precio salida: `${px:,.2f}`\n"
                            f"Tamaño: `{sz:.5f} BTC`\n"
                            f"PnL Bruto: `{closed_pnl:+.4f} USDC`\n"
                            f"Comisión: `-{fee:.4f} USDC`\n"
                            f"*PnL Neto: {net_pnl:+.4f} USDC*\n"
                            f"Capital actual: `${self._state.account.equity + net_pnl:,.2f} USDC`",
                            alert_type="FILL",
                        )

                    if self._executor:
                        await self._executor.on_position_closed(f"Fill {dir_str}")

    # ── Reconciliador REST ─────────────────────────────────────────────────────

    async def _reconcile_loop(self) -> None:
        """
        Reconcilia posición y órdenes en memoria contra el exchange cada 30s.
        Detecta discrepancias que indican pérdida de mensajes WebSocket.
        """
        # Reconciliación inicial inmediata al arrancar (segundo 0)
        try:
            await self._reconcile_with_exchange()
        except Exception:
            logger.exception("Initial reconciliation failed")

        while self._running:
            await asyncio.sleep(self.RECONCILE_INTERVAL_S)
            try:
                await self._reconcile_with_exchange()
            except Exception:
                logger.exception("Reconciliation failed")

    async def _reconcile_with_exchange(self) -> None:
        """
        Consulta el estado real del exchange via REST y compara con el estado
        en memoria. Lanza alerta si hay discrepancia de posición.

        Soporta Unified Account de Hyperliquid:
        - Intenta leer equity de marginSummary (perps).
        - Si es 0, lee USDC de spotClearinghouseState (Unified Account).
        """
        import httpx
        from datetime import date

        url = self._settings.api_url + "/info"
        main_address = self._settings.hl_main_address

        async with httpx.AsyncClient(timeout=10) as client:
            # ── 1. Estado de perps ─────────────────────────────────────────────
            resp = await client.post(url, json={
                "type": "clearinghouseState",
                "user": main_address,
            })
            resp.raise_for_status()
            state_data = resp.json()

            # ── 2. Estado de spot (Unified Account) ────────────────────────────
            spot_resp = await client.post(url, json={
                "type": "spotClearinghouseState",
                "user": main_address,
            })
            spot_data = spot_resp.json() if spot_resp.status_code == 200 else {}

        # ── Equity (Unified Account) ──────────────────────────────────────────
        # En Hyperliquid Unified Account, el balance total de la cuenta es el USDC de Spot.
        # Cuando se abre una posición, parte del balance se asigna temporalmente a perps
        # (marginSummary.accountValue), por lo que leer solo perps causaba una falsa caída del 60%.
        spot_balances = spot_data.get("balances", []) if isinstance(spot_data, dict) else []
        spot_usdc = next(
            (float(b["total"]) for b in spot_balances if b.get("coin") == "USDC"),
            0.0,
        )
        perp_account_val = float(
            state_data.get("marginSummary", {}).get("accountValue", 0)
        )
        if spot_usdc > 0:
            equity = spot_usdc
        else:
            equity = perp_account_val

        withdrawable = float(state_data.get("withdrawable", equity))

        # Actualizar estado en memoria
        self._state.account.equity = equity
        self._state.account.available_margin = withdrawable

        # ── Posición ───────────────────────────────────────────────────────────
        positions = state_data.get("assetPositions", [])
        real_pos = 0.0
        entry_px = 0.0
        unreal_pnl = 0.0
        margin_used = 0.0
        for pos in positions:
            p = pos.get("position", {})
            if p.get("coin") == self._symbol:
                real_pos = float(p.get("szi", 0))
                entry_px = float(p.get("entryPx") or 0)
                unreal_pnl = float(p.get("unrealizedPnl") or 0.0)
                margin_used = float(p.get("marginUsed") or 0.0)
                break

        self._state.account.unrealized_pnl = unreal_pnl
        self._state.account.margin_used = margin_used

        mem_pos = self._state.account.position_size
        if abs(real_pos - mem_pos) > 1e-6:
            logger.warning(
                "RECONCILE MISMATCH: memory=%.6f, exchange=%.6f",
                mem_pos,
                real_pos,
            )
            self._state.account.position_size = real_pos
            self._state.account.position_entry_price = entry_px

        # Inicializar / actualizar daily_risk en BD para el Dashboard
        if equity > 0:
            today = date.today().isoformat()
            self._db_worker.enqueue(
                DBEvent(
                    table="daily_risk",
                    event_type="upsert",
                    data={
                        "date_utc": today,
                        "equity_open": equity,
                        "equity_peak": equity,
                        "realized_pnl": 0.0,
                        "unrealized_pnl": unreal_pnl,
                        "paused_until": None,
                        "kill_switch": 0,
                    },
                )
            )

        # ── 3. Órdenes abiertas (TP / SL en exchange) ──────────────────────────
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                orders_resp = await client.post(url, json={
                    "type": "frontendOpenOrders",
                    "user": main_address,
                })
                open_orders = orders_resp.json() if orders_resp.status_code == 200 else []
        except Exception:
            open_orders = []

        active_tp = 0.0
        active_sl = 0.0
        if isinstance(open_orders, list):
            for o in open_orders:
                if o.get("coin") == self._symbol:
                    order_type = str(o.get("orderType", ""))
                    tpsl_type = str(o.get("tpsl", ""))
                    trig_px = float(o.get("triggerPx") or 0.0)
                    if not trig_px and "triggerPx" in o:
                        trig_px = float(o["triggerPx"])
                    if tpsl_type == "tp" or "take" in order_type.lower():
                        active_tp = trig_px
                    elif tpsl_type == "sl" or "stop" in order_type.lower():
                        active_sl = trig_px

        self._state.account.active_tp_px = active_tp
        self._state.account.active_sl_px = active_sl

        # Si no hay posición en exchange pero el ejecutor sigue activo, liberar
        if abs(real_pos) < 1e-6:
            self._state.account.active_tp_px = 0.0
            self._state.account.active_sl_px = 0.0
            if self._executor and hasattr(self._executor, "_sm"):
                from execution.state_machine import ExecutorState
                if self._executor._sm.state in (ExecutorState.OPEN_PROTECTED, ExecutorState.OPEN_UNPROTECTED):
                    await self._executor.on_position_closed("Reconcile: position is 0 on exchange")

        # ── 4. Sincronizar fills recientes desde REST (resiliencia) ───────────
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                fills_resp = await client.post(url, json={
                    "type": "userFills",
                    "user": main_address,
                })
                recent_user_fills = fills_resp.json() if fills_resp.status_code == 200 else []
            if recent_user_fills and hasattr(self, "_db_conn") and self._db_conn:
                for f in recent_user_fills[:20]:
                    if f.get("coin") == self._symbol:
                        tid = int(f.get("tid", 0))
                        oid = int(f.get("oid", 0))
                        px = float(f.get("px", 0.0))
                        sz = float(f.get("sz", 0.0))
                        side = str(f.get("side", ""))
                        dir_str = str(f.get("dir", ""))
                        closed_pnl = float(f.get("closedPnl", 0.0))
                        fee = float(f.get("fee", 0.0))
                        ts_ms = int(f.get("time", 0))
                        await self._db_conn.execute(
                            """
                            INSERT OR IGNORE INTO live_fills
                            (tid, oid, symbol, dir, side, px, sz, closed_pnl, fee, ts_ms)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (tid, oid, self._symbol, dir_str, side, px, sz, closed_pnl, fee, ts_ms),
                        )
                await self._db_conn.commit()
        except Exception:
            logger.exception("Failed to sync recent fills into live_fills")

        logger.debug(
            "Reconciled: equity=%.4f, pos=%.6f, unreal_pnl=%.4f, tp=%.1f, sl=%.1f, tick_age=%.0fms",
            equity,
            real_pos,
            unreal_pnl,
            active_tp,
            active_sl,
            self._state.last_tick_age_ms,
        )

    # ── Volcado Parquet ────────────────────────────────────────────────────────

    async def _parquet_dump_loop(self) -> None:
        """Vuelca el buffer de trades a Parquet cada 5 minutos."""
        while self._running:
            await asyncio.sleep(self.PARQUET_DUMP_INTERVAL_S)
            if self._trades_buffer:
                buffer_copy = self._trades_buffer.copy()
                self._trades_buffer.clear()
                # Ejecutar en thread pool para no bloquear el loop
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    lambda: asyncio.run(
                        dump_trades_to_parquet(buffer_copy, self._symbol)
                    ),
                )

    def stop(self) -> None:
        self._running = False
