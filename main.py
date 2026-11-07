"""
main.py
=======
Punto de entrada del bot. Orquesta todos los módulos.

Secuencia de arranque:
1. Cargar configuración y validar credenciales
2. Inicializar BD → escribir estado STARTING (antes del WebSocket)
3. Arrancar DatabaseWorker (Cold Path)
4. Arrancar Ingestor WebSocket (Hot Path)
5. Inicializar Motor de Decisiones
6. Inicializar Ejecutor con StateMachine
7. Arrancar Telegram Bot
8. Bucle principal de evaluación de señales
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from pathlib import Path

# Fix Windows UTF-8 encoding for console output
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/bot.log", mode="a"),
    ],
)

Path("logs").mkdir(exist_ok=True)
logger = logging.getLogger("main")


async def main() -> None:
    from config.settings import get_settings
    from data.database import BotProcessState, DatabaseWorker, DBEvent, init_db, set_bot_state
    from data.ingestor import HyperliquidIngestor
    from data.state import market_state
    from execution.executor import HyperliquidExecutor
    from execution.state_machine import ExecutorStateMachine
    from monitoring.telegram_bot import build_telegram_app
    from risk.validator import RiskValidator
    from signals.engine import DecisionEngine
    from data.external_signals import ExternalSignalsFetcher

    settings = get_settings()

    logger.info("=" * 60)
    logger.info("CyberVision HL Bot v2.0 starting")
    logger.info("Environment: %s", settings.env)
    logger.info("Symbol: %s", settings.trading_symbol)
    logger.info("Shadow mode: %s", settings.shadow_mode)
    logger.info("=" * 60)

    # ── 1. Inicializar BD ──────────────────────────────────────────────────────
    db = await init_db()

    # CORRECCIÓN #5: Escribir STARTING ANTES de conectar al WebSocket
    await set_bot_state(db, BotProcessState.STARTING, {
        "env": settings.env,
        "symbol": settings.trading_symbol,
        "shadow_mode": settings.shadow_mode,
    })

    # FIX #2: Precargar velas históricas desde SQLite al arrancar
    # Evita esperar 60 min para que atr_1h sea válido (TrendMomentum requiere 55 velas)
    try:
        from data.state import Candle as _Candle
        rows = await db.execute_fetchall(
            "SELECT ts_ms, open, high, low, close, volume "
            "FROM candles_1m WHERE symbol=? ORDER BY ts_ms DESC LIMIT 240",
            (settings.trading_symbol,),
        )
        if rows:
            for row in reversed(rows):
                market_state.candles_1m.append(_Candle(
                    timestamp_ms=row[0],
                    open=row[1],
                    high=row[2],
                    low=row[3],
                    close=row[4],
                    volume=row[5],
                    interval="1m",
                ))
            logger.info(
                "Precargadas %d velas desde SQLite (bot listo en segundos)",
                len(rows),
            )
        else:
            logger.info("Sin velas hist\u00f3ricas en BD — esperando datos del WebSocket")
    except Exception:
        logger.exception("Error precargando velas desde SQLite (no cr\u00edtico)")

    # ── 2. DatabaseWorker (Cold Path) ──────────────────────────────────────────
    db_worker = DatabaseWorker(db)

    # ── 3. External signals fetcher ────────────────────────────────────────────
    ext_signals = ExternalSignalsFetcher()

    # ── 3. Componentes del Hot Path ────────────────────────────────────────────
    ingestor = HyperliquidIngestor(
        state=market_state,
        db_worker=db_worker,
        symbol=settings.trading_symbol,
    )

    decision_engine = DecisionEngine(shadow_mode=settings.shadow_mode)
    state_machine = ExecutorStateMachine()

    # ── 4. Ejecutor (solo si no es shadow mode) ────────────────────────────────
    executor: HyperliquidExecutor | None = None
    if not settings.shadow_mode:
        try:
            import eth_account
            from hyperliquid.exchange import Exchange
            from hyperliquid.info import Info

            account = eth_account.Account.from_key(
                settings.hl_agent_private_key.get_secret_value()
            )
            exchange = Exchange(
                account,
                settings.api_url,
                account_address=settings.hl_main_address,
            )
            executor = HyperliquidExecutor(exchange, state_machine, settings.trading_symbol)
            logger.info("Executor initialized (LIVE MODE)")
        except Exception:
            logger.exception("Failed to initialize executor — falling back to shadow mode")
            settings.shadow_mode = True  # type: ignore[misc]

    risk_validator = RiskValidator(db)

    # ── 5. Telegram ────────────────────────────────────────────────────────────
    async def kill_switch_fn() -> None:
        if executor:
            await executor.kill_switch("Telegram /kill command")
        await set_bot_state(db, BotProcessState.HALTED, {"reason": "kill_switch"})

    async def pause_fn(seconds: int) -> None:
        from datetime import date
        today = date.today().isoformat()
        paused_until = time.time() + seconds
        equity_now = market_state.account.equity
        try:
            await db.execute(
                """
                INSERT INTO daily_risk (date_utc, equity_open, equity_peak, paused_until)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(date_utc) DO UPDATE SET
                    paused_until = excluded.paused_until,
                    updated_at = unixepoch('now', 'subsec')
                """,
                (today, equity_now, equity_now, paused_until),
            )
            await db.commit()
            logger.info("Bot paused for %ds (until %.0f) via Telegram", seconds, paused_until)
        except Exception:
            logger.exception("Error persisting pause_fn to SQLite")

    telegram_app, alerter = build_telegram_app(
        market_state=market_state,
        kill_switch_fn=kill_switch_fn,
        pause_fn=pause_fn,
        db=db,
        state_machine=state_machine,
    )

    # Vincular dependencias de ingestor y ejecutor con alerter
    ingestor.set_dependencies(executor=executor, alerter=alerter)
    if executor:
        executor.set_alerter(alerter)

    # ── 6. Bucle principal ─────────────────────────────────────────────────────
    async def signal_evaluation_loop() -> None:
        """Evalua señales cada 1 segundo (en velas de 1m)."""
        _last_manage_at: float = 0.0
        # v7: anti-spam de alertas de veto
        _last_veto_alert_reason: str = ""
        _last_veto_alert_at: float = 0.0
        while True:
            await asyncio.sleep(1.0)
            try:
                now = time.monotonic()

                # ── Gestión activa de posición abierta (cada 5s) ──────────────
                if executor and (now - _last_manage_at) >= 5.0:
                    _last_manage_at = now
                    await executor.manage_open_position(market_state)

                # v7: skip evaluación de nuevas entradas si hay posición abierta (LIVE)
                if not settings.shadow_mode and executor:
                    from execution.state_machine import ExecutorState
                    if executor._sm.state != ExecutorState.FLAT:
                        continue

                if market_state.is_warm:
                    signal = decision_engine.evaluate(market_state)

                    if signal is not None:
                        # ── Filtro de señales externas ──────────────────────
                        ext = ext_signals.snapshot
                        side = signal.side.value.upper()  # "BUY" o "SELL"

                        if side in ("BUY", "LONG"):
                            blocked, reason = ext.should_block_long()
                            if blocked:
                                logger.info("Signal LONG bloqueada por señal externa: %s", reason)
                                decision_engine.mark_signal_rejected(60.0)
                                continue
                        elif side in ("SELL", "SHORT"):
                            blocked, reason = ext.should_block_short()
                            if blocked:
                                logger.info("Signal SHORT bloqueada por señal externa: %s", reason)
                                decision_engine.mark_signal_rejected(60.0)
                                continue

                        # Validar con el guardián de riesgo
                        try:
                            approved = await risk_validator.validate(
                                signal, market_state, shadow_mode=settings.shadow_mode
                            )

                            # Ajustar tamaño según contexto externo
                            size_mult = ext.size_multiplier()
                            if size_mult < 1.0:
                                logger.info(
                                    "Tamaño reducido x%.1f por F&G=%d (%s)",
                                    size_mult, ext.fng_value, ext.fng_label,
                                )
                                approved = risk_validator.apply_size_multiplier(approved, size_mult)

                            # Registrar decisión en la BD para el Dashboard
                            await db_worker.put(
                                DBEvent(
                                    table="decisions",
                                    data={
                                        "signal_id": getattr(signal, "signal_id", str(id(signal))),
                                        "symbol": settings.trading_symbol,
                                        "strategy": signal.strategy,
                                        "side": signal.side.value if hasattr(signal.side, "value") else str(signal.side),
                                        "regime": signal.regime.value if hasattr(signal.regime, "value") else str(signal.regime),
                                        "obi": signal.obi,
                                        "cvd_1m": signal.cvd_1m,
                                        "cvd_5m": signal.cvd_5m,
                                        "basis_bps": signal.basis_bps,
                                        "oi_change_5m": signal.oi_change_5m,
                                        "proposed_price": approved.approved_price,
                                        "proposed_size": approved.approved_size,
                                        "stop_loss": approved.approved_stop_loss,
                                        "shadow_mode": 1 if settings.shadow_mode else 0,
                                        "vetoed": 0,
                                        "veto_reason": None,
                                    },
                                )
                            )

                            if settings.shadow_mode:
                                decision_engine.mark_signal_dispatched()
                                logger.info(
                                    "SHADOW SIGNAL: %s %s @ %.2f (SL=%.2f) [F&G=%d, Funding=%+.4f%%]",
                                    signal.strategy,
                                    signal.side.value if hasattr(signal.side, "value") else str(signal.side),
                                    approved.approved_price,
                                    approved.approved_stop_loss,
                                    ext.fng_value,
                                    ext.binance_funding_8h_pct,
                                )
                                await alerter.send(
                                    f"👻 *Shadow Signal*\n"
                                    f"`{signal.strategy}` {signal.side.value if hasattr(signal.side, 'value') else signal.side}\n"
                                    f"Entry: `${approved.approved_price:,.2f}`\n"
                                    f"SL: `${approved.approved_stop_loss:,.2f}`\n"
                                    f"Size: `{approved.approved_size:.5f} BTC`\n"
                                    f"Fear&Greed: `{ext.fng_value}` ({ext.fng_label})\n"
                                    f"Funding: `{ext.binance_funding_8h_pct:+.4f}%`/8h",
                                    alert_type="SIGNAL",
                                )
                            elif executor:
                                side_str = signal.side.value if hasattr(signal.side, "value") else str(signal.side)
                                side_emoji = "🟢" if side_str == "BUY" else "🔴"
                                logger.info(
                                    "LIVE SIGNAL: %s %s @ %.2f (SL=%.2f, Size=%.5f BTC) [F&G=%d, Funding=%+.4f%%]",
                                    signal.strategy,
                                    side_str,
                                    approved.approved_price,
                                    approved.approved_stop_loss,
                                    approved.approved_size,
                                    ext.fng_value,
                                    ext.binance_funding_8h_pct,
                                )
                                await alerter.send(
                                    f"🚀 *LIVE ORDER ENVIADA*\n"
                                    f"{side_emoji} `{signal.strategy}` {side_str}\n"
                                    f"Entry: `${approved.approved_price:,.2f}`\n"
                                    f"SL: `${approved.approved_stop_loss:,.2f}`\n"
                                    f"Size: `{approved.approved_size:.5f} BTC` (~${approved.approved_size * approved.approved_price:.2f})\n"
                                    f"Type: `{signal.order_type.value if hasattr(signal.order_type, 'value') else signal.order_type}`\n"
                                    f"Fear&Greed: `{ext.fng_value}` ({ext.fng_label})\n"
                                    f"Funding: `{ext.binance_funding_8h_pct:+.4f}%`/8h",
                                    alert_type="SIGNAL",
                                )
                                await executor.execute(approved)
                                if executor.last_order_filled:
                                    decision_engine.mark_signal_dispatched()
                                    decision_engine.register_trade_executed()
                                else:
                                    logger.info(
                                        "Orden no se llenó — esperando solo 60s sin consumir trade diario"
                                    )
                                    decision_engine.mark_signal_rejected(60.0)

                        except Exception as veto:
                            logger.info("Signal vetoed: %s", veto)
                            # Marcar señal rechazada → cooldown 60s anti-spam
                            decision_engine.mark_signal_rejected(60.0)
                            if not settings.shadow_mode and alerter:
                                try:
                                    # v7: anti-spam — solo enviar alerta si cambió la razón o pasaron 30 min
                                    veto_str = str(veto)
                                    if veto_str != _last_veto_alert_reason or (time.monotonic() - _last_veto_alert_at) > 1800:
                                        _last_veto_alert_reason = veto_str
                                        _last_veto_alert_at = time.monotonic()
                                        await alerter.send(
                                            f"🛡️ *SEÑAL VETADA POR RIESGO*\n"
                                            f"Estrategia: `{signal.strategy}` ({signal.side.value if hasattr(signal.side, 'value') else signal.side})\n"
                                            f"Motivo: `{veto_str}`\n"
                                            f"Capital protegido. Reevaluando en 60s...",
                                            alert_type="WARNING",
                                        )
                                except Exception:
                                    pass
                            await db_worker.put(
                                DBEvent(
                                    table="decisions",
                                    data={
                                        "signal_id": getattr(signal, "signal_id", str(id(signal))),
                                        "symbol": settings.trading_symbol,
                                        "strategy": signal.strategy,
                                        "side": signal.side.value if hasattr(signal.side, "value") else str(signal.side),
                                        "regime": signal.regime.value if hasattr(signal.regime, "value") else str(signal.regime),
                                        "obi": signal.obi,
                                        "cvd_1m": signal.cvd_1m,
                                        "cvd_5m": signal.cvd_5m,
                                        "basis_bps": signal.basis_bps,
                                        "oi_change_5m": signal.oi_change_5m,
                                        "proposed_price": signal.proposed_price,
                                        "proposed_size": None,
                                        "stop_loss": signal.stop_loss,
                                        "shadow_mode": 1 if settings.shadow_mode else 0,
                                        "vetoed": 1,
                                        "veto_reason": str(veto),
                                    },
                                )
                            )

            except Exception:
                logger.exception("Error in signal evaluation loop")

    async def heartbeat_loop() -> None:
        """FIX #4: Actualiza BD cada 30s (watchdog MAX_HEARTBEAT_AGE_S=90).
        Envía resumen a Telegram cada 3600s."""
        telegram_interval = 3600
        db_interval = 30
        elapsed = 0
        while True:
            await asyncio.sleep(db_interval)
            elapsed += db_interval
            try:
                # Heartbeat en BD cada 30s para que el watchdog no marque frozen
                await set_bot_state(db, BotProcessState.RUNNING)
                # Alertar a Telegram solo cada hora
                if elapsed >= telegram_interval:
                    elapsed = 0
                    await alerter.send_heartbeat(
                        market_state,
                        state_machine.state.value,
                    )
            except Exception:
                logger.exception("Heartbeat error")

    async def shadow_pnl_resolver_loop() -> None:
        """
        Resuelve el PnL virtual (shadow_pnl) para decisiones en modo sombra.
        Corre cada 30s. Para cada decisión aprobada (vetoed=0, shadow_mode=1),
        evalúa horizontes de 5m (300s), 15m (900s) y 1h (3600s).
        Calcula pnl_pct e inserta la fila en shadow_pnl para el Dashboard.
        """
        horizons = [
            ("5m", 300),
            ("15m", 900),
            ("1h", 3600),
        ]
        while True:
            await asyncio.sleep(30)
            try:
                rows = await db.execute_fetchall(
                    """
                    SELECT id, side, proposed_price, created_at
                    FROM decisions
                    WHERE vetoed = 0 AND shadow_mode = 1
                      AND proposed_price IS NOT NULL AND proposed_price > 0
                    ORDER BY id ASC
                    """
                )
                if not rows:
                    continue

                existing_rows = await db.execute_fetchall(
                    "SELECT decision_id, horizon_label FROM shadow_pnl"
                )
                existing = {(r[0], r[1]) for r in existing_rows}

                now = time.time()
                current_price = (
                    market_state.asset_ctx.mark_price
                    or (market_state.candles_1m[-1].close if market_state.candles_1m else None)
                    or (market_state.l2_book.mid_price if market_state.l2_book else None)
                )
                if not current_price or current_price <= 0:
                    continue

                for dec_id, side, entry_price, created_at in rows:
                    age_s = now - created_at
                    for label, duration_s in horizons:
                        if (dec_id, label) in existing:
                            continue
                        if age_s >= duration_s:
                            target_ts_ms = int((created_at + duration_s) * 1000)
                            exit_price = current_price
                            matching_candles = [
                                c for c in market_state.candles_1m
                                if abs(c.timestamp_ms - target_ts_ms) <= 60000
                            ]
                            if matching_candles:
                                exit_price = matching_candles[0].close

                            TAKER_ROUNDTRIP_FEE_PCT = 0.000864  # 2 x 0.0432% Taker fee en Hyperliquid
                            if str(side).upper() == "BUY":
                                pnl_pct = ((exit_price - entry_price) / entry_price) - TAKER_ROUNDTRIP_FEE_PCT
                            else:
                                pnl_pct = ((entry_price - exit_price) / entry_price) - TAKER_ROUNDTRIP_FEE_PCT

                            await db.execute(
                                """
                                INSERT OR IGNORE INTO shadow_pnl
                                (decision_id, horizon_label, entry_price, exit_price, pnl_pct, resolved_at)
                                VALUES (?, ?, ?, ?, ?, ?)
                                """,
                                (dec_id, label, entry_price, exit_price, pnl_pct, now),
                            )
                            await db.commit()
                            existing.add((dec_id, label))
                            logger.info(
                                "Shadow PnL resuelto: Decisión #%d (%s) [%s] Entry=%.2f Exit=%.2f PnL=%+.4f%%",
                                dec_id, side, label, entry_price, exit_price, pnl_pct * 100,
                            )
            except Exception:
                logger.exception("Error en shadow_pnl_resolver_loop")

    # ── 7. Arrancar todo ───────────────────────────────────────────────────────
    logger.info("Starting all async tasks...")

    try:
        async def telegram_loop() -> None:
            """Arranca Telegram con reintentos infinitos — nunca bloquea el bot principal."""
            nonlocal alerter   # FIX #3: vincular alerter al scope externo
            import telegram.error
            attempt = 0
            while True:
                attempt += 1
                # Reconstruir app en cada intento para evitar estado corrupto
                _app, _alerter = build_telegram_app(
                    market_state=market_state,
                    kill_switch_fn=kill_switch_fn,
                    pause_fn=pause_fn,
                    db=db,
                    state_machine=state_machine,
                )
                try:
                    await _app.initialize()
                    await _app.start()
                    alerter = _alerter   # FIX #3: actualizar referencia tras initialize()
                    # Re-vincular alerter al ingestor y executor tras el reinicio de Telegram
                    ingestor.set_dependencies(executor=executor, alerter=alerter)
                    if executor:
                        executor.set_alerter(alerter)
                    logger.info("Telegram bot @CriptoLiquidBot activo (intento %d)", attempt)
                    await _app.updater.start_polling(drop_pending_updates=True)
                    return  # polling terminó limpiamente
                except telegram.error.Conflict:
                    wait = min(60, 15 * attempt)  # máximo 60s de espera
                    logger.warning(
                        "Telegram Conflict — otra instancia activa. "
                        "Cierra el CMD con python main.py si tienes uno abierto. "
                        "Reintentando en %ds... (intento %d)",
                        wait, attempt,
                    )
                    try:
                        await _app.stop()
                        await _app.shutdown()
                    except Exception:
                        pass
                    await asyncio.sleep(wait)
                except Exception as e:
                    logger.warning("Telegram no disponible: %s — bot continua sin alertas", e)
                    return

        async def reconciliation_loop() -> None:
            """
            Reconciliación periódica cada 60s:
            1. Consulta el estado real del exchange (equity, position_size).
            2. Reconcilia la máquina de estados del executor con la posición real.
            """
            while True:
                await asyncio.sleep(60)
                try:
                    await ingestor.fetch_initial_account_state()
                    if executor:
                        pos_sz = market_state.account.position_size
                        await executor.reconcile_state(pos_sz)
                except Exception:
                    logger.exception("Error en reconciliation_loop")

        # Iniciar dashboard web HTTP nativo en el puerto 8080 (Fly.io)
        from monitoring.web_server import start_web_server
        port = int(os.environ.get("PORT", "8080"))
        try:
            start_web_server(port, market_state, executor)
        except Exception as e:
            logger.warning("No se pudo iniciar el dashboard web en puerto %d: %s", port, e)

        # Cargar estado inicial de cuenta y posición desde el exchange
        try:
            await ingestor.fetch_initial_account_state()
            logger.info(
                "Estado inicial de cuenta cargado: Equity=$%.2f, Pos=%.5f BTC",
                market_state.account.equity,
                market_state.account.position_size,
            )
        except Exception as e:
            logger.warning("No se pudo cargar el estado inicial de cuenta: %s", e)

        tasks = [
            asyncio.create_task(db_worker.run(), name="db_worker"),
            asyncio.create_task(ingestor.run(db), name="ingestor"),
            asyncio.create_task(ext_signals.run(), name="external_signals"),
            asyncio.create_task(signal_evaluation_loop(), name="signal_loop"),
            asyncio.create_task(reconciliation_loop(), name="reconciliation"),
            asyncio.create_task(heartbeat_loop(), name="heartbeat"),
            asyncio.create_task(shadow_pnl_resolver_loop(), name="shadow_pnl_resolver"),
            asyncio.create_task(telegram_loop(), name="telegram"),
        ]

        await asyncio.gather(*tasks)

    except asyncio.CancelledError:
        logger.info("Main task group cancelled")
    except Exception as eg:
        logger.critical("Fatal error in task group: %s", eg)
    finally:
        await set_bot_state(db, BotProcessState.HALTED, {"reason": "shutdown"})
        await db_worker.stop()
        await db.close()
        logger.info("Bot stopped cleanly")


if __name__ == "__main__":
    asyncio.run(main())
