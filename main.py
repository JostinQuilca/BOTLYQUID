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
        logger.info("Bot paused for %ds via Telegram", seconds)

    telegram_app, alerter = build_telegram_app(
        market_state=market_state,
        kill_switch_fn=kill_switch_fn,
        pause_fn=pause_fn,
    )

    # ── 6. Bucle principal ─────────────────────────────────────────────────────
    async def signal_evaluation_loop() -> None:
        """Evalua señales cada 1 segundo (en velas de 1m)."""
        while True:
            await asyncio.sleep(1.0)
            try:
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
                                continue
                        elif side in ("SELL", "SHORT"):
                            blocked, reason = ext.should_block_short()
                            if blocked:
                                logger.info("Signal SHORT bloqueada por señal externa: %s", reason)
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

                            # Registrar decisión en la BD para el Dashboard
                            await db_worker.put(
                                DBEvent(
                                    table="decisions",
                                    data={
                                        "signal_id": getattr(signal, "signal_id", getattr(signal, "id", "")),
                                        "symbol": getattr(signal, "symbol", settings.symbol),
                                        "strategy": signal.strategy,
                                        "side": signal.side.value if hasattr(signal.side, "value") else str(signal.side),
                                        "regime": signal.regime.value if hasattr(signal.regime, "value") else str(signal.regime),
                                        "obi": market_state.microstructure.obi,
                                        "cvd_1m": market_state.microstructure.cvd_1m,
                                        "cvd_5m": market_state.microstructure.cvd_5m,
                                        "basis_bps": market_state.microstructure.basis_bps,
                                        "oi_change_5m": market_state.microstructure.oi_change_5m,
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
                                await executor.execute(approved)

                        except Exception as veto:
                            logger.info("Signal vetoed: %s", veto)
                            await db_worker.put(
                                DBEvent(
                                    table="decisions",
                                    data={
                                        "signal_id": getattr(signal, "signal_id", getattr(signal, "id", "")),
                                        "symbol": getattr(signal, "symbol", settings.symbol),
                                        "strategy": signal.strategy,
                                        "side": signal.side.value if hasattr(signal.side, "value") else str(signal.side),
                                        "regime": signal.regime.value if hasattr(signal.regime, "value") else str(signal.regime),
                                        "obi": market_state.microstructure.obi,
                                        "cvd_1m": market_state.microstructure.cvd_1m,
                                        "cvd_5m": market_state.microstructure.cvd_5m,
                                        "basis_bps": market_state.microstructure.basis_bps,
                                        "oi_change_5m": market_state.microstructure.oi_change_5m,
                                        "proposed_price": getattr(signal, "proposed_price", getattr(signal, "entry_price", None)),
                                        "proposed_size": getattr(signal, "size", None),
                                        "stop_loss": getattr(signal, "stop_loss", None),
                                        "shadow_mode": 1 if settings.shadow_mode else 0,
                                        "vetoed": 1,
                                        "veto_reason": str(veto),
                                    },
                                )
                            )

            except Exception:
                logger.exception("Error in signal evaluation loop")

    async def heartbeat_loop() -> None:
        """Envía heartbeat a Telegram cada hora."""
        while True:
            await asyncio.sleep(3600)
            try:
                await alerter.send_heartbeat(
                    market_state,
                    state_machine.state.value,
                )
                # Actualizar heartbeat en BD
                await set_bot_state(db, BotProcessState.RUNNING)
            except Exception:
                logger.exception("Heartbeat error")

    # ── 7. Arrancar todo ───────────────────────────────────────────────────────
    logger.info("Starting all async tasks...")

    try:
        async def telegram_loop() -> None:
            """Arranca Telegram con reintentos infinitos — nunca bloquea el bot principal."""
            import telegram.error
            attempt = 0
            while True:
                attempt += 1
                # Reconstruir app en cada intento para evitar estado corrupto
                _app, _alerter = build_telegram_app(
                    market_state=market_state,
                    kill_switch_fn=kill_switch_fn,
                    pause_fn=pause_fn,
                )
                try:
                    await _app.initialize()
                    await _app.start()
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

        tasks = [
            asyncio.create_task(db_worker.run(), name="db_worker"),
            asyncio.create_task(ingestor.run(db), name="ingestor"),
            asyncio.create_task(ext_signals.run(), name="external_signals"),
            asyncio.create_task(signal_evaluation_loop(), name="signal_loop"),
            asyncio.create_task(heartbeat_loop(), name="heartbeat"),
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
