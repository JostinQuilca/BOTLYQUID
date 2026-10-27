"""
monitoring/watchdog.py
======================
Watchdog externo e independiente del proceso principal del bot.

Correcciones integradas:
- CORRECCIÓN #5: No alerta durante los primeros 60s de estado STARTING.
  Sin esto, el watchdog lanzaría falsos positivos durante el arranque.

Ejecutar como proceso separado:
  python -m monitoring.watchdog

El watchdog NUNCA importa ni depende del proceso principal del bot.
Lee únicamente la base de datos SQLite en modo read-only.
"""
from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

import aiosqlite
import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WATCHDOG] %(levelname)s %(message)s",
)
logger = logging.getLogger("watchdog")

DB_PATH = "data/db/bot.db"
CHECK_INTERVAL_S = 30.0
MAX_HEARTBEAT_AGE_S = 90.0
STARTING_GRACE_PERIOD_S = 60.0   # CORRECCIÓN #5: gracia para estado STARTING


class Watchdog:
    """
    Proceso independiente que verifica que el bot principal esté vivo.

    Lógica de alerta:
    - Lee la tabla `bot_state` en la BD (modo read-only).
    - Si estado == RUNNING y la última actualización > 90s: ALERTA
    - Si estado == STARTING y la última actualización < 60s: IGNORAR (gracia de arranque)
    - Si estado == STARTING y la última actualización > 60s: ALERTA (arrancó pero se colgó)
    - Si estado == HALTED: solo log, no alerta (parada intencional)
    - Si la BD no existe: ALERTA (bot nunca arrancó o se borró)
    """

    def __init__(self) -> None:
        from config.settings import get_settings
        self._settings = get_settings()
        self._alert_sent_at: float = 0.0
        self._alert_cooldown_s = 300.0    # Máximo 1 alerta cada 5 min

    async def run(self) -> None:
        logger.info(
            "Watchdog started. Checking every %.0fs, alerting if age > %.0fs.",
            CHECK_INTERVAL_S,
            MAX_HEARTBEAT_AGE_S,
        )
        while True:
            await asyncio.sleep(CHECK_INTERVAL_S)
            await self._check()

    async def _check(self) -> None:
        db_path = Path(DB_PATH)

        if not db_path.exists():
            await self._alert("BD no encontrada: el bot nunca arrancó o el archivo fue eliminado")
            return

        try:
            # Abrir en modo read-only para no interferir con el bot principal
            async with aiosqlite.connect(f"file:{db_path}?mode=ro", uri=True) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT state, updated_at FROM bot_state WHERE id = 1") as cur:
                    row = await cur.fetchone()
        except Exception as e:
            await self._alert(f"Error leyendo BD del bot: {e}")
            return

        if row is None:
            await self._alert("Tabla bot_state vacía: el bot nunca escribió su estado")
            return

        state = row["state"]
        updated_at = row["updated_at"]
        age_s = time.time() - updated_at

        logger.info("Bot state: %s, last update %.1fs ago", state, age_s)

        # ── CORRECCIÓN #5: Lógica de estado STARTING ──────────────────────────
        if state == "STARTING":
            if age_s < STARTING_GRACE_PERIOD_S:
                # Dentro del período de gracia: el bot está arrancando
                logger.info(
                    "Bot is STARTING (%.0fs ago). Grace period = %.0fs. No alert.",
                    age_s,
                    STARTING_GRACE_PERIOD_S,
                )
                return
            else:
                # Pasó el tiempo de gracia pero sigue en STARTING: se colgó
                await self._alert(
                    f"Bot lleva {age_s:.0f}s en estado STARTING "
                    f"(grace period {STARTING_GRACE_PERIOD_S:.0f}s). "
                    f"Posible bloqueo durante inicialización."
                )
                return

        if state == "HALTED":
            logger.info("Bot is HALTED (intentional stop). No alert.")
            return

        if state == "RUNNING" and age_s > MAX_HEARTBEAT_AGE_S:
            await self._alert(
                f"Bot CONGELADO: último heartbeat hace {age_s:.0f}s "
                f"(máximo {MAX_HEARTBEAT_AGE_S:.0f}s). "
                f"Estado: {state}"
            )

    async def _alert(self, message: str) -> None:
        """
        Envía alerta directamente via Telegram API (sin depender del bot principal).
        Rate limitado: máximo 1 alerta cada 5 minutos.
        """
        now = time.time()
        if now - self._alert_sent_at < self._alert_cooldown_s:
            logger.warning("Alert suppressed by cooldown: %s", message)
            return

        logger.critical("ALERT: %s", message)

        try:
            token = self._settings.telegram_bot_token.get_secret_value()
            chat_id = self._settings.telegram_chat_id
            text = f"🐕 *WATCHDOG ALERT*\n```\n{message}\n```"

            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={
                        "chat_id": chat_id,
                        "text": text,
                        "parse_mode": "Markdown",
                    },
                )
                resp.raise_for_status()

            self._alert_sent_at = now
            logger.info("Alert sent to Telegram")

        except Exception:
            logger.exception("Failed to send Telegram alert")


async def main() -> None:
    watchdog = Watchdog()
    await watchdog.run()


if __name__ == "__main__":
    asyncio.run(main())
