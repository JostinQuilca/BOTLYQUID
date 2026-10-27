"""
monitoring/telegram_bot.py
===========================
Bot de Telegram bidireccional con rate limiting y comandos de control.

Comandos autorizados (solo para TELEGRAM_CHAT_ID configurado):
  /status   — Estado del bot y métricas de sistema
  /posicion — Posición actual y PnL no realizado
  /riesgo   — Estado de límites de riesgo del día
  /pausar   — Pausa el bot por 1 hora
  /kill     — Activa el kill switch completo

Rate limiting: máximo 1 alerta del mismo tipo cada N segundos.
Heartbeat: mensaje automático cada hora si el bot está RUNNING.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from typing import Any, Callable, Coroutine

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

from config.settings import get_settings
from data.state import MarketState

logger = logging.getLogger(__name__)

# Rate limits por tipo de alerta (segundos mínimos entre alertas del mismo tipo)
ALERT_RATE_LIMITS: dict[str, float] = {
    "FILL": 5.0,
    "SIGNAL": 30.0,
    "RISK_WARNING": 60.0,
    "ERROR": 30.0,
    "HEARTBEAT": 3600.0,
    "KILL_SWITCH": 0.0,      # Sin rate limit: siempre se envía
    "EMERGENCY": 0.0,
}


class TelegramAlerter:
    """
    Sistema de alertas con rate limiting.
    Las alertas críticas (KILL_SWITCH, EMERGENCY) siempre se envían.
    """

    def __init__(self, app: Application) -> None:
        self._app = app
        self._settings = get_settings()
        self._last_sent: dict[str, float] = defaultdict(float)

    async def send(
        self,
        text: str,
        alert_type: str = "INFO",
        parse_mode: str = ParseMode.MARKDOWN_V2,
    ) -> None:
        """
        Envía un mensaje respetando el rate limit del tipo de alerta.
        """
        limit = ALERT_RATE_LIMITS.get(alert_type, 60.0)
        now = time.time()

        if now - self._last_sent[alert_type] < limit:
            logger.debug(
                "Alert '%s' suppressed by rate limit (%.0fs remaining)",
                alert_type,
                limit - (now - self._last_sent[alert_type]),
            )
            return

        try:
            await self._app.bot.send_message(
                chat_id=self._settings.telegram_chat_id,
                text=text,
                parse_mode=parse_mode,
            )
            self._last_sent[alert_type] = now
        except Exception:
            try:
                # Fallback sin parse_mode para evitar que caracteres especiales bloqueen la alerta
                clean_text = text.replace("*", "").replace("`", "").replace("\\", "")
                await self._app.bot.send_message(
                    chat_id=self._settings.telegram_chat_id,
                    text=clean_text,
                )
                self._last_sent[alert_type] = now
            except Exception:
                logger.exception("Failed to send Telegram alert")

    async def send_fill(self, side: str, size: float, price: float, pnl: float | None = None) -> None:
        emoji = "🟢" if side == "BUY" else "🔴"
        pnl_str = f"\nPnL: `{pnl:+.2f} USDC`" if pnl is not None else ""
        await self.send(
            f"{emoji} *FILL* \\- {side}\n"
            f"Size: `{size:.5f} BTC`\n"
            f"Price: `${price:,.2f}`{pnl_str}",
            alert_type="FILL",
        )

    async def send_kill_switch(self, reason: str) -> None:
        await self.send(
            f"🚨 *KILL SWITCH ACTIVATED*\n`{reason}`",
            alert_type="KILL_SWITCH",
        )

    async def send_emergency(self, message: str) -> None:
        await self.send(
            f"🆘 *EMERGENCY*\n`{message}`",
            alert_type="EMERGENCY",
        )

    async def send_heartbeat(self, state: MarketState, executor_state: str) -> None:
        await self.send(
            f"💓 *Heartbeat*\n"
            f"Executor: `{executor_state}`\n"
            f"Tick age: `{state.last_tick_age_ms:.0f}ms`\n"
            f"Clock skew: `{state.clock_skew_ms:.0f}ms`\n"
            f"Warm: `{state.is_warm}`",
            alert_type="HEARTBEAT",
        )


class TelegramCommandHandler:
    """
    Manejador de comandos entrantes. Solo responde al TELEGRAM_CHAT_ID autorizado.
    """

    def __init__(
        self,
        alerter: TelegramAlerter,
        market_state: MarketState,
        kill_switch_fn: Callable[[], Coroutine[Any, Any, None]],
        pause_fn: Callable[[int], Coroutine[Any, Any, None]],
    ) -> None:
        self._alerter = alerter
        self._state = market_state
        self._kill_switch_fn = kill_switch_fn
        self._pause_fn = pause_fn
        self._settings = get_settings()

    def _is_authorized(self, update: Update) -> bool:
        return update.effective_chat is not None and (
            update.effective_chat.id == self._settings.telegram_chat_id
        )

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_authorized(update):
            return
        s = self._state
        msg = (
            f"📊 *Bot Status*\n"
            f"Warm: `{s.is_warm}`\n"
            f"Tick age: `{s.last_tick_age_ms:.0f}ms`\n"
            f"Clock skew: `{s.clock_skew_ms:.0f}ms`\n"
            f"Mark: `${s.asset_ctx.mark_price:,.2f}`\n"
            f"Funding: `{s.asset_ctx.funding_annualized_pct:.2f}%/yr`\n"
            f"OI: `{s.asset_ctx.open_interest:,.0f}`"
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)

    async def cmd_posicion(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_authorized(update):
            return
        acc = self._state.account
        if not acc.has_position:
            await update.message.reply_text("📭 Sin posición abierta")
            return
        msg = (
            f"📈 *Posición Activa*\n"
            f"Size: `{acc.position_size:.5f} BTC`\n"
            f"Entry: `${acc.position_entry_price:,.2f}`\n"
            f"PnL: `{acc.unrealized_pnl:+.2f} USDC`\n"
            f"Leverage: `{acc.effective_leverage:.2f}x`"
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)

    async def cmd_riesgo(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_authorized(update):
            return
        msg = (
            f"🛡️ *Límites de Riesgo*\n"
            f"Equity: `${self._state.account.equity:,.2f}`\n"
            f"Max leverage: `{self._settings.max_leverage}x`\n"
            f"Risk/trade: `{self._settings.risk_per_trade_pct*100:.2f}%`\n"
            f"Daily limit: `{self._settings.daily_loss_limit_pct*100:.1f}%`\n"
            f"Max drawdown: `{self._settings.max_drawdown_pct*100:.1f}%`"
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)

    async def cmd_pausar(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_authorized(update):
            return
        await self._pause_fn(3600)
        await update.message.reply_text("⏸️ Bot pausado por 1 hora")

    async def cmd_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_authorized(update):
            return
        await update.message.reply_text("🚨 Activando kill switch...")
        await self._kill_switch_fn()
        await self._alerter.send_kill_switch("Telegram command /kill")


def build_telegram_app(
    market_state: MarketState,
    kill_switch_fn: Callable[[], Coroutine[Any, Any, None]],
    pause_fn: Callable[[int], Coroutine[Any, Any, None]],
) -> tuple[Application, TelegramAlerter]:
    """Construye y retorna la app de Telegram y el alerter."""
    settings = get_settings()
    app = Application.builder().token(
        settings.telegram_bot_token.get_secret_value()
    ).build()

    alerter = TelegramAlerter(app)
    handler = TelegramCommandHandler(alerter, market_state, kill_switch_fn, pause_fn)

    app.add_handler(CommandHandler("status", handler.cmd_status))
    app.add_handler(CommandHandler("posicion", handler.cmd_posicion))
    app.add_handler(CommandHandler("riesgo", handler.cmd_riesgo))
    app.add_handler(CommandHandler("pausar", handler.cmd_pausar))
    app.add_handler(CommandHandler("kill", handler.cmd_kill))

    return app, alerter
