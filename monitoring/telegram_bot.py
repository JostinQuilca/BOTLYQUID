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
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine

import aiosqlite
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

from config.settings import get_settings
from data.state import MarketState

logger = logging.getLogger(__name__)

# Rate limits por tipo de alerta (segundos mínimos entre alertas del mismo tipo)
ALERT_RATE_LIMITS: dict[str, float] = {
    "FILL": 0.0,             # Sin rate limit: alertas inmediatas de órdenes y cierres
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
    Dashboard móvil interactivo para monitoreo en vivo de PnL y posición.
    """

    def __init__(
        self,
        alerter: TelegramAlerter,
        market_state: MarketState,
        kill_switch_fn: Callable[[], Coroutine[Any, Any, None]],
        pause_fn: Callable[[int], Coroutine[Any, Any, None]],
        db: aiosqlite.Connection | None = None,
        state_machine: Any = None,
    ) -> None:
        self._alerter = alerter
        self._state = market_state
        self._kill_switch_fn = kill_switch_fn
        self._pause_fn = pause_fn
        self._db = db
        self._sm = state_machine
        self._settings = get_settings()

    def _is_authorized(self, update: Update) -> bool:
        return update.effective_chat is not None and (
            update.effective_chat.id == self._settings.telegram_chat_id
        )

    async def cmd_ayuda(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_authorized(update):
            return
        msg = (
            "🤖 *CyberVision HL Bot — Centro de Control*\n\n"
            "Comandos disponibles:\n"
            "📊 /status — Estado del bot, ejecutor y última señal evaluada\n"
            "📈 /posicion — Posición en vivo, PnL flotante, TP y SL activos\n"
            "💰 /pnl — Dashboard financiero completo: hoy y acumulado histórico\n"
            "📜 /historial — Lista de últimas 10 operaciones cerradas con PnL neto\n"
            "🛡️ /riesgo — Límites de apalancamiento y drawdown\n"
            "⏸️ /pausar — Pausa evaluación por 1 hora\n"
            "🚨 /kill — Kill switch inmediato (cierra y cancela todo)"
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_authorized(update):
            return
        s = self._state
        acc = s.account
        sm_state = self._sm.state.value if (self._sm and hasattr(self._sm, "state")) else "UNKNOWN"

        last_decision_str = "Sin decisiones previas"
        if self._db:
            try:
                async with self._db.execute(
                    "SELECT agreed_side, agreement_ratio, meta_confidence, created_at "
                    "FROM decisions ORDER BY id DESC LIMIT 1"
                ) as cur:
                    row = await cur.fetchone()
                    if row:
                        side, ratio, conf, _ = row
                        last_decision_str = f"{side} (Consenso: {ratio*100:.0f}%, Conf: {conf*100:.0f}%)"
            except Exception:
                pass

        msg = (
            f"📊 *Bot Status & Sistema*\n\n"
            f"• Estado Ejecutor: `{sm_state}`\n"
            f"• Capital (Equity): `${acc.equity:,.2f} USDC`\n"
            f"• Margen Disponible: `${acc.available_margin:,.2f} USDC`\n"
            f"• Mark Price BTC: `${s.asset_ctx.mark_price:,.2f}`\n"
            f"• Latencia WS: `{s.last_tick_age_ms:.0f}ms`\n"
            f"• Funding: `{s.asset_ctx.funding_annualized_pct:.2f}%/año`\n"
            f"• Open Interest: `{s.asset_ctx.open_interest:,.0f}`\n"
            f"• Última Evaluación: `{last_decision_str}`"
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def cmd_posicion(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_authorized(update):
            return
        acc = self._state.account
        if not acc.has_position:
            await update.message.reply_text(
                f"📭 *Sin posición abierta en Hyperliquid*\n\n"
                f"• Capital: `${acc.equity:,.2f} USDC`\n"
                f"• Margen disponible: `${acc.available_margin:,.2f} USDC`",
                parse_mode=ParseMode.MARKDOWN,
            )
            return

        is_buy = acc.position_size > 0
        side_lbl = "LONG 🟢" if is_buy else "SHORT 🔴"
        mark_px = self._state.asset_ctx.mark_price
        notional = abs(acc.position_size) * mark_px
        pnl = acc.unrealized_pnl
        pnl_emoji = "🟢" if pnl >= 0 else "🔴"

        roe_str = ""
        if acc.margin_used > 0:
            roe = (pnl / acc.margin_used) * 100
            roe_str = f" ({roe:+.2f}% ROE)"

        tp_str = f"${acc.active_tp_px:,.2f}" if acc.active_tp_px > 0 else "No detectado"
        sl_str = f"${acc.active_sl_px:,.2f}" if acc.active_sl_px > 0 else "No detectado"

        msg = (
            f"📈 *Posición Activa ({side_lbl})*\n\n"
            f"• Tamaño: `{abs(acc.position_size):.5f} BTC` (~${notional:.2f} USD)\n"
            f"• Entrada: `${acc.position_entry_price:,.2f}`\n"
            f"• Mark Price: `${mark_px:,.2f}`\n"
            f"• PnL Flotante: {pnl_emoji} `{pnl:+.4f} USDC`{roe_str}\n"
            f"• Margen Usado: `${acc.margin_used:,.2f} USDC`\n"
            f"• Apalancamiento: `{acc.effective_leverage:.2f}x`\n\n"
            f"🎯 *Take Profit:* `{tp_str}`\n"
            f"🛑 *Stop Loss:* `{sl_str}`"
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def cmd_pnl(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_authorized(update):
            return
        acc = self._state.account
        from datetime import date
        today_str = date.today().isoformat()
        today_pnl = 0.0

        total_closed = 0
        wins = 0
        losses = 0
        gross_pnl = 0.0
        total_fee = 0.0
        net_pnl = 0.0
        today_trades = 0

        if self._db:
            try:
                # 1. PnL de hoy desde daily_risk
                async with self._db.execute(
                    "SELECT realized_pnl FROM daily_risk WHERE date_utc = ?",
                    (today_str,),
                ) as cur:
                    row = await cur.fetchone()
                    if row:
                        today_pnl = float(row[0] or 0.0)

                # 2. Resumen histórico de live_fills (operaciones con cierre)
                async with self._db.execute(
                    """
                    SELECT 
                        COUNT(*),
                        SUM(CASE WHEN (closed_pnl - fee) > 0 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN (closed_pnl - fee) < 0 THEN 1 ELSE 0 END),
                        SUM(closed_pnl),
                        SUM(fee),
                        SUM(closed_pnl - fee)
                    FROM live_fills
                    WHERE closed_pnl != 0.0 OR dir LIKE 'Close%'
                    """
                ) as cur:
                    row = await cur.fetchone()
                    if row and row[0]:
                        total_closed = int(row[0])
                        wins = int(row[1] or 0)
                        losses = int(row[2] or 0)
                        gross_pnl = float(row[3] or 0.0)
                        total_fee = float(row[4] or 0.0)
                        net_pnl = float(row[5] or 0.0)

                # 3. Operaciones cerradas hoy
                now_utc = datetime.now(timezone.utc)
                start_of_day_ts = int(datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc).timestamp() * 1000)
                async with self._db.execute(
                    "SELECT COUNT(*) FROM live_fills WHERE ts_ms >= ? AND (closed_pnl != 0.0 OR dir LIKE 'Close%')",
                    (start_of_day_ts,),
                ) as cur:
                    row = await cur.fetchone()
                    if row:
                        today_trades = int(row[0] or 0)
            except Exception:
                logger.exception("Error querying pnl stats from SQLite")

        win_rate = (wins / total_closed * 100) if total_closed > 0 else 0.0
        today_emoji = "🟢" if today_pnl >= 0 else "🔴"
        net_emoji = "🟢" if net_pnl >= 0 else "🔴"

        floating_str = ""
        if acc.has_position:
            fl_emoji = "🟢" if acc.unrealized_pnl >= 0 else "🔴"
            floating_str = f"\n📊 *PnL Flotante Vivo:* {fl_emoji} `{acc.unrealized_pnl:+.4f} USDC`"

        msg = (
            f"💰 *PANEL FINANCIERO & PnL*\n\n"
            f"💼 *Saldo de Cuenta:*\n"
            f"• Capital Total: `${acc.equity:,.2f} USDC`\n"
            f"• Margen Disponible: `${acc.available_margin:,.2f} USDC`"
            f"{floating_str}\n\n"
            f"📅 *Rendimiento de Hoy ({today_str}):*\n"
            f"• PnL Neto Hoy: {today_emoji} `{today_pnl:+.4f} USDC`\n"
            f"• Operaciones cerradas hoy: `{today_trades}`\n\n"
            f"🏆 *Rendimiento Histórico Real:*\n"
            f"• Total Operaciones Cerradas: `{total_closed}`\n"
            f"• Victorias / Derrotas: `{wins}W / {losses}L`\n"
            f"• Win Rate: `{win_rate:.1f}%`\n"
            f"• PnL Bruto: `{gross_pnl:+.4f} USDC`\n"
            f"• Comisiones pagadas: `-{total_fee:.4f} USDC`\n"
            f"• *PnL Neto Acumulado:* {net_emoji} *`{net_pnl:+.4f} USDC`*"
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def cmd_historial(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_authorized(update):
            return
        if not self._db:
            await update.message.reply_text("Base de datos no disponible")
            return

        lines = ["📜 *Últimas 10 Operaciones Cerradas:*\n"]
        try:
            async with self._db.execute(
                """
                SELECT dir, side, px, sz, closed_pnl, fee, ts_ms
                FROM live_fills
                WHERE closed_pnl != 0.0 OR dir LIKE 'Close%'
                ORDER BY ts_ms DESC
                LIMIT 10
                """
            ) as cur:
                rows = await cur.fetchall()
                if not rows:
                    await update.message.reply_text("📭 Sin operaciones cerradas registradas aún en `live_fills`.")
                    return

                for dir_str, side, px, sz, closed_pnl, fee, ts_ms in rows:
                    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%d/%m %H:%M")
                    net = (closed_pnl or 0.0) - (fee or 0.0)
                    res_emoji = "🟢" if net >= 0 else "🔴"
                    lines.append(
                        f"• `{dt}` | `{dir_str}`\n"
                        f"  Salida: `${px:,.1f}` | {res_emoji} PnL: `{net:+.4f} USDC` (Fee: `-{fee:.4f}`)"
                    )
        except Exception:
            logger.exception("Error reading live_fills history")
            await update.message.reply_text("Error al consultar el historial.")
            return

        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

    async def cmd_riesgo(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_authorized(update):
            return
        msg = (
            f"🛡️ *Límites de Riesgo*\n\n"
            f"• Capital: `${self._state.account.equity:,.2f} USDC`\n"
            f"• Apalancamiento Máx: `{self._settings.max_leverage}x`\n"
            f"• Riesgo por Operación: `{self._settings.risk_per_trade_pct*100:.2f}%`\n"
            f"• Límite Pérdida Diaria: `{self._settings.daily_loss_limit_pct*100:.1f}%`\n"
            f"• Drawdown Máximo: `{self._settings.max_drawdown_pct*100:.1f}%`"
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

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
    db: aiosqlite.Connection | None = None,
    state_machine: Any = None,
) -> tuple[Application, TelegramAlerter]:
    """Construye y retorna la app de Telegram y el alerter con comandos interactivos."""
    settings = get_settings()
    app = Application.builder().token(
        settings.telegram_bot_token.get_secret_value()
    ).build()

    alerter = TelegramAlerter(app)
    handler = TelegramCommandHandler(
        alerter=alerter,
        market_state=market_state,
        kill_switch_fn=kill_switch_fn,
        pause_fn=pause_fn,
        db=db,
        state_machine=state_machine,
    )

    app.add_handler(CommandHandler("start", handler.cmd_ayuda))
    app.add_handler(CommandHandler("ayuda", handler.cmd_ayuda))
    app.add_handler(CommandHandler("status", handler.cmd_status))
    app.add_handler(CommandHandler("posicion", handler.cmd_posicion))
    app.add_handler(CommandHandler("pnl", handler.cmd_pnl))
    app.add_handler(CommandHandler("historial", handler.cmd_historial))
    app.add_handler(CommandHandler("riesgo", handler.cmd_riesgo))
    app.add_handler(CommandHandler("pausar", handler.cmd_pausar))
    app.add_handler(CommandHandler("kill", handler.cmd_kill))

    return app, alerter
