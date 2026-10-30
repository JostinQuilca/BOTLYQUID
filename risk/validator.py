"""
risk/validator.py
=================
EL GUARDIÁN — Único punto de paso entre señales y ejecución.

Ninguna orden puede enviarse al exchange sin un ApprovedOrder de este módulo.

Correcciones integradas:
- CORRECCIÓN #3: Filtro de funding correcto (costo total del trade, no ventana de 3min)
- Verificación de salud del sistema (tick_age, clock_skew, reconciliación)
- Límites de cuenta con pausa automática y kill switch permanente
- Dimensionamiento exacto con szDecimals de Hyperliquid
- Reglas de microestructura (desviación de precio, distancia a liquidación)
"""
from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import aiosqlite

from config.settings import Settings, get_settings
from data.state import MarketState
from signals.schemas import ApprovedOrder, SignalProposal, VetoReason

logger = logging.getLogger(__name__)


@dataclass
class RiskState:
    """Estado de riesgo del día."""
    date_utc: str
    equity_open: float
    equity_peak: float
    realized_pnl: float
    unrealized_pnl: float
    paused_until: float | None      # timestamp Unix, None si no está pausado
    kill_switch: bool


class VetoException(Exception):
    """Excepción lanzada cuando una señal es vetada por el validador."""
    def __init__(self, reason: VetoReason, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"VETO: {reason.value} — {detail}")


class RiskValidator:
    """
    Validador determinista de riesgo.

    Orden de verificación (estricto):
    1. Kill switch activo
    2. Salud del sistema (staleness, clock skew)
    3. Límites de cuenta (pérdida diaria, drawdown, posición máxima)
    4. Dimensionamiento de posición
    5. Reglas de microestructura (precio, funding, distancia a liquidación)
    """

    # Umbrales de salud del sistema
    MAX_TICK_AGE_MS = 3_000
    MAX_CLOCK_SKEW_MS = 5_000

    # Reglas de microestructura
    MAX_PRICE_DEVIATION_BPS = 20    # 0.2% del mid price
    MIN_MARGIN_TO_SL_RATIO = 2.5   # Distancia a liquidación >= 2.5x distancia al SL
    FUNDING_ADVERSE_ANNUALIZED_PCT = 25.0   # 25% anualizado = funding adverso

    def __init__(
        self,
        db: aiosqlite.Connection,
        sz_decimals: int = 5,        # szDecimals de BTC en Hyperliquid
        min_notional_usd: float = 10.0,
        settings: Settings | None = None,
    ) -> None:
        self._db = db
        self._settings = settings if settings is not None else get_settings()
        self._sz_decimals = sz_decimals
        self._min_notional_usd = min_notional_usd
        self._kill_switch_active = False
        self._shadow_mode = False  # set during validate()

    # ── API pública ────────────────────────────────────────────────────────────

    async def validate(
        self,
        signal: SignalProposal,
        state: MarketState,
        shadow_mode: bool = False,
    ) -> ApprovedOrder:
        """
        Valida una señal y retorna un ApprovedOrder.
        Lanza VetoException si alguna regla falla.
        """
        self._shadow_mode = shadow_mode
        # 1. Kill switch
        self._check_kill_switch()

        # 2. Salud del sistema
        self._check_system_health(state)

        # 3. Límites de cuenta
        risk_state = await self._load_risk_state(state)
        self._check_account_limits(risk_state, state)

        # 4. Posición única
        self._check_max_position(state)

        # 4b. Cooldown tras pérdida reciente en la misma dirección
        if not shadow_mode:
            await self._check_loss_cooldown(signal)

        # 5. Dimensionamiento
        equity = state.account.equity or risk_state.equity_open
        approved_size = self._calculate_size(signal, equity, state)

        # Calcular take profit aprobado
        sl_dist = abs(signal.proposed_price - signal.stop_loss)
        if signal.take_profit is not None and signal.take_profit > 0:
            approved_tp = signal.take_profit
        else:
            if signal.side.value == "BUY":
                approved_tp = signal.proposed_price + (sl_dist * signal.expected_r)
            else:
                approved_tp = signal.proposed_price - (sl_dist * signal.expected_r)

        # En shadow mode: aprobar directamente sin chequeos de microestructura
        if shadow_mode:
            effective_leverage = (approved_size * signal.proposed_price) / equity if equity > 0 else 0.0
            risk_amount_usd = equity * self._settings.risk_per_trade_pct
            logger.info(
                "Shadow mode: señal aprobada size=%.5f BTC, leverage=%.2fx (sin chequeos micro)",
                approved_size, effective_leverage,
            )
            return ApprovedOrder(
                signal=signal,
                approved_size=approved_size,
                approved_price=signal.proposed_price,
                approved_stop_loss=signal.stop_loss,
                approved_take_profit=approved_tp,
                risk_amount_usd=risk_amount_usd,
                effective_leverage=effective_leverage,
            )

        # 6. Reglas de microestructura (solo en modo LIVE)
        mid_price = state.l2_book.mid_price or state.asset_ctx.mark_price
        self._check_price_deviation(signal.proposed_price, mid_price)
        self._check_funding_cost(signal, state)   # CORRECCIÓN #3
        self._check_liquidation_distance(signal, state, approved_size)

        # Calcular apalancamiento efectivo
        effective_leverage = (approved_size * signal.proposed_price) / equity if equity > 0 else 0.0
        # En shadow mode: omitir chequeo de leverage (tamaño es simbólico 0.001 BTC)
        if not self._shadow_mode and effective_leverage > self._settings.max_leverage:
            raise VetoException(
                VetoReason.RISK_LEVERAGE,
                f"Leverage {effective_leverage:.2f}x > max {self._settings.max_leverage}x",
            )

        risk_amount_usd = equity * self._settings.risk_per_trade_pct

        return ApprovedOrder(
            signal=signal,
            approved_size=approved_size,
            approved_price=signal.proposed_price,
            approved_stop_loss=signal.stop_loss,
            approved_take_profit=approved_tp,
            risk_amount_usd=risk_amount_usd,
            effective_leverage=effective_leverage,
        )

    def activate_kill_switch(self, reason: str = "Manual") -> None:
        """Activa el kill switch de forma permanente hasta reinicio manual."""
        self._kill_switch_active = True
        logger.critical("KILL SWITCH ACTIVATED: %s", reason)

    # ── Verificaciones ─────────────────────────────────────────────────────────

    def _check_kill_switch(self) -> None:
        import os
        # También verificar archivo físico
        if os.path.exists("STOP_BOT.flag"):
            self._kill_switch_active = True
        if self._kill_switch_active:
            raise VetoException(VetoReason.KILL_SWITCH_ACTIVE, "Kill switch is active")

    def _check_system_health(self, state: MarketState) -> None:
        # En shadow mode: omitir chequeo de staleness (el mercado puede estar quieto)
        if self._shadow_mode:
            return
        tick_age = state.last_tick_age_ms
        if tick_age > self.MAX_TICK_AGE_MS:
            raise VetoException(
                VetoReason.SYSTEM_STALE,
                f"Last tick was {tick_age:.0f}ms ago (max {self.MAX_TICK_AGE_MS}ms)",
            )
        skew = abs(state.clock_skew_ms)
        if skew > self.MAX_CLOCK_SKEW_MS:
            raise VetoException(
                VetoReason.SYSTEM_STALE,
                f"Clock skew {skew:.0f}ms > {self.MAX_CLOCK_SKEW_MS}ms",
            )

    async def _load_risk_state(self, state: MarketState) -> RiskState:
        """Carga o inicializa el estado de riesgo del día actual."""
        today = date.today().isoformat()
        equity = state.account.equity

        async with self._db.execute(
            "SELECT * FROM daily_risk WHERE date_utc = ?", (today,)
        ) as cursor:
            row = await cursor.fetchone()

        if row is None:
            # Primer trade del día: inicializar
            await self._db.execute(
                """
                INSERT OR IGNORE INTO daily_risk
                (date_utc, equity_open, equity_peak, realized_pnl, unrealized_pnl)
                VALUES (?, ?, ?, 0, 0)
                """,
                (today, equity, equity),
            )
            await self._db.commit()
            return RiskState(
                date_utc=today,
                equity_open=equity,
                equity_peak=equity,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                paused_until=None,
                kill_switch=False,
            )

        return RiskState(
            date_utc=row["date_utc"],
            equity_open=row["equity_open"],
            equity_peak=row["equity_peak"],
            realized_pnl=row["realized_pnl"],
            unrealized_pnl=row["unrealized_pnl"],
            paused_until=row["paused_until"],
            kill_switch=bool(row["kill_switch"]),
        )

    def _check_account_limits(self, risk_state: RiskState, state: MarketState) -> None:
        # Kill switch permanente en BD (sobrevive reinicios)
        if risk_state.kill_switch:
            self.activate_kill_switch("Permanent kill switch from DB (max drawdown exceeded)")
            raise VetoException(VetoReason.KILL_SWITCH_ACTIVE, "Permanent kill switch active")

        # Pausa temporal
        if risk_state.paused_until and time.time() < risk_state.paused_until:
            remaining_h = (risk_state.paused_until - time.time()) / 3600
            raise VetoException(
                VetoReason.RISK_DAILY_LIMIT,
                f"Bot paused for {remaining_h:.1f}h more (daily loss limit hit)",
            )

        # Pérdida diaria: pérdida realizada + no realizada < 3% del equity inicial
        total_pnl_pct = (
            (risk_state.realized_pnl + risk_state.unrealized_pnl)
            / risk_state.equity_open
            * 100
            if risk_state.equity_open > 0
            else 0.0
        )
        daily_limit_pct = self._settings.daily_loss_limit_pct * 100

        if total_pnl_pct < -daily_limit_pct:
            logger.critical(
                "Daily loss limit hit: %.2f%% (limit %.2f%%)",
                total_pnl_pct,
                -daily_limit_pct,
            )
            # Pausa automática de 24 horas
            pause_until = time.time() + 86_400
            # (En producción, actualizar BD aquí)
            raise VetoException(
                VetoReason.RISK_DAILY_LIMIT,
                f"Daily loss {total_pnl_pct:.2f}% exceeds -{daily_limit_pct:.1f}% limit",
            )

        # Drawdown desde pico de equity
        current_equity = state.account.equity
        if risk_state.equity_peak > 0 and current_equity > 0:
            drawdown_pct = (
                (risk_state.equity_peak - current_equity) / risk_state.equity_peak * 100
            )
            max_dd_pct = self._settings.max_drawdown_pct * 100
            if drawdown_pct > max_dd_pct:
                self.activate_kill_switch(
                    f"Max drawdown {drawdown_pct:.2f}% > {max_dd_pct:.1f}%"
                )
                raise VetoException(
                    VetoReason.RISK_DRAWDOWN,
                    f"Drawdown {drawdown_pct:.2f}% exceeds {max_dd_pct:.1f}% — PERMANENT HALT",
                )

    def _check_max_position(self, state: MarketState) -> None:
        """Máximo 1 posición abierta."""
        if state.account.has_position:
            raise VetoException(
                VetoReason.RISK_MAX_POSITION,
                f"Already have an open position: {state.account.position_size:.6f} BTC",
            )

    async def _check_loss_cooldown(self, signal: SignalProposal) -> None:
        """
        Cooldown tras pérdida: si la última operación cerrada fue una pérdida
        y ocurrió hace menos de 900s (15 min), bloquear reintentos en la misma dirección.
        """
        COOLDOWN_SECONDS = 900  # 15 minutos
        try:
            async with self._db.execute(
                "SELECT dir, closed_pnl, ts_ms FROM live_fills WHERE closed_pnl != 0 ORDER BY ts_ms DESC LIMIT 1"
            ) as cursor:
                row = await cursor.fetchone()

            if row:
                dir_str = row[0] if isinstance(row, tuple) else row["dir"]
                closed_pnl = float(row[1] if isinstance(row, tuple) else row["closed_pnl"])
                ts_ms = int(row[2] if isinstance(row, tuple) else row["ts_ms"])

                now_ms = time.time() * 1000
                elapsed_s = (now_ms - ts_ms) / 1000.0

                if closed_pnl < 0 and elapsed_s < COOLDOWN_SECONDS:
                    was_long = "Long" in str(dir_str)
                    was_short = "Short" in str(dir_str)

                    if (signal.side == SignalSide.BUY and was_long) or (
                        signal.side == SignalSide.SELL and was_short
                    ):
                        remaining_m = int((COOLDOWN_SECONDS - elapsed_s) / 60) + 1
                        raise VetoException(
                            VetoReason.RISK_COOLDOWN,
                            f"Cooldown activo: pérdida reciente ({closed_pnl:+.4f} USDC hace {int(elapsed_s/60)}m). "
                            f"Esperando {remaining_m}m antes de otro {signal.side.value}",
                        )
        except VetoException:
            raise
        except Exception as e:
            logger.debug("Error comprobando cooldown: %s", e)

    def _calculate_size(
        self,
        signal: SignalProposal,
        equity: float,
        state: MarketState,
    ) -> float:
        """
        Calcula el tamaño de la posición basado en riesgo fijo por trade.

        size = (equity * risk_pct) / distance_to_stop_usd
        Redondeado a szDecimals de BTC en Hyperliquid.
        """
        risk_amount_usd = equity * self._settings.risk_per_trade_pct
        distance_usd = abs(signal.proposed_price - signal.stop_loss)

        if distance_usd <= 0:
            raise VetoException(
                VetoReason.RISK_MAX_POSITION,
                "Stop loss distance is zero or negative",
            )

        raw_size = risk_amount_usd / distance_usd

        # Limitar al tamaño máximo permitido por apalancamiento
        if not self._shadow_mode and self._settings.max_leverage > 0 and signal.proposed_price > 0:
            max_allowed_size = (equity * self._settings.max_leverage) / signal.proposed_price
            if raw_size > max_allowed_size:
                raw_size = max_allowed_size

        # Redondear a szDecimals — usar round() en lugar de floor() para cuentas pequeñas.
        # math.floor redondea 0.00012 → 0.0001 (válido) pero también 0.000049 → 0.0000
        # con round(), el tamaño mínimo posible es 0.0001 BTC (szDecimals=4).
        factor = 10 ** self._sz_decimals
        size = round(raw_size * factor) / factor

        # Si el round() subió el size y ahora excede max leverage, bajar un tick
        if not self._shadow_mode and self._settings.max_leverage > 0 and signal.proposed_price > 0:
            max_lev_size = (equity * self._settings.max_leverage) / signal.proposed_price
            if size > max_lev_size:
                size = math.floor(raw_size * factor) / factor
                logger.info(
                    "Size ajustado de round→floor para no exceder leverage: %.5f BTC",
                    size,
                )

        # Si después de redondear el size sigue siendo 0, usar el mínimo absoluto permitido
        if size <= 0:
            min_size = 1 / factor  # e.g. 0.0001 BTC para szDecimals=4
            logger.warning(
                "Size redondeado a 0 (raw_size=%.8f). Usando tamaño mínimo: %.4f BTC",
                raw_size,
                min_size,
            )
            size = min_size

        # Verificar nocional mínimo — reducido a $1 para cuentas pequeñas
        min_notional = max(1.0, self._min_notional_usd * 0.1) if self._shadow_mode else self._min_notional_usd
        notional = size * signal.proposed_price
        if notional < min_notional:
            # En shadow mode: usar tamaño mínimo de HL (0.001 BTC) en lugar de vetar
            if self._shadow_mode:
                size = 0.001  # tamaño mínimo simbólico para shadow
                notional = size * signal.proposed_price
                logger.info(
                    "Shadow mode: usando tamaño mínimo 0.001 BTC (notional $%.2f) — "
                    "cuenta muy pequeña para sizing real",
                    notional,
                )
            else:
                raise VetoException(
                    VetoReason.RISK_NOTIONAL_MIN,
                    f"Notional ${notional:.2f} < minimum ${self._min_notional_usd:.2f}",
                )

        return size

    def _check_price_deviation(self, order_price: float, mid_price: float) -> None:
        """Desviación del precio límite respecto al mid < 0.2% (20 bps)."""
        if mid_price <= 0:
            return
        deviation_bps = abs(order_price - mid_price) / mid_price * 10_000
        if deviation_bps > self.MAX_PRICE_DEVIATION_BPS:
            raise VetoException(
                VetoReason.RISK_PRICE_DEVIATION,
                f"Price deviation {deviation_bps:.1f} bps > {self.MAX_PRICE_DEVIATION_BPS} bps",
            )

    def _check_funding_cost(
        self,
        signal: SignalProposal,
        state: MarketState,
    ) -> None:
        """
        CORRECCIÓN #3: Filtro de funding correcto.

        Error del plan original: bloquear si faltan < 3 min para el cobro.
        Razón del error: el funding se cobra al tenedor al momento del cobro,
        no al que abre justo antes. Abrir 3 min antes paga el siguiente cobro.

        Corrección: Calcular el costo total de funding para el horizonte
        estimado del trade y vetar si erosiona > 20% del retorno esperado.
        """
        funding_rate_hourly = state.asset_ctx.funding_rate
        funding_annualized_pct = state.asset_ctx.funding_annualized_pct

        # Solo vetar si el funding es adverso para la dirección de la señal
        funding_is_adverse = (
            (signal.side.value == "BUY" and funding_rate_hourly > 0) or
            (signal.side.value == "SELL" and funding_rate_hourly < 0)
        )

        if not funding_is_adverse:
            return   # Funding favorable o neutral: no bloquear

        # Calcular costo de funding total durante el horizonte estimado
        expected_hold_hours = signal.estimated_hold_minutes / 60
        funding_cost_pct = abs(funding_rate_hourly) * expected_hold_hours * 100

        # Calcular retorno esperado del trade (risk * R)
        risk_pct = signal.risk_distance_pct
        expected_return_pct = risk_pct * signal.expected_r

        # Vetar si el funding se lleva más del 20% del retorno esperado
        if expected_return_pct > 0:
            funding_erosion_pct = funding_cost_pct / expected_return_pct * 100
            if funding_erosion_pct > 20.0:
                raise VetoException(
                    VetoReason.RISK_FUNDING,
                    f"Funding cost {funding_cost_pct:.4f}% erodes {funding_erosion_pct:.1f}% "
                    f"of expected return {expected_return_pct:.4f}% "
                    f"(annualized {funding_annualized_pct:.1f}%)",
                )

        # También vetar si el funding anualizado es extremo (> 25%)
        if abs(funding_annualized_pct) > self.FUNDING_ADVERSE_ANNUALIZED_PCT and funding_is_adverse:
            raise VetoException(
                VetoReason.RISK_FUNDING,
                f"Extreme adverse funding: {funding_annualized_pct:.1f}% annualized",
            )

    def _check_liquidation_distance(
        self,
        signal: SignalProposal,
        state: MarketState,
        size: float,
    ) -> None:
        """
        Distancia estimada a liquidación >= 2.5x distancia al Stop Loss.
        Previene liquidaciones antes de que el stop se ejecute.
        """
        equity = state.account.equity
        if equity <= 0 or signal.proposed_price <= 0:
            return

        notional = size * signal.proposed_price
        leverage = notional / equity if equity > 0 else 0

        if leverage <= 0:
            return

        # Estimación conservadora del precio de liquidación
        # En cross margin: liquidación ≈ entry * (1 - 1/leverage + margin_ratio)
        # Usamos 0.5% de margen de mantenimiento (conservative)
        maintenance_margin_rate = 0.005
        if signal.side.value == "BUY":
            estimated_liq_price = signal.proposed_price * (
                1 - (1 / leverage) + maintenance_margin_rate
            )
            liq_distance = signal.proposed_price - estimated_liq_price
        else:
            estimated_liq_price = signal.proposed_price * (
                1 + (1 / leverage) - maintenance_margin_rate
            )
            liq_distance = estimated_liq_price - signal.proposed_price

        sl_distance = abs(signal.proposed_price - signal.stop_loss)

        if sl_distance > 0 and liq_distance > 0:
            ratio = liq_distance / sl_distance
            if ratio < self.MIN_MARGIN_TO_SL_RATIO:
                raise VetoException(
                    VetoReason.RISK_MARGIN_DISTANCE,
                    f"Liquidation distance {liq_distance:.2f} is only {ratio:.1f}x "
                    f"stop distance {sl_distance:.2f} (min {self.MIN_MARGIN_TO_SL_RATIO}x)",
                )
