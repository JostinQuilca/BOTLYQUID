"""
signals/schemas.py
==================
Esquemas Pydantic inmutables para el Motor de Decisiones.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, computed_field


class MarketRegime(str, Enum):
    COMPRESION = "COMPRESION"
    RANGO = "RANGO"
    TENDENCIA = "TENDENCIA"
    MODO_EVENTO = "MODO_EVENTO"


class SignalSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    ALO = "ALO"     # Add Liquidity Only (Post-Only, maker rebate)
    IOC = "IOC"     # Immediate Or Cancel (taker, para breakouts)


class VetoReason(str, Enum):
    SYSTEM_STALE = "SYSTEM_STALE"
    SHADOW_MODE = "SHADOW_MODE"
    NOT_WARM = "NOT_WARM"
    REGIME_MISMATCH = "REGIME_MISMATCH"
    EVENT_BLACKOUT = "EVENT_BLACKOUT"
    RISK_DAILY_LIMIT = "RISK_DAILY_LIMIT"
    RISK_DRAWDOWN = "RISK_DRAWDOWN"
    RISK_MAX_POSITION = "RISK_MAX_POSITION"
    RISK_LEVERAGE = "RISK_LEVERAGE"
    RISK_FUNDING = "RISK_FUNDING"
    RISK_NOTIONAL_MIN = "RISK_NOTIONAL_MIN"
    RISK_PRICE_DEVIATION = "RISK_PRICE_DEVIATION"
    RISK_MARGIN_DISTANCE = "RISK_MARGIN_DISTANCE"
    KILL_SWITCH_ACTIVE = "KILL_SWITCH_ACTIVE"


class SignalProposal(BaseModel):
    """Propuesta de señal generada por el Motor Determinista. Inmutable."""

    model_config = {"frozen": True}

    signal_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    strategy: str
    side: SignalSide
    order_type: OrderType
    proposed_price: float
    stop_loss: float
    take_profit: float | None = None
    regime: MarketRegime
    obi: float | None = None
    cvd_1m: float | None = None
    cvd_5m: float | None = None
    basis_bps: float | None = None
    oi_change_5m: float | None = None
    estimated_hold_minutes: float = 30.0
    expected_r: float = 1.5          # Risk-reward esperado
    confidence: float = 0.0          # [0.0, 1.0]
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def risk_distance_pct(self) -> float:
        """Distancia porcentual al stop loss."""
        if self.proposed_price <= 0:
            return 0.0
        return abs(self.proposed_price - self.stop_loss) / self.proposed_price * 100


class DecisionLog(BaseModel):
    """Registro completo de una decisión (señal + resultado del validador)."""

    signal_id: str
    strategy: str
    side: str
    regime: str
    obi: float | None
    cvd_1m: float | None
    cvd_5m: float | None
    basis_bps: float | None
    oi_change_5m: float | None
    proposed_price: float
    proposed_size: float
    stop_loss: float
    shadow_mode: bool
    vetoed: bool
    veto_reason: str | None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class ApprovedOrder(BaseModel):
    """
    Orden aprobada por el Validador de Riesgo.
    Solo el Validador puede instanciar este objeto.
    El Ejecutor rechaza cualquier orden que no sea ApprovedOrder.
    """

    model_config = {"frozen": True}

    signal: SignalProposal
    approved_size: float           # En BTC, redondeado a szDecimals
    approved_price: float          # Precio límite final
    approved_stop_loss: float
    risk_amount_usd: float
    effective_leverage: float
    approved_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    approver_version: str = "risk_validator_v2"
