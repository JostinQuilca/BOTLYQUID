"""
tests/test_smoke.py
===================
Test de conexión a Hyperliquid (mainnet o testnet según .env).
Verifica credenciales y estado de la cuenta.
"""
from __future__ import annotations

import os
import pytest


@pytest.mark.asyncio
@pytest.mark.skipif(
    not os.environ.get("HL_AGENT_PRIVATE_KEY")
    or "your_agent" in os.environ.get("HL_AGENT_PRIVATE_KEY", ""),
    reason="Requiere credenciales reales en .env (HL_AGENT_PRIVATE_KEY)"
)
async def test_connection():
    """Conecta a Hyperliquid y extrae datos de BTC."""
    from hyperliquid.info import Info
    from config.settings import get_settings

    settings = get_settings()
    print(f"\n🌐 Conectando a: {settings.api_url}")
    print(f"📍 Dirección principal: {settings.hl_main_address}")
    print(f"🔒 Shadow mode: {settings.shadow_mode}")

    info = Info(settings.api_url, skip_ws=True)

    # ── Verificar datos de mercado ─────────────────────────────────────────────
    meta_and_ctxs = info.meta_and_asset_ctxs()
    assert meta_and_ctxs is not None, "meta_and_asset_ctxs() retornó None"

    meta, ctxs = meta_and_ctxs

    btc_idx = None
    for i, asset in enumerate(meta["universe"]):
        if asset["name"] == "BTC":
            btc_idx = i
            break

    assert btc_idx is not None, "BTC no encontrado en el universo de activos"
    btc_ctx = ctxs[btc_idx]
    sz_decimals = meta["universe"][btc_idx]["szDecimals"]

    mark_px = float(btc_ctx["markPx"])
    oracle_px = float(btc_ctx["oraclePx"])
    funding = float(btc_ctx["funding"])
    oi = float(btc_ctx.get("openInterest", 0))

    assert mark_px > 0, f"markPx inválido: {mark_px}"
    assert oracle_px > 0, f"oraclePx inválido: {oracle_px}"
    assert sz_decimals >= 0

    basis_bps = (mark_px - oracle_px) / oracle_px * 10_000
    funding_annualized = funding * 8760 * 100

    print(f"\n✅ BTC en {settings.env.upper()}:")
    print(f"   Mark Price:  ${mark_px:,.2f}")
    print(f"   Oracle:      ${oracle_px:,.2f}")
    print(f"   Basis:       {basis_bps:+.2f} bps")
    print(f"   Funding:     {funding_annualized:+.2f}% anualizado")
    print(f"   OI:          {oi:,.0f} BTC")
    print(f"   szDecimals:  {sz_decimals}")

    # ── Verificar estado de la cuenta ──────────────────────────────────────────
    user_state = info.user_state(settings.hl_main_address)
    assert user_state is not None, "No se pudo obtener estado de la cuenta"

    margin_summary = user_state.get("marginSummary", {})
    account_value = float(margin_summary.get("accountValue", 0))
    print(f"\n💰 Cuenta ({settings.hl_main_address[:10]}...):")
    print(f"   Equity: ${account_value:,.2f} USDC")

    positions = user_state.get("assetPositions", [])
    open_pos = [p for p in positions if float(p.get("position", {}).get("szi", 0)) != 0]
    print(f"   Posiciones abiertas: {len(open_pos)}")

    if open_pos:
        for p in open_pos:
            pos = p["position"]
            print(f"   → {pos['coin']}: {pos['szi']} @ ${float(pos.get('entryPx', 0)):,.2f}")
