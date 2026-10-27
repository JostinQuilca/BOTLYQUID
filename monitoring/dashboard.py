"""
monitoring/dashboard.py
=======================
Dashboard en Streamlit — Solo lectura de SQLite.

Usa ?mode=ro para conectarse en modo read-only y NO bloquear el bot principal.
Muestra: precio vs. niveles clave, métricas OBI/CVD en vivo, límites de riesgo,
         registro de decisiones del Modo Sombra.

Ejecutar: streamlit run monitoring/dashboard.py
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pandas as pd
import streamlit as st

# ── Configuración de página ────────────────────────────────────────────────────

st.set_page_config(
    page_title="CyberVision HL Bot — Dashboard",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = "data/db/bot.db"

# ── CSS personalizado ──────────────────────────────────────────────────────────

st.markdown("""
<style>
    .stApp { background-color: #0a0e1a; color: #e0e6ff; }
    .metric-card {
        background: linear-gradient(135deg, #1a1f35, #0d1225);
        border: 1px solid #2a3050;
        border-radius: 12px;
        padding: 16px;
        margin: 8px 0;
    }
    .status-running { color: #00ff88; font-weight: bold; }
    .status-halted { color: #ff4466; font-weight: bold; }
    .status-starting { color: #ffaa00; font-weight: bold; }
    h1, h2, h3 { color: #7b9cff; }
    .stDataFrame { background-color: #0d1225; }
</style>
""", unsafe_allow_html=True)


# ── Conexión read-only ─────────────────────────────────────────────────────────

@st.cache_resource
def get_db_connection() -> sqlite3.Connection:
    """
    Conexión SQLite en modo read-only (?mode=ro).
    NO bloquea al bot principal (corrección del plan original).
    """
    db_path = Path(DB_PATH).absolute()
    if not db_path.exists():
        return None
    conn = sqlite3.connect(
        f"file:{db_path}?mode=ro",
        uri=True,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    return conn


def query_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(sql, conn, params=params)
    except Exception:
        return pd.DataFrame()


# ── Layout ─────────────────────────────────────────────────────────────────────

st.title("🤖 CyberVision — Hyperliquid Bot Dashboard")
st.caption(f"Actualizando cada 5 segundos · {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")

# Auto-refresh cada 5 segundos
st.markdown(
    '<meta http-equiv="refresh" content="5">',
    unsafe_allow_html=True,
)

# ── Estado del bot ─────────────────────────────────────────────────────────────

col1, col2, col3, col4 = st.columns(4)

bot_state_df = query_df("SELECT state, updated_at FROM bot_state WHERE id = 1")
if not bot_state_df.empty:
    state = bot_state_df["state"].iloc[0]
    updated_at = bot_state_df["updated_at"].iloc[0]
    age_s = time.time() - updated_at

    css_class = {
        "RUNNING": "status-running",
        "HALTED": "status-halted",
        "STARTING": "status-starting",
    }.get(state, "")

    col1.markdown(
        f'<div class="metric-card">Estado<br>'
        f'<span class="{css_class}">{state}</span></div>',
        unsafe_allow_html=True,
    )
    col2.metric("Heartbeat", f"{age_s:.0f}s atrás", delta=None)
else:
    col1.warning("BD no encontrada — ¿está el bot corriendo?")

# ── Señales externas (Fear & Greed + Funding) ──────────────────────────────────

st.subheader("🌐 Contexto Externo de Mercado")

try:
    import requests as _req
    _fng = _req.get("https://api.alternative.me/fng/?limit=1", timeout=5).json()
    _fng_val = int(_fng["data"][0]["value"])
    _fng_lbl = _fng["data"][0]["value_classification"]

    _bf = _req.get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT", timeout=5).json()
    _funding = float(_bf["lastFundingRate"]) * 100
    _mark = float(_bf["markPrice"])

    # Color del F&G
    if _fng_val < 25:
        _fng_color = "#ff4466"
        _fng_icon  = "😱"
    elif _fng_val < 45:
        _fng_color = "#ff8844"
        _fng_icon  = "😟"
    elif _fng_val < 55:
        _fng_color = "#aabbcc"
        _fng_icon  = "😐"
    elif _fng_val < 75:
        _fng_color = "#44cc88"
        _fng_icon  = "😊"
    else:
        _fng_color = "#ff4444"
        _fng_icon  = "🤑"

    _funding_color = "#ff4466" if _funding > 0.05 else ("#44cc88" if _funding > 0 else "#ffaa00")

    ec1, ec2, ec3 = st.columns(3)
    ec1.markdown(
        f'<div class="metric-card"><b>{_fng_icon} Fear & Greed</b><br>'
        f'<span style="font-size:2em;color:{_fng_color};font-weight:bold">{_fng_val}</span><br>'
        f'<span style="color:{_fng_color}">{_fng_lbl}</span></div>',
        unsafe_allow_html=True,
    )
    ec2.markdown(
        f'<div class="metric-card"><b>💸 Binance Funding /8h</b><br>'
        f'<span style="font-size:1.8em;color:{_funding_color};font-weight:bold">{_funding:+.4f}%</span><br>'
        f'<span style="color:#aaa">≈ {_funding*3*365:.1f}%/año</span></div>',
        unsafe_allow_html=True,
    )
    ec3.markdown(
        f'<div class="metric-card"><b>📍 BTC Mark (Binance)</b><br>'
        f'<span style="font-size:1.8em;color:#7b9cff;font-weight:bold">${_mark:,.0f}</span><br>'
        f'<span style="color:#aaa">Referencia externa</span></div>',
        unsafe_allow_html=True,
    )

    # Alertas visuales
    if _fng_val > 75:
        st.warning("⚠️ **Extreme Greed** — El bot bloqueará entradas LONG hasta que baje.")
    elif _fng_val < 25:
        st.warning("⚠️ **Extreme Fear** — El bot bloqueará entradas SHORT hasta que suba.")
    if _funding > 0.05:
        st.warning("⚠️ **Longs sobreapalancados** — Funding alto, riesgo de cascada bajista.")

except Exception:
    st.info("Señales externas no disponibles (sin conexión o timeout).")

# ── Riesgo diario ──────────────────────────────────────────────────────────────

risk_df = query_df(
    "SELECT * FROM daily_risk ORDER BY date_utc DESC LIMIT 1"
)

st.header("🛡️ Estado de Riesgo del Día")

if not risk_df.empty:
    r = risk_df.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Equity Apertura", f"${r['equity_open']:,.2f}")
    c2.metric("Equity Pico", f"${r['equity_peak']:,.2f}")

    total_pnl = r["realized_pnl"] + r["unrealized_pnl"]
    pnl_pct = total_pnl / r["equity_open"] * 100 if r["equity_open"] > 0 else 0
    c3.metric(
        "PnL Total",
        f"${total_pnl:+,.2f}",
        delta=f"{pnl_pct:+.2f}%",
        delta_color="normal",
    )
    c4.metric(
        "Kill Switch",
        "🔴 ACTIVO" if r["kill_switch"] else "🟢 Inactivo",
    )

# ── Registro de decisiones (Modo Sombra) ──────────────────────────────────────

st.header("📋 Decisiones — Modo Sombra")

decisions_df = query_df("""
    SELECT
        datetime(created_at, 'unixepoch') as timestamp,
        strategy,
        side,
        regime,
        ROUND(obi, 4) as obi,
        ROUND(cvd_1m, 4) as cvd_1m,
        ROUND(proposed_price, 2) as entry_px,
        ROUND(stop_loss, 2) as sl_px,
        CASE WHEN vetoed THEN '❌ ' || veto_reason ELSE '✅ Aprobada' END as resultado
    FROM decisions
    ORDER BY created_at DESC
    LIMIT 50
""")

if not decisions_df.empty:
    st.dataframe(
        decisions_df,
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No hay decisiones registradas aún.")

# ── PnL Virtual del Modo Sombra ────────────────────────────────────────────────

st.header("💹 PnL Virtual (Shadow Mode)")

shadow_pnl_df = query_df("""
    SELECT
        d.strategy,
        d.side,
        sp.horizon_label,
        ROUND(sp.entry_price, 2) as entry,
        ROUND(sp.exit_price, 2) as exit,
        ROUND(sp.pnl_pct * 100, 4) as pnl_pct,
        datetime(sp.resolved_at, 'unixepoch') as resolved_at
    FROM shadow_pnl sp
    JOIN decisions d ON sp.decision_id = d.id
    WHERE sp.exit_price IS NOT NULL
    ORDER BY sp.resolved_at DESC
    LIMIT 100
""")

if not shadow_pnl_df.empty:
    # Estadísticas
    c1, c2, c3 = st.columns(3)
    wins = (shadow_pnl_df["pnl_pct"] > 0).sum()
    total = len(shadow_pnl_df)
    win_rate = wins / total * 100 if total > 0 else 0
    avg_pnl = shadow_pnl_df["pnl_pct"].mean()

    c1.metric("Operaciones", total)
    c2.metric("Win Rate", f"{win_rate:.1f}%")
    c3.metric("PnL Promedio", f"{avg_pnl:+.4f}%")
    st.dataframe(shadow_pnl_df, use_container_width=True, hide_index=True)
else:
    st.info("No hay PnL virtual calculado aún (Modo Sombra activo).")

# ── Velas recientes ────────────────────────────────────────────────────────────

st.header("🕯️ Velas 1m Recientes")

candles_df = query_df("""
    SELECT
        datetime(ts_ms / 1000, 'unixepoch') as tiempo,
        ROUND(open, 2) as open,
        ROUND(high, 2) as high,
        ROUND(low, 2) as low,
        ROUND(close, 2) as close,
        ROUND(volume, 4) as volume
    FROM candles_1m
    ORDER BY ts_ms DESC
    LIMIT 30
""")

if not candles_df.empty:
    st.write(candles_df.to_html(index=False), unsafe_allow_html=True)

# ── Órdenes recientes ──────────────────────────────────────────────────────────

st.header("📝 Órdenes Recientes")

orders_df = query_df("""
    SELECT
        datetime(created_at, 'unixepoch') as timestamp,
        side,
        order_type,
        ROUND(size, 5) as size,
        ROUND(limit_price, 2) as limit_price,
        ROUND(stop_price, 2) as stop_price,
        status,
        ROUND(fill_price, 2) as fill_price,
        ROUND(fee_paid, 4) as fee
    FROM orders
    ORDER BY created_at DESC
    LIMIT 20
""")

if not orders_df.empty:
    st.write(orders_df.to_html(index=False), unsafe_allow_html=True)
else:
    st.info("No hay órdenes registradas (Shadow Mode activo o sin señales).")

st.caption("⚡ Dashboard conectado en modo **read-only** — no interfiere con el bot")
