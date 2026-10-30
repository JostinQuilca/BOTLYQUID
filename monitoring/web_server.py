"""
monitoring/web_server.py
========================
Servidor web HTTP multihilo ultraligero (estándar Python).
Sirve el Dashboard en tiempo real en el puerto 8080 para Fly.io.
"""
from __future__ import annotations

import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from data.state import MarketState

logger = logging.getLogger(__name__)


def render_dashboard(state: MarketState, executor: Any = None) -> str:
    now_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    
    # Precios y métricas
    mark_px = state.asset_ctx.mark_price if state.asset_ctx else 0.0
    funding = (state.asset_ctx.funding_rate * 100) if state.asset_ctx else 0.0
    oi = state.asset_ctx.open_interest if state.asset_ctx else 0.0
    equity = state.account.equity if state.account else 0.0
    
    # Posición
    has_pos = state.account.has_position if state.account else False
    pos_sz = state.account.position_size if state.account else 0.0
    pos_entry = state.account.position_entry_price if state.account else 0.0
    pos_pnl = state.account.unrealized_pnl if state.account else 0.0
    
    pnl_color = "#00ff88" if pos_pnl >= 0 else "#ff4466"
    pnl_sign = "+" if pos_pnl >= 0 else ""
    
    # Estado de protección TP/SL
    tp_oid = getattr(executor, "_active_tp_oid", None) if executor else None
    sl_oid = getattr(executor, "_active_sl_oid", None) if executor else None
    tp_status = "Activo en Hyperliquid" if tp_oid else ("Configurado" if (state.account and state.account.active_tp_px > 0) else "Esperando señal")
    sl_status = "Activo en Hyperliquid" if sl_oid else ("Configurado" if (state.account and state.account.active_sl_px > 0) else "Esperando señal")
    
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="5">
    <title>CyberVision Bot — Live Dashboard</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            background-color: #0a0e1a;
            color: #e0e6ff;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
            padding: 24px;
        }}
        .header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid #1a223f;
            padding-bottom: 16px;
            margin-bottom: 24px;
        }}
        .title {{
            font-size: 24px;
            font-weight: bold;
            color: #7b9cff;
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        .badge-live {{
            background: #00ff8822;
            color: #00ff88;
            border: 1px solid #00ff8866;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 13px;
            font-weight: bold;
        }}
        .time {{ color: #6b7a9f; font-size: 13px; }}
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }}
        .card {{
            background: linear-gradient(135deg, #13192e, #0e1324);
            border: 1px solid #1f2747;
            border-radius: 12px;
            padding: 18px;
        }}
        .card-label {{ color: #7f8fae; font-size: 13px; margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.5px; }}
        .card-value {{ font-size: 24px; font-weight: bold; color: #ffffff; }}
        .card-sub {{ color: #6b7a9f; font-size: 12px; margin-top: 4px; }}
        
        .pos-card {{
            background: linear-gradient(135deg, #151d38, #10162a);
            border: 1px solid {'#00ff8855' if has_pos else '#2a3558'};
            border-radius: 14px;
            padding: 20px;
            margin-bottom: 24px;
        }}
        .pos-header {{ font-size: 16px; font-weight: bold; color: #7b9cff; margin-bottom: 14px; display: flex; justify-content: space-between; }}
        .pos-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 14px; }}
        
        .footer {{
            text-align: center;
            color: #4b587a;
            font-size: 12px;
            margin-top: 30px;
            border-top: 1px solid #161c33;
            padding-top: 16px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <div class="title">
            <span>🤖 CyberVision HL Bot</span>
            <span class="badge-live">● EN VIVO</span>
        </div>
        <div class="time">Actualizado: {now_str} (Auto-refresh 5s)</div>
    </div>

    <!-- Métricas principales -->
    <div class="grid">
        <div class="card">
            <div class="card-label">Precio BTC (Mark)</div>
            <div class="card-value">${mark_px:,.1f}</div>
            <div class="card-sub">Hyperliquid Perp</div>
        </div>
        <div class="card">
            <div class="card-label">Balance Total</div>
            <div class="card-value">${equity:,.2f} USDC</div>
            <div class="card-sub">Capital de cuenta</div>
        </div>
        <div class="card">
            <div class="card-label">Funding Rate</div>
            <div class="card-value" style="color: {'#ff4466' if funding > 0.04 else ('#00ff88' if funding < 0 else '#e0e6ff')};">{funding:+.4f}%/h</div>
            <div class="card-sub">≈ {funding * 24 * 365:.1f}% anual</div>
        </div>
        <div class="card">
            <div class="card-label">Open Interest</div>
            <div class="card-value">{oi:,.1f} BTC</div>
            <div class="card-sub">Contratos activos</div>
        </div>
    </div>

    <!-- Posición Activa -->
    <div class="pos-card">
        <div class="pos-header">
            <span>📊 Posición Actual en Hyperliquid</span>
            <span style="color: {'#00ff88' if has_pos else '#8899bb'}; font-weight: bold;">
                {'🟢 ABIERTA (LONG)' if (has_pos and pos_sz > 0) else ('🔴 ABIERTA (SHORT)' if (has_pos and pos_sz < 0) else '⚪ FLAT (Sin posición)')}
            </span>
        </div>
        <div class="pos-grid">
            <div>
                <div class="card-label">Tamaño</div>
                <div class="card-value" style="font-size: 20px;">{pos_sz:+.5f} BTC</div>
            </div>
            <div>
                <div class="card-label">Precio Entrada</div>
                <div class="card-value" style="font-size: 20px;">{('$' + f'{pos_entry:,.1f}') if pos_entry > 0 else '—'}</div>
            </div>
            <div>
                <div class="card-label">PnL No Realizado</div>
                <div class="card-value" style="font-size: 20px; color: {pnl_color};">{pnl_sign}${pos_pnl:.4f} USDC</div>
            </div>
            <div>
                <div class="card-label">Protección TP / SL</div>
                <div class="card-value" style="font-size: 16px; color: #7b9cff;">TP: {tp_status} | SL: {sl_status}</div>
            </div>
        </div>
    </div>

    <!-- Estrategias Activas -->
    <div class="grid">
        <div class="card">
            <div class="card-label">Estrategias Activas v3</div>
            <div style="font-size: 14px; line-height: 1.8; color: #a5b4d8;">
                ✓ TrendMomentum (Trend + RSI + CVD)<br>
                ✓ AbsorptionMeanReversion (VWAP + OBI)<br>
                ✓ VolatilityBreakout (Spike + OI Expansion)<br>
                ✓ StopHuntReversal (Sweep Reversal R:R 2:1)
            </div>
        </div>
        <div class="card">
            <div class="card-label">Gestión de Riesgo</div>
            <div style="font-size: 14px; line-height: 1.8; color: #a5b4d8;">
                ✓ Órdenes: ALO (Maker Rebate 0.015%)<br>
                ✓ Stop Loss: Mínimo 0.8% (Anti-ruido)<br>
                ✓ Cooldown: 15 min tras stop loss<br>
                ✓ Auto-protección: Reconciliador 30s
            </div>
        </div>
    </div>

    <div class="footer">
        CyberVision HL Bot v3.0 · Conectado vía EIP-712 Agent Wallet · Hyperliquid L1
    </div>
</body>
</html>
"""


def start_web_server(port: int, state: MarketState, executor: Any = None) -> Any:
    """Inicia el servidor HTTP estándar dual-stack (IPv4 + IPv6) en un hilo demonio."""
    import socket

    class DualStackServer(ThreadingHTTPServer):
        address_family = socket.AF_INET6

        def server_bind(self) -> None:
            try:
                self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            except (AttributeError, OSError) as e:
                logger.debug("Could not set IPV6_V6ONLY: %s", e)
            super().server_bind()

    class DashboardHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            try:
                if self.path == "/health":
                    body = b"OK"
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain")
                    self.send_header("Content-Length", str(len(body)))
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.wfile.write(body)
                    self.wfile.flush()
                    return

                html = render_dashboard(state, executor)
                body = html.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)
                self.wfile.flush()
            except Exception as e:
                logger.exception("Error serving dashboard GET %s: %s", self.path, e)
                try:
                    err_body = f"<h1>Error 500</h1><pre>{e}</pre>".encode("utf-8")
                    self.send_response(500)
                    self.send_header("Content-Type", "text/html")
                    self.send_header("Content-Length", str(len(err_body)))
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.wfile.write(err_body)
                    self.wfile.flush()
                except Exception:
                    pass

        def log_message(self, format: str, *args: Any) -> None:
            # Silenciar logs para no saturar consola
            pass

    try:
        server = DualStackServer(("::", port), DashboardHandler)
        listen_addr = f"[::]:{port}"
    except Exception as e:
        logger.warning("DualStackServer (::) failed (%s), falling back to 0.0.0.0:%d", e, port)
        server = ThreadingHTTPServer(("0.0.0.0", port), DashboardHandler)
        listen_addr = f"0.0.0.0:{port}"

    thread = threading.Thread(target=server.serve_forever, daemon=True, name="web_dashboard")
    thread.start()
    logger.info("Web dashboard HTTP server listening on %s (thread daemon)", listen_addr)
    return server
