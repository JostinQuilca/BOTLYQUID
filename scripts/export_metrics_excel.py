"""
scripts/export_metrics_excel.py
===============================
Exporta la base de datos de auditoría completa del bot a un archivo Excel (.xlsx)
con múltiples hojas estructuradas y métricas financieras de rendimiento.
"""
import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path

DB_PATH = Path("data/db/bot.db")
OUTPUT_PATH = Path("reporte_rendimiento_bot.xlsx")

def export():
    print(f"Leyendo base de datos desde {DB_PATH}...")
    con = sqlite3.connect(DB_PATH)

    # 1. Decisiones
    decisions_df = pd.read_sql_query("""
        SELECT 
            id as decision_id,
            datetime(created_at, 'unixepoch', 'localtime') as fecha_hora,
            created_at as timestamp_raw,
            strategy as estrategia,
            side as lado,
            regime as regimen,
            ROUND(proposed_price, 2) as precio_entrada,
            ROUND(proposed_size, 5) as tamano_btc,
            ROUND(stop_loss, 2) as stop_loss,
            CASE WHEN vetoed = 0 THEN 'APROBADA' ELSE 'VETOADA' END as estado,
            veto_reason as razon_veto,
            ROUND(obi, 4) as orderbook_imbalance,
            ROUND(cvd_1m, 2) as cvd_1m,
            ROUND(cvd_5m, 2) as cvd_5m,
            ROUND(basis_bps, 2) as basis_bps,
            ROUND(oi_change_5m, 2) as oi_change_5m
        FROM decisions
        ORDER BY id ASC
    """, con)

    # 2. Shadow PnL
    shadow_pnl_df = pd.read_sql_query("""
        SELECT 
            sp.id as pnl_id,
            sp.decision_id,
            datetime(d.created_at, 'unixepoch', 'localtime') as fecha_senal,
            datetime(sp.resolved_at, 'unixepoch', 'localtime') as fecha_resolucion,
            d.created_at as created_at_raw,
            d.strategy as estrategia,
            d.side as lado,
            sp.horizon_label as horizonte,
            ROUND(sp.entry_price, 2) as precio_entrada,
            ROUND(sp.exit_price, 2) as precio_salida,
            ROUND(sp.pnl_pct * 100, 4) as pnl_pct,
            CASE WHEN sp.pnl_pct > 0 THEN 'WIN' WHEN sp.pnl_pct < 0 THEN 'LOSS' ELSE 'BREAKEVEN' END as resultado,
            -- Proyección de ganancia con $50 y $1000
            ROUND(sp.pnl_pct * 50 * 2, 4) as ganancia_est_cuenta_50usd,
            ROUND(sp.pnl_pct * 1000 * 2, 2) as ganancia_est_cuenta_1000usd
        FROM shadow_pnl sp
        JOIN decisions d ON sp.decision_id = d.id
        WHERE sp.exit_price IS NOT NULL
        ORDER BY sp.id ASC
    """, con)

    # 3. Resumen de Métricas
    summary_data = []

    # Métricas Globales
    total_trades = len(shadow_pnl_df)
    wins = (shadow_pnl_df['pnl_pct'] > 0).sum()
    losses = (shadow_pnl_df['pnl_pct'] < 0).sum()
    be = (shadow_pnl_df['pnl_pct'] == 0).sum()
    win_rate = (wins / total_trades * 100) if total_trades else 0
    avg_pnl = shadow_pnl_df['pnl_pct'].mean() if total_trades else 0
    cum_pnl = shadow_pnl_df['pnl_pct'].sum() if total_trades else 0

    summary_data.append({"Categoría": "GLOBAL", "Subcategoría": "Todas las Operaciones", "Total Trades": total_trades, "Wins": wins, "Losses": losses, "Win Rate %": round(win_rate, 2), "PnL Promedio %": round(avg_pnl, 4), "PnL Acumulado %": round(cum_pnl, 4)})

    # Métricas por Horizonte
    for hz in ['5m', '15m', '1h']:
        sub = shadow_pnl_df[shadow_pnl_df['horizonte'] == hz]
        t = len(sub)
        w = (sub['pnl_pct'] > 0).sum()
        l = (sub['pnl_pct'] < 0).sum()
        wr = (w / t * 100) if t else 0
        ap = sub['pnl_pct'].mean() if t else 0
        cp = sub['pnl_pct'].sum() if t else 0
        summary_data.append({"Categoría": "HORIZONTE", "Subcategoría": f"Horizonte {hz}", "Total Trades": t, "Wins": w, "Losses": l, "Win Rate %": round(wr, 2), "PnL Promedio %": round(ap, 4), "PnL Acumulado %": round(cp, 4)})

    # Métricas por Estrategia
    for strat in shadow_pnl_df['estrategia'].unique():
        sub = shadow_pnl_df[shadow_pnl_df['estrategia'] == strat]
        t = len(sub)
        w = (sub['pnl_pct'] > 0).sum()
        l = (sub['pnl_pct'] < 0).sum()
        wr = (w / t * 100) if t else 0
        ap = sub['pnl_pct'].mean() if t else 0
        cp = sub['pnl_pct'].sum() if t else 0
        summary_data.append({"Categoría": "ESTRATEGIA", "Subcategoría": strat, "Total Trades": t, "Wins": w, "Losses": l, "Win Rate %": round(wr, 2), "PnL Promedio %": round(ap, 4), "PnL Acumulado %": round(cp, 4)})

    # 4. Periodo FOMC (Hoy entre las 12:30 y las 15:30 hora local)
    # Convertir a datetime local para filtrar
    shadow_pnl_df['dt_senal'] = pd.to_datetime(shadow_pnl_df['fecha_senal'])
    fomc_sub = shadow_pnl_df[(shadow_pnl_df['dt_senal'].dt.date == pd.to_datetime('2026-10-28').date()) & 
                             (shadow_pnl_df['dt_senal'].dt.hour >= 12) & 
                             (shadow_pnl_df['dt_senal'].dt.hour <= 15)]

    if not fomc_sub.empty:
        t = len(fomc_sub)
        w = (fomc_sub['pnl_pct'] > 0).sum()
        l = (fomc_sub['pnl_pct'] < 0).sum()
        wr = (w / t * 100) if t else 0
        ap = fomc_sub['pnl_pct'].mean() if t else 0
        cp = fomc_sub['pnl_pct'].sum() if t else 0
        summary_data.append({"Categoría": "EVENTO FOMC", "Subcategoría": "Ventana FOMC (12:30 - 15:30)", "Total Trades": t, "Wins": w, "Losses": l, "Win Rate %": round(wr, 2), "PnL Promedio %": round(ap, 4), "PnL Acumulado %": round(cp, 4)})

    summary_df = pd.DataFrame(summary_data)

    # 5. Guardar en Excel con formato profesional
    print(f"Generando Excel en {OUTPUT_PATH}...")
    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Resumen_Metricas", index=False)
        shadow_pnl_df.drop(columns=['created_at_raw', 'dt_senal'], errors='ignore').to_excel(writer, sheet_name="Shadow_PnL", index=False)
        decisions_df.drop(columns=['timestamp_raw'], errors='ignore').to_excel(writer, sheet_name="Decisiones_Todas", index=False)
        if not fomc_sub.empty:
            fomc_sub.drop(columns=['created_at_raw', 'dt_senal'], errors='ignore').to_excel(writer, sheet_name="Operaciones_FOMC", index=False)

    con.close()
    print("Excel generado exitosamente.")
    return summary_df

if __name__ == "__main__":
    df = export()
    print(df.to_string())
