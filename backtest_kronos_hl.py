"""
backtest_kronos_hl.py
=====================
Walk-Forward Backtest de KronosFoundationStrategy sobre BTC en Hyperliquid Mainnet.
Descarga las últimas 600 velas de 5m y 15m directamente de Hyperliquid.
Simula ejecución realista con:
- Capital inicial: $42.85 USDC (Apalancamiento 2x -> $85.70 USD nocionales)
- Lookback: 256 velas, Pred_len: 6 velas
- Consenso Kronos: >= 80% (bull_ratio o bear_ratio >= 0.80) y retorno proyectado >= +0.32%
- SL: 0.30%
- TP: min(max(proj_ret, 0.32%), 0.55%) (base 0.40%)
- Break-Even Temprano: +0.18% de avance sube SL a +0.10%
- Time-Stop: Cierre en vela 6
- Comisiones Taker: 0.0864% round-trip
"""

import sys
import time
import requests
import numpy as np
import pandas as pd
import torch

from kronos_engine import Kronos, KronosTokenizer, KronosPredictor


INITIAL_CAPITAL = 42.85
LEVERAGE = 2.0
NOTIONAL = INITIAL_CAPITAL * LEVERAGE  # $85.70 USD
TAKER_FEE_PCT = 0.000864               # 0.0864% (2x 0.0432%)
LOOKBACK = 256
PRED_LEN = 6
SAMPLE_COUNT = 5
TEMPERATURE = 0.6
TOP_P = 0.9


def fetch_hyperliquid_candles(interval: str, limit: int = 600) -> list[dict]:
    """Descarga las últimas `limit` velas de BTC desde Hyperliquid Mainnet."""
    url = "https://api.hyperliquid.xyz/info"
    now_ms = int(time.time() * 1000)
    # Intervalo en milisegundos
    int_mins = 5 if interval == "5m" else 15
    span_ms = limit * int_mins * 60 * 1000
    start_ms = now_ms - span_ms

    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": "BTC",
            "interval": interval,
            "startTime": start_ms,
            "endTime": now_ms,
        }
    }
    resp = requests.post(url, json=payload, timeout=20)
    resp.raise_for_status()
    raw_candles = resp.json()
    if not isinstance(raw_candles, list):
        raise ValueError(f"Respuesta inesperada de Hyperliquid: {raw_candles}")

    # Ordenar cronológicamente por timestamp 't'
    raw_candles.sort(key=lambda c: c["t"])
    return raw_candles[-limit:]


def run_walkforward_backtest(
    candles_raw: list[dict],
    interval: str,
    predictor: KronosPredictor,
) -> dict:
    """Ejecuta el walk-forward backtest vela por vela sobre la lista de velas."""
    n_candles = len(candles_raw)
    freq_str = "5min" if interval == "5m" else "15min"
    interval_delta = pd.Timedelta(minutes=5 if interval == "5m" else 15)

    # Convertir a listas indexables
    opens = np.array([float(c["o"]) for c in candles_raw])
    highs = np.array([float(c["h"]) for c in candles_raw])
    lows = np.array([float(c["l"]) for c in candles_raw])
    closes = np.array([float(c["c"]) for c in candles_raw])
    volumes = np.array([float(c["v"]) for c in candles_raw])
    amounts = volumes * closes
    timestamps = pd.Series(pd.to_datetime([c["t"] for c in candles_raw], unit="ms"))

    full_df = pd.DataFrame({
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": volumes,
        "amount": amounts,
    })

    trades = []
    equity = INITIAL_CAPITAL
    equity_curve = [equity]
    peak_equity = equity
    max_drawdown_usd = 0.0
    max_drawdown_pct = 0.0

    print(f"\n=======================================================")
    print(f" Inició Walk-Forward Backtest BTC ({interval})")
    print(f" Total velas: {n_candles} | Lookback: {LOOKBACK} | Pred_len: {PRED_LEN}")
    print(f" Capital: ${INITIAL_CAPITAL:.2f} USDC | Nocional (2x): ${NOTIONAL:.2f} USD")
    print(f"=======================================================")

    idx = LOOKBACK
    start_time_all = time.time()
    evaluated_candles = 0

    while idx < n_candles - PRED_LEN:
        evaluated_candles += 1
        # Ventana de datos históricos sin fuga del futuro
        df_window = full_df.iloc[idx - LOOKBACK: idx].copy().reset_index(drop=True)
        ts_window = timestamps[idx - LOOKBACK: idx].reset_index(drop=True)
        last_ts = ts_window.iloc[-1]
        last_close = float(df_window["close"].iloc[-1])

        # Timestamps futuros para la predicción
        y_timestamps = pd.date_range(last_ts + interval_delta, periods=PRED_LEN, freq=freq_str)

        # Inferencia con Kronos Ensemble
        t_inf_start = time.time()
        try:
            pred_res = predictor.predict_ensemble(
                df=df_window,
                x_timestamp=ts_window,
                y_timestamp=pd.Series(y_timestamps),
                pred_len=PRED_LEN,
                T=TEMPERATURE,
                top_p=TOP_P,
                sample_count=SAMPLE_COUNT,
            )
        except Exception as e:
            print(f"[Error inferencia vela {idx}]: {e}")
            idx += 1
            continue

        bull_ratio = pred_res["bull_ratio"]
        bear_ratio = pred_res["bear_ratio"]
        mean_end_close = float(pred_res["mean_df"]["close"].iloc[-1])
        proj_ret = (mean_end_close - last_close) / last_close

        signal_side = None
        if bull_ratio >= 0.80 and proj_ret >= 0.0032:
            signal_side = "LONG"
        elif bear_ratio >= 0.80 and -proj_ret >= 0.0032:
            signal_side = "SHORT"

        if signal_side is None:
            # Progreso periódico cada 40 velas evaluadas
            if evaluated_candles % 40 == 0 or idx == n_candles - PRED_LEN - 1:
                pct_done = (idx - LOOKBACK) / (n_candles - PRED_LEN - LOOKBACK) * 100
                print(f"  [{interval}] Vela {idx}/{n_candles - PRED_LEN} ({pct_done:.1f}%) | Trades: {len(trades)} | Equity: ${equity:.2f}")
            idx += 1
            continue

        # ── SEÑAL DETECTADA: SIMULACIÓN DE EJECUCIÓN ───────────────────────────
        entry_idx = idx
        entry_ts = timestamps[entry_idx]
        entry_price = last_close

        # Calibración TP/SL según Plan v10
        target_ret = min(max(abs(proj_ret), 0.0032), 0.0055)
        sl_pct = 0.0030   # 0.30%
        tp_pct = target_ret

        if signal_side == "LONG":
            tp_price = entry_price * (1.0 + tp_pct)
            initial_sl = entry_price * (1.0 - sl_pct)
            be_trigger_px = entry_price * (1.0 + 0.0018)  # +0.18%
            be_sl_px = entry_price * (1.0 + 0.0010)       # +0.10%
        else:
            tp_price = entry_price * (1.0 - tp_pct)
            initial_sl = entry_price * (1.0 + sl_pct)
            be_trigger_px = entry_price * (1.0 - 0.0018)  # -0.18%
            be_sl_px = entry_price * (1.0 - 0.0010)       # -0.10%

        current_sl = initial_sl
        moved_to_be = False
        exit_price = None
        exit_reason = None
        exit_idx = None

        # Recorrer hasta 6 velas futuras
        for step in range(PRED_LEN):
            curr_fwd_idx = entry_idx + step
            c_high = highs[curr_fwd_idx]
            c_low = lows[curr_fwd_idx]
            c_close = closes[curr_fwd_idx]

            if signal_side == "LONG":
                # 1. Break-Even check
                if not moved_to_be and c_high >= be_trigger_px:
                    current_sl = be_sl_px
                    moved_to_be = True

                # 2. SL hit check
                if c_low <= current_sl:
                    exit_price = current_sl
                    exit_reason = "BE_SL" if moved_to_be else "SL"
                    exit_idx = curr_fwd_idx
                    break

                # 3. TP hit check
                if c_high >= tp_price:
                    exit_price = tp_price
                    exit_reason = "TP"
                    exit_idx = curr_fwd_idx
                    break

            else:  # SHORT
                # 1. Break-Even check
                if not moved_to_be and c_low <= be_trigger_px:
                    current_sl = be_sl_px
                    moved_to_be = True

                # 2. SL hit check
                if c_high >= current_sl:
                    exit_price = current_sl
                    exit_reason = "BE_SL" if moved_to_be else "SL"
                    exit_idx = curr_fwd_idx
                    break

                # 3. TP hit check
                if c_low <= tp_price:
                    exit_price = tp_price
                    exit_reason = "TP"
                    exit_idx = curr_fwd_idx
                    break

            # 4. Time-Stop al término de la vela 6 (step == PRED_LEN - 1)
            if step == PRED_LEN - 1:
                exit_price = c_close
                exit_reason = "TIME_STOP"
                exit_idx = curr_fwd_idx
                break

        # Calcular resultado neto
        if signal_side == "LONG":
            raw_ret = (exit_price - entry_price) / entry_price
        else:
            raw_ret = (entry_price - exit_price) / entry_price

        net_ret = raw_ret - TAKER_FEE_PCT
        trade_pnl = NOTIONAL * net_ret
        equity += trade_pnl
        equity_curve.append(equity)

        # Actualizar Max Drawdown
        if equity > peak_equity:
            peak_equity = equity
        dd_usd = peak_equity - equity
        dd_pct = (dd_usd / peak_equity) * 100
        if dd_usd > max_drawdown_usd:
            max_drawdown_usd = dd_usd
        if dd_pct > max_drawdown_pct:
            max_drawdown_pct = dd_pct

        trade_record = {
            "entry_time": str(entry_ts),
            "exit_time": str(timestamps[exit_idx]),
            "side": signal_side,
            "entry_px": entry_price,
            "exit_px": exit_price,
            "exit_reason": exit_reason,
            "raw_ret_pct": raw_ret * 100,
            "net_ret_pct": net_ret * 100,
            "pnl_usd": trade_pnl,
            "equity": equity,
            "bull_ratio": bull_ratio,
            "bear_ratio": bear_ratio,
            "proj_ret_pct": proj_ret * 100,
        }
        trades.append(trade_record)

        print(
            f"  ⚡ TRADE {len(trades):02d} | {signal_side:5s} @ ${entry_price:,.1f} -> ${exit_price:,.1f} "
            f"({exit_reason:9s}) | Net: {net_ret*100:+.2f}% (${trade_pnl:+.3f}) | Equity: ${equity:.2f}"
        )

        # Avanzar el índice al cierre del trade para evitar trades superpuestos
        idx = exit_idx + 1

    total_time = time.time() - start_time_all
    print(f" Backtest finalizado en {total_time:.1f}s. Evaluadas {evaluated_candles} velas.")

    # ── MÉTRICAS Y ESTADÍSTICAS ───────────────────────────────────────────────
    total_trades = len(trades)
    if total_trades > 0:
        winning_trades = [t for t in trades if t["pnl_usd"] > 0]
        losing_trades = [t for t in trades if t["pnl_usd"] <= 0]
        win_rate = (len(winning_trades) / total_trades) * 100

        total_gain = sum(t["pnl_usd"] for t in winning_trades)
        total_loss = abs(sum(t["pnl_usd"] for t in losing_trades))
        profit_factor = (total_gain / total_loss) if total_loss > 0 else (99.0 if total_gain > 0 else 0.0)

        net_pnl_usd = equity - INITIAL_CAPITAL
        net_return_pct = (net_pnl_usd / INITIAL_CAPITAL) * 100
        avg_trade_usd = net_pnl_usd / total_trades
    else:
        winning_trades = []
        losing_trades = []
        win_rate = 0.0
        profit_factor = 0.0
        net_pnl_usd = 0.0
        net_return_pct = 0.0
        avg_trade_usd = 0.0

    stats = {
        "interval": interval,
        "total_trades": total_trades,
        "winning_trades": len(winning_trades),
        "losing_trades": len(losing_trades),
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "initial_capital": INITIAL_CAPITAL,
        "final_equity": equity,
        "net_pnl_usd": net_pnl_usd,
        "net_return_pct": net_return_pct,
        "max_drawdown_usd": max_drawdown_usd,
        "max_drawdown_pct": max_drawdown_pct,
        "avg_trade_usd": avg_trade_usd,
        "trades": trades,
    }
    return stats


def print_summary_table(results_5m: dict, results_15m: dict) -> None:
    """Imprime la tabla comparativa final formateada."""
    print("\n" + "=" * 78)
    print(" 📊 RESULTADOS WALK-FORWARD BACKTEST: KRONOS FOUNDATION MODEL (HYPERLIQUID)")
    print("=" * 78)
    header = f"{'Métrica':<35} | {'BTC 5 Minutos':<18} | {'BTC 15 Minutos':<18}"
    print(header)
    print("-" * 78)

    def row(label, val_5m, val_15m):
        print(f"{label:<35} | {val_5m:<18} | {val_15m:<18}")

    row("Total Operaciones Ejecutadas", str(results_5m["total_trades"]), str(results_15m["total_trades"]))
    row("Operaciones Ganadoras (Netas)", f"{results_5m['winning_trades']}", f"{results_15m['winning_trades']}")
    row("Operaciones Perdedoras (Netas)", f"{results_5m['losing_trades']}", f"{results_15m['losing_trades']}")
    row("Win Rate Neto (post-comisiones)", f"{results_5m['win_rate']:.2f}%", f"{results_15m['win_rate']:.2f}%")
    row("Profit Factor Neto", f"{results_5m['profit_factor']:.2f}", f"{results_15m['profit_factor']:.2f}")
    row("Capital Inicial", f"${results_5m['initial_capital']:.2f} USDC", f"${results_15m['initial_capital']:.2f} USDC")
    row("Capital Final (Equity)", f"${results_5m['final_equity']:.2f} USDC", f"${results_15m['final_equity']:.2f} USDC")
    row("Ganancia / Pérdida Neta ($)", f"${results_5m['net_pnl_usd']:+.3f} USDC", f"${results_15m['net_pnl_usd']:+.3f} USDC")
    row("Retorno Total (%)", f"{results_5m['net_return_pct']:+.2f}%", f"{results_15m['net_return_pct']:+.2f}%")
    row("Max Drawdown ($)", f"${results_5m['max_drawdown_usd']:.3f} USDC", f"${results_15m['max_drawdown_usd']:.3f} USDC")
    row("Max Drawdown (%)", f"{results_5m['max_drawdown_pct']:.2f}%", f"{results_15m['max_drawdown_pct']:.2f}%")
    row("PnL Promedio por Trade", f"${results_5m['avg_trade_usd']:+.3f} USDC", f"${results_15m['avg_trade_usd']:+.3f} USDC")
    print("=" * 78)

    # Detalle de razones de salida
    for res, name in [(results_5m, "5m"), (results_15m, "15m")]:
        if res["total_trades"] > 0:
            reasons = {}
            for t in res["trades"]:
                r = t["exit_reason"]
                reasons[r] = reasons.get(r, 0) + 1
            breakdown = ", ".join([f"{k}: {v}" for k, v in sorted(reasons.items())])
            print(f" Desglose de salidas en BTC {name}: {breakdown}")
    print("=" * 78 + "\n")


def main():
    print("==================================================================")
    print(" Iniciando Sistema de Backtest Walk-Forward Kronos Foundation")
    print("==================================================================")

    # 1. Cargar modelo Kronos-mini
    print("Cargando tokenizer y modelo NeoQuasar/Kronos-mini...")
    t0 = time.time()
    tokenizer = KronosTokenizer.from_pretrained("NeoQuasar/Kronos-Tokenizer-2k")
    model = Kronos.from_pretrained("NeoQuasar/Kronos-mini")
    device = "cuda:0" if torch.cuda.is_available() else "cpu"
    predictor = KronosPredictor(model, tokenizer, device=device, max_context=512)
    print(f"Modelo listo en {device.upper()} ({time.time() - t0:.2f}s).")

    # 2. Descargar datos de Hyperliquid
    print("\nDescargando últimas 600 velas de 5m de BTC...")
    candles_5m = fetch_hyperliquid_candles("5m", limit=600)
    print(f"  Recibidas {len(candles_5m)} velas de 5m. Rango: {pd.to_datetime(candles_5m[0]['t'], unit='ms')} -> {pd.to_datetime(candles_5m[-1]['t'], unit='ms')}")

    print("\nDescargando últimas 600 velas de 15m de BTC...")
    candles_15m = fetch_hyperliquid_candles("15m", limit=600)
    print(f"  Recibidas {len(candles_15m)} velas de 15m. Rango: {pd.to_datetime(candles_15m[0]['t'], unit='ms')} -> {pd.to_datetime(candles_15m[-1]['t'], unit='ms')}")

    # 3. Ejecutar Backtest 5m
    res_5m = run_walkforward_backtest(candles_5m, "5m", predictor)

    # 4. Ejecutar Backtest 15m
    res_15m = run_walkforward_backtest(candles_15m, "15m", predictor)

    # 5. Mostrar Tabla Comparativa
    print_summary_table(res_5m, res_15m)


if __name__ == "__main__":
    main()
