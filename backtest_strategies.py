"""
backtest_strategies.py
======================
Backtester honesto y rápido para comparar estrategias SIMPLES de BTC contra
baselines (comprar-y-aguantar y entradas al azar), con comisiones + slippage.

NO usa ML (corre en segundos): sirve para ver si ALGÚN enfoque simple tiene un
edge real ANTES de invertir en algo más complejo. Si ninguna estrategia supera
de forma consistente a "comprar y aguantar" y a las entradas al azar, es señal de
que no hay edge fácil — que es lo normal.

Modelo: posición por barra (+1 long, -1 short, 0 flat), aplicada de cierre a
cierre. La posición de la barra i se decide con datos hasta el cierre de i-1
(sin look-ahead). Cada cambio de posición paga comisión + slippage (turnover).
"""
import os
import sys
import time

import numpy as np
import pandas as pd
import requests

os.environ.setdefault("PYTHONIOENCODING", "utf-8")
try:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

FEE = 0.000864      # comisión round-trip (taker) en Hyperliquid
SLIP = 0.0003       # slippage por lado
URL = "https://api.hyperliquid.xyz/info"
INT_MIN = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 240}


def fetch_candles(interval: str, days: int) -> pd.DataFrame:
    """Descarga velas de BTC paginando hacia atrás hasta cubrir `days` días."""
    im = INT_MIN[interval]
    now = int(time.time() * 1000)
    start_target = now - days * 86_400_000
    chunk_ms = 4000 * im * 60 * 1000
    got: dict[int, dict] = {}
    end = now
    while end > start_target:
        start = max(start_target, end - chunk_ms)
        r = requests.post(
            URL,
            json={"type": "candleSnapshot",
                  "req": {"coin": "BTC", "interval": interval,
                          "startTime": start, "endTime": end}},
            timeout=30,
        )
        r.raise_for_status()
        batch = r.json()
        if not isinstance(batch, list) or not batch:
            break
        for c in batch:
            got[int(c["t"])] = c
        oldest = min(int(c["t"]) for c in batch)
        if oldest >= end:
            break
        end = oldest - 1
        time.sleep(0.25)
    rows = sorted(got.values(), key=lambda c: int(c["t"]))
    return pd.DataFrame({
        "ts": [int(c["t"]) for c in rows],
        "open": [float(c["o"]) for c in rows],
        "high": [float(c["h"]) for c in rows],
        "low": [float(c["l"]) for c in rows],
        "close": [float(c["c"]) for c in rows],
        "volume": [float(c["v"]) for c in rows],
    })


def backtest(df: pd.DataFrame, position: np.ndarray) -> dict:
    """position[i] = posición decidida al cierre de la barra i (se aplica en i+1)."""
    close = df["close"].to_numpy()
    n = len(close)
    equity = 1.0
    prev_pos = 0.0
    peak = 1.0
    maxdd = 0.0
    changes = 0
    wins = 0
    losses = 0
    seg_start_eq = 1.0
    for i in range(1, n):
        pos = float(position[i - 1])          # sin look-ahead
        if pos != prev_pos:
            if prev_pos != 0.0:               # cerrar el tramo anterior
                (wins := wins + 1) if equity > seg_start_eq else (losses := losses + 1)
            equity *= (1.0 - (FEE + SLIP) * abs(pos - prev_pos))
            changes += 1
            seg_start_eq = equity
        equity *= (1.0 + pos * (close[i] - close[i - 1]) / close[i - 1])
        prev_pos = pos
        peak = max(peak, equity)
        maxdd = max(maxdd, (peak - equity) / peak)
    if prev_pos != 0.0:                        # cerrar el último tramo abierto
        wins, losses = (wins + 1, losses) if equity > seg_start_eq else (wins, losses + 1)
    tr = wins + losses
    return {
        "ret_pct": (equity - 1.0) * 100,
        "trades": tr,
        "winrate": (100.0 * wins / tr) if tr else 0.0,
        "maxdd_pct": maxdd * 100,
    }


# ── Estrategias: array de posición (+1 long / -1 short / 0 flat) por barra ─────
def s_buyhold(df):
    return np.ones(len(df))


def s_random(df, seed=42, flip_prob=0.04):
    rng = np.random.default_rng(seed)
    pos, cur = np.zeros(len(df)), 0
    for i in range(len(df)):
        if rng.random() < flip_prob:
            cur = int(rng.choice([-1, 0, 1]))
        pos[i] = cur
    return pos


def s_ema_ls(df, fast=20, slow=50):
    ef = df["close"].ewm(span=fast, adjust=False).mean()
    es = df["close"].ewm(span=slow, adjust=False).mean()
    return np.where(ef > es, 1.0, -1.0)


def s_ema_long(df, fast=20, slow=50):
    ef = df["close"].ewm(span=fast, adjust=False).mean()
    es = df["close"].ewm(span=slow, adjust=False).mean()
    return np.where(ef > es, 1.0, 0.0)


def _rsi(close, period=14):
    d = close.diff()
    g = d.clip(lower=0).ewm(alpha=1 / period, adjust=False).mean()
    loss = (-d).clip(lower=0).ewm(alpha=1 / period, adjust=False).mean()
    rs = g / loss.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(50)


def s_rsi_meanrev(df, lo=35, hi=55):
    rsi = _rsi(df["close"]).to_numpy()
    pos, cur = np.zeros(len(df)), 0
    for i in range(len(df)):
        if cur == 0 and rsi[i] < lo:
            cur = 1
        elif cur == 1 and rsi[i] > hi:
            cur = 0
        pos[i] = cur
    return pos


def s_donchian(df, entry=20, exit_=10):
    high, low, close = df["high"].to_numpy(), df["low"].to_numpy(), df["close"].to_numpy()
    pos, cur = np.zeros(len(df)), 0
    for i in range(len(df)):
        if i < entry:
            continue
        if cur == 0 and close[i] >= high[i - entry:i].max():
            cur = 1
        elif cur == 1 and close[i] <= low[i - exit_:i].min():
            cur = 0
        pos[i] = cur
    return pos


STRATS = {
    "Buy&Hold":        s_buyhold,
    "Random (azar)":   s_random,
    "EMA trend L/S":   s_ema_ls,
    "EMA trend long":  s_ema_long,
    "RSI mean-rev":    s_rsi_meanrev,
    "Donchian break":  s_donchian,
}


def main():
    for interval, days in [("1h", 180), ("15m", 60)]:
        print(f"\n{'=' * 72}")
        print(f" BTC {interval} — últimos {days} días (comisiones {FEE*100:.4f}% + slippage {SLIP*100:.2f}%/lado)")
        print(f"{'=' * 72}")
        try:
            df = fetch_candles(interval, days)
        except Exception as e:
            print(f" Error descargando datos: {e}")
            continue
        if len(df) < 120:
            print(f" Datos insuficientes ({len(df)} velas).")
            continue
        span = (df["ts"].iloc[-1] - df["ts"].iloc[0]) / 86_400_000
        bh = (df["close"].iloc[-1] / df["close"].iloc[0] - 1) * 100
        print(f" Velas: {len(df)} | periodo real: {span:.0f} días | BTC comprar-y-aguantar: {bh:+.2f}%\n")
        print(f" {'Estrategia':16} | {'Retorno':>9} | {'Trades':>6} | {'Winrate':>7} | {'MaxDD':>7}")
        print(f" {'-'*16}-+-{'-'*9}-+-{'-'*6}-+-{'-'*7}-+-{'-'*7}")
        for name, fn in STRATS.items():
            m = backtest(df, fn(df))
            print(f" {name:16} | {m['ret_pct']:+8.2f}% | {m['trades']:6d} | {m['winrate']:6.1f}% | {m['maxdd_pct']:6.1f}%")


if __name__ == "__main__":
    main()
