"""Verificación final — soporta Unified Account."""
import os, requests
from dotenv import load_dotenv
load_dotenv()

from hyperliquid.info import Info

MAIN_ADDRESS = os.environ['HL_MAIN_ADDRESS']
BASE_URL = 'https://api.hyperliquid.xyz'
info = Info(BASE_URL, skip_ws=True)

# BTC market data
meta, ctxs = info.meta_and_asset_ctxs()
btc_idx = next(i for i, a in enumerate(meta['universe']) if a['name'] == 'BTC')
ctx = ctxs[btc_idx]
mark = float(ctx['markPx'])
funding = float(ctx['funding'])
oi = float(ctx.get('openInterest', 0))

# Perps equity
perp = info.user_state(MAIN_ADDRESS)
perp_equity = float(perp.get('marginSummary', {}).get('accountValue', 0))

# Spot USDC (Unified Account fallback)
spot_resp = requests.post(BASE_URL + '/info',
    json={"type": "spotClearinghouseState", "user": MAIN_ADDRESS}, timeout=10)
spot_balances = spot_resp.json().get('balances', [])
spot_usdc = next((float(b['total']) for b in spot_balances if b.get('coin') == 'USDC'), 0.0)

# Equity efectiva
effective_equity = perp_equity if perp_equity > 0 else spot_usdc
account_type = "Perps" if perp_equity > 0 else "Unified Account (Spot USDC)"

print("=" * 55)
print(f"BTC Mark:  ${mark:,.2f}")
print(f"Funding:   {funding * 8760 * 100:+.2f}% anualizado")
print(f"OI:        {oi:,.0f} BTC")
print()
print(f"Cuenta:    {MAIN_ADDRESS[:10]}...{MAIN_ADDRESS[-4:]}")
print(f"Equity:    ${effective_equity:,.4f} USDC ({account_type})")
print()
if effective_equity > 0:
    risk_usd = effective_equity * 0.005
    print(f"OK — listo para arrancar")
    print(f"Riesgo por trade (0.5%): ${risk_usd:.4f} USDC")
    print(f"Shadow mode: SIN ordenes reales")
    print()
    print("Ejecuta: python main.py")
else:
    print("Sin fondos detectados. Verifica la direccion en .env")
print("=" * 55)
