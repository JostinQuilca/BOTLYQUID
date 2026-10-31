import httpx, json

u = '0x529A6B3e3b4b3C01a42448f10ed007CF11e27e2F'
fills = httpx.post('https://api.hyperliquid.xyz/info', json={'type': 'userFills', 'user': u}).json()

total_pnl = 0
total_fees = 0
wins = 0
losses = 0

closed = []
for f in fills:
    pnl = float(f.get('closedPnl', 0))
    fee = float(f.get('fee', 0))
    if f.get('dir') in ['Close Long', 'Close Short']:
        total_pnl += pnl
        total_fees += fee
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
        closed.append(f)

total = wins + losses
print(f'Total trades cerrados: {total}')
print(f'Ganadas: {wins} | Perdidas: {losses}')
print(f'Win Rate: {wins/total*100:.1f}%' if total > 0 else 'N/A')
print(f'PnL bruto (sin fees): {total_pnl:.4f} USDC')
print(f'Total fees pagadas: -{total_fees:.4f} USDC')
print(f'PnL neto: {total_pnl - total_fees:.4f} USDC')
print()
print('=== Detalle de operaciones cerradas ===')
for f in closed:
    pnl = float(f.get('closedPnl', 0))
    fee = float(f.get('fee', 0))
    px = float(f['px'])
    dir_ = f['dir']
    neto = pnl - fee
    print(f"  {dir_:12s} @ {px:,.0f} | PnL: {pnl:+.4f} | Fee: -{fee:.4f} | Neto: {neto:+.4f}")
