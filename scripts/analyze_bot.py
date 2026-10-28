import sqlite3
import time

con = sqlite3.connect('data/db/bot.db')
cur = con.cursor()

print('=== 1. BOT STATE & HEARTBEAT ===')
state = cur.execute('SELECT state, updated_at, metadata FROM bot_state WHERE id=1').fetchone()
if state:
    print(f'State: {state[0]} | Last update: {time.time() - state[1]:.1f}s ago | Meta: {state[2]}')

print('\n=== 2. DECISIONS SUMMARY ===')
total_dec = cur.execute('SELECT count(*) FROM decisions').fetchone()[0]
approved_dec = cur.execute('SELECT count(*) FROM decisions WHERE vetoed=0').fetchone()[0]
vetoed_dec = cur.execute('SELECT count(*) FROM decisions WHERE vetoed=1').fetchone()[0]
print(f'Total decisions: {total_dec} | Approved: {approved_dec} | Vetoed: {vetoed_dec}')

print('\nDecisions by Strategy:')
for row in cur.execute('SELECT strategy, side, count(*), avg(proposed_price) FROM decisions GROUP BY strategy, side').fetchall():
    print(f'  - {row[0]} ({row[1]}): {row[2]} signals (avg px: ${row[3]:,.2f})')

print('\nDecisions by Regime:')
for row in cur.execute('SELECT regime, count(*) FROM decisions GROUP BY regime').fetchall():
    print(f'  - Regime {row[0]}: {row[1]} decisions')

if vetoed_dec > 0:
    print('\nVeto Reasons:')
    for row in cur.execute('SELECT veto_reason, count(*) FROM decisions WHERE vetoed=1 GROUP BY veto_reason').fetchall():
        print(f'  - {row[0]}: {row[1]}')

print('\n=== 3. ALL DECISIONS LOG ===')
for row in cur.execute('SELECT id, datetime(created_at, "unixepoch", "localtime"), strategy, side, proposed_price, proposed_size, vetoed, veto_reason FROM decisions ORDER BY id ASC').fetchall():
    status = "APPROVED" if row[6] == 0 else f"VETOED ({row[7]})"
    print(f'  #{row[0]} [{row[1]}] {row[2]} {row[3]} @ ${row[4]:,.2f} (sz: {row[5]}) -> {status}')

print('\n=== 4. SHADOW PNL PERFORMANCE ===')
pnl_rows = cur.execute('''
    SELECT 
        sp.id, d.strategy, d.side, sp.horizon_label, 
        sp.entry_price, sp.exit_price, sp.pnl_pct,
        datetime(sp.resolved_at, 'unixepoch', 'localtime') as resolved_time,
        sp.decision_id
    FROM shadow_pnl sp
    JOIN decisions d ON sp.decision_id = d.id
    ORDER BY sp.resolved_at ASC
''').fetchall()

print(f'Total resolved horizons: {len(pnl_rows)}')
by_horizon = {}
for r in pnl_rows:
    hz = r[3]
    if hz not in by_horizon:
        by_horizon[hz] = []
    by_horizon[hz].append(r)
    pnl_pct = r[6] * 100
    print(f'  [{r[3]}] Dec #{r[8]} {r[1]} {r[2]}: Entry=${r[4]:,.2f} Exit=${r[5]:,.2f} PnL={pnl_pct:+.4f}% (Resolved: {r[7]})')

print('\nMetrics per horizon:')
all_pnls = []
for hz in ['5m', '15m', '1h']:
    if hz in by_horizon:
        rows = by_horizon[hz]
        pnls = [r[6] for r in rows]
        all_pnls.extend(pnls)
        wins = sum(1 for p in pnls if p > 0)
        total = len(pnls)
        wr = (wins / total) * 100 if total else 0
        avg_p = (sum(pnls) / total) * 100 if total else 0
        cum_p = sum(pnls) * 100
        print(f'  * Horizon {hz:4s}: {total} trades | Win Rate: {wr:5.1f}% ({wins}/{total}) | Avg PnL: {avg_p:+.4f}% | Cum PnL: {cum_p:+.4f}%')

if all_pnls:
    total_all = len(all_pnls)
    wins_all = sum(1 for p in all_pnls if p > 0)
    print(f'\n  * TOTAL GLOBAL: {total_all} resoluciones | Win Rate: {(wins_all/total_all)*100:.1f}% ({wins_all}/{total_all}) | Avg PnL: {(sum(all_pnls)/total_all)*100:+.4f}%')

print('\n=== 5. DAILY RISK ===')
for row in cur.execute('SELECT date_utc, equity_open, equity_peak, realized_pnl, unrealized_pnl, kill_switch, datetime(updated_at, "unixepoch", "localtime") FROM daily_risk ORDER BY date_utc DESC LIMIT 2').fetchall():
    print(f'Date: {row[0]} | Open: ${row[1]:.2f} | Peak: ${row[2]:.2f} | Realized: ${row[3]:.2f} | Unrealized: ${row[4]:.2f} | Kill: {row[5]} | Updated: {row[6]}')

print('\n=== 6. CANDLES RECENT ===')
candles = cur.execute('SELECT count(*), min(ts_ms), max(ts_ms) FROM candles_1m').fetchone()
latest_c = cur.execute('SELECT datetime(ts_ms/1000, "unixepoch", "localtime"), open, high, low, close, volume FROM candles_1m ORDER BY ts_ms DESC LIMIT 1').fetchone()
if latest_c:
    print(f'Candles in DB: {candles[0]} | Latest: {latest_c[0]} | Close: ${latest_c[4]:,.2f} | Vol: {latest_c[5]:.2f}')

con.close()
