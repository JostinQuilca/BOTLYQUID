import requests, datetime

# Fear & Greed Index
fng = requests.get('https://api.alternative.me/fng/?limit=3', timeout=10).json()
print('=== FEAR & GREED INDEX ===')
for d in fng['data']:
    ts = datetime.datetime.fromtimestamp(int(d['timestamp'])).strftime('%Y-%m-%d')
    val = d['value']
    cls = d['value_classification']
    print(f'  {ts}: {val:>3} — {cls}')

# Binance funding rate
br = requests.get('https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT', timeout=10).json()
print()
print('=== BINANCE BTCUSDT FUNDING ===')
funding_pct = float(br['lastFundingRate']) * 100
mark = float(br['markPrice'])
index = float(br['indexPrice'])
print(f'  Funding rate:  {funding_pct:.4f}% per 8h')
print(f'  Annual equiv:  {funding_pct * 3 * 365:.1f}%/yr')
print(f'  Mark price:    ${mark:,.2f}')
print(f'  Index price:   ${index:,.2f}')
print(f'  Premium:       {(mark/index - 1)*100:.3f}%')
