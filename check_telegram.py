import requests, os
from dotenv import load_dotenv
load_dotenv()

token = os.environ['TELEGRAM_BOT_TOKEN']
r = requests.get(f'https://api.telegram.org/bot{token}/getMe', timeout=10)
data = r.json()

if data.get('ok'):
    bot = data['result']
    username = bot.get('username', '?')
    name = bot.get('first_name', '?')
    print(f'Token VALIDO')
    print(f'Bot: @{username}')
    print(f'Nombre: {name}')
else:
    print(f'ERROR: {data}')
