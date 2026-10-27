"""Diagnóstico directo del WebSocket de Hyperliquid — muestra los primeros mensajes."""
import asyncio, json, os
from dotenv import load_dotenv
load_dotenv()

import websockets

WS_URL = "wss://api.hyperliquid.xyz/ws"
SYMBOL = "BTC"

async def main():
    print(f"Conectando a {WS_URL}...")
    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        print("Conectado. Enviando suscripciones...")

        subs = [
            {"method": "subscribe", "subscription": {"type": "l2Book", "coin": SYMBOL}},
            {"method": "subscribe", "subscription": {"type": "trades", "coin": SYMBOL}},
            {"method": "subscribe", "subscription": {"type": "activeAssetCtx", "coin": SYMBOL}},
            {"method": "subscribe", "subscription": {"type": "candle", "coin": SYMBOL, "interval": "1m"}},
        ]
        for s in subs:
            await ws.send(json.dumps(s))
            print(f"  Suscrito: {s['subscription']['type']}")

        print("\nEsperando mensajes (30 segundos)...")
        count = 0
        channels_seen = set()
        try:
            async with asyncio.timeout(30):
                async for raw in ws:
                    msg = json.loads(raw)
                    ch = msg.get("channel", msg.get("type", "unknown"))
                    if ch not in channels_seen:
                        channels_seen.add(ch)
                        print(f"\n[NUEVO CANAL] '{ch}'")
                        print(f"  Keys del mensaje: {list(msg.keys())}")
                        data = msg.get("data", {})
                        if isinstance(data, dict):
                            print(f"  Keys de data: {list(data.keys())[:10]}")
                        elif isinstance(data, list):
                            print(f"  data es lista de {len(data)} elementos")
                        print(f"  Raw (primeros 200 chars): {raw[:200]}")
                    count += 1
                    if count % 10 == 0:
                        print(f"  [{count} mensajes recibidos] Canales vistos: {channels_seen}")
        except TimeoutError:
            pass

        print(f"\n{'='*50}")
        print(f"Total mensajes en 30s: {count}")
        print(f"Canales recibidos: {channels_seen}")
        if count == 0:
            print("PROBLEMA: No se recibió ningún mensaje del WebSocket")
        else:
            print("WebSocket OK - datos fluyendo")

asyncio.run(main())
