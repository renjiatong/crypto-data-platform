import json
import asyncio
import websockets

RAW_DATA_FILE = "data/raw/agg_trade_stream.jsonl"
url = "wss://fstream.binance.com/ws/btcusdt@aggTrade"

async def binance_agg_trade_all():
    async with websockets.connect(url) as ws:
        while True:
            message = await ws.recv()
            data = json.loads(message)
            print(data)

if __name__ == "__main__":
    asyncio.run(binance_agg_trade_all())