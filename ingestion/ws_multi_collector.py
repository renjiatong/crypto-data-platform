import aiohttp
import asyncio
import json

EXCHANGE_INFO_URL = "https://testnet.binancefuture.com/fapi/v1/exchangeInfo"
BINANCE_WS_BASE = "wss://fstream.binance.com/stream?streams="

BATCH_SIZE = 50

async def get_all_symbols():
    async with aiohttp.ClientSession() as session:
        async with session.get(EXCHANGE_INFO_URL) as resp:
            data = await resp.json()

    symbols = [
        s["symbol"].lower()
        for s in data["symbols"]
        if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING"
    ]

    print(f"Testnet symbols: {symbols}")
    return symbols

def chunk_symbols(symbols, size=BATCH_SIZE):
    for i in range(0, len(symbols), size):
        yield symbols[i:i + size]

async def subscribe_batch(symbol_batch, batch_id):
    streams = "/".join([f"{s}@aggTrade" for s in symbol_batch])
    ws_url = f"{BINANCE_WS_BASE}{streams}"

    print(f"[Batch {batch_id}] Connecting {ws_url}")

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url) as ws:
            print(f"[Batch {batch_id}] Connected ({len(symbol_batch)} symbols)")
            async for msg in ws:
                data = msg.json()
                print(f"[Batch {batch_id}] {data}")
async def main():
    symbols = await get_all_symbols()
    print(f"Total Testnet symbols: {len(symbols)}")

    tasks = []

    for batch_id, batch in enumerate(chunk_symbols(symbols, BATCH_SIZE)):
        tasks.append(asyncio.create_task(subscribe_batch(batch, batch_id)))

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())