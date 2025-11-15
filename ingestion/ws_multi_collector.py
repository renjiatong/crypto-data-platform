import aiohttp
import asyncio
import json
from aiokafka import AIOKafkaProducer

EXCHANGE_INFO_URL = "https://testnet.binancefuture.com/fapi/v1/exchangeInfo"
BINANCE_WS_BASE = "wss://fstream.binance.com/stream?streams="

BATCH_SIZE = 50
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "crypto.raw.aggtrade"

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

async def create_kafka_producer():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
    )
    await producer.start()
    print("[Kafka] Producer started")
    return producer

async def subscribe_batch(symbol_batch, batch_id, producer):
    streams = "/".join([f"{s}@aggTrade" for s in symbol_batch])
    ws_url = f"{BINANCE_WS_BASE}{streams}"

    print(f"[Batch {batch_id}] Connecting {ws_url}")

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url) as ws:
            print(f"[Batch {batch_id}] Connected ({len(symbol_batch)} symbols)")
            async for msg in ws:
                data = msg.json()

                stream_name = data.get("stream", "")
                symbol = stream_name.split("@")[0] if "@" in stream_name else "unknown"

                try:
                    await producer.send_and_wait(
                        KAFKA_TOPIC,
                        key=symbol,
                        value=data,
                    )
                except Exception as e:
                    print(f"[Batch {batch_id}] Failed to send to Kafka: {e}")


async def main():
    symbols = await get_all_symbols()
    print(f"Total Testnet symbols: {len(symbols)}")

    producer = await create_kafka_producer()

    tasks = []

    for batch_id, batch in enumerate(chunk_symbols(symbols, BATCH_SIZE)):
        tasks.append(asyncio.create_task(subscribe_batch(batch, batch_id, producer)))

    try:
        await asyncio.gather(*tasks)
    finally:
        await producer.stop()
        print("[Kafka] Producer stopped")

if __name__ == "__main__":
    asyncio.run(main())