import json
import asyncio
from aiokafka import AIOKafkaConsumer

KAFKA_SERVERS = "localhost:9092"
TOPIC = "crypto.raw.aggtrade"

def clean_aggtrade(raw_msg: dict):
    data = raw_msg["data"]

    cleaned = {
        "symbol": data["s"].lower(),
        "price": float(data["p"]),
        "qty": float(data["q"]),
        "trade_id": data["a"],
        "first_trade_id": data["f"],
        "last_trade_id": data["l"],
        "event_time": data["E"],
        "trade_time": data["T"],
        "is_buyer_maker": data["m"],
        "source": "binance",
    }

    return cleaned


async def consume_and_clean():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="day5-cleaner",
        auto_offset_reset="earliest"
    )

    await consumer.start()
    print("[Cleaner] Started")

    try:
        count = 0
        async for msg in consumer:
            raw_data = json.loads(msg.value)
            cleaned = clean_aggtrade(raw_data)

            print("[Cleaned Tick]", cleaned)

            count += 1
            if count >= 10:
                break

    finally:
        await consumer.stop()
        print("[Cleaner] Stopped")


if __name__ == "__main__":
    asyncio.run(consume_and_clean())
