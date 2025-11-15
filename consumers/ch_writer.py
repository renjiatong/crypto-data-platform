import json
import asyncio
import clickhouse_connect
from datetime import datetime
from aiokafka import AIOKafkaConsumer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "crypto.raw.aggtrade"

client = clickhouse_connect.get_client(
    host="localhost",
    port=8123,
    username="admin",
    password="admin",
    database="crypto",
)


def clean_aggtrade(raw_msg):
    data = raw_msg["data"]

    return {
        "symbol": data["s"].lower(),
        "price": float(data["p"]),
        "qty": float(data["q"]),
        "trade_id": data["a"],
        "first_trade_id": data["f"],
        "last_trade_id": data["l"],
        "event_time": datetime.fromtimestamp(data["E"] / 1000),
        "trade_time": datetime.fromtimestamp(data["T"] / 1000),
        "is_buyer_maker": 1 if data["m"] else 0,
        "source": "binance",
    }

    return cleaned


async def consume_and_write():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="day6-clickhouse-writer",
        auto_offset_reset="earliest",
    )

    await consumer.start()
    print("[Writer] Started")

    columns = [
        "symbol",
        "price",
        "qty",
        "trade_id",
        "first_trade_id",
        "last_trade_id",
        "event_time",
        "trade_time",
        "is_buyer_maker",
        "source",
    ]

    try:
        async for msg in consumer:
            raw = json.loads(msg.value)
            row = clean_aggtrade(raw)

            values = [[row[col] for col in columns]]

            client.insert(
                "ticks",
                values,
                column_names=columns,
            )

            print("[Inserted]", row)

    finally:
        await consumer.stop()
        print("[Writer] Stopped")


if __name__ == "__main__":
    asyncio.run(consume_and_write())
