import asyncio
from aiokafka import AIOKafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "crypto.raw.aggtrade"

async def consume():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="test-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    print("[Consumer] Starting...")
    await consumer.start()

    try:
        count = 0
        async for msg in consumer:
            print(f"[Message] key={msg.key} value={msg.value[:120]}...")

            count += 1
            if count >= 10:
                break
    finally:
        await consumer.stop()
        print("[Consumer] Stopped")

if __name__ == "__main__":
    asyncio.run(consume())
