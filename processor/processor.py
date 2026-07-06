import json, os, signal, sys
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
import asyncpg
import asyncio

KAFKA_BROKER  = os.getenv("KAFKA_BROKER", "localhost:9092")
DATABASE_URL  = os.getenv("DATABASE_URL")
ALERT_THRESHOLD = 2000.0

conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "order-processor",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

window_counts: dict[str, int]   = {}
window_gmv:    dict[str, float] = {}
category_cnt:  dict[str, int]   = {}
category_gmv:  dict[str, float] = {}

async def get_pool():
    return await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

async def persist_order(pool, order: dict):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO orders (order_id, user_id, product, category, amount, region, status, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (order_id) DO NOTHING
        """, order["order_id"], order["user_id"], order["product"],
            order["category"], float(order["amount"]), order["region"],
            order["status"], datetime.fromisoformat(order["timestamp"]))

async def check_alert(pool, order: dict):
    if float(order["amount"]) > ALERT_THRESHOLD:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO alerts (order_id, alert_type, reason, amount)
                VALUES ($1,$2,$3,$4)
            """, order["order_id"], "HIGH_VALUE",
                f"Amount ${order['amount']} exceeds threshold ${ALERT_THRESHOLD}",
                float(order["amount"]))
        print(f"🚨 ALERT {order['order_id']} — ${order['amount']}")

async def aggregate(pool, order: dict):
    minute_key = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
    cat_key    = f"{order['category']}:{order['region']}"

    window_counts[minute_key] = window_counts.get(minute_key, 0) + 1
    window_gmv[minute_key]    = window_gmv.get(minute_key, 0.0)  + float(order["amount"])
    category_cnt[cat_key]     = category_cnt.get(cat_key, 0)     + 1
    category_gmv[cat_key]     = category_gmv.get(cat_key, 0.0)   + float(order["amount"])

    count = window_counts[minute_key]
    gmv   = window_gmv[minute_key]

    print(f"[{minute_key}] orders={count}  GMV=${gmv:.2f}")

    if count % 10 == 0:
        cat, region = cat_key.split(":", 1)
        now = datetime.now(timezone.utc)
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO order_metrics (window_start, window_end, order_count, total_gmv, avg_order_value)
                VALUES ($1,$2,$3,$4,$5)
            """, now.replace(second=0, microsecond=0), now,
                count, round(gmv, 2), round(gmv / count, 2))

            await conn.execute("""
                INSERT INTO category_stats (category, region, order_count, total_gmv, updated_at)
                VALUES ($1,$2,$3,$4,NOW())
                ON CONFLICT (category, region) DO UPDATE
                SET order_count = EXCLUDED.order_count,
                    total_gmv   = EXCLUDED.total_gmv,
                    updated_at  = NOW()
            """, cat, region,
                category_cnt[cat_key], round(category_gmv[cat_key], 2))

async def process_message(pool, msg_value: bytes):
    order = json.loads(msg_value)
    await asyncio.gather(
        persist_order(pool, order),
        check_alert(pool, order),
        aggregate(pool, order),
    )

async def main():
    pool = await get_pool()
    consumer = Consumer(conf)
    consumer.subscribe(["orders"])

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT,  stop.set)

    print(f"Processor started — broker={KAFKA_BROKER}")
    try:
        while not stop.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka error: {msg.error()}", file=sys.stderr)
                continue
            await process_message(pool, msg.value())
    finally:
        consumer.close()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())