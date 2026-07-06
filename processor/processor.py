import faust, json, os
from datetime import datetime, timezone
import asyncpg

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka://localhost:9092")
DATABASE_URL = os.getenv("DATABASE_URL")
ALERT_THRESHOLD = 2000.0

app = faust.App(
    "order-processor",
    broker=f"kafka://{KAFKA_BROKER.replace('kafka://', '')}",
    value_serializer="json",
)

orders_topic = app.topic("orders")

window_counts = app.Table("window_counts", default=int)
window_gmv = app.Table("window_gmv", default=float)
category_gmv = app.Table("category_gmv", default=float)
category_cnt = app.Table("category_cnt", default=int)

async def get_db():
    return await asyncpg.connect(DATABASE_URL)

@app.agent(orders_topic)
async def persist_orders(orders):
    async for order in orders:
        conn = await get_db()
        try:
            await conn.execture("""
                INSERT INTO orders (order_id, user_id, product, category, amount, region, status, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (order_id) DO NOTHING                      
            """, order["order_id"], order["user_id"], order["product"],
            order["category"], order["amount"], order["region"],
            order["status"], datetime.fromisoformat(order["timestamp"])
            )
        finally:
            await conn.close()

@app.agent(orders_topic)
async def detect_anomalies(orders):
    async for order in orders:
        if order["amount"] > ALERT_THRESHOLD:
            conn = await get_db()
            try:
                await conn.execute("""
                    INSERT INTO alerts (order_id, alert_type, reason, amount)
                    VALUES ($1, $2, $3, $4)
                """, order["order_id"], "HIGH_VALUE",
                    f"Order amount ${order['amount']:.2f} exceeds threshold ${ALERT_THRESHOLD:.2f}",
                    order["amount"])
                print(f"ALERT: {order['order_id']} - ${order['amount']:.2f}")
            finally:
                await conn.close()
        
@app.agent(orders_topic)
async def aggregate_metrics(orders):
    async for order in orders:
        minute_key = datetime.utcnow().strftime("%Y-%m-%dT%H:%M")
        cat_key = f"{order['category']:{order['region']}}"

        window_counts[minute_key] += 1
        window_gmv[minute_key] += order["amount"]
        category_cnt[cat_key] += 1
        category_gmv[cat_key] += order["amount"]

        count = window_counts[minute_key]
        gmv = window_gmv[minute_key]

        if count % 10 == 0:
            conn = await get_db()
            try:
                now = datetime.now(timezone.utc)
                await conn.execute("""
                    INSERT INTO order_metrics (window_start, window_end, order_count, total_gmv, avg_order_value)                   
                    VALUES ($1, $2, $3, $4, $5)
                    """, now.replace(second=0, microsecond=0), now,
                    count, round(gmv, 2), round(gmv / count, 2))
                
                cat, region = cat_key.split(":")
                await conn.execute("""
                    INSERT INTO category_stats (category, region, order_count, total_gmv, updated_at)                
                    VALUES ($1, $2, $3, $4, NOW())
                    ON CONFLICT (category, region)
                    DO UPDATE SET
                            order_count = EXCLUDED.order_count,
                            total_gmv = EXCLUDED.total_gmv,
                            updated_at = NOW()
                    """, cat, region,
                        category_cnt[cat_key], round(category_gmv[cat_key], 2))
            finally:
                await conn.close()
        print(f"[{minute_key}] orders={count} GMV=${gmv:.2f}")
        