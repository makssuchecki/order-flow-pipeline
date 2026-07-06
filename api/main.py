# api/main.py
from fastapi import FastAPI, Query
from contextlib import asynccontextmanager
import asyncpg, os

DATABASE_URL = os.getenv("DATABASE_URL")
pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    yield
    await pool.close()

app = FastAPI(title="Order Flow API", lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/metrics/summary")
async def metrics_summary():
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                COUNT(*)                        AS total_orders,
                ROUND(SUM(amount)::numeric, 2)  AS total_gmv,
                ROUND(AVG(amount)::numeric, 2)  AS avg_order_value,
                MAX(created_at)                 AS last_order_at
            FROM orders
            WHERE created_at > NOW() - INTERVAL '1 hour'
        """)
        return dict(row)

@app.get("/metrics/timeseries")
async def metrics_timeseries(minutes: int = Query(30, le=120)):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT window_start, order_count, total_gmv, avg_order_value
            FROM order_metrics
            WHERE window_start > NOW() - ($1 || ' minutes')::interval
            ORDER BY window_start ASC
        """, str(minutes))
        return [dict(r) for r in rows]

@app.get("/alerts/recent")
async def recent_alerts(limit: int = Query(20, le=100)):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT order_id, alert_type, reason, amount, created_at
            FROM alerts
            ORDER BY created_at DESC
            LIMIT $1
        """, limit)
        return [dict(r) for r in rows]

@app.get("/categories/top")
async def top_categories(region: str = None):
    async with pool.acquire() as conn:
        if region:
            rows = await conn.fetch("""
                SELECT category, region, order_count, total_gmv
                FROM category_stats
                WHERE region = $1
                ORDER BY total_gmv DESC LIMIT 10
            """, region)
        else:
            rows = await conn.fetch("""
                SELECT category, SUM(order_count) AS order_count, SUM(total_gmv) AS total_gmv
                FROM category_stats
                GROUP BY category
                ORDER BY total_gmv DESC LIMIT 10
            """)
        return [dict(r) for r in rows]