import asyncpg
from app.config import DATABASE_URL

db_pool = None

async def init_db_pool():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    print("✅ DB pool initialized")

async def fetch_all_data():
    if db_pool is None:
        raise RuntimeError("Database pool not initialized. Did you forget to call init_db_pool()?")

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT ticker, timestamp, open, high, low, close, volume FROM market_snapshot ORDER BY timestamp"
        )
        return [dict(row) for row in rows]
