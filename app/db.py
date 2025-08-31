import asyncpg
from typing import Optional
from app.config import DATABASE_URL


class DatabaseManager:
    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        """Initialize database connection pool"""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=1,
                max_size=10
            )
            print("✅ DB pool initialized")
    
    async def disconnect(self):
        """Close database connection pool"""
        if self._pool:
            await self._pool.close()
            self._pool = None
            print("✅ DB pool closed")
    
    @property
    def pool(self) -> asyncpg.Pool:
        """Get the connection pool, raising error if not connected"""
        if self._pool is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._pool
    
    async def fetch_all_data(self):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT ticker, timeframe, timestamp, open, high, low, close FROM market_snapshot ORDER BY timestamp"
            )
            return [dict(row) for row in rows]


# Global instance
db = DatabaseManager()
