import asyncpg
from loguru import logger
from typing import Optional
from datetime import datetime
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
            logger.info("✅ DB pool initialized")
    
    async def disconnect(self):
        """Close database connection pool"""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("✅ DB pool closed")
    
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
    
    async def get_last_candle_timestamp(self, ticker: str, timeframe: str) -> datetime | None:
        """Get the most recent candle timestamp from database"""
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT MAX(timestamp) as last_timestamp FROM market_snapshot WHERE ticker = $1 and timeframe = $2",
                ticker, timeframe
            )
            return result['last_timestamp'] if result and result['last_timestamp'] else None
    
    async def upsert_candles(self, ticker: str, timeframe: str, candles_data: list):
        if not candles_data:
            logger.warning(f"No candles data provided for {ticker} {timeframe}")
            return
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    rows = [
                        (ticker, candle['timestamp'], timeframe, 
                        float(candle['open']), float(candle['high']), 
                        float(candle['low']), float(candle['close']), 
                        )
                        for candle in candles_data
                    ]
                    
                    result = await conn.executemany("""
                        INSERT INTO market_snapshot (ticker, timestamp, timeframe, open, high, low, close)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (ticker, timeframe, timestamp) 
                        DO UPDATE SET 
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            updated_at = NOW()
                    """, rows)
                    
                    logger.info(f"✅ Upserted {len(candles_data)} candles for {ticker} {timeframe}")
                    
        except Exception as e:
            logger.error(f"❌ Failed to upsert candles for {ticker} {timeframe}: {e}")
            raise


db = DatabaseManager()
