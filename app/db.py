import asyncpg
from loguru import logger
from typing import Optional
from datetime import datetime
import pandas as pd
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
    
    async def fetch_market_snapshot_by_ticker_by_timeframe(self, ticker: str, timeframe: str, limit: int | None = None):
        async with self.pool.acquire() as conn:
            if limit is not None:
                rows = await conn.fetch(
                    "SELECT * FROM market_snapshot WHERE ticker = $1 AND timeframe = $2 ORDER BY timestamp LIMIT $3",
                    ticker, timeframe, limit
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM market_snapshot WHERE ticker = $1 AND timeframe = $2 ORDER BY timestamp",
                    ticker, timeframe
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
    
    async def get_recent_candles(self, ticker: str, timeframe: str, limit: int = 1000):
        cols = [
            "ticker", "timeframe", "timestamp", "open", "high", "low", "close",
            "trading_date", "ema20", "prev_day_high", "prev_day_low", "prev2_day_high", "prev2_day_low"
        ]
        async with self.pool.acquire() as conn:
            query = f"""
            SELECT {', '.join(cols)}
            FROM market_snapshot
            WHERE ticker = $1 AND timeframe = $2
            ORDER BY timestamp DESC
            LIMIT {limit}
            """
            rows = await conn.fetch(query, ticker, timeframe)

            if not rows:
                return pd.DataFrame(columns=cols)

            # reverse to chronological order
            rows = list(reversed(rows))

            df = pd.DataFrame([dict(r) for r in rows])
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            return df.reset_index(drop=True)
            # return [dict(r) for r in reversed(rows)]
    
    async def upsert_candles(self, ticker: str, timeframe: str, candles_data: list):
        if not candles_data:
            logger.warning(f"No candles data provided for {ticker} {timeframe}")
            return
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():

                    rows = []
                    for candle in candles_data:
                        ts = candle["timestamp"]
                        o = float(candle["open"])
                        h = float(candle["high"])
                        l = float(candle["low"])
                        c = float(candle["close"])
                        trading_date = candle.get("trading_date")
                        ema20 = candle.get("ema20")
                        prev_day_high = candle.get("prev_day_high")
                        prev_day_low = candle.get("prev_day_low")
                        prev2_day_high = candle.get("prev2_day_high")
                        prev2_day_low = candle.get("prev2_day_low")

                        rows.append((ticker, ts, timeframe, o, h, l, c, trading_date, ema20, prev_day_high, prev_day_low, prev2_day_high, prev2_day_low))
                    
                    await conn.executemany("""
                        INSERT INTO market_snapshot
                            (ticker, timestamp, timeframe, open, high, low, close, trading_date, ema20, prev_day_high, prev_day_low, prev2_day_high, prev2_day_low)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                        ON CONFLICT (ticker, timeframe, timestamp)
                        DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            trading_date = EXCLUDED.trading_date,
                            ema20 = EXCLUDED.ema20,
                            prev_day_high = EXCLUDED.prev_day_high,
                            prev_day_low = EXCLUDED.prev_day_low,
                            prev2_day_high = EXCLUDED.prev2_day_high,
                            prev2_day_low = EXCLUDED.prev2_day_low,
                            updated_at = NOW()
                    """, rows)
                    
                    logger.info(f"✅ Upserted {len(candles_data)} candles for {ticker} {timeframe}")
                    
        except Exception as e:
            logger.error(f"❌ Failed to upsert candles for {ticker} {timeframe}: {e}")
            raise
    
    async def create_trade(self, trade_data: dict):
        """Insert a new trade and return the inserted row"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO trades (
                    ticker, direction, size, entry_price, exit_price,
                    entry_time, exit_time, notes, created_at
                )
                VALUES (
                    LOWER($1), $2, $3, $4, $5,
                    $6, $7, $8, NOW()
                )
                RETURNING id, ticker, direction, size, entry_price, exit_price,
                          entry_time, exit_time, notes, created_at
                """,
                trade_data["ticker"],
                trade_data["direction"],
                trade_data["size"],
                trade_data["entry_price"],
                trade_data.get("exit_price"),
                trade_data["entry_time"],
                trade_data.get("exit_time"),
                trade_data.get("notes"),
            )
            return dict(row)
    
    async def list_trades(self, limit: int = 100):
        """Fetch recent trades, ordered by created_at descending"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, ticker, direction, entry_price, exit_price, size,
                       entry_time, exit_time, notes, created_at
                FROM trades
                ORDER BY created_at DESC
                LIMIT $1
                """,
                limit
            )
            return [dict(row) for row in rows]
    
    async def delete_trade(self, trade_id: int) -> bool:
        """Delete a trade by ID. Returns True if deleted, False if not found."""
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM trades WHERE id = $1",
                trade_id
            )
            # asyncpg returns 'DELETE <n>', extract <n>
            deleted_count = int(result.split(" ")[1])
            return deleted_count > 0
    
    async def fetch_backtest_results_by_ticker_by_timeframe(
        self,
        ticker: str,
        timeframe: str,
        strategy: str,
        limit: int = 5000
    ):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT trading_date, equity, pnl
                FROM backtest_results
                WHERE ticker = $1 AND timeframe = $2 AND strategy = $3
                ORDER BY trading_date DESC
                LIMIT $4
                """,
                ticker, timeframe, strategy, limit
            )
        # Return in chronological order
        return list(reversed([dict(r) for r in rows]))

    async def save_backtest_results(
        self,
        ticker: str,
        timeframe: str,
        results: list[dict],
        # strategy_params: Optional[dict] = None
    ):
        """
        Save backtest results to the database.

        Args:
            ticker: ticker symbol
            timeframe: timeframe string
            results: list of dicts with keys: trading_date, equity, pnl
            strategy_params: optional dict of strategy parameters
        """
        if not results:
            return

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                rows = []
                for r in results:
                    rows.append((
                        ticker,
                        timeframe,
                        r["trading_date"],
                        r["equity"],
                        r["pnl"],
                        r["strategy"],
                    ))

                await conn.executemany("""
                    INSERT INTO backtest_results
                        (ticker, timeframe, trading_date, equity, pnl, strategy)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (ticker, timeframe, trading_date, strategy)
                    DO UPDATE SET
                        equity = EXCLUDED.equity,
                        pnl = EXCLUDED.pnl,
                        created_at = NOW()
                """, rows)


db = DatabaseManager()
