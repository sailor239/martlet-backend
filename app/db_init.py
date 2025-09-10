import asyncpg
import asyncio
import pandas as pd
from app.config import DATABASE_URL, CSV_PATH
from app.utils.date_utils import get_trading_date, add_prev_days_high_and_low


INSERT_ROW_SQL = """
INSERT INTO market_snapshot (
    timestamp, ticker, timeframe, open, high, low, close,
    trading_date, ema20, prev_day_high, prev_day_low, prev2_day_high, prev2_day_low
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
"""
TIMEFRAME = "5min"

async def init_db_with_csv():
    print("📦 Starting DB init from CSV...")
    conn = None

    try:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute("DELETE FROM market_snapshot")  # optional: clear table

        # --- Step 1: Load CSV into DataFrame ---
        df = pd.read_csv(CSV_PATH, parse_dates=["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)
        
        # --- Step 2: Compute derived features ---
        df["timeframe"] = TIMEFRAME
        df["trading_date"] = df["timestamp"].apply(get_trading_date)
        df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
        df = add_prev_days_high_and_low(df)

        # Replace NaNs with None for asyncpg
        df = df.where(pd.notna(df), None)

        # df.to_csv("debug_processed.csv", index=False)  # for debugging

        # --- Step 3: Prepare rows for insertion ---
        rows = [
            (
                row.timestamp,
                row.ticker,
                row.timeframe,
                row.open,
                row.high,
                row.low,
                row.close,
                row.trading_date,
                row.ema20,
                row.prev_day_high,
                row.prev_day_low,
                row.prev2_day_high,
                row.prev2_day_low
            )
            for row in df.itertuples(index=False)
        ]

        # --- Step 4: Insert into DB ---
        if rows:
            await conn.executemany(INSERT_ROW_SQL, rows)
            print(f"✅ Inserted {len(rows)} rows into database")
        else:
            print("⚠️ No valid rows to insert")

        print("✅ DB init from CSV completed successfully!")

    except Exception as e:
        print(f"❌ DB init from CSV failed: {e}")
    
    finally:
        if conn:
            await conn.close()
            print("✅ DB connection closed")


if __name__ == "__main__":
    asyncio.run(init_db_with_csv())
