import csv
import asyncpg
import asyncio
from datetime import datetime
from app.config import DATABASE_URL, CSV_PATH


INSERT_ROW_SQL = """
INSERT INTO market_snapshot (timestamp, ticker, timeframe, open, high, low, close)
VALUES ($1, $2, $3, $4, $5, $6, $7)
"""
TIMEFRAME = "5min"

async def init_db_with_csv():
    print("📦 Starting DB init from CSV...")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute("DELETE FROM market_snapshot")  # optional: clear table

        with open(CSV_PATH, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            rows = []
            for row in reader:
                try:
                    # Parse ISO8601 timestamp (remove 'Z' if present)
                    timestamp_str = row["date"].rstrip("Z")
                    timestamp_dt = datetime.fromisoformat(timestamp_str)
                    # For data from forexsb
                    # timestamp_dt = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M")
                    rows.append((
                        timestamp_dt,
                        row["ticker"],
                        TIMEFRAME,
                        float(row["open"]),
                        float(row["high"]),
                        float(row["low"]),
                        float(row["close"]),
                    ))
                except Exception as row_err:
                    print(f"⚠️ Skipping bad row: {row} -> {row_err}")

        # Use executemany to insert all at once (faster & more reliable)
        await conn.executemany(INSERT_ROW_SQL, rows)

        await conn.close()
        print("✅ Database initialization from CSV completed successfully!")

    except Exception as e:
        print(f"❌ DB init failed: {e}")


if __name__ == "__main__":
    asyncio.run(init_db_with_csv())
