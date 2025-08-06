import csv
import asyncpg
import asyncio
from datetime import datetime
from app.config import DATABASE_URL, CSV_PATH

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS market_snapshot (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION
)
"""

INSERT_ROW_SQL = """
INSERT INTO market_snapshot (timestamp, open, high, low, close, volume)
VALUES ($1, $2, $3, $4, $5, $6)
"""

async def init_db_with_csv():
    print("📦 Starting DB init from CSV...")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute(CREATE_TABLE_SQL)
        await conn.execute("DELETE FROM market_snapshot")  # optional: clear table

        with open(CSV_PATH, newline="") as csvfile:
            reader = csv.DictReader(csvfile)

            rows = []
            for row in reader:
                try:
                    timestamp_dt = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M")
                    rows.append((
                        timestamp_dt,
                        float(row["open"]),
                        float(row["high"]),
                        float(row["low"]),
                        float(row["close"]),
                        float(row["volume"]),
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
