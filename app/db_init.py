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
    conn = None

    try:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute("DELETE FROM market_snapshot")  # optional: clear table

        with open(CSV_PATH, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            rows = []
            processed_count = 0
            error_count = 0

            for row in reader:
                try:
                    timestamp_str = row["timestamp"]
                    # timestamp_str = row["date"].rstrip("Z") # Parse ISO8601 timestamp (remove 'Z' if present)
                    timestamp_dt = datetime.fromisoformat(timestamp_str)
                    # timestamp_dt = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M")    # For data from forexsb
                    rows.append((
                        timestamp_dt,
                        row["ticker"],
                        TIMEFRAME,
                        float(row["open"]),
                        float(row["high"]),
                        float(row["low"]),
                        float(row["close"]),
                    ))
                    processed_count += 1

                    # Progress indicator for large files
                    if processed_count % 1000 == 0:
                        print(f"📊 Processed {processed_count} rows...")
                    
                except Exception as row_err:
                    error_count += 1
                    print(f"⚠️ Skipping bad row {processed_count + error_count}: {row} -> {row_err}")
        
        print(f"✅ Finished processing CSV. Total valid rows: {processed_count}, Errors: {error_count}")

        # Use executemany to insert all at once (faster & more reliable)
        if rows:
            await conn.executemany(INSERT_ROW_SQL, rows)
            print(f"✅ Inserted {len(rows)} rows into database")
        else:
            print("⚠️ No valid rows to insert")

        await conn.close()
        print("✅ Database initialization from CSV completed successfully!")

    except Exception as e:
        print(f"❌ DB init failed: {e}")
    
    finally:
        if conn is not None:
            await conn.close()


if __name__ == "__main__":
    asyncio.run(init_db_with_csv())
