import json
from starlette.responses import StreamingResponse
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from app.db import db
from app.db_init import init_db_with_csv
from loguru import logger
from app.services.scheduler import scheduler_service
from app.utils.date_utils import get_trading_date
from app.models import CandleRequest
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Starting Martlet backend...")
    try:
        await db.connect()
        # await init_db_with_csv()
        scheduler_service.start()
        logger.info("✅ Application startup complete")
    except Exception as e:
        logger.error(f"❌ Error during startup: {e}")
    
    yield

    logger.info("🛑 Stopping Martlet backend")
    try:
        scheduler_service.stop()
        await db.disconnect()
        logger.info("✅ DB disconnected")
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse)
async def home():
    html_content = """
    <html>
        <head>
            <title>Martlet Backend Service</title>
        </head>
        <body>
            <h1>Welcome to Martlet Backend Service</h1>
            <p>Use the <a href="/candles/xauusd">/candles</a> endpoint to see the data.</p>
        </body>
    </html>
    """
    return html_content

@app.get("/candles")
async def get_candle_data():
    return await db.fetch_all_data()

@app.post("/candles/")
async def get_ticker_candles(payload: CandleRequest):
    """Stream today's trading date candles with derived trading_date column."""

    ticker = payload.ticker
    timeframe = payload.timeframe

    # now_utc = datetime.now(timezone.utc)

    async def row_stream():
        async with db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT *
                FROM (
                    SELECT timestamp, ticker, timeframe, open, high, low, close
                    FROM market_snapshot
                    WHERE ticker = $1 AND timeframe = $2
                    ORDER BY timestamp DESC
                    LIMIT 500
                ) sub
                ORDER BY timestamp ASC
                """,
                ticker, timeframe
            )

        if not rows:
            return
        
        # Find latest trading date in the rows
        latest_trading_date = max(get_trading_date(r["timestamp"]) for r in rows)

        # Yield rows matching latest trading date
        for record in rows:
            trading_date = get_trading_date(record["timestamp"])
            if trading_date != latest_trading_date:
                continue

            row = dict(record)
            row["trading_date"] = trading_date

            ts_utc = row["timestamp"]
            if ts_utc.tzinfo is None:
                ts_utc = ts_utc.replace(tzinfo=timezone.utc)
            row["timestamp_sgt"] = ts_utc.astimezone(ZoneInfo("Asia/Singapore"))

            yield json.dumps(row, default=str) + "\n"

    return StreamingResponse(row_stream(), media_type="application/json")

@app.get("/status")
async def get_status():
    """Get system status"""
    try:
        async with db.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return {
        "database": db_status,
        "scheduler": "running" if scheduler_service.is_running else "stopped",
        "jobs": len(scheduler_service.get_jobs())
    }
