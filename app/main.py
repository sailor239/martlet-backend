import json
from starlette.responses import StreamingResponse
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from app.db import db
from app.db_init import init_db_with_csv
from loguru import logger
from app.services.scheduler import scheduler_service
from app.models import CandleRequest
from fastapi.middleware.cors import CORSMiddleware
from datetime import timezone
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
        </body>
    </html>
    """
    return html_content

# @app.get("/candles")
# async def get_candle_data():
#     return await db.fetch_all_data()

@app.post("/candles/")
async def get_ticker_candles(payload: CandleRequest):
    """Stream candle data for a given ticker and timeframe"""

    ticker = payload.ticker
    timeframe = payload.timeframe
    trading_date = payload.trading_date

    # now_utc = datetime.now(timezone.utc)

    async def row_stream():
        async with db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT *
                FROM (
                    SELECT timestamp, ticker, timeframe, open, high, low, close, trading_date, ema20, prev_day_high, prev_day_low
                    FROM market_snapshot
                    WHERE ticker = $1 AND timeframe = $2 AND trading_date = $3
                    ORDER BY timestamp DESC
                ) sub
                ORDER BY timestamp ASC
                """,
                ticker, timeframe, trading_date
            )

        if not rows:
            return
        
        # latest_trading_date = max([r["trading_date"] for r in rows])
        # latest_rows = [r for r in rows if r["trading_date"] == latest_trading_date]

        # Yield rows matching latest latest_trading_date
        for record in rows:
            row = dict(record)

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
