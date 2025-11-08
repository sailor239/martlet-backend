from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from app.db import db
from app.db_init import init_db_with_csv
from loguru import logger
from app.services.scheduler import scheduler_service
from app.schemas.core import CandleRequest
from fastapi.middleware.cors import CORSMiddleware
from datetime import timezone
from zoneinfo import ZoneInfo
from app.routes import (
    auth, backtest, trades, status
)


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
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(trades.router)
app.include_router(status.router)
app.include_router(backtest.router)

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


@app.post("/intraday/")
async def fetch_intraday_data(payload: CandleRequest):
    """Return intraday data for a given ticker, timeframe and trading_date"""

    ticker = payload.ticker
    timeframe = payload.timeframe
    trading_date = payload.trading_date

    INTRADAY_TIMEFRAMES = {"5min"}

    if timeframe not in INTRADAY_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Timeframe '{timeframe}' is not supported. Allowed: {INTRADAY_TIMEFRAMES}"
        )
    
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp, ticker, timeframe, open, high, low, close, trading_date, ema20, prev_day_high, prev_day_low
            FROM market_snapshot
            WHERE ticker = $1 AND timeframe = $2 AND trading_date = $3
            ORDER BY timestamp ASC
            """,
            ticker, timeframe, trading_date
        )

    result = []
    for record in rows:
        row = dict(record)
        ts_utc = row["timestamp"]
        if ts_utc.tzinfo is None:
            ts_utc = ts_utc.replace(tzinfo=timezone.utc)
        row["timestamp_sgt"] = ts_utc.astimezone(ZoneInfo("Asia/Singapore"))
        result.append(row)

    return result
