import json
from starlette.responses import StreamingResponse
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from app.db import db
from app.db_init import init_db_with_csv
from loguru import logger
from app.services.scheduler import scheduler_service
from app.services.backtest import run_backtest
from app.utils.backtest_utils import get_daily_summary
from app.models import CandleRequest, Trade, TradeCreate, BacktestRequest, BacktestResult, BacktestSettings
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone, timedelta
import random
from zoneinfo import ZoneInfo
from typing import Literal, cast
from pandas import DataFrame


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


@app.get("/trades/{ticker}/{trading_date}")
async def fetch_trades(ticker: str, trading_date: str):
    logger.info(f"Fetching trades for {ticker} on {trading_date}")

    # Parse trading_date string to a date object
    try:
        trading_date_obj = datetime.strptime(trading_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid trading_date format. Use YYYY-MM-DD.")

    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, direction, entry_price, exit_price, entry_time, exit_time, size, notes
            FROM trades
            WHERE ticker = $1
              AND DATE(entry_time AT TIME ZONE 'Asia/Singapore') = $2
            ORDER BY entry_time ASC
            """,
            ticker, trading_date_obj
        )

    trades = []
    for r in rows:
        trade = dict(r)

        # Convert to SGT
        for key in ["entry_time", "exit_time"]:
            ts = trade.get(key)
            if ts:
                # Ensure UTC
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                trade[key] = ts.astimezone(ZoneInfo("Asia/Singapore")).isoformat()

        trades.append(trade)

    return trades


@app.get("/trades/", response_model=list[Trade])
async def list_trades(limit: int = 100):
    """Return recent trades, most recent first"""
    trades = await db.list_trades(limit=limit)
    return trades


@app.post("/trades/")
async def create_trade(trade: TradeCreate):
    try:
        new_trade = await db.create_trade(trade.model_dump())
        return new_trade
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    return new_trade


@app.delete("/trades/{trade_id}", status_code=204)
async def delete_trade(trade_id: int):
    deleted = await db.delete_trade(trade_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Trade not found")
    return


@app.get("/backtest-results/", response_model=list[BacktestResult])
async def backtest_results(
    strategy: str = Query(..., description="Trading strategy name"),
    ticker: str = Query(..., description="Ticker symbol"),
    timeframe: str = Query(..., description="Timeframe, e.g., 5min, 15min")
):
    logger.debug(f"Fetching backtest results for {strategy} | {ticker} | {timeframe}")
    results = await db.fetch_backtest_results(strategy, ticker, timeframe)
    
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f'No backtest results found for "{strategy}" | "{ticker}" | "{timeframe}"'
        )
    return [
        BacktestResult(
            timestamp=r["trading_date"],
            equity=r["equity"],
            pnl=r["pnl"]
        )
        for r in results
    ]

@app.post("/trigger_backtest_run/", response_model=list[BacktestResult])
async def trigger_backtest_run(req: BacktestRequest):
    data = await db.fetch_market_snapshot_by_ticker_by_timeframe(req.ticker, req.timeframe)
    if not data:
        raise HTTPException(status_code=404, detail=f'No market data found for "{req.ticker}" and "{req.timeframe}"')

    df = DataFrame(data)
    
    start_date = datetime(2022, 1, 1, tzinfo=timezone.utc)
    df = df[df['timestamp'] >= start_date]

    if req.strategy == "previous_day_breakout":
        backtest_settings = BacktestSettings()
        backtest_settings.strategy.take_profit = 4
        backtest_settings.strategy.stop_loss = 5
        backtest_settings.strategy.risk_per_trade = 0.05
    elif req.strategy == "compression_breakout_scalp":
        backtest_settings = BacktestSettings()
        backtest_settings.strategy.take_profit = 1.2
        backtest_settings.strategy.stop_loss = 28
        backtest_settings.strategy.risk_per_trade = 1
    else:
        raise HTTPException(status_code=400, detail=f'Unknown strategy "{req.strategy}"')

    results = run_backtest(df, req.strategy, backtest_settings)

    df_daily_summary, drawdown_periods = get_daily_summary(results, backtest_settings.account.starting_cash)
    logger.info(f'Backtest completed for "{req.ticker}" | "{req.timeframe}"')
    results_to_save = [
        {
            "trading_date": row["trading_date"],
            "equity": row["equity"],
            "pnl": row["pnl"],
            "strategy": req.strategy,
        }
        for _, row in df_daily_summary.iterrows()
    ]

    await db.save_backtest_results(req.ticker, req.timeframe, results_to_save)

    # Return as API response
    latest_results = results_to_save[-100:]

    return [
        BacktestResult(timestamp=r["trading_date"], equity=r["equity"], pnl=r["pnl"])
        for r in results_to_save
    ]


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
