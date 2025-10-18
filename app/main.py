from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from app.db import db
from app.db_init import init_db_with_csv
from loguru import logger
from app.services.scheduler import scheduler_service
from app.services.backtest import run_backtest
from app.utils.backtest_utils import get_daily_summary
from app.models import CandleRequest, BacktestRequest, BacktestResult, BacktestSettings
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pandas import DataFrame, date_range, concat
from app.routes import (
    trades, status
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

app.include_router(trades.router)
app.include_router(status.router)

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
    logger.debug(f"Fetched {len(results)} records from DB")

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
    logger.debug(f"Fetched {len(df)} rows of data from DB for {req.ticker} | {req.timeframe}")

    all_dates = date_range(
        start=df["timestamp"].min(),
        end=df["timestamp"].max(),
    )

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

    calendar_df = DataFrame({"trading_date": all_dates})
    calendar_df["trading_date"] = calendar_df["trading_date"].dt.date
    df_daily_summary = calendar_df.merge(df_daily_summary, on="trading_date", how="left")

    # Insert the start row at the beginning
    start_row = DataFrame({
        "trading_date": [start_date.date()],
        "pnl": [0],
        "equity": [10000],
    })
    df_daily_summary = concat([start_row, df_daily_summary[["trading_date", "pnl", "equity"]]], ignore_index=True)

    # forward-fill equity, fill pnl=0 for missing days
    df_daily_summary["equity"] = df_daily_summary["equity"].ffill()
    df_daily_summary["pnl"] = df_daily_summary["pnl"].fillna(0)

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
