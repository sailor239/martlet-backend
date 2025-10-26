from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timezone
from app.db import db
from app.schemas.backtest import BacktestRequest, BacktestResult, BacktestSettings
from app.utils.backtest_utils import get_daily_summary
from loguru import logger
from pandas import DataFrame, date_range, concat
from app.services.backtest import run_backtest
# from app.services._backtest import _run_backtest


router = APIRouter(prefix="/backtest", tags=["Backtest"])

@router.get("/results/", response_model=list[BacktestResult])
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


@router.post("/run/", response_model=list[BacktestResult])
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

        results = run_backtest(df, req.strategy, backtest_settings)
    elif req.strategy == "compression_breakout_scalp":
        backtest_settings = BacktestSettings()
        backtest_settings.strategy.take_profit = 1.2
        backtest_settings.strategy.stop_loss = 28
        backtest_settings.strategy.risk_per_trade = 1

        results = run_backtest(df, req.strategy, backtest_settings)
    else:
        raise HTTPException(status_code=400, detail=f'Unknown strategy "{req.strategy}"')

    

    df_daily_summary, drawdown_periods = get_daily_summary(results, backtest_settings.account.starting_cash)
    logger.info(f'Backtest completed for "{req.strategy} | {req.ticker}" | "{req.timeframe}"')

    calendar_df = DataFrame({"trading_date": all_dates})
    calendar_df["trading_date"] = calendar_df["trading_date"].dt.date
    df_daily_summary = calendar_df.merge(df_daily_summary, on="trading_date", how="left")

    # Insert the start row at the beginning
    start_row = DataFrame({
        "trading_date": [start_date.date()],
        "pnl": [0],
        "equity": [backtest_settings.account.starting_cash],
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

    return [
        BacktestResult(timestamp=r["trading_date"], equity=r["equity"], pnl=r["pnl"])
        for r in results_to_save
    ]
