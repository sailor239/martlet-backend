from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from app.db import db
from app.schemas.trade import Trade, TradeCreate
from app.utils.date_utils import get_trading_date
from loguru import logger

router = APIRouter(prefix="/trades", tags=["Trades"])

@router.get("/", response_model=list[Trade])
async def list_trades(limit: int = 100):
    return await db.list_trades(limit=limit)

@router.get("/{ticker}/{trading_date}")
async def fetch_trades(
    ticker: str,
    trading_date: str,
    type: str = Query("real", pattern="^(real|simulated)$", description="Trade type: real or simulated"),
):
    """
    Fetch trades for a specific ticker and trading date, filtered by trade type.
    Example: /trades/xauusd/2025-10-12?type=real
    """
    logger.debug(f"Fetching trades for {ticker} | {trading_date} | type={type}")

    # Parse trading_date string to a date object
    try:
        date_obj = datetime.strptime(trading_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid trading_date format (YYYY-MM-DD required)")

    rows = await db.fetch_trades_by_ticker_date_type(ticker, date_obj, type)
    

    # rows = await db.pool.fetch("""
    #     SELECT id, direction, entry_price, exit_price, entry_time, exit_time, size, type, notes
    #     FROM trades
    #     WHERE ticker = $1 AND DATE(entry_time AT TIME ZONE 'Asia/Singapore') = $2 AND type = $3
    #     ORDER BY entry_time ASC
    # """, ticker, date_obj, type)
    print(rows)

    trades = []
    for r in rows:
        trade = dict(r)
        for tkey in ["entry_time", "exit_time"]:
            ts = trade.get(tkey)
            if ts:
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                trade[tkey] = ts.astimezone(ZoneInfo("Asia/Singapore")).isoformat()
        trades.append(trade)
    print(trades)

    return trades

@router.post("/", response_model=Trade)
async def create_trade(trade: TradeCreate):
    if not trade.trading_date:
        trade.trading_date = get_trading_date(trade.entry_time)
    
    try:
        return await db.create_trade(trade)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

@router.delete("/{trade_id}", status_code=204)
async def delete_trade(trade_id: int):
    deleted = await db.delete_trade(trade_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Trade not found")
