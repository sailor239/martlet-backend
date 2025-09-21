from pydantic import BaseModel
from datetime import date, datetime
from typing import Literal, Optional

class CandleRequest(BaseModel):
    ticker: str
    timeframe: str
    trading_date: date | None = None

class Trade(BaseModel):
    id: int
    ticker: str
    direction: str
    entry_price: float
    exit_price: Optional[float] = None
    size: float
    entry_time: datetime
    exit_time: Optional[datetime] = None
    notes: Optional[str] = None
    created_at: datetime

class TradeCreate(BaseModel):
    ticker: str
    direction: str
    entry_price: float
    exit_price: Optional[float] = None
    size: float
    entry_time: datetime
    exit_time: Optional[datetime] = None
    notes: Optional[str] = None

class BacktestRequest(BaseModel):
    ticker: str
    timeframe: str

class BacktestResult(BaseModel):
    timestamp: datetime
    equity: float
    position: Literal["long", "short", "flat"]
    pnl: float