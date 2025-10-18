from pydantic import BaseModel
from datetime import date, datetime
from typing import Literal, Optional


class Trade(BaseModel):
    id: int
    ticker: str
    direction: str
    entry_price: float
    exit_price: Optional[float] = None
    size: float
    type: Literal["simulated", "real"]
    entry_time: datetime
    exit_time: Optional[datetime] = None
    trading_date: Optional[date] = None
    notes: Optional[str] = None
    created_at: datetime

class TradeCreate(BaseModel):
    ticker: str
    direction: str
    entry_price: float
    exit_price: Optional[float] = None
    size: float
    type: Literal["simulated", "real"]
    entry_time: datetime
    exit_time: Optional[datetime] = None
    trading_date: Optional[date] = None
    notes: Optional[str] = None
