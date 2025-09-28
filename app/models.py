from pydantic import BaseModel
from pydantic_settings import BaseSettings
from datetime import date, time, datetime
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
    pnl: float

class AccountSettings(BaseModel):
    starting_cash: float = 10000.0
    commission: float = 7
    leverage: int = 100

class StrategySettings(BaseModel):
    take_profit: float = 4
    stop_loss: float = 5
    risk_per_trade: float = 0.02
    trade_until_loss: bool = False
    trade_until_win: bool = True
    position_size_limit_enabled: bool = False
    position_size_limit: float = 100.00
    max_holding_bars: int = 0

    # Allow trade entries only between these times (SGT)
    enable_time_filter: bool = False
    trade_entry_start_time: time = time(9, 0)
    trade_entry_end_time: time = time(20, 0)

class BacktestSettings(BaseSettings):
    account: AccountSettings = AccountSettings()
    strategy: StrategySettings = StrategySettings()