from pydantic import BaseModel
from pydantic_settings import BaseSettings
from datetime import time, datetime
from typing import Literal


class BacktestRequest(BaseModel):
    ticker: str
    timeframe: str
    strategy: Literal["previous_day_breakout", "compression_breakout_scalp", "ema_respect_follow"]

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
    risk_per_trade: float = 0.05
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