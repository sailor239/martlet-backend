from pydantic import BaseModel
from datetime import date


class CandleRequest(BaseModel):
    ticker: str
    timeframe: str
    trading_date: date | None = None
