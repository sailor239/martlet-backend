from pydantic import BaseModel

class CandleRequest(BaseModel):
    ticker: str
    timeframe: str
