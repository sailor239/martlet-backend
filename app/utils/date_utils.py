from datetime import datetime, date, timedelta, time
from zoneinfo import ZoneInfo

def get_trading_date(utc_timestamp: datetime) -> date:
    """
    Simple trading date assignment:
    - If time >= 22:00 UTC: trading_date = next day
    - If time < 22:00 UTC: trading_date = current day
    """
    if utc_timestamp.hour >= 22:
        return (utc_timestamp.date() + timedelta(days=1))
    else:
        return utc_timestamp.date()
