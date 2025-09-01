from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

def get_trading_day_bounds(tz_name="UTC"):
    """
    Returns (start_utc, end_utc) for the current trading day.
    Forex trading day: 22:00 previous UTC → 21:00 current UTC
    """
    now_utc = datetime.now(tz=ZoneInfo("UTC"))

    # Trading day start in UTC
    today = now_utc.date()
    start_utc = datetime.combine(today, time(hour=22, tzinfo=ZoneInfo("UTC")))

    # If current time is before 22:00 UTC, we are still in previous trading day
    if now_utc < start_utc:
        start_utc -= timedelta(days=1)

    # Trading day end is 23 hours after start
    end_utc = start_utc + timedelta(hours=23)

    return start_utc, end_utc
