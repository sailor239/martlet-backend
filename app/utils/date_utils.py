from datetime import datetime, date, timedelta, time
from zoneinfo import ZoneInfo
from pandas import DataFrame
from typing import Optional, Iterable
from loguru import logger

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


def add_prev_days_high_and_low(df: DataFrame) -> DataFrame:
    df = df.copy()

    # Daily highs and lows by date
    daily_highs = df.groupby('trading_date')['high'].max()
    daily_lows = df.groupby('trading_date')['low'].min()

    # Yesterday's high/low
    prev_day_highs = daily_highs.shift(1)
    prev_day_lows = daily_lows.shift(1)

    # Day before yesterday's high/low
    prev2_day_highs = daily_highs.shift(2)
    prev2_day_lows = daily_lows.shift(2)

    # Map back to intraday df
    df['prev_day_high'] = df['trading_date'].map(prev_day_highs)
    df['prev_day_low'] = df['trading_date'].map(prev_day_lows)
    df['prev2_day_high'] = df['trading_date'].map(prev2_day_highs)
    df['prev2_day_low'] = df['trading_date'].map(prev2_day_lows)

    # Drop rows where we don’t have enough history
    df = df.dropna(
        subset=['prev_day_high', 'prev_day_low', 'prev2_day_high', 'prev2_day_low'],
        how='any'
    ).reset_index(drop=True)

    return df


def process_candles(df: DataFrame, timeframe: str) -> DataFrame:
    df = df.copy()
    df["timeframe"] = timeframe
    df["trading_date"] = df["timestamp"].apply(get_trading_date)
    df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
    df = add_prev_days_high_and_low(df)
    return df
