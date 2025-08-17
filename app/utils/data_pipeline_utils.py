import logging
import requests
import time
from pydantic import BaseModel
from datetime import datetime, timedelta
from pandas import DataFrame, concat
from zoneinfo import ZoneInfo
from app.config import (
    TS_FORMAT, LOGGING_LEVEL, LOGGING_FORMAT, LOGGING_DATE_FORMAT
)
from app.constants import (
    UTC, API_KEY_TIINGO
)

logging.basicConfig(
    level=LOGGING_LEVEL,
    format=LOGGING_FORMAT,
    datefmt=LOGGING_DATE_FORMAT
)


class DateRange(BaseModel):
    start_date: str
    end_date: str


def round_down_to_n_mins(dt: datetime, n: int) -> datetime:
    return dt - timedelta(
        minutes=dt.minute % n,
        seconds=dt.second,
        microseconds=dt.microsecond
    )


def fetch_data(source: str, ticker: str, timeframe: str, start_date: str = ''):
    logging.info(f'Fetching data from {source}')
    dates = get_all_dates(source, start_date, timeframe)
    dfs = []
    for d in dates:
        if source == "tiingo":
            tmp_df = get_hist_price_from_tiingo(ticker, timeframe, d.start_date, d.end_date)
            dfs.append(tmp_df)
            time.sleep(1)
    df = concat(dfs, ignore_index=True)
    df.to_csv(f"data/{source}_{ticker}_{timeframe}.csv", index=False)
    logging.info(f"Data saved to data/{source}_{ticker}_{timeframe}.csv")


def get_hist_price_from_tiingo(ticker: str, timeframe: str, start_date: str, end_date: str):
    logging.info(f"\tFetching <{ticker} | {timeframe}> from {start_date} to {end_date}")
    url = f"https://api.tiingo.com/tiingo/fx/{ticker}/prices?startDate={start_date}&endDate={end_date}&resampleFreq={timeframe}&token={API_KEY_TIINGO}"
    logging.debug(url)
    try:
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()
        if not data or 'date' not in data[0]:
            raise ValueError("No valid data returned from the API.")
        return DataFrame(data)
    except Exception as e:
        logging.error(f"Failed to fetch data: {e}", exc_info=True)
        return DataFrame()


def get_n_days(source: str, timeframe: str) -> int:
    if source == "tiingo":
        if timeframe == '5min':
            n_days = 27
        elif timeframe == '1hour':
            n_days = 181
        else:
            raise ValueError(f"Invalid timeframe provided for {source}!")
    elif source == "tradermade":
        if timeframe == '5min':
            n_days = 2
        else:
            raise ValueError(f"Invalid timeframe provided for {source}!")
    else:
        raise ValueError(f"Invalid source: {source}!")
    
    return n_days


def get_all_dates(source: str, start_str: str, timeframe: str) -> list[DateRange]:
    dates = []
    n_days = get_n_days(source, timeframe)

    start_obj = datetime.strptime(start_str, TS_FORMAT[source])
    current_time = datetime.now(ZoneInfo(UTC)).replace(tzinfo=None)
    
    while start_obj <= current_time:
        end_obj = start_obj + timedelta(days=n_days)
        if (source == "tradermade") and (end_obj >= current_time):
            end_obj = round_down_to_n_mins(current_time, 5)

        dates.append(
            DateRange(
                start_date=start_obj.strftime(TS_FORMAT[source]),
                end_date=end_obj.strftime(TS_FORMAT[source])
            )
        )

        start_obj += timedelta(days=n_days)
        if start_obj.weekday() == 5:    # If it is Saturday, move to Sunday
            start_obj += timedelta(days=1)

    return dates
