import httpx
import time
from loguru import logger
from pydantic import BaseModel
import pandas as pd
from datetime import datetime, timedelta
from pandas import concat
from zoneinfo import ZoneInfo
from app.config import (
    TS_FORMAT
)
from app.constants import (
    UTC, API_KEY_TIINGO
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


async def fetch_data(source: str, ticker: str, timeframe: str, start_date: str = ''):
    logger.info(f'Fetching data from {source}')
    dates = get_all_dates(source, start_date, timeframe)
    dfs = []
    for d in dates:
        if source == "tiingo":
            records = await get_hist_price_from_tiingo(ticker, timeframe, d.start_date, d.end_date)
            if not records:
                logger.warning(f"\tNo data fetched for {ticker} {timeframe} from {d.start_date} to {d.end_date}")
                continue
            logger.info(f"\tFetched {len(records)} rows for {ticker} {timeframe} from {d.start_date} to {d.end_date}")
            
            # Process dataframe
            tmp_df = pd.DataFrame.from_records(records)
            tmp_df.rename(columns={"date": "timestamp"}, inplace=True)
            tmp_df = tmp_df.sort_values("timestamp").reset_index(drop=True)
            tmp_df['timestamp'] = pd.to_datetime(tmp_df['timestamp']).dt.tz_convert(UTC)

            # Save each chunk
            tmp_df.to_csv(f"data/{source}_{ticker}_{timeframe}_{d.start_date}_{d.end_date}.csv", index=False)

            dfs.append(tmp_df)
            time.sleep(5)
        else:
            raise ValueError(f"Invalid source: {source}!")
    # df = concat(dfs, ignore_index=True)
    # df = df.sort_values("timestamp").reset_index(drop=True)
    # df.to_csv(f"data/{source}_{ticker}_{timeframe}.csv", index=False)
    # logger.info(f"Data saved to data/{source}_{ticker}_{timeframe}.csv")


async def get_hist_price_from_tiingo(ticker: str, timeframe: str, start_date: str, end_date: str) -> list:
    logger.info(f"\tFetching <{ticker} | {timeframe}> from {start_date} to {end_date}")
    url = (
        f"https://api.tiingo.com/tiingo/fx/{ticker}/prices"
        f"?startDate={start_date}&endDate={end_date}"
        f"&resampleFreq={timeframe}&token={API_KEY_TIINGO}"
    )
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            res = await client.get(url)
            res.raise_for_status()
            data = res.json()
            if not data or "date" not in data[0]:
                raise ValueError(
                    f"No valid data returned from API for {ticker} {timeframe} "
                    f"from {start_date} to {end_date}!"
                )
            logger.info(f"Fetched {len(data)} records from Tiingo")
            return data
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}", exc_info=True)
        return []


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
