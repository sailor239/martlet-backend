import math
from datetime import time, datetime, timedelta
import pandas as pd
import numpy as np
from pandas import DataFrame
from app.models import BacktestSettings

FOREX_MARKET_HOURS = [
    (time(7, 0), time(6, 0)),  # 07:00 → next day 05:55
    (time(6, 0), time(5, 0)),  # 06:00 → next day 04:55
]
CSV_COLS = ["timestamp", "open", "high", "low", "close", "volume"]
entry_colors = {
    ("long", True): "black",    # Bright green for profitable long entry
    ("long", False): "black",    # Dark green for losing long entry
    ("short", True): "black",    # Bright red for profitable short entry
    ("short", False): "black",    # Dark red for losing short entry
}
exit_colors = {
    ("long", True): "green",    # Bright green for profitable long entry
    ("long", False): "red",    # Dark green for losing long entry
    ("short", True): "green",    # Bright red for profitable short entry
    ("short", False): "red",    # Dark red for losing short entry
}


def round_down(x, decimals=0):
    factor = 10 ** decimals
    return math.floor(x * factor) / factor


# def load_df() -> DataFrame:
#     # df = pd.read_csv("./data/prod/XAUUSD_M1_MAX.csv", header=None, names=CSV_COLS)
#     df = pd.read_csv("./data/prod/XAUUSD_M5.csv", header=None, names=CSV_COLS)
#     # df = pd.read_csv("./data/prod/CHFJPY_M5.csv", header=None, names=CSV_COLS)
#     # df = pd.read_csv("./data/prod/XAUUSD_H1.csv", header=None, names=CSV_COLS)
#     # df = pd.read_csv("./data/prod/USA500IDXUSD_M5.csv", header=None, names=CSV_COLS)
#     df['timestamp'] = pd.to_datetime(df['timestamp'], format="%Y-%m-%d %H:%M")
#     df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
#     df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Singapore')
#     df = df.sort_values('timestamp').reset_index(drop=True)
#     df['trading_date'] = df['timestamp'].apply(lambda ts: assign_trading_date(ts, FOREX_MARKET_HOURS))
#     df = add_prev_day_high_and_low(df)
#     # df = df.iloc[53334:]

#     return df


def get_position_size(equity: float, backtest_settings: BacktestSettings) -> float:
    risk_based_position_size = round_down((equity * backtest_settings.strategy.risk_per_trade) / (backtest_settings.strategy.stop_loss * backtest_settings.account.leverage), 2)
    if backtest_settings.strategy.position_size_limit_enabled:
        return min(risk_based_position_size, backtest_settings.strategy.position_size_limit)
    return risk_based_position_size


def update_iteration_data(equity: float, peak_equity: float, max_drawdown: float) -> tuple[float, float, float]:
    # Update peak equity
    if equity > peak_equity:
        peak_equity = equity

    # Calculate current drawdown
    drawdown = peak_equity - equity

    # Update max drawdown
    if drawdown > max_drawdown:
        max_drawdown = drawdown

    return drawdown, max_drawdown, peak_equity


def get_daily_summary(trades: DataFrame, starting_cash: float) -> tuple[DataFrame, list]:
    df_daily_summary = trades.groupby('trading_date').agg(
        pnl=('pnl', 'sum'),
        position_size=('position_size', 'mean'),
        num_trades=('pnl', 'count'),
        avg_trade_duration=('trade_duration', 'mean')
    ).reset_index()

    df_daily_summary['equity'] = df_daily_summary['pnl'].cumsum()
    df_daily_summary['equity'] = df_daily_summary['equity'] + starting_cash
    df_daily_summary['pnl_pct'] = (df_daily_summary['pnl'] / df_daily_summary['equity']).round(4)

    df_daily_summary['cummax'] = df_daily_summary['equity'].cummax()
    df_daily_summary['drawdown'] = df_daily_summary['equity'] / df_daily_summary['cummax'] - 1

    # Identify drawdown regions
    drawdown_periods = []
    in_drawdown = False
    start_date = None

    for i in range(len(df_daily_summary)):
        if df_daily_summary.loc[i, 'drawdown'] < 0 and not in_drawdown:
            # drawdown starts
            start_date = df_daily_summary.loc[i, 'trading_date']
            in_drawdown = True
        elif df_daily_summary.loc[i, 'drawdown'] == 0 and in_drawdown:
            # drawdown ends
            end_date = df_daily_summary.loc[i, 'trading_date']
            drawdown_periods.append((start_date, end_date))
            in_drawdown = False

    # If curve ends in drawdown
    if in_drawdown:
        drawdown_periods.append((start_date, df_daily_summary['trading_date'].iloc[-1]))

    return df_daily_summary, drawdown_periods


def analyze_equity_curve(df: pd.DataFrame) -> dict:
    df = df.copy()
    df['trading_date'] = pd.to_datetime(df['trading_date'], format='%Y-%m-%d')

    df = df.sort_values("trading_date").reset_index(drop=True)

    # Track running max equity
    df["running_max"] = df["equity"].cummax()

    # Days since last high
    df["days_since_high"] = (df["trading_date"] -
                             df["trading_date"].where(
                                 df["equity"] == df["running_max"]
                             ).ffill()).dt.days

    # Find the max time to make new high
    max_recovery_time = df["days_since_high"].max()

    max_drawdown_period = df.loc[
        df["days_since_high"] == max_recovery_time,
        ["trading_date", "equity"]
    ]

    # Create a boolean mask: True if in drawdown (equity below running max)
    df['in_drawdown'] = df['equity'] < df['running_max']

    # Count how many rows are in drawdown
    drawdown_days = df['in_drawdown'].sum()

    # Total trading days
    total_days = len(df)

    # Percentage of time in drawdown
    drawdown_pct = drawdown_days / total_days

    # Calculate daily returns
    df["daily_return"] = df["equity"].pct_change()
    df = df.dropna()

    total_return = df["equity"].iloc[-1] / df["equity"].iloc[0] - 1
    num_days = len(df)
    # num_days = (df["trading_date"].iloc[-1] - df["trading_date"].iloc[0]).days

    # Annualized return and volatility (assuming 252 trading days/year)
    annualized_return = (1 + total_return) ** (365.25 / num_days) - 1
    annualized_volatility = df["daily_return"].std() * np.sqrt(365.25)

    sharpe_ratio = np.nan
    if annualized_volatility != 0:
        sharpe_ratio = annualized_return / annualized_volatility

    # Max drawdown
    cumulative_max = df["equity"].cummax()
    drawdown = df["equity"] / cumulative_max - 1
    max_drawdown = drawdown.min()

    # Win/Loss days
    positive_days = (df["daily_return"] > 0).sum()
    negative_days = (df["daily_return"] < 0).sum()
    win_rate = positive_days / (positive_days + negative_days)

    return {
        "Total Return": total_return,
        "Annualized Return": annualized_return,
        "Annualized Volatility": annualized_volatility,
        "Sharpe Ratio": sharpe_ratio,
        "Max Drawdown": max_drawdown,
        "Positive Days": positive_days,
        "Negative Days": negative_days,
        "Win Rate": win_rate,
        "Number of Days": num_days,
        "Max Recovery Time": max_recovery_time,
        "Max Drawdown Period": max_drawdown_period,
        "Drawdown %": drawdown_pct
    }






def add_prev_day_high_and_low(df):
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
        how='all'
    ).reset_index(drop=True)

    # Daily ranges
    df['prev_day_range'] = df['prev_day_high'] - df['prev_day_low']

    return df
