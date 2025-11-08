from pandas import DataFrame
from app.services.backtest.core import BacktestEngine
from app.services.backtest.strategies.previous_day_breakout import previous_day_breakout
from app.services.backtest.strategies.compression_breakout_scalp import compression_breakout_scalp


STRATEGY_MAP = {
    "previous_day_breakout": previous_day_breakout,
    "compression_breakout_scalp": compression_breakout_scalp,
}


def run_backtest(df: DataFrame, strategy_name: str, backtest_settings, enable_time_filter=False):
    if strategy_name not in STRATEGY_MAP:
        raise ValueError(f"Unknown strategy: {strategy_name}")
    engine = BacktestEngine(df, backtest_settings)
    trades = engine.run(STRATEGY_MAP[strategy_name], enable_time_filter)
    return DataFrame(trades)
