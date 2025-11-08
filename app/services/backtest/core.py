from pandas import DataFrame
from loguru import logger
from app.schemas.backtest import BacktestSettings
from app.utils.backtest_utils import (
    get_position_size, update_iteration_data
)

class BacktestEngine:
    def __init__(self, df: DataFrame, settings: BacktestSettings):
        self.df = df.copy()
        self.settings = settings
        self.equity = self.peak_equity = settings.account.starting_cash
        self.max_drawdown = 0.0
        self.trades = []
        self.current_trade = None
        self.active_day = None

    def open_trade(self, side: str, entry_price: float, entry_time, entry_index: int, trading_date):
        self.current_trade = {
            "side": side,
            "entry_price": entry_price,
            "entry_time": entry_time,
            "entry_index": entry_index,
        }
        self.active_day = trading_date

    def close_trade(self, exit_price: float, exit_time, reason: str, row):
        side = self.current_trade["side"]
        entry_price = self.current_trade["entry_price"]
        position_size = get_position_size(self.equity, self.settings)

        # raw PnL calculation
        if reason == "take_profit":
            raw_pnl = self.settings.strategy.take_profit
        elif reason == "stop_loss":
            raw_pnl = -self.settings.strategy.stop_loss
        else:
            raw_pnl = (exit_price - entry_price) * (1 if side == "long" else -1)

        pnl = round(
            (float(raw_pnl) * self.settings.account.leverage - float(self.settings.account.commission))
            * position_size,
            2,
        )

        self.equity += pnl
        drawdown, self.max_drawdown, self.peak_equity = update_iteration_data(
            self.equity, self.peak_equity, self.max_drawdown
        )

        self.trades.append({
            "trade_id": self.current_trade["entry_index"],
            "trading_date": row["trading_date"],
            "side": side,
            "position_size": position_size,
            "entry_time": self.current_trade["entry_time"],
            "exit_time": exit_time,
            "entry_price": entry_price,
            "exit_price": exit_price,
            'trade_duration': (exit_time - self.current_trade['entry_time']).total_seconds() / 60 + 5,
            "exit_reason": reason,
            "pnl": pnl,
            "drawdown": drawdown,
            "max_drawdown": self.max_drawdown,
        })

        self.current_trade = None

    def run(self, strategy_fn, enable_time_filter=False):
        logger.info(f"Running backtest: {strategy_fn.__name__} with below settings:")
        logger.info(f"{self.settings}")
        return strategy_fn(self, self.df, enable_time_filter)
