from pandas import DataFrame, notna
from app.models import BacktestSettings
from app.utils.backtest_utils import (
    get_position_size, update_iteration_data
)
from loguru import logger


def run_backtest(df: DataFrame, strategy: str, backtest_settings: BacktestSettings, enable_time_filter: bool = False) -> DataFrame:
    logger.info("Running backtest...")
    df = df.copy()

    equity = peak_equity = backtest_settings.account.starting_cash
    max_drawdown = 0.0
    trades, current_trade = [], None
    active_day = None

    def close_trade(exit_price, exit_time, reason, row):
        nonlocal equity, peak_equity, max_drawdown, current_trade
        side = current_trade['side']
        entry_price = current_trade['entry_price']
        position_size = get_position_size(equity, backtest_settings)

        multiplier = 1 if reason == 'take_profit' else -1 if reason == 'stop_loss' else (1 if side == 'long' else -1)
        raw_pnl = (
                          exit_price - entry_price) * multiplier if reason == 'eod_close' else backtest_settings.strategy.take_profit if reason == 'take_profit' else backtest_settings.strategy.stop_loss * multiplier
        
        pnl = round(
            (float(raw_pnl) * backtest_settings.account.leverage - float(backtest_settings.account.commission)) * position_size,
            2
        )

        equity += pnl
        drawdown, max_drawdown, peak_equity = update_iteration_data(equity, peak_equity, max_drawdown)

        trades.append({
            'trade_id': current_trade['entry_index'],
            'trading_date': row['trading_date'],
            'side': side,
            'position_size': position_size,
            'entry_time': current_trade['entry_time'],
            'exit_time': exit_time,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'trade_duration': (exit_time - current_trade['entry_time']).total_seconds() / 60 + 5,
            'exit_reason': reason,
            'pnl': pnl,
            'drawdown': drawdown,
            'max_drawdown': max_drawdown
        })
        current_trade = None

    if strategy == "previous_day_breakout":
        for i in range(1, len(df)):
            row = df.iloc[i]
            prev_row = df.iloc[i - 1]

            if current_trade:
                side = current_trade['side']
                entry_price = current_trade['entry_price']

                # Exit: take profit / stop loss
                if side == 'long':
                    if row['low'] <= entry_price - backtest_settings.strategy.stop_loss:
                        close_trade(entry_price - backtest_settings.strategy.stop_loss, row['timestamp'], 'stop_loss', row)
                        if backtest_settings.strategy.trade_until_win:
                            active_day = None
                        continue
                    elif row['high'] >= entry_price + backtest_settings.strategy.take_profit:
                        close_trade(entry_price + backtest_settings.strategy.take_profit, row['timestamp'], 'take_profit', row)
                        if backtest_settings.strategy.trade_until_loss:
                            active_day = None
                        continue
                elif side == 'short':
                    if row['high'] >= entry_price + backtest_settings.strategy.stop_loss:
                        close_trade(entry_price + backtest_settings.strategy.stop_loss, row['timestamp'], 'stop_loss', row)
                        if backtest_settings.strategy.trade_until_win:
                            active_day = None
                        continue
                    elif row['low'] <= entry_price - backtest_settings.strategy.take_profit:
                        close_trade(entry_price - backtest_settings.strategy.take_profit, row['timestamp'], 'take_profit', row)
                        if backtest_settings.strategy.trade_until_loss:
                            active_day = None
                        continue

                # Exit if max holding bars reached
                if backtest_settings.strategy.max_holding_bars:
                    holding_bars = i - current_trade['entry_index']
                    if holding_bars >= backtest_settings.strategy.max_holding_bars:
                        close_trade(row['close'], row['timestamp'], 'max_holding_bars', row)
                        continue

                # Exit at end of day
                if row['trading_date'] != prev_row['trading_date']:
                    close_trade(prev_row['close'], prev_row['timestamp'], 'eod_close', row)
                    continue
            else:
                if row['trading_date'] == active_day:
                    continue

                if enable_time_filter:
                    bar_time = row['timestamp'].time()
                    if not (backtest_settings.strategy.trade_entry_start_time <= bar_time <= backtest_settings.strategy.trade_entry_end_time):
                        continue

                if notna(row['prev_day_high']) and notna(row['prev2_day_high']):
                    if row['close'] - row['prev_day_high'] > 0:
                        if i + 1 < len(df):
                            next_bar = df.iloc[i + 1]
                            current_trade = {
                                'side': 'long',
                                'entry_time': next_bar['timestamp'],
                                'entry_price': next_bar['open'],
                                'entry_index': i + 1,
                            }
                            active_day = row['trading_date']

                if notna(row['prev_day_low']) and notna(row['prev2_day_low']):
                    if row['close'] - row['prev_day_low'] < 0:
                        if i + 1 < len(df):
                            next_bar = df.iloc[i + 1]
                            current_trade = {
                                'side': 'short',
                                'entry_time': next_bar['timestamp'],
                                'entry_price': next_bar['open'],
                                'entry_index': i + 1,
                            }
                            active_day = row['trading_date']
    elif strategy == "compression_breakout_scalp":
        for i in range(1, len(df)):
            row = df.iloc[i]
            prev_row = df.iloc[i - 1]

            if current_trade:
                side = current_trade['side']
                entry_price = current_trade['entry_price']

                # Exit: take profit / stop loss
                if side == 'long':
                    if row['low'] <= float(entry_price) - backtest_settings.strategy.stop_loss:
                        close_trade(float(entry_price) - backtest_settings.strategy.stop_loss, row['timestamp'], 'stop_loss', row)
                        if backtest_settings.strategy.trade_until_win:
                            active_day = None
                        continue
                    elif row['high'] >= float(entry_price) + backtest_settings.strategy.take_profit:
                        close_trade(float(entry_price) + backtest_settings.strategy.take_profit, row['timestamp'], 'take_profit', row)
                        if backtest_settings.strategy.trade_until_loss:
                            active_day = None
                        continue
                elif side == 'short':
                    if row['high'] >= float(entry_price) + backtest_settings.strategy.stop_loss:
                        close_trade(float(entry_price) + backtest_settings.strategy.stop_loss, row['timestamp'], 'stop_loss', row)
                        if backtest_settings.strategy.trade_until_win:
                            active_day = None
                        continue
                    elif row['low'] <= float(entry_price) - backtest_settings.strategy.take_profit:
                        close_trade(float(entry_price) - backtest_settings.strategy.take_profit, row['timestamp'], 'take_profit', row)
                        if backtest_settings.strategy.trade_until_loss:
                            active_day = None
                        continue

                # Exit if max holding bars reached
                if backtest_settings.strategy.max_holding_bars:
                    holding_bars = i - current_trade['entry_index']
                    if holding_bars >= backtest_settings.strategy.max_holding_bars:
                        close_trade(row['close'], row['timestamp'], 'max_holding_bars', row)
                        continue

                # Exit at end of day
                if row['trading_date'] != prev_row['trading_date']:
                    close_trade(prev_row['close'], prev_row['timestamp'], 'eod_close', row)
                    continue
            else:
                if row['trading_date'] == active_day:
                    continue

                if enable_time_filter:
                    bar_time = row['timestamp'].time()
                    if not (backtest_settings.strategy.trade_entry_start_time <= bar_time <= backtest_settings.strategy.trade_entry_end_time):
                        continue

                if row['prev_day_high'] < row['prev2_day_high'] and row['prev_day_low'] > row['prev2_day_low']:
                    if notna(row['prev_day_high']) and notna(row['prev2_day_high']):
                        if row['close'] - row['prev_day_high'] > 0:
                            if i + 1 < len(df):
                                next_bar = df.iloc[i + 1]
                                current_trade = {
                                    'side': 'long',
                                    'entry_time': next_bar['timestamp'],
                                    'entry_price': next_bar['open'],
                                    'entry_index': i + 1,
                                }
                                active_day = row['trading_date']

                    if notna(row['prev_day_low']) and notna(row['prev2_day_low']):
                        if row['close'] - row['prev_day_low'] < 0:
                            if i + 1 < len(df):
                                next_bar = df.iloc[i + 1]
                                current_trade = {
                                    'side': 'short',
                                    'entry_time': next_bar['timestamp'],
                                    'entry_price': next_bar['open'],
                                    'entry_index': i + 1,
                                }
                                active_day = row['trading_date']

    return DataFrame(trades)
