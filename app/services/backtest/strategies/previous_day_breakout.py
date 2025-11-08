from pandas import notna
from app.services.backtest.core import BacktestEngine


def previous_day_breakout(engine: BacktestEngine, df, enable_time_filter=False):
    settings = engine.settings

    for i in range(1, len(df)):
        row, prev_row = df.iloc[i], df.iloc[i - 1]

        # --- Exit conditions ---
        if engine.current_trade:
            side = engine.current_trade["side"]
            entry_price = engine.current_trade["entry_price"]

            if side == "long":
                if row["low"] <= entry_price - settings.strategy.stop_loss:
                    engine.close_trade(entry_price - settings.strategy.stop_loss, row["timestamp"], "stop_loss", row)
                    if settings.strategy.trade_until_win:
                        engine.active_day = None
                    continue
                elif row["high"] >= entry_price + settings.strategy.take_profit:
                    engine.close_trade(entry_price + settings.strategy.take_profit, row["timestamp"], "take_profit", row)
                    if settings.strategy.trade_until_loss:
                        engine.active_day = None
                    continue
            elif side == "short":
                if row["high"] >= entry_price + settings.strategy.stop_loss:
                    engine.close_trade(entry_price + settings.strategy.stop_loss, row["timestamp"], "stop_loss", row)
                    if settings.strategy.trade_until_win:
                        engine.active_day = None
                    continue
                elif row["low"] <= entry_price - settings.strategy.take_profit:
                    engine.close_trade(entry_price - settings.strategy.take_profit, row["timestamp"], "take_profit", row)
                    if settings.strategy.trade_until_loss:
                        engine.active_day = None
                    continue

            if row["trading_date"] != prev_row["trading_date"]:
                engine.close_trade(prev_row["close"], prev_row["timestamp"], "eod_close", row)
                continue

        # --- Entry conditions ---
        else:
            if engine.active_day == row["trading_date"]:
                continue

            if enable_time_filter:
                bar_time = row["timestamp"].time()
                if not (settings.strategy.trade_entry_start_time <= bar_time <= settings.strategy.trade_entry_end_time):
                    continue

            if notna(row["prev_day_high"]):
                if row["close"] > row["prev_day_high"]:
                    next_bar = df.iloc[i + 1] if i + 1 < len(df) else None
                    if next_bar is not None:
                        engine.open_trade("long", next_bar["open"], next_bar["timestamp"], i + 1, row["trading_date"])

            if notna(row["prev_day_low"]):
                if row["close"] < row["prev_day_low"]:
                    next_bar = df.iloc[i + 1] if i + 1 < len(df) else None
                    if next_bar is not None:
                        engine.open_trade("short", next_bar["open"], next_bar["timestamp"], i + 1, row["trading_date"])

    return engine.trades
