-- migrate:up
CREATE TABLE backtest_results (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    timeframe VARCHAR(20) NOT NULL,
    trading_date DATE NOT NULL,
    equity NUMERIC NOT NULL,
    pnl NUMERIC NOT NULL,
    strategy VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(ticker, timeframe, trading_date, strategy)
);


-- migrate:down
DROP TABLE backtest_results;
