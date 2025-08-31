-- migrate:up
CREATE TABLE IF NOT EXISTS market_snapshot (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    open DECIMAL(12, 5) NOT NULL,
    high DECIMAL(12, 5) NOT NULL,
    low DECIMAL(12, 5) NOT NULL,
    close DECIMAL(12, 5) NOT NULL,
    volume BIGINT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(ticker, timeframe, timestamp)
);

-- migrate:down
DROP TABLE IF EXISTS market_snapshot;
