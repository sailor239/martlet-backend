-- migrate:up
CREATE TABLE IF NOT EXISTS market_snapshot (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    open DECIMAL(12, 5) NOT NULL,
    high DECIMAL(12, 5) NOT NULL,
    low DECIMAL(12, 5) NOT NULL,
    close DECIMAL(12, 5) NOT NULL,
    volume BIGINT DEFAULT 0,

    -- Derived / frequently used features
    trading_date DATE NOT NULL,
    ema20 DECIMAL(12, 5),
    prev_day_high DECIMAL(12, 5),
    prev_day_low DECIMAL(12, 5),
    prev2_day_high DECIMAL(12, 5),
    prev2_day_low DECIMAL(12, 5),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(ticker, timeframe, timestamp)
);

-- Trigger function to auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger on UPDATE
CREATE TRIGGER trg_update_updated_at
BEFORE UPDATE ON market_snapshot
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- migrate:down
DROP TABLE IF EXISTS market_snapshot CASCADE;
DROP FUNCTION IF EXISTS update_updated_at_column CASCADE;
