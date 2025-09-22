-- migrate:up
CREATE TABLE trades (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    direction VARCHAR(10) NOT NULL,
    entry_price DECIMAL(12, 5) NOT NULL,
    exit_price DECIMAL(12, 5) NOT NULL,
    size DOUBLE PRECISION NOT NULL,
    entry_time TIMESTAMPTZ NOT NULL,
    exit_time TIMESTAMPTZ NOT NULL,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_trades_ticker ON trades (ticker);

-- migrate:down
DROP TABLE trades;
