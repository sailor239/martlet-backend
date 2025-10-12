-- migrate:up
CREATE TABLE strategies (
    id SERIAL PRIMARY KEY,
    identifier VARCHAR(64) UNIQUE NOT NULL,
    name VARCHAR(128) NOT NULL,
    description TEXT,
    enabled BOOLEAN DEFAULT TRUE,
    parameters JSONB,
    category VARCHAR(64),
    timeframe VARCHAR(16),
    version VARCHAR(32) DEFAULT '1.0',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    last_backtested_at TIMESTAMP,
    notes TEXT
);

CREATE INDEX idx_strategies_enabled ON strategies (enabled);
CREATE INDEX idx_strategies_category ON strategies (category);
CREATE INDEX idx_strategies_identifier ON strategies (identifier);


-- migrate:down
DROP INDEX IF EXISTS idx_strategies_enabled;
DROP INDEX IF EXISTS idx_strategies_category;
DROP INDEX IF EXISTS idx_strategies_identifier;
DROP TABLE strategies;
