-- PostgreSQL initialization script
-- Creates schema and tables for token metrics storage

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS token_data;

-- Set search path
SET search_path TO token_data, public;

-- Create token_metrics table
CREATE TABLE IF NOT EXISTS token_data.token_metrics (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,

    -- Token identification
    contract_address VARCHAR(48) NOT NULL,
    chain VARCHAR(50) NOT NULL,
    decimals INTEGER,

    -- Token metadata
    symbol VARCHAR(20),
    name VARCHAR(255),

    -- Price and market data
    price_usd DOUBLE PRECISION DEFAULT 0,
    market_cap_usd DOUBLE PRECISION DEFAULT 0,

    -- Supply data
    supply DOUBLE PRECISION DEFAULT 0,
    burned DOUBLE PRECISION DEFAULT 0,
    total_minted BIGINT DEFAULT 0,
    total_burned BIGINT DEFAULT 0,

    -- Liquidity data
    largest_lp_pool_usd DOUBLE PRECISION DEFAULT 0,
    source VARCHAR(100),

    -- Temporal data
    first_tx_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- View source metadata (which materialized view this came from)
    view_source VARCHAR(100)

    -- No unique constraint - allow multiple records per token for historical tracking
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_contract_address ON token_data.token_metrics(contract_address);
CREATE INDEX IF NOT EXISTS idx_chain ON token_data.token_metrics(chain);
CREATE INDEX IF NOT EXISTS idx_updated_at ON token_data.token_metrics(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_cap ON token_data.token_metrics(market_cap_usd DESC);
CREATE INDEX IF NOT EXISTS idx_price ON token_data.token_metrics(price_usd DESC);
CREATE INDEX IF NOT EXISTS idx_view_source ON token_data.token_metrics(view_source);
CREATE INDEX IF NOT EXISTS idx_contract_chain ON token_data.token_metrics(contract_address, chain);

-- Create composite index for common queries
CREATE INDEX IF NOT EXISTS idx_contract_updated ON token_data.token_metrics(contract_address, updated_at DESC);

-- Partial index for efficient decimals lookup (only indexes rows with known decimals)
CREATE INDEX IF NOT EXISTS idx_contract_chain_decimals
ON token_data.token_metrics(contract_address, chain, decimals, updated_at DESC)
WHERE decimals IS NOT NULL;

-- Create a view for latest metrics per token
CREATE OR REPLACE VIEW token_data.latest_token_metrics AS
SELECT DISTINCT ON (contract_address, chain)
    id,
    contract_address,
    chain,
    decimals,
    symbol,
    name,
    price_usd,
    market_cap_usd,
    supply,
    burned,
    total_minted,
    total_burned,
    largest_lp_pool_usd,
    source,
    first_tx_date,
    created_at,
    updated_at,
    view_source
FROM token_data.token_metrics
ORDER BY contract_address, chain, updated_at DESC;

-- Create materialized view for top tokens by market cap
CREATE MATERIALIZED VIEW IF NOT EXISTS token_data.top_tokens_by_market_cap AS
SELECT DISTINCT ON (contract_address)
    contract_address,
    chain,
    decimals,
    symbol,
    price_usd,
    market_cap_usd,
    supply,
    largest_lp_pool_usd,
    source,
    updated_at
FROM token_data.token_metrics
WHERE market_cap_usd > 0
ORDER BY contract_address, updated_at DESC, market_cap_usd DESC
LIMIT 10000;

CREATE INDEX IF NOT EXISTS idx_top_tokens_market_cap
ON token_data.top_tokens_by_market_cap(market_cap_usd DESC);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION token_data.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for auto-updating updated_at
CREATE TRIGGER update_token_metrics_updated_at
    BEFORE UPDATE ON token_data.token_metrics
    FOR EACH ROW
    EXECUTE FUNCTION token_data.update_updated_at_column();

-- Grant permissions (adjust user as needed)
GRANT USAGE ON SCHEMA token_data TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA token_data TO PUBLIC;
-- Note: Views are already covered by the ALL TABLES grant above
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA token_data TO PUBLIC;

-- Add comments for documentation
COMMENT ON TABLE token_data.token_metrics IS 'Stores token metrics from various sources with historical tracking';
COMMENT ON COLUMN token_data.token_metrics.contract_address IS 'Token contract address (44 chars for Solana, 42 for EVM)';
COMMENT ON COLUMN token_data.token_metrics.chain IS 'Chain identifier (e.g., solana, ethereum, bsc)';
COMMENT ON COLUMN token_data.token_metrics.decimals IS 'Token decimals (e.g., 6 for USDC, 18 for ETH)';
COMMENT ON COLUMN token_data.token_metrics.price_usd IS 'Token price in USD';
COMMENT ON COLUMN token_data.token_metrics.market_cap_usd IS 'Market capitalization in USD (price * supply)';
COMMENT ON COLUMN token_data.token_metrics.supply IS 'Circulating supply (total_minted - total_burned)';
COMMENT ON COLUMN token_data.token_metrics.largest_lp_pool_usd IS 'TVL of largest liquidity pool in USD';
COMMENT ON COLUMN token_data.token_metrics.source IS 'DEX source of the largest pool (e.g., raydium, orca)';
COMMENT ON COLUMN token_data.token_metrics.first_tx_date IS 'Date of first transaction (mint or swap)';
COMMENT ON COLUMN token_data.token_metrics.updated_at IS 'Timestamp of last update';
COMMENT ON COLUMN token_data.token_metrics.view_source IS 'Source materialized view (e.g., sol_500_swaps_7_days)';

-- Create admin user (optional - for production)
-- CREATE USER token_admin WITH PASSWORD 'your_secure_password';
-- GRANT ALL PRIVILEGES ON SCHEMA token_data TO token_admin;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA token_data TO token_admin;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA token_data TO token_admin;

-- Display table info
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'token_data'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Success message
DO $$
BEGIN
    RAISE NOTICE 'Token Data schema initialized successfully!';
    RAISE NOTICE 'Schema: token_data';
    RAISE NOTICE 'Main table: token_data.token_metrics';
    RAISE NOTICE 'Latest view: token_data.latest_token_metrics';
END $$;
