-- SQL script to create unverified_tokens table in existing database
-- Run this in your existing database: psql -U postgres default < create_unverified_tokens_table.sql
-- Or inside psql: \i create_unverified_tokens_table.sql

-- Create unverified_tokens table
CREATE TABLE IF NOT EXISTS unverified_tokens (
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
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_unverified_contract_address ON unverified_tokens(contract_address);
CREATE INDEX IF NOT EXISTS idx_unverified_chain ON unverified_tokens(chain);
CREATE INDEX IF NOT EXISTS idx_unverified_updated_at ON unverified_tokens(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_unverified_market_cap ON unverified_tokens(market_cap_usd DESC);
CREATE INDEX IF NOT EXISTS idx_unverified_price ON unverified_tokens(price_usd DESC);
CREATE INDEX IF NOT EXISTS idx_unverified_view_source ON unverified_tokens(view_source);
CREATE INDEX IF NOT EXISTS idx_unverified_contract_chain ON unverified_tokens(contract_address, chain);
CREATE INDEX IF NOT EXISTS idx_unverified_contract_updated ON unverified_tokens(contract_address, updated_at DESC);

-- Partial index for efficient decimals lookup (only indexes rows with known decimals)
CREATE INDEX IF NOT EXISTS idx_unverified_contract_chain_decimals
ON unverified_tokens(contract_address, chain, decimals, updated_at DESC)
WHERE decimals IS NOT NULL;

-- Create a view for latest metrics per token
CREATE OR REPLACE VIEW latest_unverified_tokens AS
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
FROM unverified_tokens
ORDER BY contract_address, chain, updated_at DESC;

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_unverified_tokens_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for auto-updating updated_at
CREATE TRIGGER update_unverified_tokens_updated_at_trigger
    BEFORE UPDATE ON unverified_tokens
    FOR EACH ROW
    EXECUTE FUNCTION update_unverified_tokens_updated_at();

-- Add comments for documentation
COMMENT ON TABLE unverified_tokens IS 'Stores unverified token metrics from various sources with historical tracking';
COMMENT ON COLUMN unverified_tokens.contract_address IS 'Token contract address (44 chars for Solana, 42 for EVM)';
COMMENT ON COLUMN unverified_tokens.chain IS 'Chain identifier (e.g., solana, ethereum, bsc)';
COMMENT ON COLUMN unverified_tokens.decimals IS 'Token decimals (e.g., 6 for USDC, 18 for ETH)';
COMMENT ON COLUMN unverified_tokens.price_usd IS 'Token price in USD';
COMMENT ON COLUMN unverified_tokens.market_cap_usd IS 'Market capitalization in USD (price * supply)';
COMMENT ON COLUMN unverified_tokens.supply IS 'Circulating supply (total_minted - total_burned)';
COMMENT ON COLUMN unverified_tokens.largest_lp_pool_usd IS 'TVL of largest liquidity pool in USD';
COMMENT ON COLUMN unverified_tokens.source IS 'DEX source of the largest pool (e.g., raydium, orca)';
COMMENT ON COLUMN unverified_tokens.first_tx_date IS 'Date of first transaction (mint or swap)';
COMMENT ON COLUMN unverified_tokens.updated_at IS 'Timestamp of last update';
COMMENT ON COLUMN unverified_tokens.view_source IS 'Source materialized view (e.g., sol_500_swaps_7_days)';

-- Display table info
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE tablename = 'unverified_tokens'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✅ Table unverified_tokens created successfully!';
    RAISE NOTICE 'Table: public.unverified_tokens';
    RAISE NOTICE 'Latest view: public.latest_unverified_tokens';
    RAISE NOTICE 'Indexes created: 9 indexes';
    RAISE NOTICE 'Trigger: auto-update updated_at on UPDATE';
END $$;
