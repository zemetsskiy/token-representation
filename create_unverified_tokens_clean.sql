-- Clean SQL script to create unverified_tokens table with minimal required fields
-- Run: psql -U postgres default < create_unverified_tokens_clean.sql

-- Drop existing table if needed (uncomment if recreating)
-- DROP TABLE IF EXISTS unverified_tokens CASCADE;

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
    supply DOUBLE PRECISION DEFAULT 0,

    -- Liquidity data
    largest_lp_pool_usd DOUBLE PRECISION DEFAULT 0,

    -- Temporal data
    first_tx_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Metadata
    view_source VARCHAR(100),

    -- Unique constraint: one record per token per chain
    CONSTRAINT unique_token_chain UNIQUE (contract_address, chain)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_unverified_contract_address ON unverified_tokens(contract_address);
CREATE INDEX IF NOT EXISTS idx_unverified_chain ON unverified_tokens(chain);
CREATE INDEX IF NOT EXISTS idx_unverified_updated_at ON unverified_tokens(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_unverified_market_cap ON unverified_tokens(market_cap_usd DESC);
CREATE INDEX IF NOT EXISTS idx_unverified_price ON unverified_tokens(price_usd DESC);
CREATE INDEX IF NOT EXISTS idx_unverified_contract_chain ON unverified_tokens(contract_address, chain);

-- Partial index for efficient decimals lookup
CREATE INDEX IF NOT EXISTS idx_unverified_contract_chain_decimals
ON unverified_tokens(contract_address, chain, decimals, updated_at DESC)
WHERE decimals IS NOT NULL;

-- View for latest metrics per token
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
    largest_lp_pool_usd,
    first_tx_date,
    created_at,
    updated_at,
    view_source
FROM unverified_tokens
ORDER BY contract_address, chain, updated_at DESC;

-- Trigger function to auto-update updated_at
CREATE OR REPLACE FUNCTION update_unverified_tokens_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for auto-updating updated_at
CREATE TRIGGER update_unverified_tokens_updated_at_trigger
    BEFORE UPDATE ON unverified_tokens
    FOR EACH ROW
    EXECUTE FUNCTION update_unverified_tokens_updated_at();

-- Comments
COMMENT ON TABLE unverified_tokens IS 'Unverified token metrics with UPSERT logic: one record per (contract_address, chain). On conflict, preserves decimals and first_tx_date, updates all other fields.';
COMMENT ON COLUMN unverified_tokens.contract_address IS 'Token contract address (unique per chain)';
COMMENT ON COLUMN unverified_tokens.chain IS 'Chain identifier (solana, ethereum, bsc)';
COMMENT ON COLUMN unverified_tokens.decimals IS 'Token decimals (preserved on updates, only set once)';
COMMENT ON COLUMN unverified_tokens.first_tx_date IS 'Date of first transaction (preserved on updates, only set once)';
COMMENT ON COLUMN unverified_tokens.supply IS 'Circulating supply (normalized by decimals, updated on each run)';
COMMENT ON COLUMN unverified_tokens.largest_lp_pool_usd IS 'Largest liquidity pool TVL in USD (updated on each run)';

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✅ Table unverified_tokens created!';
    RAISE NOTICE 'Fields: 14 (removed burned, total_minted, total_burned, source)';
END $$;
