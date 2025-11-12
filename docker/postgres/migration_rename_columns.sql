-- Migration script: Rename columns and add decimals field
-- This script updates existing token_metrics table to new schema
-- Run this on existing databases before deploying new code

BEGIN;

-- Step 1: Add decimals column if it doesn't exist
ALTER TABLE token_data.token_metrics
ADD COLUMN IF NOT EXISTS decimals INTEGER;

-- Step 2: Rename token_address to contract_address
ALTER TABLE token_data.token_metrics
RENAME COLUMN token_address TO contract_address;

-- Step 3: Rename blockchain to chain
ALTER TABLE token_data.token_metrics
RENAME COLUMN blockchain TO chain;

-- Step 4: Drop old indexes
DROP INDEX IF EXISTS token_data.idx_token_address;
DROP INDEX IF EXISTS token_data.idx_blockchain;
DROP INDEX IF EXISTS token_data.idx_token_blockchain;
DROP INDEX IF EXISTS token_data.idx_token_updated;

-- Step 5: Create new indexes with updated column names
CREATE INDEX IF NOT EXISTS idx_contract_address ON token_data.token_metrics(contract_address);
CREATE INDEX IF NOT EXISTS idx_chain ON token_data.token_metrics(chain);
CREATE INDEX IF NOT EXISTS idx_contract_chain ON token_data.token_metrics(contract_address, chain);
CREATE INDEX IF NOT EXISTS idx_contract_updated ON token_data.token_metrics(contract_address, updated_at DESC);

-- Step 6: Recreate views with new column names
DROP VIEW IF EXISTS token_data.latest_token_metrics CASCADE;

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

-- Step 7: Recreate materialized view
DROP MATERIALIZED VIEW IF EXISTS token_data.top_tokens_by_market_cap;

CREATE MATERIALIZED VIEW token_data.top_tokens_by_market_cap AS
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

-- Step 8: Update comments
COMMENT ON COLUMN token_data.token_metrics.contract_address IS 'Token contract address (44 chars for Solana, 42 for EVM)';
COMMENT ON COLUMN token_data.token_metrics.chain IS 'Chain identifier (e.g., solana, ethereum, bsc)';
COMMENT ON COLUMN token_data.token_metrics.decimals IS 'Token decimals (e.g., 6 for USDC, 18 for ETH)';

COMMIT;

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✅ Migration completed successfully!';
    RAISE NOTICE 'Renamed: token_address → contract_address';
    RAISE NOTICE 'Renamed: blockchain → chain';
    RAISE NOTICE 'Added: decimals column';
    RAISE NOTICE 'Updated: all indexes and views';
END $$;
