-- Migration: Fix unique constraint to allow duplicates in same batch
-- Run this on existing database to fix the constraint issue

\c token_metrics

-- Drop the old constraint
ALTER TABLE token_data.token_metrics
DROP CONSTRAINT IF EXISTS unique_token_per_update;

-- Don't add new constraint - allow multiple records per token with same timestamp
-- This enables historical tracking where multiple updates can happen at the same time

-- Add index for better query performance
CREATE INDEX IF NOT EXISTS idx_token_updated_desc
ON token_data.token_metrics(token_address, blockchain, updated_at DESC);

-- Show result
\d token_data.token_metrics

SELECT 'Migration completed successfully!' as status;
