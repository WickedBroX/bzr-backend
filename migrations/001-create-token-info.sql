-- Migration: Create token_info table for persistent token metadata
-- This eliminates dependency on live Etherscan calls for token basic info

CREATE TABLE IF NOT EXISTS token_info (
  contract_address TEXT PRIMARY KEY,
  chain_id INTEGER NOT NULL,
  token_name TEXT NOT NULL,
  token_symbol TEXT NOT NULL,
  token_decimals INTEGER NOT NULL,
  total_supply TEXT,
  circulating_supply TEXT,
  formatted_total_supply TEXT,
  formatted_circulating_supply TEXT,
  source_chain_id INTEGER,
  source_data JSONB,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  last_fetch_success BOOLEAN DEFAULT TRUE,
  last_fetch_error TEXT
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_token_info_contract ON token_info(contract_address);
CREATE INDEX IF NOT EXISTS idx_token_info_updated ON token_info(updated_at DESC);

-- Comments for documentation
COMMENT ON TABLE token_info IS 'Persistent storage for token metadata to reduce Etherscan API dependency';
COMMENT ON COLUMN token_info.source_chain_id IS 'Chain ID where metadata was fetched from (e.g., 137 for Polygon)';
COMMENT ON COLUMN token_info.source_data IS 'Raw upstream response for debugging';
