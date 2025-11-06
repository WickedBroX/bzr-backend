#!/usr/bin/env node

/**
 * Historical Backfill Script for BZR Token Transfers
 * 
 * This script fetches ALL historical transfer events from block 0 to the current
 * ingester cursor position using Etherscan API V2 PRO endpoints.
 * 
 * Features:
 * - Uses PRO plan with 10,000 records per request (vs regular 100)
 * - Load balances across 3 API keys
 * - Tracks progress and resumes from failures
 * - Batch inserts with ON CONFLICT DO NOTHING to avoid duplicates
 * - Supports all 10 chains
 * 
 * Usage:
 *   node scripts/backfill-historical.js [chainId]
 *   
 * Examples:
 *   node scripts/backfill-historical.js 137    # Backfill Polygon only
 *   node scripts/backfill-historical.js all    # Backfill all chains
 */

require('dotenv').config();
const axios = require('axios');
const { Pool } = require('pg');

// Database connection (use connection string from .env)
const pool = new Pool({
  connectionString: process.env.TRANSFERS_DATABASE_URL,
});

// Configuration
const BZR_ADDRESS = process.env.BZR_TOKEN_ADDRESS || '0x85Cb098bdcD3Ca929d2cD18Fc7A2669fF0362242';
const API_KEYS = (process.env.ETHERSCAN_V2_API_KEY || '').split(',').filter(Boolean);
const ETHERSCAN_BASE_URL = process.env.ETHERSCAN_V2_BASE_URL || 'https://api.etherscan.io/v2/api';
const PRO_PAGE_SIZE = 10000; // PRO plan allows 10K records per request
const BATCH_INSERT_SIZE = 1000; // Insert 1000 records at a time
const RATE_LIMIT_DELAY = 350; // 350ms = 2.85 requests/sec (safe with 3 keys = ~8.5 req/sec total)

let currentKeyIndex = 0;

const CHAINS = [
  { id: 1, name: 'Ethereum', provider: 'etherscan' },
  { id: 10, name: 'Optimism', provider: 'etherscan' },
  { id: 56, name: 'BSC', provider: 'etherscan' },
  { id: 137, name: 'Polygon', provider: 'etherscan' },
  { id: 324, name: 'zkSync Era', provider: 'etherscan' },
  { id: 5000, name: 'Mantle', provider: 'etherscan' },
  { id: 8453, name: 'Base', provider: 'etherscan' },
  { id: 42161, name: 'Arbitrum', provider: 'etherscan' },
  { id: 43114, name: 'Avalanche', provider: 'etherscan' },
  { id: 25, name: 'Cronos', provider: 'cronos' }, // Skip for now - uses different API
];

// Get next API key for load balancing
function getNextApiKey() {
  const key = API_KEYS[currentKeyIndex];
  currentKeyIndex = (currentKeyIndex + 1) % API_KEYS.length;
  return key;
}

// Sleep helper
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Fetch current ingester cursor for a chain
async function getCurrentCursor(chainId) {
  const result = await pool.query(
    'SELECT last_block_number FROM transfer_ingest_cursors WHERE chain_id = $1',
    [chainId]
  );
  
  if (result.rows.length === 0) {
    return null; // No cursor yet, chain not ingested
  }
  
  return result.rows[0].last_block_number;
}

// Get backfill progress (if resuming)
async function getBackfillProgress(chainId) {
  const result = await pool.query(
    `SELECT last_backfilled_block 
     FROM transfer_backfill_progress 
     WHERE chain_id = $1`,
    [chainId]
  );
  
  if (result.rows.length === 0) {
    return 0; // Start from block 0
  }
  
  return result.rows[0].last_backfilled_block;
}

// Update backfill progress
async function updateBackfillProgress(chainId, blockNumber, totalFetched) {
  await pool.query(
    `INSERT INTO transfer_backfill_progress (chain_id, last_backfilled_block, total_fetched, updated_at)
     VALUES ($1, $2, $3, NOW())
     ON CONFLICT (chain_id) 
     DO UPDATE SET 
       last_backfilled_block = $2,
       total_fetched = transfer_backfill_progress.total_fetched + $3,
       updated_at = NOW()`,
    [chainId, blockNumber, totalFetched]
  );
}

// Fetch transfers from Etherscan API V2 PRO
async function fetchTransfersFromEtherscan(chainId, startBlock, endBlock, page = 1) {
  const params = {
    module: 'account',
    action: 'tokentx',
    contractaddress: BZR_ADDRESS,
    chainid: chainId,
    startblock: startBlock,
    endblock: endBlock,
    page: page,
    offset: PRO_PAGE_SIZE,
    sort: 'asc',
    apikey: getNextApiKey(),
  };

  try {
    const response = await axios.get(ETHERSCAN_BASE_URL, { 
      params,
      timeout: 30000, // 30 second timeout
    });
    
    const payload = response.data || {};
    
    if (payload.status === '1' && Array.isArray(payload.result)) {
      return payload.result;
    }
    
    if (payload.status === '0') {
      const message = String(payload.message || payload.result || '').toLowerCase();
      
      // No records is OK, means we're done
      if (message.includes('no transactions') || message.includes('no records found')) {
        return [];
      }
      
      const errorDetails = JSON.stringify(payload, null, 2);
      throw new Error(`Etherscan API error:\n${errorDetails}`);
    }
    
    throw new Error(`Unexpected response: ${JSON.stringify(payload)}`);
  } catch (error) {
    if (error.response?.status === 429) {
      console.log('  ⚠️  Rate limit hit (HTTP 429), waiting 2 seconds...');
      await sleep(2000);
      return fetchTransfersFromEtherscan(chainId, startBlock, endBlock, page);
    }
    
    // Check if error message contains rate limit info
    if (error.message && error.message.includes('rate limit')) {
      console.log('  ⚠️  Rate limit detected, waiting 2 seconds...');
      await sleep(2000);
      return fetchTransfersFromEtherscan(chainId, startBlock, endBlock, page);
    }
    
    throw error;
  }
}

// Batch insert transfers into database
async function batchInsertTransfers(chainId, transfers) {
  if (transfers.length === 0) return 0;

  const client = await pool.connect();
  let inserted = 0;

  try {
    await client.query('BEGIN');

    // Process in batches
    for (let i = 0; i < transfers.length; i += BATCH_INSERT_SIZE) {
      const batch = transfers.slice(i, i + BATCH_INSERT_SIZE);
      
      const values = [];
      const placeholders = [];
      
      batch.forEach((transfer, idx) => {
        const offset = idx * 10;
        placeholders.push(
          `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10})`
        );
        
        values.push(
          chainId,
          parseInt(transfer.blockNumber),
          transfer.hash,
          parseInt(transfer.transactionIndex || 0),
          new Date(parseInt(transfer.timeStamp) * 1000),
          transfer.from.toLowerCase(),
          transfer.to.toLowerCase(),
          transfer.value,
          transfer.input?.substring(0, 10) || null,
          JSON.stringify(transfer)
        );
      });

      const query = `
        INSERT INTO transfer_events 
        (chain_id, block_number, tx_hash, log_index, time_stamp, from_address, to_address, value, method_id, payload)
        VALUES ${placeholders.join(', ')}
        ON CONFLICT (chain_id, tx_hash, log_index) DO NOTHING
      `;

      const result = await client.query(query, values);
      inserted += result.rowCount;
    }

    await client.query('COMMIT');
    return inserted;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

// Main backfill function for a single chain
async function backfillChain(chainId, chainName) {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`🔄 Starting backfill for ${chainName} (Chain ID: ${chainId})`);
  console.log(`${'='.repeat(60)}\n`);

  // Get current cursor (where ingester is at)
  const currentCursor = await getCurrentCursor(chainId);
  
  if (!currentCursor) {
    console.log(`⚠️  No ingester cursor found for ${chainName}. Skipping (chain not yet ingested).`);
    return;
  }

  // Get backfill progress (where we left off, if resuming)
  const lastBackfilledBlock = await getBackfillProgress(chainId);
  
  // For Polygon, token was deployed around October 2024 (block ~60M)
  // Start from a safe earlier block if no progress exists
  const safeStartBlock = chainId === 137 ? 60000000 : 0;
  const startBlock = lastBackfilledBlock > 0 ? lastBackfilledBlock + 1 : safeStartBlock;
  const endBlock = currentCursor - 1; // Don't overlap with ingester
  
  if (startBlock > endBlock) {
    console.log(`✅ ${chainName} already fully backfilled (blocks 0 to ${currentCursor - 1})`);
    return;
  }

  console.log(`📊 Backfill range: Block ${startBlock} to ${endBlock}`);
  console.log(`   Current ingester cursor: ${currentCursor}`);
  console.log(`   Blocks to backfill: ${endBlock - startBlock + 1}\n`);

  let currentBlock = startBlock;
  let totalFetched = 0;
  let totalInserted = 0;
  const startTime = Date.now();

  // Fetch in chunks (Etherscan limits to 10K results per request)
  // With PRO, we can get 10K records per page
  // Use smaller chunks to avoid API limits
  while (currentBlock <= endBlock) {
    const chunkEnd = Math.min(endBlock, currentBlock + 99999); // 100K block chunks
    
    console.log(`  📥 Fetching blocks ${currentBlock} to ${chunkEnd}...`);
    
    let page = 1;
    let hasMore = true;
    
    while (hasMore) {
      try {
        const transfers = await fetchTransfersFromEtherscan(chainId, currentBlock, chunkEnd, page);
        
        if (transfers.length === 0) {
          hasMore = false;
          break;
        }

        totalFetched += transfers.length;
        
        // Insert into database
        const inserted = await batchInsertTransfers(chainId, transfers);
        totalInserted += inserted;
        
        console.log(`     Page ${page}: Fetched ${transfers.length}, Inserted ${inserted} (${totalInserted} total)`);
        
        // Update progress after each successful batch
        const lastBlock = Math.max(...transfers.map(t => parseInt(t.blockNumber)));
        await updateBackfillProgress(chainId, lastBlock, inserted);
        
        // If we got less than 10K, we're done with this chunk
        if (transfers.length < PRO_PAGE_SIZE) {
          hasMore = false;
        } else {
          page++;
        }
        
        // Rate limiting
        await sleep(RATE_LIMIT_DELAY);
      } catch (error) {
        console.error(`  ❌ Error fetching blocks ${currentBlock}-${chunkEnd}, page ${page}:`);
        console.error(`     ${error.message}`);
        
        // If it's a "no transactions" error, just move to next chunk
        if (error.message.includes('no transactions') || error.message.includes('no records')) {
          console.log(`     ℹ️  No transactions in this range, continuing...`);
          hasMore = false;
          break;
        }
        
        throw error;
      }
    }
    
    currentBlock = chunkEnd + 1;
  }

  const duration = ((Date.now() - startTime) / 1000).toFixed(1);
  const blocksProcessed = endBlock - startBlock + 1;
  const rate = (blocksProcessed / parseFloat(duration)).toFixed(0);

  console.log(`\n✅ ${chainName} backfill complete!`);
  console.log(`   Total fetched: ${totalFetched}`);
  console.log(`   Total inserted: ${totalInserted}`);
  console.log(`   Duration: ${duration}s`);
  console.log(`   Rate: ${rate} blocks/sec`);
}

// Main execution
async function main() {
  const args = process.argv.slice(2);
  const targetChainArg = args[0] || 'all';

  console.log('\n🚀 BZR Historical Backfill Tool');
  console.log(`   Using ${API_KEYS.length} API keys for load balancing`);
  console.log(`   PRO page size: ${PRO_PAGE_SIZE} records/request`);
  console.log(`   Rate limit: ${1000 / RATE_LIMIT_DELAY} req/sec\n`);

  // Create progress tracking table if it doesn't exist
  await pool.query(`
    CREATE TABLE IF NOT EXISTS transfer_backfill_progress (
      chain_id INTEGER PRIMARY KEY,
      last_backfilled_block BIGINT NOT NULL,
      total_fetched INTEGER DEFAULT 0,
      updated_at TIMESTAMP WITH TIME ZONE NOT NULL
    )
  `);

  if (targetChainArg === 'all') {
    // Backfill all chains (except Cronos for now)
    const chainsToBackfill = CHAINS.filter(c => c.provider === 'etherscan');
    
    for (const chain of chainsToBackfill) {
      try {
        await backfillChain(chain.id, chain.name);
      } catch (error) {
        console.error(`\n❌ Failed to backfill ${chain.name}:`, error.message);
        console.error('   Continuing with next chain...\n');
      }
    }
  } else {
    // Backfill specific chain
    const chainId = parseInt(targetChainArg);
    const chain = CHAINS.find(c => c.id === chainId);
    
    if (!chain) {
      console.error(`❌ Unknown chain ID: ${chainId}`);
      process.exit(1);
    }
    
    if (chain.provider === 'cronos') {
      console.error(`❌ Cronos backfill not yet implemented (uses different API)`);
      process.exit(1);
    }
    
    await backfillChain(chain.id, chain.name);
  }

  console.log('\n' + '='.repeat(60));
  console.log('🎉 Backfill operation complete!');
  console.log('='.repeat(60) + '\n');

  await pool.end();
  process.exit(0);
}

// Run
main().catch(error => {
  console.error('\n💥 Fatal error:', error);
  pool.end();
  process.exit(1);
});
