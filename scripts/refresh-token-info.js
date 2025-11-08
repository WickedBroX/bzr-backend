#!/usr/bin/env node

/**
 * Script to fetch and persist token metadata to database
 * This makes /api/info resilient to upstream API failures
 * 
 * Usage:
 *   node scripts/refresh-token-info.js [--force]
 */

require('dotenv').config();
const axios = require('axios');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.TRANSFERS_DATABASE_URL,
});

const BZR_ADDRESS = process.env.BZR_TOKEN_ADDRESS || '0x85Cb098bdcD3Ca929d2cD18Fc7A2669fF0362242';
const API_V2_BASE_URL = process.env.ETHERSCAN_V2_BASE_URL || 'https://api.etherscan.io/v2/api';

// Parse API keys
const API_KEYS_RAW = process.env.ETHERSCAN_V2_API_KEY || '';
const ETHERSCAN_API_KEYS = (API_KEYS_RAW.includes(',')
  ? API_KEYS_RAW.split(',').map((value) => value.trim())
  : [API_KEYS_RAW]).filter((value) => value.length > 0);

let currentKeyIndex = 0;

const getNextApiKey = () => {
  if (ETHERSCAN_API_KEYS.length === 0) {
    return '';
  }
  const key = ETHERSCAN_API_KEYS[currentKeyIndex] || '';
  currentKeyIndex = (currentKeyIndex + 1) % ETHERSCAN_API_KEYS.length;
  return key;
};

async function fetchTokenInfoFromEtherscan(chainId = 137) {
  console.log(`Fetching token info from Etherscan (chain ${chainId})...`);
  
  const params = {
    chainid: chainId,
    apikey: getNextApiKey(),
  };

  // Three parallel API calls
  const supplyParams = {
    ...params,
    module: 'stats',
    action: 'tokensupply',
    contractaddress: BZR_ADDRESS,
  };

  const txParams = {
    ...params,
    module: 'account',
    action: 'tokentx',
    contractaddress: BZR_ADDRESS,
    page: 1,
    offset: 1,
    sort: 'desc',
  };

  const tokenInfoParams = {
    ...params,
    module: 'token',
    action: 'tokeninfo',
    contractaddress: BZR_ADDRESS,
  };

  const [supplyResponse, txResponse, tokenInfoResponse] = await Promise.all([
    axios.get(API_V2_BASE_URL, { params: supplyParams, timeout: 15000 }),
    axios.get(API_V2_BASE_URL, { params: txParams, timeout: 15000 }),
    axios.get(API_V2_BASE_URL, { params: tokenInfoParams, timeout: 15000 }),
  ]);

  // Check for errors
  if (supplyResponse.data.status !== '1' || txResponse.data.status !== '1') {
    throw new Error(`Etherscan API error: ${supplyResponse.data.message || txResponse.data.message}`);
  }

  // Extract data
  const totalSupply = supplyResponse.data.result;
  const lastTx = txResponse.data.result[0];
  
  if (!lastTx) {
    throw new Error('No transactions found to extract token metadata');
  }

  const { tokenName, tokenSymbol, tokenDecimal } = lastTx;

  // Circulating supply (may be null)
  let circulatingSupply = null;
  let formattedCirculatingSupply = null;
  
  if (tokenInfoResponse.data.status === '1' && Array.isArray(tokenInfoResponse.data.result)) {
    const tokenInfoData = tokenInfoResponse.data.result[0];
    if (tokenInfoData && tokenInfoData.circulatingSupply) {
      circulatingSupply = tokenInfoData.circulatingSupply;
      try {
        formattedCirculatingSupply = (BigInt(circulatingSupply) / BigInt(10 ** parseInt(tokenDecimal, 10))).toString();
      } catch (e) {
        console.warn('Could not format circulating supply:', e.message);
      }
    }
  }

  const decimals = parseInt(tokenDecimal, 10);
  const formattedTotalSupply = (BigInt(totalSupply) / BigInt(10 ** decimals)).toString();

  return {
    tokenName,
    tokenSymbol,
    tokenDecimal: decimals,
    totalSupply,
    circulatingSupply,
    formattedTotalSupply,
    formattedCirculatingSupply,
    sourceData: {
      supply: supplyResponse.data,
      tx: txResponse.data,
      tokenInfo: tokenInfoResponse.data,
    },
  };
}

async function persistTokenInfo(tokenInfo, chainId = 137) {
  const query = `
    INSERT INTO token_info (
      contract_address,
      chain_id,
      token_name,
      token_symbol,
      token_decimals,
      total_supply,
      circulating_supply,
      formatted_total_supply,
      formatted_circulating_supply,
      source_chain_id,
      source_data,
      updated_at,
      last_fetch_success,
      last_fetch_error
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), TRUE, NULL)
    ON CONFLICT (contract_address)
    DO UPDATE SET
      chain_id = EXCLUDED.chain_id,
      token_name = EXCLUDED.token_name,
      token_symbol = EXCLUDED.token_symbol,
      token_decimals = EXCLUDED.token_decimals,
      total_supply = EXCLUDED.total_supply,
      circulating_supply = EXCLUDED.circulating_supply,
      formatted_total_supply = EXCLUDED.formatted_total_supply,
      formatted_circulating_supply = EXCLUDED.formatted_circulating_supply,
      source_chain_id = EXCLUDED.source_chain_id,
      source_data = EXCLUDED.source_data,
      updated_at = NOW(),
      last_fetch_success = TRUE,
      last_fetch_error = NULL
    RETURNING *;
  `;

  const values = [
    BZR_ADDRESS.toLowerCase(),
    chainId,
    tokenInfo.tokenName,
    tokenInfo.tokenSymbol,
    tokenInfo.tokenDecimal,
    tokenInfo.totalSupply,
    tokenInfo.circulatingSupply,
    tokenInfo.formattedTotalSupply,
    tokenInfo.formattedCirculatingSupply,
    chainId,
    JSON.stringify(tokenInfo.sourceData),
  ];

  const result = await pool.query(query, values);
  return result.rows[0];
}

async function updateTokenInfoError(error) {
  const query = `
    INSERT INTO token_info (
      contract_address,
      chain_id,
      last_fetch_success,
      last_fetch_error,
      updated_at
    ) VALUES ($1, 137, FALSE, $2, NOW())
    ON CONFLICT (contract_address)
    DO UPDATE SET
      last_fetch_success = FALSE,
      last_fetch_error = EXCLUDED.last_fetch_error,
      updated_at = NOW();
  `;

  await pool.query(query, [BZR_ADDRESS.toLowerCase(), error.message]);
}

async function main() {
  const args = process.argv.slice(2);
  const force = args.includes('--force');

  console.log('🔄 Token Info Refresh Script');
  console.log(`   Token: ${BZR_ADDRESS}`);
  console.log(`   Force: ${force ? 'Yes' : 'No'}`);
  console.log('');

  try {
    // Check if we need to refresh (unless forced)
    if (!force) {
      const checkQuery = `
        SELECT updated_at, last_fetch_success
        FROM token_info
        WHERE contract_address = $1
      `;
      const checkResult = await pool.query(checkQuery, [BZR_ADDRESS.toLowerCase()]);
      
      if (checkResult.rows.length > 0) {
        const lastUpdate = new Date(checkResult.rows[0].updated_at);
        const hoursSinceUpdate = (Date.now() - lastUpdate.getTime()) / (1000 * 60 * 60);
        
        if (hoursSinceUpdate < 1 && checkResult.rows[0].last_fetch_success) {
          console.log(`✅ Token info was updated ${hoursSinceUpdate.toFixed(1)} hours ago. Skipping.`);
          console.log('   Use --force to refresh anyway.');
          process.exit(0);
        }
      }
    }

    // Fetch from Etherscan
    const tokenInfo = await fetchTokenInfoFromEtherscan(137);
    
    console.log('✅ Fetched token info:');
    console.log(`   Name: ${tokenInfo.tokenName}`);
    console.log(`   Symbol: ${tokenInfo.tokenSymbol}`);
    console.log(`   Decimals: ${tokenInfo.tokenDecimal}`);
    console.log(`   Total Supply: ${tokenInfo.formattedTotalSupply}`);
    
    // Save to database
    const saved = await persistTokenInfo(tokenInfo, 137);
    
    console.log('');
    console.log('✅ Token info saved to database');
    console.log(`   Updated at: ${saved.updated_at}`);
    console.log('');
    
  } catch (error) {
    console.error('');
    console.error('❌ Error refreshing token info:', error.message);
    
    // Try to log error to database
    try {
      await updateTokenInfoError(error);
      console.error('   Error logged to database');
    } catch (dbError) {
      console.error('   Could not log error to database:', dbError.message);
    }
    
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
