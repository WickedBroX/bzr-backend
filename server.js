// Load environment variables from .env file
require('dotenv').config();

const express = require('express');
const cors = require('cors');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 3001;

// --- Middleware ---
app.use(cors());
app.use(express.json());

// --- Constants ---
const API_KEY = process.env.ETHERSCAN_V2_API_KEY;
const BZR_ADDRESS = process.env.BZR_TOKEN_ADDRESS;
const API_V2_BASE_URL = 'https://api.etherscan.io/v2/api';
console.log(`i API Base URL: ${API_V2_BASE_URL}`);

// [Milestone 2.2] Define all 10 chains
const CHAINS = [
  { id: 1, name: 'Ethereum' },
  { id: 10, name: 'Optimism' },
  { id: 56, name: 'BSC' },
  { id: 137, name: 'Polygon' },
  { id: 324, name: 'zkSync' },
  { id: 5000, name: 'Mantle' },
  { id: 42161, name: 'Arbitrum' },
  { id: 43114, name: 'Avalanche' },
  { id: 8453, name: 'Base' },
  { id: 25, name: 'Cronos' }, // Note: Etherscan V2 list has Cronos as 25
];

// --- Helper Functions ---
/**
 * In-memory cache for our two endpoints.
 * 'info' is cached for 5 minutes.
 * 'transfers' is cached for 2 minutes.
 */
const cache = {
  info: null,
  infoTimestamp: 0,
  transfers: null,
  transfersTimestamp: 0,
};

const FIVE_MINUTES = 5 * 60 * 1000;
const TWO_MINUTES = 2 * 60 * 1000;

/**
 * [Milestone 2.2]
 * Fetches ERC-20 token transfers for a single chain.
 * @param {object} chain - A chain object { id, name }
 * @returns {Promise<Array>} A promise that resolves to an array of transactions.
 */
const fetchTransfersForChain = async (chain) => {
  const params = {
    chainid: chain.id,
    apikey: API_KEY,
    module: 'account',
    action: 'tokentx',
    contractaddress: BZR_ADDRESS,
    page: 1,
    offset: 50, // Get the latest 50 transfers from this chain
    sort: 'desc',
  };

  try {
    const response = await axios.get(API_V2_BASE_URL, { params });
    if (response.data.status === '1') {
      // Add the chain name to each transaction object
      return response.data.result.map(tx => ({
        ...tx,
        chainName: chain.name,
        chainId: chain.id,
      }));
    } else {
      console.warn(`! No transfers found or API error on chain ${chain.name}: ${response.data.message}`);
      return []; // Return empty array on API error (e.g., "No transactions found")
    }
  } catch (error) {
    console.error(`X Failed to fetch transfers for chain ${chain.name}: ${error.message}`);
    return null; // Return null on critical network error
  }
};

// --- API Routes ---

// [Milestone 2.1] - Token Info
app.get('/api/info', async (req, res) => {
  console.log(`[${new Date().toISOString()}] Received request for /api/info`);

  // --- Caching Logic ---
  const now = Date.now();
  if (cache.info && (now - cache.infoTimestamp < FIVE_MINUTES)) {
    console.log('-> Returning cached /api/info data.');
    return res.json(cache.info);
  }
  // --- End Caching Logic ---

  if (!API_KEY || !BZR_ADDRESS) {
    return res.status(500).json({ message: 'Server is missing API_KEY or BZR_ADDRESS' });
  }

  // We only need to get this info from one chain, so we'll use Ethereum (chainid=1)
  const params = {
    chainid: 1, // Ethereum Mainnet
    apikey: API_KEY,
  };

  // We will make two API calls in parallel to get all the info we need

  // Call 1: Get Total Supply
  const supplyParams = {
    ...params,
    module: 'stats',
    action: 'tokensupply',
    contractaddress: BZR_ADDRESS,
  };

  // Call 2: Get token details (name, symbol, decimals) from the last transaction
  const txParams = {
    ...params,
    module: 'account',
    action: 'tokentx',
    contractaddress: BZR_ADDRESS,
    page: 1,
    offset: 1, // We only need one transaction to read the token data
    sort: 'desc',
  };

  try {
    console.log('-> Fetching new /api/info data from Etherscan...');
    // Run both requests in parallel
    const [supplyResponse, txResponse] = await Promise.all([
      axios.get(API_V2_BASE_URL, { params: supplyParams }),
      axios.get(API_V2_BASE_URL, { params: txParams }),
    ]);

    // Check for API errors
    if (supplyResponse.data.status !== '1' || txResponse.data.status !== '1') {
      console.error('Etherscan API Error:', supplyResponse.data.message, txResponse.data.message);
      return res.status(500).json({ 
        message: 'Error from Etherscan API', 
        supplyError: supplyResponse.data.message,
        txError: txResponse.data.message 
      });
    }

    // --- Parse the data ---
    
    // 1. Data from Total Supply call
    const totalSupply = supplyResponse.data.result;

    // 2. Data from Token Transaction call
    const lastTx = txResponse.data.result[0];
    if (!lastTx) {
      return res.status(404).json({ message: 'No transactions found for this token to read info from.' });
    }

    const { tokenName, tokenSymbol, tokenDecimal } = lastTx;
    
    // --- Combine and Send ---
    const tokenInfo = {
      tokenName,
      tokenSymbol,
      tokenDecimal: parseInt(tokenDecimal, 10),
      totalSupply,
      // We add this helper to format the supply on the frontend
      formattedTotalSupply: (BigInt(totalSupply) / BigInt(10 ** parseInt(tokenDecimal, 10))).toString(),
    };

    // Store in cache
    cache.info = tokenInfo;
    cache.infoTimestamp = Date.now();
    console.log('-> Successfully fetched and cached new /api/info data.');

    res.json(tokenInfo);

  } catch (error) {
    console.error('Error in /api/info handler:', error.message);
    res.status(500).json({ message: 'Failed to fetch token info', error: error.message });
  }
});


// [Milestone 2.2] - All Transfers (Implementation)
app.get('/api/transfers', async (req, res) => {
  console.log(`[${new Date().toISOString()}] Received request for /api/transfers`);

  // --- Caching Logic ---
  const now = Date.now();
  if (cache.transfers && (now - cache.transfersTimestamp < TWO_MINUTES)) {
    console.log('-> Returning cached /api/transfers data.');
    return res.json(cache.transfers);
  }
  // --- End Caching Logic ---

  console.log('-> Fetching new /api/transfers data from 10 chains...');

  try {
    // 1. Create an array of promises, one for each chain
    const allPromises = CHAINS.map(chain => fetchTransfersForChain(chain));

    // 2. Run all fetches in parallel and wait for them all to finish
    // We use 'allSettled' so that if one chain fails, the others still return data
    const allResults = await Promise.allSettled(allPromises);

    // 3. Aggregate all successful results
    let allTransfers = [];
    allResults.forEach((result, index) => {
      if (result.status === 'fulfilled' && result.value) {
        // result.value is the array of transactions from fetchTransfersForChain
        allTransfers.push(...result.value);
      } else if (result.status === 'rejected') {
        console.error(`X Critical error fetching from chain ${CHAINS[index].name}: ${result.reason}`);
      }
    });

    console.log(`-> Aggregated ${allTransfers.length} total transfers from all chains.`);

    // 4. Sort the combined list by timestamp (most recent first)
    allTransfers.sort((a, b) => b.timeStamp - a.timeStamp);

    // 5. Cache and send the response
    cache.transfers = allTransfers;
    cache.transfersTimestamp = Date.now();

    res.json(allTransfers);

  } catch (error) {
    console.error('Error in /api/transfers handler:', error.message);
    res.status(500).json({ message: 'Failed to fetch transfers', error: error.message });
  }
});

// [Milestone 4.1] - Admin Panel
// We will add this at the end to manage API keys
// TODO: Build admin routes

// --- Health Check Route ---
// A simple route to make sure the server is alive
app.get('/', (req, res) => {
  res.send('BZR Token Explorer Backend is running! (v3: /api/info and /api/transfers are live)');
});

// --- Start Server ---
app.listen(PORT, () => {
  console.log(`BZR Backend server listening on http://localhost:${PORT}`);
  
  if (!API_KEY) {
    console.warn('---');
    console.warn('WARNING: ETHERSCAN_V2_API_KEY is not set in your .env file.');
    console.warn('The API calls will fail until this is set.');
    console.warn('---');
  } else {
    console.log('Etherscan API key loaded successfully.');
  }

  if (!BZR_ADDRESS) {
    console.warn('WARNING: BZR_TOKEN_ADDRESS is not set. Please set it in .env');
  } else {
    console.log(`Tracking BZR Token: ${BZR_ADDRESS}`);
  }
});