// Load environment variables from .env file
require('dotenv').config();

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { promises } = require('dns');

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

// --- Helper Function ---
/**
 * A simple in-memory cache to avoid hitting the API rate limit for static data.
 * The 'info' data will be cached for 5 minutes.
 */
const cache = {
  info: null,
  infoTimestamp: 0,
};

const FIVE_MINUTES = 5 * 60 * 1000; // 5 minutes in milliseconds

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


// [Milestone 2.2] - All Transfers
app.get('/api/transfers', (req, res) => {
  console.log(`[${new Date().toISOString()}] Received request for /api/transfers`);
  // TODO: Build this logic after /api/info
  res.status(501).json({ message: 'Milestone 2.2: /api/transfers not implemented yet' });
});

// [Milestone 4.1] - Admin Panel
// We will add this at the end to manage API keys
// TODO: Build admin routes

// --- Health Check Route ---
// A simple route to make sure the server is alive
app.get('/', (req, res) => {
  res.send('BZR Token Explorer Backend is running! (v2: /api/info is live)');
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