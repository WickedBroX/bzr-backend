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
const MAX_CONCURRENT_REQUESTS = Number(process.env.ETHERSCAN_CONCURRENCY || 3);
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
  transfersChainStatus: [],
  stats: null, // [Milestone 2.3] Cache for stats
  statsTimestamp: 0,
  tokenPrice: null,
  tokenPriceTimestamp: 0,
  finality: null,
  finalityTimestamp: 0,
};

const FIVE_MINUTES = 5 * 60 * 1000;

const TWO_MINUTES = 2 * 60 * 1000; // Transfers and Stats are cached for 2 mins
const CACHE_WARM_INTERVAL_MS = Number(process.env.CACHE_WARM_INTERVAL_MS || 90_000);
const TOKEN_PRICE_TTL_MS = Number(process.env.TOKEN_PRICE_TTL_MS || 60_000);
const FINALITY_TTL_MS = Number(process.env.FINALITY_TTL_MS || 30_000);

const toNumericTimestamp = (timestamp) => {
  const parsed = Number(timestamp);
  return Number.isFinite(parsed) ? parsed : 0;
};

const respondUpstreamFailure = (res, message, details = {}) => {
  return res.status(502).json({
    message,
    upstream: 'etherscan',
    ...details,
  });
};

const isProOnlyResponse = (payload = {}) => {
  const segments = [];
  if (typeof payload === 'string') {
    segments.push(payload);
  } else {
    if (payload?.message) segments.push(payload.message);
    if (payload?.result) segments.push(typeof payload.result === 'string' ? payload.result : JSON.stringify(payload.result));
  }

  const combined = segments.join(' ').toLowerCase();
  return combined.includes('pro') && (combined.includes('endpoint') || combined.includes('plan'));
};

const mapWithConcurrency = async (items, limit, task) => {
  const results = new Array(items.length);
  let cursor = 0;
  const workerCount = Math.max(1, Math.min(limit, items.length));

  const worker = async () => {
    while (true) {
      const index = cursor++;
      if (index >= items.length) break;

      try {
        const value = await task(items[index], index);
        results[index] = { status: 'fulfilled', value };
      } catch (error) {
        results[index] = { status: 'rejected', reason: error };
      }
    }
  };

  await Promise.all(Array.from({ length: workerCount }, worker));
  return results;
};

let transfersRefreshPromise = null;

const buildTransfersResponsePayload = (isStale = false) => ({
  data: Array.isArray(cache.transfers) ? cache.transfers : [],
  chains: Array.isArray(cache.transfersChainStatus) ? cache.transfersChainStatus : [],
  timestamp: cache.transfersTimestamp || null,
  stale: Boolean(isStale),
});

const updateTransfersCache = async () => {
  console.log('-> Refreshing /api/transfers cache from 10 chains...');
  const allResults = await mapWithConcurrency(
    CHAINS,
    MAX_CONCURRENT_REQUESTS,
    (chain) => fetchTransfersForChain(chain)
  );

  let allTransfers = [];
  const chainStatuses = CHAINS.map((chain, index) => {
    const result = allResults[index];

    if (result && result.status === 'fulfilled' && Array.isArray(result.value)) {
      const sanitized = result.value
        .filter((tx) => {
          if (!tx || typeof tx.timeStamp === 'undefined') {
            console.warn(`! Skipping transfer with missing timestamp on ${chain.name}`);
            return false;
          }
          if (!Number.isFinite(Number(tx.timeStamp))) {
            console.warn(`! Skipping transfer with non-numeric timestamp on ${chain.name}`);
            return false;
          }
          return true;
        })
        .map((tx) => ({
          ...tx,
          timeStamp: String(tx.timeStamp),
        }));

      allTransfers.push(...sanitized);

      return {
        chainId: chain.id,
        chainName: chain.name,
        status: 'ok',
        transferCount: sanitized.length,
      };
    }

    const errorReason = result?.status === 'rejected'
      ? result.reason?.message || String(result.reason)
      : 'Upstream response unavailable';

    if (result?.status === 'rejected') {
      console.error(`X Critical error fetching from chain ${chain.name}: ${errorReason}`);
    } else {
      console.error(`X Failed to fetch transfers for chain ${chain.name}: ${errorReason}`);
    }

    return {
      chainId: chain.id,
      chainName: chain.name,
      status: 'error',
      transferCount: 0,
      error: errorReason,
    };
  });

  allTransfers.sort((a, b) => toNumericTimestamp(b.timeStamp) - toNumericTimestamp(a.timeStamp));

  cache.transfers = allTransfers;
  cache.transfersTimestamp = Date.now();
  cache.transfersChainStatus = chainStatuses;

  console.log(`-> Cached ${allTransfers.length} transfers (updated ${new Date(cache.transfersTimestamp).toISOString()}).`);

  return allTransfers;
};

const triggerTransfersRefresh = () => {
  if (transfersRefreshPromise) {
    return transfersRefreshPromise;
  }

  transfersRefreshPromise = updateTransfersCache()
    .catch((error) => {
      console.error('X Failed to refresh transfers cache:', error.message || error);
      throw error;
    })
    .finally(() => {
      transfersRefreshPromise = null;
    });

  return transfersRefreshPromise;
};

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
    offset: 50,
    sort: 'desc',
  };

  try {
    const response = await axios.get(API_V2_BASE_URL, { params });
    if (response.data.status === '1') {
      return response.data.result.map(tx => ({
        ...tx,
        chainName: chain.name,
        chainId: chain.id,
      }));
    } else {
      console.warn(`! No transfers found on chain ${chain.name}: ${response.data.message}`);
      return [];
    }
  } catch (error) {
    console.error(`X Failed to fetch transfers for chain ${chain.name}: ${error.message}`);
    return null; // null on critical error
  }
};

/**
 * [Milestone 2.3]
 * Fetches token holder count for a single chain.
 * @param {object} chain - A chain object { id, name }
 * @returns {Promise<object>} A promise that resolves to a stats object.
 */
const fetchStatsForChain = async (chain) => {
  const params = {
    chainid: chain.id,
    apikey: API_KEY,
    module: 'token',
    action: 'tokenholdercount',
    contractaddress: BZR_ADDRESS,
  };

  try {
    const response = await axios.get(API_V2_BASE_URL, { params });
    if (response.data.status === '1') {
      return {
        chainName: chain.name,
        chainId: chain.id,
        holderCount: parseInt(response.data.result, 10),
      };
    } else {
      console.warn(`! No stats found on chain ${chain.name}: ${response.data.message}`);
      // Return 0 if API says "No transactions found" or similar error
      return { chainName: chain.name, chainId: chain.id, holderCount: 0 };
    }
  } catch (error) {
    console.error(`X Failed to fetch stats for chain ${chain.name}: ${error.message}`);
    return null; // null on critical network error
  }
};

const fetchTokenPrice = async () => {
  const params = {
    chainid: 1,
    apikey: API_KEY,
    module: 'token',
    action: 'tokeninfo',
    contractaddress: BZR_ADDRESS,
  };

  try {
    const response = await axios.get(API_V2_BASE_URL, { params });
    if (response.data?.status === '1' && Array.isArray(response.data.result) && response.data.result.length) {
      const [tokenInfo] = response.data.result;
      const numericPrice = Number(tokenInfo.tokenPriceUSD);
      return {
        available: true,
        priceUsd: Number.isFinite(numericPrice) ? numericPrice : null,
        priceUsdRaw: tokenInfo.tokenPriceUSD,
        source: 'etherscan',
        timestamp: Date.now(),
        proRequired: false,
      };
    }

    if (isProOnlyResponse(response.data)) {
      return {
        available: false,
        priceUsd: null,
        priceUsdRaw: null,
        source: 'etherscan',
        timestamp: Date.now(),
        proRequired: true,
        message: response.data?.result || response.data?.message || 'Pro endpoint required',
      };
    }

    const errorMessage = response.data?.message || 'Unknown error fetching token price';
    throw new Error(errorMessage);
  } catch (error) {
    if (error.response?.data && isProOnlyResponse(error.response.data)) {
      return {
        available: false,
        priceUsd: null,
        priceUsdRaw: null,
        source: 'etherscan',
        timestamp: Date.now(),
        proRequired: true,
        message: error.response.data.result || error.response.data.message || 'Pro endpoint required',
      };
    }

    console.error('X Failed to fetch token price:', error.message || error);
    throw error;
  }
};

const fetchFinalizedBlock = async () => {
  const params = {
    chainid: 1,
    apikey: API_KEY,
    module: 'proxy',
    action: 'eth_getBlockByNumber',
    tag: 'finalized',
    boolean: 'true',
  };

  try {
    const response = await axios.get(API_V2_BASE_URL, { params });
    const result = response.data?.result;

    if (!result || typeof result.number !== 'string') {
      throw new Error('Finalized block response missing number');
    }

    const blockNumberHex = result.number;
    const blockNumber = Number.parseInt(blockNumberHex, 16);

    return {
      blockNumber,
      blockNumberHex,
      timestamp: Date.now(),
      source: 'etherscan',
    };
  } catch (error) {
    console.error('X Failed to fetch finalized block:', error.message || error);
    throw error;
  }
};

// --- API Routes ---

// [Milestone 2.1] - Token Info
app.get('/api/info', async (req, res) => {
  console.log(`[${new Date().toISOString()}] Received request for /api/info`);
  const now = Date.now();
  if (cache.info && (now - cache.infoTimestamp < FIVE_MINUTES)) {
    console.log('-> Returning cached /api/info data.');
    return res.json(cache.info);
  }

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
      return respondUpstreamFailure(res, 'Upstream Etherscan API error while fetching token info', {
        supplyError: supplyResponse.data.message,
        txError: txResponse.data.message,
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
    if (error.response?.data) {
      return respondUpstreamFailure(res, 'Failed to fetch token info from Etherscan', {
        upstreamResponse: error.response.data,
      });
    }

    res.status(500).json({ message: 'Failed to fetch token info', error: error.message });
  }
});


// [Milestone 2.2] - All Transfers (Implementation)
app.get('/api/transfers', async (req, res) => {
  console.log(`[${new Date().toISOString()}] Received request for /api/transfers`);

  const forceRefresh = String(req.query.force).toLowerCase() === 'true';
  const now = Date.now();
  const cacheAge = cache.transfersTimestamp ? now - cache.transfersTimestamp : Infinity;
  const hasCache = Array.isArray(cache.transfers) && cache.transfersTimestamp;
  const isFresh = cacheAge < TWO_MINUTES;

  if (!hasCache || forceRefresh) {
    if (!hasCache) {
      console.log('-> No cached transfers available. Performing blocking refresh.');
    } else if (forceRefresh) {
      console.log('-> Force refresh requested. Waiting for fresh transfer data.');
    }

    try {
      await triggerTransfersRefresh();
      return res.json(buildTransfersResponsePayload(false));
    } catch (error) {
      console.error('Error in forced /api/transfers refresh:', error.message || error);
      if (error.response?.data) {
        return respondUpstreamFailure(res, 'Failed to fetch transfers from Etherscan', {
          upstreamResponse: error.response.data,
        });
      }

      return res.status(500).json({ message: 'Failed to fetch transfers', error: error.message || String(error) });
    }
  }

  const isStale = !isFresh;
  if (isStale) {
    console.log('-> Returning stale cached /api/transfers data while refreshing in background.');
  } else {
    console.log('-> Returning fresh cached /api/transfers data.');
  }

  res.json(buildTransfersResponsePayload(isStale));

  if (isStale) {
    triggerTransfersRefresh().catch((error) => {
      console.error('X Background refresh for /api/transfers failed:', error.message || error);
    });
  }
});

// [Milestone 2.3] - All Stats (Holders)
app.get('/api/stats', async (req, res) => {
  console.log(`[${new Date().toISOString()}] Received request for /api/stats`);
  const now = Date.now();
  if (cache.stats && (now - cache.statsTimestamp < TWO_MINUTES)) {
    console.log('-> Returning cached /api/stats data.');
    return res.json(cache.stats);
  }

  console.log('-> Fetching new /api/stats data from 10 chains...');
  try {
    const allResults = await mapWithConcurrency(
      CHAINS,
      MAX_CONCURRENT_REQUESTS,
      (chain) => fetchStatsForChain(chain)
    );
    let allStats = [];
    let totalHolders = 0; // We'll sum this up for a total count

    allResults.forEach((result, index) => {
      if (result.status === 'fulfilled' && result.value) {
        allStats.push(result.value);
        totalHolders += result.value.holderCount;
      } else if (result.status === 'rejected') {
        console.error(`X Critical error fetching stats from ${CHAINS[index].name}: ${result.reason}`);
      }
    });

    // Sort by holder count, descending
    allStats.sort((a, b) => b.holderCount - a.holderCount);

    const response = {
      totalHolders,
      chains: allStats,
    };

    console.log(`-> Aggregated stats. Total holders (estimated): ${totalHolders}.`);
    cache.stats = response;
    cache.statsTimestamp = Date.now();
    res.json(response);
  } catch (error) {
    console.error('Error in /api/stats handler:', error.message);
    if (error.response?.data) {
      return respondUpstreamFailure(res, 'Failed to fetch token holder stats from Etherscan', {
        upstreamResponse: error.response.data,
      });
    }

    res.status(500).json({ message: 'Failed to fetch stats', error: error.message });
  }
});

app.get('/api/cache-health', (req, res) => {
  const now = Date.now();

  const withMeta = (data, timestamp, ttl) => {
    if (!data || !timestamp) {
      return {
        status: 'empty',
        ageMs: null,
        ttlMs: ttl,
        expiresInMs: null,
      };
    }

    const age = now - timestamp;
    return {
      status: age < ttl ? 'fresh' : 'stale',
      ageMs: age,
      ttlMs: ttl,
      expiresInMs: Math.max(ttl - age, 0),
    };
  };

  res.json({
    info: withMeta(cache.info, cache.infoTimestamp, FIVE_MINUTES),
    transfers: withMeta(cache.transfers, cache.transfersTimestamp, TWO_MINUTES),
    transferChains: cache.transfersChainStatus,
    stats: withMeta(cache.stats, cache.statsTimestamp, TWO_MINUTES),
    tokenPrice: {
      ...withMeta(cache.tokenPrice, cache.tokenPriceTimestamp, TOKEN_PRICE_TTL_MS),
      proRequired: cache.tokenPrice?.proRequired || false,
      available: cache.tokenPrice?.available || false,
      priceUsd: typeof cache.tokenPrice?.priceUsd === 'number' ? cache.tokenPrice.priceUsd : null,
      priceUsdRaw: cache.tokenPrice?.priceUsdRaw ?? null,
    },
    finality: {
      ...withMeta(cache.finality, cache.finalityTimestamp, FINALITY_TTL_MS),
      blockNumber: cache.finality?.blockNumber ?? null,
      blockNumberHex: cache.finality?.blockNumberHex ?? null,
    },
    serverTime: new Date(now).toISOString(),
  });
});

app.get('/api/token-price', async (req, res) => {
  const now = Date.now();
  if (cache.tokenPrice && (now - cache.tokenPriceTimestamp) < TOKEN_PRICE_TTL_MS) {
    return res.json(cache.tokenPrice);
  }

  try {
    const payload = await fetchTokenPrice();
    cache.tokenPrice = payload;
    cache.tokenPriceTimestamp = payload.timestamp;
    res.json(payload);
  } catch (error) {
    res.status(500).json({
      message: 'Failed to fetch token price',
      error: error.message || String(error),
    });
  }
});

app.get('/api/finality', async (req, res) => {
  const now = Date.now();
  if (cache.finality && (now - cache.finalityTimestamp) < FINALITY_TTL_MS) {
    return res.json(cache.finality);
  }

  try {
    const payload = await fetchFinalizedBlock();
    cache.finality = payload;
    cache.finalityTimestamp = payload.timestamp;
    res.json(payload);
  } catch (error) {
    res.status(500).json({
      message: 'Failed to fetch finalized block',
      error: error.message || String(error),
    });
  }
});

// [Milestone 4.1] - Admin Panel
// TODO: Build admin routes

// --- Health Check Route ---
app.get('/', (req, res) => {
  res.send('BZR Token Explorer Backend is running! (v4: ALL ENDPOINTS LIVE)');
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

  if (CACHE_WARM_INTERVAL_MS > 0) {
    console.log(`Cache warming enabled (interval ${CACHE_WARM_INTERVAL_MS}ms).`);

    triggerTransfersRefresh().catch((error) => {
      console.error('X Initial transfers cache warm failed:', error.message || error);
    });

    const interval = setInterval(() => {
      triggerTransfersRefresh().catch((error) => {
        console.error('X Scheduled transfers cache warm failed:', error.message || error);
      });
    }, CACHE_WARM_INTERVAL_MS);

    if (typeof interval.unref === 'function') {
      interval.unref();
    }
  }
});