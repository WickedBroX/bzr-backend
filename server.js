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
  transfersWarmStatus: [],
  transfersWarmTimestamp: 0,
  transfersPageCache: new Map(),
  transfersTotalCache: new Map(),
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
const FINALITY_FALLBACK_RPC_URL = process.env.FINALITY_FALLBACK_RPC_URL || 'https://eth.llamarpc.com';
const TOKEN_PRICE_FALLBACK_ENABLED = process.env.TOKEN_PRICE_FALLBACK_ENABLED !== 'false';
const TOKEN_PRICE_FALLBACK_QUERY = process.env.TOKEN_PRICE_FALLBACK_QUERY || 'BZR';
const TOKEN_PRICE_FALLBACK_SYMBOLS = (process.env.TOKEN_PRICE_FALLBACK_SYMBOLS || 'BZR')
  .split(',')
  .map((value) => value.trim().toUpperCase())
  .filter(Boolean);
const TOKEN_PRICE_FALLBACK_BASE_ADDRESSES = (process.env.TOKEN_PRICE_FALLBACK_BASE_ADDRESSES || '')
  .split(',')
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);
const TOKEN_PRICE_FALLBACK_CHAIN_IDS = (process.env.TOKEN_PRICE_FALLBACK_CHAIN_IDS || '')
  .split(',')
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);
const TOKEN_PRICE_FALLBACK_TIMEOUT_MS = Number(process.env.TOKEN_PRICE_FALLBACK_TIMEOUT_MS || 5_000);
const TOKEN_PRICE_FALLBACK_SYMBOL_SET = new Set(TOKEN_PRICE_FALLBACK_SYMBOLS);
const TOKEN_PRICE_FALLBACK_BASE_ADDRESS_SET = new Set(TOKEN_PRICE_FALLBACK_BASE_ADDRESSES);
const TOKEN_PRICE_FALLBACK_CHAIN_ID_SET = new Set(TOKEN_PRICE_FALLBACK_CHAIN_IDS);
const TRANSFERS_DEFAULT_CHAIN_ID = Number(process.env.TRANSFERS_DEFAULT_CHAIN_ID || 1);
const TRANSFERS_DEFAULT_PAGE_SIZE = Number(process.env.TRANSFERS_DEFAULT_PAGE_SIZE || 25);
const TRANSFERS_MAX_PAGE_SIZE = Number(process.env.TRANSFERS_MAX_PAGE_SIZE || 100);
const TRANSFERS_PAGE_TTL_MS = Number(process.env.TRANSFERS_PAGE_TTL_MS || TWO_MINUTES);
const TRANSFERS_TOTAL_TTL_MS = Number(process.env.TRANSFERS_TOTAL_TTL_MS || 15 * 60 * 1000);
const TRANSFERS_TOTAL_FETCH_LIMIT = Number(process.env.TRANSFERS_TOTAL_FETCH_LIMIT || 10_000);

const toNumericTimestamp = (timestamp) => {
  const parsed = Number(timestamp);
  return Number.isFinite(parsed) ? parsed : 0;
};

const clampTransfersPageSize = (value) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return TRANSFERS_DEFAULT_PAGE_SIZE;
  }

  return Math.max(1, Math.min(Math.floor(numeric), TRANSFERS_MAX_PAGE_SIZE));
};

const normalizePageNumber = (value) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return 1;
  }

  return Math.floor(numeric);
};

const parseOptionalBlockNumber = (value) => {
  if (typeof value === 'undefined') return undefined;
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric >= 0 ? Math.floor(numeric) : undefined;
};

const getChainDefinition = (chainId) => CHAINS.find((chain) => chain.id === chainId);

const buildTransfersPageCacheKey = ({ chainId, page, pageSize, sort, startBlock, endBlock }) => {
  return [
    chainId,
    page,
    pageSize,
    sort,
    typeof startBlock === 'number' ? startBlock : '',
    typeof endBlock === 'number' ? endBlock : '',
  ].join('|');
};

const buildTransfersTotalCacheKey = ({ chainId, startBlock, endBlock }) => {
  return [
    chainId,
    typeof startBlock === 'number' ? startBlock : '',
    typeof endBlock === 'number' ? endBlock : '',
  ].join('|');
};

const getCachedTransfersPage = (key) => {
  const entry = cache.transfersPageCache.get(key);
  if (!entry) {
    return null;
  }

  const age = Date.now() - entry.timestamp;
  return {
    payload: entry.payload,
    timestamp: entry.timestamp,
    age,
    stale: age > TRANSFERS_PAGE_TTL_MS,
  };
};

const setCachedTransfersPage = (key, payload) => {
  cache.transfersPageCache.set(key, {
    timestamp: Date.now(),
    payload,
  });
};

const getCachedTransfersTotal = (key) => {
  const entry = cache.transfersTotalCache.get(key);
  if (!entry) {
    return null;
  }

  const age = Date.now() - entry.timestamp;
  return {
    payload: entry.payload,
    timestamp: entry.timestamp,
    age,
    stale: age > TRANSFERS_TOTAL_TTL_MS,
  };
};

const setCachedTransfersTotal = (key, payload) => {
  cache.transfersTotalCache.set(key, {
    timestamp: Date.now(),
    payload,
  });
};

const transfersPagePromises = new Map();
const transfersTotalPromises = new Map();

const sanitizeTransfers = (transfers, chain) => {
  if (!Array.isArray(transfers)) {
    return [];
  }

  const sanitized = [];
  for (const tx of transfers) {
    if (!tx || typeof tx.timeStamp === 'undefined') {
      console.warn(`! Skipping transfer with missing timestamp on ${chain.name}`);
      continue;
    }

    const numericTimestamp = Number(tx.timeStamp);
    if (!Number.isFinite(numericTimestamp)) {
      console.warn(`! Skipping transfer with non-numeric timestamp on ${chain.name}`);
      continue;
    }

    sanitized.push({
      ...tx,
      chainName: chain.name,
      chainId: chain.id,
      timeStamp: String(tx.timeStamp),
    });
  }

  return sanitized;
};

const respondUpstreamFailure = (res, message, details = {}) => {
  return res.status(502).json({
    message,
    upstream: 'etherscan',
    ...details,
  });
};

const getCachedTransfersWarmSummary = () => {
  return {
    chains: Array.isArray(cache.transfersWarmStatus) ? cache.transfersWarmStatus : [],
    timestamp: cache.transfersWarmTimestamp || null,
  };
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
const warmTransfersCacheForChain = async (chain, { forceRefresh = false, pageSize } = {}) => {
  const startedAt = Date.now();
  const normalizedPageSize = clampTransfersPageSize(pageSize || TRANSFERS_DEFAULT_PAGE_SIZE);
  const summary = {
    chainId: chain.id,
    chainName: chain.name,
    status: 'ok',
    forceRefresh,
    pageSize: normalizedPageSize,
    durationMs: 0,
    timestamp: Date.now(),
    error: null,
    errorCode: null,
    warmed: false,
    totalsWarmed: false,
  };

  try {
    await resolveTransfersPageData({
      chain,
      page: 1,
      pageSize: normalizedPageSize,
      sort: 'desc',
      startBlock: undefined,
      endBlock: undefined,
      forceRefresh,
    });
    summary.warmed = true;

    await resolveTransfersTotalData({
      chain,
      sort: 'desc',
      startBlock: undefined,
      endBlock: undefined,
      forceRefresh,
    });
    summary.totalsWarmed = true;
  } catch (error) {
    summary.status = 'error';
    summary.error = error.message || String(error);
    summary.errorCode = error.code || null;
    summary.upstream = error.payload || null;
  }

  summary.durationMs = Date.now() - startedAt;
  summary.timestamp = Date.now();
  return summary;
};

const warmTransfersCaches = async ({ forceRefresh = false } = {}) => {
  console.log('-> Warming paginated transfers cache across configured chains...');
  const results = await mapWithConcurrency(
    CHAINS,
    MAX_CONCURRENT_REQUESTS,
    async (chain) => {
      const summary = await warmTransfersCacheForChain(chain, { forceRefresh });
      return summary;
    },
  );

  const summaries = results.map((result, index) => {
    if (result.status === 'fulfilled') {
      return result.value;
    }

    const chain = CHAINS[index];
    return {
      chainId: chain.id,
      chainName: chain.name,
      status: 'error',
      error: result.reason?.message || String(result.reason),
      errorCode: result.reason?.code || null,
      upstream: result.reason?.payload || null,
      durationMs: 0,
      timestamp: Date.now(),
      forceRefresh,
    };
  });

  cache.transfersWarmStatus = summaries;
  cache.transfersWarmTimestamp = Date.now();
  return summaries;
};

const triggerTransfersRefresh = ({ forceRefresh = false } = {}) => {
  if (transfersRefreshPromise) {
    return transfersRefreshPromise;
  }

  transfersRefreshPromise = warmTransfersCaches({ forceRefresh })
    .catch((error) => {
      console.error('X Failed to warm transfers cache:', error.message || error);
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
const fetchTransfersPageFromChain = async ({ chain, page, pageSize, sort, startBlock, endBlock }) => {
  const params = {
    chainid: chain.id,
    apikey: API_KEY,
    module: 'account',
    action: 'tokentx',
    contractaddress: BZR_ADDRESS,
    page,
    offset: pageSize,
    sort,
  };

  if (typeof startBlock === 'number') {
    params.startblock = startBlock;
  }
  if (typeof endBlock === 'number') {
    params.endblock = endBlock;
  }

  try {
    const response = await axios.get(API_V2_BASE_URL, { params });
    const payload = response?.data || {};

    if (payload.status === '1' && Array.isArray(payload.result)) {
      const sanitized = sanitizeTransfers(payload.result, chain);
      return {
        transfers: sanitized,
        upstream: payload,
        timestamp: Date.now(),
        page,
        pageSize,
        sort,
        startBlock,
        endBlock,
        resultLength: payload.result.length,
      };
    }

    if (payload.status === '0') {
      if (isProOnlyResponse(payload)) {
        const error = new Error(payload?.result || payload?.message || 'Etherscan PRO plan required');
        error.code = 'ETHERSCAN_PRO_ONLY';
        error.payload = payload;
        throw error;
      }

      const noRecordsMessage = String(payload?.message || payload?.result || '').toLowerCase();
      if (noRecordsMessage.includes('no transactions')) {
        return {
          transfers: [],
          upstream: payload,
          timestamp: Date.now(),
          page,
          pageSize,
          sort,
          startBlock,
          endBlock,
          resultLength: 0,
        };
      }

      const error = new Error(payload?.message || payload?.result || 'Failed to fetch token transfers');
      error.code = 'ETHERSCAN_ERROR';
      error.payload = payload;
      throw error;
    }

    const error = new Error('Unexpected response from Etherscan');
    error.code = 'ETHERSCAN_UNEXPECTED_RESPONSE';
    error.payload = payload;
    throw error;
  } catch (error) {
    if (error.response?.data) {
      const wrappedError = new Error(error.message || 'Etherscan request failed');
      wrappedError.code = 'ETHERSCAN_HTTP_ERROR';
      wrappedError.payload = error.response.data;
      throw wrappedError;
    }

    throw error;
  }
};

const fetchTransfersTotalCount = async ({ chain, sort, startBlock, endBlock }) => {
  const params = {
    chainid: chain.id,
    apikey: API_KEY,
    module: 'account',
    action: 'tokentx',
    contractaddress: BZR_ADDRESS,
    page: 1,
    offset: TRANSFERS_TOTAL_FETCH_LIMIT,
    sort,
  };

  if (typeof startBlock === 'number') {
    params.startblock = startBlock;
  }
  if (typeof endBlock === 'number') {
    params.endblock = endBlock;
  }

  try {
    const response = await axios.get(API_V2_BASE_URL, { params });
    const payload = response?.data || {};

    if (payload.status === '1') {
      const numericCount = Number(payload.count);
      const hasNumericCount = Number.isSafeInteger(numericCount) && numericCount >= 0;
      const resultLength = Array.isArray(payload.result) ? payload.result.length : 0;
      const total = hasNumericCount ? numericCount : resultLength;
      const truncated = hasNumericCount
        ? numericCount > TRANSFERS_TOTAL_FETCH_LIMIT
        : resultLength === TRANSFERS_TOTAL_FETCH_LIMIT;

      return {
        total,
        truncated,
        timestamp: Date.now(),
        resultLength,
      };
    }

    if (payload.status === '0') {
      if (isProOnlyResponse(payload)) {
        const error = new Error(payload?.result || payload?.message || 'Etherscan PRO plan required');
        error.code = 'ETHERSCAN_PRO_ONLY';
        error.payload = payload;
        throw error;
      }

      const noRecordsMessage = String(payload?.message || payload?.result || '').toLowerCase();
      if (noRecordsMessage.includes('no transactions')) {
        return {
          total: 0,
          truncated: false,
          timestamp: Date.now(),
          resultLength: 0,
        };
      }

      const error = new Error(payload?.message || payload?.result || 'Failed to fetch transfers total');
      error.code = 'ETHERSCAN_ERROR';
      error.payload = payload;
      throw error;
    }

    const error = new Error('Unexpected response from Etherscan when computing total');
    error.code = 'ETHERSCAN_UNEXPECTED_RESPONSE';
    error.payload = payload;
    throw error;
  } catch (error) {
    if (error.response?.data) {
      const wrappedError = new Error(error.message || 'Etherscan request failed');
      wrappedError.code = 'ETHERSCAN_HTTP_ERROR';
      wrappedError.payload = error.response.data;
      throw wrappedError;
    }

    throw error;
  }
};

const fetchTransfersPage = async ({ chain, page, pageSize, sort, startBlock, endBlock, cacheKey }) => {
  const key = cacheKey || buildTransfersPageCacheKey({
    chainId: chain.id,
    page,
    pageSize,
    sort,
    startBlock,
    endBlock,
  });

  let promise = transfersPagePromises.get(key);
  if (!promise) {
    promise = fetchTransfersPageFromChain({
      chain,
      page,
      pageSize,
      sort,
      startBlock,
      endBlock,
    })
      .then((result) => {
        setCachedTransfersPage(key, result);
        return result;
      })
      .finally(() => {
        transfersPagePromises.delete(key);
      });

    transfersPagePromises.set(key, promise);
  }

  return promise;
};

const fetchTransfersTotal = async ({ chain, sort, startBlock, endBlock, cacheKey }) => {
  const key = cacheKey || buildTransfersTotalCacheKey({
    chainId: chain.id,
    startBlock,
    endBlock,
  });

  let promise = transfersTotalPromises.get(key);
  if (!promise) {
    promise = fetchTransfersTotalCount({
      chain,
      sort,
      startBlock,
      endBlock,
    })
      .then((result) => {
        setCachedTransfersTotal(key, result);
        return result;
      })
      .finally(() => {
        transfersTotalPromises.delete(key);
      });

    transfersTotalPromises.set(key, promise);
  }

  return promise;
};

const resolveTransfersPageData = async ({
  chain,
  page,
  pageSize,
  sort,
  startBlock,
  endBlock,
  forceRefresh = false,
}) => {
  const cacheKey = buildTransfersPageCacheKey({
    chainId: chain.id,
    page,
    pageSize,
    sort,
    startBlock,
    endBlock,
  });

  if (forceRefresh) {
    cache.transfersPageCache.delete(cacheKey);
    transfersPagePromises.delete(cacheKey);
  }

  const cached = forceRefresh ? null : getCachedTransfersPage(cacheKey);
  if (cached && !cached.stale && cached.payload) {
    return {
      ...cached.payload,
      cacheTimestamp: cached.timestamp,
      stale: false,
      source: 'cache',
    };
  }

  try {
    const fresh = await fetchTransfersPage({
      chain,
      page,
      pageSize,
      sort,
      startBlock,
      endBlock,
      cacheKey,
    });

    return {
      ...fresh,
      cacheTimestamp: fresh.timestamp,
      stale: false,
      source: 'network',
    };
  } catch (error) {
    if (cached?.payload) {
      console.warn(`! Returning stale /api/transfers data for chain ${chain.name}: ${error.message || error}`);
      return {
        ...cached.payload,
        cacheTimestamp: cached.timestamp,
        stale: true,
        source: 'stale-cache',
        error,
      };
    }

    throw error;
  }
};

const resolveTransfersTotalData = async ({
  chain,
  sort,
  startBlock,
  endBlock,
  forceRefresh = false,
}) => {
  const cacheKey = buildTransfersTotalCacheKey({
    chainId: chain.id,
    startBlock,
    endBlock,
  });

  if (forceRefresh) {
    cache.transfersTotalCache.delete(cacheKey);
    transfersTotalPromises.delete(cacheKey);
  }

  const cached = forceRefresh ? null : getCachedTransfersTotal(cacheKey);
  if (cached && !cached.stale && cached.payload) {
    return {
      ...cached.payload,
      cacheTimestamp: cached.timestamp,
      stale: false,
      source: 'cache',
    };
  }

  try {
    const fresh = await fetchTransfersTotal({
      chain,
      sort,
      startBlock,
      endBlock,
      cacheKey,
    });

    return {
      ...fresh,
      cacheTimestamp: fresh.timestamp,
      stale: false,
      source: 'network',
    };
  } catch (error) {
    if (cached?.payload) {
      console.warn(`! Returning stale transfers total for chain ${chain.name}: ${error.message || error}`);
      return {
        ...cached.payload,
        cacheTimestamp: cached.timestamp,
        stale: true,
        source: 'stale-cache',
        error,
      };
    }

    throw error;
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

const fetchTokenPriceFromEtherscan = async () => {
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

const parsePositiveNumber = (value) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return null;
  }

  return numeric;
};

const fetchTokenPriceFromDexscreener = async () => {
  if (!TOKEN_PRICE_FALLBACK_ENABLED) {
    throw new Error('Token price fallback disabled');
  }

  const query = TOKEN_PRICE_FALLBACK_QUERY;
  if (!query) {
    throw new Error('Token price fallback query not configured');
  }

  const endpoint = `https://api.dexscreener.com/latest/dex/search?q=${encodeURIComponent(query)}`;

  try {
    const response = await axios.get(endpoint, {
      timeout: TOKEN_PRICE_FALLBACK_TIMEOUT_MS,
    });

    const pairs = response.data?.pairs;

    if (!Array.isArray(pairs) || pairs.length === 0) {
      throw new Error('DexScreener returned no pairs');
    }

    const hasSymbolFilters = TOKEN_PRICE_FALLBACK_SYMBOL_SET.size > 0;
    const hasAddressFilters = TOKEN_PRICE_FALLBACK_BASE_ADDRESS_SET.size > 0;
    const hasChainFilters = TOKEN_PRICE_FALLBACK_CHAIN_ID_SET.size > 0;

    let selectedPair = null;
    let selectedLiquidity = 0;

    pairs.forEach((pair) => {
      const baseSymbol = String(pair?.baseToken?.symbol || '').toUpperCase();
      const baseAddress = String(pair?.baseToken?.address || '').toLowerCase();
      const chainId = String(pair?.chainId || '').toLowerCase();
      const priceUsdRaw = pair?.priceUsd;
      const priceUsd = parsePositiveNumber(priceUsdRaw);
      const liquidityUsd = parsePositiveNumber(pair?.liquidity?.usd);

      if (!priceUsd || priceUsd <= 0) {
        return;
      }

      if (hasSymbolFilters && !TOKEN_PRICE_FALLBACK_SYMBOL_SET.has(baseSymbol)) {
        return;
      }

      if (hasChainFilters && !TOKEN_PRICE_FALLBACK_CHAIN_ID_SET.has(chainId)) {
        return;
      }

      if (hasAddressFilters && !TOKEN_PRICE_FALLBACK_BASE_ADDRESS_SET.has(baseAddress)) {
        return;
      }

      const effectiveLiquidity = liquidityUsd && liquidityUsd > 0 ? liquidityUsd : 0;

      if (!selectedPair || effectiveLiquidity > selectedLiquidity) {
        selectedPair = {
          pair,
          priceUsd,
          priceUsdRaw,
          liquidityUsd: effectiveLiquidity,
        };
        selectedLiquidity = effectiveLiquidity;
      }
    });

    if (!selectedPair) {
      throw new Error('DexScreener fallback found no usable pairs');
    }

    const {
      pair,
      priceUsd,
      priceUsdRaw,
      liquidityUsd,
    } = selectedPair;

    const liquidityDescriptor = liquidityUsd > 0
      ? `liq ~$${liquidityUsd.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
      : 'liquidity unavailable';

    return {
      available: true,
      priceUsd,
      priceUsdRaw: typeof priceUsdRaw === 'string' ? priceUsdRaw : String(priceUsd),
      source: `dexscreener:${pair.chainId}:${pair.pairAddress}`,
      timestamp: Date.now(),
      proRequired: false,
      message: `DexScreener ${pair.dexId} pair ${pair.baseToken?.symbol}/${pair.quoteToken?.symbol} (${liquidityDescriptor})`,
    };
  } catch (error) {
    if (error.response?.data) {
      throw new Error(`DexScreener error: ${JSON.stringify(error.response.data)}`);
    }

    throw error;
  }
};

const fetchTokenPrice = async () => {
  let primaryPayload = null;
  let primaryError = null;

  try {
    primaryPayload = await fetchTokenPriceFromEtherscan();

    if (
      primaryPayload.available &&
      typeof primaryPayload.priceUsd === 'number' &&
      primaryPayload.priceUsd > 0
    ) {
      return primaryPayload;
    }

    const reason = primaryPayload.proRequired
      ? (primaryPayload.message || 'Etherscan PRO plan required for token price')
      : 'Etherscan returned no price data';
    primaryError = new Error(reason);
  } catch (error) {
    primaryError = error;
  }

  try {
    const fallbackPayload = await fetchTokenPriceFromDexscreener();
    if (primaryError) {
      fallbackPayload.message = [fallbackPayload.message, `Etherscan fallback: ${primaryError.message || primaryError}`]
        .filter(Boolean)
        .join(' · ');
    }
    return fallbackPayload;
  } catch (fallbackError) {
    if (primaryPayload) {
      return {
        ...primaryPayload,
        available: primaryPayload.available && typeof primaryPayload.priceUsd === 'number' && primaryPayload.priceUsd > 0,
        message: [primaryPayload.message, `Fallback failed: ${fallbackError.message || fallbackError}`]
          .filter(Boolean)
          .join(' · '),
      };
    }

    if (primaryError) {
      const aggregate = new Error('Unable to fetch token price from Etherscan and fallback provider');
      aggregate.cause = {
        primaryError: primaryError.message || primaryError,
        fallbackError: fallbackError.message || fallbackError,
      };
      throw aggregate;
    }

    throw fallbackError;
  }
};

const parseFinalizedBlockPayload = (result, source) => {
  if (!result || typeof result.number !== 'string') {
    throw new Error(`[${source}] Finalized block response missing number`);
  }

  const blockNumberHex = result.number;
  const blockNumber = Number.parseInt(blockNumberHex, 16);

  if (!Number.isFinite(blockNumber)) {
    throw new Error(`[${source}] Invalid block number: ${blockNumberHex}`);
  }

  return {
    blockNumber,
    blockNumberHex,
    timestamp: Date.now(),
    source,
  };
};

const fetchFinalizedBlockFromEtherscan = async () => {
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

    return parseFinalizedBlockPayload(result, 'etherscan');
  } catch (error) {
    console.error('X Failed to fetch finalized block:', error.message || error);
    throw error;
  }
};

const fetchFinalizedBlockFromRpc = async () => {
  const payload = {
    jsonrpc: '2.0',
    id: Date.now(),
    method: 'eth_getBlockByNumber',
    params: ['finalized', true],
  };

  try {
    const response = await axios.post(
      FINALITY_FALLBACK_RPC_URL,
      payload,
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );

    if (response.data?.error) {
      const message = response.data.error?.message || 'Unknown RPC error';
      const code = response.data.error?.code;
      throw new Error(`[rpc:${FINALITY_FALLBACK_RPC_URL}] ${message}${typeof code !== 'undefined' ? ` (code ${code})` : ''}`);
    }

    return parseFinalizedBlockPayload(response.data?.result, `rpc:${FINALITY_FALLBACK_RPC_URL}`);
  } catch (error) {
    console.error('X Fallback RPC finalized block request failed:', error.message || error);
    throw error;
  }
};

const fetchFinalizedBlock = async () => {
  try {
    return await fetchFinalizedBlockFromEtherscan();
  } catch (primaryError) {
    const message = primaryError?.response?.data?.error?.message || primaryError?.response?.data?.message || primaryError?.message || '';
    const code = primaryError?.response?.data?.error?.code;
    const shouldFallback =
      code === -32602 ||
      /invalid hex string/i.test(message) ||
      /unsupported/i.test(message) ||
      /missing number/i.test(message);

    if (!shouldFallback) {
      throw primaryError;
    }

    console.warn('! Etherscan does not support finalized tag currently — attempting fallback RPC.');

    try {
      return await fetchFinalizedBlockFromRpc();
    } catch (fallbackError) {
      const aggregate = new Error('Failed to fetch finalized block from both Etherscan and fallback RPC');
      aggregate.cause = {
        primaryError: primaryError.message || primaryError,
        fallbackError: fallbackError.message || fallbackError,
      };
      throw aggregate;
    }
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

  if (!API_KEY || !BZR_ADDRESS) {
    return res.status(500).json({ message: 'Server missing ETHERSCAN_V2_API_KEY or BZR_TOKEN_ADDRESS' });
  }

  const forceRefresh = String(req.query.force).toLowerCase() === 'true';
  const requestedChainId = Number(req.query.chainId || TRANSFERS_DEFAULT_CHAIN_ID);
  const requestedPage = normalizePageNumber(req.query.page || 1);
  const requestedPageSize = clampTransfersPageSize(req.query.pageSize || TRANSFERS_DEFAULT_PAGE_SIZE);
  const sortParam = typeof req.query.sort === 'string' ? req.query.sort.toLowerCase() : 'desc';
  const sort = sortParam === 'asc' ? 'asc' : 'desc';
  const startBlock = parseOptionalBlockNumber(req.query.startBlock);
  const endBlock = parseOptionalBlockNumber(req.query.endBlock);
  const includeTotals = req.query.includeTotals !== 'false';

  if (typeof startBlock === 'number' && typeof endBlock === 'number' && startBlock > endBlock) {
    return res.status(400).json({
      message: 'startBlock cannot be greater than endBlock',
      startBlock,
      endBlock,
    });
  }

  const chain = getChainDefinition(requestedChainId) || getChainDefinition(TRANSFERS_DEFAULT_CHAIN_ID) || CHAINS[0];
  if (!chain) {
    return res.status(400).json({
      message: 'Unsupported chain requested',
      chainId: requestedChainId,
      availableChains: CHAINS,
    });
  }

  try {
    const pageData = await resolveTransfersPageData({
      chain,
      page: requestedPage,
      pageSize: requestedPageSize,
      sort,
      startBlock,
      endBlock,
      forceRefresh,
    });

    let totalsData = null;
    if (includeTotals) {
      totalsData = await resolveTransfersTotalData({
        chain,
        sort,
        startBlock,
        endBlock,
        forceRefresh,
      });
    }

    const warmSummary = getCachedTransfersWarmSummary();

    const totalCount = totalsData ? totalsData.total : pageData.resultLength || 0;
    const totalPages = totalCount > 0 ? Math.ceil(totalCount / requestedPageSize) : 0;
    const hasMore = totalCount > requestedPage * requestedPageSize;
    const timestamp = pageData.timestamp || pageData.cacheTimestamp || Date.now();
    const warnings = [];

    if (pageData.stale && pageData.error) {
      warnings.push({
        scope: 'page',
        code: pageData.error.code || 'STALE_PAGE',
        message: pageData.error.message || 'Page data returned from stale cache after upstream failure.',
      });
    }

    if (totalsData?.stale && totalsData.error) {
      warnings.push({
        scope: 'total',
        code: totalsData.error.code || 'STALE_TOTAL',
        message: totalsData.error.message || 'Total count returned from stale cache after upstream failure.',
      });
    }

    res.json({
      data: Array.isArray(pageData.transfers) ? pageData.transfers : [],
      pagination: {
        page: requestedPage,
        pageSize: requestedPageSize,
        total: totalCount,
        totalPages,
        hasMore,
      },
      totals: totalsData
        ? {
            total: totalsData.total,
            truncated: totalsData.truncated,
            resultLength: totalsData.resultLength,
            timestamp: totalsData.timestamp,
            stale: totalsData.stale,
            source: totalsData.source,
          }
        : null,
      chain: {
        id: chain.id,
        name: chain.name,
      },
      sort,
      filters: {
        startBlock: typeof startBlock === 'number' ? startBlock : null,
        endBlock: typeof endBlock === 'number' ? endBlock : null,
      },
      timestamp,
      stale: Boolean(pageData.stale || totalsData?.stale),
      source: pageData.source,
      warnings,
      limits: {
        maxPageSize: TRANSFERS_MAX_PAGE_SIZE,
        totalFetchLimit: TRANSFERS_TOTAL_FETCH_LIMIT,
      },
      defaults: {
        chainId: TRANSFERS_DEFAULT_CHAIN_ID,
        pageSize: TRANSFERS_DEFAULT_PAGE_SIZE,
        sort: 'desc',
      },
      warm: {
        chains: warmSummary.chains,
        timestamp: warmSummary.timestamp,
      },
      chains: warmSummary.chains,
      availableChains: CHAINS.map((c) => ({ id: c.id, name: c.name })),
      request: {
        forceRefresh,
        includeTotals,
      },
    });
  } catch (error) {
    console.error('Error handling /api/transfers request:', error.message || error);

    if (error.code === 'ETHERSCAN_PRO_ONLY') {
      return res.status(402).json({
        message: 'Etherscan PRO plan required for this request',
        details: error.payload || null,
      });
    }

    if (error.code && error.payload) {
      return respondUpstreamFailure(res, 'Failed to fetch transfers from Etherscan', {
        errorCode: error.code,
        upstreamResponse: error.payload,
      });
    }

    return res.status(500).json({
      message: 'Failed to fetch transfers',
      error: error.message || String(error),
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
    transfersWarm: withMeta(cache.transfersWarmStatus, cache.transfersWarmTimestamp, TWO_MINUTES),
    transferWarmChains: cache.transfersWarmStatus,
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