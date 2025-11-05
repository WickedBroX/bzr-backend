// Load environment variables from .env file
require('dotenv').config();

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const NodeCache = require('node-cache');
const compression = require('compression');

const app = express();
const PORT = process.env.PORT || 3001;

// --- Cache Setup ---
const apiCache = new NodeCache({ stdTTL: 60, checkperiod: 120 }); // Default 60s TTL for new caching

// --- Security Middleware ---
// Helmet for security headers
app.use(helmet({
  contentSecurityPolicy: false, // Disable CSP for API
  crossOriginEmbedderPolicy: false,
}));

// Compression for response optimization
app.use(compression());

// CORS with origin restrictions
const allowedOrigins = process.env.ALLOWED_ORIGINS 
  ? process.env.ALLOWED_ORIGINS.split(',').map(origin => origin.trim())
  : ['http://localhost:5173', 'http://localhost:3000'];

app.use(cors({
  origin: (origin, callback) => {
    // Allow requests with no origin (mobile apps, Postman, etc.)
    if (!origin) return callback(null, true);
    
    if (allowedOrigins.indexOf(origin) !== -1 || allowedOrigins.includes('*')) {
      callback(null, true);
    } else {
      callback(new Error('Not allowed by CORS'));
    }
  },
  credentials: true,
  methods: ['GET', 'POST'],
  maxAge: 86400 // 24 hours
}));

app.use(express.json());

// --- Rate Limiting ---
// General API rate limiter
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // limit each IP to 100 requests per windowMs
  message: { error: 'Too many requests from this IP, please try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});

// Strict rate limiter for expensive endpoints
const strictLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 10, // 10 requests per minute
  message: { error: 'Rate limit exceeded. Please slow down your requests.' },
  standardHeaders: true,
  legacyHeaders: false,
});

// Apply rate limiting to all API routes
app.use('/api/', apiLimiter);

// --- Constants ---
const ETHERSCAN_API_KEY = process.env.ETHERSCAN_V2_API_KEY;
const BZR_ADDRESS = process.env.BZR_TOKEN_ADDRESS;
const API_V2_BASE_URL = 'https://api.etherscan.io/v2/api';
const CRONOS_API_KEY = process.env.CRONOS_API_KEY;
const CRONOS_API_BASE_URL = process.env.CRONOS_API_BASE_URL || 'https://explorer-api.cronos.org/mainnet/api/v2';
const MAX_CONCURRENT_REQUESTS = Number(process.env.ETHERSCAN_CONCURRENCY || 3);
const TOKEN_PRICE_COINGECKO_ID = (process.env.TOKEN_PRICE_COINGECKO_ID || 'bazaars').trim();
const TOKEN_PRICE_COINGECKO_TIMEOUT_MS = Number(process.env.TOKEN_PRICE_COINGECKO_TIMEOUT_MS || 5_000);
const TOKEN_PRICE_COINGECKO_ENABLED = TOKEN_PRICE_COINGECKO_ID.length > 0 && TOKEN_PRICE_COINGECKO_ID.toLowerCase() !== 'disabled';

const PROVIDERS = {
  etherscan: {
    apiKey: ETHERSCAN_API_KEY,
    baseUrl: API_V2_BASE_URL,
    requiresChainId: true,
  },
  cronos: {
    apiKey: CRONOS_API_KEY,
    baseUrl: CRONOS_API_BASE_URL,
    requiresChainId: false,
  },
};

console.log(`i API Base URL: ${API_V2_BASE_URL}`);
console.log(`i Cronos API Base URL: ${CRONOS_API_BASE_URL}`);

// --- Caching Middleware ---
/**
 * Generic caching middleware using NodeCache
 * @param {number} duration - Cache duration in seconds
 */
const cacheMiddleware = (duration) => (req, res, next) => {
  // Only cache GET requests
  if (req.method !== 'GET') {
    return next();
  }
  
  const key = req.originalUrl || req.url;
  const cachedResponse = apiCache.get(key);
  
  if (cachedResponse) {
    console.log(`[CACHE HIT] ${key}`);
    return res.json(cachedResponse);
  }
  
  console.log(`[CACHE MISS] ${key}`);
  
  // Store the original res.json to intercept the response
  res.originalJson = res.json;
  res.json = function(data) {
    // Cache the response
    apiCache.set(key, data, duration);
    // Call the original res.json
    res.originalJson(data);
  };
  
  next();
};

// [Milestone 2.2] Define all 10 chains
const CHAINS = [
  { id: 1, name: 'Ethereum', provider: 'etherscan' },
  { id: 10, name: 'Optimism', provider: 'etherscan' },
  { id: 56, name: 'BSC', provider: 'etherscan' },
  { id: 137, name: 'Polygon', provider: 'etherscan' },
  { id: 324, name: 'zkSync', provider: 'etherscan' },
  { id: 5000, name: 'Mantle', provider: 'etherscan' },
  { id: 42161, name: 'Arbitrum', provider: 'etherscan' },
  { id: 43114, name: 'Avalanche', provider: 'etherscan' },
  { id: 8453, name: 'Base', provider: 'etherscan' },
  { id: 25, name: 'Cronos', provider: 'cronos' },
];

const DEFAULT_PROVIDER_KEY = 'etherscan';

const getProviderKeyForChain = (chain) => (chain?.provider ? String(chain.provider) : DEFAULT_PROVIDER_KEY);

const getProviderConfigForChain = (chain, { requireApiKey = true } = {}) => {
  const providerKey = getProviderKeyForChain(chain);
  const provider = PROVIDERS[providerKey];

  if (!provider) {
    throw new Error(`Provider configuration missing for chain ${chain?.name || chain?.id} (${providerKey})`);
  }

  if (requireApiKey && !provider.apiKey) {
    throw new Error(`Missing API key for provider "${providerKey}" (chain ${chain?.name || chain?.id})`);
  }

  return { ...provider, key: providerKey };
};

const buildProviderRequest = (chain, params = {}, options = {}) => {
  const { includeApiKey = true } = options;
  const provider = getProviderConfigForChain(chain, { requireApiKey: includeApiKey });
  const nextParams = { ...params };

  if (includeApiKey) {
    nextParams.apikey = provider.apiKey;
  }

  if (provider.requiresChainId) {
    nextParams.chainid = chain.id;
  }

  return {
    provider,
    params: nextParams,
  };
};

const isCronosChain = (chain) => getProviderKeyForChain(chain) === 'cronos';

const HEX_PREFIX = /^0x/i;
const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000';
const TRANSFER_EVENT_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
const CRONOS_MAX_LOG_BLOCK_RANGE = Number(process.env.CRONOS_LOG_BLOCK_RANGE || 9_999);
const CRONOS_MAX_LOG_ITERATIONS = Number(process.env.CRONOS_LOG_MAX_ITERATIONS || 12);
const CRONOS_TOTAL_MAX_ITERATIONS = Number(process.env.CRONOS_TOTAL_LOG_MAX_ITERATIONS || 60);
const BZR_TOKEN_NAME = process.env.BZR_TOKEN_NAME || 'Bazaars';
const BZR_TOKEN_SYMBOL = process.env.BZR_TOKEN_SYMBOL || 'BZR';
const BZR_TOKEN_DECIMALS = Number(process.env.BZR_TOKEN_DECIMALS || 18);

const normalizeHex = (value) => {
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      return '0x0';
    }
    return `0x${value.toString(16)}`;
  }

  if (typeof value === 'bigint') {
    return `0x${value.toString(16)}`;
  }

  if (typeof value === 'string') {
    return HEX_PREFIX.test(value) ? value : `0x${value}`;
  }

  return '0x0';
};

const hexToBigInt = (value) => {
  try {
    return BigInt(normalizeHex(value));
  } catch (error) {
    return BigInt(0);
  }
};

const hexToDecimalString = (value) => hexToBigInt(value).toString(10);

const hexToNumberSafe = (value) => {
  const normalized = normalizeHex(value);
  const parsed = Number.parseInt(normalized, 16);
  return Number.isFinite(parsed) ? parsed : 0;
};

const topicToAddress = (topic) => {
  if (typeof topic !== 'string' || topic.length < 42) {
    return ZERO_ADDRESS;
  }

  const trimmed = topic.slice(-40);
  return `0x${trimmed.toLowerCase()}`;
};

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

if (typeof BZR_ADDRESS === 'string' && BZR_ADDRESS) {
  TOKEN_PRICE_FALLBACK_BASE_ADDRESS_SET.add(BZR_ADDRESS.toLowerCase());
}
const TRANSFERS_DEFAULT_CHAIN_ID = Number(process.env.TRANSFERS_DEFAULT_CHAIN_ID || 1);
const TRANSFERS_DEFAULT_PAGE_SIZE = Number(process.env.TRANSFERS_DEFAULT_PAGE_SIZE || 25);
const TRANSFERS_MAX_PAGE_SIZE = Number(process.env.TRANSFERS_MAX_PAGE_SIZE || 100);
const TRANSFERS_PAGE_TTL_MS = Number(process.env.TRANSFERS_PAGE_TTL_MS || TWO_MINUTES);
const TRANSFERS_TOTAL_TTL_MS = Number(process.env.TRANSFERS_TOTAL_TTL_MS || 15 * 60 * 1000);
const TRANSFERS_TOTAL_FETCH_LIMIT = Number(process.env.TRANSFERS_TOTAL_FETCH_LIMIT || 25_000);
const ETHERSCAN_RESULT_WINDOW = Number(process.env.ETHERSCAN_RESULT_WINDOW || 10_000);

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
  const upstreamProvider = details?.upstreamProvider || details?.provider || 'etherscan';

  return res.status(502).json({
    message,
    upstream: upstreamProvider,
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

const fetchLatestBlockNumberForChain = async (chain) => {
  const { provider, params } = buildProviderRequest(chain, {
    module: 'proxy',
    action: 'eth_blockNumber',
  });

  try {
    const response = await axios.get(provider.baseUrl, { params });
    const payload = response?.data || {};
    if (typeof payload.result === 'string') {
      return hexToNumberSafe(payload.result);
    }

    const error = new Error(payload?.message || 'Invalid block number payload');
    error.code = 'PROXY_BLOCKNUMBER_INVALID';
    error.payload = payload;
    throw error;
  } catch (error) {
    if (error.response?.data) {
      const wrapped = new Error(error.message || 'Failed to fetch latest block number');
      wrapped.code = 'PROXY_BLOCKNUMBER_HTTP_ERROR';
      wrapped.payload = error.response.data;
      throw wrapped;
    }

    throw error;
  }
};

const mapCronosLogToTransfer = (log, chain, latestBlockNumber) => {
  const blockNumber = hexToNumberSafe(log.blockNumber);
  const timeStamp = hexToNumberSafe(log.timeStamp);
  const transactionIndex = hexToNumberSafe(log.transactionIndex);
  const logIndex = hexToNumberSafe(log.logIndex);
  const gasUsed = hexToDecimalString(log.gasUsed || '0x0');
  const gasPrice = hexToDecimalString(log.gasPrice || '0x0');
  const confirmations = latestBlockNumber >= blockNumber
    ? String(Math.max(0, latestBlockNumber - blockNumber))
    : '0';

  return {
    blockNumber: String(blockNumber),
    timeStamp: String(timeStamp),
    hash: log.transactionHash,
    nonce: '0',
    blockHash: log.blockHash,
    from: topicToAddress(log.topics?.[1]),
    contractAddress: log.address,
    to: topicToAddress(log.topics?.[2]),
    value: hexToDecimalString(log.data || '0x0'),
    tokenName: BZR_TOKEN_NAME,
    tokenSymbol: BZR_TOKEN_SYMBOL,
    tokenDecimal: String(BZR_TOKEN_DECIMALS),
    transactionIndex: String(transactionIndex),
    gas: gasUsed,
    gasPrice,
    gasUsed,
    cumulativeGasUsed: gasUsed,
    input: '0x',
    methodId: '0x',
    functionName: 'Transfer(address,address,uint256)',
    confirmations,
    logIndex: String(logIndex),
  };
};

const fetchCronosLogsWindow = async (chain, fromBlock, toBlock) => {
  const { provider, params } = buildProviderRequest(chain, {
    module: 'logs',
    action: 'getLogs',
    fromBlock,
    toBlock,
    address: BZR_ADDRESS,
    topic0: TRANSFER_EVENT_TOPIC,
  });

  const response = await axios.get(provider.baseUrl, { params });
  const payload = response?.data || {};

  return {
    payload,
    request: {
      fromBlock,
      toBlock,
    },
  };
};

const fetchCronosTransfersPage = async ({
  chain,
  page,
  pageSize,
  sort,
  startBlock,
  endBlock,
}) => {
  const normalizedPage = normalizePageNumber(page);
  const normalizedPageSize = clampTransfersPageSize(pageSize);
  const latestBlockNumber = await fetchLatestBlockNumberForChain(chain);
  const effectiveEnd = typeof endBlock === 'number' ? endBlock : latestBlockNumber;
  const effectiveStart = typeof startBlock === 'number' ? startBlock : 0;

  let currentTo = effectiveEnd;
  let iterations = 0;
  const collected = [];
  const batches = [];
  const requiredItems = normalizedPage * normalizedPageSize + normalizedPageSize;

  while (
    currentTo >= effectiveStart &&
    iterations < CRONOS_MAX_LOG_ITERATIONS &&
    collected.length < requiredItems
  ) {
    const span = CRONOS_MAX_LOG_BLOCK_RANGE > 0 ? CRONOS_MAX_LOG_BLOCK_RANGE : 9_999;
    const currentFrom = Math.max(effectiveStart, currentTo - (span - 1));
    const { payload, request } = await fetchCronosLogsWindow(chain, currentFrom, currentTo);
    const resultLength = Array.isArray(payload.result) ? payload.result.length : 0;

    batches.push({
      ...request,
      status: payload.status,
      message: payload.message,
      resultLength,
    });

    if (payload.status === '1' && Array.isArray(payload.result)) {
      collected.push(...payload.result);
    } else if (payload.status === '0') {
      const message = String(payload.message || payload.result || '').toLowerCase();
      if (!(message.includes('no records') || message.includes('no logs'))) {
        const error = new Error(payload.message || payload.result || 'Failed to fetch Cronos token transfers');
        error.code = 'CRONOS_LOGS_ERROR';
        error.payload = payload;
        throw error;
      }
    } else {
      const error = new Error('Unexpected response from Cronos logs endpoint');
      error.code = 'CRONOS_LOGS_UNEXPECTED_RESPONSE';
      error.payload = payload;
      throw error;
    }

    iterations += 1;
    currentTo = currentFrom - 1;
  }

  const decorated = collected.map((log) => ({
    log,
    blockNumber: hexToNumberSafe(log.blockNumber),
    timeStamp: hexToNumberSafe(log.timeStamp),
  }));

  decorated.sort((a, b) => {
    const comparator = sort === 'asc' ? 1 : -1;
    if (a.blockNumber !== b.blockNumber) {
      return comparator * (a.blockNumber - b.blockNumber);
    }

    return comparator * (a.timeStamp - b.timeStamp);
  });

  const startIndex = (normalizedPage - 1) * normalizedPageSize;
  const pageLogs = decorated.slice(startIndex, startIndex + normalizedPageSize).map((entry) => entry.log);
  const mapped = pageLogs.map((log) => mapCronosLogToTransfer(log, chain, latestBlockNumber));
  const transfers = sanitizeTransfers(mapped, chain);

  return {
    transfers,
    upstream: {
      provider: 'cronos',
      latestBlock: latestBlockNumber,
      iterations,
      batches,
      collected: collected.length,
    },
    timestamp: Date.now(),
    page: normalizedPage,
    pageSize: normalizedPageSize,
    sort,
    startBlock,
    endBlock,
    resultLength: transfers.length,
    totalCollected: decorated.length,
  };
};

const fetchCronosTransfersTotalCount = async ({ chain, startBlock, endBlock }) => {
  const latestBlockNumber = await fetchLatestBlockNumberForChain(chain);
  const effectiveEnd = typeof endBlock === 'number' ? endBlock : latestBlockNumber;
  const effectiveStart = typeof startBlock === 'number' ? startBlock : 0;

  // [OPTIMIZATION] For all-time queries (no specific block range), return early
  // to avoid timeout from iterating through entire blockchain history
  const isAllTimeQuery = typeof startBlock !== 'number' && typeof endBlock !== 'number';
  if (isAllTimeQuery) {
    console.log(`[Cronos] Skipping expensive all-time total count to prevent timeout`);
    return {
      total: TRANSFERS_TOTAL_FETCH_LIMIT,
      truncated: true,
      timestamp: Date.now(),
      resultLength: TRANSFERS_TOTAL_FETCH_LIMIT,
      estimated: true,
      upstream: {
        provider: 'cronos',
        iterations: 0,
        batches: [],
        latestBlock: latestBlockNumber,
        note: 'All-time count estimation to prevent timeout',
      },
    };
  }

  let currentTo = effectiveEnd;
  let iterations = 0;
  let total = 0;
  let truncated = false;
  const batches = [];

  while (currentTo >= effectiveStart && iterations < CRONOS_TOTAL_MAX_ITERATIONS) {
    const span = CRONOS_MAX_LOG_BLOCK_RANGE > 0 ? CRONOS_MAX_LOG_BLOCK_RANGE : 9_999;
    const currentFrom = Math.max(effectiveStart, currentTo - (span - 1));
    const { payload, request } = await fetchCronosLogsWindow(chain, currentFrom, currentTo);
    const resultLength = Array.isArray(payload.result) ? payload.result.length : 0;

    batches.push({
      ...request,
      status: payload.status,
      message: payload.message,
      resultLength,
    });

    if (payload.status === '1' && Array.isArray(payload.result)) {
      total += resultLength;
      if (total >= TRANSFERS_TOTAL_FETCH_LIMIT) {
        truncated = true;
        total = TRANSFERS_TOTAL_FETCH_LIMIT;
        break;
      }
    } else if (payload.status === '0') {
      const message = String(payload.message || payload.result || '').toLowerCase();
      if (!(message.includes('no records') || message.includes('no logs'))) {
        const error = new Error(payload.message || payload.result || 'Failed to fetch Cronos transfers total');
        error.code = 'CRONOS_LOGS_TOTAL_ERROR';
        error.payload = payload;
        throw error;
      }
    } else {
      const error = new Error('Unexpected response from Cronos logs endpoint while counting');
      error.code = 'CRONOS_LOGS_TOTAL_UNEXPECTED_RESPONSE';
      error.payload = payload;
      throw error;
    }

    iterations += 1;
    currentTo = currentFrom - 1;
  }

  if (currentTo >= effectiveStart) {
    truncated = true;
  }

  return {
    total,
    truncated,
    timestamp: Date.now(),
    resultLength: total,
    upstream: {
      provider: 'cronos',
      iterations,
      batches,
      latestBlock: latestBlockNumber,
    },
  };
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
    totalsWarning: null,
  };

  try {
    // Always warm the page cache first
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

    // Try to warm totals, but don't fail if it errors
    try {
      await resolveTransfersTotalData({
        chain,
        sort: 'desc',
        startBlock: undefined,
        endBlock: undefined,
        forceRefresh,
      });
      summary.totalsWarmed = true;
    } catch (totalsError) {
      // Log but don't fail - totals are nice to have but not critical
      console.warn(`! Could not warm totals for ${chain.name}: ${totalsError.message || totalsError}`);
      summary.totalsWarning = totalsError.message || String(totalsError);
      summary.totalsErrorCode = totalsError.code || null;
      // Still mark status as 'ok' since page warming succeeded
    }
  } catch (error) {
    // Only mark as error if page warming failed
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
 * Handles aggregated transfers from all chains
 */
const handleAggregatedTransfers = async (req, res, options) => {
  const {
    forceRefresh,
    requestedPage,
    requestedPageSize,
    sort,
    startBlock,
    endBlock,
    includeTotals,
  } = options;

  console.log('-> Fetching aggregated transfers from all chains...');

  try {
    // Fetch first page from all chains in parallel
    const results = await mapWithConcurrency(
      CHAINS,
      MAX_CONCURRENT_REQUESTS,
      async (chain) => {
        try {
          const pageData = await resolveTransfersPageData({
            chain,
            page: 1,
            pageSize: requestedPageSize,
            sort,
            startBlock,
            endBlock,
            forceRefresh,
          });
          return { chain, data: pageData, error: null };
        } catch (error) {
          console.warn(`! Failed to fetch transfers from ${chain.name}: ${error.message || error}`);
          return { chain, data: null, error };
        }
      }
    );

    // Combine all transfers
    const allTransfers = [];
    const chainSummaries = [];
    let displayCount = 0; // Count of transfers fetched for display
    let allTimeTotal = 0; // True all-time total from cached individual chain totals
    let allTimeTotalAvailable = true;

    // Fetch cached totals from each chain for all-time stats
    const chainTotalsPromises = CHAINS.map(async (chain) => {
      try {
        const cacheKey = buildTransfersTotalCacheKey({
          chainId: chain.id,
          startBlock: undefined,
          endBlock: undefined,
        });
        const cached = getCachedTransfersTotal(cacheKey);
        return cached?.payload?.total || 0;
      } catch (error) {
        console.warn(`! Could not get cached total for ${chain.name}`);
        return 0;
      }
    });

    const chainTotals = await Promise.all(chainTotalsPromises);
    allTimeTotal = chainTotals.reduce((sum, total) => sum + (total || 0), 0);
    
    // If no cached totals available, mark as unavailable
    if (allTimeTotal === 0) {
      allTimeTotalAvailable = false;
    }

    results.forEach((result) => {
      if (result.status === 'fulfilled' && result.value.data) {
        const { chain, data } = result.value;
        allTransfers.push(...(data.transfers || []));
        chainSummaries.push({
          chainId: chain.id,
          chainName: chain.name,
          status: 'ok',
          transferCount: (data.transfers || []).length,
          durationMs: 0,
          timestamp: data.timestamp || Date.now(),
        });
      } else if (result.status === 'fulfilled' && result.value.error) {
        const { chain, error } = result.value;
        chainSummaries.push({
          chainId: chain.id,
          chainName: chain.name,
          status: 'error',
          transferCount: 0,
          error: error.message || String(error),
          errorCode: error.code || null,
          durationMs: 0,
          timestamp: Date.now(),
        });
      }
    });

    // Sort all transfers by timestamp
    allTransfers.sort((a, b) => {
      const aTime = Number(a.timeStamp);
      const bTime = Number(b.timeStamp);
      return sort === 'asc' ? aTime - bTime : bTime - aTime;
    });

    // Paginate the combined results
    const start = (requestedPage - 1) * requestedPageSize;
    const end = start + requestedPageSize;
    const paginatedTransfers = allTransfers.slice(start, end);
    displayCount = allTransfers.length; // Transfers fetched for current display

    const totalPages = displayCount > 0 ? Math.ceil(displayCount / requestedPageSize) : 1;
    const hasMore = displayCount > requestedPage * requestedPageSize;

    const warmSummary = getCachedTransfersWarmSummary();
    const warnings = [];
    
    if (!allTimeTotalAvailable) {
      warnings.push({
        scope: 'total',
        code: 'ALL_TIME_TOTAL_UNAVAILABLE',
        message: 'All-time transfer totals not yet cached. Displaying current page data only.',
        retryable: true,
      });
    }

    res.json({
      data: paginatedTransfers,
      pagination: {
        page: requestedPage,
        pageSize: requestedPageSize,
        total: displayCount, // Display count for current view pagination
        totalPages,
        hasMore,
        windowExceeded: false,
        maxWindowPages: null,
        resultWindow: null,
      },
      totals: includeTotals
        ? {
            total: displayCount, // Display total for pagination
            allTimeTotal: allTimeTotalAvailable ? allTimeTotal : null, // True all-time total across all chains
            truncated: false,
            resultLength: displayCount,
            timestamp: Date.now(),
            stale: false,
            source: 'aggregated',
            allTimeTotalAvailable,
          }
        : null,
      chain: {
        id: 0,
        name: 'All Chains',
      },
      sort,
      filters: {
        startBlock: typeof startBlock === 'number' ? startBlock : null,
        endBlock: typeof endBlock === 'number' ? endBlock : null,
      },
      timestamp: Date.now(),
      stale: false,
      source: 'aggregated',
      warnings,
      limits: {
        maxPageSize: TRANSFERS_MAX_PAGE_SIZE,
        totalFetchLimit: TRANSFERS_TOTAL_FETCH_LIMIT,
        resultWindow: null,
      },
      defaults: {
        chainId: 0,
        pageSize: TRANSFERS_DEFAULT_PAGE_SIZE,
        sort: 'desc',
      },
      warm: {
        chains: warmSummary.chains,
        timestamp: warmSummary.timestamp,
      },
      chains: chainSummaries,
      availableChains: [{ id: 0, name: 'All Chains' }, ...CHAINS.map((c) => ({ id: c.id, name: c.name }))],
      request: {
        forceRefresh,
        includeTotals,
      },
    });
  } catch (error) {
    console.error('Error handling aggregated transfers request:', error.message || error);
    return res.status(500).json({
      message: 'Failed to fetch aggregated transfers',
      error: error.message || String(error),
    });
  }
};

/**
 * [Milestone 2.2]
 * Fetches ERC-20 token transfers for a single chain.
 * @param {object} chain - A chain object { id, name }
 * @returns {Promise<Array>} A promise that resolves to an array of transactions.
 */
const fetchTransfersPageFromChain = async ({ chain, page, pageSize, sort, startBlock, endBlock }) => {
  if (isCronosChain(chain)) {
    return fetchCronosTransfersPage({
      chain,
      page,
      pageSize,
      sort,
      startBlock,
      endBlock,
    });
  }

  const baseParams = {
    module: 'account',
    action: 'tokentx',
    contractaddress: BZR_ADDRESS,
    page,
    offset: pageSize,
    sort,
  };

  if (typeof startBlock === 'number') {
    baseParams.startblock = startBlock;
  }
  if (typeof endBlock === 'number') {
    baseParams.endblock = endBlock;
  }

  const { provider, params } = buildProviderRequest(chain, baseParams);

  try {
    const response = await axios.get(provider.baseUrl, { params });
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

    if (payload.status === '0' || String(payload?.message || '').toUpperCase() === 'NOTOK') {
      const errorMessage = payload?.message || payload?.result || '';
      const errorString = String(errorMessage);

      // Log the full error for debugging
      console.warn(`! Etherscan API error for ${chain.name}:`, errorString, 'Full payload:', JSON.stringify(payload));

      if (isProOnlyResponse(payload)) {
        const error = new Error(payload?.result || payload?.message || 'Etherscan PRO plan required');
        error.code = 'ETHERSCAN_PRO_ONLY';
        error.payload = payload;
        throw error;
      }

      const noRecordsMessage = errorString.toLowerCase();
      if (noRecordsMessage.includes('no transactions') || noRecordsMessage.includes('no records found')) {
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

      const error = new Error(errorString || 'Failed to fetch token transfers');
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
  if (isCronosChain(chain)) {
    return fetchCronosTransfersTotalCount({
      chain,
      startBlock,
      endBlock,
    });
  }

  // [Master-level fix] Respect Etherscan window limit (pageNo × offset ≤ 10,000)
  // Use maximum safe offset to get best total estimate
  const safeOffset = ETHERSCAN_RESULT_WINDOW; // 10,000 is the max we can request

  const baseParams = {
    module: 'account',
    action: 'tokentx',
    contractaddress: BZR_ADDRESS,
    page: 1,
    offset: safeOffset,
    sort,
  };

  if (typeof startBlock === 'number') {
    baseParams.startblock = startBlock;
  }
  if (typeof endBlock === 'number') {
    baseParams.endblock = endBlock;
  }

  const { provider, params } = buildProviderRequest(chain, baseParams);

  try {
    const response = await axios.get(provider.baseUrl, { params });
    const payload = response?.data || {};

    if (payload.status === '1') {
      const numericCount = Number(payload.count);
      const hasNumericCount = Number.isSafeInteger(numericCount) && numericCount >= 0;
      const resultLength = Array.isArray(payload.result) ? payload.result.length : 0;
      const total = hasNumericCount ? numericCount : resultLength;
      
      // If we got exactly 10k results, there are likely more
      const windowCapped = resultLength >= safeOffset;
      const truncated = hasNumericCount
        ? numericCount > safeOffset
        : windowCapped;

      if (windowCapped) {
        console.warn(`! Chain ${chain.name} has more than ${safeOffset} transfers. Total count may be underestimated.`);
      }

      return {
        total,
        truncated,
        windowCapped,
        timestamp: Date.now(),
        resultLength,
        maxSafeOffset: safeOffset,
      };
    }

    if (payload.status === '0' || String(payload?.message || '').toUpperCase() === 'NOTOK') {
      const errorMessage = payload?.message || payload?.result || '';
      const errorString = String(errorMessage);

      // Log the full error for debugging
      console.warn(`! Etherscan API error for ${chain.name} (totals):`, errorString, 'Full payload:', JSON.stringify(payload));

      if (isProOnlyResponse(payload)) {
        const error = new Error(payload?.result || payload?.message || 'Etherscan PRO plan required');
        error.code = 'ETHERSCAN_PRO_ONLY';
        error.payload = payload;
        throw error;
      }

      const noRecordsMessage = String(payload?.message || payload?.result || '').toLowerCase();
      if (noRecordsMessage.includes('no transactions') || noRecordsMessage.includes('no records')) {
        return {
          total: 0,
          truncated: false,
          windowCapped: false,
          timestamp: Date.now(),
          resultLength: 0,
        };
      }

      // Check if it's the result window error
      const windowErrorKeywords = ['result window', 'too large', 'must be less than'];
      const isWindowError = windowErrorKeywords.some(keyword => 
        errorString.toLowerCase().includes(keyword)
      );

      if (isWindowError) {
        console.warn(`! Chain ${chain.name} exceeded result window. Returning estimated total.`);
        return {
          total: safeOffset,
          truncated: true,
          windowCapped: true,
          timestamp: Date.now(),
          resultLength: safeOffset,
          error: 'RESULT_WINDOW_EXCEEDED',
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
  if (isCronosChain(chain)) {
    console.warn('! Cronos tokenholdercount endpoint unavailable – defaulting to 0');
    return { chainName: chain.name, chainId: chain.id, holderCount: 0, unsupported: true };
  }

  const { provider, params } = buildProviderRequest(chain, {
    module: 'token',
    action: 'tokenholdercount',
    contractaddress: BZR_ADDRESS,
  });

  try {
    const response = await axios.get(provider.baseUrl, { params });
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
    apikey: ETHERSCAN_API_KEY,
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

const fetchTokenPriceFromCoingecko = async () => {
  if (!TOKEN_PRICE_COINGECKO_ENABLED) {
    const error = new Error('CoinGecko price source disabled');
    error.code = 'COINGECKO_DISABLED';
    throw error;
  }

  const endpoint = 'https://api.coingecko.com/api/v3/simple/price';
  const params = {
    ids: TOKEN_PRICE_COINGECKO_ID,
    vs_currencies: 'usd',
    include_last_updated_at: 'true',
  };

  try {
    const response = await axios.get(endpoint, {
      params,
      timeout: TOKEN_PRICE_COINGECKO_TIMEOUT_MS,
    });

    const payload = response?.data || {};
    const entry = payload?.[TOKEN_PRICE_COINGECKO_ID];
    const usdRaw = entry?.usd;
    const priceUsd = parsePositiveNumber(usdRaw);

    if (!priceUsd || priceUsd <= 0) {
      throw new Error('CoinGecko returned no USD price');
    }

    const lastUpdatedAt = Number(entry?.last_updated_at);
    const timestamp = Number.isFinite(lastUpdatedAt) && lastUpdatedAt > 0
      ? lastUpdatedAt * 1000
      : Date.now();

    return {
      available: true,
      priceUsd,
      priceUsdRaw: typeof usdRaw === 'string' ? usdRaw : String(priceUsd),
      source: `coingecko:${TOKEN_PRICE_COINGECKO_ID}`,
      timestamp,
      proRequired: false,
      message: 'Price provided by CoinGecko',
    };
  } catch (error) {
    if (error.response?.data) {
      const wrapped = new Error(error.message || 'CoinGecko request failed');
      wrapped.code = 'COINGECKO_HTTP_ERROR';
      wrapped.payload = error.response.data;
      throw wrapped;
    }

    throw error;
  }
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
  let coingeckoMessage = null;

  if (TOKEN_PRICE_COINGECKO_ENABLED) {
    try {
      const coingeckoPayload = await fetchTokenPriceFromCoingecko();
      if (
        coingeckoPayload.available &&
        typeof coingeckoPayload.priceUsd === 'number' &&
        coingeckoPayload.priceUsd > 0
      ) {
        return coingeckoPayload;
      }

      coingeckoMessage = coingeckoPayload.message || 'CoinGecko returned no usable price data';
    } catch (error) {
      coingeckoMessage = error?.message || String(error);
    }
  }

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
    if (coingeckoMessage) {
      fallbackPayload.message = [fallbackPayload.message, `CoinGecko fallback: ${coingeckoMessage}`]
        .filter(Boolean)
        .join(' · ');
    }
    return fallbackPayload;
  } catch (fallbackError) {
    if (primaryPayload) {
      return {
        ...primaryPayload,
        available: primaryPayload.available && typeof primaryPayload.priceUsd === 'number' && primaryPayload.priceUsd > 0,
        message: [
          primaryPayload.message,
          coingeckoMessage ? `CoinGecko fallback: ${coingeckoMessage}` : null,
          `Fallback failed: ${fallbackError.message || fallbackError}`,
        ]
          .filter(Boolean)
          .join(' · '),
      };
    }

    if (primaryError) {
      const aggregate = new Error('Unable to fetch token price from Etherscan and fallback provider');
      aggregate.cause = {
        primaryError: primaryError.message || primaryError,
        fallbackError: fallbackError.message || fallbackError,
        coingeckoError: coingeckoMessage,
      };
      throw aggregate;
    }

    if (coingeckoMessage) {
      const aggregate = new Error('Unable to fetch token price from CoinGecko and fallback provider');
      aggregate.cause = {
        coingeckoError: coingeckoMessage,
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
    apikey: ETHERSCAN_API_KEY,
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
app.get('/api/info', strictLimiter, cacheMiddleware(300), async (req, res) => {
  console.log(`[${new Date().toISOString()}] Received request for /api/info`);
  const now = Date.now();
  if (cache.info && (now - cache.infoTimestamp < FIVE_MINUTES)) {
    console.log('-> Returning cached /api/info data.');
    return res.json(cache.info);
  }

  if (!ETHERSCAN_API_KEY || !BZR_ADDRESS) {
    return res.status(500).json({ message: 'Server is missing ETHERSCAN_V2_API_KEY or BZR_TOKEN_ADDRESS' });
  }

  // We only need to get this info from one chain, so we'll use Ethereum (chainid=1)
  const params = {
    chainid: 1, // Ethereum Mainnet
    apikey: ETHERSCAN_API_KEY,
  };

  // We will make three API calls in parallel to get all the info we need

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

  // Call 3: Get detailed token info including circulating supply
  const tokenInfoParams = {
    ...params,
    module: 'token',
    action: 'tokeninfo',
    contractaddress: BZR_ADDRESS,
  };

  try {
    console.log('-> Fetching new /api/info data from Etherscan...');
    // Run all three requests in parallel
    const [supplyResponse, txResponse, tokenInfoResponse] = await Promise.all([
      axios.get(API_V2_BASE_URL, { params: supplyParams }),
      axios.get(API_V2_BASE_URL, { params: txParams }),
      axios.get(API_V2_BASE_URL, { params: tokenInfoParams }),
    ]);

    // Check for API errors
    if (supplyResponse.data.status !== '1' || txResponse.data.status !== '1') {
      console.error('Etherscan API Error:', supplyResponse.data.message, txResponse.data.message);
      return respondUpstreamFailure(res, 'Upstream Etherscan API error while fetching token info', {
        supplyError: supplyResponse.data.message,
        txError: txResponse.data.message,
        tokenInfoError: tokenInfoResponse.data.message,
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
    
    // 3. Data from Token Info call (includes circulating supply)
    let circulatingSupply = null;
    let formattedCirculatingSupply = null;
    if (tokenInfoResponse.data.status === '1' && Array.isArray(tokenInfoResponse.data.result)) {
      const tokenInfoData = tokenInfoResponse.data.result[0];
      if (tokenInfoData && tokenInfoData.circulatingSupply) {
        circulatingSupply = tokenInfoData.circulatingSupply;
        try {
          formattedCirculatingSupply = (BigInt(circulatingSupply) / BigInt(10 ** parseInt(tokenDecimal, 10))).toString();
        } catch (e) {
          console.warn('! Could not format circulating supply:', e.message);
        }
      }
    }
    
    // --- Combine and Send ---
    const tokenInfo = {
      tokenName,
      tokenSymbol,
      tokenDecimal: parseInt(tokenDecimal, 10),
      totalSupply,
      circulatingSupply,
      // We add helpers to format the supply on the frontend
      formattedTotalSupply: (BigInt(totalSupply) / BigInt(10 ** parseInt(tokenDecimal, 10))).toString(),
      formattedCirculatingSupply,
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

  if (!BZR_ADDRESS) {
    return res.status(500).json({ message: 'Server missing BZR_TOKEN_ADDRESS' });
  }

  const forceRefresh = String(req.query.force).toLowerCase() === 'true';
  const requestedChainId = Number(req.query.chainId || 0);
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

  // Special handling for "All Chains" aggregation (chainId = 0)
  const isAggregatedView = requestedChainId === 0;

  if (isAggregatedView) {
    return handleAggregatedTransfers(req, res, {
      forceRefresh,
      requestedPage,
      requestedPageSize,
      sort,
      startBlock,
      endBlock,
      includeTotals,
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
    getProviderConfigForChain(chain);
  } catch (providerError) {
    return res.status(500).json({ message: providerError.message });
  }

  const chainIsCronos = isCronosChain(chain);
  const resultWindowLimit = !chainIsCronos && Number.isFinite(ETHERSCAN_RESULT_WINDOW)
    ? Math.max(0, ETHERSCAN_RESULT_WINDOW)
    : null;
  const maxWindowPagesForRequest = resultWindowLimit
    ? Math.max(1, Math.floor(resultWindowLimit / requestedPageSize) || 1)
    : null;
  const requestExceedsWindow = Boolean(resultWindowLimit && requestedPage > maxWindowPagesForRequest);

  try {
    const pagePromise = requestExceedsWindow
      ? Promise.resolve({
          transfers: [],
          upstream: null,
          timestamp: Date.now(),
          page: requestedPage,
          pageSize: requestedPageSize,
          sort,
          startBlock,
          endBlock,
          resultLength: 0,
          windowExceeded: true,
        })
      : resolveTransfersPageData({
          chain,
          page: requestedPage,
          pageSize: requestedPageSize,
          sort,
          startBlock,
          endBlock,
          forceRefresh,
        });

    const totalsPromise = includeTotals
      ? resolveTransfersTotalData({
          chain,
          sort,
          startBlock,
          endBlock,
          forceRefresh,
        })
      : Promise.resolve(null);

    const [pageOutcome, totalsOutcome] = await Promise.allSettled([pagePromise, totalsPromise]);

    if (pageOutcome.status !== 'fulfilled') {
      throw pageOutcome.reason;
    }

    const pageData = pageOutcome.value;
    const warmSummary = getCachedTransfersWarmSummary();
    const warnings = [];

    let totalsData = null;
    if (totalsOutcome.status === 'fulfilled') {
      totalsData = totalsOutcome.value;
      
      // Add warning if totals are window-capped
      if (totalsData?.windowCapped) {
        warnings.push({
          scope: 'total',
          code: 'TOTAL_COUNT_CAPPED',
          message: `This chain has more than ${totalsData.maxSafeOffset || ETHERSCAN_RESULT_WINDOW} transfers. Total count may be underestimated due to Etherscan result window limits.`,
          retryable: false,
        });
      }
    } else if (includeTotals) {
      const reason = totalsOutcome.reason || {};
      console.warn(`! Failed to refresh transfer totals for ${chain.name}: ${reason.message || reason}`);
      warnings.push({
        scope: 'total',
        code: reason.code || 'TOTAL_FETCH_FAILED',
        message: reason.message || 'Failed to compute total transfer count; returning latest page data only.',
        retryable: true,
      });
    }

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

    const windowExceeded = Boolean(pageData.windowExceeded || requestExceedsWindow);
    if (windowExceeded && resultWindowLimit) {
      warnings.push({
        scope: 'page',
        code: 'RESULT_WINDOW_CAP',
        message: `Etherscan only returns the latest ${resultWindowLimit.toLocaleString()} transfers per query. Reduce the page size or apply block filters to view older activity.`,
      });
    }

    const totalCount = totalsData ? totalsData.total : pageData.resultLength || 0;
    const totalPagesRaw = totalCount > 0 ? Math.ceil(totalCount / requestedPageSize) : (pageData.resultLength === requestedPageSize ? requestedPage + 1 : requestedPage);
    const totalPages = maxWindowPagesForRequest
      ? Math.min(totalPagesRaw || maxWindowPagesForRequest, maxWindowPagesForRequest)
      : totalPagesRaw;
    let hasMore = pageData.resultLength === requestedPageSize;
    if (totalCount > 0) {
      hasMore = totalCount > requestedPage * requestedPageSize;
    }
    if (maxWindowPagesForRequest && requestedPage >= maxWindowPagesForRequest) {
      hasMore = false;
    }
    if (windowExceeded) {
      hasMore = false;
    }
    const timestamp = pageData.timestamp || pageData.cacheTimestamp || Date.now();

    res.json({
      data: Array.isArray(pageData.transfers) ? pageData.transfers : [],
      pagination: {
        page: requestedPage,
        pageSize: requestedPageSize,
        total: totalCount,
        totalPages,
        hasMore,
        windowExceeded,
        maxWindowPages: maxWindowPagesForRequest,
        resultWindow: resultWindowLimit,
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
        resultWindow: resultWindowLimit,
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

    const providerKey = getProviderKeyForChain(chain);
    const providerLabel = providerKey === 'cronos' ? 'Cronos explorer' : 'Etherscan';

    if (error.code === 'ETHERSCAN_PRO_ONLY') {
      return res.status(402).json({
        message: 'Etherscan PRO plan required for this request',
        details: error.payload || null,
      });
    }

    if (error.code && error.payload) {
      return respondUpstreamFailure(res, `Failed to fetch transfers from ${providerLabel}`, {
        upstreamProvider: providerKey,
        errorCode: error.code,
        upstreamResponse: error.payload,
      });
    }

    return res.status(500).json({
      message: `Failed to fetch transfers from ${providerLabel}`,
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

// [Phase 2.1] - Token Holders List
app.get('/api/holders', strictLimiter, cacheMiddleware(180), async (req, res) => {
  console.log(`[${new Date().toISOString()}] Received request for /api/holders`);
  
  if (!BZR_ADDRESS) {
    return res.status(500).json({ message: 'Server missing BZR_TOKEN_ADDRESS' });
  }

  const requestedChainId = Number(req.query.chainId || 1); // Default to Ethereum
  const page = Math.max(1, Number(req.query.page || 1));
  const pageSize = Math.min(100, Math.max(10, Number(req.query.pageSize || 50))); // 10-100, default 50
  
  const chain = getChainDefinition(requestedChainId);
  if (!chain) {
    return res.status(400).json({
      message: 'Invalid chain ID',
      chainId: requestedChainId,
      availableChains: CHAINS,
    });
  }

  // Cronos doesn't support tokenholderlist
  if (isCronosChain(chain)) {
    return res.status(501).json({
      message: 'Cronos chain does not support token holder list',
      chainId: requestedChainId,
      chainName: chain.name,
    });
  }

  try {
    const provider = getProviderConfigForChain(chain);
    const response = await axios.get(provider.baseUrl, {
      params: {
        chainid: chain.id,
        apikey: provider.apiKey,
        module: 'token',
        action: 'tokenholderlist',
        contractaddress: BZR_ADDRESS,
        page,
        offset: pageSize,
      },
      timeout: 30000,
    });

    if (response.data.status !== '1') {
      console.error(`X Etherscan tokenholderlist error for ${chain.name}:`, response.data.message);
      return res.status(502).json({
        message: response.data.message || 'Failed to fetch holders from Etherscan',
        chainId: chain.id,
        chainName: chain.name,
      });
    }

    const holders = Array.isArray(response.data.result) ? response.data.result : [];
    
    console.log(`-> Fetched ${holders.length} holders for ${chain.name} (page ${page})`);
    
    res.json({
      data: holders,
      chain: {
        id: chain.id,
        name: chain.name,
      },
      pagination: {
        page,
        pageSize,
        resultCount: holders.length,
      },
      timestamp: Date.now(),
    });
  } catch (error) {
    console.error(`Error fetching holders for ${chain.name}:`, error.message);
    if (error.response?.data) {
      return respondUpstreamFailure(res, 'Failed to fetch token holders from Etherscan', {
        upstreamResponse: error.response.data,
      });
    }
    res.status(500).json({ message: 'Failed to fetch holders', error: error.message });
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

app.get('/api/token-price', cacheMiddleware(60), async (req, res) => {
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

// Cache invalidation endpoint - useful for forcing fresh data after contract changes
app.post('/api/cache/invalidate', (req, res) => {
  console.log(`[${new Date().toISOString()}] Cache invalidation requested`);
  
  // Clear all caches
  cache.info = null;
  cache.infoTimestamp = 0;
  cache.stats = null;
  cache.statsTimestamp = 0;
  cache.tokenPrice = null;
  cache.tokenPriceTimestamp = 0;
  cache.finalizedBlock = null;
  cache.finalizedBlockTimestamp = 0;
  
  // Clear transfers caches
  cache.transfersPageCache.clear();
  cache.transfersTotalCache.clear();
  cache.transfersWarmStatus = [];
  cache.transfersWarmTimestamp = null;
  
  // Clear in-flight promises
  transfersPagePromises.clear();
  transfersTotalPromises.clear();
  
  console.log('-> All caches cleared successfully');
  console.log(`-> Current BZR Token Address: ${BZR_ADDRESS}`);
  
  // Trigger immediate cache warm
  if (CACHE_WARM_INTERVAL_MS > 0) {
    console.log('-> Triggering immediate cache warm...');
    triggerTransfersRefresh({ forceRefresh: true }).catch((error) => {
      console.error('X Cache warm after invalidation failed:', error.message || error);
    });
  }
  
  res.json({
    message: 'All caches invalidated successfully',
    tokenAddress: BZR_ADDRESS,
    timestamp: Date.now(),
  });
});

// --- Start Server ---
app.listen(PORT, () => {
  console.log(`BZR Backend server listening on http://localhost:${PORT}`);
  
  if (!ETHERSCAN_API_KEY) {
    console.warn('---');
    console.warn('WARNING: ETHERSCAN_V2_API_KEY is not set in your .env file.');
    console.warn('The API calls will fail until this is set.');
    console.warn('---');
  } else {
    // Mask API key in logs for security
    const maskedKey = '***' + ETHERSCAN_API_KEY.slice(-4);
    console.log(`Etherscan API key loaded successfully (${maskedKey}).`);
  }

  if (!CRONOS_API_KEY) {
    console.warn('---');
    console.warn('WARNING: CRONOS_API_KEY is not set in your .env file.');
    console.warn('Cronos chain requests will fail until this is configured.');
    console.warn('---');
  } else {
    // Mask API key in logs for security
    const maskedKey = '***' + CRONOS_API_KEY.slice(-4);
    console.log(`Cronos API key loaded successfully (${maskedKey}).`);
  }

  if (!BZR_ADDRESS) {
    console.warn('WARNING: BZR_TOKEN_ADDRESS is not set. Please set it in .env');
  } else {
    console.log(`Tracking BZR Token: ${BZR_ADDRESS}`);
  }

  if (CACHE_WARM_INTERVAL_MS > 0) {
    console.log(`Cache warming enabled (interval ${CACHE_WARM_INTERVAL_MS}ms).`);

    triggerTransfersRefresh({ forceRefresh: true }).catch((error) => {
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