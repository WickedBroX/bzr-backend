'use strict';

const axios = require('axios');

const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000';
const TRANSFER_EVENT_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';

const BZR_ADDRESS = process.env.BZR_TOKEN_ADDRESS;
const BZR_TOKEN_NAME = process.env.BZR_TOKEN_NAME || 'Bazaars';
const BZR_TOKEN_SYMBOL = process.env.BZR_TOKEN_SYMBOL || 'BZR';
const BZR_TOKEN_DECIMALS = Number(process.env.BZR_TOKEN_DECIMALS || 18);

const API_V2_BASE_URL = process.env.ETHERSCAN_V2_BASE_URL || 'https://api.etherscan.io/v2/api';
const CRONOS_API_KEY = process.env.CRONOS_API_KEY;
const CRONOS_API_BASE_URL = process.env.CRONOS_API_BASE_URL || 'https://explorer-api.cronos.org/mainnet/api/v2';

const CRONOS_MAX_LOG_BLOCK_RANGE = Number(process.env.CRONOS_LOG_BLOCK_RANGE || 9_999);
const CRONOS_MAX_LOG_ITERATIONS = Number(process.env.CRONOS_LOG_MAX_ITERATIONS || 12);
const CRONOS_TOTAL_MAX_ITERATIONS = Number(process.env.CRONOS_TOTAL_LOG_MAX_ITERATIONS || 60);

const API_KEYS_RAW = process.env.ETHERSCAN_V2_API_KEY || '';
const ETHERSCAN_API_KEYS = (() => {
  const keys = (API_KEYS_RAW.includes(',')
    ? API_KEYS_RAW.split(',').map((value) => value.trim())
    : [API_KEYS_RAW]).filter((value) => value.length > 0);
  return keys.length > 0 ? keys : [''];
})();
let currentKeyIndex = 0;

const getNextApiKey = () => {
  if (ETHERSCAN_API_KEYS.length === 0) {
    return '';
  }

  const key = ETHERSCAN_API_KEYS[currentKeyIndex] || '';
  currentKeyIndex = (currentKeyIndex + 1) % ETHERSCAN_API_KEYS.length;
  return key;
};

const PROVIDERS = {
  etherscan: {
    apiKey: ETHERSCAN_API_KEYS[0] || '',
    baseUrl: API_V2_BASE_URL,
    requiresChainId: true,
  },
  cronos: {
    apiKey: CRONOS_API_KEY,
    baseUrl: CRONOS_API_BASE_URL,
    requiresChainId: false,
  },
};

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
    return value.startsWith('0x') || value.startsWith('0X') ? value : `0x${value}`;
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

const getChainDefinition = (chainId) => CHAINS.find((chain) => chain.id === chainId);

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
    if (provider.key === 'etherscan') {
      nextParams.apikey = getNextApiKey();
    } else {
      nextParams.apikey = provider.apiKey;
    }
  }

  if (provider.requiresChainId) {
    nextParams.chainid = chain.id;
  }

  return {
    provider,
    params: nextParams,
  };
};

const clampTransfersPageSize = (value) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return 25;
  }

  const max = Number(process.env.TRANSFERS_MAX_PAGE_SIZE || 100);
  const min = 1;
  return Math.max(min, Math.min(Math.floor(numeric), max));
};

const normalizePageNumber = (value) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return 1;
  }

  return Math.floor(numeric);
};

const sanitizeTransfers = (transfers, chain) => {
  if (!Array.isArray(transfers)) {
    return [];
  }

  const sanitized = [];
  for (const tx of transfers) {
    if (!tx || typeof tx.timeStamp === 'undefined') {
      continue;
    }

    const numericTimestamp = Number(tx.timeStamp);
    if (!Number.isFinite(numericTimestamp)) {
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

const fetchCronosTransfersTotalCount = async ({ chain, startBlock, endBlock, limit = Number(process.env.TRANSFERS_TOTAL_FETCH_LIMIT || 25_000) }) => {
  const latestBlockNumber = await fetchLatestBlockNumberForChain(chain);
  const effectiveEnd = typeof endBlock === 'number' ? endBlock : latestBlockNumber;
  const effectiveStart = typeof startBlock === 'number' ? startBlock : 0;

  const isAllTimeQuery = typeof startBlock !== 'number' && typeof endBlock !== 'number';
  if (isAllTimeQuery) {
    return {
      total: limit,
      truncated: true,
      timestamp: Date.now(),
      resultLength: limit,
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
      if (total >= limit) {
        truncated = true;
        total = limit;
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

const fetchTransfersPageFromChain = async ({
  chain,
  page,
  pageSize,
  sort,
  startBlock,
  endBlock,
}) => {
  if (getProviderKeyForChain(chain) === 'cronos') {
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

const fetchTransfersTotalCount = async ({ chain, sort, startBlock, endBlock, limit = Number(process.env.TRANSFERS_TOTAL_FETCH_LIMIT || 25_000) }) => {
  if (getProviderKeyForChain(chain) === 'cronos') {
    return fetchCronosTransfersTotalCount({ chain, startBlock, endBlock, limit });
  }

  const baseParams = {
    module: 'account',
    action: 'tokentx',
    contractaddress: BZR_ADDRESS,
    sort,
    page: 1,
    offset: limit,
  };

  if (typeof startBlock === 'number') {
    baseParams.startblock = startBlock;
  }
  if (typeof endBlock === 'number') {
    baseParams.endblock = endBlock;
  }

  const { provider, params } = buildProviderRequest(chain, baseParams);

  const response = await axios.get(provider.baseUrl, { params });
  const payload = response?.data || {};

  if (payload.status === '1' && Array.isArray(payload.result)) {
    return {
      total: payload.result.length,
      truncated: payload.result.length >= limit,
      timestamp: Date.now(),
      resultLength: payload.result.length,
      upstream: payload,
    };
  }

  if (payload.status === '0' || String(payload?.message || '').toUpperCase() === 'NOTOK') {
    if (isProOnlyResponse(payload)) {
      const error = new Error(payload?.result || payload?.message || 'Etherscan PRO plan required');
      error.code = 'ETHERSCAN_PRO_ONLY';
      error.payload = payload;
      throw error;
    }

    const errorMessage = payload?.message || payload?.result || 'Failed to count transfers';
    const error = new Error(errorMessage);
    error.code = 'ETHERSCAN_TOTAL_ERROR';
    error.payload = payload;
    throw error;
  }

  const error = new Error('Unexpected response from Etherscan when counting transfers');
  error.code = 'ETHERSCAN_TOTAL_UNEXPECTED_RESPONSE';
  error.payload = payload;
  throw error;
};

module.exports = {
  BZR_ADDRESS,
  BZR_TOKEN_NAME,
  BZR_TOKEN_SYMBOL,
  BZR_TOKEN_DECIMALS,
  TRANSFER_EVENT_TOPIC,
  ZERO_ADDRESS,
  CHAINS,
  PROVIDERS,
  getChainDefinition,
  getProviderKeyForChain,
  getProviderConfigForChain,
  buildProviderRequest,
  sanitizeTransfers,
  fetchTransfersPageFromChain,
  fetchTransfersTotalCount,
  fetchCronosTransfersPage,
  fetchCronosTransfersTotalCount,
  clampTransfersPageSize,
  normalizePageNumber,
  isProOnlyResponse,
  getNextApiKey,
  hexToNumberSafe,
  hexToDecimalString,
};
