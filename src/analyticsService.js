const {
  isPersistentStoreReady,
  queryDailyAnalytics,
  queryAnalyticsSummary,
  queryChainDistribution,
  queryTopAddresses,
  queryTopTransfers,
  getMaxTimestamp,
} = require('./persistentStore');

const TIME_RANGE_TO_DAYS = {
  '7d': 7,
  '30d': 30,
  '90d': 90,
};

const formattersCache = new Map();

const getDisplayFormatter = (locale = 'en-US') => {
  if (!formattersCache.has(locale)) {
    formattersCache.set(
      locale,
      new Intl.DateTimeFormat(locale, {
        month: 'short',
        day: 'numeric',
      })
    );
  }
  return formattersCache.get(locale);
};

const convertRawToToken = (value, decimals) => {
  if (!value) {
    return 0;
  }

  const numeric = typeof value === 'string' ? Number(value) : Number(value || 0);
  if (!Number.isFinite(numeric)) {
    return 0;
  }

  const divisor = 10 ** Math.max(0, Number.parseInt(decimals, 10) || 18);
  return numeric / divisor;
};

const clamp = (value, min, max) => {
  return Math.min(Math.max(value, min), max);
};

const roundNumber = (value, precision = 2) => {
  if (!Number.isFinite(value)) {
    return 0;
  }
  const factor = 10 ** precision;
  return Math.round(value * factor) / factor;
};

const createTimeline = (startDate, endDate) => {
  const timeline = [];
  const cursor = new Date(startDate);
  cursor.setUTCHours(0, 0, 0, 0);
  const boundary = new Date(endDate);
  boundary.setUTCHours(0, 0, 0, 0);

  while (cursor <= boundary) {
    timeline.push(new Date(cursor));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }

  return timeline;
};

const buildPredictionSeries = (values = [], length = 7) => {
  if (!Array.isArray(values) || values.length === 0 || length <= 0) {
    return [];
  }

  const recent = values.slice(-Math.max(2, Math.min(values.length, 5)));
  const deltas = [];
  for (let i = 1; i < recent.length; i += 1) {
    const prev = recent[i - 1];
    const next = recent[i];
    deltas.push(next - prev);
  }

  const avgDelta = deltas.length ? deltas.reduce((acc, value) => acc + value, 0) / deltas.length : 0;
  const baseline = recent[recent.length - 1];
  const predictions = [];

  for (let step = 1; step <= length; step += 1) {
    const projected = Math.max(0, baseline + avgDelta * step);
    predictions.push(roundNumber(projected, 2));
  }

  return predictions;
};

const computeAnomalies = (values = []) => {
  if (!Array.isArray(values) || values.length < 3) {
    return [];
  }

  const mean = values.reduce((acc, value) => acc + value, 0) / values.length;
  const variance = values.reduce((acc, value) => acc + (value - mean) ** 2, 0) / values.length;
  const stdDev = Math.sqrt(variance);

  if (stdDev === 0) {
    return [];
  }

  return values
    .map((value, index) => {
      const zScore = (value - mean) / stdDev;
      if (Math.abs(zScore) >= 2) {
        return {
          index,
          value,
          zScore: zScore.toFixed(2),
        };
      }
      return null;
    })
    .filter(Boolean);
};

const computePercentChange = (current, previous) => {
  if (!Number.isFinite(current) || !Number.isFinite(previous)) {
    return null;
  }
  if (previous === 0) {
    return current === 0 ? 0 : 100;
  }
  return roundNumber(((current - previous) / previous) * 100, 2);
};

const getChainName = (chains, chainId) => {
  const numericId = Number(chainId);
  const found = Array.isArray(chains) ? chains.find((chain) => Number(chain.id) === numericId) : null;
  return found?.name || `Chain ${numericId}`;
};

const ensurePersistentStoreReady = () => {
  if (!isPersistentStoreReady()) {
    const error = new Error('Persistent store is not ready');
    error.code = 'PERSISTENT_STORE_UNAVAILABLE';
    throw error;
  }
};

const computePersistentAnalytics = async ({
  timeRange,
  chainIds,
  chains,
  decimals,
}) => {
  ensurePersistentStoreReady();

  const now = new Date();
  const normalizedEnd = new Date(now);
  normalizedEnd.setUTCHours(23, 59, 59, 999);

  const rangeDays = TIME_RANGE_TO_DAYS[timeRange] || null;
  let normalizedStart = null;

  if (rangeDays) {
    normalizedStart = new Date(normalizedEnd);
    normalizedStart.setUTCHours(0, 0, 0, 0);
    normalizedStart.setUTCDate(normalizedStart.getUTCDate() - (rangeDays - 1));
  }

  const startTimeMs = Date.now();

  const [dailyRows, summary, chainRows, topAddressRows, topTransferRows, latestTimestamp] = await Promise.all([
    queryDailyAnalytics({ chainId: chainIds, startTime: normalizedStart, endTime: normalizedEnd }),
    queryAnalyticsSummary({ chainId: chainIds, startTime: normalizedStart, endTime: normalizedEnd }),
    queryChainDistribution({ chainId: chainIds, startTime: normalizedStart, endTime: normalizedEnd }),
    queryTopAddresses({ chainId: chainIds, startTime: normalizedStart, endTime: normalizedEnd, limit: 15 }),
    queryTopTransfers({ chainId: chainIds, startTime: normalizedStart, endTime: normalizedEnd, limit: 15 }),
    getMaxTimestamp({ chainId: chainIds }),
  ]);

  let previousSummary = { totalTransfers: 0, volumeRaw: 0, uniqueAddresses: 0 };
  if (rangeDays && normalizedStart) {
    const previousEnd = new Date(normalizedStart);
    previousEnd.setUTCDate(previousEnd.getUTCDate() - 1);
    previousEnd.setUTCHours(23, 59, 59, 999);
    const previousStart = new Date(previousEnd);
    previousStart.setUTCDate(previousStart.getUTCDate() - (rangeDays - 1));
    previousStart.setUTCHours(0, 0, 0, 0);

    previousSummary = await queryAnalyticsSummary({
      chainId: chainIds,
      startTime: previousStart,
      endTime: previousEnd,
    });
  }

  const timeline = normalizedStart ? createTimeline(normalizedStart, normalizedEnd) : dailyRows.map((row) => row.day);
  const timelineKeys = timeline.map((date) => {
    const copy = new Date(date);
    copy.setUTCHours(0, 0, 0, 0);
    return copy.toISOString().split('T')[0];
  });

  const dailyMap = new Map();
  dailyRows.forEach((row) => {
    if (!row.day) {
      return;
    }
    const date = new Date(row.day);
    date.setUTCHours(0, 0, 0, 0);
    const key = date.toISOString().split('T')[0];
    dailyMap.set(key, row);
  });

  const formatter = getDisplayFormatter();
  const dailyData = [];
  const dailyTransferCounts = [];
  const dailyVolumeValues = [];

  timelineKeys.forEach((key, index) => {
    const raw = dailyMap.get(key);
    const date = timeline[index] instanceof Date ? timeline[index] : new Date(key);
    const count = raw?.transferCount || 0;
    const volume = convertRawToToken(raw?.volumeRaw, decimals);
    const medianTransferSize = raw?.medianRaw ? convertRawToToken(raw.medianRaw, decimals) : 0;
    const uniqueAddresses = raw
      ? new Set([...(raw.fromAddresses || []), ...(raw.toAddresses || [])]).size
      : 0;

    const entry = {
      date: key,
      displayDate: formatter.format(date),
      count,
      volume: roundNumber(volume, 4),
      uniqueAddresses,
      avgTransferSize: count > 0 ? roundNumber(volume / count, 4) : 0,
      medianTransferSize,
    };

    dailyData.push(entry);
    dailyTransferCounts.push(count);
    dailyVolumeValues.push(volume);
  });

  const totalTransfers = summary.totalTransfers || dailyTransferCounts.reduce((acc, value) => acc + value, 0);
  const totalVolume = convertRawToToken(summary.volumeRaw, decimals);
  const activeAddresses = summary.uniqueAddresses || 0;
  const daysCount = dailyData.length || 1;

  const sortedCounts = [...dailyTransferCounts].sort((a, b) => a - b);
  const middleIndex = Math.floor(sortedCounts.length / 2);
  const medianDailyTransfers = sortedCounts.length % 2 === 0
    ? (sortedCounts[middleIndex - 1] + sortedCounts[middleIndex]) / 2
    : sortedCounts[middleIndex] || 0;

  const transfersChange = rangeDays ? computePercentChange(totalTransfers, previousSummary.totalTransfers || 0) : null;
  const volumeChange = rangeDays ? computePercentChange(totalVolume, convertRawToToken(previousSummary.volumeRaw, decimals)) : null;
  const addressesChange = rangeDays ? computePercentChange(activeAddresses, previousSummary.uniqueAddresses || 0) : null;

  let peakActivity = null;
  dailyData.forEach((entry) => {
    if (!peakActivity || entry.count > peakActivity.transfers || entry.volume > peakActivity.volume) {
      peakActivity = {
        transfers: entry.count,
        volume: entry.volume,
        date: entry.date,
      };
    }
  });

  const volatility = (() => {
    if (dailyTransferCounts.length < 2) {
      return 0;
    }
    const mean = dailyTransferCounts.reduce((acc, value) => acc + value, 0) / dailyTransferCounts.length;
    const variance = dailyTransferCounts.reduce((acc, value) => acc + (value - mean) ** 2, 0) / dailyTransferCounts.length;
    return roundNumber(Math.sqrt(variance), 2);
  })();

  const predictions = {
    transfers: buildPredictionSeries(dailyTransferCounts, clamp(Math.ceil(daysCount / 4), 3, 7)),
    volume: buildPredictionSeries(dailyVolumeValues, clamp(Math.ceil(daysCount / 4), 3, 7)),
  };

  const anomalies = {
    transferSpikes: computeAnomalies(dailyTransferCounts),
    volumeSpikes: computeAnomalies(dailyVolumeValues),
  };

  const chainDistribution = chainRows.map((row) => {
    const volume = convertRawToToken(row.volumeRaw, decimals);
    const percentage = totalVolume > 0 ? `${roundNumber((volume / totalVolume) * 100, 2)}%` : '0%';
    return {
      chain: getChainName(chains, row.chainId),
      count: row.transferCount,
      volume: roundNumber(volume, 4),
      uniqueAddresses: row.uniqueAddresses,
      percentage,
    };
  });

  const topAddresses = topAddressRows.map((row) => ({
    address: row.address,
    totalTxs: row.totalTxs,
    sent: row.sent,
    received: row.received,
    volume: roundNumber(convertRawToToken(row.volumeRaw, decimals), 4),
  }));

  const topWhales = topTransferRows.map((row) => ({
    hash: row.txHash,
    from: row.from,
    to: row.to,
    value: roundNumber(convertRawToToken(row.volumeRaw, decimals), 4),
    timeStamp: row.timestamp || null,
    chain: getChainName(chains, row.chainId),
  }));

  const computeTimeMs = Date.now() - startTimeMs;

  return {
    analyticsData: {
      success: true,
      timeRange,
      chainId: Array.isArray(chainIds) && chainIds.length === 1 ? String(chainIds[0]) : 'all',
      dailyData,
      analyticsMetrics: {
        totalTransfers,
        totalVolume: roundNumber(totalVolume, 4),
        avgTransferSize: totalTransfers > 0 ? roundNumber(totalVolume / totalTransfers, 4) : 0,
        activeAddresses,
        transfersChange,
        volumeChange,
        addressesChange,
        dailyAvgTransfers: roundNumber(totalTransfers / daysCount, 2),
        dailyAvgVolume: roundNumber(totalVolume / daysCount, 4),
        peakActivity,
        volatility,
        medianDailyTransfers: roundNumber(medianDailyTransfers, 2),
      },
      predictions,
      anomalies,
      chainDistribution,
      topAddresses,
      topWhales,
      performance: {
        computeTimeMs,
        dataPoints: dailyData.length,
        totalTransfersAnalyzed: totalTransfers,
        cacheStatus: 'persistent-cache',
        cacheAge: latestTimestamp?.lagSeconds ?? null,
      },
      timestamp: Date.now(),
    },
    totals: {
      totalTransfers,
      totalVolume,
      activeAddresses,
    },
  };
};

module.exports = {
  computePersistentAnalytics,
  convertRawToToken,
  roundNumber,
  buildPredictionSeries,
  computeAnomalies,
  TIME_RANGE_TO_DAYS,
};
