'use strict';

const pLimit = require('p-limit');
const {
  storeTransfers,
  updateIngestCursor,
  recordIngestEvent,
  getIngestCursor,
  refreshChainTotals,
  refreshChainAggregates,
  recordIngestStatusSuccess,
  recordIngestStatusFailure,
} = require('./persistentStore');

const INGEST_PAGE_SIZE = Number(process.env.TRANSFERS_INGEST_PAGE_SIZE || 100);
const INGEST_MAX_PAGES_PER_CYCLE = Number(process.env.TRANSFERS_INGEST_MAX_PAGES || 5);
const INGEST_BASE_INTERVAL_MS = Number(process.env.TRANSFERS_INGEST_INTERVAL_MS || 30_000);
const INGEST_MAX_BACKOFF_MS = Number(process.env.TRANSFERS_INGEST_MAX_BACKOFF_MS || 10 * 60 * 1000);
const INGEST_BACKOFF_FACTOR = Number(process.env.TRANSFERS_INGEST_BACKOFF_FACTOR || 2);
const INGEST_CONCURRENCY = Number(process.env.TRANSFERS_INGEST_CONCURRENCY || 2);
const INGEST_JITTER_MS = Number(process.env.TRANSFERS_INGEST_JITTER_MS || 5_000);
const INGEST_IDLE_DELAY_MS = Number(process.env.TRANSFERS_INGEST_IDLE_DELAY_MS || 5_000);

const chainTimers = new Map();
const chainStates = new Map();
let limiter = null;
let stopping = false;

const initializeLimiter = () => {
  if (!limiter) {
    limiter = pLimit(Math.max(1, INGEST_CONCURRENCY));
  }
  return limiter;
};

const sanitizeTimestamp = (value) => {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value === 'string') {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
  }

  return null;
};

const clearTimer = (chainId) => {
  if (chainTimers.has(chainId)) {
    clearTimeout(chainTimers.get(chainId));
    chainTimers.delete(chainId);
  }
};

const stopTransferIngestion = () => {
  stopping = true;
  for (const timer of chainTimers.values()) {
    clearTimeout(timer);
  }
  chainTimers.clear();
  chainStates.clear();
  limiter = null;
};

const runIngestionCycle = async ({ chain, fetchPage }) => {
  const limit = initializeLimiter();
  return limit(async () => {
    let inserted = 0;
    let page = 1;
    let hasMore = true;
    let pagesAttempted = 0;
    let newestTimestamp = null;
    let oldestTimestamp = null;
    let newestCursor = null;

    try {
      const cursor = await getIngestCursor(chain.id);
      const maxPages = Math.max(1, INGEST_MAX_PAGES_PER_CYCLE);

      while (hasMore && page <= maxPages) {
        pagesAttempted += 1;
        const pageResult = await fetchPage({
          chain,
          page,
          pageSize: INGEST_PAGE_SIZE,
          sort: 'desc',
        });

        const transfers = Array.isArray(pageResult?.transfers) ? pageResult.transfers : [];
        if (!transfers.length) {
          hasMore = false;
          break;
        }

        const storeResult = await storeTransfers(chain.id, transfers);
        inserted += storeResult.inserted || 0;

        const newest = transfers[0];
        const oldest = transfers[transfers.length - 1];

        const newestTs = sanitizeTimestamp(newest?.timeStamp ?? newest?.time_stamp);
        const oldestTs = sanitizeTimestamp(oldest?.timeStamp ?? oldest?.time_stamp);

        if (typeof newestTs === 'number') {
          newestTimestamp = newestTimestamp === null ? newestTs : Math.max(newestTimestamp, newestTs);
        }

        if (typeof oldestTs === 'number') {
          oldestTimestamp = oldestTimestamp === null ? oldestTs : Math.min(oldestTimestamp, oldestTs);
        }

        if (newest) {
          newestCursor = {
            blockNumber: Number.parseInt(newest.blockNumber || newest.block_number || '0', 10) || null,
            txHash: newest.hash || newest.txHash,
            logIndex: Number.parseInt(newest.logIndex || newest.log_index || '0', 10) || null,
            timeStamp: sanitizeTimestamp(newest.timeStamp || newest.time_stamp),
          };

          await updateIngestCursor(chain.id, newestCursor);
        }

        hasMore = transfers.length === INGEST_PAGE_SIZE;
        page += 1;

        if (cursor && newest && newest.hash === cursor.txHash && newest.logIndex === cursor.logIndex) {
          hasMore = false;
        }
      }

      await recordIngestEvent(chain.id, 'ok', 'Ingestion cycle completed', {
        inserted,
        pagesAttempted,
        newestTimestamp,
        oldestTimestamp,
        ingestedAt: new Date().toISOString(),
      });

      return {
        inserted,
        pagesAttempted,
        newestCursor,
        boundaries: {
          startTime: typeof oldestTimestamp === 'number' ? new Date(oldestTimestamp * 1000) : null,
          endTime: typeof newestTimestamp === 'number' ? new Date(newestTimestamp * 1000) : null,
        },
      };
    } catch (error) {
      console.error(`X Transfer ingestion failed for chain ${chain.name}:`, error.message || error);
      await recordIngestEvent(chain.id, 'error', error.message || 'Ingestion failure', {
        stack: error.stack,
      });
      throw error;
    }
  });
};

const startTransferIngestion = ({ chains, fetchPage, logger = console }) => {
  if (!Array.isArray(chains) || !chains.length) {
    logger.warn('! Transfer ingestion skipped – no chains configured');
    return {
      stop: stopTransferIngestion,
    };
  }

  stopping = false;

  const safeFetchPage = async ({ chain, page, pageSize, sort }) => fetchPage({ chain, page, pageSize, sort });

  const scheduleChain = (chain, state, delayOverride = null) => {
    if (stopping) {
      return;
    }

    clearTimer(chain.id);

    const baseDelay = typeof delayOverride === 'number' ? delayOverride : state.delayMs;
    const jitter = Math.floor(Math.random() * Math.max(0, INGEST_JITTER_MS + 1));
    const wait = Math.max(0, baseDelay) + jitter;

    const timer = setTimeout(async () => {
      if (stopping) {
        return;
      }

      const cycleStart = Date.now();
      try {
        const result = await runIngestionCycle({ chain, fetchPage: safeFetchPage });
        const durationMs = Date.now() - cycleStart;

        let totals = null;
        try {
          if (result.inserted > 0 || !state.hasRun) {
            totals = await refreshChainAggregates(chain.id, {
              startTime: result.boundaries.startTime,
              endTime: result.boundaries.endTime,
            });
          } else {
            totals = await refreshChainTotals(chain.id);
          }
        } catch (aggregateError) {
          console.error('X Failed to refresh aggregates:', aggregateError.message || aggregateError);
        }

        const ready = Boolean(totals && totals.totalTransfers > 0);

        try {
          await recordIngestStatusSuccess(chain.id, {
            ready,
            meta: {
              inserted: result.inserted,
              pagesAttempted: result.pagesAttempted,
              durationMs,
              boundaries: {
                startTime: result.boundaries.startTime ? result.boundaries.startTime.toISOString() : null,
                endTime: result.boundaries.endTime ? result.boundaries.endTime.toISOString() : null,
              },
            },
          });
        } catch (statusError) {
          console.error('X Failed to record ingest success status:', statusError.message || statusError);
        }

        state.failureCount = 0;
        state.delayMs = INGEST_BASE_INTERVAL_MS;
        state.hasRun = true;

        if (logger?.info) {
          logger.info(`✓ Ingestion cycle succeeded for chain ${chain.name} (${chain.id}) – inserted ${result.inserted}, duration ${durationMs}ms`);
        } else {
          console.log(`✓ Ingestion cycle succeeded for chain ${chain.name} (${chain.id}) – inserted ${result.inserted}, duration ${durationMs}ms`);
        }
      } catch (error) {
        const durationMs = Date.now() - cycleStart;
        state.failureCount += 1;
        state.delayMs = Math.min(
          INGEST_MAX_BACKOFF_MS,
          Math.max(INGEST_BASE_INTERVAL_MS, state.delayMs * INGEST_BACKOFF_FACTOR)
        );
        const backoffUntil = new Date(Date.now() + state.delayMs);

        if (logger?.error) {
          logger.error(`X Ingestion cycle failed for chain ${chain.name} (${chain.id}) – ${error.message || error}`);
        } else {
          console.error(`X Ingestion cycle failed for chain ${chain.name} (${chain.id}) –`, error.message || error);
        }

        try {
          await recordIngestStatusFailure(chain.id, error, {
            failureCount: state.failureCount,
            backoffUntil,
            meta: {
              occurredAt: new Date().toISOString(),
              durationMs,
            },
          });
        } catch (statusError) {
          console.error('X Failed to record ingest failure status:', statusError.message || statusError);
        }
      } finally {
        if (!stopping) {
          scheduleChain(chain, state, state.delayMs);
        }
      }
    }, wait);

    chainTimers.set(chain.id, timer);
    state.nextRunAt = new Date(Date.now() + wait);
  };

  chains.forEach((chain) => {
    const state = {
      delayMs: INGEST_BASE_INTERVAL_MS,
      failureCount: 0,
      hasRun: false,
      nextRunAt: null,
    };
    chainStates.set(chain.id, state);

    const initialDelay = Math.floor(Math.random() * Math.max(1, INGEST_IDLE_DELAY_MS));
    scheduleChain(chain, state, initialDelay);
  });

  if (logger?.log) {
    logger.log(`✓ Transfer ingestion supervisor started for ${chains.length} chain(s)`);
  } else {
    console.log(`✓ Transfer ingestion supervisor started for ${chains.length} chain(s)`);
  }

  return {
    stop: stopTransferIngestion,
  };
};

module.exports = {
  startTransferIngestion,
  stopTransferIngestion,
};
