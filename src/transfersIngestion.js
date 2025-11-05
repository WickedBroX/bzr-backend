'use strict';

const pLimit = require('p-limit');
const {
  storeTransfers,
  updateIngestCursor,
  recordIngestEvent,
  getIngestCursor,
} = require('./persistentStore');

const INGEST_PAGE_SIZE = Number(process.env.TRANSFERS_INGEST_PAGE_SIZE || 100);
const INGEST_MAX_PAGES_PER_CYCLE = Number(process.env.TRANSFERS_INGEST_MAX_PAGES || 5);
const INGEST_INTERVAL_MS = Number(process.env.TRANSFERS_INGEST_INTERVAL_MS || 30_000);
const INGEST_CONCURRENCY = Number(process.env.TRANSFERS_INGEST_CONCURRENCY || 2);
const INGEST_IDLE_DELAY_MS = Number(process.env.TRANSFERS_INGEST_IDLE_DELAY_MS || 5_000);

const ACTIVE_TIMERS = new Map();
let limit = null;

const initializeLimiter = () => {
  if (!limit) {
    limit = pLimit(Math.max(1, INGEST_CONCURRENCY));
  }
  return limit;
};

const stopTransferIngestion = () => {
  for (const timer of ACTIVE_TIMERS.values()) {
    clearTimeout(timer);
  }
  ACTIVE_TIMERS.clear();
};

const scheduleNextRun = (chainId, task, delay) => {
  const timer = setTimeout(task, delay);
  ACTIVE_TIMERS.set(chainId, timer);
};

const runIngestionCycle = async ({ chain, fetchPage }) => {
  const limiter = initializeLimiter();
  return limiter(async () => {
    let inserted = 0;
    let page = 1;
    let hasMore = true;

    try {
      const cursor = await getIngestCursor(chain.id);
      const maxPages = Math.max(1, INGEST_MAX_PAGES_PER_CYCLE);

      while (hasMore && page <= maxPages) {
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
        if (newest) {
          await updateIngestCursor(chain.id, {
            blockNumber: Number.parseInt(newest.blockNumber || newest.block_number || '0', 10) || null,
            txHash: newest.hash || newest.txHash,
            logIndex: Number.parseInt(newest.logIndex || newest.log_index || '0', 10) || null,
            timeStamp: Number.parseInt(newest.timeStamp || newest.time_stamp || '0', 10) || null,
          });
        }

        hasMore = transfers.length === INGEST_PAGE_SIZE;
        page += 1;

        // If we already had a cursor (ongoing sync) and we fetched a page containing it, stop early
        if (cursor && newest && newest.hash === cursor.txHash && newest.logIndex === cursor.logIndex) {
          hasMore = false;
        }
      }

      await recordIngestEvent(chain.id, 'ok', 'Ingestion cycle completed', {
        inserted,
        pagesAttempted: page - 1,
        ingestedAt: new Date().toISOString(),
      });

      return inserted;
    } catch (error) {
      console.error(`X Transfer ingestion failed for chain ${chain.name}:`, error.message || error);
      await recordIngestEvent(chain.id, 'error', error.message || 'Ingestion failure', {
        stack: error.stack,
      });
      throw error;
    }
  });
};

const startTransferIngestion = ({ chains, fetchPage }) => {
  if (!Array.isArray(chains) || !chains.length) {
    console.warn('! Transfer ingestion skipped – no chains configured');
    return;
  }

  const safeFetchPage = async ({ chain, page, pageSize, sort }) => {
    const result = await fetchPage({ chain, page, pageSize, sort });
    return result;
  };

  chains.forEach((chain) => {
    const execute = async () => {
      try {
        await runIngestionCycle({ chain, fetchPage: safeFetchPage });
      } catch (error) {
        // Error already logged within runIngestionCycle
      } finally {
        scheduleNextRun(chain.id, execute, INGEST_INTERVAL_MS);
      }
    };

    // Kick off after slight stagger to avoid burst
    const initialDelay = Math.floor(Math.random() * INGEST_IDLE_DELAY_MS);
    scheduleNextRun(chain.id, execute, initialDelay);
  });

  console.log(`✓ Transfer ingestion started for ${chains.length} chain(s) with interval ${INGEST_INTERVAL_MS}ms`);

  return {
    stop: stopTransferIngestion,
  };
};

module.exports = {
  startTransferIngestion,
  stopTransferIngestion,
};
