'use strict';

const path = require('path');
const fs = require('fs');

const envPath = process.env.ENV_FILE || path.resolve(process.cwd(), '.env');
if (fs.existsSync(envPath)) {
  require('dotenv').config({ path: envPath });
} else {
  require('dotenv').config();
}

const pLimitModule = require('p-limit');
const createLimit = typeof pLimitModule === 'function'
  ? pLimitModule
  : (pLimitModule && typeof pLimitModule.default === 'function'
    ? pLimitModule.default
    : null);

if (!createLimit) {
  throw new Error('p-limit module did not provide a callable export');
}

const {
  CHAINS,
  fetchTransfersPageFromChain,
} = require('../src/providers/transfersProvider');
const {
  initPersistentStore,
  closePersistentStore,
  storeTransfers,
  updateIngestCursor,
  refreshChainAggregates,
  recordIngestEvent,
  recordIngestStatusSuccess,
  recordIngestStatusFailure,
} = require('../src/persistentStore');

const args = process.argv.slice(2);

const parseNumberOption = (flag, defaultValue) => {
  const prefix = `--${flag}=`;
  const raw = args.find((arg) => arg.startsWith(prefix));
  if (!raw) {
    return defaultValue;
  }

  const value = Number(raw.slice(prefix.length));
  if (!Number.isFinite(value) || value <= 0) {
    return defaultValue;
  }
  return Math.floor(value);
};

const parseChainsOption = () => {
  const prefix = '--chains=';
  const raw = args.find((arg) => arg.startsWith(prefix));
  if (!raw) {
    return null;
  }

  const segments = raw.slice(prefix.length).split(',').map((segment) => Number(segment.trim()));
  const chainIds = segments.filter((value) => Number.isFinite(value) && value > 0);
  return chainIds.length ? new Set(chainIds) : null;
};

const hasFlag = (flag) => args.includes(`--${flag}`) || args.includes(`-${flag}`);

const showHelp = () => {
  /* eslint-disable no-console */
  console.log('\nUsage: node scripts/seed-transfers.js [options]\n');
  console.log('Options:');
  console.log('  --pages=<n>         Number of pages (per chain) to fetch, default 10');
  console.log('  --page-size=<n>     Transfers per page, default TRANSFERS_INGEST_PAGE_SIZE or 100');
  console.log('  --concurrency=<n>   Number of chains to process concurrently, default 2');
  console.log('  --chains=<ids>      Comma-separated list of chain IDs to seed (defaults to all)');
  console.log('  --dry-run           Fetch transfers but do not write to the database');
  console.log('  --help              Show this help message');
  console.log('');
  /* eslint-enable no-console */
};

if (hasFlag('help') || hasFlag('h')) {
  showHelp();
  process.exit(0);
}

const options = {
  pages: parseNumberOption('pages', 10),
  pageSize: parseNumberOption('page-size', Number(process.env.TRANSFERS_INGEST_PAGE_SIZE || 100)),
  concurrency: parseNumberOption('concurrency', 2),
  chains: parseChainsOption(),
  dryRun: hasFlag('dry-run'),
};

const limit = createLimit(Math.max(1, options.concurrency));

const selectChains = () => {
  if (!options.chains) {
    return CHAINS;
  }

  const selected = CHAINS.filter((chain) => options.chains.has(chain.id));
  if (!selected.length) {
    throw new Error(`No chains matched the provided IDs: ${Array.from(options.chains).join(', ')}`);
  }
  return selected;
};

const parseTimestampSeconds = (value) => {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? Math.floor(value) : null;
  }

  if (typeof value === 'string') {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return Math.floor(parsed);
    }
  }

  return null;
};

const buildCursorFromTransfer = (transfer) => {
  if (!transfer) {
    return null;
  }

  const timeStamp = parseTimestampSeconds(transfer.timeStamp || transfer.time_stamp);
  const blockNumber = Number.parseInt(transfer.blockNumber || transfer.block_number || '0', 10) || null;
  const logIndex = Number.parseInt(transfer.logIndex || transfer.log_index || '0', 10) || null;
  const txHash = transfer.hash || transfer.txHash || transfer.tx_hash || null;

  if (!txHash) {
    return null;
  }

  return {
    blockNumber,
    txHash,
    logIndex,
    timeStamp,
  };
};

const formatDuration = (ms) => `${ms.toFixed(0)}ms`;

const seedChain = async (chain) => {
  const start = Date.now();
  let page = 1;
  let insertedTotal = 0;
  let pagesAttempted = 0;
  let newestCursor = null;
  let newestTimestamp = null;
  let oldestTimestamp = null;

  console.log(`\n[${chain.name}] Starting seed: pages=${options.pages}, pageSize=${options.pageSize}`);

  while (page <= options.pages) {
    pagesAttempted += 1;

    const result = await fetchTransfersPageFromChain({
      chain,
      page,
      pageSize: options.pageSize,
      sort: 'desc',
    });

    const transfers = Array.isArray(result?.transfers) ? result.transfers : [];
    console.log(`[${chain.name}] Page ${page}: fetched ${transfers.length} transfer(s)`);

    if (!transfers.length) {
      break;
    }

    if (!newestCursor) {
      newestCursor = buildCursorFromTransfer(transfers[0]);
    }

    const newest = parseTimestampSeconds(transfers[0]?.timeStamp);
    const oldest = parseTimestampSeconds(transfers[transfers.length - 1]?.timeStamp);

    if (typeof newest === 'number') {
      newestTimestamp = newestTimestamp === null ? newest : Math.max(newestTimestamp, newest);
    }
    if (typeof oldest === 'number') {
      oldestTimestamp = oldestTimestamp === null ? oldest : Math.min(oldestTimestamp, oldest);
    }

    if (!options.dryRun) {
      const outcome = await storeTransfers(chain.id, transfers);
      insertedTotal += outcome.inserted || 0;
    }

    if (transfers.length < options.pageSize) {
      break;
    }

    page += 1;
  }

  if (options.dryRun) {
    console.log(`[${chain.name}] Dry run complete in ${formatDuration(Date.now() - start)}.`);
    return {
      chain,
      inserted: 0,
      pagesAttempted,
      newestTimestamp,
      oldestTimestamp,
      cursor: newestCursor,
    };
  }

  if (insertedTotal > 0) {
    await refreshChainAggregates(chain.id, {
      startTime: typeof oldestTimestamp === 'number' ? new Date(oldestTimestamp * 1000) : undefined,
      endTime: typeof newestTimestamp === 'number' ? new Date(newestTimestamp * 1000) : undefined,
    });
  }

  if (newestCursor) {
    await updateIngestCursor(chain.id, newestCursor);
  }

  const summaryMeta = {
    inserted: insertedTotal,
    pagesAttempted,
    newestTimestamp,
    oldestTimestamp,
    durationMs: Date.now() - start,
    source: 'seed-transfers',
  };

  if (insertedTotal > 0) {
    await recordIngestStatusSuccess(chain.id, {
      ready: insertedTotal > 0,
      meta: summaryMeta,
    });
    await recordIngestEvent(chain.id, 'seed', 'Seeded persistent store from upstream providers', summaryMeta);
  } else {
    await recordIngestEvent(chain.id, 'seed', 'Seed completed without new transfers', summaryMeta);
  }

  console.log(`[${chain.name}] Seed complete: inserted ${insertedTotal} transfer(s) in ${formatDuration(summaryMeta.durationMs)}.`);

  return {
    chain,
    inserted: insertedTotal,
    pagesAttempted,
    newestTimestamp,
    oldestTimestamp,
    cursor: newestCursor,
  };
};

const run = async () => {
  console.log('Initializing persistent store...');
  const status = await initPersistentStore();
  if (!status.enabled) {
    throw new Error('Persistent store is not configured. Aborting.');
  }

  console.log('Persistent store ready. Beginning seed.');

  const chains = selectChains();
  const results = [];
  const failures = [];

  await Promise.all(chains.map((chain) => limit(async () => {
    try {
      const result = await seedChain(chain);
      results.push(result);
    } catch (error) {
      console.error(`[${chain.name}] Seed failed:`, error.message || error);
      failures.push({ chain, error });
      if (!options.dryRun) {
        await recordIngestStatusFailure(chain.id, error, {
          failureCount: 1,
          meta: { source: 'seed-transfers', pagesAttempted: options.pages },
        });
      }
    }
  })));

  await closePersistentStore();

  console.log('\nSeed results:');
  for (const result of results) {
    const lagSeconds = typeof result.newestTimestamp === 'number'
      ? Math.max(0, Math.floor(Date.now() / 1000) - result.newestTimestamp)
      : null;
    console.log(`  - ${result.chain.name}: inserted=${result.inserted}, pages=${result.pagesAttempted}, lagSeconds=${lagSeconds}`);
  }

  if (failures.length) {
    console.log('\nFailures:');
    for (const failure of failures) {
      console.log(`  - ${failure.chain.name}: ${failure.error.message || failure.error}`);
    }
  }

  console.log('\nSeed complete.');

  if (failures.length) {
    const error = new Error(`Seed failed for ${failures.map((failure) => failure.chain.name).join(', ')}`);
    error.failures = failures;
    throw error;
  }
};

run().catch((error) => {
  console.error('\nSeed process failed:', error.stack || error.message || error);
  process.exit(1);
});
