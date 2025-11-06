'use strict';

const { Pool } = require('pg');

const DEFAULT_MAX_CLIENTS = Number(process.env.TRANSFERS_DB_MAX_CLIENTS || process.env.DB_MAX_CLIENTS || 10);
const DEFAULT_IDLE_TIMEOUT_MS = Number(process.env.TRANSFERS_DB_IDLE_TIMEOUT_MS || 30_000);
const DEFAULT_CONNECTION_TIMEOUT_MS = Number(process.env.TRANSFERS_DB_CONNECTION_TIMEOUT_MS || 5_000);
const DEFAULT_STALE_THRESHOLD_SECONDS = Number(process.env.TRANSFERS_STALE_THRESHOLD_SECONDS || 900);

let pool = null;
let storeEnabled = false;
let storeReady = false;
let initializationError = null;

const getDatabaseConfig = () => {
  const connectionString = process.env.TRANSFERS_DATABASE_URL || process.env.DATABASE_URL;
  if (connectionString) {
    return {
      connectionString,
      max: DEFAULT_MAX_CLIENTS,
      idleTimeoutMillis: DEFAULT_IDLE_TIMEOUT_MS,
      connectionTimeoutMillis: DEFAULT_CONNECTION_TIMEOUT_MS,
      ssl: process.env.TRANSFERS_DB_SSL === 'true' || process.env.DATABASE_SSL === 'true'
        ? { rejectUnauthorized: false }
        : undefined,
    };
  }

  const host = process.env.TRANSFERS_DB_HOST || process.env.DB_HOST;
  const port = Number(process.env.TRANSFERS_DB_PORT || process.env.DB_PORT || 5432);
  const database = process.env.TRANSFERS_DB_NAME || process.env.DB_NAME;
  const user = process.env.TRANSFERS_DB_USER || process.env.DB_USER;
  const password = process.env.TRANSFERS_DB_PASSWORD || process.env.DB_PASSWORD;

  if (!host || !database || !user) {
    return null;
  }

  return {
    host,
    port,
    database,
    user,
    password,
    max: DEFAULT_MAX_CLIENTS,
    idleTimeoutMillis: DEFAULT_IDLE_TIMEOUT_MS,
    connectionTimeoutMillis: DEFAULT_CONNECTION_TIMEOUT_MS,
    ssl: process.env.TRANSFERS_DB_SSL === 'true' || process.env.DATABASE_SSL === 'true'
      ? { rejectUnauthorized: false }
      : undefined,
  };
};

const ensurePool = () => {
  if (pool) {
    return pool;
  }

  const config = getDatabaseConfig();
  if (!config) {
    return null;
  }

  pool = new Pool(config);
  pool.on('error', (error) => {
    console.error('! Postgres pool error in persistent store:', error.message || error);
  });

  return pool;
};

const initPersistentStore = async () => {
  const clientPool = ensurePool();
  if (!clientPool) {
    storeEnabled = false;
    storeReady = false;
    initializationError = new Error('Persistent store disabled: no Postgres configuration found');
    console.warn('! Persistent transfer store disabled – no Postgres configuration provided');
    return { enabled: false, ready: false, reason: initializationError.message };
  }

  storeEnabled = true;

  try {
    await runMigrations(clientPool);
    storeReady = true;
    initializationError = null;
    console.log('✓ Persistent transfer store ready');
    return { enabled: true, ready: true };
  } catch (error) {
    storeReady = false;
    initializationError = error;
    console.error('X Failed to initialize persistent transfer store:', error.message || error);
    return { enabled: true, ready: false, error };
  }
};

const runMigrations = async (clientPool) => {
  const client = await clientPool.connect();
  try {
    await client.query('BEGIN');

    await client.query(`
      CREATE TABLE IF NOT EXISTS transfer_events (
        chain_id INTEGER NOT NULL,
        block_number BIGINT NOT NULL,
        tx_hash TEXT NOT NULL,
        log_index INTEGER NOT NULL,
        time_stamp TIMESTAMPTZ NOT NULL,
        from_address TEXT NOT NULL,
        to_address TEXT NOT NULL,
        value TEXT NOT NULL,
        method_id TEXT,
        payload JSONB NOT NULL,
        inserted_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (chain_id, tx_hash, log_index)
      );
    `);

    await client.query('CREATE INDEX IF NOT EXISTS idx_transfer_events_chain_time ON transfer_events (chain_id, time_stamp DESC);');
    await client.query('CREATE INDEX IF NOT EXISTS idx_transfer_events_chain_block ON transfer_events (chain_id, block_number DESC);');

    await client.query(`
      CREATE TABLE IF NOT EXISTS transfer_ingest_cursors (
        chain_id INTEGER PRIMARY KEY,
        last_block_number BIGINT,
        last_tx_hash TEXT,
        last_log_index INTEGER,
        last_time TIMESTAMPTZ,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS transfer_ingest_events (
        id BIGSERIAL PRIMARY KEY,
        chain_id INTEGER NOT NULL,
        status TEXT NOT NULL,
        message TEXT,
        meta JSONB,
        occurred_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS transfer_warm_jobs (
        id BIGSERIAL PRIMARY KEY,
        chain_id INTEGER NOT NULL,
        job_type TEXT NOT NULL,
        status TEXT NOT NULL,
        requested_at TIMESTAMPTZ DEFAULT NOW(),
        completed_at TIMESTAMPTZ,
        payload JSONB
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS transfer_chain_totals (
        chain_id INTEGER PRIMARY KEY,
        total_transfers BIGINT NOT NULL DEFAULT 0,
        total_volume_raw NUMERIC(78, 0) NOT NULL DEFAULT 0,
        first_time TIMESTAMPTZ,
        last_time TIMESTAMPTZ,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    await client.query('CREATE INDEX IF NOT EXISTS idx_transfer_chain_totals_updated_at ON transfer_chain_totals (updated_at DESC);');

    await client.query(`
      CREATE TABLE IF NOT EXISTS transfer_daily_aggregates (
        chain_id INTEGER NOT NULL,
        day DATE NOT NULL,
        transfer_count INTEGER NOT NULL DEFAULT 0,
        volume_raw NUMERIC(78, 0) NOT NULL DEFAULT 0,
        unique_addresses INTEGER NOT NULL DEFAULT 0,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (chain_id, day)
      );
    `);

    await client.query('CREATE INDEX IF NOT EXISTS idx_transfer_daily_aggregates_day ON transfer_daily_aggregates (day DESC);');

    await client.query(`
      CREATE TABLE IF NOT EXISTS transfer_ingest_status (
        chain_id INTEGER PRIMARY KEY,
        ready BOOLEAN NOT NULL DEFAULT FALSE,
        last_success_at TIMESTAMPTZ,
        last_error_at TIMESTAMPTZ,
        last_error TEXT,
        consecutive_failures INTEGER NOT NULL DEFAULT 0,
        backoff_until TIMESTAMPTZ,
        meta JSONB,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
};

const isPersistentStoreEnabled = () => storeEnabled;
const isPersistentStoreReady = () => storeReady;
const getPersistentStoreError = () => initializationError;

const withClient = async (task) => {
  const clientPool = ensurePool();
  if (!clientPool) {
    throw new Error('Persistent store not configured');
  }

  const client = await clientPool.connect();
  try {
    return await task(client);
  } finally {
    client.release();
  }
};

const normalizeChainIds = (chainId) => {
  if (Array.isArray(chainId)) {
    return chainId
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value));
  }

  if (typeof chainId === 'number' && Number.isFinite(chainId)) {
    return [chainId];
  }

  return [];
};

const normalizeTransferRecord = (chainId, transfer) => {
  const blockNumber = Number.parseInt(transfer.blockNumber || transfer.block_number || '0', 10) || 0;
  const logIndex = Number.parseInt(transfer.logIndex || transfer.log_index || '0', 10) || 0;
  const timestampSeconds = Number.parseInt(transfer.timeStamp || transfer.time_stamp || '0', 10) || 0;
  const timestamp = new Date(timestampSeconds * 1000);

  return {
    chainId,
    blockNumber,
    txHash: transfer.hash || transfer.txHash || transfer.tx_hash,
    logIndex,
    timeStamp: timestamp,
    fromAddress: (transfer.from || transfer.fromAddress || transfer.from_address || '').toLowerCase(),
    toAddress: (transfer.to || transfer.toAddress || transfer.to_address || '').toLowerCase(),
    value: transfer.value || '0',
    methodId: transfer.methodId || transfer.method_id || null,
    payload: transfer,
  };
};

const storeTransfers = async (chainId, transfers = []) => {
  if (!storeEnabled || !storeReady) {
    return { inserted: 0 };
  }

  if (!Array.isArray(transfers) || transfers.length === 0) {
    return { inserted: 0 };
  }

  const normalized = transfers
    .map((transfer) => normalizeTransferRecord(chainId, transfer))
    .filter((record) => record.txHash);

  const uniqueRecords = [];
  const seenKeys = new Set();

  for (const record of normalized) {
    const key = `${record.chainId}:${record.txHash}:${record.logIndex}`;
    if (seenKeys.has(key)) {
      continue;
    }
    seenKeys.add(key);
    uniqueRecords.push(record);
  }

  if (uniqueRecords.length === 0) {
    return { inserted: 0 };
  }

  const text = `
    INSERT INTO transfer_events (
      chain_id,
      block_number,
      tx_hash,
      log_index,
      time_stamp,
      from_address,
      to_address,
      value,
      method_id,
      payload
    ) VALUES ${uniqueRecords
      .map((_, index) => `($${index * 10 + 1}, $${index * 10 + 2}, $${index * 10 + 3}, $${index * 10 + 4}, $${index * 10 + 5}, $${index * 10 + 6}, $${index * 10 + 7}, $${index * 10 + 8}, $${index * 10 + 9}, $${index * 10 + 10})`)
      .join(', ')}
    ON CONFLICT (chain_id, tx_hash, log_index) DO UPDATE SET
      block_number = EXCLUDED.block_number,
      time_stamp = EXCLUDED.time_stamp,
      from_address = EXCLUDED.from_address,
      to_address = EXCLUDED.to_address,
      value = EXCLUDED.value,
      method_id = EXCLUDED.method_id,
      payload = EXCLUDED.payload,
      inserted_at = NOW();
  `;

  const values = uniqueRecords.flatMap((record) => [
    record.chainId,
    record.blockNumber,
    record.txHash,
    record.logIndex,
    record.timeStamp,
    record.fromAddress,
    record.toAddress,
    record.value,
    record.methodId,
    record.payload,
  ]);

  const result = await withClient((client) => client.query(text, values));
  return { inserted: result.rowCount || 0 };
};

const updateIngestCursor = async (chainId, cursor) => {
  if (!storeEnabled || !storeReady) {
    return;
  }

  const { blockNumber, txHash, logIndex, timeStamp } = cursor || {};
  await withClient((client) => client.query(
    `
      INSERT INTO transfer_ingest_cursors (chain_id, last_block_number, last_tx_hash, last_log_index, last_time, updated_at)
      VALUES ($1, $2, $3, $4, $5, NOW())
      ON CONFLICT (chain_id) DO UPDATE SET
        last_block_number = EXCLUDED.last_block_number,
        last_tx_hash = EXCLUDED.last_tx_hash,
        last_log_index = EXCLUDED.last_log_index,
        last_time = EXCLUDED.last_time,
        updated_at = NOW();
    `,
    [
      chainId,
      blockNumber || null,
      txHash || null,
      typeof logIndex === 'number' ? logIndex : null,
      timeStamp ? new Date(Number(timeStamp) * 1000) : null,
    ]
  ));
};

const recordIngestEvent = async (chainId, status, message, meta = null) => {
  if (!storeEnabled || !storeReady) {
    return;
  }

  await withClient((client) => client.query(
    `
      INSERT INTO transfer_ingest_events (chain_id, status, message, meta)
      VALUES ($1, $2, $3, $4);
    `,
    [chainId, status, message || null, meta]
  ));
};

const refreshChainTotals = async (chainId) => {
  if (!storeEnabled || !storeReady) {
    return null;
  }

  const result = await withClient((client) => client.query(
    `
      WITH stats AS (
        SELECT
          COUNT(*)::BIGINT AS total_transfers,
          COALESCE(SUM(value::numeric), 0::NUMERIC) AS total_volume_raw,
          MIN(time_stamp) AS first_time,
          MAX(time_stamp) AS last_time
        FROM transfer_events
        WHERE chain_id = $1
      )
      INSERT INTO transfer_chain_totals (
        chain_id,
        total_transfers,
        total_volume_raw,
        first_time,
        last_time,
        updated_at
      )
      SELECT
        $1,
        stats.total_transfers,
        stats.total_volume_raw,
        stats.first_time,
        stats.last_time,
        NOW()
      FROM stats
      ON CONFLICT (chain_id) DO UPDATE SET
        total_transfers = EXCLUDED.total_transfers,
        total_volume_raw = EXCLUDED.total_volume_raw,
        first_time = EXCLUDED.first_time,
        last_time = EXCLUDED.last_time,
        updated_at = NOW()
      RETURNING chain_id, total_transfers, total_volume_raw, first_time, last_time, updated_at;
    `,
    [chainId]
  ));

  const row = result.rows[0];
  if (!row) {
    return null;
  }

  return {
    chainId: row.chain_id,
    totalTransfers: Number.parseInt(row.total_transfers, 10) || 0,
    totalVolumeRaw: row.total_volume_raw ? String(row.total_volume_raw) : '0',
    firstTime: row.first_time ? new Date(row.first_time) : null,
    lastTime: row.last_time ? new Date(row.last_time) : null,
    updatedAt: row.updated_at ? new Date(row.updated_at) : new Date(),
  };
};

const refreshChainDailyAggregates = async (chainId, options = {}) => {
  if (!storeEnabled || !storeReady) {
    return { upserted: 0 };
  }

  const normalizedStart = normalizeDateInput(options.startTime);
  const normalizedEnd = normalizeDateInput(options.endTime);

  const params = [chainId];
  const filters = ['chain_id = $1'];

  if (normalizedStart) {
    params.push(normalizedStart);
    filters.push(`time_stamp >= $${params.length}`);
  }

  if (normalizedEnd) {
    params.push(normalizedEnd);
    filters.push(`time_stamp <= $${params.length}`);
  }

  const clause = filters.length ? `WHERE ${filters.join(' AND ')}` : '';

  const result = await withClient((client) => client.query(
    `
      WITH filtered AS (
        SELECT *
        FROM transfer_events
        ${clause}
      ),
      daily AS (
        SELECT
          DATE_TRUNC('day', time_stamp)::DATE AS day,
          COUNT(*)::BIGINT AS transfer_count,
          COALESCE(SUM(value::numeric), 0::NUMERIC) AS volume_raw
        FROM filtered
        GROUP BY day
      ),
      participants AS (
        SELECT DATE_TRUNC('day', time_stamp)::DATE AS day, from_address AS address FROM filtered
        UNION
        SELECT DATE_TRUNC('day', time_stamp)::DATE AS day, to_address AS address FROM filtered
      ),
      daily_participants AS (
        SELECT day, COUNT(DISTINCT address) AS unique_addresses
        FROM participants
        GROUP BY day
      )
      INSERT INTO transfer_daily_aggregates (
        chain_id,
        day,
        transfer_count,
        volume_raw,
        unique_addresses,
        updated_at
      )
      SELECT
        $1,
        daily.day,
        daily.transfer_count,
        daily.volume_raw,
        COALESCE(daily_participants.unique_addresses, 0),
        NOW()
      FROM daily
      LEFT JOIN daily_participants ON daily_participants.day = daily.day
      ON CONFLICT (chain_id, day) DO UPDATE SET
        transfer_count = EXCLUDED.transfer_count,
        volume_raw = EXCLUDED.volume_raw,
        unique_addresses = EXCLUDED.unique_addresses,
        updated_at = NOW();
    `,
    params
  ));

  return { upserted: result.rowCount || 0 };
};

const refreshChainAggregates = async (chainId, options = {}) => {
  if (!storeEnabled || !storeReady) {
    return null;
  }

  const totals = await refreshChainTotals(chainId);
  await refreshChainDailyAggregates(chainId, options);
  return totals;
};

const recordIngestStatusSuccess = async (chainId, { ready = false, meta = null } = {}) => {
  if (!storeEnabled || !storeReady) {
    return;
  }

  await withClient((client) => client.query(
    `
      INSERT INTO transfer_ingest_status (
        chain_id,
        ready,
        last_success_at,
        last_error_at,
        last_error,
        consecutive_failures,
        backoff_until,
        meta,
        updated_at
      )
      VALUES ($1, $2, NOW(), NULL, NULL, 0, NULL, $3, NOW())
      ON CONFLICT (chain_id) DO UPDATE SET
        ready = EXCLUDED.ready,
        last_success_at = NOW(),
        last_error_at = NULL,
        last_error = NULL,
        consecutive_failures = 0,
        backoff_until = NULL,
        meta = COALESCE(EXCLUDED.meta, transfer_ingest_status.meta),
        updated_at = NOW();
    `,
    [chainId, ready, meta]
  ));
};

const recordIngestStatusFailure = async (chainId, error, { failureCount = 1, backoffUntil = null, meta = null } = {}) => {
  if (!storeEnabled || !storeReady) {
    return;
  }

  const message = error && error.message ? String(error.message).slice(0, 500) : String(error || 'Unknown error');
  const failureMeta = meta || { error: message };

  await withClient((client) => client.query(
    `
      INSERT INTO transfer_ingest_status (
        chain_id,
        ready,
        last_success_at,
        last_error_at,
        last_error,
        consecutive_failures,
        backoff_until,
        meta,
        updated_at
      )
      VALUES ($1, FALSE, NULL, NOW(), $2, $3, $4, $5, NOW())
      ON CONFLICT (chain_id) DO UPDATE SET
        ready = transfer_ingest_status.ready,
        last_error_at = NOW(),
        last_error = $2,
        consecutive_failures = $3,
        backoff_until = $4,
        meta = COALESCE(transfer_ingest_status.meta, '{}'::JSONB) || COALESCE($5::JSONB, '{}'::JSONB),
        updated_at = NOW();
    `,
    [chainId, message, failureCount, backoffUntil, failureMeta]
  ));
};

const getChainSnapshots = async (chainId) => {
  if (!storeEnabled || !storeReady) {
    return [];
  }

  const chainIds = normalizeChainIds(chainId);
  if (!chainIds.length) {
    return [];
  }

  const result = await withClient((client) => client.query(
    `
      WITH requested AS (
        SELECT UNNEST($1::INT[]) AS chain_id
      )
      SELECT
        requested.chain_id,
        totals.total_transfers,
        totals.total_volume_raw,
        totals.first_time,
        totals.last_time,
        totals.updated_at AS totals_updated_at,
        status.ready,
        status.last_success_at,
        status.last_error_at,
        status.last_error,
        status.consecutive_failures,
        status.backoff_until,
        status.meta,
        cursor.last_time AS cursor_last_time,
        cursor.updated_at AS cursor_updated_at
      FROM requested
      LEFT JOIN transfer_chain_totals totals ON totals.chain_id = requested.chain_id
      LEFT JOIN transfer_ingest_status status ON status.chain_id = requested.chain_id
      LEFT JOIN transfer_ingest_cursors cursor ON cursor.chain_id = requested.chain_id;
    `,
    [chainIds]
  ));

  const now = Date.now();

  return result.rows.map((row) => {
    const totalTransfers = Number.parseInt(row.total_transfers, 10) || 0;
    const lastTime = row.last_time ? new Date(row.last_time) : null;
    const cursorTime = row.cursor_last_time ? new Date(row.cursor_last_time) : null;
    const referenceTime = lastTime || cursorTime;
    const indexLagSeconds = referenceTime ? Math.max(0, Math.floor((now - referenceTime.getTime()) / 1000)) : null;
    const ready = Boolean(row.ready) || totalTransfers > 0;
    const stale = typeof indexLagSeconds === 'number' ? indexLagSeconds > DEFAULT_STALE_THRESHOLD_SECONDS : false;

    return {
      chainId: Number.parseInt(row.chain_id, 10) || 0,
      totals: {
        totalTransfers,
        totalVolumeRaw: row.total_volume_raw ? String(row.total_volume_raw) : '0',
        firstTime: row.first_time ? new Date(row.first_time) : null,
        lastTime,
        updatedAt: row.totals_updated_at ? new Date(row.totals_updated_at) : null,
      },
      status: {
        ready,
        stale,
        lastSuccessAt: row.last_success_at ? new Date(row.last_success_at) : null,
        lastErrorAt: row.last_error_at ? new Date(row.last_error_at) : null,
        lastError: row.last_error || null,
        consecutiveFailures: Number.parseInt(row.consecutive_failures, 10) || 0,
        backoffUntil: row.backoff_until ? new Date(row.backoff_until) : null,
        indexLagSeconds,
        cursorUpdatedAt: row.cursor_updated_at ? new Date(row.cursor_updated_at) : null,
        cursorTime,
        meta: row.meta || null,
      },
    };
  });
};

const getDailyAggregates = async ({ chainId, startDate, endDate }) => {
  if (!storeEnabled || !storeReady) {
    return [];
  }

  const chainIds = normalizeChainIds(chainId);
  if (!chainIds.length) {
    return [];
  }

  const normalizedStart = normalizeDateInput(startDate);
  const normalizedEnd = normalizeDateInput(endDate);

  const params = [chainIds];
  const filters = ['chain_id = ANY($1::INT[])'];

  if (normalizedStart) {
    params.push(normalizedStart);
    filters.push(`day >= $${params.length}::DATE`);
  }

  if (normalizedEnd) {
    params.push(normalizedEnd);
    filters.push(`day <= $${params.length}::DATE`);
  }

  const clause = filters.length ? `WHERE ${filters.join(' AND ')}` : '';

  const result = await withClient((client) => client.query(
    `
      SELECT
        chain_id,
        day,
        transfer_count,
        volume_raw,
        unique_addresses,
        updated_at
      FROM transfer_daily_aggregates
      ${clause}
      ORDER BY chain_id ASC, day ASC;
    `,
    params
  ));

  return result.rows.map((row) => ({
    chainId: Number.parseInt(row.chain_id, 10) || 0,
    day: row.day ? new Date(row.day) : null,
    transferCount: Number.parseInt(row.transfer_count, 10) || 0,
    volumeRaw: row.volume_raw ? String(row.volume_raw) : '0',
    uniqueAddresses: Number.parseInt(row.unique_addresses, 10) || 0,
    updatedAt: row.updated_at ? new Date(row.updated_at) : null,
  }));
};

const getIngestCursor = async (chainId) => {
  if (!storeEnabled || !storeReady) {
    return null;
  }

  const result = await withClient((client) => client.query(
    'SELECT chain_id, last_block_number, last_tx_hash, last_log_index, last_time, updated_at FROM transfer_ingest_cursors WHERE chain_id = $1;',
    [chainId]
  ));

  if (!result.rows.length) {
    return null;
  }

  const row = result.rows[0];
  return {
    chainId: row.chain_id,
    blockNumber: row.last_block_number,
    txHash: row.last_tx_hash,
    logIndex: row.last_log_index,
    timeStamp: row.last_time ? Math.floor(new Date(row.last_time).getTime() / 1000) : null,
    updatedAt: row.updated_at,
  };
};

const getLatestIngestSummary = async () => {
  if (!storeEnabled || !storeReady) {
    return [];
  }

  const result = await withClient((client) => client.query(
    `
      SELECT
        chain_id,
        last_block_number,
        last_time,
        updated_at
      FROM transfer_ingest_cursors;
    `
  ));

  return result.rows.map((row) => ({
    chainId: row.chain_id,
    lastBlockNumber: row.last_block_number,
    lastTime: row.last_time,
    lagSeconds: row.last_time ? Math.max(0, Math.floor((Date.now() - new Date(row.last_time).getTime()) / 1000)) : null,
    updatedAt: row.updated_at,
  }));
};

const normalizeDateInput = (value) => {
  if (!value) return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value;
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    const numericDate = new Date(value);
    if (!Number.isNaN(numericDate.getTime())) {
      return numericDate;
    }
  }

  if (typeof value === 'string') {
    const parsed = Date.parse(value);
    if (!Number.isNaN(parsed)) {
      return new Date(parsed);
    }
  }

  return null;
};

const buildFilterClause = ({ chainId, startBlock, endBlock, startTime, endTime }) => {
  const conditions = [];
  const values = [];

  if (Array.isArray(chainId)) {
    const sanitized = chainId
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value));
    if (sanitized.length > 0) {
      values.push(sanitized);
      conditions.push(`chain_id = ANY($${values.length}::BIGINT[])`);
    }
  } else if (typeof chainId === 'number' && Number.isFinite(chainId) && chainId > 0) {
    values.push(Number(chainId));
    conditions.push(`chain_id = $${values.length}::BIGINT`);
  }

  if (typeof startBlock === 'number') {
    values.push(Number(startBlock));
    conditions.push(`block_number >= $${values.length}::BIGINT`);
  }

  if (typeof endBlock === 'number') {
    values.push(Number(endBlock));
    conditions.push(`block_number <= $${values.length}::BIGINT`);
  }

  const normalizedStartTime = normalizeDateInput(startTime);
  if (normalizedStartTime) {
    values.push(normalizedStartTime);
    conditions.push(`time_stamp >= $${values.length}::TIMESTAMPTZ`);
  }

  const normalizedEndTime = normalizeDateInput(endTime);
  if (normalizedEndTime) {
    values.push(normalizedEndTime);
    conditions.push(`time_stamp <= $${values.length}::TIMESTAMPTZ`);
  }

  const clause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  return { clause, values };
};

const queryTransfersPage = async ({
  chainId,
  page,
  pageSize,
  sort,
  startBlock,
  endBlock,
}) => {
  if (!storeEnabled || !storeReady) {
    throw new Error('Persistent store unavailable');
  }

  const normalizedPage = Math.max(1, Number.parseInt(page, 10) || 1);
  const normalizedPageSize = Math.max(1, Number.parseInt(pageSize, 10) || 25);
  const offset = (normalizedPage - 1) * normalizedPageSize;
  const orderDirection = sort === 'asc' ? 'ASC' : 'DESC';
  const chainFilter = Array.isArray(chainId) ? chainId : [chainId];
  const { clause, values } = buildFilterClause({
    chainId: chainFilter,
    startBlock,
    endBlock,
  });

  const queryValues = [...values, normalizedPageSize, offset];
  const rows = await withClient((client) => client.query(
    `
      SELECT chain_id, block_number, tx_hash, log_index, time_stamp, payload
      FROM transfer_events
      ${clause}
      ORDER BY time_stamp ${orderDirection}, block_number ${orderDirection}, log_index ${orderDirection}
  LIMIT $${values.length + 1}::INT
  OFFSET $${values.length + 2}::INT;
    `,
    queryValues
  ));

  const transfers = rows.rows.map((row) => ({
    ...row.payload,
    chainId: row.chain_id,
    blockNumber: String(row.block_number),
    timeStamp: row.payload?.timeStamp || row.payload?.time_stamp || String(Math.floor(new Date(row.time_stamp).getTime() / 1000)),
  }));

  return {
    transfers,
    timestamp: Date.now(),
    resultLength: transfers.length,
  };
};

const countTransfers = async ({ chainId, startBlock, endBlock }) => {
  if (!storeEnabled || !storeReady) {
    throw new Error('Persistent store unavailable');
  }

  const chainFilter = Array.isArray(chainId) ? chainId : [chainId];
  const { clause, values } = buildFilterClause({ chainId: chainFilter, startBlock, endBlock });

  const result = await withClient((client) => client.query(
    `
      SELECT COUNT(*) AS total
      FROM transfer_events
      ${clause};
    `,
    values
  ));

  const total = Number.parseInt(result.rows[0]?.total || '0', 10) || 0;
  return total;
};

const getMaxTimestamp = async ({ chainId }) => {
  if (!storeEnabled || !storeReady) {
    throw new Error('Persistent store unavailable');
  }

  const chainFilter = Array.isArray(chainId) ? chainId : [chainId];
  const { clause, values } = buildFilterClause({ chainId: chainFilter });

  const result = await withClient((client) => client.query(
    `
      SELECT MAX(time_stamp) AS last_time
      FROM transfer_events
      ${clause};
    `,
    values
  ));

  const lastTime = result.rows[0]?.last_time ? new Date(result.rows[0].last_time) : null;
  const lagSeconds = lastTime ? Math.max(0, Math.floor((Date.now() - lastTime.getTime()) / 1000)) : null;

  return {
    lastTime,
    lagSeconds,
  };
};

const queryDailyAnalytics = async ({ chainId, startTime, endTime }) => {
  if (!storeEnabled || !storeReady) {
    throw new Error('Persistent store unavailable');
  }

  const chainFilter = Array.isArray(chainId) ? chainId : [chainId];
  const { clause, values } = buildFilterClause({ chainId: chainFilter, startTime, endTime });

  const result = await withClient((client) => client.query(
    `
      SELECT
        DATE_TRUNC('day', time_stamp) AS day,
        COUNT(*) AS transfer_count,
  SUM(value::numeric) AS volume_raw,
        ARRAY_REMOVE(ARRAY_AGG(DISTINCT from_address), NULL) AS from_addresses,
        ARRAY_REMOVE(ARRAY_AGG(DISTINCT to_address), NULL) AS to_addresses,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY value::numeric) AS median_raw
      FROM transfer_events
      ${clause}
      GROUP BY day
      ORDER BY day ASC;
    `,
    values
  ));

  return result.rows.map((row) => ({
    day: row.day ? new Date(row.day) : null,
    transferCount: Number.parseInt(row.transfer_count, 10) || 0,
    volumeRaw: row.volume_raw || '0',
    medianRaw: row.median_raw || null,
    fromAddresses: Array.isArray(row.from_addresses) ? row.from_addresses : [],
    toAddresses: Array.isArray(row.to_addresses) ? row.to_addresses : [],
  }));
};

const queryAnalyticsSummary = async ({ chainId, startTime, endTime }) => {
  if (!storeEnabled || !storeReady) {
    throw new Error('Persistent store unavailable');
  }

  const chainFilter = Array.isArray(chainId) ? chainId : [chainId];
  const { clause, values } = buildFilterClause({ chainId: chainFilter, startTime, endTime });

  const result = await withClient((client) => client.query(
    `
      WITH filtered AS (
        SELECT *
        FROM transfer_events
        ${clause}
      )
      SELECT
        COALESCE((SELECT COUNT(*) FROM filtered), 0) AS total_transfers,
  COALESCE((SELECT SUM(value::numeric) FROM filtered), 0::NUMERIC) AS volume_raw,
        (SELECT MIN(time_stamp) FROM filtered) AS first_time,
        (SELECT MAX(time_stamp) FROM filtered) AS last_time,
        COALESCE((
          SELECT COUNT(DISTINCT address)
          FROM (
            SELECT from_address AS address FROM filtered
            UNION
            SELECT to_address AS address FROM filtered
          ) participants
        ), 0) AS unique_addresses
      ;
    `,
    values
  ));

  const row = result.rows[0] || {};
  return {
    totalTransfers: Number.parseInt(row.total_transfers, 10) || 0,
    volumeRaw: row.volume_raw || '0',
    firstTime: row.first_time ? new Date(row.first_time) : null,
    lastTime: row.last_time ? new Date(row.last_time) : null,
    uniqueAddresses: Number.parseInt(row.unique_addresses, 10) || 0,
  };
};

const queryChainDistribution = async ({ chainId, startTime, endTime }) => {
  if (!storeEnabled || !storeReady) {
    throw new Error('Persistent store unavailable');
  }

  const chainFilter = Array.isArray(chainId) ? chainId : [chainId];
  const { clause, values } = buildFilterClause({ chainId: chainFilter, startTime, endTime });

  const result = await withClient((client) => client.query(
    `
      WITH filtered AS (
        SELECT *
        FROM transfer_events
        ${clause}
      ),
      unique_addresses AS (
        SELECT
          chain_id,
          COUNT(DISTINCT address) AS unique_addresses
        FROM (
          SELECT chain_id, from_address AS address FROM filtered
          UNION
          SELECT chain_id, to_address AS address FROM filtered
        ) participants
        GROUP BY chain_id
      )
      SELECT
        f.chain_id,
        COUNT(*) AS transfer_count,
  SUM(f.value::numeric) AS volume_raw,
        COALESCE(u.unique_addresses, 0) AS unique_addresses
      FROM filtered f
      LEFT JOIN unique_addresses u ON u.chain_id = f.chain_id
      GROUP BY f.chain_id, u.unique_addresses
      ORDER BY SUM(f.value::numeric) DESC NULLS LAST;
    `,
    values
  ));

  return result.rows.map((row) => ({
    chainId: Number.parseInt(row.chain_id, 10) || 0,
    transferCount: Number.parseInt(row.transfer_count, 10) || 0,
    volumeRaw: row.volume_raw || '0',
    uniqueAddresses: Number.parseInt(row.unique_addresses, 10) || 0,
  }));
};

const queryTopAddresses = async ({ chainId, startTime, endTime, limit = 20 }) => {
  if (!storeEnabled || !storeReady) {
    throw new Error('Persistent store unavailable');
  }

  const chainFilter = Array.isArray(chainId) ? chainId : [chainId];
  const { clause, values } = buildFilterClause({ chainId: chainFilter, startTime, endTime });
  const adjustedLimit = Math.max(1, Number.parseInt(limit, 10) || 20);

  const result = await withClient((client) => client.query(
    `
      WITH filtered AS (
        SELECT *
        FROM transfer_events
        ${clause}
      ),
      senders AS (
        SELECT from_address AS address, COUNT(*) AS sent, SUM(value::numeric) AS sent_volume
        FROM filtered
        GROUP BY from_address
      ),
      receivers AS (
        SELECT to_address AS address, COUNT(*) AS received, SUM(value::numeric) AS received_volume
        FROM filtered
        GROUP BY to_address
      )
      SELECT
        COALESCE(s.address, r.address) AS address,
        COALESCE(s.sent, 0) AS sent,
        COALESCE(r.received, 0) AS received,
  COALESCE(s.sent_volume, 0::NUMERIC) + COALESCE(r.received_volume, 0::NUMERIC) AS volume_raw,
        COALESCE(s.sent, 0) + COALESCE(r.received, 0) AS total_txs
      FROM senders s
      FULL OUTER JOIN receivers r ON s.address = r.address
      WHERE COALESCE(s.address, r.address) IS NOT NULL
  ORDER BY total_txs DESC, (COALESCE(s.sent_volume, 0::NUMERIC) + COALESCE(r.received_volume, 0::NUMERIC)) DESC
      LIMIT $${values.length + 1}::INT;
    `,
    [...values, adjustedLimit]
  ));

  return result.rows.map((row) => ({
    address: row.address,
    sent: Number.parseInt(row.sent, 10) || 0,
    received: Number.parseInt(row.received, 10) || 0,
    totalTxs: Number.parseInt(row.total_txs, 10) || 0,
    volumeRaw: row.volume_raw || '0',
  }));
};

const queryTopTransfers = async ({ chainId, startTime, endTime, limit = 20 }) => {
  if (!storeEnabled || !storeReady) {
    throw new Error('Persistent store unavailable');
  }

  const chainFilter = Array.isArray(chainId) ? chainId : [chainId];
  const { clause, values } = buildFilterClause({ chainId: chainFilter, startTime, endTime });
  const adjustedLimit = Math.max(1, Number.parseInt(limit, 10) || 20);

  const result = await withClient((client) => client.query(
    `
      WITH filtered AS (
        SELECT *
        FROM transfer_events
        ${clause}
      )
      SELECT
        chain_id,
        tx_hash,
        from_address,
        to_address,
        value::numeric AS volume_raw,
        EXTRACT(EPOCH FROM time_stamp) AS timestamp,
        block_number,
        log_index
      FROM filtered
      ORDER BY value::numeric DESC
      LIMIT $${values.length + 1}::INT;
    `,
    [...values, adjustedLimit]
  ));

  return result.rows.map((row) => ({
    chainId: Number.parseInt(row.chain_id, 10) || 0,
    txHash: row.tx_hash,
    from: row.from_address,
    to: row.to_address,
    volumeRaw: row.volume_raw || '0',
    timestamp: row.timestamp ? Number(row.timestamp) : null,
    blockNumber: Number.parseInt(row.block_number, 10) || null,
    logIndex: Number.parseInt(row.log_index, 10) || null,
  }));
};

const getPersistentStoreStatus = async () => {
  if (!storeEnabled) {
    return {
      enabled: false,
      ready: false,
      reason: initializationError ? initializationError.message : 'Persistent store disabled',
    };
  }

  if (!storeReady) {
    return {
      enabled: true,
      ready: false,
      reason: initializationError ? initializationError.message : 'Initialization pending',
    };
  }

  const summary = await getLatestIngestSummary();
  return {
    enabled: true,
    ready: true,
    summary,
  };
};

const closePersistentStore = async () => {
  if (pool) {
    await pool.end();
    pool = null;
    storeReady = false;
    storeEnabled = false;
  }
};

module.exports = {
  initPersistentStore,
  isPersistentStoreEnabled,
  isPersistentStoreReady,
  getPersistentStoreError,
  storeTransfers,
  updateIngestCursor,
  recordIngestEvent,
  refreshChainTotals,
  refreshChainDailyAggregates,
  refreshChainAggregates,
  recordIngestStatusSuccess,
  recordIngestStatusFailure,
  getIngestCursor,
  getLatestIngestSummary,
  getChainSnapshots,
  getDailyAggregates,
  queryTransfersPage,
  countTransfers,
  getMaxTimestamp,
  queryDailyAnalytics,
  queryAnalyticsSummary,
  queryChainDistribution,
  queryTopAddresses,
  queryTopTransfers,
  getPersistentStoreStatus,
  closePersistentStore,
};
