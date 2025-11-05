'use strict';

const { Pool } = require('pg');

const DEFAULT_MAX_CLIENTS = Number(process.env.TRANSFERS_DB_MAX_CLIENTS || process.env.DB_MAX_CLIENTS || 10);
const DEFAULT_IDLE_TIMEOUT_MS = Number(process.env.TRANSFERS_DB_IDLE_TIMEOUT_MS || 30_000);
const DEFAULT_CONNECTION_TIMEOUT_MS = Number(process.env.TRANSFERS_DB_CONNECTION_TIMEOUT_MS || 5_000);

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

  if (normalized.length === 0) {
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
    ) VALUES ${normalized
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

  const values = normalized.flatMap((record) => [
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

const buildFilterClause = ({ chainId, startBlock, endBlock }) => {
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
  getIngestCursor,
  getLatestIngestSummary,
  queryTransfersPage,
  countTransfers,
  getMaxTimestamp,
  getPersistentStoreStatus,
  closePersistentStore,
};
