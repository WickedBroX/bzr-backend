'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { newDb } = require('pg-mem');

// Replace pg Pool with pg-mem implementation before requiring persistent store
const db = newDb({ autoCreateForeignKeyIndices: true });
const pgMem = db.adapters.createPg();
const pgModulePath = require.resolve('pg');
require.cache[pgModulePath] = {
  exports: {
    Pool: pgMem.Pool,
  },
};

process.env.TRANSFERS_DATABASE_URL = 'postgres://user:pass@localhost:5432/test_db';

const persistentStore = require('../src/persistentStore');

const SAMPLE_TRANSFER = {
  blockNumber: '12345',
  timeStamp: '1700000000',
  hash: '0xabc',
  nonce: '1',
  blockHash: '0xblock',
  from: '0x1111111111111111111111111111111111111111',
  contractAddress: '0xcontract',
  to: '0x2222222222222222222222222222222222222222',
  value: '1000000000000000000',
  tokenName: 'Bazaars',
  tokenSymbol: 'BZR',
  tokenDecimal: '18',
  transactionIndex: '0',
  gas: '21000',
  gasPrice: '1000000000',
  gasUsed: '21000',
  cumulativeGasUsed: '21000',
  input: '0x',
  methodId: '0xa9059cbb',
  functionName: 'transfer(address,uint256)',
  confirmations: '10',
  logIndex: '0',
};

test('persistent store initializes and stores transfers', async (t) => {
  t.after(async () => {
    await persistentStore.closePersistentStore();
  });

  const status = await persistentStore.initPersistentStore();
  assert.equal(status.ready, true);

  const insertResult = await persistentStore.storeTransfers(1, [SAMPLE_TRANSFER]);
  assert.equal(insertResult.inserted, 1);

  const page = await persistentStore.queryTransfersPage({
    chainId: 1,
    page: 1,
    pageSize: 10,
    sort: 'desc',
  });
  assert.equal(page.transfers.length, 1);
  assert.equal(page.transfers[0].hash, SAMPLE_TRANSFER.hash);

  const total = await persistentStore.countTransfers({ chainId: 1 });
  assert.equal(total, 1);

  await persistentStore.updateIngestCursor(1, {
    blockNumber: Number(SAMPLE_TRANSFER.blockNumber),
    txHash: SAMPLE_TRANSFER.hash,
    logIndex: Number(SAMPLE_TRANSFER.logIndex),
    timeStamp: Number(SAMPLE_TRANSFER.timeStamp),
  });

  const summary = await persistentStore.getLatestIngestSummary();
  assert.equal(summary.length, 1);
  assert.equal(summary[0].chainId, 1);

  const statusSnapshot = await persistentStore.getPersistentStoreStatus();
  assert.equal(statusSnapshot.ready, true);
});
