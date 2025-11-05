'use strict';

require('dotenv').config();

const persistentStore = require('./src/persistentStore');
const { startTransferIngestion, stopTransferIngestion } = require('./src/transfersIngestion');
const {
  CHAINS,
  fetchTransfersPageFromChain,
} = require('./src/providers/transfersProvider');

const logger = console;

const createFetchPage = () => async ({ chain, page, pageSize, sort, startBlock, endBlock }) => {
  return fetchTransfersPageFromChain({ chain, page, pageSize, sort, startBlock, endBlock });
};

const start = async () => {
  logger.log('🚀 Starting BZR ingester service...');

  const storeStatus = await persistentStore.initPersistentStore();
  if (!storeStatus.enabled) {
    logger.error('X Persistent store disabled – cannot start ingester');
    process.exit(1);
  }

  if (!storeStatus.ready) {
    logger.error('X Persistent store not ready – cannot start ingester');
    process.exit(1);
  }

  const controller = startTransferIngestion({
    chains: CHAINS,
    fetchPage: createFetchPage(),
    logger,
  });

  const shutdown = async (signal) => {
    logger.warn(`⚠️  Received ${signal}. Shutting down ingester...`);
    try {
      stopTransferIngestion();
      await persistentStore.closePersistentStore();
    } catch (error) {
      logger.error('X Error during ingester shutdown:', error.message || error);
    } finally {
      process.exit(0);
    }
  };

  process.on('SIGINT', shutdown.bind(null, 'SIGINT'));
  process.on('SIGTERM', shutdown.bind(null, 'SIGTERM'));

  process.on('unhandledRejection', (error) => {
    logger.error('X Unhandled promise rejection in ingester:', error);
  });

  return controller;
};

start().catch((error) => {
  logger.error('X Failed to start ingester:', error.message || error);
  process.exit(1);
});
