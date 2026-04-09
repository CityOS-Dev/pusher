// src/queue/processor.js
const apnProvider = require('../providers/apn');
const fcmProvider = require('../providers/fcm');
const { upsertStats } = require('../db/stats');
const { logError } = require('../db/errors');
const { addInvalidTokens } = require('../db/tokens');
const apps = require('../../config/apps.config');
const pino = require('pino');

const logger = pino({ level: 'info' });

function getAppConfig(appId) {
  return apps.apps.find(app => app.id === appId);
}

async function processJob(job) {
  const { app_id, recipients, request } = job.data;

  logger.info({ jobId: job.id, appId: app_id, recipientCount: recipients.length }, 'Processing job');

  const appConfig = getAppConfig(app_id);

  if (!appConfig) {
    throw new Error(`App config not found: ${app_id}`);
  }

  let provider;
  if (appConfig.type === 'ios') {
    provider = apnProvider;
  } else if (appConfig.type === 'android') {
    provider = fcmProvider;
  } else {
    throw new Error(`Unknown app type: ${appConfig.type}`);
  }

  try {
    const result = await provider.send(recipients, request, appConfig);

    // Record stats
    const hour = new Date().toISOString().slice(0, 13) + ':00';
    upsertStats(hour, app_id, result.success, result.failures.length);

    // Log invalid tokens
    if (result.invalidTokens.length > 0) {
      addInvalidTokens(result.invalidTokens);
      logger.info({ count: result.invalidTokens.length }, 'Invalid tokens logged');
    }

    // Log individual failures
    for (const failure of result.failures) {
      logError(app_id, 'SEND_FAILED', JSON.stringify(failure));
    }

    logger.info({ jobId: job.id, success: result.success, failures: result.failures.length }, 'Job completed');

    return result;
  } catch (error) {
    logger.error({ error, jobId: job.id }, 'Job failed');
    logError(app_id, 'JOB_ERROR', error.message);
    throw error;
  }
}

module.exports = {
  getAppConfig,
  processJob
};
