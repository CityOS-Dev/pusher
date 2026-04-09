// src/providers/fcm.js
const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');
const pino = require('pino');

const logger = pino({ level: 'info' });

const instances = new Map();

function getApp(appConfig) {
  if (instances.has(appConfig.id)) {
    return instances.get(appConfig.id);
  }

  const keyPath = path.resolve(appConfig.serviceAccount);

  if (!fs.existsSync(keyPath)) {
    throw new Error(`Firebase service account key not found: ${keyPath}`);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(keyPath, 'utf8'));

  const app = admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  }, appConfig.id);

  instances.set(appConfig.id, app);
  return app;
}

function isInvalidTokenError(error) {
  if (!error) return false;
  const code = error.code;
  return code === 'messaging/invalid-registration-token' ||
         code === 'messaging/registration-token-not-registered';
}

async function send(recipients, request, appConfig) {
  const app = getApp(appConfig);
  const tokens = recipients.map(r => r.token);
  const failures = [];
  const invalidTokens = [];

  const message = {
    ...request.payload,
    tokens
  };

  // Add FCM-specific options from headers
  if (request.headers) {
    message.options = {};
    if (request.headers['fcm-priority']) {
      message.options.priority = request.headers['fcm-priority'];
    }
    if (request.headers['fcm-ttl']) {
      message.options.timeToLive = parseInt(request.headers['fcm-ttl']);
    }
  }

  let result;

  try {
    result = await app.messaging().sendEachForMulticast(message);
  } catch (error) {
    logger.error({ error, appId: appConfig.id }, 'FCM send failed');
    throw error;
  }

  // Process responses
  result.responses.forEach((response, index) => {
    const recipient = recipients[index];

    if (!response.success) {
      const error = response.error;
      failures.push({
        id: recipient.id,
        token: recipient.token,
        error: error ? error.message : 'Unknown error'
      });

      if (isInvalidTokenError(error)) {
        invalidTokens.push({
          external_id: recipient.id,
          token: recipient.token,
          app_id: appConfig.id,
          type: 'android',
          reason: error.code
        });
      }
    }
  });

  return {
    success: result.successCount,
    failures,
    invalidTokens
  };
}

module.exports = {
  send
};
