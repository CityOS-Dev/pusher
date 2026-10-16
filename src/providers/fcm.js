// src/providers/fcm.js
const admin = require('firebase-admin')
const fs = require('fs')
const path = require('path')
const pino = require('pino')
const { describeFailure } = require('./provider_errors')

const logger = pino({ level: 'info' })

const instances = new Map()

function getApp(appConfig) {
  if (instances.has(appConfig.id)) {
    return instances.get(appConfig.id)
  }

  const keyPath = path.resolve(appConfig.serviceAccount)

  if (!fs.existsSync(keyPath)) {
    throw Object.assign(new Error('Firebase service account key not found'), { code: 'MISSING_CREDENTIALS' })
  }

  const serviceAccount = JSON.parse(fs.readFileSync(keyPath, 'utf8'))

  const app = admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  }, appConfig.id)

  instances.set(appConfig.id, app)
  return app
}

function isInvalidTokenError(error) {
  if (!error) return false
  const code = error.code
  return code === 'messaging/invalid-registration-token' ||
    code === 'messaging/registration-token-not-registered'
}

// Top-level errors from Firebase SDK that we should retry: transient network
// blips where Google reset the HTTP/2 stream or DNS failed. Firebase SDK does
// NOT auto-retry these — observed 2026-05-19 как pusher_http_error for SIP
// home_80/user_328. После единственного ECONNRESET вся multicast улетает в
// throw, и PHP-сторона видит HTTP error => call-tracker красным.
function isRetriableNetworkError(error) {
  if (!error) return false
  const code = error.errorInfo?.code || error.code
  if (code === 'app/network-error') return true
  const msg = String(error.message || '')
  return /ECONNRESET|ETIMEDOUT|EAI_AGAIN|socket hang up|network timeout/i.test(msg)
}

const RETRY_DELAYS_MS = [500, 1000, 2000]   // exp.backoff: 1 initial + 3 retries = до 4 попыток total

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms))
}

async function sendMulticastWithRetry(app, message, appId) {
  let lastError
  for (let attempt = 1; attempt <= RETRY_DELAYS_MS.length + 1; attempt++) {
    try {
      const result = await app.messaging().sendEachForMulticast(message)
      if (attempt > 1) {
        logger.info({ appId, attempt, recipientCount: message.tokens.length },
          'FCM send succeeded after retry')
      }
      return result
    } catch (error) {
      lastError = error
      if (!isRetriableNetworkError(error)) throw error
      if (attempt > RETRY_DELAYS_MS.length) break
      const delay = RETRY_DELAYS_MS[attempt - 1]
      logger.warn({
        appId, attempt, delay,
        ...describeFailure(error, 'fcm'),
      }, 'FCM transient network error, retrying')
      await sleep(delay)
    }
  }
  logger.error({
    appId, attempts: RETRY_DELAYS_MS.length + 1,
    ...describeFailure(lastError, 'fcm'),
  }, 'FCM send failed after all retries')
  throw lastError
}

async function send(recipients, request, appConfig, delivery = {}) {
  const app = getApp(appConfig)
  const tokens = recipients.map(r => r.token)
  const failures = []
  const invalidTokens = []

  const message = {
    ...request.payload,
    tokens,
  }

  // FCM HTTP v1 options from headers.
  // NOTE: The HTTP v1 API carries priority and TTL in the `android` config,
  // NOT in the legacy `options` object. `options.priority` / `options.timeToLive`
  // are legacy FCM fields that the HTTP v1 endpoint silently ignores, so any
  // priority / TTL set there was never actually applied.
  if (request.headers) {
    const fcmPriority = request.headers['fcm-priority']
    const fcmTtl = request.headers['fcm-ttl']
    if (fcmPriority || fcmTtl) {
      message.android = { ...(message.android || {}) }
      if (fcmPriority) {
        // v1 expects one of: "high" | "normal"
        message.android.priority = fcmPriority
      }
      if (fcmTtl) {
        // fcm-ttl header is in seconds (legacy `time_to_live` convention);
        // the v1 AndroidConfig.ttl expects a duration in milliseconds.
        message.android.ttl = parseInt(fcmTtl, 10) * 1000
      }
    }
  }

  let result

  try {
    result = delivery.singleAttempt ? await app.messaging().sendEachForMulticast(message) : await sendMulticastWithRetry(app, message, appConfig.id)
  } catch (error) {
    logger.error({ ...describeFailure(error, 'fcm'), appId: appConfig.id }, 'FCM send failed')
    throw error
  }

  // Process responses
  recipients.forEach((recipient, index) => {
    const response = result.responses?.[index]
    if (!response || typeof response.success !== 'boolean') {
      failures.push({id:recipient.id,token:recipient.token,entry_index:recipient.entry_index,error_code:'MISSING_PROVIDER_RESULT',category:'unknown',retryable:false})
      return
    }

    if (!response.success) {
      const error = response.error
      failures.push({
        id: recipient.id,
        entry_index: recipient.entry_index,
        token: recipient.token,
        ...describeFailure(error, 'fcm'),
      })

      if (isInvalidTokenError(error)) {
        invalidTokens.push({
          external_id: recipient.id,
          token: recipient.token,
          app_id: appConfig.id,
          type: 'android',
          reason: error.code,
        })
      }
    }
  })

  return {
    success: recipients.reduce((count,_,i)=>count+(result.responses?.[i]?.success === true ? 1:0),0),
    failures,
    invalidTokens,
  }
}

module.exports = {
  send,
}
