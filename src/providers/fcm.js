// src/providers/fcm.js
const admin = require('firebase-admin')
const fs = require('fs')
const path = require('path')
const pino = require('pino')

const logger = pino({ level: 'info' })

const instances = new Map()

function getApp(appConfig) {
  if (instances.has(appConfig.id)) {
    return instances.get(appConfig.id)
  }

  const keyPath = path.resolve(appConfig.serviceAccount)

  if (!fs.existsSync(keyPath)) {
    throw new Error(`Firebase service account key not found: ${keyPath}`)
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
        errCode: error.errorInfo?.code || error.code,
        errMsg: String(error.message || '').slice(0, 200),
      }, 'FCM transient network error, retrying')
      await sleep(delay)
    }
  }
  logger.error({
    appId, attempts: RETRY_DELAYS_MS.length + 1,
    errCode: lastError.errorInfo?.code || lastError.code,
  }, 'FCM send failed after all retries')
  throw lastError
}

async function send(recipients, request, appConfig) {
  const app = getApp(appConfig)
  const tokens = recipients.map(r => r.token)
  const failures = []
  const invalidTokens = []

  const message = {
    ...request.payload,
    tokens,
  }

  // Add FCM-specific options from headers
  if (request.headers) {
    message.options = {}
    if (request.headers['fcm-priority']) {
      message.options.priority = request.headers['fcm-priority']
    }
    if (request.headers['fcm-ttl']) {
      message.options.timeToLive = parseInt(request.headers['fcm-ttl'])
    }
  }

  let result

  try {
    result = await sendMulticastWithRetry(app, message, appConfig.id)
  } catch (error) {
    logger.error({ error, appId: appConfig.id }, 'FCM send failed')
    throw error
  }

  // Process responses
  result.responses.forEach((response, index) => {
    const recipient = recipients[index]

    if (!response.success) {
      const error = response.error
      failures.push({
        id: recipient.id,
        token: recipient.token,
        error: error ? error.message : 'Unknown error',
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
    success: result.successCount,
    failures,
    invalidTokens,
  }
}

module.exports = {
  send,
}
