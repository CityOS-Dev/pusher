// src/providers/apn.js
const apn = require('@parse/node-apn')
const fs = require('fs')
const path = require('path')
const { describeFailure } = require('./provider_errors')
const logger = require('pino')({ level: 'info' })

function createProvider(appConfig) {
  const certPath = path.resolve(appConfig.cert)

  if (!fs.existsSync(certPath)) {
    throw Object.assign(new Error('APN certificate not found'), { code: 'MISSING_CREDENTIALS' })
  }

  const options = {
    cert: fs.readFileSync(certPath),
    key: fs.readFileSync(certPath),
    passphrase: appConfig.passphrase || undefined,
    production: appConfig.production !== false,
  }

  return new apn.Provider(options)
}

function isInvalidTokenError(response) {
  if (!response) return false
  const reason = response.reason
  return reason === 'BadDeviceToken' || reason === 'Unregistered'
}

async function send(recipients, request, appConfig) {
  const provider = createProvider(appConfig)
  try {
    const failures = []
    const invalidTokens = []

    const notification = new apn.Notification()
    notification.payload = { ...request.payload }

    // Add custom headers
    if (request.headers) {
      Object.entries(request.headers).forEach(([key, value]) => {
        if (key === 'apns-priority') {
          notification.priority = parseInt(value)
        } else if (key === 'apns-expiration') {
          notification.expiry = parseInt(value)
        } else if (key === 'apns-push-type') {
          notification.pushType = value  // For VoIP: 'voip', 'alert', etc.
        } else if (key === 'apns-topic') {
          notification.topic = value  // Override topic from config
        }
      })
    }

    // Use topic from header, config, or undefined
    notification.topic = notification.topic || appConfig.topic || undefined

    const deviceTokens = recipients.map(r => r.token)
    const result = await provider.send(notification, deviceTokens)

    // Attribute each response to its token entry, not an optional/shared id.
    const failedByToken = new Map(result.failed.map(f => [f.device, f]))
    for (const recipient of recipients) {
      const failed = failedByToken.get(recipient.token)
      if (!failed) continue
      failures.push({
        id: recipient.id ?? null,
        token: recipient.token,
        ...describeFailure(failed, 'apns'),
      })
      if (isInvalidTokenError(failed.response)) {
        invalidTokens.push({
          external_id: recipient.id ?? null,
          token: recipient.token,
          app_id: appConfig.id,
          type: 'ios',
          reason: failed.response.reason,
        })
      }
    }

    return {
      success: result.sent.length,
      failures,
      invalidTokens,
    }
  } finally {
    try { await provider.shutdown() }
    catch (error) { logger.warn(describeFailure(error, 'apns'), 'APN provider cleanup failed') }
  }
}

module.exports = {
  send,
}
