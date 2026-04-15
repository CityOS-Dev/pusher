// src/providers/apn.js
const apn = require('@parse/node-apn')
const fs = require('fs')
const path = require('path')

function createProvider(appConfig) {
  const certPath = path.resolve(appConfig.cert)

  if (!fs.existsSync(certPath)) {
    throw new Error(`APN certificate not found: ${certPath}`)
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

  // Map tokens to their external IDs for error tracking
  const tokenToIdMap = {}
  recipients.forEach(r => {
    tokenToIdMap[r.token] = r.id
  })

  result.failed.forEach(failed => {
    const tokenId = tokenToIdMap[failed.device]
    if (tokenId) {
      failures.push({
        id: tokenId,
        token: failed.device,
        error: failed.response ? failed.response.reason : 'Unknown error',
      })

      if (isInvalidTokenError(failed.response)) {
        invalidTokens.push({
          external_id: tokenId,
          token: failed.device,
          app_id: appConfig.id,
          type: 'ios',
          reason: failed.response.reason,
        })
      }
    }
  })

  provider.shutdown()

  return {
    success: result.sent.length,
    failures,
    invalidTokens,
  }
}

module.exports = {
  send,
}
