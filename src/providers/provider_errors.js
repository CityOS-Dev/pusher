// Public machine causes plus hashes; SDK messages may contain tokens/URLs.
const crypto = require('node:crypto')
const TRANSIENT = new Set([
  'TooManyRequests', 'InternalServerError', 'ServiceUnavailable', 'Shutdown',
  'ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'ERR_HTTP2_STREAM_CANCEL',
  'app/network-error', 'messaging/server-unavailable', 'messaging/internal-error',
  'messaging/quota-exceeded', 'messaging/message-rate-exceeded',
])
const TRANSPORT = new Set(['ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'ERR_HTTP2_STREAM_CANCEL', 'app/network-error'])
const INVALID_TOKEN = new Set(['BadDeviceToken', 'Unregistered', 'messaging/invalid-registration-token', 'messaging/registration-token-not-registered'])
const CONFIGURATION = new Set([
  'BadCertificate', 'BadCertificateEnvironment', 'ExpiredProviderToken', 'InvalidProviderToken',
  'MissingProviderToken', 'BadTopic', 'TopicDisallowed', 'DeviceTokenNotForTopic',
  'ERR_TLS_CERT_ALTNAME_INVALID', 'CERT_HAS_EXPIRED', 'MISSING_CREDENTIALS',
  'messaging/mismatched-credential', 'messaging/authentication-error', 'app/invalid-credential',
])
const PAYLOAD = new Set(['BadExpirationDate', 'BadPriority', 'PayloadEmpty', 'PayloadTooLarge', 'messaging/invalid-argument', 'messaging/payload-size-limit-exceeded'])
const hash = value => crypto.createHash('sha256').update(String(value)).digest('hex')

function describeFailure(input, provider = 'unknown') {
  const failure = input || {}
  const error = failure.error || failure
  const reason = failure.response && failure.response.reason
  const message = typeof error === 'string' ? error : String(error.message || '')
  let code = reason || error.errorInfo?.code || error.code
  if (typeof code !== 'string' || !/^[A-Za-z0-9_./-]{1,128}$/.test(code)) {
    code = [...TRANSPORT, ...CONFIGURATION].find(c => message.includes(c)) || 'UNKNOWN_ERROR'
  }
  const status = failure.status ?? error.statusCode ?? error.status
  const httpStatus = Number.isInteger(status) && status >= 100 && status <= 599 ? status : null
  let category = 'unknown'
  if (INVALID_TOKEN.has(code)) category = 'invalid_token'
  else if (CONFIGURATION.has(code)) category = 'configuration'
  else if (PAYLOAD.has(code)) category = 'payload'
  else if (TRANSIENT.has(code) || httpStatus === 429 || httpStatus >= 500) category = 'transient'
  return {
    error: code === 'UNKNOWN_ERROR' ? 'Unknown error' : code,
    error_code: code,
    error_type: TRANSPORT.has(code) ? 'transport' : provider,
    http_status: httpStatus,
    retryable: category === 'transient',
    category,
    error_hash: hash(JSON.stringify({ code, reason: reason || null, httpStatus, message })),
  }
}

function tokenFingerprint(token) {
  return { token_hash: token ? hash(token) : null, token_tail: token ? String(token).slice(-4) : null }
}

module.exports = { describeFailure, tokenFingerprint }
