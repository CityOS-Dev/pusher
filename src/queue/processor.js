// src/queue/processor.js
const apnProvider = require('../providers/apn')
const fcmProvider = require('../providers/fcm')
const tracker = require('../providers/call_tracker')
const { upsertStats } = require('../db/stats')
const { logError } = require('../db/errors')
const { addInvalidTokens } = require('../db/tokens')
const apps = require('../../config/apps.config')
const pino = require('pino')
const { describeFailure, tokenFingerprint } = require('../providers/provider_errors')

const logger = pino({ level: 'info' })

function getAppConfig(appId) {
  return apps.apps.find(app => app.id === appId)
}

async function processJob(job) {
  const { app_id, recipients, request } = job.data
  const p = request && request.payload
  const callId = p?.call_id || p?.callId || p?.data?.call_id || p?.data?.callId || null
  const attempt = { call_id: callId, job_id: job.id, attempt_index: (job.attemptsMade || 0) + 1 }

  logger.info({ jobId: job.id, appId: app_id, recipientCount: recipients.length }, 'Processing job')

  const appConfig = getAppConfig(app_id)

  if (!appConfig) {
    throw new Error(`App config not found: ${app_id}`)
  }

  let provider
  if (appConfig.type === 'ios') {
    provider = apnProvider
  } else if (appConfig.type === 'android') {
    provider = fcmProvider
  } else {
    throw new Error(`Unknown app type: ${appConfig.type}`)
  }

  try {
    const result = await provider.send(recipients, request, appConfig)

    // Record stats
    const hour = new Date().toISOString().slice(0, 13) + ':00'
    upsertStats(hour, app_id, result.success, result.failures.length)

    // Log invalid tokens
    if (result.invalidTokens.length > 0) {
      addInvalidTokens(result.invalidTokens)
      logger.info({ count: result.invalidTokens.length }, 'Invalid tokens logged')
    }

    // Log individual failures
    for (const failure of result.failures) {
      const { token, ...diagnostic } = failure
      logError(app_id, 'SEND_FAILED', JSON.stringify({ ...diagnostic, ...tokenFingerprint(token), ...attempt }))
    }

    logger.info({ jobId: job.id, success: result.success, failures: result.failures.length }, 'Job completed')

    // Call tracker: emit per-recipient stage. callId arrives in:
    //   - request.payload.call_id (iOS, PushNotification.php hik)
    //   - request.payload.data.call_id (Android, PushNotification.php hik)
    //   - request.payload.callId / data.callId (camelCase fallback — на случай если
    //     какой-то источник пришлёт camel-case; SIP-демон до фикса 2026-05-19 так делал)
    if (callId) {
      const isIos = appConfig.type === 'ios'
      const okStage   = isIos ? 'apns_ok'   : 'fcm_ok'
      const failStage = isIos ? 'apns_fail' : 'fcm_fail'
      for (const rcpt of recipients) {
        const fingerprint = tokenFingerprint(rcpt.token)
        const failureInfo = result.failures.find(f => f.token === rcpt.token && (f.id == null || String(f.id) === String(rcpt.id)))
        if (failureInfo) {
          await tracker.report(callId, failStage, {
            user_id: rcpt.id,
            ...fingerprint,
            error: failureInfo.error,
            error_code: failureInfo.error_code,
            error_type: failureInfo.error_type,
            http_status: failureInfo.http_status,
            retryable: failureInfo.retryable,
            category: failureInfo.category,
            error_hash: failureInfo.error_hash,
            ...attempt,
          })
        } else {
          await tracker.report(callId, okStage, {
            user_id: rcpt.id,
            ...fingerprint,
            ...attempt,
          })
        }
      }
    }

    return result
  } catch (error) {
    const cause = describeFailure(error, appConfig.type === 'ios' ? 'apns' : 'fcm')
    logger.error({ ...cause, ...attempt }, 'Job failed')
    logError(app_id, 'JOB_ERROR', JSON.stringify({ ...cause, ...attempt }))

    if (callId) {
      const failStage = appConfig.type === 'ios' ? 'apns_fail' : 'fcm_fail'
      for (const rcpt of recipients) {
        await tracker.report(callId, failStage, {
          user_id: rcpt.id,
          ...tokenFingerprint(rcpt.token),
          ...cause,
          ...attempt,
        })
      }
    }

    throw error
  }
}

module.exports = {
  getAppConfig,
  processJob,
}
