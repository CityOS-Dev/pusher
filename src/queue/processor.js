const apnProvider = require('../providers/apn')
const fcmProvider = require('../providers/fcm')
const tracker = require('../providers/call_tracker')
const { upsertStats } = require('../db/stats')
const { logError } = require('../db/errors')
const { addInvalidTokens } = require('../db/tokens')
const { getStore } = require('../db/delivery')
const { providerBudget } = require('./provider_deadline')
const { DelayedError } = require('bullmq')
const DEFER_MS = 2000
const PROVIDER_TIMEOUT_MS = 15000
const RETRY_MARGIN_MS = 5000
const sipSourceGuard = require('./sip_source_guard')
const apps = require('../../config/apps.config')
const { describeFailure, tokenFingerprint } = require('../providers/provider_errors')
const logger = require('pino')({ level: 'info' })
let checkpointAvailable = true
function getAppConfig(appId) { return apps.apps.find(app => app.id === appId) }
function secondary(fn) {
  try { fn() } catch (error) { logger.warn(describeFailure(error), 'Secondary recording failed; delivery will not be repeated') }
}
async function defer(job, token, reason) {
  logger.warn({ reason_code: reason }, 'Delivery deferred before retrying safely')
  await job.moveToDelayed(Date.now() + DEFER_MS, token)
  throw new DelayedError()
}
async function storage(operation, job, token) {
  try {
    const result = operation()
    return result
  }
  catch (error) {
    checkpointAvailable = false
    logger.warn(describeFailure(error), 'Checkpoint storage unavailable; provider replay remains fenced')
    return defer(job, token, 'checkpoint_storage_unavailable')
  }
}
async function processJob(job, token) {
  const { app_id, recipients, request, delivery } = job.data
  const p = request && request.payload
  const callId = p?.call_id || p?.callId || p?.data?.call_id || p?.data?.callId || null
  const rawNamespace = p?.data?.user_id_namespace || p?.user_id_namespace
  const namespace = ['sb', 'tenant'].includes(rawNamespace) ? rawNamespace : null
  const attempt = { call_id: callId, job_id: job.id, attempt_index: (job.attemptsMade || 0) + 1 }
  const appConfig = getAppConfig(app_id)
  if (!appConfig || !['ios', 'android'].includes(appConfig.type)) throw new Error('Invalid app configuration')
  const store = await storage(() => getStore(), job, token)
  const deadline = delivery?.expires_at_ms
  const expired = () => Number.isSafeInteger(deadline) && Date.now() >= deadline
  let sourceEnded = false
  if (delivery?.source && !expired()) {
    const status = await sipSourceGuard.check(delivery.source)
    if (status === 'unknown') return defer(job, token, 'source_status_unknown')
    sourceEnded = status === 'ended'
  }
  const lease = sourceEnded || expired() ? null : providerBudget.acquire()
  if (!lease && !sourceEnded && !expired()) return defer(job, token, 'provider_capacity_unavailable')
  try {
    const claimed = await storage(() => store.claimMany(job.id, recipients.map((_, index) => index)), job, token)
    if (claimed.length) checkpointAvailable = true
    const entries = claimed.map(index => ({ ...recipients[index], entry_index: index }))
    if (!entries.length) return { success: 0, failures: [], invalidTokens: [] }
    const finish = async outcomes => {
      await storage(() => store.finishMany(job.id, outcomes), job, token)
      checkpointAvailable = true
    }
    if (sourceEnded || expired()) {
      const state = sourceEnded ? 'skipped' : 'expired'
      await finish(entries.map(r => ({ index: r.entry_index, state })))
      if (callId) for (const r of entries) secondary(() => tracker.report(callId, 'push_' + state, {
        ...attempt, user_id: r.id, user_id_namespace: namespace, entry_index: r.entry_index,
        ...tokenFingerprint(r.token), reason_code: sourceEnded ? 'call_ended' : 'expired',
      }))
      return { success: 0, failures: [], invalidTokens: [] }
    }
    const provider = appConfig.type === 'ios' ? apnProvider : fcmProvider
    let result
    try {
      // An SDK timeout is unknown, not cancellation. Its capacity lease remains
      // occupied until the real SDK operation settles, including APNs cleanup.
      result = await lease.send(() => provider.send(entries, request, appConfig, { singleAttempt: true, expires_at_ms: deadline }),
        Math.max(1, Math.min(PROVIDER_TIMEOUT_MS, Number.isSafeInteger(deadline) ? deadline - Date.now() : PROVIDER_TIMEOUT_MS)))
    } catch (error) {
      const cause = describeFailure(error, appConfig.type === 'ios' ? 'apns' : 'fcm')
      await finish(entries.map(r => ({ index: r.entry_index, state: 'unknown' })))
      for (const r of entries) if (callId) secondary(() => tracker.report(callId, 'provider_unknown', {
        ...attempt, user_id: r.id, user_id_namespace: namespace, entry_index: r.entry_index,
        ...tokenFingerprint(r.token), ...cause, outcome: 'unknown',
      }))
      secondary(() => logError(app_id, 'PROVIDER_UNKNOWN', JSON.stringify({ ...cause, ...attempt })))
      return { success: 0, unknown: entries.length, failures: [], invalidTokens: [] }
    }
    let retry = false
    const outcomes = entries.map(r => {
      const failure = result.failures.find(x => x.entry_index != null ? x.entry_index === r.entry_index : x.token === r.token && (x.id == null || String(x.id) === String(r.id)))
      const eligible = !!failure?.retryable && failure.category !== 'unknown' && Number.isSafeInteger(deadline) && Date.now() + RETRY_MARGIN_MS < deadline &&
        (job.attemptsMade || 0) + 1 < (job.opts?.attempts || 3)
      retry ||= eligible
      return { index: r.entry_index, state: failure ? failure.category === 'unknown' ? 'unknown' : eligible ? 'retryable' : 'permanent' : 'accepted', failure }
    })
    await finish(outcomes)
    for (const [index, r] of entries.entries()) {
      const f = outcomes[index].failure
      const { token: omitted, ...diagnostic } = f || {}
      const safe = { ...diagnostic, ...tokenFingerprint(r.token), ...attempt, user_id: r.id, user_id_namespace: namespace, entry_index: r.entry_index }
      if (f) secondary(() => logError(app_id, 'SEND_FAILED', JSON.stringify(safe)))
      if (callId) secondary(() => tracker.report(callId, f?.category === 'unknown' ? 'provider_unknown' : appConfig.type === 'ios' ? f ? 'apns_fail' : 'apns_ok' : f ? 'fcm_fail' : 'fcm_ok', safe))
    }
    secondary(() => upsertStats(new Date().toISOString().slice(0, 13) + ':00', app_id, result.success, result.failures.length))
    if (result.invalidTokens.length) secondary(() => addInvalidTokens(result.invalidTokens))
    if (retry) throw new Error('Retry eligible transient recipients only')
    return result
  } finally { lease?.release() }
}
module.exports = { getAppConfig, processJob, checkpointHealth: () => ({ available: checkpointAvailable }) }
