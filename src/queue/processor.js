const apnProvider = require('../providers/apn')
const fcmProvider = require('../providers/fcm')
const tracker = require('../providers/call_tracker')
const { upsertStats } = require('../db/stats')
const { logError } = require('../db/errors')
const { addInvalidTokens } = require('../db/tokens')
const { getStore } = require('../db/delivery')
const { sendWithin } = require('./provider_deadline')
const sipSourceGuard = require('./sip_source_guard')
const apps = require('../../config/apps.config')
const { describeFailure, tokenFingerprint } = require('../providers/provider_errors')
const logger = require('pino')({ level: 'info' })
function getAppConfig(appId) { return apps.apps.find(app => app.id === appId) }
function secondary(fn) {
  try { fn() } catch (error) { logger.warn(describeFailure(error), 'Secondary recording failed; delivery will not be repeated') }
}
async function processJob(job, token) {
  const { app_id, recipients, request, delivery } = job.data
  const p = request && request.payload
  const callId = p?.call_id || p?.callId || p?.data?.call_id || p?.data?.callId || null
  const rawNamespace = p?.data?.user_id_namespace || p?.user_id_namespace
  const namespace = ['sb','tenant'].includes(rawNamespace) ? rawNamespace:null
  const attempt = { call_id: callId, job_id: job.id, attempt_index: (job.attemptsMade || 0) + 1 }
  const appConfig = getAppConfig(app_id)
  if (!appConfig || !['ios','android'].includes(appConfig.type)) throw new Error('Invalid app configuration')
  const store = getStore()
  const deadline = delivery?.expires_at_ms
  let sourceEnded = false
  if (delivery?.source && !(Number.isSafeInteger(deadline) && Date.now() >= deadline)) {
    const status = await sipSourceGuard.check(delivery.source)
    if (status === 'unknown') {
      await job.moveToDelayed(Date.now()+2000,token)
      const {DelayedError}=require('bullmq')
      throw new DelayedError()
    }
    sourceEnded = status === 'ended'
  }
  const entries = []
  for (const [index, rcpt] of recipients.entries()) {
    const entry_index = index
    if (!store.claim(job.id, entry_index)) continue
    if (sourceEnded) {
      store.finish(job.id,entry_index,'skipped')
      if(callId) secondary(()=>tracker.report(callId,'push_skipped',{...attempt,user_id:rcpt.id,user_id_namespace:namespace,entry_index,...tokenFingerprint(rcpt.token),reason_code:'call_ended'}))
    } else if (Number.isSafeInteger(deadline) && Date.now() >= deadline) {
      store.finish(job.id, entry_index, 'expired')
      if (callId) secondary(() => tracker.report(callId, 'push_expired', { ...attempt, user_id: rcpt.id, user_id_namespace:namespace, entry_index, ...tokenFingerprint(rcpt.token), reason_code:'expired' }))
    } else entries.push({ ...rcpt, entry_index })
  }
  if (!entries.length) return { success:0, failures:[], invalidTokens:[] }
  const provider = appConfig.type === 'ios' ? apnProvider : fcmProvider
  if (Number.isSafeInteger(deadline) && Date.now() >= deadline) {
    for (const r of entries) store.finish(job.id,r.entry_index,'expired')
    return {success:0,failures:[],invalidTokens:[]}
  }
  let result
  try {
    // Delivery retry belongs to the checkpointed queue; SDK wrapper must not resend an ambiguous request.
    result = await sendWithin(() => provider.send(entries, request, appConfig, { singleAttempt: true, expires_at_ms: deadline }), Math.max(1,Math.min(15000,Number.isSafeInteger(deadline)?deadline-Date.now():15000)))
  } catch (error) {
    const cause = describeFailure(error, appConfig.type === 'ios' ? 'apns' : 'fcm')
    for (const r of entries) {
      store.finish(job.id, r.entry_index, 'unknown')
      if (callId) secondary(() => tracker.report(callId, 'provider_unknown', { ...attempt, user_id:r.id, user_id_namespace:namespace, entry_index:r.entry_index, ...tokenFingerprint(r.token), ...cause, outcome:'unknown' }))
    }
    secondary(() => logError(app_id, 'PROVIDER_UNKNOWN', JSON.stringify({ ...cause, ...attempt })))
    return { success:0, unknown:entries.length, failures:[], invalidTokens:[] }
  }
  let retry = false
  for (const r of entries) {
    const f = result.failures.find(x => x.entry_index != null ? x.entry_index === r.entry_index : x.token === r.token && (x.id == null || String(x.id) === String(r.id)))
    const eligible = !!f?.retryable && Number.isSafeInteger(deadline) && Date.now() + 5000 < deadline &&
      (job.attemptsMade || 0) + 1 < (job.opts?.attempts || 3)
    store.finish(job.id, r.entry_index, f ? f.category === 'unknown' ? 'unknown' : eligible ? 'retryable' : 'permanent' : 'accepted')
    retry ||= eligible
    const { token, ...diagnostic } = f || {}
    const safe = { ...diagnostic, ...tokenFingerprint(r.token), ...attempt, user_id:r.id, user_id_namespace:namespace, entry_index:r.entry_index }
    if (f) secondary(() => logError(app_id, 'SEND_FAILED', JSON.stringify(safe)))
    if (callId) secondary(() => tracker.report(callId, f?.category === 'unknown' ? 'provider_unknown' : appConfig.type === 'ios' ? f ? 'apns_fail':'apns_ok' : f ? 'fcm_fail':'fcm_ok', safe))
  }
  secondary(() => upsertStats(new Date().toISOString().slice(0,13)+':00', app_id, result.success, result.failures.length))
  if (result.invalidTokens.length) secondary(() => addInvalidTokens(result.invalidTokens))
  if (retry) throw new Error('Retry eligible transient recipients only')
  return result
}
module.exports = { getAppConfig, processJob }
