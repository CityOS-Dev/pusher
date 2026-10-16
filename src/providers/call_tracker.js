// Bounded durable diagnostic emission. Networking runs outside Bull worker slots.
const { getStore, fingerprint } = require('../db/delivery')
const TRACKER_URL = process.env.TRACKER_URL || 'http://127.0.0.1:3001/api/calls'
const KEYS = new Set('user_id user_id_namespace token_hash token_tail error_code error_type http_status retryable category error_hash call_id job_id attempt_index entry_index outcome reason_code'.split(' '))
function report(callId, stage, meta = {}) {
  if (!callId) return
  const safe = Object.fromEntries(Object.entries(meta).filter(([key, value]) => KEYS.has(key) && (value == null || typeof value === 'boolean' || typeof value === 'number' && Number.isFinite(value) || typeof value === 'string' && value.length <= 128 && /^[A-Za-z0-9_.:+() /-]*$/.test(value) && !value.includes('://'))))
  const event_id = fingerprint([callId, stage, safe.job_id, safe.attempt_index, safe.entry_index])
  getStore().enqueueEvent({ callId, source:'pusher', stage, ts:Date.now(), meta:{ ...safe, diagnostics_version:2, ts_unit:'ms', diagnostic_principal:'server:pusher', device_instance_id:'pusher-local', event_id } })
}
let draining = false, timer
async function drain(fetchImpl = fetch) {
  if (draining) return
  draining = true
  try {
    const store = getStore()
    store.expireEvents()
    await Promise.all(store.events().map(async row => {
      try {
        const { callId, ...body } = row.event
        const res = await fetchImpl(`${TRACKER_URL}/${encodeURIComponent(callId)}/event`, {
          method:'POST',headers:{'Content-Type':'application/json','X-Tracker-Ack':'2'},body:JSON.stringify(body),signal:AbortSignal.timeout(800),
        })
        const result = res.ok ? await res.json() : null
        const receipt = result?.receipt
        if (result?.ok === true && receipt?.diagnostics_version === 2 && receipt.persisted === true && Number.isSafeInteger(receipt.received_at_ms) && typeof receipt.duplicate === 'boolean' && receipt.device_instance_id === body.meta.device_instance_id && receipt.event_id === body.meta.event_id && receipt.call_id === callId && receipt.diagnostic_principal === body.meta.diagnostic_principal) {
          store.deleteEvent(row.id)
        } else store.retryEvent(row.id, Date.now()+Math.min(60000, 1000 * 2**Math.min(row.attempts,6)))
      } catch { store.retryEvent(row.id, Date.now()+Math.min(60000, 1000 * 2**Math.min(row.attempts,6))) }
    }))
  } finally { draining = false }
}
function start() { if (!timer) { timer=setInterval(()=>{drain().catch(()=>{})},250);timer.unref() } }
function stop() { clearInterval(timer);timer=null }
module.exports = { report, drain, start, stop }
