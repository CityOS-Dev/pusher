// Call Tracker integration. Fire-and-forget POST per recipient after send.
// Loopback only (127.0.0.1:3001) — no token required, loopback is contained.

const TRACKER_URL = process.env.TRACKER_URL || 'http://127.0.0.1:3001/api/calls'
const TIMEOUT_MS = 800

async function report(callId, stage, meta) {
  if (!callId) return
  try {
    await fetch(`${TRACKER_URL}/${encodeURIComponent(callId)}/event`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        source: 'pusher',
        stage,
        ts: Date.now(),
        meta: meta || {},
      }),
      signal: AbortSignal.timeout(TIMEOUT_MS),
    })
  } catch (_) {
    // fire-and-forget; никогда не ронять основной push-flow
  }
}

module.exports = { report }
