// Durable delivery checkpoints contain fingerprints/state, never push credentials.
const crypto = require('node:crypto')
const canonical = value => Array.isArray(value) ? value.map(canonical) : value && typeof value === 'object'
  ? Object.fromEntries(Object.keys(value).sort().map(k => [k, canonical(value[k])])) : value
const fingerprint = value => crypto.createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex')
const jobId = (key, index) => fingerprint([key, index])
function createStore(db, { outboxLimit = 10000 } = {}) {
  db.pragma('synchronous = FULL')
  db.exec(`CREATE TABLE IF NOT EXISTS delivery_requests (key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, created_at INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS delivery_entries (job_id TEXT NOT NULL, entry_index INTEGER NOT NULL, state TEXT NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY(job_id,entry_index));
    CREATE TABLE IF NOT EXISTS diagnostic_outbox (id INTEGER PRIMARY KEY, event TEXT NOT NULL, next_at INTEGER NOT NULL, created_at INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE IF NOT EXISTS delivery_counters (key TEXT PRIMARY KEY, count INTEGER NOT NULL DEFAULT 0);`)
  const bump = key => db.prepare('INSERT INTO delivery_counters VALUES (?,1) ON CONFLICT(key) DO UPDATE SET count=count+1').run(key)
  const readEntry = db.prepare('SELECT state FROM delivery_entries WHERE job_id=? AND entry_index=?')
  const writeEntry = db.prepare(`INSERT INTO delivery_entries VALUES (?,?,?,?)
    ON CONFLICT(job_id,entry_index) DO UPDATE SET state=excluded.state,updated_at=excluded.updated_at`)
  const finishEntry = db.prepare('UPDATE delivery_entries SET state=?,updated_at=? WHERE job_id=? AND entry_index=?')
  // One durable transaction before submission and one after its result; never
  // keep a transaction open over provider I/O or weaken FULL durability.
  const claimMany = db.transaction((id, indexes, now = Date.now()) => {
    const claimed = []
    for (const index of indexes) {
      const row = readEntry.get(String(id), index)
      if (row && row.state !== 'retryable') {
        if (row.state === 'in_flight') {
          finishEntry.run('unknown', now, String(id), index)
          bump('ambiguous_recovery')
        }
        continue
      }
      writeEntry.run(String(id), index, 'in_flight', now)
      claimed.push(index)
    }
    return claimed
  })
  const finishMany = db.transaction((id, outcomes, now = Date.now()) => {
    for (const { index, state } of outcomes) finishEntry.run(state, now, String(id), index)
  })
  return {
    reserve: db.transaction((key, value) => {
      const row = db.prepare('SELECT fingerprint FROM delivery_requests WHERE key=?').get(key)
      if (row && row.fingerprint !== value) throw Object.assign(new Error('Idempotency key conflicts with existing request'), { statusCode: 409 })
      if (row) return false
      db.prepare('INSERT INTO delivery_requests VALUES (?,?,?)').run(key, value, Date.now())
      return true
    }),
    claimMany,
    finishMany,
    claim(id, index, now = Date.now()) { return claimMany(id, [index], now).length === 1 },
    finish(id, index, state) { finishMany(id, [{ index, state }]) },
    enqueueEvent(event, now = Date.now()) {
      return db.transaction(() => {
        if (db.prepare('SELECT COUNT(*) AS n FROM diagnostic_outbox').get().n >= outboxLimit) { bump('diagnostic_overflow'); return false }
        db.prepare('INSERT INTO diagnostic_outbox(event,next_at,created_at) VALUES (?,?,?)').run(JSON.stringify(event), now, now)
        return true
      })()
    },
    events(now = Date.now()) { return db.prepare('SELECT * FROM diagnostic_outbox WHERE next_at<=? ORDER BY id LIMIT 2').all(now).map(r => ({ ...r, event: JSON.parse(r.event) })) },
    deleteEvent(id) { db.prepare('DELETE FROM diagnostic_outbox WHERE id=?').run(id) },
    retryEvent(id, at) { db.prepare('UPDATE diagnostic_outbox SET next_at=?,attempts=attempts+1 WHERE id=?').run(at, id) },
    expireEvents(now = Date.now()) {
      const n = db.prepare('DELETE FROM diagnostic_outbox WHERE created_at<?').run(now - 7 * 86400000).changes
      if (n) db.prepare('INSERT INTO delivery_counters VALUES (?,?) ON CONFLICT(key) DO UPDATE SET count=count+excluded.count').run('diagnostic_expired', n)
    },
    health() { return { diagnostic_pending: db.prepare('SELECT COUNT(*) AS n FROM diagnostic_outbox').get().n,
      entries: db.prepare('SELECT state,COUNT(*) AS count FROM delivery_entries GROUP BY state').all(),
      counters: db.prepare('SELECT * FROM delivery_counters').all() } },
  }
}
let instance
const getStore = () => instance || (instance = createStore(require('./index')))
module.exports = { createStore, getStore, fingerprint, jobId }
