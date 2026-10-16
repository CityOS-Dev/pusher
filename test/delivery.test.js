const { test } = require('node:test')
const assert = require('node:assert/strict')
const { createStore, fingerprint, jobId } = require('../src/db/delivery')
const Database = require('better-sqlite3')

test('idempotency survives queue retry and rejects different content', () => {
  const store = createStore(new Database(':memory:'))
  assert.equal(store.reserve('k', 'a'), true)
  assert.equal(store.reserve('k', 'a'), false)
  assert.throws(() => store.reserve('k', 'b'), e => e.statusCode === 409)
  assert.equal(fingerprint({ b: 2, a: 1 }), fingerprint({ a: 1, b: 2 }))
  assert.match(jobId('k', 0), /^[a-f0-9]+$/)
})
test('accepted/permanent/unknown entries are never claimed again; transient only before deadline', () => {
  const store = createStore(new Database(':memory:'))
  for (const [index, state] of ['accepted', 'permanent', 'unknown', 'retryable'].entries()) {
    assert.equal(store.claim('j', index, 100), true)
    store.finish('j', index, state)
    assert.equal(store.claim('j', index, 200), state === 'retryable')
  }
  assert.equal(store.claim('j', 9, 100), true)
  assert.equal(store.claim('j', 9, 200), false, 'in-flight recovery is ambiguous, not eligible for resend')
})
test('diagnostic outbox bounded and retry preserves event identity', () => {
  const store = createStore(new Database(':memory:'), { outboxLimit: 2 })
  store.enqueueEvent({ event_id: 'one' }, 100)
  store.enqueueEvent({ event_id: 'two' }, 100)
  assert.equal(store.enqueueEvent({ event_id: 'three' }, 100), false)
  assert.equal(store.events(101).length, 2)
  store.retryEvent(store.events(101)[0].id, 200)
  assert.equal(store.events(101).length, 1)
  assert.equal(store.events(201)[0].event.event_id, 'one')
})

test('500 recipient checkpoints use two atomic commits and preserve ambiguous recovery', () => {
  const statements = []
  const db = new Database(':memory:', { verbose: sql => statements.push(sql) })
  const store = createStore(db)
  const indexes = Array.from({ length: 500 }, (_, i) => i)
  statements.length = 0
  assert.deepEqual(store.claimMany('batch', indexes), indexes)
  store.finishMany('batch', indexes.map(index => ({ index, state: 'accepted' })))
  assert.equal(statements.filter(sql => sql === 'COMMIT').length, 2)
  assert.deepEqual(store.claimMany('batch', indexes), [])
  assert.deepEqual(store.claimMany('interrupted', [0, 1]), [0, 1])
  assert.deepEqual(store.claimMany('interrupted', [0, 1]), [])
  assert.equal(store.health().counters.find(row => row.key === 'ambiguous_recovery').count, 2)
  db.close()
})

test('a failed batch finish rolls back every recipient; subsequent claim cannot resend', () => {
  const db = new Database(':memory:'), store = createStore(db)
  store.claimMany('batch', [0, 1])
  db.exec(`CREATE TRIGGER disk_failure BEFORE UPDATE ON delivery_entries
    WHEN NEW.entry_index=1 AND NEW.state='accepted' BEGIN SELECT RAISE(ABORT, 'fixture'); END`)
  assert.throws(() => store.finishMany('batch', [{ index: 0, state: 'accepted' }, { index: 1, state: 'accepted' }]), /fixture/)
  assert.equal(db.prepare("SELECT COUNT(*) AS n FROM delivery_entries WHERE state='in_flight'").get().n, 2)
  assert.deepEqual(store.claimMany('batch', [0, 1]), [])
  db.close()
})
