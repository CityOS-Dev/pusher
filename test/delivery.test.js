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
