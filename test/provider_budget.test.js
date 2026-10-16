const { test } = require('node:test')
const assert = require('node:assert/strict')
const { createBudget } = require('../src/queue/provider_deadline')

test('timed-out SDK work retains a bounded permit until its actual completion', async () => {
  const budget = createBudget(2)
  const completions = []
  let started = 0
  const send = () => { started++; return new Promise(resolve => completions.push(resolve)) }
  const leases = [budget.acquire(), budget.acquire()]
  assert.equal(budget.acquire(), null)
  await Promise.all(leases.map(lease => assert.rejects(lease.send(send, 5), error => error.code === 'ETIMEDOUT')))
  leases.forEach(lease => lease.release())
  assert.equal(started, 2)
  assert.equal(budget.health().pending, 2)
  assert.equal(budget.acquire(), null, 'timeout does not pretend to cancel the SDK')
  completions[0]('late accepted')
  await new Promise(resolve => setImmediate(resolve))
  const available = budget.acquire()
  assert.ok(available)
  available.release()
  completions[1]('late accepted')
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(budget.health().pending, 0)
})

test('rejected SDK work and unused leases release exactly once', async () => {
  const budget = createBudget(1), first = budget.acquire()
  first.release(); first.release()
  const next = budget.acquire()
  await assert.rejects(next.send(async () => { throw Error('fixture') }, 20), /fixture/)
  next.release()
  assert.equal(budget.health().pending, 0)
})
