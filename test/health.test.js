const { test } = require('node:test'), assert = require('node:assert/strict')
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm')
const Fastify = require('fastify')

async function server(health, getQueueStats) {
  const filename = path.resolve('src/server.js'), module = { exports: {} }
  const noopPlugin = async () => {}
  const deps = {
    fastify: Fastify, './queue': { queues: {}, getQueueStats },
    './api/routes': noopPlugin, './dashboard/routes': noopPlugin,
    '@bull-board/api': { createBullBoard() {} },
    '@bull-board/fastify': { FastifyAdapter: class { setBasePath() {} registerPlugin() { return noopPlugin } } },
    '@bull-board/api/bullMQAdapter': {}, '../config': { log: { level: 'info' } },
    './providers/call_tracker': {}, './db/delivery': { getStore: () => ({ health }) },
    './queue/processor': { checkpointHealth: () => ({ available: true }) },
    pino: () => ({ info() {}, warn() {} }),
  }
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), { module, exports: module.exports, process: { on() {} }, Date,
    require: key => deps[key] || require(path.resolve(path.dirname(filename), key)),
  }, { filename })
  return module.exports.buildServer()
}

test('storage health failure returns explicit degraded503 without database exception text', async () => {
  const app = await server(() => { throw Error('PRIVATE_STORAGE_PATH') }, async () => ({ worker: { ready: true } }))
  try {
    const response = await app.inject('/health')
    assert.equal(response.statusCode, 503)
    assert.equal(response.json().status, 'degraded')
    assert.equal(response.json().delivery.storage_available, false)
    assert.ok(!response.body.includes('PRIVATE_STORAGE_PATH'))
  } finally { await app.close() }
})

test('healthy endpoint reads both independently bounded queue snapshots concurrently', async () => {
  let active = 0, max = 0
  const app = await server(() => ({ diagnostic_pending: 0 }), async () => {
    max = Math.max(max, ++active)
    await new Promise(resolve => setImmediate(resolve)); active--
    return { worker: { ready: true } }
  })
  try {
    const response = await app.inject('/health')
    assert.equal(response.statusCode, 200)
    assert.equal(response.json().status, 'ok')
    assert.equal(max, 2)
  } finally { await app.close() }
})
