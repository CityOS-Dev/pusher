// Load the real send/processor code; only external SDK, credentials, DB and
// logging boundaries are replaced. No Redis, certificates or provider traffic.
const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const vm = require('node:vm')
const crypto = require('node:crypto')
const root = path.resolve(__dirname, '..')
function load(file, deps) {
  const filename = path.join(root, file)
  const module = { exports: {} }
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), {
    module, exports: module.exports,
    require: key => Object.hasOwn(deps, key) ? deps[key] : require(path.resolve(path.dirname(filename), key)),
    Buffer, Date, Set, Map, setTimeout,
  }, { filename })
  return module.exports
}
const logger = records => () => Object.fromEntries(['info', 'warn', 'error'].map(k => [k, (...args) => records.push(args)]))
function apns(outcome, records = [], sendError = null) {
  let shutdownCount = 0
  const provider = load('src/providers/apn.js', {
    '@parse/node-apn': {
      Notification: class {},
      Provider: class {
        async send() { if (sendError) throw sendError; return outcome }
        async shutdown() { shutdownCount++ }
      },
    },
    fs: { existsSync: () => true, readFileSync: () => Buffer.from('fake') },
    path: { resolve: v => v }, pino: logger(records),
  })
  return { provider, shutdownCount: () => shutdownCount }
}
const config = { id: 'fake-ios', type: 'ios', cert: 'fake' }

test('APNs transport failure preserves ETIMEDOUT instead of collapsing to Unknown error', async () => {
  const error = Object.assign(new Error('private transport detail'), { code: 'ETIMEDOUT' })
  const { provider } = apns({ sent: [], failed: [{ device: 'fake-token', error }] })
  const result = await provider.send([{ id: '4251', token: 'fake-token' }], { payload: {} }, config)
  assert.equal(result.failures[0].error_code, 'ETIMEDOUT')
  assert.equal(result.failures[0].error_type, 'transport')
  assert.equal(result.failures[0].retryable, true)
  assert.equal(result.failures[0].category, 'transient')
})

test('APNs rejects are recorded even when the optional recipient id is absent', async () => {
  const { provider } = apns({ sent: [], failed: [{ device: 'fake-token', status: 410, response: { reason: 'Unregistered' } }] })
  const result = await provider.send([{ token: 'fake-token' }], { payload: {} }, config)
  assert.equal(result.failures.length, 1)
  assert.equal(result.failures[0].error_code, 'Unregistered')
  assert.equal(result.failures[0].http_status, 410)
  assert.equal(result.failures[0].retryable, false)
  assert.equal(result.invalidTokens[0].token, 'fake-token', 'operational pruning still has its token')
})

test('APNs releases its provider when send throws and retains the original failure', async () => {
  const error = Object.assign(new Error('private detail'), { code: 'ECONNRESET' })
  const harness = apns(null, [], error)
  await assert.rejects(harness.provider.send([{ id: '4251', token: 'fake' }], { payload: {} }, config), e => e === error)
  assert.equal(harness.shutdownCount(), 1)
})

function processor(provider, records, logs, events) {
  return load('src/queue/processor.js', {
    '../providers/apn': provider, '../providers/fcm': provider,
    '../providers/call_tracker': { report: async (...args) => events.push(args) },
    '../db/stats': { upsertStats() {} },
    '../db/delivery': { getStore: () => ({ claimMany: (id, indexes) => indexes, finishMany() {} }) },
    bullmq: { DelayedError: class DelayedError extends Error {} },
    '../db/errors': { logError: (...args) => logs.push(args) },
    '../db/tokens': { addInvalidTokens() {} },
    '../../config/apps.config': { apps: [config] }, pino: logger(records),
  })
}
const job = recipients => ({ id: 'job-4251', attemptsMade: 0, data: {
  app_id: 'fake-ios', recipients, request: { payload: { call_id: 'test-exact' } },
} })

test('the processor records call/job/attempt and machine cause without full tokens or error messages', async () => {
  const marker = 'PRIVATE_PUSHER_MARKER'
  const records = [], logs = [], events = []
  const { provider } = apns({ sent: [], failed: [{ device: marker, error: Object.assign(new Error(`token=${marker}`), { code: 'ETIMEDOUT' }) }] }, records)
  const result = await processor(provider, records, logs, events).processJob(job([{ id: '4251', token: marker }]))
  assert.equal(result.failures.length, 1)
  const saved = JSON.parse(logs[0][2])
  assert.equal(saved.call_id, 'test-exact')
  assert.equal(saved.job_id, 'job-4251')
  assert.equal(saved.attempt_index, 1)
  assert.equal(saved.error_code, 'ETIMEDOUT')
  assert.equal(saved.token_hash, crypto.createHash('sha256').update(marker).digest('hex'))
  assert.equal(events[0][0], 'test-exact')
  assert.equal(events[0][1], 'apns_fail')
  assert.equal(events[0][2].error_code, 'ETIMEDOUT')
  assert.equal(events[0][2].token_hash, saved.token_hash)
  assert.ok(!JSON.stringify({ records, logs, events }).includes(marker))
})

test('partial failure for one token does not mark the other token sharing an id as failed', async () => {
  const records = [], logs = [], events = []
  const { provider } = apns({ sent: [{ device: 'fake-success' }], failed: [{ device: 'fake-failed', status: 410, response: { reason: 'Unregistered' } }] }, records)
  await processor(provider, records, logs, events).processJob(job([{ id: 'same-user', token: 'fake-success' }, { id: 'same-user', token: 'fake-failed' }]))
  assert.deepEqual(events.map(e => [e[1], e[2].token_tail]), [['apns_ok', 'cess'], ['apns_fail', 'iled']])
  assert.equal(events[0][2].token_hash, crypto.createHash('sha256').update('fake-success').digest('hex'))
})

test('a thrown provider failure includes the exact token fingerprint in its call trace', async () => {
  const events = []
  const error = Object.assign(new Error('private detail'), { code: 'ECONNRESET' })
  const provider = { send: async () => { throw error } }
  await processor(provider, [], [], events).processJob(job([{ id: '4251', token: 'fake-throw-token' }]))
  assert.equal(events[0][2].token_hash, crypto.createHash('sha256').update('fake-throw-token').digest('hex'))
})

test('a completed recipient failure is classified but does not enable a new unbounded retry loop', async () => {
  let sends = 0
  const result = { success: 0, failures: [{ id: '4251', token: 'fake', error: 'ServiceUnavailable', error_code: 'ServiceUnavailable', error_type: 'apns', retryable: true, category: 'transient', http_status: 503 }], invalidTokens: [] }
  const provider = { send: async () => { sends++; return result } }
  const returned = await processor(provider, [], [], []).processJob(job([{ id: '4251', token: 'fake' }]))
  assert.equal(returned, result)
  assert.equal(sends, 1, 'retry enablement requires the separate deadline contract')
})

test('FCM recipient failures retain SDK codes and suppress private messages from diagnostics', async () => {
  const records = [], marker = 'PRIVATE_FCM_MARKER'
  const provider = load('src/providers/fcm.js', {
    'firebase-admin': {
      credential: { cert: () => ({}) },
      initializeApp: () => ({ messaging: () => ({ sendEachForMulticast: async () => ({ successCount: 0, responses: [{ success: false, error: { code: 'messaging/server-unavailable', message: marker } }] }) }) }),
    },
    fs: { existsSync: () => true, readFileSync: () => '{}' },
    path: { resolve: v => v }, pino: logger(records),
  })
  const result = await provider.send([{ id: '4793', token: 'fake-token' }], { payload: {} }, { id: 'fake-android', serviceAccount: 'fake' })
  assert.equal(result.failures[0].error_code, 'messaging/server-unavailable')
  assert.equal(result.failures[0].retryable, true)
  assert.ok(!JSON.stringify(result.failures).includes(marker))
})
test('APNs missing token outcome is unknown rather than accepted',async()=>{
 const {provider}=apns({sent:[],failed:[]})
 const result=await provider.send([{id:'x',token:'missing'}],{payload:{}},config)
 assert.equal(result.failures[0].category,'unknown')
})
test('FCM missing recipient result is unknown rather than accepted', async()=>{
 const provider=load('src/providers/fcm.js',{
 'firebase-admin':{credential:{cert:()=>({})},initializeApp:()=>({messaging:()=>({sendEachForMulticast:async()=>({successCount:0,responses:[]})})})},
 fs:{existsSync:()=>true,readFileSync:()=>'{}'},path:{resolve:v=>v},pino:logger([])})
 const result=await provider.send([{id:'x',token:'missing',entry_index:2}],{payload:{}},{id:'fake',serviceAccount:'fake'})
 assert.equal(result.failures[0].category,'unknown');assert.equal(result.failures[0].entry_index,2)
})

test('untrusted machine-code fields cannot smuggle credentials into diagnostic logs', () => {
  const { describeFailure } = require('../src/providers/provider_errors')
  const marker = 'PRIVATE_CREDENTIAL_IN_CODE'
  for (const error of [{ code: marker, message: marker }, { response: { reason: marker } }]) {
    const safe = describeFailure(error)
    assert.equal(safe.error_code, 'UNKNOWN_ERROR')
    assert.ok(!JSON.stringify(safe).includes(marker))
  }
  assert.equal(describeFailure({ code: 'SQLITE_FULL' }).error_code, 'SQLITE_FULL')
})

test('invalid-token batch database failure logs a fingerprint instead of the operational token', () => {
  const marker = 'PRIVATE_TOKEN_SQLITE_FAILURE', records = []
  const db = {
    prepare: () => ({ run: () => { throw Object.assign(Error(marker), { code: 'SQLITE_FULL' }) } }),
    transaction: fn => fn,
  }
  const tokens = load('src/db/tokens.js', { './index': db, pino: logger(records) })
  tokens.addInvalidTokens([{ external_id: '4251', token: marker, app_id: 'fixture', type: 'ios', reason: 'Unregistered' }])
  assert.ok(records.length)
  assert.ok(!JSON.stringify(records).includes(marker))
  assert.equal(records[0][0].token_hash, crypto.createHash('sha256').update(marker).digest('hex'))
})
