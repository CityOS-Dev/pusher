const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm')
const Database = require('better-sqlite3')
const { createStore } = require('../src/db/delivery')
function harness(send, stats = () => {}, sourceStatus='active', options = {}) {
  const db = new Database(':memory:'), store = createStore(db), events = []
  const deps = { './sip_source_guard': options.guard || {check:async()=>sourceStatus}, bullmq:{DelayedError:class DelayedError extends Error{}}, '../providers/apn': { send }, '../providers/fcm': { send },
    '../providers/call_tracker': { report: async (...x) => events.push(x) },
    '../db/stats': { upsertStats: stats }, '../db/errors': { logError() {} }, '../db/tokens': { addInvalidTokens() {} },
    '../db/delivery': { getStore: () => options.store ? options.store(store) : store }, '../../config/apps.config': { apps: [{ id: 'a', type: 'ios' }] },
    './provider_deadline': { ...require('../src/queue/provider_deadline'), ...(options.budget ? { providerBudget: options.budget } : {}) },
    pino: () => ({ info() {}, error() {}, warn() {} }) }
  const filename = path.resolve('src/queue/processor.js'), module = { exports: {} }
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), { module, exports: module.exports, Date,
    require: k => deps[k] || require(path.resolve(path.dirname(filename), k)) }, { filename })
  return { process: module.exports.processJob, health: module.exports.checkpointHealth, store, db, events }
}
const job = () => ({ id: 'j', opts: { attempts: 3 }, attemptsMade: 0, data: { app_id: 'a', recipients: [{ id:'x',token:'one' },{id:'x',token:'two'}], request: { payload: { call_id:'call',user_id_namespace:'sb' } }, delivery: { version:1,idempotency_key:'key',expires_at_ms: Date.now()+60000 } } })
test('mixed transient retry sends only failed entry and statistics failure cannot resend acceptance', async () => {
  const sent = []; let n = 0
  const h = harness(async entries => { sent.push(Array.from(entries,e=>e.token)); return ++n === 1
    ? {success:1,failures:[{token:'two',id:'x',entry_index:1,retryable:true}],invalidTokens:[]}
    : {success:1,failures:[],invalidTokens:[]} }, () => { throw Error('disk full') })
  const j = job()
  await assert.rejects(h.process(j), /retry/i)
  j.attemptsMade++
  await h.process(j)
  await h.process(j)
  assert.deepEqual(sent, [['one','two'],['two']])
})

test('unavailable SQLite defers without spending attempts, then recovers with no unguarded send', async () => {
  let unavailable = true, sends = 0
  const h = harness(async entries => { sends++; return { success: entries.length, failures: [], invalidTokens: [] } }, () => {}, 'active', {
    store: store => { if (unavailable) throw Object.assign(Error('private disk marker'), { code: 'SQLITE_FULL' }); return store },
  })
  const j = job(), moves = []
  j.moveToDelayed = async (at, token) => moves.push({ at, token })
  await assert.rejects(h.process(j, 'lock'), error => error.constructor.name === 'DelayedError')
  assert.equal(sends, 0); assert.equal(j.attemptsMade, 0); assert.equal(moves[0].token, 'lock')
  unavailable = false
  await h.process(j, 'lock'); await h.process(j, 'lock')
  assert.equal(sends, 1)
})

test('SQLite failure after provider submission leaves recovery ambiguous and never resends', async () => {
  let sends = 0
  const h = harness(async entries => {
    sends++; h.db.pragma('query_only = ON')
    return { success: entries.length, failures: [], invalidTokens: [] }
  })
  const j = job(); j.moveToDelayed = async () => {}
  await assert.rejects(h.process(j, 'lock'), error => error.constructor.name === 'DelayedError')
  h.db.pragma('query_only = OFF')
  await h.process(j, 'lock')
  assert.equal(sends, 1)
  assert.equal(h.store.health().entries.find(row => row.state === 'unknown').count, 2)
})

test('saturated provider budget delays before claiming or submitting any recipient', async () => {
  const budget = require('../src/queue/provider_deadline').createBudget(1), held = budget.acquire()
  let sends = 0
  const h = harness(async entries => { sends++; return { success: entries.length, failures: [], invalidTokens: [] } }, () => {}, 'active', { budget })
  const j = job(); j.moveToDelayed = async () => {}
  await assert.rejects(h.process(j, 'lock'), error => error.constructor.name === 'DelayedError')
  assert.equal(h.store.health().entries.length, 0); assert.equal(sends, 0)
  held.release()
  await h.process(j, 'lock')
  assert.equal(sends, 1)
})

test('cached store reads and source-only deferral cannot report checkpoint-write recovery', async () => {
  const h = harness(async entries => ({ success: entries.length, failures: [], invalidTokens: [] }), () => {}, 'unknown')
  const j = job(); j.moveToDelayed = async () => {}
  const completed = job(); completed.id = 'completed'
  await h.process(completed, 'lock')
  h.db.pragma('query_only = ON')
  await assert.rejects(h.process(j, 'lock'), error => error.constructor.name === 'DelayedError')
  assert.equal(h.health().available, false)
  const other = job(); other.moveToDelayed = async () => {}
  other.data.delivery.source = { kind: 'sip', issuer: 'asterisk:fixture', session_id: '123.4' }
  await assert.rejects(h.process(other, 'lock'), error => error.constructor.name === 'DelayedError')
  assert.equal(h.health().available, false)
  await h.process(completed, 'lock')
  assert.equal(h.health().available, false, 'reading terminal tombstones does not prove writable storage')
  h.db.pragma('query_only = OFF')
  await h.process(j, 'lock')
  assert.equal(h.health().available, true)
})

test('rejected AMQP status transport delays the real processor before any claim or provider call', async () => {
  let sends = 0
  const guard = require('../src/queue/sip_source_guard').createGuard({
    loadConfig: () => ({}), connect: async () => { throw Object.assign(Error('private broker'), { code: 'ECONNREFUSED' }) },
  })
  const h = harness(async () => { sends++ }, () => {}, 'active', { guard })
  const j = job(); j.moveToDelayed = async () => {}
  j.data.delivery.source = { kind: 'sip', issuer: 'asterisk:fixture', session_id: '123.4' }
  await assert.rejects(h.process(j, 'lock'), error => error.constructor.name === 'DelayedError')
  assert.equal(j.attemptsMade, 0); assert.equal(sends, 0); assert.equal(h.store.health().entries.length, 0)
})

test('two concurrent inline workers processing the same legacy job submit at most once', async () => {
  let sends = 0
  const h = harness(async entries => {
    sends++; await new Promise(resolve => setImmediate(resolve))
    return { success: entries.length, failures: [], invalidTokens: [] }
  })
  const j = job(); delete j.data.delivery
  await Promise.all([h.process(j), h.process(j)])
  assert.equal(sends, 1)
  assert.equal(h.store.health().entries.find(row => row.state === 'accepted').count, 2)
})
test('ambiguous provider throw never sends same entries again', async () => {
  let sends=0
  const h=harness(async()=>{sends++;throw Object.assign(Error('lost response'),{code:'ETIMEDOUT'})})
  const j=job()
  await h.process(j)
  await h.process(j)
  assert.equal(sends,1)
})
test('expired and missing authoritative deadline never cause partial retry', async () => {
  let sends=0
  const h=harness(async()=>{sends++;return {success:0,failures:[{token:'one',retryable:true},{token:'two',retryable:true}],invalidTokens:[]}})
  const j=job();j.data.delivery.expires_at_ms=Date.now()-1
  await h.process(j)
  assert.equal(sends,0)
  j.id='legacy';delete j.data.delivery
  await h.process(j);await h.process(j)
  assert.equal(sends,1)
})
test('hung provider resolves as unknown within operation timeout without resending', async()=>{
 const filename=path.resolve('src/queue/provider_deadline.js')
 const {sendWithin}=require(filename)
 await assert.rejects(sendWithin(()=>new Promise(()=>{}),5),e=>e.code==='ETIMEDOUT')
})
test('two ambiguous recipients retain distinct stable entry identities in diagnostics',async()=>{
 const h=harness(async()=>{throw Object.assign(Error('timeout'),{code:'ETIMEDOUT'})}),j=job()
 await h.process(j)
 assert.deepEqual(h.events.map(e=>e[2].entry_index),[0,1])
 assert.equal(h.events[0][2].user_id_namespace,'sb')
})
test('caller cannot collide stable checkpoint entry indexes',async()=>{
 let count=0
 const h=harness(async entries=>{count=entries.length;return {success:entries.length,failures:[],invalidTokens:[]}}),j=job()
 j.data.recipients.forEach(r=>{r.entry_index=0})
 await h.process(j)
 assert.equal(count,2)
})
test('missing namespace is unknown; modern tenant namespace is preserved',async()=>{
 const h=harness(async entries=>({success:entries.length,failures:[],invalidTokens:[]})),j=job()
 delete j.data.request.payload.user_id_namespace
 await h.process(j)
 assert.equal(h.events[0][2].user_id_namespace,null)
 const other=job();other.id='modern';other.data.request.payload.user_id_namespace='tenant'
 await h.process(other)
 assert.equal(h.events[2][2].user_id_namespace,'tenant')
})
test('ended source never submits; unknown moves to delayed before claiming without using provider budget',async()=>{
 for(const status of ['ended','unknown']){
  let sends=0;const h=harness(async()=>{sends++;return {success:1,failures:[],invalidTokens:[]}},()=>{},status),j=job()
  j.data.delivery.source={kind:'sip',issuer:'asterisk:fixture',session_id:'123.4'}
  const moved=[];j.moveToDelayed=async(at,token)=>{moved.push({at,token})}
  if(status==='unknown')await assert.rejects(h.process(j,'lock-token'),e=>e.constructor.name==='DelayedError')
  else await h.process(j,'lock-token')
  assert.equal(sends,0);assert.equal(j.attemptsMade,0)
  if(status==='unknown'){assert.equal(moved[0].token,'lock-token');assert.equal(h.store.health().entries.length,0)}
 }
})
