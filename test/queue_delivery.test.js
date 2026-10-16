const { test } = require('node:test'), assert = require('node:assert/strict')
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm')
const delivery=require('../src/db/delivery')
function setup() {
 const saved=new Map(), queued=[],workerOptions=[]
 class Queue { constructor(name,options){this.name=name;this.options=options} async addBulk(jobs){queued.push(...jobs);return jobs.map(j=>({id:j.opts.jobId}))} }
 class Worker { constructor(n,p,o){workerOptions.push(o)} on(){} }
 const deps={bullmq:{Queue,Worker},'./processor':{},'../../config/queue.config':require('../config/queue.config'),pino:()=>({info(){},error(){}}),'../db/delivery':{...delivery,getStore:()=>({reserve(k,f){if(saved.has(k)&&saved.get(k)!==f)throw Object.assign(Error('conflict'),{statusCode:409});saved.set(k,f)}})}}
 const module={exports:{}},filename=path.resolve('src/queue/index.js')
 vm.runInNewContext(fs.readFileSync(filename,'utf8'),{module,exports:module.exports,require:k=>deps[k]||(k.startsWith('node:')?require(k):require(path.resolve(path.dirname(filename),k))),Date,Promise},{filename})
 return {...module.exports,queued,workerOptions}
}
test('atomic enqueue has stable ids on retry and key conflict rejects changed payload', async()=>{
 const q=setup();q.createQueues()
 const data={app_id:'a',recipients:[{token:'fake',type:'ios'}],request:{payload:{}},delivery:{version:1,idempotency_key:'key'}}
 const a=await q.addJob('high',data),b=await q.addJob('high',data)
 assert.equal(a[0],b[0]);assert.match(a[0],/^[a-f0-9]+$/)
 await assert.rejects(q.addJob('high',{...data,request:{payload:{different:true}}}),e=>e.statusCode===409)
})
test('worker uses declared jobs limiter; retention units are seconds',async()=>{
 const q=setup();q.createQueues();await q.startWorker()
 assert.equal(q.workerOptions[0].limiter.max,1000)
 assert.equal(q.queues.high.options.defaultJobOptions.removeOnComplete.age,3600)
 assert.equal(q.queues.high.options.defaultJobOptions.removeOnFail.age,86400)
})
