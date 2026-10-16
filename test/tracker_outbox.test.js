const {test}=require('node:test'),assert=require('node:assert/strict')
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm')
const Database=require('better-sqlite3'),delivery=require('../src/db/delivery')
test('500 recipient diagnostics persist immediately; network drain bounded at two; lost ACK retries same event',async()=>{
 const store=delivery.createStore(new Database(':memory:'))
 const filename=path.resolve(__dirname, '../src/providers/call_tracker.js'),module={exports:{}}
 vm.runInNewContext(fs.readFileSync(filename,'utf8'),{module,exports:module.exports,process:{env:{}},Date,Promise,Set,AbortSignal,setInterval,clearInterval,require:k=>k==='../db/delivery'?{...delivery,getStore:()=>store}:require(k)},{filename})
 const tracker=module.exports
 for(let i=0;i<500;i++) tracker.report('call','apns_ok',{job_id:'j',attempt_index:1,entry_index:i,user_id:i+1})
 assert.equal(store.health().diagnostic_pending,500)
 let active=0,max=0;const release=[]
 const pending=tracker.drain(async()=>{max=Math.max(max,++active);await new Promise(r=>release.push(r));active--;return {ok:false}})
 assert.equal(active,2);release.forEach(r=>r());await pending
 assert.equal(max,2);assert.equal(store.health().diagnostic_pending,500)
 const before=store.events(Date.now()+100000).map(r=>r.event.meta.event_id)
 assert.equal(new Set(before).size,2)
})
test('diagnostic deletion requires version and exact installation receipt',async()=>{
 const store=delivery.createStore(new Database(':memory:'))
 const filename=path.resolve(__dirname, '../src/providers/call_tracker.js'),module={exports:{}}
 vm.runInNewContext(fs.readFileSync(filename,'utf8'),{module,exports:module.exports,process:{env:{}},Date,Promise,Set,AbortSignal,setInterval,clearInterval,require:k=>k==='../db/delivery'?{...delivery,getStore:()=>store}:require(k)},{filename})
 const tracker=module.exports
 tracker.report('call','apns_ok',{job_id:'j',attempt_index:1,entry_index:0,user_id:1})
 const row=store.events()[0],m=row.event.meta
 const receipt={persisted:true,event_id:m.event_id,call_id:'call',diagnostic_principal:m.diagnostic_principal}
 await tracker.drain(async()=>({ok:true,json:async()=>({receipt})}))
 assert.equal(store.health().diagnostic_pending,1)
 store.retryEvent(row.id,Date.now()-1)
 Object.assign(receipt,{diagnostics_version:2,device_instance_id:m.device_instance_id,received_at_ms:123,duplicate:false})
 await tracker.drain(async()=>({ok:true,json:async()=>({ok:true,receipt})}))
 assert.equal(store.health().diagnostic_pending,0)
})

test('acknowledged backlog schedules another bounded batch without a fixed quarter-second gap', async () => {
 const store=delivery.createStore(new Database(':memory:'))
 const filename=path.resolve(__dirname, '../src/providers/call_tracker.js'),module={exports:{}},scheduled=[]
 const receipt=async(url,options)=>{
  const b=JSON.parse(options.body)
  return {ok:true,json:async()=>({ok:true,receipt:{persisted:true,diagnostics_version:2,duplicate:false,
   received_at_ms:123,call_id:'call',event_id:b.meta.event_id,device_instance_id:b.meta.device_instance_id,diagnostic_principal:b.meta.diagnostic_principal}})}
 }
 vm.runInNewContext(fs.readFileSync(filename,'utf8'),{module,exports:module.exports,process:{env:{}},Date,Promise,Set,AbortSignal,
  setInterval:()=>({unref(){}}),clearInterval(){},setImmediate:fn=>scheduled.push(fn),fetch:receipt,
  require:k=>k==='../db/delivery'?{...delivery,getStore:()=>store}:require(k)},{filename})
 const tracker=module.exports
 for(let i=0;i<5;i++)tracker.report('call','apns_ok',{job_id:'j',attempt_index:1,entry_index:i})
 tracker.start()
 await tracker.drain(receipt)
 assert.equal(store.health().diagnostic_pending,3)
 assert.equal(scheduled.length,1)
 await scheduled.shift()()
 assert.equal(store.health().diagnostic_pending,1)
 assert.equal(scheduled.length,1)
 tracker.stop()
 await scheduled.shift()()
 assert.equal(store.health().diagnostic_pending,1,'stop prevents scheduled work; durable backlog retained')
})

test('failed SQLite retry cannot release the drain guard while sibling HTTP remains pending', async () => {
 const db=new Database(':memory:'),store=delivery.createStore(db),held=[]
 const retry=store.retryEvent
 store.retryEvent=(...args)=>{
  db.pragma('query_only=ON')
  try{return retry(...args)}finally{db.pragma('query_only=OFF')}
 }
 const filename=path.resolve(__dirname, '../src/providers/call_tracker.js'),module={exports:{}}
 vm.runInNewContext(fs.readFileSync(filename,'utf8'),{module,exports:module.exports,process:{env:{}},Date,Promise,Set,AbortSignal,setInterval,clearInterval,
  require:k=>k==='../db/delivery'?{...delivery,getStore:()=>store}:require(k)},{filename})
 const tracker=module.exports
 for(let i=0;i<2;i++)tracker.report('call','apns_ok',{job_id:'j',attempt_index:1,entry_index:i})
 let started=0
 const fetch=async()=>{
  started++
  if(started===1)return {ok:false}
  await new Promise(resolve=>held.push(resolve))
  return {ok:false}
 }
 const first=tracker.drain(fetch).catch(()=>{})
 await new Promise(resolve=>setImmediate(resolve))
 const second=tracker.drain(fetch).catch(()=>{})
 try{assert.equal(started,2,'sibling in-flight HTTP keeps the batch guard even after SQLite failure')}
 finally{held.forEach(resolve=>resolve());await Promise.all([first,second]);db.close()}
})
