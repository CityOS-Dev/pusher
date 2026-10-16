const {test}=require('node:test'),assert=require('node:assert/strict')
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm')
const Database=require('better-sqlite3'),delivery=require('../src/db/delivery')
test('500 recipient diagnostics persist immediately; network drain bounded at two; lost ACK retries same event',async()=>{
 const store=delivery.createStore(new Database(':memory:'))
 const filename=path.resolve('src/providers/call_tracker.js'),module={exports:{}}
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
 const filename=path.resolve('src/providers/call_tracker.js'),module={exports:{}}
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
