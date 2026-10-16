const {test}=require('node:test'),assert=require('node:assert/strict')
const Fastify=require('fastify'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm')
function load(file,deps){const module={exports:{}},filename=path.resolve(file);vm.runInNewContext(fs.readFileSync(filename,'utf8'),{module,exports:module.exports,require:key=>deps[key]||require(key)},{filename});return module.exports}
test('API preserves legacy202 and forwards optional delivery; conflicting key returns409',async()=>{
 const schema=load('src/api/schema.js',{'../../config/apps.config.js':{apps:[{id:'a'}]}})
 const sent=[],app=Fastify({logger:false})
 const routes=load('src/api/routes.js',{'../queue':{addJob:async(p,data)=>{sent.push(data);if(data.delivery?.idempotency_key==='conflict')throw Object.assign(Error('conflict'),{statusCode:409});return ['job']}},'./schema':schema,'../db/tokens':{}})
 await app.register(routes)
 const body={app_id:'a',priority:'high',recipients:[{token:'fake',type:'ios'}],request:{payload:{}}}
 const first=await app.inject({method:'POST',url:'/send',payload:body})
 assert.equal(first.statusCode,202);assert.deepEqual(first.json(),{message:'Push notifications queued',jobIds:['job']})
 const second=await app.inject({method:'POST',url:'/send',payload:{...body,delivery:{version:1,idempotency_key:'key'}}})
 assert.equal(second.statusCode,202);assert.equal(sent[1].delivery.idempotency_key,'key')
 const conflict=await app.inject({method:'POST',url:'/send',payload:{...body,delivery:{version:1,idempotency_key:'conflict'}}})
 assert.equal(conflict.statusCode,409)
 await app.close()
})
