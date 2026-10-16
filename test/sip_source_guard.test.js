const {test}=require('node:test'),assert=require('node:assert/strict')
const {createGuard,decodeReply}=require('../src/queue/sip_source_guard')
const source={kind:'sip',issuer:'asterisk:fixture',session_id:'123.4'}
const body=(nonce,sessions=['123.4'])=>({kind:'nw-sip-source-status-response',protocol_version:1,source_issuer:source.issuer,request_id:nonce,complete:true,proof:'stable-cli-status-v1',channels:sessions.length,sessions})
function transport(reply) {
 let receiver,closed=0,published
 const channel={on(){},assertQueue:async()=>({queue:'amq.gen-fixture'}),consume:async(q,fn)=>{receiver=fn;return {consumerTag:'c'}},sendToQueue(q,data,props){published={q,data:JSON.parse(data),props};queueMicrotask(()=>receiver({properties:{correlationId:props.correlationId},content:Buffer.from(JSON.stringify(reply(props.correlationId)))}));return true},close:async()=>{closed++}}
 return {connect:async()=>({on(){},createChannel:async()=>channel,close:async()=>{closed++}}),closed:()=>closed,published:()=>published}
}
test('fixed nonce-bound complete proof distinguishes active and ended; closes own transport',async()=>{
 for(const [sessions,status] of [[['123.4'],'active'],[[],'ended']]){
  const sdk=transport(n=>body(n,sessions)),guard=createGuard({connect:sdk.connect,loadConfig:()=>({host:'private',port:5672,username:'u',password:'p',vhost:'/'}),timeoutMs:100})
  assert.equal(await guard.check(source),status);assert.equal(sdk.closed(),2)
  assert.match(sdk.published().q,/^nw\.sip\.status\.[a-f0-9]{64}$/)
  assert.equal(sdk.published().data.request_id,sdk.published().props.correlationId)
 }
})
test('unqualified, oversized, duplicate or mismatched proof stays unknown',()=>{
 for(const patch of [{complete:false},{proof:'other'},{request_id:'wrong'},{channels:4097},{sessions:['123.4','123.4'],channels:2},{channels:1,sessions:[]}])assert.equal(decodeReply(Buffer.from(JSON.stringify({...body('n'),...patch})),source,'n'),'unknown')
 assert.equal(decodeReply(Buffer.alloc(1048577),source,'n'),'unknown')
})
test('invalid job identity cannot choose broker or queue and never connects',async()=>{
 let connected=0;const g=createGuard({connect:async()=>{connected++},loadConfig:()=>({})})
 assert.equal(await g.check({...source,issuer:'amqp://forbidden'}),'unknown');assert.equal(connected,0)
})
test('source status I/O is capped at two; timeout stays unknown and closes resources',async()=>{
 let connections=0
 const guard=createGuard({connect:async()=>{connections++;return {on(){},createChannel:async()=>new Promise(()=>{}),close:async()=>{}}},loadConfig:()=>({}),timeoutMs:10})
 const first=guard.check(source),second=guard.check(source)
 assert.equal(await guard.check(source),'unknown')
 assert.equal(connections,2);assert.deepEqual(await Promise.all([first,second]),['unknown','unknown'])
})
test('timeout hard-destroys hung transport and holds permit until late connect settles',async()=>{
 let resolveConnect,opened=0,destroyed=0
 const connection={on(){},connection:{stream:{destroy(){destroyed++}}},createChannel:async()=>new Promise(()=>{}),close:()=>new Promise(()=>{})}
 const guard=createGuard({connect:()=>{opened++;return new Promise(r=>{resolveConnect=r})},loadConfig:()=>({}),timeoutMs:5})
 assert.equal(await guard.check(source),'unknown')
 // A pending connection retains its permit; another two calls cannot create three sockets.
 const second=guard.check(source)
 assert.equal(await guard.check(source),'unknown')
 assert.equal(opened,2)
 resolveConnect(connection)
 await second
 await new Promise(r=>setTimeout(r,10))
 assert.ok(destroyed>0)
})
