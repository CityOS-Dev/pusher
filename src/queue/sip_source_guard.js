// Fixed read-only SIP source RPC. Jobs carry identity only, never credentials or URLs.
const crypto=require('node:crypto')
const {sendWithin}=require('./provider_deadline')
const validSource=s=>s?.kind==='sip' && typeof s.issuer==='string' && /^asterisk:[A-Za-z0-9_.-]{1,119}$(?![\s\S])/.test(s.issuer) && typeof s.session_id==='string' && /^[A-Za-z0-9_.:-]{1,128}$(?![\s\S])/.test(s.session_id)
function decodeReply(content,source,nonce) {
  if (!Buffer.isBuffer(content) || content.length>1048576) return 'unknown'
  let r
  try {r=JSON.parse(content.toString('utf8'))} catch {return 'unknown'}
  if (!r || Array.isArray(r) || r.kind!=='nw-sip-source-status-response' || r.protocol_version!==1 || r.source_issuer!==source.issuer || r.request_id!==nonce || r.complete!==true || r.proof!=='stable-cli-status-v1' || !Number.isInteger(r.channels) || r.channels<0 || r.channels>4096 || !Array.isArray(r.sessions) || r.sessions.length>r.channels || new Set(r.sessions).size!==r.sessions.length || r.sessions.some(id=>typeof id!=='string'||!/^[A-Za-z0-9_.:-]{1,128}$(?![\s\S])/.test(id)) || (r.channels===0)!==(r.sessions.length===0)) return 'unknown'
  return r.sessions.includes(source.session_id)?'active':'ended'
}
function loadConfig() {
  const fs=require('node:fs'),path=require('node:path')
  const c=JSON.parse(fs.readFileSync(path.join(__dirname,'..','..','.config','sip-broker.json'),'utf8'))
  if (!c || typeof c.host!=='string' || !/^[A-Za-z0-9_.:-]{1,253}$/.test(c.host) || !Number.isInteger(c.port) || c.port<1 || c.port>65535 || typeof c.username!=='string' || c.username.length>512 || typeof c.password!=='string' || c.password.length>512 || typeof c.vhost!=='string' || c.vhost.length>255) throw Error('source_configuration_unavailable')
  return {protocol:'amqp',hostname:c.host,port:c.port,username:c.username,password:c.password,vhost:c.vhost,heartbeat:5,frameMax:131072}
}
function createGuard(options={}) {
  const connect=options.connect||((...args)=>require('amqplib').connect(...args))
  const config=options.loadConfig||loadConfig,timeout=options.timeoutMs||1500
  let running=0
  async function check(source) {
    if (!validSource(source) || running>=2) return 'unknown'
    running++
    let connection,channel,operation,operationDone=false,closed=false,released=false
    const release=()=>{if(!released){released=true;running--}}
    const destroy=resource=>{try{resource?.connection?.stream?.destroy()}catch{};try{resource?.stream?.destroy()}catch{}}
    const close=resource=>{try{return Promise.resolve(resource?.close()).catch(()=>{})}catch{return Promise.resolve()}}
    try {
      operation=(async()=>{
        const settings=config()
        connection=await connect(settings,{timeout})
        connection.on('error',()=>{})
        if(closed){destroy(connection);await sendWithin(()=>close(connection),100).catch(()=>{});return 'unknown'}
        channel=await connection.createChannel()
        if(closed){destroy(channel);destroy(connection);await sendWithin(()=>close(channel),100).catch(()=>{});return 'unknown'}
        const nonce=crypto.randomBytes(16).toString('hex')
        let settle
        const response=new Promise(resolve=>{settle=resolve})
        channel.on('error',()=>settle('unknown'))
        channel.on('close',()=>settle('unknown'))
        connection.on('close',()=>settle('unknown'))
        const {queue}=await channel.assertQueue('',{durable:false,exclusive:true,autoDelete:true,arguments:{'x-expires':5000,'x-max-length':2,'x-max-length-bytes':1048576}})
        if(closed || !/^amq\.gen-[A-Za-z0-9_.:/+=-]{1,128}$(?![\s\S])/.test(queue)) return 'unknown'
        await channel.consume(queue,message=>{
          if(!message){settle('unknown');return}
          if(message.properties?.correlationId!==nonce)return
          settle(decodeReply(message.content,source,nonce))
        },{noAck:true})
        if(closed)return 'unknown'
        const request={kind:'nw-sip-source-status-request',protocol_version:1,source_issuer:source.issuer,request_id:nonce}
        channel.sendToQueue('nw.sip.status.'+crypto.createHash('sha256').update(source.issuer).digest('hex'),Buffer.from(JSON.stringify(request)),{correlationId:nonce,replyTo:queue,expiration:String(timeout),contentType:'application/json'})
        return response
      })()
      operation.then(()=>{operationDone=true},()=>{operationDone=true})
      return await sendWithin(()=>operation,timeout)
    } catch {return 'unknown'}
    finally {
      closed=true
      destroy(channel);destroy(connection)
      await sendWithin(()=>Promise.all([close(channel),close(connection)]),100).catch(()=>{})
      if(!operation || operationDone) release()
      else operation.then(release,release)
    }
  }
  return {check}
}
const guard=createGuard()
module.exports={createGuard,decodeReply,check:guard.check}
