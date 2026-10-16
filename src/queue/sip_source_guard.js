// Fixed read-only SIP source RPC. Jobs carry identity only, never credentials or URLs.
const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const { sendWithin } = require('./provider_deadline')
const { validSource, validSession } = require('./source_protocol')
const MAX_REPLY_BYTES = 1048576
const MAX_CHANNELS = 4096
const RPC_TIMEOUT_MS = 1500
const CLEANUP_TIMEOUT_MS = 100
const MAX_QUERIES = 2

function decodeReply(content, source, nonce) {
  if (!Buffer.isBuffer(content) || content.length > MAX_REPLY_BYTES) return 'unknown'
  let reply
  try { reply = JSON.parse(content.toString('utf8')) } catch { return 'unknown' }
  if (!reply || Array.isArray(reply) || reply.kind !== 'nw-sip-source-status-response' ||
      reply.protocol_version !== 1 || reply.source_issuer !== source.issuer || reply.request_id !== nonce ||
      reply.complete !== true || reply.proof !== 'stable-cli-status-v1' ||
      !Number.isInteger(reply.channels) || reply.channels < 0 || reply.channels > MAX_CHANNELS ||
      !Array.isArray(reply.sessions) || reply.sessions.length > reply.channels ||
      new Set(reply.sessions).size !== reply.sessions.length || !reply.sessions.every(validSession) ||
      (reply.channels === 0) !== (reply.sessions.length === 0)) return 'unknown'
  return reply.sessions.includes(source.session_id) ? 'active' : 'ended'
}

function createConfigLoader(filename) {
  let cached
  return () => {
    if (cached) return cached
    let descriptor
    try {
      const uid = process.getuid()
      const directory = fs.statSync(path.dirname(filename))
      if (!directory.isDirectory() || directory.mode & 0o077 || ![0, uid].includes(directory.uid)) throw Error()
      descriptor = fs.openSync(filename, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK)
      const stat = fs.fstatSync(descriptor)
      if (!stat.isFile() || stat.mode & 0o077 || ![0, uid].includes(stat.uid) || stat.size > 4096) throw Error()
      const config = JSON.parse(fs.readFileSync(descriptor, 'utf8'))
      if (!config || typeof config.host !== 'string' || !/^[A-Za-z0-9_.:-]{1,253}(?![\s\S])/.test(config.host) ||
          !Number.isInteger(config.port) || config.port < 1 || config.port > 65535 ||
          typeof config.username !== 'string' || config.username.length > 512 ||
          typeof config.password !== 'string' || config.password.length > 512 ||
          typeof config.vhost !== 'string' || config.vhost.length > 255) throw Error()
      cached = Object.freeze({ protocol: 'amqp', hostname: config.host, port: config.port,
        username: config.username, password: config.password, vhost: config.vhost, heartbeat: 5, frameMax: 131072 })
      return cached
    } catch { throw Error('source_configuration_unavailable') }
    finally { if (descriptor !== undefined) fs.closeSync(descriptor) }
  }
}
const loadConfig = createConfigLoader(path.join(__dirname, '..', '..', '.config', 'sip-broker.json'))

function createGuard(options = {}) {
  const connect = options.connect || ((...args) => require('amqplib').connect(...args))
  const config = options.loadConfig || loadConfig
  const timeout = options.timeoutMs || RPC_TIMEOUT_MS
  let running = 0
  async function check(source) {
    if (!validSource(source) || running >= MAX_QUERIES) return 'unknown'
    running++
    let connection, channel, operation, operationDone = false, closed = false, released = false
    const release = () => { if (!released) { released = true; running-- } }
    const destroy = resource => {
      try { resource?.connection?.stream?.destroy() } catch {}
      try { resource?.stream?.destroy() } catch {}
    }
    const close = resource => {
      try { return Promise.resolve(resource?.close()).catch(() => {}) }
      catch { return Promise.resolve() }
    }
    try {
      operation = (async () => {
        connection = await connect(config(), { timeout })
        connection.on('error', () => {})
        if (closed) {
          destroy(connection)
          await sendWithin(() => close(connection), CLEANUP_TIMEOUT_MS).catch(() => {})
          return 'unknown'
        }
        channel = await connection.createChannel()
        if (closed) {
          destroy(channel); destroy(connection)
          await sendWithin(() => close(channel), CLEANUP_TIMEOUT_MS).catch(() => {})
          return 'unknown'
        }
        const nonce = crypto.randomBytes(16).toString('hex')
        let settle
        const response = new Promise(resolve => { settle = resolve })
        channel.on('error', () => settle('unknown'))
        channel.on('close', () => settle('unknown'))
        connection.on('close', () => settle('unknown'))
        const { queue } = await channel.assertQueue('', { durable: false, exclusive: true, autoDelete: true,
          arguments: { 'x-expires': 5000, 'x-max-length': 2, 'x-max-length-bytes': MAX_REPLY_BYTES } })
        if (closed || !/^amq\.gen-[A-Za-z0-9_.:/+=-]{1,128}(?![\s\S])/.test(queue)) return 'unknown'
        await channel.consume(queue, message => {
          if (!message) { settle('unknown'); return }
          if (message.properties?.correlationId !== nonce) return
          settle(decodeReply(message.content, source, nonce))
        }, { noAck: true })
        if (closed) return 'unknown'
        const request = { kind: 'nw-sip-source-status-request', protocol_version: 1,
          source_issuer: source.issuer, request_id: nonce }
        channel.sendToQueue('nw.sip.status.' + crypto.createHash('sha256').update(source.issuer).digest('hex'),
          Buffer.from(JSON.stringify(request)), { correlationId: nonce, replyTo: queue,
            expiration: String(timeout), contentType: 'application/json' })
        return response
      })()
      operation.then(() => { operationDone = true }, () => { operationDone = true })
      return await sendWithin(() => operation, timeout)
    } catch { return 'unknown' }
    finally {
      closed = true
      destroy(channel); destroy(connection)
      await sendWithin(() => Promise.all([close(channel), close(connection)]), CLEANUP_TIMEOUT_MS).catch(() => {})
      // Rejection runs release too. A genuinely unresolved transport deliberately
      // retains its permit, bounding late sockets instead of pretending cancellation.
      if (!operation || operationDone) release()
      else operation.then(release, release)
    }
  }
  return { check }
}
const guard = createGuard()
module.exports = { createGuard, createConfigLoader, decodeReply, check: guard.check }
