// src/server.js
const Fastify = require('fastify')
const { createQueues, startWorker, closeQueues, getQueueStats, queues } = require('./queue')
const { checkpointHealth } = require('./queue/processor')
const { providerBudget, sendWithin } = require('./queue/provider_deadline')
const { describeFailure } = require('./providers/provider_errors')
const apiRoutes = require('./api/routes')
const dashboardRoutes = require('./dashboard/routes')
const { createBullBoard } = require('@bull-board/api')
const { FastifyAdapter } = require('@bull-board/fastify')
const { BullMQAdapter } = require('@bull-board/api/bullMQAdapter')
const config = require('../config')
const tracker = require('./providers/call_tracker')
const { getStore } = require('./db/delivery')
const pino = require('pino')

const logger = pino({
  level: config.log.level,
  transport: { target: 'pino-pretty' },
})

async function buildServer() {
  const fastify = Fastify({
    logger: false,
    trustProxy: true,
  })

  // Add pino as logger
  fastify.addHook('onRequest', (request, reply, done) => {
    request.log = logger
    done()
  })

  // Register routes
  await fastify.register(apiRoutes, { prefix: '/api' })
  await fastify.register(dashboardRoutes, { prefix: '/dashboard' })

  // Bull Board integration
  const serverAdapter = new FastifyAdapter()

  // Set up queues for Bull Board
  const bullBoardQueues = []
  if (queues.high) {
    logger.info('Adding high priority queue to Bull Board')
    bullBoardQueues.push(new BullMQAdapter(queues.high))
  }
  if (queues.standard) {
    logger.info('Adding standard priority queue to Bull Board')
    bullBoardQueues.push(new BullMQAdapter(queues.standard))
  }

  logger.info({ queueCount: bullBoardQueues.length }, 'Bull Board queues configured')

  createBullBoard({
    queues: bullBoardQueues,
    serverAdapter,
  })

  serverAdapter.setBasePath('/dashboard/queues');
  await fastify.register(serverAdapter.registerPlugin(), {
    prefix: '/dashboard/queues',
  })

  // Health check
  fastify.get('/', async (request, reply) => {
    return {
      name: 'Push Notification Service',
      status: 'running',
      timestamp: new Date().toISOString(),
    }
  })

  // Health check endpoint (same as /api/health but without /api prefix)
  fastify.get('/health', async (request, reply) => {
    const [high, standard] = await Promise.all([getQueueStats('high'), getQueueStats('standard')])
    const stats = { high, standard }
    let delivery
    try {
      const checkpoint = checkpointHealth()
      delivery = { ...getStore().health(), storage_available: checkpoint.available, checkpoint }
    }
    catch { delivery = { storage_available: false } }
    const ready = delivery.storage_available && Object.values(stats).every(q => q?.worker?.ready)
    if (!ready) reply.code(503)
    return {
      status: ready ? 'ok' : 'degraded',
      delivery,
      provider_operations: providerBudget.health(),
      timestamp: new Date().toISOString(),
      queues: stats,
    }
  })

  // Graceful shutdown
  const close = async () => {
    logger.info('Shutting down...')
    await closeQueues()
    tracker.stop()
    try { await sendWithin(() => tracker.drain(), 1000) }
    catch (error) { logger.warn(describeFailure(error), 'Diagnostic shutdown flush incomplete; durable rows retained') }
    await fastify.close()
    process.exit(0)
  }

  process.on('SIGTERM', close)
  process.on('SIGINT', close)

  return fastify
}

async function start() {
  logger.info('Starting Push Notification Service...')

  // Create queues
  createQueues()

  // Start worker
  await startWorker()
  tracker.start()

  // Build and start server
  const server = await buildServer()

  try {
    await server.listen({
      port: config.port,
      host: config.host,
    })
    logger.info(`Server listening on ${config.host}:${config.port}`)
  } catch (error) {
    logger.error(error)
    process.exit(1)
  }
}

// Start if run directly
if (require.main === module) {
  start().then()
}

module.exports = { buildServer, start, queues }
