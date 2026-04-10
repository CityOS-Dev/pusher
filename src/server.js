// src/server.js
const Fastify = require('fastify')
const { createQueues, startWorker, closeQueues, queues } = require('./queue')
const apiRoutes = require('./api/routes')
const dashboardRoutes = require('./dashboard/routes')
const { createBullBoard } = require('@bull-board/api')
const { FastifyAdapter } = require('@bull-board/fastify')
const { BullMQAdapter } = require('@bull-board/api/bullMQAdapter')
const config = require('../config')
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
    const { getQueueStats } = require('./queue')
    const stats = {
      high: await getQueueStats('high'),
      standard: await getQueueStats('standard'),
    }

    return {
      status: 'ok',
      timestamp: new Date().toISOString(),
      queues: stats,
    }
  })

  // Graceful shutdown
  const close = async () => {
    logger.info('Shutting down...')
    await closeQueues()
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
  start()
}

module.exports = { buildServer, start, queues }
