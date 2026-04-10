// src/server.js
const Fastify = require('fastify');
const { createQueues, startWorker, closeQueues } = require('./queue');
const apiRoutes = require('./api/routes');
const dashboardRoutes = require('./dashboard/routes');
const { createBullBoard } = require('@bull-board/api');
const { FastifyAdapter } = require('@bull-board/fastify');
const { BullMQAdapter } = require('@bull-board/api/bullMQAdapter');
const config = require('../config');
const pino = require('pino');

const logger = pino({
  level: config.log.level,
  transport: { target: 'pino-pretty' }
});

async function buildServer() {
  const fastify = Fastify({
    logger: false,
    trustProxy: true
  });

  // Add pino as logger
  fastify.addHook('onRequest', (request, reply, done) => {
    request.log = logger;
    done();
  });

  // Register routes
  await fastify.register(apiRoutes, { prefix: '/api' });
  await fastify.register(dashboardRoutes, { prefix: '/dashboard' });

  // Bull Board integration
  const serverAdapter = new FastifyAdapter();

  createBullBoard({
    queues: [],
    serverAdapter,
  });

  await fastify.register(serverAdapter.registerPlugin(), {
    prefix: '/dashboard/queues',
    basePath: '/dashboard/queues'
  });

  // Add queues to Bull Board after they are created
  fastify.addHook('onReady', async () => {
    const { queues: queueMap } = require('./queue');
    const bullBoardQueues = [];
    if (queueMap.high) bullBoardQueues.push(new BullMQAdapter(queueMap.high));
    if (queueMap.standard) bullBoardQueues.push(new BullMQAdapter(queueMap.standard));

    const bullBoardApi = serverAdapter.getBullBoard();
    bullBoardApi.setQueues(bullBoardQueues);
  });

  // Health check
  fastify.get('/', async (request, reply) => {
    return {
      name: 'Push Notification Service',
      status: 'running',
      timestamp: new Date().toISOString()
    };
  });

  // Graceful shutdown
  const close = async () => {
    logger.info('Shutting down...');
    await closeQueues();
    await fastify.close();
    process.exit(0);
  };

  process.on('SIGTERM', close);
  process.on('SIGINT', close);

  return fastify;
}

async function start() {
  logger.info('Starting Push Notification Service...');

  // Create queues
  createQueues();

  // Start worker
  await startWorker();

  // Build and start server
  const server = await buildServer();

  try {
    await server.listen({
      port: config.port,
      host: config.host
    });
    logger.info(`Server listening on ${config.host}:${config.port}`);
  } catch (error) {
    logger.error(error);
    process.exit(1);
  }
}

// Start if run directly
if (require.main === module) {
  start();
}

module.exports = { buildServer, start };
