// src/api/routes.js
const { addJob, getQueueStats } = require('../queue');
const { sendSchema } = require('./schema');

async function routes(fastify, options) {
  // POST /send - Send push notifications
  fastify.post('/send', {
    schema: {
      body: sendSchema,
      response: {
        202: {
          type: 'object',
          properties: {
            message: { type: 'string' },
            jobIds: { type: 'array', items: { type: 'string' } }
          }
        }
      }
    }
  }, async (request, reply) => {
    const { app_id, priority, recipients, request: req } = request.body;

    try {
      const jobIds = await addJob(priority, {
        app_id,
        recipients,
        request: req
      });

      return reply.code(202).send({
        message: 'Push notifications queued',
        jobIds
      });
    } catch (error) {
      request.log.error(error);
      return reply.code(500).send({
        error: 'Failed to queue push notifications'
      });
    }
  });

  // GET /health - Health check
  fastify.get('/health', async (request, reply) => {
    const stats = {
      high: await getQueueStats('high'),
      standard: await getQueueStats('standard')
    };

    return {
      status: 'ok',
      timestamp: new Date().toISOString(),
      queues: stats
    };
  });
}

module.exports = routes;
