// src/api/routes.js
const { addJob } = require('../queue')
const { sendSchema } = require('./schema')
const { getInvalidTokens, getInvalidTokenCount, deleteInvalidTokens } = require('../db/tokens')
const { describeFailure } = require('../providers/provider_errors')

async function routes(fastify, options) {
  // GET /invalid-tokens - List invalid tokens (public, for external consumers)
  fastify.get('/invalid-tokens', {
    schema: {
      querystring: {
        type: 'object',
        properties: {
          app_id: { type: 'string' },
          type: { type: 'string', enum: ['ios', 'android'] },
          limit: { type: 'integer', minimum: 1, maximum: 1000, default: 100 },
          offset: { type: 'integer', minimum: 0, default: 0 },
        },
      },
    },
  }, async (request, reply) => {
    const { app_id, type, limit, offset } = request.query
    const tokens = getInvalidTokens(app_id || null, limit, offset, type || null)
    const count = getInvalidTokenCount(app_id || null, type || null)
    return { tokens, count }
  })

  // DELETE /invalid-tokens - Delete processed invalid tokens by ids and/or tokens
  fastify.delete('/invalid-tokens', {
    schema: {
      body: {
        type: 'object',
        properties: {
          ids: { type: 'array', items: { type: 'integer' } },
          tokens: { type: 'array', items: { type: 'string', minLength: 1 } },
        },
      },
    },
  }, async (request, reply) => {
    const { ids = [], tokens = [] } = request.body || {}

    if ((!Array.isArray(ids) || ids.length === 0) && (!Array.isArray(tokens) || tokens.length === 0)) {
      return reply.code(400).send({ error: 'Provide at least one of "ids" or "tokens"' })
    }

    const deleted = deleteInvalidTokens(ids, tokens)
    return { deleted }
  })

  // POST /send - Send push notifications
  fastify.post('/send', {
    schema: {
      body: sendSchema,
      response: {
        202: {
          type: 'object',
          properties: {
            message: { type: 'string' },
            jobIds: { type: 'array', items: { type: 'string' } },
          },
        },
      },
    },
  }, async (request, reply) => {
    const { app_id, priority, recipients, request: req, delivery } = request.body

    try {
      const jobIds = await addJob(priority, {
        app_id,
        recipients,
        request: req,
        ...(delivery ? { delivery } : {}),
      })

      return reply.code(202).send({
        message: 'Push notifications queued',
        jobIds,
      })
    } catch (error) {
      request.log.error({ code: error.statusCode === 409 ? 'idempotency_conflict':'enqueue_failed', ...describeFailure(error) })
      if (error.statusCode === 409) return reply.code(409).send({ error:'Idempotency key conflicts with existing request' })
      return reply.code(500).send({
        error: 'Failed to queue push notifications',
      })
    }
  })
}

module.exports = routes
