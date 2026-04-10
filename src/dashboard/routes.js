// src/dashboard/routes.js
const { getStats } = require('../db/stats')
const { getQueueStats } = require('../queue')
const { getInvalidTokens, getInvalidTokenCount, clearInvalidTokens, addInvalidToken } = require('../db/tokens')
const { send } = require('../queue')
const apnProvider = require('../providers/apn')
const fcmProvider = require('../providers/fcm')
const { testSendSchema } = require('../api/schema')
const { DASHBOARD_HTML } = require('./ui')
const apps = require('../../config/apps.config')

async function routes(fastify, options) {
  // Middleware: localhost only
  fastify.addHook('onRequest', async (request, reply) => {
    if (request.ip !== '127.0.0.1' && request.ip !== '::1') {
      return reply.code(403).send({ error: 'Access denied - dashboard is localhost only' })
    }
  })

  // GET /dashboard - Main dashboard UI
  fastify.get('/', async (request, reply) => {
    reply.type('text/html')
    return DASHBOARD_HTML
  })

  // GET /dashboard/api/stats - Get statistics
  fastify.get('/api/stats', async (request, reply) => {
    return getStats(120)
  })

  // GET /dashboard/api/queue-stats - Get queue statistics
  fastify.get('/api/queue-stats', async (request, reply) => {
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

  // GET /dashboard/api/invalid-tokens - Get invalid tokens
  fastify.get('/api/invalid-tokens', {
    schema: {
      querystring: {
        type: 'object',
        properties: {
          app_id: { type: 'string' },
          limit: { type: 'number', default: 100 },
          offset: { type: 'number', default: 0 },
        },
      },
    },
  }, async (request, reply) => {
    const { app_id, limit, offset } = request.query
    const tokens = getInvalidTokens(app_id || null, limit, offset)
    const count = getInvalidTokenCount(app_id || null)
    return { tokens, count }
  })

  // DELETE /dashboard/api/invalid-tokens - Clear invalid tokens
  fastify.delete('/api/invalid-tokens', {
    schema: {
      querystring: {
        type: 'object',
        properties: {
          app_id: { type: 'string' },
        },
      },
    },
  }, async (request, reply) => {
    const { app_id } = request.query
    const deleted = clearInvalidTokens(app_id || null)
    return { deleted }
  })

  // POST /dashboard/api/test-send - Test send single notification
  fastify.post('/api/test-send', {
    schema: {
      body: testSendSchema,
      response: {
        200: {
          type: 'object',
          properties: {
            message: { type: 'string' },
            result: { type: 'object' },
          },
        },
      },
    },
  }, async (request, reply) => {
    const { app_id, token, type, priority, request: req } = request.body

    const appConfig = apps.apps.find(app => app.id === app_id)
    if (!appConfig) {
      return reply.code(400).send({ error: 'Invalid app_id' })
    }

    const recipient = [{ id: 'test', token, type }]
    const provider = type === 'ios' ? apnProvider : fcmProvider

    try {
      const result = await provider.send(recipient, req, appConfig)

      // Log invalid tokens if any
      if (result.invalidTokens.length > 0) {
        addInvalidToken(
          result.invalidTokens[0].external_id,
          result.invalidTokens[0].token,
          result.invalidTokens[0].app_id,
          result.invalidTokens[0].type,
          result.invalidTokens[0].reason,
        )
      }

      return {
        message: result.failures.length === 0 ? 'Send successful' : 'Send completed with errors',
        result,
      }
    } catch (error) {
      request.log.error(error)
      return reply.code(500).send({ error: error.message })
    }
  })
}

module.exports = routes
