// src/api/schema.js
const apps = require('../../config/apps.config.js')

const { ISSUER_PATTERN, SESSION_PATTERN } = require('../queue/source_protocol')

const appIds = apps.apps.map(app => app.id)

const sendSchema = {
  type: 'object',
  required: ['app_id', 'priority', 'recipients', 'request'],
  properties: {
    delivery: {
      type: 'object', required: ['version', 'idempotency_key'], additionalProperties: false,
      properties: {
        version: { const: 1 },
        source: {
          type: 'object', required: ['kind', 'issuer', 'session_id'], additionalProperties: false,
          properties: {
            kind: { const: 'sip' },
            issuer: { type: 'string', pattern: ISSUER_PATTERN },
            session_id: { type: 'string', pattern: SESSION_PATTERN },
          },
        },
        idempotency_key: { type: 'string', minLength: 1, maxLength: 128, pattern: '^[A-Za-z0-9_.:-]+$' },
        expires_at_ms: { type: 'integer', minimum: 1, maximum: 8640000000000000 },
        operational_generation: { type: 'string', minLength: 1, maxLength: 128, pattern: '^[A-Za-z0-9_.:-]+$' },
      },
    },
    app_id: {
      type: 'string',
      enum: appIds,
    },
    priority: {
      type: 'string',
      enum: ['high', 'standard'],
    },
    recipients: {
      type: 'array',
      minItems: 1,
      items: {
        type: 'object',
        required: ['token', 'type'],
        properties: {
          id: { type: 'string' },
          token: { type: 'string', minLength: 1 },
          type: { type: 'string', enum: ['ios', 'android'] },
        },
      },
    },
    request: {
      type: 'object',
      required: ['payload'],
      properties: {
        headers: { type: 'object', additionalProperties: true },
        payload: { type: 'object', additionalProperties: true },
      },
    },
  },
}

const testSendSchema = {
  type: 'object',
  required: ['app_id', 'token', 'type', 'priority', 'request'],
  properties: {
    app_id: {
      type: 'string',
      enum: appIds,
    },
    token: { type: 'string', minLength: 1 },
    type: { type: 'string', enum: ['ios', 'android'] },
    priority: { type: 'string', enum: ['high', 'standard'] },
    request: {
      type: 'object',
      required: ['payload'],
      properties: {
        headers: { type: 'object', additionalProperties: true },
        payload: { type: 'object', additionalProperties: true },
      },
    },
  },
}

module.exports = {
  sendSchema,
  testSendSchema,
}
