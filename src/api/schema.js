// src/api/schema.js
const apps = require('../../config/apps.config.js')

const appIds = apps.apps.map(app => app.id)

const sendSchema = {
  type: 'object',
  required: ['app_id', 'priority', 'recipients', 'request'],
  properties: {
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
