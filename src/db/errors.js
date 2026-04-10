// src/db/errors.js
const db = require('./index')
const logger = require('pino')({ level: 'info' })

function logError(appId, errorType, message) {
  const stmt = db.prepare(`
      INSERT INTO error_log (app_id, error_type, message)
      VALUES (?, ?, ?)
  `)

  try {
    const result = stmt.run(appId, errorType, message)
    return result.lastInsertRowid
  } catch (error) {
    logger.error({ error, appId }, 'Failed to log error')
    throw error
  }
}

function getErrors(appId = null, limit = 100, offset = 0) {
  try {
    let stmt
    const params = []

    if (appId) {
      stmt = db.prepare(`
          SELECT id, timestamp, app_id, error_type, message
          FROM error_log
          WHERE app_id = ?
          ORDER BY timestamp DESC
              LIMIT ?
          OFFSET ?
      `)
      params.push(appId, limit, offset)
    } else {
      stmt = db.prepare(`
          SELECT id, timestamp, app_id, error_type, message
          FROM error_log
          ORDER BY timestamp DESC
              LIMIT ?
          OFFSET ?
      `)
      params.push(limit, offset)
    }

    return stmt.all(...params)
  } catch (error) {
    logger.error({ error }, 'Failed to get errors')
    throw error
  }
}

function getErrorCount(appId = null) {
  try {
    let stmt
    const params = []

    if (appId) {
      stmt = db.prepare('SELECT COUNT(*) as count FROM error_log WHERE app_id = ?')
      params.push(appId)
    } else {
      stmt = db.prepare('SELECT COUNT(*) as count FROM error_log')
    }

    return stmt.get(...params).count
  } catch (error) {
    logger.error({ error }, 'Failed to get error count')
    throw error
  }
}

module.exports = {
  logError,
  getErrors,
  getErrorCount,
}
