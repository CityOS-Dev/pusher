const { describeFailure, tokenFingerprint } = require('../providers/provider_errors')
// src/db/tokens.js
const db = require('./index')
const logger = require('pino')({ level: 'info' })

function addInvalidToken(externalId, token, appId, type, reason) {
  const stmt = db.prepare(`
      INSERT INTO invalid_tokens (external_id, token, app_id, type, reason)
      VALUES (?, ?, ?, ?, ?)
  `)

  try {
    const result = stmt.run(externalId, token, appId, type, reason)
    return result.lastInsertRowid
  } catch (error) {
    logger.error({ ...describeFailure(error), appId }, 'Failed to add invalid token')
    throw error
  }
}

function addInvalidTokens(tokens) {
  const stmt = db.prepare(`
      INSERT INTO invalid_tokens (external_id, token, app_id, type, reason)
      VALUES (?, ?, ?, ?, ?)
  `)

  const insertMany = db.transaction((tokenList) => {
    const results = []
    for (const token of tokenList) {
      try {
        const result = stmt.run(
          token.external_id,
          token.token,
          token.app_id,
          token.type,
          token.reason,
        )
        results.push(result.lastInsertRowid)
      } catch (error) {
        logger.error({ ...describeFailure(error), ...tokenFingerprint(token.token) }, 'Failed to add invalid token (batch)')
      }
    }
    return results
  })

  return insertMany(tokens)
}

function getInvalidTokens(appId = null, limit = 100, offset = 0, type = null) {
  try {
    const where = []
    const params = []

    if (appId) {
      where.push('app_id = ?')
      params.push(appId)
    }
    if (type) {
      where.push('type = ?')
      params.push(type)
    }

    const whereClause = where.length > 0 ? `WHERE ${where.join(' AND ')}` : ''

    const stmt = db.prepare(`
        SELECT id, external_id, token, app_id, type, reason, detected_at
        FROM invalid_tokens
        ${whereClause}
        ORDER BY detected_at DESC LIMIT ? OFFSET ?
    `)
    params.push(limit, offset)

    return stmt.all(...params)
  } catch (error) {
    logger.error(describeFailure(error), 'Failed to get invalid tokens')
    throw error
  }
}

function getInvalidTokenCount(appId = null, type = null) {
  try {
    const where = []
    const params = []

    if (appId) {
      where.push('app_id = ?')
      params.push(appId)
    }
    if (type) {
      where.push('type = ?')
      params.push(type)
    }

    const whereClause = where.length > 0 ? `WHERE ${where.join(' AND ')}` : ''
    const stmt = db.prepare(`SELECT COUNT(*) as count FROM invalid_tokens ${whereClause}`)
    return stmt.get(...params).count
  } catch (error) {
    logger.error(describeFailure(error), 'Failed to get invalid token count')
    throw error
  }
}

function isTokenInvalid(token) {
  try {
    const stmt = db.prepare('SELECT COUNT(*) as count FROM invalid_tokens WHERE token = ?')
    const result = stmt.get(token)
    return result.count > 0
  } catch (error) {
    logger.error(describeFailure(error), 'Failed to check if token is invalid')
    throw error
  }
}

function deleteInvalidTokens(ids = [], tokens = []) {
  try {
    const idsList = Array.isArray(ids) ? ids.filter(id => Number.isInteger(id)) : []
    const tokensList = Array.isArray(tokens) ? tokens.filter(t => typeof t === 'string' && t.length > 0) : []

    if (idsList.length === 0 && tokensList.length === 0) {
      return 0
    }

    const deleteByIds = (list) => {
      const placeholders = list.map(() => '?').join(',')
      const stmt = db.prepare(`DELETE FROM invalid_tokens WHERE id IN (${placeholders})`)
      return stmt.run(...list).changes
    }

    const deleteByTokens = (list) => {
      const placeholders = list.map(() => '?').join(',')
      const stmt = db.prepare(`DELETE FROM invalid_tokens WHERE token IN (${placeholders})`)
      return stmt.run(...list).changes
    }

    let deleted = 0
    if (idsList.length > 0) {
      deleted += deleteByIds(idsList)
    }
    if (tokensList.length > 0) {
      deleted += deleteByTokens(tokensList)
    }
    return deleted
  } catch (error) {
    logger.error(describeFailure(error), 'Failed to delete invalid tokens')
    throw error
  }
}

function clearInvalidTokens(appId = null) {
  try {
    if (appId) {
      const stmt = db.prepare('DELETE FROM invalid_tokens WHERE app_id = ?')
      return stmt.run(appId).changes
    } else {
      const stmt = db.prepare('DELETE FROM invalid_tokens')
      return stmt.run().changes
    }
  } catch (error) {
    logger.error(describeFailure(error), 'Failed to clear invalid tokens')
    throw error
  }
}

module.exports = {
  addInvalidToken,
  addInvalidTokens,
  getInvalidTokens,
  getInvalidTokenCount,
  isTokenInvalid,
  deleteInvalidTokens,
  clearInvalidTokens,
}
