// src/db/tokens.js
const db = require('./index');
const logger = require('pino')({ level: 'info' });

function addInvalidToken(externalId, token, appId, type, reason) {
  const stmt = db.prepare(`
    INSERT INTO invalid_tokens (external_id, token, app_id, type, reason)
    VALUES (?, ?, ?, ?, ?)
  `);

  try {
    const result = stmt.run(externalId, token, appId, type, reason);
    return result.lastInsertRowid;
  } catch (error) {
    logger.error({ error, appId }, 'Failed to add invalid token');
    throw error;
  }
}

function addInvalidTokens(tokens) {
  const stmt = db.prepare(`
    INSERT INTO invalid_tokens (external_id, token, app_id, type, reason)
    VALUES (?, ?, ?, ?, ?)
  `);

  const insertMany = db.transaction((tokenList) => {
    const results = [];
    for (const token of tokenList) {
      try {
        const result = stmt.run(
          token.external_id,
          token.token,
          token.app_id,
          token.type,
          token.reason
        );
        results.push(result.lastInsertRowid);
      } catch (error) {
        logger.error({ error, token: token.token }, 'Failed to add invalid token (batch)');
      }
    }
    return results;
  });

  return insertMany(tokens);
}

function getInvalidTokens(appId = null, limit = 100, offset = 0) {
  try {
    let stmt;
    const params = [];

    if (appId) {
      stmt = db.prepare(`
        SELECT id, external_id, token, app_id, type, reason, detected_at
        FROM invalid_tokens
        WHERE app_id = ?
        ORDER BY detected_at DESC
        LIMIT ? OFFSET ?
      `);
      params.push(appId, limit, offset);
    } else {
      stmt = db.prepare(`
        SELECT id, external_id, token, app_id, type, reason, detected_at
        FROM invalid_tokens
        ORDER BY detected_at DESC
        LIMIT ? OFFSET ?
      `);
      params.push(limit, offset);
    }

    return stmt.all(...params);
  } catch (error) {
    logger.error({ error }, 'Failed to get invalid tokens');
    throw error;
  }
}

function getInvalidTokenCount(appId = null) {
  try {
    let stmt;
    const params = [];

    if (appId) {
      stmt = db.prepare('SELECT COUNT(*) as count FROM invalid_tokens WHERE app_id = ?');
      params.push(appId);
    } else {
      stmt = db.prepare('SELECT COUNT(*) as count FROM invalid_tokens');
    }

    return stmt.get(...params).count;
  } catch (error) {
    logger.error({ error }, 'Failed to get invalid token count');
    throw error;
  }
}

function isTokenInvalid(token) {
  try {
    const stmt = db.prepare('SELECT COUNT(*) as count FROM invalid_tokens WHERE token = ?');
    const result = stmt.get(token);
    return result.count > 0;
  } catch (error) {
    logger.error({ error }, 'Failed to check if token is invalid');
    throw error;
  }
}

function clearInvalidTokens(appId = null) {
  try {
    if (appId) {
      const stmt = db.prepare('DELETE FROM invalid_tokens WHERE app_id = ?');
      return stmt.run(appId).changes;
    } else {
      const stmt = db.prepare('DELETE FROM invalid_tokens');
      return stmt.run().changes;
    }
  } catch (error) {
    logger.error({ error }, 'Failed to clear invalid tokens');
    throw error;
  }
}

module.exports = {
  addInvalidToken,
  addInvalidTokens,
  getInvalidTokens,
  getInvalidTokenCount,
  isTokenInvalid,
  clearInvalidTokens
};
