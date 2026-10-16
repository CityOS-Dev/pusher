const { describeFailure } = require('../providers/provider_errors')
// src/db/stats.js
const db = require('./index');
const logger = require('pino')({ level: 'info' });

function upsertStats(hour, appId, successDelta = 0, failDelta = 0) {
  const stmt = db.prepare(`
    INSERT INTO stats (hour, app_id, sent_success, sent_fail)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(hour, app_id) DO UPDATE SET
      sent_success = sent_success + ?,
      sent_fail = sent_fail + ?
  `);

  try {
    stmt.run(hour, appId, successDelta, failDelta, successDelta, failDelta);
  } catch (error) {
    logger.error({ ...describeFailure(error), appId }, 'Failed to upsert stats');
    throw error;
  }
}

function getStats(hoursBack = 120) {
  const stmt = db.prepare(`
    SELECT hour, app_id, sent_success, sent_fail
    FROM stats
    WHERE hour >= datetime('now', ?)
    ORDER BY hour DESC
  `);

  const offsetHours = `-${hoursBack} hours`;
  const rows = stmt.all(offsetHours);

  // Group by hour and app_id for easy charting
  const result = {
    hours: [...new Set(rows.map(r => r.hour))].sort(),
    apps: {}
  };

  rows.forEach(row => {
    if (!result.apps[row.app_id]) {
      result.apps[row.app_id] = { success: [], fail: [] };
    }
    result.apps[row.app_id].success.push(row.sent_success);
    result.apps[row.app_id].fail.push(row.sent_fail);
  });

  return rows;
}

function getStatsByApp(appId, hoursBack = 120) {
  const stmt = db.prepare(`
    SELECT hour, sent_success, sent_fail
    FROM stats
    WHERE app_id = ? AND hour >= datetime('now', ?)
    ORDER BY hour DESC
  `);

  const offsetHours = `-${hoursBack} hours`;
  return stmt.all(appId, offsetHours);
}

module.exports = {
  upsertStats,
  getStats,
  getStatsByApp
};
