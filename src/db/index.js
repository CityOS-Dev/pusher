// src/db/index.js
const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');
const config = require('../../config');
const pino = require('pino');

const logger = pino({
  level: config.log.level,
  transport: { target: 'pino-pretty' }
});

try {
  // Ensure data directory exists
  const dbDir = path.dirname(config.db.path);
  if (!fs.existsSync(dbDir)) {
    fs.mkdirSync(dbDir, { recursive: true });
  }

  // Connect to database
  const db = new Database(config.db.path);
  db.pragma('journal_mode = WAL'); // Better concurrency

  // Initialize schema
  const schema = fs.readFileSync(path.join(__dirname, 'schema.sql'), 'utf8');
  db.exec(schema);

  logger.info('Database connected and schema initialized');

  module.exports = db;
} catch (error) {
  logger.error('Failed to initialize database', error);
  process.exit(1);
}
