-- src/db/schema.sql

-- Stats table - hourly statistics
CREATE TABLE IF NOT EXISTS stats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  hour TEXT NOT NULL,           -- YYYY-MM-DD HH:00
  app_id TEXT NOT NULL,
  sent_success INTEGER DEFAULT 0,
  sent_fail INTEGER DEFAULT 0,
  UNIQUE(hour, app_id)
);

CREATE INDEX IF NOT EXISTS idx_stats_hour ON stats(hour);
CREATE INDEX IF NOT EXISTS idx_stats_app_id ON stats(app_id);

-- Error log table
CREATE TABLE IF NOT EXISTS error_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  timestamp TEXT DEFAULT (datetime('now')),
  app_id TEXT NOT NULL,
  error_type TEXT NOT NULL,
  message TEXT NOT NULL
);

-- Invalid tokens table
CREATE TABLE IF NOT EXISTS invalid_tokens (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  external_id TEXT,
  token TEXT NOT NULL,
  app_id TEXT NOT NULL,
  type TEXT NOT NULL,           -- ios or android
  reason TEXT NOT NULL,
  detected_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_invalid_tokens_token ON invalid_tokens(token);
CREATE INDEX IF NOT EXISTS idx_invalid_tokens_app_id ON invalid_tokens(app_id);
