# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a Node.js 18+ push notification microservice for iOS (APN) and Android (FCM). It uses a monolithic inline worker architecture - a single process runs both the Fastify API server and the BullMQ worker.

**Tech Stack:**
- Fastify (web framework)
- BullMQ + Redis (job queue with high/standard priority queues)
- SQLite + better-sqlite3 (stats, error logs, invalid tokens)
- node-apn (iOS push notifications)
- firebase-admin (Android push notifications)
- Pino (structured logging)
- PM2 (process management)

## Common Commands

```bash
# Development
npm start                    # Start server (node src/server.js)
npm run dev                  # Development mode with --watch flag

# Production (PM2)
npm run pm2:start            # Start service with PM2
npm run pm2:stop             # Stop PM2 app
npm run pm2:restart          # Restart PM2 app
npm run pm2:logs            # View PM2 logs
npm run pm2:delete           # Delete PM2 app

# Quick health check
curl http://127.0.0.1:3000/health
```

## Architecture

### Layer Structure

The codebase is organized into distinct layers:

0. **Config Layer** (`config/`):
   - `index.js` - Consolidates all config (apps, queue, db, server, logging)
   - `apps.config.js` - App definitions with credentials
   - `queue.config.js` - BullMQ queue settings and rate limits

1. **API Layer** (`src/api/`):
   - `routes.js` - POST /send endpoint, GET /health endpoint
   - `schema.js` - JSON Schema validation (app_id must be enum from apps.config.js)

2. **Queue Layer** (`src/queue/`):
   - `index.js` - BullMQ queue setup, worker initialization, job routing
   - `processor.js` - Job processing logic that routes to providers and updates database

3. **Provider Layer** (`src/providers/`):
   - `apn.js` - iOS/APN provider (node-apn)
   - `fcm.js` - Android/FCM provider (firebase-admin)
   - Both follow same interface: `send(recipients, request, appConfig)`

4. **Database Layer** (`src/db/`):
   - `index.js` - SQLite connection with WAL mode
   - `schema.sql` - Three tables: stats, error_log, invalid_tokens
   - `stats.js` - Hourly stats CRUD with upsert
   - `errors.js` - Error log CRUD
   - `tokens.js` - Invalid tokens CRUD with batch support

5. **Dashboard Layer** (`src/dashboard/`):
   - `routes.js` - Dashboard routes (localhost only middleware)
   - `ui.js` - Embedded HTML/JS dashboard UI

### Main Entry Point

`src/server.js` starts everything in this order:
1. Create BullMQ queues
2. Start worker
3. Build Fastify server
4. Register API and dashboard routes
5. Register Bull Board adapter
6. Listen on configured host:port

### Data Flow

```
POST /api/send
  → Validate app_id, priority, recipients (JSON Schema)
  → Split recipients into chunks of 500
  → Add jobs to BullMQ queue (push_high or push_standard)
  → Return HTTP 202 with job IDs

[BullMQ Worker]
  → ProcessJob gets job data
  → Route to APN or FCM provider based on app type
  → Provider sends push notifications
  → Record stats (hourly success/fail counts)
  → Log errors to error_log
  → Log invalid tokens to invalid_tokens
```

## Configuration

### Config Consolidation

All configuration is exported from `config/index.js`, which the main server imports. Individual config files are:
- `apps.config.js` - App definitions
- `queue.config.js` - Queue settings

### App Configuration (`config/apps.config.js`)

Each app has:
- `id` - Unique identifier used in API requests
- `type` - "ios" or "android"
- `cert` - Path to iOS certificate (.pem file)
- `passphrase` - Certificate passphrase (optional)
- `production` - Use production APN endpoint (default: true)
- `serviceAccount` - Path to Firebase service account key (.json)

**Important**: Certificate/key files must be placed in `certs/` directory (gitignored).

### Queue Configuration (`config/queue.config.js`)

- Two queues: `push_high` (1000/min), `push_standard` (300/min)
- Chunk size: 500 recipients per job
- Retry: 3 attempts with 5 second delay
- Redis prefix: `push_service:`
- Job retention: completed jobs removed after 1 hour (keep last 1000), failed jobs after 24 hours (keep last 100)

### Environment Variables

- `PORT` - Server port (default: 3000)
- `LOG_LEVEL` - Pino log level (default: info)
- `REDIS_HOST` - Redis host (default: localhost)
- `REDIS_PORT` - Redis port (default: 6379)
- `REDIS_PASSWORD` - Redis password (optional)

## Invalid Token Detection

Both providers detect and log invalid tokens to the `invalid_tokens` table:

- **iOS**: `BadDeviceToken`, `Unregistered` response reasons
- **Android**: `messaging/invalid-registration-token`, `messaging/registration-token-not-registered` error codes

## Dashboard

Dashboard is **localhost-only** (`127.0.0.1` or `::1`) for security:

- Main UI: `http://127.0.0.1:3000/dashboard/`
- Bull Board queue management: `http://127.0.0.1:3000/dashboard/queues`
- Queue stats API: `GET /dashboard/api/queue-stats`
- Stats API: `GET /dashboard/api/stats` (last 120 hours)
- Invalid tokens: `GET/DELETE /dashboard/api/invalid-tokens`
- Test send: `POST /dashboard/api/test-send` (bypasses queue)

## Database Schema

**stats** - Hourly aggregation per app
- `hour` (TEXT) - "YYYY-MM-DD HH:00"
- `app_id` (TEXT)
- `sent_success`, `sent_fail` (INTEGER)
- UNIQUE constraint on (hour, app_id)

**error_log** - All push errors
- `timestamp` (TEXT)
- `app_id`, `error_type`, `message` (TEXT)

**invalid_tokens** - Invalid device tokens
- `external_id`, `token`, `app_id`, `type`, `reason` (TEXT)
- `detected_at` (TEXT)
- Indexed by token and app_id

## Important Notes

- `.tool-versions` file specifies Node.js 18 for asdf/rtx users
- Server listens only on `127.0.0.1` (localhost)
- Use PM2 for production deployment (see pm2.config.js)
- Database uses WAL mode for better concurrency
- All SQL queries use prepared statements (better-sqlite3)
- Certificates and keys in `certs/` directory are gitignored
- Logs stored in `logs/` directory (gitignored)
- SQLite database stored in `data/pusher.db` (gitignored)


## Call-provider diagnostic outcomes (2026-10-13)

APNs/FCM failures include bounded `error_code`, `error_type`, `http_status`,
`category`, `retryable` and `error_hash`. Transport errors preserve SDK codes such
as ETIMEDOUT; absent recipient ids do not hide APNs rejects. Per-recipient tracker
outcomes match token entries, rather than grouping failures solely by an optional
recipient id.

Diagnostic error_log/tracker records include exact call_id when supplied, job_id,
attempt_index, SHA256 token_hash and token_tail. SDK message/stack and full tokens
are not copied into new diagnostic rows/logs. The separate operational invalid_tokens
store retains the token needed for pruning. Historical rows are not purged.

`retryable` classifies the cause; it does not enable an extra retry loop. The existing
queue/SDK retry schedule is unchanged until a separate call deadline/idempotency
contract is ready. APN provider resources are shut down in finally; a cleanup error
is logged safely and does not turn a successful send into a new delivery retry.

Run local isolated verification with `node --test --test-concurrency=2`.
The tests load the real provider/processor code and replace external SDK/credential,
DB and logging boundaries; they never connect to Redis, Firebase or APNs.
