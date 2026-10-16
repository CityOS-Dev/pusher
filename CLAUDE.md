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

`retryable` classifies the cause. Queue processing checkpoints every server-indexed
recipient in SQLite before retry. Explicit partial transient failures retry only
when delivery.expires_at_ms was supplied and remains valid; accepted, permanent,
expired and ambiguous entries are excluded. Provider throws/timeouts remain unknown
and are not blindly resent. Queue processing makes one SDK call per attempt; the
standalone dashboard FCM helper retains its existing wrapper policy. APN provider resources are shut down in finally; a cleanup error
is logged safely and does not turn a successful send into a new delivery retry.

Run local isolated verification with `node --test --test-concurrency=2`.
The tests load the real provider/processor code and replace external SDK/credential,
DB and logging boundaries; they never connect to Redis, Firebase or APNs.


## Delivery reliability contract (2026-10-16)

POST /api/send retains HTTP202 `{message,jobIds}`. Optional top-level `delivery`
contains `version:1`, `idempotency_key`, optional `expires_at_ms` and
`operational_generation`, and optional `source:{kind:"sip",issuer,session_id}`. Keys are app-scoped, request-content conflicts return409,
and chunks use stable hashed Bull IDs with atomic addBulk. A missing deadline does
not acquire an invented call lifetime or a new transient retry policy. The15-second
provider transport budget bounds worker occupancy; a timeout is unknown delivery.
Source guards, where supplied, are separate from diagnostic call_id and never infer
control authority from it.

Delivery request fingerprints and per-job recipient tombstones live in the existing
SQLite database with synchronous=FULL and intentionally remain until an explicit
retention/archival policy is chosen. Do not purge them with queue cleanup: a later
replay could resend accepted or ambiguous entries. Monitor database disk growth.
These checkpoints cannot make provider acceptance and local commit atomic; crash
after submission therefore reports unknown and prevents an automatic resend.

Statistics/token-log/diagnostic errors cannot cause provider resend. Diagnostics
use a10000-row SQLite outbox, maximum7-day retention, capped2-request drain and
immutable source event IDs/timestamps. Overflow/expiry are counted in /health.
Install monitoring support for loopback pusher v2 receipts before this emitter;
missing/invalid ACK retains the event. No tokens, passwords or payloads enter the
diagnostic outbox; operational invalid-token pruning remains separate.

Provider recipient namespace comes only from explicit payload user_id_namespace
(sb/tenant); absent support stays unknown. Provider acceptance is not phone receipt.
Configured1000/300-per-minute limits count jobs; workers use concurrency2 each.
Bull retention ages are seconds (3600/86400), with existing count limits.

Native SIP source delivery uses the installed fixed read-only status RPC. Only
server-supplied source identity is placed in the job. Broker connection settings
come from the fixed protected `.config/sip-broker.json` (host, port, username,
password, vhost), generated during rollout from the owning server configuration;
never put them in jobs/logs or accept a caller URL. Status responses require the
matching nonce/correlation, issuer, complete marker and stable-cli-status-v1 proof.
At most2 source queries run concurrently with a1500ms operation budget. Ended
sources become skipped; unknown sources defer2seconds before any recipient claim,
using Bull's manual delayed state without spending provider failure attempts.
Existing jobs without source identity skip only the SIP RPC gate; they still receive durable recipient checkpoints. An explicit expired
deadline suppresses delivery immediately, even when the source query is unavailable.


## Review hardening (2026-10-16)

The delivery store shares the existing `db/index.js` connection, which already
uses WAL. Keep `synchronous=FULL`. `claimMany` and `finishMany` commit a recipient
batch atomically before and after provider I/O; prepared statements are reused.
No transaction spans an SDK request. Per-recipient SQL remains intentional; this
reduces checkpoint commits, not every diagnostic/statistics write in a job.

Runtime checkpoint initialization/read/write failure defers the Bull job for two
seconds with its lock token and `DelayedError`, without consuming provider retry
attempts or bypassing durable claims. A failed post-send commit leaves in-flight
entries ambiguous; recovery never sends them again. `/health` returns503 when
checkpoint writes are known unavailable or the storage read itself fails. A
cached handle or terminal-tombstone read cannot claim write recovery. Startup
failure of the mandatory SQLite database remains fail-closed under PM2; do not
replace it with an empty database or an in-memory fallback.

APNs/FCM public send APIs do not accept a general AbortSignal. The15-second
budget therefore reports unknown; it does not claim cancellation. At most four
SDK operations can remain outstanding (two workers per queue). A timed-out SDK
keeps its capacity lease until the real promise settles, including provider
cleanup. Saturation defers before recipient claim. `/health.provider_operations`
exposes pending/limit/saturated; saturation alone can also mean ordinary busy
workers, not a proven provider outage.

The fixed broker file must be a regular non-symlink, private0600 file in a
private0700 directory, owned by root or the process UID. Deployment generates
`.config/sip-broker.json` atomically from the installed core's **root config.php**:
`rabbitmq.host/port/user/password` become `host/port/username/password`, vhost `/`.
The staged `.config` directory may link to that protected live directory; the
credential file itself may not be a symlink. Only a validated successful read is
cached for the process lifetime. Restart the pusher to use rotated credentials;
failed reads are not cached. Each status query opens its own bounded connection,
so a later delayed attempt reconnects after an outage. Never put broker URLs or
credentials into a job, log, shell argument or source commit.

Healthy diagnostic backlog schedules its next two-event batch on the next event
loop turn, without a fixed250ms gap. Failed ACKs retain per-row backoff and the
regular scheduler. All sibling operations settle before releasing the drain
guard, even if SQLite disposition fails. Shutdown flush is additionally bounded;
unacknowledged rows remain durable. No manual replay or deletion is required.

Diagnostic machine causes are allowlisted. Unknown code/reason text is hashed;
operational token-store write failures log only token fingerprints. Full tokens
remain in the operational invalid-token table because pruning needs them, never
in these new error logs. Existing tombstone retention remains unchanged: choose
an explicit safe archival policy before deleting accepted/unknown checkpoints.


## Second review clarifications (2026-10-16)

All queued jobs, including legacy jobs with **no delivery object**, receive
per-job/per-recipient checkpoints unconditionally. This is an intentional safety
improvement; legacy HTTP202 and payload shapes remain compatible. Optional
`delivery.idempotency_key` adds cross-request enqueue deduplication, optional
source adds the SIP proof gate, and optional deadline allows bounded transient
retries/expiry. Without a key, separate HTTP requests are separate jobs: their
individual checkpoints do not deduplicate those different requests. Existing
legacy jobs without source/deadline do not acquire an invented lifetime or a new
transient retry policy.

Worker recipient diagnostics now use `reportMany` → `enqueueEvents`: one capacity
count, one transaction and reused INSERT per outcome batch (up to500 recipients).
This applies to accepted/failure, unknown, expired and ended paths. Individual
`report` remains a synchronous durable one-event adapter for other callers; no
in-memory deferred capture was introduced. The outbox limit stays10000, partial
capacity admits the first fitting facts and counts every dropped fact. A failed
batch insert rolls back the whole capture; secondary diagnostics failure cannot
replay a completed provider submission.

Checkpoint health is process-local for this single-process service. A failure
has an episode number, monotonic failure revision and timestamp. Recovery needs
a full claim/result write cycle started after the latest failure; a lone claim,
read-only terminal replay, cached handle or older in-flight success cannot clear
it. Health exposes this evidence under delivery.checkpoint. Actual intermittent
write failures may still change ready/degraded; there is no artificial smoothing
that hides a new failure. This reports worker checkpoint evidence, not proof that
every SQL path is healthy or a distributed database availability oracle.

Enqueue failures log safe machine causes and an error hash alongside the fixed
code. They never log raw exception text, broker config, recipient tokens or body.
Tests resolve source paths from `__dirname` and may run from another working
directory when passed their explicit file paths.
