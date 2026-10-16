const { describeFailure } = require('../providers/provider_errors')
// src/queue/index.js
const { Queue, Worker } = require('bullmq')
const { processJob } = require('./processor')
const { getStore, fingerprint, jobId } = require('../db/delivery')
const crypto = require('node:crypto')
const workerState = new Map()
const queueConfig = require('../../config/queue.config')
const pino = require('pino')

const logger = pino({ level: 'info' })

const queues = {}
let workers = []

function createQueues() {
  const connection = {
    host: queueConfig.redis.host,
    port: queueConfig.redis.port,
    password: queueConfig.redis.password,
  }

  // Create high priority queue
  queues.high = new Queue(queueConfig.queues.high.name, {
    connection,
    prefix: queueConfig.redis.prefix,
    defaultJobOptions: {
      attempts: queueConfig.retryAttempts,
      backoff: {
        type: 'exponential',
        delay: queueConfig.retryDelay,
      },
      removeOnComplete: queueConfig.removeOnComplete,
      removeOnFail: queueConfig.removeOnFail,
    },
  })

  // Create standard priority queue
  queues.standard = new Queue(queueConfig.queues.standard.name, {
    connection,
    prefix: queueConfig.redis.prefix,
    defaultJobOptions: {
      attempts: queueConfig.retryAttempts,
      backoff: {
        type: 'exponential',
        delay: queueConfig.retryDelay,
      },
      removeOnComplete: queueConfig.removeOnComplete,
      removeOnFail: queueConfig.removeOnFail,
    },
  })

  logger.info({
    highQueue: queues.high.name,
    standardQueue: queues.standard.name,
    prefix: queueConfig.redis.prefix,
  }, 'Queues created')
}

async function startWorker() {
  const connection = {
    host: queueConfig.redis.host,
    port: queueConfig.redis.port,
    password: queueConfig.redis.password,
  }

  const queueNames = [
    queueConfig.queues.high.name,
    queueConfig.queues.standard.name,
  ]

  logger.info({ queueNames }, 'Starting workers for queues')

  // Create a worker for each queue
  for (const queueName of queueNames) {
    const worker = new Worker(
      queueName,
      async (job,token) => {
        try { return await processJob(job,token) }
        finally { const state=workerState.get(queueName);if(state)delete state.active_jobs[job.id] }
      },
      {
        connection,
        concurrency: 2,
        limiter: { max: Object.values(queueConfig.queues).find(q => q.name === queueName).rateLimit.limit, duration: Object.values(queueConfig.queues).find(q => q.name === queueName).rateLimit.interval },
        prefix: queueConfig.redis.prefix,
      },
    )

    workerState.set(queueName, { ready:false, last_completed_at:null, last_failed_at:null, last_error_at:null, active_jobs:{} })
    worker.on('ready', () => { workerState.get(queueName).ready = true })
    worker.on('closed', () => { workerState.get(queueName).ready = false })
    worker.on('completed', (job) => {
      workerState.get(queueName).last_completed_at = Date.now()
      delete workerState.get(queueName).active_jobs[job.id]
      logger.info({ jobId: job.id, queue: queueName }, 'Job completed')
    })

    worker.on('failed', (job, error) => {
      workerState.get(queueName).last_failed_at = Date.now()
      if (job) delete workerState.get(queueName).active_jobs[job.id]
      logger.error({ jobId: job?.id, ...describeFailure(error), queue: queueName }, 'Job failed')
    })

    worker.on('error', (error) => {
      workerState.get(queueName).last_error_at = Date.now()
      logger.error({ ...describeFailure(error), queue: queueName }, 'Worker error')
    })

    worker.on('stalled', (id) => {
      delete workerState.get(queueName).active_jobs[id]
      workerState.get(queueName).last_stalled_at = Date.now()
    })
    worker.on('waiting', (job) => {
      logger.info({ jobId: job.id, queue: queueName }, 'Job waiting')
    })

    worker.on('active', (job) => {
      workerState.get(queueName).active_jobs[job.id] = Date.now()
      logger.info({ jobId: job.id, queue: queueName }, 'Job active')
    })

    workers.push(worker)
  }

  logger.info('Workers started')
}

async function addJob(priority, data) {
  const queue = priority === 'high' ? queues.high : queues.standard
  logger.info({ priority, queueName: queue.name }, 'Adding job to queue')
  const chunkSize = queueConfig.chunkSize

  // Split recipients into chunks
  const chunks = []
  for (let i = 0; i < data.recipients.length; i += chunkSize) {
    chunks.push({
      ...data,
      recipients: data.recipients.slice(i, i + chunkSize),
    })
  }

  const key = data.delivery ? fingerprint([data.app_id, data.delivery.idempotency_key]) : crypto.randomUUID()
  if (data.delivery) getStore().reserve(key, fingerprint({ priority, ...data }))
  const jobs = await queue.addBulk(chunks.map((chunk, index) => ({ name:'send', data:chunk, opts:{ jobId:jobId(key,index), priority:priority === 'high' ? 10:5 } })))
  return jobs.map(job => String(job.id))
}

async function getQueueStats(queueName) {
  const { sendWithin } = require('./provider_deadline')
  return sendWithin(() => readQueueStats(queueName),1800).catch(() => ({error:'queue_unavailable',worker:{ready:false}}))
}
async function readQueueStats(queueName) {
  const queue = queues[queueName]
  if (!queue) return null

  return {
    waiting: await queue.getWaitingCount(),
    active: await queue.getActiveCount(),
    completed: await queue.getCompletedCount(),
    failed: await queue.getFailedCount(),
    delayed: await queue.getDelayedCount(),
    worker: workerState.get(queue.name) || { ready:false },
    policy: { limiter: queueConfig.queues[queueName].rateLimit, concurrency:2 },
    oldest_waiting_age_ms: await oldestAge(queue),
  }
}

async function oldestAge(queue) {
  const jobs = await queue.getJobs(['waiting','prioritized'], 0, 0, true)
  return jobs.length ? Math.max(0,Date.now()-jobs[0].timestamp):null
}
async function closeQueues() {
  for (const worker of workers) {
    await worker.close()
  }
  await queues.high.close()
  await queues.standard.close()
  logger.info('Queues and workers closed')
}

module.exports = {
  createQueues,
  startWorker,
  addJob,
  getQueueStats,
  closeQueues,
  queues,
}
