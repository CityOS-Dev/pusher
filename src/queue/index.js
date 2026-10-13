const { describeFailure } = require('../providers/provider_errors')
// src/queue/index.js
const { Queue, Worker } = require('bullmq')
const { processJob } = require('./processor')
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
      processJob,
      {
        connection,
        concurrency: 5,
        prefix: queueConfig.redis.prefix,
      },
    )

    worker.on('completed', (job) => {
      logger.info({ jobId: job.id, queue: queueName }, 'Job completed')
    })

    worker.on('failed', (job, error) => {
      logger.error({ jobId: job?.id, ...describeFailure(error), queue: queueName }, 'Job failed')
    })

    worker.on('error', (error) => {
      logger.error({ ...describeFailure(error), queue: queueName }, 'Worker error')
    })

    worker.on('waiting', (job) => {
      logger.info({ jobId: job.id, queue: queueName }, 'Job waiting')
    })

    worker.on('active', (job) => {
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

  const jobIds = []

  for (const chunk of chunks) {
    try {
      const job = await queue.add('send', chunk, {
        priority: priority === 'high' ? 10 : 5,
      })
      logger.info({ jobId: job.id }, 'Job added to queue')
      jobIds.push(job.id)
    } catch (error) {
      logger.error({ error }, 'Failed to add job to queue')
      throw error
    }
  }

  logger.info({ jobIds, count: jobIds.length }, 'Jobs added successfully')
  return jobIds
}

async function getQueueStats(queueName) {
  const queue = queues[queueName]
  if (!queue) return null

  return {
    waiting: await queue.getWaitingCount(),
    active: await queue.getActiveCount(),
    completed: await queue.getCompletedCount(),
    failed: await queue.getFailedCount(),
    delayed: await queue.getDelayedCount(),
  }
}

async function closeQueues() {
  await queues.high.close()
  await queues.standard.close()
  for (const worker of workers) {
    await worker.close()
  }
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
