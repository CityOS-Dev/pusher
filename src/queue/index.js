// src/queue/index.js
const { Queue, Worker } = require('bullmq');
const { processJob } = require('./processor');
const queueConfig = require('../../config/queue.config');
const pino = require('pino');

const logger = pino({ level: 'info' });

const queues = {};
let worker;

function createQueues() {
  const connection = {
    host: queueConfig.redis.host,
    port: queueConfig.redis.port,
    password: queueConfig.redis.password
  };

  // Create high priority queue
  queues.high = new Queue(`${queueConfig.redis.prefix}${queueConfig.queues.high.name}`, {
    connection,
    defaultJobOptions: {
      attempts: queueConfig.retryAttempts,
      backoff: {
        type: 'exponential',
        delay: queueConfig.retryDelay
      }
    }
  });

  // Create standard priority queue
  queues.standard = new Queue(`${queueConfig.redis.prefix}${queueConfig.queues.standard.name}`, {
    connection,
    defaultJobOptions: {
      attempts: queueConfig.retryAttempts,
      backoff: {
        type: 'exponential',
        delay: queueConfig.retryDelay
      }
    }
  });

  logger.info('Queues created');
}

async function startWorker() {
  const connection = {
    host: queueConfig.redis.host,
    port: queueConfig.redis.port,
    password: queueConfig.redis.password
  };

  worker = new Worker(
    [`${queueConfig.redis.prefix}${queueConfig.queues.high.name}`,
     `${queueConfig.redis.prefix}${queueConfig.queues.standard.name}`],
    processJob,
    {
      connection,
      concurrency: 10
    }
  );

  worker.on('completed', (job) => {
    logger.info({ jobId: job.id }, 'Job completed');
  });

  worker.on('failed', (job, error) => {
    logger.error({ jobId: job?.id, error }, 'Job failed');
  });

  logger.info('Worker started');
}

async function addJob(priority, data) {
  const queue = priority === 'high' ? queues.high : queues.standard;
  const chunkSize = queueConfig.chunkSize;

  // Split recipients into chunks
  const chunks = [];
  for (let i = 0; i < data.recipients.length; i += chunkSize) {
    chunks.push({
      ...data,
      recipients: data.recipients.slice(i, i + chunkSize)
    });
  }

  const jobIds = [];

  for (const chunk of chunks) {
    const job = await queue.add('send', chunk, {
      priority: priority === 'high' ? 10 : 5
    });
    jobIds.push(job.id);
  }

  return jobIds;
}

async function getQueueStats(queueName) {
  const queue = queues[queueName];
  if (!queue) return null;

  return {
    waiting: await queue.getWaitingCount(),
    active: await queue.getActiveCount(),
    completed: await queue.getCompletedCount(),
    failed: await queue.getFailedCount(),
    delayed: await queue.getDelayedCount()
  };
}

async function closeQueues() {
  await queues.high.close();
  await queues.standard.close();
  if (worker) {
    await worker.close();
  }
  logger.info('Queues and worker closed');
}

module.exports = {
  createQueues,
  startWorker,
  addJob,
  getQueueStats,
  closeQueues,
  queues
};
