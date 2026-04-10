// config/queue.config.js
module.exports = {
  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: process.env.REDIS_PORT || 6379,
    password: process.env.REDIS_PASSWORD || undefined,
    prefix: 'push_service',
  },
  queues: {
    high: {
      name: 'push_high',
      rateLimit: {
        limit: 1000,
        interval: 60000,  // 1 minute
      },
    },
    standard: {
      name: 'push_standard',
      rateLimit: {
        limit: 300,
        interval: 60000,
      },
    },
  },
  chunkSize: 500,
  retryAttempts: 3,
  retryDelay: 5000,
  // Job retention settings (remove jobs after completion/failure)
  // age: milliseconds since job completion, count: max jobs to keep
  removeOnComplete: { age: 3600000, count: 1000 },  // 1 hour, keep last 1000
  removeOnFail: { age: 86400000, count: 100 },       // 24 hours, keep last 100
}
