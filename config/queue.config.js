// config/queue.config.js
module.exports = {
  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: process.env.REDIS_PORT || 6379,
    password: process.env.REDIS_PASSWORD || undefined,
    prefix: 'push_service:'
  },
  queues: {
    high: {
      name: 'push_high',
      rateLimit: {
        limit: 1000,
        interval: 60000  // 1 minute
      }
    },
    standard: {
      name: 'push_standard',
      rateLimit: {
        limit: 300,
        interval: 60000
      }
    }
  },
  chunkSize: 500,
  retryAttempts: 3,
  retryDelay: 5000
};
