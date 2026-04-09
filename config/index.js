module.exports = {
  port: process.env.PORT || 3000,
  host: '127.0.0.1',
  log: {
    level: process.env.LOG_LEVEL || 'info',
    file: './logs/app.log'
  },
  db: {
    path: './data/pusher.db'
  }
};
