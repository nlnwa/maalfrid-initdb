const name = require('../package.json').name
const version = require('../package.json').version
const log = require('./logger')(name)

module.exports = {
  app: { name, version },
  rethinkdb: {
    servers: [
      {
        host: process.env.DB_HOST || 'localhost',
        port: parseInt(process.env.DB_PORT) || 28015
      }
    ],
    user: process.env.DB_USER || 'admin',
    password: process.env.DB_PASSWORD || '',
    log: log.info,
    silent: true
  },
  database: {
    name: process.env.CREATE_DB_NAME || 'maalfrid',
    user: process.env.CREATE_DB_USER || 'maalfrid',
    password: process.env.CREATE_DB_PASSWORD || ''
  }
}
