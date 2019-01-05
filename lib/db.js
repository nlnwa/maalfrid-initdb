const config = require('./config')
const r = require('rethinkdb-js')(config.rethinkdb)
const log = require('./logger')(config.app.name)

async function initDatabase ({ dbName }) {
  const dbExists = await r.dbList().contains(dbName).run()
  if (!dbExists) {
    log.info('Creating database:', dbName)
    await r.dbCreate(dbName).run()
  } else {
    log.info('Database', dbName, 'already exists')
  }
}

async function initTables ({ dbName, tables, indexes, tableCreateOptions }) {
  return Promise.all(
    tables.map(async table => {
      let tableName = table
      let primaryKey
      if (typeof table === 'object') {
        [tableName, primaryKey] = Object.entries(table)[0]
      }
      const tableExists = await r.db(dbName).tableList().contains(tableName).run()
      if (!tableExists) {
        let options = { ...tableCreateOptions }
        if (primaryKey !== 'undefined') {
          Object.assign(options, { primaryKey })
        }
        log.info('Creating table:', tableName, JSON.stringify(options))
        await r.db(dbName).tableCreate(tableName, options).run()

        // create secondary indexes
        if (indexes.hasOwnProperty(tableName)) {
          for (let j = 0, n = indexes[tableName].length; j < n; j++) {
            await r.db(dbName).table(tableName).indexCreate(indexes[tableName][j])
          }
          await r.db(dbName).table(tableName).indexWait()
        }
      } else {
        // update secondary indexes
        log.info('Table', tableName, 'already exists')
        const createIndexes = []
        const dropIndexes = []
        const info = await r.db(dbName).table(tableName).info().run()
        if (indexes.hasOwnProperty(tableName)) {
          indexes[tableName].forEach(index => {
            if (!info['indexes'].some(_ => _ === index)) {
              createIndexes.push(index)
            }
          })
          info['indexes'].forEach(index => {
            if (!indexes[tableName].some(_ => _ === index)) {
              dropIndexes.push(index)
            }
          })
        } else if (info['indexes'].length > 0) {
          info['indexes'].forEach(index => dropIndexes.push(index))
        }
        await Promise.all([
          createIndexes.map(async index => {
            log.info('Creating index', index, 'on table', tableName, 'in database', dbName)
            await r.db(dbName).table(tableName).indexCreate(index).run()
          }),
          dropIndexes.map(async index => {
            log.info('Dropping index', index, 'on table', tableName, 'in database', dbName)
            await r.db(dbName).table(tableName).indexDrop(index).run()
          })])

        if (createIndexes.length > 0 || dropIndexes.length > 0) {
          log.info('Waiting for indexes to be ready on table', tableName, 'in database', dbName)
          await r.db(dbName).table(tableName).indexWait().run()
        }
      }
    })
  )
}

async function initData ({ data, dbName }) {
  const inserts = Object.entries(data).map(([tableName, tableData]) =>
    r.db(dbName).table(tableName).insert(tableData, { conflict: (id, oldDoc, newDoc) => oldDoc }).run())
  return Promise.all(inserts)
}

async function initUser ({ dbUser, dbName, dbPassword }) {
  const userExists = await r.db('rethinkdb').table('users').get(dbUser)
  if (!userExists) {
    log.info('Creating user: ', dbUser)
    await r.db('rethinkdb').table('users').insert({ id: dbUser, password: dbPassword })

    log.info('Granting user', dbUser, 'read/write permission on database:', dbName)
    await r.db(dbName).grant(dbUser, { read: true, write: true }).run()

    log.info('Granting user', dbUser, 'read permissions on database: veidemann')
    await r.db('veidemann').grant(dbUser, { read: true })

    log.info('Granting user', dbUser, 'read/write permissions on: veidemann.extracted_text table')
    await r.db('veidemann').table('extracted_text').grant(dbUser, { read: true, write: true })
  }
}

;(async function initialize (options) {
  const { dbName, dbUser, dbPassword, tables, indexes, data, tableCreateOptions } = options
  log.info('Version:', config.app.version)
  log.info('Initializing database...')

  try {
    await initDatabase({ dbName })
    await initTables({ dbName, tables, indexes, tableCreateOptions })
    await initData({ dbName, data })
    await initUser({ dbName, dbUser, dbPassword })
  } catch (e) {
    log.error(e)
    process.exit(1)
  }

  log.info('Database initialized successfully')
  process.exit(0)
})({
  dbName: config.database.name,
  dbUser: config.database.user,
  dbPassword: config.database.password || false,
  tables: [
    { aggregate: 'warcId' },
    'entities',
    'seeds',
    'filter',
    { statistics: 'executionId' },
    'system'
  ],
  indexes: {
    aggregate: ['seedId', 'executionId'],
    statistics: ['jobExecutionId', 'seedId', 'entityId', 'endTime'],
    filter: ['seedId']
  },
  data: {
    filter: [
      {
        id: 'global',
        filters: []
      }
    ]
  },
  tableCreateOptions: { durability: 'soft' }
})
