const config = require('./config')
const log = require('./logger')(config.app.name)
const { r } = require('rethinkdb-ts')

/** @type {MasterPool} */
let connectionPool

async function connect () {
  connectionPool = await r.connectPool(config.rethinkdb)
}

async function disconnect () {
  await connectionPool.drain({ noreplyWait: true })
}

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
            if (typeof index === 'string') {
              log.info('Creating index', index, 'on table', tableName, 'in database', dbName)
              await r.db(dbName).table(tableName).indexCreate(index).run()
            } else {
              log.info('Creating index', index[0], 'on table', tableName, 'in database', dbName)
              await r.db(dbName).table(tableName).indexCreate(...index).run()
            }
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
  log.info('Inserting initial data')
  const inserts = Object.entries(data).map(([tableName, tableData]) =>
    r.db(dbName).table(tableName).insert(tableData, { conflict: (id, oldDoc, newDoc) => newDoc.merge(oldDoc) }).run())
  return Promise.all(inserts)
}

async function initUser ({ dbUser, dbName, dbPassword, grants }) {
  log.info('Creating user and setting permissions')

  const userExists = await r.db('rethinkdb').table('users').get(dbUser).run()
  if (!userExists) {
    log.info('Creating user: ', dbUser)
    await r.db('rethinkdb').table('users').insert({ id: dbUser, password: dbPassword }).run()
  }

  for (const grant of grants) {
    const logText = ['Granting user', dbUser, grant.options]
    let query = r.db(grant.database)

    if (grant.table) {
      query = query.table(grant.table)
      logText.push('on table', grant.table)
    }
    query = query.grant(dbUser, grant.options)
    logText.push('on database', grant.database)

    log.info(...logText)
    try {
      await query.run()
    } catch (error) {
      log.error(error)
    }
  }
}

;(async function initialize (options) {
  const { dbName, dbUser, dbPassword, tables, indexes, data, tableCreateOptions, grants } = options
  log.info('App:', config.app.version, 'Node:', process.version)
  log.info('Initializing database...')

  try {
    await connect()
    await initDatabase({ dbName })
    await initTables({ dbName, tables, indexes, tableCreateOptions })
    await initData({ dbName, data })
    await initUser({ dbName, dbUser, dbPassword, grants })
    await disconnect()
  } catch (e) {
    log.error(e)
    process.exit(1)
  }

  log.info('Database initialized successfully!')
  process.exit(0)
})({
  dbName: config.database.name,
  dbUser: config.database.user,
  dbPassword: config.database.password || false,
  grants: [
    {
      database: config.database.name,
      options: { read: true, write: true }
    },
    {
      database: 'veidemann',
      options: { read: true }
    },
    {
      database: 'veidemann',
      table: 'extracted_text',
      options: { read: true, write: true }
    }
  ],
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
    filter: ['seedId'],
    seeds: [
      ['configRefs', (row) => ['crawlEntity', row('seed')('entityRef')('id')]]
    ]
  },
  data: {
    filter: [
      {
        id: 'global',
        filters: []
      }
    ],
    system: [
      {
        id: 'inProgress',
        languageDetection: null,
        aggregation: null,
        statistics: null,
        sync: null
      }
    ]
  },
  tableCreateOptions: { durability: 'soft' }
})
