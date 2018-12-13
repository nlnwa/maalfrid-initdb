const config = require('./config')
const r = require('rethinkdbdash')(config.rethinkdb)
const log = require('./logger')(config.app.name)

;(async function initDb () {
  log.info('Version:', config.app.version)
  log.info('Initializing database...')

  try {
    const dbName = config.database.name
    const dbUser = config.database.user
    const dbPassword = config.database.password || false

    const tables = [
      { aggregate: 'warcId' },
      'entities',
      'seeds',
      'filter',
      { statistics: 'executionId' },
      'system'
    ]
    const indexes = {
      aggregate: ['seedId', 'executionId'],
      statistics: ['jobExecutionId', 'seedId', 'entityId', 'endTime'],
      filter: ['seedId']
    }
    const tableCreateOptions = { durability: 'soft' }

    const dbExists = await r.dbList().contains(dbName).run()
    if (!dbExists) {
      log.info('Creating database:', dbName)
      await r.dbCreate(dbName).run()
    } else {
      log.info('Database', dbName, 'already exists')
    }

    for (let i = 0, n = tables.length; i < n; i++) {
      const entry = tables[i]
      let tableName
      let primaryKey

      if (typeof entry === 'object') {
        tableName = Object.entries(entry)[0][0]
        primaryKey = Object.entries(entry)[0][1]
      } else {
        tableName = tables[i]
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
    }
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

    log.info('Database initialized')

    process.exit(0)
  } catch (e) {
    log.error(e)
    process.exit(1)
  }
})()
