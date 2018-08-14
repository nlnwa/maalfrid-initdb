const config = require('./config')
const r = require('rethinkdbdash')(config.rethinkdb)
const log = require('./logger')(config.name)

;(async function initDb () {
  log.info('Version:', config.app.version)
  log.info('Initializing database...')

  try {
    const dbName = 'maalfrid'
    const dbUser = 'maalfrid'
    const tables = [{aggregate: 'warcId'}, 'entities', 'seeds', {filter: 'seedId'}]
    const indexes = {
      aggregate: ['seedId', 'executionId']
    }
    const tableCreateOptions = {durability: 'soft'}

    const dbExists = await r.dbList().contains(dbName).run()
    if (!dbExists) {
      log.info('Creating database:', dbName)
      await r.dbCreate(dbName).run()

      log.info('Granting', dbUser, 'read/write permission on database:', dbName)
      await r.db(dbName).grant(dbUser, {read: true, write: true}).run()
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
        let options = {...tableCreateOptions}
        if (primaryKey !== 'undefined') {
          Object.assign(options, {primaryKey})
        }
        log.info('Creating table:', tableName, JSON.stringify(options))
        await r.db(dbName).tableCreate(tableName, options).run()

        // create secondary indexes
        if (indexes.hasOwnProperty(tableName)) {
          for (let j = 0, n = indexes[tableName].length; j < n; j++) {
            await r.db(dbName).table(tableName).indexCreate(indexes[tableName][j])
          }
          await r.db(dbName).table(tableName).indexWait(indexes[tableName])
        }
      } else {
        log.info('Table', tableName, 'already exists')
        const createIndexes = []
        const dropIndexes = []
        const info = await r.db(dbName).table(tableName).info().run()
        if (indexes.hasOwnProperty(tableName)) {
          indexes[tableName].forEach(indexName => {
            const present = info['indexes'].find(_ => _ === indexName)
            if (present === undefined) {
              createIndexes.push(indexName)
            }
          })
          info['indexes'].forEach(indexName => {
            const present = indexes[tableName].find(_ => _ === indexName)
            if (present === undefined) {
              dropIndexes.push(indexName)
            }
          })
        } else if (info['indexes'].length > 0) {
          info['indexes'].forEach(indexName => {
            dropIndexes.push(indexName)
          })
        }
        await Promise.all([
          createIndexes.map(async indexName => {
            log.info('Creating index', indexName, 'on table', tableName, 'in database', dbName)
            await r.db(dbName).table(tableName).indexCreate(indexName).run()
          }),
          dropIndexes.map(async indexName => {
            log.info('Dropping index', indexName, 'on table', tableName, 'in database', dbName)
            await r.db(dbName).table(tableName).indexDrop(indexName).run()
          })])

        if (createIndexes.length > 0 || dropIndexes.length > 0) {
          log.info('Waiting for indexes to be ready on table', tableName, 'in database', dbName)
          await r.db(dbName).table(tableName).indexWait().run()
        }
      }
    }
    log.info('Database initialized')

    process.exit(0)
  } catch (e) {
    log.error(e)
    process.exit(1)
  }
})()
