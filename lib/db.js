const config = require('./config')
const r = require('rethinkdbdash')(config.rethinkdb)
const log = require('./logger')(config.app.name)

log.info('Version:', config.app.version)

(async function initDb () {
  log.info('Initializing database...')

  try {
    const dbName = 'maalfrid'
    const dbUser = 'maalfrid'
    const tables = [{aggregate: 'warcId'}, 'entities', 'seeds', {filter: 'seedId'}]
    const indexes = {
      aggregate: ['seedId', 'endTime']
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
      }
    }
    log.info('Database initialized')

    process.exit(0)
  } catch (e) {
    process.exit(1)
  }
})()
