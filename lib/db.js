const config = require('./config').rethinkdb;
const r = require('rethinkdbdash')(config);
const log = require('./logger');


(async function initDb() {
  log.info('Initializing database...');

  try {
    const dbName = 'report';
    const dbUser = 'maalfrid';
    const tableNames = ['maalfrid_cache', 'maalfrid_report'];
    const tableCreateOptions = {durability: 'soft'};

    const dbExists = await r.dbList().contains(dbName).run();
    if (!dbExists) {
      log.info('Creating database:', dbName);
      await r.dbCreate(dbName).run();

      log.info('Granting', dbUser, 'read permission on database:', dbName);
      await r.db(dbName).grant(dbUser, {read: true}).run();
    } else {
      log.info('Database', dbName, 'already exists');
    }

    for (let i = 0; i < tableNames.length; i++) {
      const tableName = tableNames[i];

      const tableExists = await r.db(dbName).tableList().contains(tableName).run();
      if (!tableExists) {
        log.info('Creating table:', tableName);
        await r.db(dbName).tableCreate(tableName, tableCreateOptions).run();

        log.info('Granting', dbUser, 'read and write permission on table:', tableName);
        await r.db(dbName).table(tableName).grant(dbUser, {read: true, write: true}).run();
      } else {
        log.info('Table', tableName, 'already exists');
      }
    }

    log.info('Database initialized');

    process.exit(0);
  } catch (e) {
    process.exit(1);
  }
})();
