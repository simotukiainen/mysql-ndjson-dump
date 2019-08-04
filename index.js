'use strict'

const {promisify} = require('util')
const mysql = require('mysql2')
const ndjson = require('ndjson')
const multipipe = require('multipipe')
const debug = require('debug')('mysql-json-dump')

void require('yargs')
.command('$0 sql', 'Executes an SQL and dumps result as ndjson', yargs => {
  yargs.positional('sql', {
    type: 'string',
    describe: 'The SQL to execute'
  })
}, dumpSql)
.option('database', {
  type: 'string',
  describe: 'MySQL connection settings as uri e.g mysql://user:password@localhost/database',
})
.help('help')
.strict()
.argv

async function dumpSql({database, sql}) {
  debug('Opening connection %s', database)
  const conn = mysql.createConnection(database)
  try {
    await new Promise((resolve, reject) => {
      debug('query: %s', sql)
      multipipe(
        conn.query(sql).stream(),
        ndjson.stringify(),
        err => { if (!err) resolve(); else reject(err)}
      ).pipe(process.stdout)
      .on('error', reject)
    })
    debug('done')
  } finally {
    await new Promise(resolve => conn.end(() => resolve()))
  }
}
