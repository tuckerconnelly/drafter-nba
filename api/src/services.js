const pg = require('pg').native;
const bluebird = require('bluebird');
const sqornPg = require('sqorn-pg');

/*** Postgres ***/

const pgRead = new pg.Pool({
  connectionString: process.env.PG_READ_URL,
  Promise: bluebird
});

pgRead.on('error', err => {
  console.error('Unexpected error on idle client', err);
  process.exit(-1);
});

exports.pgRead = pgRead;

const pgWrite = new pg.Pool({
  connectionString: process.env.PG_WRITE_URL,
  Promise: bluebird
});

pgWrite.on('error', err => {
  console.error('Unexpected error on idle client', err);
  process.exit(-1);
});

exports.pgWrite = pgWrite;

/*** sqorn ***/

// Read
exports.rsq = sqornPg({ pg, pool: pgRead });

// Write
exports.wsq = sqornPg({ pg, pool: pgWrite });
