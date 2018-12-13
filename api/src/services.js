const sqornPg = require('sqorn-pg');

// Postgres

// sqorn

exports.rsq = sqornPg({
  connection: { connectionString: process.env.PG_READ_URL }
});

exports.wsq = sqornPg({
  connection: { connectionString: process.env.PG_WRITE_URL }
});
