import { readFile } from 'node:fs/promises';

import { Pool } from 'pg';

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error('DATABASE_URL is required to run migrations');
}

const migrationSql = await readFile(
  new URL('../migrations/001_atomic_capacity.sql', import.meta.url),
  'utf8',
);

const pool = new Pool({
  connectionString: databaseUrl,
  application_name: 'issue3-migration',
});

try {
  await pool.query(migrationSql);
} finally {
  await pool.end();
}
