// dump.js
import { Pool } from 'pg';
import createCsvWriter from 'csv-writer';
import dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT ? Number(process.env.DB_PORT) : 5432,
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASS || 'postgres',
  database: process.env.DB_NAME || 'github_data',
});

async function dump() {
  const res = await pool.query('SELECT repo_db_id, repo_node_id, name, owner, stars, url, updated_at FROM repositories ORDER BY stars DESC NULLS LAST');
  const rows = res.rows;
  const csvWriter = createCsvWriter.createObjectCsvWriter({
    path: 'repos_dump.csv',
    header: [
      {id:'repo_db_id', title:'repo_db_id'},
      {id:'repo_node_id', title:'repo_node_id'},
      {id:'name', title:'name'},
      {id:'owner', title:'owner'},
      {id:'stars', title:'stars'},
      {id:'url', title:'url'},
      {id:'updated_at', title:'updated_at'}
    ]
  });
  await csvWriter.writeRecords(rows);
  console.log('Wrote repos_dump.csv rows:', rows.length);
  await pool.end();
}

if (import.meta.url === `file://${process.argv[1]}`) {
  dump().catch(e => { console.error(e); process.exit(1); });
}
