import fs from 'fs';
import path from 'path';
import { Pool } from 'pg';
import dotenv from 'dotenv';

// Load config file
dotenv.config({ path: '.env.local' });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function runMigration() {
  try {
    console.log('⏳ Reading schema.sql file...');
    const schemaPath = path.join(__dirname, 'schema.sql');
    const sql = fs.readFileSync(schemaPath, 'utf8');

    console.log('⏳ Connecting to PostgreSQL inside Docker and executing migration queries...');
    await pool.query(sql);

    console.log('✅ Success! Database tables created successfully.');
  } catch (error) {
    console.error('❌ Migration failed:', error);
  } finally {
    await pool.end(); // Safely shut down connection pool link
  }
}

runMigration();
