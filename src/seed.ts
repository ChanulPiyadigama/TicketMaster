import { Pool } from 'pg';
import dotenv from 'dotenv';

dotenv.config({ path: '.env.local' });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function seedDatabase() {
  const client = await pool.connect();
  try {
    console.log('⏳ Starting database seeding...');
    
    // Begin a safe SQL transaction block
    await client.query('BEGIN');

    // 1. Clean out any old data from previous tests
    await client.query('TRUNCATE TABLE tickets, orders, events, users RESTART IDENTITY CASCADE');

    // 2. Insert a Mock User
    console.log('⏳ Creating test user...');
    const userResult = await client.query(`
      INSERT INTO users (email, name) 
      VALUES ('tester@faang.com', 'Alex Engineer') 
      RETURNING id
    `);
    const userId = userResult.rows[0].id;

    // 3. Insert a Mock Concert Event (e.g., 10,000 capacity)
    console.log('⏳ Creating mega concert event...');
    const eventResult = await client.query(`
      INSERT INTO events (title, date, total_capacity, price) 
      VALUES ('The Mega Concert 2026', '2026-12-31 20:00:00', 10000, 99.99) 
      RETURNING id
    `);
    const eventId = eventResult.rows[0].id;

    // 4. Generate 10,000 Tickets efficiently in bulk rows
    console.log('⏳ Generating 10,000 available seats (This might take 1-2 seconds)...');
    
    // We use a clean SQL generation script to loop 10,000 times natively inside Postgres
    await client.query(`
      INSERT INTO tickets (event_id, seat_number, status)
      SELECT 
        $1, 
        'Seat-' || i, 
        'AVAILABLE'
      FROM generate_series(1, 10000) AS i;
    `, [eventId]);

    // Commit all changes to the database disk
    await client.query('COMMIT');
    console.log('✅ Database seeded successfully!');
    console.log(`ℹ️ Created User ID: ${userId}`);
    console.log(`ℹ️ Created Event ID: ${eventId} with 10,000 available tickets.`);

  } catch (error) {
    await client.query('ROLLBACK'); // Undo everything if an error happens
    console.error('❌ Seeding failed:', error);
  } finally {
    client.release();
    await pool.end();
  }
}

seedDatabase();
