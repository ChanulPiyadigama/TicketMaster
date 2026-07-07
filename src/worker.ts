import { Worker, Job } from 'bullmq';
import { Pool } from 'pg';
import dotenv from 'dotenv';

dotenv.config({ path: '.env.local' });

const pgPool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// Enclose our initialization script within a function scope block to prevent global leaks
function startWorkerEngine() {
  console.log('👷 Background Settlement Worker initialized and listening for jobs...');

  const ticketWorker = new Worker(
    'order-settlement-queue',
    async (job: Job) => {
      const { eventId, userId } = job.data;
      console.log(`📦 Worker picked up Job #${job.id}: Processing Order for User [${userId}], Event [${eventId}]`);

      const client = await pgPool.connect();

      try {
        await client.query('BEGIN');

        const eventQuery = 'SELECT price FROM events WHERE id = $1';
        const eventResult = await client.query(eventQuery, [eventId]);
        
        if (eventResult.rows.length === 0) {
          throw new Error(`Event ID ${eventId} does not exist in relational tables.`);
        }
        const ticketPrice = eventResult.rows[0].price;

        const orderQuery = `
          INSERT INTO orders (user_id, total_price, status) 
          VALUES ($1, $2, 'PAID') 
          RETURNING id
        `;
        const orderResult = await client.query(orderQuery, [userId, ticketPrice]);
        const orderId = orderResult.rows[0].id;

        const claimTicketQuery = `
          UPDATE tickets 
          SET order_id = $1, status = 'SOLD' 
          WHERE id = (
            SELECT id FROM tickets 
            WHERE event_id = $2 AND status = 'AVAILABLE' 
            LIMIT 1 
            FOR UPDATE SKIP LOCKED
          )
        `;
        const ticketResult = await client.query(claimTicketQuery, [orderId, eventId]);

        if (ticketResult.rowCount === 0) {
          throw new Error(`PostgreSQL structural mismatch: No physical available tickets left for Event ${eventId}.`);
        }

        await client.query('COMMIT');
        console.log(`✅ Success! Job #${job.id} completed. Order ID [${orderId}] successfully written to PostgreSQL.`);

      } catch (transactionError) {
        await client.query('ROLLBACK');
        console.error(`❌ Job #${job.id} failed. Rolling back disk modifications:`, transactionError);
        throw transactionError;
      } finally {
        client.release();
      }
    },
    { 
      connection: {
        host: 'localhost',
        port: 6379
      }
    }
  );

  ticketWorker.on('failed', (job, err) => {
    console.error(`⚠️ Job ${job?.id} completely failed processing:`, err.message);
  });
}

// Fire up the scoped engine
startWorkerEngine();

// Force explicit module encapsulation boundary
export {};
