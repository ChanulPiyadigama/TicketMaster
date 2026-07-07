import express, { Request, Response } from 'express';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import { Pool } from 'pg';
import { createClient } from 'redis';
import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import { initializeExpirationListener } from './subscriber';

dotenv.config({ path: '.env.local' });

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

const pgPool = new Pool({ connectionString: process.env.DATABASE_URL });
const redisClient = createClient({ url: process.env.REDIS_URL });
redisClient.on('error', (err) => console.error('❌ Redis Error:', err));

// Initialize the BullMQ Publisher Queue link over a persistent connection
const settlementQueue = new Queue('order-settlement-queue', { 
  connection: {
    host: 'localhost',
    port: 6379
  }
});


let luaReserveScript = '';

function loadLuaScripts() {
  try {
    const scriptPath = path.join(__dirname, 'reserve.lua');
    luaReserveScript = fs.readFileSync(scriptPath, 'utf8');
    console.log('✅ Atomic Lua scripts parsed successfully.');
  } catch (err) {
    console.error('❌ Critical failure loading Lua scripts:', err);
    process.exit(1);
  }
}

async function syncInventoryToRedis(eventId: string) {
  const redisKey = `event:${eventId}:inventory`;
  const exists = await redisClient.exists(redisKey);
  if (exists) return;

  const countResult = await pgPool.query("SELECT COUNT(*) FROM tickets WHERE event_id = $1 AND status = 'AVAILABLE'", [eventId]);
const availableCount = countResult.rows[0].count;

  await redisClient.hSet(redisKey, {
    'available_tickets': availableCount,
    'total_capacity': '10000'
  });
}

// 1. Read API Route
app.get('/events/:id', async (req: Request, res: Response): Promise<void> => {
  const eventId = String(req.params.id);
  const redisKey = `event:${eventId}:inventory`;
  try {
    const cacheData = await redisClient.hGetAll(redisKey);
    if (cacheData && cacheData.available_tickets) {
      res.json({ id: parseInt(eventId, 10), tickets_left: parseInt(cacheData.available_tickets, 10) });
      return;
    }
    res.status(404).json({ error: 'Cache cold or event non-existent' });
  } catch (error) {
    res.status(500).json({ error: 'Internal system anomaly' });
  }
});

// 2. Atomic Reservation Route
app.post('/tickets/reserve', async (req: Request, res: Response): Promise<void> => {
  const { eventId, userId, quantity } = req.body;
  if (!eventId || !userId || !quantity || typeof quantity !== 'number') {
    res.status(400).json({ error: 'Invalid metadata fields in payload' });
    return;
  }

  const inventoryKey = `event:${String(eventId)}:inventory`;
  const holdTTLSeconds = '600';

  try {
    const scriptResult = await redisClient.eval(luaReserveScript, {
      keys: [inventoryKey],
      arguments: [String(quantity), String(userId), holdTTLSeconds],
    });

    const status = Number(scriptResult);
    if (status === 1) {
      res.json({ message: 'Success! Your ticket is held for 10 minutes.', status: 'HELD' });
      return;
    } else if (status === 0) {
      res.status(409).json({ error: 'Transaction rejected. This event is sold out!' });
      return;
    } else {
      res.status(500).json({ error: 'Cache engine is un-synchronized.' });
      return;
    }
  } catch (error) {
    res.status(500).json({ error: 'Internal server processing error' });
  }
});

// 3. Settlement Checkout Route linked to Background Queue
app.post('/tickets/checkout', async (req: Request, res: Response): Promise<void> => {
  const { eventId, userId } = req.body;
  if (!eventId || !userId) {
    res.status(400).json({ error: 'Missing eventId or userId in request body' });
    return;
  }

  const holdKey = `hold:event:${String(eventId)}:user:${String(userId)}`;

  try {
    const holdExists = await redisClient.exists(holdKey);
    if (!holdExists) {
      res.status(410).json({ error: 'Transaction expired! 10-minute hold window timed out.' });
      return;
    }

    // Simulate external bank clearance sequence
    const paymentSuccess = true; 
    if (!paymentSuccess) {
      res.status(402).json({ error: 'Payment authorization rejected.' });
      return;
    }

    // Delete the cache timer hold safely
    await redisClient.del(holdKey);

    // FAANG Architecture Point: Offload heavy database insert queries to background thread queue
    await settlementQueue.add('processOrderJob', {
      eventId: parseInt(eventId, 10),
      userId: parseInt(userId, 10),
    });

    res.json({
      message: 'Success! Your payment was processed and your background order is queued.',
      status: 'QUEUED',
    });

  } catch (error) {
    console.error('❌ Checkout anomaly:', error);
    res.status(500).json({ error: 'Internal checkout server processing error' });
  }
});

async function startServer() {
  try {
    await pgPool.connect();
    await redisClient.connect();
    loadLuaScripts();
    await syncInventoryToRedis('1');
    await initializeExpirationListener(process.env.REDIS_URL || 'redis://localhost:6379');

    app.listen(PORT, () => {
      console.log(`🚀 API control center live at http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error('❌ Startup execution halted:', error);
    process.exit(1);
  }
}

startServer();
