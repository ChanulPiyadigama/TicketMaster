import { createClient } from 'redis';

/**
 * FAANG Pattern: Event-Driven Cache Recovery
 * Listens for expired 10-minute hold timers and restores inventory.
 */
export async function initializeExpirationListener(redisUrl: string) {
  // Create a dedicated Redis client specifically for Pub/Sub subscriptions
  const subscriber = createClient({ url: redisUrl });
  
  // Create a separate standard client to run commands when an event fires
  const commander = createClient({ url: redisUrl });

  subscriber.on('error', (err) => console.error('❌ Subscriber Client Error:', err));
  commander.on('error', (err) => console.error('❌ Commander Client Error:', err));

  try {
    await subscriber.connect();
    await commander.connect();

    // The internal Redis topic path for key expiration events on Database 0
    const expirationTopic = '__keyevent@0__:expired';

    console.log(`📡 Subscribing to Redis Keyspace Notification Topic: ${expirationTopic}`);

    await subscriber.subscribe(expirationTopic, async (message: string) => {
      console.log(`🔔 Received Expiration Event from Redis: "${message}"`);

      // Pattern match: check if the expired key looks like "hold:event:1:user:99"
      if (message.startsWith('hold:event:')) {
        // Extract the event ID using string splitting
        const parts = message.split(':');
        const eventId = parts[2]; // Becomes "1"
        const userId = parts[4];  // Becomes "99"

        console.log(`⚠️ User ${userId} cart expired! Restoring 1 ticket back to Event ${eventId}...`);

        const inventoryKey = `event:${eventId}:inventory`;

        // Increment the inventory count back up by 1 atomically in memory
        await commander.hIncrBy(inventoryKey, 'available_tickets', 1);

        const currentCount = await commander.hGet(inventoryKey, 'available_tickets');
        console.log(`✅ Inventory successfully recovered! Event ${eventId} available tickets: ${currentCount}`);
      }
    });

  } catch (error) {
    console.error('❌ Failed to initialize Keyspace Notification Listener:', error);
  }
}
