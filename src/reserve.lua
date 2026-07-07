-- KEYS[1]: The inventory tracker hash (e.g., event:1:inventory)
-- ARGV[1]: The number of tickets requested (typically "1")
-- ARGV[2]: The unique user ID string
-- ARGV[3]: The TTL hold duration in seconds (typically "600")

local inventoryKey = KEYS[1]
local ticketsRequested = tonumber(ARGV[1])
local userId = ARGV[2]
local holdTTL = tonumber(ARGV[3])

-- 1. Grab the current active count from memory cache
local availableTickets = tonumber(redis.call("hget", inventoryKey, "available_tickets"))

-- 2. Guard Clause: Check if the cache exists or if tickets are empty
if not availableTickets then
    return -1 -- Code -1: Cache engine un-synchronized
elseif availableTickets < ticketsRequested then
    return 0  -- Code 0: Insufficient inventory / Sold out
end

-- 3. Atomic Mutation: Safely decrement the available cache count
redis.call("hincrby", inventoryKey, "available_tickets", -ticketsRequested)

-- 4. Track Hold Windows: Create temporary lease keys to protect seats
-- We create a simple unique tracker key tracking the reservation seat assignment context
local holdKey = "hold:event:" .. string.match(inventoryKey, "%d+") .. ":user:" .. userId
redis.call("set", holdKey, ticketsRequested, "EX", holdTTL)

return 1 -- Code 1: Success! Reservation hold secured cleanly
