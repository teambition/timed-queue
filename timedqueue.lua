-- KEYS[3] queueKey, activeQueueKey, queueOptionsKey
-- ARGV[4] time accuracy, ack timeout, expire time, current timestamp

local options = redis.call('hmget', KEYS[3], 'accuracy', 'timeout', 'expire')
options = {tonumber(options[1] or ARGV[1]), tonumber(options[2] or ARGV[2]), tonumber(options[3] or ARGV[3])}

local res = {}
local activeQueue = redis.call('hgetall', KEYS[2])

for i = 1, #activeQueue, 2 do
  local split = activeQueue[i + 1]:find(':')
  local activeTime = ARGV[4] - tonumber(activeQueue[i + 1]:sub(split + 1))

  if activeTime >= options[3] then
    redis.call('hdel', KEYS[2], activeQueue[i])
  elseif activeTime >= options[2] then
    res[#res + 1] = activeQueue[i]
    res[#res + 1] = activeQueue[i + 1]:sub(1, split - 1)
  end
end

local queue = redis.call('zrangebyscore', KEYS[1], 0, ARGV[4] + options[1], 'WITHSCORES')

for i = 1, #queue, 2 do
  res[#res + 1] = queue[i]
  res[#res + 1] = queue[i + 1]
  redis.call('hset', KEYS[2], queue[i], queue[i + 1] .. ':' .. ARGV[4])
  redis.call('zrem', KEYS[1], queue[i])
end

return res
