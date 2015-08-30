-- KEYS[3] queueKey, activeQueueKey, queueOptionsKey
-- ARGV[4] retry time, expire time, time accuracy, current timestamp

local options = redis.call('hmget', KEYS[3], 'retry', 'expire', 'accuracy')
options = {tonumber(options[1] or ARGV[1]), tonumber(options[2] or ARGV[2]), tonumber(options[3] or ARGV[3])}

local res = {}
local dueTime = options[3] + tonumber(ARGV[4])
local activeQueue = redis.call('hgetall', KEYS[2])

for i = 1, #activeQueue, 2 do
  local times = {}
  for time in string.gmatch(activeQueue[i + 1], '%d+') do
    times[#times + 1] = tonumber(time)
  end

  if times[2] >= options[3] then
    redis.call('hdel', KEYS[2], activeQueue[i])
  elseif times[#times] >= options[2] then
    res[#res + 1] = activeQueue[i]
    res[#res + 1] = times[1]
    redis.call('hset', KEYS[2], activeQueue[i], activeQueue[i + 1] .. ':' .. ARGV[4])
  end
end

local queue = redis.call('zrangebyscore', KEYS[1], 0, dueTime, 'WITHSCORES')

for i = 1, #queue, 2 do
  res[#res + 1] = queue[i]
  res[#res + 1] = queue[i + 1]
  redis.call('hset', KEYS[2], queue[i], queue[i + 1] .. ':' .. ARGV[4])
  redis.call('zrem', KEYS[1], queue[i])
end

return res
