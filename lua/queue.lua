-- KEYS[3] queueKey, activeQueueKey, queueOptionsKey
-- ARGV[6] count, retry time, expire time, time accuracy, current timestamp, scanActive

local options = redis.call('hmget', KEYS[3], 'count', 'retry', 'expire', 'accuracy')
options = {tonumber(options[1] or ARGV[1]), tonumber(options[2] or ARGV[2]), tonumber(options[3] or ARGV[3]), tonumber(options[4] or ARGV[4])}

local jobs = {} -- {job, timing, retryCount, job, timing, retryCount, ...}
local res = {jobs, options[2], 0} -- {jobs, retry, hasMore}
local current = tonumber(ARGV[5])
local dueTime = options[4] + current

if ARGV[6] == '1' then
  local activeQueue = redis.call('hgetall', KEYS[2])
  for i = 1, #activeQueue, 2 do
    local times = {}
    for time in string.gmatch(activeQueue[i + 1], '%d+') do
      times[#times + 1] = tonumber(time)
    end

    if (current - times[2]) >= options[3] then
      redis.call('hdel', KEYS[2], activeQueue[i])
    elseif (current - times[#times]) >= options[2] then
      jobs[#jobs + 1] = activeQueue[i]
      jobs[#jobs + 1] = times[1]
      jobs[#jobs + 1] = #times - 1
      redis.call('hset', KEYS[2], activeQueue[i], activeQueue[i + 1] .. ':' .. ARGV[5])
    end
  end
end

local queue = redis.call('zrangebyscore', KEYS[1], 0, dueTime, 'WITHSCORES', 'LIMIT', 0, options[1])
if #queue == options[1] * 2 then res[3] = 1 end

for i = 1, #queue, 2 do
  jobs[#jobs + 1] = queue[i]
  jobs[#jobs + 1] = queue[i + 1]
  jobs[#jobs + 1] = 0
  redis.call('hset', KEYS[2], queue[i], queue[i + 1] .. ':' .. ARGV[5])
  redis.call('zrem', KEYS[1], queue[i])
end

return res
