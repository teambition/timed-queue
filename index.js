'use strict'

// **Github:** https://github.com/teambition/timed-queue
//
// **License:** MIT

var fs = require('fs')
var util = require('util')
var thunks = require('thunks')
var redis = require('thunk-redis')
var EventEmitter = require('events').EventEmitter
var luaScript = fs.readFileSync(__dirname + '/lua/queue.lua', {encoding: 'utf8'})

var thunk = thunks()
module.exports = TimedQueue

function TimedQueue (options) {
  options = options || {}

  this.prefix = options.prefix || 'TIMEDQ'
  this.queuesKey = this.prefix + ':QUEUES'
  this.interval = options.interval || 1000 * 120
  this.retry = options.retry || this.interval / 2
  this.expire = options.expire || this.interval * 5
  this.accuracy = options.accuracy || this.interval / 5

  this.redis = null
  this.timer = null
  this.scanTime = 0
  this.scanning = false
  this.scanQueue = null
  this.delay = this.interval
  this.queues = Object.create(null)
  this.scanQueue = null

  var ctx = this
  this.thunk = thunks(function (error) {
    ctx.emit('error', error)
  })
  EventEmitter.call(this)
}
util.inherits(TimedQueue, EventEmitter)

TimedQueue.prototype.connect = function () {
  var ctx = this
  if (this.redis) return this
  this.redis = redis.createClient.apply(null, arguments)
    .on('connect', function () {
      ctx.emit('connect')
    })
    .on('error', function (error) {
      ctx.emit('error', error)
    })
    .on('close', function () {
      ctx.emit('close')
    })

  this.scanQueue = this.thunk.persist.call(this, this.redis.script('load', luaScript))
  // auto scan jobs
  thunk.delay.call(this, Math.random() * this.interval)(function () {
    if (!this.timer && !this.scanning) this.scan()
  })
  return this
}

TimedQueue.prototype.scan = function () {
  if (this.scanning) return this

  var ctx = this
  var scanStart
  this.scanning = true
  this.scanTime = scanStart = Date.now()

  this.redis.smembers(this.queuesKey)(function (err, queues) {
    if (err) throw err
    ctx.emit('scanStart', queues.length)
    return thunk.seq(queues.map(function (queue) {
      return ctx.queue(queue).scan()
    }))
  })(function (err, scoreQueues) {
    if (err) ctx.emit('error', err)
    else {
      ctx.emit('scanEnd', scoreQueues.length, Date.now() - scanStart)

      var count = 0
      var score = scoreQueues.reduce(function (prev, scores) {
        return prev + scores.reduce(function (p, s) {
          count++
          return p + s
        }, 0)
      }, 0)
      ctx.regulateFreq(score / count)
    }

    if (!ctx.timer && ctx.scanning) {
      ctx.scanning = false
      ctx.scan()
    } else ctx.scanning = false
  })

  clearTimeout(this.timer)
  this.timer = setTimeout(function () {
    ctx.timer = null
    if (!ctx.scanning) ctx.scan()
  }, this.delay)

  return this
}

TimedQueue.prototype.stop = function () {
  clearTimeout(this.timer)
  this.scanning = false
  this.timer = null
  return this
}

TimedQueue.prototype.queue = function (queueName, options) {
  validateString(queueName)
  if (!this.queues[queueName]) this.queues[queueName] = new Queue(this, queueName, options)
  else if (options) this.queues[queueName].init(options)
  return this.queues[queueName]
}

TimedQueue.prototype.regulateFreq = function (factor) {
  if (factor < -0.1) this.delay = Math.max(this.delay * (1 + factor), this.interval / 10)
  else if (factor > 0.1) this.delay = Math.min(this.delay * (1 + factor), this.interval * 5)
}

function Queue (timedQueue, queueName, options) {
  this.root = timedQueue
  this.name = queueName
  this.queueKey = '{' + timedQueue.prefix + ':' + queueName + '}'
  this.activeQueueKey = this.queueKey + ':-'
  this.queueOptionsKey = this.queueKey + ':O'
  this.init(options)
  EventEmitter.call(this)
}
util.inherits(Queue, EventEmitter)

Queue.prototype.init = function (options) {
  var root = this.root
  root.thunk(root.redis.sadd(root.queuesKey, this.name))()

  if (!options) return this
  root.thunk(root.redis.hmset(this.queueOptionsKey, {
    retry: options.retry || root.retry,
    expire: options.expire || root.expire,
    accuracy: options.accuracy || root.accuracy
  }))()
  return this
}

Queue.prototype.addjob = function (job, timing) {
  var args = slice(arguments)
  return thunk.call(this, function (done) {
    var current = Date.now()

    for (var i = 0, l = args.length || 2; i < l; i += 2) {
      validateString(args[i])
      timing = Math.floor(args[i + 1])
      if (!timing || timing <= current) throw new Error(String(args[i + 1]) + ' is invalid time')
    }

    args.unshift(this.queueKey)
    this.root.redis.zadd(args)(done)
  })
}

Queue.prototype.show = function (job) {
  return thunk.call(this, function (done) {
    var ctx = this
    validateString(job)

    ctx.root.redis.zscore(ctx.queueKey, job)(function (err, timing) {
      if (err) throw err
      if (timing) return new Job(ctx.name, job, timing, 0)

      return this.hget(ctx.activeQueueKey, job)(function (err, times) {
        if (err) throw err
        if (!times) return null
        times = times.split(':')
        return new Job(ctx.name, job, times[0], times[1])
      })
    })(done)
  })
}

Queue.prototype.deljob = function (job) {
  var args = slice(arguments)
  return thunk.call(this, function (done) {
    var ctx = this
    for (var i = 0, l = args.length || 1; i < l; i++) validateString(args[i])

    args.unshift(ctx.queueKey)
    ctx.root.redis.zrem(args)(function (err, count) {
      if (err) throw err
      args[0] = ctx.activeQueueKey
      return this.hdel(args)(function (err, _count) {
        if (err) throw err
        return count + _count
      })
    })(done)
  })
}

Queue.prototype.getjob = function () {
  return thunk.call(this, function (done) {
    var ctx = this
    var root = this.root
    var jobs = []
    var timestamp = Date.now()

    thunk(root.scanQueue)(function (_, luaSHA) {
      return root.redis.evalsha(luaSHA, 3,
        ctx.queueKey, ctx.activeQueueKey, ctx.queueOptionsKey,
        root.retry, root.expire, root.accuracy, timestamp
      )
    })(function (err, res) {
      if (err) throw err
      for (var i = 0, l = res.length - 1; i < l; i += 2) {
        jobs.push(new Job(ctx.name, res[i], res[i + 1], timestamp))
      }
      return jobs
    })(done)
  })
}

Queue.prototype.ackjob = function (job) {
  var args = slice(arguments)
  return thunk.call(this, function (done) {
    for (var i = 0, l = args.length || 1; i < l; i++) validateString(args[i])
    args.unshift(this.queueKey)
    return this.root.redis.hdel(args)(done)
  })
}

Queue.prototype.scan = function () {
  return thunk.call(this, function (done) {
    var ctx = this
    this.getjob()(function (err, jobs) {
      if (err) throw err
      return thunk.all(jobs.map(function (job) {
        return thunk.delay()(function () {
          var score = job.timing === job.active ? 0 : (job.timing > job.active ? 1 : -1)
          ctx.emit('job', job)
          return score
        })
      }))
    })(done)
  })
}

Queue.prototype.len = function () {
  return thunk.call(this, function (done) {
    return this.root.redis.zcard(this.queueKey)(done)
  })
}

Queue.prototype.showActive = function () {
  return thunk.call(this, function (done) {
    var ctx = this
    return this.root.redis.hgetall(this.activeQueueKey)(function (err, res) {
      if (err) throw err
      return Object.keys(res).map(function (job) {
        var times = res[job].split(':')
        return new Job(ctx.name, job, times[0], times[1])
      })
    })
  })
}

function Job (queue, job, timing, active) {
  this.queue = queue
  this.job = job
  this.timing = +timing
  this.active = +active
}

function slice (args, start) {
  start = start || 0
  if (start >= args.length) return []
  var len = args.length
  var ret = Array(len - start)
  while (len-- > start) ret[len - start] = args[len]
  return ret
}

function validateString (str) {
  if (!str || typeof str !== 'string') throw new TypeError(String(str) + ' is invalid string')
}
