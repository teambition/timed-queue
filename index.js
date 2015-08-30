// **Github:** https://github.com/teambition/timed-queue
//
// **License:** MIT

'use strict'

var fs = require('fs')
var util = require('util')
var thunks = require('thunks')
var redis = require('thunk-redis')
var EventEmitter = require('events').EventEmitter
var luaScript = fs.readFileSync(__dirname + '/lua/queue.lua', {encoding: 'utf8'})

var thunk = thunks()

module.exports = TimedQueue

function TimedQueue (options) {
  this.options = options || {}
  this.prefix = this.options.prefix || 'TIMEDQ'
  this.queuesKey = this.prefix + ':QUEUES'
  this.retry = this.options.retry || 1000 * 100
  this.expire = this.options.expire || 1000 * 300
  this.accuracy = this.options.accuracy || 1000 * 30
  this.interval = this.options.interval || 1000 * 150
  this.queues = Object.create(null)
  this.redis = null
  this.timer = null
  this.scanTime = 0
  this.scanning = false
  this.queueScript = null

  var ctx = this
  this.thunk = thunks(function (error) {
    ctx.emit('error', error)
  })
  EventEmitter.call(this)
}
util.inherits(TimedQueue, EventEmitter)

TimedQueue.prototype.connect = function () {
  var ctx = this
  if (!this.redis) {
    this.redis = redis.createClient.apply(null, arguments)
    this.redis
      .on('connect', function () {
        ctx.emit('connect')
      })
      .on('error', function (error) {
        ctx.emit('error', error)
      })
      .on('close', function () {
        ctx.emit('close')
      })

    this.queueScript = thunk.persist.call(this, this.redis.script('load', luaScript))
    this.thunk(this.queueScript)()
    // auto scan jobs
    thunk.delay.call(this, Math.random() * this.interval / 3)(function () {
      if (!this.timer && !this.scanning) this.scan()
    })
  }
  return this
}

TimedQueue.prototype.scan = function () {
  var ctx = this
  var scanStart
  if (this.scanning) return this
  this.scanTime = scanStart = Date.now()
  this.scanning = true

  this.redis.smembers(this.queuesKey)(function (err, queues) {
    if (err) throw err
    ctx.emit('scanStart')
    return thunk.seq(queues.map(function (queue) {
      return function (done) {
        ctx.queue(queue).scan()(done)
      }
    }))(function (err) {
      if (err) throw err
      ctx.emit('scanEnd', queues.length, Date.now() - scanStart)
    })
  })(function (err) {
    if (err) ctx.emit('error', err)
    if (!ctx.timer && ctx.scanning) {
      ctx.scanning = false
      ctx.scan()
    } else ctx.scanning = false
  })

  clearTimeout(this.timer)
  this.timer = setTimeout(function () {
    ctx.timer = null
    if (!ctx.scanning) ctx.scan()
  }, this.interval)
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

  if (options) {
    root.thunk(root.redis.hmset(this.queueOptionsKey, {
      retry: options.retry || root.retry,
      expire: options.expire || root.expire,
      accuracy: options.accuracy || root.accuracy
    }))()
  }
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
  return thunk.call(this, this.queueScript)(function (err, luaSHA) {
    if (err) throw err
    var ctx = this
    return function (done) {
      var timestamp = Date.now()
      ctx.root.redis.evalsha(luaSHA,
        3, ctx.queueKey, ctx.activeQueueKey, ctx.queueOptionsKey,
        ctx.root.retry, ctx.root.expire, ctx.root.accuracy, timestamp
      )(function (err, res) {
        if (err) throw err
        var jobs = []
        for (var i = 0, l = res.length - 1; i < l; i += 2) {
          jobs.push(new Job(ctx.name, res[i], res[i + 1], timestamp))
        }
        return jobs
      })(done)
    }
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
  var ctx = this
  return thunk.call(this, function (done) {
    this.getjob()(function (err, jobs) {
      if (err) throw err
      return thunk.all(jobs.map(function (job) {
        return thunk.delay()(function () {
          ctx.emit('job', job)
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

Queue.prototype.activeJobs = function () {
  var ctx = this
  return thunk.call(this, function (done) {
    return this.root.redis.hgetall(this.activeQueueKey)(function (err, res) {
      if (err) throw err
      var jobs = []
      Object.keys(res).map(function (job) {
        var times = res[job].split(':')
        jobs.push(new Job(ctx.name, job, times[0], times[1]))
      })
      return jobs
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
