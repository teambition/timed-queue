'use strict'

// **Github:** https://github.com/teambition/timed-queue
//
// **License:** MIT

const fs = require('fs')
const path = require('path')
const thunks = require('thunks')
const redis = require('thunk-redis')
const EventEmitter = require('events').EventEmitter

const thunk = thunks()
const slice = Array.prototype.slice
const luaScript = fs.readFileSync(path.join(__dirname, 'queue.lua'), { encoding: 'utf8' })

class TimedQueue extends EventEmitter {
  constructor (options) {
    super()
    options = options || {}

    this.prefix = options.prefix || 'TIMEDQ'
    this.queuesKey = '{' + this.prefix + '}:QUEUES'
    this.count = Math.floor(options.count) || 64
    this.interval = Math.floor(options.interval) || 1000 * 60
    this.expire = Math.floor(options.expire) || this.interval * 5
    this.retry = Math.floor(options.retry) || Math.floor(this.interval / 2)
    this.accuracy = Math.floor(options.accuracy) || Math.floor(this.interval / 5)
    this.autoScan = options.autoScan !== false

    this.redis = null
    this.timer = null
    this.scanTime = 0
    this.scanning = false
    this.delay = this.interval
    this.queues = Object.create(null)
    this.thunk = thunks((err) => err && this.emit('error', err))
  }

  connect (redisClient) {
    if (this.redis) return this
    if (redisClient && redisClient.info && typeof redisClient.evalauto === 'function') {
      this.redis = redisClient
    } else {
      this.redis = redis.createClient.apply(null, arguments)
    }

    this.redis.on('connect', () => this.emit('connect'))
      .on('error', (err) => this.emit('error', err))
      .on('close', () => this.emit('close'))

    // auto scan jobs
    if (this.autoScan) {
      thunk.delay(Math.random() * this.interval)(() => this.scan())
    }
    return this
  }

  queue (queueName, options) {
    validateString(queueName)
    if (!this.queues[queueName]) this.queues[queueName] = new Queue(this, queueName, options)
    else if (options) this.queues[queueName].init(options)
    return this.queues[queueName]
  }

  destroyQueue (queueName) {
    return thunk.call(this, function * () {
      let redis = this.redis
      let queue = this.queue(queueName)

      delete this.queues[queueName]
      yield [
        redis.srem(this.queuesKey, queue.name),
        redis.del(queue.queueOptionsKey),
        redis.del(queue.activeQueueKey),
        redis.del(queue.queueKey)
      ]
    })
  }

  scan () {
    if (this.scanning || !this.redis) return this

    let scanStart
    this.scanning = true
    this.scanTime = scanStart = Date.now()

    this.thunk(function * () {
      let queues = yield this.redis.smembers(this.queuesKey)
      queues = queues.filter((queueName) => this.queues[queueName])
      this.emit('scanStart', queues.length)

      let queueScores = yield queues.map((queue) => this.queue(queue).scan())
      this.emit('scanEnd', queueScores.length, Date.now() - scanStart)
      this.regulateFreq(scoresDeviation(queueScores))

      if (!this.timer && this.scanning) {
        this.scanning = false
        this.scan()
      } else this.scanning = false
    })()

    clearTimeout(this.timer)
    this.timer = setTimeout(() => {
      this.timer = null
      if (!this.scanning) this.scan()
    }, this.delay)

    return this
  }

  stop () {
    clearTimeout(this.timer)
    this.scanning = false
    this.timer = null
    return this
  }

  close () {
    this.stop()
    this.redis.clientEnd()
    this.redis = null
    return this
  }

  // x > 0 | 2 - 1 / (1 + x)
  // x < 0 | 1 / (1 - x)
  regulateFreq (factor) {
    if (factor < -0.05) this.delay = Math.max(this.delay / (1 - factor), this.interval / 10)
    else if (factor > 0.05) this.delay = Math.min(this.delay * (2 - 1 / (1 + factor)), this.interval)
    return this
  }
}

class Queue extends EventEmitter {
  constructor (timedQueue, queueName, options) {
    super()

    this.root = timedQueue
    this.name = queueName
    this.queueKey = '{' + timedQueue.prefix + ':' + queueName + '}' // hash tag
    this.activeQueueKey = this.queueKey + ':-'
    this.queueOptionsKey = this.queueKey + ':O'
    this.init(options)
  }

  init (options) {
    let root = this.root
    root.thunk(root.redis.sadd(root.queuesKey, this.name))()

    if (!options) return this
    root.thunk(root.redis.hmset(this.queueOptionsKey, {
      count: options.count || root.count,
      retry: options.retry || root.retry,
      expire: options.expire || root.expire,
      accuracy: options.accuracy || root.accuracy
    }))()
    return this
  }

  addjob (job, timing) {
    let args = slice.call(Array.isArray(job) ? job : arguments)
    return thunk.call(this, function * () {
      let data = [this.queueKey]
      let current = Date.now()

      for (let i = 0, l = args.length || 2; i < l; i += 2) {
        validateString(args[i])
        timing = Math.floor(args[i + 1])
        if (!timing || timing <= current) throw new Error(`${String(args[i + 1])} is invalid time in "${job}".`)
        data.push(timing, args[i])
      }

      return yield this.root.redis.zadd(data)
    })
  }

  show (job) {
    return thunk.call(this, function * () {
      validateString(job)

      let timing = yield this.root.redis.zscore(this.queueKey, job)
      if (timing) return new Job(this.name, job, timing, 0, 0)

      let times = yield this.root.redis.hget(this.activeQueueKey, job)
      if (!times) return null
      times = times.split(':')
      return new Job(this.name, job, times[0], times[1], times.length - 2)
    })
  }

  deljob (job) {
    let args = slice.call(Array.isArray(job) ? job : arguments)
    return thunk.call(this, function * () {
      for (let i = 0, l = args.length || 1; i < l; i++) validateString(args[i])

      args.unshift(this.queueKey)
      let count = yield this.root.redis.zrem(args)
      args[0] = this.activeQueueKey
      let _count = yield this.root.redis.hdel(args)
      return count + _count
    })
  }

  getjobs (scanActive) {
    return thunk.call(this, function * () {
      let root = this.root
      let timestamp = Date.now()

      let res = yield root.redis.evalauto(luaScript, 3,
        this.queueKey, this.activeQueueKey, this.queueOptionsKey,
        root.count, root.retry, root.expire, root.accuracy, timestamp, +(scanActive !== false))
      return new ScanResult(this.name, timestamp, res)
    })
  }

  ackjob (job) {
    let args = slice.call(Array.isArray(job) ? job : arguments)
    return thunk.call(this, function * () {
      for (let i = 0, l = args.length || 1; i < l; i++) validateString(args[i])
      args.unshift(this.activeQueueKey)
      return yield this.root.redis.hdel(args)
    })
  }

  scan () {
    return thunk.call(this, function * () {
      let scores = []
      if (!this.listenerCount('job')) {
        return // Don't scan if no 'job' listener
      }

      let res = yield this.getjobs(true) // get active jobs firstly
      while (true) {
        res.jobs.map((job) => {
          if (!job.retryCount) scores.push((job.timing - job.active) / res.retry)
          process.nextTick(() => this.emit('job', job))
        })
        if (!res.hasMore) return scores
        res = yield this.getjobs(false)
      }
    })
  }

  len () {
    return thunk.call(this, function * () {
      return yield this.root.redis.zcard(this.queueKey)
    })
  }

  showActive () {
    return thunk.call(this, function * () {
      let res = yield this.root.redis.hgetall(this.activeQueueKey)
      return Object.keys(res)
        .map((job) => {
          let times = res[job].split(':')
          return new Job(this.name, job, times[0], times[1], times.length - 2)
        }).sort((a, b) => a.timing - b.timing)
    })
  }
}

class Job {
  constructor (queue, job, timing, active, retryCount) {
    this.queue = queue
    this.job = job
    this.timing = +timing
    this.active = +active
    this.retryCount = +retryCount
  }
}

function ScanResult (name, timestamp, res) {
  let jobs = res[0]
  this.retry = +res[1]
  this.hasMore = +res[2]
  this.jobs = []
  for (let i = 0, l = jobs.length - 2; i < l; i += 3) {
    this.jobs.push(new Job(name, jobs[i], jobs[i + 1], timestamp, jobs[i + 2]))
  }
}

function scoresDeviation (queueScores) {
  let count = 0
  let score = queueScores.reduce((prev, scores) => {
    return prev + scores.reduce((p, s) => {
      count++
      return p + s
    }, 0)
  }, 0)
  return count ? (score / count) : 0
}

function validateString (str) {
  if (typeof str !== 'string' || !str) throw new TypeError(`${String(str)} is invalid string`)
}

module.exports = TimedQueue
