'use strict'

const EventEmitter = require('events')
const waterfall = require('async/waterfall')
const Queue = require('async/queue')
const vectorclock = require('vectorclock')
const debug = require('debug')
const uniq = require('lodash.uniq')

const decoding = require('./decoding')

const OPTIONS = {
  dag: {
    put: {
      format: 'dag-cbor'
    }
  }
}

module.exports = class Log extends EventEmitter {
  constructor (nodeId, log, partition, ipfs, options) {
    super()
    this._nodeId = nodeId
    this._log = log
    this._partition = partition
    this._ipfs = ipfs
    this._options = options
    this._queue = Queue(this._processQueue.bind(this), 1)
    this._debug = debug('ipfs-level:log:' + this._nodeId)

    this._gcKeys = []
  }

  // PUBLIC API:

  push (key, cid, callback) {
    this._queue.push({
      fn: this._push,
      args: {
        key: key,
        cid: cid
      }}, callback)
  }

  del (key, callback) {
    this._queue.push({
      fn: this._del,
      args: key
    }, callback)
  }

  getLatestHeadCID (callback) {
    this._log.get('tag:HEAD', { asBuffer: false }, ignoringNotFoundError(callback))
  }

  getLatestHead (callback) {
    waterfall([
      (callback) => this.getLatestHeadCID(callback),
      (logCID, callback) => logCID
        ? this._log.get('cid:' + logCID, decoding((err, logEntry) => {
          callback(err, logEntry, logCID)
        }))
        : callback(null, null, null)
    ], callback)
  }

  getLatest (key, callback) {
    debug('getting the latest log entry for key %j', key)
    waterfall([
      (callback) => this._log.get('key:' + key, { asBuffer: false }, ignoringNotFoundError(callback)),
      (logCID, callback) => {
        debug('got latest log entry for key %j: %j', key, logCID)
        return logCID
        ? this._log.get('cid:' + logCID, decoding((err, logEntry) => {
          if (err) {
            callback(err)
          } else {
            callback(null, logEntry, logCID)
          }
        }))
        : callback(null, null, null)
      }
    ], callback)
  }

  stop () {
    if (this._sync) {
      this._sync.stop()
    }
  }

  get (cid, callback) {
    this._log.get('cid:' + cid, decoding(callback))
  }

  set (cid, entry, callback) {
    this._log.put('cid:' + cid, encode(entry), callback)
  }

  impose (key, logCID, callback) {
    this._debug('imposing %s = %s', key, logCID)
    waterfall([
      (callback) => {
        if (this._options.retainLog) {
          callback(null, null)
          return // early
        }
        this._log.get('key:' + key, ignoringNotFoundError(callback))
      },
      (previousLogCID, callback) => {
        if (!this._options.retainLog && previousLogCID) {
          this._gcKeys.push('cid:' + previousLogCID)
        }
        callback()
      },
      (callback) => this._log.put('key:' + key, logCID, callback),
      (callback) => {
        this.emit('change', {
          key: key,
          logCID: logCID
        })
        callback()
      }
    ], callback)
  }

  setHead (logEntry, callback) {
    this._debug('setting head to %j', logEntry)
    waterfall(
      [
        (callback) => this._ipfs.dag.put(logEntry, OPTIONS.dag.put, callback),
        (cid, callback) => callback(null, cid.toBaseEncodedString()),
        (logCID, callback) => {
          this._log.batch(
            [
              { type: 'put', key: 'cid:' + logCID, value: encode(logEntry) },
              { type: 'put', key: 'tag:HEAD', value: logCID }
            ],
            (err) => {
              callback(err, logCID)
            }
          )
        },
        (logCID, callback) => {
          if (this._sync) {
            this.emit('new head', logCID)
            this._sync.setNewHead()
          }
          callback()
        }
      ],
      callback)
  }

  setHeadCID (cid, callback) {
    this._debug('setting head cid to %s', cid)
    this._log.put('tag:HEAD', cid, (err) => {
      if (err) {
        callback(err)
      } else {
        this.emit('new head', cid)
        callback()
      }
    })
  }

  transaction (fn, callback) {
    this._queue.push({
      fn: this._transaction,
      args: fn
    }, callback)
  }

  // QUEUE PROCESSING:

  _processQueue (task, callback) {
    task.fn.call(this, task.args, callback)
  }

  _push (push, callback) {
    this.save(push.key, push.cid, [], [], callback)
  }

  _del (key, callback) {
    this.save(key, null, [], [], callback)
  }

  _transaction (fn, callback) {
    fn(callback)
  }

  save (key, cid, parentLogCIDs, parentVectorClocks, callback) {
    this._debug('_save %j, %j', key, cid)
    waterfall(
      [
        (callback) => {
          if (this._options.retainLog) {
            callback(null, null)
            return // early
          }
          this._log.get('key:' + key, ignoringNotFoundError(callback))
        },
        (previousLogCID, callback) => {
          if (!this._options.retainLog && previousLogCID) {
            this._gcKeys.push('cid:' + previousLogCID)
          }
          callback()
        },
        (callback) => this.getLatestHead(callback),
        (latestHead, latestHeadCID, callback) => {
          this._debug('_save: latest HEAD CID: %s', latestHeadCID)
          callback(
            null,
            this._newLogEntry(
              key, cid, latestHead, latestHeadCID, parentLogCIDs, parentVectorClocks))
        },
        (logEntry, callback) => {
          this._debug('saving log entry %j', logEntry)
          this._ipfs.dag.put(logEntry, OPTIONS.dag.put, (err, cid) => {
            callback(err, cid && cid.toBaseEncodedString(), logEntry)
          })
        },
        (logCID, logEntry, callback) => {
          const ops = [
            { type: 'put', key: 'cid:' + logCID, value: encode(logEntry) },
            { type: 'put', key: 'key:' + key, value: logCID },
            { type: 'put', key: 'tag:HEAD', value: logCID }
          ]

          this._log.batch(ops, (err) => {
            if (err) {
              callback(err)
            } else {
              callback(null, logCID)
            }
          })
        },
        (logCID, callback) => {
          this._debug('saved. setting new head to %s', logCID)
          this.emit('new head', logCID)
          this.emit('change', {
            key: key,
            logCID: logCID
          })
          callback()
        }
      ],
      callback)
  }

  _garbageCollect (callback) {
    const keys = this._gcKeys
    this._gcKeys = []
    this._log.batch(keys.map((key) => ({ type: 'del', key: key })), callback)
  }

  _newLogEntry (key, cid, latest, latestCID, parentLogCIDs, parentVectorClocks) {
    if (!latest) {
      latest = {
        clock: {},
        parents: []
      }
    } else {
      latest.parents = [latestCID]
    }
    latest.parents = uniq(latest.parents.concat(parentLogCIDs).sort())

    latest.key = key
    if (!cid) {
      latest.deleted = true
      delete latest.cid
    } else {
      delete latest.deleted
      latest.cid = cid
    }

    if (!parentVectorClocks.length) {
      vectorclock.increment(latest, this._nodeId)
    } else {
      console.log('merging', parentVectorClocks)
      latest.clock = parentVectorClocks.reduce(vectorclock.merge, latest).clock
      console.log('merged latest: %s', JSON.stringify(latest, null, '\t'))
    }

    return latest
  }
}

function encode (logEntry) {
  return JSON.stringify(logEntry)
}

function ignoringNotFoundError (callback) {
  return (err, result) => {
    if (err && err.message === 'NotFound') {
      callback(null, null)
    } else {
      callback(err, result)
    }
  }
}
