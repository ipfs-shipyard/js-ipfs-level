'use strict'

const EventEmitter = require('events')
const waterfall = require('async/waterfall')
const Queue = require('async/queue')
const vectorclock = require('vectorclock')
const debug = require('debug')('ipfs-level:log')

const Sync = require('./sync')
const Merger = require('./merger')
const decoding = require('../decoding')

const OPTIONS = {
  dag: {
    put: {
      format: 'dag-cbor'
    }
  }
}

module.exports = class Log extends EventEmitter {
  constructor (nodeId, log, partition, ipfs) {
    super()
    this._nodeId = nodeId
    this._log = log
    this._partition = partition
    this._ipfs = ipfs

    this._merger = new Merger(this._nodeId, this._ipfs, this)

    this._sync = new Sync(this._nodeId, partition, this, ipfs)
    this._sync.on('error', (err) => this.emit('error', err))
    this._sync.on('message', (m) => {
      debug('remote head: %j', m)
      this._merger.processRemoteHead(m, (err) => {
        if (err) {
          this.emit('error', err)
        }
        debug('finished processing remote head %s', m)
      })
    })

    this._queue = Queue(this._processQueue.bind(this), 1)
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
      (logCID, callback) => logCID ? this._log.get('cid:' + logCID, decoding(callback)) : callback(null, null)
    ], callback)
  }

  getLatest (key, callback) {
    waterfall([
      (callback) => this._log.get('key:' + key, { asBuffer: false }, ignoringNotFoundError(callback)),
      (logCID, callback) => logCID ? this._log.get('cid:' + logCID, decoding(callback)) : callback(null, null)
    ], callback)
  }

  stop () {
    this._sync.stop()
  }

  get (cid, callback) {
    this._log.get('cid:' + cid, decoding(callback))
  }

  set (cid, entry, callback) {
    this._log.put('cid:' + cid, encode(entry), callback)
  }

  impose (key, logCID, callback) {
    debug('imposing %s = %s', key, logCID)
    this._log.put('key:' + key, logCID, callback)
  }

  setHead (logEntry, callback) {
    debug('setting head to %j', logEntry)
    waterfall([
      (callback) => this._ipfs.dag.put(logEntry, OPTIONS.dag.put, callback),
      (logCID, callback) => callback(null, logCID.toBaseEncodedString()),
      (logCID, callback) => this._log.batch(
        [
          { type: 'put', key: 'cid:' + logCID, value: encode(logEntry) },
          { type: 'put', key: 'tag:HEAD', value: logCID }
        ], callback)
    ], callback)
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
    this._save(push.key, push.cid, callback)
  }

  _del (key, callback) {
    this._save(key, null, callback)
  }

  _transaction (fn, callback) {
    fn(callback)
  }

  // INTERNALS:

  _save (key, cid, callback) {
    debug('_save %j, %j', key, cid)
    waterfall(
      [
        (callback) => this.getLatestHead(callback),
        (latestHead, callback) => callback(null, this._newLogEntry(key, cid, latestHead)),
        (logEntry, callback) => {
          debug('saving log entry %j', logEntry)
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
          debug('saved. setting new head to %s', logCID)
          this._sync.setNewHead(logCID)
          callback()
        }
      ],
      callback)
  }

  _newLogEntry (key, cid, latest) {
    if (!latest) {
      latest = {
        clock: {}
      }
    } else {
      latest.parents = [latest.cid]
    }

    latest.key = key
    if (!cid) {
      latest.deleted = true
      delete latest.cid
    } else {
      delete latest.deleted
      latest.cid = cid
    }

    vectorclock.increment(latest, this._nodeId)

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