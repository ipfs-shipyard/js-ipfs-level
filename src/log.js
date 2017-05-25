'use strict'

const waterfall = require('async/waterfall')
const Queue = require('async/queue')
const vectorclock = require('vectorclock')

const Sync = require('./sync')

const OPTIONS = {
  dag: {
    put: {
      format: 'dag-cbor'
    }
  }
}

module.exports = class Log {
  constructor (nodeId, log, partition, ipfs) {
    this._nodeId = nodeId
    this._log = log
    this._sync = new Sync(partition, this, ipfs)
    this._partition = partition
    this._ipfs = ipfs

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

  getLatestHead (callback) {
    this._log.get('tag:HEAD', { asBuffer: false }, decoding(callback))
  }

  getLatest (key, callback) {
    this._log.get('key:' + key, { asBuffer: false }, decoding(callback))
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

  // INTERNALS:

  _save (key, cid, callback) {
    waterfall(
      [
        (callback) => this.getLatestHead(callback),
        (latestHead, callback) => callback(null, this._newLogEntry(key, cid, latestHead)),
        (logEntry, callback) => this._ipfs.dag.put(logEntry, OPTIONS.dag.put, (err, cid) => {
          callback(err, cid && cid.toBaseEncodedString(), logEntry)
        }),
        (cid, logEntry, callback) => {
          const encodedEntry = encode(logEntry)
          const keys = [
            'key:' + key,
            'cid:' + cid,
            'tag:HEAD'
          ]

          const ops = keys.map((key) => ({
            type: 'put',
            key: key,
            value: encodedEntry
          }))

          this._log.batch(ops, (err) => {
            if (err) {
              callback(err)
            } else {
              callback(null, logEntry)
            }
          })
        },
        (logEntry, callback) => {
          this._sync.setNewHead(logEntry)
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
      latest.parent = latest.cid
    }

    latest.key = key
    if (!cid) {
      latest.deleted = true
      delete latest.cid
    } else {
      delete latest.deleted
      latest.cid = cid
    }

    vectorclock.increment(latest.clock, this._nodeId)

    return latest
  }
}

function encode (logEntry) {
  return JSON.stringify(logEntry)
}

function decode (str) {
  return JSON.parse(str)
}

function decoding (callback) {
  return (err, str) => {
    if (err && err.message === 'NotFound') {
      callback(null, undefined)
    } else {
      callback(err, str && decode(str))
    }
  }
}
