'use strict'

const AbstractLeveldown = require('abstract-leveldown').AbstractLevelDOWN
const EventEmitter = require('events')
const merge = require('deep-assign')
const IPFS = require('ipfs')
const waterfall = require('async/waterfall')
const eachSeries = require('async/eachSeries')
const series = require('async/series')
const clone = require('lodash.clonedeep')

const defaultOptions = require('./options')
const Iterator = require('./iterator')
const Log = require('./log')

const OPTIONS = {
  dag: {
    put: {
      format: 'dag-cbor'
    }
  }
}

function IPFSLevel (partition, _options) {
  if (!(this instanceof IPFSLevel)) {
    return new IPFSLevel(partition, _options)
  }

  if (typeof partition === 'object') {
    _options = partition
    partition = undefined
  }
  let options = merge({}, clone(defaultOptions), _options)

  if (!partition) {
    partition = options.partition
  }

  if (!partition) {
    throw new Error('need a partition to be defined')
  }

  const logDB = options.log
  if (!logDB) {
    throw new Error('need a log database')
  }

  // super constructors
  AbstractLeveldown.call(this, partition)
  EventEmitter.call(this)

  this._logDB = logDB
  this._options = options
  this._partition = partition

  this._iteratorCount = 0
  this._garbageCollecting = false
}

// IPFSLevel inherits from EventEmitter *and* AbstractLeveldown
IPFSLevel.prototype = Object.assign(
  {},
  EventEmitter.prototype,
  AbstractLeveldown.prototype,
  {
    _open (options, _callback) {
      const callback = () => {
        this._ipfs.id((err, peerInfo) => {
          if (err) {
            _callback(err)
          } else {
            this._ipfsNodeId = peerInfo.id
            this._log = new Log(this._ipfsNodeId, this._logDB, this._partition, this._ipfs, this._options)
            this._log.on('error', (err) => this.emit('error', err))
            this._log.on('change', this._onLogChange.bind(this))
            this._log.on('new head', (cid) => this.emit('new head', cid))

            this.emit('started')
            _callback()
          }
        })
      }

      this._ipfs = this._options.ipfs
      if (!this._ipfs) {
        this._ipfs = new IPFS(this._options.ipfsOptions)
      }
      if (this._ipfs.isOnline()) {
        callback()
      } else {
        this._ipfs.once('ready', () => callback())
      }
    },

    _close (callback) {
      series([
        (callback) => {
          if (this._ipfs && this._ipfs.isOnline()) {
            this._ipfs.stop((err) => {
              if (err) {
                console.error(err) // TODO: handle close error with grace
              }
              callback()
            })
          } else {
            callback()
          }
        },
        (callback) => {
          if (this._log) {
            this._log.stop()
          }
          callback()
        }
      ], callback)
    },

    _put (key, _value, options, _callback) {
      this._onceStarted(() => {
        const callback = this._maybeGarbageCollecting(_callback)
        let value = _value
        if ((typeof value === 'undefined') || value === null) {
          value = ''
          /* eslint use-isnan: 0 */
        } else if (value === NaN) {
          value = 'NaN'
        }

        /* eslint use-isnan: 0 */
        if (key === NaN) {
          key = 'NaN'
        }

        if (Buffer.isBuffer(value)) {
          value = String(value)
        }

        waterfall(
          [
            (callback) => this._ipfs.dag.put(value, OPTIONS.dag.put, callback),
            (cid, callback) => this._log.push(key, cid.toBaseEncodedString(), callback)
          ],
          callback)
      })
    },

    _get (_key, options, callback) {
      this._onceStarted(() => {
        let key = _key
        /* eslint use-isnan: 0 */
        if (key === NaN) {
          key = 'NaN'
        }

        waterfall(
          [
            (callback) => this._log.getLatest(key, callback),
            (latestHead, latestHeadCID, callback) => {
              if (!latestHead || latestHead.deleted) {
                callback(new Error('NotFound'))
              } else {
                callback(null, latestHead.cid)
              }
            },
            (cid, callback) => this._ipfs.dag.get(cid, callback),
            (result, callback) => callback(null, result.value),
            (data, callback) => {
              if (options.asBuffer && !Buffer.isBuffer(data)) {
                callback(null, Buffer.from(String(data)))
              } else {
                callback(null, data)
              }
            }
          ],
          callback)
      })
    },

    _del (key, options, _callback) {
      const callback = this._maybeGarbageCollecting(_callback)
      this._onceStarted(() => {
        this._log.del(key, callback)
      })
    },

    _batch (array, options, callback) {
      eachSeries(
        array,
        (op, callback) => {
          if (op.type === 'put') {
            this.put(op.key, op.value, callback)
          } else if (op.type === 'del') {
            this.del(op.key, callback)
          } else {
            callback(new Error('invalid operation type:' + op.type))
          }
        },
        callback)
    },

    _iterator (options) {
      this._iteratorCount ++
      const iterator = new Iterator(
        this, this._ipfs, this._logDB, options, this._onIteratorEnded.bind(this))
      if (this._garbageCollecting) {
        this.once('gc done', () => iterator.resume())
      } else {
        iterator.resume()
      }
      return iterator
    },

    _onIteratorEnded () {
      this._iteratorCount --
      if (!this._iteratorCount) {
        this._garbageCollect()
      }
    },

    _maybeGarbageCollecting (callback) {
      return (err, result) => {
        if (!this._iteratorCount) {
          this._garbageCollect()
        }
        callback(err, result)
      }
    },

    _garbageCollect () {
      this._garbageCollecting = true
      this._log._garbageCollect((err) => {
        if (err) {
          console.error('error while garbage collecting:', err)
        }
        this._garbageCollecting = false
        this.emit('gc done')
      })
    },

    _started () {
      return Boolean(this._log)
    },

    _onceStarted (cb) {
      if (this._started()) {
        cb()
      } else {
        this.once('started', cb)
      }
    },

    _onLogChange (change) {
      const key = change.key
      const logCID = change.logCID
      waterfall(
        [
          (callback) => this._ipfs.dag.get(logCID, callback),
          (result, callback) => callback(null, result.value),
          (logEntry, callback) => {
            if (logEntry.deleted) {
              callback(null, null)
            } else {
              this._ipfs.dag.get(logEntry.cid, callback)
            }
          },
          (result, callback) => callback(null, result && result.value),
          (value, callback) => {
            this.emit('change', {
              type: value ? 'put' : 'del',
              key: key,
              value: value
            })
            callback()
          }
        ],
        (err) => {
          if (err) {
            this.emit('error', err)
          }
        }
      )
    },

    getLatestHeadCID (callback) {
      this._log.getLatestHeadCID(callback)
    },

    ipfsNode () {
      return this._ipfs
    },

    ipfsNodeId () {
      return this._ipfsNodeId
    },

    partition () {
      return this._partition
    },

    log () {
      return this._log
    }
  }
)

module.exports = IPFSLevel
