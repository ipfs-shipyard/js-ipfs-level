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
const encode = require('./encode')
const Iterator = require('./iterator')
const Log = require('./log')

const OPTIONS = {
  block: {
    put: {
      format: 'raw'
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
  let options = merge({}, clone(defaultOptions.default), _options)
  if (options.sync) {
    options = merge(options, defaultOptions.sync)
  }

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
}

// IPFSLevel inherits from EventEmitter *and* AbstractLeveldown
IPFSLevel.prototype = Object.assign(
  {},
  EventEmitter.prototype,
  AbstractLeveldown.prototype,
  {
    _open: function _open (options, _callback) {
      const callback = () => {
        this._ipfs.id((err, peerInfo) => {
          if (err) {
            _callback(err)
          } else {
            this._log = new Log(peerInfo.id, this._logDB, this._partition, this._ipfs, this._options)
            // TODO: handle the error properly
            this._log.on('error', (err) => { throw err })
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

    _close: function _close (callback) {
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

    _put: function (key, _value, options, callback) {
      this._onceStarted(() => {
        let value = _value
        if ((typeof value === 'undefined') || value === null) {
          value = ''
        } else if (value === NaN) {
          value = 'NaN'
        }

        if (key === NaN) {
          key = 'NaN'
        }

        if (Buffer.isBuffer(value)) {
          value = String(value)
        }

        value = new Buffer(JSON.stringify(value))

        waterfall(
          [
            (callback) => this._ipfs.block.put(value, OPTIONS.block.put, callback),
            (block, callback) => this._log.push(key, block.cid.toBaseEncodedString(), callback)
          ],
          callback)
      })
    },

    _get: function _get (_key, options, callback) {
      this._onceStarted(() => {
        let key = _key
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
            (cid, callback) => this._ipfs.block.get(cid, callback),
            (result, callback) => callback(null, result.data),
            (data, callback) => {
              callback(null, JSON.parse(data.toString()))
            },
            (data, callback) => {
              if (options.asBuffer && !Buffer.isBuffer(data)) {
                callback(null, new Buffer(String(data)))
              } else {
                callback(null, data)
              }
            }
          ],
          callback)
      })
    },

    _del: function _del (key, options, callback) {
      this._onceStarted(() => {
        this._log.del(key, callback)
      })
    },

    _batch: function _batch (array, options, callback) {
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

    _iterator: function _iterator (options) {
      return new Iterator(this, this._ipfs, this._logDB, options)
    },

    _started: function _started () {
      return Boolean(this._log)
    },

    _onceStarted: function _onceStarted (cb) {
      if (this._started()) {
        cb()
      } else {
        this.once('started', cb)
      }
    }
  }
)

module.exports = IPFSLevel
