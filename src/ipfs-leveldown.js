'use strict'

const AbstractLeveldown = require('abstract-leveldown').AbstractLevelDOWN
const merge = require('deep-assign')
const IPFS = require('ipfs')
const waterfall = require('async/waterfall')
const eachSeries = require('async/eachSeries')
const series = require('async/series')
const clone = require('lodash.clonedeep')
const defaultOptions = require('./default-options')
const encode = require('./encode')
const Iterator = require('./iterator')
const Log = require('./log')

const OPTIONS = {
  dag: {
    put: {
      format: 'dag-cbor'
    }
  }
}

module.exports = class IPFSLeveldown extends AbstractLeveldown {
  constructor (partition, _options) {
    if (typeof _partition === 'object') {
      _options = partition
      partition = undefined
    }
    const options = merge({}, clone(defaultOptions), _options)
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

    super(partition)
    this._logDB = logDB
    this._options = options
    this._partition = partition
  }

  _open (options, _callback) {
    const callback = () => {
      this._ipfs.id((err, peerInfo) => {
        if (err) {
          _callback(err)
        } else {
          this._log = new Log(peerInfo.id, this._logDB, this._partition, this._ipfs)
          // TODO: handle the error properly
          this._log.on('error', (err) => { throw err })
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
  }

  _close (callback) {
    series([
      (callback) => {
        if (this._ipfs && this._ipfs.isOnline()) {
          this._ipfs.stop(callback)
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
  }

  _put (key, value, options, callback) {
    waterfall(
      [
        (callback) => this._ipfs.dag.put(encode.kv(key, value, options), OPTIONS.dag.put, callback),
        (cid, callback) => this._log.push(key, cid.toBaseEncodedString(), callback)
      ],
      callback)
  }

  _get (key, options, callback) {
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
        (value, callback) => {
          if (value && value.key !== key) {
            callback(new Error('expected key to be ' + key + ' and got ' + value.key))
          } else if (!value || value.deleted) {
            callback(new Error('NotFound'))
          } else {
            callback(null, value.value)
          }
        }
      ],
      callback)
  }

  _del (key, options, callback) {
    this._log.del(key, callback)
  }

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
  }

  _iterator (options) {
    return new Iterator(this, this._ipfs, this._logDB, options)
  }
}
