'use strict'

const AbstractLeveldown = require('abstract-leveldown').AbstractLevelDOWN
const merge = require('deep-assign')
const IPFS = require('ipfs')
const waterfall = require('async/waterfall')
const eachSeries = require('async/eachSeries')
const defaultOptions = require('./default-options')
const encode = require('./encode')
const Iterator = require('./iterator')

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
    const options = merge({}, defaultOptions, _options)
    if (!partition) {
      partition = options.partition
    }

    if (!partition) {
      throw new Error('need a partition to be defined')
    }

    const heads = options.heads
    if (!heads) {
      throw new Error('need a heads database')
    }

    super(partition)
    this._heads = heads
    this._options = options
    this._partition = partition
  }

  _open (options, callback) {
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
    this._ipfs.stop(callback)
  }

  _put (key, value, options, callback) {
    waterfall(
      [
        (callback) => this._ipfs.dag.put(encode.kv(key, value, options), OPTIONS.dag.put, callback),
        (cid, callback) => this._heads.put(key, cid.toBaseEncodedString(), callback)
      ],
      callback)
  }

  _get (key, options, callback) {
    waterfall(
      [
        (callback) => this._heads.get(key, { asBuffer: false }, callback),
        (cid, callback) => {
          if (!cid) {
            callback(new Error('NotFound'))
          } else {
            callback(null, cid)
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
    this._heads.del(key, callback)
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
    return new Iterator(this, this._ipfs, this._heads, options)
  }
}
