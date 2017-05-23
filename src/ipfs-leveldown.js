'use strict'

const AbstractLeveldown = require('abstract-leveldown').AbstractLevelDOWN
const merge = require('deep-assign')
const IPFS = require('ipfs')
const defaultOptions = require('./default-options')

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

    super(partition)
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

  }
}
