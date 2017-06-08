'use strict'

const merge = require('deep-assign')

const IPFSLevel = require('./ipfs-level')

exports = module.exports = (partition, options) => new IPFSLevel(partition, options)
exports.defaults = (defaultOptions) => (partition, options) => {
  const opts = merge({}, defaultOptions, options || {})
  return new IPFSLevel(partition, opts)
}
