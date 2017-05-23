'use strict'

const IPFSLeveldown = require('./ipfs-leveldown')

module.exports = (partition, options) => new IPFSLeveldown(partition, options)
