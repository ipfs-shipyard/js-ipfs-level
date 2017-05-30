const test       = require('tape')
const testCommon = require('abstract-leveldown/testCommon')
const IPFSLevel    = require('../')
const testBuffer = require('./fixtures/testdata_b64')
const Memdown = require('memdown')

const ipfsLevel = IPFSLevel({
  log: Memdown('test:abstract-leveldown')
})

require('abstract-leveldown/abstract/open-test').args(ipfsLevel, test, testCommon)
require('abstract-leveldown/abstract/open-test').open(ipfsLevel, test, testCommon)

return;
require('abstract-leveldown/abstract/del-test').all(ipfsLevel, test, testCommon)

require('abstract-leveldown/abstract/get-test').all(ipfsLevel, test, testCommon)

require('abstract-leveldown/abstract/put-test').all(ipfsLevel, test, testCommon)

require('abstract-leveldown/abstract/put-get-del-test').all(ipfsLevel, test, testCommon, testBuffer)

require('abstract-leveldown/abstract/batch-test').all(ipfsLevel, test, testCommon)

require('abstract-leveldown/abstract/close-test').close(ipfsLevel, test, testCommon)

require('abstract-leveldown/abstract/iterator-test').all(ipfsLevel, test, testCommon)

require('abstract-leveldown/abstract/ranges-test').all(ipfsLevel, test, testCommon)
