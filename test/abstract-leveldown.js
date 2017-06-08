const test = require('tape')
const Memdown = require('memdown')

const testCommon = require('./utils/testCommon')
const testBuffer = require('./fixtures/testdata_b64')
const IPFSLevel = require('../')

const ipfsLevel = (location) => {
  return IPFSLevel(location, {
    log: Memdown('test:abstract-leveldown:' + location)
  })
}

// Abstract-leveldown makes some assumptions about object serialization and deserialization on
// the browser that are not supported by JSON.
// Hence, we have to ignote some test results..
const ignoreTests = [
  'test _serialize object:should be equal',
  'test put()/get()/del() with `NaN` value:should be equal'
]

process.on('unaughtException', (err) => {
  console.log('uncaught exception', err)
})

describe('abstract-leveldown tests', () => {
  it('runs all the abstract-leveldown tests', (done) => {

    let currentTest = null

    test.createStream({ objectMode: true })
      .on('data', (result) => {
        pprint(result)
        if (result.type === 'test') {
          currentTest = result.name
        } else if (result.type === 'assert') {
          const assertId = currentTest + ':' + result.name
          if (!result.ok && ignoreTests.indexOf(assertId) === -1) {
            done(new Error('abstract leveldown test failed. Result:\n' + JSON.stringify(result)))
          }
        }
      })

    test.onFinish(() => done())

    require('abstract-leveldown/abstract/open-test').args(ipfsLevel, test, testCommon)
    // require('abstract-leveldown/abstract/open-test').open(ipfsLevel, test, testCommon)
    require('abstract-leveldown/abstract/del-test').all(ipfsLevel, test, testCommon)
    require('abstract-leveldown/abstract/get-test').all(ipfsLevel, test, testCommon)
    require('abstract-leveldown/abstract/put-test').all(ipfsLevel, test, testCommon)
    require('abstract-leveldown/abstract/put-get-del-test').all(ipfsLevel, test, testCommon, testBuffer)
    require('abstract-leveldown/abstract/batch-test').all(ipfsLevel, test, testCommon)

    // TODO: still need https://github.com/ipfs/js-ipfs/issues/874 fixed
    // before activating this test:
    // require('abstract-leveldown/abstract/close-test').close(ipfsLevel, test, testCommon)

    require('abstract-leveldown/abstract/iterator-test').all(ipfsLevel, test, testCommon)
    require('abstract-leveldown/abstract/ranges-test').all(ipfsLevel, test, testCommon)
  }).timeout(100000)
})

function pprint (result) {
  if (result.type === 'test') {
    console.log('# ' + result.name + ' (' + result.id + ')...')
  } else if (result.type === 'assert') {
    if (result.ok) {
      console.log('    ' + result.name + ': OK')
    } else {
      console.log('    ' + result.name + ': NOT OK')
      console.log('     ' + JSON.stringify(result))
    }
  }
}