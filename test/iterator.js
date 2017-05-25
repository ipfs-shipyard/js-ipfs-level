/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect

const Memdown = require('memdown')
const whilst = require('async/whilst')
const IPFSLevel = require('../')

const PARTITION = 'iterator-test'

const fixtures = [
  { type: 'put', key: 'a', value: 'value a' },
  { type: 'put', key: 'b', value: 'value b' },
  { type: 'put', key: 'c', value: 'value c' },
  { type: 'put', key: 'd', value: 'value d' }
]

describe('leveldown iterator', () => {
  let db
  const options = {
    log: Memdown(PARTITION)
  }

  before((done) => {
    db = IPFSLevel(PARTITION, options)
    db.open(done)
  })

  before((done) => db.batch(fixtures, done))

  after((done) => setTimeout(done, 4000))
  after((done) => db.close(done))

  it('can use a full iterator', (done) => {
    let ended = false
    let pos = -1
    const it = db.iterator({ keyAsBuffer: false })
    whilst(
      () => !ended,
      (callback) => {
        it.next((err, key, value) => {
          expect(err).to.not.exist()
          const expected = fixtures[++pos]
          if (expected) {
            expect(key).to.equal(expected.key)
            expect(value).to.equal(expected.value)
          } else {
            ended = true
          }
          callback()
        })
      },
      (err) => {
        expect(err).to.not.exist()
        expect(pos).to.equal(4)
        done()
      })
  })
})
