/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect

const Memdown = require('memdown')
const IPFSLevel = require('../')

const PARTITION = 'leveldown-test'

describe('leveldown interface', () => {
  let db
  const options = {
    heads: Memdown(PARTITION)
  }

  it('can create leveldown object', (done) => {
    db = IPFSLevel(PARTITION, options)
    done()
  })

  it('can be opened', (done) => {
    db.open(done)
  })

  it('can put a value', (done) => {
    db.put('key', 'value', done)
  })

  it('can get that value back', (done) => {
    db.get('key', (err, result) => {
      expect(err).to.not.exist()
      expect(result).to.equal('value')
      done()
    })
  })

  it('can delete a value', (done) => {
    db.del('key', done)
  })

  it('the value stays deleted', (done) => {
    db.get('key', (err) => {
      expect(err).to.exist()
      expect(err.message).to.equal('NotFound')
      done()
    })
  })

  it('allows batch ops', (done) => {
    db.batch(
      [
        { type: 'put', key: 'key 1', value: 'value 1' },
        { type: 'put', key: 'key 2', value: 'value 2' },
        { type: 'del', key: 'key 1' }
      ],
      done)
  })

  it('batch worked', (done) => {
    db.get('key 2', (err, result) => {
      expect(err).to.not.exist()
      expect(result).to.equal('value 2')

      db.get('key 1', (err) => {
        expect(err).to.exist()
        expect(err.message).to.equal('NotFound')
        done()
      })
    })
  })

  it('can be closed', (done) => {
    db.close(done)
  })
})
