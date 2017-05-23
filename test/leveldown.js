/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect

const IPFSLevel = require('../')

const PARTITION = 'test'

describe('leveldown interface', () => {
  let db

  it('can create leveldown object', (done) => {
    db = IPFSLevel(PARTITION)
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

  it('can be closed', (done) => {
    db.close(done)
  })
})
