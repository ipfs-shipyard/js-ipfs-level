'use strict'

module.exports = {
  karma: {
    files: [{
      pattern: 'test/fixtures/**/*.js',
      watched: false,
      served: true,
      included: false
    }]
  }
}
