# ipfs-level

> Leveldown over IPFS

[![Build Status](https://travis-ci.org/pgte/ipfs-level.svg?branch=master)](https://travis-ci.org/pgte/ipfs-level)

# Install

```bash
$ npm install ipfs-level --save
```

# Import

```js
const IPFSLevel = require('ipfs-level')
```

# API

## IPFSLevel(partition[, options])

Returns a new IPFSLevel instance. This object obeys [the LevelDown interface](https://github.com/level/leveldown).

Arguments:

* `options` (object, defaults to [this](src/default-options.js)): with the following keys:
  * `ipfsOptions` (object). [IPFS options object](https://github.com/ipfs/js-ipfs#advanced-options-when-creating-an-ipfs-node).
  * `log` (LevelDown-compatible database that stores the log)
  * `sync` (boolean, defaults to `false`): EXPERIMENTAL! syncs data between nodes


# Default arguments

You can create a constructor that curries some default arguments by using `IPFSLevel.defaults(options)` like this:

```js
const ipfsLevel = IPFSLevel.defaults({
  log: someLevelDownLogDatabase
})
```

## Sync

TODO: Explain sync


## With Levelup


This default options feature may be useful if you want to pass a constructor into which you'll have no saying about the options, like on the Levelup constructor:

```js
const LevelUp = require('levleup')
const Memdown = require('memdown') // any leveldown db will do for caching log entries
const const IPFSLevel = require('ipfs-level').defaults({
  log: Memdown('some-partition-name') // log database should be scoped to partition
})

const db = LevelUp({ db: IPFSLevel })
// now you have a levelup db you can use
```

# Internals

The internals are documented [here](docs/INTERNALS.md).

# Test and debug

This package uses [debug](https://github.com/visionmedia/debug#readme), so you can activate debug messages by setting the environment variable `DEBUG` to `ipfs-level:*`

# License

MIT

## Contribute

Feel free to join in. All welcome. Open an [issue](https://github.com/pgte/ipfs-level/issues)!

This repository falls under the IPFS [Code of Conduct](https://github.com/ipfs/community/blob/master/code-of-conduct.md).

[![](https://cdn.rawgit.com/jbenet/contribute-ipfs-gif/master/img/contribute.gif)](https://github.com/ipfs/community/blob/master/contributing.md)

## License

[MIT](LICENSE)
