var cluster = require('cluster');
var os = require('os');
var Client = require('./lib/client');
var Type = require('./common/object/Type');
require('./common/json/stringify');

/**
 * Start a Ringer process on each core.
 */
var ringer = module.exports = function (config) {

  // Decorate the default config with the provided config.
  config = Type.decorate({

    // Number of CPUs to run processes on.
    processCount: os.cpus().length,

    // Number of times to replicate a key-value pair for redundancy.
    replicas: 5,

    // Number of items to keep in LRU cache.
    cacheSize: 1e5,

    // Location of the LevelDB database.
    dataLocation: process.cwd() + '/data',

    // First process's port (incrementing for each subsequent process).
    basePort: 12300,

    // Bootstrapping pattern, (e.g. "ringer-us(east|west)-(0-99).domain.tld").
    hostPattern: os.hostname(),

    // True if this process will access the ring, but not be a peer.
    isClientOnly: false,

    // Milliseconds between attempts to discover all peers.
    discoverDelay: 1e3,

    // Milliseconds between heartbeat requests, which track average latency.
    heartbeatDelay: 1e2,

    // Logger for ring debugging.
    logger: console,

    // Path to LevelDB data on each member.
    dbPath: './data'

  }, config);

  // From the master process of a multi-process ring, fork workers.
  if (cluster.isMaster && (config.processCount > 1)) {
    for (var i = 0; i < config.processCount; i++) {
      cluster.fork();
    }
  }
  // From a worker process, return a ring Client.
  else {
    return new Client(config);
  }
};

// Expose the version number, but only load package JSON if it's requested.
Object.defineProperty(ringer, 'version', {
  get: function () {
    return require(__dirname + '/package.json').version;
  }
});

// Fill in string color functions that don't exist.
function returnThis() { return this; };
String.prototype.cyan = String.prototype.cyan || returnThis;
String.prototype.green = String.prototype.green || returnThis;
