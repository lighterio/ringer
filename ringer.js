var cluster = require('cluster');
var os = require('os');

var Ring = require('./lib/ring');

/**
 * Start a Ringer process on each core.
 */
var ringer = module.exports = function (options) {
  options = options || {};
  options.httpPort = options.httpPort || process.env.PORT || 8888;
  options.firstPort = options.firstPort || process.env.FIRST_PORT || 12300;
  if (!options.server) {
    try {
      options.server = require('za')();
      options.server.listen(options.httpPort);
    }
    catch (e) {
    }
  }
  if (cluster.isMaster) {
    os.cpus().forEach(cluster.fork);
    options.server.close();
  }
  else {
    new Ring(options);
  }
};

// Expose the version number, but only load package JSON if it's requested.
Object.defineProperty(ringer, 'version', {
  get: function () {
    return require('./package.json').version;
  }
});
