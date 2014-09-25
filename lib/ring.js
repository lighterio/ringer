var os = require('os');
var cluster = require('cluster');
var net = require('net');
var za = require('za');
var db = require('leveldown');
var cache = require('lru-cache');
var Peer = require('./peer');

var app;
var log;

module.exports = Ring;

/**
 * Apply options to the empty ring object, and export it.
 */
function Ring(options) {
  var ring = this;
  for (var key in options) {
    ring[key] = options[key];
  }
  app = ring.server || za().listen(ring.httpPort || 8888);
  log = ring.logger || console;

  var router = app.routers.https || app.routers.http;
  var host = os.hostname().toLowerCase();
  var port = options.firstPort - 1 + cluster.worker.id;
  ring.self = {
    host: host,
    port: port,
    name: host + ':' + port
  };
  ring.url = ring.url || (router.protocol + '://' + host + ':' + router.port + '/ringer');
  ring.url = ring.url.replace(/\/$/, '');
  ring.href = ring.url.replace(/https?:\/\/[^\/]+/, '');

  ring.sortPeers = ring.sortPeers || function (a, b) {
    return a.name < b.name ? -1 : 1;
  };

  ring.peers = [];
  ring.addPeer(ring.self.name);

  app.get(ring.href + '/peers', function (request, response) {
    if (request.query.add) {
      ring.addPeer(request.query.add);
    }
    var peers = [];
    ring.peers.forEach(function (peer) {
      peers.push(peer.name);
    });
    response.writeHead(200, {'content-type': 'text/plain'});
    response.end(peers.join(','));
  });

  ring.listen();
}

/**
 * Listen for messages.
 */
Ring.prototype.listen = function () {
  var ring = this;
  ring.server = net.createServer(function (socket) {
    var data = '';
    socket.on('end', function () {
      log.warn('[Ringer] Server disconnected.');
    });
    socket.on('data', function (chunk) {
      data += chunk;
      var messages = data.split(/\n/g);
      data = messages.pop();
      messages.forEach(ring.receive);
    });
  });
  ring.server.listen(ring.self.port, function (connection) {
    log.info('[Ringer] Peer listening at ' + ring.self.name + '.');
    ring.getPeers();
  });

};

/**
 * Handle a message that the ring has received.
 */
Ring.prototype.receive = function (message) {
  var ring = this;
  log.log('[Ringer] Received message: "' + message + '".');
};

/**
 * Get a list of peers from a random peer via the load balancer.
 */
Ring.prototype.getPeers = function () {
  var ring = this;
  var protocol = ring.url.split(':')[0];
  var httpOrHttps = require(protocol);
  var url = ring.url + '/peers?add=' + encodeURIComponent(ring.self.name);
  var request = httpOrHttps.get(url, function (response) {
    var data = '';
    response.on('data', function (chunk) {
      data += chunk;
    });
    response.on('end', function () {
      data.split(',').forEach(function (name) {
        ring.addPeer(name);
      });
    });
  });
  request.on('error', function (error) {
    log.error('[Ringer] Failed to get peers from "' + url + '".', error);
  });
};

/**
 * Create a peer by "host:port", and add it to the collection.
 */
Ring.prototype.addPeer = function (name) {
  var ring = this;
  if (!ring.peers[name]) {
    var peer = new Peer(name, ring);
    ring.peers.push(peer);
    ring.peers.sort(ring.sortPeers);
    ring.peers[name] = peer;
  }
};
