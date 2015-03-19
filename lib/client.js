var os = require('os');
var cluster = require('cluster');
var net = require('net');
var shortenPath = require('../common/fs/shorten-path');
var permute = require('../common/string/permute');
var crc32 = require('../common/crypto/crc32');
var Cache = require('../common/object/lru-cache');
var leveldown = require('leveldown');
var Peer = require('./peer');

/**
 * A client is a peer corresponding to the current process.
 */
var Client = module.exports = Peer.extend({

  // Number of CPUs running ringer processes on this host.
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
  hostPattern: os.hostname().toLowerCase(),

  // True if this process will access the ring, but not be a peer.
  isClientOnly: false,

  // Milliseconds between attempts to discover all peers.
  discoverDelay: 1e2,

  // Milliseconds between heartbeat requests, which track average latency.
  heartbeatDelay: 1e2,

  // Log messages to the console by default.
  log: console,

  /**
   * Create a Ring Client based on configuration options.
   */
  init: function (options) {
    var self = this;

    // Copy options.
    Peer.decorate(self, options);

    // Remember when this client started.
    self.started = Date.now();

    // The ring is unstable until it discovers peers.
    self.isStable = false;
    self.isRebalancing = false;

    // The peers array represents all known peers, whether "up" or "down".
    self.peers = [];

    // Active peers are all peers that were "up" at the last consensus time.
    self.activePeers = [];

    // A larger divisor makes for a slower-changing running mean.
    self.decayDivisor = 1e2;

    // This client is potentially a forked worker.
    self.workerIndex = cluster.worker ? cluster.worker.id - 1 : 0;

    // The current host has a client process, and potentially multiple peers.
    var host = os.hostname().toLowerCase();

    // Create Peer objects for each process on this host.
    for (var index = 0; index < self.processCount; index++) {
      var port = self.basePort + index;
      var name = host + ':' + port;

      // The client is also a peer.
      if (index == self.workerIndex) {
        self.name = name;
        Peer.prototype.init.call(self, name, self);
      }

      // Add this peer to the list of peers.
      self.addPeer(name);
    }

    // Use the host pattern string to bootstrap the list of peers.
    var hosts = permute(self.hostPattern.toLowerCase());
    hosts.forEach(function (permutation) {
      if (permutation != host) {
        self.addPeer(permutation + ':' + self.basePort);
      }
    });

    // Discover any additional peers, and open the database and cache.
    self.schedule(self.discover, 1);
    self.schedule(self.openData, 1);
  },

  /**
   * Create a TCP server to accept incoming connections from peers.
   */
  connect: function () {
    var self = this;

    // Create the server so this client can receive data.
    self.server = net.createServer(function (socket) {
      self.setPeerStatus(self, Peer.UP);
      self.listen(socket);
    });

    // Handle errors by tracking them.
    self.server.on('error', function (error) {
      self.error(error);
    });

    // Listen on this worker's port, and declare the client up when listening.
    self.server.listen(self.port, function () {
      self.log.info('[Ringer] Peer ' + self.name.cyan + ' is listening.');
    });

    self.latency = 0;
  },

  /**
   * Open the LevelDB database.
   */
  openData: function () {
    var self = this;
    self.cache = new Cache({maxSize: self.cacheSize});
    var location = self.dataLocation + '/worker' + self.workerIndex;
    var db = leveldown(location);
    db.open({cacheSize: 0}, function (error) {
      var path = shortenPath(location).cyan;
      if (error) {
        self.log.error('[Ringer] Failed to open ' + path + '.');
      }
      else {
        self.db = db;
        self.log.info('[Ringer] Opened ' + path + ' database.');
      }
    });
  },

  /**
   * Add a peer if it's not already added.
   */
  addPeer: function (name) {
    var self = this;
    var peers = self.peers;
    var peer = peers[name];
    if (!peer) {
      peer = (name == self.name ? self : new Peer(name, self));
      peers.push(peer);
      peers[name] = peer;
      self.setPeerStatus(peer, Peer.UNKNOWN);
    }
    return peer;
  },

  /**
   * Set the status of a peer.
   */
  setPeerStatus: function (peer, status) {
    var self = this;
    if (peer && (peer.status !== status)) {
      peer.status = status;
      self.updateActivePeers();
      if (self.isStable) {
        self.rebalance();
      }
    }
  },

  /**
   * Update the array of active peers.
   */
  updateActivePeers: function () {
    var self = this;
    var peers = self.peers;
    var count = peers.length;
    self.sortPeers();
    self.activePeers = [];
    self.peers.forEach(function (peer) {
      if (peer.status == Peer.UP) {
        self.activePeers.push(peer);
      }
    });
    self.activePeers.hashDivisor = Math.pow(2, 32) / count;
    self.activePeers.replicaOffset = Math.floor(count / self.replicas) || 1;
    var leader = self.activePeers[0];
    self.activePeers.forEach(function (peer) {
      peer.leader = leader;
      peer.isLeader = (peer == leader);
    });
  },

  /**
   * Schedule (or re-schedule) a future run of a function.
   */
  schedule: function (fn, delay) {
    var self = this;
    clearTimeout(fn.timer);
    fn.timer = setTimeout(function () {
      fn.apply(self);
    }, delay || fn.delay || 0);
  },

  /**
   * Build a consensus roster across peers.
   */
  discover: function () {
    var self = this;

    // Remember how many times we've attempted consensus.
    self.discover.attempt = (self.discover.attempt || 0) + 1;

    // Keep a count of requests and matches.
    self.discover.requestCount = 0;
    self.discover.matchCount = 0;

    // Prepare to send this client's data to all peers.
    var data = {
      name: self.name,
      attempt: self.discover.attempt,
      roster: self.getRoster()
    };

    // Attempt to build an identical roster on each peer.
    self.peers.forEach(function (peer) {
      var isSelf = (peer == self);
      var isDown = (peer.status == Peer.DOWN);
      if (!isSelf && !isDown) {
        self.discover.requestCount++;
        peer.send('roster:sync', data);
      }
    });

    // Schedule another attempt in case this one fails.
    self.schedule(self.discover, self.discoverDelay);
  },

  /**
   * Set the current ring as stable.
   */
  stabilize: function () {
    var self = this;
    self.log.info('[Ringer] Peer ' + self.name.cyan +
      ' stabilized'.green +
      ' on attempt ' + ('' + self.discover.attempt).cyan + '.');
    clearTimeout(self.discover.timer);
    self.isStable = true;
    self.updateActivePeers();
    self.schedule(self.heartbeat);
    self.setFlag('stabilized');
  },

  /**
   * Start or continue a rebalance if necessary.
   */
  rebalance: function () {
    var self = this;
    if (self.isStable && !self.isRebalancing) {
      self.log.log('Rebalancing ' + self.name);
      self.isRebalancing = true;
    }
  },

  /**
   * Send a heartbeat request to a random peer.
   */
  heartbeat: function () {
    var self = this;
    var peers = self.activePeers;
    var index = Math.floor(Math.random() * peers.length);
    var peer = peers[index];
    peer.send('heartbeat:start', {start: Date.now()});
    self.schedule(self.heartbeat, self.heartbeatDelay);
  },

  /**
   * A roster is this process's current map of peers and statuses.
   */
  getRoster: function () {
    var self = this;
    var roster = {};
    self.peers.forEach(function (peer) {
      roster[peer.name] = peer.status;
    });
    return roster;
  },

  /**
   * Custom sort function for sorting by host and port.
   */
  sortFn: function (a, b) {
    return a.name < b.name ? -1 : 1;
  },

  /**
   * Reorder the arrays of peers and rebalance peers.
   * NOTE: The array of stable is immutable once set.
   */
  sortPeers: function () {
    var self = this;
    self.peers.sort(self.sortFn);
    (self.rebalancePeers || []).sort(self.sortFn);
  },

  /**
   * Override the peer method to send data directly to the client.
   */
  send: function (type, data) {
    var self = this;
    self.emit(type, data, self);
  },

  /**
   * Find peers that a value hashes to.
   */
  findPeers: function (key) {
    var self = this;
    var peers = self.activePeers;
    var hash = crc32(key);
    var index = Math.floor(hash / peers.hashDivisor);
    var offset = peers.replicaOffset;
    var peerCount = peers.length;
    var found = [];
    var n = Math.min(self.replicas, peerCount);
    for (var i = 0; i < n; i++) {
      found.push(peers[index]);
      index += offset;
      if (index >= peerCount) {
        index -= peerCount;
      }
    }
    found.hash = hash;
    return found;
  },

  /**
   * Get a value from the fastest peer that has it, and run a callback.
   */
  get: function (key, fn) {
    var self = this;
    var peers = self.findPeers(key);
    var best, latency = Number.MAX_VALUE;
    for (var i = 0, n = peers.length; i < n; i++) {
      var peer = peers[i];
      if ((peer.latency < latency) && (peer.status == Peer.UP)) {
        latency = peer.latency;
        best = peer;
      }
    }
    (best || self).getValue(key, fn);
  },

  /**
   * Set a value on all replica peers, and run a callback.
   */
  set: function (key, value, fn) {
    var self = this;
    var peers = self.findPeers(key);
    var count = peers.length;
    for (var i = 0, n = count; i < n; i++) {
      peers[i].setValue(key, value, done);
    }
    function done() {
      if (!--count && fn) {
        fn();
      }
    }
  },

  /**
   * Get a value by its key, and run a callback.
   */
  getValue: function (key, fn) {
    var self = this;
    var value = self.cache.get(key);
    if (value === undefined) {
      self.db.get(key, Client.dbOptions, function (error, value) {
        if (value !== undefined) {
          self.cache.set(key, value);
        }
        fn('' + value);
      });
    }
    else {
      fn(value);
    }
  },

  /**
   * Set a value for a key, and run a callback.
   */
  setValue: function (key, value, fn) {
    var self = this;
    self.cache.set(key, value);
    self.db.put(key, value, fn);
  }

});

Client.dbOptions = {fillCache: false, asBuffer: false};
