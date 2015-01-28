var os = require('os');
var cluster = require('cluster');
var net = require('net');
var shortenPath = require('../common/fs/shorten-path');
var Emitter = require('../common/event/emitter');
var permute = require('../common/string/permute');
var crc32 = require('../common/crypto/crc32');
var Cache = require('../common/object/lru-cache');
var leveldown = require('leveldown');
var Peer = require('./peer');
var NOTHING = 0; // Zero is falsy, but also propertiable.

/**
 * A client is a peer corresponding to the current process.
 */
var Client = module.exports = Peer.extend({

  /**
   * Create a Ring based on configuration options.
   */
  init: function (config) {
    var self = this;

    // Remember the configuration.
    self.config = config;

    // Copy configuration options.
    self.logger = config.logger;
    self.replicas = config.replicas;
    self.cacheSize = config.cacheSize;
    self.dataLocation = config.dataLocation;
    self.discover.delay = config.discoverDelay;
    self.heartbeat.delay = config.heartbeatDelay;

    // Remember when this client started.
    self.started = Date.now();

    // The ring is unstable until it discovers peers.
    self.isStable = false;
    self.isRebalancing = false;

    // The peers array represents all known peers, whether "up" or "down".
    self.peers = [];

    // Stable peers are all peers that were "up" at the last consensus time.
    self.stablePeers = null;

    // Proposed peers are all peers that are "up" and awaiting consensus.
    self.proposedPeers = null;

    // A larger divisor makes for a slower-changing running mean.
    self.decayDivisor = 1e2;

    // The number to divide a CRC32 hash by to get a stable peer index.
    self.stableHashDivisor = 1;

    // This client is potentially a forked worker.
    self.workerIndex = cluster.worker ? cluster.worker.id - 1 : 0;

    // The current host has a client process, and potentially multiple peers.
    var host = os.hostname();

    // Create Peer objects for each process on this host.
    for (var index = 0; index < config.processCount; index++) {
      var port = config.basePort + index;
      var name = host + ':' + port;
      var peer = null;

      // The client is also a peer.
      if (index == self.workerIndex) {
        peer = self;
        self.peers.push(self);
        self.peers[name] = self;
        Peer.prototype.init.call(self, name, self);
      }
      // Any other worker process acts as a Peer.
      if (!config.isClientOnly) {
        peer = peer || self.addPeer(name);
      }
    }

    // Use the host pattern string to bootstrap the list of peers.
    var hosts = permute(config.hostPattern);
    hosts.forEach(function (host) {
      if (host != self.client.host) {
        self.addPeer(host + ':' + config.basePort);
      }
    });

    // Declare this client to be up (because it's obviously running).
    self.status = Peer.UP;

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
      self.listen(socket);
    });

    // Handle errors by tracking them.
    self.server.on('error', function (error) {
      self.error(error);
    });

    // Listen on this worker's port, and declare the client up when listening.
    self.server.listen(self.port, function () {
      self.status = Peer.UP;
      self.logger.info('[Ringer] Peer ' + self.name.cyan + ' is listening.');
    });

    self.latency = 0;
  },

  /**
   * Add a peer if it's not already added.
   */
  addPeer: function (name) {
    var self = this;
    var peers = self.peers;
    var peer = peers[name];
    if (!peer) {
      peer = new Peer(name, self);
      peers.push(peer);
      peers[peer.name] = peer;
      self.sortPeers();
      self.rebalance();
    }
    return peer;
  },

  /**
   * Set the status of a peer.
   */
  setPeerStatus: function (name, status) {
    var self = this;
    var peer = self.peers[name];
    if (peer && (peer.status != status)) {
      peer.status = status;
      self.rebalance();
    }
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
    self.schedule(self.discover);
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
        self.logger.error('[Ringer] Failed to open ' + path + '.');
      }
      else {
        self.db = db;
        self.logger.info('[Ringer] Opened ' + path + ' database.');
      }
    });
  },

  /**
   * Set the current ring as stable.
   */
  stabilize: function () {
    var self = this;
    self.logger.info('[Ringer] Peer ' + self.client.name.cyan +
      ' stabilized'.green +
      ' on attempt ' + ('' + self.discover.attempt).cyan + '.');
    clearTimeout(self.discover.timer);
    self.isStable = true;
    self.stablePeers = [];
    self.peers.forEach(function (peer) {
      peer.isLeader = false;
      if (peer.status == Peer.UP) {
        if (!self.stablePeers.length) {
          peer.isLeader = true;
          self.leader = peer;
        }
        self.stablePeers.push(peer);
      }
    });
    var peerCount = self.stablePeers.length;
    self.stablePeers.hashDivisor = Math.pow(2, 32) / peerCount;
    self.stablePeers.replicaOffset = Math.floor(peerCount / self.replicas);
    self.schedule(self.heartbeat);
    self.setFlag('stabilized');
  },

  /**
   * Start or continue a rebalance if necessary.
   */
  rebalance: function () {
    var self = this;
    if (self.isStable && !self.isRebalancing) {
      self.logger.log('Rebalancing ' + self.client.name);
      self.isRebalancing = true;
    }
  },

  /**
   * Send a heartbeat request to a random peer.
   */
  heartbeat: function () {
    var self = this;
    var peers = self.stablePeers;
    var index = Math.floor(Math.random() * peers.length);
    var peer = peers[index];
    peer.send('heartbeat:start', {start: Date.now()});
    self.schedule(self.heartbeat);
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
  findPeers: function (key, peers) {
    var self = this;
    peers = peers || self.stablePeers;
    var hash = crc32(key);
    var index = Math.floor(hash / peers.hashDivisor);
    var offset = peers.replicaOffset;
    var count = peers.length;
    var found = [];
    for (var i = 0, n = self.replicas; i < n; i++) {
      found.push(peers[index]);
      index += offset;
      if (index >= count) {
        index -= count;
      }
    }
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
      if (peer.latency < latency && peer.status == Peer.UP) {
        latency = peer.latency;
        best = peer;
      }
    }
    best.getValue(key, fn);
  },

  /**
   * Set a value on all replica peers, and run a callback.
   */
  set: function (key, value, fn) {
    var self = this;
    var peers = self.findPeers(key);
    var wait = peers.length;
    for (var i = 0, n = wait; i < n; i++) {
      peers[i].setValue(key, value, done);
    }
    function done() {
      if (!--wait) {
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
    if (value == undefined) {
      self.db.get(key, function (value) {
        if (value instanceof Error) {
          value = undefined;
        }
        else if (value) {
          self.cache.set(key, value);
        }
        fn(value);
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
