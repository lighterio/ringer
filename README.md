# Ringer

[![NPM Version](https://img.shields.io/npm/v/ringer.svg) ![Downloads](https://img.shields.io/npm/dm/ringer.svg)](https://npmjs.org/package/ringer)
[![Build Status](https://img.shields.io/travis/lighterio/ringer.svg)](https://travis-ci.org/lighterio/ringer)
[![Code Coverage](https://img.shields.io/coveralls/lighterio/ringer/master.svg)](https://coveralls.io/r/lighterio/ringer)
[![Dependencies](https://img.shields.io/david/lighterio/ringer.svg)](https://david-dm.org/lighterio/ringer)
[![Support](https://img.shields.io/gratipay/Lighter.io.svg)](https://gratipay.com/Lighter.io/)

## TL;DR

Ringer is a distributed data engine that runs within your load-balanced
Node.js web service. It features redundant caching and storage, with
built-in orchestration, eventual consistency and latency-optimized routing.

### Advantages

* Modern web services have features such as load balancing,
  multi-region deployment, monitoring, auto scaling, continuous integration,
  and more. When your data layer is inside your service, it gets your
  service's operational features for free.

* Configuring web services to connect to multiple caching and storage
  services is a pain - especially with multiple environments
  such as `dev`, `stage` and `prod`.

* Ringer has incredibly low latency because:
  1. In an `N` host ring with `R` redundancy, `(N - R) / N` requests route to localhost.
  2. Peers track latency, so they can request data from nearby peers.
  3. Cache and storage are on the same ring, so cache misses can immediately become disk hits.
  4. Cached data does not need to be deserialized before it is operated upon.

* Ringer peers use LevelDB and [lru-cache](https://www.npmjs.org/package/lru-cache)

#### Install Ringer
```bash
npm install --save ringer
```

#### Add Ringer to Za
```javascript
var server = require('za')().listen(8888);

var ring = require('ringer')({
  appServer: server
});

server.get('/', function (request, response) {
  var collection = 'user-agent';
  var key = request.headers[collection];
  ring.increment(collection, key, function (error, count) {
    if (error) {
      response.send('Error: ' + error);
    } else {
      response.send(count + ' requests from ' + key);
    }
  });
});

```

## ringer([options])

The `ringer` library outputs a `ring` object on which ring methods are called.
The ring object is created with several options that govern the ring's
bootstrapping and behavior.

When called without an options object, Ringer uses defaults:
```javascript
var ring = require('ringer')();
```
These are the defaults:
```javascript
var ring = require('ringer')({
  server: null,                      // Create a new Za server.
  httpPort: 8888,                    // Use the default HTTP port.
  processCount: 0,                   // Use all available cores.
  firstPort: 12300,
  url: 'http://localhost:8888/', // Orchestrate localhost.
  redundancy: 4,                     // Store data in 4 places.
  latencyExploration: 0.05           // Try a slower host 5% of the time.
  sortPeers: function (a, b) {        // Sort peers by name.
    return a.name < b.name ? -1 : 1;
  }
});
```

### options.server

Ringer is designed for use with load-balanced web services, and it uses Za
for its HTTP routing. Hosts behind a load balancer can be discovered using
`/ringer/peers`. Additionally, Ringer includes a simple UI for visualizing
your ring peers and ring data, and it is exposed at `/ringer`.

If `server` is omitted, Ringer will attempt to start a Za server and listen
on the port specified by `httpPort`.

### options.httpPort

If Ringer starts a Za server due to the `server` option being omitted, it
makes that server listen on the port specified by `httpPort`. If both
`server` and `httpPort` are omitted, Ringer starts a server on port 8888.

### options.processCount

Ringer peers run on multiple hosts, with multiple processes per host (by
default, one process per CPU). If you want to limit Ringer's process usage,
you can give a numerical value for `processCount`.

### options.firstPort

Ringer directs traffic to each process using a different port. The ports
are numbered in order starting with `firstPort`, which defaults to `12300`.

### options.url

Ringer discovers peers by sending signed requests through a load balancer.
In a production environment, `url` should point to the web service's
external-facing URL, and in a development environment, it can point to the
default: `"http://localhost:8888/ringer"`.

### options.redundancy

Each piece of data can be stored in a specified number of places for
redundancy. Low redundancy decreases reliability in the case of host
failures and datacenter outages. High redundancy uses more memory and disk
(as well as network and CPU, to a lesser extent).

### options.latencyExploration

Peers keep track of the average latency to every other peer. Latency
exploration is the percentage of queries for which a host will request data
from a slower host in addition to the fastest host and use the first result
that arrives. The default is `0.05`, meaning 5% exploration.

### options.sortPeers

The topology of the ring is represented by an array of peers, sorted
with a custom sort function. By default, peers are sorted by name, which
is of the form `"host:port"`.

## ring

### ring.increment(key[, path, number, fn])

### ring.on()

## How to Contribute
We welcome contributions from the community and are pleased to have them.
Please follow this guide when logging issues or making code changes.

### Logging Issues
All issues should be created using the
[new issue form](https://github.com/lighterio/ringer/issues/new).
Please describe the issue including steps to reproduce. Also, make sure
to indicate the version that has the issue.

### Changing Code
Code changes are welcome and encouraged! Please follow our process:

1. Fork the repository on GitHub.
2. Fix the issue ensuring that your code follows the
   [style guide](http://lighter.io/style).
3. Add tests for your new code, ensuring that you have 100% code coverage.
   (If necessary, we can help you reach 100% prior to merging.)
   * Run `npm test` to run tests quickly, without testing coverage.
   * Run `npm run cover` to test coverage and generate a report.
   * Run `npm run report` to open the coverage report you generated.
4. [Pull requests](http://help.github.com/send-pull-requests/) should be made to the [master branch](https://github.com/lighterio/ringer/tree/master).
