# <a href="http://lighter.io/ringer" style="font-size:40px;text-decoration:none;color:#000"><img src="https://cdn.rawgit.com/lighterio/lighter.io/master/public/ringer.svg" style="width:90px;height:90px"> Ringer</a>
[![NPM Version](https://img.shields.io/npm/v/ringer.svg)](https://npmjs.org/package/ringer)
[![Downloads](https://img.shields.io/npm/dm/ringer.svg)](https://npmjs.org/package/ringer)
[![Build Status](https://img.shields.io/travis/lighterio/ringer.svg)](https://travis-ci.org/lighterio/ringer)
[![Code Coverage](https://img.shields.io/coveralls/lighterio/ringer/master.svg)](https://coveralls.io/r/lighterio/ringer)
[![Dependencies](https://img.shields.io/david/lighterio/ringer.svg)](https://david-dm.org/lighterio/ringer)
[![Support](https://img.shields.io/gratipay/Lighter.io.svg)](https://gratipay.com/Lighter.io/)


Ringer is a distributed data engine, running within your load-balanced
Node.js service. It has redundant caching and storage, with hash ring
orchestration, eventual consistency and latency-optimized routing.

### Advantages

* Ringer is part of your application. It frees you from installing,
  configuring and connecting to multiple data services.

* Ringer comes with the very stable and popular
  [lru-cache](https://www.npmjs.org/package/lru-cache) and
  [leveldown](https://www.npmjs.org/package/leveldown) modules.

* Modern web services have features such as load balancing,
  multi-region deployment, process management, monitoring, auto scaling,
  continuous integration and more. Ringer is part of your service, so
  it gets your service's operational features for free.

* Ringer is crazy fast, because:
  * Peers measure read times and use the fastest routes.
  * Cache and storage use one ring. Cache misses immediately become disk hits.
  * In-process objects can be modified without deserialization.
  * In an `N` host ring with `R` redundancy, `R / N` requests are internal.
  * Frequently-written data gets local pre-aggregation.
  * Frequently-read data gets global redundancy.


#### Install Ringer
```bash
npm install --save ringer
```

#### Use Ringer
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
for its HTTP routing. Hosts behind a load balancer can be rostered using
`/ringer/peers`. Additionally, Ringer includes a simple UI for visualizing
your ring peers and ring data, and it is exposed at `/ringer`.

If `server` is omitted, Ringer will attempt to start a Za server and listen
on the port specified by `httpPort`.

### options.processCount

Ringer peers run on multiple hosts, with multiple processes per host (by
default, one process per CPU). If you want to limit Ringer's process usage,
you can give a numerical value for `processCount`.

### options.port

Ringer directs traffic to each process using a different port. The ports
are numbered in order starting with `firstPort`, which defaults to `12300`.

### options.url

Ringer rosters peers by sending signed requests through a load balancer.
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


## Acknowledgements
We would like to thank all of the amazing people who use, support,
promote, enhance, document, patch, and submit comments & issues.
Ringer couldn't exist without you.

We depend on [lru-cache](https://www.npmjs.org/package/lru-cache),
[leveldown](https://www.npmjs.org/package/leveldown) and
[xxhash](https://www.npmjs.org/package/xxhash), so thanks are due to
[Isaac Schlueter](https://github.com/isaacs),
[Rod Vagg](https://github.com/rvagg),
[C J Silverio](https://github.com/ceejbot), and all of their contributors.

Additionally, huge thanks go to [TUNE](http://www.tune.com) for employing
and supporting [Ringer](http://lighter.io/ringer) project maintainers,
and for being an epically awesome place to work (and play).


## MIT License

Copyright (c) 2014 Sam Eubank

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


## How to Contribute
We welcome contributions from the community and are happy to have them.
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
   [style guide](http://lighter.io/style-guide).
3. Add tests for your new code, ensuring that you have 100% code coverage.
   (If necessary, we can help you reach 100% prior to merging.)
   * Run `npm test` to run tests quickly, without testing coverage.
   * Run `npm run cover` to test coverage and generate a report.
   * Run `npm run report` to open the coverage report you generated.
4. [Pull requests](http://help.github.com/send-pull-requests/) should be made
   to the [master branch](https://github.com/lighterio/ringer/tree/master).

### Contributor Code of Conduct

As contributors and maintainers of Ringer, we pledge to respect all
people who contribute through reporting issues, posting feature requests,
updating documentation, submitting pull requests or patches, and other
activities.

If any participant in this project has issues or takes exception with a
contribution, they are obligated to provide constructive feedback and never
resort to personal attacks, trolling, public or private harassment, insults, or
other unprofessional conduct.

Project maintainers have the right and responsibility to remove, edit, or
reject comments, commits, code, edits, issues, and other contributions
that are not aligned with this Code of Conduct. Project maintainers who do
not follow the Code of Conduct may be removed from the project team.

Instances of abusive, harassing, or otherwise unacceptable behavior may be
reported by opening an issue or contacting one or more of the project
maintainers.

We promise to extend courtesy and respect to everyone involved in this project
regardless of gender, gender identity, sexual orientation, ability or
disability, ethnicity, religion, age, location, native language, or level of
experience.
