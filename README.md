# <a href="http://lighter.io/ringer" style="font-size:40px;text-decoration:none;color:#000"><img src="https://cdn.rawgit.com/lighterio/lighter.io/master/public/ringer.svg" style="width:90px;height:90px"> Ringer</a>
[![NPM Version](https://img.shields.io/npm/v/ringer.svg)](https://npmjs.org/package/ringer)
[![Downloads](https://img.shields.io/npm/dm/ringer.svg)](https://npmjs.org/package/ringer)
[![Build Status](https://img.shields.io/travis/lighterio/ringer.svg)](https://travis-ci.org/lighterio/ringer)
[![Code Coverage](https://img.shields.io/coveralls/lighterio/ringer/master.svg)](https://coveralls.io/r/lighterio/ringer)
[![Dependencies](https://img.shields.io/david/lighterio/ringer.svg)](https://david-dm.org/lighterio/ringer)
[![Support](https://img.shields.io/gratipay/Lighter.io.svg)](https://gratipay.com/Lighter.io/)


Ringer is a distributed data system, running within your load-balanced
Node.js service. It has redundant caching and storage, with hash ring
orchestration, eventual consistency and latency-optimized routing.

### Advantages

* Ringer is part of your application. It frees you from installing,
  configuring and connecting to multiple data services.

* Ringer uses the very stable and fast LevelDB database via the
  [leveldown](https://www.npmjs.org/package/leveldown) module for
  disk persistence.

* Modern web services have features such as load balancing,
  multi-region deployment, process management, monitoring, auto scaling,
  continuous integration and more. Ringer is part of your service, so
  it gets your service's operational features for free.

* Ringer is crazy fast, because:
  * Peers measure latency and use the fastest routes.
  * One ring handles caching and storage, so network requests are reduced.
  * In-process objects can be modified without deserialization.
  * Requests can route internally because clients can also be peers.
  * Frequently-written data can be pre-aggregated locally.
  * Frequently-accessed data can be cached globally.


#### Install Ringer
```bash
npm install --save ringer
```

#### Use Ringer
```js
var server = require('za')().listen(8888);

var ring = require('ringer')({
  hostPattern: 'app-(eu|use|usw|apn)-(1-5).lighter.io'
});

ring.set('foo', 'bar', function (err) {
  ring.get('foo', function (foo) {
    process.assert(foo == 'bar');
  });
});
```

## ringer([options])

The `ringer` library outputs a `ring` object on which ring methods are
called. A ring object is created with several options that govern the
ring's bootstrapping and behavior.

### options.processCount
The process count specifies the number of CPUs per host, and it defaults
to using all available CPUs.

### options.dataLocation
The database path is where LevelDB data is stored, with a subdirectory for
each worker. The default path is the **data** directory in the current working
directory.

### options.replicas
The replicas setting indicates the number of times to replicate a key-value
pair around the ring for redundancy. This number includes the main entry as
well as duplicates, so it must be at least 1, and the default is **5**.

### options.cacheSize
The cache size indicates the number of items to keep in LRU cache, with the
default being **10,000**.

### options.basePort
The base port is the first port on which processes will listen, with each
subsequent process incrementing that number by 1. The default is 12300. So
if a host has 4 processes and uses the default `basePort`, it will listen
on ports 12300, 12301, 12302 and 12303.

### options.hostPattern
The host pattern is used to bootstrap the host discovery process. It defaults
to the current hostname, so it must be set in order to allow multiple hosts
to interact.

The pattern supports parenthetical expressions which can be pipe-delimited
lists or a start and end number for a sequence. So if you had 10 hosts
across 2 US locales, your host pattern might look like:
`"ringer-us(east|west)-(1-5).domain.tld"`.

### options.isClientOnly (Default: false
If the current process needs to access the ring, but will not store data
as a peer, it should set `isClientOnly` to true. The default is **false**.


### options.discoverDelay (Default: 1e3
The discover delay is the number of milliseconds a peer waits between
attempts to discover all peers. The default is **1,000** (one second).


### options.heartbeatDelay
The heartbeat delay is the number of milliseconds between heartbeat
requests which ping a random peer in order to measure latency. The default
is **100** milliseconds.

### options.log
Ringer can use a custom log. The default is **console**.




## ring

### ring.increment(key[, path, number, fn])

### ring.on()


## Acknowledgements
We would like to thank all of the amazing people who use, support,
promote, enhance, document, patch, and submit comments & issues.
Ringer couldn't exist without you.

We depend on [leveldown](https://www.npmjs.org/package/leveldown), so
thanks are due to [Rod Vagg](https://github.com/rvagg), and everyone
who contributes to LevelDB and related modules.

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
