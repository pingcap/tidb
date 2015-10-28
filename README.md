![logo](./docs/logo_with_text.png)
[![Build Status](https://travis-ci.org/pingcap/tidb.svg?branch=master)](https://travis-ci.org/pingcap/tidb)
## What is TiDB?

TiDB is a distributed SQL database.
Inspired by the design of Google [F1](http://research.google.com/pubs/pub41344.html), TiDB supports the best features of both traditional RDBMS and NoSQL.

- __Horizontal scalability__  
Grow TiDB as your business grows. You can increase the capacity simply by adding more machines.

- __Asynchronous schema changes__  
Evolve TiDB schemas as your requirement evolves. You can add new columns and indices without stopping or affecting the on-going operations.

- __Consistent distributed transactions__  
Think TiDB as a single-machine RDBMS. You can start a transaction that acrosses multiple machines without worrying about consistency. TiDB makes your application code simple and robust.

- __Compatible with MySQL protocol__  
Use TiDB as MySQL. You can replace MySQL with TiDB to power your application without changing a single line of code in most cases.

- __Written in Go__  
Enjoy TiDB as much as we love Go. We believe Go code is both easy and enjoyable to work with. Go makes us improve TiDB fast and makes it easy to dive into the codebase.

- __Multiple storage engine support__  
Power TiDB with your most favorite engines. TiDB supports most of the popular storage engines in single-machine mode. You can choose from goleveldb, LevelDB, RocksDB, LMDB, BoltDB and even more to come.

## Status

TiDB is at its early age and under heavy development, some of the features mentioned above have not been fully implemented.

__Please do not use it in production.__

## Roadmap

Read the [Roadmap](./ROADMAP.md).

## Quick start

#### __Pre-requirement__

Go environment. Currently a 64-bit version of go >= 1.5 is required.
```
git clone https://github.com/pingcap/tidb.git $GOPATH/src/github.com/pingcap/tidb
cd $GOPATH/src/github.com/pingcap/tidb
make
```

#### __Run command line interpreter__

Interpreter is an interactive command line TiDB client.
You can just enter some SQL statements and get the result.
```
make interpreter
cd interpreter && ./interpreter
```
Press `Ctrl+C` to quit.

#### __Run as go library__

See [USAGE.md](./docs/USAGE.md) for detailed instructions to use TiDB as library in Go code.

#### __Run as MySQL protocol server__

```
make server
cd tidb-server && ./tidb-server
```

In case you want to compile a specific location:

```
make server TARGET=$GOPATH/bin/tidb-server
```

The default server port is `4000` and can be changed by flag `-P <port>`.

Run `./tidb-server -h` to see more flag options.

After you started tidb-server, you can use official mysql client to connect to TiDB.

```
mysql -h 127.0.0.1 -P 4000 -u root -D test
```

#### __Run as MySQL protocol server with distributed transactional KV storage engine__

Comming soon.

## Architecture

![architecture](./docs/architecture.png)

## Contributing
Contributions are welcomed and greatly appreciated. See [CONTRIBUTING.md](./docs/CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## Follow us

Twitter: [@PingCAP](https://twitter.com/PingCAP)

## License
TiDB is under the Apache 2.0 license. See the [LICENSE](./LICENSES/LICENSE) file for details.

## Acknowledgments
- Thanks [cznic](https://github.com/cznic) for providing some great open source tools.
- Thanks [GolevelDB](https://github.com/syndtr/goleveldb), [LMDB](https://github.com/LMDB/lmdb), [BoltDB](https://github.com/boltdb/bolt) and [RocksDB](https://github.com/facebook/rocksdb) for their powerful storage engines.
