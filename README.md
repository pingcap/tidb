![](docs/logo_with_text.png)

[![Build Status](https://travis-ci.org/pingcap/tidb.svg?branch=master)](https://travis-ci.org/pingcap/tidb)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/tidb)](https://goreportcard.com/report/github.com/pingcap/tidb)
![Project Status](https://img.shields.io/badge/status-rc-yellow.svg)
[![CircleCI Status](https://circleci.com/gh/pingcap/tidb.svg?style=shield)](https://circleci.com/gh/pingcap/tidb)

## What is TiDB?

TiDB (The pronunciation is: /'taɪdiːbi:/ tai-D-B, etymology: titanium) is a distributed SQL database.
Inspired by the design of Google F1 and Google Spanner, TiDB supports the best features of both traditional RDBMS and NoSQL.

- __Horizontal scalability__
Grow TiDB as your business grows. You can increase the capacity simply by adding more machines.

- __Asynchronous schema changes__
Evolve TiDB schemas as your requirement evolves. You can add new columns and indices without stopping or affecting the on-going operations.

- __Consistent distributed transactions__
Think of TiDB as a single-machine RDBMS. You can start a transaction that crosses multiple machines without worrying about consistency. TiDB makes your application code simple and robust.

- __Compatible with MySQL protocol__
Use TiDB as MySQL. You can replace MySQL with TiDB to power your application without changing a single line of code in most cases.

- __Written in Go__
Enjoy TiDB as much as we love Go. We believe Go code is both easy and enjoyable to work with. Go makes us improve TiDB fast and makes it easy to dive into the codebase.

- __NewSQL over TiKV__
  Turn [TiKV](https://github.com/pingcap/tikv) into NewSQL database.

- __Multiple storage engine support__
Power TiDB with your most favorite engines. TiDB supports many popular storage engines in single-machine mode. You can choose from GolevelDB, LevelDB, RocksDB, LMDB, BoltDB and even more to come.

For more details, read our blog [How we build TiDB](https://pingcap.github.io/blog/2016/10/17/how-we-build-tidb/)

## Roadmap

Read the [Roadmap](./docs/ROADMAP.md).

## Quick start

Read the [Quick Start](./docs/QUICKSTART.md).

## Documentation

+ [English](https://github.com/pingcap/docs)
+ [简体中文](https://github.com/pingcap/docs-cn)

## Architecture

![architecture](./docs/architecture.png)

## Contributing
Contributions are welcomed and greatly appreciated. See [CONTRIBUTING.md](CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## Follow us

### Twitter

[@PingCAP](https://twitter.com/PingCAP)

### Mailing list

tidb-user@googlegroups.com

[Google Group](https://groups.google.com/forum/#!forum/tidb-user)

## License
TiDB is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Acknowledgments
- Thanks [cznic](https://github.com/cznic) for providing some great open source tools.
- Thanks [GolevelDB](https://github.com/syndtr/goleveldb), [LMDB](https://github.com/LMDB/lmdb), [BoltDB](https://github.com/boltdb/bolt) and [RocksDB](https://github.com/facebook/rocksdb) for their powerful storage engines.
