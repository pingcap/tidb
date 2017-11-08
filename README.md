![](docs/logo_with_text.png)

[![Build Status](https://travis-ci.org/pingcap/tidb.svg?branch=master)](https://travis-ci.org/pingcap/tidb)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/tidb)](https://goreportcard.com/report/github.com/pingcap/tidb)
![Project Status](https://img.shields.io/badge/version-1.0-green.svg)
[![CircleCI Status](https://circleci.com/gh/pingcap/tidb.svg?style=shield)](https://circleci.com/gh/pingcap/tidb)
[![Coverage Status](https://coveralls.io/repos/github/pingcap/tidb/badge.svg?branch=master)](https://coveralls.io/github/pingcap/tidb?branch=master)

## What is TiDB?

TiDB (The pronunciation is: /'taɪdiːbi:/ tai-D-B, etymology: titanium) is a Hybrid Transactional/Analytical Processing (HTAP) database. Inspired by the design of Google F1 and Google Spanner, TiDB features infinite horizontal scalability, strong consistency, and high availability. The goal of TiDB is to serve as a one-stop solution for online transactions and analyses.

- __Horizontal scalability__

Grow TiDB as your business grows. You can increase the capacity for storage and computation simply by adding more machines.

- __Compatible with MySQL protocol__

Use TiDB as MySQL. You can replace MySQL with TiDB to power your application without changing a single line of code in most cases.

- __Automatic Failover and high availability__

Your data and applications are always-on. TiDB automatically handles malfunctions and protects your applications from machine failures or even downtime of an entire data-center.

- __Consistent distributed transactions__

Think of TiDB as a single-machine RDBMS. You can start a transaction that crosses multiple machines without worrying about consistency. TiDB makes your application code simple and robust.

- __Online DDL__

Evolve TiDB schemas as your requirement changes. You can add new columns and indexes without stopping or affecting the on-going operations.

- __Multiple storage engine support__

Power TiDB with your most favorite engines. TiDB supports local storage engines such as GolevelDB and BoltDB, as well as [TiKV](https://github.com/pingcap/tikv), a distributed storage engine.

For more details, see [How we build TiDB](https://pingcap.github.io/blog/2016/10/17/how-we-build-tidb/).

## Roadmap

Read the [Roadmap](https://github.com/pingcap/docs/blob/master/ROADMAP.md).

## Quick start

Read the [Quick Start](https://pingcap.com/doc-QUICKSTART).

## Documentation

+ [English](https://pingcap.com/docs)
+ [简体中文](https://pingcap.com/docs-cn)

## Architecture

![architecture](./docs/architecture.png)

## Contributing
Contributions are welcomed and greatly appreciated. See [CONTRIBUTING.md](CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## Connect with us

- **Twitter**: [@PingCAP](https://twitter.com/PingCAP)
- **Reddit**: https://www.reddit.com/r/TiDB/
- **Stack Overflow**: https://stackoverflow.com/questions/tagged/tidb
- **Mailing list**: [Google Group](https://groups.google.com/forum/#!forum/tidb-user)

## License
TiDB is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Acknowledgments
- Thanks [cznic](https://github.com/cznic) for providing some great open source tools.
- Thanks [GolevelDB](https://github.com/syndtr/goleveldb), [BoltDB](https://github.com/boltdb/bolt), and [RocksDB](https://github.com/facebook/rocksdb) for their powerful storage engines.
