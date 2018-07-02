![](docs/logo_with_text.png)

[![Build Status](https://travis-ci.org/pingcap/tidb.svg?branch=master)](https://travis-ci.org/pingcap/tidb)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/tidb)](https://goreportcard.com/report/github.com/pingcap/tidb)
![GitHub release](https://img.shields.io/github/release/pingcap/tidb.svg)
[![CircleCI Status](https://circleci.com/gh/pingcap/tidb.svg?style=shield)](https://circleci.com/gh/pingcap/tidb)
[![Coverage Status](https://coveralls.io/repos/github/pingcap/tidb/badge.svg?branch=master)](https://coveralls.io/github/pingcap/tidb?branch=master)

## What is TiDB?

TiDB (The pronunciation is: /'taɪdiːbi:/ tai-D-B, etymology: titanium) is an open-source distributed scalable Hybrid Transactional and Analytical Processing (HTAP) database. It features infinite horizontal scalability, strong consistency, and high availability. TiDB is MySQL compatible and serves as a one-stop data warehouse for both OLTP (Online Transactional Processing) and OLAP (Online Analytical Processing) workloads.

- __Horizontal scalability__

    TiDB provides horizontal scalability simply by adding new nodes. Never worry about infrastructure capacity ever again.

- __MySQL compatibility__

    Easily replace MySQL with TiDB to power your applications without changing a single line of code in most cases and still benefit from the MySQL ecosystem.

- __Distributed transaction__

    TiDB is your source of truth, guaranteeing ACID compliance, so your data is accurate and reliable anytime, anywhere.

- __Cloud Native__

    TiDB is designed to work in the cloud -- public, private, or hybrid -- making deployment, provisioning, and maintenance drop-dead simple.

- __No more ETL__

    ETL (Extract, Transform and Load) is no longer necessary with TiDB's hybrid OLTP/OLAP architecture, enabling you to create new values for your users, easier and faster.

- __High availability__

    With TiDB, your data and applications are always on and continuously available, so your users are never disappointed.

For more details, see [How we build TiDB](https://pingcap.github.io/blog/2016/10/17/how-we-build-tidb/).

## Adopters

You can view the list of TiDB adopters [here](https://github.com/pingcap/docs/blob/master/adopters.md).

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

- [**Contact PingCAP Team**](http://bit.ly/contact_us_via_github)
- **Twitter**: [@PingCAP](https://twitter.com/PingCAP)
- **Reddit**: https://www.reddit.com/r/TiDB/
- **Stack Overflow**: https://stackoverflow.com/questions/tagged/tidb
- **Mailing list**: [Google Group](https://groups.google.com/forum/#!forum/tidb-user)

## License
TiDB is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Acknowledgments
- Thanks [cznic](https://github.com/cznic) for providing some great open source tools.
- Thanks [GolevelDB](https://github.com/syndtr/goleveldb), [BoltDB](https://github.com/boltdb/bolt), and [RocksDB](https://github.com/facebook/rocksdb) for their powerful storage engines.
