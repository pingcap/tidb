![](docs/logo_with_text.png)
1
[![LICENSE](https://img.shields.io/github/license/pingcap/tidb.svg)](https://github.com/pingcap/tidb/blob/master/LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![Build Status](https://travis-ci.org/pingcap/tidb.svg?branch=master)](https://travis-ci.org/pingcap/tidb)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/tidb)](https://goreportcard.com/report/github.com/pingcap/tidb)
[![GitHub release](https://img.shields.io/github/tag/pingcap/tidb.svg?label=release)](https://github.com/pingcap/tidb/releases)
[![GitHub release date](https://img.shields.io/github/release-date/pingcap/tidb.svg)](https://github.com/pingcap/tidb/releases)
[![CircleCI Status](https://circleci.com/gh/pingcap/tidb.svg?style=shield)](https://circleci.com/gh/pingcap/tidb)
[![Coverage Status](https://codecov.io/gh/pingcap/tidb/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/tidb)
[![GoDoc](https://img.shields.io/badge/Godoc-reference-blue.svg)](https://godoc.org/github.com/pingcap/tidb)

## What is TiDB?

TiDB ("Ti" stands for Titanium) is an open-source NewSQL database that supports Hybrid Transactional and Analytical Processing (HTAP) workloads. It is MySQL compatible and features horizontal scalability, strong consistency, and high availability.

- __Horizontal Scalability__

    TiDB expands both SQL processing and storage by simply adding new nodes. This makes infrastructure capacity planning both easier and more cost-effective than traditional relational databases which only scale vertically.

- __MySQL Compatible Syntax__

    TiDB acts like it is a MySQL 5.7 server to your applications. You can continue to use all of the existing MySQL client libraries, and in many cases, you will not need to change a single line of code in your application. Because TiDB is built from scratch, not a MySQL fork, please check out the list of [known compatibility differences](https://docs.pingcap.com/tidb/stable/mysql-compatibility).

- __Distributed Transactions with Strong Consistency__

    TiDB internally shards table into small range-based chunks that we refer to as "Regions". Each Region defaults to approximately 100 MiB in size, and TiDB uses a Two-phase commit internally to ensure that Regions are maintained in a transactionally consistent way.

- __Cloud Native__

    TiDB is designed to work in the cloud -- public, private, or hybrid -- making deployment, provisioning, operations, and maintenance simple.

    The storage layer of TiDB, called TiKV, is a [Cloud Native Computing Foundation (CNCF) Graduated](https://www.cncf.io/announcements/2020/09/02/cloud-native-computing-foundation-announces-tikv-graduation/) project. The architecture of the TiDB platform also allows SQL processing and storage to be scaled independently of each other in a very cloud-friendly manner.

- __Minimize ETL__

    TiDB is designed to support both transaction processing (OLTP) and analytical processing (OLAP) workloads. This means that while you may have traditionally transacted on MySQL and then Extracted, Transformed and Loaded (ETL) data into a column store for analytical processing, this step is no longer required.

- __High Availability__

    TiDB uses the Raft consensus algorithm to ensure that data is highly available and safely replicated throughout storage in Raft groups. In the event of failure, a Raft group will automatically elect a new leader for the failed member, and self-heal the TiDB cluster without any required manual intervention. Failure and self-healing operations are also transparent to applications.

For more details and latest updates, see [TiDB docs](https://docs.pingcap.com/tidb/stable) and [release notes](https://docs.pingcap.com/tidb/dev/release-notes).

## Community

You can join these groups and chats to discuss and ask TiDB related questions:

- [Contributors Mailing list](https://lists.tidb.io/g/contributors)
- [Slack Channel](https://slack.tidb.io/invite?team=tidb-community&channel=everyone&ref=pingcap-tidb)
- [Chinese Forum](https://asktug.com)

In addition, you may enjoy following:

- [@PingCAP](https://twitter.com/PingCAP) on Twitter
- Question tagged [#tidb on StackOverflow](https://stackoverflow.com/questions/tagged/tidb)
- The PingCAP Team [English Blog](https://en.pingcap.com/blog) and [Chinese Blog](https://pingcap.com/blog-cn/)

For support, please contact [PingCAP](http://bit.ly/contact_us_via_github).

## Quick start

### To start using TiDB

See [Quick Start Guide](https://pingcap.com/docs/stable/quick-start-with-tidb/).

### To start developing TiDB

If you want to build TiDB right away, there are two options:

**You have a working [Go environment](https://golang.org/doc/install).**

```
mkdir -p $GOPATH/src/github.com/pingcap
cd $GOPATH/src/github.com/pingcap
git clone https://github.com/pingcap/tidb.git
cd tidb
make
cd bin && ./tidb-server
```

**You have a working [Docker environment](https://docs.docker.com/engine/).**

```
docker pull pingcap/tidb:latest
docker run --name tidb-server -d -p 4000:4000 pingcap/tidb:latest
```

Now you can use official mysql client to connect to TiDB.

```
mysql -h 127.0.0.1 -P 4000 -u root -D test --prompt="tidb> " 
```

## Contributing

The [community repository](https://github.com/pingcap/community) hosts all information about the TiDB community, including how to contribute to TiDB, how TiDB community is governed, how special interest groups are organized, etc.

[<img src="docs/contribution-map.png" alt="contribution-map" width="180">](https://github.com/pingcap/tidb-map/blob/master/maps/contribution-map.md#tidb-is-an-open-source-distributed-htap-database-compatible-with-the-mysql-protocol)

Contributions are welcomed and greatly appreciated. See [Contribution Guide](https://github.com/pingcap/community/blob/master/contributors/README.md) for details on submitting patches and the contribution workflow. For more contributing information, click on the contributor icon above.

## Adopters

View the current list of in-production TiDB adopters [here](https://docs.pingcap.com/tidb/stable/adopters).

## Case studies

- [English](https://pingcap.com/case-studies)
- [简体中文](https://pingcap.com/cases-cn/)

## Roadmap

Read the [Roadmap](https://pingcap.com/docs/ROADMAP).

## Architecture

![architecture](./docs/architecture.png)

## License

TiDB is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Acknowledgments

- Thanks [cznic](https://github.com/cznic) for providing some great open source tools.
- Thanks [GolevelDB](https://github.com/syndtr/goleveldb), [BoltDB](https://github.com/boltdb/bolt), and [RocksDB](https://github.com/facebook/rocksdb) for their powerful storage engines.
