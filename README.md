<img src="docs/tidb-logo-with-text.png" alt="TiDB, a distributed SQL database" height=100></img>

[![LICENSE](https://img.shields.io/github/license/pingcap/tidb.svg)](https://github.com/pingcap/tidb/blob/master/LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![Build Status](https://prow.tidb.net/badge.svg?jobs=pingcap/tidb/merged_*)](https://prow.tidb.net/?repo=pingcap%2Ftidb&type=postsubmit)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/tidb)](https://goreportcard.com/report/github.com/pingcap/tidb)
[![GitHub release](https://img.shields.io/github/tag/pingcap/tidb.svg?label=release)](https://github.com/pingcap/tidb/releases)
[![GitHub release date](https://img.shields.io/github/release-date/pingcap/tidb.svg)](https://github.com/pingcap/tidb/releases)
[![Coverage Status](https://codecov.io/gh/pingcap/tidb/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/tidb)
[![GoDoc](https://img.shields.io/badge/Godoc-reference-blue.svg)](https://godoc.org/github.com/pingcap/tidb)

## What is TiDB?

TiDB (/’taɪdiːbi:/, "Ti" stands for Titanium) is an open-source distributed SQL database that supports Hybrid Transactional and Analytical Processing (HTAP) workloads. It is MySQL compatible and features horizontal scalability, strong consistency, and high availability.

- [Key features](https://docs.pingcap.com/tidb/stable/overview#key-features)
- [Architecture](#architecture)
- [MySQL compatibility](https://docs.pingcap.com/tidb/stable/mysql-compatibility)

For more details and latest updates, see [TiDB documentation](https://docs.pingcap.com/tidb/stable) and [release notes](https://docs.pingcap.com/tidb/dev/release-notes).

For future plans, see the [TiDB roadmap](roadmap.md).

## Quick start

### Start with TiDB Cloud

TiDB Cloud is the fully-managed service of TiDB, currently available on AWS and GCP.

Quickly check out TiDB Cloud with [a free trial](https://tidbcloud.com/free-trial).

See [TiDB Cloud Quick Start Guide](https://docs.pingcap.com/tidbcloud/tidb-cloud-quickstart).

### Start with TiDB

See [TiDB Quick Start Guide](https://docs.pingcap.com/tidb/stable/quick-start-with-tidb).

### Start developing TiDB

See the [Get Started](https://pingcap.github.io/tidb-dev-guide/get-started/introduction.html) chapter of [TiDB Development Guide](https://pingcap.github.io/tidb-dev-guide/index.html).

## Community

You can join the following groups or channels to discuss or ask questions about TiDB, and to keep yourself informed of the latest TiDB updates:

- Seek help when you use TiDB
  - TiDB Forum: [English](https://ask.pingcap.com/), [Chinese](https://asktug.com)
  - Slack channels: [#everyone](https://slack.tidb.io/invite?team=tidb-community&channel=everyone&ref=pingcap-tidb) (English), [#tidb-japan](https://slack.tidb.io/invite?team=tidb-community&channel=tidb-japan&ref=github-tidb) (Japanese)
  - [Stack Overflow](https://stackoverflow.com/questions/tagged/tidb) (questions tagged with #tidb)
- Discuss TiDB's implementation and design
  - [TiDB Internals forum](https://internals.tidb.io/)
- Get the latest TiDB news or updates
    - Follow [@PingCAP](https://twitter.com/PingCAP) on Twitter
    - Read the PingCAP [English Blog](https://www.pingcap.com/blog/?from=en) or [Chinese Blog](https://cn.pingcap.com/blog/)

For support, please contact [PingCAP](http://bit.ly/contact_us_via_github).

## Contributing

The [community repository](https://github.com/pingcap/community) hosts all information about the TiDB community, including [how to contribute](https://github.com/pingcap/community/blob/master/contributors/README.md) to TiDB, how the TiDB community is governed, how [teams](https://github.com/pingcap/community/blob/master/teams/README.md) are organized.

Contributions are welcomed and greatly appreciated. You can get started with one of the [good first issues](https://github.com/pingcap/tidb/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22) or [help wanted issues](https://github.com/pingcap/tidb/issues?q=is%3Aopen+is%3Aissue+label%3A%22help+wanted%22). For more details on typical contribution workflows, see [Contribute to TiDB](https://pingcap.github.io/tidb-dev-guide/contribute-to-tidb/introduction.html). For more contributing information about where to start, click the contributor icon below.

[<img src="docs/contribution-map.png" alt="contribution-map" width="180">](https://github.com/pingcap/tidb-map/blob/master/maps/contribution-map.md#tidb-is-an-open-source-distributed-htap-database-compatible-with-the-mysql-protocol)

Every contributor is welcome to claim your contribution swag by filling in and submitting this [form](https://forms.pingcap.com/f/tidb-contribution-swag).

## Case studies

- [Case studies in English](https://www.pingcap.com/customers/)
- [中文用户案例](https://cn.pingcap.com/case/)

## Architecture

![TiDB architecture](./docs/tidb-architecture.png)

## License

TiDB is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Acknowledgments

- Thanks [cznic](https://github.com/cznic) for providing some great open source tools.
- Thanks [GolevelDB](https://github.com/syndtr/goleveldb), [BoltDB](https://github.com/boltdb/bolt), and [RocksDB](https://github.com/facebook/rocksdb) for their powerful storage engines.
