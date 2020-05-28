![](docs/logo_with_text.png)

[![LICENSE](https://img.shields.io/github/license/pingcap/tidb.svg)](https://github.com/pingcap/tidb/blob/master/LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![Build Status](https://travis-ci.org/pingcap/tidb.svg?branch=master)](https://travis-ci.org/pingcap/tidb)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/tidb)](https://goreportcard.com/report/github.com/pingcap/tidb)
[![GitHub release](https://img.shields.io/github/tag/pingcap/tidb.svg?label=release)](https://github.com/pingcap/tidb/releases)
[![GitHub release date](https://img.shields.io/github/release-date/pingcap/tidb.svg)](https://github.com/pingcap/tidb/releases)
[![CircleCI Status](https://circleci.com/gh/pingcap/tidb.svg?style=shield)](https://circleci.com/gh/pingcap/tidb)
[![Coverage Status](https://codecov.io/gh/pingcap/tidb/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/tidb)
[![GoDoc](https://img.shields.io/badge/Godoc-reference-blue.svg)](https://godoc.org/github.com/pingcap/tidb)

<!-- vim-markdown-toc GFM -->

* [What is TiDB?](#what-is-tidb)
* [Documentation](#documentation)
* [Quick start](#quick-start)
* [Deployment](#deployment)
* [Data Migration](#data-migration)
* [Getting Help](#getting-help)
* [Adopters](#adopters)
* [Roadmap](#roadmap)
* [Contributing](#contributing)
* [License](#license)
* [Acknowledgments](#acknowledgments)

<!-- vim-markdown-toc -->

## What is TiDB?

TiDB ("Ti" stands for Titanium) is an open-source NewSQL database that supports Hybrid Transactional and Analytical Processing (HTAP) workloads. It is MySQL compatible and features horizontal scalability, strong consistency, and high availability.

For a detailed introduction to TiDB architecture, see [TiDB Architecture](https://pingcap.com/docs/dev/architecture/).

## Documentation

- [English](https://pingcap.com/docs)
- [简体中文](https://pingcap.com/docs-cn)

## Quick start

- [Quick start guide](https://docs.pingcap.com/zh/tidb/v4.0/quick-start-with-tidb)
- [Learn basic SQL](https://docs.pingcap.com/zh/tidb/v4.0/basic-sql-operations)
- [Core features and application scenarios](https://docs.pingcap.com/zh/tidb/v4.0/overview)

## Deployment

- [TiUP Deployment](https://pingcap.com/docs/dev/production-deployment-using-tiup): This guide describes how to deploy TiDB using [TiUP](https://github.com/pingcap-incubator/tiup). It is strongly recommended for production deployment.
- Kubernetes Deployment: You can use [TiDB Operator](https://github.com/pingcap/tidb-operator) to deploy TiDB on:
    - [AWS EKS (Elastic Kubernetes Service)](https://pingcap.com/docs/tidb-in-kubernetes/stable/deploy-on-aws-eks/)
    - [GKE (Google Kubernetes Engine)](https://pingcap.com/docs/tidb-in-kubernetes/stable/deploy-on-gcp-gke/)
    - [Google Cloud Shell](https://pingcap.com/docs/tidb-in-kubernetes/stable/deploy-tidb-from-kubernetes-gke/)
    - [Alibaba Cloud ACK (Container Service for Kubernetes)](https://pingcap.com/docs/tidb-in-kubernetes/stable/deploy-on-alibaba-cloud/)

Read the [Quick Start Guide](https://pingcap.com/docs/QUICKSTART) for more details on deployment.

## Data Migration

See [Data Migration](https://docs.pingcap.com/zh/tidb/v4.0/data-migration-route) for details. For example you can:

- [Migrate data from MySQL to TiDB](https://docs.pingcap.com/zh/tidb/v4.0/data-migration-route#%E4%BB%8E-mysql-%E8%BF%81%E7%A7%BB%E5%88%B0-tidb)
- [Load CSV data to TiDB](https://docs.pingcap.com/zh/tidb/v4.0/data-migration-route#%E4%BB%8E-csv-%E6%96%87%E4%BB%B6%E8%BF%81%E7%A7%BB%E5%88%B0-tidb)

## Getting Help

- [Slack Channel](https://pingcap.com/tidbslack/)
- Twitter: [@PingCAP](https://twitter.com/PingCAP)
- [Reddit](https://www.reddit.com/r/TiDB/)
- Mailing-list: [Google Group](https://groups.google.com/forum/#!forum/tidb-user)
- [Blog](https://www.pingcap.com/blog/)
- [Stack Overflow](https://stackoverflow.com/questions/tagged/tidb)
- [User Group (Chinese)](https://asktug.com/)

For support, please [contact PingCAP](http://bit.ly/contact_us_via_github)

## Adopters

View the current list of in-production TiDB adopters [here](https://pingcap.com/docs/adopters/).

## Roadmap

Read the [Roadmap](https://pingcap.com/docs/ROADMAP).

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING.md](https://github.com/pingcap/community/blob/master/CONTRIBUTING.md) for details on submitting patches and the contribution workflow. For more contributing information, click on the contributor icon above.

## License

TiDB is under the Apache 2.0 license. See the [LICENSE](https://github.com/pingcap/tidb/blob/master/LICENSE) file for details.

## Acknowledgments

- Thanks [cznic](https://github.com/cznic) for providing some great open-source tools.
- Thanks [GolevelDB](https://github.com/syndtr/goleveldb), [BoltDB](https://github.com/boltdb/bolt), and [RocksDB](https://github.com/facebook/rocksdb) for their powerful storage engines.

