🥟 Dumpling
============

[![Build Status](https://travis-ci.org/pingcap/dumpling.svg?branch=master)](https://travis-ci.org/pingcap/dumpling)
[![codecov](https://codecov.io/gh/pingcap/dumpling/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/dumpling)
[![API Docs](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/pingcap/dumpling)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/dumpling)](https://goreportcard.com/report/github.com/pingcap/dumpling)

**Dumpling** is a tool and a Go library for creating SQL dump from a MySQL-compatible database.
It is intended to replace `mysqldump` and `mydumper` when targeting TiDB.

You may read the [design document](https://github.com/pingcap/community/blob/master/rfc/2019-12-06-dumpling.md), [English user guide](docs/en/user-guide.md) and [中文使用手册](docs/cn/user-guide.md) for details.

Features
--------

> Dumpling is currently in early development stage, and most features are incomplete. Contributions are welcomed!

- [x] SQL dump is split into multiple files (like `mydumper`) for easy management.
- [x] Export multiple tables in parallel to speed up execution.
- [ ] Multiple output formats: SQL, CSV, ...
- [ ] Write to cloud storage (S3, GCS) natively
- [x] Advanced table filtering

Building
--------

1. Install Go 1.13 or above
2. Run `make build` to compile. The output is in `bin/dumpling`.
3. Run `make test` to run the unit tests.
4. Run `make integration_test` to run integration tests. For integration test:
  - The following executables must be copied or generated or linked into these locations, `sync_diff_inspector` can be downloaded from [tidb-enterprise-tools-latest-linux-amd64](http://download.pingcap.org/tidb-enterprise-tools-latest-linux-amd64.tar.gz), `tidb-server` can be downloaded from [tidb-master-linux-amd64](https://download.pingcap.org/tidb-master-linux-amd64.tar.gz), and `tidb-lightning` can be downloaded from [tidb-toolkit-latest-linux-amd64](https://download.pingcap.org/tidb-toolkit-latest-linux-amd64.tar.gz):
    * `bin/sync_diff_inspector`
    * `bin/tidb-server`
    * `bin/tidb-lightning`
  - The following programs must be installed:
    * `mysql` (the CLI client)

License
-------

Dumpling is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
