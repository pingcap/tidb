🥟 Dumpling
============

[![API Docs](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/pingcap/tidb/dumpling)

**Dumpling** is a tool and a Go library for creating an SQL dump from a MySQL-compatible database.
It is intended to replace `mysqldump` and `mydumper` when targeting TiDB.

You may read the original [design document](https://github.com/pingcap/community/blob/master/archive/misc/rfc/2019-12-06-dumpling.md). The end-user documentation is available here: [en](https://docs.pingcap.com/tidb/stable/dumpling-overview), [zh](https://docs.pingcap.com/zh/tidb/stable/dumpling-overview), [ja](https://docs.pingcap.com/ja/tidb/stable/dumpling-overview).

Features
--------

> Dumpling is in active development. Contributions are welcomed!

- [x] SQL dump is split into multiple files (like `mydumper`) for easy management.
- [x] Export multiple tables in parallel to speed up execution.
- [x] Multiple output formats: SQL, CSV, ...
- [x] Write to cloud storage (S3, GCS) natively
- [x] Advanced table filtering

Any questions? Discord, Slack and other contact options can be found [here](https://docs.pingcap.com/tidb/stable/support)

Building
--------

0. Under directory `tidb`
1. Install Go 1.23.4 or above
2. Run `make build_dumpling` to compile. The output is in `bin/dumpling`.
3. Run `make dumpling_unit_test` to run the unit tests.
4. Run `make dumpling_integration_test` to run integration tests. For integration test:
  - The following executables must be copied or generated or linked into these locations:
    * `bin/sync_diff_inspector` (download from [tidb-enterprise-tools-latest-linux-amd64](http://download.pingcap.org/tidb-enterprise-tools-latest-linux-amd64.tar.gz))
    * `bin/tidb-server` (download from [tidb-master-linux-amd64](https://download.pingcap.org/tidb-master-linux-amd64.tar.gz))
    * `bin/tidb-lightning` (download from [tidb-toolkit-latest-linux-amd64](https://download.pingcap.org/tidb-toolkit-latest-linux-amd64.tar.gz))
    * `bin/minio` (download from <https://min.io/download>)
    * Now, you can run `sh ./dumpling/install.sh` to get the above binary files.
  - The following programs must be installed:
    * `mysql` (the CLI client)
  - There must be a local mysql server listening on `127.0.0.1:3306`, and an active user with no password that can be connected through this TCP address.

License
-------

Dumpling is under the Apache 2.0 license. See the [LICENSE](../LICENSE) file for details.
