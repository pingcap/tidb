# BR

[![Build Status](https://internal.pingcap.net/idc-jenkins/job/build_br_multi_branch/job/master/badge/icon)](https://internal.pingcap.net/idc-jenkins/job/build_br_multi_branch/job/master/)
[![codecov](https://codecov.io/gh/pingcap/br/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/br)
[![LICENSE](https://img.shields.io/github/license/pingcap/br.svg)](https://github.com/pingcap/br/blob/master/LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![GoDoc](https://img.shields.io/badge/Godoc-reference-blue.svg)](https://godoc.org/github.com/pingcap/br)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/br)](https://goreportcard.com/report/github.com/pingcap/br)
[![GitHub release](https://img.shields.io/github/tag/pingcap/br.svg?label=release)](https://github.com/pingcap/br/releases)
[![GitHub release date](https://img.shields.io/github/release-date/pingcap/br.svg)](https://github.com/pingcap/br/releases)

**Backup & Restore (BR)** is a command-line tool for distributed backup and restoration of the TiDB cluster data.

## Architecture

<img src="images/arch.svg?sanitize=true" alt="architecture" width="600"/>

## Documentation

[Chinese Document](https://docs.pingcap.com/zh/tidb/v4.0/backup-and-restore-tool)

[English Document](https://docs.pingcap.com/tidb/v4.0/backup-and-restore-tool)

[Backup SQL Statement](https://docs.pingcap.com/tidb/v4.0/sql-statement-backup)

[Restore SQL Statement](https://docs.pingcap.com/tidb/v4.0/sql-statement-restore)

## Building

To build binary and run test:

```bash
$ make
$ make test
```

Notice BR supports building with Go version `Go >= 1.16`

When BR is built successfully, you can find binary in the `bin` directory.

## Quick start(docker-compose)

```sh
# Start TiDB cluster
docker-compose -f docker-compose.yaml rm -s -v && \
docker-compose -f docker-compose.yaml build && \
docker-compose -f docker-compose.yaml up --remove-orphans

# Attach to control container to run BR
docker exec -it br_control_1 bash

# Load testing data to TiDB
go-ycsb load mysql -p workload=core \
    -p mysql.host=tidb -p mysql.port=4000 -p mysql.user=root \
    -p recordcount=100000 -p threadcount=100

# How many rows do we get? 100000 rows.
mysql -uroot -htidb -P4000 -E -e "SELECT COUNT(*) FROM test.usertable"

# Build BR and backup!
make build && \
bin/br backup full --pd pd0:2379 --storage "local:///data/backup/full" \
    --log-file "/logs/br_backup.log"

# Let's drop database.
mysql -uroot -htidb -P4000 -E -e "DROP DATABASE test; SHOW DATABASES;"

# Restore!
bin/br restore full --pd pd0:2379 --storage "local:///data/backup/full" \
    --log-file "/logs/br_restore.log"

# How many rows do we get again? Expected to be 100000 rows.
mysql -uroot -htidb -P4000 -E -e "SELECT COUNT(*) FROM test.usertable"

# Test S3 compatible storage (MinIO).
# Create a bucket to save backup by mc (a MinIO Client).
mc config host add minio $S3_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY && \
mc mb minio/mybucket

# Backup to S3 compatible storage.
bin/br backup full --pd pd0:2379 --storage "s3://mybucket/full" \
    --s3.endpoint="$S3_ENDPOINT"

# Drop database and restore!
mysql -uroot -htidb -P4000 -E -e "DROP DATABASE test; SHOW DATABASES;" && \
bin/br restore full --pd pd0:2379 --storage "s3://mybucket/full" \
    --s3.endpoint="$S3_ENDPOINT"
```

## Quick Start(tiup)

```sh
# Using tiup to start a TiDB cluster
tiup playground --db 2 --pd 3 --kv 3 --monitor

# Using tiup bench to generater test data.
tiup bench tpcc --warehouses 1 prepare

# How many row do we get? 300242 rows.
mysql --host 127.0.0.1 --port 4000 -E -e "SELECT COUNT(*) FROM test.order_line" -u root -p

# Build br.
make build

# Backup TPC-C test data.
bin/br backup table --db test \
	--table order_line \
	-s local:///tmp/backup_test/ \
	--pd ${PD_ADDR}:2379 \
	--log-file backup_test.log \

# Let's drop the table.
mysql -uroot --host 127.0.0.1 -P4000 -E -e "USE test; DROP TABLE order_line; show tables" -u root -p

# Restore from the backup.
bin/br restore table --db test \
	--table order_line \
	-s local:///tmp/backup_test/ \
	--pd ${PD_ADDR}:2379 \
	--log-file restore_test.log

# How many rows do we get after restore? Expected to be 300242 rows.
mysql --host 127.0.0.1 -P4000 -E -e "SELECT COUNT(*) FROM test.order_line" -uroot -p
```

## Compatible test

See [COMPATBILE_TEST](./COMPATIBLE_TEST.md)

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING](./CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## License

BR is under the Apache 2.0 license. See the [LICENSE](./LICENSE.md) file for details.
