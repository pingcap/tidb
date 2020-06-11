# Dumpling User Guide

**Dumpling** is a tool and a Go library for creating SQL dump (CSV/SQL format) from a MySQL-compatible database.

It is intended to replace `mysqldump` and `mydumper` when targeting TiDB; as a result, its basic usage is similar to that of Mydumper.

The following table lists the major parameters of Dumpling.


| Parameter | Description |
| --------| --- |
| -B or --database | Dump the specified databases. |
| -T or --tables-list | Dump the specified tables |
| -f or --filter | Dump only the tables matching the patterns. See [table-filter](https://github.com/pingcap/tidb-tools/blob/master/pkg/table-filter/README.md) for syntax. |
| --case-sensitive | whether the filter should be case-sensitive, default false(insensitive) |
| -h or --host | Host to connect to. (default: `127.0.0.1`) |
| -t or --threads | Number of threads for concurrent backup. |
| -r or --rows | Split table into multiple files by number of rows. This allows Dumpling to generate multiple files concurrently. (default: unlimited) |
| --loglevel | Log level. {debug, info, warn, error, dpanic, panic, fatal}. (default: `info`) |
| -d or --no-data | Don't dump data, for schema-only case. |
| --no-header | Dump table CSV without header. |
| -W or --no-views | Don't dump views. (default: `true`) |
| -m or --no-schemas | Don't dump schemas, dump data only. |
| -s or --statement-size | Control the size of Insert Statement. Unit: byte. |
| -F or --filesize | The approximate size of the output file. The unit should be explicitly provided (such as `128B`, `64KiB`, `32MiB`, `1.5GiB`) |
| --filetype| The type of dump file. (sql/csv, default "sql")           |
| -o or --output | Output directory. The default value is based on time. |
| -S or --sql | Dump data with given sql. This argument doesn't support concurrent dump |
| --consistency | Which consistency control to use (default `auto`):<br>`flush`: Use FTWRL (flush tables with read lock)<br>`snapshot`: use a snapshot at a given timestamp<br>`lock`: execute lock tables read for all tables that need to be locked <br>`none`: dump without locking. It cannot guarantee consistency <br>`auto`: `flush` on MySQL, `snapshot` on TiDB |
| --snapshot | Snapshot position. Valid only when consistency=snapshot. |
| --where | Specify the dump range by `where` condition. Dump only the selected records. |
| -p or --password | User password. |
| -P or --port | TCP/IP port to connect to. (default: `4000`) |
| -u or --user | Username with privileges to run the dump. (default "root") |

To see more detailed usage, run the flag `-h` or `--help`.

## Mydumper Reference

[Mydumper usage](https://github.com/maxbube/mydumper/blob/master/docs/mydumper_usage.rst)

[TiDB Mydumper Instructions](https://pingcap.com/docs/stable/reference/tools/mydumper/)

## Download

Download the nightly version of Dumpling [here](https://download.pingcap.org/dumpling-nightly-linux-amd64.tar.gz).
