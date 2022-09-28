# Proposal: Flashback To Timestamp
- Author(s):     [Defined2014](https://github.com/Defined2014) and [JmPotato](https://github.com/JmPotato)
- Tracking Issues: https://github.com/pingcap/tidb/issues/37197 and https://github.com/tikv/tikv/issues/13303

## Abstract

This proposal aims to support `Flashback To Timestamp` and describe what `Flashback To Timestamp` should looks like and how to implement it.

## Background

Some users want to `Flashback table/database/cluster` to the specified timestamp when there is a problem with the data like deleted some important keys, updated wrong values etc.

TiDB uses MVCC to store key-values, which means it can easily get historical data at any timestamp. Based on this feature, TiDB already supports `Flashback To Timestamp`, for example, users can read historical data and update it through Snapshot Read which is not only inelegant, but also inefficient.

Therefore, we propose to use a series of new SQL syntaxes to support this feature, and push down the read-write operations to storage side.

## Detailed Design

### Implementation Overview

In TiKV, a multi-version concurrency control (MVCC) mechanism is introduced to avoid the overhead of introducing locks when data is updated concurrently. Under this mechanism, when TiDB modified data, it doesn't directly operate on the original value, but writes a data with the latest timestamp to cover it. The GC Worker in the background of TiDB will periodically update `tikv_gc_safe_point` and delete the version older than this point. `Flashback To Timestamp` developmente based on this feature of TiKV. In order to improve execution efficiency and reduce data transmission overhead, TiKV has added two RPC interfaces called `PrepareFlashbackToVersion` and `FlashbackToVersion`.

Then a `Flashback To Timestamp` DDL job can be simply divided into the following stages

* Save values of some global variables and PD schedule. Those values will be changed during `Flashback`.

* Pre-checks. After all checks are passed, TiDB will disable GC and closed PD schedule for the cluster. The specific checks are as follows:
    * The flashbackTS is before `tikv_gc_safe_point`.
    * No related DDL history in flashback time range.
    * No running related DDL jobs.

* TiDB get flashback key ranges and send `PrepareFlashbackToVersion` RPC requests to lock them in TiKV. Once locked, no more read, write and scheduling operations are allowed for those key ranges.

* After locked all relevant key ranges, the DDL Owner will update schema version and synchronize it to other TiDBs. When other TiDB applies the `SchemaDiff` of type `Flashback To Timestamp`, it will disconnect all relevant links.

* Send `FlashbackToVersion` RPC requests to all relevant key ranges with same `commit_ts`. Each region handles its own flashback progress independently.
    * Read the old MVCC data and write it again with the given `commit_ts` to pretend it's a new transaction commit.
    * Release the Raft proposing lock and resume the lease read.

* TiDB checks whether all the requests returned successfully, and retries those that failed with same `commit_ts` until the whole flashback is done.

* After `Flashback To Timestamp` is finished, TiDB will restore all changed global variables and restart PD schedule. At the same time, notify `Stats Handle` to reload statistics from TiKV.

### New Syntax Overview

TiDB will support 3 new syntaxes as follows.

1. Flashback whole cluster except some system tables to the specified timestamp.

```sql
FLASHBACK CLUSTER TO TIMESTAMP '2022-07-05 08:00:00';
```

2. Flashback some databases to the specified timestamp.

```sql
FLASHBACK DATABASE [db1], [db2] TO TIMESTAMP '2022-07-05 08:00:00';
```

3. Flashback some tables to the specified timestamp.

```sql
FLASHBACK TABLE [table1], [table2] TO TIMESTAMP '2022-08-10 08:00:00';
```

### Limitations and future Work

1. DDL history exists for the flashback time period is not currently supported, the error message is shown below:

```sql
mysql> ALTER TABLE t ADD INDEX i(a);
Query OK, 0 rows affected (2.99 sec)

mysql> FLASHBACK CLUSTER TO TIMESTAMP '2022-10-10 11:53:30';
ERROR 1105 (HY000): Had ddl history during [2022-10-10 11:53:30 +0800 CST, now), can't do flashback
```

2. Compare with the other DDL jobs, `Flashback To Timestamp` job cannot be rollbacked after some regions failure and also needs to resend rpc to all regions when ddl owner crashed. In the future, we will improve those two issues with a new TiKV interface and new distributed processing ddl framework.

### Alternative Solutions

1. Read historical data via `As of timestamp` clause and write back with the lastest timestamp. But it's much slower than `Flashback To Timestamp`, the data needs to be read to TiDB first then written back to TiKV.

2. Use `Reset To Version` interface to delete all historical version. After this operation, the user can't find the deleted version any more and this interface is incompatible with snapshot read.
