# Proposal: Flashback To Timestamp
- Author(s): [Defined2014](https://github.com/Defined2014) and [JmPotato](https://github.com/JmPotato)
- Last updated: 2022-12-19
- Tracking Issues: https://github.com/pingcap/tidb/issues/37197 and https://github.com/tikv/tikv/issues/13303

## Abstract

This proposal aims to support `Flashback To Timestamp` and describe what `Flashback To Timestamp` should look like and how to implement it.

## Background

Some users want to `Flashback table/database/cluster` to the specified timestamp when there is a problem with the data like deleted some important keys, updated wrong values etc.

TiDB uses MVCC to store key-values, which means it can easily get historical data at any timestamp. Based on this feature, TiDB already supports `Flashback To Timestamp`, for example, users can read historical data and update it through Snapshot Read which is not only inelegant, but also inefficient.

Therefore, we propose to use a series of new SQL syntaxes to support this feature, and push down the read-write operations to storage side.

## Detailed Design

### Implementation Overview

In TiKV, a multi-version concurrency control (MVCC) mechanism is introduced to avoid the overhead of introducing locks when data is updated concurrently. Under this mechanism, when TiDB modified data, it doesn't directly operate on the original value, but writes a data with the latest timestamp to cover it. The GC Worker in the background of TiDB will periodically update `tikv_gc_safe_point` and delete the version older than this point. `Flashback To Timestamp` is developmented based on this feature of TiKV. In order to improve execution efficiency and reduce data transmission overhead, TiKV has added two RPC interfaces called `PrepareFlashbackToVersion` and `FlashbackToVersion`. The protobuf related change shown below:

```protobuf
// Preparing the flashback for a region/key range will "lock" the region
// so that there is no any read, write or schedule operation could be proposed before
// the actual flashback operation.
message PrepareFlashbackToVersionRequest {
    Context context = 1;
    bytes start_key = 2;
    bytes end_key = 3;
}

message PrepareFlashbackToVersionResponse {
    errorpb.Error region_error = 1;
    string error = 2;
}

// Flashback the region to a specific point with the given `version`, please
// make sure the region is "locked" by `PrepareFlashbackToVersionRequest` first,
// otherwise this request will fail.
message FlashbackToVersionRequest {
    Context context = 1;
    // The TS version which the data should flashback to.
    uint64 version = 2;
    bytes start_key = 3;
    bytes end_key = 4;
    // The `start_ts`` and `commit_ts` which the newly written MVCC version will use.
    uint64 start_ts = 5;
    uint64 commit_ts = 6;
}

message FlashbackToVersionResponse {
    errorpb.Error region_error = 1;
    string error = 2;
}
```

Then a `Flashback To Timestamp` DDL job can be simply divided into the following steps

* Save values of some global variables and PD schedule. Those values will be changed during `Flashback`.

* Pre-checks. After all checks are passed, TiDB will disable GC and closed PD schedule for the cluster. The specific checks are as follows:
    * The FlashbackTS is after `tikv_gc_safe_point`.
    * The FlashbackTS is before the minimal store resolved timestamp.
    * No DDL job was executing at FlashbackTS.
    * No running related DDL jobs.

* TiDB get flashback key ranges and splits them into separate regions to avoid locking unrelated key ranges. Then TiDB send `PrepareFlashbackToVersion` RPC requests to lock regions in TiKV. Once locked, no more read, write and scheduling operations are allowed for those regions.

* After locked all relevant key ranges, the DDL owner will update schema version and synchronize it to other TiDBs. When other TiDB applies the `SchemaDiff` of type `Flashback To Timestamp`, it will disconnect all relevant links.

* Send `FlashbackToVersion` RPC requests to all relevant key ranges with same `commit_ts`. Each region handles its own flashback progress independently.
    * Read the old MVCC data and write it again with the given `commit_ts` to pretend it's a new transaction commit.
    * Release the Raft proposing lock and resume the lease read.

* TiDB checks whether all the requests returned successfully, and retries those failed requests with the same `commit_ts`. After all requests finished, the DDL owner update the schema version with new type named `ReloadSchemaMap`, so that all TiDB servers regenerate the schema map from TiKV.

* After `Flashback To Timestamp` is finished, TiDB will restore all changed global variables and restart PD schedule. At the same time, notify `Stats Handle` to reload statistics from TiKV.

### New Syntax Overview

TiDB will support 3 new syntaxes as follows.

1. Flashback whole cluster except some system tables to the specified timestamp.

```sql
FLASHBACK CLUSTER TO TIMESTAMP '2022-07-05 08:00:00';
```

2. Flashback some databases to the specified timestamp.

```sql
FLASHBACK DATABASE [db] TO TIMESTAMP '2022-07-05 08:00:00';
```

3. Flashback some tables to the specified timestamp.

```sql
FLASHBACK TABLE [table1], [table2] TO TIMESTAMP '2022-08-10 08:00:00';
```

### Limitations and future Work

1. Compare with the other DDL jobs, `Flashback To Timestamp` job cannot be rollbacked after some regions failure and also needs to resend rpc to all regions when ddl owner crashed. In the future, we will improve those two issues with a new TiKV interface and new distributed processing ddl framework.

### Alternative Solutions

1. Read historical data via `As of timestamp` clause and write back with the lastest timestamp. But it's much slower than `Flashback To Timestamp`, the data needs to be read to TiDB first then written back to TiKV.

2. Use `Reset To Version` interface to delete all historical version. After this operation, the user can't find the deleted version any more and this interface is incompatible with snapshot read.
