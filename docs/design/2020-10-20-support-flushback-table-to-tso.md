# Proposal: Support flushback table to specific TSO

- Author(s):     [yangfei](https://github.com/amyangfei)
- Last updated:  2020-10-20
- Discussion at: https://github.com/pingcap/tidb/issues/20302

## Abstract

TiDB supports reverting table data back to an arbitrary time(later than GC safe time).

## Background

[TiCDC](https://github.com/pingcap/ticdc) is a change data capture for TiDB, it supports to replicate change data from upstream TiDB to downstream TiDB. TiCDC supports a [tso-map feature](https://github.com/pingcap/ticdc/pull/932), which means TiCDC can replicate data to a consistent state periodically (under the conditions that upstream TiDB is available), and persists a tso map which records a consistent state between upstream and downstream. Based on that premise, when upstream meets disaster, if downstream TiDB can recover data to the latest TSO in tso-map, we could get a latest consistent state compared to upstream.

We would like TiDB to provide the table flashback feature, it can be used not only in TiCDC consistent replication, but also in many data recovery scenarios, such as recovery table to eliminate recent misoperation.

## Proposal

### Syntax

Reuse the statement syntax of existing `FLASHBACK` but add the `TIMESTAMP` keyword.

```sql
FLASHBACK TABLE tbl TIMESTAMP 420264149562425345;
```

In the feture we could probably extend the syntax to support flashback a database or all databases.

```sql
FLASHBACK DATABASE db TIMESTAMP 420264149562425345;
FLASHBACK DATABASE * TIMESTAMP 420264149562425345;
```

### Execution

The `FLASHBACK x TIMESTAMP` statement is **synchronous**, it blocks the current session until completion. Nevertheless, the query can be canceled using the standard `KILL TIDB QUERY n` statement.

The `FLASHBACK x TIMESTAMP` statement interacts with TiKV to read data, and could write to TiKV via SQL execute API or KV API, so the statement requires SUPER privilege to execute.

### Status query

The status of the queued and running tasks can be queried using the `SHOW` statement:

```
SHOW FLASHBACK;
```

Example:

```
MySQL [(none)]> show backups;
+------------+---------+----------+---------------------+---------------------+---------------------+
| Table      | State   | Progress | Queue_Time          | Execution_Time      | TSO                 |
+------------+---------+----------+---------------------+---------------------+---------------------+
| test_db.t1 | Scan    | 20.01    | 2020-10-12 23:09:03 | 2020-10-12 23:09:25 | 420264149562425345  |
+------------+---------+----------+---------------------+---------------------+---------------------+
```

## Rationale

Currently the `FLASHBACK x TIMESTAMP` only supports TiKV as backend store. TiKV provides a gRPC API [EventFeed](https://github.com/pingcap/kvproto/blob/cdcb788eaebd513df2e5e98a7b8b4fe6132713cc/proto/cdcpb.proto#L146) which can return all raw KV changed log of a region from given TSO to now. Besides each KV changed log can provide both the before row value and after row value if the `old-value` switch is turned on. With the help of old value feature, it is easily to distinguish the DML type including INSERT/UPDATE/DELETE, and revert data from new value to old value.

Some RDBMS which supports FLASHBACK feature:

- [Oracle flashback](https://www.oracle.com/database/technologies/high-availability/flashback.html)
- [Mariadb flashback](https://mariadb.com/kb/en/flashback/)

## Implementation

### Execution procedure

- TiDB receives the FLASHBACK command, check TiKV GC safepoint, using a new FLASHBACK executor and dispatch a job, the FLASHBACK executor maintains a global Job queue, similar to DDL Job queue.
- The job requests TiKV via EventFeed API for an incremental data scan from given TSO, the old-value switch must be turned on in the request.
- After the incremental scan of all regions finish, sort the received kv entries by start-ts.
- Decode the sorted data, generate undo SQL into a list.
- Execute the undo SQLs in the reversed order.

### More work

- Support `FLASHBACK DATABASE`
- Write data via KV API instead of SQL executor.

### Corner case

1. Supposing flashback to TSO-1 from TSO-2, what happens if DDL exists between TSO-1 and TSO-2

   DDL increases the complexity, we should handle the table schema, and decode raw data with multiple version of table info.

   However in the implementation of TiCDC, DDL is a strong consistent barrier, which means only after the syncpoint is recorded, more DMLs can continue to be replicated. So in the usage scenario of TiCDC, we can avoid DDL in the undo procedure.

2. GC of the TiDB server

   The incremental scan must enable the old-value feature of TiKV, both the old value and new value should not be GCed.

3. What happens if DML executes during FLASHBACK x TIMESTAMP

   This behavior will cause data conflict, which is not allowed in our scenario.

## Testing Plan

Since `FLASHBACK x TIMESTAMP` relies on proper TiKV, the built-in testing framework using mocktikv will not work. Testing is going to be relying entirely on integration tests.
