# Proposal: Support Stale Read in TiDB

- Author(s): [Yisaer](https://github.com/Yisaer) (Song Gao)
- Last updated: 2021-09-22
- Tracking Issue: https://github.com/pingcap/tidb/issues/21094
- Related Document: https://docs.google.com/document/d/1dSbXbudTK-hpz0vIBsmpfuzeQhhTFJ-zDenygt8fmCk/edit?usp=sharing

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
    * [Implementation Overview](#implementation-overview)
    * [New Syntax Overview](#new-syntax-overview)
* [Compatibility](#compatibility)
    * [ForBidden Behavior](#forbidden-behavior)
    * [Statement Priority](#statement-priority)

## Introduction

This proposal aims to support Stale Read and describe what Stale Read looks like and how to keep compatibility with other queries.

## Motivation or Background

Currently, TiDB supports Follower Read to read data from follower replicas in order to increase the query throughput. TiDB also supports Snapshot Read to read old data with a given snapshot timestamp. 

However, Follower Read still requires an RPC calling between follower replica and leader replica during querying which also increased query latency. 

For Snapshot Read, it only reads data from leader replica which might be a bottleneck, especially in hot spot cases.

Stale Read is more like the combination of Follower Read and Snapshot Read. It supports TiDB to read data from both leader replicas and follower replicas with no extra RPC calling during followers and leaders. 

It also supports read data with a given snapshot timestamp like Snapshot Read.

## Detailed Design

### Implementation Overview

Normally, TiDB will send data query requests to the storage with a given timestamp to the leader replicas. 

In Stale Read, each replica could handle the data query request because every replica maintains the resolved timestamp from the leader replica. 

This makes each store could know the smallest resolved timestamp they could handle. Thus, TiDB could send data requests with a staleness timestamp to get the data they want.

### New Syntax Overview

Here we will introduce how to use Stale Read in TiDB. 

1. Use Stale Read with given specific timestamp in a single statement:

```sql
SELECT * FROM t AS OF TIMESTAMP '2021-09-22 15:04:05' WHERE id = 1;
SELECT * FROM t AS OF TIMESTAMP NOW() - INTERVAL 20 SECOND WHERE id = 1;
```

2. Use Stale Read with a given timestamp in a single interactive transaction:

```sql
START TRANSACTION READ ONLY AS OF TIMESTAMP '2021-09-22 15:04:05';
SELECT * FROM t WHERE id = 1;
COMMIT;
```

3. Use Stale Read with given exact seconds ago staleness in a single statement:

```sql
// query data with exact 20 seconds ago timestamp
SELECT * FROM t AS OF TIMESTAMP NOW() - INTERVAL 20 SECOND WHERE id = 1;
```

4. Use Stale Read with given exact seconds ago staleness in an interactive statement:

```sql
// query data with exact 20 seconds ago timestamp
START TRANSACTION READ ONLY AS OF TIMESTAMP NOW() - INTERVAL 20 SECOND;
SELECT * FROM t where id = 1;
COMMIT;
```

5. Use Stale Read with given max tolerant staleness in a single statement:

```sql
// query data with at most 20 seconds staleness
SELECT * FROM t AS OF TIMESTAMP NOW() - INTERVAL 20 SECOND WHERE id = 1;
```

6. Use Stale Read with given max tolerant staleness in an interactive transaction:

```sql
START TRANSACTION READ ONLY AS OF timestamp NOW() - INTERVAL 20 SECOND;
SELECT * FROM t WHERE id = 1;
COMMIT;
```

7. Use Stale Read by `SET TRANSACTION` statement to affect the next query.

```sql
SET TRANSACTION READ ONLY AS OF TIMESTAMP '2021-09-22 15:04:05';
BEGIN;
SELECT * FROM t  WHERE id = 1;
COMMIT;
// it also affects next query statement 
SET TRANSACTION READ ONLY AS OF TIMESTAMP '2021-09-22 15:04:05';
SELECT * FROM t  WHERE id = 1;
```

8. Enable Stale Read with given max tolerant staleness in session:

```sql
// enable stale read with max tolerant 5 seconds ago in session
SET @@tidb_read_staleness='-5';
select * from t where id = 1;
```

Note that Stale Read will only affect the following query statement `SELECT`, it won't affect the interactive transaction, or updating statement like `INSERT`,`DELETE` and `UPDATE`

### Compatibility

This section we will talk about the compatibility between each Stale Read situations.

#### Forbidden Behavior

Stale Read by SET TRANSACTION Statement will make next interactive transaction or query with staleness timestamp. 

Thus it is not allowed that querying stale read statement after stale read by SET TRANSACTION statement like following:

```sql
mysql> set transaction read only as of timestamp now(1);
Query OK, 0 rows affected (0.00 sec)

mysql> select * from t as of timestamp now(2);
ERROR 8135 (HY000): invalid as of timestamp: can't use select as of while already set transaction as of
```

#### Statement Priority 

After enabling `tidb_read_staleness`, the following querying will use given staleness timestamp to get data from replicas.

And you can still use Stale Read with other forms with different timestamp:

```sql
SET @@tidb_read_staleness='-5';
// query data with max tolerant 5 seconds ago
select * from t where id = 1;
// query data with exact 10 seconds ago timestamp for this statement
select * from t as of timestamp now(10) where id = 1;
// query data with exact 10 seconds ago timestamp for next statement
set transaction read only as of timestamp now(10);
select * from t where id = 1;
```

 

