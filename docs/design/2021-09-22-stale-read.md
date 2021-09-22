# Proposal: Support Stale Read in TiDB

- Author(s): [Yisaer](https://github.com/Yisaer) (Song Gao)
- Last updated: 2021-08-18
- Tracking Issue: https://github.com/pingcap/tidb/issues/21094
- Related Document: https://docs.google.com/document/d/1dSbXbudTK-hpz0vIBsmpfuzeQhhTFJ-zDenygt8fmCk/edit?usp=sharing

## Introduction

This proposal aims to support Stale Read and described what stale read looks like and how to keep compatibility with other
query.

## Motivation or Background

Currently, TiDB support Follower Read to read data from follower replicas in order to increase the query throughput. TiDB
also support Snapshot Read to read old data with given snapshot timestamp. However, Follower Read still require an rpc calling
between follower replica and leader replica during querying which also increased query latency. For Snapshot Read, it only read
data from leader replica which might be bottleneck especially in hot spot cases.

Stale Read is more like the combination of Follower Read and Snapshot Read. It supports tidb to read data from both leader replicas
and follower replicas with no extra rpc calling during followers and leader. It also supports read data with given snapshot timestamp
like Snapshot Read.

## Detailed Design

### Stale Read Syntax Overview

Here we will introduce how to use Stale Read in TiDB. 

1. Use Stale Read with given specific timestamp in single statement:

```sql
SELECT * FROM t AS OF TIMESTAMP '2021-09-22 15:04:05' WHERE id = 1;
SELECT * FROM t AS OF TIMESTAMP NOW(20) WHERE id = 1;
```

2. Use Stale Read with given specific timestamp in single interactive transaction:

```sql
START TRANSACTION READ ONLY AS OF TIMESTAMP '2021-09-22 15:04:05';
SELECT * FROM t WHERE id = 1;
COMMIT;
```

3. Use Stale Read with given exact seconds ago staleness in single statement:

```sql
// query data with exact 20 seconds ago timestamp
SELECT * FROM t AS OF TIMESTAMP NOW(20) WHERE id = 1;
```

4. Use Stale Read with given exact seconds ago staleness in interactive statement:

```sql
// query data with exact 20 seconds ago timestamp
START TRANSACTION READ ONLY AS OF TIMESTAMP NOW(20);
SELECT * FROM t where id = 1;
COMMIT;
```

5. Use Stale Read with given max tolerant staleness in single statement:

```sql
// query data with at most 20 seconds staleness
SELECT * FROM t AS OF TIMESTAMP(NOW() - INTERVAL 20 SECOND) WHERE id = 1;
```

6. Use Stale Read with given max tolerant staleness in interactive transaction:

```sql
START TRANSACTION READ ONLY AS OF timestamp(NOW() - INTERVAL 20 SECOND);
SELECT * FROM t WHERE id = 1;
COMMIT;
```

7. Use Stale Read by `SET TRANSACTION` statement to affect next query.

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

Note that Stale Read will only affect the following simple query statement. 

### Compatibility

This section we will talk about the compatibility between each Stale Read situations.

#### Stale Read by SET TRANSACTION Statement

Stale Read by SET TRANSACTION Statement will make next interactive transaction or query with staleness timestamp. Thus 
it is not allowed that querying stale read statement after stale read by SET TRANSACTION statement like following:

```sql
mysql> set transaction read only as of timestamp now(1);
Query OK, 0 rows affected (0.00 sec)

mysql> select * from t as of timestamp now(2);
ERROR 8135 (HY000): invalid as of timestamp: can't use select as of while already set transaction as of
```

#### Stale Read Statement Priority 

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

 

