# TiDB SQL SEQUENCE

- Feature Name: Sequences
- Author(s):     [AilinKid](https://github.com/ailinkid) (Lingxiang Tai)
- Last updated:  2020-04-17
- Discussion at: https://tidbcommunity.slack.com/archives/CQNKBDVM0

## Summary

“Sequence Generator” (hereinafter referred to as “Sequence”) is a database feature introduced in SQL 2003 Standard. It is defined as “a mechanism for generating continuous numerical values, which can be either internal or external objects”.

Based on the definition above, the conventional auto-increment columns of MySQL/TiDB can be considered as an internal implementation of Sequence Generator. This RFC describes the design of external Sequence Generator. Unless otherwise noted, the “Sequence” described below refers to external Sequence Generator.

## Motivation

- Standardization: Admittedly sequence is a well-defined syntax standard in SQL 2003. Although auto-increment can be regarded as a internal sequence implementation, it hasn't supported all the external features (such as CYCLE).

- Usability: A sequence can be independent of tables, shared by multiple tables and be specified with CACHE details (either specify the number of CACHE or disable CACHE), which improves usability of the product.

- Business Value: Sequence is a feature widely implemented in mainstream commercial databases. Users tend to expect TiDB can syntactically support sequence to reduce migration costs who migrate their business from other databases to TiDB. 

## Background

TiDB auto-increment will cache a batch values by default in each instance to ensure better performance, which means auto-increment only guarantee the uniqueness under a multi-tidb-node cluster. This kind of cache mode is uncontrollable and implicit in auto-increment.

But in sequence, we take it open. Users can specify whether to use cache mode or not, what the cache size should be, what's the min and max of the range like and so on. This factors make sequence more flexible, controllable and easy to use.

## Syntax and Semantics

TiDB sequence is borrowed from MariaDB, which is also close to DB2 because DB2 is similar to MariaDB regarding the Sequence syntax.

create sequence statement:

```
CREATE SEQUENCE [IF NOT EXISTS] sequence_name
[ INCREMENT [ BY | = ] increment ]
[ MINVALUE [=] minvalue | NO MINVALUE | NOMINVALUE ]
[ MAXVALUE [=] maxvalue | NO MAXVALUE | NOMAXVALUE ]
[ START [ WITH | = ] start ] 
[ CACHE [=] cache | NOCACHE | NO CACHE ]
[ CYCLE | NOCYCLE | NO CYCLE]
```
 
show create sequence statement:

```
SHOW CREATE SEQUENCE sequence_name
```

drop sequence sequence statement:

```
DROP SEQUENCE sequence_name_list
```

get the next value of sequence:

```
NEXT VALUE FOR sequence_name / NEXTVAL(sequence_name)
```

get the previous value of sequence:

```
LASTVAL(sequence_name)
```

move the current position in sequence number axis

```
SETVAL(sequence_name, num)
```

## Rationale

- Sequence metadata

Sequence is a table-level object, sharing the same namespace with the base table and view objects, whose meta are recorded in `ast.tableInfo`. But actually sequence definition do not have columns in it's meta, which means you can't operate the sequence value by `DML` interface. The only way you can do it is by `setval` function, after all, the sequence value is essentially a `KV` pair with some constraints.

- Sequence values

Each sequence value will be stored in its own range, with the key format below:

```
DB:<dbID> SID:<sequenceID> ——> int64 
```

Reads and writes to the sequence value (via the functions `nextval`, `setval`), will be implemented by direct calls to the `KV` layer's `Get`, `Inc`, and `Set` functions. Since sequence object store the value at the range of its own, it will cause hotpoint at sequence meta under insertion intensive cases.

- Sequence Transaction

Storing sequence values in their own ranges means that inserts to tables which use sequences will always touch two ranges. However, since the sequence update takes place outside of the SQL transaction, this should not trigger the `2PC` commit protocol. 

Sequence computation is under the expression framework, evaluating one value at a time. The values fetched by these functions won't be rolled back when the outer SQL transaction is aborted.

- Sequence Binlog

Distinguished from auto-increment's rebase functionality, columns with sequence as default won't rebase it's value automatically when the inserted value was specified explicitly. Besides, sequence can also be used directly as the inserted expression value. Thus, it's necessary to sync the sequence value down to the upstream cluster.

Sequence binlog is triggered by both `nextval` and `setval` functions when the update reaches the storage directly rather than in the cache.

Sequence binlog is not a sort of `DML` binlog, but mocked as a kind of `DDL` `job` by specifying the job id equal to -1 specially. Therefore, the drainer won't take it for granted that it's a real ddl job and it should be occurred in ddl history queue. 

- Sequence Performance

The insertion performance of sequences is comparable to the `auto-increment` of the same cache size (now can be specified with `auto_id_cache` table option) in our recent tests on IDC cluster. The default cache size of sequence is 1000, which could achieve approximately 3 thousands TPS with 64 threads under our own SysBench. 

## Recommended scenarios for Sequence

- Users have requirements for generating sequence value shared by multiple tables.

- The user business tier prefers to encapsulate the unique value, for instance, combining the unique value with the date and the user ID to form a complete ID. And users wish that the unique value is maintained by the database.

- Users are sensitive to the sequence space. To save the sequence space as much as possible, the trade-off is between the performance and the amount of `CACHE`, and even using the `NOCACHE` mode.

## Compatibility

About specific compatibility, you can view [MySQL’s compatibility](https://github.com/pingcap/docs/blob/master/sql/mysql-compatibility.md#ddl).
