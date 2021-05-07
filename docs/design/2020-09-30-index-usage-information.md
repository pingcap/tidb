# Proposal:

- Author(s):     [rebelice](https://github.com/rebelice)
- Last updated:  Sep. 30, 2020
- Discussion at: N/A

## Abstract

This document describes the design of recording index usage information.

## Background

There may be unused indexes in the database. In addition, modifying database tables, indexes, and query statements may cause some indexes to not be used in the future. Users may want to clear these long-unused indexes to relieve storage and performance pressure.

Related issues:

- https://github.com/pingcap/tidb/issues/14998
- https://github.com/pingcap/tidb/issues/14607
- https://github.com/pingcap/tidb/issues/17508
- https://asktug.com/t/topic/34614/12

## Proposal

### SCHEMA_INDEX_USAGE

Design system tables to record index usage information. The system table is designed as follows:

1. The system table is located in `mysql` database and is named `SCHEMA_INDEX_USAGE`.
2. Columns of `SCHEMA_INDEX_USAGE`:

| Column name  | Data type | Description |
|--------------|-----------|-------------|
| TABLE_ID     | bigint    | ID of the table or view on which the index is defined.|
| INDEX_ID     | bigint    | ID of the index.|
| QUERY_COUNT  | longlong  | Number of the SQL using this index.|
| ROWS_SELECTED| longlong  | Number of rows read from the index. We can check the average fetched rows count of each query of the index through `ROWS_READ` / `QUERY_COUNT`.|
| LAST_USED_AT | timestamp | The last time of the SQL using this index.|


3. Typical usage: `select * from mysql.SCHEMA_INDEX_USAGE`.

#### Table creation: 

```sql
create table SCHEMA_INDEX_USAGE (
	TABLE_ID bigint(21),
	INDEX_ID bigint(21),
	QUERY_COUNT BIGINT,
	ROUWS_SELECTED BIGINT,
	LAST_USED_AT timestamp,
	PRIMARY KEY (SCHEMA_NAME, TABLE_NAME, INDEX_NAME)
);
```

According to the [TiDB Doc](https://docs.pingcap.com/zh/tidb/stable/information-schema-tidb-indexes#tidb_indexes), `INDEX_ID` should be `bigint(21)`. Similarly, [this doc](https://docs.pingcap.com/zh/tidb/stable/information-schema-tables#tables) tells us that `TABLE_ID` should be `bigint(21)`.

#### Table update: 

```sql
insert into mysql.SCHEMA_INDEX_USAGE values (schemaA, tableA, indexA, count, row, used_at) 
on duplicate key update query_count=query_count+count, rows_selected=rows_selected+row, last_used_at=greatest(last_used_at, used_at)
```

#### Update method:

Statistics update is divided into statistics and persistence of index usage information. Index usage information is counted by the exec-info collector. And persistence is periodically writing data to system tables. We add a global variable `index-usage-sync-lease` to control the persistence cycle. It is set to 1 minute by default. In addition, add a global SQL variable to control whether to turn on this feature.

### SCHEMA_UNUSED_INDEXES

Due to MySQL compatibility, add the system table `SCHEMA_UNUSED_INDEXES`.

1. Create a view `SCHEMA_UNUSED_INDEXES` on table `SCHEMA_INDEX_USAGE`.
2. Columns of it:

| Column name   | Data type | Description           |
| -----------   | --------- | --------------------- |
| object_schema | varchar   | The schema name.      |
| object_name   | varchar   | The table name.       |
| index_name    | varchar   | The unused index name.|

#### View creation:

```sql
create view information_schema.schema_unused_indexes 
as select i.table_schema as table_schema, i.table_name as table_name, i.index_name as index_name 
from mysql.tidb_indexes as i left join mysql.schema_index_usage as u 
on i.table_schema=u.table_schema and i.table_name=u.table_name and i.index_name=u.index_name
where u.query_count=0 or u.query_count is null;
```

### INFORMATION_SCHEMA.SCHEMA_INDEX_USAGE

We use `TABLE_ID` and `INDEX_ID` as ID `mysql.SCHEMA_INDEX_USAGE`. Because of `TABLE_ID` and `INDEX_ID` is not user-friendly, we need a more user-friendly view.
Columns of it:

| Column name  | Data type | Description |
|--------------|-----------|-------------|
| TABLE_SCHEMA | varchar   | Name of the database on which the table or view is defined.|
| TABLE_NAME   | varchar   | Name of the table or view on which the index is defined.|
| INDEX_NAME   | varchar   | Name of the index.|
| QUERY_COUNT  | longlong  | Number of the SQL using this index.|
| ROWS_SELECTED| longlong  | Number of rows read from the index. We can check the average fetched rows count of each query of the index through `ROWS_READ` / `QUERY_COUNT`.|
| LAST_USED_AT | timestamp | The last time of the SQL using this index.|

#### View creation:

```sql
create view information_schema.schema_index_usage
as select idx.table_schema as table_schema, idx.table_name as table_name, idx.key_name as index_name, stats.query_count as query_count, stats.rows_selected as rows_selected
from mysql.schema_index_usage as stats, information_schema.tidb_indexes as idx, information_schema.tables as tables
where tables.table_name = idx.table_schema
	AND tables.table_name = idx.table_name
	AND tables.tidb_table_id = stats.table_id
	AND idx.index_id = stats.index_id
```

### FLUSH SCHEMA_INDEX_USAGE

#### User story

Users may have just completed a deployment which changes query patterns such that they expect there will be unused indexes. They can potentially look at the `LAST_USED_AT` column, but sometimes flushing is more desired.

Similar usage: `FLUSH INDEX_STATISTICS` from https://www.percona.com/doc/percona-server/LATEST/diagnostics/user_stats.html.

SQL Syntax: `FLUSH SCHEMA_INDEX_USAGE`
Users can use this to initialize SCHEMA_INDEX_USAGE as
```sql
delete from mysql.schema_index_usage;
```
And it needs a [RELOAD privilege](https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html#priv_reload) check.

### Privilege

Read privilege: Reading SCHEMA_INDEX_USAGE and SCHEMA_UNUSED_INDEXES need permission. A user can see only the rows in the tables that correspond to tables for which the user has the proper access privileges, such as information_schema.tables.
Write privilege: These tables are read-only. You can use FLUSH SCHEMA_INDEX_USAGE command to reinitialize tables, which requires the RELOAD privilege.

## Rationale

### MySQL

- Doc: The [schema_unused_indexes](https://dev.mysql.com/doc/refman/5.7/en/sys-schema-unused-indexes.html) view shows indexes for which there are no events, which indicates that they are not being used. By default, rows are sorted by schema and table.
- User interface
	- SQL:  `select * from schema_unused_indexes;`
	- Columns of schema_unused_indexes
		- object_schema (The schema name)
		- object_name (The table name)
		- index_name (The unused index name)
- The data for this view comes from the [table_io_waits_summary_by_index_usage](https://dev.mysql.com/doc/refman/5.7/en/table-waits-summary-tables.html#performance-schema-table-io-waits-summary-by-table-table). The table contains the following columns
	- object_typje, object_schema, object_name, index_name
	- In addition, there are columns related to statistical information with different granularities, such as: statistical information of all read operations, statistical information of write operations, or statistical information of all operations.

### SQL-Server

- Doc: [sys.dm_db_index_usage_stats](https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-db-index-usage-stats-transact-sql?view=sql-server-ver15) returns counts of different types of index operations and the time each type of operation was last performed.
- User interface
	- SQL: `select * from sys.dm_db_index_usage_stats;`
	- Columns of sys.dm_db_index_usage_stats
		- database_id
		- object_id
		- index_id
		- The number and final execution time of different types of index operations, including seek, scan, lookup, update. Each operation distinguishes user operations and system operations
	- Whenever the index is used, the information in the table will be updated.

### Oracle

- Doc: [V$OBJECT_USAGE](https://docs.oracle.com/cd/B28359_01/server.111/b28320/dynviews_2077.htm#REFRN30162) displays statistics about index usage gathered from the database.
- User interface
	- SQL: `select * from v$object_usage;`
	- Columns of v$object_usage
		- index_name
		- table_name
		- monitoring
		- used
		- start_monitoring
		- end_monitoring
- Oracle can set whether to monitor an index.

## Compatibility and Migration Plan

MySQL supports `SCHEMA_UNUSED_INDEXES`. We are considering compatibility and also support this view.

## Implementation

My implementation plan is in [issues/19209](https://github.com/pingcap/tidb/issues/19209)

## Testing Plan

The test method is similar to general statistics.

## Open issues (if applicable)
