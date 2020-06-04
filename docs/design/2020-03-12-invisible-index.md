# Proposal: Invisible index

- Author(s):     [Deardrops](https://github.com/Deardrops), [wjhuang2016](https://github.com/wjhuang2016)
- Last updated:  Mar. 12, 2020
- Discussion at: https://github.com/pingcap/tidb/issues/9246

## Abstract

MySQL supports [invisible indexes](https://dev.mysql.com/doc/refman/8.0/en/invisible-indexes.html); that is, indexes that are not used by the optimizer. 

This is a useful feature for dropping an index in a safe way. Invisible indexes make it possible to test the effect of removing an index on query performance, without making a destructive change that must be undone should the index turn out to be required. Dropping and re-adding an index can be expensive for a large table, whereas making it invisible and visible are fast, in-place operations.

Support the option of `VISIBLE | INVISIBLE`.
```
CREATE [...] INDEX index_name
    [index_type]
    ON tbl_name (key_part,...)
    [index_option]

index_option:
    {VISIBLE | INVISIBLE}
```

Also, the following behaviors need to be supported as well:

- Display information about invisible indexes in the output of `INFORMATION_SCHEMA.STATISTICS` or `SHOW INDEX`.
- Index hints need to report errors when used in invisible indexes.
- The primary key cannot be set to an invisible index.

## Background

Index has a great influence on the performance of the database. Whether there is an index or whether the optimizer chooses the right index determines the performance of reading and writing the database to a great extent. In some cases, we would like to know the impact of deleting an index on the read-write performance of the database.  At present, our method is to tell the optimizer to ignore the index through Index Hint.  Although this method can achieve the goal, it is not practical to modify all SQL statements.

## Proposal

Adding an option (visible or not) to the index. If it is not visible, it's called **Invisible Index**. Invisible indexes cannot be used by the optimizer (with the `use_invisible_indexes` switch on), but the index is maintained during DML operations. For a query statement, invisible indexes have the same effect as ignoring the index through Index Hint.

The option `INVISIBLE` of an index can be changed by using the following DDL statement:

```
ALTER TABLE table_name ALTER INDEX index_name { INVISIBLE | VISIBLE };
```

Or by setting option `INVISIBLE` when creating an index:

```
CREATE INDEX index_name ON table_name(key) [ INVISIBLE | VISIBLE ];
```

`INVISIBLE`, `VISIBLE` is as part of the index option, they can also be set when creating a table.

In order to know whether an index is invisible, it can be read from the `INFORMATION_SCHEMA.STATISTICS` table or through the `SHOW INDEX` command.

In addition, add a new flag `use_invisible_indexes` in system variable `optimizer_switch`, which determine whether the option `INVISIBLE` takes effect. If `use_invite_indexes` is on, the optimizer can still use invisible index.

A table with no explicit primary key may still have an effective implicit primary key if it has any UNIQUE indexes on NOT NULL columns. In this case, the first such index places the same constraint on table rows as an explicit primary key and that index cannot be made invisible.

## Rationale

Another solution for implement invisible indexes is: Indicate invisibility by DDL state `WriteOnly`. This solution has the following problems:

- The logic of schema change needs to be changed, which is relatively complicated to implement.
- Cannot distinguish between indexes that are currently in WriteOnly and invisible indexes.
- Handling the switch is troublesome.

## Compatibility and Migration Plan

This the a new feature and it's absolutly compatible with old TiDB versions, also, it does not impact any data migration.
The syntax and functions are basically compatible with MySQL expect:

	When use invisible index in `SQL Hint`, and set `use_invisible_indexes = false`, MySQL allow use the invisible index.
	But in TiDB, It's **not allowed** and an `Unresolved name` error will be thrown.

## Implementation

- Add syntax support in parser
- Add a new column `IS_VISIBLE` in `information_schema.statistics`
- Add a new column `VISIBLE` in `SHOW INDEX FROM table` statement
- Show invisiable column infomations in `SHOW CREATE TABLE` statement
- Add `use_invisible_indexes` in system variable `@@optimizer_switch`
- Add new error message `ERROR 3522 (HY000): A primary key index cannot be invisible`
- Ignore invisible index in optimizer and add unit tests

## Testing Plan

- Unit test
- Learn from MySQL test cases related to invisible indexes

## Open issues

https://github.com/pingcap/tidb/issues/9246
