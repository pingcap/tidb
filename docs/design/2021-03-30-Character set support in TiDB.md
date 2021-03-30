# Proposal: Character set support in TiDB

- Author(s): [xiongjiwei](https://github.com/xiongjiwei)
- Last updated:  2021-03-30
- Discussion at: N/A

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This document is discuss charset in TiDB. It is intended to add a framework about charset in TiDB.

## Motivation or Background

MySQL has many charset support like `GBK`, `utf8`, `utf8mb4`, etc. However, only `utf8` and `utf8mb4` in TiDB are supported. Many users in China use `GBK` and, there are some issue related charset ([incorrect encoding for latin1](https://github.com/pingcap/tidb/issues/18955), [extended latin not support](https://github.com/pingcap/tidb/issues/3888)). It is hard to add a charset in TiDB now, and this proposal describes the implementation of charset framework. 

## Detailed Design

Let us use `GBK` charset as an example. When the charset framework is done, user can:

- Send `GBK` encoding query and TiDB can handle it.
- User can create database, table, column with `GBK` charset.
- It shall be available for sql/command such as `SET NAMES`, `_gbk`, `CHARACTER SET gbk`, etc.
- Conversion between `GBK` and other charsets(`utf8`, etc.) shall be supported. So SQLs like `CONVERT (xxx USING gbk)` shall work.


### Some detail in MySQL

There are some important system variables in MySQL about charset: 

- `character_set_client`, 
- `character_set_connection`, 
- `character_set_results`. 

The server will take `character_set_client` to be the character set in which statements are sent by the client.

The server converts statements sent by the client from `character_set_client` to `character_set_connection`. Exception: For string literals that have an introducer

The server will take `character_set_results` to be the character set in which the server returns query results to the client. This includes result data such as column values, result metadata such as column names, and error messages. If `character_set_results` is set to `NULL` or `binary`, the server will do no conversion.

What MySQL does:

1. convert identifier from `character_set_client` to `character_set_system` which is always `utf8`.
2. if string literal has no [charset introducer](https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html), convert it from `character_set_client` to `character_set_connection` and mark `character_set_connection` as the charset of string literal.
3. if string literal has [charset introducer](https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html), mark the `charset introducer` as the charset of string literal.
3. convert result data, result metadata and error message to `character_set_results` and return to client.

### Note.

MySQL handle escape string using `character_set_client` even if it has `charset introducer`.

example:
```sql
mysql> SET NAMES latin1;
mysql> SELECT HEX('à\n'), HEX(_sjis'à\n');
+------------+-----------------+
| HEX('à\n') | HEX(_sjis'à\n') |
+------------+-----------------+
| E00A       | E00A            |
+------------+-----------------+

mysql> SET NAMES sjis;
mysql> SELECT HEX('à\n'), HEX(_latin1'à\n');
+------------+-------------------+
| HEX('à\n') | HEX(_latin1'à\n') |
+------------+-------------------+
| E05C6E     | E05C6E            |
+------------+-------------------+
```

### What if convert target character set has no such character

use `?` instead or return error:
```sql
mysql> create table t(a char(10) charset utf8mb4);
mysql> insert into t values ('一');
mysql> select convert(a using latin1) from t;
+-------------------------+
| convert(a using latin1) |
+-------------------------+
| ?                       |
+-------------------------+
1 row in set (0.00 sec)
```
```sql
mysql> create table t(a char(10) charset latin1);
mysql> insert into t values ('一');
ERROR 1366 (HY000): Incorrect string value: '\xE4\xB8\x80' for column 'a' at row 1

mysql> create table t(a char(10) charset utf8mb4);
mysql> insert into t values ('一');
mysql> alter table t modify a char(10) charset latin1;
ERROR 1366 (HY000): Incorrect string value: '\xE4\xB8\x80' for column 'a' at row 1
```

### Character Set Repertoire

[Character Set Repertoire](https://dev.mysql.com/doc/refman/8.0/en/charset-repertoire.html) is used for mix use collation with different charset. It will be more complex to infer expression collation when it has other charset. 

We allow applying automatic character set conversion in some cases.
  The conditions when conversion is possible are:
  - arguments `A` and `B` have different charsets
  - `A` wins according to coercibility rules
  - character set of `A` is either **superset** for character set of `B`,
    or `B` is a string constant which can be converted into the
    character set of `A` without data loss.

  If all the above is true, then it's possible to convert
  `B` into the character set of `A`, and then compare according
  to the collation of `A`.

MySQL has the following rules to decide which collation wins:

1. if the two charsets are the same, or one of them is `binary`, always use lower [Coercibility](https://dev.mysql.com/doc/refman/8.0/en/charset-collation-coercibility.html). If `Coercibility` are equal,
    - and the two collations are all `xxx_bin`, it cannot be decided, unless there are third explicit collation, or return an error. (see below)
    - otherwise, use `xxx_bin` as the inferred collation, and the coercibility should be set to `1`

3. if charset is different,
    - if one is **superset** of another and has not greater `Coercibility`, use the superset collation.

```sql
mysql root@(none):test> create table t(a char(10) collate utf8mb4_bin, b char(10) collate utf8mb4_0900_bin);
Query OK, 0 rows affected
Time: 0.060s
mysql root@(none):test> insert into t values ('a', 'b');
Query OK, 1 row affected
Time: 0.014s
mysql root@(none):test> select a=b from t;
(1267, "Illegal mix of collations (utf8mb4_bin,IMPLICIT) and (utf8mb4_0900_bin,IMPLICIT) for operation '='")
```

**Superset**: 1) unicode is superset of non-unicode. 2) 4-byte utf8 a superset over 3-byte utf8. 3) `UNICODE` repertoire is superset of `ASCII` repertoire.

### Design

Add a field in `collationInfo`, which is `repertoire`, the value can be `ASCII` or `UNICODE`.

The interface will look like 
```go
type Charset interface {
	// convert encoding to rune
	Mb2Wc([]byte) rune
	// convert rune to encoding
	Wc2Mb(rune) []byte
}
```

There are 2 way to achieve the charset support: 

1. Do the same with MySQL, use the `gbk` encoding everywhere 
    - It may change a lot of code, such as builtin function `LEFT`, `REVERSE`. It's better to not use `string`, use `[]byte` instead.
    - Since the metadata also encoding with the `gbk`, the place we used it should also be considered.

2. Use `utf8` as intermediate encoding
    - It has overhead cost in some case, such as compare string with `gbk_bin` collation, which happens very often. We should convert to relate charset first and use collation to compare.
    - Other components need consider charset and do convert jobs.

### Parser

Now, `parser` parse the string literal with `utf8` encoding to handle the escape string, it should be changed to `character_set_client`. It implies we have to implement the interface in `parser` repo.

### ddl

DDL should support create database, table, column with charset. It should be same with MySQL. Also alter column charset [charset-conversion](https://dev.mysql.com/doc/refman/8.0/en/charset-conversion.html), [alter-table-character-set](https://dev.mysql.com/doc/refman/8.0/en/alter-table.html#alter-table-character-set) (change column type)

### Upgrade

Old cluster cannot use `gbk`, so there is no upgrade compatibility problem.
```sql
mysql root@localhost:test> create table t(a char(10) charset gbk)
(1115, "Unknown character set: 'gbk'")
mysql root@localhost:test> select _gbk'一'
(1054, "Unknown column '_gbk' in 'field list'")
```

### Misc

- `latin1` charset is treated as a subset of `utf8` for now, it should be corrected.
- When do restore SQL in `parser`, it should consider the charset.
- There are many places rely on the correct restore, we should test all of it.
- `TiKV`, `TiFlash`, `BR`, `TiCDC`, `Dumpling` should consider the charset.
- There will be a config to enable this feature(`tidb_enable_charset`). This feature should be enabled only when `new_collations_enabled_on_first_bootstrap` and `tidb_enable_charset` all true.

### Downgrade

If downgrade, we should convert the `non-utf8mb4` charset table to `utf8mb4` charset table. The metadata should also be changed. For the `binary` charset column, it may have no chance to convert to `utf8mb4` again.

## Test Design

### Functional Tests

- Unit tests should cover all the builtin string function with `gbk` charset.
- Unit tests should cover the query with different values of system variable `charset_set_xxx`.

### Scenario Tests

- Explicit use `gbk` charset shall be work.
- Mix use collation with different charset shall be work.
- If `new_collations_enabled_on_first_bootstrap` is `false`, this feature should not enable.

### Compatibility Tests

### Benchmark Tests

## Impacts & Risks

- We support a widely used feature, and some `latin` charset related issues can be solved.
- It may have performance regression.

## Unresolved Questions

- Which way should we choice to implement.