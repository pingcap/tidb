# Proposal: Collations in TiDB

- Author(s):     [Wang Cong](http://github.com/bb7133), [Kolbe Kegel](http://github.com/kolbe)
- Last updated:  2020-02-09
- Discussion at: https://github.com/pingcap/tidb/issues/14573

## Abstract

For now, TiDB only supports binary collations, this proposal is aimed to provide full collations support for TiDB.

## Background

### Collations

The term "Collation" is defined as the process and function of determining the sorting order of strings of characters, it varies according to language and culture.

Collations are useful when comparing and sorting data. Different languages have different rules for how to order data and how to compare characters. For example, in Swedish, "ö" and "o" are in fact different letters, so they should not be equivalent in a query (`SELECT ... WHERE col LIKE '%o%'`); additionally, Swedish sorting rules place "ö" after "z" in the alphabet. Meanwhile, in German, "ö" is officially considered a separate letter from "o", but it can be sorted along with "o", expanded to sort along with "oe", or sort at the end of the alphabet, depending on the context (dictionary, phonebook, etc.). In French, diacritics never affect comparison or sorting. The issue becomes even more complex with Unicode and certain Unicode encodings (especially UTF-8) where the same logical character can be represented in many different ways, using a variable number of bytes. For example, "ö" in UTF-8 is represented by the byte sequence C3B6, but it can also be represented using the "combining diaeresis" as 6FCC88. These should compare equivalently when using a multibyte-aware collation. Implementing this functionality as far down as possible in the database reduces the number of rows that must be shipped around.

### Current collations in MySQL

In MySQL, collation is treated as an attribute of the character set: for each character set has options for different collations(and one of them is the default collation), every collation belongs to one character set only. The effects of collation are:

  * It defines a total order on all characters of a character set.
  * It determines if padding is applied when comparing two strings.
  * It describes comparisons of logically identical but physically different byte sequences.
Some examples of the effects of collation can be found in [this part](https://dev.mysql.com/doc/refman/8.0/en/charset-collation-effect.html) of the MySQL Manual and [this webpage](https://web.archive.org/web/20171030065123/http://demo.icu-project.org/icu-bin/locexp?_=en_US&x=col).

Several collation implementations are described in [this part](https://dev.mysql.com/doc/refman/8.0/en/charset-collation-implementations.html) of MySQL Manual:

  * Per-character table lookup for 8-bit character sets, for example: `latin1_swedish_ci`.
  * Per-character table lookup for multibyte character sets, including some of the Unicode collations, for example: `utf8mb4_general_ci`.
    - Full UCA implementation for some Unicode collations, for example: `utf8mb4_0900_ai_ci`.

### Current collations in TiDB

For all character sets that are supported, TiDB accepts all of their collations by syntax and stores them as a part of metadata. The real collate-related behaviors are always in binary. For example,

```
tidb> create table t(a varchar(20) charset utf8mb4 collate utf8mb4_general_ci key);
Query OK, 0 rows affected
tidb> show create table t;
+-------+-------------------------------------------------------------+
| Table | Create Table                                                |
+-------+-------------------------------------------------------------+
| t     | CREATE TABLE `t` (                                          |
|       |   `a` varchar(20) COLLATE utf8mb4_general_ci NOT NULL,      |
|       |   PRIMARY KEY (`a`)                                         |
|       | ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin |
+-------+-------------------------------------------------------------+
1 row in set
tidb> insert into t values ('A');
Query OK, 1 row affected
tidb> insert into t values ('a');
Query OK, 1 row affected // Should report error "Duplicate entry 'a'"
```
In the case above, the user creates a column with a case-insensitive collation `utf8mb4_general_ci`. Since the 'real' collation of TiDB is always `utf8mb4_bin`/`binary`, inserting data into the primary key with "A" and "a" does not lead to an error.

What's more, the collation `utf8mb4_bin` in MySQL is defined with attribute `PAD SPACE`, that is, trailing spaces are ignored when comparison. Currently `utf8mb4_bin` in TiDB is actually with attribute `NO PAD`:

```
tidb> create table t1(a varchar(20) charset utf8mb4 collate utf8mb4_bin key);
Query OK, 0 rows affected
tidb> insert into t1 values ('a')
Query OK, 1 row affected
tidb> insert into t1 values ('a ');
Query OK, 1 row affected // Should report error "Duplicate entry 'a '"
```

### Changes to make

Before diving into the details, we should notice that ALL strings in MySQL(and TiDB as well) are with implicit collations, if they're not explicitly specified. So, if we aim to update the collations in TiDB, codes related to strings comparison/lookup should be checked:

  - Encoding/Decoding: update codec related functions in `codec`/`tablecodec` package.
  - SQL Runtime:
	  + implement `WEIGHT_STRING()` expression, which returns the sorting hexadecimal value(or `sortKeys`) for a string. This expression will be helpful for debugging collations.
      + update like/regex/string comparison related functions in `expression` package.
	  + update the comparisons logic in Join/Aggregation executors in `executor` package.
	  + update the codes in `UnionScan`(the internal buffer of transaction).
  - SQL Optimizer: the optimizer may need to be alignd with the encoding changes in `planner` package.
  - DDL/Schema: check if the new collation is supported according to the versions of tables/columns, the related codes are in `ddl` and `infoschema` package.
  - Misc
      + update string comparison related functions in `util` package.
      + check if range functions defined in table partitions work with collations.

## Proposal

### Collate Interface

There are bascially two functions needed for the collate comparison:

  * Function that is used to compare two strings according to the given collation.
  * Function that is used to make a memory-comparable `sortKey` corresponding to the given string.

The interface can be defined as following:

```
type CharsetCollation interface {
    // Compare returns an integer comparing the two byte slices. The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
    Compare(a, b []byte) (int, error)
    // Key returns the collation key for str, the returned slice will point to an allocation in Buffer.
    Key(buf *Buffer, str []byte) ([]byte, error)
}
```
The interface is quite similar to the Go [collate package](https://godoc.org/golang.org/x/text/collate).

### Row Format

The encoding layout of TiDB has been described in our [previous article](https://pingcap.com/blog/2017-07-11-tidbinternal2/#map). The row format should be changed to make it memory comparable, this is important to the index lookup. Basic principle is that all keys encoded for strings should use the `sortKeys` result from `Key()`/`KeyFromString()` function. However, most of the `sortKeys` calculations are not reversible.

  * For table data, encodings stay unchanged. All strings are compared after decoding with the `Compare()` function.
  * For table indices, we replace current `ColumnValue` with `sortKey` and encode the `ColumnValue` to the value,:
    - For unique indices:
	```
      Key: tablePrefix{tableID}_indexPrefixSep{indexID}_sortKey
      Value: rowID_indexedColumnsValue
	```
    - For non-unique indices:
	```
      Key: tablePrefix{tableID}_indexPrefixSep{indexID}_sortKeys_rowID
      Value: indexedColumnsValue
	```

Pros: Value of the index can be recovered from `ColumnValue` in value. It is able to scan just the index in order to upgrade the on-disk storage to fix bugs in collations, because it has the `sortKey` and the value in the index.

Cons: The size of index value on string column with new collations is doubled(we need to write both `sortKey` and `indexedColumnsValue`, their sizes are equal for most of the collations).

#### Rejected Alternative

A possible alternative option was considered as following:

  * For table data, encodings stay unchanged.
  * For table indices, we replace current `ColumnValue` with `sortKey` without additional change:
    - For unique indices:
	```
      Key: tablePrefix{tableID}_indexPrefixSep{indexID}_sortKey
      Value: rowID
	```
    - For non-unique indices:
    ```
      Key: tablePrefix{tableID}_indexPrefixSep{indexID}_sortKeys_rowID
      Value: null
	```

Pros:
    The current encoding layout keeps unchanged.

Cons:
    Since index value can not be recovered from `sortKey`, extra table lookup is needed for current index-only queries, this may lead to performance regressions and the "covering index" can not work. The extra table lookup also requires corresponding changes to the TiDB optimizer.

The reason to reject this option is, as a distributed system, the cost of potential table lookup high so that it should be avoided it as much as possible. What's more, adding a covering index is a common SQL optimization method for the DBAs: table lookup can not be avoided through covering index is counter-intuitive.

## Compatibility

### Compatibility with MySQL

As stated before, MySQL has various different collation implementations. For example, in MySQL 5.7, the default collation of `utf8mb4` is `utf8mb4_general_ci`, which has two main flaws:

  * It only implement the `simple mappings` of UCA.
  * It used a very old version of UCA.

In MySQL 8, the default collation of `utf8mb4` has been changed to `utf8mb4_0900_ai_ci`, in which the Unicode version is upgraded to 9.0, with full UCA implementation.

Should TiDB aim to support all collation of MySQL?

  * If TiDB supports the latest collations only(for example, support `utf8mb4_0900_ai_ci` only and treat `utf8mb4_general_ci` as one of its alias). This seems to be 'standard-correct' but may lead to potential compatibility issues.
  * If TiDB supports all the collations, all of their implementations should be analyzed carefully. That is a lot of work.

### Compatibility between TiDB versions

In this proposal, both of the compatibility issues can be solved in the new version of TiDB, and with the help of the version of metadata in TiDB, we can always indicate if a collation is old(the "fake one") or new(the "real one"). However, here we propose several requirements below and careful considerations are needed to meet them as many as possible:

  1. For an existing TiDB cluster, its behavior remains unchanged after upgrading to newer version
    - a. After upgrade, the behavior of existing tables remains unchanged.
    - b. After upgrade, the behavior of newly-created tables remains unchanged.
    - c. If a TiDB cluster is replicating the data to a MySQL instance through Binlog, all  replications remain unchanged.
  2. Users of MySQL who requires collations can move to TiDB without modifying their scripts/applications.
  3. Users of TiDB can distinguish the tables with old collation and new collations easily, if they both exist in a TiDB cluster.
  4. All replications/backups/recovers through TiDB tools like Binlog-related mechanism, CDC and BR should not encounter unexpected errors caused from the changes of collations in TiDB. For example, 
    - When using BR, if backup data from old cluster is applied to the new cluster, the new cluster should know that the data is binary collated without padding, or otherwise 'Duplicate entry' error may be reported for primary key/unique key columns.
    - If we don't allow both old collations and new collations exist in a TiDB cluster(the Option 4, see below), trying to make replications/backup & recovery between old/new clusters will break this prerequisite.

Based on the requirements listed above, the following behaviors are proposed:

1. Only TiDB clusters that initially boostrapped with the new TiDB version are allowed to enable the new collations. For old TiDB clusters, everything remains unchanged after the upgrade.

2. We can also provide a configuration entry for the users to choose between old/new collations when deploying the new clusters.

3. Mark old collations with syntax comment. For a upgraded TiDB cluster, add comment like "`/* AS_BINARY_COLLATION */`" for the old collations; all collations with "`/* AS_BINARY_COLLATION */`" comment are treated as the old ones.

Pros: Requirement 1 and 2 are met, requirement 3 can be met if we allow old/new collations exist in the same cluster in the future, since old collations are marked with syntax comment.

Cons: Since existing TiDB cluster can't get new collations enabled, requirement 3 is eliminated; Requirement 4 is not met.

#### Rejected Alternatives

The main reason to reject those options are: new collations that are not exist in MySQL have to be added, which may be confusing to the users and potentially break the compatibility between TiDB and MySQL.

##### Option 1

Add a series of new collatins named with the suffix "`_np_bin`"(meaning "NO PADDING BINARY"), for example, `utf8mb4_np_bin`. Such new collations don't exist in MySQL, but they're the "real ones" used by current TiDB. After upgrading to newer TiDB versions, all old collations are shown as "`_np_bin`", MySQL collations behave the same with MySQL.

Pros: Requirement 1.a, 2, 3 and 4 are met.

Cons: Requirement 1.b, 1.c are not met.

##### Option 2

Keep all collations defined in TiDB as what they were, define a series of new collations that are actually compatible with MySQL collations. For example, we can create a new `tidb_utf8_mb4_general_ci` that is the same as `utf8mb4_general_ci` in MySQL. When TiDB users want the "real" collations, they can modify their `CREATE TABLE` statements to use the new ones.

Pros: Requirement 1.a, 1.b and 3 are met.

Cons: Requirement 1.c, 2 and 4 are not met, the namings are also confusing: collations that are named from MySQL are different from MySQL, but the ones named different from MySQL behave the same as MySQL.

#### Bug-fixing

We should also expect potential bugs in implementations of collations and get prepared for fixing them in the newer version of TiDB. However, this is not trivial since the collation affects the on-storage data, so a online data reorganization is required, which is a known limitation of TiDB. For now, another proposal is planning to support reorganizing data(the "online column type change"), we can expect it to be finished in the near future. Based on that, a possible solution for collation bug-fixing can be:

  * Store a new version for the collations in the metadata of the column, it can be named as "CollationVersion" just like the current [ColumnInfoVersion](https://github.com/pingcap/parser/blob/b27aebeca4ba3fd938900fb57c0ea45c55d3753a/model/model.go#L66). This version is used to tracking the on-storage data generated by the collation.
  * When we want to fix the bugs, related column index can be reorganized by a TiDB-specific DDL SQL like `ALTER TABLE ... TIDB UPGRADE COLLATION`. When the job is done, collation version is updated to the latest one.

## Implementation

### Collations for 8-bit charsets

The implementation can be simply per-character mapping, same as MySQL.

### Collations for Unicode charsets

The following features of the general collation algorithm will be supported:

  * Primary Weight i.e. character
  * Secondary Weight i.e. accent
  * Tertiary Weight i.e. case
  * PAD / NOPAD

All of them are supported by `text/collate` package of Go, so it is possible to map Go collations to some of UCA-based collations in MySQL like `utf8mb4_unicode_ci`/`utf8mb4_0900_ai_ci`, if we ignore the differences between UCA versions: current `text/collate` uses UCA version `6.2.0` and it is not changable. However, the collations in MySQL are with different UCA versions marked in the names, for example, `utf8mb4_0900_ai_ci` uses version `9.0`.

For non-standard UCA implementations in MySQL, i.e. the `utf8mb4_general_ci`. The implementation depends on our choice to the [Compatibility with MySQL](#compatibility-with-mysql) chapter, if a 100% compatibility of `utf8mb4_general_ci` is chosen, we need to implement it by our hands.

### Collations in TiKV

For all collations supported by TiDB, they should be supported by TiKV as well so that the coprocessors can be pushed-down. Several libraries may be helpful for the work:

  * [Rust unicode_collation](https://docs.rs/unicode-collation/0.0.1/unicode_collation)
  * [Rust servo/rust-icu](https://github.com/servo/rust-icu)
  * [C++ std::collate](https://en.cppreference.com/w/cpp/locale/collate)
  * [C/C++ ICU4C](https://unicode-org.github.io/icu-docs/apidoc/released/icu4c)

The libraries above work for standard UCA only, for all other collations, implementations should be done from nothing.

## Migration Plans

The plan depends on the option chosen in [Compatibility](#compatibility) chapter.

  * For existing TiDB clusters with current binary collations, nothing need to be done if the users are happy with them.
  * For the potential migrations from MySQL to TiDB:
     - If the collations used in MySQL have been implemented by TiDB, users from MySQL do not need to care about the collations when mirgrating to TiDB except Compatibility Option 2, in which the those collations need to be updated to their corresponding names.
     - If there are colltions that are not supported by TiDB yet, users may need to change the them to the supported ones and check if no constraint is broken after the change. The check can be done following the approach mentioned in [this article](https://mysqlserverteam.com/mysql-8-0-collations-migrating-from-older-collations).

## Testing Plans

Here lists possible tests that can be done:

  * The unit / integration tests corresponding to the updates in [Changes to make](#changes-to-make)
  * The unit / integration tests corresponding to the [implementations](#implementation)
  * The collations cases that ported from mysql-tests
  * The copr-pushdown tests of TiDB/TiKV

## Possible Future Works

We have an opportunity to do a much better job than MySQL has done.

A collation, logically, should not be a property of a character set. Since all character sets are subsets of Unicode, collations can apply to Unicode codepoints without needing to be tied to individual character sets.

Further, a collation should not need to be a property of a column, it could rather be a property of an index, so that multiple collations can be defined for individual database objects. This would permit efficient querying of data by many different means.

## Related issues

- https://github.com/pingcap/tidb/issues/222
- https://github.com/pingcap/tidb/issues/1161
- https://github.com/pingcap/tidb/issues/3580
- https://github.com/pingcap/tidb/issues/4353
- https://github.com/pingcap/tidb/issues/7519
- https://github.com/pingcap/tidb/issues/10192

