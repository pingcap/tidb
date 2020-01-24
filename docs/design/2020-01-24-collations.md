# Proposal: Collations in TiDB

- Author(s):     [Wang Cong](http://github.com/bb7133), [Kolbe Kegel](http://github.com/kolbe)
- Last updated:  2020-01-24
- Discussion at: https://github.com/pingcap/tidb/issues/14573

## Abstract

For now, TiDB only supports binary collations, this proposal is aimed to provide full collation support for TiDB.

## Background

The term "Collation" is defined as the process and function of determining the sorting order of strings of characters, it varies according to language and culture.

Collations are useful when comparing and sorting data. Different languages have different rules for how to order data and how to compare characters. For example, in Swedish, "ö" and "o" are in fact different letters, so they should not be equivalent in a query (`SELECT ... WHERE col LIKE '%o%'`); additionally, Swedish sorting rules place "ö" after "z" in the alphabet. Meanwhile, in German, "ö" is officially considered a separate letter from "o", but it can be sorted along with "o", expanded to sort along with "oe", or sort at the end of the alphabet, depending on the context (dictionary, phonebook, etc.). In French, diacritics never affect comparison or sorting. The issue becomes even more complex with Unicode and certain Unicode encodings (especially UTF-8) where the same logical character can be represented in many different ways, using a variable number of bytes. For example, "ö" in UTF-8 is represented by the byte sequence C3B6, but it can also be represented using the "combining diaeresis" as 6FCC88. These should compare equivalently when using a multibyte-aware collation. Implementing this functionality as far down as possible in the database reduces the number of rows that must be shipped around.

In MySQL, collation is treated as an attribute of the character set: for each character set has options for different collations(and one of them is the default collation), every collation belongs to one character set only. The effects of collation are:
  * It defines a total order on all characters of a character set.
  * It determines if padding is applied when comparing two strings.
  * It describes comparisons of logically identical but physically different byte sequences.
Some examples of the effects of collation can be found in [this part](https://dev.mysql.com/doc/refman/8.0/en/charset-collation-effect.html) of the MySQL Manual and [this webpage](http://demo.icu-project.org/icu-bin/locexp?_=en_US&x=col).

Several collation implementations are described in [this part](https://dev.mysql.com/doc/refman/8.0/en/charset-collation-implementations.html) of MySQL Manual:
  * Per-character table lookup for 8-bit character sets, for example: `latin1_swedish_ci`.
  * Per-character table lookup for multibyte character sets, including some of the Unicode collations, for example: `utf8mb4_general_ci`.
    - Full UCA implementation for some Unicode collations, for example: `utf8mb4_900_ai_ci`.

## Proposal

### Collate Interface

There are two basic functions needed for collation `sortKey` calculation:
  * Function that is used to compare two strings according to the given collation.
  * Function that is used to make a memory-comparable `sortKey` corresponding to the given string.
The interface can be defined as following:

```
type CollAttrFlag int

const (
    FlagIgnoreCase uint64=1 << iota
    FlagIgnoreAccent
    FlagPadding
)

type CharsetCollation interface {
    // Compare returns an integer comparing the two byte slices. The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
    Compare(a, b []byte) (int, error)
    // CompareString returns an integer comparing the two strings. The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
    CompareString(a, b string) (int, error)
    // Key returns the collation key for str, the returned slice will point to an allocation in Buffer.
    Key(buf *Buffer, str []byte) error
    // KeyFromString returns the collation key for str, the returned slice will point to an allocation in Buffer.
    KeyFromString(buf *Buffer, str string) error

    /******* Possible Optimization ********/
    // FromKey returns the original byte slice from collation key.
    FromKey(buf *Buffer, str []byte) error
    // StringFromKey returns the original string from collation key.
    StringFromKey(str []byte) string
}
```
The interface is quite similar to the Go [collate package](https://godoc.org/golang.org/x/text/collate), it is possible to implement some of the MySQL collation by simply mapping it to an existing Go collation.

### Row Format

The encoding layout of TiDB has been described in our [previous article](https://pingcap.com/blog/2017-07-11-tidbinternal2/#map). The row format should be changed to make it memory comparable, this is important to the index lookup. Basic principle is that all keys encoded should use the `sortKeys` result from `Key()`/`KeyFromString()` function. However, most of the `sortKeys` calculations are not reversible.

#### Option 1

  * For table data, encodings stay unchanged. All strings are compared after decoding with the `Compare()` function.
  * For table indices, we replace current `ColumnValue` with `sortKey`:
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
     Since index value can not be recovered from `sortKey`, extra table lookup is needed for current Index-only queries, this may lead to performance regressions. The extra table lookup also requires corresponding changes to the TiDB optimizer.

#### Option 2

  * Table data encodings is the same with Option 1.
  * For table indices, we encode the `ColumnValue` to the value of the storage, so as to avoid extra table lookup for index queries:
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

Pros: Extra table lookup is avoided and no change to the TIDB optimizer.

Cons: The size of index value on string column with new collations is doubled(we need to write both `sortKey` and `indexedColumnsValue`, their sizes are equal for most of the collations).

## Compatibility

### Compatibility with MySQL

As stated before, MySQL has various different collation implementations. For example, in MySQL 5.7, the default collation of `utf8mb4` is `utf8mb4_general_ci`, which has two main flaws:
  * It only implement the `simple mappings` of UCA.
  * It used a very old version of Unicode.
In MySQL 8, the default collation of `utf8mb4` has been changed to `utf8mb4_900_ai_ci`, in which the Unicode version is upgraded to 9.0, with full UCA implementation.

Should TiDB aim to support all collation of MySQL?
  * If TiDB supports the latest collations only(for example, support `utf8mb4_900_ai_ci` only and treat `utf8mb4_general_ci` as one of its alias). This seems to be 'standard-correct' but may lead to potential compatibility issues.
  * If TiDB supports all the collations, all of their implementations should be analyzed carefully. That is a lot of work.

### Compatibility between TiDB versions

#### Current Status

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
In the case above, the user creates a column with a case-insensitive collation `utf8mb4_general_ci`. Since the 'real' collation of TiDB is always `utf8mb4_bin`, inserting data into the primary key with "A" and "a" does not lead to an error.

What's more, the collation `utf8mb4_bin` in MySQL is defined with attribute `PAD SPACE`, that is, trailing spaces are ignored when comparison. Currently `utf8mb4_bin` in TiDB is with attribute `NO PAD`:

```
tidb> create table t1(a varchar(20) charset utf8mb4 collate utf8mb4_bin key);
Query OK, 0 rows affected
tidb> insert into t1 values ('a')
Query OK, 1 row affected
tidb> insert into t1 values ('a ');
Query OK, 1 row affected //Should report error "Duplicate entry 'a '"
```

#### Requirements

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

#### Option 1

Add a series of new collatins named with the suffix "`_np_bin`"(meaning "NO PADDING BINARY"), for example, `utf8mb4_np_bin`. Such new collations don't exist in MySQL, but they're the "real ones" used by current TiDB. After upgrading to newer TiDB versions, all old collations are shown as "`_np_bin`", MySQL collations behave the same with MySQL.

Pros: Requirement 1.a, 2, 3 and 4 are met.

Cons: Requirement 1.b, 1.c are not met.

#### Option 2

Keep all collations defined in TiDB as what they were, define a series of new collations that are actually compatible with MySQL collations. For example, we can create a new `tidb_utf8_mb4_general_ci` that is the same as `utf8mb4_general_ci` in MySQL. When TiDB users want the "real" collations, they can modify their `CREATE TABLE` statements to use the new ones.

Pros: Requirement 1.a, 1.b, 2 and 3 are met.

Cons: Requirement 1.c and 4 are not met, the namings are also confusing: collations that are named from MySQL are different from MySQL, but the ones named different from MySQL behave the same as MySQL.

#### Option 3

Add no new collation, tag old collations with syntax comment. For a upgraded TiDB cluster, add comment like "`/* AS_BINARY_COLLATION */`" for the old collations; all collations with "`/* AS_BINARY_COLLATION */`" comment are treated as the old ones.


Pros: Requirement 1.a, 2, 3 and 4 are met.

Cons: Requirement 1.b and 1.c are not met.

#### Option 4

Only TiDB clusters that initially boostraped with the new TiDB version are allowed to enable the new collations. For old TiDB clusters, everything remains unchanged after the upgrade.

We can also provide a configuration entry for the users to choose between old/new collations when deploying a new clusters.

Pros: Requirement 1 and 2 are met.

Cons: Since existing TiDB cluster can't get new collations enabled, requirement 3 is eliminated; Requirement 4 is not met.

## Implementation

All strings in MySQL are with implicit collations, if they're not explicitly specified, so we may need to check/update all codes related to strings comparison/lookup:

  - Encoding/Decoding: update codec related functions in `codec`/`tablecodec` package.
  - SQL Runtime: update like/regex/string comparison related functions in `expression` package.
  - SQL Optimizer: If option 1 for encodings is chosen, the optimizer should build the extra  table lookup plans.
  - DDL/Schema: check if the collation is old/new from the versions of tables/columns.
  - Misc
      + update string comparison related functions in `util` package.
      + check if range functions defined in table partitions work with collations.

### Collations in TiKV

For all collations supported by TiDB, TiKV needs to do corresponding updates so that the coprocessors can be pushed-down. Several libraries may be helpful for the work:
  * [Rust unicode_collation](https://docs.rs/unicode-collation/0.0.1/unicode_collation)
  * [Rust servo/rust-icu](https://github.com/servo/rust-icu)
  * [C++ std::collate](https://en.cppreference.com/w/cpp/locale/collate)
  * [C/C++ ICU4C](https://unicode-org.github.io/icu-docs/apidoc/released/icu4c)

Unfortunately, the libraries above work for Unicode only. Even for Unicode, MySQL has some non-standard implemented collations. Maybe we can only implement them from nothing.

## Possible Future Works

We have an opportunity to do a much better job than MySQL has done.

A collation, logically, should not be a
property of a character set. Since all character sets are subsets of Unicode, collations can apply to Unicode codepoints without needing to be tied to individual character sets.

Further, a collation should not need to be a property of a column, it could rather be a property of an index, so that multiple collations can be defined for individual database objects. This would permit efficient querying of data by many different means.

## Open issues

- https://github.com/pingcap/tidb/issues/222
- https://github.com/pingcap/tidb/issues/1161
- https://github.com/pingcap/tidb/issues/7519

