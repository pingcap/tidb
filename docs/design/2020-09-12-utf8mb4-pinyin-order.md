# Proposal: support `pinyin` order for `utf8mb4` charset

- Author(s):     [xiongjiwei](https://github.com/xiongjiwei)
- Last updated:  2020-11-06
- Discussion at: https://github.com/pingcap/tidb/issues/19747

## Abstract
This proposal proposes a new feature that supports `pinyin` order for chinese character.

## Background
It's unable now to order by a column based on its pinyin order. For example:

```sql
create table t(
	a varchar(100)
)
charset = 'utf8mb4' collate = 'utf8mb4_zh_0900_as_cs';

# insert some data:
insert into t values ("中文"), ("啊中文");

# a query requires to order by column a in its pinyin order:
select * from t order by a;
+-----------+
| a         |
+-----------+
| 啊中文    |
| 中文      |
+-----------+
2 rows in set (0.00 sec)
```

## Proposal

`pinyin` order for Chinese character supported by this proposal will add a new collation named `utf8mb4_zh_pinyin_tidb_as_cs` which is support all Unicode and sort Chinese characters correctly according to the PINYIN collation in zh.xml file of [CLDR24](http://unicode.org/Public/cldr/24/core.zip), and only support those Chinese characters with `pinyin` in zh.xml currently, we support neither those CJK characters whose category defined in Unicode are Symbol with the same shape as Chinese characters nor the PINYIN characters. In `utf8mb4_zh_pinyin_tidb_as_cs`, `utf8mb4` means charset utf8mb4, `zh` means Chinese language, `pinyin` means it has pinyin order, `tidb` means a special(tidb) version, and `as_cs` means it is accent-sensitive and case-sensitive.

### Advantages

It's a lot of work if we implement `utf8mb4_zh_0900_as_cs`. The implementation of MySQL looks complicated with weight reorders, magic numbers, and some tricks. Implementing `utf8mb4_zh_pinyin_tidb_as_cs` is much easier. It supports all Chinese characters and sorts Chinese characters in pinyin order. It is good enough.

### Disadvantages

It is not compatible with MySQL. MySQL does not have a collation named `utf8mb4_zh_pinyin_tidb_as_cs`.

## Rationale

### How to implement

#### Compare and Key

- For any Chinese character, which has non-zero seq NO. defined in zh.xml according to its gb18030 code, the final weight shall be 0xFFA00000+(seq No.)
- For any non-Chinese gb18030 character 2 bytes C, the final weight shall be C itself.
- For any non-Chinese gb18030 character 4 bytes C, the final weight shall be 0xFF000000+diff(C)(we get diff by Algorithm).

### Parser

Choose collation ID `2048` for `utf8mb4_zh_pinyin_tidb_as_cs` and add it into parser.

> MySQL supports two-byte collation IDs. The range of IDs from 1024 to 2047 is reserved for user-defined collations. [see also](https://dev.mysql.com/doc/refman/8.0/en/adding-collation-choosing-id.html)

### Compatibility with current collations

`utf8mb4_zh_pinyin_tidb_as_cs` has same priority with `utf8mb4_unicode_ci` and `utf8mb4_general_ci`, which means these three collations incompatible with each other.

### Alternative
MySQL has a lot of language specific collations, for `pinyin` order, MySQL uses collation `utf8mb4_zh_0900_as_cs`.

## Compatibility and Migration Plan

### Compatibility issues with MySQL

There is no `utf8mb4_zh_pinyin_tidb_as_cs` collation in MySQL. We can comment `utf8mb4_zh_pinyin_tidb_as_cs` when users need to replicate their data from TiDB to MySQL.

## Open issues (if applicable)

https://github.com/pingcap/tidb/issues/19747

https://github.com/pingcap/tidb/issues/10192
