# Proposal: support `pinyin` order for `utf8mb4` charset

- Author(s):     [xiongjiwei](https://github.com/xiongjiwei)
- Last updated:  2020-10-16
- Discussion at: https://github.com/pingcap/tidb/issues/19747

## Abstract
This proposal proposes a new feature that supports `pinyin` order for chinese character.

## Background
It's unable now to order by a column based on it's pinyin order. For example:

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

`pinyin` order for Chinese character supported by this proposal will add a new collation named `utf8mb4_general_zh_ci` which are exactly same with `gbk_chinese_ci`. Collation `utf8mb4_general_zh_ci` is for charset `utf8mb4`.

Following SQL statements should have same result.
```sql
# order
select * from t order by a collate utf8mb4_general_zh_ci;
select * from t order by convert(a using gbk);
select * from t order by convert(a using gbk) collate gbk_chinese_ci;

# sort key
select weight_string(a collate utf8mb4_general_zh_ci);
select weight_string(convert(a using gbk));
select weight_string(convert(a using gbk) collate gbk_chinese_ci);

# like
select a collate utf8mb4_general_zh_ci like 'regex';
select convert(a using gbk) like 'regex';
select convert(a using gbk) collate gbk_chinese_ci like 'regex';
```

### Advantages

It's a lot of work if implements `utf8mb4_zh_0900_as_cs`. The implementation of MySQL looks complicated with weight reorders, magic numbers, and some sort of trick. Before `utf8mb4_zh_0900_as_cs` collation, user archive `pinyin` order by order by `convert(... using gbk)`, it is much easier if implements `utf8mb4_general_zh_ci` exactly the same with `gbk_chinese_ci`.

### Disadvantages

It is not compatible with MySQL.

Do not support all chinese character

## Rationale

### How to implement

#### Compare and Key

Since `utf8mb4_general_zh_ci` has same behavior with `convert(... using gbk) collate gbk_chinese_ci`, `utf8mb4_general_zh_ci` should looks like convert `utf8mb4` to `gbk` first and then use collation `gbk_chinese_ci`. In real implementation, we create a map to maps all `utf8mb4` characters which can be convert to `gbk` to weight string of collation `gbk_chinese_ci` of these characters. For characters which cannot convert to `gbk` should regarded as equal to character `?`.

#### LIKE

Since `convert(a using gbk) = '?'` is `true` if a is a character which cannot convert to `gbk`, There has 
```sql
# MySQL
select convert('𠀅' using gbk) like '?', convert('𠀅' using gbk) regexp '\\?';
+---------------------------------+-------------------------------------+
| convert('?' using gbk) like '?' | convert('?' using gbk) regexp '\\?' |
+---------------------------------+-------------------------------------+
|                               1 |                                   1 |
+---------------------------------+-------------------------------------+
1 row in set (0.00 sec)

# TiDB
select '𠀅' collate utf8mb4_general_zh_ci like '?', '𠀅' utf8mb4_general_zh_ci regexp '\\?';
+---------------------------------+-------------------------------------+
| convert('?' using gbk) like '?' | convert('?' using gbk) regexp '\\?' |
+---------------------------------+-------------------------------------+
|                               1 |                                   1 |
+---------------------------------+-------------------------------------+
1 row in set (0.00 sec)
```

### Parser

choose collation ID `2048` for `utf8mb4_general_zh_ci` and add it into parser

> MySQL supports two-byte collation IDs. The range of IDs from 1024 to 2047 is reserved for user-defined collations. [see also](https://dev.mysql.com/doc/refman/8.0/en/adding-collation-choosing-id.html)

### Compatibility with current collations

`utf8mb4_general_zh_ci` has same priority with `utf8mb4_unicode_ci` and `utf8mb4_general_ci` which means these three collations incompatible with each other.

### Alternative
MySQL has a lot of language specific collation, for `pinyin` order, MySQL use collation `utf8mb4_zh_0900_as_cs`.

## Compatibility and Mirgration Plan

### Compatibility issues with MySQL

There is no `utf8mb4_general_zh_ci` collation in MySQL. We can comment `utf8mb4_general_zh_ci` when users need to replicate their data from TiDB to MySQL.

## Open issues (if applicable)

https://github.com/pingcap/tidb/issues/19747

https://github.com/pingcap/tidb/issues/10192
