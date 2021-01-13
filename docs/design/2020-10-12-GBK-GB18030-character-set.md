# Proposal: GBK/GB18030 character set

- Author(s): Wenjun Huang
- Last updated: 2020-10-12

## Abstract

This proposal proposes two new character sets, GBK and GB18030 in TiDB. Depending on the context, TiDB refers to "TiDB database" or "TiDB component". 

## Background

GBK and GB18030 are widely used when handling chinese characters. They are supported by most of the databases, such as MySQL, PostgreSQL. Currently, TiDB only support UTF-8 and its subsets. Therefore, TiDB can not handle queries encoded by GBK or GB18030. Besides, if users migrate from MySQL and use GBK or GB18030, they need modify the schema. 

## Proposal

To support two new character sets, GBK and GB18030, we need to make these things done:

1. Users can sent queries encoded by GBK or GB 18030, TiDB can process them correctly.
2. Users can designate GBK or GB 18030 as the character for a column, a table, etc. To be compatible with MySQL, when comparing two string using the default collation of GBK or GB 18030, pinyin order is expected.

The general idea is that we handle character set conversion in connection level, and store the UTF-8 encoded data. The details are as follows:

1. Before parsing an SQL, check if the `character_set_client` is UTF-8. If not, convert the SQL to UTF-8.

2. We always use UTF-8 as intermediate encoding in TiDB and TiKV. In MySQL, however, uses `character_set_connection`. We need to handle some corner cases. For example:

   ```mysql
   mysql> set @@character_set_connection=gbk;
   Query OK, 0 rows affected (0.00 sec)
   
   mysql> select hex("啊");
   +------------+
   | hex("啊")  |
   +------------+
   | B0A1       |
   +------------+
   1 row in set (0.00 sec)
   
   mysql> set @@character_set_connection=utf8;
   Query OK, 0 rows affected, 1 warning (0.00 sec)
   
   mysql> select hex("啊");
   +------------+
   | hex("啊")  |
   +------------+
   | E5958A     |
   +------------+
   1 row in set (0.00 sec)
   ```

3. When writing result to client, convert the result to `character_set_result` if it's not UTF-8.
4. If a column's character set is GBK or GB18030, to be able to write to MySQL, we need to check if the inserted value can be represented as GBK or GB18030. There should be a switch to control whether to check it.
5. Always store UTF-8 encoded data regardless of the column's character set. This is not the way MySQL does, the pro and con will be explained in the `rationale` section.
6. Support corresponding collation `gbk_bin` and `gb18030_bin`. Their "sort keys" are the GBK or GB18030 encoded value, and don't need restored data since the original data can be restored from the "sort keys".



## Rationale

This section will explain why we choose some different approaches compare to MySQL.

1. MySQL converts statements sent by the client from `character_set_client` to `character_set_connection`. Unlike MySQL, TiDB converts statements sent by the client from `character_set_client` to UTF-8.

   TiDB‘s program language is Golang and TiKV's program language is Rust. In Rust, the `str` must be UTF-8 encoded. In Golang, the `string` allows invalid UTF-8 char, but to use the string package correctly, the string must be UTF-8 encoded.

   **Pro**:

   Converting to UTF-8 is simple, and doesn't need to change too much code.

   **Con**:

   Need to handle some corner cases to be compatible with MySQL.

2. MySQL stores the column's character set encoded data. Unlike MySQL, TiDB stores UTF-8 encoded data.

   **Pro**:

   (1) Keep the encode and decode logics simple, don't need to change their logics.

   (2) Avoid extra runtime overhead to encode and decode.

   **Con**:

   (1) Chinese character need 2 bytes if encoded by GBK or GB18030, need 3 bytes if encoded by UTF-8. Storing UTF-8 encoded data needs more disk space than GBK or GB18030 encoded data.

   (2) To use Pinyin order, the `new_collations_enabled_on_first_bootstrap  ` must be true.

   (3) Other components, such as TiCDC, lightning, may need to deal with encode or decode too.

## Compatibility

1. Some results may be different with older version if involve character set. After implementing this feature, the results are correct and be compatible with MySQL.
2. Other components, such as TiCDC, lightning, need to consider this feature.
