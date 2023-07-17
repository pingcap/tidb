# Multi-valued index Design Documents

- Author(s): [Xiong Jiwei](http://github.com/xiongjiwei), [Zhang Yuanjia](https://github.com/qw4990)

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Impacts & Risks](#impacts--risks)

## Introduction

This document introduces the technical design of implementing 'Multi-valued index' in TiDB.

## Motivation or Background

Multi-valued index is a new feature introduced in MySQL 8.0.17, which allows defining indexes on a JSON array and use the index via JSON functions, similar to MongoDB in Multikey Indexes. e.g

```sql
CREATE TABLE t1 (data JSON);
CREATE INDEX zips ON t1((CAST(data->'$.zip' AS UNSIGNED ARRAY)));

INSERT INTO t1 VALUES

('{"id":1, "zip": [0,111,333]}'),('{"id":2, "zip": [123,456,0]}'),
('{"id":3, "zip": [123,123,111]}'),
('{"id":4, "zip": [456,567,222]}'),
('{"id":5, "zip": []}');

mysql> SELECT * FROM t1 WHERE 123 MEMBER OF (data->'$.zip');
+-----------------------------------+
| data                              |
+-----------------------------------+
| {"id": 2, "zip": [123, 456, 0]}   |
| {"id": 3, "zip": [123, 123, 111]} |
+-----------------------------------+
2 rows in set (0.01 sec)
```

That is: N index records point to one row record (N: 1)
A common scenario involves having rows with associated tags and the need to efficiently query all data containing a specific tag.

## Detailed Design

### Overview

- Using `cast(... as ... array)` to define a multi-valued index, which is essentially an expression index whose virtual column type is "array with type". The index is encoded in the same way as normal secondary indexes.
- The update of the multi-valued index behaves the same as the normal secondary index but the modification of one row may produce changes to multiple index records. If the type of array element is not satisfied the index definition, an error will be reported.
- Use `MEMBER OF`, `JSON_CONTAINS(subset)`, `JSON_OVERLAPS(intersection)` functions in where condition to using the multi-valued index.

### Encoding

The encoding of each index record is identical to the normal secondary index(see [TiDB Index Key/Value Format](https://docs.google.com/document/d/1Co5iMiaxitv3okJmLYLJxZYCNChcjzswJMRr-_45Eqg/edit) for more details).

For string types, the encoding result in TiDB is collation-aware, we could use `binary` collation for strings(in MySQL it is `utf8mb4_0900_as_cs` and behaves almost the same as `binary`).

- A row record may have multiple index records corresponding to it
```
row ('pk1', [1, 1, 2]) 
produces index records
1 -> 'pk1'
2 -> 'pk1'
```

- Multi-valued index can be a compound index
```
row ('pk1', c1, [1, 1, 2], c2) 
produces index records
(c1,1,c2) -> 'pk1'
(c1,2,c2) -> 'pk1'
```

- Multi-valued index can be a unique index

### Parser

New syntax: use `cast(... as... array)` to create the index. Add an `Array` field in `FuncCastExpr` indicate the use of this syntax.
```golang
type FuncCastExpr struct {
    //...
    Array bool
}
```

### Expression

Use `JSONBinary` type as the return type of expression `cast(... as... array)`. In the `FieldType` structure, add an `array` field indicates whether the type is an array, and `tp` represents the type of elements in the array.
```golang
type FieldType struct {
   // tp is type of the array elements
   tp byte
   // array indicates whether the type is an array
   array bool
}
```

Implement new built-in functions `castAsTypedArrayFunctionSig`, `MEMBER OF` and `JSON_OVERLAPS`.

### DML

Data changes cause index changes, which are handled in the same way as normal secondary indexes.
- Insert: insert a new row record and add index records for each element in the array.
- Delete: delete the row record and delete index records for each element in the array.
- Update: delete the old index records and add new index records.


### DDL

- Multi-valued index can be composite index, but only one JSON array can be used in the index definition.
- Multi-valued index is an expression index, so it has the same restrictions as expression index.
- Multi-valued index can be a unique index, but the uniqueness is not guaranteed within the same JSON array.

## Planner

### Column substitute

The column in the where condition will substituted with the corresponding expression in the index definition if the query meet the following 3 requirements
- Where condition contains any of the 3 functions: `MEMBER OF`/`JSON_CONTAINS`/`JSON_OVERLAPS`.
- Functions' parameter type must consistent with multi-valued index definitions.
- The expression is consistent with multi-valued index definition.

For example, the index definition is `create index idx on t((cast(data->'$.zip' as unsigned array)))`, the where condition is `where 123 member of (data->'$.zip')`, the column `data->'$.zip'` is substituted with `cast(data->'$.zip' as unsigned array)`.

Index selection is the same as normal secondary index.

### Build the operator

For any of the 3 functions, we can use `IndexMerge` operator to fetch the data:
- <const> MEMBER OF (<expr>)
```
IndexMerge
        IndexRangeScan(<const>)
        TableRowIDScan
```

- JSON_CONTAINS(<expr>, [<c1>, <c2>, <c3>, ...])
```
IndexMerge(AND)
        IndexRangeScan(<c1>)
        IndexRangeScan(<c2>)
        IndexRangeScan(<c3>)
        ...
        TableRowIDScan
```

- JSON_OVERLAPS(<expr>, [<c1>, <c2>, <c3>, ...])
```
IndexMerge(OR)
        IndexRangeScan(<c1>)
        IndexRangeScan(<c2>)
        IndexRangeScan(<c3>)
        ...
        TableRowIDScan
```

Each `IndexRangeScan` is a `PointGet` like operator. It will fetch the row record by the index record. Since different indexes could match the same primary key, we need to use `IndexMerge` to filter the duplicated row records. For `JSON_CONTAINS` we should use `AND` type of `IndexMerge`, because only the primary key that contained in all the `IndexRangeScan` can be filtered. For `JSON_OVERLAPS` we should use `OR` to filter the row records.

If the multi-valued index is unique, it can be further optimized to `PointGet`.

## Impacts & Risks

### Limitations, and Characteristics
- Multi-valued index will only be used when the where condition contains any of the functions `MEMBER OF`/`JSON_CONTAINS`/`JSON_OVERLAPS`. So even if SQL contains hint, force index, use index, etc., it is not necessarily possible to force multi-valued index.
- `cast(... as ... array)` can only appear once in the composite index definition, and the casted column must be a JSON column.
- If multi-valued index is a unique index, then we have
```sql
-- Allowed:
INSERT INTO t1 VALUES('[1,1,2]');
INSERT INTO t1 VALUES('[3,3,3,4,4,4]');

-- Disallowed, report dup-key error:
INSERT INTO t1 VALUES('[1,2]');
INSERT INTO t1 VALUES('[2,3]');
```
- Nullability
  - If the write data is an empty array, there will be no corresponding index record. Therefore: `not xxx` cannot use the index, because empty array data cannot access through the index.
    - workaround: `select * from t where pk not in (select pk from t where a member of (xxx))`.
  - If a column is null, add a null index record.
  - Null is not allowed as an array item, trying to write will report an error.
- The type of multi-valued index cannot be `BINARY`, `JSON`, and `YEAR`.
- Multi-valued index cannot be a primary key or a foreign key.
- Storage Space and Performance:
  - average number of array items * secondary index uses space.
  - Compared to normal indexes, DML will produce more changes to multi-valued index records, so the Multi-valued index will bring more performance impact than normal indexes.
- All other limitations in expression index.
