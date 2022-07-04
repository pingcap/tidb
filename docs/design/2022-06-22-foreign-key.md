# 外键设计文档

语法如下：

```sql
[CONSTRAINT [symbol]] FOREIGN KEY
    [index_name] (col_name, ...)
    REFERENCES tbl_name (col_name,...)
    [ON DELETE reference_option]
    [ON UPDATE reference_option]

reference_option:
    RESTRICT | CASCADE | SET NULL | NO ACTION | SET DEFAULT
```


## Conditions and Restrictions

- 创建 Foreign Key 时，对引用的父表需要有 `REFERENCES` 权限。
- Foreign Key 中的 columns 和引用父表中的 columns 的类型必须相同，对于 string 类型的列，其 character 和 collation 也必须相同。
- Foreign Key 中的 columns 可以引用自己表中的其他 columns, 但不能引用 column 自己。
- Foreign Key 中的 columns 需要有对应的索引，对于引用的父表中的 columns，也需要有对应的索引，否则会创建失败。
- Foreign Key 中的 columns 不能是 `BLOB` 和 `TEXT` 类型。
- 在分区表上不支持使用 Foreign Key。
- Foreign Key 中的 columns 不能是 virtual generated column，但可以是 stored generated column。
- 单个 column 上可以有多个 Foreign Key 约束。（这是 crdb 的，MySQL 好像也支持）

## Referential Actions

当 `UPDATE` 或者 `DELETE` 操作父表中的被引用的外键 columns 时，会影响子表中外键匹配的行数据。具体影响的行为由 `ON UPDATE` 和 `ON DELETE` 子句中指定的 `reference_option` 决定。一共有以下 `reference_option`：

- `CASCADE`: `UPDATE` 或者 `DELETE` 操作父表时，自动 `UPDATE` 或 `DELETE` 子表中相匹配行的数据。

  If a FOREIGN KEY clause is defined on both tables in a foreign key relationship, making both tables a parent and child, an ON UPDATE CASCADE or ON DELETE CASCADE subclause defined for one FOREIGN KEY clause must be defined for the other in order for cascading operations to succeed. If an ON UPDATE CASCADE or ON DELETE CASCADE subclause is only defined for one FOREIGN KEY clause, cascading operations fail with an error.

- `SET NULL`：`UPDATE` 或者 `DELETE` 操作父表时，自动将子表中相匹配的行中的 foreign key columns 设置为 NULL。
- `RESTRICT`：`UPDATE` 或者 `DELETE` 操作父表时，如果子表中存在相匹配的行，就拒绝执行 `UPDATE` 和 `DELETE` 操作。
- `NO ACTION`：行为和 `RESTRICT` 一样。
- `SET DEFAULT`：待确认，MySQL 官网说不支持，但实际上能建表成功。(CRDB 的行为是在 `UPDATE` 或者 `DELETE` 时，将子表中的 foreign key column 的值设置为默认值，如果没有默认值，或者这个 column 没有 not null 属性，这个 column 的值会被设置成 NULL)

在子表中进行 `INSERT` 或者 `UPDATE` 操作时，如果外键值在对应的父表中不存在，则拒绝执行。

`ON DELETE` 或者 `ON UPDATE` 字句没有定义时，默认行为是 `NO ACTION`。如果行为是 `NO ACTION`，则不会出现在 `SHOW CREATE TABLE` 的输出中。

> WARNING:
> For NDB tables, ON UPDATE CASCADE is not supported where the reference is to the parent table's primary key.
> 需要确认下这里是否有坑。

InnoDB performs cascading operations using a depth-first search algorithm on the records of the index that corresponds to the foreign key constraint.

stored generated column 上的外键约束不能使用 CASCADE、SET NULL 或 SET DEFAULT 作为 ON UPDATE 引用操作，也不能使用 SET NULL 或 SET DEFAULT 作为 ON DELETE 操作。

## Foreign Key Checks

Foreign Key 的约束检测由 [foreign_key_checks](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_foreign_key_checks) 控制，其默认值是 `ON`，有 `GLOBAL` 和 `SESSION` 两种作用域。

以下场景可能会用到 `foreign_key_checks`：
- Drop 被外键引用的父表失败时，关闭 `foreign_key_checks` 后就能成功删除了，以及其子表中的外键定义也会被删除。
- 用 mysqldump 导入数据时，如果启用外键约束检测，则必须按顺序先导入父表，再导入子表。关闭 `foreign_key_checks` 后就能按任意顺序导入了。
- 执行 `LOAD DATA` 操作时，关闭 `foreign_key_checks`用来加速。
- 执行 `ALTER TABLE` 操作时，关闭 `foreign_key_checks`。

当关闭 `foreign_key_checks` 时，外键约束检查将被忽略，以下情况除外：
- 如果建表时，表定义不符合引用该表的外键约束，则建表会返回错误。该表必须具有正确的列名和类型。它还必须在引用的键上有索引。如果不满足这些要求，则会返回 Error。
- `ALTER TABLE` 修改外键定义时，如果不符合外键定义约束，则返回报错。
- 删除外键约束所需的索引，在删除索引之前，必须删除外键约束。
- 创建外键约束时，列的类型和引用列的类型不匹配时，返回报错。

关闭 `foreign_key_checks` 有下列附加影响：
- 允许 `DROP DATABASE` 操作，即便是库中有被其他数据库引用的外键父表。
- 允许 `DROP TABLE` 操作，即便这个表是被外键引用的父表。
- 开启 `foreign_key_checks` 不会触发对表数据的扫描，这意味着当重新启用 `foreign_key_checks` 时，不会检查在关闭 foreign_key_checks 时添加到表的行的一致性。

## Locking

...

## Foreign Key Definitions and Metadata

`SHOW CREATE TABLE` 语句可以查看外键的定义：

```sql
mysql> SHOW CREATE TABLE child\G
*************************** 1. row ***************************
       Table: child
Create Table: CREATE TABLE `child` (
  `id` int DEFAULT NULL,
  `parent_id` int DEFAULT NULL,
  KEY `par_ind` (`parent_id`),
  CONSTRAINT `child_ibfk_1` FOREIGN KEY (`parent_id`)
  REFERENCES `parent` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
```

可以通过 `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` 表查看外键信息：

```sql
mysql> SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME
       FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
       WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL;
+--------------+------------+-------------+-----------------+
| TABLE_SCHEMA | TABLE_NAME | COLUMN_NAME | CONSTRAINT_NAME |
+--------------+------------+-------------+-----------------+
| test         | child      | parent_id   | child_ibfk_1    |
+--------------+------------+-------------+-----------------+
```

## MATCH FULL or MATCH SIMPLE

mysql 没有这个的说明，是 CRDB 文档中提到的：https://www.cockroachlabs.com/docs/v22.1/foreign-key.html#match-composite-foreign-keys-with-match-simple-and-match-full


## Technical Design

```go
// in table info

// FKChildInfo provides refered child table info.
type FKChildTableInfo struct {
       TableName   CIStr
       FKIndexName CIStr
       Cols        []CIStr
}


```

### DDL

#### Create Table with Foreign Key

建表时，在生成 DDL job 前和在 DDL owner 收到 DDL job 后，执行以下检查：
- 创建 Foreign Key 时，对引用的父表需要有 REFERENCES 权限。
- Foreign Key 中的 columns 和引用父表中的 columns 的类型必须相同，对于 string 类型的列，其 character 和 collation 也必须相同。
- Foreign Key 中的 columns 可以引用自己表中的其他 columns, 但不能引用 column 自己。
- Foreign Key 中的 columns 需要有对应的索引，对于引用的父表中的 columns，也需要有对应的索引，否则会创建失败。
- Foreign Key 中的 columns 不能是 BLOB 和 TEXT 类型。
- 在分区表上不支持使用 Foreign Key。
- Foreign Key 中的 columns 不能是 virtual generated column，但可以是 stored generated column。

DDL Owner 在创建带有外键的表时，Schema change 的变更如下：
- Option-1：
```
1. None -> Public.
- create a new table.
- update parent table info.
- **notify multi-table's schema change in 1 schema version**.

This will also need to to related change when TiDB to do `loadInfoSchema`.
```
- Option-2:
```
1. None -> Write Only(whatever): Update Parent Table info
2. Write Only -> Done: Create Table
```

#### Alter Table Add Foreign Key.

ing~

### DML

#### On Parent Table update/delete

- check related child table row exist.

1. get child table info by name(in parent table info).
2. get the child table fk index info.
3. build index reader and limit 1 to check child row exist.

- update related child table row.

1. get child table info by name(in parent table info).
2. get the child table fk index's column info.
3. build update executor to update child table rows.

#### On Child Table Insert Or Update (Or Load data?), need to Find FK column value whether exist in Parent table:

1. Get parent table info by table name.
2. Get related fk index of parent table.
3. build index reader and limit 1 to check parent row exist.
