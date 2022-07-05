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

设置这个变量 global 作用域需要检查 `SUPER` or `SYSTEM_VARIABLES_ADMIN ` 权限. 设置 session 作用域则不需要。

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

mysql test case
```sql
test> create table t1 (i int, a int,b int, index(a,b)) ;
Query OK, 0 rows affected
Time: 0.044s
test> create table t (a int, b int, foreign key fk_a(a,b) references test.t1(a,b));
Query OK, 0 rows affected
Time: 0.045s
test> insert into t values (null,1);
Query OK, 1 row affected
Time: 0.041s
test> insert into t values (null,null);
Query OK, 1 row affected
Time: 0.040s
test> insert into t values (1,null);
Query OK, 1 row affected
Time: 0.041s
```

## DDL Technical Design

### Table Information Changes
Table's foreign key information will be stored in `model.TableInfo`:

```go
// TableInfo provides meta data describing a DB table.
type TableInfo struct {
	...
	ForeignKeys      []*FKInfo         `json:"fk_info"`
	CitedForeignKeys []*CitedFKInfo    `json:"cited_fk_info"`
	...
}

// FKInfo provides meta data describing a foreign key constraint.
type FKInfo struct {
    ID        int64       `json:"id"`
    Name      CIStr       `json:"fk_name"`
    RefSchema CIStr       `json:"ref_schema"`
    RefTable  CIStr       `json:"ref_table"`
    RefCols   []CIStr     `json:"ref_cols"`
    Cols      []CIStr     `json:"cols"`
    OnDelete  int         `json:"on_delete"`
    OnUpdate  int         `json:"on_update"`
    State     SchemaState `json:"state"`
}

// CitedFKInfo provides the cited foreign key in the child table.
type CitedFKInfo struct {
    Cols         []CIStr `json:"cols"`
    ChildSchema  CIStr   `json:"child_schema"`
    ChildTable   CIStr   `json:"child_table"`
    ChildFKIndex CIStr   `json:"child_fk_index"`
}
```

Struct `FKInfo` uses for child table to record the referenced parent table.
Struct `CitedFKInfo` uses for parent table to record the child table which referenced me.

### Create Table with Foreign Key

Create a table with foreign key, do following check when build DDL job and DDL owner received DDL job(aka Double-Check):
- whether the user have `REFERENCES` privilege to the foreign key references table.
- 


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

#### DDL on parent table

rename table/column should check and update related child table.

modify column problem: https://www.percona.com/blog/2019/06/04/ddl-queries-foreign-key-columns-mysql-pxc/

drop parent table should return error.

```sql
test> drop table if exists t1;
(3730, "Cannot drop table 't1' referenced by a foreign key constraint 't2_ibfk_1' on table 't2'.")
```

drop index which used by foreign key should be rejected.

## DML Technical Design

#### On Child Table Insert Or Update (Or Load data?), need to Find FK column value whether exist in Parent table:

1. Get parent table info by table name.
2. Get related fk index of parent table.
3. tiny optimize, check fk column value exist in parent table cache(map[string(index_key)]struct).
3. get related row in parent, maybe option-c is better.
  - option-a. use SQL string and use `ExecRestrictedSQL` API to check row exist.
    - drawback: 
      - Need convert `Datum` to string when construct query SQL string. is there any risk？
      - In bulk-insert/update situation, the construct SQL string maybe very huge, may have some risk.
      - performance is bad.
  - option-b. manual construct a index reader to check.
    - drawback:
      - there is some complexity, but acceptable?
  - option-c. Construct index key and then use snapshot `Iter` and `Seek` API to scan.
    - `Iter` default scan batch size is 256, need to set 1.
    - Need manual decode index key by index schema.
4. compact column value to make sure exist.
5. put column value into parent fk column value cache.

check order should check unique/primary key constrain first:

```sql
test> create table t1 (id int key,a int, index(a));
test> create table t2 (id int key,a int, foreign key fk(a) references t1(id) ON DELETE CASCADE);
test> insert into t1 values (1, 1);
test> insert into t2 values (1, 1);
test> insert into t2 values (1, 2);
(1062, "Duplicate entry '1' for key 't2.PRIMARY'")
test> insert ignore into t2 values (1, 2);
Query OK, 0 rows affected
```

#### On Parent Table update/delete

Problems:
- How the execution plan look like? MySQL 的 plan 无法看出有 foreign key 的修改，返回的 affect row 也不包括看到 child table 的影响行数。 https://www.percona.com/blog/hidden-cost-of-foreign-key-constraints-in-mysql/

1. check related child table row exist.
2. modify related child table row by referential action:
  - `CASCADE`: update/delete related child table row.
  - `SET NULL`: set related child row's foreign key columns value to NULL.
  - `RESTRICT`, `NO ACTION`: If related row doesn't exit in child table, reject update/delete parent table.
  - `SET DEFAULT`: just like `RESTRICT`.
    ```sql
    mysql>create table t1 (a int,b int, index(a,b)) ;
    Query OK, 0 rows affected
    Time: 0.022s
    mysql>create table t (a int, b int, foreign key fk_a(a) references test.t1(a) ON DELETE SET DEFAULT);
    Query OK, 0 rows affected
    Time: 0.019s
    mysql>insert into t1 values (1,1);
    Query OK, 1 row affected
    Time: 0.003s
    mysql>insert into t values (1,1);
    Query OK, 1 row affected
    Time: 0.006s
    mysql>delete from t1 where a=1;
    (1451, 'Cannot delete or update a parent row: a foreign key constraint fails (`test`.`t`, CONSTRAINT `t_ibfk_1` FOREIGN KEY (`a`) REFERENCES `t1` (`a`))')
    mysql>select version();
    +-----------+
    | version() |
    +-----------+
    | 8.0.29    |
    +-----------+
    ```

modify related child table row by following step:
1. get child table info by name(in parent table info).
2. get the child table fk index's column info.
3. build update executor to update child table rows.

cascade modification test case:

```sql
drop table if exists t3,t2,t1;
create table t1 (id int key,a int, index(a));
create table t2 (id int key,a int, foreign key fk(a) references t1(id) ON DELETE CASCADE);
create table t3 (id int key,a int, foreign key fk(a) references t2(id) ON DELETE CASCADE);
insert into t1 values (1,1);
insert into t2 values (2,1);
insert into t3 values (3,2);
delete from t1 where id = 1;  -- both t1, t2, t3 rows are deleted.
```

### Some special case

#### Self-Referencing Tables

```sql
test> create table t (id int key,a int, foreign key fk(a) references t(id) ON DELETE CASCADE);
test> insert into t values (1,1);
test> insert into t values (2,1);
test> delete from t where id=1;
Query OK, 1 row affected
test> select * from t;
+----+---+
| id | a |
+----+---+
0 rows in set
```

```sql
test> create table t (id int key,a int, foreign key fk_a(a) references t(id) ON DELETE CASCADE, foreign key fk_id(id) references t(a) ON DELETE CASCADE);
Query OK, 0 rows affected
Time: 0.045s
test> insert into t values (1,1);
(1452, 'Cannot add or update a child row: a foreign key constraint fails (`test`.`t`, CONSTRAINT `t_ibfk_2` FOREIGN KEY (`id`) REFERENCES `t` (`a`) ON DELETE CASCADE)')
```

#### Cyclical Dependencies

```sql
create table t1 (id int key,a int, index(a));
create table t2 (id int key,a int, foreign key fk(a) references t1(id) ON DELETE CASCADE);
insert into t1 values (1,1);
ALTER TABLE t1 ADD foreign key fk(a) references t2(id) ON DELETE CASCADE;
(1452, 'Cannot add or update a child row: a foreign key constraint fails (`test`.`#sql-298_8`, CONSTRAINT `t1_ibfk_1` FOREIGN KEY (`a`) REFERENCES `t2` (`id`) ON DELETE CASCADE)')
```

```sql
set @@foreign_key_checks=0;
create table t1 (id int key,a int, foreign key fk(a) references t2(id) ON DELETE CASCADE);
create table t2 (id int key,a int, foreign key fk(a) references t1(id) ON DELETE CASCADE);
insert into t1 values (1, 2);
insert into t2 values (2, 1);
set @@foreign_key_checks=1;   -- add test case without this.
delete from t1 where id=1;
test> select * from t2;
+----+---+
| id | a |
+----+---+
0 rows in set
Time: 0.004s
test> select * from t1;
+----+---+
| id | a |
+----+---+
0 rows in set
```

## Impact

### Impact of data replication

### reference

- [3 Common Foreign Key Mistakes (And How to Avoid Them)](https://www.cockroachlabs.com/blog/common-foreign-key-mistakes/)



##### 代码碎片

```go
buildPhysicalIndexLookUpReader

```
