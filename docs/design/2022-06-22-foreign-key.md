# Foreign Key Design Doc

- Author(s): [crazycs520](https://github.com/crazycs520)
- Tracking Issue: https://github.com/pingcap/tidb/issues/18209

## Abstract

This proposes an implementation of supporting foreign key constraint.

## DDL Technical Design

### Table Information Changes
Table's foreign key information will be stored in `model.TableInfo`:

```go
// TableInfo provides meta data describing a DB table.
type TableInfo struct {
    ...
    ForeignKeys         []*FKInfo            `json:"fk_info"`
    // MaxFKIndexID uses to allocate foreign key ID.
    MaxForeignKeyID     int64                `json:"max_fk_id"`
    ...
}

// FKInfo provides meta data describing a foreign key constraint.
type FKInfo struct {
    ID          int64       `json:"id"`
    Name        CIStr       `json:"fk_name"`
    RefSchema   CIStr       `json:"ref_schema"`
    RefTable    CIStr       `json:"ref_table"`
    RefCols     []CIStr     `json:"ref_cols"`
    Cols        []CIStr     `json:"cols"`
    OnDelete    int         `json:"on_delete"`
    OnUpdate    int         `json:"on_update"`
    State       SchemaState `json:"state"`
    Version     int         `json:"version"`
}
```

Struct `FKInfo` uses for child table to record the referenced parent table. Struct `FKInfo` has existed for a long time, I just added some fields.
  - `Version`: uses to distinguish between old and new versions.

Why `FKInfo` record the table/schema name instead of table/schema id? Because we may don't know the table/schema id when build `FKInfo`. Here is an example:

```sql
>set @@foreign_key_checks=0;
Query OK, 0 rows affected
>create table t2 (a int key, foreign key fk(a) references t1(id));
Query OK, 0 rows affected
>create table t1 (id int key);
Query OK, 0 rows affected
>set @@foreign_key_checks=1;
Query OK, 0 rows affected
>insert into t2 values (1);
(1452, 'Cannot add or update a child row: a foreign key constraint fails (`test`.`t2`, CONSTRAINT `t2_ibfk_1` FOREIGN KEY (`a`) REFERENCES `t1` (`id`))')
```

As you can see, the table `t2` is refer to table `t1`, and when create table `t2`, the table `t1` still not be created, so we can't know the id of table `t1`.

### Create Table with Foreign Key

#### Build TableInfo

When build `TableInfo`, an index for the foreign key columns is created automatically if there is no index covering the foreign key columns. Here is an example:

```sql
mysql> create table t (id int key, a int, foreign key fk(a) references t(id));
Query OK, 0 rows affected
mysql> show create table t\G
***************************[ 1. row ]***************************
Table        | t
Create Table | CREATE TABLE `t` (
  `id` int NOT NULL,
  `a` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk` (`a`),
  CONSTRAINT `t_ibfk_1` FOREIGN KEY (`a`) REFERENCES `t` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
1 row in set
```

As you can see, the index `fk` is created automatically for the foreign key.

#### Validate

Create a table with foreign key, check the following conditions when a DDL job is built and the DDL owner received a DDL job(aka Double-Check):

- whether the user have `REFERENCES` privilege to the foreign key references table.
- Corresponding columns in the foreign key and the referenced key must have similar data types. The size and sign of fixed precision types such as INTEGER and DECIMAL must be the same. The length of string types need not be the same. For nonbinary (character) string columns, the character set and collation must be the same.
- Supports foreign key references between one column and another within a table. (A column cannot have a foreign key reference to itself.)
- Indexes on foreign keys and referenced keys are required, because the foreign key check can be fast and do not require a table scan. The size and sign of fixed precision types such as INTEGER and DECIMAL must be the same. The length of string types need not be the same. For nonbinary (character) string columns, the character set and collation must be the same.
- Prefixed indexes on foreign key columns are not supported. Consequently, BLOB and TEXT columns cannot be included in a foreign key because indexes on those columns must always include a prefix length.
- Does not currently support foreign keys for tables with user-defined partitioning. This includes both reference and child tables.
- A foreign key constraint cannot reference any virtual generated columns, but the stored generated columns are fine.

#### Handle In DDL Owner

When DDL Owner handle create table job, DDL owner need to create a new table.

At the same point in time, there may be two versions of the schema in the TiDB cluster, so we can't create new table and 
update all reference tables in one schema version, since this may break foreign key constraint, such as delete reference table
without foreign key constrain check in child table.

```sql
-- In TiDB-1 and Schema Version is 1
insert into t_has_foreign_key values (1, 1);

-- In TiDB-0 and  Schema Version is 0
delete from t_reference where id = 1; --Since doesn't know foreign key information in old version, so doesn't do foreign key constrain check.
```

So, when create a table with foreign key, we need multi-schema version change:

1. None -> Write Only: Create table with state is `write-only`.
2. Write Only -> Done: Update the created table state to `public`.

#### Maintain ReferredFKInfo

Why need to maintain `ReferredFKInfo` in reference table? When execute `UPDATE`/`DELETE` in reference table, we need the `ReferredFKInfo` of reference table to do foreign key check/cascade.

How to maintain `ReferredFKInfo` in reference table? When we create table with foreign key, we didn't add `ReferredFKInfo` into reference table, because the reference table may not have been created yet,
when `foreign_key_checks` variable value is `OFF`, the user can create child table before reference table.

We decided to maintain `ReferredFKInfo` while TiDB loading schema. At first, `infoSchema` will record all table's `ReferredFKInfo`:

```sql
// ReferredFKInfo provides the referred foreign key in the child table.
type ReferredFKInfo struct {
    Cols          []CIStr `json:"cols"`
    ChildSchema   CIStr   `json:"child_schema"`
    ChildTable    CIStr   `json:"child_table"`
    ChildFKName   CIStr   `json:"child_fk"`
}
```

```go
type infoSchema struct {
    // referredForeignKeyMap records all table's ReferredFKInfo.
    // referredSchemaAndTableName => child SchemaAndTableAndForeignKeyName => *model.ReferredFKInfo
    referredForeignKeyMap map[SchemaAndTableName]map[SchemaAndTableAndForeignKeyName]*model.ReferredFKInfo
}
```

Function `applyTableUpdate` uses `applyDropTable` to drop the old table, uses `applyCreateTable` to create the new table.

In function `applyDropTable`, we will delete the table's foreign key information from `infoSchema.referredForeignKeyMap`.

In function `applyCreateTable`, we will add the table's foreign key information into `infoSchema.referredForeignKeyMap` first, 
then get the table's `ReferredFKInfo` by schema name and table name, then store the `ReferredFKInfo` into `TableInfo.ReferredForeignKeys`.

Then `applyTableUpdate` will also need to reload the old/new table's referred table information, also uses `applyDropTable` to drop the old reference table, use `applyCreateTable` to create new reference table.

That's all.

### Alter Table Add Foreign Key

Here is an example:

```sql
create table t1 (id int key,a int, index(a));
create table t2 (id int key,a int);
alter  table t2 add foreign key fk(a) references t1(id) ON DELETE CASCADE;
```

Just like create table, we should validate first, and return error if the conditions for creating foreign keys are not met, and also need to do double-check.

When build `TableInfo`, we need to auto create an index for foreign key columns if there is no index cover foreign key columns.
And this is divides the problem into two cases:
- Case-1: No need to create index automatically, and only add foreign key constraint.
- Case-2: Need auto create index for foreign key

#### Case-1: Only add foreign key constrain

The DDL owner handle add foreign key constrain step is:

1. None -> Write Only: add foreign key constrain which state is `write-only` into table.
3. Write Only - Write Reorg: check all row in the table whether has related foreign key exists in reference table, we can use following SQL to check:
   ```sql
   select 1 from t2 where t2.a is not null and t2.a not in (select id from t1) limit 1;
   ```
   The expected result is `empty`, otherwise, an error is returned and cancel the ddl job.
4. Write Reorg -> Public: update the foreign key constrain state to `public`.

A problem is, How the DML treat the foreign key with on delete/update cascade behaviour which state is non-public?
Here is an example:

```sql
create table t1 (id int key,a int, index(a));
create table t2 (id int key,a int, index(a));
insert into t1 values (1,1);
insert into t2 values (1,1);
alter  table t2 add constraint fk_1 foreign key (a) references t1(id) ON DELETE CASCADE;
```

The schema change of foreign key `fk_1` is from `None` -> `Write-Only` -> `Write-Reorg` -> `Public`。
When the foreign key `fk_1` in `Write-Only` state, a DML request has come to be processed:

```sql
delete from t1 where id = 1;
```

Then, TiDB shouldn't do cascade delete for foreign key `fk_1` in state `Write-Only`, since the `Add Foreign Key` DDL job maybe
failed in `Write-Reorg` state and rollback the DDL job. But it is hard to rollback the cascade deleted executed before.

So, when execute DML with `non-publick` foreign key, TiDB will do foreign key constraint check instead of foreign key cascade behaviour.

#### Case-2: Auto create index for foreign key and add foreign key constrain

As TiDB support multi-schema change now, we can split this into 2 sub-ddl job.
- Add Index DDL job
- Add Foreign Key Constrain DDL job

We should do add index DDL job first, after index ddl job finish `write-reorg` and ready for public, then start to do add foreign key constrain ddl job.

### Drop Table

If `foreign_key_checks` is `ON`, then drop the table which has foreign key references will be rejected.

```sql
> drop table t1;
(3730, "Cannot drop table 't1' referenced by a foreign key constraint 't2_ibfk_1' on table 't2'.")
```

### Drop Database

If `foreign_key_checks` is `ON`, then drop the database which has foreign key references by other database will be rejected.

```sql
> drop database test;
(3730, "Cannot drop table 't1' referenced by a foreign key constraint 't2_ibfk_1' on table 't2'.")
```

### Drop Index

Drop index which used by foreign key will be rejected.

```sql
> set @@foreign_key_checks=0; -- Even disable foreign_key_checks, you still can't drop the index which used for foreign key constrain.
Query OK, 0 rows affected
> alter table t2 drop index fk;
(1553, "Cannot drop index 'fk': needed in a foreign key constraint")
```

### Rename Column

Rename column which has foreign key or references, should also need to update the related child/parent table info.

```sql
create table t1 (id int key,a int, index(a));
create table t2 (id int key,a int, foreign key fk(a) references t1(id) ON DELETE CASCADE);
rename table t1 to t11;
alter table t11 change column id id1 int;
show create table t2\G
***************************[ 1. row ]***************************
    Table        | t2
Create Table | CREATE TABLE `t2` (
                                     `id` int NOT NULL,
                                     `a` int DEFAULT NULL,
                                     PRIMARY KEY (`id`),
    KEY `fk` (`a`),
    CONSTRAINT `t2_ibfk_1` FOREIGN KEY (`a`) REFERENCES `t11` (`id1`) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
```

### Truncate Table

Truncate table which has foreign key or references, should also need to update the related child/parent table info.

### Modify Column

Modify column which used by foreign key will be rejected.

```sql
> alter table t1 change column id id1 bigint;
(3780, "Referencing column 'a' and referenced column 'id1' in foreign key constraint 't2_ibfk_1' are incompatible.")
```

MySQL modify column problem: https://www.percona.com/blog/2019/06/04/ddl-queries-foreign-key-columns-MySQL-pxc/

What if the user really need to modify column type, such as from `INT` to `BIGINT`. Maybe we can offer an variable such as `alter-foreign-keys-method=auto`,
then when user modify the column type, TiDB will auto modify the related foreign key column's type. For easy implementation and reduce risk, maybe only support modify column type which doesn't need to reorg table row data.

## DML Technical Design

### DML On Child Table

On Child Table Insert Or Update, need to Find FK column value whether exist in reference table:

1. Get reference table info by table name.
2. Get related fk index of reference table.
3. tiny optimize, check fk column value exist in reference table cache(map[string(index_key)]struct).
3. Get related row in reference.
  - Construct index key and then use snapshot `Iter` and `Seek` API to scan. If the index is unique and only contain
    foreign key columns, use snapshot `Get` API.
    - `Iter` default scan batch size is 256, need to set 2 to avoid read unnecessary data.
4. compact column value to make sure exist.
5. If relate row exist in reference table, also need to add lock in the related row.
6. put column value into reference fk column value cache.

#### Lock

Let's see an example in MySQL first:

prepare:
```sql
create table t1 (id int key,a int, b int, unique index(a, b, id));
create table t2 (id int key,a int, b int, index (a,b,id), foreign key fk(a, b) references t1(a, b));
insert into t1 values (-1, 1, 1);
```

Then, execute following SQL in 2 session:

| Session 1                        | Session 2                                   |
| -------------------------------- | ------------------------------------------- |
| Begin;                           |                                             |
| insert into t2 values (1, 1, 1); |                                             |
|                                  | delete from t1;  -- Blocked by wait lock    |
| Commit                           |                                             |
|                                  | ERROR: Cannot delete or update a parent row |

So we need to add lock in reference table when insert/update child table.

##### In Pessimistic Transaction

When TiDB add pessimistic locks, if relate row exist in reference table, also need to add lock in the related row.

##### In Optimistic Transaction

Just like `SELECT FOR UPDATE` statement, need to use `doLockKeys` to lock the related row in the reference table.

##### Issue

TiDB current only support `lock for update`(aka write-lock, such as `select for update`), doesn't support `lock for share`(aka read-lock, such as `select for share`).

So far we have to add `lock for update` in reference table when insert/update child table, then the performance will be poor. After TiDB support `lock for share`, we should use `lock for share` instead.

#### DML Load data

Load data should also do foreign key check, but report warning instead error:

```sql
create table t1 (id int key,a int, index(a));
create table t2 (id int key,a int, foreign key fk(a) references t1(id) ON DELETE CASCADE);

test> load data local infile 'data.csv' into table t2 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
Query OK, 0 rows affected
test> show warnings;
+---------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Level   | Code | Message                                                                                                                                                           |
+---------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Warning | 1452 | Cannot add or update a child row: a foreign key constraint fails (`test`.`t2`, CONSTRAINT `t2_ibfk_1` FOREIGN KEY (`a`) REFERENCES `t1` (`id`) ON DELETE CASCADE) |
| Warning | 1452 | Cannot add or update a child row: a foreign key constraint fails (`test`.`t2`, CONSTRAINT `t2_ibfk_1` FOREIGN KEY (`a`) REFERENCES `t1` (`id`) ON DELETE CASCADE) |
+---------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

`data.csv` is:
```
1,1
2,2
```

### DML On reference Table

On reference Table Delete Or Update:

1. modify related child table row by referential action:
- `CASCADE`: update/delete related child table row.
- `SET NULL`: set related child row's foreign key columns value to NULL.
- `RESTRICT`, `NO ACTION`: If related row exist in child table, reject update/delete reference table.
- `SET DEFAULT`: just like `RESTRICT`.

modify related child table row by following step:
1. get child table info by name(in reference table info).
2. get the child table fk index's column info.
3. build update executor to update child table rows.

### Issue need to be discussed

#### Affect Row

related article: https://www.percona.com/blog/hidden-cost-of-foreign-key-constraints-in-MySQL/

Here is a MySQL example:

prepare:
```sql
create table t1 (id int key,a int, index(a));
create table t2 (id int key,a int, foreign key fk(a) references t1(id) ON DELETE CASCADE);
insert into t1 values (1, 1);
insert into t2 values (1, 1);
```

Then delete on reference table:
```sql
> delete from t1 where id=1;
Query OK, 1 row affected
```

As you can see, in the query result, the affected row is 1, but actually should be 2, since the related row in t2 is also been deleted.

This is a MySQL behaviour, We can be compatible with it.

#### DML Execution plan

Here is a MySQL example:

prepare:
```sql
create table t1 (id int key,a int, index(a));
create table t2 (id int key,a int, foreign key fk(a) references t1(id) ON DELETE CASCADE);
insert into t1 values (1, 1);
insert into t2 values (1, 1);
```

```sql
> explain delete from t1 where id = 1;
+----+-------------+-------+------------+-------+---------------+---------+---------+-------+------+----------+-------------+
| id | select_type | table | partitions | type  | possible_keys | key     | key_len | ref   | rows | filtered | Extra       |
+----+-------------+-------+------------+-------+---------------+---------+---------+-------+------+----------+-------------+
| 1  | DELETE      | t1    | <null>     | range | PRIMARY       | PRIMARY | 4       | const | 1    | 100.0    | Using where |
+----+-------------+-------+------------+-------+---------------+---------+---------+-------+------+----------+-------------+
```

From the plan, you can't see any information about the foreign key constrain which need to delete the related row in child table `t2`.

I think this is a MySQL issue, should we make TiDB plan better, at least when we meet some slow query, we can know maybe it is caused by modify related row in child table. 

Here is an example plan with foreign key:

```sql
> explain delete from t1 where id = 1;
+--------------------------+---------+------+---------------+---------------+
| id                       | estRows | task | access object | operator info |
+--------------------------+---------+------+---------------+---------------+
| Delete_4                 | N/A     | root |               | N/A           |
| └─Point_Get_6            | 1.00    | root | table:t1      | handle:1      |
| └─Foreign_Key_Cascade    | N/A     | root | table:t2      | fk: fk_id     |
+--------------------------+---------+------+---------------+---------------+
```

##### CockroachDB DML Execution Plan

```sql
CREATE TABLE customers_2 (
    id INT PRIMARY KEY
  );

CREATE TABLE orders_2 (
                          id INT PRIMARY KEY,
                          customer_id INT REFERENCES customers_2(id) ON UPDATE CASCADE ON DELETE CASCADE
);

INSERT INTO customers_2 VALUES (1), (2), (3);
INSERT INTO orders_2 VALUES (100,1), (101,2), (102,3), (103,1);
```

```sql
> explain analyze UPDATE customers_2 SET id = 23 WHERE id = 1;
                       info
--------------------------------------------------
  planning time: 494µs
  execution time: 5ms
  distribution: local
  vectorized: true
  rows read from KV: 6 (170 B)
  cumulative time spent in KV: 978µs
  maximum memory usage: 100 KiB
  network usage: 0 B (0 messages)
  regions: us-east1

  • root
  │
  ├── • update
  │   │ nodes: n1
  │   │ regions: us-east1
  │   │ actual row count: 1
  │   │ table: customers_2
  │   │ set: id
  │   │
  │   └── • buffer
  │       │ label: buffer 1
  │       │
  │       └── • render
  │           │ nodes: n1
  │           │ regions: us-east1
  │           │ actual row count: 1
  │           │ KV rows read: 1
  │           │ KV bytes read: 27 B
  │           │
  │           └── • scan
  │                 nodes: n1
  │                 regions: us-east1
  │                 actual row count: 1
  │                 KV rows read: 1
  │                 KV bytes read: 27 B
  │                 missing stats
  │                 table: customers_2@primary
  │                 spans: [/1 - /1]
  │                 locking strength: for update
  │
  └── • fk-cascade
        fk: fk_customer_id_ref_customers_2
        input: buffer 1
```

##### PostgreSQL DML Execution Plan

```sql
postgres=# explain analyze UPDATE customers_2 SET id = 20 WHERE id = 23;
                                                             QUERY PLAN
-------------------------------------------------------------------------------------------------------------------------------------
 Update on customers_2  (cost=0.15..8.17 rows=1 width=10) (actual time=0.039..0.039 rows=0 loops=1)
   ->  Index Scan using customers_2_pkey on customers_2  (cost=0.15..8.17 rows=1 width=10) (actual time=0.016..0.016 rows=1 loops=1)
         Index Cond: (id = 23)
 Planning Time: 0.057 ms
 Trigger for constraint orders_2_customer_id_fkey on customers_2: time=0.045 calls=1
 Trigger for constraint orders_2_customer_id_fkey on orders_2: time=0.023 calls=2
 Execution Time: 0.129 ms
```

## Other Technical Design

### How to check foreign key integrity?

How MySQL to do this? Look like MySQL doesn't provide any method, but the user can use stored procedure to do this, see: https://stackoverflow.com/questions/2250775/force-innodb-to-recheck-foreign-keys-on-a-table-tables

Maybe We can use following syntax to check foreign key integrity:

```sql
ADMIN CHECK FOREIGN KEY [table_name] [foreign_key_name]
```

which implemention is use following SQL to check:

```sql
select count(*) from t where t.a not in (select id from t_refer);
```

Also check the foreign key is valid:
- the foreign key state should be public.
- the reference whether exist.
- whether has index for the foreign key to use.

## Impact

### Impact of data replication

Test sync table with foreign key:
- TiCDC
- DM
- BR

## Test Case

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

following is a MySQL test case about `SET DEFAULT`:

```sql
MySQL>create table t1 (a int,b int, index(a,b)) ;
Query OK, 0 rows affected
Time: 0.022s
MySQL>create table t (a int, b int, foreign key fk_a(a) references test.t1(a) ON DELETE SET DEFAULT);
Query OK, 0 rows affected
Time: 0.019s
MySQL>insert into t1 values (1,1);
Query OK, 1 row affected
Time: 0.003s
MySQL>insert into t values (1,1);
Query OK, 1 row affected
Time: 0.006s
MySQL>delete from t1 where a=1;
(1451, 'Cannot delete or update a parent row: a foreign key constraint fails (`test`.`t`, CONSTRAINT `t_ibfk_1` FOREIGN KEY (`a`) REFERENCES `t1` (`a`))')
MySQL>select version();
+-----------+
| version() |
+-----------+
| 8.0.29    |
+-----------+
```

### Self-Referencing Tables

For example, table `employee` has a column `manager_id` references to `employee.id`.

```sql
create table employee (id int key,manager_id int, foreign key fk(manager_id) references employee(id) ON DELETE CASCADE);
insert into employee values (1,1);
insert into employee values (2,1);

test> delete from employee where id=1;
Query OK, 1 row affected
test> select * from employee;
+----+---+
| id | a |
+----+---+
0 rows in set
```

A case of self-reference and cyclical dependencies:

```sql
test> create table t (id int key,a int, foreign key fk_a(a) references t(id) ON DELETE CASCADE, foreign key fk_id(id) references t(a) ON DELETE CASCADE);
Query OK, 0 rows affected
Time: 0.045s
test> insert into t values (1,1);
(1452, 'Cannot add or update a child row: a foreign key constraint fails (`test`.`t`, CONSTRAINT `t_ibfk_2` FOREIGN KEY (`id`) REFERENCES `t` (`a`) ON DELETE CASCADE)')
```

### Cyclical Dependencies

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

### Check order

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

### MATCH FULL or MATCH SIMPLE

This definition is from [CRDB](https://www.cockroachlabs.com/docs/v22.1/foreign-key.html#match-composite-foreign-keys-with-match-simple-and-match-full). MySQL doesn't mention it, here is a MySQL test case:

Here is an MySQL example:

```sql
create table t1 (i int, a int,b int, index(a,b)) ;
create table t (a int, b int, foreign key fk_a(a,b) references test.t1(a,b));

test> insert into t values (null,1);
Query OK, 1 row affected
test> insert into t values (null,null);
Query OK, 1 row affected
test> insert into t values (1,null);
Query OK, 1 row affected
```

### reference

- [MySQL FOREIGN KEY Constraints Document](https://dev.mysql.com/doc/refman/8.0/en/create-table-foreign-keys.html#foreign-key-adding)
- [3 Common Foreign Key Mistakes (And How to Avoid Them)](https://www.cockroachlabs.com/blog/common-foreign-key-mistakes/)
- [Hidden Cost of Foreign Key Constraints in MySQL](https://www.percona.com/blog/hidden-cost-of-foreign-key-constraints-in-MySQL/)
- [DDL Queries on Foreign Key Columns in MySQL/PXC](https://www.percona.com/blog/2019/06/04/ddl-queries-foreign-key-columns-mysql-pxc/)
