# Proposal: Table Attributes

- Author(s): [rleungx](https://github.com/rleungx)
- Tracking Issue: https://github.com/tikv/pd/issues/3839

## Background

Recently, we have encountered such a case that the scattered region of a table/partition is merged before inserting the data. We cannot prevent it from happening right now since we are not able to control the scheduling behavior at the table/partition level. However, it will sometimes lead to a serious hot spot problem. Under this circumstance, we are going to support the attributes for the table/partition so that we can handle this case. And we have designed a new mapping mechanism to establish a relationship between tables/partitions and regions which allows the user to specify the attributes to instruct the scheduling of PD. To make use of this feature to manage the table/partition level scheduling easily, we plan to introduce a new syntax in SQL.

## Proposal

### Parser

This feature needs `parser` to support the `ATTRIBUTES` keyword. And we need to introduce two new kinds of SQL syntax for both table and partition.

For the table, we can use the following SQL to add one or multiple attributes for the table `t`:

```sql
ALTER TABLE t ATTRIBUTES[=]'key=value[, key1=value1...]';
```

And reset it by using:

```sql
ALTER TABLE t ATTRIBUTES[=]DEFAULT;
```

For partition `p`, the way is almost the same as the table:

```sql
ALTER TABLE t PARTITION p ATTRIBUTES[=]'key=value[, key1=value1...]';
```

And reset it by using:

```sql
ALTER TABLE t PARTITION p ATTRIBUTES[=]DEFAULT;
```

The attributes here can be any string that doesn't contain the reserved words. 

There is no need for TiDB to know about the actual meaning of these attributes. The only thing that TiDB should do is to construct a request which contains the ID, the table/partition key range, the attributes, etc. Then synchronizing it with PD through the DDL job. All of these will be persisted in PD.



### Display Attributes

we add a new table in the `INFORMATION_SCHEMA` named `ATTRIBUTES`. If necessary, we can also support syntax like

```sql
SHOW ATTRIBUTES FOR [{DATABASE|SCHEMA} s][TABLE t][PARTITION p];
```
or 
```
SHOW CREATE TABLE t;
```

At least we should support one way to show the attributes. Here is an example:

```sql
SELECT * FROM INFORMATION_SCHEMA.ATTRIBUTES;
+-------------------+-----------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ID                | TYPE      | ATTRIBUTES           | RANGES                                                                                                                                                                                                                                   |
+-------------------+-----------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| schema/test/t1/p0 | key-range | "key=value"          | [7480000000000000ff3a5f720000000000fa, 7480000000000000ff3b5f720000000000fa]                                                                                                                                                             |
| schema/test/t1    | key-range | "key=value"          | [7480000000000000ff395f720000000000fa, 7480000000000000ff3a5f720000000000fa], [7480000000000000ff3a5f720000000000fa, 7480000000000000ff3b5f720000000000fa], [7480000000000000ff3b5f720000000000fa, 7480000000000000ff3c5f720000000000fa] |
+-------------------+-----------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

### DDL Management
Here are the concrete behaviors when doing the DDL operations with a table/partition having the attributes.

#### Drop Clause 
+ For the drop table operation, the attributes will be deleted once the GC has finished
+ For the drop schema and partition operation, the attributes will be deleted immediately

#### Truncate Clause
+ For the truncate table operation, both table range and partition range of attributes will be updated.
+ For the truncate partition operation, the partition range of attributes will be updated.

#### Recover/Flashback Clause
+ The attributes will be kept as the old values after the recover/flashback process succeed.

#### Exchange Clause
+ The attributes will be exchanged including the ID and ranges.

### Privilege Management

Privilege management is quite straightforward:

* `ALTER TABLE` statement requires `Alter` privilege
* `information_schema.attributes` only shows the attributes on the objects that visible to the current user
* `ADMIN SHOW DDL` requires `Super` privilege

### Inheritance
Consider that if we add attributes for a table, all partitions of that table will inherit the attributes by default. As for `CREATE TABLE x LIKE y`, `x` won't inherit attributes from `y`.

### Priority
We may support adding different attributes for both tables and partitions at the same time. For example, the attribute could be `merge_option=deny` on the table level but `merge_option=allow` on the partition level in the same table.
In this case, we assume that the priority of the partition is higher than the table, so the final results could be that only some partitions can be merged, but the rest cannot.

Another thing that should be noticed is that if we have already set the attributes for a partition, then setting the new attributes for the table which includes the partition will override the attributes of the partition.

### Compatibility
There is no compatibility issue.

### Implementation
In the early stage, we are going to introduce `merge_option` attribute to indicate if a table/partition can be merged. When the `merge_option` equals `deny`, PD will prevent the table or partition from being merged. Otherwise, it will be merged once it is beyond the `split-merge-interval` under the PD default settings.
