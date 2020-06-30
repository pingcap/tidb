# Defining placement rules in SQL

- Author(s):     [djshow832](https://github.com/djshow832) (Ming Zhang)
- Last updated:  2020-06-24
- Discussion at: https://docs.google.com/document/d/18Kdhi90dv33muF9k_VAIccNLeGf-DdQyUc8JlWF9Gok

## Movitation

TiDB supports placement rules, which can define the placement of data in a more flexible and more granular way. But it only provides configuration files to define them, and it’s complicated.

This article proposes an approach to configure placement rules through statements. TiDB server parses the statements and notify PD to perform the change. In this way, usability can be improved.

## Define placement rules

There are 2 kinds of operations on the placement:

* ADD: Add new `count` replicas to the table according to `label` and `role` configuration.
* ALTER: Override the current replica configuration. Old configuration is discarded.

They’re both achieved by executing ALTER statements.

### Adding replicas

Adding new replicas can be done by one or more `ADD PLACEMENT` subclauses:

```sql
ALTER TABLE table_name
	ADD PLACEMENT LABEL=label ROLE=role COUNT=count,
	...
```

This statement indicates TiDB to add replicas for all data, including indexes, of table `table_name`.

This kind of operation can be used to separate data on different data centers, which may help to reduce multi-region latency.

Adding placement can be done through adding a placement rule in PD. The statement must wait until the PD returns a message.

### Altering placement

Altering current placement rules can be done by one or more `ALTER PLACEMENT` subclauses:

```sql
ALTER TABLE table_name
	ALTER PLACEMENT LABEL=label ROLE=role COUNT=count,
	...
```

This statement indicates TiDB to overwrite the current placement rule with the same `role`. It affects all data, including indexes, of table `table_name`.

This kind of operation can be used in adding more replicas for important data, or adding a TiFlash replica.

Altering placement can be done through override the current placement rule by a new one.

Note that when the rules in the statement conflict, an error should occur. For example, an error should be reported when executing this statement:

```sql
ALTER TABLE test
	ALTER PLACEMENT LABEL="+zone=sh" ROLE=voter COUNT=3,
	ALTER PLACEMENT LABEL="+zone=bj" ROLE=voter COUNT=3;
```

`+zone=sh` conflicts with `+zone=bj`, because they’re defining the same role `voter`. These two constraints can not be satisfied both.

The statement must wait until the PD returns a message.

### Label configuration

`LABEL` in the statement indicates the label constraints. Data is placed on the TiKV nodes whose labels conform to `LABEL` constraint.

Parameter `label` should be a string and in such a format: `{+|-}key=value,...`. Prefix `+` indicates that data can only be placed on those TiKV nodes whose labels contain such labels, and `-` indicates that data can’t be placed on those TiKV nodes whose labels contain such labels.

For example, `+zone=sh,+zone=bj` indicates to place data only in `sh` and `bj` zone.

Label constraints can be implemented by defining `label_constraints` field in placement rules. `+` and `-` correspond to property `op`. Specifically, `+` is equivalent to `In` and `-` is equivalent to `NotIn`.

For example, `+zone=sh,+zone=bj` is equivalent to:

```
"label_constraints": [
	{"key": "zone", "op": "in", "values": ["sh", “bj”]}
]
```

### Role configuration

`ROLE` in the statement defines the role of replicas. Newly added replicas are in such roles.

`ROLE` is equivalent to property `role` in placement rules. it can be one of such values: leader, follower, learner, voter.

`ROLE` must be specified in the statement.

### Count configuration

`COUNT` in the statement defines replicas count in current rule, it’s equivalent to property `count` in placement rules.

`COUNT` must be specified in the statement.

### Key range configuration

Affected key range must also be defined in placement rules. Now that `table_name` is specified in the ALTER statement, key range can be inferred.

Typically, key format is in such a format: `t_{table_id}_r_{pk_value}`, where `pk_value` may be `_tidb_rowid` in some cases. `table_id` can be inferred from `table_name`, thus key range can be `t_{table_id}_` to `t_{table_id+1}_`.

### Database placement

Defining placement of databases simplifies the procedures when there are very many tables. For example, in a multi-tenant scenarios, each user may have a single database. The dataset in one database is relatively small, and it’s rare to query toward multiple databases. In this case, a whole database can be placed in a single region to reduce multi-region latency.

Placement of databases can be also defined through ALTER statements:

```sql
ALTER {DATABASE | SCHEMA} schema_name
	{ADD | ALTER} PLACEMENT LABEL=label ROLE=role COUNT=count,
	...
```

This statement indicates TiDB to add new replicas or alter placement for one database, including all tables in it.

Since key range is not successive in one database, each table in the database should correspond to a placement rule, so there may be many placement rules.

Creating or dropping a table should also affect the placement rules.

### Partition placement

Defining placement of partitions is useful for Geo-Partitioning. In the cases where data is very relevant to zones, Geo-Partitioning can be applied to reduce multi-region latency.

In Geo-Partitioning, the table must be splitted into partitions, and each partition is placed in specific zones. There are 2 kinds of partition placement:

* Place all replicas of one partition on one zone
* Place only leaders of one partition on one zone

It’s up to users to choose the right solution.

Placement of partitions can be also defined through ALTER statements:

```sql
ALTER TABLE table_name ALTER PARTITION partition_name
	{ADD | ALTER} PLACEMENT LABEL=label ROLE=role COUNT=count,
	...
```

This statement indicates TiDB to add new replicas or alter placement for one partition, including its indexes.

`LABEL`, `ROLE` and `COUNT` configurations are the same as above.

Key format of the partitioned table is in such a format: `t_{partition_id}_r_{pk_value}`. As `partition_id` is part of the prefix of the keys, all data in one partition forms only one key range.

`partition_id` can be inferred from `partition_name`, so the key range can be `t_{partition_id}_` to `t_{partition_id+1}_`.

Placement rules can also be defined on a partitioned table. Because there’s more than one key range for the table, multiple rules should be generated and sent to PD. When placement rules are defined both on the table and its partitions, the rule priorities described later should be applied.

### Unpartitioned index placement

Defining placement of indexes is more complicated, because indexes can be unpartitioned or partitioned. Each case should be considered separately.

The index here can be primary index or secondary index. When `_tidb_rowid` is used for the clustered index instead of the primary key, the primary index is actually an unclustered index. In this case, an index placement statement is applied.

Expression indexes and invisible indexes are also supported, as the key format is the same as normal.

Defining placement of an unpartitioned index:

```sql
ALTER TABLE table_name ALTER INDEX index_name
	{ADD | ALTER} PLACEMENT LABEL=label ROLE=role COUNT=count,
	...
```

This key format of unpartitioned index is as `t_{table_id}_i_{index_id}_r_{pk_value}`. The key range can be inferred by `table_id` and `index_id`.

### Partitioned index placement

Defining placement of the index in one specific partition:

```sql
ALTER TABLE table_name ALTER PARTITION partition_name ALTER INDEX index_name
	{ADD | ALTER} PLACEMENT LABEL=label ROLE=role COUNT=count,
...
```

The key format of partitioned index is as `t_{partition_id}_i_{index_id}_r_{pk_value}`. The key range can be inferred by `partition_id` and `index_id`.

When an index is partitioned, defining placement of the whole index at once is not supported. It will involve multiple key ranges, and the scenario of its application is rare.

For example, `t` is a partitioned table and index `idx` is the index on `t`. It’s not supported to do this:

```sql
ALTER TABLE `t` ALTER INDEX `i` ADD PLACEMENT ...
```

To alter the placement rule of `idx`, a partition must be specified in the statement.

Currently, global secondary index on partitioned tables is not supported, so it can be ignored for now.

### Sequence placement

Sequence is typically used to allocate ID in INSERT statements, so the placement of sequences affects the latency of INSERT statements.

However, sequence is typically used with cache enabled, which means very few requests are sent to sequence. So defining placement of sequences is not supported for now.

## Drop placement rules

Dropping a placement rule can be achieved by a DROP subclause:

```sql
ALTER TABLE table_name
	DROP PLACEMENT
```

Placement rules of indexes and partitions can also be dropped in a similar grammar. The statement must wait until PD returns a message.

## Rule priorities

When several rules are defined for one row, the most granular is chosen for this row. More specifically, the rule priority is: Index > Partition > Table > Database > Default.

For example:

1. At the beginning, all rows are placed based on the default placement rules.
2. When a placement rule is added on table `t`, all data on `t` is placed based on the rule of `t`.
3. When a placement rule is added on partition `p0` of `t`, all data on `p0` is placed based on the rule of `p0`, but other partitions stay still.
4. When the placement rule on `p0` is removed, data on `p0` is placed based on the rule of `t`, just like other partitions.

Rules priorities should be checked once a placement is added, altered, or dropped.

Rule priorities can be implemented by properties `index` and `override`. `override` is alway enabled, and `index` stands for the priority. Specifically, `index` values can be like this:

* `index` of default placement rules is 0
* `index` of database placement rules is 1
* `index` of table placement rules is 2
* `index` of partition placement rules is 3
* `index` of index placement rules is 4

In such a way, the most granular rule always works.

When multiple placement rules are defined on the same granular object, the new one always overwrites the old one. To achieve this, property `group_id` in placement rules can be used to identify the database object.

For example, `group_id` can be in such a format:

* `group_id` of a database is the database id
* `group_id` of a table is the table id
* `group_id` of a partition is the partition id
* `group_id` of an unpartitioned index is the concatenation of table id and index id, e.g. `100-1`
* `group_id` of a partitioned index is the concatenation of partition id and index id

## DDL management

DDL on database objects can also affect placement rules.

### DDL on tables

Once a table is created, it follows the default placement rule.

Once a table is dropped, the placement rules on it cannot be dropped immediately, because the table can be recovered by `FLASHBACK` or `RECOVER` statement before GC collects the data. Related placement rules should be kept temporarily and will be removed after GC lifetime.

When it’s time to remove all related placement rules, not only those rules defined on the table should be removed, but also rules defined on its partitions and indexes.

Once the table is truncated, the placement rules on the table should be updated. Since table id is updated after truncating, key range is also changed.

Since the table can be recovered later by `FLASHBACK` statement, a snapshot of the original placement rules must be saved temporarily. After recovering, the table name is changed, but the table id is the original one, so the snapshot of the original placement rules can be recovered directly.

DDL on partitions and indexes will be discussed below, and other DDL on tables won’t affect placement rules:

* Altering columns
* Renaming tables
* Change charset and collation

### DDL on partitions

TiDB supports adding and dropping partitions.

Once a partition is added, its placement rule is empty and the partition follows the rule of the table it belongs to.

Once a partition is dropped, it can’t be recovered anymore, so its placement rules can be removed immediately.

### DDL on indexes

Once a new index is created on an unpartitioned table, the index should follow the rule of the table it belongs to.

Once a new index is created on a table with partitions, each part of the index should follow the rule of the partition it belongs to.

Once an index is dropped, it can’t be recovered anymore, so its placement rules can be removed immediately.

Altering primary index is the same with altering secondary indexes. Because if a primary index can be created or dropped, it must be an unclustered index.

Other DDL on indexes won’t affect placement rules:

* Renaming index
* Change the visibility of index

### Show DDL jobs

As mentioned earlier, all statements related to placement rules must wait until PD returns. If the execution is interrupted, it should retry until it succeeds or is cancelled, just like other DDL jobs.

PD schedules regions asynchronously after it returns the message. It’s not possible to check the progress of the scheduling for now.

Ongoing and finished statements related to placement rules can also be queried through `ADMIN SHOW DDL`, `ADMIN SHOW DDL JOBS`, or other similar statements.

## View rules

All placement rules should be queried through statements.

### System table

A new system table `information_schema.placement_rules` can be added to view all placement rules. The table contains such columns:

* rule_id
* database_id and database_name
* table_id and table_name
* partition_id and partition_name
* index_id and index_name
* label
* role
* count

The system table is a virtual table, which doesn’t store data. When querying the table, TiDB queries PD and integrates the result in a table format. That also means the metadata is stored on PD instead of TiKV.

Advantages of building system table include:

* It’s easy for users to filter and aggregate
* There’s no need to support new grammar, and it’s easier to implement

### Show placement

But there’re a problem here. The system table only contains stored placement rules, and users cannot query the effective rule of one object from it.

For example, table `t` has two partitions `p0` and `p1`, and a placement rule is added on `t`. If the user wants to query the working rule of `p0`, they will find no placement rule is defined for `p0` through the system table. Based on the rule priorities described before, they must query the placement rule on `t`. This procedure may be annoying.

To simplify this procedure, a SHOW statement should be provided to query the effective rule for one specified object.

The statement is in such a format:

```sql
SHOW PLACEMENT FOR {DATABASE | SCHEMA} schema_name;
SHOW PLACEMENT FOR TABLE table_name [PARTITION partition_name];
SHOW PLACEMENT FOR INDEX index_name FROM table_name [PARTITION partition_name];
```

TiDB will automatically find the effective rule based on the rule priorities.

This statement outputs at most 1 line. For example, when querying a table, only the placement rule defined on the table itself is shown, and the partitions and indices in it will not be shown.

The output of this statement contains these fields:

* target: The object queried. It can be a table, partition, or index.
    * For table, it is shown in the format `TABLE table_name`
    * For partition, it is shown in the format `TABLE table_name PARTITION partition_name`
    * For index, it is shown in the format `INDEX index_name FROM table_name`
* placement: The original ALTER statement that affects the placement of this rule. Note that the object in this ALTER statement may be different with `target`. In the example above, if `target` is `p0`, the ALTER statement shown may be `ALTER TABLE t …`.

### Show create table

It’s useful to show rules in `SHOW CREATE TABLE` statement, because users can check the rules easily.

Since data in TiDB can be imported to MySQL, the placement rules definition must be shown as a MySQL-compatible comment in such a format `/*T![placement] placement_gammar*/`, where `placement_gammar` can be recognized by TiDB. That means TiDB needs to support two approaches to define placement rules, one in `CREATE TABLE` and another in `ALTER`.

This is complicated, and `ALTER` is able to satisfy most of the cases, so `SHOW CREATE TABLE` can be kept untouched so far.

## Privilege management

Privilege management is the same as before:

* Executing `ALTER` statement requires `Alter` privilege
* Querying `information_schema.placement_rules` only returns those placement rules on the tables that visible to the current user
* `ADMIN SHOW DDL` statement can only be executed by those users who have `Super` privilege.

## Ecosystem tools

Many tools are based on binlog or metadata. For example, TiDB-binlog is based on binlog, while Lightning and Dumpling are based on metadata. Placement rules need to be compatible with these tools.

If the downstream is not TiDB, no change needs to be made. But even if it is TiDB, TiKV nodes may have a different geographical topology, which means the labels of TiKV nodes may be different. In this case, placement rules can not be enforced on them.

Based on this consideration, placement rules need not to be exported to binlog or metadata.

However, there may be also cases where users want exactly the same placement rules as the upstream, and altering placement rules manually is very annoying. It will be considered in the future if there’s a need.
