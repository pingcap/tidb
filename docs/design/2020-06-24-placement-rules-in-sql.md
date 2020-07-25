# Defining placement rules in SQL

- Author(s):     [djshow832](https://github.com/djshow832) (Ming Zhang)
- Last updated:  2020-06-24
- Discussion at: https://docs.google.com/document/d/18Kdhi90dv33muF9k_VAIccNLeGf-DdQyUc8JlWF9Gok

## Motivation

TiDB supports placement rules, which can define the placement of data in a more flexible and more granular way. But it only provides configuration files to define them, and it’s complicated.

This article proposes an approach to configure placement rules through DDL statements. TiDB server parses the statements and notify PD to perform the change. In this way, usability can be improved.

The scenarios of defining placement rules in SQL include:

- Place data across regions to improve access locality
- Limit data within its national border to gaurantee data sovereignty
- Place latest data to SSD and history data to HDD
- Increase the replicas count of more important data
- Separate irrelevant data to different stores to improve availability

## Define placement rules

There are 2 kinds of operations on the placement:

* ADD: Add new `COUNT` replicas for the object satisfying the `LABEL` and `ROLE` configuration.
* ALTER: Override the current replica configuration. The original configuration will be discarded.

They’re both achieved by executing `ALTER TABLE` statements.

### Add placement rules

Adding new replicas can be done by one or more `ADD PLACEMENT` clauses:

```sql
ALTER TABLE table_name
	ADD PLACEMENT LABEL=label ROLE=role COUNT=count,
	...
```

This statement indicates TiDB to add replicas for all data of table `table_name`, including indexes. As statement `ALTER TABLE table_name SET TIFLASH REPLICA count` is able to add TiFlash replicas now, `ADD PLACEMENT` doesn't support adding TiFlash replicas.

`ADD PLACEMENT` is just a part of alter options, just like `ADD COLUMN` or `ADD CONSTRAINT`.

To define multiple roles at once, multiple `ADD PLACEMENT` clauses can appear in a single `ALTER TABLE` statement, each for one Raft role. For example:

```sql
ALTER TABLE table_name
	ADD PLACEMENT LABEL="+zone=sh" ROLE=leader COUNT=1,
	ADD PLACEMENT LABEL="-zone=sh" ROLE=follower COUNT=2;
```

This statement indicates PD to schedule the leader to `sh`, and do not schedule followers to `sh`.

Placement rules must conform to Raft constraints. For example, an error should be reported when executing this statement:

```sql
ALTER TABLE test
	ALTER PLACEMENT LABEL="+zone=sh" ROLE=leader COUNT=2;
```

There can only be one leader, so `COUNT` must be 1 or omitted.

Besides, at most one role can be defined on the same object. If multiple rules are added on the same role, they will be combined to one rule. For example:

```sql
ALTER TABLE test
	ADD PLACEMENT LABEL="+zone=sh" ROLE=voter COUNT=2,
	ADD PLACEMENT LABEL="+zone=bj" ROLE=voter COUNT=2;
```

The same role `voter` is defined in 2 different rules, each of which adds 2 replicas. So it is equivalent to:

```sql
ALTER TABLE test
	ADD PLACEMENT LABEL="+zone=sh:2,+zone=bj:2" ROLE=voter COUNT=4;
```

Note that as there may already exist 3 replicas by default, it will be 7 replicas after executing this statement. So `ADD PLACEMENT` can be taken as a shortcut for adding replicas to a defined role. In the example above, it can be replaced by `ALTER PLACEMENT`.

`ADD PLACEMENT` is also capable of forbidding some role from being placed on some zones. For example:

```sql
ALTER TABLE test
	ADD PLACEMENT LABEL="-zone=sh" ROLE=follower;
```

In this example, it indicates that `follower` can not reside on `sh`, rather than adding replicas. The `COUNT` option is unnecessary here.

More details of `LABEL` option is described in the "Label Configuration" section.

`ADD PLACEMENET` is implemented by adding a placement rule in PD. The statement must wait until the PD returns a message. It can be cancelled by executing `ADMIN CANCEL DDL JOBS` statement.

### Alter placement rules

Altering current placement rules can be done by one or more `ALTER PLACEMENT` clauses:

```sql
ALTER TABLE table_name
	ALTER PLACEMENT LABEL=label ROLE=role COUNT=count,
	...
```

This statement indicates TiDB to overwrite the current placement rule with the same `role`. It affects all data of table `table_name`, including indexes,.

Assuming table `test` has 3 replicas by default, the default placement rule is equivalent to:

```sql
ALTER TABLE test
	ADD PLACEMENT ROLE=voter COUNT=3;
```

`LABEL` is omitted here, because there is no label constraints on voters.

Since at most one rule can be defined for each role, `ALTER PLACEMENT` will replace the existing rule that has the same role. For example:

```sql
ALTER TABLE test
	ADD PLACEMENT LABEL="+zone=sh" ROLE=voter COUNT=2,
	ADD PLACEMENT LABEL="+zone=bj" ROLE=voter COUNT=2,
	ALTER PLACEMENT LABEL="+zone=sh" ROLE=voter COUNT=3,
	ALTER PLACEMENT LABEL="+zone=bj" ROLE=voter COUNT=3;
```

As all the rules are defined on the same role `voter`, the first 3 rules will be overwritten by the last one. So it is equivalent to:

```sql
ALTER TABLE test
	ALTER PLACEMENT LABEL="+zone=bj" ROLE=voter COUNT=3;
```

Similarly, `ALTER PLACEMENT` statements must wait until the PD returns a message. It is implemented by overwriting the current placement rule with a new one.

### Drop placement rules

Dropping a placement rule can be achieved by a `DROP PLACEMENT` clause:

```sql
ALTER TABLE table_name
	DROP PLACEMENT ROLE=role,
	...
```

In the statement, only `ROLE` option is needed. It drops the placement rule on the specified role. The rule can be either defined on the object itself or inherited from its parent. For example, if a rule on table `t` is inherited from the database where `t` belongs to, it can also be dropped through this way.

There is no shortcut to reset all the rules. It may help, but it makes the system more complicated.

Placement rules of indices and partitions can also be dropped in a similar grammar. The statement must wait until PD returns a message.

### Label configuration

`LABEL` option in the `ADD PLACEMENT` or `ALTER PLACEMENT` clauses indicates the label constraints. Data must be placed on the stores whose labels conform to `LABEL` constraints. If `LABEL` is omitted, it means no label constraint is enforced, thus the replicas can be placed anywhere.

Option `LABEL ` should be a string and in one of these formats:

- `{+|-}key=value,...`
- `+key=value:count,...`

Prefix `+` indicates that data can only be placed on the stores whose labels contain such labels, and `-` indicates that data can’t be placed on the stores whose labels contain such labels.

`key` here refers to the label name, and `value` is the label value. `key` should be defined in the store configurations.

If `count` is specified, it means that at least `count` replicas should be placed on those stores. If it's not specified, the count of replicas on those stores is not limited. But the total count of replicas should still conform to the `COUNT` option in the `ADD PLACEMENT` or `ALTER PLACEMENT` clause. When the prefix is `-`, the `count` is meaningless.

For example, `+zone=sh,+zone=bj` indicates to place data only in `sh` and `bj` zones. `zone` should have already be defined in store configuration. For example:

```sql
[server]
labels = "zone=<zone>,rack=<rack>,host=<host>"
```

For another example, `LABEL="+zone=sh:2,-zone=bj" ROLE=voter COUNT=3` indicates to place at least 2 repicas on `sh`, while another replica can be put on anywhere else but `bj`.

Label constraints can be implemented by defining `label_constraints` field in PD placement rule configuration. `+` and `-` correspond to property `op`. Specifically, `+` is equivalent to `In` and `-` is equivalent to `NotIn`.

For example, `+zone=sh,+zone=bj` is equivalent to:

```
"label_constraints": [
	{"key": "zone", "op": "in", "values": ["sh", “bj”]}
]
```

`location_labels` field in placement rules is used to isolate replicas to different zones to improve availability. For now, the global configuration can be used as the default `location_labels` for all placement rules defined in SQL.

### Role configuration

`ROLE` in the statement defines the Raft role of the replicas. It must be specified in the statement. There are 4 predefined roles:

- `leader`. Exactly one `leader` is allowed.
- `follower`.
- `voter`. It includes `leader` and `follower`.
- `learner`. It can be either TiFlash or TiKV.

If both `voter` and `follower` are defined in the rules, the replicas of `follower` are not included in the replicas of `voter`. For example:

```sql
ALTER TABLE test
	ADD PLACEMENT LABEL="+zone=bj" ROLE=follower COUNT=2,
	ALTER PLACEMENT LABEL="+zone=sh" ROLE=voter COUNT=2;
```

There are 4 replicas for table `test`, 2 of which are in `sh` and 2 are in `bj`.  Leader can only be placed on `sh`.

However, there may be some cases when rules conflict. For example:

```sql
ALTER TABLE test
	ALTER PLACEMENT LABEL="+zone=sh" ROLE=voter COUNT=3,
	ADD PLACEMENT LABEL="-zone=sh" ROLE=follower;
```

Since `voter` will all be in `sh` but `follower` can not be in `sh`, then `leader` will be all in `sh` and there won't be followers. The `COUNT` is specified to 3 but there should be only one `leader`, so the rules conflict and an error will be reported.

`ROLE` in the statement is equivalent to field `role` in PD placement rule configuration.

### Count configuration

`COUNT` in the statement indicates the replica count of the specified role.

When the `LABEL` option only contains constraints with `-`, `COUNT` can be omitted. For example, `LABEL="-zone=sh" ROLE=follower` only emphasizes that `follower` can not be placed on `sh`, but it doesn't change the replica count.

Rules defined on `leader` can also omit `COUNT`, because the count of leader is always 1.

When all the replica counts are specified in the `LABEL` option, `COUNT` can also be omitted. For example, `LABEL="+zone=bj:2,+zone=sh:1", ROLE=voter` indicates that the `COUNT` is 3.

When both `COUNT` and `count` in `LABEL` are specified, it indicates that the other replicas can be placed anywhere. For example, in the case `LABEL="+zone=bj:2,+zone=sh:1", ROLE=voter, COUNT=4`, 2 replicas are in `bj` and 1 in `sh`, and the last replica can be anywhere including `bj` and `sh`.

When the `LABEL` contains constraints with `+` and without `count`, the `COUNT` must be specified. For example, `LABEL="+zone=bj" ROLE=follower` is vague, as the count of `follower` can not be inferred.

`COUNT ` in the statement is equivalent to field `count` in PD placement rule configuration.

### Key range configuration

In placement rule configuration files, the key range must be specified. Now that `table_name` is specified in the `ALTER TABLE` statement, key range can be inferred.

Typically, key format is in such a format: `t_{table_id}_r_{pk_value}`, where `pk_value` may be `_tidb_rowid` in some cases. `table_id` can be inferred from `table_name`, thus key range can be `t_{table_id}_` to `t_{table_id+1}_`.

Similarly, key range of partitions and indices can also be inferred.

### Database placement

Defining placement of databases simplifies the procedures when there are many tables. For example, in a typical multi-tenant scenario, each user has a private database. The dataset in one database is relatively small, and it’s rare to query across databases. In this case, a whole database can be placed in a single region to reduce multi-region latency.

Placement of databases is defined through `ALTER` statements:

```sql
ALTER {DATABASE | SCHEMA} schema_name
	{ADD | ALTER} PLACEMENT LABEL=label ROLE=role COUNT=count,
	...
```

This statement indicates TiDB to add new replicas or alter placement for one database, including all tables in it.

Creating or dropping a table also affects the placement rules. If a placement rule is defined on a database, all tables in this database will automatically apply that rule, including the existing tables and the tables created later.

Once the placement rules on a database is changed, the tables should also update their placement rules. See the section "Rule inheritance" for details.

Since key range is not successive in one database, each table in the database corresponds to at least one placement rule, so there may be many placement rules.

### Partition placement

Defining placement of partitions is useful for Geo-Partitioning. In the cases where data is very relevant to zones, Geo-Partitioning can be applied to reduce multi-region latency.

In Geo-Partitioning, the table must be splitted into partitions, and each partition is placed in specific zones. There are some kinds of partition placement:

* Place all voters on one zone
* Place only leaders on one zone
* Place leaders and half of the followers on one zone

It’s up to users to choose the right solution.

Placement of partitions is also defined through `ALTER TABLE` statements:

```sql
ALTER TABLE table_name ALTER PARTITION partition_name
	{ADD | ALTER} PLACEMENT LABEL=label ROLE=role COUNT=count,
	...
```

This statement indicates TiDB to add new replicas or alter placement for one partition, including its local indexes.

`LABEL`, `ROLE` and `COUNT` configurations are the same as above.

The key format of a partitioned table is `t_{partition_id}_r_{pk_value}`. As `partition_id` is part of the key prefix, the key range of a partition is successive.

`partition_id` can be inferred from `partition_name`, so the key range is `t_{partition_id}_` to `t_{partition_id+1}_`.

Placement rules can also be defined on a partitioned table. Because there are multiple key ranges for the table, multiple rules will be generated and sent to PD. When placement rules are defined both on the table and its partitions, the rule priorities described later should be applied.

### Unpartitioned index placement

Defining placement of indices is more complicated, because indices can be unpartitioned or partitioned. Each case should be considered separately.

The index here can be primary index or secondary index. When the key of a clustered index is `_tidb_rowid` rather than the primary key, the primary index is actually an unclustered index. In this case, an index placement statement is applied.

Expression indices and invisible indices are also supported, as the key format is the same as normal.

Defining placement of an unpartitioned index is in such a statement:

```sql
ALTER TABLE table_name ALTER INDEX index_name
	{ADD | ALTER} PLACEMENT LABEL=label ROLE=role COUNT=count,
	...
```

This key format of an unpartitioned index is `t_{table_id}_i_{index_id}_r_{pk_value}`. The key range can be inferred by `table_id` and `index_id`.

### Partitioned index placement

Defining placement of an index in one specific partition is in such a statement:

```sql
ALTER TABLE table_name ALTER PARTITION partition_name ALTER INDEX index_name
	{ADD | ALTER} PLACEMENT LABEL=label ROLE=role COUNT=count,
...
```

The key format of partitioned index is `t_{partition_id}_i_{index_id}_r_{pk_value}`. The key range can be inferred by `partition_id` and `index_id`.

When an index is partitioned, defining placement of the whole index at once is not supported. It will involve multiple key ranges, and the scenario of its application is rare.

For example, `t` is a partitioned table and `idx` is the index on `t`. It’s not supported to do this:

```sql
ALTER TABLE `t` ALTER INDEX `i` ADD PLACEMENT ...
```

To alter the placement rule of `idx`, a partition must be specified in the statement.

Currently, global secondary index on partitioned tables is not supported, so it can be ignored for now.

### Sequence placement

Sequence is typically used to allocate ID in `INSERT` statements, so the placement of sequences affects the latency of `INSERT` statements.

However, sequence is typically used with cache enabled, which means very few requests are sent to sequence. So defining placement of sequences is not supported for now.

## DDL management

Some kinds of DDL on databases also affect placement rules.

### DDL on tables

Once a table is created, it follows the placement rule of its database.

Defining placement rules in a `CREATE TABLE` statement is useful, especially in data sovereignty scenarios. Data sovereignty requires sensitive data to reside within its own national border, which is very serious. So defining placement rules after creating tables is not acceptable. But for now, it's not supported, as it complicates the implementation.

Once a table is dropped, the placement rules on it cannot be dropped immediately, because the table can be recovered by `FLASHBACK` or `RECOVER` statements before GC collects the data. Related placement rules should be kept temporarily and will be removed after GC lifetime.

Since dropped tables are collected by the GC worker, when the GC worker collects a table, the related placement rules can be removed.

When it’s time to remove all related placement rules, not only those rules defined on the table should be removed, but also the rules defined on its partitions and indices.

Once a table is truncated, the table id is updated. As its key range is changed, the placement rules should also be updated.

Since the table can be recovered later by `FLASHBACK` statement, a snapshot of the original placement rules should be saved temporarily. After recovering, the table name is changed, but the table id is the original one, so the snapshot of the original placement rules can be recovered directly.

For example:

```sql
TRUNCATE TABLE t;

ALTER TABLE t 
	ALTER PLACEMENT LABEL="+zone=sh" ROLE=leader;

FLASHBACK table t to t1;
```

In this case, the placement rules of `t` is altered by the user just after truncating. Once `t` is flashbacked to `t1`, the placement rules of `t1` should be recovered to the version before truncating. However, the procedure is complicated and the  action is rare, so the placement rules will be recovered to the newest version for now.

DDL on partitions and indexes will be discussed below, and other DDL on tables won’t affect placement rules:

* Altering columns
* Renaming tables
* Altering charset and collation

### DDL on partitions

TiDB supports adding and dropping partitions.

Once a partition is added, its placement rule is empty and the partition follows the rule of the table it belongs to.

Once a partition is dropped, it can’t be recovered anymore, so its placement rules can be removed immediately.

Also note that DDL on tables may also effect partitions. It's descibed in the section "DDL on tables".

### DDL on indices

Once an index is created on an unpartitioned table, the index should follow the rule of the table it belongs to.

Once an index is created on a table with partitions, each part of the index should follow the rule of the partition it belongs to.

Once an index is dropped, it can’t be recovered anymore, so its placement rules can be removed immediately.

Altering primary index is the same with altering secondary indexes. Because if a primary index can be created or dropped, it must be an unclustered index.

Other DDL on indices won’t affect placement rules:

* Renaming index
* Altering the visibility of index

### Show DDL jobs

As mentioned earlier, all statements related to placement rules must wait until PD returns. If the execution is interrupted, it should retry until it succeeds or is cancelled, just like other DDL jobs.

PD schedules regions asynchronously after it returns the message. TiDB can query the progress of scheduling from PD. The progress is observed by executing `SHOW PLACEMENT` instead of `ADMIN SHOW DDL JOBS`, because the DDL job finishes once PD returns a message.

Ongoing and finished statements related to placement rules can also be queried through `ADMIN SHOW DDL`, `ADMIN SHOW DDL JOBS`, or other similar statements.

## View rules

All placement rules can be queried through statements.

### System table

A new system table `information_schema.placement_rules` is added to view all placement rules. The table contains such columns:

* rule_id
* database_id and database_name
* table_id and table_name
* partition_id and partition_name
* index_id and index_name
* label
* role
* count
* scheduling state

The system table is a virtual table, which doesn’t persist data. When querying the table, TiDB queries PD and integrates the result in a table format. That also means the metadata is stored on PD instead of TiKV.

Advantages of building system table include:

* It’s easy for users to filter and aggregate
* There’s no need to support a new grammar, and it’s easier to implement

### Show placement

But there’re a problem here. The system table only contains stored placement rules, and users cannot query the effective rule of one object from it.

For example, table `t` has two partitions `p0` and `p1`, and a placement rule is added on `t`. If the user wants to query the working rule of `p0`, he will find no placement rule is defined for `p0` through the system table. Based on the rule priorities described later, he must query the placement rule on `t`. This procedure is annoying.

To simplify the procedure, a `SHOW` statement is provided to query the effective rule for one specified object.

The statement is in such a format:

```sql
SHOW PLACEMENT FOR {DATABASE | SCHEMA} schema_name;
SHOW PLACEMENT FOR TABLE table_name [PARTITION partition_name];
SHOW PLACEMENT FOR INDEX index_name FROM table_name [PARTITION partition_name];
```

TiDB will automatically find the effective rule based on the rule priorities.

This statement outputs at most 1 line. For example, when querying a table, only the placement rule defined on the table itself is shown, and the partitions and indices in it will not be shown.

The output of this statement contains these fields:

* target: The object queried. It can be a database, table, partition, or index.
    * For database, it is shown in the format `DATABASE database_name`
    * For table, it is shown in the format `TABLE database_name.table_name`
    * For partition, it is shown in the format `TABLE database_name.table_name PARTITION partition_name`
    * For index, it is shown in the format `INDEX index_name FROM database_name.table_name`
* placement: A equivalent `ALTER` statement that defines the placement rule.
* scheduling state: The scheduling progress from the PD aspect.

### Show create table

It’s useful to show rules in `SHOW CREATE TABLE` statement, because users can check the rules easily.

Since data in TiDB can be imported to MySQL, the placement rules definition must be shown as a MySQL-compatible comment such as `/*T![placement] placement_gammar*/`, where `placement_gammar` can be recognized by TiDB. That means TiDB needs to support two approaches to define placement rules, one in `CREATE TABLE` and another in `ALTER TABLE`.

This is complicated, and `ALTER TABLE` is able to satisfy most of the cases, so `SHOW CREATE TABLE` can be kept untouched so far.

## Implementation

This section focuses on the implemention details of defining placement rules in SQL.

### Storing placement rules

PD uses placement rules to schedule data, so a replica of placement rules must be persistent on the PD. 

However, TiDB also uses placement rules in some cases, as discussed in section "Querying placement rules". There are basically 2 ways to achieve this:

- Save the placement rules in table information, which will be duplicated with PD
- Only PD persists the placement rules, while TiDB caches a copy of them

Before choosing the solution, transactional requirements need to be noticed:

- Defining placement rules may fail, and users will probably retry it. As retrying `ADD PLACEMENT` will add more replicas than expected, so the atomacity of the opertion needs to be gauranteed.
- `ADD PLACEMENT` needs to read the original placement rules, combine the 2 rules and then store them to PD, so external consistency should be gauranteed.

If the placement rules are stored both on TiKV and PD, the approaches to keep atomicity are as follows:

- Enforce a 2PC protocal on TiKV and PD
- Persist redo log on TiDB

The approaches to keep external consistency are as follows:

- Define placement rules in serial
- Enforce an exclusive lock on one of the replicas and release it after the job finishes

As a contrast, if the placement rules are only stored on PD, the approaches to keep atomicity are as follows:

- Write all the placement rules in one ETCD transaction
- Persist redo log on TiDB

The approaches to keep external consistency are as follows:

- Define placement rules in serial
- Enforce an exclusive lock on PD and release it after the job finishes

The comparison shows that storing placement rules only on PD is more practical. To guarantee the transactional characteristics, the easiest way is to write all placement rules in one ETCD transaction and define them in serial on the TiDB side.

### Querying placement rules

The scenarios where TiDB queries placement rules are as follows:

- (DML) The optimizer uses placement rules to decide to route cop request to TiKV or TiFlash. It's already implemented and the TiFlash information is written into table information, which is stored on TiKV.
- (DML) It will be probably used in locality-aware features in the furture, such as follower-read. Follower-read is always used when TiDB wants to read the nearest replica to reduce multi-region latency. In some distributed databases, it’s implemented by labelling data nodes and selecting the nearest replica according to the labels.
- (DDL) Once a rule is defined on a table, all the subsequent partitions added to the table should also inherit the rule. So the `ADD PARTITION` operation should query the rules on the table. The same is true for creating tables and indices.
- (DDL) `SHOW PLACEMENT` statement should output the placement rules correctly.

As placement rules will be used in DML, low latency of querying the rules must be guaranteed. As discussed in section "Storing placement rules", placement rules are only persistent on PD. To lower the latency, the only way is caching the placement rules in TiDB.

Since the cache is created, there must be a way to validate it. Different from region cache, placement rules cache can only be validated each time from PD. There are some ways to work around:

- Update the schema version once a placement rule is changed, just like other DDL. PD then broadcasts the latest schema version along with placement rules to all the TiDB instances. There will be a slight delay for queries before reading the latest placement rules. The side affect is that more transactions will retry since the schema version is changed.
- TiDB queries placement rules from PD periodly. The delay is controllable but not eliminable.
- Once a placement rule is changed, PD broadcasts it to all the TiDB instances. In this approach, schema version is not involved, so transactions are not affected. The delay is not eliminable either.

All the approaches above will result in a delay. Fortunately, for all the DML cases above, delay is acceptable. It doesn’t matter much if the optimizer doesn’t percept the placement rules changement immediately. The worst result is that the latency is relatively high for a short time.

For all the DDL cases, a delay is not acceptable. Once the placement rules are written successfully, subsequent DDL statements should fetch the latest placement rules to gaurantee external consistency.

Fortunately, the typical frequencies of such DDL are low, so a higher latency is acceptable. Thus these DDL can query PD directly every time. 

To query the placement rules on a specified object, the object ID should be written to the placement rules, or it can be inferred from other fields. Now that `group_id` contains the object ID, TiDB can decode the object ID from it. See section "Rule priorities" for details.

### DDL procedures

Defining placement rules is a type of DDL, so it's natural to implement it in a typical DDL procedure. But it differs from other DDL in that it writes to PD instead of TiKV.

The fact that the DDL procedure in TiDB is mature helps to achieve some features of defining placement rules:

- Placement rules can be defined in serial as there's only one DDL owner at the same time
- DDL is capable of disaster recovery as the middle states are persistent in TiKV
- DDL is rollbackable as the middle states can transform from one to another

But for now, the schema version is unncessary to be updated once the placement rules are updated.

### Rule priorities

When several rules are defined for one record, the most granular rule is chosen for this record. More specifically, the rule priority is: index > partition > table > database > default.

For example:

1. At the beginning, all data is placed based on the default placement rules.
2. When a placement rule is added on table `t`, all data on `t` is placed based on the rule.
3. When a placement rule is added on partition `p0` of `t`, all data on `p0` is placed based on the rule of `p0`, but other partitions stay still.
4. When the placement rule on `p0` is removed, data on `p0` is placed based on the rule of `t`, just like other partitions.

Rules priorities should be checked once a placement is added, altered, or dropped.

Rule priorities can be implemented by properties `index` and `override`. `override` is alway enabled, and `index` stands for the priority. Specifically, `index` values are like this:

* `index` of default placement rules is 0
* `index` of database placement rules is 1
* `index` of table placement rules is 2
* `index` of partition placement rules is 3
* `index` of index placement rules is 4

In such a way, the most granular rule always works.

When multiple placement rules are defined on the same granular object, the new one always overwrites the old one. To achieve this, property `group_id` in placement rules can be used to identify the database object.

`group_id` is in such a format:

* `group_id` of a database is the database id
* `group_id` of a table is the table id
* `group_id` of a partition is the partition id
* `group_id` of an unpartitioned index is the concatenation of table id and index id, e.g. `100-1`
* `group_id` of a partitioned index is the concatenation of partition id and index id

`id` field in placement rules can be anything but empty.

### Rule inheritance

In some cases, creating a new object doesn't need to store its placement rules:

- Creating a database
- Creating an index on an unpartitioned table
- Creating an index on a partition

In the last two cases, the key range of the index is included in the key range of the table or partition it belongs to. PD will guarantee the rule priorities described above.

But in other cases, creating a new object needs to store its placement rules:

- Creating a table in a database
- Creating a partition in a table

The placement rules of databases and partitioned tables don't actually work on PD, because the key ranges don't include any records. They are stored on PD and only serve for querying when new objects are created in them.

For example, when defining a placement rule on database `db` whose ID is 1, then the `group_id` is `1` and key range is empty. When a new table `t` is created in `db`, TiDB queries the placement rules of the `db` by `group_id=1` and copies them to table `t`, while the new key range corresponds to table `t`.

Once the placement rules on a database or a partitioned table are changed, the inherited placement rules are also updated, but others are kept.

Consider such a scenario:

```sql
ALTER DATABASE db
	ALTER PLACEMENT LABEL="+zone=sh" ROLE=voter COUNT=3;
	
CREATE TABLE db.t1(id int);

CREATE TABLE db.t2(id int);

ALTER TABLE db.t2
	ADD PLACEMENT LABEL="+zone=bj" ROLE=follower COUNT=1;
	
ALTER DATABASE db
	ALTER PLACEMENT LABEL="+zone=bj" ROLE=voter COUNT=3,
	ADD PLACEMENT LABEL="-zone=sh" ROLE=follower;
```

The final placement rules of `t1` and `t2` will be:

```sql
ALTER TABLE db.t1
	ALTER PLACEMENT LABEL="+zone=bj" ROLE=voter COUNT=3,
	ADD PLACEMENT LABEL="-zone=sh" ROLE=follower;

ALTER TABLE db.t2
	ALTER PLACEMENT LABEL="+zone=bj" ROLE=voter COUNT=3,
	ADD PLACEMENT LABEL="+zone=bj" ROLE=follower COUNT=1;
```

Because all the placement rules on `t1` are inherited from `db`, they will keep the same with `db` all the time. The placement rule `LABEL="+zone=bj" ROLE=follower COUNT=1` is private for `t2`, so it will be kept after the changement of `db`. But the other rule `LABEL="+zone=bj" ROLE=voter COUNT=3` is still inherited from `db`.

To achieve this goal, the placement rules should be marked with the source  where they come from.

## Privilege management

Privilege management is quite straightforward:

* `ALTER` statement requires `Alter` privilege
* `information_schema.placement_rules` and `SHOW PLACEMENT` only shows the placement rules on the objects that visible to the current user
* `ADMIN SHOW DDL` requires `Super` privilege

## Ecosystem tools

Many tools are based on binlog or metadata. For example, TiDB-binlog is based on binlog, while Lightning and Dumpling are based on metadata. Placement rules need to be compatible with these tools.

If the downstream is not TiDB, no change needs to be made. But even if it is TiDB, TiKV nodes may have a different geographical topology, which means the labels of TiKV nodes may be different. In this case, placement rules can not be enforced on them.

Based on this consideration, placement rules need not to be exported to binlog or metadata. This is applicable for all tools, including TiCDC and BR.

However, there may be also cases where users want exactly the same placement rules as the upstream, and altering placement rules manually is very annoying. It will be considered in the future if there’s a need.
