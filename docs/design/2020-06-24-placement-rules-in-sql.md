# Defining placement rules in SQL

- Author(s):     [djshow832](https://github.com/djshow832) (Ming Zhang), [morgo](https://github.com/morgo) (Morgan Tocker)
- Last updated:  2021-07-12
- Discussion at: https://docs.google.com/document/d/18Kdhi90dv33muF9k_VAIccNLeGf-DdQyUc8JlWF9Gok

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
    * [New Syntax Overview](#new-syntax-overview)
	* [Updates to Existing Syntax](#updates-to-existing-syntax)
	* [Placement Rules Syntax](#placement-rules-syntax)
	* [Additional Semantics](#additional-semantics)
* [Implementation](#implementation)
    * [Storing Placement Policies](#storing-placement-policies)
    * [Storing Placement Rules](#storing-placement-rules)
    * [Querying Placement Rules](#querying-placement-rules)
    * [DDL procedures](#ddl-procedures)
    * [Building placement rules](#building-placement-rules)
    * [Rule priorities](#rule-priorities)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)
    * [Compliance Requirements](#compliance-requirements)
    * [Behaviors](#behaviors)
* [Changelog](#changelog)

## Introduction

TiDB currently supports placement rules, which can define the placement of data in a more flexible and more granular way. But the current usage requires configuration files to manage them, and for end-users this can be complicated.

This document proposes an approach to configure placement rules through DDL statements. Usability is improved because the TiDB server parses the statements and notifies PD to perform the change.

## Motivation or Background

The scenarios of defining placement rules in SQL include:

- Place data across regions to improve access locality
- Add a TiFlash replica for a table
- Limit data within its national border to guarantee data sovereignty
- Place latest data to SSD and history data to HDD
- Place the leader of hot data to a high-performance TiKV instance
- Increase the replica count of more important data
- Separate irrelevant data into different stores to improve availability

These scenarios usually fit into one of the following categories:

1. An optimization or availability use case (such as improving data access locality)
2. A compliance use case (such as ensuring data resides within a geographic region)

This proposal makes some intentional decisions so that both categories of use-cases can be handled correctly. It improves upon the earlier proposal by reducing the risk of misconfiguration which can affect compliance use cases.

## Detailed Design

### New Syntax Overview

There are two ways to specify placement rules:

1. By assigning `PLACEMENT` directly on a database, table or partition (direct assignment)
2. By creating a new `PLACEMENT POLICY` and then applying the placement policy to a database, table or partition (placement policy)

Using a `PLACEMENT POLICY` will be recommended for compliance requirements, since it can allow administrators to better keep track of usage. This can be seen as similar to how complex environments will use `ROLES` for management instead of directly assigning privileges to users.

Both syntaxes are considered a [`table_option`](https://dev.mysql.com/doc/refman/8.0/en/alter-table.html), and available in both `CREATE TABLE` and `ALTER TABLE` contexts.

#### Direct assignment

Creating a new table with a directly assigned `PLACEMENT` constraints:

```sql
CREATE TABLE t1 (
	id INT NOT NULL PRIMARY KEY,
	b VARCHAR(100)
) PLACEMENT CONSTRAINTS="[+zone=sh]" ROLE=leader REPLICAS=1, CONSTRAINTS="[+zone=sh]" ROLE=follower REPLICAS=1, CONSTRAINTS="[+zone=gz]" ROLE=follower REPLICAS=1;
```

Adding `PLACEMENT` to an existing table:

```sql
CREATE TABLE t1 (
	id INT NOT NULL PRIMARY KEY,
	b VARCHAR(100)
);
ALTER TABLE t1 PLACEMENT CONSTRAINTS="[+zone=sh]" ROLE=leader REPLICAS=1, CONSTRAINTS="[+zone=sh]" ROLE=follower REPLICAS=1, CONSTRAINTS="[+zone=gz]" ROLE=follower REPLICAS=1;
```

#### Placement Policy

Creating a new `PLACEMENT POLICY`:

```sql
CREATE PLACEMENT POLICY `companystandardpolicy` CONSTRAINTS="[+zone=sh]" ROLE=leader REPLICAS=1, CONSTRAINTS="[+zone=sh]" ROLE=follower REPLICAS=1, CONSTRAINTS="[+zone=gz]" ROLE=follower REPLICAS=1;
```

Creating a new table with a `PLACEMENT POLICY`:

```sql
CREATE TABLE t1 (
	id INT NOT NULL PRIMARY KEY,
	b VARCHAR(100)
) PLACEMENT POLICY=`companystandardpolicy`;
```

Adding `PLACEMENT POLICY` to an existing table:

```sql
CREATE TABLE t1 (
	id INT NOT NULL PRIMARY KEY,
	b VARCHAR(100)
);
ALTER TABLE t1 PLACEMENT POLICY=`companystandardpolicy`;
```

Expected Behaviors:
- `CREATE` or `ALTER` and specifying a `PLACEMENT POLICY` that does not exist results in an error: placement policy 'x' is not defined (TBD: see "unresolved questions")
- Placement policies are globally unique names. Thus, a policy named `companyplacementpolicy` can apply to the db `test` as well as `userdb`. The namespace does not overlap with other DB objects.
- Placement Policy names are case insensitive, and follow the same rules as tables/other identifiers for length (64 chars) and special characters.
- The full placement policy can be seen with `SHOW CREATE PLACEMENT POLICY x`. This is useful for shorthand usage by DBAs, and consistent with other database objects.
- It is possible to update the definition of a placement policy with `ALTER PLACEMENT POLICY x CONSTRAINTS ..;` This is modeled on the statement `ALTER VIEW` (where the view needs to be redefined)
- The statement `DROP PLACEMENT POLICY` should return an error if the policy is in use by any objects (TBD: see "unresolved questions")

#### Metadata commands

Besides `SHOW CREATE PLACEMENT POLICY x`, it should be possible to summarize all placement for a database system. This is beneficial for compliance scenarios.

#### information_schema.placement_rules

A new system table `information_schema.placement_rules` is added to view all placement rules. The table contains such columns:

* rule_id
* target ID
* target name
* constraints
* role
* replicas
* scheduling state

The system table is a virtual table, which doesn’t persist data. When querying the table, TiDB queries PD and integrates the result in a table format. That also means the metadata is stored on PD instead of TiKV.

An object may contain multiple placement rules, each of which corresponds to a rule in PD.

Advantages of building system table include:

* It’s easy for users to filter and aggregate the result
* There’s no need to support a new grammar, and it’s easier to implement

#### SHOW PLACEMENT

The system table only contains stored placement rules, and users cannot query the effective rule of one object from it.

For example, table `t` has two partitions `p0` and `p1`, and a placement rule is added on `t`. If the user wants to query the working rule of `p0`, he will find no placement rule is defined for `p0` through the system table. Based on the inheritance rules for partiioned tables the userneeds to query the placement rule on `t`. This procedure is annoying.

To simplify the procedure, a `SHOW PLACEMENT` statement is provided to summarize the effective rules for one specified object.

The statement is in such a format:

```sql
SHOW PLACEMENT FOR {DATABASE | SCHEMA} schema_name;
SHOW PLACEMENT FOR TABLE table_name [PARTITION partition_name];
SHOW PLACEMENT FOR INDEX index_name FROM table_name [PARTITION partition_name];
```

TiDB will automatically find the effective rule based on the rule priorities.

This statement outputs at most 1 line. For example, when querying a table, only the placement rule defined on the table itself is shown, and the partitions and indices in it will not be shown.

The output of this statement contains these fields:

* Target: The object queried. It can be a database, table, partition, or index.
    * For database, it is shown in the format `DATABASE database_name`
    * For table, it is shown in the format `TABLE database_name.table_name`
    * For partition, it is shown in the format `TABLE database_name.table_name PARTITION partition_name`
    * For index, it is shown in the format `INDEX index_name FROM database_name.table_name`
* Equivalent placement: A equivalent `ALTER` statement on `target` that defines the placement rule.
* Existing placement: All the executed `ALTER` statements that affect the placement of `target`, including the statements on its parent.
* Scheduling state: The scheduling progress from the PD aspect.

### Updates to Existing Syntax

#### CREATE DATABASE / ALTER DATABASE

The semantics of a `PLACEMENT POLICY` on a database/schema should be similar to the [default character set attribute](https://dev.mysql.com/doc/refman/8.0/en/charset-database.html). For example:

```sql
CREATE DATABASE mydb [DEFAULT] PLACEMENT POLICY=`companystandardpolicy`;
CREATE TABLE t1 (a INT);
ALTER DATABASE mydb [DEFAULT] PLACEMENT POLICY=`companynewpolicy`;
CREATE TABLE t2 (a INT);
CREATE TABLE t3 (a INT) PLACEMENT POLICY=`companystandardpolicy`;
```

* The tables t1 and t3 are created with the policy `companystandardpolicy` and the table t2 is created with `companynewpolicy`.
* The `DATABASE` default only affects tables when they are created and there is no explicit placement policy defined.
* Thus, the inheritance rules only apply when tables are being created, and if the database policy changes this will not update the table values. This differs slightly for table partitions.
* The statement `SHOW CREATE DATABASE` is also available, and will show the [DEFAULT] PLACEMENT POLICY in TiDB feature specific comments (/*T![placement] DEFAULT PLACEMENT POLICY x */)

#### SHOW CREATE TABLE

The output of `SHOW CREATE TABLE` should describe any placement options that are either explicitly specified, or inherited from the default placement from `CREATE DATABASE` / `ALTER DATABASE`. This should be escaped in TiDB feature-specific comment syntax. i.e.

```sql
use test;
CREATE TABLE t1 (a int);
SHOW CREATE TABLE t1;
-->
CREATE TABLE `t1` (
  `a` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE t2 (a int) PLACEMENT POLICY='acdc';
SHOW CREATE TABLE t2;
-->
CREATE TABLE `t2` (
  `a` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`acdc` */;

ALTER DATABASE test DEFAULT PLACEMENT POLICY=`acdc`;
CREATE TABLE t2 (a int);
SHOW CREATE TABLE t3;
-->
CREATE TABLE `t3` (
  `a` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement]  PLACEMENT POLICY=`acdc` */;
```

This helps ensure the highest level of compatibility between both TiDB versions and MySQL.

### Placement Rules Syntax

#### Constraints configuration

`CONSTRAINTS` option in the `CREATE TABLE .. PLACEMENT ..` or `CREATE PLACEMENT POLICY` clauses indicates the label constraints. Data must be placed on the stores whose labels conform to `CONSTRAINTS` constraints. If `CONSTRAINTS` is omitted, it means no label constraint is enforced, thus the replicas can be placed anywhere.

Option `CONSTRAINTS` should be a string and in one of these formats:

- List: `[{+|-}key=value,...]`, e.g. `[+zone=bj,-disk=hdd]`
- Dictionary: `{"{+|-}key=value,...":count,...}`, e.g. `{"+zone=bj,-disk=hdd":1, +zone=sh:2}`

Prefix `+` indicates that data can only be placed on the stores whose labels contain such labels, and `-` indicates that data can’t be placed on the stores whose labels contain such labels. For example, `+zone=sh,+zone=bj` indicates to place data only in `sh` and `bj` zones.

`key` here refers to the label name, and `value` is the label value. The label name should have already been defined in the store configurations. For example, assuming a store has following labels:

```sql
[server]
labels = "zone=bj,rack=rack0,disk=hdd"
```

Then `+zone=bj` matches this store while `+disk=ssd` doesn't.

In the dictionary format, `count` must be specified, which indicates the number of replicas placed on those stores. When the prefix is `-`, the `count` is still meaningful.

For example, `CONSTRAINTS="{+zone=sh:1,-zone=bj:2}"` indicates to place 1 replica in `sh`, 2 replicas in anywhere but `bj`.

In the list format, `count` is not specified. The number of replicas for each constraint is not limited, but the total number of replicas should still conform to the `REPLICAS` option.

For example, `CONSTRAINTS="[+zone=sh,+zone=bj]" REPLICAS=3` indicates to place 3 replicas on either `sh` or `bj`. There may be 2 replicas on `sh` and 1 in `bj`, or 2 in `bj` and 1 in `sh`. It's up to PD.

Label constraints can be implemented by defining `label_constraints` field in PD placement rule configuration. `+` and `-` correspond to property `op`. Specifically, `+` is equivalent to `in` and `-` is equivalent to `notIn`.

For example, `+zone=sh,+zone=bj,-disk=hdd` is equivalent to:

```
"label_constraints": [
	{"key": "zone", "op": "in", "values": ["sh", "bj"]},
	{"key": "disk", "op": "notIn", "values": ["hdd"]}
]
```

Field `location_labels` in PD placement rule configuration is used to isolate replicas to different zones to improve availability. For now, the global configuration can be used as the default `location_labels` for all placement rules defined in SQL, so it's unnecessary to specify it.

`PLACEMENT` also supports adding TiFlash replicas for a table, as statement `ALTER TABLE table_name SET TIFLASH REPLICA count` does. For example:

```sql
ALTER TABLE table_name
	PLACEMENT CONSTRAINTS="[+engine=tiflash]" ROLE=learner REPLICAS=1;
```

The only way to judge whether it’s adding a TiFlash replica is to check the label. If it contains `engine=tiflash`, then it’s adding or removing a TiFlash replica. This logic is conventional in PD for now.

#### Role configuration

`ROLE` in the statement defines the Raft role of the replicas. It must be specified in the statement. There are 4 predefined roles:

- `leader`. Exactly one `leader` is allowed.
- `follower`.
- `voter`. It includes `leader` and `follower`.
- `learner`. It can be either TiFlash or TiKV.

If both `voter` and `follower` are defined in the rules, the replicas of `follower` are not included in the replicas of `voter`. For example:

```sql
ALTER TABLE test
	PLACEMENT CONSTRAINTS="[+zone=bj]" ROLE=follower REPLICAS=2,
	PLACEMENT CONSTRAINTS="[+zone=sh]" ROLE=voter REPLICAS=2;
```

There are 4 replicas for table `test`, 2 of which are in `sh` and 2 are in `bj`.  Leader can only be placed on `sh`.

`ROLE` in the statement is equivalent to field `role` in PD placement rule configuration.

#### Replicas configuration

`REPLICAS` in the statement indicates the replica count of the specified role.

Rules defined on `leader` can omit `REPLICAS`, because the count of leader is always 1.

When all the replica counts are specified in the `CONSTRAINTS` option, `REPLICAS` can also be omitted. For example, `CONSTRAINTS="{+zone=bj:2,+zone=sh:1}", ROLE=voter` indicates that the `REPLICAS` is 3.

When both `REPLICAS` and `count` in `CONSTRAINTS` are specified, it indicates that the other replicas can be placed anywhere. For example, in the case `CONSTRAINTS="{+zone=bj:2,+zone=sh:1}", ROLE=voter, REPLICAS=4`, 2 replicas are in `bj` and 1 in `sh`, and the last replica can be anywhere, including `bj` and `sh`.

When the `CONSTRAINTS` option doesn't contain `count`, `REPLICAS` must be specified. For example, `CONSTRAINTS="[+zone=bj]" ROLE=follower` is vague, as the count of `follower` can not be inferred.

`REPLICAS` in the statement is equivalent to field `count` in PD placement rule configuration.

#### Key range configuration

In PD placement rule configuration, the key range must be specified. Now that `table_name` is specified in the `ALTER TABLE` statement, key range can be inferred.

Typically, key format is in such a format: `t_{table_id}_r_{pk_value}`, where `pk_value` may be `_tidb_rowid` in some cases. `table_id` can be inferred from `table_name`, thus key range is `t_{table_id}_` to `t_{table_id+1}_`.

Similarly, key range of partitions can also be inferred.

#### Policy Validation

When placement policies are specified, they should be validated for correctness:

```sql
CREATE PLACEMENT POLICY `companystandardpolicy` CONSTRAINTS="[+zone=sh]" ROLE=leader REPLICAS=1, CONSTRAINTS="[+zone=sh]" ROLE=follower REPLICAS=1, CONSTRAINTS="[+zone=gz]" ROLE=follower REPLICAS=1;
ALTER PLACEMENT POLICY `companystandardpolicy` CONSTRAINTS="[+zone=sh]" ROLE=leader REPLICAS=1, CONSTRAINTS="[+zone=sh]" ROLE=follower REPLICAS=1, CONSTRAINTS="[+zone=gz]" ROLE=follower REPLICAS=1;
CREATE TABLE t1 (
	id INT NOT NULL PRIMARY KEY,
	b VARCHAR(100)
) PLACEMENT POLICY CONSTRAINTS="[+zone=sh]" ROLE=leader REPLICAS=1, CONSTRAINTS="[+zone=sh]" ROLE=follower REPLICAS=1, CONSTRAINTS="[+zone=gz]" ROLE=follower REPLICAS=1;
ALTER TABLE t1 PLACEMENT CONSTRAINTS="[+zone=sh]" ROLE=leader REPLICAS=1, CONSTRAINTS="[+zone=sh]" ROLE=follower REPLICAS=1, CONSTRAINTS="[+zone=gz]" ROLE=follower REPLICAS=1;
# Only rule #3 applies for these examples
CREATE TABLE t1 (a int) PLACEMENT POLICY `mycompanypolicy`;
ALTER TABLE t1 PLACEMENT POLICY `mycompanypolicy`;
```

1. An impossible policy such as violating raft constraints (2 leaders) should result in an error:
```sql
# error
ALTER TABLE test
	ALTER PLACEMENT POLICY CONSTRAINTS="[+zone=sh]" ROLE=leader REPLICAS=2;
```
2. A policy that has an incorrect number of instances for a raft group (i.e. 1 leader and 1 follower, or just 1 leader and no followers) should be a warning. This allows for some transitional topologies and developer environments.
3. A policy that is impossible based on the current topology (zone=sh and replicas=3, but there is only 1 store in sh) should be a warning. This allows for some transitional topologies.

#### Ambiguous and edge cases

The following two policies are not identical:

```sql
CREATE PLACEMENT POLICY p1 CONSTRAINTS="[+zone=sh]" ROLE=follower REPLICAS=1, CONSTRAINTS="[+zone=gz]" ROLE=follower REPLICAS=1;
CREATE PLACEMENT POLICY p2 CONSTRAINTS="[+zone=sh,+zone=gz]" ROLE=follower REPLICAS=2;
```

This is because "[+zone=sh,+zone=gz]" is interpreted as either sh or gz. It is possible that both replicas could end up in the same zone. This is useful in the case that you want to ensure that `REPLICAS=2` exists in any of a list of zones:

```sql
CREATE PLACEMENT POLICY p2 CONSTRAINTS="[+zone=sh,+zone=gz,+zone=bz]" ROLE=follower REPLICAS=2;
```

However, using "short hand" JSON syntax, the following 2 policies are identical:

```sql
ALTER TABLE test
	PLACEMENT POLICY CONSTRAINTS="{+zone=sh:2,+zone=bj:2}" ROLE=voter REPLICAS=4;

ALTER TABLE test
	PLACEMENT POLICY CONSTRAINTS="[+zone=sh]" ROLE=voter REPLICAS=2,
	PLACEMENT POLICY CONSTRAINTS="[+zone=bj]" ROLE=voter REPLICAS=2;
```

### Additional Semantics

#### Partitioned Tables

Defining placement rules of partitions is expected to be a common use case. This can be useful for both reducing multi-region latency and compliance scenarios.

In Geo-Partitioning, the table must be splitted into partitions, and each partition is placed in specific zones. There are some kinds of partition placement:

* Place all voters on one zone
* Place only leaders on one zone
* Place leaders and half of the followers on one zone

It’s up to users to choose the right solution.

The semantics of partitioning are different to the default database policy. When a table is partitioned, the partitions will by default inherit the same placement policy as the table. This can be overwritten on a per-partition basis:

```sql
CREATE TABLE t1 (id INT, name VARCHAR(50), purchased DATE)
 PARTITION BY RANGE( YEAR(purchased) ) (
  PARTITION p0 VALUES LESS THAN (2000) PLACEMENT POLICY='storeonhdd',
  PARTITION p1 VALUES LESS THAN (2005),
  PARTITION p2 VALUES LESS THAN (2010),
  PARTITION p3 VALUES LESS THAN (2015),
  PARTITION p4 VALUES LESS THAN MAXVLUE PLACEMENT POLICY='storeonfastssd'
 )
PLACEMENT POLICY='companystandardpolicy';
```

In this example, partition `p0` uses the policy `storeonhdd`, partition `p4` uses the policy `storeonfastssd` and the remaining partitions use the policy `companystandardpolicy`. Assuming the following `ALTER TABLE` statement is executed, only partitions `p1`-`p3` will be updated:

```sql
ALTER TABLE t1 PLACEMENT POLICY=`companynewpolicy`;
```

This behavior is inspired by how a `CHARACTER SET` or `COLLATE` attribute applies to a column of a table, and columns will use the character set defined at the table-level by default.

#### Removing placement from a database, table or partition

Placement policy can be removed from an object via the following syntax:
```sql
ALTER DATABASE test [DEFAULT] PLACEMENT SET DEFAULT;
ALTER TABLE t1 PLACEMENT=default;
ALTER TABLE t1 ALTER PARTITION partition_name PLACEMENT=default;
```

In this case the default rules will apply to placement, and the output from `SHOW CREATE TABLE t1` should show no placement information.

The key format of a partitioned table is `t_{partition_id}_r_{pk_value}`. As `partition_id` is part of the key prefix, the key range of a partition is successive. The key range is `t_{partition_id}_` to `t_{partition_id+1}_`.

Placement rules can also be defined on a partitioned table. Because there are multiple key ranges for the table, multiple rules will be generated and sent to PD. When placement rules are defined both on the table and its partitions, the rule priorities described later should be applied.

#### Sequences

Sequence is typically used to allocate ID in `INSERT` statements, so the placement of sequences affects the latency of `INSERT` statements.

However, sequence is typically used with cache enabled, which means very few requests are sent to sequence. So defining placement rules of sequences is not supported for now.

#### DDL on tables

The placement policy is associated with the definition of the table (and visible in `SHOW CREATE TABLE`). Thus, if a table is recovered by `FLASHBACK` or `RECOVER`, it is expected that the previous rules will be restored.

Similarly, `TRUNCATE [TABLE]` does not change the definition of a table. It is expected that as new data is inserted, it will continue to respect placement rules.

#### SHOW DDL jobs

All statements related to placement rules must wait until PD returns. If the execution is interrupted, the job will be cancelled and the DDL will rollback, just like other DDL jobs.

PD schedules regions asynchronously after it returns the message. TiDB can query the progress of scheduling from PD. The progress is observed by executing `SHOW PLACEMENT` (or using `information_schema.placement_rules`) instead of `ADMIN SHOW DDL JOBS`, because the DDL job finishes once PD returns a message.

Ongoing and finished statements can also be queried through `ADMIN SHOW DDL`, `ADMIN SHOW DDL JOBS`, or other similar statements.

### Privilege management

Privilege management is quite straightforward:

* `ALTER [DATABASE|TABLE]` statement requires `Alter` privilege
* `CREATE TABLE` statement requires `Create` privilege
* `information_schema.placement_rules` and `SHOW PLACEMENT` only shows the placement rules on the objects that visible to the current user
* `ADMIN SHOW DDL` requires `Super` privilege
* `CREATE PLACEMENT POLICY`, `DROP PLACEMENT POLICY` and `ALTER PLACEMENT POLICY` require `PLACEMENT_ADMIN` (a new dynamic privilege). This is because these objects have global scope.

## Implementation

### Storing Placement Policies

Placement policies will be stored in a table in the `mysql` schema. The policy name must be globally unique, but the definition of the table is not described in this proposal.

Because the implementation is TiDB specific (does not require any compatibility with MySQL), it is up to the implementer to decide.

### Storing placement rules

PD uses placement rules to schedule data, so a replica of placement rules for _tables and partitions_ must be persistent in PD. 

However, TiDB also uses placement rules in some cases, as discussed in section "Querying placement rules". There are basically 2 ways to achieve this:

- Save the placement rules in table information, which will be duplicated with PD
- Only PD persists the placement rules, while TiDB caches a copy of them

Defining placement rules may fail, but the handling of this is simplified from earlier proposals. Because placement rules are specified in full (i.e. there is no longer syntax for `ALTER TABLE .. ADD PLACEMENT POLICY`), it is safe to automatically retry applying rules.

If the placement rules are stored on both TiKV and PD, the approaches to keep atomicity are as follows:

- Enforce a 2PC protocol on TiKV and PD.
- Store them on TiKV along with a middle state. If TiKV succeeds, then try PD, otherwise rollback it by the middle state. The DDL procedure guarantees the atomicity even if TiDB is down.

As a contrast, if the placement rules are stored only on PD, the approaches to keep atomicity are as follows:

- Write all the placement rules in one ETCD transaction.
- Persist a middle state on TiKV before sending to PD. This middle state acts as undo log.

The comparison shows that both solutions are possible, but storing placement rules only on PD is more practical. To guarantee the transactional characteristics, the easiest way is to write all placement rules in a transaction and define them in serial on the TiDB side.

### Querying placement rules

The scenarios where TiDB queries placement rules are as follows:

1. The optimizer uses placement rules to decide to route cop request to TiKV or TiFlash. It's already implemented and the TiFlash information is written into table information, which is stored on TiKV.
2. It will be probably used in locality-aware features in the future, such as follower-read. Follower-read is always used when TiDB wants to read the nearest replica to reduce multi-region latency. In some distributed databases, it’s implemented by labelling data nodes and selecting the nearest replica according to the labels.
3. Local transactions need to know the binding relationship between Raft leader and region, which is also defined by placement rules.
4. Once a rule is defined on a table, all the subsequent partitions added to the table should also inherit the rule. So the `ADD PARTITION` operation should query the rules on the table. The same is true for creating tables and indices.
5. `SHOW PLACEMENT` statement should output the placement rules correctly.

As placement rules will be queried in case 1, 2 and 3, low latency must be guaranteed. As discussed in section "Storing placement rules", placement rules are only persistent on PD. To lower the latency, the only way is caching the placement rules in TiDB.

Since the cache is created, there must be a way to validate it. Different from region cache, placement rules cache can only be validated each time from PD. There are some ways to work around:

- Update the schema version once a placement rule is changed, just like other DDL. PD broadcasts the latest schema version to all the TiDB instances, and then TiDB instances fetch the newest placement rules from PD. There will be a slight delay for queries before reading the latest placement rules. The side affect is that more transactions will retry since the schema version is changed.
- TiDB queries placement rules from PD periodly. The delay is controllable but not eliminable.
- Once a placement rule is changed, PD broadcasts it to all the TiDB instances. In this approach, schema version is not involved, so transactions are not affected. The delay is not eliminable either.

All the approaches above will result in a delay. Fortunately, for case 1 and 2 above, delay is acceptable. It doesn’t matter much if the optimizer doesn’t perceive the placement rules changement immediately. The worst result is that the latency is relatively high for a short time.

For case 3, although delay is acceptable, but all TiDB instances must be always consistent on the placement rules. To achieve this goal, schema version needs to be updated, thus transactions with old placement rules will fail when committed.

For case 4 and 5, delay is not acceptable. Once the placement rules are written successfully, subsequent DDL statements should fetch the latest placement rules to gaurantee linearizability. Now that schema version is changed and the latest placement rules are broadcast to all the TiDB instances immediately, delay is eliminable. 

Once the schema version is changed, all TiDB instances recognize the object ID and fetch placement rules from PD, rather than TiKV.

To query the placement rules on a specified object, the object ID should be written to the placement rules, or it can be inferred from other fields. Now that `id` contains the object ID, TiDB can decode the object ID from it. See section "Building placement rules" for details.

### DDL procedures

Defining placement rules is a type of DDL, so it's natural to implement it in a typical DDL procedure. But it differs from other DDL in that it writes to PD in addition to TiKV.

The fact that the DDL procedure in TiDB is mature helps to achieve some features of defining placement rules:

- Placement rules are defined in serial as there's only one DDL owner at the same time
- DDL is capable of disaster recovery as the middle states are persistent in TiKV
- DDL is rollbackable as the middle states can transform from one to another
- Updating schema version guarantees all active transactions are based on the same version of placement rules

### Building placement rules

There needs a way to map the placement rules in SQL to PD placement rule configuration. Most of the fields are discussed above, so this part focuses on `group_id`, `id`, `start_key` and `end_key`.

`group_id` is used to identify the source of the placement rules, so `group_id` is `tidb`.

`ALTER PLACEMENT POLICY` and `DROP PLACEMENT POLICY` need to find the rules of a specified object efficiently. It can be achieved by encoding the object ID in `id`.

However, an object may have multiple rules for a single role. For example:

```sql
ALTER TABLE t
	PLACEMENT CONSTRAINTS="{+zone=bj:2,+zone=sh:1}" ROLE=voter;
```

It needs 2 placement rules for `voter` in the PD placement rule configuration, because each rule can only specify one `count`. To make `id` unique, a unique identifier must be appended to `id`. DDL job ID plus an index in the job is a good choice.

Take the case above for example, assuming the table ID of `t` is 100, the ID of the DDL job executing this statement is 200, then `id` of the placement rules are `100-200-1` and `100-200-2`.

The prefix of `id` is in such a format:

* Database: database id
* Table: table id
* Partition: partition id
* Unpartitioned index: the concatenation of table id and index id, e.g. `100_1`
* Partitioned index: the concatenation of partition id and index id

To query all the placement rules for one object, PD looks for all the `id` with a specific prefix.

As all placement rules are mapped to PD placement rule configurations, `start_key` and `end_key` must be generated for each object. However, databases and partitioned tables have no key ranges, so the only way is to generate a key range with no actual records.

As database IDs are all globally unique, it's fine to replace table ID with database ID in the key range. For example, assuming the database ID is 100, then the string format of its key range is:

- `start_key`: `t_{database_id}_`
- `end_key`: `t_{database_id+1}_`

It's same for partitioned tables.


### Rule priorities

Tables only inherit rules from databases when they are created, and the value is saved in the meta data. Thus, the rules of priorities are simplified from an earlier version of this proposal (and are more inline with how character sets are inherited).

The only rules are that indexes and partitions inherit the rules of tables. Partitions can explicitly overwrite the placement policy, but indexes currently can not.

Thus the priority is:

```
db --> table (on create only; and only when placement not explicitly specified)
unpartitioned table --> index
partitioned table --> partition (can be overwritten) --> index
```

For example:

1. At the beginning, all data is placed based on the default placement rules.
2. When a placement rule is added on table `t`, all data on `t` is placed based on the rule.
3. When a placement rule is added on partition `p0` of `t`, all data on `p0` is placed based on the rule of `p0`, but other partitions stay still.
4. When the placement rule on `p0` is removed, data on `p0` is placed based on the rule of `t`, just like other partitions.

Rules priorities are checked when a placement rule is added, altered, or dropped.

Rule priorities can be implemented by fields `index` and `override` in the PD placement rule configuration. `override` is alway enabled, and `index` stands for the priority. Rules with higher `index` will overwrite the rules with lower `index` and same key range, but rules with same `index` don't overwrite each other, they just accumulate.

Specifically, `index` is in such a format:

* `index` of default placement rules is 0
* `index` of database placement rules is 1
* `index` of table placement rules is 2
* `index` of partition placement rules is 3
* `index` of index placement rules is 4

In such a way, the most granular rule always works.

## Impact & Risks

1. The largest risk is designing a set of SQL statements that are sufficiently flexible to handle major use cases, but not too flexible that misconfiguration is likely when the user has compliance requirements. The following design decisions have been made to mitigate this risk:
  - The DDL statement to `ALTER TABLE t1 ADD PLACEMENT` has been removed from the proposal.
  - The DDL statement `CREATE PLACEMENT POLICY` has been added (allowing common configurations to be saved).
  - Configuring placement rules for indexes is no longer supported (we can add it once global indexes are added).
  - The inheritance rules have been simplified to match character set/collation inheritance.
2. There is a risk that we do not fully understand complaince requirements (see unresolved questions), or that the substaintial effort required to achieve compliance with DDL, internal SQL and DML.
3. Related to the above point, there is risk that the implementation of compliance requirements has significant burden on multiple teams, including ecosystem tools (Backup, CDC, DM). Even tools such as dumpling should ideally be able to backup placement rules in logical form.
4. There is some risk that a common use case can not be expressed in `CONSTRAINT` syntax, leading to complicated scenarios where users still need to express placement by using PD directly. Ideally, we can recommend users use SQL rules exclusively.
5. Many other features in TiDB are in development, some of which may influence placement rules. Clustered index affects the key format of primary index. Fortunately, the prefix of key range is untouched. Global secondary index largely affect the placement rules of partitioned tables. The key range of one global secondary index is not successive, so if it's necessary to define placement rules on the index, multiple rules should be generated in the PD. But for now, there's no such scenario.


## Investigation & Alternatives

For investigation, we looked at the implementation of placement rules in various datases (CockroachDB, Yugabyte, OceanBase).

The idea of using a `PLACEMENT POLICY` was inspired by how OceanBase has Placement Groups, which are then applied to tables. But the usage as proposed here is optional, which allows for more flexibility for casual cases. The idea of using a Placement Group can also be seen as similar to using a "tablespace" in a traditional database, but it's not completely the same since the choice is less binary (constraints allow the placement of roles for leaders, followers, learners etc.)

CockroachDB does not look to have something directly comparable to `PLACEMENT POLICY`, but it does have the ability to specify "replication zones" for "system ranges" such as default, meta, liveness, system, timeseries. Before dropping `ALTER TABLE t1 ADD PLACEMENT` from this proposal, it was investigated the CockroachDB does not support this syntax, presumably for simplification and minimising similar risks of misconfiguration.

## Unresolved Questions

### Compliance Requirements

For compliance use-cases, it is clear that data at rest should reside within a geographic region. What is not clear is which (if any) circumstances data in transit is permitted to leave. There are several known issues which will need to be resolved:

* **Backup**: The `BACKUP` SQL command (and br) accept a single location such as `s3://bucket-location/my-backup` to centralize the backup. This centralization likely violates compliance requirements (assuming there are >=2 requirements that conflict on a single cluster). Backing up segments of data individually is both an inconsistent backup, and likely to result in misconfiguration which violates compliance rules. Users have a reasonable expectation of backup integration with compliance placement rules.
* **CDC**: Similar to `BACKUP`, it should not be possible to subscribe to changes for data in a different jurisdiction.
* **DDL**: The current implementation of DDL uses a centralized DDL owner which reads data from relevant tables and performs operations such as building indexes. The _read_ operation may violate compliance rules.
* **Internal SQL**: Similar to DDL, several centralized background tasks, such as updating histograms/statistics need to be able to _read_ the data.
* **DML**: It is not yet known which restrictions need to be placed on user queries. For example, if a poorly written user-query does not clearly target the `USA` partition when reading user data, should it generate an error because the `EUROPE` partition needs to be read in order for the semantics of the query to be correct? This may cause problems in development and integration environments if the restrictions can not be distilled into environments with smaller topologies.

### Behaviors

* The proposal says that `DROP PLACEMENT POLICY` should error if the policy is in use by any objects. This should be discussed, because it is not a common behavior in MySQL and may create some annoying debugging scenarios?
* Are there risks to logical restore if creating a table with a non-existent `PLACEMENT POLICY` fails? This is the current proposal, but it can be changed to warning if it is an issue. If it can't be applied when creating the table, is the placement policy meta data dropped (and won't be picked up if that policy is created later?). This is regrettable, but seems like the most clear behavior because showing a non-defined `PLACEMENT POLICY` in `SHOW CREATE TABLE` is problematic.

## Changelog

* 2021-07-12:
  - Converted proposal to use the new template for technical designs.
  - Removed the syntax `ALTER TABLE t1 ADD PLACEMENT POLICY` due to ambiguity in some cases, and risk of misconfiguration for compliance cases.
  - Added `CREATE PLACEMENT POLICY` syntax.
  - Renamed `ALTER TABLE t1 ALTER PLACEMENT POLICY` to `ALTER TABLE t1 PLACEMENT` to bring syntax inline with other atomic changes, such as `ALTER TABLE t1 CHARACTER SET x`. The usage of `PLACEMENT POLICY` now refers to a placement policy defined from `CREATE PLACEMENT POLICY` (other commands like `SHOW PLACEMENT POLICY` are also updated to `SHOW PLACEMENT`).
  - Remove index as a placement option (we can add it again once global indexes for temporary tables exist, but it is not strictly required for an MVP).
  - Made implementation in `CREATE TABLE` and `SHOW CREATE TABLE` required, to support the compliance use-case.
