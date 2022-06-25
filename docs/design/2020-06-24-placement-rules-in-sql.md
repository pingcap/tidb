# Defining placement rules in SQL

- Author(s):     [djshow832](https://github.com/djshow832) (Ming Zhang), [morgo](https://github.com/morgo) (Morgan Tocker)
- Last updated:  2022-01-06
- Discussion PR: https://github.com/pingcap/tidb/pull/26221
- Tracking Issue: https://github.com/pingcap/tidb/issues/18030
- Original Document (Chinese): https://docs.google.com/document/d/18Kdhi90dv33muF9k_VAIccNLeGf-DdQyUc8JlWF9Gok

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
    * [New Syntax Overview](#new-syntax-overview)
    * [Updates to Existing Syntax](#updates-to-existing-syntax)
    * [Placement Rules Syntax](#placement-rules-syntax)
    * [Additional Semantics](#additional-semantics)
    * [Privilege management](#privilege-management)
* [Implementation](#implementation)
    * [Storing Placement Policies](#storing-placement-policies)
    * [Storage Consistency](#storage-consistency)
    * [Querying Placement Rules](#querying-placement-rules)
    * [Building placement rules](#building-placement-rules)
    * [Rule priorities](#rule-priorities)
* [Examples](#examples)
    * [Optimization: Follower read in every region](#optimization-follower-read-in-every-region)
    * [Optimization: Latest data on SSD](#optimization-latest-data-on-ssd)
    * [Optimization: Multi-tenancy / control of shared resources](#optimization-multi-tenancy--control-of-shared-resources)
    * [Compliance: User data needs geographic split](#compliance-user-data-needs-geographic-split)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
    * [Known Limitations](#known-limitations)
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

The new syntax is based on a `PLACEMENT POLICY` statement and the ability to assign a placement policy to a schema, table or partition:

```sql
CREATE PLACEMENT POLICY policyName [placementOptions];
CREATE TABLE t1 (a INT) PLACEMENT POLICY=policyName;
```

An earlier, experimental, version of this proposal featured the ability to assign placementOptions directly to objects (tables or partitions). **This is no longer supported**.

A `PLACEMENT POLICY` allows administrators to better keep track of usage. This can be seen as similar to how complex environments will use `ROLES` for management instead of directly assigning privileges to users. For example:

```sql
CREATE PLACEMENT POLICY `standardplacement` PRIMARY_REGION="us-east-1" REGIONS="us-east-1,us-east-2"
```

In this context, "PRIMARY_REGION" and "REGIONS" are syntactic sugar which map to the label `region`. The following labels have special reserved words (the plural is used in contexts such as followers where multiple is possible):
- `host` and `hosts`: expected to be the same physical machine or hypervisor.
- `rack` and `racks`: similar to host; a group of machines that are physically close together and may suffer from many of the same failures.
- `zone` and `zones`: similar to an AWS zone; much larger degree of blast radius isolation from a rack, but still vulnerable to issues such as a natural disaster.
- `region` and `regions`: expected to be distributed far enough apart that there is isolation from disasters.

To use additional labels not in this list, see "Advanced Placement" below.

Creating a new table with the `PLACEMENT POLICY` assigned:

```sql
CREATE TABLE t1 (
	id INT NOT NULL PRIMARY KEY,
	b VARCHAR(100)
) PLACEMENT POLICY=`standardplacement`;
```

Adding `PLACEMENT POLICY` to an existing table:

```sql
CREATE TABLE t2 (
	id INT NOT NULL PRIMARY KEY,
	b VARCHAR(100)
);
ALTER TABLE t2 PLACEMENT POLICY=`standardplacement`;
```

Behavior notes:

- `CREATE` or `ALTER` and specifying a `PLACEMENT POLICY` that does not exist results in an error: placement policy 'x' is not defined (see "Skipping Policy Validation" below)
- Placement policies are globally unique names. Thus, a policy named `companyplacementpolicy` can apply to the db `test` as well as `userdb`. The namespace does not overlap with other DB objects.
- Placement Policy names are case insensitive, and follow the same rules as tables/other identifiers for length (64 chars) and special characters.
- The full placement policy can be seen with `SHOW CREATE PLACEMENT POLICY x`. This is useful for shorthand usage by DBAs, and consistent with other database objects.
- It is possible to update the definition of a placement policy with `ALTER PLACEMENT POLICY x LEADER_CONSTRAINTS="[+region=us-east-1]" FOLLOWER_CONSTRAINTS="{+region=us-east-1: 1,+region=us-east-2: 1}";` This is modeled on the statement `ALTER VIEW` (where the view needs to be redefined). When `ALTER PLACEMENT POLICY x` is executed, all tables that use this placement policy will need to be updated in PD.
- The statement `RENAME PLACEMENT POLICY x TO y` renames a placement policy. The `SHOW CREATE TABLE` output of all databases, tables and partitions that used this placement policy should be updated to the new name.

#### Advanced Placement

The syntax `PRIMARY_REGION="us-east-1" REGIONS="us-east-1,us-east-2"` is the recommended syntax for users, but it only works for supported labels.
Consider the case where a user wants to allocate placement based on the label `disk`. Using constraints is required:

```sql
ALTER PLACEMENT POLICY `standardplacement` CONSTRAINTS="[+disk=ssd]";
```

When the constraints are specified as a dictionary (`{}`) numeric counts for each region must be specified and `FOLLOWERS=n` is disallowed:

```sql
ALTER PLACEMENT POLICY `standardplacement3` LEADER_CONSTRAINTS="[+region=us-east-1]" FOLLOWER_CONSTRAINTS="{+region=us-east-1: 1,+region=us-east-2: 1,+region=us-west-1: 1}";
```

The placement policy above has 3 followers: 1 each in the regions us-east-1, us-east-2 and us-west-1.

Behavior notes:

* Advanced placement is available in the context of `CREATE|ALTER PLACEMENT POLICY`, i.e. the usage of all placement syntax is expected to be the same in all contexts.
* It is possible to set `CONSTRAINTS`, `LEADER_CONSTRAINTS`, `FOLLOWER_CONSTRAINTS` and `LEARNER_CONSTRAINTS`. Assuming that both `CONSTRAINTS` and `FOLLOWER_CONSTRAINTS` are specified, the conditions are "AND"ed together.
* See "Constraints configuration" below for a full set of rules and syntax for constraints.

#### Metadata commands

Besides `SHOW CREATE PLACEMENT POLICY x`, it should be possible to summarize all placement for a database system. This is beneficial for compliance scenarios.

##### information_schema.placement_policies

A new system table `information_schema.placement_policies` is added to view all placement policies. The table definition is as follows:

```sql
+----------------------+---------------+------+------+---------+-------+
| Field                | Type          | Null | Key  | Default | Extra |
+----------------------+---------------+------+------+---------+-------+
| POLICY_ID            | bigint(64)    | NO   |      | NULL    |       |
| CATALOG_NAME         | varchar(512)  | NO   |      | NULL    |       |
| POLICY_NAME          | varchar(64)   | NO   |      | NULL    |       |
| PRIMARY_REGION       | varchar(1024) | YES  |      | NULL    |       |
| REGIONS              | varchar(1024) | YES  |      | NULL    |       |
| CONSTRAINTS          | varchar(1024) | YES  |      | NULL    |       |
| LEADER_CONSTRAINTS   | varchar(1024) | YES  |      | NULL    |       |
| FOLLOWER_CONSTRAINTS | varchar(1024) | YES  |      | NULL    |       |
| LEARNER_CONSTRAINTS  | varchar(1024) | YES  |      | NULL    |       |
| SCHEDULE             | varchar(20)   | YES  |      | NULL    |       |
| FOLLOWERS            | bigint(64)    | YES  |      | NULL    |       |
| LEARNERS             | bigint(64)    | YES  |      | NULL    |       |
+----------------------+---------------+------+------+---------+-------+
```

##### information_schema.tables and information_schema.partitions

The information_schema tables for `tables` and `partitions` should be modified to have an additional field for `tidb_placement_policy_name`:

```golang
{name: "TIDB_PLACEMENT_POLICY_NAME", tp: mysql.TypeVarchar, size: 64},
```

This helps make the information match what is available in `SHOW CREATE TABLE`, but in a structured format.

##### SHOW PLACEMENT

In addition to `information_schema.tables` and `information_schema.partitions`, a summary of the placement for a schema or table can be provided with `SHOW PLACEMENT`. The `placement` column in `SHOW PLACEMENT` shows the canonical version of the rules for an object (table or partition) with placement policies expanded. This is helpful for returning the canonical set of rules (which can get complicated since partitions inherit placement from tables, but can also be manually specified):

```sql
SHOW PLACEMENT FOR [{DATABASE | SCHEMA} schema_name] [TABLE table_name [PARTITION partition_name]];
```

Examples:

```sql
SHOW PLACEMENT;
+----------------------------+----------------------------------------------------------------------+------------------+
| target                     | placement                                                            | scheduling_state |
+----------------------------+----------------------------------------------------------------------+------------------+
| POLICY system              | PRIMARY_REGION="us-east-1" REGIONS="us-east-1,us-east-2" FOLLOWERS=4 | NULL             |
| POLICY default             | PRIMARY_REGION="us-east-1" REGIONS="us-east-1,us-east-2"             | NULL             |
| DATABASE test              | PRIMARY_REGION="us-east-1" REGIONS="us-east-1,us-east-2"             | SCHEDULED        |
| TABLE test.t1              | PRIMARY_REGION="us-east-1" REGIONS="us-east-1,us-east-2"             | SCHEDULED        |
| TABLE test.t1 PARTITION p1 | PRIMARY_REGION="us-east-1" REGIONS="us-east-1,us-east-2"             | INPROGRESS       |
+----------------------------+----------------------------------------------------------------------+------------------+
5 rows in set (0.00 sec)

SHOW PLACEMENT LIKE 'POLICY%';
+----------------------------+----------------------------------------------------------------------+------------------+
| target                     | placement                                                            | scheduling_state |
+----------------------------+----------------------------------------------------------------------+------------------+
| POLICY system              | PRIMARY_REGION="us-east-1" REGIONS="us-east-1,us-east-2" FOLLOWERS=4 | NULL             |
| POLICY default             | PRIMARY_REGION="us-east-1" REGIONS="us-east-1,us-east-2"             | NULL             |
+----------------------------+----------------------------------------------------------------------+------------------+
2 rows in set (0.00 sec)

SHOW PLACEMENT FOR DATABASE test;
+----------------------------+----------------------------------------------------------------------+------------------+
| target                     | placement                                                            | scheduling_state |
+----------------------------+----------------------------------------------------------------------+------------------+
| DATABASE test              | PRIMARY_REGION="us-east-1" REGIONS="us-east-1,us-east-2"             | SCHEDULED        |
| TABLE test.t1              | PRIMARY_REGION="us-east-1" REGIONS="us-east-1,us-east-2"             | SCHEDULED        |
| TABLE test.t1 PARTITION p1 | PRIMARY_REGION="us-east-1" REGIONS="us-east-1,us-east-2"             | INPROGRESS       |
+----------------------------+----------------------------------------------------------------------+------------------+
3 rows in set (0.00 sec)
```

TiDB will automatically find the effective rule based on the rule priorities.

This statement outputs at most 1 line. For example, when querying a table, only the placement rule defined on the table itself is shown, and the partitions in it will not be shown.

The output of this statement contains these fields:

* Target: The object queried. It can be a database, table, partition, or index.
  * For policies, it is shown as the policy name.
  * For database, it is shown in the format `DATABASE database_name`
  * For table, it is shown in the format `TABLE database_name.table_name`
  * For partition, it is shown in the format `TABLE database_name.table_name PARTITION partition_name`
* Placement: An equivalent `ALTER` statement on `target` that defines the placement rule.
* Scheduling state: The scheduling progress from the PD aspect. It is always `NULL` for policies.

For finding the current use of a placement policy, the following syntax can be used:

```sql
SHOW PLACEMENT LIKE 'POLICY standardpol%';
```

This will match for `PLACEMENT POLICY` names such as `standardpolicy`.

The `scheduling_state` is one of `SCHEDULED`, `INPROGRESS`, or `PENDING`. `PENDING` means the placement rule is semantically valid, but might not be able to be scheduled based on the current topology of the cluster.

### Updates to Existing Syntax

#### CREATE DATABASE / ALTER DATABASE

The semantics of a `PLACEMENT POLICY` on a database/schema should be similar to the [default character set attribute](https://dev.mysql.com/doc/refman/8.0/en/charset-database.html). For example:

```sql
CREATE DATABASE mydb [DEFAULT] PLACEMENT POLICY=`companystandardpolicy`;
CREATE TABLE mydb.t1 (a INT);
ALTER DATABASE mydb [DEFAULT] PLACEMENT POLICY=`companynewpolicy`;
CREATE TABLE mydb.t2 (a INT);
CREATE TABLE mydb.t3 (a INT) PLACEMENT POLICY=`companystandardpolicy`;
```

* The tables t1 and t3 are created with the policy `companystandardpolicy` and the table t2 is created with `companynewpolicy`.
* The `DATABASE` default only affects tables when they are created and there is no explicit placement policy defined.
* Thus, the inheritance rules only apply when tables are being created, and if the database policy changes this will not update the table values. This differs slightly for table partitions.
* The statement `SHOW CREATE DATABASE` is also available, and will show the [DEFAULT] PLACEMENT POLICY in TiDB feature specific comments (/*T![placement] DEFAULT PLACEMENT POLICY x */)

#### SHOW CREATE TABLE

The output of `SHOW CREATE TABLE` should escape placement policies in TiDB feature-specific comment syntax. i.e.

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
CREATE TABLE t3 (a int);
SHOW CREATE TABLE t3;
-->
CREATE TABLE `t3` (
  `a` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement]  PLACEMENT POLICY=`acdc` */;

```

This helps ensure the highest level of compatibility between both TiDB versions and MySQL.

### Placement Rules Syntax

#### Constraints configuration

The constraints syntax (described as "Advanced Placement" above) allows placement to be enforced based on labels. If CONSTRAINTS are omitted, it means no label constraint is enforced, thus the replicas can be placed anywhere.

`CONSTRAINTS` should be a string and in one of these formats:

- List: `[{+|-}key=value,...]`, e.g. `[+region=us-east-1,-disk=hdd]`
- Dictionary: `{"{+|-}key=value,...":count,...}`, e.g. `{"+region=us-east-1,-disk=hdd": 1, +region=us-east-2: 2}`

The prefix `+` indicates that data can only be placed on the stores whose labels contain such labels, and `-` indicates that data can’t be placed on the stores whose labels contain such labels. For example, `+region=us-east-1,+region=us-east-2` indicates to place data only in `us-east-1` and `us-east-2` regions.

The `key` here refers to the label name, and `value` is the label value. The label name should have already been defined in the store configurations. For example, assuming a store has following labels:

```sql
[server]
labels = "region=us-east-1,rack=rack0,disk=hdd"
```

Then `+region=us-east-1` matches this store while `+disk=ssd` doesn't.

In the dictionary format, `count` must be specified, which indicates a quantity which must match. When the prefix is `-`, the `count` is still meaningful.

For example, `FOLLOWER_CONSTRAINTS="{+region=us-east-1: 1,-region=us-east-2: 2}"` indicates to place at least 1 follower in `us-east-1`, 2 replicas in anywhere but `us-east-2` (by definition this will be `us-east-1` since there are no other regions available).

In the list format, `count` is not specified. The number of followers for each constraint is not limited, but the total number of instances should still conform to the definition.

For example, `FOLLOWER_CONSTRAINTS="[+region=us-east-1]" FOLLOWERS=3` indicates to place 3 followers on either `us-east-1`.

Label constraints can be implemented by defining `label_constraints` field in PD placement rule configuration. `+` and `-` correspond to property `op`. Specifically, `+` is equivalent to `in` and `-` is equivalent to `notIn`.

For example, `+region=us-east-1,-disk=hdd` is equivalent to:

```
"label_constraints": [
	{"key": "region", "op": "in", "values": ["us-east-1"]},
	{"key": "disk", "op": "notIn", "values": ["hdd"]}
]
```

Field `location_labels` in PD placement rule configuration is used to isolate replicas to different zones to improve availability. For now, the global configuration can be used as the default `location_labels` for all placement rules defined in SQL, so it's unnecessary to specify it.

#### Specifying role count

The roles `FOLLOWERS` and `LEARNERS` also support an optional count in *list* format. For example:

```sql
CREATE PLACEMENT POLICY `standardplacement1` PRIMARY_REGION="us-east-1" REGIONS="us-east-1,us-east-2" FOLLOWERS=4;
CREATE PLACEMENT POLICY `standardplacement2` LEADER_CONSTRAINTS="[+region=us-east-1]"  FOLLOWER_CONSTRAINTS="[+region=us-east-2]" FOLLOWERS=4;
```

If the constraints is specified as a dictionary (e.g. `{"+region=us-east-1":1}`) the count is not applicable and an error is returned:

```sql
FOLLOWER_CONSTRAINTS="{+region=us-east-1: 1,-region=us-east-2: 2}" FOLLOWERS=3; -- technically accurate, but an error
FOLLOWER_CONSTRAINTS="{+region=us-east-1: 1,-region=us-east-2: 2}" FOLLOWERS=2; -- an error
```

For dictionary format, the count is inferred by the constraint. The following constraint creates 3 followers:

```sql
FOLLOWER_CONSTRAINTS="{+region=us-east-1: 1,-region=us-east-2: 2}"
```

Explanation:
- 1 follower in `us-east-1`
- 2 followers not in `us-east-2`

#### Schedule Property

When using either the syntactic sugar or list format for placement rules, PD is free to schedule followers/leaders wherever it decides. For example:

```sql
CREATE PLACEMENT POLICY `standardplacement1` PRIMARY_REGION="us-east-1" REGIONS="us-east-1,us-east-2" FOLLOWERS=4;
```

- Are each of the followers split equally in us-east-1 and us-east-2?
- Could the majority of followers be placed in us-east-1? That would ensure fast quorum but reduced fault tolerance.

To address the ambiguity the concept of a "schedule" is introduced, which is the name for a strategy that PD uses to determine where to place instances. For an example:

```sql
CREATE PLACEMENT POLICY `standardplacement1` PRIMARY_REGION="us-east-1" REGIONS="us-east-1,us-east-2" FOLLOWERS=4 SCHEDULE="EVEN";
```

The following `SCHEDULE` options are available:

* `EVEN`: Followers will be balanced based on the value of the "region" label. So for example if `us-east-1` has 15 stores and `us-east-2` has 5, it doesn't matter. 2 stores will be placed in each. This is recommended for increased fault tolerance.
* `MAJORITY_IN_PRIMARY`: As many followers as required to achieve quorum will be placed in the primary region. The remaining followers will be scheduled in the remaining secondary regions.

#### Key range configuration

In PD placement rule implementation, the key range must be specified. Now that `table_name` is specified in the `ALTER TABLE` statement, key range can be inferred.

Typically, key format is in such a format: `t{table_id}_r{pk_value}`, where `pk_value` may be `_tidb_rowid` in some cases. `table_id` can be inferred from `table_name`, thus the key range is `t{table_id}_` to `t{table_id+1}_`.

Similarly, the key range of partitions can also be inferred.

#### Policy Validation

When placement policies are specified, they should be validated for correctness:

1. The `FOLLOWERS` count should respect raft quorum expectations. The default is `2` (which creates raft groups of 3). If the number is odd, it could lead to split brain scenarios, so a warning should be issued. Warnings should also be issued for a count less than 2 (this might be useful for development environments, so an error is not returned)
2. A policy that is impossible based on the current topology (region=us-east-1 and followers=2, but there is only 1 store in us-east-1) should be a warning. This allows for some transitional topologies.
3. If the constraints are specified as a dictionary, specifying the count (i.e. `FOLLOWERS=n`) is prohibited.

#### Skipping Placement Options

It should be possible to skip placement options. This can be seen as similar to skipping foreign key checks, which is often used by logical dumpers:

```sql
SET tidb_placement_mode='IGNORE';
CREATE TABLE t3 (a int) PLACEMENT POLICY `mycompanypolicy`;
```

If a table is imported when `tidb_placement_mode='IGNORE'`, and the placement policy does not validate, then the same rules of fallback apply as in the case `DROP PLACEMENT POLICY` (where policy is still in use).

The default value for `tidb_placement_mode` is `STRICT`. The option is an enum, and in future we may add support for a `WARN` mode.

#### Ambiguous and edge cases

The following two policies are not identical:

```sql
CREATE PLACEMENT POLICY p1 FOLLOWER_CONSTRAINTS="[+region=us-east-1,+region=us-east-2]" FOLLOWERS=2;
CREATE PLACEMENT POLICY p2 FOLLOWER_CONSTRAINTS="{+region=us-east-1: 1,-region=us-east-2: 1}";
```

This is because p2 explicitly requires a follower count of 1 per region, whereas p1 allows for 2 in any of the above (see "Schedule Property" above for an explanation).

### Additional Semantics

#### Partitioned Tables

The key format of a partitioned table is `t{partition_id}_r{pk_value}`. As `partition_id` is part of the key prefix, the key range of a partition is successive. The key range is `t{partition_id}_` to `t{partition_id+1}_`.

Defining placement rules of partitions is expected to be a common use case and is useful for both reducing multi-region latency and compliance scenarios. Because there are multiple key ranges for the table, multiple rules will be generated and sent to PD.

In Geo-Partitioning, the table must be splitted into partitions, and each partition is placed in specific zones. There are some kinds of partition placement:

* Place only leaders on one zone
* Place leaders and half of the followers on one zone

It’s up to users to choose the right solution.

The semantics of partitioning are different to the default database policy. When a table is partitioned, the partitions will by default inherit the same placement policy as the table. This can be overwritten on a per-partition basis:

```sql
CREATE TABLE t1 (id INT, name VARCHAR(50), purchased DATE)
 PLACEMENT POLICY='companystandardpolicy'
 PARTITION BY RANGE( YEAR(purchased) ) (
  PARTITION p0 VALUES LESS THAN (2000) PLACEMENT POLICY='storeonhdd',
  PARTITION p1 VALUES LESS THAN (2005),
  PARTITION p2 VALUES LESS THAN (2010),
  PARTITION p3 VALUES LESS THAN (2015),
  PARTITION p4 VALUES LESS THAN MAXVALUE PLACEMENT POLICY='storeonfastssd'
 );
```

In this example, partition `p0` uses the policy `storeonhdd`, partition `p4` uses the policy `storeonfastssd` and the remaining partitions use the policy `companystandardpolicy`. Assuming the following `ALTER TABLE` statement is executed, only partitions `p1`-`p3` will be updated:

```sql
ALTER TABLE t1 PLACEMENT POLICY=`companynewpolicy`;
```

For the syntax:
```sql
ALTER TABLE pt
    EXCHANGE PARTITION p
    WITH TABLE nt;
```	

If `nt` has placement rules associated with it, they will be retained when it becomes a partition of table `pt`. However, if no placement rules have been specified, then the rules of the table `pt` will be used. This helps protect against the case that a partition may need to have "default" placement rules, but default does not mean what the table uses (the output of `SHOW CREATE TABLE` would appear ambiguous). When the partition `p` is converted to table `nt`, it will continue to use the rules it had as a partition (either explicitly listed for the partition or the default for the table).

This behavior is inspired by how a `CHARACTER SET` or `COLLATE` attribute applies to a column of a table, and columns will use the character set defined at the table-level by default.

#### Removing placement from a database, table or partition

Placement policy can be removed from an object via the following syntax:

```sql
ALTER DATABASE test [DEFAULT] PLACEMENT POLICY SET DEFAULT; -- standard syntax for ALTER DATABASE
ALTER DATABASE test [DEFAULT] PLACEMENT POLICY=default; -- alternative
ALTER TABLE t1 PLACEMENT POLICY=default;
ALTER TABLE t1 PARTITION partition_name PLACEMENT POLICY=default;
```

In this case the default rules will apply to placement, and the output from `SHOW CREATE TABLE t1` should show no placement information. Thus, setting `PLACEMENT POLICY=default` must reset the following `table_options`:
- `FOLLOWERS=n`
- `LEARNERS=n`
- `PRIMARY REGION`
- `REGIONS`
- `SCHEDULE`
- `CONSTRAINTS`
- `FOLLOWER_CONSTRAINTS`
- `LEARNER_CONSTRAINTS`
- `PLACEMENT POLICY`

For a more complex rule using partitions, consider the following example:

```sql
ALTER TABLE t1 PARTITION p0 PLACEMENT POLICY="acdc";
--> 
CREATE TABLE t1 (id INT, name VARCHAR(50), purchased DATE)
 PARTITION BY RANGE( YEAR(purchased) ) (
  PARTITION p0 VALUES LESS THAN (2000) PLACEMENT POLICY="acdc",
  PARTITION p1 VALUES LESS THAN (2005)
 );

ALTER TABLE t1 PLACEMENT POLICY="xyz";
--> 
CREATE TABLE t1 (id INT, name VARCHAR(50), purchased DATE)
 PLACEMENT POLICY="xyz"
 PARTITION BY RANGE( YEAR(purchased) ) (
  PARTITION p0 VALUES LESS THAN (2000) PLACEMENT POLICY="acdc",
  PARTITION p1 VALUES LESS THAN (2005)
 );

ALTER TABLE t1 PARTITION p0 PLACEMENT POLICY=DEFAULT;
--> 
CREATE TABLE t1 (id INT, name VARCHAR(50), purchased DATE)
 PLACEMENT POLICY="xyz"
 PARTITION BY RANGE( YEAR(purchased) ) (
  PARTITION p0 VALUES LESS THAN (2000),
  PARTITION p1 VALUES LESS THAN (2005)
 );
 
```

The behavior above is described as `ALTER TABLE t1 PARTITION p0 PLACEMENT POLICY=DEFAULT` resets the placement of the partition `p0` to be inherited from the table `t1`.

#### Sequences

Sequence is typically used to allocate ID in `INSERT` statements, so the placement of sequences affects the latency of `INSERT` statements.

However, sequence is typically used with cache enabled, which means very few requests are sent to sequence. So defining placement rules of sequences is not supported for now.

#### DDL on tables

The placement policy is associated with the definition of the table (and visible in `SHOW CREATE TABLE`). Thus, if a table is recovered by `FLASHBACK` or `RECOVER`, it is expected that the previous rules will be restored.

Similarly, `TRUNCATE [TABLE]` does not change the definition of a table. It is expected that as new data is inserted, it will continue to respect placement rules.

#### SHOW DDL jobs

Because `CREATE TABLE` and `ALTER TABLE` are DDL, changes to placement are also considered DDL and are visible via `ADMIN SHOW DDL JOBS`.

The fact that the DDL procedure in TiDB is mature helps to achieve some features of defining placement rules:

- Placement rules are defined in serial as there's only one DDL owner at the same time
- DDL is capable of disaster recovery as the middle states are persistent in TiKV
- DDL is rollbackable as the middle states can transform from one to another
- Updating schema version guarantees all active transactions are based on the same version of placement rules

The actual "completion" of the DDL job as far as TiDB is concerned is that PD has been notified of the placement rules for all of the affected regions. PD will then asynchronously apply the placement rules to all of the regions, and this progress is not observable via `ADMIN SHOW DDL JOBS`. The progress of scheduling can be observed via `SHOW PLACEMENT` or by reading `information_schema.placement_policies`.

### Privilege management

Privilege management is quite straightforward:

* `ALTER [DATABASE|TABLE]` statement requires `Alter` privilege
* `CREATE TABLE` statement requires `Create` privilege
* `SHOW PLACEMENT` only shows the placement rules on the objects that visible to the current user
* `information_schema.placement_policies` requires `CREATE TABLE` privilege (it doesn't reveal anything about tables, but any user who creates a table can specify a placement policy, and needs to know the possible names of policies)
* `ADMIN SHOW DDL` requires `Super` privilege
* `CREATE PLACEMENT POLICY`, `DROP PLACEMENT POLICY` and `ALTER PLACEMENT POLICY` require `PLACEMENT_ADMIN` (a new dynamic privilege). This is because these objects have global scope.

## Implementation

### Storing Placement Policies

Placement policies will be stored in the internal meta data system, similar to tables or views. The policy name must be globally unique, but the definition of the table is not described in this proposal.

### Storage Consistency

PD uses placement rules to schedule data, so a replica of placement rules for _tables and partitions_ must be persistent in PD. However, because the rules are also considered part of the table definition, the placement rules are also persisted in PD.

The rules to guarantee consistency between these two sources is as follows:

- Changes to definition (`CREATE|ALTER TABLE`, `CREATE|ALTER PLACEMENT POLICY`, `CREATE|ALTER DATABASE`) will first be persisted to TiKV.
- The changes will then be applied to PD (and asynchronously apply)

It is safe to automatically retry applying the rules to PD because they are all idempotent in the current design.

If PD can not be modified, then the changes to TIKV are expected to rollback and the statement returns an error. Thus, TiKV acts as an undo log.

Because placement rules can also be configured outside of placement rules in SQL, PD should be considered the source of truth.

### Querying placement rules

The scenarios where TiDB queries placement rules are as follows:

1. The optimizer uses placement rules to decide to route cop requests to TiKV or TiFlash. It's already implemented and the TiFlash information is written into table information, which is stored on TiKV.
2. It will probably be used in locality-aware features in the future, such as follower-read. Follower-read is always used when TiDB wants to read the nearest replica to reduce multi-region latency. In some distributed databases, it’s implemented by labelling data nodes and selecting the nearest replica according to the labels.
3. Local transactions need to know the binding relationship between Raft leader and region, which is also defined by placement rules.
4. Once a rule is defined on a table, all the subsequent partitions added to the table should also inherit the rule. So the `ADD PARTITION` operation should query the rules on the table. The same is true for creating tables and indices.
5. The `SHOW PLACEMENT` statement should output the placement rules correctly.

As placement rules will be queried in case 1, 2 and 3, low latency must be guaranteed. As discussed in section "Storing placement rules", PD is the source of truth. To lower the latency, the only way is caching the placement rules in TiDB.

Since the cache is created, there must be a way to validate it. Different from region cache, placement rules cache can only be validated each time from PD. There are some ways to work around:

- Update the schema version once a placement rule is changed, just like other DDL. PD broadcasts the latest schema version to all the TiDB instances, and then TiDB instances fetch the newest placement rules from PD. There will be a slight delay for queries before reading the latest placement rules. The side effect is that more transactions will retry since the schema version is changed.
- TiDB queries placement rules from PD periodly. The delay is controllable but not eliminable.
- Once a placement rule is changed, PD broadcasts it to all the TiDB instances. In this approach, the schema version is not involved, so transactions are not affected. The delay is not eliminable either.

All the approaches above will result in a delay. Fortunately, for case 1 and 2 above, delay is acceptable. It doesn’t matter much if the optimizer doesn’t perceive the placement rules changement immediately. The worst result is that the latency is relatively high for a short time.

For case 3, although delay is acceptable, but all TiDB instances must be always consistent on the placement rules. To achieve this goal, the schema version needs to be updated, thus transactions with old placement rules will fail when committed.

For case 4 and 5, delay is not acceptable. Once the placement rules are written successfully, subsequent DDL statements should fetch the latest placement rules to guarantee linearizability. Now that the schema version is changed and the latest placement rules are broadcast to all the TiDB instances immediately, delay is eliminable. 

Once the schema version is changed, all TiDB instances recognize the object ID and fetch placement rules from PD, rather than TiKV.

To query the placement rules on a specified object, the object ID should be written to the placement rules, or it can be inferred from other fields. Now that `id` contains the object ID, TiDB can decode the object ID from it. See section "Building placement rules" for details.

### Building placement rules

There needs a way to map the placement rules in SQL to PD placement rule configuration. Most of the fields are discussed above, so this part focuses on `group_id`, `id`, `start_key` and `end_key`.

`group_id` is used to identify the source of the placement rules, so `group_id` is `tidb`.

`ALTER PLACEMENT POLICY` and `DROP PLACEMENT POLICY` need to find the rules of a specified object efficiently. It can be achieved by encoding the object ID in `id`.

However, an object (database, table, partition) may have multiple rules for a single role. For example:

```sql
CREATE PLACEMENT POLICY p1
	FOLLOWER_CONSTRAINTS="{+region=us-east-1: 2,+region=us-east-2: 1}";
CREATE TABLE t1 (a int) PLACEMENT POLICY p1;
```

It needs 2 placement rules for `follower` in the PD placement rule configuration, because each rule can only specify one `count`. To make `id` unique, a unique identifier must be appended to `id`. DDL job ID plus an index in the job is a good choice.

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

- `start_key`: `t{database_id}_`
- `end_key`: `t{database_id+1}_`

It's same for partitioned tables.

#### Region label configuration

Instead of configuring key ranges, you can also configure region labels in placement rules. PD supports label rules, which indicate the key range of a database / table / partition name. TiDB pushes label rules once the schema changes, so that PD maintains the relationship between database / table /partition names and their corresponding key ranges.

This is what a label rule may look like:

```
{
    "id": "db1/tb1",
    "labels": [
        {
            "key": "database-name",
            "value": "db1"
        },
        {
            "key": "table-name",
            "value": "db1/tb1"
        }
    ],
    "match-type": "key-range",
    "match": {
        "start-key": "7480000000000000ff0a00000000000000f8",
        "end-key": "7480000000000000ff0b00000000000000f8"
    }
}
```

It connects the table name `db1/tb1` with the key range.

Now you need to connect the label with the database / table / partition name in the placement rules.

For example:

```
{
    "group_id": "group_id",
    "id": "id",
    "region_label_key": "schema/table-name",
    "region_label_value": "db1/tb1",
    "role": "leader",
    "label_constraints": [
        {"key": "region", "op": "in", "values": ["us-east-1", "us-east-2"]}
    ]
}
```

Combined with the label rule, PD indirectly knows the key range of `db1/tb1` is marked with the label constraint `{"key": "region", "op": "in", "values": ["us-east-1", "us-east-2"]}`.

#### Database placement

Defining placement rules of databases simplifies the procedures when there are many tables.

For example, in a typical multi-tenant scenario, each user has a private database. The dataset in one database is relatively small, and it’s rare to query across databases. In this case, a whole database can be placed in a single region to reduce multi-region latency.

For another example, multiple businesses may run on a single TiDB cluster, which can reduce the overhead of maintaining multiple clusters. The resources of multiple businesses need to be isolated to avoid the risk that one business takes too many resources and affects others.

Since key range is not successive in one database, each table in the database corresponds to at least one placement rule, so there may be many placement rules. In either case above, there may be up to millions of tables in one database, which costs lots of time to update the rules and lots of space to store the rules.

Another option is to take advantage of the region label, which is described earlier.

In the example below, it defines multiple label rules for one database. Each label rule corresponds to one table or partition.

```
{
    "id": "db1/tb1",
    "labels": [
        {
            "key": "database-name",
            "value": "db1"
        },
        {
            "key": "table-name",
            "value": "db1/tb1"
        }
    ],
    "match-type": "key-range",
    "match": {
        "start-key": "7480000000000000ff0a00000000000000f8",
        "end-key": "7480000000000000ff0b00000000000000f8"
    }
},
{
    "id": "db1/tb2",
    "labels": [
        {
            "key": "database-name",
            "value": "db1"
        },
        {
            "key": "table-name",
            "value": "db1/tb2"
        }
    ],
    "match-type": "key-range",
    "match": {
        "start-key": "7480000000000000ff0c00000000000000f8",
        "end-key": "7480000000000000ff0d00000000000000f8"
    }
}
```

Then you need only one placement rule for the database. When you change the placement of the database, you need to update one placement rule. However, when you drop a database, you need to delete multiple label rules plus one placement rule.

### Rule priorities

Tables only inherit rules from databases when they are created, and the value is saved in the meta data. Thus, the rules of priorities are simplified from an earlier version of this proposal (and are more inline with how character sets are inherited).

The only rules are that indexes and partitions inherit the rules of tables. Partitions can explicitly overwrite the placement policy, but indexes currently do not allow placement policy to be defined (this simplification was made intentionally since there is not a clear use case until global secondary indexes are introduced).

Thus the priority is:

```
db --> table (Copied from db on create if placement not explicitly specified for the table)
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


## Examples

### Optimization: Follower read in every region

This optimization is straight forward:
```sql
CREATE PLACEMENT POLICY local_stale_reads FOLLOWER_CONSTRAINTS="{+us-east-1: 1,+us-east-2: 1,+us-west-1: 1,+us-west-2: 1}";
CREATE TABLE t (a int, b int) PLACEMENT POLICY=`local_stale_reads`;
```

### Optimization: Latest data on SSD

This optimization uses labels to define the storage type:

```sql
CREATE PLACEMENT POLICY storeonfastssd CONSTRAINTS="[+disk=ssd]";
CREATE PLACEMENT POLICY storeonhdd CONSTRAINTS="[+disk=hdd]";

CREATE TABLE t1 (id INT, name VARCHAR(50), purchased DATE)
 PLACEMENT POLICY='companystandardpolicy'
 PARTITION BY RANGE( YEAR(purchased) ) (
  PARTITION p0 VALUES LESS THAN (2000) PLACEMENT POLICY='storeonhdd',
  PARTITION p1 VALUES LESS THAN (2005),
  PARTITION p2 VALUES LESS THAN (2010),
  PARTITION p3 VALUES LESS THAN (2015),
  PARTITION p4 VALUES LESS THAN MAXVALUE PLACEMENT POLICY='storeonfastssd'
 )
;
```

### Optimization: Multi-tenancy / control of shared resources

This example is similar to latest data on SSD. The customer has a large TiDB Cluster with several workloads that are running on it. They might want to reduce the blast radius of individual users impacting each-other, and potentially improve QoS.

Assuming a `schema` per tenant, it is easy to create a set of "resource pools". Each pool is a label, which contains a set of tikv-servers (with sufficient capacity, and nodes to provide high availability still):

```sql
CREATE PLACEMENT POLICY poola CONSTRAINTS="[+pool=poola]";
CREATE PLACEMENT POLICY poolb CONSTRAINTS="[+pool=poolb]";
CREATE PLACEMENT POLICY poolc CONSTRAINTS="[+pool=poolc]";

ALTER DATABASE workload1 PLACEMENT POLICY=`poola`;
/* for each existing table (new ones will not require this) */
ALTER TABLE workload1.t1 PLACEMENT POLICY=`poola`;

CREATE DATABASE workload2 PLACEMENT POLICY=`poolb`;
CREATE DATABASE workload3 PLACEMENT POLICY=`poolb`;
CREATE DATABASE workload4 PLACEMENT POLICY=`poolb`;
CREATE DATABASE workload5 PLACEMENT POLICY=`poolb`;
CREATE DATABASE workload6 PLACEMENT POLICY=`poolc`;
```

### Compliance: User data needs geographic split

This example has limitations based on the current implementation. Consider the following `users` table:

```sql
CREATE TABLE users (
	id INT NOT NULL auto_increment,
	username VARCHAR(64) NOT NULL,
	email VARCHAR(64) NOT NULL,
	dateofbirth DATE NOT NULL,
	country VARCHAR(10) NOT NULL,
	PRIMARY KEY (id),
	UNIQUE (username)
);
```

We may want to ensure that users that have users in the EU store their data in a specific location. On the surface this looks straight forward:

```sql
CREATE TABLE users (
	id INT NOT NULL auto_increment,
	username VARCHAR(64) NOT NULL,
	email VARCHAR(64) NOT NULL,
	dateofbirth DATE NOT NULL,
	country VARCHAR(10) NOT NULL,
	PRIMARY KEY (id),
	UNIQUE (username)
) PARTITION BY LIST COLUMNS (country) (
	PARTITION pEurope VALUES IN ('DE', 'FR', 'GB') PLACEMENT POLICY='europe',
	PARTITION pOther VALUES IN ('US', 'CA', 'MX')
);
```

However, the definition is not valid. The unique index on `username` can not be enforced, because there are no global indexes for partitioned tables.

In the future we will need to be able to define the index for `username` as a global index, and allow it to have different placement rules where it can be read from all regions.

This example also demonstrates that this specification only provides partition level placement (and not row-level or column level security). The workaround for the user will require splitting the table:

```sql
CREATE TABLE users (
	id INT NOT NULL auto_increment PRIMARY KEY,
	/* public details */
);

CREATE TABLE user_details (
	user_id INT NOT NULL,
	/* private columns */
	/* partition this table */
);
```

Assuming that global indexes can be added to the TiDB server, this use-case can be improved. But for correct execution the server will also require the following functionality:
- The ability to mark global indexes as invisible at run-time if the `PLACEMENT POLICY` does not permit them to be read.
- The ability to mark the clustered index as invisible. i.e. in the case that there is a global unique index on `username` it may be permitted to be read, but a read of the `username` from the clustered index might need to be disabled.

## Impacts & Risks

1. The largest risk is designing a set of SQL statements that are sufficiently flexible to handle major use cases, but not too flexible that misconfiguration is likely when the user has compliance requirements. The following design decisions have been made to mitigate this risk:
  - The DDL statement to `ALTER TABLE t1 ADD PLACEMENT` has been removed from the proposal.
  - The DDL statement `CREATE PLACEMENT POLICY` has been added (allowing common configurations to be saved).
  - Configuring placement rules for indexes is no longer supported (we can add it once global indexes are added).
  - The inheritance rules have been simplified to match character set/collation inheritance.
2. There is a risk that we do not fully understand compliance requirements (see unresolved questions), or that the substantial effort required to achieve compliance with DDL, internal SQL and DML.
3. Related to (2), there is risk that the implementation of compliance requirements has a significant burden on multiple teams, including ecosystem tools (Backup, CDC, DM). Even tools such as dumpling should ideally be able to backup placement rules in logical form.
4. The compliance use-case may depend on global secondary indexes for many scenarios (see "Compliance: User data needs geographic split"), but they are not currently supported.
4. There is some risk that a common use case can not be expressed in `CONSTRAINT` syntax, leading to complicated scenarios where users still need to express placement by using PD directly. Ideally, we can recommend users use SQL rules exclusively.
5. Many other features in TiDB are in development, some of which may influence placement rules. Clustered index affects the key format of primary index, but fortunately the prefix of key range is untouched. Global secondary index largely affects the placement rules of partitioned tables.

## Investigation & Alternatives

For investigation, we looked at the implementation of placement rules in various databases (CockroachDB, Yugabyte, OceanBase).

The idea of using a `PLACEMENT POLICY` was inspired by how OceanBase has Placement Groups, which are then applied to tables. The idea of using a Placement Group/Placement Policy can also be seen as similar to using a "tablespace" in a traditional database.

CockroachDB does not look to have something directly comparable to `PLACEMENT POLICY`, but it does have the ability to specify "replication zones" for "system ranges" such as default, meta, liveness, system, timeseries. Before dropping `ALTER TABLE t1 ADD PLACEMENT` from this proposal, it was investigated the CockroachDB does not support this syntax, presumably for simplification and minimising similar risks of misconfiguration.

### Known Limitations

- In this proposal, placement policies are global-level and not specific to a database. This simplifies the configuration, but makes it restrictive for multi-tenant scenarios where a schema-per-tenant is provided. This is because creating or modifying placement policies requires a privilege which is cluster scoped (`PLACEMENT_ADMIN`). The limitation is that "tenants" will be able to `CREATE TABLE (..) PLACEMENT POLICY=x`, but they will not be able to `CREATE PLACEMENT POLICY x` or `ALTER PLACEMENT POLICY x`
- Complex scenarios may exist where there is not a column in the current table which can be used to partition the table into different regions, but instead there is a column which is a foreign key to another table from which this information can be determined. In this scenario, the user will be required to "denormalize" the usage and move the parent_id into the child table so geo-partitioning is possible.
- The name `REGION` is ambigous, since we are using it for placement as well as to refer to a chunk of data. We could avoid region here, but the problem is it is the most used term for a geographic location of a data center. We recommend instead calling both region but in documentation refering to them as "data regions" and "placement regions".

## Unresolved Questions

### Compliance Requirements

For compliance use-cases, it is clear that data at rest should reside within a geographic region. What is not clear is which (if any) circumstances data in transit is permitted to leave. There are several known issues which will need to be resolved:

* **Backup**: The `BACKUP` SQL command (and br) accept a single location such as `s3://bucket-location/my-backup` to centralize the backup. This centralization likely violates compliance requirements (assuming there are >=2 requirements that conflict on a single cluster). Backing up segments of data individually is both an inconsistent backup, and likely to result in misconfiguration which violates compliance rules. Users have a reasonable expectation of backup integration with compliance placement rules.
* **CDC**: Similar to `BACKUP`, it should not be possible to subscribe to changes for data in a different jurisdiction.
* **DDL**: The current implementation of DDL uses a centralized DDL owner which reads data from relevant tables and performs operations such as building indexes. The _read_ operation may violate compliance rules.
* **Internal SQL**: Similar to DDL, several centralized background tasks, such as updating histograms/statistics need to be able to _read_ the data.
* **DML**: It is not yet known which restrictions need to be placed on user queries. For example, if a poorly written user-query does not clearly target the `USA` partition when reading user data, should it generate an error because the `EUROPE` partition needs to be read in order for the semantics of the query to be correct? This may cause problems in development and integration environments if the restrictions can not be distilled into environments with smaller topologies.
* **Routing**: When there is a data center for Europe, and a Data center for the USA, and the data is partitioned with requirements that DML from the USA can not read data in Europe, how is that enforced? Is it configured in such a way that the tidb-servers in the USA can not route to the tikv-servers in Europe? If this is the case, then it means the European servers can not hold non-user compliance data that the USA might need to read. If it is not the case, then there might be some sort of key management/crypto scheme to control access to sensitive data.

### Behaviors

#### Syntax for Restricted Access (Compliance Case)

Assume that if the example is logically like the "Compliance: User data needs geographic split" case:

```sql
CREATE TABLE users (
	id INT NOT NULL auto_increment,
	username VARCHAR(64) NOT NULL,
	email VARCHAR(64) NOT NULL,
	dateofbirth DATE NOT NULL,
	country VARCHAR(10) NOT NULL,
	PRIMARY KEY (id),
	UNIQUE (username)
) PARTITION BY LIST COLUMNS (country) (
	PARTITION pEurope VALUES IN ('DE', 'FR', 'GB') PLACEMENT POLICY='europe',
	PARTITION pOther VALUES IN ('US', 'CA', 'MX')
);
```

What does `SHOW CREATE PLACEMENT POLICY europe` look like?

I assume that it is something like:

```sql
CREATE PLACEMENT POLICY europe CONSTRAINTS="+region=eu-west-1" RESTRICTED;
```

This specific semantic will be the hardest to implement because of the other dependencies in the server.

## Changelog

* 20221-01-06:
  - Removed Direct placement options, and support only placement policies.
  - Renamed infoschema tables to make sense based on direct placement removal.
  - Incorporate change from `PLACEMENT_CHECKS` to `tidb_placement_mode`.

* 2021-11-29:
  - Updated limits on object length.
  - Removed built-in placement policies (not supported for now, need additional discussion due to `DEFAULT` conflicts.)

* 2021-10-29:
  - Add more description to 'scheduling_state'.

* 2021-09-13:
  - Removed support for `VOTER_CONSTRAINTS` and `VOTERS`. The motivation for this change is that dictionary syntax is ambiguous cases when both `VOTER_CONSTRAINTS` and `FOLLOWER_CONSTAINTS` are set. We can re-add this syntax if there is a clear use-case requirement in future.

* 2021-07-26:
  - Converted proposal to use the new template for technical designs.
  - Removed the syntax `ALTER TABLE t1 ADD PLACEMENT POLICY` due to ambiguity in some cases, and risk of misconfiguration for compliance cases.
  - Added `CREATE PLACEMENT POLICY` syntax.
  - Renamed `ALTER TABLE t1 ALTER PLACEMENT POLICY` to `ALTER TABLE t1 PLACEMENT` to bring syntax inline with other atomic changes, such as `ALTER TABLE t1 CHARACTER SET x`. The usage of `PLACEMENT POLICY` now refers to a placement policy defined from `CREATE PLACEMENT POLICY` (other commands like `SHOW PLACEMENT POLICY` are also updated to `SHOW PLACEMENT`).
  - Remove index as a placement option (we can add it again once global indexes for temporary tables exist, but it is not strictly required for an MVP).
  - Made implementation in `CREATE TABLE` and `SHOW CREATE TABLE` required, to support the compliance use-case.
  - Changed `ALTER TABLE ALTER PARTITION p0` to `ALTER TABLE PARTITION p0`
  - Added short-hand syntactic sugar for constraints to handle default cases.
  - Changed it so that you can no longer specify multiple constraints.
  - Use defaults for `count` of each role, and `ROLE_CONSTRAINTS` syntax.
  - Added `SCHEDULE` property
  - Removed further ambiguous cases such as count when using dictionary syntax.
