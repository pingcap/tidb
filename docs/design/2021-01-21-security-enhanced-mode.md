# Proposal:

- Author(s):     [morgo](https://github.com/morgo)
- Last updated:  Jan. 21, 2021
- Discussion at: N/A

## Abstract

This document was created to discuss the design of Security Enhanced Mode. It comes from the requirement of [Permission isolation from DBaaS users and DBaaS admin](https://docs.google.com/document/d/1S7-8KqR98DYaE-2NCrPiDWlmUvsHvIZytCE8Ofmbgyg/edit) (internal document).

## Terminology

* **Configuration Option**: The name of a variable as set in a configuration file.
* **System Variable** (aka sysvar): The name of a variable that is set in a running TiDB server using the MySQL protocol.
* **Super**: The primary MySQL "admin" privilege, which is intended to be superseded by MySQL’s "dynamic" (fine-grained) privileges starting from MySQL 8.0.

## Background

Currently the MySQL SUPER privilege encapsulates a very large set of system capabilities. It does not follow the best practices of allocating _fine grained access_ to users based only on their system-access requirements.

This is particularly problematic in a DBaaS scenario such as TiDB Cloud where the `SUPER` privilege has elements that are required by both end users (TiDB Cloud Customers) and system operations (PingCAP SREs).

The design of Security Enhanced Mode (SEM) takes the approach of:

1. Restricting `SUPER` to a set of capabilities that are safe for end users.
2. Implementation of dynamic privileges ([issue #22439](https://github.com/pingcap/tidb/issues/22439)).

This approach was requested by product management based on the broad "in the wild" association of `SUPER` as "the MySQL admin privilege". Thus, proposals to create a new lesser-SUPER privilege have already been discussed and rejected.

The design and name of "Security Enhanced" is inspired by prior art with SELinux and AppArmor.

## Proposal

A boolean option called `EnableEnhancedSecurity` (default FALSE) will be added as a TiDB configuration option.  The following subheadings describe the behavior when `EnableEnhancedSecurity` is set to `TRUE`.

### System Variables

The following system variables will be hidden:

* variable.TiDBDDLSlowOprThreshold,
* variable.TiDBAllowRemoveAutoInc,
* variable.TiDBCheckMb4ValueInUTF8,
* variable.TiDBConfig,
* variable.TiDBEnableSlowLog,
* variable.TiDBEnableTelemetry,
* variable.TiDBExpensiveQueryTimeThreshold,
* variable.TiDBForcePriority,
* variable.TiDBGeneralLog,
* variable.TiDBMetricSchemaRangeDuration,
* variable.TiDBMetricSchemaStep,
* variable.TiDBOptWriteRowID,
* variable.TiDBPProfSQLCPU,
* variable.TiDBRecordPlanInSlowLog,
* variable.TiDBRowFormatVersion,
* variable.TiDBSlowQueryFile,
* variable.TiDBSlowLogThreshold,
* variable.TiDBEnableCollectExecutionInfo,
* variable.TiDBMemoryUsageAlarmRatio,

The following system variables will be reset to defaults:

* variable.Hostname

### Information Schema Tables

The following tables will be hidden:

* cluster_config
* cluster_hardware
* cluster_load
* cluster_log
* cluster_systeminfo
* inspection_result
* inspection_rules
* inspection_summary
* metrics_summary
* metrics_summary_by_label
* metrics_tables
* tidb_hot_regions

The following tables will be modified to hide columns:

* tikv_store_status
    * The address, capacity, available, start_ts and uptime columns will return NULL.
* Tidb_servers_info
    * The “IP” column will return NULL.
* cluster_* tables
    * The “instance” column will show the server ID instead of the server IP address.

### Performance Schema Tables

The following tables will be hidden:

 * pd_profile_allocs
 * pd_profile_block
 * pd_profile_cpu
 * pd_profile_goroutines
 * pd_profile_memory
 * pd_profile_mutex
 * tidb_profile_allocs
 * tidb_profile_block
 * tidb_profile_cpu
 * tidb_profile_goroutines
 * tidb_profile_memory
 * tidb_profile_mutex
 * tikv_profile_cpu

### System (mysql) Tables

The following tables will be hidden:

* expr_pushdown_blacklist
* gc_delete_range
* gc_delete_range_done
* opt_rule_blacklist
* tidb
* global_variables

### Metrics Schema

All tables will be hidden, including the schema itself.

### Commands

* `SHOW CONFIG` is disabled.
* `SET CONFIG` is disabled by the `CONFIG` Privilege (no change necessary)

## Testing Plan

### Unit test

Unit tests will be added to cover the enabling and disabling of sysvars, and tables.

Tests will need to check that invisible tables are both non-visible and non-grantable (it should work, since visibility can be plugged into the privilege manager directly).

If the user with SUPER privilege grants privileges related to these tables to other users, for example, `GRANT SELECT, INSERT, UPDATE ON information_schema.cluster_config TO 'userA'@'%%';` -- it should fail.

### Integration testing

The integration test suite will run with `EnableEnhancedSecurity=FALSE`, but new integration tests will be written to cover specific use cases.

#### Documentation Plan

Documentation is critically impacted by SEM, since it should be possible for a manual page to cover the use-case of SEM both enabled and disabled.

Supporting PRs will be required to modify both documentation and functionality so that system variables and/or tables that are hidden by SEM are not required. For example: 

* https://github.com/pingcap/tidb/pull/22286
* https://github.com/pingcap/tidb/pull/21988
* https://github.com/pingcap/docs/pull/4552

* A further change to move the `new_collation_enabled` variable from mysql.tidb to a status variable has been identified, as it appears on several manual pages. No PR has been created yet.

### Impact & Risks

The intention behind SEM is to reduce the impact on end users, who can continue to use “SUPER” as the defacto admin privilege (versus alternatives such as mentioned below).

The larger impact will be on System Operators, who will need fine grained privileges to replace the super privilege.

The largest risk is application/MySQL compatibility. There are a number of SEM behaviors which have been discussed, with the following outcomes:

| Suggestion | Observed Risk | Outcome |
| --------------- | --------------- | --------------- |
| Is it possible to make a system variable non-readable by a non privileged user? | MySQL does not have a semantic where a sysvar would ever be non readable. Non-settable however is fine.| Variables will either be invisible or visible. Never non-readable, although non-writeable is possible (example: sql_log_bin). |
| Is it possible to hide columns in information schema? | Users may depend on ordinality of information_schema table column order. This is particularly likely with tables with useful columns at the start. | Columns will appear with NULL values when they must be hidden.|
| Is it possible to hide sysvars such as hostname? | For MySQL-specific sysvars, there is an increased likelihood applications will read them, and result in an error if they are not present. | For a specific case like hostname, it is a requirement to return a placeholder value such as ‘localhost’, rather than hide the variable. |

Users will also be able to observe if the system they are using has enhanced security mode enabled via the system variable, `tidb_enable_enhanced_security` (read-only).

We will need to consider the impact on tools. When SEM is disabled, they should have no impact. When SEM is enabled, it should be possible to make recommendations to the tools team so that they can still access meta data required to operate in DBaaS environment:

* Lightning and BR will not work currently with SEM + https://github.com/pingcap/tidb/pull/21988
* In 5.0 the recommended method for BR/Lightning to get TiKV GC stats should change.
* There is one PR still pending for obtaining statistics: https://github.com/pingcap/tidb/pull/22286 

### Alternatives

The alternative to SEM is to implement fine-grained privileges for end users. This idea has been discussed and rejected. See "Background" for context.

Amazon RDS also uses the approach of not granting SUPER to users, and instead offering a set of custom stored procedures to support use-cases that would usually require SUPER. This idea has been rejected.

### Unresolved Questions

None

