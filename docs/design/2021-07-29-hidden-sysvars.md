# Proposal: Remove hidden sysvars

- Author(s): [morgo](http://github.com/morgo)

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
  * [#1 “Stable” but intended to be deprecated or advanced configuration variables](#1-stable-but-intended-to-be-deprecated-or-advanced-configuration-variables)
  * [#2 “Experimental” feature flags](#2-experimental-feature-flags)
  * [#3 “In Development” feature flags](#3-in-development-feature-flags)
  * [#4 Non-boolean Experimental Options](#4-non-boolean-experimental-options)
* [Test Design](#test-design)
* [Implementation Plan](#implementation-plan)
  * [Appendix #1 - tidb_experimental_feature_switch documentation](#appendix-1---tidb_experimental_feature_switch-documentation)
  * [Appendix #2 - Removed system variables](#appendix-2---removed-system-variables)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

Currently we create hidden/undocumented settings that are only designed for internal PingCAP users. In an internal call we unanimously agreed that this is problematic for our customers.

## Motivation or Background

The proposal is to change all hidden system variables to visible. The proposed solution differs based on the category of why the variable was made invisible.

This proposal also includes expected standards for documentation.

There are currently ~20 hidden system variables:

| Name | Usage | Category |
| ------|-------|-------------|
| tidb_enable_local_txn | Local/Global Transaction | #2 Experimental |
| tidb_txn_scope | Parameters of local transaction | #4 Non-boolean Experimental |
| tidb_txn_read_ts | "" | #4 Non-boolean Experimental |
| tidb_enable_pipelined_window_function | ‘Pipelined window function’ feature | #1 Stable |
| tidb_enable_change_multi_schema | ‘Change multi schema in one statement’ feature. | #3 In Development Feature |
| tidb_enable_point_get_cache | ‘point get cache’ feature | #2 Experimental |
| tidb_enable_alter_placement | placement rules in SQL feature | #3 In Development Feature |
| tidb_enable_extended_stats | ‘extended stats’ feature | #2 Experimental |
| tidb_partition_prune_mode | Is partition prune mode dynamic or static | #2 Experimental |
| tidb_enable_async_commit | Support Async Commit PRD  | #1 Stable |
| tidb_enable_1pc | "" | #1 Stable |
| tidb_guarantee_linearizability | "" | #1 Stable |
| tidb_enable_index_merge_join | Index merge join | #2 Experimental |
| tidb_enable_global_temporary_table | Global temporary table | #1 Stable |
| tidb_enable_stable_result_mode | Make resultset sorted(deterministic order) by default | #1 Stable |
| tidb_track_aggregate_memory_usage | Track memory usage of aggregate executor | #1 Stable |
| tidb_enable_top_sql | Top SQL (Sprint3) 总设计文档  | #2 Experimental |
| tidb_top_sql_agent_address | Parameters for Top SQL | #4 Non-boolean Experimental |
| tidb_top_sql_precision_seconds | "" | #4 Non-boolean Experimental  |
| tidb_top_sql_max_statement_count | "" | #4 Non-boolean Experimental |
| tidb_top_sql_max_collect | "" | #4 Non-boolean Experimental |
| tidb_top_sql_report_interval_seconds | "" | #4 Non-boolean Experimental |


## Detailed Design

### #1 “Stable” but intended to be deprecated or advanced configuration variables

These variables were hidden because they are intended to be removed in a future version, and are not typically recommended to be changed.

However; the problem with this decision is that these features do impact performance and behavior characteristics significantly, and can make debugging the difference between two clusters very difficult. This is exacerbated by our choice to only enable async commit for “new clusters”, and disable it for upgrades from earlier versions.

Consider the case that one cluster is upgraded from a logical dump, and the other from an inplace upgrade. They will have different behavior characteristics; and because the variable is hidden it is very difficult for a DBA to debug this issue.

Add to this the fact that async commit is not guaranteed to be bug free: there was a recent critical issue with async commit. There could be a case where one cluster has data corruption and the investigation is made more difficult.

These variables are also in the documentation, but hidden in the server. This makes me (as a DBA) believe that they don’t apply to my version. My MySQL experience works against me, because I expect that every variable is in SHOW VARIABLES.

**Recommended Solution:**

* Change all of these variables to be visible. They impact behavior, so it should be clear to a user when comparing clusters.
* We can add functionality to better handle deprecation in the sysvar system when it is intended to be removed.


### #2 “Experimental” feature flags

These examples are all boolean feature flags, and cover features which are not of GA quality, but are of sufficient quality that we invite users to test them out (see #3 for the still in-development case).

The rationale for making them hidden was to not pollute the variable system when they are intended to be removed rather quickly as they become stable.

However, the problem is that the number of experiments in TiDB is currently out of control. It is very difficult to know which flags specifically relate to an experiment and quickly ensure for production that all experiments are disabled. Or for development environments, you may choose to have all experiments enabled.

**Recommended solution:**

* These variables should be migrated to use `tidb_experimental_feature_switch`. This is modeled on how optimizer switches work in MySQL, and better caters for options that appear/disappear quickly.
* The documentation for `tidb_experimental_feature_switch` on system-variables can include a one-line description for each of the experiments (See Appendix #1). This is less “heavy” than documenting these as separate system variables, and there is no requirement to include docs on which version an experiment appeared or disappeared.

### #3 “In Development” feature flags

These are a subcategory of “experimental” feature flags, with the difference being that the feature is still in active development, and is not ready for users to try the experiment. There may be significant known pitfalls.

Because refactoring code itself can introduce bugs, if the code for these features exists in the server, in the interest of transparency it should be disclosed to the DBA, even if it is highly recommended that the user never use this feature.

**Recommended solution:**

* These variables should be migrated to use `tidb_experimental_feature_switch`.
* The sysvar documentation for `tidb_experimental_feature_switch` will not document these unsupported flags (See Appendix #1) but instead say:
   > Your TiDB server installation may include additional experimental features not in this list. Enabling them is not recommended and could result in data loss.

### #4 Non-boolean Experimental Options

This category of variables is different from Experimental/In Development feature flags because the tidb_experimental_feature_switch only handles boolean switches.

These variables almost always depend on an experimental feature flag (which will move to `tidb_experimental_feature_switch`). If they don’t, one might need to be created.

**Recommended Solution:**

* Because the system variable system supports dynamic registration/unregistration, these variables should be removed when the experiment is disabled, and added when the experiment is not enabled. 
* In some cases these variables might have been miscategorized and are instead “Stable” but intended to be deprecated or advanced configuration variables (category #1).

### Future Handling

Internally, we need an explicit guidebook for how the variables are added and removed with the evolution of TiDB:

* When and how a new variable is added. The cons/pros of adding a variable compared with a configuration item.
* When and how a variable will be removed.

Once it is decided, it’s a good idea to publish it the design docs.

### Documentation Standards

There are currently 75 undocumented system variables (not including noop sysvars). Because several hidden variables are actually already documented, the lack of documentation is really an orthogonal problem that also needs fixing.

It is expected that all system variables will be documented, with the exception of category #3 which in the recommendation is explicitly “documented as not documented”.

## Implementation Plan

#### Appendix #1 - tidb_experimental_feature_switch documentation

##### tidb_experimental_feature_switch

* Scope: SESSION | GLOBAL
* Default value: (system dependent)
* This variable allows you to enable or disable TiDB Server features which have not reached GA maturity.
Individual values can be changed by specifying a comma separated list. For example: `SET GLOBAL tidb_experimental_feature_switch = 'alter_placement=ON,cascades_planner=ON';`
* A wildcard can be used to change the value of all experiments. For example: `SET GLOBAL tidb_experimental_feature_switch = '%=OFF'`;
* Changing this variable on a SESSION basis requires the `SESSION_VARIABLES_ADMIN` permission.
* Experiments include:

| Name   | Usage      |
|----------|-------------|
| local_txn |  Enable Local/Global transactions |
| point_get_cache | Cache TiKV Point Get requests |
| extended_stats | Create statistics on multiple columns. This is useful in the case that columns do not have strong correlation. |
| dynamic_partition_prune  | Enable the dynamic partition prune mode. |
| index_merge_join | Enable the optimization where multiple indexes can be used and merged. |
| top_sql | Enable the Top SQL feature |

> **Warning:**
> Be careful when enabling experimental features. Your TiDB server installation may include additional experimental features not listed here, and enabling them may result in data loss.
> The experimental feature switch is settable on both a SESSION and GLOBAL basis, but some experiments might only be available globally. In which case changes to the session have no effect.

#### Appendix #2 - Removed system variables

For existing experimental flags, the procedure to remove the flag once the feature is GA is problematic since here could be users that have applications that explicitly enable the feature which now break.

This proposes that we support "removed system variables" by the sysvar framework. A removed sysvar:

* Is settable via a `SET` statement
* Returns an error for `SELECT @@var` queries
* Is not visible in `SHOW VARIABLES` output

This helps accommodate one of the main use cases (phantom `SET` statements in application code) while not returning inaccurate or out of date data.

## Test Design

- Because this change "makes more functionality visible", it is unlikely to cause compatibility issues (versus removing functionality).
- The change can be handled via unit tests.

## Impacts & Risks

- The largest impact is that users will expect system variables to be documented (of which some of the hidden system variables already are.)
- Users may feel more accustomed to setting variables if they are visible, which might lead to incorrect usage, or more complex deprecation cyles. However, there is a reasonable expectation that if the documentation states a particular value is unsafe, then it should not be used.

## Investigation & Alternatives

In MySQL system variables are never hidden. If a variable is settable by `SET x=y`, then `x` will show in `SHOW VARIABLES`.

Oracle does have [hidden variables](http://www.dba-oracle.com/art_so_undoc_parms_p2.htm) for special tasks such as tuning. But there are culture differences with Oracle that make this a difficult comparision.

## Unresolved Questions

* None
