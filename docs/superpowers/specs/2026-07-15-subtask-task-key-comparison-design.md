# Exact Subtask Task-Key Comparisons

## Problem

`mysql.tidb_background_subtask.task_key` and `mysql.tidb_background_subtask_history.task_key` are `VARCHAR(256)` columns containing the decimal representation of a global task's `BIGINT` ID. Current callers pass Go integer values to SQL predicates such as `task_key = %?`. Internal SQL interpolation renders those values as unquoted integer literals, so TiDB compares the string column and integer literal in the `DOUBLE` domain. The cast prevents equality-range use of `idx_task_key`, and distinct IDs above `2^53` can compare equal after binary64 rounding.

## Chosen Design

Keep the table schemas unchanged and convert task IDs to canonical base-10 strings before using them as subtask-table predicate arguments. The framework storage package will expose one small conversion helper because it owns the mapping between the integer task ID and the string storage key. Framework storage, Import Into, history readers, and test utilities will use that helper where package dependencies already permit it. Direct SQL in tests will use string parameters or explicitly quoted decimal text. Integer-valued subqueries will cast their selected task IDs to `CHAR`, leaving the indexed `task_key` column uncast.

This restores string comparison semantics and lets ranger see the indexed column directly. It also preserves the existing schema and stored representation, avoiding bootstrap, upgrade, and rolling-compatibility changes.

## Alternatives Rejected

Writing `task_key = CAST(%? AS CHAR)` at every callsite was rejected because it duplicates SQL-specific conversion policy and introduces charset and collation details throughout the codebase. Changing the columns to `BIGINT` was rejected because it requires a much broader system-table migration and compatibility analysis. Leaving test-only queries unchanged was rejected because those queries can hide production-like precision and performance failures.

## Work Items

The framework storage work item adds a regression scenario to the existing `TestGetActiveSubtasks`, proves the current failure with IDs `9007199254740992` and `9007199254740993`, adds the shared conversion helper, and updates all framework storage predicates.

The Import Into work item adds independent red/green coverage for the active/history union in `GetJobLastUpdateTime` and the history aggregation in `jobhistory.GetFromHistory`, then updates both to pass decimal strings.

The non-production work item updates every risky test and test-utility comparison found by the repository audit, including formatted SQL, prepared parameters, the `all_subtasks` view queries, and the `IN (SELECT id ...)` cases.

The authoritative production inventory is:

| File | Predicates | Required change |
| --- | ---: | --- |
| `pkg/dxf/framework/storage/task_table.go` | 13 | Pass `TaskIDToKey(taskID)` or `TaskIDToKey(task.ID)` for each subtask-table predicate. |
| `pkg/dxf/framework/storage/subtask_state.go` | 4 | Convert the task-ID argument without changing executor, state, or error arguments. |
| `pkg/dxf/framework/storage/task_state.go` | 2 | Convert the task-ID argument; leave global-task predicates unchanged. |
| `pkg/dxf/framework/storage/history.go` | 2 | Use the same decimal string for the transfer source and delete predicates. |
| `pkg/dxf/importinto/job.go` | 2 | Convert both active/history union arguments. |
| `pkg/dxf/importinto/jobhistory/history.go` | 1 | Convert `Info.TaskID` for the history aggregation. |

The authoritative risky non-production inventory is:

| File | Predicates | Required change |
| --- | ---: | --- |
| `pkg/dxf/framework/storage/table_test.go` | 3 | Quote the formatted decimal values; this file belongs to the framework-storage work item. |
| `pkg/dxf/framework/testutil/table_util.go` | 7 | Pass `storage.TaskIDToKey(taskID)`. |
| `pkg/dxf/framework/integrationtests/framework_err_handling_test.go` | 1 | Quote the formatted decimal value. |
| `pkg/dxf/framework/integrationtests/modify_test.go` | 2 | Pass `storage.TaskIDToKey(task.ID)`. |
| `tests/realtikvtest/addindextest1/disttask_test.go` | 2 | Cast selected global-task IDs to `CHAR`. |
| `tests/realtikvtest/addindextest3/temp_index_test.go` | 2 | Quote the already numeric-string values. |
| `tests/realtikvtest/importintotest4/import_summary_test.go` | 2 | Keep and bind the selected task ID as a string. |

Six string-safe predicates are intentional exclusions: the CTE predicate in `tests/realtikvtest/addindextest2/global_sort_test.go`, two predicates in `tests/realtikvtest/importintotest/import_into_test.go`, two in `tests/realtikvtest/importintotest3/cross_ks_test.go`, and the quoted formatted predicate in `tests/realtikvtest/importintotest4/manual_recovery_test.go`.

## Validation

The regression test must fail before the production fix because both colliding task keys are returned, then pass afterward with exactly one matching subtask. Package tests must follow the failpoint runner policy. Because Go import sections may change, the final diff must be checked with the Bazel preparation gate and `make bazel_prepare` run if triggered. Completion uses the Ready profile, including targeted tests, `make lint` for code changes, a repository-wide search proving no integer-valued target-table comparisons remain, and a clean self-review of generated Bazel metadata.
