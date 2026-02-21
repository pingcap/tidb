# Refactoring Progress Tracker

Last updated: 2026-02-21 (Batches 2-25: regexp through physical plan helper decompositions)

## Benchmark Validation (2026-02-17)

### Micro-benchmarks (benchstat, p=0.008, n=5)
- **Vectorized string functions**: -24% to -59% faster (SUBSTR, LOCATE, CHAR_LENGTH)
  - Substring2ArgsUTF8 Vec: -24.4%, Substring3ArgsUTF8 Vec: -38.5%
  - Locate3ArgsUTF8 Vec: -48.3% to -59.5%, Locate2ArgsUTF8 Vec: -7.6% to -15.0%
  - CharLengthUTF8 NonVec: -20.8%
  - Geomean: **-23.9% sec/op, -5.2% B/op, -4.3% allocs/op**
- **reflect.TypeOf caching**: 10.0ns → 5.65ns (**1.8x faster**)
- **Chunk iterator reuse**: negligible (compiler inlines allocation)

### go-ycsb macro (8 threads, 20k ops, 10k records, unistore)
- Workload A (50/50 read/update): ~50k OPS, no regression
- Workload C (100% read): ~78k OPS, no regression

## Status Legend

- `[ ]` Not started
- `[~]` In progress
- `[x]` Completed
- `[-]` Skipped / Won't do

---

## P0 - Critical (Do First)

- [x] **executor/builder.go split** - Replace 103-case switch with registry/factory pattern
  - File: `pkg/executor/builder.go` (6,222 lines → 1,082 lines, 83% reduction)
  - Target: Split into per-operator-type builder files with auto-registration
  - [x] Phase 1: Extract `buildMemTable` (292 lines) → `builder_memtable.go`
  - [x] Phase 1: Extract reader builders (1,652 lines) → `builder_reader.go`
  - [x] Phase 2: Extract analyze builders (376 lines) → `builder_analyze.go`
  - [x] Phase 2: Extract join builders (706 lines) → `builder_join.go`
  - [x] Phase 3: Extract DDL/admin builders (474 lines) → `builder_ddl_admin.go`
  - [x] Phase 3: Extract sort/window builders (411 lines) → `builder_sort_window.go`
  - [x] Phase 4: Extract agg/project builders (244 lines) → `builder_agg_project.go`
  - [x] Phase 4: Extract CTE/misc builders (315 lines) → `builder_cte_misc.go`
  - [x] Phase 4: Extract statement builders (601 lines) → `builder_stmt.go`
  - [x] Phase 4: Extract UnionScan builders (236 lines) → `builder_union_scan.go`

- [ ] **Hash Join V1 deprecation** - Complete V2 for all join types, remove V1
  - Files: `pkg/executor/join/hash_join_v1.go` (1,458 lines), `hash_join_v2.go` (1,538 lines)
  - Target: Single implementation with full join type coverage

- [x] **Hot-path allocation fixes** - Pool iterators, pre-allocate slices
  - [x] `pkg/executor/select.go:259` - chunk iterator recreation per Next() → cached on struct
  - [-] `pkg/executor/adapter.go:122-157` - ResultField slice per Fields() call (already cached, no action needed)
  - [x] `pkg/executor/select.go:728-729` - selected slice re-init → reuse backing array
  - [x] `pkg/executor/internal/exec/executor.go:457` - reflect.TypeOf().String() per Next() → cached in sync.Map
  - [x] `pkg/expression/builtin_string_vec.go` - 17 []rune allocations replaced with utf8 operations
  - [x] `pkg/expression/builtin_string.go` - 16 []rune allocations replaced with utf8 operations
  - [x] `pkg/expression/builtin_encryption.go` - 1 []rune allocation replaced

- [x] **Panic elimination in production code** - Replace with error returns
  - [x] 8 runtime panics replaced: builder.go, analyze.go, index_merge_reader.go,
        aggfuncs/func_value.go, encode.go, rule_partition_processor.go, txn_info.go, summary.go
  - [-] `pkg/kv/key.go` - 6 panics are intentional interface contract assertions (Handle)
  - [-] `pkg/session/txnmanager.go` - panic is inside failpoint (test-only)
  - [-] `pkg/server/http_status.go` - startup-time assertions (acceptable)

## P1 - High Priority

- [x] **session.go decomposition** - Extract transaction, variable, privilege management
  - File: `pkg/session/session.go` (5,558 → 1,087 lines, 80% reduction)
  - Target: <1000 lines per file with clear sub-package boundaries
  - [x] `session_txn.go` (1,086 lines) - transaction commit/rollback, retry logic, txn context, pipelined DML
  - [x] `session_bootstrap.go` (955 lines) - bootstrap, session factory, domain init, session creation
  - [x] `session_auth.go` (413 lines) - authentication, privilege validation
  - [x] `session_execute.go` (836 lines) - statement execution, prepared stmts
  - [x] `session_parse.go` (546 lines) - SQL parsing, process info, internal exec
  - [x] `session_restricted.go` (287 lines) - restricted SQL execution
  - [x] `session_logging.go` (387 lines) - query logging, metrics, telemetry
  - [x] `session_states.go` (212 lines) - encode/decode session states
  - [x] `session_sysvar.go` (157 lines) - system variable management

- [x] **domain.go decomposition** - Extract subsystem managers
  - File: `pkg/domain/domain.go` (3,023 → 997 lines, 67% reduction)
  - Target: Composable service managers
  - [x] `domain_stats.go` (723 lines) - statistics worker lifecycle, historical stats, analyze workers
  - [x] `domain_privilege.go` (467 lines) - privilege events, binding management, sysvar cache, notifications
  - [x] `domain_serverid.go` (356 lines) - server ID acquisition, renewal, keeper loop
  - [x] `domain_workers.go` (285 lines) - telemetry, plan cache, TTL, workload learning workers
  - [x] `domain_disttask.go` (208 lines) - distributed task framework loop
  - [x] `domain_infra.go` (192 lines) - log backup advancer, replica read check loop

- [x] **Planner-executor dependency break** - Remove executor imports from planner
  - Moved `pkg/executor/join/joinversion` → `pkg/util/joinversion`
  - Updated all 11 Go importers; BUILD.bazel files need `make bazel_prepare`

- [ ] **Cost model unification** - Deprecate one of ver1/ver2
  - Files: `pkg/planner/core/plan_cost_ver1.go`, `plan_cost_ver2.go`
  - Target: Single cost model

- [x] **SessionVars decomposition** - Split 350+ field mega-struct
  - File: `pkg/sessionctx/variable/session.go` (3,853 lines)
  - Target: Grouped sub-structs by concern
  - [x] Phase 1: Extracted `TiFlashVars` (27 fields) and `CostModelFactors` (28 fields) as embedded sub-structs
  - [x] Phase 2: Extracted `PlanCacheVars` (13 fields) and `OptimizerVars` (32 fields) as embedded sub-structs
  - [x] Phase 3a: Extracted `StatsVars` (16 statistics-related fields) as embedded sub-struct
  - [x] Phase 3b: Extracted `TransactionVars` (16 transaction-related fields) as embedded sub-struct
  - [x] Phase 4: Extracted `ExecutionVars` (11 execution-related fields) as embedded sub-struct

- [x] **planbuilder.go decomposition** - Extract statement-specific plan builders
  - File: `pkg/planner/core/planbuilder.go` (6,518 → 2,116 lines, 68% reduction)
  - Target: Focused files per statement type
  - [x] `planbuilder_analyze.go` (1,296 lines) - ANALYZE statement handlers
  - [x] `planbuilder_insert.go` (884 lines) - INSERT/LOAD/IMPORT handlers
  - [x] `planbuilder_admin.go` (570 lines) - ADMIN statements, index lookup
  - [x] `planbuilder_show.go` (539 lines) - SHOW/Simple/Grant/Revoke
  - [x] `planbuilder_split.go` (449 lines) - split/distribute, stats lock
  - [x] `planbuilder_ddl.go` (368 lines) - DDL privilege checks
  - [x] `planbuilder_bind.go` (343 lines) - SQL binding operations
  - [x] `planbuilder_explain.go` (247 lines) - EXPLAIN/TRACE statements

- [x] **logical_plan_builder.go decomposition** - Extract functional builder groups
  - File: `pkg/planner/core/logical_plan_builder.go` (7,362 → 3,897 lines, 47% reduction)
  - Target: Focused files per functional area
  - [x] `logical_plan_builder_window.go` (753 lines) - window function builders
  - [x] `logical_plan_builder_dml.go` (903 lines) - UPDATE/DELETE plan builders
  - [x] `logical_plan_builder_cte.go` (368 lines) - CTE plan builders
  - [x] `logical_plan_builder_setops.go` (448 lines) - UNION/INTERSECT/EXCEPT builders
  - [x] `logical_plan_builder_join.go` (394 lines) - JOIN plan builders
  - [x] `logical_plan_builder_datasource.go` (804 lines) - data source, view, memtable builders

- [x] **ddl/partition.go decomposition** - Extract partition operation handlers
  - File: `pkg/ddl/partition.go` (5,358 → 3,155 lines, 41% reduction)
  - Target: Focused files per partition operation
  - [x] `partition_reorganize.go` (1,185 lines) - onReorganizePartition, reorgPartitionWorker, helpers
  - [x] `partition_exchange.go` (725 lines) - onExchangeTablePartition, checkExchange*, buildCheckSQL*
  - [x] `partition_truncate.go` (411 lines) - onTruncateTablePartition, replaceTruncatePartitions

- [x] **executor/infoschema_reader.go decomposition** - Extract retriever types
  - File: `pkg/executor/infoschema_reader.go` (4,213 → 2,990 lines, 29% reduction)
  - Target: Focused files per retriever/functional area
  - [x] `infoschema_reader_lock.go` (492 lines) - tidbTrxTable, dataLockWaits, deadlocks retrievers
  - [x] `infoschema_reader_tiflash.go` (277 lines) - TiFlashSystemTableRetriever
  - [x] `infoschema_reader_resource.go` (569 lines) - attributes, policies, resources, keywords, plan cache

- [x] **ddl/index.go decomposition** - Extract index operation handlers
  - File: `pkg/ddl/index.go` (4,140 → 1,062 lines, 74% reduction)
  - Target: Focused files per index operation area
  - [x] `index_dist_task.go` (418 lines) - distributed task execution, param tuning, row size estimation
  - [x] `index_backfill_worker.go` (619 lines) - add-index txn worker, batch unique check, ingest write
  - [x] `index_columnar.go` (261 lines) - TiFlash columnar index creation and progress monitoring
  - [x] `index_analyze.go` (289 lines) - post-index-creation analyze workflow
  - [x] `index_build_info.go` (518 lines) - buildIndexColumns, BuildIndexInfo, column validation
  - [x] `index_drop.go` (318 lines) - onDropIndex, checkDropIndex, rename/visibility checks
  - [x] `index_reorg_dispatch.go` (494 lines) - reorg initialization, backfill type selection, ingest dispatch
  - [x] `index_partition_reorg.go` (423 lines) - partition iteration, cleanup workers, global index cleanup

- [x] **executor/simple.go decomposition** - Extract user and role management
  - File: `pkg/executor/simple.go` (3,198 → 1,064 lines, 67% reduction)
  - Target: Focused files per functional area
  - [x] `simple_user.go` (1,296 lines) - user creation, alteration, password management
  - [x] `simple_role.go` (917 lines) - role grant/revoke, set default/active role, rename/drop user

- [x] **executor/show.go decomposition** - Extract create statements and region ops
  - File: `pkg/executor/show.go` (2,825 → 1,412 lines, 50% reduction)
  - Target: Focused files per functional area
  - [x] `show_create.go` (714 lines) - SHOW CREATE TABLE/VIEW/DATABASE/SEQUENCE/POLICY/RESOURCE GROUP
  - [x] `show_region.go` (771 lines) - regions, distributions, import jobs, session states, builtins

- [x] **planner/core/find_best_task.go decomposition** - Extract property matching and scan conversion
  - File: `pkg/planner/core/find_best_task.go` (3,030 → 1,118 lines, 63% reduction)
  - Target: Focused files per functional area
  - [x] `find_best_task_property.go` (1,056 lines) - skyline pruning, candidate comparison, property matching
  - [x] `find_best_task_scan.go` (915 lines) - index/table/point-get scan conversion

- [x] **planner/core/exhaust_physical_plans.go decomposition** - Extract MPP join, hints, and index join inner
  - File: `pkg/planner/core/exhaust_physical_plans.go` (2,892 → 1,399 lines, 52% reduction)
  - Target: Focused files per functional area
  - [x] `exhaust_physical_plans_index_join_inner.go` (741 lines) - inner side task building for index joins
  - [x] `exhaust_physical_plans_hints.go` (382 lines) - hint recording, prefer-task logic, force/filter hints
  - [x] `exhaust_physical_plans_mpp_join.go` (367 lines) - MPP/TiFlash BCJ and shuffle join enumeration

- [x] **planner/core/expression_rewriter.go decomposition** - Extract subquery handling, visitor leave, and column resolution
  - File: `pkg/planner/core/expression_rewriter.go` (2,792 → 479 lines, 83% reduction)
  - Target: Focused files per functional area
  - [x] `expression_rewriter_subquery.go` (1,049 lines) - subquery plan construction (Enter, semi-apply, scalar, EXISTS, IN)
  - [x] `expression_rewriter_leave.go` (1,089 lines) - Leave visitor, AST-to-expression conversions
  - [x] `expression_rewriter_column.go` (289 lines) - column name resolution, DEFAULT expression evaluation

- [x] **server/conn.go decomposition** - Extract handshake/auth and query handling
  - File: `pkg/server/conn.go` (2,819 → 1,134 lines, 60% reduction)
  - Target: Focused files per functional area
  - [x] `conn_handshake.go` (869 lines) - handshake, authentication, session opening
  - [x] `conn_query.go` (913 lines) - query handling, result set writing

- [x] **expression/builtin_time.go decomposition** - Extract time function groups
  - File: `pkg/expression/builtin_time.go` (7,260 → 229 lines, 97% reduction)
  - [x] `builtin_time_extract.go` (956 lines) - date field extractors (HOUR, MONTH, WEEK, YEAR, etc.)
  - [x] `builtin_time_current.go` (817 lines) - SYSDATE, NOW, CURRENT_DATE/TIME, UTC_*, EXTRACT
  - [x] `builtin_time_arith.go` (1,392 lines) - baseDateArithmetical, DATE_ADD/DATE_SUB
  - [x] `builtin_time_timestamp.go` (620 lines) - TIMESTAMPDIFF, UNIX_TIMESTAMP, TIMESTAMP
  - [x] `builtin_time_addtime.go` (1,564 lines) - ADDTIME/SUBTIME, CONVERT_TZ, MAKE_DATE/TIME
  - [x] `builtin_time_diff.go` (597 lines) - date/dateLiteral/dateDiff, timeDiff with 8 typed sigs
  - [x] `builtin_time_format.go` (722 lines) - fromUnixTime, getFormat, strToDate, sysDate, addTime, timeFormat
  - [x] `builtin_time_misc.go` (585 lines) - toDays, toSeconds, utcTime, lastDay, tidbParseTso, tidbBoundedStaleness

- [x] **expression/builtin_string.go decomposition** - Extract string function groups
  - File: `pkg/expression/builtin_string.go` (4,421 → 175 lines, 96% reduction)
  - [x] `builtin_string_basic.go` (821 lines) - LENGTH, ASCII, CONCAT, LEFT, RIGHT, REPEAT, etc.
  - [x] `builtin_string_search.go` (921 lines) - STRCMP, REPLACE, CONVERT, SUBSTRING, LOCATE, HEX
  - [x] `builtin_string_trim_pad.go` (1,193 lines) - TRIM, LPAD, RPAD, CHAR, FIND_IN_SET, FIELD, OCT
  - [x] `builtin_string_misc.go` (1,425 lines) - ORD, QUOTE, FORMAT, BASE64, INSERT, WEIGHT_STRING

- [x] **expression/builtin_compare.go decomposition** - Extract comparison function groups
  - File: `pkg/expression/builtin_compare.go` (3,542 → 625 lines, 82% reduction)
  - [x] `builtin_compare_greatest_least.go` (1,025 lines) - GREATEST, LEAST, INTERVAL
  - [x] `builtin_compare_class.go` (847 lines) - compareFunctionClass, type resolution
  - [x] `builtin_compare_ops.go` (1,121 lines) - typed comparison operator signatures

- [x] **expression/builtin_cast.go decomposition** - Extract cast function groups
  - File: `pkg/expression/builtin_cast.go` (3,004 → 973 lines, 67% reduction)
  - [x] `builtin_cast_numeric.go` (771 lines) - Int/Real/Decimal cast signatures
  - [x] `builtin_cast_string_time_json.go` (849 lines) - String/Time/Duration/JSON cast signatures
  - [x] `builtin_cast_builders.go` (482 lines) - BuildCastFunction, WrapWithCast utilities

- [x] **infoschema/tables.go decomposition** - Extract column definitions
  - File: `pkg/infoschema/tables.go` (2,911 → 1,472 lines, 49% reduction)
  - [x] `tables_cols_standard.go` (686 lines) - MySQL-compatible IS column definitions
  - [x] `tables_cols_tidb.go` (797 lines) - TiDB-specific cluster/monitoring columns

- [x] **executor/adapter.go decomposition** - Extract pessimistic, observability, summary
  - File: `pkg/executor/adapter.go` (2,417 → 944 lines, 61% reduction)
  - [x] `adapter_pessimistic.go` (510 lines) - pessimistic DML, lock error recovery
  - [x] `adapter_observe.go` (622 lines) - FinishExecuteStmt, slow log, metrics
  - [x] `adapter_summary.go` (460 lines) - statement summary, plan cache, Top SQL

- [x] **expression/builtin_string_vec.go decomposition** - Extract vectorized string function groups
  - File: `pkg/expression/builtin_string_vec.go` (3,243 → 0 lines, fully replaced)
  - [x] `builtin_string_vec_basic.go` (505 lines) - basic ops (LOWER, UPPER, CONCAT, LOCATE, HEX)
  - [x] `builtin_string_vec_trim_pad.go` (864 lines) - trim/pad ops (LTRIM, RTRIM, LPAD, INSERT, CONVERT)
  - [x] `builtin_string_vec_misc.go` (1,932 lines) - remaining ops (FORMAT, CHAR, SUBSTRING, TRANSLATE)

- [x] **expression/builtin_time_vec.go decomposition** - Extract vectorized time function groups
  - File: `pkg/expression/builtin_time_vec.go` (3,010 → 0 lines, fully replaced)
  - [x] `builtin_time_vec_field.go` (1,034 lines) - field extractors and formatters
  - [x] `builtin_time_vec_convert.go` (978 lines) - conversion functions
  - [x] `builtin_time_vec_calc.go` (1,053 lines) - calculations and arithmetic

- [x] **expression/util.go decomposition** - Extract utility function groups
  - File: `pkg/expression/util.go` (2,374 → 0 lines, fully replaced)
  - [x] `util_column.go` (689 lines) - column extraction and substitution
  - [x] `util_transform.go` (637 lines) - expression transforms (NOT/cast elimination, DNF filters)
  - [x] `util_misc.go` (584 lines) - row/constant/format utilities and plan cache helpers
  - [x] `util_digest.go` (543 lines) - SQL digest retrieval and binary parameter parsing

- [x] **expression/builtin_math.go decomposition** - Extract math function groups
  - File: `pkg/expression/builtin_math.go` (2,219 → 116 lines, 95% reduction)
  - [x] `builtin_math_rounding.go` (811 lines) - ABS, ROUND, CEIL, FLOOR
  - [x] `builtin_math_algebra.go` (649 lines) - LOG, RAND, POW, CONV, CRC32, SIGN, SQRT
  - [x] `builtin_math_trig.go` (743 lines) - trigonometric functions and TRUNCATE

- [x] **planner/core/task.go decomposition** - Extract task attachment functions
  - File: `pkg/planner/core/task.go` (2,241 → 30 lines, 99% reduction)
  - [x] `task_join.go` (575 lines) - join operator attachments
  - [x] `task_limit_topn.go` (870 lines) - limit, sort, and TopN push-down
  - [x] `task_operators.go` (838 lines) - projection, aggregation, window, CTE, sequence

- [x] **server/handler/tikvhandler/tikv_handler.go decomposition** - Extract handler groups
  - File: `tikv_handler.go` (2,206 → 0 lines, fully replaced)
  - [x] `tikv_handler_settings.go` (738 lines) - handler types, constructors, settings, flash replica
  - [x] `tikv_handler_schema.go` (706 lines) - schema storage, schema handler, table handler, DDL
  - [x] `tikv_handler_region.go` (848 lines) - region, MVCC, server info, profile, misc handlers

- [x] **planner/core/preprocess.go decomposition** - Extract validation and resolution
  - File: `pkg/planner/core/preprocess.go` (2,197 → 707 lines, 68% reduction)
  - [x] `preprocess_check.go` (990 lines) - grammar validation and column/index checks
  - [x] `preprocess_resolve.go` (557 lines) - table resolution, stale read, alias checker

- [x] **types/datum.go decomposition** - Extract compare, convert, and factory functions
  - File: `pkg/types/datum.go` (2,760 → 1,091 lines, 60% reduction)
  - [x] `datum_compare.go` (349 lines) - Equals, Compare, and all type-specific comparers
  - [x] `datum_convert.go` (883 lines) - ConvertTo dispatcher and all type-specific converters
  - [x] `datum_factory.go` (518 lines) - NewDatum constructors, MakeDatums, SortDatums, utilities

- [x] **tablecodec/tablecodec.go decomposition** - Extract key/row, index, and genindex functions
  - File: `pkg/tablecodec/tablecodec.go` (2,050 → 74 lines, 96% reduction)
  - [x] `tablecodec_keyrow.go` (654 lines) - EncodeRowKey, DecodeRecordKey, EncodeRow, DecodeRow, Unflatten
  - [x] `tablecodec_index.go` (539 lines) - EncodeIndexSeekKey, DecodeIndexKV, DecodeIndexHandle, GenTablePrefix
  - [x] `tablecodec_genindex.go` (856 lines) - GenIndexKey, TempIndexValue, GenIndexValuePortal, TruncateIndexValues

- [x] **expression/builtin_json.go decomposition** - Extract JSON function groups
  - File: `pkg/expression/builtin_json.go` (2,138 → 76 lines, 96% reduction)
  - [x] `builtin_json_basic.go` (742 lines) - jsonType, jsonExtract, jsonUnquote, jsonSet, jsonInsert, jsonRemove, jsonObject, jsonArray
  - [x] `builtin_json_search.go` (722 lines) - jsonContainsPath, jsonMemberOf, jsonContains, jsonOverlaps, jsonValid, jsonArrayAppend
  - [x] `builtin_json_inspect.go` (665 lines) - jsonPretty, jsonQuote, jsonSearch, jsonDepth, jsonKeys, jsonLength, jsonSchemaValid

- [x] **expression/builtin_miscellaneous.go decomposition** - Extract miscellaneous function groups
  - File: `pkg/expression/builtin_miscellaneous.go` (2,021 → 97 lines, 95% reduction)
  - [x] `builtin_misc_lock.go` (751 lines) - sleep, advisory locks, anyValue, default, inet
  - [x] `builtin_misc_validate.go` (671 lines) - IP validation, lock status, UUID validation, nameConst
  - [x] `builtin_misc_uuid.go` (573 lines) - UUID generation/conversion, vitess hash, tidb shard

- [x] **ddl/modify_column.go decomposition** - Extract reorg, execution, and validation
  - File: `pkg/ddl/modify_column.go` (2,202 → 16 lines, 99% reduction)
  - [x] `modify_column_reorg.go` (899 lines) - helpers, handler, rollback, reorg functions
  - [x] `modify_column_execute.go` (513 lines) - type change execution with state management
  - [x] `modify_column_validate.go` (854 lines) - validation and public API functions

- [x] **expression/builtin_cast_vec.go decomposition** - Extract vectorized cast functions
  - File: `pkg/expression/builtin_cast_vec.go` (2,080 → 16 lines, 99% reduction)
  - [x] `builtin_cast_vec_part1.go` (742 lines) - Int/Real/Time/Duration cast vec functions
  - [x] `builtin_cast_vec_part2.go` (669 lines) - Real/String/Decimal/JSON cast vec functions
  - [x] `builtin_cast_vec_part3.go` (722 lines) - Duration/Decimal/String/JSON cast vec functions

- [x] **planner/core/rule/rule_partition_processor.go decomposition** - Extract partition processing
  - File: `pkg/planner/core/rule/rule_partition_processor.go` (2,144 → 145 lines, 93% reduction)
  - [x] `rule_partition_hash_list.go` (784 lines) - hash/key partition pruning and list partition pruning
  - [x] `rule_partition_range.go` (1,269 lines) - range types, range pruning, range columns, hints

- [x] **planner/core/operator/logicalop/logical_join.go decomposition** - Extract join planning functions
  - File: `logical_join.go` (2,172 → 105 lines, 95% reduction)
  - [x] `logical_join_stats.go` (655 lines) - predicate pushdown, pruning, key building, stats
  - [x] `logical_join_fd.go` (665 lines) - FD extraction, correlated cols, join keys, decorrelate
  - [x] `logical_join_rewrite.go` (837 lines) - NDV, preferences, condition extraction, rewrite

- [x] **meta/meta.go decomposition** - Extract CRUD, query, and history functions
  - File: `pkg/meta/meta.go` (2,064 → 206 lines, 90% reduction)
  - Target: Focused files per functional area
  - [x] `meta_crud.go` (850 lines) - CRUD operations for tables, databases, policies, sequences
  - [x] `meta_query.go` (608 lines) - listing/querying metadata (tables, databases, policies)
  - [x] `meta_history.go` (482 lines) - DDL history and schema diff operations

- [x] **executor/index_merge_reader.go decomposition** - Extract worker and intersection logic
  - File: `pkg/executor/index_merge_reader.go` (2,057 → 599 lines, 71% reduction)
  - Target: Focused files per functional area
  - [x] `index_merge_reader_worker.go` (731 lines) - index merge worker, partial task execution
  - [x] `index_merge_reader_intersect.go` (807 lines) - intersection logic, hash-based intersection

- [x] **types/mydecimal.go decomposition** - Extract shift/round, convert, and arithmetic
  - File: `pkg/types/mydecimal.go` (2,515 → 646 lines, 74% reduction)
  - Target: Focused files per functional area
  - [x] `mydecimal_shift_round.go` (539 lines) - shift, round, truncate operations
  - [x] `mydecimal_convert.go` (588 lines) - string/int/float conversion
  - [x] `mydecimal_arithmetic.go` (804 lines) - add, subtract, multiply, divide

- [x] **store/copr/coprocessor.go decomposition** - Extract handler, batch/cache, task, iter, lifecycle, ratelimit
  - File: `pkg/store/copr/coprocessor.go` (2,904 → 229 lines, 92% reduction)
  - Target: Focused files per functional area
  - [x] `coprocessor_handler.go` (679 lines) - handleTask, handleTaskOnce, logTimeCopTask
  - [x] `coprocessor_batch_cache.go` (413 lines) - handleBatchCopResponse, handleLockErr, buildCacheKey
  - [x] `coprocessor_task.go` (718 lines) - copTask, buildCopTasks, taskBuilder, legacy/batch builders
  - [x] `coprocessor_iter.go` (199 lines) - copIterator, copIteratorWorker, copResponse, CopInfo
  - [x] `coprocessor_iter_lifecycle.go` (639 lines) - open, run, Next, Close, lite worker, retry/error
  - [x] `coprocessor_ratelimit.go` (244 lines) - rateLimitAction, copErrorResponse, protocol helpers

- [x] **types/time.go decomposition** - Extract parsing, StrToDate, and interval functions
  - File: `pkg/types/time.go` (3,546 → 917 lines, 74% total reduction)
  - Target: Focused files per functional area
  - [x] `time_parse.go` (574 lines) - TimestampDiff, ParseDateFormat, GetTimezone, parseDatetime
  - [x] `time_str_to_date.go` (644 lines) - StrToDate, format token parsing, DateFSP
  - [x] `time_interval.go` (473 lines) - ExtractDatetimeNum, ParseDurationValue, IsClockUnit

- [x] **executor/distsql.go decomposition** - Extract index and table worker logic
  - File: `pkg/executor/distsql.go` (1,988 → 1,170 lines, 41% reduction)
  - Target: Focused files per functional area
  - [x] `distsql_index_worker.go` (411 lines) - indexWorker struct, fetchHandles, buildTableTask
  - [x] `distsql_table_worker.go` (490 lines) - tableWorker struct, executeTask, IndexLookUpRunTimeStats

- [x] **statistics/histogram.go decomposition** - Extract merge and estimation logic
  - File: `pkg/statistics/histogram.go` (1,993 → 1,244 lines, 38% reduction)
  - Target: Focused files per functional area
  - [x] `histogram_merge.go` (545 lines) - bucket4Merging, MergePartitionHist2GlobalHist
  - [x] `histogram_estimation.go` (259 lines) - calculateLeftOverlapPercent, OutOfRangeRowCount

- [x] **util/codec/codec.go decomposition** - Extract serialize, hash, and decode logic
  - File: `pkg/util/codec/codec.go` (1,932 → 666 lines, 66% reduction)
  - Target: Focused files per functional area
  - [x] `codec_serialize_keys.go` (596 lines) - EncodeKey, EncodeValue, EncodeBytesExt, encodeInt/Uint/Float
  - [x] `codec_hash_chunk.go` (379 lines) - HashCode, HashChunkRow, HashGroupKey, HashChunkColumns
  - [x] `codec_decode.go` (372 lines) - Decode, DecodeOne, DecodeRange, decode typed helpers

- [x] **privilege/privileges/cache.go decomposition** - Extract decoder, grants, and handle logic
  - File: `pkg/privilege/privileges/cache.go` (2,229 → 1,165 lines, 48% reduction)
  - Target: Focused files per functional area
  - [x] `cache_decoder.go` (444 lines) - decodeUserTableRow, decodeGlobalPrivTableRow, decodeColumnPrivTableRow
  - [x] `cache_grants.go` (545 lines) - showGrants, userPrivString, globalPrivString, dbPrivString
  - [x] `cache_handle.go` (165 lines) - Handle type and core privilege handle operations

- [x] **table/tables/tables.go decomposition** - Extract add/update/remove record and sequence logic
  - File: `pkg/table/tables/tables.go` (1,962 → 1,063 lines, 46% reduction)
  - Target: Focused files per functional area
  - [x] `tables_add_record.go` (330 lines) - AddRecord, addRecord, addIndices, genIndexKeyStrs
  - [x] `tables_update_record.go` (228 lines) - UpdateRecord, updateRecord, rebuildUpdateRecordIndices
  - [x] `tables_remove_record.go` (157 lines) - RemoveRecord, removeRecord, removeRowData, removeRowIndices
  - [x] `tables_sequence.go` (178 lines) - sequenceCommon struct and methods

- [x] **expression/builtin_other.go decomposition** - Extract IN, user var, and VALUES functions
  - File: `pkg/expression/builtin_other.go` (1,950 → 204 lines, 89% reduction)
  - Target: Focused files per functional area
  - [x] `builtin_in.go` (857 lines) - inFunctionClass with all 8 type-specific signatures
  - [x] `builtin_user_var.go` (516 lines) - SET/GET user variable functions
  - [x] `builtin_values.go` (359 lines) - VALUES() function with all type variants

- [x] **table/tables/partition.go (tables pkg) decomposition** - Extract record mutation and location
  - File: `pkg/table/tables/partition.go` (2,195 → 1,457 lines, 34% reduction)
  - Target: Focused files per functional area
  - [x] `partition_record.go` (431 lines) - AddRecord, UpdateRecord, RemoveRecord for partitioned tables
  - [x] `partition_location.go` (306 lines) - partition location/routing, GetPartition, GetPartitionByRow

- [x] **ddl/create_table.go decomposition** - Extract validation, constraints, and hidden columns
  - File: `pkg/ddl/create_table.go` (1,764 → 1,077 lines, 39% reduction)
  - Target: Focused files per functional area
  - [x] `create_table_validate.go` (280 lines) - table info validation, generated columns, TiFlash checks
  - [x] `create_table_constraints.go` (243 lines) - constraint building, naming, dedup, column flags
  - [x] `create_table_hidden.go` (254 lines) - hidden columns, PK helpers, clustered index, view info

- [x] **ddl/ddl.go decomposition** - Extract table lock and job control
  - File: `pkg/ddl/ddl.go` (1,702 → 1,171 lines, 31% reduction)
  - Target: Focused files per functional area
  - [x] `ddl_table_lock.go` (154 lines) - dead table lock cleanup, MDL switching, async commit delay
  - [x] `ddl_job_control.go` (409 lines) - job cancel/pause/resume, batch processing, query/iteration

- [x] **types/time.go further decomposition** - Extract duration and conversion functions
  - File: `pkg/types/time.go` (1,932 → 917 lines, 53% reduction)
  - Target: Focused files per functional area
  - [x] `time_duration.go` (610 lines) - Duration struct, methods, formatting, parsing, helpers
  - [x] `time_convert.go` (457 lines) - time parsing from numbers/strings/floats/decimals

- [x] **planner/core/memtable_predicate_extractor.go decomposition** - Extract helper methods
  - File: `pkg/planner/core/memtable_predicate_extractor.go` (1,740 → 1,089 lines, 37% reduction)
  - Target: Separate helper from extractor types
  - [x] `memtable_predicate_helper.go` (683 lines) - extractHelper struct and all methods

- [x] **infoschema/infoschema_v2.go decomposition** - Extract reset and builder functions
  - File: `pkg/infoschema/infoschema_v2.go` (1,788 → 1,281 lines, 28% reduction)
  - Target: Focused files per functional area
  - [x] `infoschema_v2_reset.go` (246 lines) - btree reset-before-full-load functions
  - [x] `infoschema_v2_builder.go` (309 lines) - builder integration functions (apply*, bundle update)

- [x] **store/mockstore/unistore/tikv/mvcc.go decomposition** - Extract pessimistic lock and prewrite
  - File: `pkg/store/mockstore/unistore/tikv/mvcc.go` (2,182 → 1,176 lines, 46% reduction)
  - Target: Focused files per transaction phase
  - [x] `mvcc_pessimistic_lock.go` (566 lines) - pessimistic lock, rollback, heartbeat, status checks
  - [x] `mvcc_prewrite.go` (505 lines) - optimistic/pessimistic prewrite, flush, 1PC

- [x] **executor/insert_common.go decomposition** - Extract auto-ID, duplicate key, and runtime stat
  - File: `pkg/executor/insert_common.go` (1,587 → 816 lines, 49% reduction)
  - Target: Focused files per functional area
  - [x] `insert_autoid.go` (388 lines) - auto-increment, auto-random, and implicit row ID handling
  - [x] `insert_dupkey.go` (310 lines) - duplicate key detection, batch check/insert, row removal
  - [x] `insert_runtime_stat.go` (149 lines) - InsertRuntimeStat type and methods

- [x] **executor/slow_query.go decomposition** - Extract reverse scanner, column factories, and file ops
  - File: `pkg/executor/slow_query.go` (1,554 → 820 lines, 47% reduction)
  - Target: Focused files per functional area
  - [x] `slow_query_reverse_scanner.go` (289 lines) - reverse log scanning for dashboard queries
  - [x] `slow_query_column.go` (186 lines) - column value factory functions and time parsing
  - [x] `slow_query_file.go` (350 lines) - log file discovery, time detection, and runtime stats

- [x] **store/copr/batch_coprocessor.go decomposition** - Extract task balancing and execution
  - File: `pkg/store/copr/batch_coprocessor.go` (1,727 → 1,213 lines, 30% reduction)
  - Target: Focused files per functional area
  - [x] `batch_coprocessor_balance.go` (192 lines) - region balancing between TiFlash stores
  - [x] `batch_coprocessor_task.go` (382 lines) - task execution, response handling, and PD dispatch

- [x] **types/field_type.go decomposition** - Extract merge rules and modify functions
  - File: `pkg/types/field_type.go` (1,626 → 380 lines, 77% reduction)
  - Target: Focused files per functional area
  - [x] `field_type_merge_rules.go` (1,078 lines) - mergeFieldType, mergeTypeFlag, fieldTypeMergeRules table
  - [x] `field_type_modify.go` (166 lines) - SetBinChsClnFlag, CheckModifyTypeCompatible, checkTypeChangeSupported

- [x] **sessionctx/stmtctx/stmtctx.go decomposition** - Extract counters, warnings, and stats info
  - File: `pkg/sessionctx/stmtctx/stmtctx.go` (1,532 → 1,093 lines, 29% reduction)
  - Target: Focused files per functional area
  - [x] `stmtctx_counters.go` (223 lines) - row counters, warning methods, reset for retry
  - [x] `stmtctx_stats.go` (206 lines) - UsedStatsInfoForTable, UsedStatsInfo, StatsLoadResult, stmtLabel

- [x] **util/ranger/detacher.go decomposition** - Extract shard index functions
  - File: `pkg/util/ranger/detacher.go` (1,600 → 1,210 lines, 24% reduction)
  - Target: Focused files per functional area
  - [x] `detacher_shard.go` (389 lines) - AddGcColumnCond, AddExpr4EqAndInCondition, IsValidShardIndex, etc.

- [x] **executor/importer/import.go decomposition** - Extract plan options
  - File: `pkg/executor/importer/import.go` (1,849 → 1,490 lines, 19% reduction)
  - Target: Focused files per functional area
  - [x] `import_options.go` (358 lines) - initDefaultOptions, initOptions, adjustOptions, initParameters

- [x] **lightning/checkpoints/checkpoints.go decomposition** - Extract MySQL and File checkpoint ops
  - File: `pkg/lightning/checkpoints/checkpoints.go` (2,005 → 1,548 lines, 23% reduction)
  - Target: Focused files per functional area
  - [x] `checkpoint_mysql_ops.go` (317 lines) - MySQL checkpoint management methods
  - [x] `checkpoint_file_ops.go` (138 lines) - File checkpoint management methods

- [ ] **DDL schema version lock** - Reduce global mutex scope
  - File: `pkg/ddl/ddl.go:387-445`
  - Target: Per-job or fine-grained locking

## P2 - Medium Priority

- [ ] **pkg/util/ reorganization** - Re-organize 111+ subdirectories
- [x] **String function rune optimization** - Use utf8 index-based operations
- [ ] **Goroutine pool for executors** - Aggregate, sort, projection worker pools
- [ ] **Datum type optimization** - Eliminate interface{} fallback, use typed union
- [ ] **Expression clone generation** - Auto-generate all Clone() methods
- [ ] **Aggregate executor map reduction** - Reduce O(P*F) map proliferation
- [ ] **Concurrent hash map adaptive sharding** - Replace fixed ShardCount=320
- [ ] **Statistics memoization in planner** - Cache selectivity calculations

## P3 - Low Priority

- [ ] **Remove old cascades code** - `pkg/planner/cascades/old/`
- [ ] **Executor interface split** - Lifecycle, Execution, Debug interfaces
- [ ] **Context propagation** - Replace 5,266 Background()/TODO() calls
- [x] **DDL executor.go split** - `pkg/ddl/executor.go` (7,201 → 1,113 lines, 85% reduction)
  - [x] Phase 1 (prior): 8 extracted files (72% reduction)
  - [x] Phase 2: `executor_lock.go` (253 lines) - LockTables, UnlockTables, AlterTableMode, CleanupTableLock
  - [x] Phase 2: `executor_placement.go` (252 lines) - AlterIndexVisibility, AlterTableAttributes, partition placement
  - [x] Phase 2: `executor_job.go` (449 lines) - DoDDLJob, DoDDLJobWrapper, job polling, reorg meta helpers
- [ ] **sessionctx.Context interface** - Split 76-method interface
- [ ] **nolint audit** - Investigate 738 suppressed warnings

---

## Completed Items

(Move items here when done, with date and PR link)

- [x] **SelectLockExec iterator caching** - 2026-02-17 - Cache `chunk.Iterator4Chunk` on struct to avoid heap allocation per `Next()` call
- [x] **SelectionExec selected slice reuse** - 2026-02-17 - Reuse `[]bool` backing array across `Open()`/`Close()` cycles instead of re-allocating
- [x] **exec.Next reflect.TypeOf caching** - 2026-02-17 - Cache `reflect.TypeOf(e).String()+".Next"` in `sync.Map` to avoid per-call reflection + string concatenation
- [x] **String function rune optimization** - 2026-02-17 - Replace 27 `[]rune(str)` allocations with `utf8.RuneCountInString` and `utf8.DecodeRuneInString` for zero-copy operations in LEFT, RIGHT, LOCATE, SUBSTR, INSERT, MID, LPAD, RPAD, CHAR_LENGTH
- [x] **Planner panic elimination** - 2026-02-17 - Replace `panic("unreachable")` with error return in `exhaustPhysicalPlans`
- [x] **Planner-executor dependency break** - 2026-02-17 - Move `joinversion` package from `executor/join/joinversion` to `util/joinversion`, eliminating planner→executor import
- [x] **Runtime panic elimination (8 panics)** - 2026-02-17 - Replace panics in builder.go, analyze.go, index_merge_reader.go, aggfuncs/func_value.go, encode.go, rule_partition_processor.go, txn_info.go, summary.go
- [x] **Additional rune optimizations (6 patterns)** - 2026-02-17 - SUBSTRING non-vec, INSERT non-vec, Quote, WeightString, ValidatePasswordStrength
- [x] **SessionVars decomposition phase 1** - 2026-02-17 - Extract `TiFlashVars` (27 MPP/TiFlash fields) and `CostModelFactors` (28 cost factor fields) into embedded sub-structs, reducing SessionVars from 315 to ~260 direct fields
- [x] **executor/builder.go split phases 1-2** - 2026-02-17 - Extract into 4 files: `builder_memtable.go` (324 lines), `builder_reader.go` (1,710 lines), `builder_analyze.go` (416 lines), `builder_join.go` (740 lines), reducing builder.go from 6,223 to 3,172 lines (49% reduction)
- [x] **SessionVars decomposition phase 2** - 2026-02-17 - Extract `PlanCacheVars` (13 plan cache fields) and `OptimizerVars` (32 optimizer fields) into embedded sub-structs
- [x] **executor/builder.go split phase 3** - 2026-02-17 - Extract `builder_ddl_admin.go` (474 lines) and `builder_sort_window.go` (411 lines), reducing builder.go from 3,172 to 2,360 lines (62% total reduction)
- [x] **executor/builder.go split phase 4** - 2026-02-17 - Extract `builder_agg_project.go` (244 lines), `builder_cte_misc.go` (315 lines), `builder_stmt.go` (601 lines), and `builder_union_scan.go` (236 lines), reducing builder.go from 2,360 to 1,082 lines (83% total reduction). Split complete: 10 builder files total.
- [x] **SessionVars Phase 3a (StatsVars)** - 2026-02-17 - Extract 16 statistics-related fields (EnableFastAnalyze, AnalyzeVersion, RegardNULLAsPoint, etc.) into embedded `StatsVars` sub-struct
- [x] **SessionVars Phase 3b (TransactionVars)** - 2026-02-17 - Extract 16 transaction-related fields (RetryLimit, LockWaitTimeout, TxnScope, EnableAsyncCommit, etc.) into embedded `TransactionVars` sub-struct
- [x] **session.go decomposition** - 2026-02-17 - Split into 9 focused files: `session_txn.go` (1,086), `session_bootstrap.go` (955), `session_execute.go` (836), `session_parse.go` (546), `session_auth.go` (413), `session_logging.go` (387), `session_restricted.go` (287), `session_states.go` (212), `session_sysvar.go` (157). Reduced session.go from 5,558 to 1,087 lines (80% reduction).
- [x] **SessionVars Phase 4 (ExecutionVars)** - 2026-02-17 - Extract 11 execution-related fields (DMLBatchSize, BatchInsert, BatchDelete, BatchCommit, BulkDMLEnabled, EnableChunkRPC, EnablePaging, EnableReuseChunk, MaxExecutionTime, SelectLimit, StoreBatchSize) into embedded `ExecutionVars` sub-struct. Total: 7 sub-structs, ~143 fields organized.
- [x] **DDL executor.go decomposition** - 2026-02-17 - Split into 8 focused files: `executor_partition.go` (1,084 lines), `executor_index.go` (967 lines), `executor_column.go` (472 lines), `executor_create.go` (565 lines), `executor_table.go` (532 lines), `executor_misc.go` (543 lines), `executor_schema.go` (680 lines), `executor_table_props.go` (683 lines). Reduced executor.go from 7,201 to 1,986 lines (72% reduction).
- [x] **domain.go decomposition** - 2026-02-18 - Split into 6 focused files: `domain_stats.go` (723), `domain_privilege.go` (467), `domain_serverid.go` (356), `domain_workers.go` (285), `domain_disttask.go` (208), `domain_infra.go` (192). Reduced domain.go from 3,023 to 997 lines (67% reduction).
- [x] **planbuilder.go decomposition** - 2026-02-18 - Split into 8 focused files: `planbuilder_analyze.go` (1,296), `planbuilder_insert.go` (884), `planbuilder_admin.go` (570), `planbuilder_show.go` (539), `planbuilder_split.go` (449), `planbuilder_ddl.go` (368), `planbuilder_bind.go` (343), `planbuilder_explain.go` (247). Reduced planbuilder.go from 6,518 to 2,116 lines (68% reduction).
- [x] **logical_plan_builder.go decomposition** - 2026-02-18 - Split into 6 focused files: `logical_plan_builder_window.go` (753), `logical_plan_builder_dml.go` (903), `logical_plan_builder_cte.go` (368), `logical_plan_builder_setops.go` (448), `logical_plan_builder_join.go` (394), `logical_plan_builder_datasource.go` (804). Reduced logical_plan_builder.go from 7,362 to 3,897 lines (47% reduction).
- [x] **ddl/partition.go decomposition** - 2026-02-18 - Split into 3 focused files: `partition_reorganize.go` (1,185), `partition_exchange.go` (725), `partition_truncate.go` (411). Reduced partition.go from 5,358 to 3,155 lines (41% reduction).
- [x] **executor/infoschema_reader.go decomposition** - 2026-02-18 - Split into 3 focused files: `infoschema_reader_lock.go` (492), `infoschema_reader_tiflash.go` (277), `infoschema_reader_resource.go` (569). Reduced infoschema_reader.go from 4,213 to 2,990 lines (29% reduction).
- [x] **ddl/index.go decomposition** - 2026-02-18 - Split into 4 focused files: `index_dist_task.go` (418), `index_backfill_worker.go` (619), `index_columnar.go` (261), `index_analyze.go` (289). Reduced index.go from 4,140 to 2,691 lines (35% reduction).
- [x] **executor/simple.go decomposition** - 2026-02-18 - Split into 2 focused files: `simple_user.go` (1,296), `simple_role.go` (917). Reduced simple.go from 3,198 to 1,064 lines (67% reduction).
- [x] **executor/show.go decomposition** - 2026-02-18 - Split into 2 focused files: `show_create.go` (714), `show_region.go` (771). Reduced show.go from 2,825 to 1,412 lines (50% reduction).
- [x] **planner/core/find_best_task.go decomposition** - 2026-02-18 - Split into 2 focused files: `find_best_task_property.go` (1,056), `find_best_task_scan.go` (915). Reduced find_best_task.go from 3,030 to 1,118 lines (63% reduction).
- [x] **planner/core/exhaust_physical_plans.go decomposition** - 2026-02-18 - Split into 3 focused files: `exhaust_physical_plans_index_join_inner.go` (741), `exhaust_physical_plans_hints.go` (382), `exhaust_physical_plans_mpp_join.go` (367). Reduced exhaust_physical_plans.go from 2,892 to 1,399 lines (52% reduction).
- [x] **planner/core/expression_rewriter.go decomposition** - 2026-02-18 - Split into 3 focused files: `expression_rewriter_subquery.go` (1,049), `expression_rewriter_leave.go` (1,089), `expression_rewriter_column.go` (289). Reduced expression_rewriter.go from 2,792 to 479 lines (83% reduction).
- [x] **server/conn.go decomposition** - 2026-02-18 - Split into 2 focused files: `conn_handshake.go` (869), `conn_query.go` (913). Reduced conn.go from 2,819 to 1,134 lines (60% reduction).
- [x] **expression/builtin_time.go decomposition** - 2026-02-18 - Split into 5 focused files: `builtin_time_extract.go` (956), `builtin_time_current.go` (817), `builtin_time_arith.go` (1,392), `builtin_time_timestamp.go` (620), `builtin_time_addtime.go` (1,564). Reduced builtin_time.go from 7,260 to 2,050 lines (72% reduction).
- [x] **expression/builtin_string.go decomposition** - 2026-02-18 - Split into 4 focused files: `builtin_string_basic.go` (821), `builtin_string_search.go` (921), `builtin_string_trim_pad.go` (1,193), `builtin_string_misc.go` (1,425). Reduced builtin_string.go from 4,421 to 175 lines (96% reduction).
- [x] **expression/builtin_compare.go decomposition** - 2026-02-18 - Split into 3 focused files: `builtin_compare_greatest_least.go` (1,025), `builtin_compare_class.go` (847), `builtin_compare_ops.go` (1,121). Reduced builtin_compare.go from 3,542 to 625 lines (82% reduction).
- [x] **expression/builtin_cast.go decomposition** - 2026-02-18 - Split into 3 focused files: `builtin_cast_numeric.go` (771), `builtin_cast_string_time_json.go` (849), `builtin_cast_builders.go` (482). Reduced builtin_cast.go from 3,004 to 973 lines (67% reduction).
- [x] **infoschema/tables.go decomposition** - 2026-02-18 - Split into 2 focused files: `tables_cols_standard.go` (686), `tables_cols_tidb.go` (797). Reduced tables.go from 2,911 to 1,472 lines (49% reduction).
- [x] **executor/adapter.go decomposition** - 2026-02-18 - Split into 3 focused files: `adapter_pessimistic.go` (510), `adapter_observe.go` (622), `adapter_summary.go` (460). Reduced adapter.go from 2,417 to 944 lines (61% reduction).
- [x] **expression/builtin_string_vec.go decomposition** - 2026-02-18 - Split into 3 focused files: `builtin_string_vec_basic.go` (505), `builtin_string_vec_trim_pad.go` (864), `builtin_string_vec_misc.go` (1,932). Original file fully replaced.
- [x] **expression/builtin_time_vec.go decomposition** - 2026-02-18 - Split into 3 focused files: `builtin_time_vec_field.go` (1,034), `builtin_time_vec_convert.go` (978), `builtin_time_vec_calc.go` (1,053). Original file fully replaced.
- [x] **expression/util.go decomposition** - 2026-02-18 - Split into 4 focused files: `util_column.go` (689), `util_transform.go` (637), `util_misc.go` (584), `util_digest.go` (543). Original file fully replaced.
- [x] **expression/builtin_math.go decomposition** - 2026-02-18 - Split into 3 focused files: `builtin_math_rounding.go` (811), `builtin_math_algebra.go` (649), `builtin_math_trig.go` (743). Reduced builtin_math.go from 2,219 to 116 lines (95% reduction).
- [x] **planner/core/task.go decomposition** - 2026-02-18 - Split into 3 focused files: `task_join.go` (575), `task_limit_topn.go` (870), `task_operators.go` (838). Reduced task.go from 2,241 to 30 lines (99% reduction).
- [x] **server/handler/tikvhandler/tikv_handler.go decomposition** - 2026-02-18 - Split into 3 focused files: `tikv_handler_settings.go` (738), `tikv_handler_schema.go` (706), `tikv_handler_region.go` (848). Original file fully replaced.
- [x] **planner/core/preprocess.go decomposition** - 2026-02-18 - Split into 2 focused files: `preprocess_check.go` (990), `preprocess_resolve.go` (557). Reduced preprocess.go from 2,197 to 707 lines (68% reduction).
- [x] **types/datum.go decomposition** - 2026-02-19 - Split into 3 focused files: `datum_compare.go` (349), `datum_convert.go` (883), `datum_factory.go` (518). Reduced datum.go from 2,760 to 1,091 lines (60% reduction).
- [x] **tablecodec/tablecodec.go decomposition** - 2026-02-19 - Split into 3 focused files: `tablecodec_keyrow.go` (654), `tablecodec_index.go` (539), `tablecodec_genindex.go` (856). Reduced tablecodec.go from 2,050 to 74 lines (96% reduction).
- [x] **expression/builtin_json.go decomposition** - 2026-02-19 - Split into 3 focused files: `builtin_json_basic.go` (742), `builtin_json_search.go` (722), `builtin_json_inspect.go` (665). Reduced builtin_json.go from 2,138 to 76 lines (96% reduction).
- [x] **expression/builtin_time.go further decomposition** - 2026-02-19 - Split remaining 2,050 lines into 3 additional files: `builtin_time_diff.go` (597), `builtin_time_format.go` (722), `builtin_time_misc.go` (585). Reduced builtin_time.go from 2,050 to 229 lines (89% reduction, 97% total from original 7,260).
- [x] **expression/builtin_miscellaneous.go decomposition** - 2026-02-19 - Split into 3 focused files: `builtin_misc_lock.go` (751), `builtin_misc_validate.go` (671), `builtin_misc_uuid.go` (573). Reduced builtin_miscellaneous.go from 2,021 to 97 lines (95% reduction).
- [x] **ddl/modify_column.go decomposition** - 2026-02-19 - Split into 3 focused files: `modify_column_reorg.go` (899), `modify_column_execute.go` (513), `modify_column_validate.go` (854). Reduced modify_column.go from 2,202 to 16 lines (99% reduction).
- [x] **expression/builtin_cast_vec.go decomposition** - 2026-02-19 - Split into 3 focused files: `builtin_cast_vec_part1.go` (742), `builtin_cast_vec_part2.go` (669), `builtin_cast_vec_part3.go` (722). Reduced builtin_cast_vec.go from 2,080 to 16 lines (99% reduction).
- [x] **planner/core/rule/rule_partition_processor.go decomposition** - 2026-02-19 - Split into 2 focused files: `rule_partition_hash_list.go` (784), `rule_partition_range.go` (1,269). Reduced rule_partition_processor.go from 2,144 to 145 lines (93% reduction).
- [x] **planner/core/operator/logicalop/logical_join.go decomposition** - 2026-02-19 - Split into 3 focused files: `logical_join_stats.go` (655), `logical_join_fd.go` (665), `logical_join_rewrite.go` (837). Reduced logical_join.go from 2,172 to 105 lines (95% reduction).
- [x] **meta/meta.go decomposition** - 2026-02-19 - Split into 3 focused files: `meta_crud.go` (850), `meta_query.go` (608), `meta_history.go` (482). Reduced meta.go from 2,064 to 206 lines (90% reduction).
- [x] **executor/index_merge_reader.go decomposition** - 2026-02-19 - Split into 2 focused files: `index_merge_reader_worker.go` (731), `index_merge_reader_intersect.go` (807). Reduced index_merge_reader.go from 2,057 to 599 lines (71% reduction).
- [x] **types/mydecimal.go decomposition** - 2026-02-19 - Split into 3 focused files: `mydecimal_shift_round.go` (539), `mydecimal_convert.go` (588), `mydecimal_arithmetic.go` (804). Reduced mydecimal.go from 2,515 to 646 lines (74% reduction).
- [x] **store/copr/coprocessor.go decomposition** - 2026-02-19 - Split into 6 focused files: `coprocessor_handler.go` (679), `coprocessor_batch_cache.go` (413), `coprocessor_task.go` (718), `coprocessor_iter.go` (199), `coprocessor_iter_lifecycle.go` (639), `coprocessor_ratelimit.go` (244). Reduced coprocessor.go from 2,904 to 229 lines (92% reduction).
- [x] **types/time.go decomposition** - 2026-02-19 - Split into 3 focused files: `time_parse.go` (574), `time_str_to_date.go` (644), `time_interval.go` (473). Reduced time.go from 3,546 to 1,936 lines (45% reduction).
- [x] **executor/distsql.go decomposition** - 2026-02-19 - Split into 2 focused files: `distsql_index_worker.go` (411), `distsql_table_worker.go` (490). Reduced distsql.go from 1,988 to 1,170 lines (41% reduction).
- [x] **statistics/histogram.go decomposition** - 2026-02-19 - Split into 2 focused files: `histogram_merge.go` (545), `histogram_estimation.go` (259). Reduced histogram.go from 1,993 to 1,244 lines (38% reduction).
- [x] **util/codec/codec.go decomposition** - 2026-02-19 - Split into 3 focused files: `codec_serialize_keys.go` (596), `codec_hash_chunk.go` (379), `codec_decode.go` (372). Reduced codec.go from 1,932 to 666 lines (66% reduction).
- [x] **privilege/privileges/cache.go decomposition** - 2026-02-19 - Split into 3 focused files: `cache_decoder.go` (444), `cache_grants.go` (545), `cache_handle.go` (165). Reduced cache.go from 2,229 to 1,165 lines (48% reduction).
- [x] **table/tables/tables.go decomposition** - 2026-02-19 - Split into 4 focused files: `tables_add_record.go` (330), `tables_update_record.go` (228), `tables_remove_record.go` (157), `tables_sequence.go` (178). Reduced tables.go from 1,962 to 1,063 lines (46% reduction).
- [x] **expression/builtin_other.go decomposition** - 2026-02-19 - Split into 3 focused files: `builtin_in.go` (857), `builtin_user_var.go` (516), `builtin_values.go` (359). Reduced builtin_other.go from 1,950 to 204 lines (89% reduction).
- [x] **table/tables/partition.go decomposition** - 2026-02-19 - Split into 2 focused files: `partition_record.go` (431), `partition_location.go` (306). Reduced partition.go from 2,195 to 1,457 lines (34% reduction).
- [x] **expression/builtin_info.go decomposition** - 2026-02-19 - Split into 2 focused files: `builtin_info_session.go` (449), `builtin_info_tidb.go` (906). Reduced builtin_info.go from 1,791 to 486 lines (73% reduction).
- [x] **store/gcworker/gc_worker.go decomposition** - 2026-02-19 - Split into 2 focused files: `gc_delete_range.go` (358), `gc_rules.go` (321). Reduced gc_worker.go from 1,881 to 1,272 lines (32% reduction).
- [x] **executor/aggfuncs/func_max_min.go decomposition** - 2026-02-19 - Split into 3 focused files: `func_max_min_numeric.go` (655), `func_max_min_complex.go` (704), `func_max_min_special.go` (351). Reduced func_max_min.go from 1,903 to 251 lines (87% reduction).
- [x] **ddl/table.go decomposition** - 2026-02-19 - Split into 4 focused files: `table_rename.go` (267), `table_tiflash.go` (177), `table_placement.go` (250), `table_cache.go` (165). Reduced table.go from 1,798 to 1,040 lines (42% reduction).
- [x] **meta/model/job_args.go decomposition** - 2026-02-19 - Split into 8 focused files: `job_args_schema.go` (104), `job_args_table.go` (219), `job_args_partition.go` (189), `job_args_column.go` (262), `job_args_misc.go` (218), `job_args_table_alter.go` (159), `job_args_cluster.go` (236), `job_args_index.go` (500). Reduced job_args.go from 1,844 to 122 lines (93% reduction).
- [x] **store/copr/coprocessor.go further decomposition** - 2026-02-19 - Split 4 additional files: `coprocessor_task.go` (718), `coprocessor_iter.go` (199), `coprocessor_iter_lifecycle.go` (639), `coprocessor_ratelimit.go` (244). Reduced coprocessor.go from 1,911 to 229 lines (88% further, 92% total from 2,904).
- [x] **ddl/create_table.go decomposition** - 2026-02-19 - Split into 3 focused files: `create_table_validate.go` (280), `create_table_constraints.go` (243), `create_table_hidden.go` (254). Reduced create_table.go from 1,764 to 1,077 lines (39% reduction).
- [x] **ddl/ddl.go decomposition** - 2026-02-19 - Split into 2 focused files: `ddl_table_lock.go` (154), `ddl_job_control.go` (409). Reduced ddl.go from 1,702 to 1,171 lines (31% reduction).
- [x] **types/time.go further decomposition** - 2026-02-19 - Split into 2 additional files: `time_duration.go` (610), `time_convert.go` (457). Reduced time.go from 1,932 to 917 lines (53% reduction, 74% total from original 3,546).
- [x] **planner/core/memtable_predicate_extractor.go decomposition** - 2026-02-20 - Extracted `memtable_predicate_helper.go` (683 lines) with extractHelper struct and all methods. Reduced memtable_predicate_extractor.go from 1,740 to 1,089 lines (37% reduction).
- [x] **infoschema/infoschema_v2.go decomposition** - 2026-02-20 - Split into 2 focused files: `infoschema_v2_reset.go` (246), `infoschema_v2_builder.go` (309). Reduced infoschema_v2.go from 1,788 to 1,281 lines (28% reduction).
- [x] **store/mockstore/unistore/tikv/mvcc.go decomposition** - 2026-02-20 - Split into 2 focused files: `mvcc_pessimistic_lock.go` (566), `mvcc_prewrite.go` (505). Reduced mvcc.go from 2,182 to 1,176 lines (46% reduction).
- [x] **lightning/backend/local/engine.go decomposition** - 2026-02-20 - Split into 2 focused files: `engine_range_properties.go` (~215), `engine_sst.go` (~343). Reduced engine.go from 1,750 to 1,216 lines (30% reduction).
- [x] **store/mockstore/unistore/cophandler/mpp_exec.go decomposition** - 2026-02-20 - Extracted `mpp_exec_scan.go` (~594 lines) with tableScanExec, indexScanExec, indexLookUpExec. Reduced mpp_exec.go from 1,500 to 903 lines (40% reduction).
- [x] **lightning/backend/local/local.go decomposition** - 2026-02-20 - Split into 2 focused files: `local_import_client.go` (~137), `local_partition_range.go` (~150). Reduced local.go from 1,960 to 1,701 lines (13% reduction).
- [x] **executor/join/hash_join_v2.go decomposition** - 2026-02-20 - Extracted `hash_table_context.go` (~206 lines) with hashTableContext struct and all methods. Reduced hash_join_v2.go from 1,538 to 1,348 lines (12% reduction).
- [x] **lightning/config/config.go decomposition** - 2026-02-20 - Extracted `enums.go` (~402 lines) with PostOpLevel, CheckpointKeepStrategy, MaxError, DuplicateResolutionAlgorithm, CompressionType. Reduced config.go from 1,660 to 1,274 lines (23% reduction).
- [x] **executor/builder_reader.go decomposition** - 2026-02-20 - Extracted `builder_reader_indexjoin.go` (~577 lines) with dataReaderBuilder, index join executor building, KV range construction. Reduced builder_reader.go from 1,710 to 1,145 lines (33% reduction).
- [x] **types/json_binary_functions.go decomposition** - 2026-02-20 - Split into 2 focused files: `json_binary_modify.go` (~499), `json_binary_search.go` (~714). Reduced json_binary_functions.go from 1,417 to 259 lines (82% reduction).
- [x] **planner/cascades/old/transformation_rules.go decomposition** - 2026-02-20 - Split into 2 focused files: `transformation_rules_scan.go` (~345), `transformation_rules_transform.go` (~791). Reduced transformation_rules.go from 2,625 to 1,553 lines (41% reduction).
- [x] **expression/builtin_arithmetic.go decomposition** - 2026-02-20 - Extracted `builtin_arithmetic_mul_div.go` (~875 lines) with multiply, divide, int-divide, mod functions. Reduced builtin_arithmetic.go from 1,386 to 540 lines (61% reduction).
- [x] **expression/builtin_regexp.go decomposition** - 2026-02-20 - Extracted `builtin_regexp_instr_replace.go` (~843 lines) with regexpInStr and regexpReplace functions. Reduced builtin_regexp.go from 1,435 to 618 lines (57% reduction).
- [x] **planner/core/common_plans.go decomposition** - 2026-02-20 - Extracted `common_plans_explain.go` (~783 lines) with Explain struct and all rendering/binary plan methods. Reduced common_plans.go from 1,444 to 699 lines (52% reduction).
- [x] **expression/builtin_json_vec.go decomposition** - 2026-02-20 - Extracted `builtin_json_vec_modify.go` (~790 lines) with vectorized JSON modify/set/replace/insert functions. Reduced builtin_json_vec.go from 1,451 to 691 lines (52% reduction).
- [x] **planner/core/point_get_plan.go decomposition** - 2026-02-20 - Extracted `point_get_plan_dml.go` (~363 lines) with update/delete point plan builders, subquery checker, utility helpers. Reduced point_get_plan.go from 1,447 to 1,117 lines (23% reduction).
- [x] **expression/distsql_builtin.go decomposition** - 2026-02-20 - Extracted `distsql_builtin_convert.go` (~278 lines) with PBToExprs, PBToExpr, and all convert* functions. Reduced distsql_builtin.go from 1,417 to 1,162 lines (18% reduction).
- [x] **executor/show.go further decomposition** - 2026-02-20 - Extracted `show_extra.go` (~702 lines) with fetchShowIndex, fetchShowCharset, fetchShowVariables, fetchShowCollation, fetchShowCreateUser, fetchShowGrants, fetchShowWarnings. Reduced show.go from 1,412 to 745 lines (47% reduction).
- [x] **planner/core/exhaust_physical_plans.go further decomposition** - 2026-02-20 - Extracted `exhaust_physical_plans_join.go` (~334 lines) with tryToEnumerateIndexJoin, tryToGetIndexJoin, exhaustPhysicalPlans4LogicalJoin, exhaustPhysicalPlans4LogicalApply. Reduced exhaust_physical_plans.go from 1,399 to 1,094 lines (22% reduction).
- [x] **planner/core/indexmerge_path.go decomposition** - 2026-02-20 - Extracted `indexmerge_path_mvindex.go` (~631 lines) with MV index path building functions. Reduced indexmerge_path.go from 1,394 to 790 lines (43% reduction).
- [x] **expression/builtin_time_arith.go decomposition** - 2026-02-20 - Extracted `builtin_time_arith_vec.go` (~679 lines) with addSubDate function class and all date add/sub signature types. Reduced builtin_time_arith.go from 1,392 to 735 lines (47% reduction).
- [x] **expression/builtin_op.go decomposition** - 2026-02-20 - Extracted `builtin_op_istrue_isnull.go` (~910 lines) with isTrueOrFalse, unaryNot, bitNeg, unaryMinus, isNull function classes. Reduced builtin_op.go from 1,344 to 458 lines (66% reduction).
- [x] **expression/expression.go decomposition** - 2026-02-20 - Extracted `expression_compose.go` (~501 lines) with Assignment, VarAssignment, CNF/DNF split, EvaluateExprWithNull, TableInfo2Schema. Reduced expression.go from 1,331 to 855 lines (36% reduction).
- [x] **planner/core/plan_cost_ver2.go decomposition** - 2026-02-20 - Extracted `plan_cost_ver2_helpers.go` (~340 lines) with cost helper functions and factors. Reduced plan_cost_ver2.go from 1,300 to 989 lines (24% reduction).
- [x] **planner/core/plan_cost_ver1.go decomposition** - 2026-02-20 - Extracted `plan_cost_ver1_join.go` (~523 lines) with reader/join cost functions. Reduced plan_cost_ver1.go from 1,234 to 739 lines (40% reduction).
- [x] **executor/simple_user.go decomposition** - 2026-02-20 - Extracted `simple_user_password.go` (~267 lines) with password reuse/history verification functions. Reduced simple_user.go from 1,296 to 1,065 lines (18% reduction).
- [x] **planner/core/planbuilder_analyze.go decomposition** - 2026-02-20 - Extracted `planbuilder_analyze_options.go` (~474 lines) with analyze options management, build/index functions. Reduced planbuilder_analyze.go from 1,296 to 853 lines (34% reduction).
- [x] **planner/core/optimizer.go decomposition** - 2026-02-20 - Extracted `optimizer_shuffle.go` (~370 lines) with TiFlash fine-grained shuffle logic. Reduced optimizer.go from 1,260 to 917 lines (27% reduction).
- [x] **executor/inspection_result.go decomposition** - 2026-02-20 - Extracted `inspection_result_threshold.go` (~537 lines) with threshold check inspection functions. Reduced inspection_result.go from 1,255 to 744 lines (41% reduction).
- [x] **expression/builtin_math_vec.go decomposition** - 2026-02-20 - Extracted `builtin_math_vec_int.go` (~643 lines) with int/decimal/conversion vectorized math ops. Reduced builtin_math_vec.go from 1,224 to 605 lines (51% reduction).
- [x] **expression/builtin_control.go decomposition** - 2026-02-20 - Split into 2 focused files: `builtin_casewhen.go` (411), `builtin_control_if.go` (505). Reduced builtin_control.go from 1,180 to 309 lines (74% reduction).
- [x] **server/server.go decomposition** - 2026-02-20 - Extracted `server_conn_mgmt.go` (311 lines) with connection management, Kill, TLS, DrainClients. Reduced server.go from 1,271 to 989 lines (22% reduction).
- [x] **ddl/backfilling.go decomposition** - 2026-02-20 - Extracted `backfilling_range.go` (264 lines) with loadTableRanges, splitRangesByKeys, validateAndFillRanges, getBatchTasks, getActualEndKey, sendTasks. Reduced backfilling.go from 1,275 to 1,043 lines (18% reduction).
- [x] **executor/mem_reader.go decomposition** - 2026-02-20 - Extracted `mem_buffer_iter.go` (178 lines) with txnMemBufferIter, iterTxnMemBuffer, getSnapIter. Reduced mem_reader.go from 1,191 to 1,035 lines (13% reduction).
- [x] **sessionctx/variable/slow_log.go decomposition** - 2026-02-20 - Extracted `slow_log_accessors.go` (427 lines) with SlowLogFieldAccessor, parsing functions, SlowLogRuleFieldAccessors. Reduced slow_log.go from 1,154 to 753 lines (35% reduction).
- [x] **ddl/reorg.go decomposition** - 2026-02-20 - Extracted `reorg_table_scan.go` (258 lines) with table scan DAG builders, GetTableMaxHandle, buildHandleCols, getTableRange. Reduced reorg.go from 1,193 to 968 lines (19% reduction).
- [x] **distsql/select_result.go decomposition** - 2026-02-20 - Extracted `select_result_iter.go` (440 lines) with channel iter, selectResultIter, CopRuntimeStats, selectResultRuntimeStats. Reduced select_result.go from 1,166 to 754 lines (35% reduction).
- [x] **planner/cardinality/selectivity.go decomposition** - 2026-02-20 - Extracted `selectivity_filter.go` (376 lines) with GetSelectivityByFilter, crossValidationSelectivity, outOfRange functions, MVIndex vars. Reduced selectivity.go from 1,196 to 846 lines (29% reduction).
- [x] **ddl/ddl.go decomposition** - 2026-02-20 - Extracted `ddl_job_ctx.go` (222 lines) with job context and reorg context management methods. Reduced ddl.go from 1,171 to 973 lines (17% reduction).
- [x] **dxf/framework/storage/task_table.go decomposition** - 2026-02-20 - Extracted `task_table_subtask.go` (262 lines) with subtask query/update/error operations. Reduced task_table.go from 1,174 to 940 lines (20% reduction).
- [x] **store/mockstore/unistore/tikv/mvcc.go decomposition** - 2026-02-20 - Extracted `mvcc_read.go` (267 lines) with Get, GetPair, BatchGet, Scan, collectRangeLock. Reduced mvcc.go from 1,173 to 930 lines (21% reduction).
- [x] **statistics/table.go decomposition** - 2026-02-20 - Extracted `table_memory.go` (170 lines) with TableMemoryUsage, TableCacheItem, ColumnMemUsage, IndexMemUsage types. Reduced table.go from 1,146 to 991 lines (14% reduction).
- [x] **util/stmtsummary/statement_summary.go decomposition** - 2026-02-20 - Extracted `stmt_summary_stats.go` (240 lines) with stmtSummaryStats.add() method. Reduced statement_summary.go from 1,100 to 879 lines (20% reduction).
- [x] **parser/lexer.go decomposition** - 2026-02-20 - Extracted `lexer_scanners.go` (300 lines) with token scanner functions (startWithXx/Nn/Bb/Dash/Slash/Star/At, scanIdentifier, scanQuotedIdent). Reduced lexer.go from 1,107 to 827 lines (25% reduction).
- [x] **ddl/executor_partition.go decomposition** - 2026-02-20 - Extracted `executor_partition_exchange.go` (259 lines) with checkFieldTypeCompatible, checkTiFlashReplicaCompatible, checkTableDefCompatible, checkExchangePartition, ExchangeTablePartition. Reduced executor_partition.go from 1,084 to 856 lines (21% reduction).
- [x] **executor/simple_user.go decomposition** - 2026-02-20 - Extracted `simple_user_options.go` (236 lines) with loadResourceOptions, whetherSavePasswordHistory, alterUserPasswordLocking, loadOptions, createUserFailedLoginJSON, alterUserFailedLoginJSON, readPasswordLockingInfo, deletePasswordLockingAttribute, isValidatePasswordEnabled. Reduced simple_user.go from 1,065 to 860 lines (19% reduction).
- [x] **util/ranger/points.go decomposition** - 2026-02-20 - Extracted `points_scalar_builder.go` (440 lines) with buildFromIsTrue, buildFromIsFalse, buildFromIn, newBuildFromPatternLike, buildFromNot, buildFromScalarFunc. Reduced points.go from 1,069 to 656 lines (39% reduction).
- [x] **expression/builtin_encryption.go decomposition** - 2026-02-20 - Extracted `builtin_encryption_compress.go` (331 lines) with deflate, inflate, compress/uncompress/uncompressedLength/validatePasswordStrength function classes and sigs. Reduced builtin_encryption.go from 1,095 to 789 lines (28% reduction).
- [x] **util/execdetails/runtime_stats.go decomposition** - 2026-02-20 - Extracted `runtime_stats_commit.go` (279 lines) with RuntimeStatsWithCommit struct, Tp, MergeCommitDetails, Merge, Clone, String, formatBackoff, formatLockKeysDetails. Reduced runtime_stats.go from 1,016 to 762 lines (25% reduction).
- [x] **executor/foreign_key.go decomposition** - 2026-02-20 - Extracted `foreign_key_cascade_ast.go` (204 lines) with GenCascadeDeleteAST, GenCascadeSetNullAST, GenCascadeUpdateAST, genTableRefsAST, genWhereConditionAst, FKCheckRuntimeStats/FKCascadeRuntimeStats methods. Reduced foreign_key.go from 1,017 to 836 lines (18% reduction).
- [x] **expression/constant_propagation.go decomposition** - 2026-02-20 - Extracted `constant_propagation_outer_join.go` (462 lines) with propOuterJoinConstSolver, propagateConstantDNF, PropConstForOuterJoin, PropagateConstantSolver, cloneJoinKeys, isJoinKey. Reduced constant_propagation.go from 1,038 to 607 lines (42% reduction).
- [x] **ddl/index.go decomposition** - 2026-02-20 - Extracted `index_condition.go` (268 lines) with CheckAndBuildIndexConditionString, checkIndexCondition, buildAffectColumn, buildIndexConditionChecker. Reduced index.go from 1,062 to 822 lines (23% reduction).
- [x] **types/json_binary.go decomposition** - 2026-02-20 - Extracted `json_binary_encode.go` (359 lines) with CalculateBinaryJSONSize, appendBinaryJSON, appendZero, appendUint32, calculateBinary*, appendBinary*, field struct. Reduced json_binary.go from 1,044 to 714 lines (32% reduction).
- [x] **executor/simple.go decomposition** - 2026-02-20 - Extracted `simple_misc.go` (277 lines) with executeAlterInstance, executeDropStats, autoNewTxn, executeShutdown, executeSetSessionStates, executeAdmin*, executeSetResourceGroupName, executeAlterRange. Reduced simple.go from 1,064 to 818 lines (23% reduction).
- [x] **store/helper/helper.go decomposition** - 2026-02-20 - Extracted `helper_region.go` (328 lines) with TableInfoWithKeyRange, GetRegionsTableInfo, GetTablesInfoWithKeyRange, ParseRegionsTableInfos, GetPDAddr, GetPDRegionStats, CollectTiFlashStatus, SyncTableSchemaToTiFlash. Reduced helper.go from 937 to 642 lines (31% reduction).
- [x] **session/session_txn.go decomposition** - 2026-02-20 - Extracted `session_txn_prepare.go` (299 lines) with PrepareTxnCtx, decideTxnMode, shouldUsePessimisticAutoCommit, isDMLStatement, PrepareTSFuture, GetPreparedTxnFuture, RefreshTxnCtx, GetStore, usePipelinedDmlOrWarn. Reduced session_txn.go from 1,086 to 819 lines (25% reduction).
- [x] **planner/core/exhaust_physical_plans.go decomposition** - 2026-02-20 - Extracted `exhaust_physical_plans_index_join.go` (340 lines) with indexJoinInnerChildWrapper, checkOpSelfSatisfyPropTaskTypeRequirement, checkIndexJoinInnerTaskWithAgg, admitIndexJoinInnerChildPattern, extractIndexJoinInnerChildPattern, buildDataSource2IndexScanByIndexJoinProp, buildDataSource2TableScanByIndexJoinProp, filterIndexJoinBySessionVars, getIndexJoinSideAndMethod. Reduced exhaust_physical_plans.go from 1,094 to 783 lines (28% reduction).
- [x] **executor/join/joiner.go decomposition** - 2026-02-20 - Extracted `joiner_outer_inner.go` (268 lines) with leftOuterJoiner, rightOuterJoiner, innerJoiner structs and all TryToMatchInners, TryToMatchOuters, OnMissMatch, Clone methods. Reduced joiner.go from 1,084 to 836 lines (23% reduction).
- [x] **table/tables/tables.go decomposition** - 2026-02-20 - Extracted `tables_util.go` (270 lines) with FindIndexByColName, getDuplicateError, TryGetHandleRestoredDataWrapper, TryTruncateRestoredData, ConvertDatumToTailSpaceCount, BuildTableScanFromInfos, BuildPartitionTableScanFromInfos, SetPBColumnsDefaultValue, TemporaryTable. Reduced tables.go from 1,062 to 825 lines (22% reduction).
- [x] **executor/mem_reader.go decomposition** - 2026-02-20 - Extracted `mem_reader_index.go` (248 lines) with memRowsIterForIndex, memIndexMergeReader.getMemRowsIter/getHandles/getMemRows, getColIDAndPkColIDs. Reduced mem_reader.go from 1,035 to 824 lines (20% reduction).
- [x] **ddl/backfilling.go decomposition** - 2026-02-20 - Extracted `backfilling_iter.go` (181 lines) with recordIterFunc, iterateSnapshotKeys, GetRangeEndKey, mergeWarningsAndWarningsCount, logSlowOperations, doneTaskKeeper. Reduced backfilling.go from 1,043 to 891 lines (15% reduction).
- [x] **ddl/table.go decomposition** - 2026-02-20 - Extracted `table_check.go` (211 lines) with checkTableNotExists, checkConstraintNamesNotExists, checkTableIDNotExists, checkTableNotExistsFromInfoSchema, updateVersionAndTableInfoWithCheck, updateVersionAndTableInfo, updateTable, schemaIDAndTableInfo, onRepairTable. Reduced table.go from 1,040 to 855 lines (18% reduction).
- [x] **planner/core/find_best_task_property.go decomposition** - 2026-02-20 - Extracted `find_best_task_pruning.go` (269 lines) with skylinePruning, hasOnlyEqualPredicatesInDNF, equalPredicateCount, getPruningInfo. Reduced find_best_task_property.go from 1,057 to 815 lines (23% reduction).
- [x] **statistics/table.go decomposition** - 2026-02-20 - Extracted `table_pseudo.go` (199 lines) with ID2UniqueID, GenerateHistCollFromColumnInfo, PseudoHistColl, PseudoTable, CheckAnalyzeVerOnTable, PrepareCols4MVIndex. Reduced table.go from 991 to 814 lines (18% reduction).
- [x] **domain/plan_replayer_dump.go decomposition** - 2026-02-20 - Extracted `plan_replayer_dump_util.go` (202 lines) with extractTableNames, getStatsForTable, getShowCreateTable, resultSetToStringSlice, getRows, dumpDebugTrace, dumpOneDebugTrace, dumpErrorMsgs. Reduced plan_replayer_dump.go from 973 to 800 lines (18% reduction).
- [x] **planner/core/expression_rewriter_leave.go decomposition** - 2026-02-21 - Extracted `expression_rewriter_funccall.go` (190 lines) with rewriteFuncCall, funcCallToExpressionWithPlanCtx, funcCallToExpression. Reduced expression_rewriter_leave.go from 1,089 to 925 lines (15% reduction).
- [x] **table/tables/index.go decomposition** - 2026-02-21 - Extracted `index_util.go` (200 lines) with FindChangingCol, IsIndexWritable, BuildRowcodecColInfoForIndexColumns, BuildFieldTypesForIndexColumns, TryAppendCommonHandleRowcodecColInfos, GenIndexValueFromIndex, ExtractColumnsFromCondition, DedupIndexColumns, extractColumnsFromExpr, init. Reduced index.go from 1,030 to 858 lines (17% reduction).
- [x] **planner/core/preprocess_check.go decomposition** - 2026-02-21 - Extracted `preprocess_check_util.go` (261 lines) with checkDuplicateColumnName, checkIndexInfo, checkUnsupportedTableOptions, checkColumn, isDefaultValNowSymFunc, isInvalidDefaultValue. Reduced preprocess_check.go from 990 to 757 lines (24% reduction).
- [x] **ddl/reorg.go decomposition** - 2026-02-21 - Extracted `reorg_handle.go` (205 lines) with reorgHandler struct and methods, getReorgInfoFromPartitions, UpdateReorgMeta, getSplitKeysForTempIndexRanges, adjustEndKeyAcrossVersion. Reduced reorg.go from 968 to 795 lines (18% reduction).
- [x] **planner/core/plan_cost_ver2.go decomposition** - 2026-02-21 - Extracted `plan_cost_ver2_misc.go` (190 lines) with getNumberOfRanges, getPlanCostVer24PhysicalApply/UnionAll/PointGet/ExchangeReceiver/BatchPointGet/CTE. Reduced plan_cost_ver2.go from 989 to 823 lines (17% reduction).
- [x] **executor/join/index_lookup_hash_join.go decomposition** - 2026-02-21 - Extracted `index_lookup_hash_join_inner.go` (230 lines) with doJoinUnordered, getMatchedOuterRows, joinMatchedInnerRow2Chunk, collectMatchedInnerPtrs4OuterRows, doJoinInOrder. Reduced index_lookup_hash_join.go from 981 to 779 lines (21% reduction).
- [x] **executor/memtable_reader.go decomposition** - 2026-02-21 - Extracted `memtable_reader_region_peers.go` (165 lines) with tikvRegionPeersRetriever struct and methods. Reduced memtable_reader.go from 1,025 to 890 lines (13% reduction).
- [x] **planner/core/memtable_infoschema_extractor.go decomposition** - 2026-02-21 - Extracted `memtable_infoschema_extractor_columns.go` (180 lines) with InfoSchemaColumnsExtractor, InfoSchemaTiDBIndexUsageExtractor. Reduced memtable_infoschema_extractor.go from 1,016 to 861 lines (15% reduction).
- [x] **expression/scalar_function.go decomposition** - 2026-02-21 - Extracted `scalar_function_resolve.go` (206 lines) with ResolveIndices, ResolveIndicesByVirtualExpr, RemapColumn, GetSingleColumn, Coercibility/Charset/Collation methods, MemoryUsage. Reduced scalar_function.go from 963 to 779 lines (19% reduction).
- [x] **infoschema/infoschema.go decomposition** - 2026-02-21 - Extracted `infoschema_session_ext.go` (168 lines) with SessionExtendedInfoSchema, FindTableByTblOrPartID, getTableInfo, getTableInfoList. Reduced infoschema.go from 961 to 818 lines (15% reduction).
- [x] **distsql/request_builder.go decomposition** - 2026-02-21 - Extracted `request_builder_index.go` (251 lines) with IndexRangesToKVRanges*, CommonHandleRangesToKVRanges, VerifyTxnScope, EncodeIndexKey, BuildTableRanges. Reduced request_builder.go from 918 to 697 lines (24% reduction).
- [x] **executor/join/base_join_probe.go decomposition** - 2026-02-21 - Extracted `base_join_probe_factory.go` (195 lines) with isKeyMatched, commonInitForScanRowTable, NewJoinProbe, mockJoinProbe. Reduced base_join_probe.go from 983 to 813 lines (17% reduction).
- [x] **server/server.go decomposition** - 2026-02-21 - Extracted `server_conn_util.go` (222 lines) with connectInfo, session management, timezone setup, txn checks, status vars. Reduced server.go from 989 to 795 lines (20% reduction).
- [x] **ddl/executor_index.go decomposition** - 2026-02-21 - Extracted `executor_index_drop.go` (235 lines) with DropForeignKey, DropIndex, dropIndex, CheckIsDropPrimaryKey, validateCommentLength, validateGlobalIndexWithGeneratedColumns. Reduced executor_index.go from 967 to 764 lines (21% reduction).
- [x] **executor/internal/mpp/local_mpp_coordinator.go decomposition** - 2026-02-21 - Extracted `local_mpp_coordinator_exec.go` (189 lines) with handleMPPStreamResponse, nextImpl, Next, Execute, GetNodeCnt. Reduced local_mpp_coordinator.go from 978 to 825 lines (16% reduction).
- [x] **planner/core/joinorder/conflict_detector.go decomposition** - 2026-02-21 - Extracted `conflict_detector_rules.go` (180 lines) with ruleTableEntry type and assocRuleTable, leftAsscomRuleTable, rightAsscomRuleTable. Reduced conflict_detector.go from 963 to 799 lines (17% reduction).
- [x] **executor/adapter.go decomposition** - 2026-02-21 - Extracted `adapter_fk_cascade.go` (211 lines) with handleForeignKeyCascade, prepareFKCascadeContext, handleFKTriggerError, buildExecutor, openExecutor, next. Reduced adapter.go from 944 to 768 lines (19% reduction).
- [x] **planner/core/optimizer.go decomposition** - 2026-02-21 - Extracted `optimizer_physical.go` (221 lines) with physical plan iteration/transform, existsCartesianProduct, DefaultDisabledLogicalRulesList, chunk reuse helpers, overlong type checks. Reduced optimizer.go from 917 to 720 lines (21% reduction).
