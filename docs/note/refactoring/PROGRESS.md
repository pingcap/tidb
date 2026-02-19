# Refactoring Progress Tracker

Last updated: 2026-02-19 (job_args, coprocessor decompositions)

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
  - File: `pkg/ddl/index.go` (4,140 → 2,691 lines, 35% reduction)
  - Target: Focused files per index operation area
  - [x] `index_dist_task.go` (418 lines) - distributed task execution, param tuning, row size estimation
  - [x] `index_backfill_worker.go` (619 lines) - add-index txn worker, batch unique check, ingest write
  - [x] `index_columnar.go` (261 lines) - TiFlash columnar index creation and progress monitoring
  - [x] `index_analyze.go` (289 lines) - post-index-creation analyze workflow

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
  - File: `pkg/types/time.go` (3,546 → 1,936 lines, 45% reduction)
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
- [x] **DDL executor.go split** - `pkg/ddl/executor.go` (7,201 → 1,986 lines, 72% reduction)
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
