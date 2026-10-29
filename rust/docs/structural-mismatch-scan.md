# Structural mismatch scan (2026-10-29)

## Status (2026-10-29)

Done (each is its Go package's crate now): every `tidb-util` entry (one crate per `pkg/util/*` package; `tidb-util` keeps `misc`); `planner/cascades/base` (`hash_equaler`, `base_traits`, `cascades_base`); `planner/util` (`by_item`, `column_length`, `index_columns`, `null_misc`, `handle_cols`), `util/costusage`, `util/tablesampler`, `util/coreusage`; `planner/indexadvisor` (the executor's `index_advisor` minus `RecommendIndexExec`); `util/ranger`; `parser/format` (`tidb-ast`'s `format.rs` restore state) and `parser/opcode`; `parser/util`; `timer/runtime` and `timer/tablestore` (with `notifier`); unistore's `lockstore` (with `arena`, `iterator`, `load_dump`), `util/lockwaiter` and `tikv/mvcc`; `util/resourcegrouptag`, `util/trxevents`, `util/tiflash`, `util/cteutil`, `session/txninfo`, `session/sessmgr`, `pkg/lock`; `select_result_stats` into tidb-distsql; `util/domainutil`, `util/replayer`; `executor/internal/pdhelper`; `table/tblctx`; the import-graph inversions (sessionctx -> table, deadlockhistory -> txnkv, domain-core -> executor, session -> store-driver).

Blocked on a behavior-level change, not a move:
- `planner/property` (`physical_property`, `stats_info`, `task_type`) and `planner/cardinality`: the planner's `stats_info::HistColl` reimplements Go `statistics.HistColl`, and cardinality reads planner core (`access_path`, logical plans).
- `planner/util/path.go` (`access_path`) and `domainmisc`: read planner core, which Go's `planner/util` reaches only through `core/base`, still inside the planner crate.
- `tidb-error`'s `catalog`: done except `infosync` (each package's prototypes are its crate's `<package>_errors`); `infosync`'s `ErrHTTPServiceError` is raised by tidb-txnkv's `cluster_http`, Go `store/driver` woven into txnkv -- the txnkv / store-driver split below.
- tidb-txnkv holds Go `pkg/kv` plus `store/driver` and `store/driver/txn`, and 20 crates below the store driver hold the concrete transaction opener where Go's go through `kv.Storage`: the storage-seam redesign (ExecPlan). codec's `table_key` (Go `tablecodec`) waits on it too.
- txnkv's `mpp_probe`, `key_ranges`, `range_diagnostics` (Go `store/copr`): tidb-copr depends on tidb-distsql, the reverse of Go's `distsql -> copr`.

## Disposition of every remaining entry (2026-10-29)

Moved since the status above: `util/rowcodec` (tidb-rowcodec), `store/driver/txn` (tidb-store-driver-txn), `store/driver/error` (tidb-store-driver-error), `domain/{serverinfo,infosync,affinity,globalconfigsync}`, `config/{kerneltype,configtypes,deploymode}`, `lightning/mydump` (csv_parser), `ddl/bdr`, `util/ranger/context`, `parser/types` errors, infoschema's `metric_table_def` / `virtual_tables`, `ruv2_metrics` (back into tidb-execdetails), the clustered-handle range builders (`tidb_executor::handle_range`).

Checked, and they stay where they are (the citation is to a Go file the module ports, inside the crate's own Go package, or a helper Go keeps in both packages):
- tidb-table: `kv_table`, `kv_table/index_entries`, `kv_table/table_meta` (Go `table/tables`), `partition_fold` (shared by Go's ddl and table partition code), `context/static_mutate` (Go's session-less `MutateContext`).
- tidb-executor: `split_region` (Go `executor/split.go`), `stats_lock` and `table_size_stats` (the executor's adapters over the statistics handle), `ddl/indexes` (the executor's rendering of an index backfill failure).
- tidb-session: `ddl_exec` (Go `executor/ddl.go`'s DDLExec over the session's `temptable`), `analyze_service`, `savepoints` (Go `TxnCtx.Savepoints`), `table_lock` (Go `session.go`'s lock set), `domain/*` (the node stacks: `cluster_session_node/statistics`, `real_tikv_node/schema_following`, `mock_store` = Go `testkit`, `wire_status`, `embedded_resource_groups`).
- tidb-privilege: `privs` (wraps parser/mysql's privilege names with the privileges package's filters).
- tidb-server: `server_metrics`, `auth_plugin_registry`, `cluster_boot`.
- tidb-gcworker: `seams` (the GC worker's collaborator interfaces).
- tidb-br: `rtree` (Go `br/pkg/rtree` under br).
- tidb-vardef: `global_sysvar_initial`.
- tidb-protocol: `textrow`, `result_encoder` (the server's text-protocol formatting).
- tidb-expr: `binary_literal`, `time_fn/duration_parse` (expression evaluation over `types` rules), `aggregation/names`.
- tidb-datatype: `session_time_zone` (the `*time.Location` shape the temporal code converts with).
- tidb-domain-core: `domain/schema_lease`, `session_transaction`, `domain/stat`.
- tidb-txnkv: `mvcc_metadata`, `mem_storage`, `prefix_ops`, `client_go_metrics`, `driver/cop_store` (client-go surfaces; see the txnkv entry below).
- tidb-metadef: `bootstrap_tables` (the bootstrap table list metadef owns for the session).

Re-layerings, recorded in the ExecPlan (behavior-level, not moves): planner `property` / `cardinality` / `access_path` / `domain_misc` / `stats_info`; tidb-table `sequence` (the allocator: Go `meta/autoid`'s sequence half and `tables`' `sequenceCommon`; its options are tidb-model's `SequenceInfo`), `column_default`'s DDL half, `handle_range`'s metadata half; tidb-executor `compiler/non_prepared_plan_cache` (planner/core over executor tables), `process`'s sessionctx types; tidb-txnkv's `driver` (Go `store/driver` woven into its transaction modules); tidb-codec `table_key` (tablecodec, called from kv/txnkv); tidb-error's `infosync` catalog; the `StmtContext` in tidb-sessionctx (it no longer reaches tidb-table: `hypo_index` is tidb-variable's, `table/tblctx` is tidb-tblctx, the sequence lookup is `tidb_util::misc`'s); the cluster-storage seam.

Each Rust module whose `//!` docs cite a Go package (most-cited `pkg/...` path) unrelated to its crate's Go package, after dropping modules whose file name matches a Go file in the crate's package. A citation is a lead, not proof: each entry needs a check against the Go source before it moves.

| Lines | Crate | Module | Cited Go package | Crate's Go package |
|---:|---|---|---|---|
| 5936 | `tidb-metadef` | `metric_table_def.rs` | `infoschema` | `meta/metadef` |
| 5558 | `tidb-unistore` | `mvcc_store.rs` | `store/mockstore/unistore/tikv` | `store/mockstore/unistore/lockstore, store/mockstore/unistore/util/lockwaiter` |
| 4557 | `tidb-table` | `kv_table.rs` | `executor` | `table, table/tables` |
| 3371 | `tidb-unistore` | `cophandler.rs` | `store/mockstore/unistore/cophandler` | `store/mockstore/unistore/lockstore, store/mockstore/unistore/util/lockwaiter` |
| 2996 | `tidb-planner` | `cardinality/row_count_estimator.rs` | `planner/cardinality` | `planner/core, planner/core/base` |
| 2856 | `tidb-planner` | `ranger/go_cases.rs` | `util/ranger` | `planner/core, planner/core/base` |
| 2851 | `tidb-planner` | `ranger/detacher.rs` | `util/ranger` | `planner/core, planner/core/base` |
| 2800 | `tidb-executor` | `partition_pruning.rs` | `planner/core/rule` | `executor` |
| 2320 | `tidb-planner` | `ranger/points.rs` | `util/ranger` | `planner/core, planner/core/base` |
| 2127 | `tidb-util` | `execdetails/runtime_stats.rs` | `util/execdetails` | `util/memory` |
| 2055 | `tidb-util` | `ruv2_metrics.rs` | `util/execdetails` | `util/memory` |
| 1907 | `tidb-util` | `logutil/mod.rs` | `util/logutil` | `util/memory` |
| 1872 | `tidb-unistore` | `kv_handler.rs` | `store/mockstore/unistore/tikv` | `store/mockstore/unistore/lockstore, store/mockstore/unistore/util/lockwaiter` |
| 1810 | `tidb-planner` | `access_path.rs` | `planner/util` | `planner/core, planner/core/base` |
| 1736 | `tidb-stats-handle-syncload` | `stats_watch.rs` | `domain` | `statistics/handle/syncload` |
| 1551 | `tidb-util` | `execdetails/tiflash_stats.rs` | `util/execdetails` | `util/memory` |
| 1536 | `tidb-util` | `topsql_reporter/ru_datamodel.rs` | `util/topsql/reporter` | `util/memory` |
| 1408 | `tidb-util` | `topsql_reporter/datamodel.rs` | `util/topsql/reporter` | `util/memory` |
| 1406 | `tidb-table` | `kv_table/index_entries.rs` | `tablecodec` | `table, table/tables` |
| 1335 | `tidb-util` | `dbterror/ddl_errors.rs` | `util/dbterror` | `util/memory` |
| 1323 | `tidb-util` | `execdetails/exec_details.rs` | `util/execdetails` | `util/memory` |
| 1306 | `tidb-util` | `memoryusagealarm.rs` | `util/memoryusagealarm` | `util/memory` |
| 1305 | `tidb-executor` | `compiler/non_prepared_plan_cache.rs` | `planner/core` | `executor` |
| 1231 | `tidb-planner` | `physical_property.rs` | `planner/property` | `planner/core, planner/core/base` |
| 1227 | `tidb-table` | `column_default.rs` | `ddl` | `table, table/tables` |
| 1223 | `tidb-util` | `topsql_reporter/ru_window_aggregator.rs` | `util/topsql/reporter` | `util/memory` |
| 1138 | `tidb-executor` | `process.rs` | `session/sessmgr` | `executor` |
| 1117 | `tidb-planner` | `ranger/types.rs` | `util/ranger` | `planner/core, planner/core/base` |
| 1090 | `tidb-util` | `cgroup.rs` | `util/cgroup` | `util/memory` |
| 1053 | `tidb-codec` | `column.rs` | `util/chunk` | `util/rowcodec` |
| 1048 | `tidb-planner` | `ranger/ranger.rs` | `util/ranger` | `planner/core, planner/core/base` |
| 1006 | `tidb-executor` | `store_helper.rs` | `store/helper` | `executor` |
| 1003 | `tidb-planner` | `cardinality/selectivity.rs` | `planner/cardinality` | `planner/core, planner/core/base` |
| 993 | `tidb-executor` | `load_data/csv_parser.rs` | `lightning/mydump` | `executor` |
| 969 | `tidb-table` | `kv_table/table_meta.rs` | `meta/model` | `table, table/tables` |
| 964 | `tidb-kv` | `mem_quota.rs` | `executor` | `distsql, kv` |
| 921 | `tidb-util` | `set.rs` | `util/set` | `util/memory` |
| 905 | `tidb-planner` | `stats_info.rs` | `planner/property` | `planner/core, planner/core/base` |
| 888 | `tidb-timer` | `runtime/mod.rs` | `timer/runtime` | `timer/api` |
| 863 | `tidb-error` | `terror.rs` | `parser/terror` | `errno, parser/mysql` |
| 846 | `tidb-executor` | `index_advisor/mod.rs` | `planner/indexadvisor` | `executor` |
| 778 | `tidb-util` | `intset.rs` | `util/intset` | `util/memory` |
| 762 | `tidb-util` | `globalconn/mod.rs` | `util/globalconn` | `util/memory` |
| 755 | `tidb-distsql` | `request_builder.rs` | `distsql` | `store/copr` |
| 745 | `tidb-executor` | `split_region.rs` | `planner/core` | `executor` |
| 728 | `tidb-codec` | `package.rs` | `util/codec` | `util/rowcodec` |
| 714 | `tidb-timer` | `table_store/sql.rs` | `timer/tablestore` | `timer/api` |
| 713 | `tidb-ast` | `format.rs` | `parser/format` | `parser/ast` |
| 710 | `tidb-util` | `stringutil.rs` | `util/stringutil` | `util/memory` |
| 706 | `tidb-timer` | `runtime/worker.rs` | `timer/runtime` | `timer/api` |
| 695 | `tidb-table` | `sequence.rs` | `meta/model` | `table, table/tables` |
| 665 | `tidb-util` | `encrypt/aes.rs` | `util/encrypt` | `util/memory` |
| 648 | `tidb-util` | `tiflashcompute.rs` | `util/tiflashcompute` | `util/memory` |
| 646 | `tidb-util` | `traceevent/mod.rs` | `util/traceevent` | `util/memory` |
| 625 | `tidb-error` | `plannererrors.rs` | `util/dbterror/plannererrors` | `errno, parser/mysql` |
| 622 | `tidb-executor` | `index_advisor/algorithm.rs` | `planner/indexadvisor` | `executor` |
| 620 | `tidb-protocol` | `textrow.rs` | `format/textrow` | `server/internal` |
| 619 | `tidb-planner` | `logical/expand.rs` | `expression` | `planner/core, planner/core/base` |
| 612 | `tidb-expr` | `time_fn/duration_parse.rs` | `types` | `expression` |
| 611 | `tidb-util` | `context/warn.rs` | `util/context` | `util/memory` |
| 607 | `tidb-planner` | `cardinality/cross_estimation.rs` | `planner/cardinality` | `planner/core, planner/core/base` |
| 605 | `tidb-table` | `handle_range.rs` | `planner/core` | `table, table/tables` |
| 595 | `tidb-infoschema` | `table_source.rs` | `table` | `infoschema` |
| 591 | `tidb-planner` | `cardinality/pseudo.rs` | `planner/cardinality` | `planner/core, planner/core/base` |
| 584 | `tidb-planner` | `ranger/checker.rs` | `util/ranger` | `planner/core, planner/core/base` |
| 579 | `tidb-txnkv` | `mpp_probe.rs` | `store/copr` | `kv, store/driver` |
| 579 | `tidb-session` | `domain/cluster_session_node/statistics.rs` | `executor` | `session` |
| 556 | `tidb-executor` | `index_advisor/query.rs` | `planner/indexadvisor` | `executor` |
| 539 | `tidb-util` | `sqlkiller.rs` | `util/sqlkiller` | `util/memory` |
| 539 | `tidb-planner` | `cardinality/row_size.rs` | `planner/cardinality` | `planner/core, planner/core/base` |
| 528 | `tidb-session` | `table_lock.rs` | `lock` | `session` |
| 515 | `tidb-util` | `sem_v2/mod.rs` | `util/sem/v2` | `util/memory` |
| 486 | `tidb-expr` | `metabuild.rs` | `meta/metabuild` | `expression` |
| 485 | `tidb-timer` | `notifier.rs` | `timer/tablestore` | `timer/api` |
| 484 | `tidb-protocol` | `result_encoder.rs` | `format/textrow` | `server/internal` |
| 483 | `tidb-util` | `dbterror/exeerrors.rs` | `util/dbterror/exeerrors` | `util/memory` |
| 473 | `tidb-metadef` | `bootstrap_tables.rs` | `session` | `meta/metadef` |
| 458 | `tidb-util` | `encrypt/aes_layer.rs` | `util/encrypt` | `util/memory` |
| 458 | `tidb-session` | `domain/real_tikv_node/schema_following.rs` | `ddl` | `session` |
| 457 | `tidb-util` | `sem.rs` | `util/sem` | `util/memory` |
| 455 | `tidb-distsql` | `query_runtime.rs` | `distsql` | `store/copr` |
| 452 | `tidb-table` | `partition_fold.rs` | `ddl` | `table, table/tables` |
| 441 | `tidb-planner` | `selectivity_greedy.rs` | `planner/cardinality` | `planner/core, planner/core/base` |
| 440 | `tidb-unistore` | `mvcc/mod.rs` | `store/mockstore/unistore/tikv/mvcc` | `store/mockstore/unistore/lockstore, store/mockstore/unistore/util/lockwaiter` |
| 428 | `tidb-util` | `partialjson.rs` | `util/partialjson` | `util/memory` |
| 421 | `tidb-unistore` | `detector.rs` | `store/mockstore/unistore/tikv` | `store/mockstore/unistore/lockstore, store/mockstore/unistore/util/lockwaiter` |
| 412 | `tidb-util` | `redact.rs` | `util/redact` | `util/memory` |
| 412 | `tidb-table` | `context/static_mutate.rs` | `lightning/backend/kv` | `table, table/tables` |
| 407 | `tidb-executor` | `select_result_stats.rs` | `distsql` | `executor` |
| 403 | `tidb-model` | `ddl_bdr.rs` | `ddl/bdr` | `meta/model` |
| 402 | `tidb-domain` | `cdcutil.rs` | `util/cdcutil` | `domain` |
| 392 | `tidb-util` | `context/plancache.rs` | `util/context` | `util/memory` |
| 392 | `tidb-txnkv` | `client_go_metrics.rs` | `metrics` | `kv, store/driver` |
| 392 | `tidb-distsql` | `distsql_runtime.rs` | `distsql` | `store/copr` |
| 383 | `tidb-error` | `errctx.rs` | `errctx` | `errno, parser/mysql` |
| 379 | `tidb-server` | `server_metrics.rs` | `metrics` | `server` |
| 376 | `tidb-planner` | `cardinality/row_count_column.rs` | `planner/cardinality` | `planner/core, planner/core/base` |
| 371 | `tidb-session` | `domain/mock_store.rs` | `testkit` | `session` |
| 368 | `tidb-util` | `context/warning_publication.rs` | `util/context` | `util/memory` |
| 368 | `tidb-infoschema` | `cluster_catalog.rs` | `meta` | `infoschema` |
| 360 | `tidb-executor` | `stats_lock.rs` | `statistics/handle/lockstats` | `executor` |
| 355 | `tidb-util` | `generic/bounded_min_heap.rs` | `util/generic` | `util/memory` |
| 350 | `tidb-util` | `topsql_state.rs` | `util/topsql/state` | `util/memory` |
| 344 | `tidb-util` | `prefetch.rs` | `util/prefetch` | `util/memory` |
| 343 | `tidb-util` | `topsql_reporter/metrics.rs` | `util/topsql/reporter/metrics` | `util/memory` |
| 342 | `tidb-timer` | `runtime/cache.rs` | `timer/runtime` | `timer/api` |
| 341 | `tidb-util` | `dbterror/mod.rs` | `util/dbterror` | `util/memory` |
| 341 | `tidb-datatype` | `session_time_zone.rs` | `sessionctx/variable` | `types` |
| 328 | `tidb-planner` | `handle_cols.rs` | `planner/util` | `planner/core, planner/core/base` |
| 327 | `tidb-parser` | `util_parser.rs` | `util/parser` | `parser` |
| 326 | `tidb-util` | `sem_compat.rs` | `util/sem/compat` | `util/memory` |
| 326 | `tidb-util` | `master_key/mod.rs` | `util/encrypt` | `util/memory` |
| 323 | `tidb-planner` | `aggregation_descriptor.rs` | `expression/aggregation` | `planner/core, planner/core/base` |
| 322 | `tidb-executor` | `cte_storage.rs` | `util/cteutil` | `executor` |
| 321 | `tidb-txnkv` | `key_ranges.rs` | `store/copr` | `kv, store/driver` |
| 320 | `tidb-planner` | `cascades_base.rs` | `planner/cascades/base` | `planner/core, planner/core/base` |
| 316 | `tidb-util` | `queue.rs` | `util/queue` | `util/memory` |
| 315 | `tidb-txnkv` | `driver/cop_store.rs` | `store/copr` | `kv, store/driver` |
| 309 | `tidb-txnkv` | `mvcc_metadata.rs` | `store/mockstore/unistore/tikv/mvcc` | `kv, store/driver` |
| 292 | `tidb-util` | `mvmap/mod.rs` | `util/mvmap` | `util/memory` |
| 289 | `tidb-privilege` | `privs.rs` | `parser/mysql` | `privilege/privileges` |
| 287 | `tidb-util` | `cpu.rs` | `util/cpu` | `util/memory` |
| 285 | `tidb-util` | `intest/mod.rs` | `util/intest` | `util/memory` |
| 284 | `tidb-util` | `compress.rs` | `util/compress` | `util/memory` |
| 284 | `tidb-txnkv` | `mem_storage.rs` | `store/mockstore` | `kv, store/driver` |
| 282 | `tidb-session` | `ddl_exec.rs` | `executor` | `session` |
| 282 | `tidb-planner` | `ranger/mod.rs` | `util/ranger` | `planner/core, planner/core/base` |
| 278 | `tidb-util` | `selection.rs` | `util/selection` | `util/memory` |
| 275 | `tidb-server` | `auth_plugin_registry.rs` | `extension` | `server` |
| 275 | `tidb-executor` | `table_size_stats.rs` | `statistics/handle/cache` | `executor` |
| 273 | `tidb-planner` | `cost_usage.rs` | `planner/util/costusage` | `planner/core, planner/core/base` |
| 261 | `tidb-util` | `bitmap.rs` | `util/bitmap` | `util/memory` |
| 258 | `tidb-codec` | `value.rs` | `util/codec` | `util/rowcodec` |
| 250 | `tidb-server` | `cluster_boot.rs` | `session` | `server` |
| 247 | `tidb-domain-core` | `domain/schema_lease.rs` | `sessiontxn/isolation` | `domain` |
| 245 | `tidb-planner` | `txn_mode.rs` | `session` | `planner/core, planner/core/base` |
| 244 | `tidb-executor` | `txn_summary.rs` | `session/txninfo` | `executor` |
| 244 | `tidb-domain` | `domainutil.rs` | `util/domainutil` | `domain` |
| 233 | `tidb-txnkv` | `range_diagnostics.rs` | `store/copr` | `kv, store/driver` |
| 232 | `tidb-planner` | `hash_equaler.rs` | `planner/cascades/base` | `planner/core, planner/core/base` |
| 231 | `tidb-ast` | `opcode.rs` | `parser/opcode` | `parser/ast` |
| 229 | `tidb-util` | `arena.rs` | `util/arena` | `util/memory` |
| 225 | `tidb-gcworker` | `seams.rs` | `ddl/util` | `store/gcworker` |
| 224 | `tidb-distsql` | `envelope.rs` | `distsql` | `store/copr` |
| 222 | `tidb-planner` | `cardinality/ndv.rs` | `planner/cardinality` | `planner/core, planner/core/base` |
| 221 | `tidb-util` | `execdetails/mod.rs` | `util/execdetails` | `util/memory` |
| 220 | `tidb-table` | `hypo_index.rs` | `ddl` | `table, table/tables` |
| 218 | `tidb-planner` | `null_misc.rs` | `planner/util` | `planner/core, planner/core/base` |
| 212 | `tidb-vardef` | `global_sysvar_initial.rs` | `sessionctx/variable` | `sessionctx/vardef` |
| 212 | `tidb-util` | `sli.rs` | `util/sli` | `util/memory` |
| 205 | `tidb-executor` | `lock.rs` | `lock` | `executor` |
| 200 | `tidb-expr` | `user_vars.rs` | `sessionctx/variable` | `expression` |
| 193 | `tidb-util` | `encrypt/crypt.rs` | `util/encrypt` | `util/memory` |
| 193 | `tidb-executor` | `index_advisor/model.rs` | `planner/indexadvisor` | `executor` |
| 191 | `tidb-util` | `cgmon.rs` | `util/cgmon` | `util/memory` |
| 182 | `tidb-util` | `sys/linux.rs` | `util/sys/linux` | `util/memory` |
| 177 | `tidb-distsql` | `request.rs` | `distsql` | `store/copr` |
| 176 | `tidb-expr` | `binary_literal.rs` | `types` | `expression` |
| 176 | `tidb-ddl` | `placement_delivery.rs` | `domain/infosync` | `ddl` |
| 174 | `tidb-util` | `zeropool/mod.rs` | `util/zeropool` | `util/memory` |
| 174 | `tidb-session` | `domain/wire_status.rs` | `server` | `session` |
| 169 | `tidb-executor` | `index_advisor/options.rs` | `planner/indexadvisor` | `executor` |
| 169 | `tidb-executor` | `index_advisor/keyset.rs` | `util/set` | `executor` |
| 167 | `tidb-datatype` | `parser_types_errors.rs` | `parser/types` | `types` |
| 166 | `tidb-planner` | `cardinality/index_range_policy.rs` | `planner/cardinality` | `planner/core, planner/core/base` |
| 162 | `tidb-session` | `ddl_session_pool.rs` | `ddl` | `session` |
| 160 | `tidb-expr` | `ranger_context.rs` | `util/ranger/context` | `expression` |
| 157 | `tidb-distsql` | `transport.rs` | `kv` | `store/copr` |
| 151 | `tidb-session` | `domain/embedded_resource_groups.rs` | `store/mockstore/unistore` | `session` |
| 150 | `tidb-txnkv` | `resource_group_tag.rs` | `util/resourcegrouptag` | `kv, store/driver` |
| 147 | `tidb-codec` | `duration.rs` | `util/codec` | `util/rowcodec` |
| 143 | `tidb-util` | `logutil/hex.rs` | `util/logutil` | `util/memory` |
| 143 | `tidb-util` | `extsort/mod.rs` | `util/extsort` | `util/memory` |
| 139 | `tidb-session` | `temptable.rs` | `table/temptable` | `session` |
| 138 | `tidb-planner` | `cardinality/join.rs` | `planner/cardinality` | `planner/core, planner/core/base` |
| 136 | `tidb-planner` | `index_columns.rs` | `planner/util` | `planner/core, planner/core/base` |
| 135 | `tidb-util` | `texttree.rs` | `util/texttree` | `util/memory` |
| 134 | `tidb-distsql` | `signed_handle_range.rs` | `distsql` | `store/copr` |
| 133 | `tidb-util` | `promutil/mod.rs` | `util/promutil` | `util/memory` |
| 131 | `tidb-domain` | `replayer.rs` | `util/replayer` | `domain` |
| 130 | `tidb-session` | `savepoints.rs` | `sessionctx/variable` | `session` |
| 129 | `tidb-planner` | `cardinality/uniform.rs` | `planner/cardinality` | `planner/core, planner/core/base` |
| 127 | `tidb-distsql` | `table_handle_ranges.rs` | `distsql` | `store/copr` |
| 123 | `tidb-util` | `paging.rs` | `util/paging` | `util/memory` |
| 122 | `tidb-session` | `util_config.rs` | `util/config` | `session` |
| 119 | `tidb-util` | `generic/sync_map.rs` | `util/generic` | `util/memory` |
| 115 | `tidb-util` | `ppcpuusage.rs` | `util/ppcpuusage` | `util/memory` |
| 115 | `tidb-txnkv` | `prefix_ops.rs` | `util` | `kv, store/driver` |
| 113 | `tidb-planner` | `table_sampler.rs` | `planner/util/tablesampler` | `planner/core, planner/core/base` |
| 113 | `tidb-error` | `catalog/mod.rs` | `util/dbterror` | `errno, parser/mysql` |
| 108 | `tidb-planner` | `task_type.rs` | `planner/property` | `planner/core, planner/core/base` |
| 106 | `tidb-planner` | `cardinality.rs` | `planner/cardinality` | `planner/core, planner/core/base` |
| 103 | `tidb-domain-core` | `session_transaction.rs` | `sessiontxn/isolation` | `domain` |
| 102 | `tidb-util` | `backoff.rs` | `util/backoff` | `util/memory` |
| 102 | `tidb-planner` | `column_length.rs` | `planner/util` | `planner/core, planner/core/base` |
| 102 | `tidb-domain` | `disttask.rs` | `util/disttask` | `domain` |
| 101 | `tidb-util` | `topsql_stmtstats/mod.rs` | `util/topsql/stmtstats` | `util/memory` |
| 94 | `tidb-util` | `fastrand/runtime.rs` | `util/fastrand` | `util/memory` |
| 94 | `tidb-model` | `cascades_hash.rs` | `planner/cascades/base` | `meta/model` |
| 92 | `tidb-util` | `context/mod.rs` | `util/context` | `util/memory` |
| 92 | `tidb-planner` | `by_item.rs` | `planner/util` | `planner/core, planner/core/base` |
| 91 | `tidb-txnkv` | `trxevents.rs` | `util/trxevents` | `kv, store/driver` |
| 89 | `tidb-util` | `mathutil/exponential_average.rs` | `util/mathutil` | `util/memory` |
| 89 | `tidb-expr` | `aggregation/names.rs` | `parser/ast` | `expression` |
| 86 | `tidb-planner` | `domain_misc.rs` | `planner/util/domainmisc` | `planner/core, planner/core/base` |
| 85 | `tidb-txnkv` | `transaction/schema_lease.rs` | `sessiontxn/isolation` | `kv, store/driver` |
| 85 | `tidb-planner` | `cardinality/out_of_range.rs` | `planner/cardinality` | `planner/core, planner/core/base` |
| 81 | `tidb-error` | `server_errors.rs` | `server/err` | `errno, parser/mysql` |
| 80 | `tidb-unistore` | `tikv_util.rs` | `store/mockstore/unistore/tikv` | `store/mockstore/unistore/lockstore, store/mockstore/unistore/util/lockwaiter` |
| 78 | `tidb-metadef` | `virtual_tables.rs` | `infoschema` | `meta/metadef` |
| 76 | `tidb-txnkv` | `tiflash.rs` | `util/tiflash` | `kv, store/driver` |
| 73 | `tidb-distsql` | `kv_request.rs` | `kv` | `store/copr` |
| 71 | `tidb-domain-core` | `domain/stat.rs` | `ddl` | `domain` |
| 67 | `tidb-br` | `rtree.rs` | `util/rowcodec` | `tablecodec` |
| 64 | `tidb-util` | `size/mod.rs` | `util/size` | `util/memory` |
| 62 | `tidb-util` | `slice.rs` | `util/slice` | `util/memory` |
| 60 | `tidb-util` | `topsql_reporter/mod.rs` | `util/topsql/reporter` | `util/memory` |
| 60 | `tidb-util` | `breakpoint.rs` | `util/breakpoint` | `util/memory` |
| 56 | `tidb-util` | `format.rs` | `util/format` | `util/memory` |
| 55 | `tidb-util` | `systimemon.rs` | `util/systimemon` | `util/memory` |
| 54 | `tidb-util` | `sys/storage.rs` | `util/sys/storage` | `util/memory` |
| 52 | `tidb-session` | `analyze_service.rs` | `executor` | `session` |
| 51 | `tidb-util` | `versioninfo.rs` | `util/versioninfo` | `util/memory` |
| 49 | `tidb-util` | `mvmap/fnv.rs` | `util/mvmap` | `util/memory` |
| 48 | `tidb-timer` | `table_store/mod.rs` | `timer/tablestore` | `timer/api` |
| 41 | `tidb-planner` | `base_traits.rs` | `planner/cascades/base` | `planner/core, planner/core/base` |
| 40 | `tidb-executor` | `ddl/indexes.rs` | `ddl` | `executor` |
| 37 | `tidb-stats-handle-lockstats` | `lock_target.rs` | `executor/lockstats` | `statistics/handle/lockstats` |
| 36 | `tidb-util` | `mathutil/mod.rs` | `util/mathutil` | `util/memory` |
| 36 | `tidb-util` | `disk/mod.rs` | `util/disk` | `util/memory` |
| 36 | `tidb-planner` | `core_usage.rs` | `planner/util/coreusage` | `planner/core, planner/core/base` |
| 35 | `tidb-util` | `filter/schema.rs` | `util/filter` | `util/memory` |
| 35 | `tidb-util` | `encrypt/mod.rs` | `util/encrypt` | `util/memory` |
| 30 | `tidb-util` | `fastrand/mod.rs` | `util/fastrand` | `util/memory` |
| 28 | `tidb-util` | `kvcache.rs` | `util/kvcache` | `util/memory` |
| 27 | `tidb-util` | `channel.rs` | `util/channel` | `util/memory` |
| 24 | `tidb-util` | `generic/mod.rs` | `util/generic` | `util/memory` |
