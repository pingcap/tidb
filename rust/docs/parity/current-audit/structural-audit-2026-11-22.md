# Structural audit, 2026-11-22

A fresh full re-audit after the 2026-11-18 rows were all closed. The earlier
audit matched Go packages to Rust crates by name, so a module missing inside a
matched package (a Go file with no Rust counterpart) could hide. This pass
works at file level, re-runs the refusal scan and the duplicate scan, and
probes a Go and a Rust server side by side.

## Method

1. File coverage. Every non-test, non-generated Go file of the 451 `pkg/`
   packages linked into `cmd/tidb-server` (`go list -deps ./cmd/tidb-server`)
   was scored by how many of its top-level functions and types appear in the
   Rust sources (the Go name, or its snake case). The 113 files under 40% with
   at least 120 lines were read against the Rust code. Most are ported under
   other names or decompositions (the auto-analyze priority queue, the hash
   join spill helper, semi-join probes, stale-read processing, show placement,
   predicate-column collection, index pruning); metrics init, parser
   recursive-descent helpers, Lightning and test utilities are not findings.
2. Refusal scan. Every Rust error text that is not a Go string ("this node
   does not ...", "waits on its DDL course", "not yet supported", "not
   ported") outside tests was classified: reachable ones were probed against
   Go, unreachable ones (cascades-only operators, a dead converter arm) were
   left.
3. Live differential. A Go server built from this tree's `pkg/` and the Rust
   `tidb-server` ran side by side on unistore; scripts went through pymysql
   and the answers were compared after masking timestamps, TSOs and ids.
   Probed: ALTER TABLE (72 variants), CREATE TABLE column options and
   partitioning, index kinds, DEFAULT and generated expressions, check
   constraints, joins and join-reorder thresholds, both cost models, EXPLAIN
   EXPLORE, ANALYZE (partitioned, index, options), TRAFFIC, SHOW PLACEMENT,
   ADMIN RELOAD, client error summaries, TRX_SUMMARY, column stats usage,
   index pruning, IMPORT INTO, SELECT INTO OUTFILE, the information_schema /
   performance_schema / mysql / metrics_schema / sys column catalogs, and
   every global and session variable.
4. Duplicate implementations. Every Rust definition whose first doc line
   names a Go symbol was grouped by that symbol; a Go function defined once in
   `pkg/` and implemented by two or more same-named Rust functions was read.
   Interface implementations (`exprstatic` and `sessionexpr` evaluation
   contexts, `ConstLevel` per expression kind), BR's own copies (Go's `br/`
   has them too), DXF `handle` wrappers over `storage`, and thin delegating
   wrappers are not findings.

## Found and fixed in this pass

| Finding | Go | Rust before | Fix |
| --- | --- | --- | --- |
| SIGUSR1 | `signal.SetupUSR1Handler` dumps stacks | default action killed the server | `shutdown_signal::setup_usr1_handler` |
| CREATE TABLE / ADD COLUMN column options | `columnDefToCol` ignores COLUMN_FORMAT, STORAGE, SECONDARY_ENGINE_ATTRIBUTE, inline REFERENCES | refused | ignored as Go |
| DEFAULT naming a column | `EvalSimpleAst`: 1054 `Unknown column 'x' in 'expression'` | resolved against preceding columns, then 1105 | resolved against no columns |
| CREATE SPATIAL INDEX | 8200 `SPATIAL index is not supported` | 8200 Rust text | Go text |
| CREATE TABLE PARTITION BY SYSTEM_TIME | warning 8200, normal table | 1105 refusal | Go warning |
| ALTER TABLE STATS_OPTIONS | default arm `ErrUnsupportedAlterTableSpec` | `ErrUnsupportedAlterTableOption` | Go arm |
| client_errors_summary_* | counts every ERR and the flushed warnings | empty: the packet writer dropped the counts | forwarded; warnings counted once per response |
| max_connections | checked in `openSession` during the handshake, against registered clients and the live limit | refused at accept, before any handshake, against the startup limit | `ServerClients::check_connection_count` |
| SEND_LONG_DATA bound | session `max_allowed_packet` | wire default | session value |
| DEFAULT (@@var) / ((SELECT 1)) | `requirePlanCtx` error | Rust text | Go text |
| SELECT ... INTO OUTFILE | `SelectIntoExec` writes the file, OK packet | rows sent to the client, no file | `select_into::SelectIntoExec` |

## Not registered

- `ALTER TABLE t PARTITION BY SYSTEM_TIME (...)`: Go keeps the old
  partitioning when `buildTablePartitionInfo` does not enable the type, and the
  reorganization it then runs leaves an unpartitioned table showing
  `PARTITION BY NONE () (PARTITION pFullTable ...)`. This is a Go bug; Rust
  refuses the statement.
- `IMPORT INTO` on unistore: Go answers a nil-pointer panic message; Rust says
  the instance has no TiKV store.
- Go master surface the branch lacks (`TIDB_MVIEWS`, `TIDB_MLOGS`,
  `TIDB_TABLE_MVIEW_DEPENDENCIES`, the `tidb_mview_*` / `tidb_mlog_*`
  variables, `tidb_enable_full_outer_join`): Rust follows Go master.

## Duplicate scan (X01, X02)

All were collapsed to one body: X01 in this pass, and `getJobsBySQL` with
X02 (one DDL session port, 2026-11-23). `AggFunctionModeToPB`
was the visible end of a whole second ToPB family in `builder_utils.rs` for
TiDB-store readers, removed with it.

| Go function | Rust implementations |
| --- | --- |
| `aggregation.AggFunctionModeToPB` | `tidb-executor/src/builder_utils.rs`, `tidb-expression-aggregation/src/agg_to_pb.rs` |
| `variable.BoolToOnOff` | `tidb-variable/src/sysvar/ldap_hooks.rs`, `tidb-variable/src/vars.rs` |
| `ddl.CheckDropTablePartition` | `tidb-ddl-build/src/partition_ddl.rs`, `tidb-ddl/src/cluster_ddl/partition.rs` |
| `expression.ContainCorrelatedColumn`, `ContainVirtualColumn` | `tidb-expr/src/expr_util/predicates.rs`, `tidb-planner-core-operator-physicalop/src/final_mode_agg.rs` |
| `expression.IsBinaryLiteral` | `tidb-expr/src/builtin.rs`, `tidb-expr/src/scalar_function/pb_builtin.rs` |
| `expression.base64NeededEncodedLength` | `tidb-expr/src/builtin_string.rs`, `tidb-expr/src/string_packet.rs` |
| `expression.newReturnFieldTypeForBaseBuiltinFunc` | `tidb-expr/src/builtin.rs`, `tidb-expr/src/builtin_arithmetic.rs` |
| `expression.setDecimalFromArgs` | `tidb-expr/src/builtin_control.rs`, `tidb-expr/src/rewriter/control_type.rs` |
| `session.IsUpgradingClusterState` | `tidb-session/src/sync_upgrade.rs`, `tidb-session/src/upgrade_run.rs` |
| `ddl.getJobsBySQL` | `tidb-ddl/src/ddl_history.rs`, `tidb-ddl/src/job_control.rs` |
| `expression.InferType4ControlFuncs`, `setDecimalFromArgs`, `setFlenFromArgs` | `tidb-expr/src/builtin_control.rs`, `tidb-expr/src/rewriter/control_type.rs` |
| `domain.insertPlanReplayerStatus` | `tidb-domain/src/plan_replayer.rs`, `tidb-session/src/plan_replayer.rs` |
| `ddl.verifyNoOverflowShardBits` | `tidb-ddl/src/cluster_ddl.rs`, `tidb-table/src/tables.rs` |
| `ddl.isNullToNotNullChange` | `tidb-ddl-build/src/modify_column_type.rs`; `tidb-ddl/src/cluster_ddl/materialized_view.rs` re-implements it |

Read and not kept: `CurrentTime`, `GetCharsetInfo`, `GetMaxAllowedPacket`
(the two evaluation-context implementations Go has too), `ConstLevel` (one per
expression kind), `GetGCSafePoint`, `GetPDScheduleConfig`, `GetPlacementRule`,
`createTable`, `updateChecksumResponse` (BR's own copies), the DXF `handle`
wrappers over `storage`, `RegisterSQL` / `RegisterPlan` (`topsql` over its
reporter), `doLockKeys` (trait and implementation), `applyPredicateSimplification`
(logicalop's function-variable trait and the rule's body),
`checkModifyCharsetAndCollation` and `checkTableInfoValidExtra` (error-adapting
wrappers over the one body), `readLastLines` (the
executor's and `sysutil`'s).
