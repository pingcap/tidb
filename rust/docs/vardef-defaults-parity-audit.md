# vardef defaults parity audit: Go `pkg/sessionctx/vardef` vs `tidb-vardef`

Audit date: 2026-09-05. Method: mechanical — a script parses every Go
`Def*` constant in `pkg/sessionctx/vardef/tidb_vars.go` (395) and every
`/// Go \`DefX\` (= \`value\`).` entry in
`rust/crates/tidb-vardef/src/defaults.rs` (400), then diffs both the name
sets and the cited values.

## Results

- **Value staleness: zero.** All 400 name-matched cited values are
  byte-current against today's Go literals.
- **Missing defaults: 4, FIXED this batch.** `DefTiDBQueryCopStoreLimit`
  (15), `DefTiDBColumnarStorageEnabled` (true),
  `DefTiDBMergePartitionStatsConcurrency` (1), and
  `DefTiDBServerMemoryLimit` (`serverMemoryLimitDefaultValue()`, which
  resolves to `"80%"` whenever the machine total is readable) are now in
  `defaults.rs`, pinned by `late_added_defaults_match_go`.
- **Missing name constants: 2, FIXED this batch.** `TiDBQueryCopStoreLimit`
  and `TiDBColumnarStorageEnabled` added byte-identical to
  `tidb_vars.go:322/804`.
- **Rust-only constants: 9, deliberately kept.** The
  `DefTiDBMView*`/`DefTiDBTxnFile*`/`DefTiDBEmbedOpenAI*`/
  `DefTiDBEnableFullOuterJoin`/`DefTiDBEnableSharedLockUpgrade` family has
  no Go counterpart anywhere under `pkg/sessionctx`; they belong to this
  fork's own extensions and another active unit's surface.
- **Registry consistency.** Go's `sysvar.go` registry carries no
  `SysVar` entries for `tidb_query_cop_store_limit` /
  `tidb_columnar_storage_enabled` (only a `setvar_affect.go` mention), so
  the Rust session catalog matching their absence is correct, not a gap.

## Validation

`cargo test -p tidb-vardef` 44+3+3 green (including the new regression);
fmt/clippy/`git diff --check`/`make lint` pass on the batch commit.

## Registry attribute diff (2026-09-05, second pass)

A second pass compared every single-line `SysVar` entry in Go's
`defaultSysVars`/`noopSysVars` (427 parseable, field-level: scope, value,
type, min, max) against the Rust `SysVarDef` catalog. Result: **zero real
divergences.** The only four flagged rows (`tidb_auto_analyze_start_time`,
`tidb_auto_analyze_end_time`, `tidb_evolve_plan_task_start_time`,
`tidb_evolve_plan_task_end_time`) were an artifact of the diff script
lacking Go's `TypeTime` in its type table — Go declares `TypeTime`
(`sysvar.go:865-866`) and Rust's `VarType::Time` mirrors it exactly.

Multi-line entries (those carrying `GetGlobal`/`SetGlobal`/`Validation`
hooks) are outside this mechanical pass; their behavior parity is the
per-variable audit surface, not a table diff.

## Validation-hook coverage (2026-09-05, third pass)

Of the 75 Validation-bearing Go sysvar entries, the following have their
hook behavior ported and regression-pinned in Rust: the
`validate_password.*` coupling (5), `tidb_read_consistency`,
`collation_database`, `character_set_database`,
`mpp_exchange_compression_mode`, `runtime_filter_type/mode` (2),
`init_connect`, and `mpp_version`. `tidb_dml_type` needs no port (its
hook is next-gen-only), and `tiflash_hashagg_preaggregation_mode` does
not exist on this Go master.

The remaining 30 split into: 2 already covered by the generic enum
validation (`tidb_enable_global_index`, `tidb_replica_read`), 1
deprecated warn-only (`tidb_enable_new_cost_interface`), 10 with real
validation logic to port (`tidb_enable_tiflash_pipeline_model`,
`tidb_gogc_tuner_threshold`, `tidb_mem_arbitrator_mode`,
`tidb_mem_arbitrator_query_reserved`, `tidb_mem_arbitrator_soft_limit`,
`tidb_mem_arbitrator_wait_averse`, `tidb_opt_index_join_build_v2`,
`tidb_pessimistic_txn_fair_locking`, `tidb_schema_cache_size`,
`tx_read_ts`), and 17 whose entries the block parser could not reach
(mostly deprecated warn-only shapes; verify individually when
processed).
