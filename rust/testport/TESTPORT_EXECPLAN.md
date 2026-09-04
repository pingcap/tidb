# Go-parity audit for the Rust SQL path

This ExecPlan is a living document per `PLANS.md`. Keep `Progress`,
`Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective`
current while the audit proceeds.

## Purpose / Big Picture

Make the Rust SQL implementation behaviorally match TiDB at the selected Go
source revision for each package. Historical receipts through 2026-08-31 use
the pinned Go commit `e2788410d8d696605e8cb002585877a063ccc909`; the current
rolling comparison follows the user's Go `master` request and records the
exact fetched `origin/master` commit in each new receipt. The comparison
source is the Go code itself. Rust-only execution policy, cache-specific
pipelines, documentary gap tests, and receipts that imply parity without
executable behavior are not part of the target. A completed transcreation
claim is package-atomic as required by root `AGENTS.md`; partial work remains
implementation progress and must not be presented as a completed Go-package
port.

The user explicitly permits coordinated changes across multiple Rust crates.
Go source and Bazel metadata may be restored when the current branch is
missing behavior present at the fetched Go master; such changes remain tied to
the complete owning package and are recorded in the package receipt.

## Method

For each bounded behavior cluster:

1. Read the complete owning Go package at the pinned commit, including package
   documentation, production variants, tests, fixtures, and support artifacts.
2. Inventory the corresponding Rust owners and all call sites before editing.
3. Preserve executable behavior that matches Go. Delete empty ignored tests,
   stale docs, and receipts that merely describe unavailable behavior.
4. Where Go behavior is missing, implement it in the ordinary Rust planning or
   execution path rather than adding a cache-only or benchmark-only workaround.
5. Add or retain a Go-derived regression test and run the smallest WIP gate that
   proves the changed behavior. Use the Ready profile only before a final
   completion or PR-readiness claim.
6. Record remaining package inventory honestly; no repository-wide or
   package-complete parity claim is made while gaps remain.

## Progress
- 2026-09-05 (`pkg/sessionctx/variable` instance-scoped getters): added the
  live config/atomic read-through used by Go's `GetSessionOrGlobalSystemVar`
  for the `TestInstanceScopedVars` matrix, preserving explicit `SET INSTANCE`
  overrides and covering the JSON config getter. The focused regression and
  receipt now record the remaining process-wide setter/unlisted-variable
  boundaries.
- 2026-09-05 (`pkg/sessionctx/variable` next-gen DML/replica-read hooks): added
  typed `BulkDMLEnabled` and replica-read state to Rust `SessionVars`, wired
  inheritance/restore and statement snapshots, and matched Go's next-gen 1235
  refusal branches for BULK and non-LEADER replica modes. Added focused
  session and statement regressions; external next-gen build-tag and storage
  selection fixtures remain explicit receipt boundaries.
- 2026-09-05 (`pkg/sessionctx/variable` pessimistic fair-locking hook): added
  typed `SessionVars` state for `tidb_pessimistic_transaction_fair_locking`
  and wired the Go next-generation validation branch, including the 1235
  refusal and canonical OFF behavior. Added a focused setter/restore
  regression; the receipt remains partial because this Rust harness does not
  run Go's external next-gen build-tag fixture end-to-end.
- 2026-09-05 (`pkg/sessionctx/variable` TTL job global hook): added the
  process-wide `EnableTTLJob` atomic and wired GLOBAL writes, resets, cluster
  loads, and committed-image replacement to the same getter/setter authority
  Go's TTL worker uses. Removed the now-stale empty TiFlash/TTL gap tests and
  added a focused regression; external-workload notification remains a
  documented partial boundary.
- 2026-09-05 (`pkg/sessionctx/variable` receipt cleanup): removed the stale
  empty ignored shims for every sysvar test whose executable coverage now
  lives in `tidb-session`; the remaining `tidb-vardef` inventory is 13
  passing constant/initial-value checks and 8 explicit gap tests.
- 2026-09-05 (`pkg/sessionctx/variable` embedding inventory): removed the
  three stale vardef embedding shims now covered by the session-owned
  endpoint normalizer, GLOBAL configuration-version hook, and API-key
  masking/version tests.
- 2026-09-05 (`pkg/sessionctx/variable` deprecated retry switch): wired
  `tidb_disable_txn_auto_retry` so deprecated OFF assignments warn with 1287
  and remain canonically ON for both SESSION and GLOBAL SQL writes; added a
  focused regression and removed its stale vardef shim.
- 2026-09-05 (`pkg/sessionctx/variable` low-resolution TSO interval): added
  SQL-level GLOBAL clamp/readback coverage for the 10..60000ms bounds and
  original-value 1292 warnings, preserving the process-wide setter callback
  as an explicit boundary.
- 2026-09-05 (`pkg/sessionctx/variable` schema-cache hook): added the
  process-wide `SchemaCacheSize` byte publication for validated GLOBAL,
  startup, cluster-load, reset, and committed-image paths, with focused
  suffix/floor/default regressions and the optional cache-resize callback
  retained as an explicit boundary.
- 2026-09-05 (`pkg/sessionctx/variable` circuit-breaker/window hooks): added
  the process-wide PD metadata error-rate ratio publication with Go's [0, 1]
  clamp/warning behavior, plus typed `SessionVars.EnableWindowFunction`
  updates for ON/0/1 session writes. Focused regressions cover the SQL
  readback and typed products; optional PD callback and absent planner consumer
  remain explicit boundaries.
- 2026-09-05 (`pkg/sessionctx/variable` auto-analyze concurrency gate): wired
  the `RunAutoAnalyze`, priority-queue, and concurrency process products and
  made GLOBAL concurrency writes enforce Go's prerequisite switches against
  the pending table image. The focused regression covers both refusal paths,
  the source error text, and successful atomic/readback publication.
- 2026-09-05 (`pkg/sessionctx/variable` auto-analyze ratio validation): ported
  Go's minimum-ratio closure while preserving values above one and the exact
  bare refusal error. Focused SQL coverage verifies failed writes leave the
  previous GLOBAL value intact and that the 0.00001 minimum is accepted.
- 2026-09-05 (`pkg/sessionctx/variable` resource-control hooks): published
  Go's process-wide enable and strict-mode switches on named GLOBAL writes,
  startup/cluster/reset paths, and changed resource-group hint admission to
  consume the authoritative typed global state. Added a focused ON/OFF
  regression; the optional domain callback remains outside this Rust owner.
- 2026-09-05 (`pkg/sessionctx/variable` TTL schedule-window inventory): wired
  GLOBAL TypeTime writes through the issuing session's timezone so short
  `HH:MM` values retain the same numeric offset as Go's `ParseInLocation`;
  expanded-value preservation and invalid-clock refusal remain covered by
  focused regressions, and the stale vardef shim is removed.
- 2026-09-05 (`pkg/sessionctx/variable` status registry inventory): reconciled
  `TestStatusVar` to the existing `tidb-exec` provider registry, added source
  coverage for last-provider overwrite and provider-error propagation, and
  removed the stale `tidb-vardef` ignored shim. The process-global mutex and
  SessionVars-aware default provider remain an explicit partial boundary.
- 2026-09-05 (`pkg/sessionctx/variable` TiFlash session hooks): added typed
  SessionVars state for the three external-spill thresholds, per-node memory
  quota, and query spill ratio. Session writes, statement restore, GLOBAL
  inheritance for new sessions, and materialized-view execution capture now
  follow Go's SetSession hook products instead of reparsing strings. Added
  focused regressions; the receipt retains the downstream planner-consumer
  boundary as partial.
- 2026-09-05 (`pkg/sessionctx/variable` server memory limit): wired the
  existing Go-compatible memory-limit parser into the Rust sysvar registry,
  including unit/percentage normalization, the 512MiB floor, and tested
  `u64::MAX` acceptance; relaxed the process helper's unnecessary signed
  conversion. Added end-to-end byte-form GLOBAL coverage and reconciled both
  server-memory-limit receipt rows, retaining host/failpoint and exact-error
  boundaries explicitly.
- 2026-09-05 (`pkg/sessionctx/variable` mock accessor auth-plugin path):
  covered the real GLOBAL `default_authentication_plugin` enum rejection and
  accepted value path from `TestMockAPI`. The Go test's helper-only
  `GetTiDBTableValue("tikv_gc_life_time")` lookup remains outside Rust's
  production sysvar owner and is recorded as partial.
- 2026-09-05 (`pkg/sessionctx/variable` trace-event global hook): connected
  `SET GLOBAL tidb_trace_event` to the existing Rust flight-recorder runtime,
  including JSON trigger compilation, replacement, and empty-value close.
  Added an end-to-end regression; the Go classic-kernel refusal branch remains
  an explicit partial boundary.
- 2026-09-05 (`pkg/sessionctx/variable` memory-limit parsing and remaining
  defaults): matched Go's byte-suffix parsing and 128-byte clamp for
  `tidb_server_memory_limit_sess_min_size`, canonicalized GC-trigger
  percentages/fractions with the Go range, and added GLOBAL regressions.
  Reconciled the partition-prune and ignore-inlist default rows; process-wide
  memory tuner atomics and threshold coupling remain explicitly partial.
- 2026-09-05 (`pkg/sessionctx/variable` DDL and optimizer globals): added
  executable Go-derived coverage for DDL reorg worker/batch unsigned bounds
  and shared GLOBAL round trips for index merge, fast reorg, disk quota,
  aggregate pushdown, and derive top-N. Reconciled the corresponding b010
  rows as already-ported; process-wide hooks outside the SQL sysvar accessor
  remain separate inventory items.
- 2026-09-05 (`pkg/sessionctx/variable` locale and charset compatibility):
  reconciled `TestLcTimeNamesReadOnly`, `TestLcMessages`, and
  `TestDefaultCharsetAndCollation`. A focused session regression now pins the
  `en_US`, `utf8mb4`, and `utf8mb4_bin` defaults, the mutable `lc_messages`
  setter, and the read-only `lc_time_names` 1238 refusal.
- 2026-09-05 (`pkg/sessionctx/variable` live session getters): wired the
  missing `tidb_last_query_info` getter to session state, preserving Go's
  zero-value `QueryInfo` JSON shape alongside the existing current-TS,
  last-transaction, plan-cache, and binding getters. Added a focused
  `TestSessionGetterFuncs` regression and reconciled the timestamp and
  identity receipt rows with their already-live Rust owners; query-diagnostic
  mutation/RU accounting remains explicitly partial.
- 2026-09-05 (`pkg/sessionctx/variable` removed options): ported Go's removed
  sysvar compatibility table, parse-but-ignore SET behavior, and explicit
  8136 read errors with replacement guidance. Added a case-insensitive
  end-to-end regression for both SET scopes and reconciled `TestRemovedOpt`.
- 2026-09-05 (`pkg/sessionctx/variable` numeric bounds): added focused
  regressions for NetBufferLength, TiDBBatchPendingTiFlashCount,
  TiFlashMaxBytes, TiFlashMemQuotaQueryPerNode, TiFlashQuerySpillRatio,
  QueryLogMaxLen, CommitterConcurrency, and DDLFlashbackConcurrency. The
  TiFlash numeric validation closures now match Go's -1 and 0.85 behavior;
  typed SessionVars TiFlash hook fields remain explicitly partial.
- 2026-09-05 (`pkg/sessionctx/variable` remaining scalar hooks): added TTL
  schedule-window UTC normalization, memory-debug defaults, and distributed
  reorg global round-trip coverage; reconciled their b010 receipt rows while
  retaining the process-wide typed TTL getter as an explicit boundary.
- 2026-09-05 (`pkg/sessionctx/variable` identity getter): added the missing
  direct `@@last_insert_id` regression for the zero value and a value above
  `i64::MAX`, including the `@@identity` alias, and reconciled `TestLastInsertID`.
- 2026-09-05 (`pkg/sessionctx/variable` secure_auth validation): ported
  `TestSecureAuth`'s per-variable validation. `secure_auth=OFF` now returns
  Go's 1231 wrong-value error and leaves the global ON default unchanged;
  ON remains accepted. Added both registry-level and end-to-end GLOBAL
  assignment regressions and reconciled the b010 receipt row.
- 2026-09-05 (`pkg/sessionctx/variable` SQL mode and no-op read-only gates):
  reconciled the remaining `TestSQLModeVar`, `TestReadOnlyNoop`, and
  `TestSQLAutoIsNull` receipt rows with their live Rust owners. Added a
  global-scope regression covering all five `noop.go` variables: OFF refuses
  with 1235 and leaves the value unchanged, while the global
  `tidb_enable_noop_functions` gate allows ON and reset. Existing focused
  tests cover Go SQL-mode normalization/typed state and the relaxed
  `sql_auto_is_null` SET_VAR path.
- 2026-09-05 (`pkg/sessionctx/variable` transaction isolation): reconciled
  `TestTxnIsolation` with the existing Go-aligned validation and session hook.
  Added a focused regression proving `SET GLOBAL
  tidb_skip_isolation_level_check=ON` does not change the writer's session,
  while a subsequently seeded session inherits the relaxed check and stores
  `SERIALIZABLE` with warning 8048. The receipt now records the aliases,
  normalization, refusal, warning, and inheritance coverage as live.
- 2026-09-05 (`pkg/sessionctx/variable` timezone typed state): aligned
  `tidb-session` with Go's `TestTimeZone`. The validated session zone is now
  resolved once into statement-facing typed state (including fixed-offset
  names, named IANA zones, and SYSTEM fallback), restored and inherited with
  the session image, and consumed directly by statement snapshots. Focused
  regressions cover Go's validation boundaries and typed propagation.
- 2026-09-05 (`pkg/sessionctx/variable` SQL SELECT limit state): aligned
  `tidb-session` with Go's `TestSQLSelectLimit` setter contract. The
  normalized limit now lives in typed session state, survives restore and
  GLOBAL-to-new-session inheritance, and `TryAddExtraLimit` consumes that
  state. The focused typed-state regression passes; the broader existing
  scalar-subquery fixture remains a separate planner-schema failure and is
  recorded as a partial-port boundary in b010.
- 2026-09-05 (`pkg/sessionctx/variable` multi-statement mode state): aligned
  `tidb-session` with Go's `TestTiDBMultiStatementMode` and the connection
  admission path. The normalized OFF/ON/WARN enum now lives in typed session
  state, statement splitting uses that state instead of reparsing raw text,
  and restore/GLOBAL inheritance are covered. Focused regressions cover the
  Go value matrix and capability/refusal/deferred-warning behavior.
- 2026-09-05 (`pkg/sessionctx/variable` max-execution-time typed state): aligned
  `tidb-session` with Go's `TestMaxExecutionTime`. The validated
  `max_execution_time` value now lives in typed session state, survives
  statement restore and GLOBAL-to-new-session inheritance, and the statement
  snapshot consumes that state for the millisecond deadline instead of
  reparsing raw text. The source-derived regression pins clamping, setter,
  restore, and inheritance behavior; existing SET_VAR coverage verifies the
  statement-level overlay.
- 2026-09-05 (`pkg/sessionctx/variable` max-keys-read typed state): aligned
  `tidb-session` with Go's `TestTiDBMaxKeysRead` and `TestGetMaxKeysRead`.
  `SessionVars` now maintains the validated `tidb_max_keys_read` value in
  typed state, exposes the Go SELECT-only accessor contract, restores and
  inherits it with the session image, and the statement snapshot consumes that
  accessor instead of reparsing raw text. The source-derived regression covers
  clamping, setter propagation, statement restore, global inheritance, and the
  outside-SELECT zero sentinel; the existing hint-updatable registry test pins
  the Go marker.
- 2026-09-05 (`pkg/sessionctx/variable` default utf8mb4 collation): aligned
  `tidb-session` with Go's registry validation and deprecation warning. Valid
  names now canonicalize and emit warning 1681; non-utf8mb4 collations fail
  with the registered 3721 diagnostic without leaking a warning. The focused
  source-derived regression is live and b010 records the row as ported.
- 2026-09-05 (`pkg/sessionctx/variable` collation-server validation): aligned
  `tidb-session` with Go's `checkCollation` and `CollationServer.SetSession`
  hook.  `collation_server` now canonicalizes registry names, rejects unknown
  names with 1273, and mirrors the selected charset into
  `character_set_server`; the source-derived session regression is live and
  the b010 receipt no longer records this behavior as an ignored gap.
- 2026-09-05 (`pkg/sessionctx/variable` embedding receipt): re-audited the
  b010 rows against the live session owner. `tidb-session::embedding` and
  `tests_session_embedding_source` already cover endpoint normalization and
  whitelist errors, GLOBAL base-URL generation semantics, and six masked API
  keys; b010 now points to those passing tests instead of stale vardef-only
  ignored stubs.
- 2026-09-05 (`pkg/ddl` kill-flag helpers): transcreated the pure DDL
  `isSessionDone`, `convertKillFlag`, and `isRetryableDDLCancelErr` contracts
  in `tidb-executor::ddl`. Focused regressions cover clean/interrupted killer
  states, zero/non-zero conversion, wrapped terminal cancellation errors, and
  retryable failures; the deferred TiFlash wait loop and DDL job queue remain
  explicitly out of scope.
- 2026-09-05 (`pkg/ddl` global-index version selection): verified the
  existing `tidb-exec::cluster_ddl::set_global_index_version` against Go's
  `setGlobalIndexVersion` shape matrix. A focused regression now covers the
  capability gate, non-unique indexes, unique nullable indexes, unique
  NOT-NULL indexes, clustered tables, and the legacy fallback; the b105
  receipt no longer records this helper as an untranscreated gap.
- 2026-09-05 (`pkg/ddl` continuous key-range merge): transcreated Go
  `mergeContinuousKeyRanges` and its `keyRangeMayExclude` wrapper in
  `tidb-executor::ddl`. Excluded ranges now terminate a run while kept ranges
  coalesce into owned half-open `KeyRange`s; the seven source cases from
  `pkg/ddl/ddl_test.go:360` are live rather than ignored. The DDL job queue and
  flashback-cluster orchestration remain separate documented gaps.
- 2026-09-05 (`pkg/meta/autoid` service ownership transfer): aligned
  `tidb-exec::AutoIdServiceAllocator` with Go master `singlePointAlloc.Transfer`.
  Transfer now takes an exclusive binding lock, refreshes the authoritative
  source base through `Alloc(0, 1, 1)`, rebases the destination to the greatest
  observed value, treats same-binding transfers as no-ops, and restores the
  source binding if the destination rebase fails. Focused Rust regressions
  cover request identities, monotonic transfer bases, no-op transfers, and
  rollback after an RPC error; the package receipt is updated.
- 2026-09-05 (`pkg/planner/core` row comparison): activated the Go-derived
  `TestCompareRow` regression now that Rust's AST-level `ROW(...)` comparison
  path is live in `tidb-expr`. The registry still excludes standalone `row`
  from its builtin-name list, matching Go's `GetBuiltinList` skip; this is not
  a behavior gap for comparison/`IN` operands. The planner source test now
  covers equality, inequality, lexicographic ordering, and NULL poisoning.
- 2026-09-05 (`pkg/ddl` non-touched partition traversal): aligned
  `tidb-model::PartitionInfo` with Go master `findNextNonTouchedPartitionID`.
  The metadata helper now walks definitions after the current ID, skips every
  dropping definition, and returns Go's zero sentinel for unknown/current-last
  IDs or an exhausted untouched suffix. The source-derived DDL regression is
  active; queue and cluster-lifecycle helpers remain explicitly documentary.
- 2026-09-05 (`pkg/ddl` anonymous index naming): aligned Rust `tidb-executor`
  ALTER ADD INDEX naming with Go's `GetName4AnonymousIndex`. Unnamed indexes
  now use the first key column (or `expression_index`), reserve the bare
  `PRIMARY` name, and suffix case-insensitive collisions (`c1_2`, `c1_4`,
  `primary_3`). The source-derived regression
  `add_anonymous_index_generates_the_next_free_suffix` is active and covers
  both ordinary and reserved-name collisions; the package receipt is updated.
- 2026-09-05 (`pkg/executor/sortexec` TopN zero-count short-circuit): aligned
  Rust's TopN limit handling with Go's `Limit.Count == 0` early return. The
  executor now retains the effective count separately from `offset + count`
  and avoids draining the child for `OFFSET > 0, LIMIT 0`; the focused
  fail-before/pass-after child-consumption regression and Ready checks are
  recorded in `receipts/executor_root_distsql_indexjoin.md`.
- 2026-09-05 (`pkg/planner/core` IndexJoin probe cardinality): aligned the
  Rust dispatcher with Go master `19a41f0d4a`. When an IndexJoin probe can
  build ranges from only a leading subset of equality join keys, the scan
  profile now receives the conservative `TableStats.RowCount / NDV(used
  prefix)` floor before costing; complete-key, pseudo-statistics, integer
  handle, range-tail, invalid-NDV, and Fix44855-off cases fail closed. The
  focused Rust regression is active, while the source-derived SQL plan-tree
  comparison remains an explicit mock-store/analyze/cascades boundary. See
  `receipts/planner_index_join_row_floor.md` and the updated `b082.md` row.
- 2026-09-05 (`pkg/parser` parenthesized temporal intervals): aligned the
  hand-written Rust expression parser with Go master `5bdb1b6bd1`. The
  keyword-form `INTERVAL (expr) UNIT` now wins over the scalar
  `INTERVAL(expr, ...)` function only when the balanced value has no
  top-level comma and is followed by a recognized time unit; zero- and
  one-argument scalar `INTERVAL` calls are rejected like Go. The complete
  Go-root inventory and the source-derived builtin regression matrix are
  recorded in `receipts/parser_root.md`.
- 2026-09-05 (`pkg/executor/sortexec` RankTopN prefix-group short-circuit):
  aligned Rust with Go's `RankInfo` prefix-index read boundary. Physical
  TopN metadata now resolves the prefix column into `TopNExec`, which cuts
  values through `index_prefix_cut`, preserves exact-value semantics for the
  `-1` sentinel, and retains only the `offset + count` rows plus the
  contiguous boundary-prefix group. Focused fail-before/pass-after regressions
  and Ready checks are recorded in
  `receipts/executor_root_distsql_indexjoin.md`; full planner partial-order
  candidate generation remains an explicit boundary because Rust's `CopTask`
  does not yet carry Go's `PartialOrderMatchResult`.
- 2026-09-05 (`pkg/executor/sortexec` serial sort cancellation polling):
  aligned the serial `SortPartition` comparator and spill writer with Go's
  10,240-comparison and post-spill-chunk SQL-killer checkpoints. The focused
  fail-before/pass-after regression and existing serial sort/spill checks are
  recorded in `receipts/executor_root_distsql_indexjoin.md`.
- 2026-09-05 (`pkg/executor/sortexec` parallel worker cancellation polling):
  aligned Rust's persistent parallel sort workers with Go's SQL-killer
  checkpoints. Worker batch comparators now poll the statement killer every
  20,000 comparisons, and local K-way merges poll every 100 emitted rows; the
  two focused fail-before/pass-after regressions, existing parallel-sort tests,
  and Ready checks are recorded in
  `receipts/executor_root_distsql_indexjoin.md`.
- 2026-09-05 (`pkg/executor/sortexec` TopN spill cancellation polling): aligned
  `SpilledRun::write` with Go's `topNSpillHelper.spillHeap` SQL-killer polling.
  The Rust writer now checks the statement killer every 100 original heap
  positions, including the output-time suffix index, and relies on the disk
  container's drop cleanup for cancelled partial runs. The focused
  fail-before/pass-after regression and Ready checks are recorded in
  `receipts/executor_root_distsql_indexjoin.md`.
- 2026-09-05 (`pkg/executor/aggfuncs` max/min-count pair state): ported the
  dependency-closed evaluator state into `tidb-exec`. `MaxMinCountState` now
  ignores NULLs, tracks rows tied at the winning MAX/MIN extreme, merges
  partial states with Go's winner-and-tie semantics, returns zero for empty
  groups, and preserves native UInt/Decimal/collated-string comparisons.
  Focused source-derived regressions and the `tidb-exec` Ready profile pass.
  Live `tidb-executor` hash-aggregation dispatch, tipb expression enums,
  row-based final mode, DISTINCT/window state, and SQL integration remain
  explicit cross-crate boundaries in `receipts/expression_aggregation_audit.md`.
- 2026-09-05 (`pkg/executor/aggfuncs` max/min-count live executor path):
  completed the dependency-closed runtime wiring beyond the standalone
  accumulator. `tidb-executor` now constructs `MinCount`/`MaxCount` hash
  aggregators, serializes and merges their pair state across spill rounds,
  routes physical partial aggregation, and computes matching local index/table
  partials. `tidb-exec` lowers the new TiKV aggregate kinds to Go's tipb enum
  values (`MinCount = 3022`, `MaxCount = 3023`), with blacklist coverage. The
  focused executor and cop-scan regressions pass; row-based final mode,
  DISTINCT/window sliding state, aggregate protobuf adapter harness, and SQL
  integration remain explicit boundaries in `receipts/expression_aggregation_audit.md`.
- 2026-09-05 (`pkg/executor/aggfuncs` max/min-count sliding state): ported the
  Go `minMaxCountDeque` tie-index contract and
  `partialResult4MaxMinCountSliding` lifecycle into `tidb-exec`. Equal extreme
  values retain all source indices, incoming rows are enqueued before the
  outgoing boundary is expired, NULL-only frames return the count-shaped zero,
  and mixed typed comparison domains remain errors. Focused sliding/reset
  regressions pass; window operator/frame dispatch and SQL integration remain
  explicit boundaries in `receipts/expression_aggregation_audit.md`.
- 2026-09-05 (`pkg/executor/sortexec` repeated TopN worker spill rounds):
  aligned Rust with Go's post-spill fetch loop. Workers now drain their
  bounded heaps once per shared spill generation while input continues, then
  write final runs at EOF; all runs feed the existing merge. The focused
  fail-before/pass-after regression and Ready checks are recorded in
  `receipts/executor_root_distsql_indexjoin.md`.
- 2026-09-05 (`pkg/expression` UNCOMPRESS output bound): aligned Rust's
  `tidb-expr::builtin_ext::crypto::inflate` with Go's `limitedBuffer` security
  contract. Zlib output is now bounded while streaming at the framed
  four-byte declared length; over-limit output produces the distinct
  `ZlibZBuf`/1258 warning, while malformed streams retain 1259. A focused
  inflater regression and the two source-derived overlong-payload tests pass.
  The Go statement-memory tracker/`LogOnExceed` accounting and vectorized
  differential harness remain explicit cross-layer boundaries. The complete
  208-artifact `pkg/expression` inventory and owner/validation evidence are
  recorded in `receipts/b067.md`.
- 2026-09-05 (`pkg/planner/core/operator/physicalop` max/min-count split
  schema): aligned `tidb-planner::final_mode_agg` with Go's
  `BuildFinalModeAggregation` `NeedValue` arm. `MAX_COUNT`/`MIN_COUNT` partial
  extrema columns now preserve the first argument's type, charset, and
  collation instead of reusing the count-shaped aggregate return type. The
  formerly ignored Go-derived schema regression is active and passes; the
  aggregate evaluator pair-state and tipb conversion remain explicit
  cross-crate boundaries in `receipts/b091.md`.
- 2026-09-05 (`pkg/planner/core` full-text admission predicates): restored the
  dependency-closed `ftsModifierAllowsNativePushdown` and
  `tableHasPublicFTSIndexOnColumn` contracts in `tidb-planner::fulltext`.
  Native FTS pushdown now admits only unmodified natural-language searches and
  requires a public full-text index on each referenced column, with exact Go
  truth-table regressions active. TiFlash/session index-resolution cases remain
  explicit boundaries in `receipts/b087.md`.
- 2026-09-05 (`pkg/planner/core` aggregation identity properties): aligned
  `LogicalAggregation::hash64`/`equals` with Go's nested
  `PossiblePropertiesInfo.Orders` identity while excluding runtime
  `HasTiFlash`; the source-derived property mutation regression is active in
  `receipts/b090.md`.
- 2026-09-05 (`pkg/executor/sortexec` constant by-item evaluation): aligned
  Rust with Go's omission of constant `ByItems` from materialized sort keys.
  Deferred constants are no longer evaluated during sort/TopN key handling;
  positional placeholders preserve key-vector shape while comparison skips
  them. The focused fail-before/pass-after regression and Ready checks are
  recorded in `receipts/executor_root_distsql_indexjoin.md`.
- 2026-09-05 (`pkg/expression/aggregation` max_count/min_count descriptor):
  restored Go's aggregate descriptor semantics for the new `MAX_COUNT` and
  `MIN_COUNT` family. Rust now recognizes both names, infers the count-shaped
  binary BIGINT return/default metadata, includes them in `NeedCount`/
  `NeedValue`, applies the one-stage TiFlash-only pushdown gate, preserves the
  original extreme-value type through `AggFuncDesc::Split`, and returns the
  count default for outer-join null input. The two former source-carrier gap
  tests are active and focused descriptor regressions pass. Aggregate evaluator
  pair state and tipb aggregate conversion remain explicit cross-crate gaps;
  details and the complete 25-artifact Go inventory are recorded in
  `receipts/expression_aggregation_audit.md` and `receipts/b066.md`.
- 2026-09-05 (`pkg/executor/sortexec` output-time TopN spill): restored Go's
  ten-row spill-flag polling while an in-memory TopN emits results. Rust now
  spills only the un-emitted suffix and resumes through the run without
  replaying earlier output; the focused regression's fail-before/pass-after
  evidence and Ready checks are recorded in
  `receipts/executor_root_distsql_indexjoin.md`.
- 2026-09-05 (`pkg/executor/sortexec` by-item validation): aligned Rust's
  `SortExec` and `TopNExec` with Go's `buildKeyColumns` contract. Scalar,
  correlated, and other non-column/non-constant order expressions are now
  rejected before child consumption, and low-level key helpers fail closed;
  focused Sort/TopN regressions and Ready evidence are recorded in
  `receipts/executor_root_distsql_indexjoin.md`.
- 2026-09-05 (`pkg/sessionctx/variable` shared-lock upgrade gate): aligned
  Rust's vendored `LockContext` and `SessionVars` with Go's
  `AllowSharedLockUpgrade` / `EnableSharedLockUpgrade` behavior. The bool is
  GLOBAL|SESSION, defaults OFF, inherits only for newly connected sessions,
  and permits the shared-to-exclusive transaction-buffer transition only when
  explicitly enabled. Focused session and vardef regressions pass; the nested
  vendored client test remains blocked by its pre-existing standalone Tonic
  test-helper mismatch and tempfile lock split. The SQL executor consumer that
  constructs lock contexts for `SELECT ... FOR UPDATE` remains a separate
  unported boundary. Receipt: `receipts/shared_lock_upgrade_gate.md`.
- 2026-09-05 (`pkg/planner/core/operator/logicalop` LATERAL Apply stats):
  removed the Rust-only recursive `unported_stats` refusal for keyed LATERAL
  Apply nodes. Rust now uses the existing full-join NDV estimator for explicit
  `ON` keys, records Go's `EqualCondOutCnt`, and keeps the current Go product
  fallback for correlated lateral inners without explicit keys. Focused
  recursive/operator regressions pass; the complete 43-artifact Go owner
  inventory and Ready evidence are recorded in
  `receipts/logical_apply_lateral_stats.md`.
- 2026-09-05 (`pkg/planner/core` LEFT JOIN LATERAL builder): removed Rust's
  stale rejection of LEFT JOIN beside LATERAL. The Rust plan builder now
  creates a `LeftOuter` Apply, enables Go's outer-join optimization flags, and
  clears NOT NULL from the inner columns in Schema and FullSchema. RIGHT,
  NATURAL, and USING LATERAL errors remain source-aligned. Focused plan-builder
  regressions and the complete 107-artifact Go owner inventory are recorded in
  `receipts/logical_plan_builder_left_lateral.md`.
- 2026-09-04 (`pkg/executor/sortexec` TopN spill threshold): aligned Rust's
  `TopNSpillAction` with Go's package-level tenth-of-quota guard. The focused
  action-level regression fails at exactly 10% under the old aggregation
  one-fifth reuse and passes after the fix; the package inventory and Ready
  evidence are recorded in `receipts/executor_root_distsql_indexjoin.md`.
- 2026-09-04 (`tidb-expr` GetTimeValue): implemented the missing
  `get_time_value` AST/value helper. Raw clock/date sentinels, parser-driver
  string/int/NULL values, function-call markers, unary numeric conversion,
  temporal parsing, and zero-value fallback now follow Go's type-switch
  boundary. The former ignored `helper_test.go::TestGetTimeValue` carrier is
  active; focused pre-fix compilation failed because no carrier existed. The
  shared current-timestamp predicate now governs both CREATE-table and
  ALTER-table `ON UPDATE` validation, with focused DDL regressions; the
  post-fix Ready sweep is recorded in `receipts/b075.md`.
- 2026-09-04 (`pkg/executor/sortexec` parallel spill trigger guard): aligned
  Rust's `ParallelSortSpillAction` with Go's requirement that the triggering
  tracker is over quota before a spill is requested. The focused action-level
  regression fails when the sort threshold is reached but the trigger remains
  below quota, and passes after the guard is restored; the package inventory and
  Ready evidence are recorded in `receipts/executor_root_distsql_indexjoin.md`.
- 2026-09-04 (`tidb-expr` IsValidCurrentTimestampExpr): implemented the
  missing AST-level `is_valid_current_timestamp_expr` helper with Go's
  bare-vs-explicit FSP matching and first-argument-only behavior. The former
  ignored `helper_test.go::TestIsCurrentTimestampExpr` carrier is active.
  Focused and Ready evidence is recorded in `receipts/b075.md`.
- 2026-09-04 (`pkg/executor/sortexec` parallel spill threshold): aligned
  Rust's `ParallelSortSpillAction` with Go's inclusive
  `sortTracker.BytesConsumed() >= quota/10` guard. The focused action-level
  regression fails at the exact boundary before the fix and passes after it;
  the nested package inventory and Ready evidence are recorded in
  `receipts/executor_root_distsql_indexjoin.md`.
- 2026-09-04 (`tidb-expr` truncate-level context wrapper): transcreated
  `exprctx.CtxWithHandleTruncateErrLevel` for the static expression context.
  The wrapper changes only `TruncateAsWarning`/`IgnoreTruncateErr` and the
  `ErrGroupTruncate` level, preserves all other evaluation state, leaves the
  original context untouched, and keeps the no-op evaluation-context identity.
  The Go-derived three-level regression is active in
  `tests::context_override_values_source`; the live generated-column resolver
  remains a higher-layer boundary because it has no context override hook.
- 2026-09-04 (`tidb-expr` VALUES constructor/runtime): activated the Go
  `TestValues` and shared `TestNewValuesFunc` behavior. `ScalarFunction` now
  carries the immutable VALUES offset, `Columns::current_insert_value` exposes
  the current insert row, and `StmtContext` provides the mutable session
  carrier. Empty rows and NULL slots return NULL, out-of-range offsets preserve
  the source-shaped error, and malformed non-zero-argument nodes are rejected.
  The focused regression and Ready profile are recorded in `receipts/b070.md`;
  concrete per-signature identity/Repertoire remain the documented object-model
  gap in `receipts/b076.md`.
- 2026-09-04 (`tidb-expr` typed IN signatures): completed the generated
  `builtin_other_vec_generated_test.go` temporal, duration, and JSON arms.
  Rust's rewritten `ScalarFunction` now follows Go's first-argument eval-type
  selection, casts each list member into DATETIME/TIMESTAMP, DURATION, or JSON
  (including the JSON list-member ParseToJSONFlag distinction), compares the
  native typed values, and preserves three-valued NULL membership. The former
  ignored regression is active in
  `tests::in_func_decimal_collation_source::generated_in_harness_temporal_duration_json_arms`;
  remaining IN cache/performance differences are not claimed as value gaps.
- 2026-09-04 (`tidb-expr` TIDB_BOUNDED_STALENESS): activated the previously
  ignored Go `TestTiDBBoundedStaleness` carrier. Rust now applies the
  ETDatetime argument casts, rejects invalid-zero endpoints through the
  statement truncation policy, clamps an inclusive SafeTS to the requested
  window, preserves reversed-window NULL, and emits DATETIME(3). The new
  `Columns::bounded_staleness_safe_time` seam represents Go's statement-cached
  KV SafeTS/failpoint input; scalar and AST/value regressions are active in
  `builtin_time_calendars_source`, with the narrowed storage wiring recorded
  in `receipts/b072.md`.
- 2026-09-04 (`tidb-expr` GETPARAM): activated the Go
  `builtinGetParamStringSig` path. The `Columns` seam now carries the
  plan-cache parameter lookup, `GETPARAM` casts its selector to ETInt,
  stringifies the selected datum, and preserves Go's out-of-range error
  identity. Scalar and AST/value regressions are active in
  `setvar_getvar_values_getparam_source`; package receipt `receipts/b070.md`
  records the focused and Ready evidence.
- 2026-09-04 (`tidb-expr`/`tidb-session` typed GETVAR time): aligned the
  session variable binder, result metadata, and evaluator with Go's
  `builtinGetTimeVarSig`. A stored MySQL time now mints `getvar_time`, keeps
  its DATETIME result type, and returns the same temporal value (or NULL when
  unset). The Go-derived typed-time regression is active in
  `setvar_getvar_values_getparam_source`; package receipt `receipts/b070.md`
  records the narrowed inventory.
- 2026-09-04 (`pkg/executor/sortexec` required-row pull boundary): completed
  the nested Go `sortexec` inventory (21 artifacts, 5,919 lines) and aligned
  Rust `SortExec::next` with Go's `req.IsFull()` boundary. A focused regression
  covers the `[1, 5, 3, 10] -> [1, 5, 3, 1]` required-row sequence; the
  package inventory, pre-fix failure, post-fix evidence, and remaining Rust
  owner boundaries are recorded in
  `receipts/executor_root_distsql_indexjoin.md`.
- 2026-09-04 (`tidb-expr` CURRENT_RESOURCE_GROUP row-path parity): wired the
  AST/value evaluator through the same session-state accessor already used by
  the rewritten ScalarFunction path. Added a direct row/AST regression so a
  zero-argument call returns the effective group (or NULL without a session)
  regardless of evaluator entry point. Focused and Ready evidence is recorded
  in `receipts/types_explain_format_audit.md`.
- 2026-09-04 (`tidb-expr` CURRENT_RESOURCE_GROUP): added the missing
  session-context accessor and live scalar evaluator for the effective
  statement resource-group name, with Go-derived value/NULL and
  statement-backed executor regressions. Focused and Ready evidence is
  recorded in `receipts/types_explain_format_audit.md`.
- 2026-09-04 (`tidb-expr` CRC32 session charset): aligned CRC32 with Go's
  byte-oriented `EvalString` path and activated the GBK connection-charset
  rows. The live rewrite now hashes `to_binary`'s raw GBK bytes, including
  values that are not valid UTF-8. Focused and Ready evidence is recorded in
  `receipts/types_explain_format_audit.md`.
- 2026-09-04 (`tidb-expr` MD5/PASSWORD session charset): activated the
  Go-derived GBK constant-conversion rows through a connection-aware resolver.
  Valid GBK strings now exercise the normal `to_binary` boundary before
  hashing, while the unrepresentable `ㅂ123` rows surface the same
  construction/evaluation error as Go. Focused and Ready evidence is recorded
  in `receipts/types_explain_format_audit.md`.
- 2026-09-04 (`tidb-expr` DATE zero-date SQL modes): activated the Go-derived
  zero-date rows through explicit `NO_ZERO_DATE` and `NO_ZERO_IN_DATE`
  statement contexts. The live `DATE()` cast now has regression coverage for
  preserving zero components when the modes are disabled, returning SQL NULL
  with Go's 1292 warning when each mode rejects its corresponding shape.
  Focused and Ready evidence is recorded in
  `receipts/types_explain_format_audit.md`.
- 2026-09-04 (`tidb-expr` TO_BASE64 session charset): activated the Go-derived
  GBK connection-charset rows. A connection-aware resolver now stamps string
  literals with the session charset, allowing the ordinary `to_binary` boundary
  to encode GBK bytes before base64; focused evidence is recorded in
  `receipts/types_explain_format_audit.md`.
- 2026-09-04 (`tidb-expr` BINARY-source string casts): aligned the
  `BuildCastFunction`/`HandleBinaryLiteral` boundary for `CAST(... AS CHAR)`.
  Rust now decodes a BINARY-charset source through the target charset, keeps
  the valid prefix in non-strict mode, and publishes Go's 3854 conversion
  warning for invalid bytes. The focused valid/invalid BinaryLiteral
  regression and Ready profile are recorded in
  `receipts/types_explain_format_audit.md`.
- 2026-09-04 (`tidb-expr` cast-wrapper metadata tables): activated the complete
  Go-derived `TestCastConstAsDecimalFieldType` (51 rows) and
  `TestCastAsCharFieldType` (40 rows). The normal Rust wrapper and cast builder
  now have executable coverage for source widths, decimal precision/scale
  caps, temporal FSP, JSON widening, and blob/string families. Focused results
  are recorded in `receipts/types_explain_format_audit.md`.
- 2026-09-04 (`tidb-expr` UNION decimal casts): aligned Go's
  `BuildCastFunction4Union` source-specific signatures. Rust now selects
  REAL/int/string/DECIMAL-to-DECIMAL UNION arms from the source eval type,
  clamps negative values as Go does before applying the merged decimal
  precision/scale, and preserves positive decimal values. Focused regressions
  and the Ready profile are recorded in
  `receipts/types_explain_format_audit.md`.
- 2026-09-04 (D5 follow-up, `tidb-expr` decimal wrapper casts): carried Go's
  unspecified decimal scale through the internal cast dispatch instead of
  rounding it to scale 0, and restored `WrapWithCastAsDecimal`'s strict-
  constant precision/scale refinement. The REAL `123.555` regression and the
  Ready profile are recorded in `receipts/types_explain_format_audit.md`.
- 2026-09-04 (D5 caller integration, `tidb-expr` temporal comparisons):
  replaced the live `time_compare_ordering` UTC/permissive constants with
  `Columns::date_modes()` and `Columns::time_zone()`, preserving strict
  invalid-date rejection, `ALLOW_INVALID_DATES` acceptance, and 1292 warning
  publication. Focused expression regressions and the Ready profile are
  recorded in `receipts/types_explain_format_audit.md`.
- 2026-09-04 (D5, `pkg/types` comparison context seam): added
  `Datum::compare_with_context`, which forwards statement zero-in-date and
  invalid-date flags plus an explicit session timezone through temporal string
  comparisons while preserving Go's ordering/error pair. Focused regressions
  cover `ALLOW_INVALID_DATES` and timezone-offset ordering. The context-free
  wrapper remains compatible; live expression warning/context wiring is still
  open and is recorded in the receipt.
- 2026-09-04 (M6, `pkg/types` decimal add/sub fixed-word boundary): aligned
  Rust `Decimal::add_mysql` and opposite-sign `sub_mysql` with Go's
  leading-word carry precheck, including the nine-word capacity limit. The
  81-digit `999999999… + 1` regression now returns Go's overflow/max-value
  pair while ordinary smaller carries remain valid. Focused and Ready
  evidence are recorded in `receipts/types_explain_format_audit.md`.
- 2026-09-04 (D4, `pkg/types` comparison result shape): added
  `Datum::compare_with_error`, retaining Go's ordering beside temporal and
  duration parse errors and beside numeric/decimal string truncation events.
  The strict `compare` wrapper remains available for callers that require an
  error-only result. Focused bidirectional temporal and numeric regressions,
  plus the datatype Ready profile, are recorded in
  `receipts/types_explain_format_audit.md`; statement-context warning policy
  remains the separate D5 boundary.
- 2026-09-04 (F3, `pkg/types` empty-collation predicate): reconciled the
  field-type audit with the spelling-authoritative implementation and added a
  legacy JSON regression proving an empty `Collate` remains a character string
  with restored data. Ready evidence is recorded in
  `receipts/types_explain_format_audit.md`.
- 2026-09-04 (F4, `pkg/types` field-type formatter): `FieldType::source_string`
  now uses the strict integer display-width default, matching Go's
  server-initialized `TiDBStrictIntegerDisplayWidth`. The focused BIGINT/BINARY
  regression and serialized Ready profile are recorded in
  `receipts/types_explain_format_audit.md`.
- 2026-09-04 (M10, `pkg/types` decimal parser error identity): preserved
  Go's distinct `ErrTruncatedWrongVal("DECIMAL", ...)` outcome for empty or
  digit-less fixed-word decimal input, while keeping exponent overflow as
  `BadNumber`. The focused regression and serialized Ready profile are
  recorded in `receipts/types_explain_format_audit.md`.
- 2026-09-04 (M9, `pkg/types` decimal multiplication overflow sign): aligned
  Rust bounded `Decimal::mul_mysql` with Go's early `to.negative` assignment,
  preserving `-0` for opposite-signed overflow products on every overflow
  exit. The focused regression and complete Ready profile are recorded in
  `receipts/types_explain_format_audit.md`.
- 2026-09-04 (D9, `pkg/types` float-prefix NUL warning subject): added the
  shared trim-and-NUL diagnostic helper and applied it to every Rust DOUBLE
  warning site. Focused regressions and Ready evidence are in
  `receipts/types_float_warning_nul.md`.
- 2026-09-04 (M7, `pkg/types` decimal `FromBin` corrupt payload): added a
  structured Rust failure outcome carrying Go's zero receiver and legal
  payload size alongside `BadNumber`, while retaining the strict legacy
  wrapper. Focused regression and Ready evidence are in
  `receipts/types_decimal_from_bin_failure.md`.
- 2026-09-04 (M5, `pkg/types` decimal `ModeCeiling`): aligned the Rust
  value-layer rounder with Go's intentionally split source behavior. Aligned
  cuts inspect all discarded words; non-word-aligned cuts inspect only the
  first discarded digit. Focused decimal regression and Ready evidence are in
  `receipts/types_decimal_round_ceiling.md`.
- 2026-09-04 (T11 closure, expression `STR_TO_DATE` punctuation): recorded
  the already-pushed Unicode punctuation alignment in the main temporal audit.
  Both datatype and expression parsers use `is_go_punctuation`; the focused
  `2013¿5`/`2013+5` regression and owner profile remain in
  `receipts/expression_collation_audit.md`.
- 2026-09-04 (T9, `pkg/types` `StrToDate` zero-in-date validation): aligned
  `Time.StrToDate` with Go's `FlagIgnoreZeroInDate` by adding an explicit
  parser argument and forwarding it to `Time::validate`. Default vector and
  benchmark callers remain permissive; strict callers can now reject zero
  month/day values. The focused regression and owner Ready evidence are in
  `receipts/types_str_to_date_zero_in_date.md`.
- 2026-09-04 (T8, `pkg/types` numeric zero-date conversion): aligned Go
  `ParseTimeFromNum(0)` with `FlagIgnoreZeroDateErr`. Rust now returns a
  `TimeError::ZeroDate` beside the zero fallback when strict flags clear the
  bit, while default expression/internal callers retain zero. Datum temporal
  conversion threads the statement flag. Focused parser and conversion tests,
  owner Ready validation, and the remaining known profile blockers are in
  `receipts/types_parse_time_from_num_zero.md`.
- 2026-09-04 (`pkg/types` TIMESTAMP DST-gap parsing, finding T7): aligned
  Go's `parseTime`/`adjustTimestampErrForDST` boundary. `ParsedTime` now
  preserves the adjusted value and marker for a nonexistent local wall clock;
  packed numeric TIMESTAMP parsing follows the same rule. Expression casts
  emit 8179 while returning the adjusted value, and write conversion carries a
  dedicated event so lenient writes store the value with the warning while
  strict writes return 8179. Focused datatype, expression, and executor
  regressions plus owner Ready results are recorded in
  `receipts/types_timestamp_dst_gap.md`.
- 2026-09-04: aligned Rust `tidb-expr` `TIMESTAMP` string parsing with Go's
  compact `ParseTime` forms. Delimiter-free 6/8/11/12/14-digit strings now
  use the source field-width table, full 14-digit datetime fractions round
  half-up to six digits, and date-only fractional suffixes use Go's hour rule.
  Receipt:
  `receipts/expression_collation_audit.md`.
- 2026-09-04: aligned the Rust `tidb-expr` `MAKETIME` evaluator with Go's
  unsigned-hour guard. `Datum::UInt` now carries the source `UnsignedFlag`
  signal through the value-tier seam, so wrapped negative hours (including
  `CAST(-1 AS UNSIGNED)`) clamp to `838:59:59`; the floating-second MaxFsp
  rows are active in `go_time_values`. Receipt:
  `receipts/expression_collation_audit.md`.
- 2026-09-04: aligned Rust `tidb-expr` `TIMESTAMP` numeric and DECIMAL
  arguments with Go's `isFloat` signature route. Integer, real, float32, and
  DECIMAL datums now use the shared `parse_time(..., is_float = true)` path,
  covering packed numeric fractions and Issue #25093 zero-date decimal rows;
  string/temporal datums retain their compact string parser. Focused source
  regressions, inventory, and Ready evidence are recorded in
  `receipts/expression_collation_audit.md`.
- 2026-09-04: aligned Rust `tidb-expr` `TIMESTAMPADD` zero-date handling with
  Go's pre-arithmetic `Time.GoTime` conversion. A zero or month/day-zero third
  argument now returns NULL before day/month arithmetic, while valid packed
  integer dates continue through the existing add-unit logic. Focused source
  regression, inventory, and Ready evidence are recorded in
  `receipts/expression_collation_audit.md`.
- 2026-09-04: aligned Rust `tidb-expr` `UNIX_TIMESTAMP` with Go's
  source-kind-sensitive DATETIME cast. Packed 12/14-digit integer, real, and
  DECIMAL arguments now use the shared float-string parser (including six-digit
  half-up rounding), while strings retain their ordinary parser. All-zero
  dates remain NULL and partial zero-in-date values return numeric zero as in
  Go. Focused source regressions, the complete package inventory, and Ready
  evidence are recorded in `receipts/expression_collation_audit.md`.
- 2026-09-04: aligned Rust `tidb-expr` `PERIOD_ADD`/`PERIOD_DIFF` invalid
  period diagnostics with Go's `ErrWrongArguments` function-specific text.
  The source-derived invalid-period carrier now asserts the exact
  `EvalError::IncorrectArguments` values; valid arithmetic and NULL ordering
  remain unchanged. Ready evidence is recorded in
  `receipts/expression_collation_audit.md`.
- 2026-09-04 (batch 22, `pkg/ddl` MV remaining pure surface): implemented
  Go master `94a9cbedab`'s `MViewExecutionSessionVarsFromJob` (tidb-session,
  per-field fallback to the captured defaults over the job's system-variable
  envelope), `BuildMViewImportIntoOptions` and `buildMLogPurgeMeta`
  (tidb-executor::ddl::mview_helpers — the ALTER-path purge wording, the
  option order/escaping, and batch 9's schedule-expression validation). The
  pure surface of `materialized_view.go` is now fully ported; the data-build
  execution and the session-variable image remain the recorded seams.
  Failure sets: exec 7 (base), session byte-identical to base on the final
  full run. Receipt: `receipts/ddl_mview_pure_surface.md`.

`db08e71dbd3` (`rust: align pkg/ddl mview remaining pure surface with Go master`).
- 2026-09-04: aligned the Rust `tidb-expr` `ADDTIME`/`SUBTIME` datetime
  parser and DATE-string FSP with Go `ParseTimeWithString`. Packed
  `YYYYMMDDHHMMSS`/`YYMMDDHHMMSS` text produced by the integer-cast signature
  now reaches the shared numeric datetime parser, and DATE+STRING uses
  `getFsp4TimeAddSub` (so an all-zero fraction stays second precision). The
  duration-operand and packed-integer carriers are active; inventory and
  fail-before/pass-after evidence are recorded in
  `receipts/expression_collation_audit.md`.

- 2026-09-04: aligned the Rust `tidb-expr` `ADDTIME`/`SUBTIME` typed
  DATETIME path with Go's row/vector split. Constant-folded (row-path) calls
  now retain the parsed second operand's fractional precision, while the
  vectorized DATETIME+TIME arm keeps Go's `Fsp=-1` behavior; the existing
  issue-56861 carrier now pins DATE/DATETIME FSP and malformed-string NULL
  rows. Complete package inventory and fail-before/pass-after evidence are
  recorded in `receipts/expression_collation_audit.md`.

- 2026-09-04: aligned the Rust `tidb-expr` `FORMAT` precision coercion with
  Go's `evalNumDecArgsForFormat` and activated the existing
  `WEIGHT_STRING AS BINARY(n)` warning carrier. Malformed string/byte precision
  now uses the shared warning-aware integer conversion, preserving the parsed
  value and emitting Go's 1292 event; the weight-string test now supplies a
  warning-capable statement context and pins all three cut rows. Complete
  package inventory and fail-before/pass-after evidence are recorded in
  `receipts/expression_collation_audit.md`.

- 2026-09-04: aligned the Rust `tidb-expr` `FROM_UNIXTIME` real-input path
  with Go `pkg/expression/builtin_time.go` at `origin/master`
  `fc7788ff...`. Real and float32 datum arguments now pass through the shared
  Go-shortest `Decimal::from_f64` conversion before FSP-6 half-up rounding;
  the focused boundary regression, complete package inventory, and existing
  source vectors are recorded in `receipts/expression_collation_audit.md`.

- 2026-09-04: aligned the Rust `tidb-expr` `FROM_BASE64` evaluator with Go
  `pkg/expression/builtin_string.go` at `origin/master` `fc7788ff...`. The
  context-aware entry now estimates decoded size from the original input
  length (before whitespace stripping), applies Go's `int` overflow bound and
  `maxAllowedPacket` warning/error path, and keeps the value-only helper for
  callers without statement context. The complete `pkg/expression` inventory,
  fail-before/pass-after regression, and stale zero-date/%x test activation
  are recorded in `receipts/expression_collation_audit.md`; the package batch
  is validated before commit.

- 2026-09-04: aligned the Rust `tidb-datatype` signed subtraction owner with
  Go `pkg/types/overflow.go` at `origin/master` `fc7788ff...`. Go's
  `SubInt64` negates `MinInt64` before its guard, so positive-minus-
  `MinInt64` inputs pass the check and wrap in the final subtraction; Rust's
  `checked_sub` previously rejected them as a Rust-only overflow. The owner
  now mirrors the guard with explicit wrapping arithmetic, and a focused
  regression covers both positive boundary shapes. Complete `pkg/types`
  inventory, fail-before/pass-after evidence, and Ready validation are
  recorded in `receipts/types_explain_format_audit.md`.

- 2026-09-04 (batch 19, `pkg/ddl` MV job-envelope metadata): implemented
  Go master `94a9cbedab`'s `initMaterializedViewReorgMetaFromVariables` +
  `NewDDLReorgMeta` (the empty warning maps, recorded SQL mode/zone/resource
  group, current metadata version, `use_new_collate`, and the default
  reorg worker count / batch size / max write speed; `tidb-model` gains the
  matching `DDLReorgMeta::new`) and
  `AddMViewExecutionSessionVarsToJob`'s twelve MV-execution session
  variables with Go's exact formatting. The view job now carries the
  reorg metadata and the maintenance variable snapshot the build and later
  refreshes run under; the values are the default session's (the statement
  context carries no session-variable image — the standing reduction). The
  submitted-spec test pins the reorg fields and thirteen job variables.
  Failure sets unchanged (exec's one new failure is the placement_delivery
  POST fixture flake, verified failing on the stashed base in the same
  environment and recovering across runs). Receipt:
  `receipts/ddl_mview_job_envelope.md`.

d8d033a882 (rust: align pkg/ddl mview job envelope metadata with Go master)

- 2026-09-04 (batch 18, `pkg/ddl` MV purge-schedule derivation): implemented
  Go master `94a9cbedab`'s `deriveCreateMaterializedViewLogNextUnixSeconds`
  as `MlogPurgeDerived::derive` — Go's near-now decision tree (START WITH
  beyond ten seconds wins, otherwise NEXT; NULL evaluations degrade to the
  INSERT IGNORE shape with the Go log message) evaluated through the
  driver's FROM-less `SELECT NOW(6)` / `SELECT <expr>` under the log's
  recorded SQL mode and schedule zone, closing the batch-15 session-eval
  seam in the pure planner (the worker step now derives internally). The
  derivation exposed a pre-existing planner panic — FROM-less projections
  with non-foldable expressions hit
  `eliminate_physical_projection`'s child-schema expect because the
  physical `TableDual` copied the logical dual's `None` schema;
  `find_best_task_4_logical_table_dual` now materialises the empty default,
  matching Go's non-nil schema invariant. One regression derives a
  scheduled log's deadline into the purge row; the failure set is the
  exact base set. Receipt: `receipts/ddl_mview_purge_derivation.md`.

- 2026-09-04 (`CAST` target-type production family, audit item 5): aligned
  `rewriter::result_type::cast_target` with Go's `parser.y` `CastType` rule —
  where each target's `FieldType` is born. `BINARY(N)` now switches to
  `TypeString` (plain `BINARY` stays `VarString`), `CAST AS YEAR` produces
  `TypeYear` instead of `LongLong` (eval type stays `ETInt`; the eval arm is
  unchanged), `FLOAT` keeps `TypeFloat` with the `{12, -1}` defaults instead
  of folding onto `Double`, `DOUBLE` gains the `{22, -1}` defaults, and
  `SIGNED`/`UNSIGNED`/`DECIMAL`/`DOUBLE`/`FLOAT`/`JSON` gain the missing
  binary charset/`BinaryFlag` (JSON also the utf8mb4 charset/collation);
  `VECTOR` sets only charset/collation — the one target without
  `BinaryFlag` — matching the existing semantics pin. The pre-existing
  `CHAR` charset boundary (restore-only) stands. One fail-before regression
  pins every row of the CastType table; full tidb-expr sweep green except
  the documented network flake, and the cast/sysvar/restore consumer tests
  in exec/planner pass. Receipt: `receipts/cast_target_type_family.md`.

- 2026-09-04 (`pkg/util/chunk` clamped decimal read-back, A-1 follow-up):
  closed the observable gap `c59b2bd60e`'s lossy bridge left — its
  `FromString`-tail `resultFrac = digitsFrac` leaked hidden fraction words
  past a value's visible scale on chunk read-backs of clamped cells. The
  lossy fallback now pins `resultFrac = min(visible scale, kept fraction)`
  (the exact path's and Go producers' convention; the client-visible decimal
  digit count is the `resultFrac` count on both engines), widening
  `MyDecimal::set_result_frac` from test-only to `pub(crate)` with its
  invariant intact. Fail-before regression on the hidden-word product
  (resultFrac 72 vs visible 71), integer-overflow byte-equality oracles
  against Go's `MyDecimal.FromString`, the Go-master all-zero sign
  normalization pin (`mydecimal.go:531-543`), and the chunk read-back
  integration pin; the two-crate sweep's 40-name failure set is identical to
  the base control run. Receipt: `receipts/chunk_a1_readback_parity.md`.
- 2026-09-04: aligned `tidb-error::TerrorError::registered_std` with Go
  `pkg/util/dbterror`'s complete standard-message lookup (10 Go artifacts,
  1,118 Go lines; 42 Rust artifacts, 34,598 Rust lines). Overlapping error
  codes now prefer the TiDB/`errno` catalogue before the parser/MySQL
  fallback; focused 3143/1243/1820 regressions and the serialized owner
  profile are recorded in `receipts/dbterror_registered_std_precedence.md`.

- 2026-09-04: aligned the Rust `pkg/kv` write-conflict error boundary with Go
  master's complete `pkg/kv` inventory (30 artifacts, 5,319 Go lines) and the
  `tidb-executor` owner (290 artifacts, 195,724 Rust lines). The live 9007
  `TxnErrorKind::WriteConflict` path now appends Go's compatibility marker
  `[try again later]`; the focused code/state/message regression and Ready
  results are recorded in `receipts/kv_write_conflict_retry_marker.md`.

- 2026-09-04 (T14, `pkg/types` DST-adjusted convert metadata): closed the
  type-metadata reversion Go's `Time.Convert` DST branch performs —
  `Time{FromGoTime(tAdj)}` zeroes the type and fsp, so the adjusted value
  reverts to DATETIME/fsp 0 (time.go:467). `convert_kind` now applies that
  reversion after the adjusted-instant substitution; the fail-before
  regression pins kind DateTime + fsp 0 + the adjusted rendering over the
  2018 US DST gap. Full tidb-datatype sweep 474/474. Receipt:
  `receipts/types_time_dst_metadata.md`; audit entry T14 marked FIXED.

- 2026-09-04 (`ROW_COUNT()`/`LAST_INSERT_ID()` evaluation): the two info
  functions were registered and typed but had no evaluation arms — a built
  node answered `Unsupported` where Go answers numbers. Added the arms
  (builtin_info.go:913-923 ROW_COUNT → PrevAffectedRows; :482-489
  LAST_INSERT_ID → PrevLastInsertID through the UNSIGNED result; :508-521
  LAST_INSERT_ID(expr) records via SetLastInsertID and returns the value),
  with the vectorized source's `UInt(7)` pin kept green and a capturing
  context regression proving the record side effect. Fail-before verified.
  Receipt: `receipts/info_row_count_last_insert_id.md`.

- 2026-09-04 (`FORMAT_BYTES()`/`FORMAT_NANO_TIME()` evaluation): the two
  performance-schema formatters were registered and result-typed but had no
  evaluation arms. Added the arms coercing the declared-ETReal argument
  through the numeric ladder and rendering with Go's
  `GetFormatBytes`/`GetFormatNanoTime` (`util.go:1804-1879`): IEC byte units
  and the ns→d time ladder, 0 decimals at divisor 1, 2 decimals above, and
  Go's scientific form (`1.02e+08`) at 100000+. Fail-before (the arms did
  not exist) with Go's own TestFormatBytes/TestFormatNanoTime vectors plus
  the negative and scientific rows. Receipt:
  `receipts/cast_hybrid_push.md` sibling — the vectors live in
  `simple_expr::tests::format_bytes_and_nano_time_follow_the_go_unit_tables`.

- 2026-09-04 (deferred clock functions under plan reuse — wrong-value): the
  constant folder froze NOW()/CURDATE()/UTC_DATE and the rest of Go's
  `IsDeferredFunctions` clock set (function_traits.go:159-171) at build
  time, so a prepared statement or cached plan served the first execution's
  clock on every reuse. The fold now marks those constants with
  `deferred_expr` (Go `expression_rewriter.go:3016-3029`) and evaluation
  re-runs the function against the statement clock per execution; the
  vectorized constant repeat follows the same rule. Fail-before: folding
  under clock 1000 then evaluating under clock 5000 served the stale
  00:16:40 instead of 01:23:20. Receipt:
  `receipts/deferred_clock_functions.md`.

- 2026-09-04 (charset-transcode classification parity): entry-by-entry
  verification of Go's `convertActionMap` (pkg/expression/
  builtin_convert_charset.go:300-330) against `convert_charset::func_prop` —
  27 None + 17 BinAware + 37 Auto names and the five-name
  `isLegacyCharset` set all match, closing the wrong-bytes tier for the
  implicit charset transcode. The deliberate non-modeling of the
  builder-level `from_binary` arm and the `CHAR(n) BINARY` flag residue are
  recorded. Receipt: `receipts/convert_funcs_prop_parity.md`.

- 2026-09-04 (registry arity parity verification): systematic
  accept/reject-arity check across all 283 Go `baseFunctionClass`
  declarations (pkg/expression, every `ast.<Const>` resolved through
  `pkg/parser/ast/functions.go`) against the Rust registry's 276 entries —
  zero divergences. The seven naive name-diff flags were each traced to a
  dedicated mechanism (CAST build path, typed getvar signatures, the INSERT
  VALUES path, the builtin_ext JSON dispatch). Verification receipt:
  `receipts/registry_arity_parity.md`.

- 2026-09-04 (hybrid-type cast push, `builtin_cast.go:2898`): ported
  `TryPushCastIntoControlFunctionForHybridType` — a numeric-target cast over
  IF/CASE/ELT pushes INTO the branches when one is a hybrid type (Enum/Set,
  Bit excluded), rebuilding the control function over cast-wrapped branches
  with the re-inferred numeric ret type. `IF(1, e, 'a')` cast to SIGNED over
  an enum column now answers the ordinal 2 instead of the enum name flowing
  through the string result. The wrap is Go's
  `WrapWithCastAsInt`/`WrapWithCastAsReal` shape; the enum
  `ENUM_SET_AS_INT` stamp is unnecessary because the built cast node
  evaluates the ordinal via `cast_arg_as_int`'s hybrid short-circuit.
  Fail-before shape+value regression over an enum chunk row; full
  tidb-expr sweep green except the documented network flake. Receipt:
  `receipts/cast_hybrid_push.md`.

- 2026-09-04 (parser coded diagnostics): aligned two grammar refusals with
  Go's coded terrors. `CAST(1 AS FLOAT(54))` now raises
  `[expression:1426]Too-big precision 54 specified for 'CAST'. Maximum is
  53.` (Go `FLOAT FloatOpt`, parser.y:10078), and `ALTER TABLE t ALGORITHM =
  FOO` raises `[parser:1800]Unknown ALGORITHM 'FOO'`
  (`terror.ClassParser.NewStd(mysql.ErrUnknownAlterAlgorithm)`). Both were
  uncoded generic refusals; the fail-before rows live in
  `parser_root_source::test_error_msg`'s compatibility-message table.
  Receipt: `receipts/cast_target_type_family.md` (FLOAT diagnostic
  follow-up).

- 2026-09-04 (`CAST ... CHARSET` boundary closed): the `CHAR` cast charset
  clause is now modeled end to end. Parse time resolves and refuses unknown
  charsets like Go's `GetDefaultCollation` diagnostic (parser.y:9971);
  `cast_target` stamps the charset name plus its default collation (the
  `BINARY` suffix adds `BinaryFlag`); and evaluation follows the ret charset
  — `ProduceStrWithSpecifiedTp`'s `chs == CharsetBin` branch byte-truncates
  while `padZeroForBinaryType` never pads a `TypeVarString`. Three
  fail-before regressions (parse refusal, target metadata, byte-vs-char
  truncation). Receipt: `receipts/cast_target_type_family.md` (CHAR charset
  follow-up section).

- 2026-09-04 (planner identifier keys + NUL-padding gate): extended finding
  #196 to the planner — `SchemaTableKey`/`TableAliasKey` and the
  view-recursion, hint-table, alias-collision, and `USING`-column key sites
  (`schema_table_key.rs`, `plan_builder/from.rs`) now lowercase with the Go
  simple mapping, matching `ast.CIStr.L` (`ast/model.go:302`,
  `schema_table_key.go` `.L` keys); tidb-planner gains the `tidb-mysql`
  dependency. The CHAR-width landing then exposed the evaluator's
  binary-padding seam: it padded any binary-charset target to its flen where
  Go's `padZeroForBinaryType` gates on the FIXED `TypeString` code only
  (`builtin_cast.go:2251`) — `CAST(1 AS BINARY)` in Go is ret flen 20 with
  the one-byte value `1`. `cast_type_of` now reconstructs the padding length
  only for `FieldTypeCode::String` targets. Two fail-before regressions
  (Greek-sigma schema key; the 20-byte padded `CAST(1 AS BINARY)`), and the
  pre-existing planner pin returned to green. Receipts:
  `identifier_simple_case_mapping.md` (planner keys section),
  `cast_char_width_estimation.md` (padding-gate follow-up).

- 2026-09-04 (identifier lowercasing, plan finding #196 parser surface):
  replaced the five identifier-facing `str::to_lowercase` sites (digester
  literal `digest.rs`, user/role `@host`, `ADMIN ALTER DDL JOBS` option
  name) with `tidb_mysql::to_lowercase` — the `strings.ToLower` simple-case
  port CiString already uses. Go lowercases all of these with
  `strings.ToLower` (digester.go:227, parser.y:12219/12223/12257/12261/
  12577); the full mapping turned a trailing capital sigma into final sigma
  (U+03C2), so non-ASCII identifier digests diverged from Go byte-for-byte.
  A fail-before regression pins `SELECT * FROM ΟΔΟΣ` digesting to the
  simple-mapped `οδοσ`. Lexer + parser sweep 925/925. Receipt:
  `receipts/identifier_simple_case_mapping.md`.

- 2026-09-04 (`CAST(... AS CHAR)` width production): ported Go's
  `adjustRetFtForCastString` table (`builtin_cast.go`) into
  `rewriter::result_type::adjust_ret_ft_for_cast_string`, applied by
  `build_cast_function` for `VarString` CHAR targets with unspecified flen:
  per-type int widths (3/4, 5/6, 8/9, 10/11, 20, Year 4, Bit source flen),
  Real 87/370, `decimalPrecisionToLength`, temporal 10/19 `+1+decimal`,
  JSON → 4294967295 with the code widening to LongBlob, and the string
  family inherit/blob sizes. Fixed `TypeString` targets stay untouched
  (Go's early return), so `BINARY(N)` is unaffected. The CHAR
  charset-coercibility seam (`isExplicitCharset` → coercibility/repertoire)
  remains the recorded boundary. One fail-before regression (pre-fix the
  unsized target kept flen -1); full tidb-expr sweep green except the
  documented network flake. Receipt:
  `receipts/cast_char_width_estimation.md`.

- 2026-09-04 (`pkg/parser` `@@instance.` scanner prefix, finding #12):
  removed the Rust-only `instance.` entry from `scan_at`'s scope-prefix list —
  Go's `startWithAt` (lexer.go:671) matches exactly `global.`/`session.`/
  `local.`, and its identifier run folds `@@instance.` into the same single
  variable token with the grammar splitting the scope from the literal
  (parser.y SystemVariable/VariableAssignment actions). The old prefix leak
  accepted `SET @@instance."x" = 1` as a variable named `x` (Go: syntax
  error) and rejected bare `@@instance.` (Go: parses as an instance-scoped
  variable with an empty name); both shapes are now pinned fail-before at
  the token and statement levels, and the plain `@@instance.x` span is
  byte-identical before and after. Lexer + parser sweep: 925 run, 925
  passed. Receipt: `receipts/parser_instance_scope_prefix.md`; finding #12
  closed in `rust/docs/parser-lexer-divergence.md`.

- 2026-09-04 (`pkg/types` DATETIME maximum precision, finding T13):
  aligned `tidb-datatype::Time::validate` with Go's complete
  `checkDateRange` ceiling. The exact `9999-12-31 23:59:59.999999` value is
  accepted, while a synthetic packed microsecond above `999999` at that exact
  second is rejected; earlier dates retain Go's lexicographic acceptance. The
  complete 60-artifact `pkg/types` / 104-artifact `tidb-datatype` inventory,
  focused regression, serialized owner profile, and Ready results are in
  `receipts/types_time_validate_max_datetime.md`.

- 2026-09-04 (`pkg/types` raw temporal packing, finding T15): aligned
  `Time::to_packed_uint` with Go's infallible raw bit-pack. The Rust method no
  longer routes stored fields through the strict `PackedTime::from_parts`
  validator; a synthetic invalid clock/fraction now produces the same packed
  bits as Go. The focused regression, complete temporal inventory, and Ready
  results are recorded in `receipts/types_time_packed_raw.md`.

- 2026-09-04 (`pkg/types` duration rounding, finding T6): aligned
  `round_duration_fsp` with Go's `Duration.RoundFrac` delegation to
  `time.Time.Round`. Rust now rounds nearest values with exact negative ties
  toward positive infinity (toward zero), while values past the midpoint still
  round away from zero. The focused exact-tie/past-tie regression, complete
  temporal inventory, and Ready results are recorded in
  `receipts/types_duration_round_ties.md`.

- 2026-09-04 (`pkg/types`/`pkg/expression` `STR_TO_DATE` exhaustion, finding
  T10): aligned both Rust format parsers with Go's `ctx[token] = 0` state when
  input ends before the remaining format. `%p`/`%H` meridiem fixes now see
  token presence, while empty-fraction and skip-token behavior stays intact.
  Focused datatype and live-expression regressions, complete owner profiles,
  and the known external JSON-schema fixture failure are recorded in
  `receipts/types_str_to_date_exhaustion.md`.

- 2026-09-04 (`pkg/types` float-string datetime, finding T12): aligned the
  numeric branch of `parse_datetime_core` with Go's
  `ParseTimeFromFloatString` context flags. `allow_zero_in_date` and
  `allow_invalid_date` now flow into `parse_time_from_num`, so
  `ALLOW_INVALID_DATES` preserves `2020-02-31` while strict mode rejects it.
  The focused regression, complete temporal inventory, and Ready results are
  recorded in `receipts/types_float_string_invalid_date.md`.

- 2026-09-04 (`pkg/expression` decimal `DIV`, findings A/B): aligned the Rust
  decimal `IntDiv` value path with Go's post-`DecimalDiv` `ToInt`/`ToUint`
  conversion. `Decimal::div_rem_unbounded` preserves quotients above
  `i64::MAX`; the evaluator now returns a full-range `Datum::UInt` when either
  operand is unsigned, rejects negative/over-wide unsigned quotients, and
  keeps Go's `(-1, 0]` zero exception. The complete recursive expression
  inventory, focused arithmetic regressions, and Ready results are recorded in
  `receipts/expression_intdiv_unsigned_width.md`.

- 2026-09-04: aligned Rust `tidb-datatype` `JSON_MERGE_PRESERVE` with Go's
  adjacent-object grouping from the complete `pkg/types` JSON inventory.
  `merge_binary_nodes` now groups object runs, recursively merges duplicate
  keys, and flattens one array layer; the interrupted-object regression is
  pinned in `receipts/json_merge_preserve.md`.

22e85fc7e6 (rust: align pkg/ddl mview purge-schedule derivation with Go master)

- 2026-09-04 (batch 17, `pkg/ddl` MV view-create worker phase 1): implemented
  Go master `94a9cbedab`'s `onCreateMaterializedView` `StateNone` arm and
  `rollbackCreateMaterializedView` as
  `plan_persisted_materialized_view_create_job_step` over the batch-16
  submitted view job — argument validation cancelling on nil/empty/zero/
  duplicate base IDs, the per-base execution-time re-checks
  (`onCreateMaterializedViewBaseCheck` + log metadata/public state), the
  view `TableInfo` landing PUBLIC through `createTable`, every base's
  `MViewIDs` gaining the view (`updateMaterializedViewBaseInfoOnCreate`),
  the schema-version bump with the create-table event, the
  `mysql.tidb_mview_refresh_info` prewrite row via the new
  `mview_refresh_info_table` storage, and the non-terminal transition to
  `Running`/`StateWriteReorganization`. The `StateWriteReorganization` data
  build (import-into / insert-select at the build read TS) stays the
  recorded seam: the tick refuses retryably and leaves the queued job where
  Go's own `ErrWaitReorgTimeout` tick would. The rollback path drops the
  created view with its auto-ID allocators, clears every base's view
  reference (dropping empty metadata), deletes the refresh row, and ends
  `RollbackDone`. One regression drives submit to phase 1 to the seam to
  rollback; failure sets unchanged (exec 7, executor 165 + 1 pre-existing
  metadef mismatch). Receipt: `receipts/ddl_mview_create_worker.md`.
- 2026-09-04: aligned the Rust `tidb-chunk` datum-to-decimal boundary with
  Go master's complete `pkg/util/chunk` inventory (29 source/build/test
  artifacts, 11,261 Go lines). `Chunk::append_datum`, `MutRow::from_datums`,
  `SetValue`, and `SetDatum` now preserve Go's fixed `MyDecimal` prefix/
  truncation result for value-layer decimals wider than nine words instead of
  panicking during a Rust-only conversion. A focused regression covers the
  ten-word fractional case; details and the full inventory are in
  `receipts/chunk_a1_datum.md`.

- 2026-09-04 (batch 16, `pkg/ddl` MV view-create submission): implemented
  Go master `94a9cbedab`'s `CreateMaterializedView` submission body — the
  restricted-SQL column-type derivation (`SELECT * FROM (<canonical>) AS
  tidb_mv_query LIMIT 0` over a driver catalog bridge registering the single
  admitted base table), Go's column-count refusal, the view `TableInfo`
  through the ordinary create-table build with the
  `mviewQueryAnalysis`-derived PRIMARY KEY/UNIQUE group constraint and each
  column re-stamped with the flag-stripped planner result type, the full
  `MaterializedViewInfo` metadata, and the typed
  `CreateMaterializedViewArgs` job through the shared submit preflight.
  Both MV statements now route through `prepare_materialized_view_job_submission`
  (`plan_ddl` answers the CHECK-style job-route guard). The view create's
  initial-build reorg phase stays the recorded seam: a submitted view job
  queues until that worker batch lands. The view tests drive the refusals,
  the count mismatch, and the submitted spec (derived column types, PK
  shape, metadata) through to a queued job row; failure sets unchanged
  (exec 7 = base minus the batch-15 repair, session subset of base).
  Receipt: `receipts/ddl_mview_view_submission.md`.
- 2026-09-04: aligned the Rust `tidb-datatype` decimal division owner for the
  complete Go-master `pkg/types` fixed-word boundary. Quotients now retain
  Go's `ErrTruncated` disposition when fractional words exceed the nine-word
  buffer, and the expression `/` path routes that event through the session
  truncation policy. Focused fail-before/pass-after regressions, package
  inventory, and Ready validation are recorded in
  `receipts/types_explain_format_audit.md`.
- 2026-09-04: aligned the Rust `tidb-datatype` runtime default-field-type
  owner for Go's float-width spelling boundary. `DefaultTypeForValue` now
  measures `+Inf`/`-Inf`/`NaN` through the source-compatible format helpers,
  matching the protocol `flen` for non-finite float32/float64 values. The
  focused fail-before/pass-after regression, package inventory, and Ready
  validation are recorded in `receipts/types_explain_format_audit.md`.

- 2026-09-04: aligned the Rust `tidb-datatype` float-to-decimal owner for
  the complete Go-master `pkg/types` formatting boundary. `Decimal::from_f64`
  now uses shortest `%g`-compatible scientific formatting before the fixed
  word parser, preserving both tiny exponents and the 81-digit overflow edge.
  The focused fail-before regression, package inventory, and Ready validation
  are recorded in `receipts/types_explain_format_audit.md`.
- 2026-09-04: aligned Go `BuildCastFunction4Union`'s unsigned integer
  `inUnion` behavior across `tidb-ast`, `tidb-expr`, and `tidb-planner`.
  `CastType::UnsignedInUnion` is an internal carrier; ordinary SQL casts still
  restore and dispatch as `UNSIGNED`. UNION and recursive-CTE projections now
  build `cast_unsigned_in_union`, whose evaluator clamps negative integer,
  real, decimal, and string-as-int inputs to zero according to their Go
  signature while leaving temporal-source casts on the ordinary path. Focused
  expression/planner regressions and the complete inventory are recorded in
  `receipts/expression_planner_in_union.md`.

- 2026-09-04 validation: focused expression/planner tests, the planner owner
  suite (35/35), all-target compilation, formatting, and diff checks passed.
  The owner all-target run retained one unrelated loopback-PD label-delivery
  failure; the full `tidb-expr` nextest run retained one unrelated loopback HTTP
  JSON-schema fixture failure. Strict clippy remains blocked by pre-existing
  diagnostics in unrelated workspace/generated code; the receipt records the
  exact commands and outcomes.

- 2026-09-04: aligned the Rust `tidb-datatype` decimal shift owner for the
  complete Go-master `pkg/types` fixed-word boundary. A rounding carry is now
  discarded when every original digit lies outside the retained fractional
  words, matching Go's post-round digit-bound check for `9e-82` (zero plus
  truncation). The focused fail-before regression, package inventory, and
  Ready validation are recorded in `receipts/types_explain_format_audit.md`.

- 2026-09-04: aligned the Rust `tidb-datatype` `STR_TO_DATE` owner for the
  complete Go-master `pkg/types` Unicode punctuation boundary. The `%.'` token
  now follows Go's `unicode.IsPunct` categories, including non-ASCII
  punctuation, while excluding the 13 punctuation code points present in the
  dependency's Unicode 16 table but absent from Go 1.25's Unicode 15 table.
  The focused fail-before regression, package inventory, and Ready validation
  are recorded in `receipts/types_explain_format_audit.md`.

- 2026-09-04: aligned `get_fsp` in the Rust `tidb-datatype` owner for the
  complete Go-master `pkg/types` temporal literal boundary. FSP now counts all
  bytes after the source fraction dot (including timezone/trailing bytes) and
  caps at six; the focused fail-before regression also checks the live
  `parse_datetime` metadata path. Evidence is recorded in
  `receipts/types_explain_format_audit.md`.

- 2026-09-04: aligned signed string/bytes integer conversion in the Rust
  `tidb-datatype` owner for complete Go-master `pkg/types`. Narrow signed
  targets now preserve Go's original parse/truncation error when range
  clamping also reports overflow; unsigned precedence remains unchanged. The
  focused fail-before regression and source inventory are recorded in
  `receipts/types_explain_format_audit.md`.

- 2026-09-04: aligned the Rust `tidb-datatype` decimal value parser for the
  complete Go-master `pkg/types` clamped-exponent boundary. A bad exponent now
  clears the intermediate value but continues through Go's signed bound checks,
  preserving max-decimal-plus-overflow and zero-plus-truncation outcomes. The
  focused fail-before regression and source inventory are recorded in
  `receipts/types_explain_format_audit.md`.

- 2026-09-04: aligned the Rust `tidb-datatype` fixed-word decimal parser for
  the complete Go-master `pkg/types` Unicode-whitespace boundary. The source
  `MyDecimal.FromString` trims Go `strings.TrimSpace` around exponent and
  trailing text; Rust now decodes valid UTF-8 whitespace at those byte
  boundaries while preserving malformed bytes. The focused fail-before
  regression, package inventory, and validation evidence are recorded in
  `receipts/types_explain_format_audit.md`.

- 2026-09-04: aligned the Rust `tidb-datatype` owner for the complete
  Go-master `pkg/types` binary-literal signed-conversion boundary. Direct
  `Datum::to_i64` now preserves Go's split between bounded `BinaryLiteral`
  conversion and MySQL BIT reinterpretation, while `Datum::convert_to` returns
  zero beside the truncation event for literals wider than eight bytes. The
  focused fail-before regression and the package-wide Ready validation are
  recorded in `receipts/types_explain_format_audit.md`; the remaining
  comparison-context findings stay explicit follow-ups.

- 2026-09-04: aligned the Rust `tidb-datatype` comparison owner for the
  complete Go-master `pkg/types` non-UTF-8 numeric-byte boundary. Raw bytes
  now follow Go's ASCII prefix scan through a lossy view for both float and
  decimal comparison, so invalid UTF-8 yields the source zero-prefix ordering
  instead of a Rust-only refusal. The focused fail-before regression and the
  package Ready validation are recorded in
  `receipts/types_explain_format_audit.md`; warning-sink/context semantics
  remain explicit D4/D5 follow-ups.

- 2026-09-04: aligned the Rust `tidb-datatype` owner for the complete
  Go-master `pkg/types` float-to-decimal conversion boundary. `Datum::to_decimal`
  now routes REAL and FLOAT32 through Go's shortest `%g` formatting and the
  existing `MyDecimal.FromString`-equivalent parser, preserving the saturated
  81-digit value plus overflow event for `1e308` and narrowing FLOAT32 payloads
  before widening. The focused fail-before regression, full 372-test datatype
  owner suite, dependent `tidb-expr` suite (1114 passed, 130 documented gaps),
  and Ready gates are recorded in `receipts/types_explain_format_audit.md`.

- 2026-09-04 (batch 15, `pkg/ddl` MV log-create worker): implemented Go
  master `94a9cbedab`'s `onCreateMaterializedViewLog` and its rollback as
  `plan_persisted_materialized_view_log_job_step` over the batch-14
  submitted job — the execution-time base re-checks (cancelling exactly as
  Go's `job.State = Cancelled` returns), the `$mlog$` table landing PUBLIC
  as Go's `createTable` does, the base's `MaterializedViewBase.MLogID`
  back-reference with its already-has-log refusal, the
  `mysql.tidb_mlog_purge_info` row via the new `mlog_purge_info_table`
  storage (Go's INSERT IGNORE / ON DUPLICATE semantics), the schema-version
  bump with the create-table event, `FinishMultipleTableJob`, and the
  rollback path that drops the created table with its auto-ID allocators
  and clears the base. The bootstrap now creates Go master's missing
  masking-policy and materialized-view `mysql.*` groups at their reserved
  IDs; the stale bootstrap-count assertion (failing since the notifier
  batch) is repaired and the bootstrap corpus test now names the six
  target-only tables whose Go captures await a master checkout. Schedule
  evaluation for logs WITH a purge clause stays behind the session-eval
  seam (the step takes the derived value). One regression drives submit to
  step to terminal and the rollback transition; tidb-exec ends at 7
  failures (base minus the repaired stale count), executor/session sets
  byte-identical to base. Receipt: `receipts/ddl_mview_log_worker.md`.

- 2026-09-04 (batch 14, `pkg/ddl` MV log-create submission): implemented Go
  master `94a9cbedab`'s `CreateMaterializedViewLog` submission and the
  portable prefix of `CreateMaterializedView`. The log create now submits
  through `prepare_materialized_view_job_submission` (the CHECK-shaped
  durable-job route): the admission order, `BuildMaterializedViewLogTableInfo`
  in full (flag-deleted base-column copies stamped with Go's exact
  `FieldTypeForMaterializedViewLogColumn` result, the reserved-name/JSON/BLOB
  refusals, the NOT NULL `_MLOG$_DML_TYPE` VARCHAR(1) and `_MLOG$_OLD_NEW`
  TINYINT(4) physical columns, the 1059 derived-name cap, PURGE
  IMMEDIATE/NEXT validation through batch 9's schedule-expression builder,
  `BuildMLogAccumulationAlertRows`, and `MaterializedViewLogInfo` with the
  SQL mode and `GetTimeZone` pair), then the job envelope with typed args
  and the shared submit preflight; the GID-allocation MV arms in
  `ddl_job_submit` (batch 10's recorded gap) assign the table ID inside the
  args and stamp `Job.TableID`. The view create advances through hint-DB
  normalization, canonical restore, refresh/attributes meta and the returned
  `mviewQueryAnalysis` (the SELECT-coverage error now quotes the written
  GROUP BY name like Go), stopping at Go's restricted-SQL derivation seam.
  `job.session_vars` moved to a plain-string key map so Go's
  `map[string]string` envelope serializes (GoString raw-value keys cannot be
  JSON map keys — exercised here for the first time). One regression drives
  the log create from statement to active job row; the full `tidb-exec`
  failure set is identical to the base (8; re-verified on the stashed base),
  `tidb-executor`'s too (165, pre-existing on the current base — the earlier
  "29" note was stale), `tidb-session` matches its recorded 690, and
  `tidb-model` is 321/321. Receipt: `receipts/ddl_mview_log_submit.md`.

- 2026-09-03 (batch 13, `pkg/ddl` MV query analysis): implemented Go master
  `94a9cbedab`'s `validateCreateMaterializedViewQuery` analysis body in
  `plan_validate_materialized_view_query`: `resolveMViewColumnName` (schema/
  table/alias qualifier rules, 1054 refusals), the GROUP BY item loop (plain
  columns only, duplicates refuse), WHERE construction over the base columns
  via a `ColumnResolver` with Go's `CheckNonDeterministic` (unfoldable
  functions through `tidb_expr::constant_fold::is_unfoldable`), the SELECT
  field loop (wildcard refusal, bare columns in GROUP BY, duplicates, the
  count/sum/min/max whitelist with DISTINCT-aggregate and arity refusals,
  SUM over temporal columns, count(*)/count(1) required, nullable-SUM COUNT
  pairing, MIN/MAX index coverage via batch 4's helper, and the mlog
  column-coverage closure). One end-to-end regression: a fully valid
  statement reaches the job seam while every malformed variant carries Go's
  refusal; the full `tidb-exec` failure set is identical to the pre-batch
  base. Receipt: `receipts/ddl_mview_query_analysis.md`.

- 2026-09-03 (batch 12, `pkg/ddl` MV query-clause refusals): implemented Go
  master `94a9cbedab`'s `validateCreateMaterializedViewQuery` clause
  refusals in `plan_create_materialized_view`: HAVING, ORDER BY, LIMIT and
  DISTINCT each refuse with Go's exact 8200 message, after the GROUP BY
  requirement. One parse-fixture regression pins all four; the full
  `tidb-exec` failure set is identical to the batch-9-era baseline (8
  pre-existing). Recorded remainder: the GROUP BY item analysis, WHERE
  determinism and the per-field aggregation / mlog column-coverage checks
  need the expression-analysis owner. Receipt:
  `receipts/ddl_mview_query_clauses.md`.

- 2026-09-03 (batch 11, `pkg/ddl` MV lowering — ExecPlan entry restored: the- 2026-09-03 (batch 12, `pkg/ddl` MV query-clause refusals): implemented Go
  master `94a9cbedab`'s `validateCreateMaterializedViewQuery` clause
  refusals in `plan_create_materialized_view`: HAVING, ORDER BY, LIMIT and
  DISTINCT each refuse with Go's exact 8200 message, after the GROUP BY
  requirement. One parse-fixture regression pins all four; the full
  `tidb-exec` failure set is identical to the batch-9-era baseline (8
  pre-existing). Recorded remainder: the GROUP BY item analysis, WHERE
  determinism and the per-field aggregation / mlog column-coverage checks
  need the expression-analysis owner. Receipt:
  `receipts/ddl_mview_query_clauses.md`.

- 2026-09-03 (batch 11, `pkg/ddl` MV lowering — ExecPlan entry restored: the- 2026-09-03 (batch 11, `pkg/ddl` MV lowering — ExecPlan entry restored: the
  rebase that published this batch's commit dropped it): implemented Go
  master `94a9cbedab`'s `CreateMaterializedView` /
  `CreateMaterializedViewLog` lowering and admission in
  `tidb-exec::cluster_ddl`: the `DdlStatement` variants, the
  `checkMaterializedViewEnabled` gate (`StmtContext::with_enable_mview`,
  default OFF), `ErrNoDB`, the 1024-byte comment cap (8020), SELECT-only,
  single-base-table, same-schema, `TableNotExists`, `ErrWrongObject` (1347)
  for view/sequence/temporary bases, the partitioned-base refusals (8200),
  the derived `$mlog$` existence and identity checks (1105),
  `mviewutil::check_materialized_view_select` (batch 4), GROUP BY required
  and WITH-ROLLUP refusals, and the documented job seam for valid
  statements. Six parse-fixture regressions pass; the full `tidb-exec`
  failure set is identical to the pre-batch base. Receipt:
  `receipts/ddl_mview_lowering.md`.

- 2026-09-03: verified divergence item 4 (`tidb-parser` identifier gate) is- 2026-09-03 (batch 10, `pkg/ddl/jobsubmit`): aligned `job2TableIDs`'s two
  new materialized-view arms in `tidb-exec::ddl_job_submit::job_table_ids`:
  a view create reports its id plus every created log id, a log create
  reports its id plus the recorded base table id, both through Go's
  `makeStringForIDs` set-dedupe + lexicographic string sort. One regression
  pins all four shapes (including the nil-metadata fall-throughs); fail-by
  pre-batch default fall-through. Recorded gaps: the GID-allocation MV arms
  and `SetSchemaDiffForCreateTable`'s rollback/reorg arms have no Rust owner
  (job-submission ID allocation and the DDL-job commit diff writer are
  unported infrastructure); `delete_range`/`rollingback`/`sanity`/`reorg` MV
  arms belong to the DDL worker batch. Receipt:
  `receipts/ddl_jobsubmit_mview_ids.md`.

- 2026-09-03 (batch 9, `pkg/ddl` create-path): implemented Go master
  `94a9cbedab`'s `BuildAndValidateMViewScheduleExpr` +
  `restoreNodeToCanonicalSQL` (canonical restore through the
  WITHOUT_CHARSET flags, empty-column-scope build, type inference with Go's
  8200 refusal and `TypeStr` text) in
  `tidb-executor::ddl::mview_schedule_expr`, and `BuildTableInfoWithLike`'s
  clearing of the three materialized-view metadata fields in the
  `CREATE TABLE LIKE` copy (`tidb-exec/src/cluster_ddl.rs`). One parse-
  fixture build-and-validate regression plus one cluster-DDL LIKE regression
  pass; fail-before by symbol absence and by the metadata assertion. Full
  `tidb-executor` delta vs base: candidate-new failures reproduced at the
  pre-batch commit (pre-existing environmental baseline); zero attributable
  new failures. Receipt: `receipts/ddl_mview_create_path.md`.

- 2026-09-03: verified divergence item 4 (`tidb-parser` identifier gate) is
  already fixed on this tree — the fallback gate is Go's 13-word
  `isReservedClauseKeyword` list — and pinned it with a regression
  (`SELECT rows FROM t`). Items #10/#11 recorded as parity-by-API/open in
  the divergence doc.

- 2026-09-03 (batch 8, `pkg/ddl/session` seam + `mview_schedule_expr.go`):
  verified `pkg/ddl/session` is already fully transcreated in
  `tidb-ddl-session`, then landed the schedule-eval capabilities the MV
  chain needs: `SessionContext::install_schedule_eval_session` /
  `restore_schedule_eval_session` (Go's sql-mode/type-flags/err-levels/
  time-zone swap and its restore closure) and `eval_schedule_expression`
  (Go's parse + BuildSimpleExpr + Eval + DATETIME@MaxFsp convert), plus
  `ScheduleEvalOriginals`. Ported the session-driven half of
  `mview_schedule_expr.go` into `tidb-executor/src/ddl/mview_schedule_expr.rs`:
  the NOW(6) load, START-precedence with the 10-second near-now threshold,
  NEXT fallback, NULL-branch logging (error/warn split) and the
  `(next_unix_seconds, should_update)` contract, generic over the session
  context. Nine mock-driven regressions pass; `tidb-ddl-session` 5/5; the
  full `tidb-executor` failure set is identical to the pre-batch base (29
  documented pre-existing baseline failures, zero new).
  `BuildAndValidateMViewScheduleExpr` lands with create-path sub-batch (b).
  Receipt: `receipts/ddl_session_mview_schedule.md`.

- 2026-09-03 (batch 7 SEAM FINDING, `pkg/ddl/mview_schedule_expr.go`):
  sub-batch (a) is SEAM-BLOCKED, journal-verified. The file's behavior is
  woven into the live DDL session: `sess.Session.Execute("SELECT NOW(6)")`,
  `expression.BuildSimpleExpr` + `Eval` against the session's expression
  context (with the schedule sql-mode/type-flags/err-levels/time-zone
  installed by `setCreateMaterializedViewScheduleEvalSession`),
  `generatedexpr.ParseExpression`, datum `ConvertTo` to DATETIME, and the
  two structured NULL-schedule loggers. The Rust DDL owner
  (`tidb-executor/src/ddl/`) is a metadata-only carrier with no session
  execute seam; inventing one now would be a speculative API (forbidden).
  The READY PIECES are inventoried for the landing batch:
  `tidb-expr::build_simple_expr` (simple_expr.rs), the generated-expression
  parser (`tidb-model::generated_expr`), AST restore, the batch-4/5 helpers
  (`materialized_schedule_*` in `tidb-expr::expr_util::mview_schedule`), and
  the `TimeConversionError`/`ResolvedTimeZone` conversions. Sub-batches
  (a)–(d) all require the DDL-session seam first; that seam batch owns the
  `sess.Session` port and unblocks the whole chain.

- 2026-09-03 (batch 7 boundary, `pkg/sqlexec` + `pkg/session`): the
  `ExecOption.SessionVarsSetup` / `SessionVarsSetup` / `ExecOptionWithSessionVarsSetup`
  drift and its `getInternalSession` consumer are RECORDED, NOT ported:
  Go's callback type is `fn(*variable.SessionVars) -> fn()`, but the Rust
  `SessionVars` owner lives in `tidb-session`, ABOVE `tidb-sqlexec` — a
  verbatim field would invert the crate layering. Both halves belong to the
  future session-executor seam batch (which owns `ExecRestrictedSQL`'s
  option plumbing); the mview DDL core that CALLS this option is the same
  terminal batch. Also the terminal batch: the `pkg/ddl` materialized-view
  core (`materialized_view.go` 1181, `mview_worker.go` 921,
  `mview_schedule_expr.go` 302, plus `delete_range`, `rollingback`,
  `job_worker`, `executor.go`, `create_table.go`, `backfilling_txn_executor`,
  `sanity_check`, `schema_version`, `reorg`, `jobsubmit`, schematracker, and
  the `pkg/executor`/`pkg/planner/core` wiring), planned as ordered
  dependency-closed sub-batches: (a) `mview_schedule_expr.go` + create-time
  checks in `materialized_view.go` over the batch-4 helpers; (b) the
  create/reload path in `create_table.go`/`executor.go`/`materialized_view.go`
  writing batch-1 metadata; (c) delete-range + rolling-back + sanity
  arms; (d) the refresh/maintenance worker and service wiring.

- 2026-09-03 (batch 6, `pkg/infoschema`): aligned `builder.go`'s diff
  classification with Go master `94a9cbedab`: the incremental reload's
  create arm accepts `ACTION_CREATE_MATERIALIZED_VIEW` and
  `ACTION_CREATE_MATERIALIZED_VIEW_LOG` exactly where Go's `getTableIDs`
  treats them as `ActionCreateTable`. Two parsed-fixture regressions prove
  an MV log and an MV diff reload incrementally with their batch-1 metadata
  intact, with fail-before evidence from reverting the arms; the full
  `tidb-exec` failure set is identical to the pre-batch base (8 documented
  pre-existing baseline failures, zero new). Recorded gap: Go's placement
  bundle-cache arms have no Rust owner (the reload tier refuses to a full
  reload). Receipt: `receipts/infoschema_mview_diff.md`.
- 2026-09-03: closed divergence item A-2 (`tidb-codec` chunk wire decode):
  the offset-table loop drops the Go-absent first-zero and monotonicity
  validations, preserving non-zero-based tables verbatim for the
  `ReuseIntermChk` rebase path; two regressions proven to fail against the
  stricter checks. See `receipts/codec_chunk_decode_receipt.md`.

- 2026-09-03 (batch 5, `pkg/expression/helper.go`): implemented Go master
  `94a9cbedab`'s three materialized-view schedule helpers in
  `tidb-expr::expr_util::mview_schedule`:
  `MaterializedScheduleTimeToUnixSeconds` (nil-time/nil-zone boundaries,
  `CoreTime::to_datetime` under Local/Named/Fixed resolved zones),
  `MaterializedScheduleTypeFlagsWithSQLMode` (STRICT_FLAGS plus Go's four
  mode-driven flag overrides), and `MaterializedScheduleErrLevelsWithSQLMode`
  (strict-base LevelMap with Go's four group resolutions, including the
  ignore-over-warn DividedByZero rule). Four parse-shaped regressions pass;
  the full `tidb-expr` suite is 1127/1127; fmt and whitespace gates clean.
  `tidb-expr` gains an acyclic `tidb-model` dependency for the
  `ResolvedTimeZone` parameter type. Receipt:
  `receipts/expression_mview_schedule_helpers.md`.

- 2026-09-03 (batch 4, `pkg/util/mviewutil`): transcreated the new package
  (created by Go master `94a9cbedab`) into `tidb-util::mviewutil`:
  `CheckMaterializedViewSelect` (the `QueryStmt::Select` assertion, WITH /
  locking / SELECT INTO / AS OF / TABLESAMPLE refusals carrying Go's
  `ErrGeneralUnsupportedDDL` 8200 identity and exact detail text, and the
  multi-table/derived/set-opr fall-throughs) plus the four
  prefix-covering-index helpers (PRIMARY-handle branch order, the excluded
  `PRIMARY` guard, visible/public filtering, prefix-length and
  duplicate-column rejection). Five parse-fixture regressions pass; the full
  `tidb-util` suite is 560/560; fmt and whitespace gates clean. The package
  is new on master with no test file, so fixtures are parsed SQL and the
  fail-before is module absence. Receipt: `receipts/util_mviewutil.md`.

- 2026-09-03 (batch 3, `pkg/kv`): aligned `option.go`'s internal transaction
  types with Go master `94a9cbedab`: `InternalTxnMViewMaintenance =
  "mview_maintain"` now exists in `tidb-txnkv` `option.rs`. Focused check and
  the Ready fmt/whitespace gates pass. The remaining materialized-view drift
  from `94a9cbedab` is QUEUED, not dropped, because each remaining slice's
  Rust owner needs its own package-atomic batch: `pkg/util/mviewutil` (new
  154-line package) needs `tidb-ast` to model `SelectStmt.LockInfo`,
  `SelectIntoOpt`, `TableName.AsOf` and `TableSample` first;
  `pkg/expression/helper.go`'s three `MaterializedSchedule*` helpers need the
  Rust owners of `types.Flags`/`errctx.LevelMap` and a `types.Time` Go-time
  conversion; `pkg/infoschema/builder.go`'s action-type switches have no Rust
  bundle-update owner yet; `pkg/sqlexec`'s `ExecOption.SessionVarsSetup` and
  the `pkg/session` internal-session consumer are an own pair; and the
  `pkg/ddl` core (18 files: `materialized_view.go`, `mview_worker.go`,
  `mview_schedule_expr.go`, schematracker, delete-range, rolling-back, plus
  the `pkg/executor` and `pkg/planner/core` wiring) is the terminal large
  batch of this feature. QUEUE PRUNING CONCLUSION: every other
  wide-window package (`e2788410d8..origin/master`) was re-verified at
  pins from 2026-09-01/02 (`049e0e2ba7` or later, e.g. the aggfuncs area
  at `d663c50ff1`), so the only uncovered master drift is exactly the
  `94a9cbedab` materialized-view feature — batches 1–3 covered its
  `pkg/meta/model`, `pkg/sessionctx/*`, and `pkg/kv` slices; the queued
  slices above are the complete remainder.

- 2026-09-03 (batch 2, `pkg/sessionctx/variable` + `pkg/sessionctx/vardef`):
  aligned the sessionctx slice of Go master `94a9cbedab`'s materialized-view
  commit: the five `tidb_mview_*` variable names and defaults plus
  `MaxConfigurableConcurrency` in `tidb-vardef`; five registry entries, the
  shared `normalizeIsolationReadEnginesValue` helper (now also wired for the
  pre-existing `tidb_isolation_read_engines`, closing that SET's case/
  empty/unknown validation gap), the `<128 → 128` mem-quota truncated-value
  clamp, the go-units disk-quota check, the `MViewExecutionSessionVars`
  capture/apply/restore machinery with Go's twelve ordered assignments and
  best-effort/strict failure semantics (Go's restore closure becomes a
  restore handle taking the session), and the programmatic
  `InMViewMaintenance` flag. Eleven source-derived regressions pass with
  fail-before evidence from reverting the validation cases; vardef+config
  suites 143/143; the full `tidb-session` failure set is identical to the
  pre-batch base commit (690 documented pre-existing baseline failures,
  zero new). Go's `SetSession` hook halves follow the crate's documented
  hooks-not-modelled policy. Receipt:
  `receipts/sessionctx_variable_mview.md`.
- 2026-09-03: closed divergence item F2 (`tidb-datatype`
  FieldTypeBuilder): `new()` now starts at Go's zero value (flen/decimal 0,
  `field_type_builder.go:23-25`) instead of seeding -1. All five consumer
  crates re-run; the executor's 136 SQL-source failures are a pre-existing
  baseline (identical with/without the change). One regression proven to
  fail pre-fix. See `receipts/datatype_json_fieldtype_receipt.md`.

- 2026-09-03 (batch 1, `pkg/meta/model`): implemented Go master
  `94a9cbedab`'s materialized-view metadata in `tidb-model`: action types
  85/86 with names and BDR `SafeDDL` classification, `MayNeedReorg`/
  `IsRollbackable` coverage, `SubJob.InvolvingSchemaInfo` with omitempty
  persistence and `ToProxyJob`/`FromProxyJob` propagation plus the runtime
  `MultiSchemaInfo` field, `TableInfo`'s three omitempty materialized-view
  pointers with deep `Clone`, the `MaterializedViewBaseInfo`/
  `MaterializedViewInfo`/`MaterializedViewLogInfo` model with
  `MViewInitBuildState` display/access-error contracts, the `$mlog$`
  constants and rune-budgeted log-table naming, and the
  `CreateMaterializedViewLogArgs`/`CreateMaterializedViewArgs` v1+v2 job-arg
  round trips. Nine source-derived regressions in
  `tests_pkg_meta_model_materialized_view.rs` pass, with fail-before
  evidence from reverting the two behavioral arms; full owner suite 321/321,
  five dependent crates check clean. Recorded gap:
  `SetTiFlashReplicaArgs` (and its `SkipColumnarStorageGate` field) has no
  Rust owner yet. Receipt: `receipts/meta_model_materialized_view.md`.
  This batch's ExecPlan edit also repaired the stacked conflict markers
  (`<<<<<<< HEAD` ×8) that a prior parallel rebase committed into this
  file; the duplicated journal entries were deduplicated, no content lost.

- 2026-09-03 (parity sweep, worktree `~/Documents/GitHub/tidb-parity-sweep`,
  branch `codex/hparser-parity-sweep-20260903`, push target
  `origin/hparser-integration`, fetch refspec master-only so
  `hparser-integration` must be fetched explicitly): resumed the rolling
  master-drift walk. Synced to remote tip `5e20d9150b` (six zcode-parity
  planner commits, including the pending `9693463ff2`). Standing queue: 206
  drifted leaf Go packages in `e2788410d8..origin/master`, ordered by drift
  size, each batch re-pinned and pruned at batch time (skip when no drift
  since the package's freshest receipt pin). Fresh master tip `94a9cbedab`
  (materialized-view DDL) is the newest drift. CURRENT POSITION: batch 1 =
  `pkg/meta/model` → DONE above; next = `pkg/sessionctx/variable` +
  `pkg/sessionctx/vardef` drift (`94a9cbedab`), then `pkg/executor` /
  `pkg/planner/core` / `pkg/kv` / `pkg/infoschema` / `pkg/session` /
  `pkg/util/mviewutil` / `pkg/expression` slices of the same commit, then
  the standing wide-window queue.
- 2026-09-03: closed divergence item 6 (`tidb-datatype` lone surrogates):
  JSON_UNQUOTE's escape decoding combines an adjacent `\u` via
  `utf16.DecodeRune` (U+FFFD for invalid pairs, Go's error when absent),
  `decode_escaped_unicode` substitutes U+FFFD, and `BinaryJSON::parse`
  sanitizes lone-surrogate escapes before the serde retry. Per-surface
  regressions; item 7 (invalid UTF-8) stays a documented gap per the
  audit's reachability note. See
  `receipts/datatype_json_fieldtype_receipt.md`.

- 2026-09-03: closed divergence item 8 (`tidb-datatype` json path): a
  dangling `to` in `$[N to]` degrades to a plain index exactly as Go's
  `tryReadString` leaves the stream past `to` (`json_path_expr.go:462-480`),
  instead of erroring 3143; `$[0 tox]`/`$[0 to3]`/`$[0 to ]` stay rejected.
  One Go-derived regression proven to fail pre-fix. See
  `receipts/datatype_json_fieldtype_receipt.md`.

- 2026-09-03: closed divergence item 3 (`tidb-parser` predicate/`IS`
  chaining) with Go master `a85e0fd5df`: both `parseInfixExpr` latches
  (`noMorePredicate`, `noMoreIS`) are implemented with Go's exact arm
  coverage, `parseIsExpr`'s chainable split (NULL chains; TRUE/FALSE/UNKNOWN
  do not, UNKNOWN included) is ported, and the doc's inaccurate BETWEEN
  claim is corrected (the high side chains with a fresh latch in Go). Four
  rejection regressions plus two chaining pins; the full tidb-parser suite
  is green. See `receipts/parser_expr_latches.md`.

- 2026-09-03: closed the last two deterministic baseline failures — both
  were test assertions exceeding Go (the recursive-CTE error is Go's
  refined `ErrCTERecursiveRequiresUnion`, and a Dual table plan is never
  index-lookup-push-downable per `detachRootTableScanPlan`'s TableScan
  assertion). The `tidb-planner --lib` suite is now fully green: 903
  passed, zero failures.

- 2026-09-03: triaged the five deterministic baseline `--lib` failures
  (single-threaded reproduction: no isolation flake). Fixed three —
  `check_index_can_be_key` pruned-schema rejection, the union-all child-TopN
  fixture id collision, and the per-partition index-hint double-application —
  and diagnosed the remaining two (recursive-CTE error refinement and
  `index_lookup_push_down`) to specific unwired Go paths. See
  `receipts/planner_rule_child_access.md`.

- 2026-09-03: audited the doc-admitted narrowings of `tidb-planner` into
  `receipts/planner_doc_admitted_narrowings.md` (audit-only, no code
  change): sixteen self-declared refusals/narrowings across cost, enforce,
  task conversion, window/CTE build, DML, ranger, and scan surfaces, each
  still owing an individual Go comparison; they are feature-surface gaps,
  not the child-access refusals the boundary sweeps fixed.

- 2026-09-03: aligned the unary-walk and sequence-collapse boundaries with
  Go master `a85e0fd5df` (same worktree/branch): empty-selection elimination
  replaces a tested selection with `Children()[0]` directly, the
  result-reorder handle walk indexes its unary chain, and sequence push-down
  expects the main-query child in both the descend and the collapse. Two
  regressions proven to fail pre-fix; constant-propagation verified as
  iteration parity and left alone. See
  `receipts/planner_rule_child_access.md`.

- 2026-09-03: aligned aggregation-elimination boundaries with Go master
  `a85e0fd5df` (same worktree/branch): distinct elimination reads the child
  schema inside Go's per-function all-column-args branch, the PKOrUK
  coverage check indexes `Children()[0]`, and `rewrite_aggregate` reads
  `Args[0]` unguarded — while Go's explicit `len(Children()) != 1` guard
  stays pinned. Two regressions proven to fail pre-fix; see
  `receipts/planner_rule_child_access.md`.

- 2026-09-03: landed the remaining `tidb-planner` boundary deltas on
  `hparser-integration` (extract-FD, semi-join rewrite, physical
  join/projection schema assembly, and `LogicalProjection.DeriveStats`),
  each verified against Go master `a85e0fd5df` with pre-fix failing
  regressions; the join-reorder cluster was audited and needs no change.
  Details in `receipts/planner_rule_child_access.md`.

- 2026-09-03: aligned `LogicalProjection.DeriveStats` with Go master
  `a85e0fd5df` (same worktree/branch): the per-expression NDV loop indexes
  `selfSchema.Columns[i]` directly as Go does instead of breaking on a
  short schema; the join-reorder cluster was audited and needs no change
  (all four candidate sites are parity or unreachable dead tolerance). One
  regression proven to fail pre-fix; see
  `receipts/planner_rule_child_access.md`.

- 2026-09-03: aligned the Rust transaction and SQL error consumers for Go
  master `pkg/store/driver/txn`'s shared-lock-loss response. Both direct
  `KeyError.shared_lock_lost` responses and the vendored client error now
  become a typed terminal `TransactionCause`, preserving the transaction
  start timestamp and client-go key redaction. The `tidb-exec` boundary emits
  the registered `[tikv:9015]` message instead of the Rust generic 1105 path;
  focused classification and SQL-scope regressions pass. Session-level
  rollback wiring remains a separately inventoried boundary. Details are in
  `rust/testport/receipts/store_driver_txn.md`.

- 2026-09-03: closed the two executable sequence-arithmetic gaps in the
  `pkg/ddl` sequence-function carrier. Go master’s
  `pkg/meta/autoid.CalcSequenceBatchSize` relies on wrapping signed `int64`
  operations after the unsigned-domain seek; Rust had used checked operators,
  so valid negative-start and near-`i64`-boundary ladders panicked in debug.
  `tidb-executor/src/sequence.rs` now spells those operations explicitly as
  wrapping subtraction, multiplication, addition, negation, and seed/cycle
  boundary updates. The two source-derived tests are active and pass, with
  pre-fix failures recorded at `sequence.rs:219`; details are in the b110
  follow-up receipt. No Go source was edited.

- 2026-09-03: aligned the Rust `tidb-parser` SQL and optimizer-hint scanners
  with Go master `e2b6ce7333`'s 10,000-level parentheses guard. Rust now
  computes token nesting before recursive parsing and returns the matching
  `parentheses nesting depth exceeds maximum 10000` diagnostic from SQL,
  multi-statement, and standalone hint entrypoints. The same batch activates
  Go's `AST nesting depth exceeds maximum 10064` contract for 11,000-term
  binary, unary, and nested-CASE expressions; recursive forms are rejected
  before descent and large post-parse trees are visited on a bounded larger
  stack. The focused SQL regression reproduced a pre-fix process stack
  overflow, while all depth regressions now pass. No Go source or generated
  parser output changed. Details are appended to `receipts/parser_root.md`.

- 2026-09-03: aligned the Rust `tidb-expr` expression-tree
  `Decorrelate` and `PropagateType(ETReal)` surfaces with Go master
  `049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5`. Constants and plain columns now
  support identity decorrelation, correlated columns are recursively replaced
  through an outer schema, and scalar-function argument caches are invalidated
  on rebuild; a nil schema on a correlated node retains Go's panic boundary.
  The shared ETReal propagation helper now applies Go's DECIMAL
  width/scale safeguards and is used by the aggregate real-cast wrapper. The
  focused constant/scalar regression, owner all-target check, full owner suite,
  file-scoped rustfmt, and Ready lint gates pass; the package receipt records
  the complete inventory and the clean fail-before compile errors.

- 2026-09-03: removed the stale Rust-only Unix-endpoint gap stub from the
  `pkg/store/mockstore/unistore` carrier. The dependency-closed
  `tidb-txnkv::unistore` owner already runs the full Go-derived service URL
  regression, so the duplicate `#[ignore]` function that only panicked was
  deleted and the package receipt now records the active owner test. No Go or
  production Rust source changed.

- 2026-09-03: aligned Rust `tidb-expr` DECIMAL-to-DOUBLE metadata
  propagation with Go master `049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5`.
  `wrap_with_cast_as_real` now updates an owned DECIMAL child to Go's
  propagated `(flen=48, decimal=30)` domain before constructing the real cast.
  The focused metadata regression, cast source module, owner all-target check,
  full owner suite, and Ready lint gates pass; details are in the expression
  receipt.

- 2026-09-03: refreshed the Rust `tidb-tikvutil` owner for Go master
  `049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5`. The complete two-artifact
  `pkg/util/tikvutil` package is unchanged and already exposes Go's one
  process-wide `AtomicI32`; a focused public-contract test now pins its 128
  default, signed width, and atomic update behavior. Details are in
  `receipts/util_tikvutil.md`.

- 2026-09-03: aligned Rust `tidb-expr` comparison refinement ordering with Go
  master `049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5`. The AST rewriter now
  applies the context-independent integer/string constant rule before
  comparison signature casts, so floor/ceiling rewrites such as
  `a < '1.0'` retain Go's `a < 1` shape. A focused source regression and the
  complete comparison-control table pass; the package receipt records the
  complete inventory, clean fail-before output, and Ready validation.

- 2026-09-03: aligned the Rust `tidb-ast` `Expr::ConvertUsing::format`
  boundary with Go master `049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5`.
  Go's `FuncCallExpr.Format` prints this parsed form as a generic lowercase
  call with comma-separated, double-quoted arguments, while Rust previously
  panicked with a Rust-only not-implemented path. The focused source-shaped
  regression now passes; canonical `Restore` remains unchanged. Details are
  appended to `receipts/parser_ast.md`.

- 2026-09-03: completed the next Rust `tidb-ast` formatter boundary from
  `pkg/parser/ast/functions.go`. `Expr::MemberOf` now preserves Go's infix
  `Format` text (including its historical double space before `(`), while
  `Position`, `WeightString`, and `Trim` now use Go's generic lowercase,
  comma-separated argument formatting. The source-shaped regression failed
  before the change on the Rust `MEMBER OF` and `POSITION` not-implemented
  panics and passes after the batch; canonical `Restore` remains unchanged.
  Details are in `receipts/parser_ast.md`.

- 2026-09-03: aligned the Rust `tidb-planner` aggregation `AggFuncs` access
  boundaries with Go master `049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5`.
  `agg_funcs_cols_for_first_row` and `prune_columns_local` now direct-index
  `AggFuncs` (and `Args[0]`) exactly where Go does, preserving panic behavior
  for malformed plans; `agg_funcs_cols_for_const_result` keeps Go's explicit
  length guard. Three pre-fix-failing panic regressions plus the guarded-loop
  pin were added. Complete `logicalop` inventory and Ready evidence are in
  `receipts/planner_rule_child_access.md`.

- 2026-09-02: aligned the Rust `tidb-ast` SET restoration security boundary
  with Go master `17daba3dfd`. The complete `pkg/parser/ast` inventory remains
  36 tracked artifacts (34,448 lines, including generated visitor inputs and
  output, support tests, fixture, and three BUILD files). Go's six-entry
  `embeddingAPIKeySysVars` allowlist now has a case-insensitive Rust owner:
  matching system assignments restore as `'******'`, while user variables and
  similarly named future variables retain their values. Added the focused
  source-derived regression and `SetStmt::secure_text()` API, and completed
  the existing `IndexOptions::auto_pre_split` restore fixture exposed by the
  all-target AST gate; details and Ready evidence are in
  `receipts/parser_ast.md`.

- 2026-09-02: aligned the Rust `tidb-domain` RU-statistics GC boundary with
  Go master `a85e0fd5df`. After enumerating the complete 31-artifact root
  `pkg/domain` boundary and all 17 `tidb-domain` owner artifacts, changed
  `GCOutdatedRecords` to direct-index the required `count(*)` row like Go;
  an impossible empty result now panics instead of returning the Rust-only
  `MissingCountRow` error. The focused regression fails against the pre-fix
  tree and passes after the change. Details are in
  `receipts/domain_ru_stats_audit.md`.

- 2026-09-02: aligned the Rust `tidb-util` master-key AES-GCM nonce boundary
  against Go master `a85e0fd5df`. The complete direct Go package inventory is
  11 artifacts and 943 lines (including BUILD metadata and all four test
  files); all 9 Rust `master_key` owner files and 33 in-module tests were read
  before editing. Rust previously returned `GcmError::InvalidNonceLength` for
  a 16-byte CTR IV, while Go's `cipher.AEAD.Seal`/`Open` panic on any nonce
  other than 12 bytes. Rust now preserves that panic with the Go diagnostic,
  removes the Rust-only error variant, and adds low-level and backend
  regressions. Full owner validation and the Ready gates are recorded in
  `receipts/br_encryption_master_key.md`.

- 2026-09-02: corrected the Rust `tidb-meta` `TxStructure::hclear` boundary
  against Go master `a85e0fd5df`. The complete `pkg/structure` inventory is
  eight artifacts and 1,423 lines; all 19 `tidb-meta` owner artifacts (7,886
  lines, including 10 aggregated test sources) were inventoried before
  editing. Go directly dereferences its nil `readWriter` when clearing a
  populated read-only hash, while Rust returned `WriteOnSnapshot`; Rust now
  panics at that boundary and has a focused pre-fix-failing regression. The
  updated package receipt is `receipts/structure.md`.

- 2026-09-02: aligned the Rust `tidb-expr` collation-name boundary with Go
  master `a85e0fd5df`. After inventorying all 137 root `pkg/expression`
  artifacts and all 175 `tidb-expr` owner artifacts, changed
  `Coercibility::name` from a Rust-only `Option` refusal to direct indexing of
  Go's seven-entry `coerString`; invalid signed/out-of-range values now panic
  instead of silently formatting as `EXPLICIT`. Updated the formatter and
  added the focused invalid-value regression. The package receipt is
  `receipts/expression_collation_audit.md`; the full library run has 1,076
  passes, nine documented baseline failures, and 139 ignored gap tests.

- 2026-09-02: extended the bounded Rust `tidb-expr` expression-utility batch
  against Go master `a85e0fd5df`. `GetFuncArg` now direct-indexes function
  arguments like Go (while retaining `None` for non-functions), and
  `ExtractColumnsFromColOpCol` now panics on two-argument non-column values
  after returning `None` for non-two-argument shapes. Focused pre-fix-failing
  regressions cover both unchecked boundaries; details are appended to
  `receipts/expression_collation_audit.md`.

- 2026-09-02: aligned the Rust `tidb-codec` schema-aware BIT decoder with Go
  master `17daba3dfd`. `decode_one_typed` now preserves Go's direct
  `NewBinaryLiteralFromUint` invalid-width panic instead of converting zero,
  negative, or oversized widths into a Rust-only unpadded literal. The
  focused `BIT(0)` regression fails against the pre-fix tree and passes after
  the change; package inventory and validation details are in
  `receipts/util_codec_audit.md`.

- 2026-09-02: completed a Rust-only `pkg/expression` duration-to-YEAR
  follow-up against Go master `17daba3dfd`. After rechecking all 137 direct
  root Go artifacts (and the 71 nested package artifacts) plus all 175
  `tidb-expr` owner artifacts, routed duration YEAR casts through the existing
  datatype conversion with the statement clock, session zone, and Go concat
  flag. The focused regression failed before the fix (`125959` instead of the
  pinned 2020 statement year) and now passes, alongside a `00:20:12` concat
  row yielding 2012. Details and the Ready validation record are in
  `receipts/expression_collation_audit.md`.

- 2026-09-02: extended the same Rust-only `pkg/expression` batch for
  duration-column comparisons. Reusing Go's post-`foldConstant` constant
  predicate makes a foldable `CONCAT` select ETDuration instead of the
  string/numeric fallback; invalid duration constants now return Go's NULL
  comparison result without converting an unspecified-FSP column to a number.
  The focused literal/concat/column/invalid/NULL-safe regression failed on
  clean `368ab79bb1` with the FSP panic and passes after the fix. The complete
  package inventory and Ready evidence are appended to
  `receipts/expression_collation_audit.md`.

- 2026-09-03: aligned Rust `tidb-expr` canonical semantic equality with Go
  master `049e0e2ba79d`. `Expression`, `Constant`, and `ScalarFunction` now
  derive the Go-compatible canonical hash bytes, including commutative child
  ordering, directed comparison reversal, `NOT` comparison rewrites, and
  cast result-type identity. The source-shaped semantic-equality table is
  live with a pre-fix compile-failure record and passes the focused Ready
  test; the separate `Values` gap remains explicit. Details are appended to
  `receipts/expression_collation_audit.md`.

- 2026-09-03: aligned Rust `tidb-expr` grouping metadata and scalar-function
  hash identity with Go master `049e0e2ba79d`. `ScalarFunction` now carries
  validated `GROUPING` mode/mark metadata, rejects uninitialized default
  construction, emits Go-compatible deterministic `ReHashCode` bytes,
  preserves metadata through column substitution, marks builder results
  unsigned, and evaluates grouping IDs with NULL propagation. The former
  ignored `TestColumnSubstituteGroupingCleansHashCode` is live, with a
  clean-tree fail-before record and focused Ready regressions. Details are
  appended to `receipts/expression_collation_audit.md`.

- 2026-09-03: aligned Rust `tidb-parser` charset diagnostics with Go
  `pkg/parser/ast/functions_test.go::{TestConvert,TestChar}`. `CONVERT(...
  USING ...)` and `CHAR(... USING ...)` now retain the raw charset token and
  emit Go's `[parser:1115]Unknown character set: '<name>'` message for invalid
  names; the source-shaped parser table is live and passing. Details are
  appended to `receipts/parser_ast.md` and `receipts/b058.md`.

- 2026-09-03: aligned Rust `tidb-ast` semantic commands with Go's
  `ShowCreateImport` enum case. `ShowCreateKind::Import` now participates in
  AST restore/visitor paths and maps to `SHOW CREATE IMPORT`; the former
  ignored source-shaped regression is live. Details are appended to
  `receipts/parser_ast.md` and `receipts/b059.md`.

- 2026-09-03: closed the parser-owned portion of the remaining `pkg/parser/ast`
  visitor gaps. `tidb-parser` now applies a balanced visitor over the exact
  Go multi-statement DDL/DML scripts (including the foreign-key tail,
  UNION-with-hints, LOAD DATA, and IMPORT forms); its existing procedure
  source-row and visitor tests are recorded as the owner-side closure as
  well. The AST-crate carriers remain ignored only for dependency direction.
  Details are appended to `receipts/parser_ast.md` and `receipts/b058.md`.

- 2026-09-03: completed the adjacent Rust `tidb-expr` scalar-function
  `Hash64`/`Equals` boundary against the same Go authority. The structural
  function tag, name, nullable return type, argument count, and recursive
  argument identity now participate in the Rust owner; the source-shaped
  name/argument/return-type regression was activated after a pre-fix missing-
  method compile failure. Ready evidence and the retained leaf-hash boundary
  are recorded in `receipts/expression_collation_audit.md`.

- 2026-09-03: aligned the Rust `tidb-expr` arithmetic construction and
  constant-folding boundaries with Go master. Direct function builders now
  infer arithmetic return types before comparison refinement, folded
  parameter/deferred subtrees retain context-only provenance, and the AST
  unary-minus binary-literal expectation matches Go's REAL result. Focused
  source regressions cover the three cases plus the complete constant-folding
  and `ConstLevel` tables; fail-before and Ready evidence are recorded in
  `receipts/expression_collation_audit.md`.

- 2026-09-02: aligned `LogicalPlan::extract_col_groups` child access with Go
  master `a85e0fd5df`. Join and apply outer-side arms now direct-index the
  required child schemas, and the window arm preserves Go's empty-group early
  return before indexing its child. Four focused malformed-tree regressions
  fail against the unfixed dispatcher and pass after; details are in
  `receipts/planner_rule_child_access.md`.

- 2026-09-02: aligned three bounded Rust `tidb-planner` logical-optimization
  rule entry boundaries with Go master `a85e0fd5df`. Union-all-dual,
  derive-TopN-from-window, and max/min elimination now direct-index the same
  `Children()[0]`, `AggFuncs[0]`, `Args[0]`, and `Schema().Columns[i]` slots
  that Go indexes, preserving panic behavior on malformed trees while
  retaining all ordinary early-return guards. Four focused regressions fail
  before the fix and pass after it; details are in
  `receipts/planner_rule_child_access.md`.
- 2026-09-02: aligned expression-rewriter boundaries with Go master
  `a85e0fd5df` (same worktree/branch). `push_last_schema_column` indexes
  `Columns[Len()-1]`/`OutputNames()[Len()-1]` like Go's four subquery
  handlers, the natural-join name-resolution walks index their unary
  children, and `build_quantifier_plan` expects the aggregation schema. Two
  regressions proven to fail pre-fix; see
  `receipts/planner_rule_child_access.md`.

- 2026-09-02: aligned column-pruning and TopN-pushdown walk boundaries with
  Go master `a85e0fd5df` (same worktree/branch). The prune rebuild arms
  expect the child schema, the TopN pushdown expects the projection's own
  schema for `ColumnSubstitute`, and the ID-0 check indexes the projection's
  child. Two regressions proven to fail pre-fix; see
  `receipts/planner_rule_child_access.md`.

- 2026-09-02: aligned physical schema assembly with Go master `a85e0fd5df`
  (same worktree/branch). `build_physical_join_schema` indexes both children
  and expects the left/own schemas exactly where Go nil-derefs or indexes;
  `eliminate_physical_projection` indexes the child inside the strict
  identity check and expects both schemas; the logical apply's
  `BuildLogicalJoinSchema` port `apply_schema` does the same while keeping
  Go's Inner-arm nil merge. Five regressions (four panic contracts proven to
  fail pre-fix, one nil-merge pin); see
  `receipts/planner_rule_child_access.md`.

- 2026-09-02: aligned the aggregation `AggFuncs` index boundaries with Go
  master `a85e0fd5df` (same worktree/branch). `agg_funcs_cols_for_first_row`
  and `prune_columns_local` now index the aggregate-function list directly
  (and the firstrow argument as `Args[0]`) wherever Go does, while
  `getAggFuncsColsForConstResult`'s explicit Go length guard is pinned by a
  regression test instead of being "fixed away". Three panic contracts
  proven to fail pre-fix plus one guard pin; see
  `receipts/planner_rule_child_access.md`.

- 2026-09-02: aligned the join-family rule bodies with Go master
  `a85e0fd5df` (same worktree/branch). Outer-join elimination indexes both
  children and gates its schema reads exactly where Go does; the
  outer-join-to-semi conversion reads the selection's schema/names through
  `children[0]` and indexes the join children directly; semi-join rewrite
  expects the left child schema where Go's `Schema().Clone()` nil-derefs.
  Four focused panic-contract regressions proven to fail pre-fix; see
  `receipts/planner_rule_child_access.md`.

- 2026-09-02: aligned the `GcSubstituter` schema selection with Go master
  `a85e0fd5df` (same worktree/branch). The selection, projection, and sort
  arms now substitute against the first CHILD's schema by direct index — Go
  passes the child schema for all three, and the previous own-schema-first
  fallback could mute projection substitution entirely; the aggregation arm
  drops its child fallback and reads only the own producer schema as Go
  does. Four in-module regressions added, the three child-index contracts
  proven to fail against the unfixed arms; see
  `receipts/planner_rule_child_access.md`.

- 2026-09-02: aligned `LogicalPlan::extract_fd` child access with Go master
  `a85e0fd5df` (same worktree/branch). The selection, join, and apply FD arms
  previously refused malformed subtrees with empty/partial sets where Go
  indexes `Children()[0]`/`Children()[1]`; the join arm now also answers the
  default join types with the empty set BEFORE any child access, exactly as
  Go's switch does. Five focused regressions (four panic contracts proven to
  fail pre-fix, one ordering pin) added; see
  `receipts/planner_rule_child_access.md`.

- 2026-09-02: aligned the entry boundaries of three `tidb-planner`
  logical-optimization rule bodies with Go master `a85e0fd5df` (independent
  worktree/branch `codex/zcode-parity-sweep`). The union-all-dual rule,
  derive-TopN-from-window, and max/min elimination previously answered
  malformed subtrees with Rust-only `None`/internal-error refusals where the
  Go bodies index `Children()[0]`, `AggFuncs[0]`, `Args[0]`, or
  `Schema().Columns[i]` and panic; the Rust rules now preserve those index
  contracts while keeping every Go early-return guard unchanged. Four focused
  panic-contract regressions fail before the fix and pass after; details and
  gates are in `receipts/planner_rule_child_access.md`.

- 2026-09-02: extended the bounded Rust-only `tidb-planner` child-accessor
  batch against Go master `a85e0fd5df` after the first validation exposed the
  physical sequence-construction boundary. Logical and physical sequence
  schema dispatch now selects the last attached main-query child (while a
  childless physical sequence can still use its stamped construction schema),
  sequence helper/stat inputs panic on impossible empty shapes like Go, logical
  output-name dispatch preserves schema-producer ownership, and physical
  child-request property access direct-indexes like Go. Added focused
  regressions for each boundary; the package inventory and validation details
  remain in `receipts/planner_child_accessors.md`.

- 2026-09-02: completed a bounded Rust-only `tidb-planner` child-accessor
  alignment against Go master `a85e0fd5df`. The Rust base logical and physical
  plans now preserve Go's direct-index panic contract for `SetChild`; logical
  child stats access likewise panics on absent children, and the base join
  accessor panics for non-join operators while join/apply access indexes both
  children. Focused logical/physical regressions cover the invalid-index and
  invalid-shape boundaries, with a pre-fix baseline proving the old logical
  `SetChild` returned `None`. This is a Rust-only bounded change and does not
  claim completion of the larger Go planner packages; details are in
  `receipts/planner_child_accessors.md`.

- 2026-09-02: completed a Rust-only `tidb-codec` alignment follow-up for Go
  master `febee17ec716d86b1e355e5400ef9e4f4f190bad` (hash join v2 TypeNull
  build-key regression). The complete Rust owner inventory covers 52 tracked
  manifest, production, unit/integration test, benchmark, and generated-test
  inputs (14,222 lines; the aggregate build script emits `OUT_DIR/all_tests.rs`;
  no platform-specific variant exists). `serialize_keys` now marks every
  `FieldTypeCode::Null` row as a skipped NULL key even when row-backed input
  carries a non-NULL placeholder datum, preventing collision with an empty
  byte key. The focused source regression fails before the change and passes
  after it. Details are recorded in `receipts/util_codec_audit.md`; the
  complete crate suite, pinned Rust formatting, `make lint`, and diff checks
  are the Ready gates for this batch.

- 2026-09-02: synchronized the bounded `pkg/infoschema/perfschema` Go
  package with fetched Go master `a85e0fd5df`. The complete eight-artifact
  inventory (1,597 counted lines including the two binary profile fixtures)
  remains recorded in `receipts/infoschema_perfschema.md`. Current Go master
  retains the production profile-request logger but has removed the global
  logger-replacement regression and its test-only zap dependencies; the
  package BUILD target and test imports now match that source. The
  failpoint-aware package suite passes in a detached Go-master worktree;
  `make lint`, Rust formatting, and diff checks are the Ready gates, while
  `make bazel_prepare` remains blocked by the unavailable Bazel executable.

- 2026-09-02: completed the Analyze consumer follow-up in the root
  `pkg/executor` boundary against Go master
  `78cac443a4f46c13bfe27eb247b5c80657952547`. The five-file batch propagates
  the Analyze plan ID, restores unsigned full-sampling range handling, wires
  `AnalyzeStoreBatchSize` into request flags, closes partially opened index
  results on error, and adds `TestAnalyzeBuildsRequest` for request shape and
  boundary buckets. Focused failpoint-aware validation passes in the detached
  Go-master worktree; details are in
  `receipts/executor_analyze_store_batch.md`.

- 2026-09-02: restored the Go-master analyze-store-batch contract in one
  dependency-closed source batch. `pkg/sessionctx/vardef` now exposes the
  `tidb_analyze_store_batch_size` name/default/bound, `pkg/sessionctx/variable`
  initializes and validates the session field, and
  `pkg/statistics/handle/util` refreshes the global value in source order while
  forwarding explicit SQL contexts. The complete
  `pkg/statistics/handle/storage` inventory is restored with one-snapshot
  column-distribution reads and statement-local table-size stats that skip
  histogram scans for TABLE_ROWS-only requests. Focused failpoint-aware Go
  regressions pass in the detached Go-master worktree; current-branch Ready
  lint remains gated by the concurrent distsql/execdetails source sync and
  `make bazel_prepare` remains blocked because `bazel` is unavailable. Receipts:
  `sessionctx_vardef_audit.md`, `sessionctx_variable.md`,
  `statistics_handle_util.md`, and `statistics_handle_storage_audit.md`.

- 2026-09-02: completed the bounded `pkg/metrics` Go-package batch against
  current Go master `78cac443a4f46c13bfe27eb247b5c80657952547` as one
  package commit. The complete inventory covers 60 artifacts and 139,824
  lines, including all production/tests, build/ownership metadata,
  alertmanager rules, Jsonnet inputs, generator scripts, and generated Grafana
  dashboards. Restored RUV3 metric/label definitions and registration, removed
  obsolete memory-arbitrator labels, and synchronized the resource-control
  dashboards. The pre-fix regression failed on missing RUV3 symbols; focused
  and tagged full package tests pass. No dependency-closed Rust shared-registry
  owner exists, so that boundary remains explicit in `receipts/metrics.md`.
  Bazel preparation remains blocked by the unavailable local executable.

- 2026-09-02: completed the bounded `pkg/privilege/privileges` Go package
  batch against Go master
  `78cac443a4f46c13bfe27eb247b5c80657952547`. The complete direct inventory
  covers ten upstream artifacts and 7,411 lines, plus one focused branch
  regression file. Restored the OPERATE VIEW cache/load/visibility behavior,
  MySQL USER_ATTRIBUTES row-filter modes, and the SEM-v2 columnar-variable
  privilege regression. Focused failpoint-aware tests pass. The end-to-end
  USER_ATTRIBUTES integration case remains explicitly owned by the missing
  `pkg/executor/infoschema_reader.go` consumer; Bazel preparation is blocked
  by the missing local executable. Details are in
  `receipts/privilege_privileges.md`.

- 2026-09-02: completed the bounded `pkg/executor` USER_ATTRIBUTES consumer
  follow-up against Go master
  `78cac443a4f46c13bfe27eb247b5c80657952547`. The complete root inventory
  (173 direct artifacts, 101,740 lines) had already been read; this batch
  restores the ordinary `infoschema_reader.go` privilege filter handoff and
  the SQL-visible regression owned by the privilege test suite. The pre-fix
  test exposed all rows to an ordinary account and passes after filtering.
  The required Bazel preparation gate remains blocked by the unavailable
  local executable. Details are in
  `receipts/executor_user_attributes.md`.

- 2026-09-02: completed the bounded `pkg/planner/core` Go package storage
  signal batch against Go master
  `78cac443a4f46c13bfe27eb247b5c80657952547`. The complete direct inventory
  covers 116 artifacts and 76,570 lines (106 Go files, BUILD metadata, and
  nine direct fixture books). Restored `READ_FROM_STORAGE` hint marking and
  missing-TiFlash-path marking, including explicit-transaction FOR UPDATE
  handling, with focused plan-builder regressions. Details are in
  `receipts/planner_core_storage_signals.md`; Bazel preparation is
  blocked by the missing local executable.

- 2026-09-02: completed the bounded `pkg/planner/core/casetest/mpp` Go
  package batch against Go master
  `78cac443a4f46c13bfe27eb247b5c80657952547`. The complete direct inventory
  covers seven artifacts and 7,369 lines, including all three Go test files,
  the BUILD target, and the three MPP fixture files. Restored the
  alternative-engine round regression source, mock TiFlash fixtures, recorded
  plan outputs, and BUILD dependencies/shard metadata as one package-level
  batch. No dependency-closed Rust testkit owner exists for this fixture
  package; the remaining cross-runtime MPP boundary is explicit in
  `receipts/planner_core_casetest_mpp_engine_rounds.md`. Focused Go validation,
  lint, Rust formatting, and diff checks are required for Ready; Bazel
  preparation is blocked by the missing local executable.

- 2026-09-02: completed a follow-up `pkg/util/codec` Go-package batch against
  Go master `1c1a334d2be1dce64888b6e1f054462c566b0734`: restored the TypeNull
  pre-allocation `canSkip` call that marks null join keys before hashing, which
  prevents empty NULL keys from colliding with empty byte keys. The focused
  failpoint-aware codec suite passes; `make bazel_prepare` is blocked by the
  unavailable Bazel executable. The focused `pkg/executor/join` regression is
  recorded in its own package receipt.

- 2026-09-02: completed a focused `pkg/executor/join` Go-package batch against
  Go master `1c1a334d2be1dce64888b6e1f054462c566b0734`: inventoried all 47
  direct artifacts (45 Go files, BUILD, and OWNERS), restored the
  `TestAntiSemiJoinTypeNullBuildKey` regression and its BUILD dependency, and
  verified it through the paired codec fix. The nested join test packages and
  Rust distributed join integration remain explicit boundaries. Details are in
  `receipts/executor_join.md`.

- 2026-09-02: completed a bounded `pkg/meta/model` Go-package batch against Go
  master `1c1a334d2be1dce64888b6e1f054462c566b0734`. The complete direct
  inventory covers 23 artifacts and 9,454 lines; restored the v2-only
  `SkipColumnarStorageGate` field and the separate `IndexArg.AutoPreSplit`
  marker, with v1 compatibility and focused JSON/job-argument regressions. The
  focused and full failpoint-aware model suites pass; lint, Rust formatting,
  and diff checks pass, while `make bazel_prepare` is blocked by the missing
  local Bazel executable. Details are in
  `receipts/meta_model_job_args.md`.

- 2026-09-02: completed a bounded `pkg/planner/core/operator/physicalop`
  Go-package batch against Go master `1c1a334d2b`: restored
  `StorageEngineUsage` and `HasSingleScanIndexJoin` with focused physical-tree
  regressions and BUILD metadata. The Rust `tidb-planner` owner now exposes
  equivalent reader-boundary and inner-probe predicates over its closed
  `PhysicalPlan` tree. The failpoint-aware Go tests and lint pass; Rust planner
  compilation is blocked by missing OpenSSL/pkg-config, and
  `make bazel_prepare` is blocked by the missing Bazel executable. Details are
  in `receipts/planner_physicalop_engine_usage.md`.

- 2026-09-02: completed a second bounded `pkg/sessionctx/vardef` Go-package
  batch against Go master `1c1a334d2be1dce64888b6e1f054462c566b0734`:
  restored the process-global plan-replayer file-retention duration, its
  system-variable name and seven-day default, and focused Go/Rust setter-getter
  regressions. Rust keeps Go's signed nanosecond `time.Duration` representation
  in an `AtomicI64`. The package receipt now records the current source hash and
  the remaining SysVar/domain integration boundary. Ready validation passes
  for the Go package, Rust owner/source tests, Rust formatting, lint, and diff
  checks; `make bazel_prepare` is required by the restored Go files and is
  blocked by the missing local Bazel executable.

- 2026-09-02: completed a bounded `pkg/sessionctx/stmtctx` Go-package batch
  against Go master `1c1a334d2b`: restored the three alternative-plan storage
  signals (mixed TiKV/TiFlash, missing TiFlash path, and explicit
  `READ_FROM_STORAGE` hint) with mark/reset behavior and a focused Go
  regression. The dependency-closed Rust `tidb-exec::AlternativePlanSignals`
  owner now carries the same eleven-field state, with source-backed mark and
  reset tests. The full failpoint-aware Go package and focused Rust target
  pass; `make bazel_prepare` is blocked by the missing local executable.
  Details are in `receipts/sessionctx_stmtctx.md`.

- 2026-09-02: completed a bounded `pkg/bindinfo` Go-package batch against
  Go master `1c1a334d2b`: restored `mayHaveSQLBinding` and its parser matrix,
  excluding INSERT/REPLACE `VALUES` and `SET` forms while preserving
  `... SELECT` and EXPLAIN delegation. The Rust `tidb-session` matcher now
  applies the same recursive filter, with a fail-before/pass-after regression
  covering INSERT, REPLACE, and EXPLAIN. The complete 25-artifact inventory
  remains the atomic boundary; the focused Go and Rust tests pass, the broad
  Go package run was killed by existing log/resource pressure (exit 137), and
  Bazel preparation is blocked by the missing local executable. Details are in
  `receipts/bindinfo.md`.

- 2026-09-02: completed a follow-up `pkg/store/copr` Go-package batch against
  Go master `1c1a334d2b`: updated the dependency-closed client-go API-v2 pin,
  retained source-shaped bucket-version cache handling, and added the focused
  StoreBatch child-lock regression proving decoded primary keys are encoded
  exactly once. The complete 20-artifact copr inventory remains the atomic
  boundary; failpoint-wrapped validation passes, while Bazel preparation is
  blocked by the missing local executable. Details are in
  `receipts/store_copr.md`.

- 2026-09-02: completed a follow-up `pkg/distsql` Go-package batch for
  coprocessor request-limiter wait statistics at current `origin/master`
  `1c1a334d2b`. The complete 15-artifact root inventory remains the atomic
  boundary. `selectResult` now preserves response close errors while recording
  limiter wait total/max statistics through close, clone, merge, and string
  rendering; focused Go regression coverage passes. The bounded Rust
  `tidb-distsql` runtime aggregate and source-level regression were added, but
  full cargo validation is blocked by missing local OpenSSL/pkg-config
  dependencies. Details and remaining transport boundaries are in
  `receipts/distsql_audit.md`.

- 2026-09-02: audited the complete Go-master root `pkg/executor` boundary
  before editing: 173 direct artifacts and 101,740 lines, including 171 Go
  production/test files, `BUILD.bazel`, and `OWNERS`, with 1,395
  test/benchmark/fuzz declarations and 2,633 functions. Recursive inspection
  covered 519 artifacts, 69 nested build/ownership files, eight testdata
  fixtures, and the separate executor subpackages. This bounded batch restores
  IndexJoin range-memory tracking and the typed merge-sort coprocessor
  limiter handoff, with focused regressions in the existing package test
  suite. Rust `tidb-executor` has no dependency-closed owner for these Go
  executor consumers, so the remaining root and nested executor differences
  stay explicit boundaries. Details are in
  `receipts/executor_root_distsql_indexjoin.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/store/copr` root
  boundary at `a74cc596996d8a4c940b4d64fca46ac1c6d5c0d7`: 20 direct
  artifacts, 11,165 lines, 177 function declarations, and 61
  test/benchmark declarations, with no package docs, fixtures, generated
  source, or platform variants. The package now carries Go's request/query
  coprocessor limiter callback, cancellation and wait statistics, merged
  StoreBatch response handling, safe pre-dispatch whole-batch rebuild, exact
  fallback accounting, read-pool runtime details, and all current focused
  regressions while retaining the child-lock fix. The nested `copr_test` and
  `metrics` packages remain separate boundaries; its integration tests await
  the owning executor/distsql API migration. Rust has no dependency-closed
  `pkg/store/copr` worker owner, so no speculative Rust facade was added.
  Details are in `receipts/store_copr.md`.

- 2026-09-02: completed the bounded shared-lock-loss rollback audit for the
  complete Go-master `pkg/session` root package at
  `a74cc596996d8a4c940b4d64fca46ac1c6d5c0d7` (behavior introduced by
  `94eb995357f34b7bab4889a82f0405797046447d`). The direct package inventory
  covers 26 artifacts and 20,094 lines, including all production/tests,
  `BUILD.bazel`, and `OWNERS`, with no package docs, fixtures, generated
  source, or platform variants. Go now rolls back valid transactions on the
  shared-lock-lost catalog error while preserving pessimistic deadlock
  behavior, returns the original error, records abort metrics, and has a
  three-case regression. The Rust `tidb-session` owner lacks a
  dependency-closed shared-lock error/statement-abort path, so session
  rollback wiring remains an explicit boundary; typed low-level decoding is
  covered by the separate `pkg/store/driver/txn` batch. Details are in
  `receipts/session_tidb_shared_lock.md`.

- 2026-09-02: audited the complete Go-master `pkg/store/driver/txn`
  boundary at `94eb995357f34b7bab4889a82f0405797046447d`: 12 tracked
  artifacts, 2,208 lines, 114 functions, and eight test declarations, with
  no Go fixtures, generated inputs, platform variants, or package docs. The
  Rust TiKV owner now carries KeyError fields 13/14, preserves typed shared
  lock loss, maps upgrade conflicts to a non-retryable deadlock, and supports
  the source-compatible shared-to-exclusive buffer transition behind the
  existing lock-context admission guard. Focused owner regressions and the
  dependency-closed `tidb-txnkv` checks are passing; the Rust lock-context
  session gate, session rollback, and SQL consumer wiring remain separate
  package boundaries. Details are in
  `receipts/store_driver_txn.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/kv` inventory at
  `94eb995357f34b7bab4889a82f0405797046447d`: 30 tracked artifacts, 5,435
  lines, 288 functions, 36 test/benchmark declarations, and no fixture,
  generated, platform, or package-doc inputs. The new request-scoped
  aggregate/per-store coprocessor limiters and the shared-lock-lost error
  identity are implemented in `tidb-txnkv`, with focused cancellation,
  release, per-store reuse, and error-catalog regressions. This remains a
  package-level boundary while distsql/session/store/copr consumers are
  audited separately. Details are in `receipts/kv.md`.

- 2026-09-02: completed the complete `pkg/kv` Go-package batch in one commit
  after pulling Go master `febee17ec716d86b1e355e5400ef9e4f4f190bad`.
  Restored cancellation-aware aggregate/per-store coprocessor limiters,
  request limiter fields, and the shared-lock-lost error identity; added the
  focused wait/cancel/release/concurrency regressions and updated the BUILD
  shard metadata. The pre-fix limiter tests failed on missing constructors;
  post-fix failpoint-aware focused and full package suites passed. The Rust
  owner and downstream distsql/session consumers remain separate boundaries.

- 2026-09-02: completed the direct `pkg/distsql/context` Go-package batch in
  one commit after pulling Go master `febee17ec716d86b1e355e5400ef9e4f4f190bad`.
  Restored `DistSQLContext.QueryCopStoreLimiter` and extended
  `TestContextDetach` to preserve the statement-scoped limiter. The focused
  pre-fix test failed at compile time on the missing struct field; focused and
  full post-fix package tests passed. The package inventory covered all three
  artifacts and has no fixtures, generated inputs, or platform variants.

- 2026-09-02: completed the bounded `pkg/sessionctx/vardef` Go-package batch
  in one commit after pulling Go master
  `febee17ec716d86b1e355e5400ef9e4f4f190bad`. Restored the query cop-store
  limiter system-variable name and default constant and added an exact-value
  regression. Focused and full vardef tests passed; the broader vardef/runtime
  and variable registry deltas remain explicit package boundaries.

- 2026-09-02: completed the bounded `pkg/sessionctx/variable` Go-package
  batch in one commit after restoring its dependent vardef constants. Added
  `SessionVars.QueryCopStoreLimit` default initialization, the validated
  global/session sysvar and hint registration, and a focused default/update
  regression. The failpoint-aware focused and full variable suites plus lint
  passed; `make bazel_prepare` was attempted and blocked by the unavailable
  local Bazel executable. Remaining variable-system deltas stay separate.

- 2026-09-02: completed the bounded `pkg/util/mock` Go-package batch in one
  commit after restoring its `vardef` and distsql dependencies. Wired positive
  `SessionVars.QueryCopStoreLimit` values into the mock `DistSQLContext`, kept
  zero as disabled, and added a focused propagation regression. Focused and
  full mock tests passed; the required Bazel-prepare gate was attempted and
  blocked by the unavailable local executable.

- 2026-09-02: completed a follow-up `pkg/distsql` Go-package batch to reconnect
  the typed cop-request limiter after the `pkg/kv` API migration. The request
  builder now projects `kv.CoprRequestLimiter`, the query-scoped limiter is
  copied from `DistSQLContext`, and Go-master store-batching option setters are
  restored. The focused request-builder regression passes under failpoint
  enablement; `make bazel_prepare` remains blocked by the unavailable local
  Bazel executable. The executor merge-sort caller is a separate package
  boundary and remains queued for its own migration.

- 2026-09-02: restored the current Go-master `pkg/distsql` consumer delta in
  one Go-package batch at
  `78cac443a4f46c13bfe27eb247b5c80657952547`. The complete 14-artifact
  inventory covers root and context targets, tests, benchmark support, and
  ownership metadata. `selectResult` now preserves close errors, propagates
  read-pool/runtime evidence, validates response-summary coverage, and records
  raw Analyze execution details; focused and full failpoint-aware tests pass.
  The changed `RecordCopStats` API is now wired to this package, while the
  separate `pkg/store/copr` transport and Rust response-owner boundaries remain
  explicit. `make lint` and the required Bazel gate are run before commit;
  Bazel is unavailable locally. Details are in `receipts/distsql_audit.md`.

- 2026-09-02: completed the Go-master `pkg/parser` materialized-view DDL
  syntax delta in one parser-package batch. Before editing, the full root
  parser inventory (33 artifacts, 64,956 lines, generated grammar/keyword
  inputs and outputs, and 150 test/benchmark/fuzz entries) and nested
  `pkg/parser/ast` inventory (36 artifacts, 34,448 lines, visitor generator,
  generated output, and fixture) were read. Rust now owns all six typed DDL
  forms, canonical restore, visitor/label/semantic wiring, parser precedence,
  Go-compatible option/purge diagnostics, and the four missing generated
  lexer keywords. Twelve Go source forms plus duplicate/order/incomplete
  regressions pass. The Go change is parser/AST-only; no dependency-closed
  refresh/log executor exists, so Rust retains an explicit unsupported-DDL
  boundary. Details are in `receipts/parser_materialized_view.md`.

- 2026-09-02: completed the current Go-master `pkg/ddl` root inventory before
  editing: 139 direct artifacts (63 production, 74 tests, `BUILD.bazel`, and
  `OWNERS`), 89,091 Go/BUILD lines, and 1,016 top-level test/benchmark
  declarations. The automatic-index-pre-split delta from
  `c6054025ed4c32ab3672a2a24ea46892714d21ec` is now carried through the Rust
  parser/AST, independent `auto_presplit` job argument, feature/keyword
  catalogs, DDL catalog write, and a pure TopN/histogram quantile planner with
  focused regressions. The branch has no dependency-closed DDL
  `SplittableStore` owner, so PD split execution is recorded as an explicit
  boundary instead of inventing a second region client. Details are in
  `receipts/ddl_auto_presplit_audit.md`; this is one package-level batch, not a
  repository-wide parity claim.

- 2026-09-02: repaired the focused Rust `pkg/ddl` automatic-pre-split test
  helpers after the datatype owner removed its blanket `FieldType::default()`
  implementation. The two existing quantile regressions now construct the
  source-shaped `LongLong` field type explicitly; the `tidb-exec` owner suite
  passes 8 tests and the broader automatic-pre-split source suite passes 3.
  This compile-correctness follow-up changes no production planner behavior;
  details are recorded in `receipts/ddl_auto_presplit_audit.md`.

- 2026-09-02: refreshed and completed the Go-master
  `pkg/statistics/handle/storage` package boundary at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: all 12 production, test, and
  build artifacts (5,051 lines), 29 top-level tests, and the absence of
  fixtures, generated inputs, benchmarks, and platform variants are
  inventoried. The foreground column-distribution read now owns one PD
  timestamp/read-only transaction, metadata→TopN→bucket ordering, v1
  metadata-only behavior, negative-null-count validation, and no cache
  publication on failure. Its focused ordered/atomic regression passes; DDL
  auto-presplit consumption is tracked under the separate `pkg/ddl` boundary.
  Details are in `receipts/statistics_handle_storage_audit.md`.

- 2026-09-02: restored the direct multipart-write limit guard in the complete
  `pkg/objstore/s3store` Go-master inventory. A writer now returns the shared
  `ErrExceedMaxUploadParts` sentinel before issuing part 10,001; the focused
  regression panicked before the fix and passes afterward. The full
  failpoint-wrapped package suite and scoped lint pass; Ready repository lint
  and Bazel preparation retain the documented baseline/tooling blockers. The
  remaining GCS/Tencent/Aliyun and uploader deltas stay an explicit boundary.
  Details are in `receipts/objstore_s3store.md`.

- 2026-09-02: restored the shared multipart-upload contract in the complete
  `pkg/objstore/storeapi` inventory (three artifacts, 402 lines, with no
  generated/platform/fixture inputs). `MaxUploadParts = 10000` and the exact
  `ErrExceedMaxUploadParts` sentinel now match Go master, with a focused
  regression that failed before the symbols existed and passes afterward. The
  legacy `ReadSeekCloser` alias remains until its `dumpformat` callers migrate
  as one package. Go tests and scoped lint pass; the Ready repository lint
  profile reports 986 pre-existing var-naming diagnostics outside this package,
  and Bazel preparation is blocked only by the unavailable executable. Details are in
  `receipts/objstore_storeapi.md`.

- 2026-09-02: restored the two self-contained Go-master runtime deltas in the
  complete `pkg/types` inventory (61 artifacts, including the nested
  `parser_driver` support package): checked `uint64` vector-size arithmetic
  with an overflow regression, and the public `ExplainFormatRU` constant and
  ordered validator entry with a value/position regression. The parser-driver
  in-place visitor additions remain an explicit dependency boundary. Root and
  support-package Go tests pass; `make bazel_prepare` was attempted and is
  blocked only by the unavailable local `bazel` executable. Details are in
  `receipts/types_explain_format_audit.md`.

- 2026-09-02: refreshed the complete Go-master
  `pkg/statistics/handle/internal` boundary at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: two artifacts and 68 lines,
  with no source tests, fixtures, generated/platform variants, or build
  extras. The single Rust helper remains an actual-table equality owner for
  Go's count, histogram text, CMSketch, TopN, and existence-map semantics;
  current/detached probes, owner checks, and Ready gates pass. Details are in
  `receipts/statistics_handle_internal.md` and
  `docs/operations/statistics-handle-internal-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master
  `pkg/statistics/handle/logutil` boundary at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: two artifacts and 67 lines,
  with no source tests, fixtures, generated/platform variants, or build
  extras. The single Rust owner still composes the shared background,
  error-verbose, and sampled logger factories with exact category, lifetime,
  admission, and five-/ten-minute window behavior. Current/detached probes,
  owner checks, and Ready gates pass. Details are in
  `receipts/statistics_handle_logutil.md` and
  `docs/operations/statistics-handle-logutil-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master
  `pkg/statistics/handle/util/test` support boundary at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: two artifacts and 49 lines,
  with no package tests, fixtures, generated/platform variants, or build
  extras. The distinct typed-context matcher owner remains aligned with the
  strict Go assertion, TiKV request-source extraction, and exact diagnostic
  text; current/detached probes, owner checks, and Ready gates pass. Details
  are in `receipts/statistics_handle_util_test.md` and
  `docs/operations/statistics-handle-util-test-audit-execplan.md`.

- 2026-09-02: corrected the complete Go-master
  `pkg/statistics/handle/util` boundary at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: seven root artifacts and 927
  lines, plus a separately owned 49-line nested package. Rust now refreshes
  analyze-store batch size in the exact source order, applies the timeout
  failpoint at the explicit-context row boundary, and exposes the missing
  caller-context option path; the Rust-only merge-concurrency reset was
  removed. The focused tests failed before and pass after the fix, and both
  Go package variants, 21 Rust owner tests, 258 statistics consumer tests,
  the server compile, and Ready gates pass. Details are in
  `receipts/statistics_handle_util.md` and
  `docs/operations/statistics-handle-util-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master
  `pkg/util/servermemorylimit` inventory at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: three artifacts and 375 lines,
  including the failpoint-backed controller and history test. The Rust owner
  retained live memory-limit behavior and exact history rows; removed its one
  Rust-only constructor `#[must_use]` annotation and added a regression that
  failed before the edit with one lint error and passes afterward. The
  failpoint-wrapped current Go test, detached latest-master Go test, focused
  Rust tests, Ready formatting, pinned lint, and diff hygiene pass. Details
  are in `receipts/util_servermemorylimit.md` and
  `docs/operations/util-servermemorylimit-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/util/sem/compat`
  inventory at `c6054025ed4c32ab3672a2a24ea46892714d21ec`: five artifacts and
  522 lines, including the SEM integration fixture and BUILD metadata. The
  Rust owner retained its six active-policy wrappers and source tests; removed
  six Rust-only `#[must_use]` annotations and added a regression that failed
  before the edit with six lint errors and passes afterward. The
  failpoint-wrapped current Go suite, all six Rust tests, Ready formatting,
  pinned lint, and diff hygiene pass. Details are in
  `receipts/util_sem_compat.md` and
  `docs/operations/util-sem-compat-audit-execplan.md`; detached latest-master
  full Go execution remains unverified because it did not terminate locally.

- 2026-09-02: re-audited the complete root `pkg/util/sem` package at current
  Go master `c6054025ed4c32ab3672a2a24ea46892714d21ec`: four artifacts, five
  source tests, and no hidden variants or fixtures. The existing native SEM
  owner and cross-crate session-default integration remain aligned; current
  and detached Go suites, five Rust predicate tests, the focused session
  regression, Ready formatting, and the recorded lint gates pass. Details are
  in `receipts/util_sem.md` and `docs/operations/sem-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/util/disk` inventory at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: five artifacts and 283 lines,
  with directory locking, cleanup, tracker aliases, and the concurrent source
  test covered. Removed the two Rust-only tracker-constructor `#[must_use]`
  annotations and added a regression that failed before the edit with two
  lint errors and passes afterward. Current and detached latest-master Go
  tests, focused Rust regressions, Ready formatting, pinned lint, and diff
  hygiene pass. Details are in `receipts/util_disk.md` and
  `docs/operations/util-disk-audit-execplan.md`.

- 2026-09-02: re-audited the complete root `pkg/util/traceevent` boundary at
  current Go master `c6054025ed4c32ab3672a2a24ea46892714d21ec`: seven
  artifacts and 2,359 lines, including every adapter/flight-recorder/event
  production and test function, both benchmarks, and BUILD metadata. The live
  `tidb-util` owner, vendored client hooks, structured fields/context, and
  source-derived tests remain aligned; current/detached Go suites and twelve
  focused Rust tests pass. Details are in `receipts/util_traceevent.md` and
  `docs/operations/util-traceevent-audit-execplan.md`.

- 2026-09-02: re-audited the complete root `pkg/util/tracing` boundary at
  current Go master `c6054025ed4c32ab3672a2a24ea46892714d21ec`: six artifacts
  and 715 lines, including the context/span production surface, package test
  harness, six source tests, four benchmarks, and BUILD metadata. The live
  `tidb-util` owner retains shared span state, open phases, CE record identity,
  source-derived tests, and benchmark carriers; current/detached Go suites,
  seven focused Rust tests, and benchmark compilation pass. Details are in
  `receipts/util_tracing.md` and
  `docs/operations/util-tracing-audit-execplan.md`.

- 2026-09-02: re-audited the complete `pkg/session/syssession` boundary at
  current Go master `c6054025ed4c32ab3672a2a24ea46892714d21ec`: eight
  artifacts and 3,130 lines, including the `!codes` support variant, full
  unit/integration tests, package harness, and flaky 21-shard BUILD target.
  The single `tidb-syssession` owner retains the complete ownership/pool
  state machine and timer integration; active and detached failpoint-wrapped
  Go suites pass after the documented flaky retry, with fourteen Rust owner
  tests and eight timer integration tests passing. Details are in
  `receipts/session_syssession.md` and
  `docs/operations/session-syssession-audit-execplan.md`.

- 2026-09-02: refreshed the complete `pkg/util/sqlexec/mock` boundary at
  current Go master `c6054025ed4c32ab3672a2a24ea46892714d21ec`: three
  artifacts and 152 lines covering BUILD metadata, the exact context key, and
  the complete generated three-method restricted executor mock. The
  dependency-closed `tidb-sqlexec-mock` owner remains aligned; both Go package
  probes and three focused Rust tests pass. Details are in
  `receipts/util_sqlexec_mock.md` and
  `docs/operations/util-sqlexec-mock-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/util/stringutil`
  inventory at `c6054025ed4c32ab3672a2a24ea46892714d21ec`: four artifacts and
  927 lines, including all source tests, benchmarks, the goleak harness, and
  BUILD metadata. The Rust owner already forwards Go's explicit LIKE escape
  byte; removed its 15 Rust-only `#[must_use]` annotations and added a
  discardable-return regression that failed before the edit with 15 lint
  errors and passes afterward. Current and detached latest-master Go tests,
  all nine Rust tests, Ready formatting, pinned lint, and diff hygiene pass.
  Details are in `receipts/util_stringutil.md` and
  `docs/operations/util-stringutil-audit-execplan.md`.

- 2026-09-02: completed the Go-master `pkg/keyspace` package audit at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: read all five artifacts and
  404 lines, confirmed no generated/platform/fixture/nested artifacts or Go
  source delta, and retained the dependency-closed `tidb-util::keyspace`
  owner. Removed its 11 Rust-only `#[must_use]` annotations and added the
  discardable-return regression, which failed before the fix with 11 lint
  errors and passes afterward. Current and detached latest-master Go tests,
  all seven Rust keyspace tests, Ready formatting, pinned lint, and diff
  hygiene pass. Details are in `receipts/keyspace_audit.md` and
  `docs/operations/keyspace-audit-execplan.md`.

- 2026-09-02: fixed the remaining Rust-only return diagnostics in the
  complete Go-master `pkg/util/checksum` owner. The package has four tracked
  artifacts and 786 lines at `c6054025ed4c32ab3672a2a24ea46892714d21ec`,
  including ten source tests, with no fixtures, generated/platform Go
  variants, benchmarks, fuzz targets, or nested packages. The prior
  Rust-only `Writer::underlying` accessor remains removed; six explicit
  `#[must_use]` annotations were also removed. Added
  `TestReturnValuesMayBeIgnoredLikeGo`, which failed before the fix with six
  lint errors and passes afterward. Current and exact detached Go tests, all
  eleven Rust owner tests, encrypted spill consumer checks, formatting, diff
  checks, and the pinned detached `make lint` gate pass. Updated
  `receipts/util_checksum.md` and added the Ready plan at
  `docs/operations/util-checksum-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/util/cpu` inventory at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: four tracked artifacts, 308
  lines, the usage observer/CPU-count production surface, two source tests,
  and the common goleak harness, with no fixtures, generated/platform Go
  variants, benchmarks, or nested packages. The Rust Unix/Windows/fallback
  owner variants, cgroup preflight, EMA observer, metric, failpoint, scheduler,
  and startup consumers preserve source behavior without Rust-only policy.
  Current and exact detached Go tests used the failpoint wrapper; both Rust
  source tests, owner/scheduler/server checks, formatting, and diff checks pass.
  Updated `receipts/util_cpu.md` and added the Ready documentation-only plan at
  `docs/operations/util-cpu-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/util/errmsg` inventory at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: three tracked artifacts, 288
  lines, one production function plus helper, five source tests, and no
  fixtures, generated/platform variants, benchmarks, fuzz targets, or nested
  packages. The Rust error/config owners and ordinary server packet writer
  preserve nil handling, prepared regex ordering, first-match punctuation,
  concurrent publication, and raw packet bytes without Rust-only behavior.
  Current and exact detached Go tests, all five Rust source tests, owner/server
  checks, formatting, and diff checks pass. Updated
  `receipts/util_errmsg.md` and added the Ready documentation-only plan at
  `docs/operations/util-errmsg-audit-execplan.md`.

- 2026-09-02: fixed the Rust-only `#[must_use]` diagnostics in the complete
  Go-master `pkg/util/engine` owner. The package has three tracked artifacts
  and 253 lines at `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: three
  production classifiers and two five-case HTTP source matrices, with no
  fixtures, generated/platform variants, benchmarks, fuzz targets, or nested
  packages. Added `TestReturnValuesMayBeIgnoredLikeGo`, which failed before
  the fix with three `unused_must_use` errors and passes after removing the
  annotations. Current and exact detached Go tests, focused Rust tests,
  package checking, formatting, and diff checks pass. Updated
  `receipts/util_engine.md` and added the Ready plan at
  `docs/operations/util-engine-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/util/texttree` inventory at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: four tracked artifacts, 174
  lines, five tree constants, two production functions, TestMain, and two
  source tests, with no fixtures, generated/platform variants, benchmarks, or
  nested packages. The byte-preserving Rust owner plus plancodec/planner
  consumers retain Go rune and arbitrary-byte semantics without Rust-only
  behavior. Current and exact detached Go tests, both Rust owner tests, planner
  consumer check, formatting, and diff checks pass. Updated
  `receipts/util_texttree.md` and added the Ready documentation-only plan at
  `docs/operations/util-texttree-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/util/paging` inventory at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: four tracked artifacts, 162
  lines, six constants, two production functions, TestMain, and two source
  tests, with no fixtures, generated/platform variants, benchmarks, or nested
  packages. The Rust owner and DistSQL default consumer preserve wrapping
  growth and seek-count formulas without Rust-only behavior. Current and exact
  detached Go tests, both Rust owner tests, the consumer default-authority test,
  formatting, and diff checks pass. Updated `receipts/util_paging.md` and
  added the Ready documentation-only plan at
  `docs/operations/util-paging-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/util/vitess` inventory at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: four tracked artifacts, 154
  lines, one production function, five source vectors, TestMain, and no
  fixtures, generated/platform variants, benchmarks, or nested packages. The
  existing Rust DES owner and expression consumer preserve byte order, key,
  block width, and nil-error semantics without Rust-only behavior. Current and
  exact detached Go tests, focused Rust vector test, consumer check,
  formatting, and diff checks pass. Updated `receipts/util_vitess.md` and
  added the Ready documentation-only plan at
  `docs/operations/util-vitess-audit-execplan.md`.

- 2026-09-02: fixed the Rust-only `must_use` diagnostics in the complete
  Go-master `pkg/util/fastrand` owner. The Go package remains five artifacts and
  227 lines at `c6054025ed4c32ab3672a2a24ea46892714d21ec`, including one source
  test and four benchmarks; its 64-bit/32-bit runtime algorithms and bounded
  reductions remain aligned. Added a deny-lint regression proving all four
  public return values may be ignored like Go; the pre-fix test failed with
  four lint errors and the post-fix suite passes. Current and exact detached Go
  tests, focused Rust tests, all-target/benchmark checks, formatting, diff
  checks, and the pinned detached `make lint` gate pass. Updated
  `receipts/util_fastrand.md` and added the Ready plan at
  `docs/operations/util-fastrand-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/util/backoff` inventory at
      `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: three tracked artifacts, 113
  lines, the `Backoffer`/`Exponential` declarations, one source vector test,
  and no fixtures, generated/platform variants, benchmarks, fuzz targets, or
  nested packages. The existing Rust owner preserves signed duration
  arithmetic, reset-on-zero, multiplier/cap behavior, and the source test
  without Rust-only behavior. Current and exact detached Go tests, the
  focused Rust test, all-target check, formatting, and diff checks pass.
      Updated `receipts/util_backoff.md` and added the Ready documentation-only
      plan at `docs/operations/util-backoff-audit-execplan.md`.
- 2026-09-02: re-audited the complete Go-master `pkg/util/backoff` inventory
      at current authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`; all
      three artifacts and 113 lines remain unchanged. Re-read the production,
      test, and Bazel files and confirmed the existing Rust owner and source
      vector still preserve signed-duration arithmetic, reset-on-zero, and
      multiplier/cap behavior. Details are in `receipts/util_backoff.md` and
      `docs/operations/util-backoff-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/util/zeropool` inventory
  at `c6054025ed4c32ab3672a2a24ea46892714d21ec`: three tracked artifacts,
  281 lines, three production methods, one four-subtest `TestPool`, and four
  benchmarks, with no fixtures, generated/platform variants, or nested
  packages. The existing Rust owner and benchmark translations preserve the
  zero-value/factory, concurrent get/put, move-out, no-copy, test, and
  benchmark contracts without Rust-only behavior. Current and exact detached
  Go tests, focused Rust test, all-target check, formatting, and diff checks
  pass. Updated `receipts/util_zeropool.md` and added the Ready
  documentation-only plan at
  `docs/operations/util-zeropool-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/util/watcher` inventory
  at `c6054025ed4c32ab3672a2a24ea46892714d21ec`: four tracked artifacts,
  605 lines, thirteen production functions/methods, the `TestWatcher` suite,
  and no fixtures, generated files, platform variants, or nested packages.
  The existing Rust owner preserves Go's polling lifecycle, event priority,
  metadata, identity, and source event sequence without Rust-only behavior.
  Current and exact detached Go tests, the focused Rust test, formatting, and
  diff checks pass. Updated `receipts/util_watcher.md` and added the Ready
  documentation-only plan at
  `docs/operations/util-watcher-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/util/size` inventory at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: two tracked artifacts, 86
  lines, five binary units, fifteen ABI constants, and no tests, fixtures,
  generated or platform variants, or nested packages. Corrected the prior
  receipt's stale blob hashes and line count. The existing `tidb-util::size`
  owner preserves Go ABI header values and architecture-width constants with no
  Rust-only behavior or missing Go behavior. Current and exact detached Go
  checks, Rust owner check, formatting, and diff checks pass. Updated
  `receipts/util_size.md` and added the Ready documentation-only plan at
  `docs/operations/util-size-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/util/tikvutil` inventory
  at `c6054025ed4c32ab3672a2a24ea46892714d21ec`: two tracked artifacts, 31
  lines, one exported atomic setting, and no tests, fixtures, generated or
  platform variants, or nested packages. Traced all Go config, GLOBAL sysvar,
  and upgrade consumers and confirmed the existing Rust atomic/config/session
  owners preserve default 128, signed width, SeqCst ordering, and validation
  semantics without Rust-only behavior. Current and exact detached Go checks,
  Rust owner/consumer checks, formatting, and diff checks pass. Updated
  `receipts/util_tikvutil.md` and added the Ready documentation-only plan at
  `docs/operations/util-tikvutil-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/util/nocopy` inventory at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: two tracked artifacts, 32
  lines, one marker type, two methods, and no tests, fixtures, generated or
  platform variants, or nested packages. The existing
  `tidb-util::nocopy::NoCopy` remains a zero-sized non-`Copy`/non-`Clone`
  marker with only Go's no-op lock methods; no Rust-only behavior or missing
  Go behavior was found. Current and exact detached Go checks, Rust owner
  check, formatting, and diff checks pass. Updated
  `receipts/util_nocopy.md` and added the Ready documentation-only plan at
  `docs/operations/util-nocopy-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/util/slice` inventory at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: four tracked artifacts, 149
  lines, three production functions, `TestMain`, and one four-row source test;
  no fixtures, generated files, platform variants, or nested packages. The
  existing `tidb-util::slice` owner remains behaviorally aligned for empty
  truth/short-circuiting, signed decimal conversion, nil-versus-empty cloning,
  and the source test identity. Current and exact detached Go tests, the Rust
  source test and sole consumer check, formatting, and diff checks pass. The
  refreshed receipt and Ready documentation-only plan are in
  `receipts/util_slice.md` and `docs/operations/util-slice-audit-execplan.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/util/channel` inventory
  (two artifacts, 30 lines, one generic production function, and no source
  tests, fixtures, generated files, or platform variants) at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`. The existing borrowed
  `tidb-util::channel::clear` remains behaviorally equivalent to Go's blocking
  channel-range cleanup, with no Rust-only policy or missing behavior found.
  Revalidated the exact detached and current Go package checks plus all 523
  `tidb-util` library tests, refreshed both channel receipts, and recorded the
  Ready documentation-only gate in `docs/operations/channel-audit-execplan.md`.

- 2026-09-01: audited the complete Go-master `pkg/sessionctx` root package
  before editing: four tracked artifacts and 319 lines containing six public
  interface contracts, one snapshot-read timestamp helper, three context-key
  constants, one regression test, TestMain, and the race/flaky BUILD target.
  Rust preserves the context-key value/label behavior and selected cache,
  transaction, cursor, advisory-lock, and timestamp leaves, but has no
  dependency-closed `Context` composition or storage-oracle validation owner.
  No Rust-only behavior or safe standalone implementation was found; the
  exact Go-master suite passed in 0.488s and the boundary is recorded in
  `receipts/sessionctx_root.md`.

- 2026-09-01: audited the complete Go-master `pkg/sessionctx/sessionstates`
  package before editing: five tracked artifacts and 2,578 lines covering the
  migratable state schema, token timing, certificate rotation,
  RSA/ECDSA/Ed25519 signing and validation, eighteen tests, binary-protocol
  helpers, failpoints, and the eighteen-shard flaky BUILD target. Rust has
  selected session-state carriers and a token-timing owner, but no
  dependency-closed JSON restore, migration, TLS/signature, prepared-protocol,
  or authentication owner. No Rust-only behavior or safe standalone
  implementation was found; the complete Go-master failpoint suite passed in
  29.543s and the boundary is recorded in
  `receipts/sessionctx_sessionstates.md`.

- 2026-09-01: audited the complete Go-master `pkg/sessionctx/sysproctrack`
  package before editing: two tracked artifacts and 48 lines containing the
  public `TrackProc`/`Tracker` interfaces and BUILD dependency closure. The
  package has no tests, fixtures, generated/platform variants, or build
  inputs and is unchanged from the pinned source. Rust preserves the callback
  seam through `tidb-sqlexec` and server lifecycle guards; concrete process-map
  ownership remains outside this package. Recorded the explicit boundary in
  `receipts/sessionctx_sysproctrack.md`.

- 2026-09-01: audited the complete Go-master `pkg/sessionctx/slowlogrule`
  package before editing: two tracked artifacts and 73 lines, covering the
  public BUILD target and all condition/rule/session/global metadata plus its
  constructor. The package has no tests, fixtures, generated/platform
  variants, or build inputs and is unchanged from the pinned source. Rust's
  `tidb-exec::slow_log_rules` and `slow_log_parse` preserve the data model and
  parser contracts; session-variable evaluator wiring remains a larger
  boundary. Recorded the explicit boundary in
  `receipts/sessionctx_slowlogrule.md`.

- 2026-09-01: audited the complete Go-master `pkg/sessionctx/stmtctx`
  package before editing: four tracked artifacts and 2,416 lines, including
  129 production functions, 17 statement-context tests, one benchmark,
  TestMain, and the 17-shard flaky BUILD target. Rust has executable
  executor/session/exec owners for selected flags, warnings, caches, stale TSO,
  status, and statistics, but no dependency-closed owner for the full
  cross-cutting `StatementContext` contract and its TestKit/Domain test
  surface. No Rust-only behavior or safe standalone implementation was found;
  the exact Go-master failpoint suite passed in 2.811s and the boundary is
  recorded in `receipts/sessionctx_stmtctx.md`.

- 2026-09-01: audited all 31 Go-master `pkg/sessionctx/variable` artifacts
  (18,433 lines: 24 root artifacts plus seven nested test/slow-log artifacts)
  before editing, including every production/test/support file, BUILD/OWNERS
  metadata, and the absence of fixtures or generated/platform variants. Rust
  now owns the seven embedding variables (endpoint normalization, process-wide
  key/base storage, masking, generation, and live publication) and registers
  six current Go-master additions with exact metadata. Source-derived tests
  cover all provider keys, endpoint allowlist cases, version idempotence, and
  transaction-file validation. The exact Go-master failpoint suite, focused
  Rust tests, formatting, `make lint`, and diff checks pass. Recorded the
  complete inventory and explicit remaining boundaries in
  `receipts/sessionctx_variable.md` and the living implementation plan in
  `rust/docs/operations/sessionctx-variable-audit-execplan.md`.

- 2026-09-01: audited the separate nested
  `pkg/sessionctx/variable/tests` package: four Go-master artifacts and 1,904
  lines, including its 47-shard BUILD target, TestMain/goleak harness, 18
  session tests plus helper, and 29 variable tests. The exact failpoint suite
  reproduced the asynchronous TestHookContext panic and the
  `tidb_auto_analyze_concurrency` default dependency assertion. Removed six
  redundant empty vardef carriers now covered by executable `tidb-session` or
  `tidb-exec` owner tests; all remaining cross-crate integration gaps stay
  explicit. Complete inventory and Ready evidence are recorded in
  `receipts/sessionctx_variable_tests.md` and
  `rust/docs/operations/sessionctx-variable-tests-audit-execplan.md`.

- 2026-09-01: audited the separate nested
  `pkg/sessionctx/variable/tests/slowlog` package: three artifacts and 766
  lines covering the ten-shard BUILD target, goleak harness, and all ten
  field-accessor, matching, and parser tests. The exact Go-master failpoint
  suite passed; Rust's executable `slow_log_parse`, `slow_log_match`, and
  `slow_log_threshold` leaves cover parser/grouping/precedence contracts, while
  live accessor matching remains dependency-blocked on SessionVars,
  StmtContext, and execdetails owners. Complete inventory and Ready evidence
  are recorded in `receipts/sessionctx_variable_tests_slowlog.md` and
  `rust/docs/operations/sessionctx-variable-tests-slowlog-audit-execplan.md`.

- 2026-09-01: audited the separate nested `pkg/sessionctx/variable/tests`
  package: four artifacts and 1,904 lines covering the 47-shard BUILD target,
  goleak harness, 18 session tests, and 29 registry/validation tests. The
  focused Go-master failpoint subset passed; the full package attempt retained
  an existing `TestHookContext` assertion panic during optimizer bootstrap.
  Rust's session sysvar and executor slow-log leaves cover the
  dependency-closed portions; TestKit/Domain/session mutation boundaries stay
  explicit. Complete inventory and Ready evidence are recorded in
  `receipts/sessionctx_variable_tests.md` and
  `rust/docs/operations/sessionctx-variable-tests-audit-execplan.md`.

- 2026-09-01: audited the three-artifact Go-master `pkg/dumpformat` root
  package (46 lines: BUILD, OWNERS, and `kind.go`'s `FieldKind` enum). The
  root has no tests, fixtures, generated/platform variants, or writer call
  sites in Rust; no speculative enum owner was added. Nested CSV, Parquet,
  parser-definition, SQL, and test-utils packages remain separate claims.
  The Go compile, Rust format, lint, and diff gates are recorded in
  `receipts/dumpformat.md` and `rust/docs/operations/dumpformat-audit-execplan.md`.

- 2026-09-01: audited the complete two-artifact Go-master
  `pkg/lightning/backend/encode` package: 107 lines containing only the BUILD
  target and the encoding backend's six exported configuration/interface
  contracts. There are no tests, fixtures, generated/platform variants,
  benchmarks, or executable function bodies. Rust has adjacent Lightning
  modules but no dependency-closed encoder owner or call site, so no Rust-only
  behavior was removed and no speculative facade was added. The focused Go
  package check passed; the explicit boundary is recorded in
  `receipts/lightning_backend_encode.md` and
  `rust/docs/operations/lightning-backend-encode-audit-execplan.md`.

- 2026-09-01: audited the complete 13-artifact Go-master
  `pkg/lightning/backend/kv` package: 3,150 lines, 110 production
  declarations, 22 tests, one benchmark, and a 27-shard flaky BUILD target.
  The inventory covers pooled Lightning sessions, table KV encoding/decoding,
  generated columns, auto-ID conversion, checksums, row grouping, and every
  test/support artifact. Go master adds five new-collation context/test deltas;
  both current-branch and detached exact-master suites pass. Rust has only
  adjacent generic tablecodec/transaction-buffer utilities and no
  dependency-closed owner, so no Rust-only behavior was removed and no
  speculative facade was added. The explicit boundary is recorded in
  `receipts/lightning_backend_kv.md` and
  `rust/docs/operations/lightning-backend-kv-audit-execplan.md`.

- 2026-09-01: audited the complete three-artifact Go-master
  `pkg/lightning/backend/tidb` package: 2,227 lines, 42 production
  declarations, 17 functional tests, two failpoint branches, and a 17-shard
  flaky BUILD target. The inventory covers SQL literal encoding, metadata
  discovery, TiDB 4.x auto-ID compatibility, retries, conflict/error-manager
  recording, prepared statements, batching, and all SQL-mock fixtures. The
  exact Go-master failpoint suite passes with cleanup. Rust has no
  dependency-closed SQL/Lightning backend owner, so no Rust-only behavior was
  removed and no speculative facade was added. The explicit boundary is
  recorded in `receipts/lightning_backend_tidb.md` and
  `rust/docs/operations/lightning-backend-tidb-audit-execplan.md`.

- 2026-09-01: audited the complete three-artifact Go-master
  `pkg/lightning/backend` package: 846 lines, 18 production declarations,
  14 lifecycle tests, one engine-count failpoint, and a 14-shard flaky BUILD
  target. The inventory covers deterministic engine UUIDs, open/close and
  unsafe-close sequencing, metric accounting, import retries, duplicate
  handling, cleanup, and writer interfaces. Current and detached exact-master
  failpoint suites pass with cleanup. Rust has no dependency-closed engine
  manager or writer owner, so no Rust-only behavior was removed and no
  speculative wrapper was added. The explicit boundary is recorded in
  `receipts/lightning_backend.md` and
  `rust/docs/operations/lightning-backend-audit-execplan.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/dxf/importinto/conflictrows` package: four tracked artifacts and 872
  lines covering its BUILD target, unique task/subtask path construction,
  seven-day success retention policy, task metadata decisions, bounded
  cleanup batches and diagnostic samples, object-store deletion/retry
  behavior, and all four top-level tests with 38 leaf cases. The exact
  Go-master package suite passed. Rust owns adjacent DXF task-step vocabulary
  but has no dependency-closed importer/object-store/task-cleanup execution
  path, so no detached policy was added. Recorded the explicit boundary in
  `receipts/dxf_importinto_conflictrows.md` and
  `rust/docs/operations/dxf-importinto-conflictrows-audit-execplan.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/dxf/importinto/taskkey` package: two tracked artifacts and 57 lines
  containing the BUILD target and all three classic/next-generation
  ImportInto task-key constructors. The package has no tests, fixtures,
  generated/platform variants, or owner metadata. Rust preserves adjacent DXF
  task-type/step labels and task-key table columns but has no
  dependency-closed mode/configuration/key-consumer owner, so no speculative
  formatter was added. Recorded the explicit boundary in
  `receipts/dxf_importinto_taskkey.md` and
  `rust/docs/operations/dxf-importinto-taskkey-audit-execplan.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/dxf/importinto/jobhistory` package: three tracked artifacts and 408
  lines covering its BUILD target, history-table SQL and JSON aggregation,
  mode-sensitive task-key lookup, byte/rate/duration formatters, and the one
  end-to-end test with large task IDs and missing-job coverage. The exact
  failpoint-enabled Go-master suite and compile probe passed. Rust has only
  adjacent history-table definitions and DXF labels, not a dependency-closed
  ImportInto history API; no speculative facade was added. Recorded the
  boundary and validation evidence
  in `receipts/dxf_importinto_jobhistory.md` and
  `rust/docs/operations/dxf-importinto-jobhistory-audit-execplan.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/dxf/importinto/conflictedkv` package: 11 tracked artifacts and 2,667
  lines covering its package contract, BUILD target, duplicate-KV collection,
  checksum and conflict-row file limits, data/index re-encoding, keyspace
  codecs, snapshot traffic, transactional deletion, retry/backoff, filters,
  failpoints, and all nine top-level tests with nested cases. The exact
  failpoint-enabled Go-master suite passed. Rust owns adjacent DXF step labels
  and generic DML conflict logic but has no dependency-closed ImportInto
  collect/resolve pipeline, so no speculative facade was added. Recorded the
  explicit boundary in `receipts/dxf_importinto_conflictedkv.md` and
  `rust/docs/operations/dxf-importinto-conflictedkv-audit-execplan.md`.

- 2026-09-01: audited the complete ten-artifact Go-master
  `pkg/lightning/config` package: 4,005 lines, 66 production declarations,
  52 tests, one CPU failpoint, and a 50-shard flaky BUILD target. The inventory
  covers the full Lightning schema/default and endpoint/TLS adjustment,
  checkpoint and duplicate policy, TOML/flag loading, redaction, task queue,
  byte-size codecs, all tests, and ownership/build metadata. A focused
  `TestRemoveAllowAllFiles` regression now checks parsed DSN semantics instead
  of dependency-sensitive query ordering. Current and detached exact-master
  failpoint suites pass with cleanup. Rust's `tidb-config` ByteSize owner maps
  to the separate `pkg/config/configtypes` package; no dependency-closed
  Lightning config owner or consumer exists, so no Rust-only behavior was
  removed and no speculative facade was added. Recorded the explicit boundary
  in `receipts/lightning_config.md` and
  `rust/docs/operations/lightning-config-audit-execplan.md`.

- 2026-09-02: audited the complete 24-artifact Go-master
  `pkg/lightning/common` package: 3,875 lines, 94 production declarations,
  31 test functions, three benchmarks, two logical failpoints, Unix/Windows
  storage variants, and a 30-shard flaky BUILD target. The inventory covers
  allocator compatibility, gRPC pooling, duplicate detection, error and retry
  policy, key adapters, pause gates, TLS/HTTP/PD/TiKV conversion, platform
  storage accounting, SQL/index helpers, row-count safety, and every test and
  benchmark. The current and detached exact-master failpoint suites pass after
  a clean binding-generation rerun. Rust has no dependency-closed common owner
  or consumer, so no Rust-only behavior was removed and no speculative utility
  facade was added. Recorded the explicit boundary in
  `receipts/lightning_common.md` and
  `rust/docs/operations/lightning-common-audit-execplan.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/dxf/importinto/mock` package: two tracked artifacts and 74 lines,
  consisting of the public Bazel target and the complete MockGen
  `MiniTaskExecutor` recorder. It has no tests, fixtures, platform variants,
  or generator inputs beyond the generated source. Rust's existing test mocks
  implement unrelated traits and has no dependency-closed ImportInto writer /
  collector seam, so no speculative Rust mock or Rust-only behavior was
  introduced. Recorded the explicit generated-support boundary in
  `receipts/dxf_importinto_mock.md` and
  `rust/docs/operations/dxf-importinto-mock-audit-execplan.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/dxf/framework/dxfutil` package: three tracked artifacts and 314 lines,
  covering cross-keyspace runtime acquisition/release, store and session
  keyspace validation, stable DXF holder IDs, and all acquire/error/mismatch
  tests. No fixtures, generated/platform variants, benchmarks, fuzz targets,
  or generator inputs exist. Rust's generic `tidb-dxf` task/resource model has
  no SQL-server runtime or keyspace session-pool owner, so no speculative
  facade or Rust-only behavior was added. Recorded the explicit Go-only
  boundary in `receipts/dxf_framework_dxfutil.md` and
  `rust/docs/operations/dxf-framework-dxfutil-audit-execplan.md`.

- 2026-09-02: audited the complete Go-master
  `pkg/dxf/framework/dxfmetric` package: three tracked artifacts and 296
  lines, covering atomic task/subtask snapshots, Prometheus descriptors,
  status aggregation, pending/running duration gauges, DXF event vectors,
  UUID test labels, and registration. It has no tests, fixtures,
  generated/platform variants, benchmarks, fuzz targets, or generator inputs.
  Rust's `tidb-dxf` owns task/resource/step data but no dependency-closed DXF
  Prometheus registry or collector, so no speculative metrics facade or
  Rust-only behavior was added. Recorded the explicit boundary in
  `receipts/dxf_framework_dxfmetric.md` and
  `rust/docs/operations/dxf-framework-dxfmetric-audit-execplan.md`.

- 2026-09-02: audited the complete Go-master
  `pkg/dxf/framework/mock/execute` package: two tracked artifacts and 253
  lines, consisting of the public Bazel target and complete generated
  `StepExecutor` GoMock recorder. The package has no tests, fixtures,
  platform variants, benchmarks, fuzz targets, or generator inputs beyond the
  generated source. Rust's generic `tidb-dxf` task/resource/step model has no
  dependency-closed StepExecutor lifecycle or GoMock owner, so no speculative
  mock or Rust-only behavior was added. Recorded the explicit generated
  support boundary in `receipts/dxf_framework_mock_execute.md` and
  `rust/docs/operations/dxf-framework-mock-execute-audit-execplan.md`.

- 2026-09-02: audited the complete direct Go-master
  `pkg/dxf/framework/mock` package: five tracked artifacts and 1,556 lines,
  consisting of the public Bazel target and generated GoMock implementations
  for nine planner, scheduler, cleaner, storage, task-table, task-executor,
  and extension interfaces (195 generated functions). It has no direct tests,
  fixtures, platform variants, benchmarks, fuzz targets, or generator inputs;
  the nested `mock/execute` package is inventoried separately. Rust's generic
  `tidb-dxf` task/resource/step model has no dependency-closed owner for these
  Go lifecycle and recorder contracts, so no speculative mocks or Rust-only
  behavior were added. Recorded the explicit generated-support boundary in
  `receipts/dxf_framework_mock.md` and
  `rust/docs/operations/dxf-framework-mock-audit-execplan.md`.

- 2026-09-02: audited the complete Go-master `pkg/dxf/example` package: six
  tracked artifacts and 332 lines, including the package guide, JSON metadata,
  scheduler/executor implementation, mock-store end-to-end test, and Bazel
  targets (one flaky test). The 15 declarations cover Example factory
  registration, Init→StepOne→StepTwo→Done planning, per-subtask metadata,
  retry/idempotence hooks, and completion waiting. Rust already owns generic
  Example task/step constants but no dependency-closed scheduler/executor demo
  or mock-store harness, so no speculative runtime or Rust-only behavior was
  added. Recorded the explicit boundary in `receipts/dxf_example.md` and
  `rust/docs/operations/dxf-example-audit-execplan.md`.

- 2026-09-02: audited the complete Go-master `pkg/dxf/framework/testutil`
  package: seven tracked artifacts and 1,258 lines, with 68 test-support
  function/method declarations covering multi-node DXF contexts, owner
  election, GoMock scheduler/executor extensions, SQL-backed task/subtask
  helpers, keyspace selection, failpoint setup, and interval reduction. It has
  no package-local tests, fixtures, generated/platform variants, benchmarks,
  fuzz targets, or OWNERS files. Rust's generic `tidb-dxf` types have no
  dependency-closed mock-store/session/failpoint harness, so no speculative
  utility facade or Rust-only behavior was added. Recorded the explicit
  boundary in `receipts/dxf_framework_testutil.md` and
  `rust/docs/operations/dxf-framework-testutil-audit-execplan.md`.

- 2026-09-02: audited the complete Go-master `pkg/dxf/framework/metering`
  package: seven tracked artifacts and 1,411 lines, with 30 production and 16
  test/helper declarations. The inventory covers classic/next-gen recorder
  gates, monotonic object-store/cluster deltas, SDK writer creation, flush and
  retry loops, final-unregister cleanup, failure metrics, two failpoint hooks,
  and the 12-shard flaky test target. Rust has no dependency-closed DXF
  metering owner beyond unrelated config fields/comments, so no speculative
  writer or Rust-only behavior was added. Recorded the explicit boundary in
  `receipts/dxf_framework_metering.md` and
  `rust/docs/operations/dxf-framework-metering-audit-execplan.md`.

- 2026-09-02: audited the complete Go-master `pkg/dxf/framework/handle`
  package: six tracked artifacts and 1,361 lines, with 31 production and 11
  top-level test declarations. The inventory covers task submission/history,
  wait/cancel/pause/resume, retry and context cancellation, classic/next-gen
  region/scope defaults, PD-aware cloud-store prefixes, schedule status/node
  accounting, TTL flags/tune factors, object-store and metering entrypoints,
  and failpoint-backed SQL integration. Rust's generic `tidb-dxf` types have no
  dependency-closed service-handle owner, so no speculative facade or Rust-only
  behavior was added. Recorded the explicit boundary in
  `receipts/dxf_framework_handle.md` and
  `rust/docs/operations/dxf-framework-handle-audit-execplan.md`.

- 2026-09-02: audited the complete Go-master `pkg/dxf/framework/planner`
  package: five tracked artifacts and 320 lines, with four production and two
  top-level test declarations. The inventory covers `PlanCtx`, logical and
  physical plan contracts, processor-step filtering, pipeline subtask metadata,
  session-aware task creation, and a two-shard flaky Bazel target. Rust's
  `tidb-planner` is the separate SQL optimizer and has no dependency-closed DXF
  planner/storage owner, so no speculative facade or Rust-only behavior was
  added. Recorded the explicit boundary in
  `receipts/dxf_framework_planner.md` and
  `rust/docs/operations/dxf-framework-planner-audit-execplan.md`.

- 2026-09-02: audited the complete direct Go-master
  `pkg/dxf/framework/scheduler` package: 17 tracked artifacts and 6,321 lines,
  with 103 production declarations, 47 top-level tests, and 25 test helpers.
  The inventory covers autoscaling, slot/stripe reservation, node liveness and
  scopes, subtask balancing, task-state transitions, BaseScheduler and Manager
  loops, cleanup/history GC, keyspace runtime ownership, failpoints, and the
  11-shard flaky test target. The nested generated `scheduler/mock` package is
  separate. Rust owns only generic DXF values/status and executor identity, no
  dependency-closed scheduler runtime, so no speculative port or Rust-only
  behavior was added. Recorded the explicit boundary in
  `receipts/dxf_framework_scheduler.md` and
  `rust/docs/operations/dxf-framework-scheduler-audit-execplan.md`.

- 2026-09-02: aligned the scheduler package with Go master’s bounded cleanup
  contract: `GetCleanupTasks`, `Cleaner`/`BatchCleaner`, startup draining,
  cleanup-progress accounting, and data-error metric classification. Added
  focused cleanup-batch/error-path regressions; legacy cleanup names remain
  explicit adapters for packages not yet migrated. Updated the scheduler
  receipt and ExecPlan with the failpoint-aware package pass.

- 2026-09-02: audited the complete generated Go-master nested
  `pkg/dxf/framework/scheduler/mock` package: exactly two tracked artifacts
  and 173 lines (`BUILD.bazel` plus the MockGen `scheduler_mock.go`), with 19
  generated declarations covering the scheduler `Extension` constructor,
  recorder, and method pairs. There are no package-local tests, fixtures,
  testdata, platform variants, generator inputs, or build artifacts beyond
  the Bazel target. Rust has no dependency-closed scheduler extension/mock
  owner and no Rust-only behavior was found to remove; the Go-only generated
  support boundary remains explicit. Recorded the receipt in
  `receipts/dxf_framework_scheduler_mock.md` and
  `rust/docs/operations/dxf-framework-scheduler-mock-audit-execplan.md`.

- 2026-09-02: regenerated the direct DXF GoMock outputs for the scheduler
  `Cleaner`/`GetCleanupTasks` contract and removed stale task-table methods;
  the generated-mock receipt now records this implementation alignment.

- 2026-09-02: aligned the complete Go-master `pkg/dxf/framework/proto`
  package: 11 tracked artifacts and 1,283 lines, with the owner-local DXF
  task-cleanup batch-size knob, [1, 1000] validation, test restore helper,
  and 11-shard Bazel metadata. Rust owns generic DXF values but no
  dependency-closed scheduler/storage tuning surface, so no speculative Rust
  API or Rust-only behavior was added. Recorded the updated boundary in
  `receipts/dxf_framework_proto.md` and
  `rust/docs/operations/dxf-framework-proto-audit-execplan.md`.

- 2026-09-02: aligned the complete Go-master `pkg/dxf/framework/storage`
  package: 11 tracked artifacts and 4,841 lines, including canonical VARCHAR
  task-key conversion, atomic task/subtask history transfer, bounded cleanup
  selection, redacted history error categories, and all 35 top-level tests.
  Rust has no dependency-closed SQL/session-backed DXF storage owner; the
  branch retains `GetTasksInStates` only for older scheduler consumers while
  exposing Go master’s `GetCleanupTasks`. Recorded the boundary in
  `receipts/dxf_framework_storage.md` and
  `rust/docs/operations/dxf-framework-storage-audit-execplan.md`.

- 2026-09-02: aligned DXF test utilities with the canonical Go-master cleaner
  API and VARCHAR task-key conversion in all SQL inspection helpers; focused
  scheduler/integration consumers now use the updated generated mock seam.

- 2026-09-02: aligned the complete Go-master
  `pkg/dxf/framework/integrationtests` package: exactly 11 tracked artifacts
  and 2,210 lines. Cleanup registrations now use `Cleaner`, manual-recovery
  updates use canonical numeric task IDs, and subtask SQL checks use
  `TaskIDToKey`; the full inventory and Go-only harness boundary are recorded
  in `receipts/dxf_framework_integrationtests.md` and
  `rust/docs/operations/dxf-framework-integrationtests-audit-execplan.md`.

- 2026-09-02: audited the complete top-level Go-master `pkg/dxf/framework`
  package: exactly two artifacts and 190 lines, consisting of the public Bazel
  target and the full framework package guide. Both files are byte-identical
  to Go master; Rust's `tidb-dxf` owns selected generic values but no
  dependency-closed framework runtime or SQL/session integration. Recorded the
  explicit documentation-only boundary in `receipts/dxf_framework.md` and
  `rust/docs/operations/dxf-framework-audit-execplan.md`.

- 2026-09-02: audited the complete Go-master `pkg/ddl/jobsubmit` package:
  exactly six artifacts and 1,119 lines, including transactional DDL
  submission, global-ID retry, BDR/upgrading-state validation, table-mode job
  construction, and six top-level tests. All artifacts are byte-identical to
  Go master and the failpoint-aware suite passes. Rust has no dependency-closed
  SQL/session-backed DDL job submitter, so no Rust-only behavior was removed or
  speculatively reimplemented. Recorded the boundary in
  `receipts/ddl_jobsubmit.md` and
  `rust/docs/operations/ddl-jobsubmit-audit-execplan.md`.

- 2026-09-02: aligned the complete Go-master `pkg/ddl/notifier` package:
  exactly eight artifacts and 1,999 lines, including the persistent event
  store, owner-listener delivery, 12 top-level tests, and 12-shard target. A
  one-second cleanup eventual timeout was restored to Go master's five-second
  reliability contract in `TestDeliverOrderAndCleanup`; the focused and full
  failpoint-aware suites pass. Rust has no dependency-closed SQL-backed DDL
  notifier, so no Rust-only behavior was removed or speculatively added.
  Recorded the inventory and boundary in `receipts/ddl_notifier.md` and
  `rust/docs/operations/ddl-notifier-audit-execplan.md`.

- 2026-09-02: audited the complete Go-master `pkg/domain/affinity` package:
  exactly four artifacts and 706 lines, including PD affinity-group
  compatibility fallbacks, bounded URL selection, retry logging, and 11
  top-level tests. All files are byte-identical to Go master and the package
  suite passes. Rust has no dependency-closed PD HTTP/DDL affinity owner, so no
  Rust-only behavior was removed or speculatively implemented. Recorded the
  boundary in `receipts/domain_affinity.md` and
  `rust/docs/operations/domain-affinity-audit-execplan.md`.

- 2026-09-02: aligned the complete Go-master `pkg/ddl/ingest` package:
  exactly 20 artifacts and 4,777 lines, including all local-storage,
  checkpoint, engine, memory/disk admission, integration, and 34 top-level test
  functions. Restored Go master's local-sort disk-space admission and
  retry/fatal error classification, removed the Rust-only exported disk helper
  and test, and restored the 33-shard embedded Bazel target. The focused and
  complete failpoint-aware suites pass; the inventory and Go-only boundary are
  recorded in `receipts/ddl_ingest.md` and
  `rust/docs/operations/ddl-ingest-audit-execplan.md`.

- 2026-09-02: restored the complete Go-master `pkg/domain/serverinfo`
  boundary: exactly five artifacts and 2,295 lines, including the leased
  status-endpoint claim state machine, registration/revocation cleanup,
  shutdown ordering, and six top-level tests. Focused claim tests and the full
  failpoint-aware package suite pass. Rust has no dependency-closed domain or
  etcd owner; the current Go-only inventory supersedes the earlier partial
  status-claim receipt in `receipts/domain_serverinfo_audit.md`, with details in
  `rust/docs/operations/domain-serverinfo-audit-execplan.md`.

- 2026-09-02: aligned the complete Go-master `pkg/domain/crossks` package:
  exactly eight artifacts and 2,094 lines, including runtime eviction,
  cross-keyspace DDL submission, server-info cleanup, and eight top-level tests.
  Restored failed-bootstrap/manager-close cleanup and the min-job-ID refresher
  control seam; focused and complete failpoint-aware suites pass. Four test-only
  protobuf literals retain this branch's older `kvproto` field spelling. The
  Go-only boundary is recorded in `receipts/domain_crossks.md` and
  `rust/docs/operations/domain-crossks-audit-execplan.md`.

- 2026-09-02: aligned the complete Go-master `pkg/domain/infosync` package:
  exactly twelve artifacts and 3,618 Go-master lines, including etcd/PD
  registration, placement/schedule/resource/TiFlash managers, all tests, and
  BUILD metadata. Restored variadic `serverinfo.SyncerOption` forwarding in
  `GlobalInfoSyncerInit`, added the focused constructor-option regression, and
  restored mock resource-manager metastore `Get`/`Put` methods. The branch's
  older kvproto keeps one test-only keyspace literal spelling. Focused and
  complete failpoint-aware suites pass; the inventory and Go-only boundary are
  recorded in `receipts/domain_infosync.md` and
  `rust/docs/operations/domain-infosync-audit-execplan.md`.

- 2026-09-02: audited the complete Go-master `pkg/domain/globalconfigsync`
  package: exactly three artifacts and 203 lines, including the PD global
  configuration bridge, OpenCensus/session integration tests, and BUILD target.
  All artifacts are byte-identical to Go master and the complete package suite
  passes. Rust has no dependency-closed owner for this TiDB-session/PD adapter,
  so no Rust-only behavior was removed or speculatively implemented. Recorded
  the explicit boundary in `receipts/domain_globalconfigsync.md` and
  `rust/docs/operations/domain-globalconfigsync-audit-execplan.md`.

- 2026-09-02: audited the complete Go-master `pkg/domain/sqlsvrapi` root
  package: exactly two artifacts and 82 lines containing the public runtime,
  keyspace-handle, and SQL-server interfaces. All files are byte-identical to
  Go master and the package plus generated mock package compile. Rust has no
  dependency-closed SQL/session/DDL API owner, so no Rust-only behavior was
  removed or speculatively implemented. Recorded the root boundary in
  `receipts/domain_sqlsvrapi.md` and
  `rust/docs/operations/domain-sqlsvrapi-audit-execplan.md`.

- 2026-09-02: audited the complete Go-master nested
  `pkg/domain/sqlsvrapi/mock` package: exactly four artifacts and 301 lines,
  including all three MockGen outputs and BUILD metadata. Generated files are
  byte-identical to Go master and were not hand-edited; parent/mock compilation
  passes. Recorded the generated boundary in
  `receipts/domain_sqlsvrapi_mock.md` and
  `rust/docs/operations/domain-sqlsvrapi-mock-audit-execplan.md`.

- 2026-09-02: aligned the complete Go-master `pkg/owner` package: exactly
  eight artifacts and 1,883 lines, including etcd owner election, distributed
  lock and failpoint/goleak tests, BUILD metadata, and OWNERS policy. Go source,
  tests, and BUILD files were already byte-identical; restored the Go-master
  BUILD-specific approver filter in OWNERS. The full failpoint-aware suite and
  Ready gates pass. Recorded the boundary in `receipts/owner_parity.md` and
  `rust/docs/operations/owner-audit-execplan.md`.

- 2026-09-02: audited the complete Go-master `pkg/ddl/schematracker` package:
  exactly six artifacts and 2,979 lines, including the DDL checker, schema
  tracker, InfoStore, 17 tests, and BUILD target. All Go artifacts are
  byte-identical to Go master and the complete failpoint-aware suite passes.
  Rust's `tidb-exec` InfoStore is explicitly a one-file seed without the
  dependency-closed tracker/checker/DDL graph, so no Rust-only behavior was
  removed or speculatively added. Recorded the boundary in
  `receipts/ddl_schematracker.md` and
  `rust/docs/operations/ddl-schematracker-audit-execplan.md`.

- 2026-09-02: updated the complete five-artifact Go-master
  `pkg/server/handler/tests` consumer inventory (3,630 lines) for the DXF
  history redaction contract. Its focused API regression now requires
  `ErrorCode`/`ErrorCategory` and rejects raw sensitive task-error text. Rust
  has no dependency-closed HTTP handler server or session-backed history
  owner, so this remains Go-native consumer coverage. Recorded the boundary
  in `receipts/server_handler_tests.md` and
  `rust/docs/operations/server-handler-tests-audit-execplan.md`.

- 2026-09-02: aligned the complete direct Go-master
  `pkg/dxf/framework/taskexecutor` package: exactly 12 tracked artifacts and
  4,129 lines, including cancellation classification, the `GetExecID` API,
  and observer-backed regression coverage. Rust has no dependency-closed
  task-executor runtime owner, so no speculative Rust implementation was
  added. Recorded the inventory and validation boundary in
  `receipts/dxf_framework_taskexecutor.md` and
  `rust/docs/operations/dxf-framework-taskexecutor-audit-execplan.md`.

- 2026-09-02: audited the complete Go-master
  `pkg/dxf/framework/taskexecutor/execute` leaf: exactly three artifacts and
  517 lines, with 17 production declarations and one speed-window test. The
  checkout is byte-identical to Go master; Rust has no dependency-closed
  StepExecutor runtime or equivalent metering-summary owner, so no speculative
  trait or Rust-only behavior change was made. Recorded the explicit boundary
  in `receipts/dxf_framework_taskexecutor_execute.md` and
  `rust/docs/operations/dxf-framework-taskexecutor-execute-audit-execplan.md`.

- 2026-09-01: audited the complete direct Go-master parent
  `pkg/dxf/importinto` package: 26 tracked artifacts and 9,158 lines, with
  170 production function/method declarations and 45 top-level test
  functions/suite methods. The inventory covers planner metadata and range
  splitting, scheduler/keyspace transaction boundaries, local/global
  encode/merge/ingest, conflict collection/resolution, checksum, cleanup,
  metering, metrics, and every direct mock-store/failpoint test; no direct
  fixtures, platform/generated variants, benchmarks, fuzz targets, or
  generator inputs exist. Rust owns only generic DXF task/step vocabulary and
  SQL IMPORT INTO parser/session support, not a dependency-closed ImportInto
  runtime, so no speculative Rust behavior was added. Recorded the explicit
  parent boundary in `receipts/dxf_importinto.md` and
  `rust/docs/operations/dxf-importinto-audit-execplan.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/session/test/nontransactionaltest` package before editing: three
  tracked artifacts and 614 lines covering six batch-DML behavior tests, the
  shard-composition helper, failpoint/goleak harness, and six-shard flaky
  BUILD target. The package is unchanged from the pinned Go source. Rust has
  typed admission and metric-label leaves but no dependency-closed shard
  planner/worker/storage owner; recorded the explicit boundary in
  `receipts/session_test_nontransactionaltest.md`.

- 2026-09-01: audited the complete Go-master `pkg/session/test/common`
  package before editing: four tracked artifacts and 600 lines, including
  seven session metadata/protocol tests, five prepared-statement dedup-cache
  lifecycle tests, the failpoint/goleak harness, and the twelve-shard flaky
  BUILD target. The package is unchanged from the pinned Go source. Rust's
  ignored carriers remain explicit because no dependency-closed TestKit +
  Domain + storage transaction + PlanCacheStmt protocol owner is available;
  recorded the boundary in `receipts/session_test_common.md`.

- 2026-09-01: audited the complete Go-master `pkg/session/test/meta` package
  before editing: three tracked artifacts and 376 lines, covering DDL/meta
  table initialization, region keys, TTL transaction metrics, timezone-aware
  information-schema create time, next-generation reserved IDs, the
  failpoint/goleak harness, and the six-shard flaky BUILD target. The only
  master delta is the reserved base-table assertion changing from 60 to 65;
  no Rust behavior was changed because the dependency-closed bootstrap,
  Domain, mock TiKV, DDL, tablecodec, and SQL owner is not transcreated.
  Recorded the boundary in `receipts/session_test_meta.md`.

- 2026-09-01: audited the complete Go-master `pkg/ingestor/ingestctrl`
  package before editing: 33 tracked artifacts and 16,709 lines, including
  the BUILD target, 18 production sources, all four platform RLimit variants,
  14 test/benchmark sources with 79 tests and five benchmarks, Pebble engine,
  duplicate/checksum managers, split/import pipeline, worker/retry/dispatcher,
  rate/disk gates, and TiKV mode switching. The exact Go-master failpoint
  suite passed, including current worker-cancellation regressions. Rust
  contains only generated protocol vocabulary and adjacent helpers, with no
  dependency-closed ingest-controller owner, so no Rust-only behavior was
  removed and no speculative implementation was added. Recorded the explicit
  boundary in `receipts/ingestor_ingestctrl.md`.

- 2026-09-01: audited the complete Go-master root `pkg/objstore` package
  before editing: 27 root-level tracked artifacts and 8,388 lines, including
  the public BUILD target, all 17 production/support files, Azure/GCS/HDFS and
  local/memory/no-op backends, URL/flag parsing, multipart upload, compression,
  lock transactions, range readers, and all nine root test files. The current
  master source adds Azure concurrent block upload, GCS multipart abort/error
  handling, parser URL redaction, and their focused tests; the exact
  failpoint-enabled Go-master suite passed. Rust has no dependency-closed
  object-storage owner (plan-replayer storage traits and generated protocol
  vocabulary are not this package), so no Rust-only behavior was removed and
  no speculative backend was added. Nested `objstore/*` packages remain
  separate package audits. Recorded the explicit boundary in
  `receipts/objstore_root.md`.

- 2026-09-01: audited the complete Go-master `pkg/objstore/compressedio`
  package before editing: five tracked artifacts and 261 lines, covering the
  BUILD target plus all 12 buffer, enum/parser, reader, and writer
  functions. It has no tests, fixtures, generated files, or platform variants
  and is unchanged from the pinned source. Rust's `tidb-util::compress` owns
  the separate `pkg/util/compress` gzip pool, not this object-store codec
  contract, so no Rust-only behavior was removed and no speculative facade was
  added. Recorded the explicit boundary in
  `receipts/objstore_compressedio.md`.

- 2026-09-02: completed the Go-master `pkg/objstore/objectio` parity unit in
  one package batch. Its four tracked artifacts were inventoried in full;
  `NewIOWriter` now binds the supplied context while adapting the package's
  context-aware `Writer` to `io.Writer`, with a focused forwarding regression.
  The pre-fix test failed to compile because the helper was missing, and the
  focused plus full package tests pass after restoration. Rust's
  plan-replayer `ObjectWriter` remains a separate narrow boundary, so no
  Rust-only behavior was removed. Ready validation was attempted: package
  lint and `git diff --check` pass, while `make bazel_prepare` lacks a local
  Bazel binary; repository `make lint` and `git diff --check` pass. Updated
  `receipts/objstore_objectio.md`.

- 2026-09-01: audited the complete Go-master `pkg/objstore/recording`
  package before editing: three tracked artifacts and 167 lines, including
  the BUILD target, nine atomic request/traffic methods, and the one test for
  GET/HEAD/PUT/POST plus ignored methods and nil requests. It is unchanged
  from the pinned source. Rust's TiKV RPC traffic counters are unrelated to
  object-storage access recording, so no Rust-only behavior was removed and
  no speculative metrics facade was added. Recorded the explicit boundary in
  `receipts/objstore_recording.md`.

- 2026-09-01: audited the complete Go-master `pkg/objstore/storeapi`
  package before editing: three tracked artifacts and 404 lines, including
  the public storage contracts, permission/options fields, prefix/range
  helpers, shared multipart-limit sentinel, and two focused tests. Go master
  removes the obsolete `ReadSeekCloser` alias and adds `MaxUploadParts` /
  `ErrExceedMaxUploadParts`; Rust has no dependency-closed object-store
  contract owner, so no Rust-only behavior was removed and no speculative
  trait was added. Recorded the explicit boundary in
  `receipts/objstore_storeapi.md`.

- 2026-09-02: refreshed the complete Go-master `pkg/objstore/s3like` package
  before editing: eight root artifacts and 1,424 lines, including all S3-like
  response/client contracts, ranged reader and async writer, metrics,
  permissions, retry adapter, options/flags, CRUD/walk/range/presign paths,
  and the generated mock boundary. Restored Go master's retry-log suppressor
  and positive presign-expiration validation, with focused regressions in the
  existing permission test target. Rust has no dependency-closed S3-compatible
  backend owner, so no Rust-only behavior was removed and no speculative cloud
  stack was added. Ready evidence and the explicit parity boundary are in
  `receipts/objstore_s3like.md`.

- 2026-09-01: audited the separate generated Go package
  `pkg/objstore/s3like/mock` in full: its 2 tracked artifacts and 261 lines,
  including the BUILD target and all 31 MockGen methods/recorders generated
  from `PrefixClient`. It has no tests, fixtures, platform variants, or
  additional generator inputs, and is unchanged from the pinned source. Rust
  has no GoMock-compatible owner; recorded the generated-support boundary in
  `receipts/objstore_s3like_mock.md`.

- 2026-09-01: audited the complete Go-master Alibaba OSS backend package
  `pkg/objstore/ossstore` before editing: ten root artifacts and 1,941 lines,
  including the SDK client adapter, ranged/multipart operations, temporary
  credential refresher, retry/logger bridges, all 14 permission/presign/
  credential tests, the skipped live-service CRUD/walk workflow, and the
  14-shard BUILD target. Go master adds presigning, public-endpoint logging
  isolation, and opt-in credential forwarding; Rust has no dependency-closed
  OSS owner, so no Rust-only behavior was removed and no speculative backend
  was added. Recorded the explicit boundary in
  `receipts/objstore_ossstore.md`.

- 2026-09-01: audited the separate generated Go package
  `pkg/objstore/ossstore/mock` in full: three artifacts and 416 lines,
  including both MockGen outputs for the OSS API and credentials provider. It
  contains 38 generated methods/recorders, no tests, fixtures, platform
  variants, or generator inputs, and only the current-master `Presign` mock
  delta. Rust has no GoMock-compatible owner; recorded the generated-support
  boundary in `receipts/objstore_ossstore_mock.md`.

- 2026-09-01: audited the complete Go-master AWS/compatible S3 backend package
  `pkg/objstore/s3store` before editing: 16 root artifacts and 5,619 lines,
  including AWS CRUD/multipart/presign, GCS signer, KS3 reader/uploader,
  Alibaba fallback credentials, Tencent COS provider, region/retry/logger
  handling, all 72 top-level tests and support helpers, and the 50-shard BUILD
  target. Current-master additions include GCS/Tencent variants, multipart
  overflow and content-MD5 handling, credential-chain fallback, and region
  redirect-log suppression; the exact failpoint-enabled suite passed. Rust has
  no dependency-closed cloud-object-store owner, so no Rust-only behavior was
  removed and no speculative backend was added. Recorded the explicit boundary
  in `receipts/objstore_s3store.md`.

- 2026-09-01: audited the separate generated Go package
  `pkg/objstore/s3store/mock` in full: two artifacts and 338 lines, including
  the BUILD target and all 31 MockGen methods/recorders for `S3API`. It has no
  tests, fixtures, platform variants, or generator inputs and is unchanged from
  the pinned source. Rust has no GoMock-compatible owner; recorded the
  generated-support boundary in `receipts/objstore_s3store_mock.md`.

- 2026-09-01: audited the separate generated Go package
  `pkg/objstore/mockobjstore` in full: two artifacts and 232 lines, including
  the BUILD target and all 27 MockGen methods/recorders for the complete
  `storeapi.Storage` contract. It has no production sources, tests, fixtures,
  platform variants, or generator inputs and is unchanged from the pinned
  source. Rust's plan-replayer and TiKV test mocks implement unrelated storage
  traits, so no Rust-only behavior was removed and no speculative mock was
  added. Recorded the explicit boundary in
  `receipts/objstore_mockobjstore.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/dumpformat/parsedef` package: two artifacts and 50 lines containing
  the public BUILD target, the `Row` data carrier, and its zap array-marshaling
  method. Rust's execution rows are not a dump-format/importer row or zapcore
  owner, so no Rust-only behavior or speculative facade was added. The exact
  pinned Go package compiles with no local tests; the explicit boundary is
  recorded in `receipts/dumpformat_parsedef.md` and
  `rust/docs/operations/dumpformat-parsedef-audit-execplan.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/dumpformat/testutils` package: two artifacts and 296 lines covering
  the public BUILD target, all typed Parquet column dispatch and slicing,
  object-store wrapper, and row-group writer. Go Parquet/importer tests call
  this support helper, but Rust has no dependency-closed Arrow/Parquet fixture
  writer or matching object-store owner. No Rust-only behavior or speculative
  generator was added; the exact package compile and boundary are recorded in
  `receipts/dumpformat_testutils.md` and
  `rust/docs/operations/dumpformat-testutils-audit-execplan.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/dumpformat/csvfile` package: five artifacts and 403 lines covering the
  BUILD target, CSV framing configuration, escaping/quoting and binary-format
  implementation, streaming writer, and eleven focused tests. Rust has parser
  support for CSV syntax but no dependency-closed CSV export writer or
  `sql.RawBytes`/`io.Writer` owner. No Rust-only behavior or speculative
  facade was added; the exact detached Go suite and boundary are recorded in
  `receipts/dumpformat_csvfile.md` and
  `rust/docs/operations/dumpformat-csvfile-audit-execplan.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/dumpformat/sqlfile` package: four artifacts and 318 lines covering the
  BUILD target, SQL literal/hex escaping, INSERT tuple writer and splitting
  accounting, and four focused tests. Rust's `sqlescape` is only a generic
  argument formatter and has no dependency-closed dump writer owner. No
  Rust-only behavior or speculative facade was added; the exact detached Go
  suite and boundary are recorded in `receipts/dumpformat_sqlfile.md` and
  `rust/docs/operations/dumpformat-sqlfile-audit-execplan.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/dumpformat/parquetfile` package: 20 artifacts, 9,567 Go text lines,
  29 tests, three benchmarks, the generated 3,108-line Spark rebase table,
  and Aurora/Hive binary fixtures. The full failpoint-enabled Go suite passed.
  Rust has no Arrow/Parquet, object-store reader, Spark rebase, or fixture
  owner, so no Rust-only behavior or speculative implementation was added.
  The complete inventory and explicit boundary are recorded in
  `receipts/dumpformat_parquetfile.md` and
  `rust/docs/operations/dumpformat-parquetfile-audit-execplan.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/dumpformat/parquetfile` package: 20 artifacts and 9,586 lines,
  including 126 production functions, 32 test/benchmark declarations, the
  29-shard BUILD target, generated Spark 3.5.7 rebase data, and two binary
  Parquet fixtures. The exact detached Go-master failpoint suite passed in
  1.261s. Rust has only parser/decimal leaves and no dependency-closed
  Arrow/Parquet owner, so no Rust-only behavior or speculative crate was
  added. Complete inventory and boundary evidence are recorded in
  `receipts/dumpformat_parquetfile.md` and
  `rust/docs/operations/dumpformat-parquetfile-audit-execplan.md`.

- 2026-09-02: completed the current Go-master `pkg/ingestor/globalsort`
  package batch after reading all 17 tracked artifacts and 6,814 lines,
  including the external engine, object-store readers, merge and merge-v2
  operators, range splitter, metadata/file-group planner, benchmark/profiler
  helpers, all source tests, the 3K-file stress surface, and the 41-shard BUILD
  target. Restored grouped merge target-file accounting, nil split semantics,
  multi-directory cleanup, and explicit `GlobalSort:TooManyDataFiles`
  validation; focused regressions failed before the fix and pass afterward, as
  does the complete failpoint-enabled suite. Rust has only DXF step/resource
  metadata, with no dependency-closed owner for global-sort SST execution, so
  no Rust-only behavior was removed and no speculative implementation was
  added. Ready lint and diff checks pass; `make bazel_prepare` is blocked only
  by the missing local Bazel executable. Recorded the explicit boundary in
  `receipts/ingestor_globalsort.md`.

- 2026-09-01: audited the complete Go-master `pkg/ingestor/simplesst`
  package before making any change: 19 tracked artifacts and 6,545 lines,
  including the object-storage byte reader, codec/range properties, concurrent
  reader, file and KV abstractions, merge iterators, one-file writer, stats,
  utilities, writer/engine adapter, all eight test files, and the package
  BUILD target. The current-master variadic file-enumeration API and named
  per-core connection-limit constant were recorded in the inventory. The
  failpoint-enabled package suite passed. Rust has no dependency-closed owner
  for this external-storage SST protocol (local `tidb-util::extsort` and DXF
  metadata are not equivalents), so no Rust-only behavior was removed and no
  speculative implementation was added. Recorded the explicit boundary in
  `receipts/ingestor_simplesst.md`.

- 2026-09-01: completed the package-level `pkg/ingestor` audit after reading
  `doc.go` first: three tracked artifacts and 42 lines at Go master,
  including the current BUILD/OWNERS filters. The root is an empty
  documentation and ownership landing package; no Rust root ingestor owner or
  executable behavior exists, so no placeholder crate was invented. Recorded
  the boundary in `receipts/ingestor_root.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/ingestor/ingestcli` client plus its generated mock package: eight
  artifacts and 1,086 lines covering the `/write_sst` stream, `/ingest_s3`
  request, protobuf error mapping, metrics, interfaces, tests, and Bazel
  targets. All Go source tests pass. Rust has no next-generation TiKV worker
  HTTP/PD client owner; local external-sort helpers and DXF step metadata do
  not implement this protocol. No Rust-only behavior was found and no
  disconnected client API was invented. Recorded the explicit boundary in
  `receipts/ingestor_ingestcli.md`.

- 2026-09-01: audited four complete bounded ingestor packages against Go
  master before entering the large engine implementations:
  `pkg/ingestor/engineapi` (three artifacts, 212 lines), `errdef` (two
  artifacts, 76 lines, including the current-master
  `GlobalSort:TooManyDataFiles` addition), `ingestmetric` (two artifacts, 67
  lines), and `testutils` (two artifacts, 73 lines). All compile as Go
  packages and have no package-local tests. Rust owns DXF step metadata but no
  ingest engine/global-sort planner, next-generation client, metric consumer,
  or object-store test surface, so unused compatibility APIs were not
  invented. Recorded the explicit boundaries in four package receipts.

- 2026-09-01: audited the complete Go-master `pkg/importsdk` package before
  editing: 18 tracked artifacts (including both Bazel targets and the
  generated GoMock output) and 3,879 lines, with no platform variants,
  fixtures, benchmarks, or fuzz inputs. The Rust workspace owns only
  `IMPORT INTO` parser/AST and related metadata; it has no external-storage
  scanner, schema importer, SQL-generator SDK, or job-manager owner. No
  Rust-only behavior was present and no speculative implementation was added.
  The full source-test subset passed; fake-GCS tests remain blocked by missing
  Application Default Credentials. Recorded the explicit boundary in
  `receipts/importsdk.md`.

- 2026-09-01: audited the complete Go-master `pkg/testkit` package: 38 tracked
  artifacts and 4,202 lines, including all root/nested support code, source
  tests, `!codes` variants, and Bazel targets. Rust has no dependency-closed
  owner for the Go mock-store/domain bootstrap, TestKit SQL API, database/sql
  driver, result/stepped runners, testdata recorder, or logging/failpoint
  helpers. No Rust-only behavior or safe missing production behavior was found;
  the full tagged Go package suite passed and the explicit boundary is recorded
  in `receipts/testkit.md`.

- 2026-09-01: audited the complete Go-master `pkg/inference` package: 43
  artifacts and 7,368 lines, including the Domain adaptor, runtime cache and
  cancellation path, batching layer, seven provider adapters, protocol models,
  deterministic mock, shared contract fixtures, 122 tests, and all Bazel
  targets. The exact Go-master worktree suite passed. Rust has only embedding
  variable constants and ignored `EMBED_TEXT` gap tests; it has no
  dependency-closed inference/provider/vector owner. No Rust-only behavior or
  safe partial production fix was found, so the explicit SEED boundary is
  recorded in `receipts/inference.md`.

- 2026-09-01: audited the complete Go-master `pkg/sessiontxn/staleread`
  package: 10 tracked artifacts and 1,746 lines, including the stale-read
  processor, transaction-context provider, AS OF/read-staleness/external-ts
  utilities, failpoint hook, all 13 package tests, and the BUILD target. The
  exact Go-master failpoint-managed suite passed. Rust has bounded AS OF
  history support and timestamp parsing, but no dependency-closed owner for
  the provider lifecycle, session `tidb_snapshot`/`tidb_read_staleness`
  semantics, external timestamp cache, follower-read options, or prepared
  evaluator. No Rust-only behavior or safe partial production fix was found;
  the explicit SEED boundary is recorded in
  `receipts/sessiontxn_staleread.md`.

- 2026-09-01: audited the complete Go-master `pkg/sessiontxn/isolation`
  package: 13 tracked artifacts and 3,992 lines, including the shared
  transaction-provider lifecycle, optimistic/RC/RR/serializable providers,
  nested RC metrics package, all 29 tests, failpoint/goleak setup, and both
  BUILD targets. The exact Go-master failpoint-managed suite passed. Rust
  owns isolation value semantics and partial cluster transaction seams, but
  has no dependency-closed owner for the integrated provider lifecycle,
  per-isolation timestamp policy, snapshot overlays, RC metrics/retries, or
  full pessimistic lock/error behavior. No Rust-only behavior or safe partial
  production fix was found; the explicit SEED boundary is recorded in
  `receipts/sessiontxn_isolation.md`.

- 2026-09-01: audited the complete Go-master root `pkg/sessiontxn` package:
  seven tracked artifacts and 3,113 lines, including the manager/provider
  interfaces, constructors, constant timestamp future, failpoint/test
  support, all 27 root tests, and the BUILD target. Nested `isolation` and
  `staleread` packages remain separate audited units. Rust owns transaction
  state, cluster-session routing, isolation metadata, and KV snapshot traits
  in adjacent crates, but has no dependency-closed replacement for this
  interface/support package. No Rust-only behavior or safe missing root
  production behavior was found; the exact Go-master failpoint suite passed
  and the explicit boundary is recorded in `receipts/sessiontxn.md`.

- 2026-09-01: audited the complete Go-master nested
  `pkg/sessiontxn/internal` package: two tracked artifacts and 98 lines,
  including all three transaction/snapshot option helpers and the 16-line
  BUILD target. It has no package-local tests, fixtures, generated outputs,
  benchmarks, fuzz inputs, or platform variants; root sessiontxn tests cover
  its callers. Rust owns assertion parsing, transaction-boundary routing,
  request-source/replica-read propagation, and snapshot/interceptor traits in
  adjacent crates, but no dependency-closed replacement for this support
  package. No Rust-only behavior or safe missing behavior was found; exact
  Go-master compilation passed and the explicit boundary is recorded in
  `receipts/sessiontxn_internal.md`.

- 2026-09-01: audited the complete Go-master `pkg/session/txninfo` package:
  three tracked artifacts and 473 lines, including the FNV/LRU transaction
  summary recorder, five running-state labels, Prometheus state metrics,
  `TxnInfo`/`ProcessInfo` fields and Datum conversion map, and the BUILD
  target. The package has no local tests, fixtures, generated outputs,
  benchmarks, fuzz inputs, or platform variants. Rust owns state labels and a
  partial live transaction registry, but no dependency-closed summary,
  metrics, and `TIDB_TRX` Datum datasource equivalent. No Rust-only behavior
  or safe missing behavior was found; exact Go-master compilation passed and
  the explicit boundary is recorded in `receipts/session_txninfo.md`.

- 2026-09-01: audited the complete Go-master `pkg/session/sessionapi`
  package: two tracked artifacts and 111 lines, including the public
  `Session` interface (embedded `sessionctx.Context` plus 34 explicit
  methods), identity error sentinel, and BUILD target. It has no tests,
  fixtures, generated outputs, benchmarks, fuzz inputs, or platform variants.
  Rust owns concrete session/server/authentication/prepared-execution pieces
  in adjacent crates, but no dependency-closed replacement for this public
  plugin-facing API. No Rust-only behavior or safe missing behavior was
  found; exact Go-master compilation passed and the explicit boundary is
  recorded in `receipts/session_sessionapi.md`.

- 2026-09-01: audited the complete Go-master `pkg/session/cursor` package:
  four tracked artifacts and 247 lines, including cursor state, the
  concurrent `sync.Map` tracker/handle lifecycle, five focused tests (with
  the 100-thread create/delete stress case), and the five-shard flaky BUILD
  target. Rust's `cursor_state` is the prepared-protocol cursor rather than
  this session result-set tracker, and no dependency-closed owner spans the
  session, static-recordset, and infosync consumers. No Rust-only behavior or
  safe missing behavior was found; exact Go-master tests passed and the
  explicit boundary is recorded in `receipts/session_cursor.md`.

- 2026-09-01: audited the complete Go-master `pkg/session/metrics` package:
  two tracked artifacts and 158 lines, including 49 exported
  counter/observer bindings for transaction, timing, parse/compile,
  partition, account-lock, CTE, index-merge, and store-batched telemetry,
  plus the BUILD target. It has no tests, fixtures, generated outputs,
  benchmarks, fuzz inputs, or platform variants. Rust's `tidb-exec` leaf owns
  only the three non-transactional DML labels and not the Prometheus/session
  registry. No Rust-only behavior or safe missing behavior was found; exact
  Go-master compilation passed and the explicit boundary is recorded in
  `receipts/session_metrics.md`.

- 2026-09-01: audited the complete Go-master `pkg/session/sessmgr` package:
  three tracked artifacts and 392 lines, including process/transaction row
  conversion, status and kill helpers, manager/coordinator interfaces, the
  shallow-clone test, and the flaky BUILD target. Rust's process registry is
  only a partial owner and lacks the full Go process-info fields and session
  manager/coordinator APIs. No Rust-only behavior or safe missing behavior
  was found; the exact Go-master failpoint suite passed and the explicit
  boundary is recorded in `receipts/session_sessmgr.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/session/test/nontransactionaltest` package: three tracked artifacts
  and 614 lines, including the TestMain/goleak harness, six SQL behavior
  tests, batch-DML sharding/error/constraint/FK/metrics/max-exec-time
  coverage, and six-shard flaky BUILD target. Rust has BATCH parser and
  admission leaves plus six ignored source carriers, but actual execution is
  owned by un-audited `pkg/session/nontransactional.go`; no
  dependency-closed test/production owner exists. No Rust-only behavior or
  safe missing behavior was found; exact Go-master failpoint suite passed and
  the boundary is recorded in `receipts/session_nontransactionaltest.md`.

- 2026-09-01: audited the complete nontransactional production owner
  `pkg/session/nontransactional.go` (873 lines, 21 functions), its root BUILD
  registration, and the fully inventoried 614-line focused test package. Go
  master behavior tests passed under failpoint management; Rust parser tests
  passed, while the admission target was environment-blocked by missing
  pkg-config/OpenSSL. Rust currently owns only BATCH parsing and dependency-
  free admission; sharding, workers, constraints, metrics, timing, and error
  aggregation remain cross-owner. No Rust-only behavior or safe missing
  behavior was found; the root `pkg/session` package remains open outside
  this slice and the boundary is recorded in
  `receipts/session_nontransactional.md`.

- 2026-09-01: audited the complete Go-master `pkg/session/test/privileges`
  package: three tracked test/BUILD artifacts and 138 lines, including the
  failpoint/goleak harness, `SkipWithGrant` role/auth assertions, unknown-user
  rejection, and two-shard flaky target. Rust's configured-user-store and
  session privilege owners already exercise bypass and authentication in
  executable server/session tests, while the exact Go TestKit/global state
  remains an explicit source-carrier boundary. No Rust-only behavior or safe
  missing behavior was found; the exact Go-master failpoint suite passed and
  the boundary is recorded in `receipts/session_test_privileges.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/session/test/resourcegrouptest` package: two tracked artifacts and 76
  lines, including the failpoint-driven statement/transaction resource-group
  test and flaky BUILD target. Rust already owns statement hint resolution
  and transaction resource-group propagation, but not the Go resource-group
  catalog/cost controller and `TxnResourceGroupChecker` observation seam. No
  Rust-only behavior or safe standalone implementation was found; the exact
  Go-master failpoint suite passed and the explicit boundary is recorded in
  `receipts/session_test_resourcegrouptest.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/session/test/schematest` package: three tracked artifacts and 506 lines,
  including the TestMain/goleak harness, ten schema/chunk/transaction/
  validation tests, and ten-shard flaky BUILD target. Rust has partial
  session, transaction, and chunk owners but no dependency-closed schema
  lease/MDL, mock-cluster DistSQL, transaction-size observation, or recursive
  variable-validation owner. No Rust-only behavior or safe standalone
  implementation was found; the exact Go-master failpoint suite passed and
  the boundary is recorded in `receipts/session_test_schematest.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/session/test/temporarytabletest` package: three tracked artifacts and
  512 lines, including the TestMain/goleak harness, three local/global
  temporary-table tests, and three-shard flaky BUILD target. Rust owns
  temporary-table session overlays, row lifetime, DDL guards, and core DML
  assertions, but exact mock TiKV point/batch/index-scan coverage and the
  cross-session schema lease/MDL lifecycle are not dependency-closed. No
  Rust-only behavior or safe standalone implementation was found; the exact
  Go-master failpoint suite passed, the Rust attempt was OpenSSL/pkg-config
  blocked, and the boundary is recorded in
  `receipts/session_test_temporarytabletest.md`.

- 2026-09-01: audited the complete Go-master `pkg/session/test/txn` package:
  three tracked artifacts and 622 lines, including the TestMain/goleak
  harness, eleven transaction lifecycle/conflict/timestamp/membuffer tests,
  and eleven-shard flaky BUILD target. Rust owns session autocommit/status,
  a narrow lazy-transaction predicate, and typed storage retry/membuffer
  primitives, but not the dependency-closed Go mock-TiKV conflict, read-only,
  Oracle timestamp, UnionScan, memory, or killed-transaction choreography.
  Added an explicit ignored source carrier for the Go-master
  `TestPanicOnRollbackKilledTxn`; no Rust-only behavior or safe standalone
  implementation was found. The exact Go-master failpoint suite and Ready
  lint/format checks passed; the targeted Rust carrier build was blocked only
  by missing OpenSSL/pkg-config. The boundary is recorded in
  `receipts/session_test_txn.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/session/test/variable` package: three tracked artifacts and 593 lines,
  including the TestMain/goleak harness, twelve variable/coprocessor/
  execution/replica/logging tests, three `mockZapCore` methods, and
  twelve-shard flaky BUILD target. Rust has executable owners for selected
  scope, isolation-read, replica-read, hint, and max-execution-time contracts,
  but not the dependency-closed coprocessor OOM/rate-limit, query RU,
  general-log, or full snapshot/staleness lifecycle seams. No Rust-only
  behavior or safe standalone implementation was found; the exact Go-master
  failpoint suite passed and the boundary is recorded in
  `receipts/session_test_variable.md`.

- 2026-09-01: audited the complete Go-master `pkg/session/test/vars` package:
  three tracked artifacts and 638 lines, including the TestMain/goleak
  harness, twelve variable/upgrade/timezone/hint/timestamp tests, and
  twelve-shard flaky BUILD target. Rust has executable owners for selected
  variable state, scope, hints, timezone, and timestamp contracts, but not
  the dependency-closed mock-TiKV transport, persistent upgrade values, TTL
  callback, deployment-mode policy, or checkpoint integration. No Rust-only
  behavior or safe standalone implementation was found; the exact Go-master
  failpoint suite passed and the boundary is recorded in
  `receipts/session_test_vars.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/session/test/bootstraptest` package: four tracked artifacts and 2,967
  lines, including the TestMain/goleak harness, 50 runnable bootstrap/upgrade
  tests, ten helpers, and the 45-shard flaky BUILD target. Rust has partial
  session/meta/metadef/exec/server bootstrap owners and ignored source
  carriers, but no dependency-closed owner for historical schema upgrades,
  Domain/DDL pause-resume, failpoint choreography, system-table validation,
  and mock-TiKV lifecycle. No Rust-only behavior or safe standalone
  implementation was found; the exact Go-master failpoint suite timed out in
  `TestUpgradeVersionForSystemPausedJob` after 601.691s, and the explicit
  boundary is recorded in `receipts/session_test_bootstraptest.md`.

- 2026-09-01: refreshed the complete Go-master
  `pkg/util/password-validation` inventory (three artifacts, 379 lines) and
  confirmed it is unchanged from the prior pinned implementation. The Go
  package now passes its five source tests under the current toolchain, and
  Rust's shared validator still passes all five source-derived tests with the
  same byte/rune policy and caller-owned enablement semantics. Updated
  `receipts/util_password_validation.md` with current-master hashes.

- 2026-09-02: refreshed the complete Go-master `pkg/util/compress` inventory at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec` (two artifacts, 45 lines) and
  corrected the prior receipt's Bazel line count
  and current-master authority metadata. The package is unchanged from the implementation
  batch; pooled gzip ownership, statistics block integration, four focused
  stream regressions, and the explicit ingest-control boundary remain intact.
  The dedicated audit ExecPlan is
  `docs/operations/util-compress-audit-execplan.md`.
  Updated `receipts/util_compress_audit.md` with current-master hashes.

- 2026-09-01: refreshed the complete Go-master `pkg/util/nocopy` inventory
  (two artifacts, 32 lines) and confirmed it is unchanged from the prior
  pinned audit. Rust's zero-sized `tidb-util::nocopy::NoCopy` marker and
  no-op lock methods already preserve Go's vet-oriented no-copy contract after
  removal of the former Rust-only traits and tests. Updated
  `receipts/util_nocopy.md` with current-master hashes; no new behavior was
  invented.

- 2026-09-01: refreshed the complete Go-master `pkg/util/tikvutil` inventory
  (two artifacts, 31 lines) and confirmed it is unchanged from the prior
  pinned audit. The Rust `tidb-tikvutil` atomic and its config/session
  consumers already preserve Go's single process authority, width, default,
  sequential consistency, and GLOBAL publication/reset semantics. Updated
  `receipts/util_tikvutil.md` with current-master hashes; no new behavior was
  invented.

- 2026-09-01: refreshed the complete Go-master `pkg/util/channel` inventory
  (two artifacts, 30 lines) and confirmed it is unchanged from the prior
  pinned audit. Rust's borrowed `tidb-util::channel::clear` already matches
  Go's blocking channel-drain contract after the earlier removal of arbitrary
  iterable support. Updated `receipts/util_channel.md` and the package
  ExecPlan with the current `origin/master` authority; no new behavior was
  invented.

- 2026-09-01: audited the complete Go-master `pkg/extension/_import`
  package (two artifacts, 26 lines), including the empty registration landing
  package and its public Bazel target. The pinned package has no generated
  imports, functions, tests, variants, or fixtures; Rust has no external
  extension-import generator/build hook. Recorded the explicit build boundary
  in `receipts/extension_import.md` without inventing a placeholder registry
  entry or crate.

- 2026-09-01: audited the complete Go-master
  `pkg/executor/internal/mpp` package (five artifacts, 1,595 lines),
  including MPP retry/recovery wrappers, bounded result holding, local
  TiFlash task construction/dispatch/cancellation, stream handling, zone
  inference, execution-summary reporting, every package test, and the
  two-shard Bazel target. Rust currently exposes only MPP protocol/client
  contracts, failed-store probing, planner properties, and TiFlash statistics;
  no coordinator or TiFlash execution implementation exists. Recorded the
  explicit boundary in `receipts/executor_internal_mpp.md` without inventing
  an uncalled execution layer.

- 2026-09-01: audited the complete Go-master
  `pkg/executor/internal/calibrateresource` package (four artifacts, 1,613
  lines), including static/dynamic calibration production code, every
  workload/time-window/metric/TiFlash test, failpoint/goleak harness, and the
  two-shard Bazel target. Rust has only the CALIBRATE RESOURCE grammar and no
  dependency-closed execution owner for cluster metrics or quota calculation;
  recorded the explicit boundary in
  `receipts/executor_internal_calibrateresource.md` without inventing an
  uncalled admin executor.

- 2026-09-01: re-audited the complete Go-master
  `pkg/executor/internal/pdhelper` package (four artifacts, 288 lines),
  including PD-region and exact-count fallback, process-global TTL/LRU cache,
  cleanup lifecycle, failpoint/goleak harness, expiry tests, and the two-shard
  Bazel target. Rust already owns the production cache and cluster provider;
  removed the uncalled Rust-only public `get_or_load*`, `len`, and `is_empty`
  helpers and rewrote source-derived tests through `get`/`insert`. Receipt:
  `receipts/executor_internal_pdhelper.md`.

- 2026-09-01: audited the complete Go-master
  `pkg/executor/internal/testutil` package (six artifacts, 909 lines),
  including all aggregate/limit/sort/window defaults, mock physical-plan and
  data-source helpers, typed random chunk generation, OOM-action probe, and
  internal Bazel target. The package has no Go tests and is only test/benchmark
  scaffolding; Rust constructs chunk and planner fixtures directly. Recorded
  the explicit boundary in
  `receipts/executor_internal_testutil.md` without creating a compatibility
  API for uncalled test helpers.

- 2026-09-01: audited the complete Go-master
  `pkg/executor/internal/querywatch` package (four artifacts, 542 lines),
  including query-watch production logic, failpoint/goleak harness, all
  exact/similar/plan/drop tests, and the two-shard Bazel target. Rust has the
  parser/AST grammar and runaway-watch table metadata but no dependency-closed
  runaway manager, resource-group controller, or query-watch executor owner;
  recorded the explicit boundary in
  `receipts/executor_internal_querywatch.md` without inventing an uncalled
  execution layer.

- 2026-09-01: audited all 22 current Go-master `pkg/privilege` artifacts
  (8,415 lines), including manager/connection interfaces, privilege cache and
  manager, LDAP source/tests, JWT source/tests, all Bazel targets, goleak
  harness, and embedded certificate/key fixtures. The Rust
  `tidb-session::PrivilegeRegistry` owner had one measured Unicode mismatch:
  database privilege matching used ASCII-only case folding while Go uses
  Unicode-aware `strings.ToUpper`. Changed the matcher to Unicode folding and
  added `database_matching_folds_non_ascii_like_go_strings_to_upper`, which
  fails before the fix and passes after it. LDAP/JWKS/extension and full
  manager/session/storage lifecycle remain explicit package boundaries. Also
  fixed virtual INFORMATION_SCHEMA privilege-table headers by preserving
  logical mem-table output names and scan-column `orig_name`; planner and
  owner-level regressions now pass (50 passed, 3 ignored). The Go subtree run
  was attempted; one restricted-privilege test requires `--tags=intest` and
  one LDAP timeout assertion exceeded its local timing budget;
  receipt: `receipts/privilege.md`, ExecPlan:
  `docs/operations/privilege-audit-execplan.md`.

- 2026-09-01: audited the complete Go-master `pkg/executor/internal/exec`
  package (five artifacts, 1,610 lines), including the internal Executor
  protocol, panic/killer/tracing/RU-v2 wrappers, clustered-index usage
  reporter, all mock-domain/prepared/partition/global-index tests, and the
  eight-shard Bazel target. Found and fixed a parity gap where Rust skipped
  index-usage reporting for a common-handle table with no primary-index entry;
  Go's zero-value contract reports index 0. The fail-before regression now
  passes, focused Go and Rust tests pass, and `make lint` is green. Receipt:
  `receipts/executor_internal_exec.md`.

- 2026-09-01: audited all 25 current Go-master `pkg/bindinfo` artifacts
  (7,917 lines), including binding/cache production code, automatic binding
  generation, operators, plan evolution/generation, JSON fixtures, package
  tests, nested SQL integration tests, and both Bazel targets. The Rust
  `tidb-session` owner selected bindings correctly but lost the
  `PrevFoundInBinding` marker across the nested prepared-execution boundary.
  Re-armed the marker after cached and fallback execution and added a
  fail-before/pass-after regression for the plan-cache-disabled path. The
  binding owner suite passes; an unrelated parallel HashAgg panic remains in
  the broader prepared-plan-cache selector. Automatic-binding persistence and
  manager/session integration remain explicit boundaries. Receipt:
  `receipts/bindinfo.md`, ExecPlan:
  `docs/operations/bindinfo-audit-execplan.md`.

- 2026-09-01: audited the complete Go-master `pkg/executor/internal/util`
  package (three artifacts, 161 lines), including recursive executor ID
  rewriting, all child-node cases and unknown-type errors, test-only spill
  helpers, and Bazel metadata. Rust selects physical IDs before constructing
  per-table TiKV requests and has no TiFlash MPP caller for this mutator, so
  no dependency-closed owner exists. Recorded the explicit boundary in
  `receipts/executor_internal_util.md` without adding an uncalled protobuf
  rewriter or Rust test scaffold.

- 2026-09-01: audited the complete Go-master `pkg/executor/internal/builder`
  package (two artifacts, 123 lines), including its DAG tree/list helpers,
  metadata fields, error propagation, and internal Bazel target. The Rust
  `tidb-exec::dag_request` owner covers the bounded TiKV list path, while this
  workspace has no TiFlash physical-plan tree builder or index-merge
  non-natural parent-index caller. Recorded the explicit boundary in
  `receipts/executor_internal_builder.md` without inventing an uncalled
  transport or planner path.

- 2026-09-01: audited the complete current Go-master
  `pkg/executor/internal/vecgroupchecker` package (four artifacts, 939
  lines), including its vectorized grouping implementation, common test
  harness, four-shard Bazel target, and all source tests. Rust's
  `tidb-executor::vec_group_checker` is used only by `shuffle` and in-crate
  source-derived tests; its previously public module/type/methods were
  Rust-only API. Narrowed them to `pub(crate)` with all 12 grouping tests
  passing, and recorded `receipts/executor_internal_vecgroupchecker.md`.

- 2026-09-01: completed the Go-master `pkg/executor/internal/applycache`
  inventory (four artifacts, 338 lines) and removed its Rust-only public
  surface. `tidb-executor::apply_cache` is now crate-internal, the uncalled
  `len`/`is_empty` observers and supplemental external test file are gone,
  while the live ApplyExec cache path and source-derived tests remain. The
  Go package tests and Ready gates (`cargo` source tests plus `make lint`) pass;
  receipt: `receipts/executor_internal_applycache.md`.

- 2026-09-01: audited the complete current Go-master `pkg/param` package (two
  artifacts, 44 lines). It is a data-only `BinaryParam`/error declaration;
  the dependency-closed Rust owner is the existing `tidb-protocol` splitter
  sourced from `pkg/server/conn_stmt_params.go`, with 12 focused binary
  parameter tests passing. Recorded `receipts/param.md` without duplicating a
  Rust-only carrier or changing source behavior.

- 2026-09-01: audited the complete current Go-master `pkg/resourcegroup`
  family (14 artifacts, 4,242 lines): the root interface package, the
  failpoint-backed `runaway` checker/manager/record/syncer package, and the
  nine-shard integration-test package. Rust has only resource-group model/DDL
  conversion, process-global RU management, and an unimplemented request
  carrier; it has no dependency-closed runaway owner. Three receipts record
  the exact inventories and explicit boundaries, with failpoint-safe root,
  runaway, and integration tests passing.

- 2026-09-01: audited all 18 current Go-master `pkg/plugin` artifacts (2,734
  lines), including the dynamic/static plugin framework, audit event SPI,
  etcd flush watcher, lifecycle/error paths, the `conn_ip_example` fixture and
  manifest, all source/integration tests, and nested Bazel metadata. Rust has
  only plugin config and a separate auth-registry fragment, so the complete
  framework boundary is recorded in `receipts/plugin.md` without inventing an
  uncalled Rust loader or callback path.

- 2026-09-01: audited all nine current Go-master `pkg/extworkload` artifacts
  (1,326 lines across the manager and nested client packages), including every
  gRPC operation, role/deadline/metrics wrapper, request/error path, source
  test, and both Bazel targets. Rust has external-workload config validation
  and explicit ignored integration hooks but no dependency-closed manager or
  client owner; the complete inventory and boundary are recorded in
  `receipts/extworkload.md` without adding an uncalled protocol path.

- 2026-09-01: audited all six current Go-master `pkg/workloadlearning`
  artifacts (1,154 lines), including statement-stat analysis, binary-plan
  operator extraction, table-cost persistence/cache behavior, both test files,
  and Bazel metadata. Rust has a distinct workload-repository worker plus
  related defaults, but no dependency-closed workload-learning owner; the
  complete inventory and explicit boundary are recorded in
  `receipts/workloadlearning.md` without merging the two execution paths.

- 2026-09-01: audited all five current Go-master `pkg/standby` artifacts
  (1,423 lines), including the idle watcher, activation/exit/checkconn HTTP
  handlers, restart-log and manager-free shutdown paths, classic tests, the
  `nextgen` build-tag test variant, and Bazel metadata. Rust currently has only
  standby configuration/CLI and generic signal fragments, not a
  dependency-closed lifecycle owner, so the complete inventory and explicit
  boundary are recorded in `receipts/standby.md` without inventing a
  controller.

- 2026-09-01: audited all five current Go-master `pkg/metaservice` artifacts
  (708 lines), including PD/etcd discovery, keyspace-group validation,
  optional real-etcd coverage, and Bazel metadata. Rust has transport and
  keyspace-loading fragments but no dependency-closed meta-service group
  owner, so the boundary is recorded in `receipts/metaservice.md` without
  inventing a second routing client.

- 2026-09-01: audited all three current Go-master `pkg/tidbmanager` artifacts
  (212 lines), including the HTTP/TLS client, all success/error tests, and
  Bazel metadata. No Rust crate owns the `/api/tidb/free` manager protocol or
  pod lifecycle contract, so the package is recorded as an explicit boundary
  in `receipts/tidbmanager.md` without adding an uncalled Rust-only path.

- 2026-09-01: audited all nine current Go-master `pkg/telemetry` artifacts
  (2,190 lines), including the complete feature/TTL/window production code,
  26 SQL/counter/failpoint tests, goleak harness, and Bazel dependencies.
  Rust currently has only configuration/bootstrap and planner-classification
  fragments, not the dependency-closed session/domain/metrics/report owner;
  the package is recorded as an explicit boundary in
  `receipts/telemetry.md` without inventing a reporting path.

- 2026-09-01: audited the complete `pkg/lock` and nested
  `pkg/lock/context` Go-master boundaries (five tracked Go/Bazel artifacts,
  232 source/support lines, no tests or fixtures). The parent checker and
  child session interfaces require a shared infoschema/table-lock registry
  that no Rust crate currently owns; existing executor tests record that
  dependency gap. Receipts `receipts/lock.md` and
  `receipts/lock_context.md` preserve the inventory without inventing a
  second authorization path.

- 2026-09-01: audited the complete current Go-master `pkg/structure`
  inventory (8 artifacts, 1,423 lines), including all string/hash/list
  operations, reverse and bounded iterators, error declarations, test
  harness, fixtures (none), and Bazel metadata. The dependency-closed
  `tidb-meta::structure` owner and its raw transaction adapter already match
  the Go key layout, ordering, missing-key behavior, integer encoding, and
  snapshot mutation boundary; all five source-derived structure tests pass.
  The complete inventory and explicit no-change decision are recorded in
  `receipts/structure.md`.

- 2026-09-01: synchronized the rolling parity work with the latest
  `origin/hparser-integration` tip `f99acafe16`. The merged remote batch
  supplies the dependency-ordered `pkg/util` memory/cgroup/cgmon,
  breakpoint, external-sort, memory-alarm, and plan-codec implementations,
  plus the Lightning, DXF, resource-manager, executor, session, and metadata
  consumers recorded in their package receipts. Local batches retain the
  current logutil, collate, and ranger fixes. Conflicting memory callers now
  use the remote `memory::mem_total` cache contract, and the latest receipts
  are selected for packages whose source behavior advanced on the remote
  stack. The merge itself is a synchronization commit; package-specific
  regressions and Ready evidence remain recorded in each receipt.

- 2026-09-01: audited and fixed the complete top-level Go-master
  `pkg/util/logutil`
  inventory (8 artifacts, 1,260 lines) and its separate nested
  `pkg/util/logutil/consistency` package (2 artifacts, 336 lines), including
  every production function, source test, goleak harness, Bazel dependency,
  and Rust logger/hex/rotation owner file. The existing `tidb-util::logutil`
  owner retains the prior focused logger regressions and now forwards Go's
  `FileLogConfig.MaxDays` to age-aware rotation, with a focused regression for
  expired backups and invalid lookalikes. Go normal/race suites and the full
  `tidb-util` test suite pass. gRPC/OpenTracing/runtime trace hooks and the
  MVCC consistency reporter remain explicit integration boundaries with no
  dependency-closed Rust owner; receipts are recorded in
  `receipts/util_logutil.md` and `receipts/util_logutil_consistency.md`.

- 2026-09-02: refreshed the complete current Go-master `pkg/util/dbutil`
  boundary at `c6054025ed4c32ab3672a2a24ea46892714d21ec`: all seventeen
  artifacts (2,518 lines across the public utility and nested `dbutiltest`
  helper), every SQL-mock/table/index/retry/variable test, and all build
  metadata were re-read and validated in both worktrees. Rust has independent
  SQL, privilege, stats, table-mode, and retry fragments but no dependency-
  closed `dbutil` owner; the Ready receipt and living plan are recorded in
  `receipts/util_dbutil.md` and `operations/util-dbutil-audit-execplan.md` with
  no speculative Rust behavior.

- 2026-09-02: refreshed the complete current Go-master `pkg/util/mock`
  boundary at `c6054025ed4c32ab3672a2a24ea46892714d21ec`: all ten artifacts
  (1,318 lines), including the broad context, fake transaction, KV client and
  store, iterator, metrics mock, `!codes` constructor variant, source tests,
  benchmark, harness, and Bazel dependencies, were re-read and validated in
  both worktrees. Rust has only crate-local trait-specific mocks, not a
  dependency-closed package owner; the Ready receipt and living plan are
  recorded in `receipts/util_mock.md` and
  `operations/util-mock-audit-execplan.md`, with no speculative Rust behavior.

- 2026-09-01: audited all ten current Go-master `pkg/util/schemacmp`
  artifacts (3,293 lines), including charset/collation, lattice, table/type
  production files, all nine source tests, and Bazel metadata. The existing
  `tidb-schemacmp` crate is dependency-closed and its aggregate harness passes
  all nine source tests; the complete inventory and current-master parity
  receipt are recorded in `receipts/util_schemacmp.md`.

- 2026-09-02: re-audited all ten `pkg/util/schemacmp` artifacts at current Go
  master `c6054025ed4c32ab3672a2a24ea46892714d21ec` (3,293 lines). The
  dependency-closed `tidb-schemacmp` owner and nine source-derived tests remain
  aligned with no Go source delta or Rust-only duplicate behavior. Details are
  in `receipts/util_schemacmp.md` and
  `docs/operations/util-schemacmp-audit-execplan.md`.

- 2026-09-01: revalidated the complete ten-artifact `pkg/util/table-filter`
  package against current Go master. The concrete `ColumnFilterRules` API,
  ASCII regexp authority, source test rows, and Rust consumer wiring are
  unchanged from the earlier Ready batch; the receipt now pins
  `origin/master` explicitly and no further source fix is needed.

- 2026-09-01: audited all thirteen current Go-master `cmd/importer`
  artifacts (1,812 Go lines plus the 155,728-byte `stats.json` fixture),
  including the CLI README/configuration, complete generator/parser/database
  sources, the decimal-format test, and Bazel binary/test targets. Rust has
  separate SQL `IMPORT INTO` and BR restore implementations but no
  dependency-closed owner for this standalone legacy generator command. The
  complete boundary is recorded in `receipts/cmd_importer.md`; no speculative
  Rust behavior was added.

- 2026-09-01: revalidated the six-artifact `pkg/util/sys/storage` inventory
  against current Go master, including POSIX, Windows, unsupported-target,
  test, harness, and Bazel variants. Source is unchanged from the earlier
  Ready transcreation; the Rust storage-capacity owner and focused platform
  mapping remain valid. The receipt now records `origin/master` explicitly;
  no additional fix is needed.

- 2026-09-01: audited and Ready-validated the six-artifact
  `pkg/util/sys/linux` package, including Linux, Windows, unsupported-target,
  source-test, goleak harness, and Bazel selections. The existing
  `tidb-util::sys::linux` owner matches OS identity, affinity, and Unix peer
  credentials and is wired into the server affinity startup path; no Rust-only
  production behavior or missing dependency-closed Go behavior was found. The
  complete inventory and host/platform risk notes are recorded in
  `receipts/util_sys_linux.md`.

- 2026-09-02: refreshed both current Go-master `pkg/util/injectfailpoint`
  artifacts (90 lines) at `c6054025ed4c32ab3672a2a24ea46892714d21ec`
  in full. Its five exported helpers are named DXF
  failpoint/random-error test infrastructure; Rust has no dependency-closed
  failpoint registry or matching production consumer. The explicit boundary is
  recorded in `receipts/util_injectfailpoint.md`, with the dedicated ExecPlan
  at `docs/operations/util-injectfailpoint-audit-execplan.md`; no Rust-only
  replacement was added.

- 2026-09-02: refreshed the complete two-artifact Go-master
  `pkg/util/breakpoint` package at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec` (47 lines: public Bazel target and the typed
  failpoint-backed session callback). Rust still has no failpoint runtime or
  session-context hook, so the package remains explicitly unclaimed. The
  current-authority receipt is `receipts/util_breakpoint.md`, with the
  dedicated ExecPlan at `docs/operations/util-breakpoint-audit-execplan.md`.

- 2026-09-01: revalidated the complete four-artifact
  `pkg/util/column-mapping` package against current Go master. The source,
  tests, README, and Bazel target are unchanged from the pinned transcreation;
  Rust's sole `tidb-util::column_mapping` owner retains the exact source
  expression/type/error behavior and only the two documented source-derived
  regressions. The receipt now pins `origin/master` explicitly; no new source
  change is needed.

- 2026-09-01: audited all eight current Go-master `pkg/util/importer`
  artifacts (1,081 lines: configuration, DDL/index parser, typed random and
  unique SQL-literal generation, MySQL lifecycle, worker batches, and the
  Bazel target). Rust has independent SQL `IMPORT INTO` and BR restore paths
  but no dependency-closed owner of this standalone command utility, so the
  package remains explicitly unclaimed with no speculative Rust behavior.
  The complete inventory and boundary are recorded in
  `receipts/util_importer.md`; `cmd/importer` remains a separate command
  boundary to audit.

- 2026-09-02: revalidated the complete current Go-master `pkg/util/skip` at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec` and the existing
  `pkg/util/syncutil` inventory at the same pin (five artifacts, 130 lines
  total), including
  the `deadlock`/`!deadlock` build-tag variants and all exported test/lock
  helpers. Rust has no dependency-closed replacement for Go's test-flag
  helpers or package-wide lock type identity/deadlock detector. The explicit
  Go-only boundaries are recorded in `receipts/util_skip.md` and
  `receipts/util_syncutil.md`, with package ExecPlans at
  `docs/operations/util-skip-audit-execplan.md` and
  `docs/operations/util-syncutil-audit-execplan.md`; no source changed.

- 2026-09-01: audited both current Go-master `pkg/util/regionsplit` artifacts
  (`BUILD.bazel` and `split_handle.go`, 256 lines total) in full, including
  all production helpers and their DDL/executor call sites. Rust currently
  owns only lower-level table-key encoders, split transport APIs, and split
  policy metadata; it has no dependency-closed owner for the Go package's
  integer/common-handle/index key derivation and typed-error contract. The
  complete inventory and explicit unclaimed boundary are recorded in
  `receipts/util_regionsplit.md`; the package source is unchanged at
  `origin/master`.

- 2026-09-01: re-audited all four current Go-master `pkg/util/kvcache`
  artifacts (including `main_test.go` and every source LRU case) after finding
  a rolling delta that invalidated the pinned receipt. Go master adds
  `SimpleLRUCache.Peek`, a non-promoting O(1) lookup; the native
  `tidb-kvcache` owner now exposes `peek` and the source-shaped LRU test
  verifies key 4 remains newest after reading key 2. The receipt is updated
  in `receipts/util_kvcache.md`; all eight Rust owner tests pass.

- 2026-09-01: refreshed the complete `pkg/util/naming` inventory against
  current Go master `c6054025ed4c32ab3672a2a24ea46892714d21ec`. Its
  production, test, and Bazel files remain behaviorally identical; only the
  active worktree's older `OWNERS` routing differs. The Rust validator owner
  and its two consumers remain aligned, with the current source/owner
  validation recorded in `receipts/util_naming.md` and
  `docs/operations/util-naming-audit-execplan.md`.

- 2026-09-01: audited both current Go-master `pkg/util/tableutil` artifacts
  (58 lines: interface/global factory and Bazel library), confirming there are
  no source tests, fixtures, generated/platform variants, or nested packages.
  Rust's temporary-table behavior is distributed across model, session,
  transaction, executor, and DDL owners without one dependency-closed
  `TempTable` object/factory seam. The complete boundary is recorded in
  `receipts/util_tableutil.md`; the package has no standalone fix to test.

- 2026-09-01: audited all seven Go-master `pkg/util/profile` artifacts,
  including the 1,206-byte pprof fixture, two production files, three test
  files, and Bazel fixture glob. The Rust workspace only preserves the six
  performance-schema table names; it has no pprof/flamegraph collector,
  CPU-profiler lifecycle, goroutine parser, or ordinary profile-table
  executor. The complete inventory and explicit dependency boundary are
  recorded in `receipts/util_profile.md`; both Go source tests pass with the
  required `intest` tag where applicable.

- 2026-09-01: audited all three Go-master `pkg/util/etcd` artifacts (140
  lines including the embedded-etcd namespace test and Bazel target). The
  Rust PD etcd owner already had the KV transport, but its single-key delete
  omitted Go's per-attempt deadline and bounded retry contract. Added the
  source constants, timeout-carrying delete command, deterministic retry
  helper/regression, and wired server-info cleanup to five attempts with its
  one-second deadline. `SetEtcdCliByNamespace` remains an explicit boundary
  because no Rust startup path requires a mutable clientv3 namespace wrapper.
  Inventory and validation are recorded in `receipts/util_etcd.md`.

- 2026-09-02: refreshed the complete `pkg/util/regionsplit` boundary against
  Go master `c6054025ed4c32ab3672a2a24ea46892714d21ec`: two artifacts, 256
  lines, 12 declarations, and no tests/fixtures/variants. Rust has only
  lower-level key encoders, so the high-level DDL/executor split-key owner
  remains explicitly unclaimed. Added the dedicated audit ExecPlan and Ready
  compile evidence.

- 2026-09-01: audited the complete Go `pkg/util/dbterror/plannererrors`
  package at `origin/master`
  `db35d47066648fe73abce6318d53fc625df51490` against the Rust
  `tidb-error::plannererrors` owner on `origin/hparser-integration`. The
  package has exactly `planner_terror.go`, `errors_test.go`, and `BUILD.bazel`:
  98 source prototypes and the exact 59-entry `TestError` are represented by
  98 Rust statics, an all-entry initialization guard, and the source-derived
  test. No source drift or missing behavior was found; the inventory and Ready
  gates are recorded in `receipts/util_dbterror_plannererrors.md`.

- 2026-09-01: audited every production, test, harness, and build artifact in
  Go-master `pkg/util/execdetails` (8 artifacts, 5,919 Go lines) against all
  four Rust `tidb-exec` owners. The Rust files are explicitly `SEED`s and
  remain open across context plumbing, client-go/protobuf/resource-manager
  details, Prometheus side effects, zap fields, read-pool and row-summary
  evidence, hash-state/Explain-RU types, and ordinary executor integration.
  Focused Go and Rust owner tests pass, but the package is not dependency
  closed; no partial production fix or duplicate regression carrier was added.
  The complete inventory and boundary are recorded in
  `receipts/util_execdetails_audit.md`.

- 2026-09-02: refreshed the complete `pkg/util/execdetails` boundary against
  Go master `c6054025ed4c32ab3672a2a24ea46892714d21ec`: the same eight
  artifacts and 5,919 lines now have a current-authority receipt, exact
  detached Go test evidence, and a dedicated ExecPlan. The three-file delta
  adds read-pool, row/summary coverage, scan-byte, hash-state, and Explain-RU
  behavior that still crosses context, protobuf, metrics, and executor seams;
  no partial Rust fix was introduced. See
  `receipts/util_execdetails_audit.md` and
  `docs/operations/util-execdetails-audit-execplan.md`.

- 2026-09-02: restored the current Go-master `pkg/util/execdetails` source
  delta as one Go-package batch at
  `78cac443a4f46c13bfe27eb247b5c80657952547`. The complete eight-artifact
  inventory remains the atomic boundary. Added read-pool detail propagation,
  checked runtime row/summary evidence, Analyze scan-byte estimation,
  hash-state lifecycle, Explain-RU types, and focused source regressions. The
  package suite passes, while the changed `RecordCopStats` API leaves the
  pending `pkg/distsql` and `pkg/store/copr` callers as an explicit integration
  boundary; no Rust seed owner was modified. Ready lint/Bazel gates are
  required before commit, with Bazel unavailable locally. Details are in
  `receipts/util_execdetails_audit.md`.

- 2026-09-01: completed the Go-master `pkg/util/sqlkiller` package as one
  three-artifact unit. Ported the concurrent-reset lock/swap ordering and
  source failpoint interleaves, made pre-reset receivers close instead of
  receiving a Rust-only reset token, and added the focused race regression.
  The owner, affected memory/executor/server consumers, formatting, lint, and
  locked Ready gates are recorded in `receipts/util_sqlkiller.md`.

- 2026-09-01: completed the Go-master `pkg/util/serialization` package as one
  four-artifact unit. Added the missing length-prefixed VectorFloat32
  serializer/decoder to the native spill owner and a focused empty/non-empty
  round-trip regression. The complete source inventory, Go build blocker,
  Rust consumer check, lint, and Ready gates are recorded in
  `receipts/util_serialization.md`.

- 2026-09-02: completed the thirteen-artifact Go-master `pkg/ttl/cache`
  inventory (3,566 lines), including every TTL task/table cache source, test,
  and BUILD input. Restored dependency-closed task scan-range
  `codec.EncodeKey`/`codec.Decode` behavior through `tidb-codec`, decoded
  `TTLTaskState` with Go-compatible `serde_json` defaults and error handling,
  and added focused range/JSON regressions. The cache owner suite (20 tests),
  Go tagged package suite (19.402s), Rust all-target check, formatting, lint,
  and diff gates are recorded in `receipts/ttl_cache.md`; only the two
  info-schema `Update` traversals remain explicit boundaries. Published as
  `cca2f7711b4ac393d8ef0d979dda8accd9c3d243`; local, tracking, and remote SHAs
  match after the push/pull.

- 2026-09-02: audited the complete four-artifact Go-master
  `pkg/ttl/sqlbuilder` package (1,510 lines), including every SQL formatter,
  builder/generator branch, test, and BUILD input. Restored Go's distinction
  between a nil and non-nil empty continuation key in the Rust scan generator;
  the focused regression failed before the fix and the six-test Rust owner plus
  tagged Go suite now pass. The package receipt and ExecPlan record the
  explicit parser-driver and arbitrary-byte writer boundaries. Published as
  `8bf78c07e0b82a8738a1e8e5cd1e222a1c032fd3`; local, tracking, and remote SHAs
  match after the push/pull.

- 2026-09-01: completed the Go-master `pkg/util/stringutil` package as one
  four-artifact unit. The rolling source delta changes `CompileLike2Regexp`
  from an implicit backslash escape to an explicit escape byte; Rust now
  forwards that byte into the canonical pattern compiler. The source-shaped
  default suite, custom-escape regression, Rust expression consumer checks,
  benchmark compilation, formatting, lint, and Ready gates are recorded in
  `receipts/util_stringutil.md`.

- 2026-09-01: completed the Go-master `pkg/util/table-filter` package as one
  ten-artifact unit. Exported the concrete `ColumnFilterRules` and
  `ParseColumnFilterRules` API, kept the interface-returning parser as a
  delegating compatibility entry point, and added the focused concrete-rule
  regression. The complete source/owner inventory, contract checks, formatting,
  lint, and Ready gates are recorded in `receipts/util_table_filter.md`.

- 2026-09-01: completed the Go-master `pkg/util/chunk` delta as one
  twenty-nine-artifact package audit. The source adds `Chunk.UsedMemoryUsage`,
  which reports current buffer lengths rather than retained capacities; the
  Rust `tidb-chunk` owner now exposes the equivalent per-column and aggregate
  methods and extends the source memory regression through reset. The full
  Go source/test/build inventory and Rust owner mapping are recorded in
  `receipts/util_chunk_audit.md`. Focused Rust, executor-consumer, formatting,
  lint, and diff gates pass; broad Go/Rust chunk sweeps retain unrelated
  spill-path/failpoint and timing failures in the receipt.

- 2026-09-01: completed the Go-master `pkg/util/codec` delta as one
  twelve-artifact package audit. Go master makes `Encoder` key-only; Rust's
  encoder value/hash methods were Rust-only and are removed, with free value
  and hash functions retaining one non-collating path. The executor and
  expression consumers now call those package functions, and a raw-string
  value regression covers the mode split. The complete source/owner inventory
  and 163-test Ready evidence are recorded in
  `receipts/util_codec_audit.md`.

- 2026-09-01: audited all seven Go-master `pkg/sessionctx/vardef` artifacts
  (3,046 lines, including BUILD/OWNERS, runtime and test files) against the
  `tidb-vardef` owner. Added the 13 current system-variable names, 7 defaults,
  and 2 bounds introduced since the Rust extraction point; removed the stale
  Rust-only `DefTiDBMergePartitionStatsConcurrency`; and added source-derived
  regressions for every added literal. The mutable runtime globals, SysVar
  registry, SessionVars, and slow-log/session tests remain an explicit
  dependency boundary. Complete inventory and Ready evidence are recorded in
  `receipts/sessionctx_vardef_audit.md`.

- 2026-09-01: audited all 14 Go-master `pkg/util/memory` artifacts (11,388
  lines, including BUILD, platform-sensitive memory discovery, stress tests,
  and benchmarks) against the `tidb-util::memory` owner. Replaced the removed
  Unicode `HashStr` surface with Go's length-prefixed `DigestIDBuilder`, added
  the `InvalidDigestID` sentinel guard to digest-cache lookup/update, and
  covered component-boundary/order and no-cache-creation regressions. The
  broader process/global-arbitrator and tracker transition deltas remain an
  explicit dependency boundary. Complete inventory and Ready evidence are
  recorded in `receipts/util_memory_audit.md`.

- 2026-09-01: audited all 22 Go-master `pkg/util/stmtsummary` artifacts
  (11,214 lines, including both BUILD targets, v1/v2 production and test
  files, benchmarks, and the nested table-test harness). Compared the
  `8bab3c26d7`, `655769534b`, and `381ac705f9` source deltas: Rust retains the
  stale-interval evicted-row guard and lock-safe window/record ownership,
  adds IA execution-count tracking through v1/v2 readers, eviction rollups,
  merges, chunk paths, and JSON, and fixes v1 plan-encoding fallback plus
  first-statement internal-summary initialization. Source-shaped regressions
  cover IA-vs-ordinary executions, history/chunk/JSON paths, plan failure, and
  internal-only cleanup. The v2 history reader, logger, table tests, and the
  executor/infoschema/planner integration remain an explicit dependency
  boundary; the complete inventory and Ready evidence are in
  `receipts/util_stmtsummary_audit.md`.

- 2026-09-02: completed the `pkg/util/stmtsummary` root-package parity batch
  as one Go-package commit. Restored current Go-master behavior for
  lock-safe evicted-count snapshots, stale-interval filtering, IA execution
  counts and column order, internal-query LRU cleanup, and plan-encoding error
  fallback; added focused regressions for each path and updated the v1 BUILD
  shard metadata. The failpoint-aware focused and full root suites plus
  `make lint` passed; `make bazel_prepare` remains blocked by the missing local
  Bazel executable. The v2 and executor/infoschema/planner surfaces remain the
  explicit package boundary recorded in `receipts/util_stmtsummary_audit.md`.

- 2026-09-02: completed the direct `pkg/util/stmtsummary/v2` package as one
  Go-package batch. Restored IA execution-count fields and column factories,
  persisted JSON/history readback, rotate-safe eviction begin-time snapshots,
  and record-locked internal-query cleanup; added focused regressions for the
  IA, LRU, and concurrent-rotate paths and updated its BUILD shard metadata.
  The pre-fix suite failed on the missing Go symbols; post-fix failpoint-aware
  focused and full v2 suites passed. The nested `v2/tests` table package and
  SQL integration consumers remain the explicit boundary in the receipt.

- 2026-09-02: applied the latest `origin/master`
  `78cac443a4f46c13bfe27eb247b5c80657952547` `pkg/util/stmtsummary` delta as
  one package-scoped batch. Restored newest-first history retention with
  chronological output, execution-count average columns, empty-table-name
  filtering, newest-entry history reset, normalized v2 record text, and the
  rotation-safe, descriptor-bounded v2 history reader. Added source-derived
  regressions and updated all affected BUILD shard metadata. Root and v2
  failpoint-aware focused/full suites pass; the v2 table harness is blocked by
  an unrelated unfinished statistics edit that does not compile. `make lint`,
  Rust formatting, and diff checks are required Ready gates; `make
  bazel_prepare` remains blocked by the unavailable local Bazel executable.
  Details are in `receipts/util_stmtsummary_audit.md`.

- 2026-09-01: audited all six Go-master `pkg/util/parser` artifacts (581
  lines) against the complete `tidb-parser::util_parser` owner. Go's
  `StmtNode.Accept` to `ast.Walk` migration changes visitor signatures but
  not traversal semantics; Rust already uses a non-replacing visitor. Added a
  multi-table default-database regression and recorded the exact inventory and
  Ready gates in `receipts/util_parser_audit.md`.

- 2026-09-01: audited all 46 Go-master `pkg/util/topsql` artifacts (14,503
  Go/Bazel lines, including profiler, gRPC, mocks, generated Top-RU cases,
  benchmarks, and all 144 source tests). The dependency-closed Rust owners
  now enforce Go's CAS statement-stats cap and lock normalized SQL/plan map
  admission with `take`; focused concurrent regressions pass. The reporter
  channel backpressure and single-target panic-recovery changes are recorded
  as explicit boundaries because Rust has no corresponding worker or gRPC
  owner. Complete inventory and validation evidence are in
  `receipts/util_topsql_audit.md`.

- 2026-09-02: completed the standalone `pkg/util/topsql/reporter/metrics`
  leaf in one package batch. The Go backpressure-drop counter and its exact
  label binding are restored, with a focused counter-increment regression and
  full package test. The parent reporter worker remains an explicit
  cross-package boundary with no dependency-closed Rust owner. Ready gates
  include `make lint` and `git diff --check`; `make bazel_prepare` is blocked
  only by the missing local Bazel executable. Updated
  `receipts/util_topsql_audit.md`.

- 2026-09-01: audited all four Go-master `pkg/util/generatedexpr` artifacts
  (181 lines) against `tidb-model::generated_expr`. The Go-master visitor
  migration is API-only and leaves parsing/name-resolution semantics intact;
  Rust's existing five tests already cover the complete leaf behavior. No
  speculative production adapter was added because the surrounding
  `pkg/meta/model` owner remains a seed; the complete inventory and boundary
  are recorded in `receipts/util_generatedexpr_audit.md`.

- 2026-09-01: audited all 25 Go-master `pkg/expression/aggregation` artifacts
  (4,193 lines). Go's `max_count`/`min_count` feature crosses parser,
  descriptor, aggregate runtime, hash aggregation, planner, protobuf, and KV
  owners. Rust's current `tidb-expr` aggregation code is explicitly a seed and
  lacks the dependency closure for a safe leaf patch; the complete inventory,
  ignored source tests, and implementation boundary are in
  `receipts/expression_aggregation_audit.md`.

- 2026-09-01: implemented the dependency-closed Go-master `pkg/parser/mysql`
  `OPERATE VIEW` delta as one coordinated Rust batch. The 15-artifact,
  4,847-line parser/mysql inventory and the seven-artifact, 1,347-line
  `pkg/meta/metadef` inventory were read in full before editing. Rust now
  synchronizes the generated lexer keyword catalogs, parser restoration,
  privilege bit/name/set/column maps, global/database/table masks, bootstrap
  root row, executor account load/write columns, and source-derived table-info
  fixture. Focused parser, mysql, bootstrap, executor round-trip, metadata,
  and session regressions pass except for the existing unrelated infoschema
  header assertion. The exact inventory, fail-before/pass-after evidence,
  Ready commands, and explicit version285/materialized-view scheduler
  boundaries are recorded in `receipts/parser_mysql_operate_view_audit.md`.

- 2026-09-01: completed the rolling Go-master `pkg/kv` checker delta after
  reading all 30 package artifacts (5,145 Go/Bazel lines). Rust's native
  request-support matrix now accepts `ExprType_MinCount` (3022) and
  `ExprType_MaxCount` (3023), moves them into the supported TIPB disposition,
  and executes the source-derived regression that was previously ignored.
  The package-level test inventory and remaining benchmark/integration
  boundaries are recorded in `receipts/b065.md`.

- 2026-09-01: re-audited all 61 current Go-master `pkg/types` artifacts
  (56 root files plus the five `parser_driver` files) against the existing
  `tidb-datatype`/`tidb-ast` owners. Rust already contained the checked vector
  overflow behavior; it now adds the missing `ExplainFormatRU` literal and
  14-entry validator order with a focused regression. The parser-driver
  `AcceptInPlace` API remains an explicit AST-driver boundary. Inventory and
  Ready evidence are recorded in `receipts/types_explain_format_audit.md`.

- 2026-09-01: audited the complete Go `pkg/util/dbterror/exeerrors` package at
  `origin/master` `db35d47066648fe73abce6318d53fc625df51490` against the Rust
  owner on `origin/hparser-integration`. The package has exactly `errors.go`
  and `BUILD.bazel`, no functions or tests, and an 82-prototype public error
  catalog. Go master's three dual-password additions were already present in
  the Rust owner and its complete 82-row Go-generated fixture, so no duplicate
  production or test behavior was added. Inventory and Ready evidence are in
  `receipts/util_dbterror_exeerrors.md`.

- 2026-09-02: refreshed the complete Go `pkg/util/plancodec` package at
  `origin/master` `c6054025ed4c32ab3672a2a24ea46892714d21ec` against the Rust
  owner on `origin/hparser-integration`. The seven-artifact package has no
  generated, platform, fixture, or benchmark variant. Go master appends the
  stable `Analyze` plan type at physical ID 64; the checkout's Go table was
  missing the source constant and both switch cases even though Rust already
  carried the entry. Restored the Go mapping and focused two-way regression;
  complete inventory and Ready validation are recorded in
  `receipts/util_plancodec.md`.

- 2026-08-31: completed pinned `pkg/util/disk` as one five-artifact package.
  Added the missing temp-directory lifecycle and global-tracker constructor,
  wired server/chunk/memory-alarm consumers to those canonical owners, moved
  the cross-package spill integration seam out of `tidb_util::disk`, and
  removed its duplicate lease/sweep implementation plus six Rust-only tests.
  The complete inventory and WIP gates are in `receipts/util_disk.md`.

- 2026-08-31: re-audited and completed pinned `pkg/util/tiflash` as one
  two-artifact package. The existing open native-integer `ReplicaRead` owner
  and live distsql request propagation remain; removed a Rust-only type alias,
  five adapter methods, three duplicated vardef string constants, const-only
  API capability, and the source-test-free package's three supplemental tests.
  The owner now imports the canonical vardef spellings exactly like Go. The
  complete inventory and WIP gates are in `receipts/util_tiflash.md`.

- 2026-09-02: re-audited the complete two-artifact `pkg/util/tiflash`
  boundary at current Go master `c6054025ed4c32ab3672a2a24ea46892714d21ec`
  (81 lines, no source tests or hidden variants). The dependency-closed
  `tidb-txnkv::tiflash` owner and distsql projection remain aligned after the
  earlier Rust-only surface removal; current and detached Go probes plus the
  focused distsql consumer test pass. The Ready receipt and living plan are in
  `receipts/util_tiflash.md` and
  `operations/util-tiflash-audit-execplan.md`.

- 2026-09-02: completed and re-audited the unclaimed Go `pkg/util/cgroup`
  package against latest master `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
  Read and mapped all nine production, test, platform, and Bazel artifacts;
  corrected raw controller-count and mount-separator parsing, preserved the
  pinned hybrid memory-usage fallback, and changed CPU quota conversion to
  retain Go's signed `-1` unsupported sentinel. Moved host-memory/process-RSS
  helpers out of the cgroup owner into `tidb-util::memory::process`, removed
  Rust-only scheduler recommendation wrappers, and added complete source
  memory/CPU fixture matrices plus public unsupported-platform checks. The
  inventory, integration boundary, and host validation limits are recorded in
  `receipts/util_cgroup.md` and
  `docs/operations/cgroup-audit-execplan.md`; the package commit was pushed to
  `hparser-integration`, and the latest authority remains unchanged.

- 2026-08-31: re-audited and completed pinned `pkg/statistics/util`. Corrected
  the shared JSON model to retain protobuf scalar zero fields and count them in
  generated-message `Size()` equivalents, changed predicate ordering to Go's
  unstable `slices.SortFunc` contract, and removed the source-absent predicate
  constructor and four-test carrier. Also completed atomic audits of the
  two-artifact `pkg/util/breakpoint` and `pkg/util/compress` leaves. Breakpoint
  remains explicitly unclaimed because it needs the ordinary session value
  store and both executor failpoint injection points. The compression audit
  was selected for completion in the next batch. Receipts are
  `receipts/statistics_util.md`, `receipts/util_breakpoint_audit.md`, and
  `receipts/util_compress_audit.md`.

- 2026-09-01: completed the currently unclaimed pinned Go `pkg/util/compress`
  package as one two-artifact unit. Added the process-wide pooled gzip reader
  and writer owner in `tidb-util`, including reset/close/discard and invalid
  header behavior, and routed statistics JSON block framing through both pools.
  Removed the executor's direct `flate2` dependency so the generic owner is
  the only compression implementation on that path.
  The writer reset path suppresses flate2's Drop-time trailer so an unfinished
  stream is discarded like Go's `gzip.Writer.Reset`. The absent Rust
  `pkg/ingestor/ingestctrl` owner is recorded as an explicit future integration
  boundary rather than a fabricated consumer. The complete inventory,
  focused regressions, and validation receipt are in
  `receipts/util_compress_audit.md`.

- 2026-09-01: completed inventory and implementation for the currently
  unclaimed pinned Go `pkg/util/sys/storage` package. Its Linux/macOS `statfs`,
  Windows `GetDiskFreeSpaceEx`, and unsupported-platform `math.MaxInt64`
  variants are owned by `tidb-util::sys::storage`; its POSIX path now uses
  `statfs` (matching Go's `syscall.Statfs`) rather than the prior
  behaviorally different `statvfs`. Go's direct startup quota
  check is represented by the Rust `open_spill_storage -> SpillStorage::open`
  path. Removed the Rust-only in-module fallback and missing-path test
  carriers, restored the sole Go source test in an external carrier, and added
  focused POSIX arithmetic and error-boundary regressions. The complete
  six-artifact Go
  inventory and validation status are recorded in
  `receipts/util_sys_storage.md` and
  `docs/operations/storage-audit-execplan.md`; the Ready profile passes using
  command-local bundled Go/OpenSSL tooling, with Windows and unsupported-target
  execution explicitly unrun. The package commit is pushed.

- 2026-08-29: completed the pinned Go
  `pkg/statistics/handle/usage/indexusage` package in
  `tidb-stats-handle-usage-indexusage`. Restored the real `model.TableInfo`
  GC boundary and Go's year-1 zero time, moved all four source tests and the
  parallel benchmark into the owner, restored the exact 64 × 100,000
  concurrent test workload, and removed flattened public constants, test
  accessors, map snapshots, copied-map reporting, three duplicate tests, five
  supplemental tests, and the parent package's direct-closure GC workaround.
  The 2026-09-02 Go-master re-audit read all 568 lines and found no
  source-vs-owner behavior gap or Rust-only production path; the native
  benchmark harness adaptation is documented. Inventory and Ready gates are
  in `receipts/statistics_handle_usage_indexusage.md` and
  `docs/operations/statistics-handle-usage-indexusage-audit-execplan.md`.
- 2026-08-29: completed the pinned Go
  `pkg/statistics/handle/usage/collector` package in the distinct
  `tidb-stats-handle-usage-collector` owner. Moved its three source tests out
  of the aggregate statistics crate, removed a source-absent capacity test,
  preserved repeated worker starts and the source channel behavior after
  close, and rewired index usage to consume the package directly. The
  2026-09-02 Go-master re-audit read all 289 lines and found no source-vs-owner
  behavior gap or Rust-only production path; the nil `closeCh` close regression
  remains executable. Inventory and Ready gates are in
  `receipts/statistics_handle_usage_collector.md` and
  `docs/operations/statistics-handle-usage-collector-audit-execplan.md`.
- 2026-08-29: completed the pinned Go
  `pkg/statistics/handle/internal` support package in
  `tidb-stats-handle-internal`. Removed Rust's opaque, caller-encoded table
  snapshot carrier and its three source-absent tests. The replacement compares
  real `tidb_stats::Table` values with Go's count, textual-histogram,
  CMSketch, nil-aware TopN, and existence-map semantics. Inventory and WIP
  gates are in `receipts/statistics_handle_internal.md`.
- 2026-08-30: audited the complete pinned Go
  `pkg/statistics/handle/metrics` package. It remains unclaimed because its
  shared 60-artifact `pkg/metrics` collector owner is absent. The wired private
  collectors remain seed integration only; two source-absent leaf tests were
  removed. Inventory is in
  `receipts/statistics_handle_metrics_audit.md`.
- 2026-08-30: completed the pinned two-artifact `pkg/domain/metrics` package.
  All seven historical-stat and plan-replayer handles bind their shared
  collectors together, and the live domain/server consumers increment or set
  them at Go's generation, dump, channel, and collection points. Inventory is
  in `receipts/domain_metrics.md`.
- 2026-08-29: completed the pinned Go `pkg/statistics/handle/logutil`
  package in `tidb-stats-handle-logutil`. Its four exported constructors now
  compose the completed shared background/error-verbose and sampled logger
  facilities with the exact statistics category, five-/ten-minute windows,
  and first-one policy. The source package has no tests, so none were added.
  Inventory and WIP gates are in `receipts/statistics_handle_logutil.md`.
- 2026-08-29: completed the adjacent pinned Go
  `pkg/statistics/handle/util/test` support package. Replaced the detached
  string predicate and its two non-Go tests with a distinct matcher over the
  real TiKV typed request context, and changed the main package's `StatsCtx`
  from a custom marker to that ordinary context so request-source extraction
  observes `internal_StatsForegroundPriority`. Inventory and WIP gates are in
  `receipts/statistics_handle_util_test.md`.
- 2026-08-29: completed the pinned Go `pkg/statistics/handle/util` package as
  one `tidb-stats-handle-util` owner. Removed five narrowed `tidb-stats`
  implementations and their supplemental tests, restored the complete process
  tracker, signed lease, worker/session pool facade, versioned table lookup,
  session synchronization, transaction, executor, timestamp, and model-backed
  index behavior, and replaced all four historical ignored package-test gaps
  with executable tests. Complete inventory and WIP gates are recorded in
  `receipts/statistics_handle_util.md`.
- 2026-08-29: completed the pinned Go `pkg/planner/core/resolve` package (two
  production files and `BUILD.bazel`; no package tests). Replaced the unusable
  Arc-of-a-cloned-table key with stable per-AST-occurrence identity matching
  Go's `*ast.TableName` map key, retained shared context across `NodeW` clones,
  and wired the ordinary query planner through `NodeW`. Production catalog
  tables now populate the context with shared `DBInfo`/`TableInfo`, columns,
  and indexes. Removed the stale planner claim that Go's resolve context was
  "unsound" and deliberately dropped, plus the expression documentation that
  still claimed resolve/sqlexec were absent. Broader statement-family support
  remains with its owning planner packages. Complete inventory and WIP gates
  are recorded in `receipts/planner_core_resolve.md`.
- 2026-08-29: completed the pinned Go `pkg/util/sqlexec` main package (two
  production files, one test harness with no functional tests, and
  `BUILD.bazel`). Added its shared Rust owner with the complete restricted and
  ordinary executor, parser, statement, record-set, detach, no-delay-result,
  option, drain, and simple-record-set contracts. Removed expression's empty
  executor marker, metrics reader's local SQL-to-rows trait, and timer's
  pre-drained internal-SQL trait; all now use the ordinary shared interfaces,
  and timer drains the returned record set through `ExecSQL`. Added the
  Go-owned parser parameters and the resolve result-field prerequisite at
  their actual package boundaries. The concrete session implementation and
  separate generated mock package remain later package units rather than
  being included in this claim.
  Complete inventory and WIP gates are recorded in
  `receipts/util_sqlexec.md`.
- 2026-09-02: refreshed the complete Go-master `pkg/util/sli` inventory at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`. Read both source artifacts in
  full (132 lines), confirmed the package is test/fixture/generated/platform
  variant-free, and re-ran the existing source-derived integration regression.
  No Rust-only behavior or missing Go behavior remained in the owner, so this
  boundary is a receipt/ExecPlan refresh only. Details are in
  `receipts/util_sli.md` and `docs/operations/util-sli-audit-execplan.md`.
- 2026-08-29: re-audited the complete pinned Go `pkg/util/slice` package.
  Removed Rust-only `must_use` diagnostics from `Int64sToStrings` and
  `DeepClone`; all production behavior and the sole source test identity were
  already aligned.
- 2026-08-29: re-audited the complete pinned Go `pkg/util/serialization`
  package. Removed Rust-only derives/diagnostics, the unused public
  `MY_DECIMAL_LEN` and `Cursor::remaining`, and public exposure of Go-private
  buffer helpers; FIRST_ROW now crosses the package boundary through the
  exported string serializer/deserializer, as Go does.
- 2026-08-29: re-audited the complete pinned Go `pkg/util/selection`
  package. Removed the Rust-only public `Selectable::is_empty` operation and
  restored the exact four Go test identities; the benchmark-only quickselect
  remains behind its native test-export boundary.
- 2026-09-02: re-audited the complete Go-master `pkg/util/ppcpuusage` package
  at `c6054025ed4c32ab3672a2a24ea46892714d21ec`, which has no test artifact.
  Removed its remaining Rust-only
  signed-overflow regression and `must_use` diagnostic; the production owner
  and statement-summary consumers retain Go's signed wrapping durations.
  Current and exact detached Go probes, the zero-test owner, the production
  consumer, Ready formatting, and diff hygiene pass. Details are in
  `receipts/util_ppcpuusage.md` and
  `docs/operations/util-ppcpuusage-audit-execplan.md`.
- 2026-08-29: re-audited the complete pinned Go `pkg/util/texttree` package.
  Removed Rust-only `must_use` diagnostics, its arbitrary-byte supplemental
  regression, and the corresponding temporary-probe narrative; exactly the
  two Go test identities and both ordinary consumers remain.
- 2026-08-29: re-audited the complete pinned Go `pkg/util/arena` package.
  Removed the remaining Rust-only reset-reuse regression and restored exactly
  the two Go test identities; shared backing and reset reuse remain production
  behavior rather than an extra test surface.
- 2026-09-02: re-audited the complete `pkg/util/arena` inventory against
      current Go master (`c6054025ed4c32ab3672a2a24ea46892714d21ec`). Its four
  artifacts and 202 textual lines are unchanged; the existing safe Rust
  `ArenaBytes` owner still covers shared backing, fitting/fresh allocation,
  reset, and the exact two source test identities. The receipt now records
  current hashes and environment-qualified validation.
- 2026-08-29: re-audited the complete pinned Go `pkg/util/bitmap` package.
  Removed its remaining Rust-only signed-boundary regression and restored the
  exact three Go test identities while retaining the source-derived signed
  length behavior in production.
- 2026-09-02: re-audited the complete `pkg/util/bitmap` inventory against
      current Go master (`c6054025ed4c32ab3672a2a24ea46892714d21ec`). Its four
  artifacts and 282 textual lines are unchanged; the native atomic owner
  still preserves segment numbering, winner-on-CAS semantics, reset/clone,
  signed-width behavior, and the exact three source tests. The receipt now
  records current hashes and environment-qualified validation.
- 2026-09-01: audited both Go-master `pkg/util/breakpoint` artifacts (47
      lines: the public Bazel target and failpoint-backed session callback).
      Rust has no failpoint runtime or session-context breakpoint hook; adding
      a callback registry would be test-only Rust behavior. The package
      remains explicitly unclaimed with no source change. Details are in
      `receipts/util_breakpoint.md`.
- 2026-08-29: re-audited the complete pinned Go `pkg/util/encrypt` package
  against its existing Rust owner. Removed three Rust-only regression cases
  and the extra alias assertion from the source-named suite; the 17 Go test
  identities and source benchmark remain the complete package surface.
- 2026-09-01: refreshed the complete `pkg/util/encrypt` inventory against Go
  master (`0bc44483e3e41a8ea917d4382dc202369468d200`). All eight Go artifacts
  are byte-for-byte unchanged from the prior pin; the existing Rust AES,
  random-access CTR, SQL codec, benchmark, and consumer ownership remains
  dependency-closed. Owner and expression gates pass; encrypted-spill tests
  remain blocked by the unrelated temporary-directory setup helper and are
  recorded as such in `receipts/b021.md`.
- 2026-08-29: audited pinned Go `pkg/util/cpu` and found that an atomic port
  requires the absent `pkg/metrics` EMA gauge plus domain/resource-manager
  lifecycle consumers, in addition to a cross-platform process CPU clock.
  Deferred it as a multi-package dependency rather than landing a standalone
  sampler that could not satisfy the package behavior.
- 2026-08-29: completed the pinned Go `pkg/util/errmsg` package (one
  production file, one source test, and `BUILD.bazel`). The dedicated Rust
  owner, prepared config snapshot, and ordinary ERR-packet consumer already
  matched Go. Removed a supplemental nil assertion and restored exactly the
  five source test identities; both the package suite and live packet-boundary
  regression pass. Complete inventory and WIP gates are recorded in
  `receipts/util_errmsg.md`.
- 2026-08-29: completed the pinned Go `pkg/util/engine` package (one
  production file, one source test, and `BUILD.bazel`). The existing three
  protobuf/PD-HTTP classifiers already matched Go. Replaced the combined
  supplemental Rust matrix with exactly the two Go source identities and
  their five cases each, and registered the package as a standalone Cargo
  test. Complete inventory and WIP gates are recorded in
  `receipts/util_engine.md`.
- 2026-08-29: completed the pinned Go `pkg/util/cteutil` package (one
  production file, one source test, one test harness, and `BUILD.bazel`).
  Restored the explicit closed/open/reference-counted lifecycle in the
  spill-backed Rust storage and wired it through the ordinary physical CTE
  builder and recursive producer. Removed the unused catalog-backed `CteTable`
  relation and all of its DML/DDL/SHOW branches, plus unused row-matrix helpers
  that Go does not expose. The standalone suite now has exactly the six source
  identities; ordinary and spill-backed recursive execution pass. Complete
  inventory and WIP gates are recorded in `receipts/util_cteutil.md`.
- 2026-08-29: completed the pinned Go `pkg/util/cdcutil` package (one
  production file, one external test, one test-support file, and
  `BUILD.bazel`). Added the formerly absent Rust owner against the production
  PD-etcd boundary, including both key generations, state/checkpoint rules,
  safe-TS filtering, grouping, messages, and source logging. The single source
  test identity covers both upstream subtests. Complete inventory and WIP
  gates are recorded in `receipts/util_cdcutil.md`.
- 2026-09-02: refreshed the complete `pkg/util/cdcutil` inventory against
  current Go master (`c6054025ed4c32ab3672a2a24ea46892714d21ec`). All four
  artifacts and 489 textual lines are unchanged; the `tidb-domain::cdcutil`
  owner and embedded-etcd source matrix still cover both key generations,
  checkpoint/state rules, grouping, and message output. The receipt and
  package ExecPlan now record current authority and Ready validation;
  BR/Lightning/executor composition roots remain outside this package claim.
- 2026-08-29: completed the pinned Go `pkg/util/resourcegrouptag` package
  (one production file, one source test, one test harness, and `BUILD.bazel`).
  Split its decoder, label classifier, and first-key extraction out of the
  mixed Rust `pkg/kv` builder module; the builder and decode hook remain with
  their actual Go owner. Reduced the package suite to the three source test
  identities and gave it an independent Cargo target matching Go's package
  test artifact. Complete inventory and WIP gates are recorded in
  `receipts/util_resourcegrouptag.md`.
- 2026-09-02: refreshed the complete Go-master `pkg/util/deadlockhistory`
  inventory at `c6054025ed4c32ab3672a2a24ea46892714d21ec`: four artifacts and
  669 lines, including the four source tests, `TestMain`, and the flaky BUILD
  target. The existing `tidb-executor::deadlock_history` owner preserves
  bounded retention, IDs, datum/null rules, timestamp precision, digest
  conversion, and resize behavior; current and detached Go suites plus the
  four Rust owner tests pass. Details are in `receipts/util_deadlockhistory.md`
  and `docs/operations/util-deadlockhistory-audit-execplan.md`.
- 2026-09-02: refreshed the complete Go-master `pkg/util/disttask` inventory
  at `c6054025ed4c32ab3672a2a24ea46892714d21ec`: three artifacts and 133
  lines, including the single source vector and flaky BUILD target. The
  existing `tidb-domain::disttask` owner preserves JoinHostPort formatting,
  first-match/`-1` lookup, and infosync error/missing-ID behavior; current and
  detached Go tests plus the Rust source-vector test pass. Details are in
  `receipts/util_disttask.md` and
  `docs/operations/util-disttask-audit-execplan.md`.
- 2026-08-29: completed the pinned Go `pkg/util/keydecoder` package (one
  production file, one source test, one test harness, and `BUILD.bazel`). Read
  the complete package first, then removed four supplemental Rust test
  identities and folded the remaining assertions into the sole source-shaped
  `TestDecodeKey`. Corrected existing-index empty values to retain Go's
  non-nil empty slice while still honoring JSON `omitempty`. The ordinary
  `DEADLOCKS` and `DATA_LOCK_WAITS` consumers now share this decoder;
  `DATA_LOCK_WAITS` no longer routes raw lock keys through the unrelated
  hexadecimal `TIDB_DECODE_KEY` builtin decoder. Complete inventory and WIP
  gates are recorded in `receipts/util_keydecoder.md`.
- 2026-08-29: completed the pinned Go `pkg/util/workloadrepo` package
  audit (eight production files, one test file, and `BUILD.bazel`; no package
  doc, generated, or platform variants). Added its Rust owner and ordinary
  server/session/etcd/DDL integration. The first behavioral regression exposed
  that catalog memory tables were incorrectly planned as TiKV table scans, so
  the common SELECT path now carries Go's `LogicalMemTable -> PhysicalMemTable
  -> executorBuilder` shape, including column pruning and the source plan-cache
  refusal. Snapshot retries now retain parse errors for all five Go attempts,
  manual initiation is serialized with worker control changes, snapshot error
  aggregation is source ordered, and repository-table creation no longer
  prematurely freezes insert SQL. An empty etcd value now follows Go's
  missing-key recovery attempt before the create-revision conflict. Removed
  thirteen tests that merely reused Go test names while asserting constants,
  interval fields, or vector lengths; they were not source-test
  transcreations. `TIDB_INDEX_USAGE` now enumerates visible catalog indexes
  and reads the node-global `tidb-stats` collector, including Go's synthetic
  integer-primary-key ID zero, seven buckets, and nullable timestamp; the
  cluster session factory shares one collector across its sessions.
  `TIDB_STATEMENTS_STATS` now uses the existing cumulative statement-summary
  reader through the same ordinary memory-table query path, with session user,
  PROCESS visibility, and the full `SessionTimeZone` boundary instead of the
  former named-zone-only narrowing. The three client-error source tables now
  read the existing `pkg/errno`-shaped shared counters with Go's PROCESS and
  own-user visibility, and `FLUSH CLIENT_ERRORS_SUMMARY` clears those same
  counters instead of succeeding as a no-op. The ordinary MySQL packet
  boundary now records every emitted ERR packet, including authentication and
  session-open failures, against the parsed user and peer host; successful
  text statements publish their real warning codes at the response boundary,
  matching Go's `clientConn.writeError`/`flush` ownership instead of counting
  internal session errors. SQL-level regressions read all five providers from
  their real shared collectors. Go's `TestSettingSQLVariables` now runs
  through the ordinary `SET GLOBAL` path; it exposed and fixed a root hook bug
  where Rust passed the original out-of-range text to the worker after the
  sysvar layer had accepted and clamped it. Removed the crate's empty
  aggregate-test harness and two Rust-only snapshot edge tests. Restored Go's
  parameterized `getHouseKeeper` loop and its unarmed-timer behavior after a
  non-owner tick or partition error. A shared repository-session test
  authority now transcreates the source races, two-worker election handoff,
  global/admin control, sampling and snapshot timing, stop/restart,
  partition-create/drop/startup maintenance, housekeeper retention changes,
  three owner-loss modes, clock calculation, and snapshot-ID recovery;
  `TestSettingSQLVariables` remains at the ordinary session/sysvar boundary.
  `TIDB_TRX` now reads the live process registry through ordinary memory-table
  execution, applies Go's PROCESS/own-user filter, reports source state and
  timestamps, records related physical table IDs, and caps statement digest
  history at Go's 50 entries; its SQL-level visibility/history regression
  passes. Completed transaction memory and lock-wait publication through the
  ordinary transaction/runtime owners: `TIDB_TRX` now reports the native
  mutation-buffer footprint, waiting timestamps, related table IDs, and
  statement-summary-resolved current SQL text; `DATA_LOCK_WAITS` queries every
  PD store through TiKV's `GetLockWaitInfo`, appends the node's resolving-lock
  registry, applies Go's PROCESS visibility, and decodes key/digest metadata.
  Prepared-statement responses now update the same warning collector as text
  responses at the common wire boundary. The complete package inventory,
  integration decisions, and WIP gates are recorded in
  `receipts/util_workloadrepo.md`; this package checkpoint is complete.
- 2026-08-29: completed the pinned Go `pkg/owner` prerequisite (three
  production files, three test files, `BUILD.bazel`, and `OWNERS`) in a new
  `tidb-owner` crate over the ordinary `tidb-pd-client` etcd authority. Added
  the source election's create-revision ordering, leased create, mod-revision
  CAS, atomic force-owner mutation, revision-resuming delete watch, session
  refresh, mock global state, distributed lock, exact eleven-test surface,
  and no polling/single-node production fallback. Complete inventory and WIP
  gates are recorded in `receipts/owner.md`; `pkg/util/workloadrepo` integration
  is next.
- 2026-09-02: refreshed the complete Go-master `pkg/util/generic` inventory at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: five artifacts and 478 lines,
  including seven heap tests, one map test, and the flaky BUILD target. The
  existing `tidb-util::generic` owner preserves signed capacity, comparator
  panic/order and wrapping sort behavior, synchronized-map semantics, and the
  stats TopN consumer; current and detached Go suites plus all eight Rust owner
  tests pass. Details are in `receipts/util_generic.md` and
  `docs/operations/util-generic-audit-execplan.md`.
- 2026-08-29: re-read all four pinned Go `pkg/util/checksum` artifacts and
  removed two supplemental signed-overflow tests plus their private fixture.
  Production keeps Go's wrapping arithmetic and zeropool-backed reader path;
  the suite now contains exactly the ten source tests.
- 2026-08-29: re-read every pinned Go `pkg/util/redact` artifact and replaced
  Rust's narrowed string-returning de-redaction convenience with Go's general
  line-oriented reader/writer path. `DeRedactFile` and the exact three-test
  surface now use that one ordinary implementation.
- 2026-08-29: re-read every pinned Go `pkg/util/promutil` artifact and removed
  its remaining Rust-only public option aliases and `Send + Sync` interface
  restrictions. The sole noop-registry test and six direct-return factory
  methods remain source-shaped.
- 2026-08-29: re-read the complete testless pinned Go `pkg/util/nocopy`
  package and removed its remaining Rust-only `Default` and compile-time call
  surfaces. The unit marker still provides Go's constructible zero value and
  ordinary no-op lock methods without `Copy` or `Clone`.
- 2026-08-29: re-read the complete pinned Go `pkg/util/regexpr-router`
  package and corrected its earlier receipt: removed the two supplemental
  regexp tests that the Go package does not have, leaving exactly its eight
  source tests and single canonical production owner.
- 2026-08-29: audited every production, test, harness, benchmark, and build
  artifact in pinned Go `pkg/util/mvmap`. Removed Rust-only default and iterator
  extensions plus four supplemental tests, restored the exact two-test and
  two-benchmark surface, and replaced DISTINCT aggregation's bypassing
  `HashSet` with the canonical packed map. Complete inventory, consumer
  integration decision, and WIP gates are recorded in
  `receipts/util_mvmap.md`.
- 2026-08-29: audited every production, test, benchmark, and build artifact in
  pinned Go `pkg/util/globalconn`. Restored the global-kill test's injected
  connection-ID widths and source-shaped lock-free-pool layout, removed a
  Rust-only allocator convenience, alignment policy, and five supplemental
  tests, and retained the exact nine-test and two-benchmark surface. Complete
  inventory and WIP gates are recorded in `receipts/util_globalconn.md`.
- 2026-08-29: audited all five production, test, harness, benchmark, and build
  artifacts in pinned Go `pkg/util/fastrand`, plus the linked Go 1.25.10
  `runtime.cheaprand` boundary. Added Go's missing 32-bit xorshift branch,
  removed four supplemental Rust tests, and retained the exact one-test and
  four-benchmark surface. Complete inventory and WIP gates are recorded in
  `receipts/util_fastrand.md`.
- 2026-08-29: audited all seven production variants, test, and build artifacts
  in pinned Go `pkg/util/intest`. Fixed the default build to ignore
  `EnableAssert` exactly like `no_assert.go`, removed the extra failpoint
  spelling, deleted the duplicate three-test external suite and supplemental
  inline cases, and retained the one exact source test. Complete inventory and
  WIP gates are recorded in `receipts/util_intest.md`.
- 2026-09-02: re-audited all seven `pkg/util/intest` artifacts at current Go
  master `c6054025ed4c32ab3672a2a24ea46892714d21ec`; the complete build-tag
  owner, source regression, failpoint startup contract, and feature-shape
  checks remain aligned. Details are in `receipts/util_intest.md` and
  `docs/operations/intest-audit-execplan.md`.
- 2026-08-29: audited every production, test, harness, benchmark, and build
  artifact in pinned Go `pkg/util/stringutil`. Removed Rust-only optional
  escape handling, UTF-8-narrow wrappers, duplicate byte APIs, public wrapper
  internals, and supplemental tests; restored the exported inner compilers,
  exact seven-test surface, and all three benchmarks. The ordinary expression
  LIKE path now uses the source-shaped compiler. Complete inventory and WIP
  gates are recorded in `receipts/util_stringutil.md`.
- 2026-08-29: audited every source and build artifact in pinned Go
  `pkg/util/config` and its sole executor consumer. Added the one-function
  package owner beside Rust's session-variable authority: TOML string-map
  decoding, the lock-wait exclusion, source validation/normalization, session
  publication, failed-variable reporting, and warning logs. The executor ZIP
  load path has no Rust owner and remains an explicit consumer-package gap.
  Complete inventory and WIP gates are recorded in
  `receipts/util_config.md`.
- 2026-08-29: audited every source and build artifact in pinned Go
  `pkg/util/replayer`. Added the single Rust package owner, moved
  `PlanReplayerTaskKey` out of the domain consumer, removed its Rust-only
  ordering and constructor API, and changed both domain test surfaces to call
  the real filename generator and directory getter. Complete inventory,
  native storage/writer boundaries, and WIP gates are recorded in
  `receipts/util_replayer.md`.
- 2026-09-02: refreshed the complete Go-master `pkg/util/domainutil`
  inventory at `c6054025ed4c32ab3672a2a24ea46892714d21ec`: two artifacts and
  207 lines, with no package tests, fixtures, generated/platform variants, or
  nested packages. The existing `tidb-domain::domainutil` owner preserves the
  process-global repair registry, Go lowercasing, quarantine/removal rules,
  and session-key strings; current and detached Go no-test checks plus the
  Rust owner compile pass. Details are in `receipts/util_domainutil.md` and
  `docs/operations/util-domainutil-audit-execplan.md`.
- 2026-08-29: audited every production and build artifact in pinned Go
  `pkg/util/trxevents` and its ordinary Distsql callback path. Replaced owned
  deep-copy payloads with source-shaped shared pointers, removed value equality
  absent from Go, and deleted the five-test Rust-only suite for the testless Go
  package while retaining downstream callback coverage. Complete inventory and
  WIP gates are recorded in `receipts/util_trxevents.md`.
- 2026-08-29: audited every production and build artifact in pinned Go
  `pkg/util/tikvutil` plus all three pinned consumers. Replaced Rust's private
  atomic and extra public default/getter/setter API with the single public
  source-shaped atomic, wired config and sysvar consumers directly, and
  removed the package's Rust-only unit test. Complete inventory and WIP gates
  are recorded in `receipts/util_tikvutil.md`.
- 2026-09-02: re-audited every production and build-tag artifact in Go-master
  `pkg/util/israce` at `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
  The production mapping already matched both source build
  variants. Removed the two Rust-only unit tests, retired semantic-gate
  manifest, and stale standalone audit plan; the ordinary printer remains the
  source-shaped consumer. Complete inventory and Ready gates are recorded in
  `receipts/util_israce.md`; the living plan is
  `docs/operations/util-israce-audit-execplan.md`.
- 2026-09-02: re-audited all three Go-master `pkg/util/prefetch` artifacts and
  300 lines at `c6054025ed4c32ab3672a2a24ea46892714d21ec`. The native owner
  retains the exact four source tests, unbuffered handoff, alternating
  buffers, EOF conversion, and explicit source-close ordering; the earlier
  Rust-only no-close constructor, Drop-time close, and two supplemental tests
  remain removed. Current and exact detached Go tests plus all four Rust owner
  tests pass. Details are in `receipts/util_prefetch.md` and
  `docs/operations/util-prefetch-audit-execplan.md`.
- 2026-08-29: audited the complete pinned Go `pkg/util/versioninfo` package
  and re-read every `pkg/util/printer` artifact before changing its consumers.
  Removed Rust's twelve-field per-node/per-session `VersionInfo` snapshots and
  their cache/connection ownership tests. Build identity, edition, MySQL
  versions, effective config, kernel type, and deploy mode now come from the
  same process-wide owners as Go, and the ordinary handshake, sysvar,
  `TIDB_VERSION()`, startup-log, and server-info paths all read those owners.
  Complete inventory and WIP gates are recorded in
  `receipts/util_versioninfo.md` and `receipts/util_printer.md`.
- 2026-08-29: audited every production, test, test-harness, and build artifact
  in pinned Go `pkg/util/systimemon`. Removed the Rust-only stoppable monitor
  guard, public cadence constant, cleanup join, duplicate inline test, and
  lifecycle-log test. The package now exposes only the source-shaped blocking
  monitor; the ordinary server caller owns spawning its process-lifetime
  thread, and the sole source backward-jump test is retained. Complete
  inventory and WIP gates are recorded in `receipts/util_systimemon.md`.
- 2026-08-29: audited every production, test, test-harness, and build artifact
  in pinned Go `pkg/util/texttree`. Removed the Rust API's valid-UTF-8
  narrowing: indentation now follows Go's per-invalid-byte `[]rune`
  conversion, while `PrettyIdentifier` appends identifier bytes unchanged.
  Updated the ordinary binary-plan and EXPLAIN consumers at their known-valid
  UTF-8 boundaries, removed duplicate supplemental tests, and retained both
  original Go tests plus one malformed-byte regression. Complete inventory
  and WIP gates are recorded in `receipts/util_texttree.md`.
- 2026-08-28: audited every production, test, and build file in pinned Go
  `pkg/parser/terror`. Removed the redundant `b003` Rust test module, receipt,
  and manifest claim: portable error-code, class, JSON, equality, logging, and
  stack-capture behavior remains covered by `tidb-error/tests/terror_source.rs`,
  while the removed ignored assertion depended on Go runtime stack-frame
  arithmetic rather than SQL-visible behavior.
- 2026-08-28: audited every production, test, benchmark, build-tag variant,
  and build file in pinned Go `pkg/util/hack`. Restored Go's checkpointed
  `MemAwareMap` byte-delta policy and eight-slot source group geometry in the
  Rust owner; removed the duplicate `b004` test module, receipt, and manifest
  claim. Exact `RealBytes` necessarily measures the owned Rust table rather
  than Go's private runtime ABI.
- 2026-08-28: audited every production, test, ownership, and build file in
  pinned Go `pkg/meta/metadef`. The local Go package is byte-identical to the
  pin and Rust's whole-package contract matches every public system-table SQL
  constant. Removed the duplicate `b006` tests, receipt, and manifest claim;
  the owning `db.rs` and `system.rs` tests retain all four Go cases.
- 2026-08-28: audited every production, test, build-tag variant, and build file
  in pinned Go `pkg/util/intest`. Removed the duplicate `b023` test module,
  receipt, and manifest claim, and pinned its semantic contract to the audit
  commit. The owner test plus independent default/intest/enableassert contract
  retain the sole Go test and all build-shape behavior.
- 2026-08-28: audited every production, test, ownership, and build file in
  pinned Go `pkg/util/naming`. The `tidb-naming` owner already matches the
  complete validation and error contract, so removed only the duplicate
  `b026` test module, receipt, and manifest claim.
- 2026-08-28: audited every production, test, and build file in pinned Go
  `pkg/util/backoff`. Its owner and supplementary boundary tests remain
  behaviorally valid (including checked Go duration-conversion results), so no
  code was removed; repinned the semantic evidence and corrected the receipt's
  complete-package source inventory.
- 2026-09-01: refreshed the complete `pkg/util/backoff` inventory against
  current Go master (`0bc44483e3e41a8ea917d4382dc202369468d200`). All three
  artifacts and 113 textual lines are unchanged; the native owner still
  preserves signed-duration width, reset-on-zero, exponential multiplication,
  and cap semantics with the sole source test identity. The receipt now
  records current hashes and environment-qualified validation.
- 2026-08-28: audited every production, test, test-harness, and build file in
  pinned Go `pkg/util/slice`. The owner already preserves all three production
  functions and their nil/empty, ordering, formatting, and short-circuit
  contracts. Removed the duplicate `b029` test module, receipt, and manifest
  claim while retaining the complete owner test surface.
- 2026-09-02: refreshed the complete Go-master `pkg/util/disjointset`
  inventory at `c6054025ed4c32ab3672a2a24ea46892714d21ec`: six artifacts and
  302 lines, including both source tests, `TestMain`, and the flaky BUILD
  target. The existing `tidb-util::disjointset` owner preserves dense/sparse
  union, signed native-width indices, path compression, and current-value
  lookup; current and detached Go suites plus all three Rust owner tests pass.
  Details are in `receipts/util_disjointset.md` and
  `docs/operations/util-disjointset-audit-execplan.md`.
- 2026-08-28: audited the complete pinned Go `pkg/util/dbterror` root package
  and its distinct `exeerrors` and `plannererrors` subpackages without mixing
  their atomic inventories. Restored the root package's missing 19-code
  `ReorgRetryableErrCodes` set and four-message `ReorgRetryableErrMsgs` list,
  narrowed a Rust-only constructor from public to crate-private, moved the
  executor fixture test beside its owning subpackage, and removed the duplicate
  plannererrors test carrier because the owning `tidb-error` test already
  ports the exact Go assertion. Repinned the root receipt to the audit commit.
- 2026-08-28: audited every production, test, benchmark, harness, and build
  file in pinned Go `pkg/util/encrypt`. Removed the public export of Go's
  private default block-size constant, Rust-only equality/cloning semantics
  from the error representation, and a negative-offset guard absent from Go's
  `Reader.ReadAt` path. A second source audit restored Go's wrapping `int64`
  writer/cursor arithmetic, made PKCS#7 unpadding borrow the original input,
  and matched each pinned Go cipher constructor's invalid-IV panic. Repinned
  its complete package receipt; all 17 source tests pass with downstream
  encrypted-spill and expression consumers.
- 2026-08-28: audited every production, test, benchmark, harness, and build
  file in pinned Go `pkg/util/fastrand`. The owning module already contains
  `TestRand` and the real benchmark target contains all four Go benchmark
  workloads, so removed the duplicate external test plus four ignored smoke
  tests, its stale receipt, and manifest claim. The package owner remains the
  sole executable parity surface.
- 2026-08-28: audited every production, test, harness, and build file in
  pinned Go `pkg/util/mathutil`. The three owner modules already implement the
  complete package and inline tests retain all eight Go behavior tests,
  including the million-value decimal-length oracle. Removed the duplicate
  external test carrier, stale receipt, and manifest claim, and repinned the
  complete semantic contract to the audit commit.
- 2026-08-28: audited every production, test, and build file in pinned Go
  `pkg/util/redact`. Removed public exports for three constants owned by Go's
  external `pingcap/errors` package, removed the Rust-only public
  `set_redact_mode` convenience and error value equality/cloning, and moved
  session publication to the shared error authority exactly like Go's sysvar
  hook. Removed the duplicate three-test carrier, receipt, and manifest claim,
  and repinned the complete semantic contract.
- 2026-08-28: audited every production, test, benchmark, harness, and build
  file in pinned Go `pkg/util/sqlescape`. The owner retains all five exported
  Go functions, every private-helper behavior test, all 42 SQL escaping rows,
  and all four benchmark workloads. Removed the duplicate `TestMustUtils`
  carrier, stale receipt, and manifest batch claim. Narrowed Go-float rendering
  back to a private sqlescape implementation and moved slow-log consumers to
  their own crate-private helper, matching Go's package ownership instead of
  exporting a Rust-only sqlescape API. The consumer audit also found TTL
  sqlbuilder incorrectly using that `%v`/`'g'` formatter for Go
  `ValueExpr.Restore`; wired it to the existing datatype value-expression
  implementation so float32 and float64 now retain Go's distinct `'e'`
  formatting behavior.
- 2026-08-28: audited every production, test, benchmark, harness, and build
  file in pinned Go `pkg/util/zeropool`. Removed the stale receipt and duplicate
  manifest batch claim while retaining the complete `TestPool` and four
  benchmark workloads. Reworked the internal item source so factory-backed
  `Pool::new` accepts non-`Default` values like Go `New[T any]`; only the
  Rust representation of Go's zero-valued pool requires `Default`. A second
  benchmark audit restored the source `sync.Pool` value case's per-`Put` type
  erasure and boxing instead of measuring an unrelated concrete-vector pool.
- 2026-08-28: audited every production, test, harness, and build file in pinned
  Go `pkg/util/schemacmp`. Reused the existing exact Go `%g` float and `%q`
  string formatters in place of local Rust approximations, and made the Rust
  `Equality` adapter carry Go `%v` rendering so public incompatibility errors
  match Go for custom equality values. Removed error equality/cloning and a
  public rendered-message accessor absent from Go, removed string-list value
  equality absent from Go's slice type, extended the shared Go quote authority
  to arbitrary string bytes, and deleted executor's less-exact duplicate quote
  module. Deleted the stale receipt and duplicate manifest batch claim.
- 2026-08-28: audited every production, generated-table, test, benchmark,
  harness, and build file in pinned Go `pkg/parser/charset`. Restored the Go
  package boundary: the charset registry and typed charset defaults now start
  with `gbk_bin`/`gb18030_bin`, while the separate collation switch changes
  both views only when explicitly enabled. Removed the unit-test-shaped
  benchmark substitute, added the actual charset lookup benchmark target, and
  deleted the stale receipt and duplicate manifest batch claim.
- 2026-08-28: re-audited the complete pinned `pkg/parser/charset` package after
  its first package-level landing. Removed three remaining Rust policies:
  Unicode full-case expansion in encoding case conversion, ASCII-only
  normalization in charset and HTML encoding lookup, and the defensive
  GB18030 truncated-prefix length check. Reused the generated Go Unicode 15
  simple-case authority from `tidb-mysql`, restored `strings.TrimSpace`
  behavior, exported the source TiFlash charset set, and reproduced the
  source `RemoveCharset` slice-mutation behavior. The regenerated tables are
  unchanged and the complete source/Rust/downstream validation is recorded in
  `receipts/parser_charset.md`.
- 2026-08-28: audited every production, test, and build file in pinned Go
  `pkg/util/errmsg`. Moved the behavior out of the config crate's Rust-only
  string helper into a dedicated `tidb-errmsg` owner with Go's SQL-error
  mutation API, reused the regular expressions prepared during global-config
  publication, wired the ordinary MySQL error packet path, and transcreated
  all five source tests including the complete twelve-row regex table and
  concurrent publication case. The complete inventory and gates are recorded
  in `receipts/b152.md`.
- 2026-08-28: audited all 15 direct artifacts and all 39 test/harness
  entrypoints in the pinned root `pkg/config` package, independently from its
  three nested Go packages. Filled initialization, starter TLS, temp-storage,
  global/TiKV publication, conversion, native `uint`, validation, cloning, and
  reflection-order merge behavior; removed a Rust-owned external metering SDK
  parser and a stale absent-at-pin gap. Both default and nextgen package
  matrices pass; the atomic inventory and gates are in `receipts/b001.md`.
- 2026-08-28: audited the complete three-artifact pinned
  `pkg/config/configtypes` package independently from root `pkg/config`.
  Removed public Rust APIs that actually modeled `docker/go-units` and Go's
  `time` package, removed a duplicate source-test carrier, restored the pinned
  dependency's full numeric grammar, and made duration overflow handling match
  the Go standard library. The Go package, Rust owner, and downstream log and
  server boundaries pass as recorded in `receipts/config_configtypes.md`.
- 2026-08-28: audited every production, generated catalog/table, test,
  harness, and build file in pinned Go `pkg/parser/mysql`, including its
  `tidb-error` ownership split. Removed a Rust-only test that failed on host Go
  toolchain version rather than package behavior, narrowed private Go
  constants back out of Rust's public API, removed comparable/clonable
  semantics from fresh Go-style errors, and replaced an invented unreachable
  locale error with `Infallible`. SQL-mode validation now forwards Go's
  catalogued error unchanged through the session and executor layers instead
  of reconstructing it from a duplicated invalid-token field. Deleted the
  stale receipt and duplicate manifest batch claim; all eleven Go tests remain
  executable across the two owner crates.
- 2026-09-01: audited the complete four-artifact pinned `pkg/util/hint`
  package and moved statement, plan, AST-transfer, query-block, and view-hint
  behavior into the canonical `tidb-hint` owner. Removed four Rust-only package
  tests absent from Go, removed split consumer logic, and wired parser, planner,
  binding/cache, session, executor, and server consumers through the ordinary
  statement path. Exact inventory, mappings, and WIP gates are recorded in
  `receipts/util_hint.md`; TiFlash columnar-index identity and MPP enforcement
  remain planner-package work and are not included in this package claim.
- 2026-08-28: audited every production, test, harness, and build file in pinned
  Go `pkg/util/checksum`. Removed public exports for Go's three private framing
  constants and the Rust-only negative-offset refusal; downstream spill tests
  now keep their own fixture geometry rather than extending the production API.
  The spill consumer now retains a separate cipher-writer handle, as Go does,
  so the Rust-only checksum accessor for its nested writer was removed while
  preserving both live cache overlays. Deleted the stale semantic manifest and
  historical audit plan; all ten Go behavior tests remain in the owning
  checksum module, with source-derived signed-counter coverage alongside them.
- 2026-08-28: audited the complete pinned Go `pkg/util/rowcodec` package:
  `BUILD.bazel`, `common.go`, `decoder.go`, `encoder.go`, `row.go`, all three
  test/harness files, and the benchmark file. Removed Rust-only framing errors,
  static default arrays, checksum copying, and divergent handle predicates;
  restored Go's lazy defaults, map reuse, map-vs-chunk decimal/time behavior,
  payload-driven large-row ID promotion, strict BIT encoding, raw-checksum
  mutation/prefix semantics, old-byte cache prefixes, and unchecked packed-time
  extraction. The 27-test package suite and its four framing sub-suites pass,
  and `tidb-codec`, `tidb-tablecodec`, and `tidb-executor` compile together.
- 2026-08-28: audited the complete pinned Go root `pkg/tablecodec` package:
  `BUILD.bazel`, `OWNERS`, `tablecodec.go`, `tablecodec_test.go`,
  `bench_test.go`, and `main_test.go`. Removed Rust-only wrapper APIs and typed
  malformed-input refusals; restored Go's V0/V1 handle decoding, nullable
  extensible handles, full-column restored-value offsets, mem-comparable binary
  padding, prefix truncation, row portal, unflattening, and panic behavior.
  The 55-test package suite passes, including all 469 Go-generated prefix
  vectors, and the executor compiles against the nullable handle contract.
- 2026-08-28: audited the complete pinned Go
  `pkg/tablecodec/rowindexcodec` package independently: `BUILD.bazel`,
  `rowindexcodec.go`, `rowindexcodec_test.go`, and `main_test.go`. The existing
  `tidb-codec` implementation already matches the minimal 11-byte prefix
  classifier exactly. Removed an unrelated root-tablecodec test from the
  rowindexcodec source suite; the original four-row test table and explicit
  prefix-boundary coverage pass (2/2 tests).
- 2026-08-28: audited the complete pinned Go `pkg/util/codec` package: all six
  production files, five test/harness files, the benchmark file, and
  `BUILD.bazel`. Removed the Rust-only decimal metadata and injected-failure
  APIs, folded typed and untyped range decoding into Go's single optional-type
  path, and narrowed Go-private framing/value-size helpers. Restored Go's
  malformed peek/remainder behavior, typed range restoration, decimal error,
  size, header, negative-zero and 81-digit boundaries, UTC timestamp guard,
  unsigned hash tag, and collation initialization/runtime selection. The full
  `tidb-codec` suite passes (45 unit + 163 integration), and the complete
  `tidb-datatype` dependency suite passes (424 tests).
- 2026-08-28: audited the complete pinned Go `pkg/util/collate` package: all 35
  production, generated, generator-input, test, benchmark, harness, and build
  artifacts. The checkout package is byte-identical to the pin. Removed stale
  unresolved-finding documentation; restored caller spelling on successful
  substitution, borrowed `ImmutableKey` storage for binary collators,
  arbitrary-byte wildcard compilation, the source pinyin panic, and Go's UCA
  9.0 missing-map zero value. The data generator checks all seven images and
  the 11-test/45-benchmark inventory is fully assigned. The complete
  `tidb-datatype` and `tidb-codec` suites pass, benchmark targets compile, and
  downstream executor owners compile. Go `ucadata` passes; the root Go package
  is blocked by unrelated host-toolchain `checkMapABI` and
  `http2.TrailerPrefix` build failures.

- [x] Pin the comparison baseline to Go commit
      `e2788410d8d696605e8cb002585877a063ccc909`.
- [x] Fold cached-select execution into the ordinary physical executor path and
      remove the legacy cache-specific runner in prior commits.
- [x] Remove Rust-specific cached SUM and HashAgg crossover policy in prior
      commits, using the wired planner path instead.
- [x] Implement Go-derived ANALYZE prefix-index behavior across executor and
      cluster ANALYZE owners, with a runnable regression.
- [x] Implement Go physical-property MPP equivalence comparison in the planner
      owner and remove false proto/integration gap carriers.
- [x] Remove identified empty/documentary gap modules from executor, lexer,
      funcdep, proto, and planner; preserve runnable tests.
- [x] Remove manifest entries and receipts for batches b009, b051-b056, b060,
      b097, b099, b101, b103, and b112 that did not establish executable
      parity.
- [x] Audit the complete current-master `pkg/util/kvcache` package, preserve
      its pinned cleanup, and add the rolling `Peek` API with a no-promotion
      LRU regression; all 8 Rust owner tests pass.
- [x] Remove empty expression carriers for Go-only nil-interface,
      `baseBuiltinFunc`, and concrete `*Sig` object-model call shapes, plus the
      no-op SQL-digest retriever test whose complete production owner is not
      implemented.
- [x] Complete the pinned `pkg/config/deploymode` package: replace the
      Rust-only invalid enum variant with Go's integer-backed mode semantics,
      preserve the package documentation contract, and remove the duplicate
      external source-test carrier.
- [x] Complete the pinned `pkg/config/kerneltype` package in both build
      selections, preserve its architecture/binary contract, and remove its
      duplicate external source-test carrier.
- [x] Complete the pinned `pkg/util/table-filter` package: share the existing
      Go-regexp authority so Perl classes and word boundaries remain ASCII,
      remove Rust-only construction/cloning APIs, and retain all fourteen
      source tests plus the public source-contract gates.
- [x] Complete the pinned `pkg/util/regexpr-router` package in its wired
      `tidb-util` owner and remove the unused 766-line `tidb-exec` duplicate
      implementation, duplicate tests, and public module path.
- [x] Complete the pinned `pkg/util/table-router` package in its wired
      `tidb-util` owner, route all extractor regexes through the Go-regexp
      authority, remove Rust-only constructors/error traits, and delete the
      unused 1,029-line `tidb-exec` duplicate.
- [x] Complete the pinned `pkg/util/filter` package in its wired `tidb-util`
      owner, restore the source's distinct validation errors and complete
      system-schema table, remove supplementary non-source tests, and delete
      the unused 1,448-line `tidb-exec` duplicate.
- [x] Complete the pinned `pkg/util/password-validation` package across its
      `tidb-util`, expression, executor, and session owners: retain exactly
      five source tests, restore arbitrary-byte Go strings, move enablement
      policy back to its two Go callers, and remove exported sysvar catalogs,
      error-code helpers, derives, and supplemental tests absent from Go.
- [x] Complete the pinned `pkg/util/vitess` package in its `tidb-util` owner:
      retain the exact five-row source test and remove Rust-only API policy,
      expanded package narrative, named key constant, and supplemental vectors.
- [x] Complete the pinned `pkg/util/watcher` package in its `tidb-util` owner:
      restore Go's three polling lock phases, no-follow child metadata,
      platform file identity, signed sizes, real ticker, and exact single test;
      remove injected-ticker, lifecycle-policy, accessor, doc, and test extras.
- [x] Complete the pinned `pkg/util/tls` package across `tidb-util`, session,
      server, and status owners: collapse duplicate Rust modules, restore the
      process atomic and starter/cluster publication hooks, retain exactly one
      source test, and remove Rust-only tables, helpers, docs, and tests.
- [x] Complete the pinned `pkg/util/table-rule-selector` package in its wired
      `tidb-util` owner: restore Go string-range indices, nil RuleSets, open
      insert types, and exact errors; remove the unused 1,270-line selector and
      1,544-line column-mapping duplicates from `tidb-exec`.
- [x] Complete the current Go-master `pkg/util/queue` package in its
      `tidb-util` owner and
      remove the unused executor duplicate whose `Clear` eagerly dropped
      backing values and whose public head/tail accessors existed only for its
      duplicate external tests. The current inventory and package ExecPlan are
      recorded in `receipts/util_queue.md` and
      `docs/operations/util-queue-audit-execplan.md`.
- [x] Complete the current Go-master `pkg/util/sli` package in its
      `tidb-util` owner; retain the existing source-shaped accumulator and
      session/executor integration after a complete 132-line inventory. The
      current receipt and package ExecPlan record the no-delta audit and Ready
      evidence: `receipts/util_sli.md` and
      `docs/operations/util-sli-audit-execplan.md`.
- [x] Complete the current Go-master `pkg/util/set` package in its `tidb-util`
      owner:
      restore all five concrete memory-aware constructors and tracker rules,
      retain exactly seven source tests and three benchmarks, remove public
      generic wrapper and ordered-tree policy, restore the free keyed-set API
      and current-key clone/order behavior, pre-size memory-aware constructors,
      and wire HashAgg to Go's concrete string set. Current inventory and
      package ExecPlan: `receipts/util_set.md` and
      `docs/operations/util-set-audit-execplan.md`.
- [x] Complete the pinned `pkg/util/slice` package in its `tidb-util` owner,
      retain its three production functions and one source test, and remove
      four supplementary non-source tests.
- [x] Complete the pinned `pkg/util/selection` package in its `tidb-util`
      owner: restore signed index results and empty `-1`, remove Rust-only
      saturating rank policy, retain its four source tests, restore all 21
      benchmark cases, and migrate the HashAgg percentile caller.
- [x] Complete the pinned `pkg/util/size` package in its `tidb-util` owner,
      retain all twenty source constants with Go ABI sizing, and remove the
      supplementary Rust test absent from this test-free Go package.
- [x] Audit the complete pinned `pkg/util/breakpoint` package. Do not add a
      detached callback wrapper: its sole behavior requires the ordinary
      session value store, failpoint registry, and both executor injection
      sites. The two-artifact inventory is in
      `receipts/util_breakpoint_audit.md` and remains unclaimed.
- [x] Complete the complete pinned `pkg/util/compress` package in `tidb-util`:
      preserve both process-wide pools, reset/close lifecycle, discard-bound
      writer, invalid-header errors, and unfinished-reset behavior; route the
      statistics JSON block consumer through the owner, and leave the absent
      ingest-control owner as an explicit integration boundary. The complete
      two-artifact inventory and Ready gates are in
      `receipts/util_compress_audit.md`.
- [x] Complete the pinned `pkg/util/tiflash` package in its live `tidb-txnkv`
      owner: preserve Go's open native-integer policy and exact fallbacks,
      consume vardef's three canonical spellings, retain the threshold and
      distsql propagation, and remove alias/adapter/constant/test extras. The
      atomic inventory and Ready gates are in `receipts/util_tiflash.md` and
      `operations/util-tiflash-audit-execplan.md`.
- [x] Complete the pinned `pkg/util/disk` package in `tidb-util`: restore the
      exact temp-directory lifecycle and both tracker constructors, route its
      real server/chunk/memory-alarm consumers through them, and remove the
      Rust-only spill-policy surface from the package. The five-artifact
      inventory and Ready gates are in `receipts/util_disk.md` and
      `docs/operations/util-disk-audit-execplan.md`.
- [x] Complete the pinned root `pkg/util/sem` package in its `tidb-util`
      owner, verify its full policy and cross-crate sysvar wiring, retain its
      five source tests, and remove supplementary Rust-only assertions. The
      current Ready receipt and living plan are in `receipts/util_sem.md` and
      `docs/operations/sem-audit-execplan.md`.
- [x] Complete the pinned root `pkg/util/traceevent` package across
      `tidb-util`, the vendored `tikv-client`, and server initialization:
      replace the disconnected fake client registry with live hooks, restore
      ordinary startup registration, preserve structured fields and context,
      remove Rust-only public/test surfaces, and port both source benchmarks.
      The atomic inventory and Ready gates are in
      `receipts/util_traceevent.md` and
      `docs/operations/util-traceevent-audit-execplan.md`.
- [x] Inventory the nested `pkg/util/traceevent/test` package independently:
      two artifacts and 461 lines, four next-gen session/flight-recorder
      integration tests, and its flaky Bazel target. The root Rust owner covers
      unit/adapter behavior, but no dependency-closed SQL-session harness
      exists; the explicit integration boundary and interrupted linker run are
      recorded in `receipts/util_traceevent_test.md` and
      `docs/operations/util-traceevent-test-audit-execplan.md`.
- [x] Complete the pinned root `pkg/util/tracing` package in `tidb-util`:
      preserve shared span-handle semantics, restore its open string phase and
      pointer-preserving CE deduplication, add the four source benchmarks and
      empty `OptimizeTracer`, and remove supplementary Rust-only APIs/tests.
      The atomic inventory and Ready gates are in `receipts/util_tracing.md`
      and `docs/operations/util-tracing-audit-execplan.md`.
- [x] Complete the pinned `pkg/session/syssession` package in the
      `tidb-syssession` owner: replace the executor-local policy fragments
      with the full owner/operation/pool lifecycle, remove ignored empty
      carriers, and migrate timer storage off its local session/pool
      imitation. The atomic inventory and Ready gates are in
      `receipts/session_syssession.md` and
      `docs/operations/session-syssession-audit-execplan.md`.
- [x] Complete the pinned `pkg/util/sqlexec/mock` support package in a
      distinct `tidb-sqlexec-mock` owner: preserve the context-key identity
      and the generated restricted-executor mock's full three-method
      contract without introducing a second SQL interface. The atomic
      inventory and Ready gates are in `receipts/util_sqlexec_mock.md` and
      `docs/operations/util-sqlexec-mock-audit-execplan.md`.
- [x] Complete the pinned `pkg/statistics/handle/util` package in a distinct
      `tidb-stats-handle-util` owner: remove five partial policy modules,
      implement every production artifact over shared model/executor/session
      contracts, and replace its four ignored source tests with executable
      behavior. The current Go-master inventory, corrections, and Ready gates
      are in `receipts/statistics_handle_util.md` and
      `docs/operations/statistics-handle-util-audit-execplan.md`.
- [x] Complete the pinned `pkg/statistics/handle/util/test` support package in
      `tidb-stats-handle-util-test`: match a typed request context, preserve
      the wrong-type panic and exact description, remove the string-only
      predicate and its two supplemental tests, and make ordinary `StatsCtx`
      carry the matching request source. The current Go-master inventory and
      Ready gates are in `receipts/statistics_handle_util_test.md` and
      `docs/operations/statistics-handle-util-test-audit-execplan.md`.
- [x] Complete the pinned `pkg/statistics/handle/logutil` package in
      `tidb-stats-handle-logutil`: preserve all four category-tagged logger
      constructors and both source sampling policies over the shared logutil
      owner, without adding tests to the source-test-free package. The current
      Go-master inventory and Ready gates are in
      `receipts/statistics_handle_logutil.md` and
      `docs/operations/statistics-handle-logutil-audit-execplan.md`.
- [x] Complete the pinned `pkg/statistics/handle/internal` support package in
      `tidb-stats-handle-internal`: replace the opaque snapshot workaround
      with `AssertTableEqual` over actual statistics tables, and remove its
      three non-Go tests. The current Go-master inventory and Ready gates are
      in `receipts/statistics_handle_internal.md` and
      `docs/operations/statistics-handle-internal-audit-execplan.md`.
- [x] Complete the pinned `pkg/statistics/handle/usage/collector` package in
      `tidb-stats-handle-usage-collector`: preserve both bounded channels,
      synchronous timeout escalation, worker priority/drain/close behavior,
      and all three source tests while removing the supplemental capacity
      assertion. The atomic inventory and Ready gates are in
      `receipts/statistics_handle_usage_collector.md` and
      `docs/operations/statistics-handle-usage-collector-audit-execplan.md`.
- [x] Complete the pinned `pkg/statistics/handle/usage/indexusage` package in
      `tidb-stats-handle-usage-indexusage`: own the real model-driven GC,
      samples, global/session/statement collectors, four source tests, and
      parallel benchmark; remove narrowed and duplicate aggregate surfaces.
      The atomic inventory and Ready gates are in
      `receipts/statistics_handle_usage_indexusage.md` and
      `docs/operations/statistics-handle-usage-indexusage-audit-execplan.md`.
- [x] Audit the pinned parent `pkg/statistics/handle/usage` package as one
      atomic unit and remove its disconnected key, pending-ID,
      transaction-mode, SQL-string, empty-test, and function-batch carriers.
      The package remains explicitly unclaimed until the ordinary stats
      handle, session, schema, transaction, persistence, and integration paths
      exist. The complete Go-master inventory and Ready validation are in
      `receipts/statistics_handle_usage_audit.md` and
      `docs/statistics/usage-package-parity-execplan.md`.
- [x] Complete the pinned `pkg/statistics/handle/cache/internal` interface
      package in `tidb-stats-handle-cache-internal`: bind it to shared actual
      statistics tables, preserve all eleven pointer-receiver methods,
      and remove the generic value, extra method, and source-absent mock tests.
      The atomic inventory and Ready gates are in
      `receipts/statistics_handle_cache_internal.md` and
      `docs/operations/statistics-handle-cache-internal-audit-execplan.md`.
- [x] Re-audit the complete `pkg/statistics/handle/cache/internal/mapcache`
      package at Go master `c6054025ed4c32ab3672a2a24ea46892714d21ec`: two
      production/build artifacts and 151 lines, with no hidden variants or
      source tests. The native owner derives costs from actual statistics
      tables, retains shared pointers across independent copies, implements
      the complete cache contract, and adds only the synchronization required
      by shared Rust cache ownership. The receipt, focused owner tests, and
      Ready gates are in `receipts/statistics_handle_cache_internal_mapcache.md`
      and `docs/operations/statistics-handle-cache-internal-mapcache-audit-execplan.md`.
- [x] Re-audit the complete `pkg/statistics/handle/cache/internal/lfu` unit at
      Go master `c6054025ed4c32ab3672a2a24ea46892714d21ec`: all five source and
      BUILD artifacts (782 lines), ten race-enabled source tests, and no hidden
      fixtures or generated/platform variants. The Rust owner now reproduces
      Go's negative-ID shard failure and test-mode zero-quota override through
      focused regressions; the package remains explicitly unclaimed for full
      parity until the external Ristretto dependency has a complete pinned
      owner. Inventory, gap disposition, and Ready gates are in
      `receipts/statistics_handle_cache_internal_lfu_audit.md` and
      `docs/operations/statistics-handle-cache-internal-lfu-audit-execplan.md`.
- [x] Re-audit the complete `pkg/statistics/handle/cache/internal/testutil`
      support package at Go master `c6054025ed4c32ab3672a2a24ea46892714d21ec`:
      two artifacts and 109 lines, with no source tests or hidden variants.
      The native owner constructs actual tables with optional CMS/TopN/
      histogram payloads, full-load status, native memory accounting, and both
      append helpers. The receipt and Ready gates are in
      `receipts/statistics_handle_cache_internal_testutil.md` and
      `docs/operations/statistics-handle-cache-internal-testutil-audit-execplan.md`.
- [x] Re-audit the complete `pkg/statistics/handle/cache/metrics` package at Go
      master `c6054025ed4c32ab3672a2a24ea46892714d21ec`: two artifacts and 67
      lines, with eight initialized Prometheus handles and no source tests,
      fixtures, generated/platform variants, or benchmarks. The obsolete
      label-only Rust carrier is removed, while the remaining private-vector
      seed stays explicitly unclaimed until the atomic `pkg/metrics` owner
      exists. Inventory and Ready blocker gates are in
      `receipts/statistics_handle_cache_metrics_audit.md` and
      `docs/operations/statistics-handle-cache-metrics-audit-execplan.md`.
- [x] Re-audit the complete root `pkg/statistics/handle/cache` package at Go
      master `c6054025ed4c32ab3672a2a24ea46892714d21ec`: five artifacts and
      1,051 lines, with two tests, six benchmark shapes, and no fixtures or
      generated/platform variants. Remove the obsolete Rust-only process-wide
      `StatsTableRowCache` (Go deleted `stats_table_row_cache.go`), move its
      replacement to statement-local `TableSizeStats` reads, skip histogram
      reads for TABLE_ROWS-only projections, keep estimates off the shared
      catalog, and zero them on restricted-read failure. Root cache refresh,
      batching, lifecycle, health, and backend behavior remain aligned. The
      full inventory and Ready gates are in
      `receipts/statistics_handle_cache_audit.md`; the living implementation
      log is `docs/operations/statistics-handle-cache-audit-execplan.md`.
- [x] Audit the complete pinned `pkg/statistics/handle/history` package.
      Remove the maximum-version-only carrier and its three source-absent
      tests. Go owns a session-backed history service with filtering,
      transactions, SQL validation, JSON blocking, and storage writes; its
      storage/runtime dependencies are not complete. The inventory is in
      `receipts/statistics_handle_history_audit.md`.
- [x] Complete the pinned `pkg/statistics/handle/initstats` package in
      `tidb-stats-handle-initstats`. Replace concurrency/progress arithmetic
      carriers and five source-absent tests with the live config/runtime
      policy, atomic percentage, bounded range worker, task processing,
      sampled/error logging, and close/wait lifecycle. Wire `/status` to the
      shared percentage. Inventory and gates are in
      `receipts/statistics_handle_initstats.md`.
- [x] Audit and wire the complete pinned `pkg/statistics/handle/syncload`
      package behavior. Distinct statistics handles own distinct configured
      queues/workers, shutdown joins active workers, global singleflight
      preserves Go's locked completion order, and a dropped expired task waits
      for its original timer. Keep the atomic package unclaimed until the
      complete shared `pkg/metrics` owner supplies its five collectors. The
      inventory and three fail-before/pass-after regressions are in
      `receipts/statistics_handle_syncload_audit.md`.
- [x] Audit the complete pinned `pkg/statistics/handle/updatetest` package.
      Remove all 15 ignored empty Rust functions: Go's 23 tests and benchmark
      are an integrated stats-handle validation package, not independently
      portable scalar cases. The package remains unclaimed until the ordinary
      session/storage statistics runtime exists. The inventory is in
      `receipts/statistics_handle_updatetest_audit.md`.
- [x] Audit the complete pinned `pkg/statistics/handle/types` package. Remove
      the Rust-only `StatsLockTable` constructor and two source-absent tests;
      restore the exact nested cache-update payload; and match Go's complete
      analysis-job JSON ordering, integer-key, float, and error contract. The
      composite interface family remains unclaimed until every embedded owner
      package is complete; an unused umbrella trait crate would add no Go
      behavior. Inventory and the full declaration map are in
      `receipts/statistics_handle_types_audit.md`.
- [x] Audit the complete pinned `pkg/statistics/handle/lockstats` package.
      Remove three deterministic leaf modules, their eight tests, one duplicate
      batch test, and five empty gap tests. The actual package is transactional
      SQL/session behavior and cannot land before the complete handle-types and
      ordinary stats-handle owners. Inventory is in
      `receipts/statistics_handle_lockstats_audit.md`.
- [x] Audit the complete pinned `pkg/statistics/handle/ddl/testutil` package.
      Remove the generic slice-search carrier and its two source-absent tests:
      Go owns four blocking channel, timeout, transaction, notifier-context,
      and handle helpers, while the Rust function exercised none of them.
      Inventory is in
      `receipts/statistics_handle_ddl_testutil_audit.md`.
- [x] Audit the complete pinned root `pkg/statistics/handle/ddl` package.
      Remove the port-based subscriber, its extracted physical-ID and SQL
      leaves, a source-absent compatibility alias, their tests, and one empty
      gap test. Go owns an integrated notifier/session/storage/cache/handle
      package whose dependencies and 24 integration tests are not complete in
      Rust. Inventory is in `receipts/statistics_handle_ddl_audit.md`.
- [x] Audit the complete pinned `pkg/statistics/handle/globalstats` package.
      Remove four disconnected layout/SQL/task/TopN carriers, their tests, two
      empty gap tests, and stale port claims. Go owns a complete blocking and
      async handle/storage/session merge pipeline with 28 tests and two
      benchmarks. Inventory is in
      `receipts/statistics_handle_globalstats_audit.md`.
- [x] Audit the complete pinned root `pkg/statistics/handle` package. Remove
      the SQL-builder and pseudo-cache scalar leaves plus their eight tests.
      Go owns the composed ordinary handle and transactional, storage-backed,
      memory-aware bootstrap lifecycle; its dependency graph remains
      incomplete. Inventory is in
      `receipts/statistics_handle_root_audit.md`.
- [x] Audit the complete pinned `pkg/statistics/handle/storage` package. Remove
      six public scalar/SQL carriers, their 19 tests, and the ignored empty
      test functions. Go owns one transactional session/storage/cache
      read-writer package with 12 artifacts and 29 integrated tests at the
      current Go-master revision; its DDL auto-presplit consumer remains a
      separate package boundary. Inventory is in
      `receipts/statistics_handle_storage_audit.md`.
- [x] Audit the complete pinned `pkg/statistics/handle/autoanalyze/exec`
      package. Remove an unconsumed ratio-parser leaf, a duplicate window
      compatibility module, their six source-absent tests, and duplicate root
      aliases. Go owns integrated session ANALYZE execution, process tracking,
      metrics, warnings, cache effects, and interruption. Inventory is in
      `receipts/statistics_handle_autoanalyze_exec_audit.md`.
- [x] Complete the pinned
      `pkg/statistics/handle/autoanalyze/priorityqueue` package. Its complete
      production/test/build inventory maps to one live keyed queue, three job
      forms, source-owned lifecycle, DML/retry refresh, DDL mutation, validation,
      and ordinary ANALYZE/cache effects. Inventory is in
      `receipts/statistics_handle_autoanalyze_priorityqueue_audit.md`.
- [x] Complete the pinned
      `pkg/statistics/handle/autoanalyze/priorityqueue/calculatoranalysis`
      package. The production parent calculator now runs all 690 source matrix
      cases and byte-compares the exact pinned golden CSV. Inventory is in
      `receipts/statistics_handle_autoanalyze_calculatoranalysis_audit.md`.
- [x] Complete the pinned
      `pkg/statistics/handle/autoanalyze/priorityqueue/intervaltimezone`
      test package. Its complete build/test inventory maps to the production
      statistics-session pool, analyze-job persistence plans, and restricted
      duration query; reused sessions refresh the live global timezone before
      evaluating the persisted interval. Inventory is in
      `receipts/statistics_handle_autoanalyze_intervaltimezone_audit.md`.
- [x] Audit the complete pinned `pkg/statistics/handle/autoanalyze/refresher`
      package. Remove two unconsumed scalar condition leaves and their six
      source-absent tests. Go owns the live priority-queue lifecycle, session
      parameters, DDL registration, worker concurrency, ANALYZE execution,
      panic recovery, and stats effects. Inventory is in
      `receipts/statistics_handle_autoanalyze_refresher_audit.md`.
- [x] Complete the pinned root `pkg/statistics/handle/autoanalyze` package as
      one composition unit. Its complete production/test/build inventory maps
      to the live priority queue/refresher, statistics owner, system sessions,
      ordinary physical ANALYZE path, job cleanup, locks, windows, skipped
      types, and cache publication. Queue SQL now keeps Go's automatic job
      identity, and the stale planner missing-partition gap is executable in
      the server owner. The package decision and all 14 original test
      dispositions are in
      `receipts/statistics_handle_autoanalyze_root_audit.md`.
- [x] Audit the complete pinned external test packages
      `pkg/statistics/handle/handletest` and its `analyze`, `initstats`,
      `lockstats`, and `statstest` children. Remove two origin/master batch
      carriers containing 72 ignored empty tests, one duplicate utility test,
      and their stale batch receipts. The pinned packages own integrated
      session/domain/storage/ANALYZE/lock/cache tests, not independent Rust
      production APIs. Complete inventories are in the five
      `receipts/statistics_handle_handletest*_audit.md` receipts.
- [x] Audit the complete pinned `pkg/statistics/asyncload` package. Remove the
      caller-owned sharded map, duplicate model identities, synthetic
      validity-result load requests, and seven source-absent tests. Go owns a
      process-global queue composed with parent statistics producers and
      handle/storage/DDL consumers; without those consumers, the standalone
      Rust queue was an alternate runtime rather than package parity. The
      complete inventory is in `receipts/statistics_asyncload_audit.md`.
- [x] Remove the two legacy partial `pkg/statistics` batch receipts, their
      duplicate carrier, and 33 ignored empty test functions. They compared
      `origin/master`, counted missing behavior as skipped coverage, and did
      not constitute a package-atomic claim against the pinned source. Keep
      only executable behavior while the complete root-package audit remains
      in progress.
- [x] Fill pinned `pkg/statistics/statistics_test.go::TestPruneTopN` in the
      private builder owner. All five source cases now execute without
      exporting the private pruning helper or adding a compatibility wrapper.
- [x] Replace Rust's caller-supplied ANALYZE default fields with Go's two
      `vardef` process atomics. The statistics builder now reads the globals
      directly, and the existing validated `SET GLOBAL` publication path
      updates them. Remove the duplicate three-test sample carrier after the
      pinned root test became the single executable owner.
- [x] Fill pinned `pkg/statistics.Histogram::ValueToString`, including Go's
      deliberate malformed-index-key behavior: keep the successfully decoded
      prefix, ignore the codec error, and render the undecoded suffix as one
      bytes datum. Do not fake `NewPseudoHistogramReuseChunk` with an identity
      token while Rust still lacks Go's `Tp` plus shared `Bounds` structure.
- [x] Move the in-process statistics cache out of transactional catalog clone
      semantics. Catalog images now share one stats-handle cache, so ANALYZE
      reads the user transaction's rows but its publication survives ROLLBACK
      and cannot be overwritten by a later commit of an older catalog image.
      Physical scan EXPLAIN now reads that retained statistics identity, as
      Go does, instead of unconditionally printing `stats:pseudo`.
- [x] Port `pkg/statistics/integration_test.go`'s full-sampling NULL contract
      for both column and index statistics, and carry the analyze snapshot TSO
      through `ClusterTableStats`, `mysql.stats_meta`, and the reload path.
- [x] Port `TestOutdatedStatsCheck`/`TestOutdatedAnalyze`: preserve Go's
      process-wide 0.7 policy atom, snapshot the session switch per statement,
      and mark only the planner-local copy pseudo without mutating cached stats.
- [x] Port `TestSingleColumnIndexNDV`'s exact 96-row NDV/NULL contract and
      `TestIssue44369`'s analyze-then-rename composite-index regression.
- [x] Run the Ready validation profile for the selected complete
      `pkg/util/compress` package: locked Rust owner/consumer checks and
      regressions, workspace formatting, repository `make lint`, and diff
      hygiene all pass with the command-local toolchains recorded in
      `receipts/util_compress_audit.md`.
- [x] Audit the complete Go-master `pkg/util/plancodec` package, implement its
      appended Analyze physical ID, and record the package receipt.
- [x] Run the Ready validation profile for `pkg/util/plancodec`: Go source
      tests, all 15 Rust owner tests, direct Rust consumer checks, formatting,
      repository lint, and diff hygiene pass with the command-local toolchains
      recorded in `receipts/util_plancodec.md`.
- [x] Audit the complete Go-master `pkg/util/dbterror/exeerrors` package,
      confirm all 82 source prototypes against the 82 Rust statics and 82-row
      generated fixture, and record the package receipt without inventing a
      redundant production or test change.
- [x] Run the Ready validation profile for `pkg/util/dbterror/exeerrors`: Go
      package compilation, the complete Rust fixture test, owner compilation,
      formatting, repository lint, and diff hygiene pass with the command-local
      toolchains recorded in `receipts/util_dbterror_exeerrors.md`.
- [x] Audit the complete Go-master `pkg/util/dbterror/plannererrors` package,
      confirm all 98 source prototypes and the 59-entry source test against the
      Rust owner, and record the package receipt without inventing a duplicate
      catalog or test.
- [x] Run the Ready validation profile for
      `pkg/util/dbterror/plannererrors`: Go source tests, both Rust owner tests,
      owner compilation, formatting, repository lint, and diff hygiene pass
      with the command-local toolchains recorded in
      `receipts/util_dbterror_plannererrors.md`.
- [x] Audit the complete Go-master `pkg/util/execdetails` package, including
      all eight direct artifacts and all four Rust seed owners; record its
      dependency-closed parity boundary without inventing a partial fix.
- [x] Complete the Go-master `pkg/util/sqlkiller` package: port the
      concurrent-reset state ordering and event-closure behavior, add the
      focused source regression, and update its package receipt.
- [x] Complete the Go-master `pkg/util/serialization` package: port the
      VectorFloat32 spill framing and decoder, add its focused regression, and
      update the package receipt.
- [x] Complete the Go-master `pkg/util/stringutil` package: port the explicit
      LIKE escape byte for regexp conversion, add its focused regression, and
      update the package receipt.
- [x] Complete the Go-master `pkg/util/table-filter` package: port the
      concrete `ColumnFilterRules` parser API, add its focused regression, and
      update the package receipt.
- [x] Complete the Go-master `pkg/util/chunk` delta: inventory all 29 source,
      test, harness, and build artifacts; port `Chunk.UsedMemoryUsage` with a
      length-versus-capacity regression; and update its package receipt.
- [x] Complete the Go-master `pkg/ttl/cache` package: inventory all thirteen
      Go artifacts, restore TTL task range encoding/decoding and JSON state
      parity through the shared Rust codec, add focused regressions, and update
      its package receipt and ExecPlan.
- [x] Complete the Go-master `pkg/ttl/sqlbuilder` package: inventory all four
      Go artifacts, restore nil-versus-empty continuation-key behavior in the
      Rust scan generator, add the focused regression, and update its package
      receipt and ExecPlan.
- [x] Complete the Go-master `pkg/ttl/session` package: inventory all five Go
      artifacts, restore the omitted `GetSessionVars`/`GetSQLExecutor`
      interface forwarding, remove the Rust-only absent-callback constructor,
      add the focused identity regression, and update its package receipt and
      ExecPlan.
- [x] Run the Ready validation profile for `pkg/ttl/session`: complete Rust
      TTL owner tests, tagged Go session tests, Rust formatting/check,
      repository lint, and diff hygiene pass with the command-local toolchains
      recorded in `receipts/ttl_session.md`.
- [x] Audit the complete Go-master `pkg/ttl/metrics` package: inventory all
      three artifacts, confirm there is no Rust owner for the Prometheus/context
      registry, retain the explicit phase-tracer boundary without speculative
      behavior, and update its package receipt and ExecPlan.
- [x] Run the Ready validation profile for `pkg/ttl/metrics`: tagged Go package
      tests, repository lint, and diff hygiene pass with the command-local
      toolchains recorded in `receipts/ttl_metrics.md`.
- [x] Audit the complete Go-master `pkg/ttl/client` package: inventory all four
      artifacts, confirm no Rust owner exists for the etcd command/notification
      protocol, retain the explicit dependency boundary without speculative
      behavior, and update its package receipt and ExecPlan.
- [x] Run the Ready validation profile for `pkg/ttl/client`: tagged Go client
      integration tests, repository lint, and diff hygiene pass with the
      command-local toolchains recorded in `receipts/ttl_client.md`.
- [x] Complete the Go-master `pkg/ttl/ttlworker` package: inventory all 25
      production, test, integration-support, and BUILD artifacts; restore the
      removed external-workload recycling and TTL owner-election behavior,
      restore its focused regressions and BUILD dependencies, and record the
      package receipt and audit ExecPlan.
- [x] Run the Ready validation profile for `pkg/ttl/ttlworker`: focused
      failpoint-enabled owner/recycle regressions, repository lint, and diff
      hygiene pass; `make bazel_prepare` was attempted but is unavailable
      locally because Bazel is not installed. Details are in
      `receipts/ttl_ttlworker.md`.
- [x] Audit the complete Go-master `pkg/table/tblsession` package: inventory
      all three source, test, and BUILD artifacts (386 lines), confirm the
      deleted Rust seed is an unwired partial carrier, and record the explicit
      session/DML dependency boundary in its receipt and ExecPlan.
- [x] Run the Ready validation profile for `pkg/table/tblsession`: tagged Go
      package tests, repository lint, and diff hygiene pass. No source or BUILD
      artifact changed, so Bazel preparation and Rust cargo checks were not
      applicable. Details are in `receipts/table_tblsession.md`.
- [x] Audit the complete Go-master `pkg/table/tblctx` package: inventory all
      four source, test, and BUILD artifacts (654 lines), verify the branch's
      explicit codec encoder threading remains value-compatible with Go, and
      record the deleted Rust seed's dependency boundary in its receipt and
      ExecPlan.
- [x] Run the Ready validation profile for `pkg/table/tblctx`: tagged Go
      package tests, repository lint, and diff hygiene pass. No source or BUILD
      artifact changed in this audit batch, so Bazel preparation and Rust
      cargo checks were not applicable. Details are in
      `receipts/table_tblctx.md`.
- [x] Complete the Go-master `pkg/table/tables/testutil` package: inventory
      the helper and BUILD metadata, restore table-snapshot collation selection
      instead of the process default, remove the obsolete dependency, add the
      focused fail-before/pass-after regression, and record its receipt and
      ExecPlan.
- [x] Run the Ready validation profile for `pkg/table/tables/testutil`:
      complete tagged Go tests, repository lint, and diff hygiene pass;
      required `make bazel_prepare` was attempted but Bazel is unavailable
      locally. Details are in `receipts/table_tables_testutil.md`.
- [x] Complete the Go-master `pkg/ingestor/errdef` package: inventory the
      error definitions and BUILD metadata, restore the deleted
      `ErrTooManyDataFiles` sentinel required by global-sort, add the focused
      RFC/message regression, and update its receipt and ExecPlan.
- [x] Run the Ready validation profile for `pkg/ingestor/errdef`: complete
      Go tests, repository lint, and diff hygiene pass; required
      `make bazel_prepare` was attempted but Bazel is unavailable locally.
      Details are in `receipts/ingestor_errdef.md`.
- [x] Complete the Go-master `pkg/infoschema/perfschema` package: inventory
      all eight production, test, BUILD, and profile-fixture artifacts, restore
      profile-request logging and its dependencies, and update its receipt and
      ExecPlan.
- [x] Run the Ready validation profile for `pkg/infoschema/perfschema`:
      failpoint-enabled package tests, repository lint, and diff hygiene pass;
      required `make bazel_prepare` was attempted but Bazel is unavailable
      locally. Details are in `receipts/infoschema_perfschema.md`.
- [x] Audit the complete Go-master `pkg/server/err` package: inventory its
      two artifacts and 15 server-class error prototypes, compare every code,
      message, RFC identity, and SQL state with the Rust error catalog, and
      record the explicit parity boundary in its receipt and ExecPlan.
- [x] Complete the Go-master `pkg/util/codec` delta: inventory all 12 source,
      test, benchmark, harness, and build artifacts; remove Rust-only encoder
      value/hash methods; update consumers; add the raw-value regression; and
      update the package receipt.
- 2026-09-01: revalidated the complete `pkg/util/codec` delta against the
      fetched Go `origin/master` commit `0bc44483e3e41a8ea917d4382dc202369468d200`.
      The key-only `Encoder` surface, package-level raw value/hash functions,
      consumer wiring, and focused collated-versus-raw regressions are
      recorded in `receipts/util_codec_audit.md`; no additional Go artifact
      drift was found after the earlier `db35d470...` audit.
- [x] Audit the current Go-master `pkg/tablecodec` root package and its
      `pkg/util/rowcodec` and `pkg/util/rowDecoder` caller seams: read every
      production/test/benchmark/harness/build artifact, certify the no-encoder
      API cleanup, and update the three package receipts.
- [x] Fix the dependency-closed row-decoder parity gap found by the focused
      old-collation common-handle regression: route that V2 shape through the
      mode-sensitive map path while retaining the typed fast path elsewhere.
- 2026-09-01: the tablecodec/rowcodec/rowDecoder batch is recorded in
      `receipts/tablecodec_master_audit.md`, `receipts/util_rowcodec_audit.md`,
      and `receipts/util_rowdecoder_audit.md`; Go tagged tests and Rust owner
      suites pass, including 55 tablecodec, 27 rowcodec, and 11 rowDecoder
      source cases.
- [x] Audit the next bounded package cluster by reading the requested Go
      `origin/master` first, then fill executable gaps and remove false
      carriers.
- 2026-09-01: completed the four-artifact Go-master `pkg/autoid_service`
      inventory (969 lines, 22 production methods, 10 test/helper functions).
      Its kvproto accessor-only delta is already represented by Rust's scalar
      auto-ID request boundary and wire-compatible oneof bindings; no duplicate
      server or adapter was added. The six Rust allocator tests pass, while
      the Go mock-domain suite retains its pre-existing schema-bootstrap
      `require.True` failure. The full inventory and explicit etcd/GRPC server
      boundary are recorded in `receipts/autoid_service_audit.md`.
- 2026-09-01: completed the 15-artifact Go-master `pkg/distsql` inventory
      (5,455 lines, 126 production methods, 63 test/helper declarations).
      Implemented the `b1daa76b65` request batching flags through context,
      immutable KV metadata, coprocessor wire tags 18/19, and explicit
      unhinted merge opt-in task batching; 252 Rust distsql tests pass (2
      ignored). Go runtime-stat additions from `bc04813887`/`db35d47066` and
      the concrete Go coprocessor worker remain explicit dependency boundaries
      in `receipts/distsql_audit.md`.
- 2026-09-01: completed the 11-artifact Go-master `pkg/meta/autoid` inventory
      (4,402 lines, 102 production methods, 43 test/benchmark declarations).
      The existing Rust `tidb-exec` service client now records the greatest
      allocation response with signed/unsigned CAS ordering, keeps ordinary
      rebases monotonic while preserving exact forced rebases, and fast-fails
      repeated RPC errors at Go's count-and-duration limit. The focused owner
      suite passes 8 tests; the etcd service owner and live gRPC integration
      remain explicit boundaries in `receipts/meta_autoid_audit.md`.
- 2026-09-01: audited all three Go-master `pkg/errctx` artifacts (389 lines,
      13 production declarations, and one source test) against the complete
      `tidb-error::errctx` owner. Every level, group membership, context copy,
      warning/error path, and `ResolveErrLevel` rule is already represented;
      no source delta, Rust-only behavior, or safe production edit was found.
      The full inventory and targeted Go/Rust validation are recorded in
      `receipts/errctx_audit.md`.
- 2026-09-01: audited all five Go-master `pkg/format/textrow` artifacts
      (732 lines, 11 production declarations, and seven test/helper
      declarations), including the package's BUILD target and every scalar,
      charset, metadata, result-stream, and generated-vector owner. The
      dependency-closed `tidb-protocol` implementation already matches the
      source formatter and its 1,516-row Go-generated fixture; five source
      vectors and eight result-encoder tests pass. No production edit or
      Rust-only behavior removal was justified. Details are in
      `receipts/format_textrow_audit.md`.
- 2026-09-02: refreshed the complete eight-artifact `pkg/errno` inventory to
      Go `94eb995357` (2,820 lines) and added `ErrSharedLockLost` 9015 to the
      Rust source-ordered code and message catalogs. The exact template and key
      redaction position are pinned by the focused regression; the catalog now
      contains 1,167 codes and 1,165 messages. The Go package, full Rust error
      suite, Ready lint/format, and diff checks pass. Transaction-driver and
      session consumption remain adjacent package boundaries documented in
      `receipts/errno_audit.md`.
- 2026-09-02: completed the `pkg/errno` Go-package batch in one commit after
      pulling the latest Go master (`febee17ec716d86b1e355e5400ef9e4f4f190bad`).
      Restored the 9015 constant and exact redacted message in `errcode.go` and
      `errname.go`, added the focused catalog regression, and ran the full Go
      package test and Ready lint/diff gates. `make bazel_prepare` is required
      for the new top-level test and remains blocked by the missing local Bazel
      executable; the Rust catalog and downstream transaction/session
      consumers remain the documented boundaries.
- 2026-09-01: audited all five Go-master `pkg/keyspace` artifacts (404 lines,
      17 production function/method declarations, three tests, and one
      benchmark) including the package contract, Bazel target, and every
      source artifact. The existing `tidb-util::keyspace` owner already
      preserves namespace construction, cached keyspace bytes, logger-field
      adaptation, API-context selection, and both username policies with
      source-shaped regressions. The Go package, seven filtered Rust owner
      tests, and Rust formatting check pass; details are in
      `receipts/keyspace_audit.md`.
- 2026-09-01: audited all four Go-master `pkg/config/deploymode` artifacts
      (319 lines, 11 production function/method declarations, and three
      tests), including the package contract, Bazel target, and all encoding
      paths. The existing `tidb-config::deploymode` owner already preserves
      mode state, kernel gating, parsing, validation, JSON/TOML behavior, and
      source error strings; the Go package and three filtered Rust owner tests
      pass. Details are in `receipts/config_deploymode_audit.md`.
- 2026-09-01: audited all six Go-master `pkg/config/kerneltype` artifacts
      (196 lines, both build-tagged production variants, and two source
      tests), including the package contract and Bazel variant list. The
      existing `tidb-config::kerneltype` owner already preserves compile-time
      Classic/NextGen selection, canonical names, and old-PD empty-type
      matching; the Go package and two filtered Rust owner tests pass. Details
      are in `receipts/config_kerneltype_audit.md`.
- 2026-09-01: audited all three Go-master `pkg/config/configtypes` artifacts
      (204 lines, 10 production marshal/unmarshal methods, and two source
      tests), including the Bazel target and both JSON/TOML wrappers. The
      existing `tidb-config::configtypes` owner preserves docker/go-units
      byte-size parsing/rendering and Go duration grammar. The audit fixed its
      missing Go 1.25 hexadecimal/digit-separator float forms in `RAMInBytes`,
      added the focused regression, and passed the Go package, six filtered
      Rust owner tests, formatting check, and Ready lint gate. Details are in
      `receipts/config_configtypes_audit.md`.
- 2026-09-01: audited both Go-master `pkg/util/channel` artifacts (30 Go
      lines, one generic production function, and no source test), including
      its BUILD target. The existing `tidb-util::channel` receiver drain is an
      exact equivalent of Go's channel-range cleanup; the Go package and Rust
      utility crate check pass. Details are in `receipts/util_channel_audit.md`.
- 2026-09-02: re-audited all four Go-master `pkg/util/column-mapping` artifacts
      (888 lines across `README.md`, partition/mapping production code, seven
      source tests, and the Bazel target) at authority
      `c6054025ed4c32ab3672a2a24ea46892714d21ec`; the package is unchanged.
      The existing `tidb-util::column_mapping` owner preserves the prior
      signed partition, numeric conversion, DDL tuple, and lowercase fixes, so
      no new source change was justified. Details are in
      `receipts/util_column_mapping.md` and
      `docs/operations/util-column-mapping-audit-execplan.md`.
- 2026-09-02: fixed the Rust-only `#[must_use]` diagnostic on
      `tidb-util::format::output_format`. The complete Go-master
      `pkg/util/format` package is four artifacts and 318 lines at authority
      `c6054025ed4c32ab3672a2a24ea46892714d21ec`; its formatter state machine
      remains owned by `tidb-datatype`. Added a deny-lint regression proving
      the return may be ignored like Go; the pre-fix test failed with one
      unused-return error and the focused Rust/Go suites plus Ready lint pass.
      Details are in `receipts/util_format.md` and
      `docs/operations/util-format-audit-execplan.md`.
- 2026-09-02: fixed five Rust-only `#[must_use]` diagnostics in the complete
      `pkg/util/context` owner (two static-warning constructors and three
      plan-cache accessors). The Go package remains five artifacts and 757
      lines at authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`; added a
      deny-lint regression whose pre-fix compile failed with five errors, then
      passed the focused Rust/Go suites, formatting, and Ready lint gate.
      Details are in `receipts/util_context.md` and
      `docs/operations/util-context-audit-execplan.md`.
- 2026-09-02: re-audited all four Go-master `pkg/util/selection` artifacts
      (433 lines across introselect/median-of-medians production code, four
      source tests, benchmark registrations, and the Bazel target) at
      authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`; the package is
      unchanged. The existing Rust owner preserves signed empty results,
      duplicate handling, fallback depth, and the percentile consumer, so no
      new source change was justified. Details are in
      `receipts/util_selection.md` and
      `docs/operations/util-selection-audit-execplan.md`.
- 2026-09-02: fixed four Rust-only `#[must_use]` diagnostics in the complete
      `pkg/util/filter` owner (`is_system_schema`, `apply_on`, `apply`, and
      `matches`). The Go package remains six artifacts and 914 lines at
      authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`; added a deny-lint
      regression whose pre-fix compile failed with four errors, then passed
      the focused Rust/Go suites, formatting, and Ready lint gate. Details are
      in `receipts/util_filter.md` and
      `docs/operations/util-filter-audit-execplan.md`.
- 2026-09-02: refreshed all five Go-master `pkg/util/globalconn` artifacts
      (1,391 lines covering GCID packing/parsing, both allocators, both pools,
      nine source tests, two benchmark families, and the Bazel target) at
      authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`. Removed the two
      remaining Rust-only `#[must_use]` diagnostics from `Gcid::to_conn_id`
      and `SimpleAllocator::new`; the focused deny-lint regression failed with
      two errors before the fix and passes afterward. Current and detached
      latest Go tests, ten Rust owner tests, formatting, Ready lint, and diff
      checks pass. Details are in `receipts/util_globalconn.md` and
      `docs/operations/globalconn-audit-execplan.md`.
- 2026-09-02: refreshed all three Go-master `pkg/util/queue` artifacts (198
      lines covering the circular-buffer owner, four source subtests, and the
      Bazel target) at authority
      `c6054025ed4c32ab3672a2a24ea46892714d21ec`. Removed four Rust-only
      `#[must_use]` diagnostics from `Queue::new`, `len`, `is_empty`, and
      `cap`; the focused deny-lint regression failed with four errors before
      the fix and passes afterward. Current/detached Go tests, nine Rust owner
      tests, formatting, Ready lint, and diff checks pass. Details are in
      `receipts/util_queue.md` and
      `docs/operations/util-queue-audit-execplan.md`.
- 2026-09-02: refreshed all four Go-master `pkg/util/kvcache` artifacts (600
      lines covering the LRU owner, eight source tests, and the BUILD target)
      at authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`. Removed five
      cache-owner and one global-tracker Rust-only `#[must_use]` diagnostics;
      focused deny-lint regressions failed with five and one errors before the
      fixes and pass afterward. Current/detached Go tests, nine cache-owner
      tests, the global-tracker test, formatting, Ready lint, and diff checks
      pass. Details are in `receipts/util_kvcache.md` and
      `docs/operations/kvcache-audit-execplan.md`.
- 2026-09-02: refreshed all twelve Go-master `pkg/util/set` artifacts (1,001
      lines covering five production files, seven source tests, three
      benchmarks, the test harness, and BUILD) at authority
      `c6054025ed4c32ab3672a2a24ea46892714d21ec`. Removed all 55 explicit
      Rust-only `#[must_use]` diagnostics; the focused deny-lint regression
      failed with 47 errors before the fix and passes afterward. Current and
      detached Go tests, focused Rust owner tests, formatting, Ready lint, and
      diff checks pass. Details are in `receipts/util_set.md` and
      `docs/operations/util-set-audit-execplan.md`.
- 2026-09-01: audited all four Go-master `pkg/util/cpu` artifacts (308 lines,
      six production functions/methods, two source tests, and one test
      harness), including its failpoint, race/flaky BUILD target, cgroup/EMA,
      metric, scheduler, and server lifecycle boundaries. The direct Go test
      failure was a command error; the canonical failpoint wrapper passes the
      complete package and restores failpoint state. Rust owns the required
      cgroup support but not the observer, process sampler, shared metric,
      resource-manager scheduler, or startup consumers, so a detached partial
      owner was not added and the package remains explicitly unclaimed.
      Details are in `receipts/util_cpu_audit.md`.
- 2026-09-01: audited all nine Go-master `pkg/util/hack` artifacts (1,325
      lines, both Go 1.25/1.26 ABI variants, three source tests, two benchmark
      functions, and one test harness). The `tidb-hack` owner already preserves
      zero-copy mutable views, source Swiss-map geometry, checkpointed
      `MemAwareMap` deltas, seed/clear behavior, and the Go 1.26 layout split;
      its safe owned table is the required `RealBytes` boundary. No Rust-only
      behavior or current-baseline source gap justified a change. The Go
      package and six owner tests pass; details are in
      `receipts/util_hack_audit.md`.
- 2026-09-01: audited all nine Go-master `pkg/util/gctuner` artifacts (993
      Go/Bazel lines, 30 production methods, seven source tests, failpoint
      races, and the five-way race-enabled test target). The source's
      self-reinstalling finalizer, GOGC tuner, runtime memory-limit adjustment
      window, global-arbitration callback, and BR/server lifecycle are not
      owned by one Rust crate; `tidb-util::memory*` pieces are supporting
      boundaries, not substitutes. The canonical failpoint workflow passed
      each focused test (the long issue-48741 test passed before wrapper
      cleanup exceeded its outer poll window), so no partial Rust runtime
      owner was added. Details are in `receipts/util_gctuner_audit.md`.
- 2026-09-02: completed the nine-artifact, 993-line `pkg/util/gctuner`
      package batch against current Go master `94eb995357f34b7bab4889a82f0405797046447d`.
      The arbitration-safe server-limit cap and removal of the obsolete global
      callback/reset path are restored; the focused regression failed before
      the fix and passes afterward, as does the full failpoint-wrapped suite.
      Rust still has no dependency-closed finalizer/GOGC/memory-limit owner, so
      no Rust-only behavior was added. Ready lint and diff checks pass; Bazel
      preparation is blocked only by the missing local executable. Details are
      in `receipts/util_gctuner_audit.md` and
      `docs/operations/util-gctuner-audit-execplan.md`.
- 2026-09-01: inventoried all five Go-master `pkg/domain/serverinfo`
      artifacts (2,295 lines, including the embedded-etcd/fault harness and
      Bazel target). Added the missing status-endpoint claim to the ordinary
      server registration path with atomic create-or-observe, revision-guarded
      reattachment/cleanup, warning-only conflicts, report-status wiring, and
      focused state-machine regressions; also restored `ServerInfo.String` via
      the Rust model's display carrier. The selected Go tests, 24 filtered Rust
      tests, three-crate owner/consumer check, formatting, Ready lint, and diff
      gates pass. Cross-keyspace construction, minimum-start-TS reporting,
      DDL-owner cleanup, session cancellation, and the live-etcd concurrency
      matrix remain explicit package boundaries in
      `receipts/domain_serverinfo_audit.md`.
- 2026-09-01: audited the complete two-artifact Go-master `pkg/param`
      package (72 lines, no source tests, fixtures, generated/platform
      variants, or nested package). The existing `tidb-protocol` binary
      parameter carrier and prepared-statement adapter already preserve all
      four fields and the dedicated `ErrUnknownFieldType` errno; the two
      source-derived protocol suites pass and no Rust-only behavior was found.
      Details are in `receipts/param_audit.md`.
- 2026-09-01: audited all seven Go-master `pkg/util/profile` artifacts (611
      textual lines plus the 1,206-byte gzip pprof fixture), including the
      flamegraph DAG, runtime CPU/pprof dispatch, goroutine parser, infoschema
      profile-table integration, logging assertions, fixture, and Bazel
      target. Rust has parser/show-profile surfaces but no dependency-closed
      runtime profiler, pprof decoder/flamegraph renderer, goroutine parser,
      remote profile provider, or infoschema owner; the package is therefore
      explicitly unclaimed and no partial Rust behavior was added. Details are
      in `receipts/util_profile_audit.md`.
- 2026-09-01: inventoried all 33 Go-master root `pkg/util` artifacts (3,978
      lines, 118 production declarations, four platform variants, 11 source
      tests, and the Bazel fixture glob). The package remains explicitly
      unclaimed because its TLS/certificate, etcd, runtime, SQL-log, pool, and
      goroutine consumers do not have one dependency-closed Rust owner. Fixed
      the scoped Rust mock-PD URL boundary by removing extra ftp/tcp/udp/ws/wss
      schemes, accepting opaque unix/unixs endpoints, trimming input, and
      storing normalized URLs; the pre-fix regression failed and the focused
      owner tests, Rust format check, and Ready lint passed. Details are in
      `receipts/util_root_audit.md`.
- 2026-09-01: refreshed the root `pkg/util` receipt's comparison source to the
      requested current Go master (`5e8a1a229a`); its 33-artifact inventory is
      byte-identical to the previously audited snapshot, with the only
      intervening Go change in nested `pkg/util/dbterror`.
- 2026-09-01: audited the complete Go-master `pkg/session/test/bootstraptest2`
      package: three artifacts and 377 lines covering six historical bootstrap
      upgrades, distributed-task states, DDL-table-version persistence, the
      six-shard target, and goleak/failpoint harness. A targeted Go upgrade test
      passes; Rust's combined BootstrapSession/Domain/DDL/mock-TiKV upgrade
      pipeline remains an explicit boundary. Details are in
      `receipts/session_test_bootstraptest2.md`.
- 2026-09-01: audited the complete Go-master
      `pkg/session/test/clusteredindextest` package: three artifacts and 253
      lines covering the TestMain/goleak harness, three clustered-index tests,
      one snapshot-cache interface, and the three-shard race/flaky target.
      Rust has partial row/partition/session owners and ignored carriers, but
      no dependency-closed mock-TiKV snapshot-cache, old-row-format DML,
      TestKit executor, and randomized partition-scan owner. No Rust-only
      behavior or safe standalone implementation was found; the exact
      Go-master failpoint suite passed in 5.087s and the explicit boundary is
      recorded in `receipts/session_test_clusteredindextest.md`.
- 2026-09-02: re-audited all three Go-master `pkg/util/cgmon` artifacts (229
      lines: Linux cgroup monitor, fallback test, and Bazel target) against
      latest master `c6054025ed4c32ab3672a2a24ea46892714d21ec`. The existing
      Rust cgroup reader covers quota/memory discovery but not this package's
      ten-second scheduler, process-global lifecycle, metrics, or server
      wiring, so no detached Rust monitor was added. Details are in
      `receipts/util_cgmon.md` and
      `docs/operations/util-cgmon-audit-execplan.md`.
- 2026-09-02: re-audited all six Go-master `pkg/util/cpuprofile` artifacts
      (790 lines across the profiler, HTTP adapter, source tests, labelled load
      harness, and two Bazel targets). Rust has no dependency-closed
      process-wide runtime/pprof sampler, Google pprof merger, labelled
      goroutine source, or HTTP/profile-table consumer; no detached sampler or
      endpoint was added. Details are in
      `receipts/util_cpuprofile.md` and
      `docs/operations/util-cpuprofile-audit-execplan.md`.
- 2026-09-02: re-audited all four Go-master `pkg/util/admin` artifacts at
      authority `c6054025ed4c32ab3672a2a24ea46892714d21ec` (412 lines; the
      package is unchanged from the prior authority). Re-ran the tagged Go
      integration package tests in current and detached latest-master
      worktrees and confirmed the existing clustered-primary source fix and
      Ready evidence remain valid. Details are in
      `receipts/util_admin.md` and
      `docs/operations/util-admin-audit-execplan.md`.
- 2026-09-02: re-audited all three Go-master `pkg/util/expensivequery`
      artifacts (220 lines across the polling monitor, common goleak harness,
      and Bazel target) at authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`;
      the package is unchanged. Rust has no dependency-closed owner for the
      session-manager worker, metrics/logging throttles, or kill policies, so
      no Rust-only timer was added. Details are in
      `receipts/util_expensivequery.md` and
      `docs/operations/util-expensivequery-audit-execplan.md`.
- 2026-09-02: re-audited all four Go-master `pkg/util/benchdaily` artifacts
      (261 lines across the benchmark JSON adapter, daily combiner, common
      harness, and Bazel target) at authority
      `c6054025ed4c32ab3672a2a24ea46892714d21ec`; the package is unchanged.
      Rust has no dependency-closed CI owner for the reflection adapter or
      result-file envelope, so no Rust-only serializer was added. Details are
      in `receipts/util_benchdaily.md` and
      `docs/operations/util-benchdaily-audit-execplan.md`.
- 2026-09-02: re-audited all three Go-master `pkg/util/deeptest` artifacts
      (503 lines across the reflection comparator, exhaustive failure matrix,
      and Bazel target) at authority
      `c6054025ed4c32ab3672a2a24ea46892714d21ec`; the package is unchanged.
      It is Go-only test infrastructure with no reusable Rust comparator, so no
      Rust-only assertion framework was added. Details are in
      `receipts/util_deeptest.md` and
      `docs/operations/util-deeptest-audit-execplan.md`.
- 2026-09-01: audited all four Go-master `pkg/util/admin` artifacts (412
      lines including the restricted count path, row/index consistency scan,
      corruption integration test, and Bazel target). The existing Rust
      executor/session owner falsely scanned a clustered PRIMARY metadata
      index that Go intentionally omits because its key is the record key.
      Filtered that index from whole-table checks, made an explicitly named
      clustered primary a successful zero-reader check, and added a focused
      regression. Details are in `receipts/util_admin.md`.
- 2026-09-01: audited all three Go-master `pkg/util/expensivequery` artifacts
      (220 lines: handler, common TestMain/goleak harness, and Bazel target;
      no source tests). Threshold variables and kill signals exist in Rust,
      but the polling worker, session-manager enumeration, metrics/logging
      throttles, and domain bootstrap registration do not have a
      dependency-closed owner. The package remains explicitly unclaimed;
      details are in `receipts/util_expensivequery.md`.
- 2026-09-01: audited all four Go-master `pkg/util/benchdaily` artifacts (261
      lines: benchmark JSON conversion/writer, aggregation test, common
      harness, and Bazel target). It is CI-only `testing.B` reflection and
      repository-file aggregation with no Rust runtime owner or source
      behavior gap; the package remains explicitly unclaimed. Details are in
      `receipts/util_benchdaily.md`.
- 2026-09-01: audited all four Go-master `pkg/util/admin` artifacts (412
      lines: consistency production owner, corrupted-index integration test,
      common harness, and Bazel target). Rust's executor already owns a
      substantial admin-check implementation and source-derived corruption
      tests, but restricted-SQL/session, invisible-index, row-decoder,
      partition-handle, reporter, and SQL-command seams are not dependency
      closed for the root utility package. No duplicate checker was added;
      details are in `receipts/util_admin.md`.
- 2026-09-01: audited all three Go-master `pkg/util/deeptest` artifacts (503
      lines: reflection comparator, exhaustive failure matrix, and Bazel
      target). This is test-only infrastructure with no Rust production or
      reusable comparator owner; local Rust assertions remain intentionally
      scoped. The package remains explicitly unclaimed; details are in
      `receipts/util_deeptest.md`.
- 2026-09-02: re-audited all five Go-master `pkg/util/signal` artifacts at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` (289 lines across the Bazel
      target and POSIX, Windows, WASM, and exit
      variants). The Go package's one-shot termination handler, POSIX
      SIGUSR1 goroutine dump, Windows best-effort process signaling, and WASM
      no-op matrix are complete and unchanged from the current source. Rust
      has server-local shutdown/exit-code wiring but no dependency-closed
      cross-platform utility owner; adding another signal thread or stack
      endpoint would be Rust-only behavior. Host, Windows, and JS/WASM Go
      selections plus the adjacent Rust server library compile. Details are in
      `receipts/util_signal.md` and
      `docs/operations/util-signal-audit-execplan.md`.
- 2026-09-01: audited both Go-master `pkg/util/linter/constructor` artifacts
      (34 lines: public Bazel library and the zero-sized `Constructor` marker).
      It is static-analysis-only metadata consumed by the Go constructor
      linter; Rust has no equivalent linter or runtime consumer. The package
      remains explicitly unclaimed with no source change. Details are in
      `receipts/util_linter_constructor.md`.
- 2026-09-01: audited all three Go-master `pkg/util/tiflashcompute` artifacts
      (501 lines: dispatch-policy conversions, mock/AWS/test AutoScaler
      topology fetchers, HTTP parsing, recovery query construction, and Bazel
      target). Rust preserves AutoScaler configuration names and the TiFlash
      Compute endpoint identity but has no dependency-closed topology cache or
      startup consumer. The package remains explicitly unclaimed with no
      source change. Details are in `receipts/util_tiflashcompute.md`.
- 2026-09-02: refreshed all four Go-master `pkg/util/ddl-checker` artifacts
      (351 lines: mock-session executable checker, upstream DDL syncer, parse
      and execute matrix, and flaky Bazel test target) at authority
      `c6054025ed4c32ab3672a2a24ea46892714d21ec`. Rust still has no
      dependency-closed mock SQL executor, parser table-existence classifier,
      or upstream `SHOW CREATE TABLE` syncer; the package remains explicitly
      unclaimed. Current and detached `-tags=intest` Go tests pass. Details are
      in `receipts/util_ddl_checker.md` and
      `docs/operations/util-ddl-checker-audit-execplan.md`.
- 2026-09-01: re-audited all three Go-master `pkg/util/memoryusagealarm`
      artifacts (744 lines, including the race-enabled flaky test target and
      Go-runtime goroutine-profile cases). Rust already has a source-shaped
      `tidb-util::memoryusagealarm` seed with threshold/formatting/refresh
      tests, but no dependency-closed startup handle, TiDB config provider,
      session integration, or real heap/goroutine profile recorder. The
      package remains explicitly unclaimed; details are in
      `receipts/util_memoryusagealarm.md`.
- 2026-09-02: refreshed all five Go-master `pkg/util/extsort` artifacts (2,667
      lines: Pebble-backed SST sorting, merge/compaction, 16 source tests, and
      the Bazel target) at authority
      `c6054025ed4c32ab3672a2a24ea46892714d21ec`. Rust still has no
      Pebble/SSTable or Lightning importer owner, so executor row-spill sorting
      is not a substitute; current and detached complete Go suites pass. The
      package remains explicitly unclaimed. Details are in
      `receipts/util_extsort.md` and
      `docs/operations/util-extsort-audit-execplan.md`.
- 2026-09-02: re-audited all Go-master `pkg/util/gcutil` artifacts at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` (109 lines: GC toggles,
      safe-point SQL loader, snapshot validation, and Bazel target). Its
      behavior spans session globals/restricted SQL, vardef, model time
      conversion, TiDB errors, and TiKV GC state without one dependency-closed
      Rust helper. The package remains explicitly unclaimed; details are in
      `receipts/util_gcutil.md` and
      `docs/operations/util-gcutil-audit-execplan.md`.
- 2026-09-02: re-audited all three Go-master `pkg/util/httputil` artifacts at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` (199 lines: TLS-aware
      30-second client construction, context GET/JSON/text helpers, non-200
      body errors, two source tests, and Bazel target). Rust has separate HTTP
      transports but no shared client or BR/Lightning/object-storage
      composition root with this contract, so no adapter was added. The
      package remains explicitly unclaimed; details are in
      `receipts/util_httputil.md` and
      `docs/operations/util-httputil-audit-execplan.md`.
- 2026-09-02: re-audited all three Go-master `pkg/util/metricsutil` artifacts
      at `c6054025ed4c32ab3672a2a24ea46892714d21ec` (269 lines: process/BR
      metrics registration, keyspace labels, PD retry, one source test, and
      Bazel target). Its initialization fans out across nearly every Go metrics
      family and BR/PD consumer; Rust has only split collector fragments and
      no dependency-closed registry owner. The package remains explicitly
      unclaimed; details are in `receipts/util_metricsutil.md` and
      `docs/operations/util-metricsutil-audit-execplan.md`.
- 2026-09-01: audited both Go-master `pkg/util/gcutil` artifacts (109 lines:
      GC enable toggles, restricted `tikv_gc_safe_point` lookup, Oracle
      timestamp conversion, snapshot validation, and Bazel target). Rust has
      safe-point/flashback support but no dependency-closed session global
      accessor or `mysql.tidb` helper owner. The package remains explicitly
      unclaimed with no source change; details are in
      `receipts/util_gcutil.md`.
- 2026-09-01: audited all five Go-master `pkg/util/extsort` artifacts (2,624
  lines: Pebble-backed SST writer/reader pool, compaction/merge machinery,
  interfaces, 16 source tests, and the Bazel target). Rust has no Pebble or
  Lightning importer owner; executor row-spill sorting is a different
  dependency graph and cannot substitute for the key/value sorter. The
  package remains explicitly unclaimed with no speculative Rust behavior;
  details are in `receipts/util_extsort.md`.
- 2026-09-01: audited all three Go-master `pkg/util/httputil` artifacts (199
      lines: shared TLS/timeout client, context-bound JSON/text GET helpers,
      response/error handling, httptest matrix, and Bazel target). Rust has
      isolated HTTP transports but no dependency-closed shared client or the
      BR/Lightning/object-store composition roots. The package remains
      explicitly unclaimed with no source change; details are in
      `receipts/util_httputil.md`.
- 2026-09-01: audited the complete Go-master `pkg/expression/expropt`
      package: 13 tracked artifacts and 1,116 lines, including all eleven
      provider/reader production files, the provider-array type checks, the
      ten-key optional-property test, the new EMBED_TEXT session-context
      source, and the BUILD target. Rust's `tidb-expr` owner had only nine
      keys/providers; the focused regression failed at `OPT_PROPS_CNT == 9`.
      Added the tenth key and descriptor, a nil-preserving
      `SessionContextPropProvider`/reader, the complete provider test case,
      and live `sessionexpr` installation through a narrow adapter forwarding
      trace context, session variables, and domain values. Exact Go-master
      tests and Rust expropt/exprctx/sessionexpr suites pass; details are in
      `receipts/expression_expropt.md`.
- 2026-09-01: audited the complete Go-master `pkg/expression/exprctx`
      package: six tracked artifacts and 740 lines, including allocator,
      Eval/Build/Expr and static-conversion contracts, the truncate override,
      all ten optional-property keys, empty parameter values, the four source
      tests, and the four-shard Bazel target. The focused regression initially
      failed because Rust exposed only the parameter-error string; the batch
      adds the typed `ParamValues`/`EmptyParamValues` contract and implements it
      for static and live evaluators. It also closes the master-added
      `NewCollationEnabled` divergence by capturing the mode in static contexts,
      exposing the live-session read, and preserving it across static cloning.
      The umbrella interfaces and truncate-level wrapper remain explicit
      dependency boundaries with no Rust-only substitute. Exact Go-master tests,
      focused Rust tests, formatting, diff checks, and Ready lint pass; details
      are in `receipts/expression_exprctx.md`.
- 2026-09-01: audited the complete Go-master `pkg/expression/exprstatic`
      package: five tracked artifacts and 2,036 lines, including both static
      context implementations, all thirteen source tests, the current-time,
      warning, optional-property, parameter-copy, cloning, and system-variable
      paths, plus the thirteen-shard Bazel target. The only master delta is the
      per-context `NewCollationEnabled` field and its `collate` dependency;
      `tidb-expr` now captures, overrides, clones, and exposes that value in
      static and live contexts. Exact Go-master and Rust carrier suites pass;
      the full inventory and remaining sysvar-catalog boundary are recorded in
      `receipts/expression_exprstatic.md`.
- 2026-09-01: audited the complete Go-master `pkg/expression/sessionexpr`
      package: three tracked artifacts and 837 lines, including the live
      expression/evaluation contexts, timestamp and privilege paths, all ten
      optional-property registrations, sequence operator, five source tests,
      and the five-shard Bazel target. The master additions (`NewCollationEnabled`
      and `OptPropSessionContext`) are implemented through the live runtime
      mode and a narrow session-context adapter, with the shared parameter
      contract applied to live evaluation. Exact Go-master and Rust carrier
      suites pass; details are in `receipts/expression_sessionexpr.md`.
- 2026-09-01: audited the complete Go-master `pkg/expression/generator`
      directory: eight tracked artifacts and 3,162 lines, including the
      compiled thread-safety generator, five build-ignore vector/template
      inputs, seven helper type contexts, and both Bazel targets. The current
      master snapshot has no delta from the pinned source. This is an explicit
      Go-only code-generation boundary; generated parent-package outputs remain
      covered by the expression test-port receipts. Details are in
      `receipts/expression_generator.md`.
- 2026-09-01: audited the complete Go-master `pkg/expression/integration_test`
      package: four tracked artifacts and 4,906 lines, including 63 SQL tests,
      the common failpoint/goleak harness, README rule, and 50-shard target.
      The master-only EMBED_TEXT/auto-embedding additions are recorded as a
      dependency-closed provider→DDL→executor gap; details are in
      `receipts/expression_integration_test.md`.
- 2026-09-01: audited the complete Go-master
      `pkg/expression/test/constantpropagation` package: three artifacts and
      148 lines. Its single plan-tree regression passes in Go; Rust carries the
      expression-level propagation but not the mock-storage planner fixture.
      Details are in `receipts/expression_constantpropagation_test.md`.
- 2026-09-01: audited the complete Go-master
      `pkg/expression/test/multivaluedindex` package: three artifacts and 405
      lines. Four exact KV-key/duplicate/partition regressions pass in Go;
      Rust's ARRAY index admission remains an explicit dependency-closed DDL,
      executor, and storage gap. Details are in
      `receipts/expression_multivaluedindex_test.md`.
- [ ] Run Ready validation and self-review only when the requested parity scope
      is genuinely complete enough for a final-status claim.

- 2026-09-02: completed a bounded `pkg/domain` Go-package batch against Go
  master `1c1a334d2be1dce64888b6e1f054462c566b0734`: the
  `DumpFileGcCheckerLoop` now reads the process-global plan-replayer file
  retention setting on each GC round, and a focused domain regression pins
  that lookup. The complete direct root inventory is 31 artifacts and 9,140
  lines; nested domain packages remain separate boundaries. The Rust
  `tidb-domain` GC primitive already accepts caller-supplied durations, so no
  Rust production edit was needed. Focused and full failpoint-aware Go domain
  suites pass; lint, Rust formatting, and diff checks are Ready gates, while
  `make bazel_prepare` is blocked by the unavailable Bazel executable. Details
  are in `receipts/domain_plan_replayer_retention.md`.

- 2026-09-02: completed a bounded `pkg/meta/autoid` Go-package batch against
  Go master `1c1a334d2be1dce64888b6e1f054462c566b0734`. Restored the complete
  Go-master `autoid_service.go` synchronization/retry behavior, its focused
  transfer/concurrency/RPC regressions, and the 17-shard BUILD metadata in one
  package unit. The Rust `tidb-exec` client slice was already aligned; its
  etcd-backed server owner remains a separate boundary. Focused and full
  failpoint-aware Go package suites pass, with lint, Rust formatting, and diff
  checks as Ready gates; `make bazel_prepare` is blocked by the unavailable
  Bazel executable. Details are in `receipts/meta_autoid_audit.md`.

- 2026-09-02: completed a bounded direct `pkg/planner` Go-package batch against
  Go master `1c1a334d2be1dce64888b6e1f054462c566b0734`. Restored the optimizer's
  TiKV-only/TiFlash-only alternative rounds, mixed-engine and storage-hint
  gates, per-invocation cleanup closures, and Explain/binding hint warning
  coverage; added a focused gate/restoration regression and BUILD test target.
  The focused and full failpoint-aware package suites pass, as do lint, Rust
  formatting, and diff checks; `make bazel_prepare` is blocked by the missing
  local Bazel executable. Full mock TiFlash MPP fixture execution remains a
  separate nested-package boundary. Details are in
  `receipts/planner_engine_rounds.md`.

- 2026-09-02: completed the direct `pkg/meta` starter-bootstrap batch against
  Go master `1c1a334d2be1dce64888b6e1f054462c566b0734`. The complete seven-
  artifact, 4,107-line inventory was read before editing. Restored Go's
  `StarterBootstrapKey` mutator/Reader accessors and materialized-view
  next-generation boot-table version 3, then added matching Rust `tidb-meta`
  key, transaction, malformed-scalar, and round-trip coverage. Focused and
  full failpoint-aware Go tests plus the focused Rust test pass; Rust format,
  lint, and diff checks pass. `make bazel_prepare` is required by the Go
  package change but is locally blocked by the missing Bazel executable.
  Details are in `receipts/meta_starter_bootstrap.md`.

- 2026-09-02: completed a bounded `pkg/meta/metadef` schema-definition batch
  against Go master `78cac443a4f46c13bfe27eb247b5c80657952547`. The complete
  seven-artifact, 1,260-line direct inventory was read before editing. Go now
  declares `Operate_view_priv` in the user and database privilege tables and
  `Operate View` in the table-privilege SET, matching the existing Rust
  `tidb-metadef` owner. A focused DDL contract regression and the full Go
  package suite pass; the required Bazel preparation gate remains blocked by
  the unavailable local executable. Materialized-view table constants and
  upgraded-cluster migration remain explicit `pkg/session` boundaries. See
  `receipts/meta_metadef_operate_view.md`.

- 2026-09-02: completed the follow-up `pkg/meta/metadef` materialized-view
  system-definition batch against Go master
  `78cac443a4f46c13bfe27eb247b5c80657952547`. The complete seven-artifact,
  1,375-line direct inventory was reread before editing. Go now restores the
  five reserved materialized-view maintenance IDs and their exact CREATE TABLE
  constants already present in Rust, with a focused ID/DDL contract regression
  and the full package suite passing. No Rust-only duplicate behavior was
  added; the required Bazel preparation gate remains blocked by the unavailable
  local executable. Details are in `receipts/meta_metadef_operate_view.md`.

## Decision Log

- Decision: implement compact `TIMESTAMP` STRING parsing in the shared
  datetime value domain with Go's width table (`14/12/11/10/9/8/7/6/5`),
  pivot two-digit years, six-digit half-up fractional rounding, and the
  date-only compact hour suffix. Select the parser's `is_float` branch from
  the runtime datum kind so numeric and STRING suffix meanings remain distinct.
  Date/Author: 2026-09-04, Codex.

- Decision: treat `Datum::UInt` as the value-tier equivalent of Go's
  `UnsignedFlag` for `MAKETIME`'s first argument. Detect a wrapped negative
  hour before sign selection, set the Go overflow clamp to +838 hours, and
  leave signed negative hours and all minute/second validation unchanged.
  Date/Author: 2026-09-04, Codex.

- Decision: preserve the source datum kind for `TIMESTAMP`'s one-argument
  parser. Route integer, real, float32, and DECIMAL values through the Go
  `ParseTimeFromFloatString` equivalent (`parse_time` with `is_float = true`),
  while retaining the compact STRING parser for textual and temporal values.
  Map the shared parsed core/FSP into `GoDateTime` so numeric zero-date
  fractions and packed datetime fractions retain Go's output precision; string
  date-only fractions retain Go's hour interpretation.
  Date/Author: 2026-09-04, Codex.

- Decision: reject `TIMESTAMPADD` bases that cannot pass Go's `Time.GoTime`
  conversion before invoking `addUnitToTime`. Check `GoDateTime::in_range`
  immediately after parsing, so zero or month/day-zero values cannot be
  normalized by the signed day-number helper into a non-zero date. Keep the
  existing result-range and unit-overflow checks unchanged. Date/Author:
  2026-09-04, Codex.

- Decision: preserve `UNIX_TIMESTAMP`'s source datum kind through the
  value/chunk seam. Route integer, real, float32, and DECIMAL inputs through
  `tidb_datatype::parse_time` with `is_float = true` so packed YYMMDDHHMMSS /
  YYYYMMDDHHMMSS values and six-digit half-up fractions match Go; route text
  and temporal values through the ordinary string branch. Keep an explicit
  all-zero-date NULL check before the partial zero-in-date numeric-zero
  sentinel. Date/Author: 2026-09-04, Codex.

- Decision: map invalid `PERIOD_ADD` and `PERIOD_DIFF` values to
  `EvalError::IncorrectArguments` with the lower-case function name, matching
  Go's `errIncorrectArgs.GenWithStackByArgs` message while retaining the
  evaluator's existing omission of the server error-code prefix. Preserve
  argument evaluation order and the unsigned month arithmetic. Date/Author:
  2026-09-04, Codex.

- Decision: reuse `tidb_datatype::parse_time_from_num` for delimiter-free
  12/14-digit datetime strings and map its validated fields into the local
  `GoDateTime`, preserving Go's packed numeric normalization and two-digit-year
  window. Select `fsp_for_time_add_sub` only for the DATE+STRING signature;
  DATETIME and DURATION string arms retain `GetFsp`. Date/Author: 2026-09-04,
  Codex.

- Decision: make `add_sub_time` select the Go `Time.Add` duration FSP by
  evaluator tier. Preserve `getFsp` for row-path constants and string-second
  vector calls, but use `Fsp=-1` only for the vectorized DATETIME+TIME arm;
  this mirrors the generated Go bodies without changing DATE or untyped
  string dispatch. Date/Author: 2026-09-04, Codex.

- Decision: make `FORMAT` precision conversion accept the statement context
  and delegate string/byte inputs to `to_i64_signed_with_warnings`, rather
  than duplicating prefix parsing and warning rules. Keep integer, decimal,
  real, and NULL branches unchanged. Treat `WEIGHT_STRING`'s warning-only
  carrier as a test-context integration cleanup because its production chunk
  arm already appends the source 1292 message. Date/Author: 2026-09-04,
  Codex.

- Decision: reuse `tidb_datatype::Decimal::from_f64` for `FROM_UNIXTIME`
  real and float32 arguments instead of formatting with a fixed precision.
  That helper is already pinned to Go's `strconv.FormatFloat(..., 'g', -1,
  64)` spelling, while `evalFromUnixTime` retains the source FSP-6 rounding,
  range, and timezone rules. Date/Author: 2026-09-04, Codex.

- Decision: route `FROM_BASE64` through a context-aware evaluator arm before
  the existing values-only dispatch table. Compute Go's estimated decoded
  length from the unstripped input and guard multiplication with the Go `int`
  limit (`isize::MAX / 3`), while retaining the context-free helper for direct
  value calls. Date/Author: 2026-09-04, Codex.

- Decision: mirror Go's `SubInt64` guard and wrapping subtraction exactly,
  including the `MinInt64` negation edge. Rust's checked subtraction was a
  Rust-only refusal for positive-minus-`MinInt64`; `wrapping_neg` and
  `wrapping_sub` preserve the source's two's-complement result while the
  explicit zero-minus-`MinInt64` guard still reports overflow. Date/Author:
  2026-09-04, Codex.

- Decision: preserve Go's explicit `math.MaxUint32` spelling for negative
  `%X`/`%x` week-years by centralizing the sign check in the Rust formatter's
  local week-year helper. Positive years retain their existing zero-padded
  output, and no week-number arithmetic is changed. Date/Author: 2026-09-04,
  Codex.

- Decision: share the already source-pinned `is_go_punctuation` classifier
  from `tidb-datatype` with the duplicate expression-level `STR_TO_DATE`
  parser instead of introducing a second Unicode dependency/table. This keeps
  Go's Unicode-version exclusions single-sourced while changing only the
  previously ASCII-only expression token. Date/Author: 2026-09-04, Codex.
- 2026-09-04: mirror Go `ErrClass.NewStd` by resolving `registered_std`
  messages from the TiDB/`errno` catalogue first and using parser/MySQL only
  for codes absent there. Keep explicit `registered_standard` callers intact,
  because their hand-selected message is a separate Go source decision.

- 2026-09-04: preserve Go's `TxnRetryableMark` as one Rust constant and append
  it in the ordinary `TxnErrorKind::WriteConflict` wire-rendering arm. Do not
  synthesize absent structured conflict fields or conflate the separate 8005
  undetermined-commit pipeline; those require a distinct Go comparison and
  remain documented boundaries.

- 2026-09-04: replace the Rust JSON merge left fold with Go's three-stage shape
  (`MergeBinaryJSON` run grouping, recursive `mergeBinaryObject`, and one-level
  `mergeBinaryArray` flattening). This preserves the existing `JSONNode` model
  while restoring the source's behavior when arrays interrupt object runs.

- Decision: remove only the Rust-side empty-charset suppression in
  `FieldType::restore_as_cast_type`; Go's formatter intentionally appends the
  `CHARSET ` clause for every charset other than `binary` and `utf8mb4`, and
  the degenerate empty spelling is therefore observable. Date/Author:
  2026-09-04, Codex.

- Decision: share the existing Go-compatible float spelling helpers between
  the runtime and parser-driver default field-type owners. This keeps the
  source's `FormatFloat(..., 'f', -1, bits)` width contract in one place and
  avoids a runtime-only special case for infinities. Date/Author: 2026-09-04,
  Codex.
- 2026-09-04: retain `serde_json` as the general JSON string escaper, but wrap
  it at the binary-JSON text boundary to replace only U+2028/U+2029 with the
  Go safety escapes. This keeps ordinary quoting stable while matching Go's
  JSONP-safe output in scalar values, object keys, and path-key quoting.

- Decision: expose fixed-word truncation from decimal division as a separate
  warning-bearing API while keeping the existing value-only wrappers for
  callers that do not carry a statement context; wire only the SQL `/` path
  to `EvalContext::handle_truncate`, matching Go's `DecimalDiv`/`HandleTruncate`
  order. Date/Author: 2026-09-04, Codex.
- 2026-09-04: keep `Datum::Decimal` as the exact value-layer representation,
  but add a chunk-boundary lossy conversion that delegates overflow/truncation
  to `MyDecimal::from_string`. Go datums already own that fixed cell, so
  `AppendDatum`/`MutRow` must not invent a panic when Rust's intermediate value
  has more than nine base-1e9 words; exact conversions still retain hidden
  fraction words unchanged.

- Decision: use ryu's human-readable shortest formatter for
  `Decimal::from_f64`, apply Go's `%g` fixed/scientific threshold to those
  digits, then pass the result directly to the existing Go-shaped decimal
  parser. Expanding `f64::to_string()` positionally changes fixed nine-word
  overflow/truncation behavior. Date/Author: 2026-09-04, Codex.

- 2026-09-04: encode Go's `inUnion` cast flag in the internal function name
  `cast_unsigned_in_union`, backed by `CastType::UnsignedInUnion`, instead of
  adding session state to `ScalarFunction`. This keeps the plan immutable and
  lets the existing source-type-aware evaluator apply the same clamp rules.
- 2026-09-04: scope this batch to unsigned integer-target casts, as specified
  by the next-work-unit queue. Go's string-to-DECIMAL `inUnion` tests remain
  documented ignored gaps until a target-specific decimal carrier exists.

- Decision: preserve Go's pre-round digit-bound check in Rust decimal shifting
  by inspecting the source digit prefix before accepting a rounding carry.
  Checking only the rounded numeric value would incorrectly retain a carry
  after all source digits had fallen outside the fixed word buffer. Date/Author:
  2026-09-04, Codex.

- Decision: use `unicode-general-category` for `STR_TO_DATE`'s `%.'` token and
  explicitly subtract the 13 punctuation code points introduced in Unicode
  16.0. The fetched Go 1.25 source uses Unicode 15.0, so Rust's default
  Unicode table would otherwise accept newer punctuation that Go rejects.
  Date/Author: 2026-09-04, Codex.

- Decision: keep top-level `pkg/util/logutil` on the existing
  `tidb-util::logutil` owner and record `pkg/util/logutil/consistency` as a
  separate explicitly unclaimed package. The logger owner already carries the
  source-derived configuration, contextual fields, slow-log, hex, rotation,
  and sampler behavior; adding a second context/logger path would be
  Rust-only duplication. The consistency reporter additionally requires the
  helper-storage MVCC RPC, tablecodec/model decoding, redaction, and zap
  reporting stack, while Rust's `admin_check` is a consumer-specific checker
  rather than a drop-in reporter. Date/Author: 2026-09-01, Codex.

- Decision: forward Go `FileLogConfig.MaxDays` into the existing rotating sink
  and parse only lumberjack-shaped backup names for age/count cleanup. The
  previous Rust sink silently ignored age retention and deleted any filename
  sharing the log stem; the focused regression covers both source contracts.
  Keep the four-argument `RotatingFile::open` API as a zero-age compatibility
  wrapper while `build_sink` uses the new age-aware constructor. Date/Author:
  2026-09-01, Codex.

- Decision: add Go master's `SimpleLRUCache.Peek` to the existing indexed
  `tidb-kvcache` owner as an immutable lookup, not by reusing `get` and then
  restoring list links. This keeps the O(1) no-promotion contract visible to
  stmt-summary callers and leaves all eviction/callback paths unchanged.
  Date/Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/tableutil` unclaimed until the model, session,
  transaction, executor, and auto-ID owners can provide one real temporary
  table object plus its package-initialized factory. A Rust-only trait or
  global constructor would duplicate existing SQL paths and cannot satisfy
  Go's cross-package consumers. Date/Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/profile` as an explicit boundary until the pprof
  decoder, CPU-profiler lifecycle, performance-schema result tables, and
  session/logging consumers can move as one package unit. The existing Rust
  SEM table-name list is metadata only; a detached flamegraph parser or
  sampling thread would not be an ordinary SQL execution path. Date/Author:
  2026-09-01, Codex.
- Decision: carry Go `DeleteKeyFromEtcd`'s per-attempt timeout and retry loop
  in `tidb-pd-client::EtcdClient`, then bind the server-info trait adapter to
  its source five-attempt/one-second values. Keep namespace wrapping outside
  this batch because Rust has no caller or dependency-closed owner for a
  mutable clientv3 KV/Watcher/Lease namespace; an unused prefixing client
  would be a second transport path. Date/Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/profile` explicitly unclaimed. A detached Rust
  pprof parser or flamegraph renderer would omit the shared CPU profiler,
  runtime goroutine dump, infoschema profile tables, remote TiKV/PD fetches,
  logging contract, and fixture-backed integration test; those dependencies
  must land atomically before a package-complete claim. Date/Author:
  2026-09-01, Codex.
- Decision: keep root `pkg/util` explicitly unclaimed despite aligning the
  mock-PD URL test boundary. The Go package combines independent runtime,
  security, etcd, SQL/session, and storage consumers; only the URL helper's
  test seam was dependency-closed. Do not introduce a detached Rust root-util
  crate or silently claim the whole package until those consumers and their
  platform/build/test artifacts can move atomically. Date/Author: 2026-09-01,
  Codex.
- Decision: keep `pkg/util/cgmon` unclaimed. `tidb-util::cgroup` and process
  memory readers are supporting authorities, while the Go monitor owns its
  Linux-only refresh cadence, metric publication, panic recovery, and startup
  lifecycle; a second Rust timer would be Rust-only behavior. Date/Author:
  2026-09-01, Codex.
- Decision: keep `pkg/util/cpuprofile` unclaimed until the runtime profiler,
      pprof decoder/merge path, HTTP handler, profile-table consumers, and
      labelled test harness can move as one dependency-closed package. A detached
      Rust sampler or endpoint would duplicate Go runtime ownership and create
      behavior without its SQL/logging consumers. Date/Author: 2026-09-01, Codex.
- Decision: preserve the clustered PRIMARY metadata entry but exclude it from
      Rust ADMIN CHECK scans, matching Go's `buildPhysicalIndexLookUpReaders`
      zero-reader path. A clustered primary key is the record key, not an `_i`
      range; treating it as a secondary index produces a false inconsistency.
      Date/Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/expensivequery` unclaimed until the session manager,
  process-list, histogram/logging, kill-action, and domain bootstrap owners
  can move together. Rust threshold constants alone are not a substitute for
  the Go polling worker and would create Rust-only policy. Date/Author:
  2026-09-01, Codex.
- Decision: keep `pkg/util/benchdaily` as Go-only CI tooling. Rust benchmark
  targets are not a substitute for Go's `testing.B` reflection and repository
  JSON aggregation flags; adding a second report format would be Rust-only
  behavior with no runtime consumer. Date/Author: 2026-09-01, Codex.
- Decision: retain `tidb-executor::admin_check` as the sole native consistency
  checker while keeping root `pkg/util/admin` unclaimed. Its existing tests
  cover the byte-level corruption core, but adding a utility wrapper would
  duplicate the Go session/restricted-SQL/index/reporting integration that is
  still outside the dependency closure. Date/Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/deeptest` Go-only test infrastructure. Its
  reflection, pointer-alias, path-glob, and expected-failure semantics have no
  production consumer; a Rust assertion framework would be a second test
  policy rather than a dependency-closed transcreation. Date/Author:
  2026-09-01, Codex.
- Decision: keep `pkg/util/signal` explicitly unclaimed until the Rust server
  can own the complete cross-platform signal contract (termination ordering,
  POSIX goroutine dump, Windows process signaling, and WASM no-ops) together
  with every startup consumer. The existing `shutdown_signal`/`signal_exit`
  modules cover only server-local Unix shutdown and exit-code behavior; a
  detached signal adapter would duplicate ownership and create Rust-only
  behavior. Date/Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/linter/constructor` Go-only. Its empty
  `Constructor` embedding is interpreted by an external Go AST linter and has
  no Rust runtime or build consumer; adding a native marker would create a
  second, Rust-only static-analysis policy. Date/Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/tiflashcompute` explicitly unclaimed until the
  Rust server owns dispatch conversion, AutoScaler HTTP/mock topology fetch,
  monotonic timestamp caching, recovery parameters, and all startup
  consumers together. Configuration constants and endpoint identity alone do
  not reproduce the Go package; a detached Rust fetcher would be Rust-only
  behavior. Date/Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/breakpoint` Go-only. Its sole behavior is a named
  failpoint invoking a typed callback stored in `sessionctx`; Rust has no
  equivalent failpoint runtime or callback consumer, so adding one would be
  test-only Rust behavior. Date/Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/ddl-checker` explicitly unclaimed until parser,
  mockstore/session execution, DDL table-existence classification, and the
  upstream database syncer can move as one dependency-closed owner. Rust's
  ordinary DDL planner is not a substitute for this test/tooling contract;
  adding a checker-only session would create Rust-only behavior. Date/Author:
  2026-09-01, Codex.
- Decision: keep `pkg/util/memoryusagealarm` explicitly unclaimed despite the
  existing Rust seed. The Go contract couples a 100ms lifecycle, global
  config/vardef reads, session-manager SQL snapshots, retention, and runtime
  heap/goroutine profile side effects; a provider-only tick API or detached
  recorder would omit observable behavior and create Rust-only policy. Date/
  Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/gcutil` explicitly unclaimed until the session,
  restricted-SQL, TiKV Oracle, and GC-worker owners can provide the complete
  safe-point/global-variable contract together. Existing Rust safe-point
  caches and flashback tests are supporting boundaries, not a replacement for
  these public helpers; a detached SQL bridge would be Rust-only behavior.
  Date/Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/httputil` explicitly unclaimed until the shared
  client, TLS/timeout policy, body ownership, and BR/Lightning/object-store
  consumers can move as one dependency-closed owner. A Rust-only generic HTTP
  helper would duplicate isolated transports and risk observable divergence.
  Date/Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/regionsplit` explicitly unclaimed. Rust's table-key
  encoders, split transport, and policy metadata do not provide the Go
  package's dependency-closed integer/common-handle/index key derivation,
  prefix-boundary insertion, minimum-step checks, and typed DDL error contract.
  Adding a detached key generator would omit its `pkg/ddl` and
  `pkg/executor` consumers and create Rust-only split behavior. Date/Author:
  2026-09-01, Codex.
- Decision: keep `pkg/util/skip` and `pkg/util/syncutil` explicitly unclaimed.
  `skip` is Go test-selection infrastructure, while `syncutil` is a
  package-wide type-identity and `deadlock` build-tag boundary used by dozens
  of Go consumers. Rust test attributes and crate-local standard locks cannot
  replace those contracts; adding facades would be Rust-only behavior or a
  broad uncoordinated source break. Date/Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/injectfailpoint` explicitly unclaimed. Its named
  DXF random-error callbacks and partial-read injection depend on Go's runtime
  failpoint registry and have no matching Rust production consumer; adding a
  probabilistic Rust hook would be test-only Rust behavior. Date/Author:
  2026-09-01, Codex.
- Decision: retain `tidb-util::sys::linux` as the sole native owner of the
  six-artifact OS utility boundary. Its Linux/Windows/other-target variants
  and server affinity call are dependency-closed; the extra peer-UID and
  no-op checks are platform corroboration, not alternate Rust policy. Date/
  Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/mock` explicitly unclaimed as Go-only test
  infrastructure. Its session-context, fake-transaction, KV-store/client,
  iterator, and metrics mocks span too many Go interface owners for a
  dependency-closed Rust port; crate-local mocks are not substitutes and a
  shared Rust framework would be Rust-only behavior. Date/Author: 2026-09-01,
  Codex.
- Decision: retain `tidb-schemacmp` as the sole owner of the complete
  `pkg/util/schemacmp` package. Its lattice, metadata encoding, restore, and
  parser/model joins are dependency-closed and the nine source tests pass;
  no duplicate Rust-only schema comparator is needed. Date/Author:
  2026-09-01, Codex.
- Decision: preserve the Rust planner-error declaration table and its
  all-prototype initialization guard. Go package initialization registers all
  98 entries, while Go's only explicit test checks 59; the Rust guard is the
  executable equivalent of validating the remaining initializers and is not a
  Rust-only runtime policy. Keep `ErrAccessDenied`'s isolated special-message
  initializer even though its static is declared last. Date/Author:
  2026-09-01, Codex.
- Decision: Go master's three dual-password executor errors require no Rust
  edit because the requested hparser branch already owns and fixture-checks
  them. Preserve the complete generated fixture as source evidence and avoid a
  second partial test that cannot fail independently of the all-entry guard.
  Date/Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/execdetails` explicitly unclaimed until its four
  Rust seed owners can include context, client-go/protobuf/resource-manager,
  Prometheus, zap, and ordinary executor integration. The recent Go-master
  additions (`ReadPoolTaskDetails`, row/summary evidence, analyze scan bytes,
  hash-state, and Explain-RU stats) cannot be added as isolated Rust fields;
  removing the existing seed wrappers now would strand their current callers.
  Date/Author: 2026-09-01, Codex.
- Decision: serialize `SqlKiller`'s signal CAS/swap and kill-event state under
  one mutex, as Go master does. A receiver's sender is dropped after the
  trigger token and on reset, giving the cancellation consumer a permanently
  ready closed generation instead of the prior Rust-only one-shot reset
  message. Logging stays outside the lock, while the memory-arbitrator status
  reloads signal and reason under it. Date/Author: 2026-09-01, Codex.
- Decision: add VectorFloat32 to the existing native spill serialization
  owner, not to a new aggregate-specific carrier. Go wraps the vector's
  little-endian image in the same native-width buffer prefix as strings and
  JSON; Rust's datatype image and owned decoder already provide that exact
  contract. Higher-level `pkg/executor/aggfuncs` callers remain consumers,
  not a second utility implementation. Date/Author: 2026-09-01, Codex.
- Decision: change `compile_like_to_regexp` to accept and forward the Go
  `CompileLike2Regexp` escape byte. The utility has no Rust production callers
  of this helper, so the API change is dependency-closed; the existing
  expression LIKE path already forwards its escape through `compile_pattern`.
  A focused regression covers custom `+` escaping and confirms a backslash is
  ordinary data when it is not the selected escape. Date/Author: 2026-09-01,
  Codex.
- Decision: represent Go's exported `ColumnFilterRules` as a public,
  non-clonable Rust rule-list owner with an inherent `match_column` method and
  the existing `ColumnFilter` trait implementation. `ParseColumnFilter` boxes
  that same concrete value, while `ParseColumnFilterRules` returns it directly;
  this preserves both Go entry points without inventing a second matcher path.
  Date/Author: 2026-09-01, Codex.
- Decision: implement Go master's `Chunk.UsedMemoryUsage` in the existing
  `tidb-chunk` owner, retaining the shared 112-byte column payload term and
  using lengths for null-bitmap, offsets, data, and element buffers. Keep
  `MemoryUsage` capacity-based and do not invent a consumer for this
  informational API. Date/Author: 2026-09-01, Codex.
- Decision: keep `tidb-codec::Encoder` limited to comparable-key encoding,
      matching Go master's key-only type. Move value and hash implementations to
      the existing package-level functions and update every searched Rust
      consumer; do not preserve a Rust-only fixed-collation value/hash method.
      Date/Author: 2026-09-01, Codex.
- Decision: extend the existing `tidb-vardef` constants owner with the exact
      Go-master name/default/bound additions, including duration defaults as
      nanoseconds. Delete the removed `DefTiDBMergePartitionStatsConcurrency`
      default while retaining its backward-compatible name constant, because Go
      still registers that name with a literal value. Do not fabricate the
      unported SysVar registry or SessionVars layer in this constants batch.
      Date/Author: 2026-09-01, Codex.
- Decision: treat the current `pkg/tablecodec`/`pkg/util/rowcodec` delta as a
      caller-surface cleanup because Rust already exposes free row/value
      functions matching Go master. Keep the retained `Encoder` only at the
      `GenIndexKey` comparable-key boundary; do not add a compatibility wrapper
      for the removed row encoder argument. Date/Author: 2026-09-01, Codex.
- Decision: preserve the V2 typed fast path for ordinary and new-collation
      reads, but route old-collation common handles through the existing map
      decoder. The typed leaf has one default restored-data policy, while Go's
      decoder is mode-sensitive; this narrow fallback restores the handle
      component without duplicating row decoding. Date/Author: 2026-09-01,
      Codex.
- Decision: for the continuing loop, newly selected packages compare against
  the fetched Go `origin/master`; the older `e2788410...` pin remains the
  historical source for receipts already completed. `pkg/util/plancodec`
  therefore includes Go master `Analyze=64` rather than treating ID 64 as
  unknown. Date/Author: 2026-09-01, Codex.
- Decision: historical package receipts use pinned Go commit
  `e2788410d8d696605e8cb002585877a063ccc909`; newly selected packages use the
  exact fetched Go `origin/master` commit recorded in their receipt. In both
  cases the behavioral reference is Go source, not Rust parity comments.
  Date/Author: 2026-08-28, updated 2026-09-01, Codex.
- Decision: ignored empty tests and documentary receipts are removed rather
  than counted as ports. Real unsupported behavior is either implemented in
  its correct owner or left as an explicit unclaimed inventory item.
  Date/Author: 2026-08-28, Codex.
- Decision: changes may cross Rust crates when ownership requires it, but each
  diff remains tied to a Go-derived behavior and scoped regression.
  Date/Author: 2026-08-28, Codex.
- Decision: package parity is atomic. File-, function-, batch-, or test-level
  progress cannot be reported as a completed transcreated Go package.
  Date/Author: 2026-08-28, Codex.
- Decision: `pkg/util/compress` owns generic pooled gzip streams, not a
  statistics-specific codec. The Rust owner uses `tidb_util::zeropool::Pool`
  with erased `Send` reader/writer targets, and statistics JSON framing is its
  first consumer. Reset disables the old target before flate2 drops it so an
  unfinished stream cannot receive a trailer; the absent ingest-control owner
  remains an explicit integration boundary. Date/Author: 2026-09-01, Codex.
- Decision: injected ports are not a substitute for an ordinary Go package
  owner when they move metadata lookup, SQL execution, DML versioning, worker
  lifecycle, or retry reconstruction to callers. Such unconsumed alternate
  runtimes are removed and the package remains unclaimed until its real
  dependencies can land atomically. Date/Author: 2026-08-29, Codex.
- Decision: `pkg/util/traceevent` must register directly with
  `rust/third_party/tikv-client-rs/src/trace.rs`; the local
  `ClientGoTraceRegistry`, category enum, and flag wrapper are deleted rather
  than retained as a second inactive implementation. The real vendored client
  has the same three global hook setters and already emits events from normal
  request, lock, and region-cache paths. Date/Author: 2026-08-29, Codex.
- Decision: `pkg/util/sli` consumes commit and scan details from the ordinary
  executor/transaction seams. Estimated row sizes, affected-row proxies, and
  backend- or cache-specific reporting pipelines are not substitutes for Go's
  `CommitDetail` and `ScanDetail`. Date/Author: 2026-08-29, Codex.
- Decision: `pkg/session/syssession` is one context-generic package owner.
  Consumers retain their concrete `sessionctx.Context` capabilities through
  that owner; they must not restate `Session`, reuse flags, or partial pool
  interfaces locally. Date/Author: 2026-08-29, Codex.
- Decision: GoMock reflection and controller internals are language-specific
  generation mechanics. `pkg/util/sqlexec/mock` owns a native strict recorder
  over the already-complete Rust executor trait; it does not own or duplicate
  SQL execution behavior. Date/Author: 2026-08-29, Codex.
- Decision: `pkg/statistics/handle/util` is one owner rather than five
  independent statistics leaves. Its worker dependency is implemented
  natively with the source-observable pool policy, while SQL/session/model
  behavior uses the already shared package contracts. The partial modules and
  their duplicate tests are deleted rather than retained beside the atomic
  owner. Date/Author: 2026-08-29, Codex.
- Decision: the `util/test.CtxMatcher` must inspect the same typed TiKV request
  context carried by `StatsCtx`; accepting a pre-extracted string bypasses the
  behavior the Go matcher exists to verify. The support package remains a
  distinct crate and adds no source-absent tests. Date/Author: 2026-08-29,
  Codex.
- Decision: `pkg/statistics/handle/logutil` composes the already complete
  `pkg/util/logutil` owner. It must not introduce another sink, sampler, or
  logging policy; cloned sampled handles retain the one shared per-factory
  state required by Go. Date/Author: 2026-08-29, Codex.
- Decision: `pkg/statistics/handle/metrics` may own only its ten health-gauge
  children and two historical-dump counter children; construction,
  reinitialization, and registration of their parent collectors belong to the
  complete `pkg/metrics` package. Do not extend leaf-local collectors. Existing
  wires are seed evidence and remain unclaimed until that owner lands.
  Date/Author: 2026-08-30, Codex.
- Decision: the complete `pkg/domain/metrics` package shares collector identity
  with the statistics metrics owner while retaining its own seven-handle
  initializer. Its consumers use those handles directly; comments describing
  omitted metric boundaries are not behavioral parity.
  Date/Author: 2026-08-30, Codex.
- Decision: `pkg/statistics/handle/internal` must compare the real statistics
  graph. Caller-provided canonical byte encodings bypass the imported
  `HistogramEqual`, `CMSketch.Equal`, `TopN.Equal`, and existence-map behavior
  and are therefore not a valid native representation of the Go helper.
  Date/Author: 2026-08-29, Codex.
- Decision: the generic usage collector is an independent package owner, not
  a statistics-core module. Its normal/high-priority channels and worker
  lifecycle are consumed directly by `usage/indexusage`; package-private Go
  timeout/capacity constants are not exposed as a Rust public policy surface.
  Date/Author: 2026-08-29, Codex.
- Decision: `usage/indexusage` consumes the real `tidb_model::TableInfo`
  dependency and owns its own `GlobalIndexID`; a callback returning only index
  ID vectors and an alias to the parent usage package's key type both erase Go
  package behavior. Pending maps use shared ownership to reproduce Go map
  header sends without cloning map contents. Date/Author: 2026-08-29, Codex.
- Decision: the parent `usage` package cannot be represented by an ID tuple,
  a target-ID sorting helper, a transaction-mode enum, or copied SQL strings.
  Those disconnected leaves bypass the package's session-list, stats-handle,
  schema, transaction, persistence, and lifecycle behavior and are removed
  until the complete ordinary execution path can own them. Date/Author:
  2026-08-29, Codex.
- Decision: the cache-internal interface is coupled to actual statistics
  tables in pinned Go despite its own TODO proposing future genericization.
  Rust therefore must not implement that unlanded TODO by exposing a generic
  value contract or additional collection conveniences. Date/Author:
  2026-08-29, Codex.
- Decision: map-cache cost is table behavior, not a caller argument. Copying
  the cache duplicates its map entries and aggregate counter while preserving
  the same table pointers; a generic value/cost container is not equivalent.
  Date/Author: 2026-08-29, Codex.
- Decision: LFU parity cannot use the workspace's synchronous
  insertion-order cache; pinned Go observably depends on Ristretto's buffered
  TinyLFU admission, primary-store update behavior, callbacks, metrics, and
  wait/close lifecycle. A subset port would violate both behavior parity and
  the external-package atomicity rule. Date/Author: 2026-08-29, Codex.
- Decision: cache test support must construct the real statistics graph.
  Retaining only its five constructor arguments cannot exercise the memory,
  eviction, status, or append behavior for which the Go package exists.
  Date/Author: 2026-08-29, Codex.
- Decision: `pkg/statistics/handle/cache/metrics` cannot privately construct
  replacement metric families. Its eight handles are children of the shared,
  registered `pkg/metrics` vectors, so detached label constants or private
  collectors have different identity and lifecycle. The package remains
  explicitly unclaimed until that dependency is complete. Date/Author:
  2026-08-29, Codex.
- Decision: the root statistics cache is one package, not independently
  claimable batch, version, and SQL-format helpers. Its two source tests and
  six benchmarks depend on the actual cache implementation and backends.
  Generic or scalar extracts bypass atomic publication, metrics, SQL/session
  work, LFU behavior, and cache lifecycle, so they are removed until the whole
  package can be completed. Date/Author: 2026-08-29, Codex.
- Decision: `pkg/statistics/handle/history` is the complete history service,
  not its final maximum-version expression. Version selection has no
  independent API or source test and cannot substitute for filtering,
  transactions, storage conversion, and SQL writes. The scalar carrier is
  removed until the whole dependency-closed package can land. Date/Author:
  2026-08-29, Codex.
- Decision: `pkg/statistics/handle/initstats` is dependency-closed and must be
  implemented as its actual concurrent worker package. Rust's effective
  process parallelism is the native `GOMAXPROCS(0)` input; all remaining
  config, channel, atomic, logging, progress, task, and wait behavior is owned
  directly by the package. Date/Author: 2026-08-29, Codex.
- Decision: `pkg/statistics/handle/syncload` is implemented through the live
  cache, session, storage, planner, queue, retry, and worker lifecycle rather
  than an extracted CPU-threshold carrier. Its queues/workers belong to each
  statistics handle, only singleflight is global, and handle shutdown waits
  for workers. The atomic package remains unclaimed solely because its five
  collectors belong to the separate incomplete 60-artifact `pkg/metrics`
  owner; duplicating them locally would change collector identity.
  Date/Author: 2026-08-30, Codex.
- Decision: `pkg/statistics/handle/updatetest` is one integration-test package
  over the real stats handle. Empty functions cannot stand in for its session,
  transaction, SQL/storage, partition, auto-analyze, usage, lock, and leak
  checks. Its two locally pure tests do not make the package independently
  claimable; TopN merge is already tested in its production owner. All empty
  carriers are removed. Date/Author: 2026-08-29, Codex.
- Decision: pinned `types.StatsLockTable` is a plain shared data struct. Rust
  direct struct construction preserves the consumed contract; a public
  convenience constructor and source-absent tests add API and validation not
  owned by Go. The wider interface package cannot be completed inside
  `tidb-stats` without a dependency cycle and remains explicitly unclaimed.
  Date/Author: 2026-08-29, Codex.
- Decision: `pkg/statistics/handle/lockstats` is not its message formatter,
  set intersection, or first-row decoder. Its observable behavior is one
  transaction-wrapped session owner performing ordered lock/unlock SQL,
  timestamp updates, partition-to-global delta propagation, warnings, errors,
  and query context. All disconnected leaves and placeholder tests are removed
  until the complete dependency-closed package can land. Date/Author:
  2026-08-29, Codex.
- Decision: `pkg/statistics/handle/ddl/testutil.FindEventWithTimeout` cannot be
  replaced by searching a caller-provided slice. Channel receive ordering,
  blocking, timer selection, notifier events, and nil-on-timeout are the
  helper's behavior, alongside three sibling handle/transaction helpers. The
  generic leaf is removed until the complete support package can land.
  Date/Author: 2026-08-29, Codex.
- Decision: root `pkg/statistics/handle/ddl` cannot be represented by a
  pre-decoded event enum plus caller-supplied session, store, and cache traits.
  The Go package's behavior is its integration with the ordinary statistics
  handle, notifier, transactional storage, infoschema, historical metadata,
  locks, and end-to-end DDL ordering. The disconnected subscriber and helper
  leaves are removed until that dependency-closed owner can land atomically.
  Date/Author: 2026-08-29, Codex.
- Decision: `pkg/statistics/handle/globalstats` is not an independently
  callable TopN algorithm. Its observable behavior includes partition/table
  resolution, storage loading, missing-stat policy, FMS/CMS/histogram merging,
  worker cancellation and panic coordination, storage publication, and
  planner-visible results. The unconsumed helper island is removed until the
  entire dependency-closed package can land. Date/Author: 2026-08-29, Codex.
- Decision: root `pkg/statistics/handle` is the composition root, not its SQL
  string builders or pseudo-cache threshold. Its behavior is construction,
  notifier registration, pooled-session/cache subsystem lifecycle, and full
  transactional bootstrap. The extracted public leaves are removed until the
  complete ordinary handle and child packages can land together.
  Date/Author: 2026-08-29, Codex.
- Decision: `pkg/statistics/handle/storage` is one transaction-bound
  `StatsReadWriter` owner, not independent batch-count, version-fallback,
  SQL-string, or slow-save predicates. The extracted Rust leaves had no
  production consumers and omitted storage execution, typed conversion,
  cache publication, history, and worker lifecycle. They and the empty test
  carriers are removed until the complete dependency-closed package can land.
  Date/Author: 2026-08-29, Codex.
- Decision (2026-09-02): keep foreground `ReadColumnDistributionStats` in
  the storage owner because its Go contract is a single caller-owned snapshot
  with ordered metadata/TopN/bucket reads and atomic failure semantics. The
  auto-presplit DDL caller is intentionally audited and committed as its own
  complete `pkg/ddl` package rather than expanding this storage commit.
- Decision: `pkg/statistics/handle/autoanalyze/exec` is defined by current
  session execution and its process, metrics, warning, cache, and interruption
  effects. Private ratio/window parsing cannot stand alone as the package, and
  Go has no public parser API or parser-only tests. Rust's detached helpers and
  aliases are removed until the full execution owner can land.
  Date/Author: 2026-08-29, Codex.
- Decision: keep `tidb-util::keyspace` as the complete owner for Go's
  `pkg/keyspace`. The absent client-go codec and PD API-context types are
  represented by a narrow trait and value enum, while logger wrapping is
  represented by the canonical `keyspaceName` field consumed by the Rust
  logger. These are carrier adaptations of source behavior; adding a fake
  client or a second logger core would be Rust-only policy, so no such path is
  added. Date/Author: 2026-09-01, Codex.
- Decision: keep `tidb-config::deploymode` as the complete owner for Go's
  `pkg/config/deploymode`. Rust's `Mode` and Serde traits are the native
  representation of Go's integer/JSON/TOML interfaces, while the same atomic
  process-wide state and NextGen gate remain the only runtime policy. Do not
  add a second configuration parser or a synthetic setter restriction that
  the Go implementation does not enforce. Date/Author: 2026-09-01, Codex.
- Decision: keep `tidb-config::kerneltype` as one compile-time owner for both
  Go build-tagged variants. `cfg!(feature = "nextgen")` preserves the
  binary-wide Classic/NextGen contract, and the old-PD empty-type match stays
  in the shared path. A runtime kernel switch or duplicated platform module
  would be Rust-only behavior, so neither is added. Date/Author: 2026-09-01,
  Codex.
- Decision: keep `tidb-config::configtypes` as the single serialization owner
  for Go's `ByteSize` and `Duration` wrappers. The Rust implementations retain
  the source parser/formatter semantics and expose them through Serde, so a
  second config-specific parser or a cache-only conversion would be
  Rust-only behavior. Date/Author: 2026-09-01, Codex.
- Decision: extend `tidb-config::configtypes`' existing `RAMInBytes` parser
  for Go's hexadecimal floating literals and valid digit separators, rather
  than changing the config schema or adding a conversion layer. The helper
  keeps the ordinary decimal path, validates Go underscore placement, and
  decodes `0x…p…` directly so the source's numeric contract reaches the same
  binary-unit conversion. Date/Author: 2026-09-01, Codex.
- Decision: model Go's advertised status endpoint as a leased etcd claim in
  `tidb-domain`, with `tidb-pd-client` owning the atomic create-or-observe and
  compare-delete transport. Conflicts and operation failures stay
  warning-only; revision and lease guards prevent an old or losing server
  generation from removing a current claim. Serving node construction uses
  `report_status`, while assumed-keyspace and explicitly disabled syncers skip
  the claim. Date/Author: 2026-09-01, Codex.
- Decision: keep `tidb-util::channel` as a synchronous receiver-drain helper,
  matching Go's blocking `for range` semantics. Do not add an async wrapper,
  timeout, or nil-channel emulation because those would be Rust-only channel
  policy absent from `pkg/util/channel`. Date/Author: 2026-09-01, Codex.
- Decision: keep `pkg/param` owned by the existing protocol carrier rather
  than adding a second crate for a two-field metadata definition. The Rust
  `BinaryParamError::UnknownFieldType` retains Go's dedicated errno and the
  ordinary prepared-statement path; no source behavior or Rust-only policy
  requires an edit. Date/Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/cpu` unclaimed until its process-time sampler, EMA
  worker, shared gauge, resource-manager CPU scheduler, and server lifecycle
  can land together. `tidb-util::cgroup` is dependency support rather than an
  observer substitute, and statement-level `ppcpuusage` is a different
  package. A CPU-count helper or detached sampling thread would be a partial
  port with no ordinary consumer. Date/Author: 2026-09-01, Codex.
- Decision: keep `tidb-hack` as the owner for `pkg/util/hack` and preserve the
  Go 1.25/1.26 ABI files as one source contract. Rust's `MutableBytes`, safe
  table wrapper, and hashbrown-backed `real_bytes` are ownership seams, not
  Rust-only map policy. Do not expose Go-runtime raw pointers or add a second
  allocator/memory-arbitrator implementation; exact private-ABI byte counts
  remain a documented portability boundary. Date/Author: 2026-09-01, Codex.
- Decision: keep `pkg/util/gctuner` unclaimed until Go's repeating finalizer,
  GOGC and SetMemoryLimit controls, global-arbitration callback, server/BR
  lifecycle, and failpoint-controlled races can be represented in one
  dependency-closed owner. `tidb-util::memory`, `memoryusagealarm`, and
  `servermemorylimit` are supporting packages with distinct contracts; a
  detached Rust GC thread or synthetic runtime policy would be Rust-only
  behavior. Date/Author: 2026-09-01, Codex.
- Decision: retain `pkg/util/gctuner` as an explicit Rust integration boundary
  after restoring the current Go arbitration cap and deleting the obsolete Go
  callback/reset path. These changes do not create a Rust owner; the complete
  runtime and lifecycle contract remains unclaimed. Date/Author: 2026-09-02,
  Codex.

## Surprises & Discoveries

- The compact parser's first generalization exposed a source distinction that
  must remain visible: `TIMESTAMP('20240315.5')` is parsed by Go's string
  signature as a five-hour suffix, while `TIMESTAMP('20170118123050.1234567')`
  uses a fractional-second suffix. Routing the shared parser's `is_float` bit
  from the datum kind keeps both interpretations active in the value tier.

- The ignored `MAKETIME` carrier's floating-second rows already passed after
  the prior MaxFsp formatting work; its remaining failure was specifically
  the unsigned type signal. `int_arg` correctly exposed the wrapped signed
  value, but the value-only dispatcher had discarded the `UnsignedFlag`, so
  Go's pre-sign clamp never ran.

- The ignored `TIMESTAMP` numeric carrier separated two issues: Go's
  `isFloat` flag is selected from static source type even after `EvalString`,
  and DECIMAL zero-date fractions must survive parsing. Reusing the shared
  float-string parser resolves both without changing the string signature;
  the string branch now also covers the date-only hour suffix.

- The source `TIMESTAMPADD(DAY, 28768, 0)` row exposed a second boundary: the
  Rust signed day-number arithmetic can turn a zero date into year 78, whereas
  Go rejects the zero `CoreTime` while converting the evaluated DATETIME to a
  `time.Time`. The pre-add range guard preserves that rejection without
  changing valid packed-date arithmetic.

- `UNIX_TIMESTAMP` exposed the same source-kind distinction as `TIMESTAMP`,
  but its result contract has an additional split: Go returns NULL for the
  all-zero date while a partially zero date such as `2017-00-02` returns the
  numeric zero sentinel. Reusing the shared parser was sufficient for packed
  numeric forms and DECIMAL rounding; the explicit all-zero check is required
  before constructing a Chrono date.

- The period arithmetic already rejected invalid values, but used a generic
  Rust `Unsupported` marker. The source error table makes the function name
  part of the client-visible `ErrWrongArguments` text, so the parity fix had
  to change the error variant/message rather than only the rejection result.

- The prior ADDTIME/SUBTIME gap was two independent source rules: integer
  arguments are cast to ETString before `ParseTimeWithString` (which accepts
  packed date-time digits), and DATE+STRING parses fractions with the legacy
  non-zero sentinel (`getFsp4TimeAddSub`) rather than the written digit count.
  Both were hidden by the ignored carriers; no new Go or generated artifact was
  needed.

- The ignored issue-56861 carrier mixed two different Go evaluator bodies:
  the row body calls `Time.Add` with the parsed string FSP, while only the
  vectorized DATETIME+TIME body forces `Fsp=-1`. Rust had applied the latter
  universally, silently dropping `.341300` during constant folding; malformed
  typed string operands already returned NULL once their static kind was
  honored.

- `WEIGHT_STRING`'s Rust implementation already appended the exact BINARY(n)
  warning; the ignored carrier used only the no-warning `chunk_e` helper, so
  it falsely classified a context-observation gap as missing behavior. In
  contrast, `FORMAT` precision really bypassed the statement handler, and its
  malformed precision rows emitted no events until the shared cast helper was
  routed in.

- A fixed nine-decimal rendering of a real Unix timestamp is not equivalent to
  Go's shortest `%g` conversion: `1451606400.0363455` becomes
  `.036345482` in the fixed spelling and rounds down, while Go retains
  `.0363455` and rounds up to `.036346`. The datatype crate already carried
  the exact shortest-float formatter, so the expression fix only had to route
  both real datum kinds through it.

- Go's `FROM_BASE64` packet check intentionally measures the 94-byte source
  string before removing its tabs, newline, carriage return, and spaces, so
  its estimate is exactly 70 bytes. Rust previously decoded regardless of
  packet size because its helper had no context. The ignored DAYOFMONTH
  IgnoreZeroInDate and `%x` zero-year sentinel rows were already fixed by
  existing runtime behavior, so they were activated and removed from the
  stale-gap ledger in this package receipt.

- Go's signed subtraction guard intentionally leaves positive-minus-
  `MinInt64` unchecked because `-b` wraps before comparison. The final Go
  integer subtraction then wraps too, so replacing it with Rust's
  `checked_sub` produced a Rust-only error; the parity fix must use explicit
  wrapping arithmetic while retaining Go's special zero-minus-minimum error.

- Go's week-year formatter deliberately converts negative `YearWeek` results
  to an unsigned `MaxUint32` sentinel. Rust's signed formatting exposed
  `-001` at the year-zero boundary, even though ordinary week-year vectors
  already matched.

- The expression crate carried a second `STR_TO_DATE` parser that had not
  inherited the datatype owner's Unicode punctuation correction. A shared
  classifier closes the duplicate boundary without changing token grammar.

- Go's cast-type formatter preserves a degenerate explicit empty charset as
  `CHAR CHARSET ` rather than omitting the clause. Rust had added an
  extra non-empty check that looked defensive but was not source-compatible;
  the focused regression made this byte-level difference visible.

- Go's default field-type width is measured from its display spelling, so
  positive infinity is four bytes (`+Inf`) even though Rust's native spelling
  is three (`inf`). The parser-driver path already had the source spelling
  helpers; the runtime path had independently reverted to native formatting.

- Go's decimal division error is not a failed value: `DecimalDiv` retains the
  quotient while `fixWordCntError` drops hidden fractional words and returns
  `ErrTruncated`. The Rust digit-string implementation had no word-boundary
  check, so its visible quotient matched while its hidden precision and
  warning stream diverged.

- Rust's standard `f64::Display` deliberately never emits scientific notation;
  `Decimal::from_f64` was therefore expanding an already positional string
  before parsing. Ryu supplies the same shortest digits, while a small wrapper
  restores Go's `%g` exponent threshold and exposes the fixed-buffer difference
  at `1e-73`.

- `Decimal::shift_mysql_with_word_limit` already rounded to the right scale,
  but it tested only `rounded.is_zero()`. For a value such as `9e-82`, Go's
  separate pre-round `digitBegin`/`digitEnd` geometry makes the carry itself
  non-surviving; the Rust equivalent is an all-zero retained prefix in the
  unrounded digit string.

- Go 1.25's `unicode.IsPunct` table is Unicode 15.0 while the already-locked
  Rust `unicode-general-category` dependency is generated from Unicode 16.0.
  Comparing the generated ranges found no Go-only punctuation and 13
  Rust-only code points; the Rust `%.'` helper excludes exactly those points
  to keep the fetched source behavior code-point-for-code-point at the
  classification boundary.

- The earlier pinned kvcache receipt said Go had no `Peek`, but current
  `origin/master` adds it inside the existing `TestGet` case. Because the
  method's observable contract is list order—not merely value equality—the
  Rust owner needed an immutable lookup rather than a `get`/relink shim.
- `pkg/util/tableutil` is only 46 lines, but its `TempTable` interface is the
  shared type that lets table construction, session overlays, and transaction
  isolation exchange mutable table state. Rust already implements each
  consumer's behavior separately; the apparent small leaf is therefore a
  cross-package integration seam, not a safe standalone port.
- `pkg/util/profile`'s current-master change is entirely integration-facing:
  the source collector is unchanged, while `TestProfiles` now starts the
  CPU profiler and checks six structured log events. Rust's SEM layer already
  knows the table names, which can look like partial parity until the missing
  profile executor and pprof path are traced.
- Go `pkg/util/etcd.DeleteKeyFromEtcd` is not a thin delete wrapper: it
  creates a fresh timeout context for each attempt and retries five times by
  default, while Rust's existing `EtcdClient::delete` only performed one
  worker call. The server-info adapter was therefore silently less resilient
  even though its trait shape looked complete; the retry loop now lives in the
  transport owner and is exercised independently of a live etcd.
- Go 1.25's `strconv.ParseFloat`, which backs `docker/go-units.RAMInBytes`,
  accepts hexadecimal floating literals and valid digit separators. The
  existing Rust configtypes owner covered the older decimal-only cases, so
  `0x1p10KiB` and `1_000KiB` exposed a real rolling-master gap; a focused
  regression now pins both accepted forms and malformed separator rejection.
- Rust retained raw ANALYZE samples but skipped prefix-index statistics in the
  cluster path; Go cuts raw index values before histogram construction.
- Rust had functional-dependency machinery but the needed equivalence closure
  was private, which led to a false planner-property gap outside the owner.
- Several testport batches consisted entirely of ignored empty functions or
  comments. They increased apparent coverage without testing Go behavior.
- The prior pinned kvcache receipt had correctly removed an obsolete
  `Peek`-absence claim, but current Go master adds `Peek` inside `TestGet`.
  The rolling audit therefore restored that API as a real no-promotion
  behavior while retaining the platform-neutral Rust memory probe.
- Go's nil receiver/interface and concrete builtin-signature identity tests
  describe implementation shapes Rust cannot invoke after adopting non-null
  references and name-keyed dispatch. Empty Rust functions for those shapes
  test nothing and must not count as parity.
- The current workspace still contains many `go-parity-gap` markers. Their
  presence is an audit queue, not evidence that every carrier should survive.
- Go master retains `TiDBMergePartitionStatsConcurrency` only as a deprecated
  compatibility name and hard-codes its registry value; its old `Def*` default
  disappeared. The Rust constants extraction had preserved that default, so
  this audit removed it as Rust-only behavior while preserving the name.
- The Rust vardef extraction predated the current master by several session
  changes. A complete source diff found the Analyze batching, plan-replayer,
  FULL OUTER JOIN, transaction-file, embedding, and connection-event constants
  together; treating them as one constants-layer batch kept the package unit
  coherent without pulling in the still-unported SessionVars registry.
- Go master removed memory's Unicode-code-point `HashStr` in favor of the
  length-prefixed byte `DigestIDBuilder` and made digest ID zero a no-op. The
  Rust memory owner had retained the old public helper, so the bounded audit
  removed that Rust-only API and matched the new sentinel behavior before any
  shard indexing.
- Go's stmt-summary map initializes `isInternal` from the first statement and
  only then applies logical AND; the Rust map had applied AND to the default
  `false`, making every new summary look external. The same source delta now
  substitutes `PlanDiscardedEncoded` when lazy plan encoding fails instead of
  returning a nil stats pointer. Both cases required ordinary v1 map fixes and
  focused regressions; the v2 record constructor already matched Go.
- Go's test-only histogram equality deliberately compares `ToString(0)` and
  therefore ignores metadata absent from that projection. Rust derived
  `PartialEq` is stricter and cannot substitute for this helper's behavior.
- The pinned generic collector's session sender is not connected to the
  global close channel when spawned. Consequently already-created senders can
  still enqueue into available channel capacity after close; Rust's previous
  early rejection was observably stricter than Go.
- Go's zero `time.Time` is year 1, not Unix epoch. The previous epoch sentinel
  happened to sort before current samples but leaked a different public value
  and required a Rust-specific information-schema check.
- The full planner test target currently encounters unrelated pre-existing
  compile errors in CTE/TopN and memory-trace test sources; scoped planner tests
  for the changed MPP property behavior pass.
- The Rust traceevent module says client-go tracing is unavailable, but the
  vendored Rust TiKV client already exposes category, event, and control
  extractor hooks and uses them in production request paths. No production
  caller registers the Rust module's fake registry, so its adapter tests prove
  only an invented boundary and not observable behavior.
- Pinned Go registers its client adapter from the package initializer and
  carries the statement `*Trace` through `context.Context`. Rust has no package
  initializer, so the native equivalent is one registration call in the
  ordinary server startup plus a typed `Arc<Trace>` value in the vendored
  client's immutable trace context.
- The SLI source regression exposed that Rust's cluster read-key collector
  counted rows served from the transaction's own mutation buffer. Go
  `ScanDetail.ProcessedKeys` counts storage processing instead: the exact
  `REPLACE ... SELECT` transaction case is `readKeys: 0`. Recording successful
  snapshot results fixed the production seam without changing the source
  test's expected output.
- Go `origin/master` moved the stable plan-ID frontier from 63 to 64 by
  appending `Analyze`. The Rust codec's unknown-ID regression made this
  drift directly observable (`Analyze` encoded as zero and ID 64 decoded as
  `UnknownPlanID64`), so the fix had to extend the table at the end rather
  than renumber any existing operator.
- Go master added three dual-password prototypes to
  `pkg/util/dbterror/exeerrors` after the Rust package had already generated
  the same complete 82-entry catalog. Source chronology alone therefore
  suggested a gap, but declaration/static/fixture comparison showed none.
- The planner-error owner has 98 entries while Go's source test names only 59;
  the remaining 39 are still behaviorally required because Go constructs and
  registers every package-level variable during initialization. Rust's
  all-entry forcing test makes that otherwise-lazy initialization observable.
- `pkg/util/execdetails` is a cross-cutting package rather than a formatting
  leaf: Go master added behavior in `runtime_stats.go` while its Rust owners
  still narrow context, client-go details, resource-manager protobufs, and
  Prometheus process state. A partial field port would be a second runtime
  path, so this loop records the full inventory and defers edits until the
  dependency-closed package can land atomically.
- Go master added a three-artifact `sqlkiller` test target after the pinned
  source had no tests. Its failpoint callbacks deliberately call Reset while
  SendKillSignal is between CAS and logging and start a second signal while
  Reset holds the event lock; the Rust regression uses the same interleaves,
  exposing the old split-lock and open-generation behavior deterministically.
- Go master added VectorFloat32 spill helpers without adding a package test or
  fixture. The source's zero-value special case is equivalent to Rust's empty
  vector serialization, so one focused regression covers both the four-byte
  zero image and a non-empty vector while preserving the existing prefix.
- Go master changed `CompileLike2Regexp`'s signature without adding a new
  package test row. Because Rust had hard-coded the old default escape, the
  source delta was only observable with a non-backslash escape; the added
  regression makes that contract executable without adding a second consumer
  path.
- Go master exported `ColumnFilterRules` for dumpling's column-filter config
  while leaving the source test suite unchanged. Rust's private implementation
  therefore compiled all historical rows yet still lacked the public parser
  shape; the focused regression now exercises the concrete API directly.
- The current chunk workspace does not reliably provide the Go spill failpoint
  or temporary spill directories: two existing Go panic tests and 35/279 Rust
  lib (40/325 nextest) cases fail for those pre-existing paths/timing reasons.
  The new `UsedMemoryUsage` regression is isolated and passes, so the failures
  remain validation boundaries rather than prompts for unrelated edits.
- Go master removed `Encoder.HashCode` while retaining package `HashCode`; the
  old Rust source carrier tested the removed method rather than a live Go
  obligation. Removing it exposed one executor append site and one expression
  source regression that had to be routed through the ordinary free functions.
- The current Go-master row API cleanup exposed a mode-specific fast-path
  issue rather than a missing tablecodec wrapper: old-collation common handles
  were skipped by the typed V2 decoder's default restored-data policy. A narrow
  fallback to the existing map path made the source regression pass without
  changing new-collation behavior.
- Go master places `IAExecCountStr` before the six IA segment aggregate columns
  and increments it only for executions whose IA segment count is positive;
  counting bytes or wait time would over-report ordinary executions. The Rust
  source-derived tests now use one IA and one ordinary execution to pin that
  distinction in both v1 and v2.
- The Go stmt-summary race fix snapshots evicted-count rows under one mutex and
  uses `Peek` while locking records during internal cleanup. Rust's v1 evicted
  rollup is always behind its owning mutex, while v2 keeps window and record
  locks separate and acquires them in the documented order; the audit found no
  missing Rust behavior to patch for those two paths.
- The stmtsummary owner is intentionally not declared package-complete: Go's
  v2 reader/logger/table-test files and the executor `SHOW SLOW`, infoschema,
  and planner consumers are outside the current Rust crate. This batch keeps
  the implemented record/reader/column behavior aligned without inventing a
  second integration path.
- The parser utility's Go-master visitor migration is API-only: `ast.Walk`
  retains the same short-circuit and table-name predicate. The Rust visitor
  already has no replacing return value, so the parity evidence is a focused
  source-derived regression and inventory receipt rather than a duplicate
  adapter API.
- TopSQL's Go-master reporter-loss and panic fixes are above the current Rust
  ownership boundary: without a reporter worker, DataSink registry, or gRPC
  agent client, implementing them in `tidb-util` would be a cache-only or
  fabricated transport path. The dependency-closed fixes are the
  statement-stats CAS reservation and normalized metadata map admission lock;
  both have concurrent source-derived regressions.
- `pkg/util/generatedexpr` has the same in-place visitor migration with no
  semantic delta. Its Rust leaf is already source-shaped, but `tidb-model`
  remains a broader seed, so the audit records the boundary instead of
  claiming package-complete metadata parity.
- `pkg/util/hint` confirms that a source-level visitor migration does not make
  the package leaf-safe: query-block/view state and warning order cross the
  parser, binding, planner, SEM, and executor seams. The audit therefore keeps
  the existing native consumers and records the missing dependency closure.
- `pkg/expression/aggregation` confirms that a new aggregate family cannot be
  made compatible by adding descriptor names alone: parser, type inference,
  pair-state runtime, planner routing, protobuf, and KV pushdown must move as a
  single dependency-closed unit.
- `pkg/parser/mysql` confirms that a new privilege cannot be made compatible by
  adding one enum value: generated lexical inputs, parser restoration, scope
  masks, persisted privilege columns, and executor display/check paths must
  preserve one bit and column order. The coordinated `OPERATE VIEW` batch now
  does so, while Go's versioned schema-upgrade and materialized-view scheduler
  remain outside the Rust dependency closure.
- `pkg/kv` confirms that the support matrix is a protocol compatibility
  surface: the two current aggregate identities must be moved together in the
  native checker and in the supported/unsupported disposition vectors, with
  the Go test's `ReqTypeSelect` assertions executable rather than ignored.
- `pkg/types` confirms that even a one-literal validator delta requires both
  the public owner export and the ordered validator collection; the separate
  parser-driver visitor methods cannot be safely reconstructed without the
  missing dependency closure.
- `pkg/meta/autoid` confirms that the service client's cached base is a
  concurrency contract, not merely a diagnostic value: allocation responses
  can complete out of order, unsigned values compare by their bit pattern, and
  a non-forced rebase must not move the observed base backwards. The Rust
  client now uses the same monotonic CAS and the Go count-and-duration retry
  limit; the etcd service/server remains a separate owner boundary.
- `pkg/lightning/mydump` removes regexp-based CSV/chunk unescaping in favor of
  Go's byte scanner, adopts `io.ReadSeekCloser`, and adds the deferred Parquet
  reader opener. The custom `*` escape regression and unescape benchmark pass
  on current and exact Go master; the branch's older parser AST keeps
  `view_import` on `Accept` until the in-place visitor migration lands as one
  dependency-closed parser change.
- `pkg/lightning/tikv` is already byte-identical to Go master across its Pebble
  SST writer, MVCC/range property collectors, TiKV RPC/version helpers, tests,
  BUILD metadata, and both binary SST fixtures. Related Rust TiKV/BR code does
  not close this Lightning-specific dependency graph, so the package remains
  an explicit ownership boundary without a speculative facade.
- `pkg/parser/ast` adds Go's generated in-place visitor API, materialized-view
  and full-join nodes, and per-node text caching. The Rust `tidb-ast` crate is
  only a partial owner (it lacks those nodes and the replacement/in-place API
  pair), so this parser, grammar, planner, and executor surface remains one
  dependency-closed boundary rather than a partial port.
- `pkg/parser/format` is byte-identical to Go master across its formatter,
  restore flags/context, CTE state, tests, and BUILD metadata. Rust's
  `tidb-ast` already owns the corresponding restore surface, so no code change
  or speculative adapter was needed.
- `pkg/parser/opcode` removed the stale `Binary` operator from Go and Rust;
  focused table-count regressions failed before the fix and pass afterward.
  Remaining `BINARY` spellings belong to distinct cast, charset, and
  weight-string concepts. The required Bazel preparation is blocked only by
  the unavailable local `bazel` executable.
- `pkg/parser/duration` is byte-identical to Go master across its parser,
  test, and BUILD inputs. Rust's parser owns the same fractional day/hour/
  minute contract and its TTL/CALIBRATE consumers, with additional malformed
  input and diagnostic coverage; no code delta was needed.
- `pkg/parser/test_driver` has six Go-master artifacts (1,274 lines) and a
  154-line AST-dependent `AcceptInPlace`/source-regression addition. The
  current AST branch does not expose `InPlaceVisitor`/`Walk`, and no Rust
  test-driver owner closes that dependency graph, so the package remains an
  explicit boundary pending the complete parser migration.
- `pkg/parser/types` is byte-identical to Go master across six artifacts and
  1,441 lines. `tidb-datatype` owns the FieldType/EvalType, type-name,
  formatting, JSON, sizing, and error-prototype behavior with source-derived
  tests; no Rust-only behavior or code delta was found.
- `pkg/parser/tidb` is byte-identical to Go master across its two artifacts
  and 75 lines of feature identifiers plus `CanParseFeature`. No Rust crate
  currently consumes this public allowlist, so no speculative facade or
  Rust-only behavior removal was justified; the package remains an explicit
  ownership boundary.
- `pkg/parser/auth` is byte-identical to Go master across eight artifacts and
  920 lines. `tidb-parser::auth` owns identity restoration, native SHA-1,
  caching-SHA2 SHA-crypt, and SM3 behavior with 20 source-derived tests;
  malformed-input and byte-domain regressions pass, with no Rust-only behavior
  requiring removal.
- `pkg/parser/charset` remains byte-identical to current Go master across 14
  artifacts and 3,319 lines, including the generated GB18030 input. Its
  existing Rust charset owner passes the focused source-derived and encoding
  suites; the receipt is refreshed to the current authority and Ready gates.
- `pkg/parser/util` is byte-identical to Go master across four artifacts and
  152 lines. `tidb-lexer` owns the byte-for-byte escape helper and `tidb-hash`
  owns the eleven-method hasher interface; their source-derived suites pass
  without a duplicate adapter or Rust-only behavior.
- The root `pkg/parser` inventory covers all 33 Go-master artifacts (64,892
  lines), 345 declarations, and 150 test/benchmark/fuzz entries, including
  grammar inputs, generated parser/keyword outputs, support scripts, and build
  metadata. Go master is a large generated-parser consolidation; Rust's
  `tidb-parser` is a partial owner without a dependency-closed AST/visitor
  equivalent, so the package remains an explicit atomic boundary with no
  speculative facade or Rust-only behavior removal.

- `pkg/parser/generate_keyword` adds four Go-master artifacts (211 lines).
  Rust's native generator now matches Go's line splitting and catalog source:
  the four missing unreserved words (`ALERT`, `FAST`, `IMMEDIATE`, and
  `MATERIALIZED`) are restored, the static catalog is 689 entries, and the
  pre-fix count/CRLF regressions pass after correction. The path/`--check` and
  stdout modes remain tooling-only Rust extensions; no SQL runtime behavior is
  duplicated or removed. The complete receipt and ExecPlan record this batch.
- `pkg/parser/goyacc` contains three Go-master generator artifacts (1,443
  lines, 46 declarations, no tests or fixtures). Its formatter, modernc yacc
  table generator, reports, and CLI flags form one build-time unit. The Rust
  hparser uses a handwritten parser and has no dependency-closed goyacc owner,
  so this remains an explicit tooling boundary; the exact Go compile is pending
  modernc module downloads after a proxy EOF.

- `pkg/server/internal/handshake` and `pkg/server/internal/parse` are now
  complete six-artifact inventories (553 lines combined). Rust's handshake
  response had source-absent `raw_attrs` and `attr_warnings` fields plus a
  lossy UTF-8 attribute view; the response is now exactly Go's eight fields
  with byte-preserving attributes. The parser emits Go-equivalent diagnostics
  through `tidb-log`, and focused source regressions cover exact field shape,
  malformed attributes, truncation, duplicate keys, NULL frames, and metric
  boundaries. Go tests, both focused Rust filters, and lint pass. See the two
  package receipts and `server-handshake-parse-audit-execplan.md`.
- `pkg/server/metrics` is byte-identical to Go master across its two artifacts
  and 135 lines. It is a Go server-facing Prometheus wiring layer with no
  dependency-closed Rust connection-loop owner; the audit records this explicit
  boundary rather than inventing a duplicate global registry facade.
- `pkg/server/internal/resultset` is byte-identical to Go master across three
  artifacts and 506 lines. Its session-bound RecordSet/chunk lifecycle,
  prepared-column cache, lazy cursor iterator, and RUv2 reporting have no
  dependency-closed Rust owner; the audit records the explicit boundary rather
  than inventing a second cursor state machine.
- `pkg/server/internal/util` is byte-identical to Go master across four
  artifacts and 467 lines. `tidb-protocol` owns and source-tests the
  length-encoded/null-terminated helpers and charset decoder; buffered TCP,
  CORS, and test-config adapters remain above the current Rust server owner, so
  no speculative transport facade was added.
- `pkg/server/internal/dump` is byte-identical to Go master across three
  artifacts and 303 lines. `tidb-protocol` owns the complete length-encoded,
  BinaryTime, BinaryDateTime, and row framing behavior with source-derived
  vectors; no duplicate dump facade or Rust-only behavior removal is needed.
- `pkg/server/internal/testutil` is byte-identical to Go master across two
  artifacts and 79 lines. The Rust protocol test owner preserves the complete
  no-op byte-buffer connection and TCP-port helper without introducing a
  production mock socket abstraction.
- `pkg/server/internal/testserverclient` is byte-identical to Go master across
  two artifacts and 3,159 lines, including all 55 integration helpers and SQL,
  TLS, load-data, DDL, metrics, and failpoint scenarios. It remains an explicit
  Go testkit/server lifecycle boundary because no dependency-closed Rust
  integration harness exists.
- `pkg/server/handler/ttlhandler` is byte-identical to Go master across two
  artifacts and 92 lines. Its POST-only HTTP endpoint depends on the Go router,
  session domain, and TTL command client; no dependency-closed Rust HTTP owner
  exists, so no speculative facade was added.
- `pkg/server/handler/extractorhandler` is byte-identical to Go master across
  four artifacts and 439 lines. Its HTTP plan-extraction endpoint depends on
  the Go domain, extstore, statement-summary persistence, and failpoint server
  lifecycle; no dependency-closed Rust owner exists, so it remains an explicit
  boundary after the focused failpoint-wrapped integration tests pass.
- `pkg/server/handler/optimizor` is byte-identical to Go master across seven
  artifacts and 2,219 lines. Its optimizer-trace, plan-replayer, and statistics
  endpoints are Go HTTP composition over Domain, InfoSyncer, external storage,
  and statistics-handle lifecycles. Rust owns several leaf decisions (trace
  naming, plan-replayer bookkeeping, historical stats, and priority-queue
  snapshots), but no dependency-closed handler owner; the complete package
  remains an explicit boundary after the failpoint-wrapped Go suite and
  `tidb-domain` owner suite pass. See
  `rust/testport/receipts/server_handler_optimizor.md` and
  `rust/docs/operations/server-handler-optimizor-audit-execplan.md`.
- `pkg/server/handler/tikvhandler` was missing Go master's TiFlash replica
  summary and DXF task-cleanup batch-size APIs. The complete five-file
  production/build inventory is restored (plus focused parser and handler
  regressions), the columnar-storage sysvar required by the summary is
  restored, and the HTTP routes and API documentation now match Go master.
  The package remains a Go HTTP/domain boundary with no dependency-closed Rust
  HTTP owner. See `rust/testport/receipts/server_handler_tikvhandler.md` and
  `rust/docs/operations/server-handler-tikvhandler-audit-execplan.md`.
- The parent `pkg/server/handler` inventory covers all six Go-master artifacts
  and 773 lines, including the auto-ID owner test, TiKV/MVCC helpers, upgrade
  state handler, and response utilities. It is byte-identical to Go master and
  remains an explicit Go HTTP/domain boundary because Rust has no complete
  dependency-closed owner. See `rust/testport/receipts/server_handler.md` and
  `rust/docs/operations/server-handler-audit-execplan.md`.
- `pkg/server/handler/tests` now restores Go-master coverage for the DXF
  cleanup-size endpoint, next-gen user-keyspace route isolation, and live
  TiFlash summary lifecycle. All five test/build artifacts were inventoried;
  the remaining profiling-log and old-kvproto encoding differences are
  explicit branch boundaries. Focused failpoint-wrapped integration tests pass.
  See `rust/testport/receipts/server_handler_tests.md` and
  `rust/docs/operations/server-handler-tests-audit-execplan.md`.
- Go master's scalar-subquery statement-boundary repair is restored across
  `pkg/executor` and `pkg/session`: normal planning and transaction replay now
  clear `MapScalarSubQ` before selecting each statement's plan. The focused
  executor, replay, and prefetched-PointGet regressions pass; this remains
  supporting seed evidence rather than a complete claim for either large
  package. See
  `rust/testport/receipts/scalar_subquery_statement_boundary.md` and
  `rust/docs/operations/scalar-subquery-statement-boundary-execplan.md`.
- `pkg/store/mockstore/unistore/tikv` is byte-identical to Go master across all
  16 artifacts and 9,286 lines after restoring the deterministic pessimistic
  deadlock failpoint. The executor now returns the original lock error instead
  of reopening a consumed `LOAD DATA LOCAL INFILE` stream, and the focused
  protocol-synchronization regression passes. See
  `rust/testport/receipts/store_mockstore_unistore_tikv.md` and
  `rust/docs/operations/store-mockstore-unistore-tikv-audit-execplan.md`.
  The Rust owner now also implements every helper in the audited `util.go`
  (range boundary, sorted de-duplication, FarmHash pipelines, and
  nil-preserving copies) with live Go-table regressions; the existing exact
  FarmHash implementation is shared from `tidb-txnkv`. The focused utility
  tests pass, while the ordinary unistore test target remains blocked by the
  pre-existing parent `InProcessClient` synchronous-dispatcher bound, as
  detailed in the receipt.

- `pkg/store/gcworker` is restored to Go master in one package batch: after a
  successful keyspace-level GC round, the worker recycles completed GCV2 work
  and registers the next task for master/TTL roles, with best-effort error
  logging. The complete four-artifact inventory, focused role/keyspace
  regression, failpoint-wrapped package suite, and Ready evidence are in
  `rust/testport/receipts/store_gcworker.md`; Rust has no dependency-closed GC
  worker owner, so no Rust-only behavior was removed.
- `pkg/planner/util` is restored to current Go master in one package batch:
  `SplitCorColAccessCondFromFilters` now marks full-length equality-matched
  index columns in `AccessPath.ConstCols`, including execution-time
  correlated equalities, so later index columns can provide scan order. The
  complete 16-artifact root inventory, pre-fix failing/post-fix passing
  regression, package suite, and Ready evidence are in
  `rust/testport/receipts/planner_util.md`; nested utility directories remain
  separate package claims, and Rust's partial order matcher remains an
  explicit correlated-planning boundary.
- `pkg/planner/cardinality` restores the Go-master zero-repeat histogram
  fallback in one package batch: column and index equality estimates now
  treat `Repeat == 0` as missing point frequency and use the uniform estimate
  instead of returning an exact zero. The complete 18-artifact inventory,
  pre-fix failing/post-fix passing regression, failpoint-wrapped package suite,
  and Ready evidence are in
  `rust/testport/receipts/planner_cardinality.md`; larger appended-handle,
  recursive-estimation, JSON-selectivity, and golden-fixture deltas remain
  explicit follow-up boundaries.
- `pkg/store/copr` now passes each store-batched child response's own lock to
  the existing lock resolver, restoring Go-master child-lock fallback without
  changing batching or retry policy. The complete 20-artifact root inventory,
  pre-fix failing/post-fix passing regression, failpoint-wrapped package suite,
  and Ready evidence are in `rust/testport/receipts/store_copr.md`; nested
  `copr_test` and `metrics` remain separate package boundaries and live TiKV
  transport parity remains recorded in `distsql_audit.md`.
- `pkg/objstore/ossstore` now follows Go master for OSS presigned downloads
  and `SendCredentials`: the data client keeps its configured endpoint while a
  logging-disabled public-endpoint client signs URLs, and refreshed credentials
  are forwarded only when requested. The complete ten-artifact inventory,
  focused presign/credential regressions, package suite, and Ready evidence are
  in `rust/testport/receipts/objstore_ossstore.md`; Rust has no dependency-closed
  OSS backend owner, so no speculative Rust behavior was added.
- `pkg/expression` unary-minus hybrid operands now match Go master: ENUM, SET,
  and binary-literal values use the REAL result domain (`-1.0`) instead of the
  generic DECIMAL fallback. The current-master expression tree was
  re-inventoried before editing (208 artifacts, including tests, generated
  inputs/outputs, support files, and BUILD metadata); the executable regression
  and fail-before evidence are recorded in `rust/testport/receipts/b073.md`.
- `pkg/expression` EXTRACT composite units now retain fractional seconds like
  Go master. The datetime and duration formulas append the parsed six-digit
  microsecond component for `SECOND_MICROSECOND`, `MINUTE_MICROSECOND`,
  `HOUR_MICROSECOND`, and `DAY_MICROSECOND`; the complete inventory, focused
  regression, and fail-before evidence are recorded in
  `rust/testport/receipts/b073.md`.
- `pkg/expression` comparison signature generation now folds non-JSON cast
  constants with the real `Columns` context, matching Go's
  `BuildCastFunction` boundary. Duration, temporal, invalid-cast warning, and
  inexact numeric regressions plus the complete inventory are recorded in
  `rust/testport/receipts/expression_collation_audit.md`.
- `pkg/expression` mixed-charset constant folding now follows Go's
  derive-before-`HandleBinaryLiteral` order. A GBK-tagged `CONCAT` argument is
  encoded before a raw BINARY suffix is appended, producing
  `D6D0CEC4D2BB`; direct `NewFunction` and AST-rewriter construction share the
  same wrapper, with inventory and fail-before evidence in
  `rust/testport/receipts/b073.md`.
- The `pkg/planner/core/rule_eliminate_unionall_dual_item.go` source-shaped
  adapter now preserves Go's narrow `planChanged` result (dropping a branch
  from a still-nonempty union is not itself a change) and directly indexes a
  projection's first child, removing the Rust-only safe fallback. The planner
  crate and companion difftest inventories, pre-fix failures, focused
  regressions, and the aggregate-target API blocker are recorded in
  `rust/testport/receipts/planner_rule_child_access.md`.
- The `pkg/expression` cast builder now strips `NOT_NULL` from the copied cast
  target when its source is nullable, matching Go's
  `BuildCastFunctionWithCheck` while preserving caller-owned target metadata.
  The focused `tidb-expr` regression and the now-active planner source test
  `cast_ret_type_clones_share_nothing_across_builds`, inventory, pre-fix
  failure, and Ready validation are recorded in
  `rust/testport/receipts/expression_collation_audit.md` and `b087.md`.
- The `pkg/planner/core` BETWEEN rewrite now resolves one Go-compatible
  comparison domain across the subject and both bounds, then casts all three
  before constructing GE/LE (or the existing NOT form). The mixed
  string/DATETIME/string source regression is active; pre-fix failure,
  inventory, validation, and the unrelated concurrent owner compile blocker
  are recorded in `rust/testport/receipts/expression_collation_audit.md`.
- `pkg/util/stmtsummary` Rust ownership now follows the current-master
  history/display fixes from `78cac443a4`: v1 and evicted history readers keep
  the newest intervals in chronological order, history reset preserves the
  current interval, KV/PD/backoff/write-response averages divide by execution
  count, v1/v2 table-name builders skip empty entries without dangling commas,
  and v2 records format normalized SQL. Focused fail-before regressions and
  the complete seven-file Rust owner inventory are recorded in
  `rust/testport/receipts/util_stmtsummary_audit.md`; no Go source was edited.

- 2026-09-03: aligned the Rust `tidb-expr` COALESCE temporal result metadata
  with Go master `049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5`. Go's
  `builtinCoalesceTimeSig` and `builtinCoalesceDurationSig` stamp the selected
  value with the merged result FSP; Rust now does the same in the typed scalar
  path without changing the instant. The focused duration and datetime
  regressions reproduce the `.000` suffixes, and the complete `tidb-expr`
  owner plus Ready gates pass. Details, inventory, fail-before evidence, and
  risks are recorded in `rust/testport/receipts/expression_collation_audit.md`.
- 2026-09-03: aligned the Rust `pkg/executor` consumer for
  `INFORMATION_SCHEMA.USER_ATTRIBUTES` with Go master. `tidb-executor` now
  serves the declared three-column schema, while `tidb-session` reads the
  JSON `metadata` member from `mysql.user` and applies the MySQL 8.0.22
  SELECT/UPDATE, CREATE USER, SYSTEM_USER, and self-only visibility matrix
  through the existing privilege registry. The focused session regression
  covers every visibility mode and metadata output; complete inventory,
  fail-before evidence, and remaining boundaries are recorded in
  `rust/testport/receipts/executor_user_attributes.md`.

## Outcomes & Retrospective

The UNION cast batch is implemented but remains a bounded parity slice rather
than a package-complete `pkg/expression` or `pkg/planner` claim. Rust now
preserves the Go `inUnion` marker through AST, expression construction, scalar
dispatch, ordinary UNION projection, and recursive-CTE projection. The
fail-before regression was the existing unsigned wrap (`u64::MAX`) for a
negative signed integer; the post-fix result is zero. The remaining
string-to-DECIMAL and vectorized harness cases stay explicit documented gaps.

The chunk A-1 batch is likewise bounded and executable: representable decimal
datums retain their exact raw layout, while over-wide value-layer decimals now
follow Go's fixed-word prefix/truncation result across Chunk and MutRow entry
points. Its focused regression and owner validation are recorded in
`receipts/chunk_a1_datum.md`; the remaining chunk wire and datum-shape items in
`docs/chunk-and-stats-divergence.md` remain separate follow-ups.

The JSON U+2028/U+2029 batch is bounded and executable: text rendering now
matches Go's JSONP-safe separator escapes without changing binary storage. Its
focused and owner validation are recorded in `receipts/json_u2028_escape.md`;
the remaining JSON merge/invalid-byte boundaries remain separate follow-ups.

The JSON_MERGE_PRESERVE batch is bounded and executable: interrupted object
runs now produce Go's grouped duplicate-key result and array flattening. Its
focused and owner validation are recorded in
`receipts/json_merge_preserve.md`; invalid-byte and surrogate rendering remain
separate follow-ups.

The `pkg/kv` write-conflict batch is bounded and executable: the live generic
9007 driver error now carries Go's `[try again later]` retry marker, with a
focused code/SQLSTATE/message regression. Its inventory and Ready outcomes are
recorded in `receipts/kv_write_conflict_retry_marker.md`; structured conflict
metadata and the 8005 path remain separate follow-ups.

The `pkg/util/dbterror` precedence batch is bounded and executable:
`registered_std` now selects TiDB `errno` messages for overlapping codes, with
the parser/MySQL catalogue as fallback. The focused placeholder/message
regressions and all-target owner profile pass; details are in
`receipts/dbterror_registered_std_precedence.md`.

The DATETIME maximum-precision batch is bounded and executable:
`Time::validate` now rejects a packed microsecond above `999999` only at Go's
`MaxDatetime` ceiling while preserving earlier-date ordering. Its focused
regression and complete datatype owner profile are recorded in
`receipts/types_time_validate_max_datetime.md`; the analogous TIMESTAMP
ceiling and other temporal context boundaries remain open.

The decimal `DIV` unsigned-width batch is bounded and executable:
`Decimal::div_rem_unbounded` keeps the complete quotient until the evaluator
applies Go's `ToInt`/`ToUint` range rules, so an upper-half `BIGINT UNSIGNED`
result is no longer misreported as overflow. Focused datatype and expression
regressions, the complete owner profiles, and the known external JSON-schema
fixture failure are recorded in
`receipts/expression_intdiv_unsigned_width.md`; declared-width/scale and
vectorized expression boundaries remain open.

The raw temporal packing batch is bounded and executable: `Time::to_packed_uint`
now mirrors Go's direct bit-pack and no longer rejects synthetic fields that
the source method does not validate. Its focused regression and datatype Ready
profile are recorded in `receipts/types_time_packed_raw.md`; strict temporal
validation remains on the parse/conversion paths.

Work remains in progress. Current validated behavior includes ANALYZE prefix
indexes, MPP equivalence comparison, retained runnable b103 DDL final-state
tests, lexer tests, funcdep graph tests, and the complete current-master
kvcache test surface including `Peek`. The 2026-09-01 rolling Go-master
plancodec batch also restores
Analyze physical ID 64 and passes its Go/Rust owner and consumer gates. The
following `pkg/util/dbterror/exeerrors` audit certifies the already-aligned
82-entry catalog without changing execution behavior. The plannererrors audit
similarly certifies all 98 prototypes and the source test without changing
execution behavior. The tablecodec/rowcodec/rowDecoder batch now certifies the
current free row/value API and routes old-collation common handles through the
mode-sensitive decoder, with all scoped source suites passing. The final
mode-sensitive decoder, with all scoped source suites passing. The logutil
batch now forwards Go's age-based file retention and records the nested
consistency reporter as an explicit boundary. The final outcome must list
exact files and commands,
remaining unverified packages, and correctness, compatibility, and performance
risks without claiming repository-wide parity.

- 2026-09-05 (F3, `pkg/planner` read-only refusal error identity): the Rust
  `ReadOnlyScanError`, prepared-plan, and prepared-bind surfaces now carry
  Go-compatible MySQL code/SQLSTATE metadata (1064, 1235, 1146, 1054, 8112,
  or the explicit 1105 fallback). The real-TiKV server preserves loaded
  table refusal diagnostics while using typed errors for text, prepared, and
  direct `RealTiKvReadError::Plan` seams. Per-variant planner and server
  regressions are in `receipts/planner_read_only_error_codes.md`; the batch is
  Rust-only and does not widen the bounded read planner.
- 2026-09-05 (`pkg/server` unknown-command wire error identity): the Rust
  command loop now separates genuinely unowned command bytes from known but
  unsupported commands. `Command::Unknown` answers with Go's generic
  `ErrUnknown` 1105/`HY000` and `command %d not supported now`; the existing
  1047/`08S01` refusal remains for known `COM_FIELD_LIST` and
  `COM_RESET_CONNECTION`. The focused fail-before/pass-after regression and
  package inventory are recorded in
  `receipts/server_unknown_command_error.md`; those known-command owners remain
  explicit follow-up boundaries.
- 2026-09-05 (`pkg/server` transaction handshake capability): the Rust
  `tidb-server` handshake now advertises Go's `CLIENT_TRANSACTIONS` bit (1<<13),
  matching the transaction status and command paths already implemented by
  the session. A focused fail-before/pass-after capability regression and the
  complete package inventory are recorded in
  `receipts/server_capability_transactions.md`; `CLIENT_FOUND_ROWS` remains an
  explicit executor/session follow-up.
- 2026-09-05 (`pkg/server/internal/column` result metadata charset): Rust
  `tidb-exec::convert_result_field` now emits the owning charset's default
  collation ID, matching Go `mysql.CharsetNameToID`, instead of leaking a
  column's non-default collation into the wire metadata. The focused
  `utf8mb4_general_ci` regression fails before the change and passes after it;
  inventory and Ready commands are recorded in
  `receipts/server_internal_column_metadata_charset.md`. Character-set result
  transcoding and the remaining command owners stay separate boundaries.
- 2026-09-05 (`pkg/server` `COM_REFRESH` command routing): Rust
  `tidb-protocol` now decodes and exports `COM_REFRESH` (`0x07`), and
  `tidb-server` runs `FLUSH PRIVILEGES` for subcommand `0x01` before emitting
  the command's second OK, matching Go's two-OK dispatch vector; other refresh
  targets remain successful no-ops. A loopback TCP regression consumes both
  responses and then sends `COM_QUIT` to prove command synchronization. The
  complete protocol/server inventory, fail-before compile evidence, and Ready
  commands are recorded in `receipts/server_refresh_command.md`.
- 2026-09-05 (`pkg/format/textrow` unknown result charset fallback): Rust
  `tidb-protocol::ResultEncoder` now keeps Go's binary fallback for an
  unregistered session charset spelling such as `"utf-8"`, emits charset
  number zero for string metadata, and preserves metadata/row bytes after a
  source-column charset is selected. The former Rust-only construction
  refusal and `UnsupportedCharsetName` variant were removed. The focused
  source regression failed before the change and passes with the complete
  textrow vectors; inventory and validation are recorded in
  `receipts/format_textrow_audit.md`.
- 2026-09-05 (`pkg/types` JSON number conversion): Rust
  `tidb-datatype::BinaryJSONValue::Number` now follows Go's
  `appendBinaryNumber` order (`Int64`, base-10 `Uint64`, then `Float64`), so
  unsigned JSON-number text such as `18446744073709551615` remains an
  `UNSIGNED INTEGER` instead of being widened to DOUBLE. The focused
  fail-before/pass-after regression, complete 60-artifact Go package and
  104-artifact Rust owner inventory are recorded in
  `receipts/datatype_json_fieldtype_receipt.md`.
- 2026-09-05 (`pkg/types` `ConvertFloatToUint` NaN behavior): Rust
  `tidb-datatype::convert_float_to_uint` now preserves Go's
  `big.Float.SetFloat64(NaN)` panic instead of silently saturating NaN to the
  unsigned upper bound. Finite and infinity conversion behavior is unchanged;
  the focused fail-before/pass-after regression and Ready owner validation are
  recorded in `receipts/types_float_to_uint_nan.md`.
- 2026-09-05 (`pkg/expression` integer overflow column names): Rust
  `tidb-expr::scalar_function::arithmetic_overflow_error` now renders a
  resolved column's qualified `OrigName` (and the embedded name of a
  correlated column) in Go's 1690 out-of-range message. The active regression
  evaluates `test.t.col1 * -1` over a `MinInt64` row and asserts
  `BIGINT value is out of range in '(test.t.col1 * -1)'`; the complete
  208-artifact Go inventory, fail-before/pass-after evidence, and Ready owner
  validation are recorded in `receipts/expression_overflow_column_name.md`.
