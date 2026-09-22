# Complete the pinned physicalop package without behavioral gaps

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` current while the work proceeds.

Reference: `PLANS.md` at repository root. Repository policy also requires one
complete pinned Go package, including production, generated, test, support, and
build artifacts, as the minimum package-completion unit. The user explicitly
requested committing and pushing progress on 2026-09-21; progress checkpoints
do not claim package acceptance.

## Purpose / Big Picture

Rust planning must produce, cost, clone, rebuild, serialize, and attach the
same physical operators and distributed tasks as pinned Go package
`pkg/planner/core/operator/physicalop` at the current audit baseline
`aba629bb455dc09d6a5d98b3c39a542bb1189b9d`. The physicalop source tree is
unchanged from `be35d4c762f4a8252c059ab0eedc24b310270b8c`; its current
57-artifact inventory and hashes are in `physicalop-source-inventory.md`.
Earlier progress below was
measured against `e2788410d8d696605e8cb002585877a063ccc909` and is historical
evidence, not acceptance of the current package. A user should observe the same
plan shape, SQL result, warning, cache-rebuild behavior, and task placement for
the same statement and session state. Completion means the package has no
Rust-only policy, cache-only physical path, named refusal, ignored substitute,
or missing Go branch.

## Progress

The entries below are checkpoints; older counts and pending items describe
their recorded stage. The latest verified state is summarized first.

- [x] (2026-09-21, Ver2 operator cost dispatch) The Rust production coster now
  reaches the Go `Apply`, `UnionAll`, `PointGet`, `BatchPointGet`, and `CTE`
  formula bodies, forwards trace/recalculation options through every priced
  formula, applies the reader/scan cost factors, and preserves the Go fast
  point-plan `AccessCols == nil` distinction. Detached
  `PhysicalPlan::get_plan_cost_ver2` delegates the same source-shaped
  dispatcher. Focused cost-dispatch and planner checks pass; exchange-receiver
  representation, statement-session/cache behavior, full physicalop inventory,
  and workload acceptance remain open.

- [x] (2026-09-21, MPP-to-root reader boundary) `MppTask.ConvertToRootTaskImpl`
  now builds Go's TiFlash `ExchangeSender(PassThrough) -> TableReader` root
  boundary, expands virtual columns before construction, preserves task
  warnings, and routes source-shaped root-only conditions through the existing
  root Selection helper. Invalid condition placement returns the Go invalid
  task. A focused planner task-shape regression passes; multi-fragment
  ExchangeReceiver construction and remaining MPP enforcement/attachment arms
  remain open.

- [x] (2026-09-21, MaxOneRow enforced-MPP warning routing) The Go
  `CanSelfBeingPushedToCopImpl` refusal is now preserved before Rust's generic
  non-root task gate, so a `MaxOneRow` MPP refusal raises the source warning
  instead of silently returning an invalid task. Planner dispatch carries a
  statement-context warning sink; enforced warnings use ordinary warning
  storage for EXPLAIN and Go's extra-warning storage otherwise. Focused red/
  green planner and executor tests pass. Other MPP operator/task gaps and
  whole-package/workload acceptance remain open.

- [x] (2026-09-21, cache expectations) Go rows, cache-hit statuses and
  explanation warnings confirm both recorded cache failures were stale test
  expectations. Corrected tests preserve typed-entry controls and row checks;
  all 28 non-prepared cases (including the serial metrics case), 45 prepared
  cases and lint pass. No cache production behavior changed.
- [x] (2026-09-21, original expression reference gate) The full original Go
  expression package passes with race detection, intest/deadlock tags and
  failpoints under go.mod's Go 1.25.12 minimum on darwin/arm64 (109.022s).
  Cleanup restores all Go sources. Native acceptance remains open.
- [x] (2026-09-21, original types reference gate) The full original Go types
  suite passes on darwin/arm64 with Go 1.25.12 and race detection (1.106s).
  A complete pinned direct-artifact inventory now records the package unit;
  native acceptance remains open.
- [x] Finalize the verified cache-test corrections and reference-gate receipts
  for ordinary commit/push.

- [x] (2026-09-21, numeric datetime refinement) Source-oracle and red/green
  evidence covers non-Constant temporal operands, live date flags, decimal
  float-string conversion, zero-date policy and DST fractional carries.
  Datatype (427), enabled expression (1210), planner (960), relevant session
  filters and lint pass. Two non-prepared-cache failures reproduce on the
  published baseline and remain open; whole-package acceptance is not claimed.

- [x] (2026-09-21, typed comparison boundaries) Go-oracle and red/green
  evidence covers exact DECIMAL rounding above 2^53, signed-min conversion,
  subquery references, and contextual unsigned diagnostics. Full datatype,
  expression, planner and targeted numeric/cache/subquery checks pass, as does
  lint. A verified narrow-integer projection improves 2.87x in calibrated
  component timing; full package and workload acceptance remain open.

- [x] (2026-09-21, mutable-constant failure state) Source oracle and red/green
  regressions establish Go's stored Datum, retained DeferredExpr, cleared
  parameter marker, traversal stop and retry behavior. The implementation
  preserves that pair privately; full expression/planner, targeted session
  consumers and required lint pass. Receipt follows below. Whole-package and
  workload gates remain open.

- [x] (2026-09-21, prepared EXPLAIN and comparison construction) Current
  statement parameters reach expression rendering; deferred display/evaluation,
  string probe ranges and brief-binary evaluation counts match source rules.
  Go-oracle expansion also corrected comparison cache admission and IN
  construction/refinement. Source race comparisons, isolated native gates and
  required lint pass; detailed receipts follow at the end. Whole-package and
  workload performance acceptance remain open.

- [x] (2026-09-21, temporal and JSON arithmetic) Shared numeric argument
  conversion now covers temporal and JSON columns. A 288-case Go/native matrix
  pins values, strict errors and warnings in both modes; 48 SQL scenarios cover
  precision and NULL warning order. Broad native, Go race, lint, performance
  control and isolated publication checks passed. Whole-package and workload
  gates remain open.
- [x] (2026-09-21, hybrid arithmetic) Native typed operands now preserve
  ENUM/SET integer conversion, signed BIT carriers and unsigned binary literal
  decimal/real values. Source overflow errors and SQL literal warning timing
  pass 242 expression scenarios and 42 SQL queries. Broader native, Go race,
  lint, performance-control and isolated publication checks passed as recorded
  below. This remains dependent-package progress only.
- [x] (2026-09-21, implicit string arithmetic) Reproduced and corrected
  scalar NULL short-circuiting, operand-batch warning/error order, and scalar
  versus vector decimal cast diagnostics for ordinary string operands. Strict
  string literal casts now fold in the SQL builder's statement context, raising
  one construction warning instead of one per row. Native fixtures cover 72
  ordering/selection scenarios, 42 decimal diagnostic scenarios and 24 SQL
  scenarios. Final validation and performance receipts follow below. Whole
  expression and dependent-package acceptance remain open.
- [x] (2026-09-21, value-level DIV precision) Published b969970ba9:
  mixed Real/Decimal/UInt DIV preserves exact decimal conversion and NULL
  behavior. Proven coefficient bounds avoid the general decimal quotient
  calculation, reducing measured decimal DIV time 42.6% and Real DIV 37.7%
  relative to fe4e828bd1. Whole-workload performance remains unaccepted.
- [x] (2026-09-21, division and modulo evaluation) Extended the shared
  numeric path through /, DIV and MOD with Go scalar NULL rules and complete
  argument-batch ordering. DIV now uses decimal input signatures for Real/
  Decimal arguments, bounded decimal division warnings, source precision/scale
  and signature/mode-specific overflow text. Regressions cover 36 checker
  ordering cases, 18 DIV value/diagnostic cases and 24 SQL cases; final gates
  and benchmark results are recorded below. Whole-package acceptance stays open.
- [x] (2026-09-21, numeric arithmetic batch ordering) Reproduced scalar
  NULL short-circuit and vector operand-order mismatches against Go, including
  mixed real/integer SQL expressions. Shared expression batch evaluation now
  covers audited +, -, * trees with typed leaves and numeric widening. Compact
  integer intermediates reuse the existing overflow kernel. Checker warnings,
  selection, unsigned widening, projection SQL and consumers pass scoped gates.
  Isolated integer projections improve 77–82% in ns/row; whole workloads and
  complete package acceptance remain unverified. Final receipts follow below.
- [x] (2026-09-21, deferred/correlated grouping) Go-oracle regressions
  reproduced duplicate batch warnings and correlated string key mismatches.
  Typed correlated evaluation and deferred forwarding to literal/correlated/
  same-domain column leaves now match Go, including the scalar-only decimal
  rounding error. Sixteen field cases cover all eight domains, NULL rebinding,
  selection and continuation in both modes. Final gates are recorded below;
  scalar-function vectorization and whole-package acceptance remain open.
- [x] (2026-09-21, typed constants and string grouping) Restored typed lazy
  conversion, current parameter types, decimal warning/error order and plain
  constant vector evaluation counts. The original GetType independence test
  now runs. Scoped native consumers and accumulated Go race oracles pass;
  borrowed string-column comparison removes repeated copies/key generation.
  Final microbenchmark/publication evidence is recorded at the end. Deferred
  vectorized and other expression domains remain open; no package acceptance.
- [x] (2026-09-21, merge-join shared grouping) Both merge inputs now use
  VecGroupChecker, including encoded cross-chunk continuation and NULL group
  skipping. Eight SQL scenarios match Go under deterministic sort setup;
  76 executor and 16 session tests pass. A 47-artifact join-package inventory
  records remaining whole-package work. No package acceptance or speedup claim.
- [x] (2026-09-21, stream aggregation shared grouping) Reproduced JSON
  cross-chunk grouping mismatch and requested-row overrun before fixing them.
  Stream aggregation now calls VecGroupChecker; its integer direct-column
  comparison is shared with window/shuffle. Go SQL oracle and 197 targeted
  Rust tests pass (76 aggregate, 14 checker, 15 merge/stream session, 68 window,
  24 shuffle). Entire dependent aggregate package inventoried but unaccepted;
  remaining error/memory/failpoint and merge-integration audits stay open.
- [ ] (2026-09-21, original fragment-test coverage audit) The untracked
  fragment.rs draft is NOT declared in lib.rs and references missing CTE
  sink/source variants and ExchangeSender.tasks. Its two test-shaped bodies
  correspond to original fragment_test.go cases but are not compiled or run.
  Do not count them as original-test coverage or integrate this partial draft.
- [x] (2026-09-21, window partition identity and group-checker evaluation)
  Reproduced JSON 1/1.0 cross-chunk partition merging absent from Go. Both
  window schedulers now consume the existing group's exact encoded boundary
  identity and adjacent comparison ranges. Go/Rust agree for 32- and 64-row
  chunks under both modes. A second regression exposed 32 versus Go's two
  conversion warnings: checker now evaluates endpoints before its single-group
  shortcut and scans columns only when needed, with a reusable row mask.
  Original Go checker tests passed; Rust source ports now call production
  chunk evaluation. Four-artifact dependent-package inventory added; stream
  aggregation's separate implementation still needs reconciliation.
- [x] (2026-09-21, RANGE evaluation and temporal diagnostics) Go's exhausted
  bound cursor skips calculation; Rust evaluated unused bounds and incorrectly
  raised BIGINT overflow. Moved evaluation into each actual comparison, in Go
  operand order. Twelve integer result cases and two overflow controls match
  Go. Temporal RANGE exposed dropped DATE_ADD overflow warnings; the shared
  calendar evaluator now applies statement policy to arithmetic overflow.
  Four window warning cases and 14 ordinary interval cases plus strict/IGNORE
  writes match Go. Broader 67 window, 13 boundary, 9 temporal, 4 read-cast and
  202 expression tests passed; the expression selection has 15 ignored tests.
- [x] (2026-09-21, approximate aggregate reachability) Current Go accepts
  approximate DISTINCT/OVER forms formerly rejected by stale session tests.
  A 20-record Go oracle established moving/empty-window and ordinary aggregate
  results. Rust incorrectly deduplicated APPROX_PERCENTILE(DISTINCT ...):
  [1,1,1,9] at 75% returned 9 instead of Go's 1. Both aggregate builders now
  ignore this modifier for percentile. SQL red/green and descriptor execution
  checks pass, alongside 15 JSON, 65 window and 70 hash-aggregation tests.
  This is dependency evidence, not acceptance of aggfuncs or windows.
- [x] (2026-09-21, window aggregate/parser integration) All eight original
  Go windows tests passed again. A new 36-case Go aggregate-window oracle
  exposed obsolete Rust rejection of APPROX_COUNT_DISTINCT OVER. Removed the
  old approximate-only grammar path, matching current Go's shared aggregate
  modifiers/multiple-argument/OVER parser. Independent Go parser oracle proves
  36 accepted forms and two empty-argument rejections. New SQL matrix is green;
  all 734 Rust parser tests pass. Broader window-suite final result below.
- [x] (2026-09-21, exact TPC-H SF1 differential) Current Rust and production
  Go completed all 22 pinned queries with byte-identical full batch outputs,
  including headers, decimal text and row order. No query errors; unchanged
  binary hashes. Session 98737 exited 0 and owned cluster cleanup is verified.
  Artifacts /tmp/tidb-tpch-exact-mocjafvl. This closes the exact-output gate for
  this SF1 dataset only; field metadata, warnings and performance remain open.
- [x] (2026-09-21, MPP session boundary) A SQL SET regression reproduced
  stale enforcement after allowMPP was disabled. Statement snapshots now
  capture allowMPP and derive enforcement as allowMPP && enforceMPP, matching
  Go. The executor planner bridge passes the captured allow flag into existing
  TopN/Expand enumeration gates. All 12 statement, 29 dispatch and 36 cost
  tests pass. MPP warning routing and complete TiFlash execution remain open.
- [x] (2026-09-21, native uint64 bounds) Reproduced debug-build overflow
  panics in LIMIT candidate generation, TopN cost and LIMIT/TopN cop pushdown.
  Five sums now explicitly use Go's wrapping uint64 arithmetic. Three new
  regressions and the 53 physical, 18 task and 36 cost-golden tests pass.
  Ordinary SQL construction clips excessive LIMIT values in Go; this closes
  an internal planner contract gap, not a demonstrated SQL crash or speedup.
- [x] (historical warning audit, closed above) MaxOneRow's enforced-MPP warning
  now routes through the statement warning sink, with normal EXPLAIN warnings
  and extra warnings for other statements under `allowMPP && enforceMPP`.
- [x] (2026-09-21, YCSB RPC attribution) Four additional 50,000-operation
  changing-value samples passed exact cross-engine reads and storage checks.
  TiKV prewrite means (3.59–4.45ms) explain most of aggregate UPDATE latency
  (4.32–5.24ms); Go client prewrite means were 3.74/4.61ms. This supports a
  storage/RPC bottleneck, not a proven client scheduling defect. Metric flush
  boundaries and background requests prevent exact per-request attribution.
  No speculative optimization or performance acceptance. Owned cluster cleaned.
- [x] (2026-09-21, changing-value YCSB and profiles) Six 100,000-operation
  read/update runs passed persisted-change, exact cross-engine table-read and
  storage checks. Unprofiled Rust durations 54.53/62.99s, Go 64.16/60.72s are
  preliminary, not a proven improvement. Rust active SQL threads spent about
  93% of sampled wall observations in prewrite completion; investigate RPC/
  storage latency before changing SQL execution. Invalid cross-process Python
  monotonic timestamps were explicitly discarded. Owned cluster cleaned up.
- [x] (2026-09-21, YCSB A–F integrity smoke) Both current-source engines
  completed all six mixes on independent 1000-row fixtures with four clients.
  Verified 12,000 loaded rows, 12,000 logical operations, 194 inserted rows,
  exact full-table reads through both engines after every phase, and all
  12 storage checks. Cleanup completed. Integrity-mode updates can be no-ops;
  random-value write benchmarks and full-package acceptance remain open.
- [x] (2026-09-21, TPC-H SF1 reference checks) Built current Go with normal
  production tags/link flags. Loaded/analyzed SF1 and ran all 22 queries once
  through each engine using the pinned go-tpc answer checker. Both covered
  Q1–Q22 without reported check failures; owned cluster cleanup is verified.
  Numeric tolerances and a single fixed run order preclude exact-result or
  performance-equivalence acceptance. YCSB and controlled comparisons remain.
- [x] (2026-09-21, current-source TPC-C smoke) Built Go at the Rust audit
  revision and installed pinned benchmark tools. On one fresh warehouse,
  both engines passed all 12 preparation checks and all 11 normal post-run
  checks after 5,000 total transactions (2,000 Rust). A Go-only control
  confirmed --check-all's extra preparation condition is invalid after
  deliveries. Owned cluster cleanup completed. This proves a smoke scenario,
  not performance equivalence, workload optimization or whole-package parity.
- [x] (2026-09-21, original shuffle scenarios) Added Go's exact 13-row
  VARCHAR range-splitter fixture and a deterministic executor-level analogue
  of TestShuffleExit's combined caller error, delayed source panic and worker
  panics. All 24 shuffle tests pass, including cleanup and replay. Injection
  exists only under cfg(test). The original SQL-level failpoint path and
  whole-package acceptance remain open.
- [x] (2026-09-21, shuffle group-key encoding) Replaced generic datum hashing
  with Go's field-type-aware HashGroupKey encoding and GetGroupKey enum/decimal
  adjustments. Captured Go bytes and worker assignments for unsigned, enum and
  decimal inputs. The unsigned regression failed before the fix; all 22
  shuffle tests pass. Encoding appends into reused row buffers, removing the
  previous temporary Vec per key part. No workload speedup is claimed.
- [x] (2026-09-21, shuffle native startup failure) Replaced infallible OS
  thread startup with fallible startup that retains executor ownership until
  the thread exists. Failure cancels and joins started work, restores original
  source/worker order and reports an execution error. The five-position
  regression is red/green; all 21 shuffle and 64 window tests pass. Join SQL
  and lint verification are recorded in the checkpoint receipt below.
- [x] (2026-09-21, shuffle receiver identity) Added a distinct receiver stub
  with a statement-allocated ID and separately owned data source. Receiver
  boundaries are installed after physical rewrites, without cloning sources
  or exposing them as normal worker children. Runtime rows aggregate across
  workers at the receiver ID. The missing-EXPLAIN-row regression is red/green;
  64 window, 60 EXPLAIN, 52 physical-tree tests and builder/reopen pass.
  Whole-package, original failure coverage and workload acceptance remain open.
- [x] (2026-09-21, shuffle runtime counters) Fixed EXPLAIN ANALYZE retaining
  only the last worker's row/call counters. Workers now share their physical
  node's accumulator, with row updates serialized under the existing call
  lock. Added Go's ShuffleConcurrency execution-info field. Both regressions
  failed before their fixes and pass afterward; all 60 EXPLAIN tests pass.
  Separate receiver-stub identities and their display/statistics remain open.
- [x] (2026-09-21, shuffled SQL and ownership) Captured five Go join/aggregate
  plans and result sets and verified Rust serial/parallel execution against
  them. Preserved shuffle-referenced projections across post-optimization;
  the missing-boundary regression is red before and green after. Shuffled
  window input also exposed unsafe TopN appends into aliased input chunks;
  matched Go chunk.List's fresh-buffer rule, with a deterministic regression.
  Validation passes: 52 physical-tree tests, builder/reopen, 14 merge-join
  tests, 51 TopN tests, and 63 window SQL tests. Whole-package and performance
  acceptance remain open.
- [x] (2026-09-21, shuffle selection) Connected resolved session concurrency
  and group-NDV skew settings to window/stream-aggregate/merge-join selection.
  Added Go's sort and NDV gates after property enforcement. A Go oracle
  confirmed unordered window shuffle and ordered non-shuffle behavior. Fixed
  nominal-sort child rewrapping that invalidated ORDER BY. All 63 window SQL
  tests, the 12-case selection matrix, and 29 dispatcher tests pass. Full
  operator/package and workload acceptance remain pending.
- [x] (2026-09-21, shuffle node and builder) Added the physical enum node,
  source-schema index binding, explain metadata, task attachment, memory
  accounting, deep-copy metadata, and Go's exact plan-cache refusal. Builder
  constructs sources once and substitutes owned receivers in worker copies.
  All 51 physical-tree tests and the new builder/reopen test pass. The
  optimizer rewrite/session-concurrency wiring remain absent, so ordinary
  SQL planning does not select shuffle yet. No package acceptance is claimed.
- [x] (2026-09-21, shuffle scheduler) Replaced the standalone sequential
  scheduler with owned source/worker threads, bounded holder channels that
  recycle chunks, cancellation, joins, and source/worker panic recovery.
  The backpressure regression failed before the change (101 fetches before
  first output) and passes afterward. All 20 focused shuffle tests pass.
  This remains seed evidence inside the ongoing complete-package work;
  production planner/builder integration and upstream validation are pending.
- [x] (2026-09-21, current source inventory) Inventoried all 57 physicalop
  artifacts at the pulled HEAD, plus direct generation/module inputs, with
  source hashes. A Python hashlib check independently verified all 62 hashes
  against worktree bytes and exact package file-set equality; `git diff
  --check` passed. `git diff be35d4c762f4a8252c059ab0eedc24b310270b8c HEAD
  -- pkg/planner/core/operator/physicalop` was empty. This checkpoint changes
  documentation only, so it adds no runtime validation claim. Every artifact
  remains pending whole-package acceptance.
- [ ] Current integration audit found no PhysicalShuffle plan variant or
  production builder path; optimizeByShuffle is explicitly omitted. The
  standalone Rust shuffle executor was sequential at that checkpoint; the
  subsequent scheduler checkpoint above replaces that implementation. Entries claiming
  shuffle integration are superseded by this current-tree finding. Complete
  the planner/executor path and its Go behavior validation before acceptance.
- [x] (2026-09-21, chunk/precision matrix) Expanded the wrapped-frame
  regression from 28 to 192 independently observed Go cases: eight aggregate
  expressions, two bounds, three chunk sizes, two precision modes, and two
  execution modes. Rust matches every result/error without further production
  changes. This strengthens the boundary evidence, not package acceptance.
- [x] (2026-09-21, wrapped sliding frames) Captured 28 upstream Go SQL
  outcomes for seven aggregate expressions, two unsigned-wrap frame bounds,
  and normal/pipelined modes. Fixed Rust's backward-frame recomputation to
  preserve Go's sliding evaluation order and recovered bounds error. All 62
  SQL window tests and 23 selected unit tests pass; wider combinations and
  the full dependency/integration audit remain pending.
- [x] (2026-09-21, ranking evaluation order) Removed eager per-row peer
  metadata. Each ranking function now advances its own result-time cursor,
  matching Go RANK/DENSE_RANK/PERCENT_RANK/CUME_DIST comparison order. The
  trace regression failed before the change and passes in normal/pipelined
  execution and after reopen. All 61 SQL window tests and 13 executor
  source/boundary tests pass; whole-package acceptance remains pending.
- [x] (2026-09-21, write profiling) Rebuilt release and ran fixed-seed
  Rust/Go/Go/Rust write-only samples on a fresh shared cluster, then a separate
  native sampling run. Rust averaged 4.17/4.19 ms per transaction; Go averaged
  4.18/4.12 ms. The former large Rust-specific gap did not reproduce. About
  71% of sampled SQL-thread stacks waited for prewrite replies. This guides
  further investigation but does not establish full performance parity.
- [x] (2026-09-21, aggregate test recovery) Migrated the stale hash-join probe
  tests to the owned-fetcher API and restored the DDL suite to the aggregate
  target because ALTER tests import its helpers. Fixed a reproduced race that
  replaced worker panic errors with input-disconnection errors. All 31
  hash-join source tests and 102 DDL/ALTER tests pass in the aggregate binary.
- [x] (2026-09-21, live verification) Rebuilt release at aba629bb45 plus
  the recorded worktree and reran the fresh Go/TiKV sysbench ladder. All eight
  Rust cells, eight Go cells, 24 SQL checks, and six concurrent-DDL checks
  passed. Go's notifier SELECT/ADMIN CHECK TABLE succeeded; no notifier
  decoding errors appeared. Owned cluster cleanup completed. Write-heavy
  timings were slower than Go in this single sample and need profiling;
  this is compatibility evidence, not performance acceptance.
- [x] (2026-09-21, notifier handle encoding) Reproduced integer row-ID keys
  being written into a clustered notifier table. Selected the existing row
  writer from stored TableInfo and stopped allocating row IDs for clustered
  tables. The byte-level regression and all 89 catalog DDL tests pass. Live
  Go decoding and the full sysbench ladder still need rerunning.
- [x] (2026-09-21, schema cache identity) Fixed catalog metadata version
  collisions across rebuilt catalogs and divergent clones. The cross-session
  prepared UPDATE regression now passes, as do point/range SELECT checks
  across ADD/DROP COLUMN. Prepared-cache, catalog snapshot, and temporary-table
  validation receipts are below. The live concurrent-DDL workload rerun and
  independent notifier interoperability failure remain pending.
- [x] (2026-09-21, workload diagnosis) Built the current Rust release and ran
  the owned-cluster sysbench ladder. All eight single-thread cells passed,
  but concurrent DDL broke prepared UPDATE and Go could not decode the DDL
  notifier table. Added an embedded-store regression reproducing stale UPDATE
  width after cross-session ADD COLUMN. This regression is intentionally red;
  the production fix and subsequent workload rerun remain pending.
- [x] (2026-09-21, current window checkpoint) Mapped original Go test actions,
  passed all eight original Go window tests with Go 1.26.0, and verified
  unsigned LEAD/ROWS edge cases against live Go SQL execution. Fixed ordinary
  frame-less input evaluation counts, update/result error ordering, ignored
  frame notes, and unnecessary peer construction for non-ranking functions.
  Current Rust validation: 61 session window tests, 23 selected executor unit
  tests, and 12 executor boundary/source tests pass. Exact receipts follow.
- [ ] Accept complete windows/physicalop packages only after the remaining
  production/dependency audit and integration gates; measure sysbench,
  TPC-C, TPC-H, and YCSB before making current performance claims.

- [x] (2026-09-21, pipelined implementation) Added the actual pipelined
  scheduler using retained chunk ranges, strict frame lookahead, pending
  partition groups, persistent partial results, and `accumulated <= dropped`
  output-alias release. Wired the session flag through both statement-context
  paths, all four planner-builder bridges, and physical executor selection.
  Added forced-pipelined OrderedWindowExec. The streaming-before-child-error
  regression failed against normal Window and passes through the new path.
  All 52 SQL window tests and 23 selected executor unit tests pass. Full
  original-Go test mapping and package acceptance are still open.
- [x] (2026-09-21, pipeline row-retention prerequisite) Replaced the ordinary
  Window executor's copied partition Chunk with owned ranges of child chunks.
  Aggregate frame evaluators now accept ranges spanning chunks and keep
  absolute row indexes when prefixes expire. The six executor tests pass,
  including output reset, shared chunks across partitions, and reopen. Three
  new ownership tests prove selected-row indexing, error short-circuiting,
  prefix expiry, release of an input chunk after its last range expires, and
  continued sliding updates after prefix expiry.
  The buffer is used by ordinary execution; the actual pipelined state machine
  and its selection remain unfinished.
- [x] (2026-09-21, extrema/XOR sliding prerequisite) Reproduced Go's newest
  collation-equal MIN/MAX value rule: Rust returned older `A` for `[A,a]`
  under `utf8mb4_general_ci`, while Go's deque retains `a`. Added typed
  monotonic MIN/MAX state and BIT_XOR inverse updates. The source regression
  now passes. All 51 SQL window tests and six executor boundary tests pass;
  typed coverage includes unsigned BIGINT, FLOAT, DOUBLE, DECIMAL, DATE,
  DATETIME, TIMESTAMP, TIME, and BIT. True pipelined/ordered execution and
  whole-package acceptance remain unfinished.
- [x] (2026-09-21, continuation) Revalidated the actual `task.rs` and found
  that the historical OriginSchema completion entry did not describe this
  checkout: double reads and index merges still rejected `NeedExtraProj`.
  Restored the task-carried original schema and Go's projection placement
  below root conditions for table, double-read, and index-merge readers.
  Both new regressions failed before the implementation and pass afterward;
  all 69 tests selected by `task::` pass. The table/double-read HashAgg and
  StreamAgg exception is covered by a four-case regression.
- [x] (2026-09-21, continuation) Updated obsolete constructors in the existing
  untracked Expand and Window test files so the aggregate executor harness
  compiles. Generated-column SQL checks pass (11 running tests; one existing
  ignored DDL concurrency case). The expanded ordinary-query regression
  covers root virtual-column filters with both table and index-lookup access;
  it passes. The Expand source regression also passes.
- [x] (2026-09-21, Window continuation) Reproduced the ready-chunk-before-error
  failure, then replaced the full-child drain with Go's per-child result queue
  and complete-partition processing. Passthrough columns alias the input using
  the resolved output indexes; the Rust-owned frame buffer retains only the
  current partition. Four source regressions pass, including chunk boundaries,
  NULL partition keys, peers, duplicate/reordered outputs, output reset, and
  reopen. All 48 existing session window tests pass.
- [ ] Complete the entire `pkg/executor/windows` package and its required
  `pkg/executor/aggfuncs` consumers. Ordinary chunk readiness is corrected;
  pipelined/ordered execution is now implemented and under audit. Remaining
  aggregate state, complete original-test mapping, concurrency/integration
  checks, and required Go/benchmark gates remain open.
  The historical Window completion entry is not current acceptance evidence.
- [x] (2026-09-21, sliding prerequisite) Added COUNT partial-result state with
  Go's departing-before-arriving evaluation order, initialized only after a
  nonempty frame and reset at ordinary output-chunk/partition boundaries.
  The 100-row/10-row-frame regression failed with 955 evaluations before the
  change and passes with 190 afterward. Six executor source tests now pass;
  the added matrix covers NULLs, COUNT(*), empty and disjoint frames, four
  chunk sizes, two partitions, and reopen. Other sliding aggregates and the
  actual pipelined executor remain unfinished.
- [x] (2026-09-21, numeric sliding prerequisite) Reproduced the ignored
  `windowing_use_high_precision=OFF` setting with SUM/AVG over a two-row
  moving frame containing `1e16, 1, 1`. Before the change OFF returned the
  high-precision final values 2 and 1; Go's add-before-subtract order returns
  0 and 0. The ON/OFF/ON SQL regression now passes. Wired the statement
  snapshot through StmtContext and Columns, and implemented decimal SUM/AVG
  sliding plus Go's real-valued precision choice. All 49 window SQL tests and
  four focused numeric tests pass. Other sliding functions and the pipelined
  executor remain open; this is not package acceptance.
- [x] (2026-09-21) Pulled `origin/hparser-integration` to `be35d4c762`
  and revalidated the physicalop source boundary. It now has 57 artifacts:
  the previous 55 plus `single_scan_index_join.go` and
  `storage_engine_usage.go`. `BUILD.bazel` and `physical_utils_test.go` also
  changed. There is no `doc.go` or platform-specific source in this package.
- [x] (2026-09-21) Reproduced two IndexMergeReader resolution failures before
  changing production code: generated-column dependencies retained stale index
  77, and partial scans were modified before the table-plan error. Implemented
  Go's generated-column binding and table-before-partials traversal. Both
  regressions pass, as do all 61 `physical::` unit tests. Ordinary planning
  calls this resolver in `tidb-executor/src/driver/planner_bridge.rs`. The
  existing cached physical index-merge executor integration test also passes.
  Schemas without generated columns are not copied by the new binding step.
- [ ] Audit the current-baseline additions and all prior unchecked behavior;
  finish ordered IndexMergeReader handle representation and resolution.
  The two new helper files have Rust implementations in
  `tidb-planner/src/storage_engine_usage.rs`, but a source search found no
  production callers; their optimizer integration remains unverified.
- [x] (2026-09-01) Enumerated the pinned package: 55 artifacts, comprising
  `BUILD.bazel`, 51 hand-written production Go files, one generated production
  Go file, two test files, and no package fixtures or platform variants.
- [x] (2026-09-01) Read the pinned task, task-base, enforcer, fragment,
  exchange sender/receiver, Sequence, UnionAll, and index-lookup task sources
  before accepting their Rust implementations.
- [x] (2026-09-01) Replaced Rust's `CopTask.OriginSchema` refusal with Go's
  column-only extra projection for table, double-read, and index-merge readers;
  two focused regressions pass.
- [x] (2026-09-01) Removed the ignored `max_count`/`min_count` physicalop test
  copied from newer `master`; those aggregate names are absent from the pinned
  commit and implementing them would add non-Go behavior to this baseline.
- [x] (2026-09-01) Implemented the pinned fragment singleton matrix and
  task-address-local CTE sink/source counts as running tests, including real
  `PhysicalCTESink` and `PhysicalCTESource` variants in the closed plan tree.
- [x] (2026-09-01) Ported Go's MPP CTE-reader enumeration, Sequence producer
  status matrix, and distinct child-bearing `PhysicalCTEStorage`; focused
  enumeration and attachment tests pass.
- [x] (2026-09-01) Replaced the logical Window and UnionScan dispatcher
  refusals with physical candidates, task attachment, and explain coverage.
  Focused physical and attachment tests pass. The ordinary executor now uses
  Go's current-partition consumption, monotone RANGE cursors, and sliding
  aggregate processors. The session's `windowing_use_high_precision` value now
  selects Go's non-sliding FLOAT SUM/AVG shape when ON and sliding shape when
  OFF; TiPB completion remains open. Pinned Go does not clone Window for plan
  cache, so the former Rust cache-expression support was removed.
- [x] (2026-09-01) Restored `buildDataSource`'s pinned UnionScan creation
  gate for transaction-dirty, local-temporary, and cached tables. Dynamic
  partition reads now append `_tidb_tid` before wrapping the DataSource, and
  focused tests distinguish local from global temporary tables.
- [x] (2026-09-01) Wired ordinary `PhysicalUnionScan` construction through
  Go's reader-shape matrix. Rust's transaction-private catalog scan already
  performs the snapshot/staged-row merge, so the physical node is an identity
  execution boundary rather than a second overlay; conditions, index order,
  and distinct runtime-plan identity have focused regressions.
- [x] (2026-09-01) Ported pinned `PhysicalShuffle` representation, explain,
  session concurrency plumbing, the Window/StreamAgg/
  MergeJoin `optimizeByShuffle` rewrite, and ordinary executor construction
  over the existing Shuffle executor. Cross-crate WIP checks and the focused
  planner rewrite regression pass. The focused builder regression is written,
  but the executor test target is currently blocked by an unrelated existing
  `tidb_model::distance_metric` test-compilation error. Pinned Go does not
  clone Shuffle for plan cache, so no cache traversal is retained.
- [x] (2026-09-01) Audited all 55 pinned artifacts against the shared Rust
  worktree and recorded their owning crates. Restored Go's exact recursive
  `CloneForPlanCache` admission matrix and removed cache rebuild handling for
  unsupported Apply, CTE, Expand, Window, Exchange, and Shuffle families; the
  focused admission regression passes.
- [x] (2026-09-01) Replaced the enum-wide infallible ordinary physical clone
  with pinned Go's operator support matrix and recursive child failure. The
  prepared SELECT and DML caches now retain immutable admitted templates,
  clone one private tree per hit, and run range rebuilding on that clone as
  Go does. Focused clone/admission regressions and cross-crate checks pass.
- [x] (2026-09-01) Restored `CopTask.FinishIndexPlan`'s statistics-version
  pin: the table scan adopts index cardinality but retains the original table
  `StatsVersion`, exactly as pinned Go does. Removed the obsolete ignored
  UnionScan/Fragment gap documentation and added the running
  Selection-Projection UnionScan attachment regression.
- [x] (2026-09-01) Restored Go task-carried DataSource statistics for
  `CopTask.handleRootTaskConds` and `MppTask.GetTblColHists`. Root-side
  conditions now use the available column NDVs instead of always taking the
  0.8 error fallback; MPP exchange, Sequence, and CTE-storage transitions
  retain the source profile while UnionAll deliberately clears it as Go does.
  Focused selectivity and task checks pass.
- [x] (2026-09-18) Reconciled the post-pull physical dispatcher with the
  pinned Go behavior and wired every physical-enumeration session input through
  the ordinary statement context: MPP permission, skew/three-stage aggregate
  switches, late materialization, partial-order TopN, prefer-range scan,
  Window/MergeJoin/StreamAgg concurrency, and fixes 45132/56318. A focused
  session regression changes all values and observes the exact planner
  snapshot; planner/executor/session checks pass.
- [x] (2026-09-01) Ported pinned prefix-index partial-order TopN end to end:
  session `COST` gating, candidate-first enumeration, projection remapping,
  exact access-path matching, task-carried match state, prefix-aware pushed
  Limit, and root TopN metadata. Candidate, projection, attachment, and full
  DataSource-to-reader regressions pass.
- [x] (2026-09-01) Ported pinned TiFlash predicate-order planning: dispatch
  grouping/order, heavy/simple classification, inverted-index preference and
  hints, forced-index behavior, and late-materialization scan state. Focused
  tests and planner/executor/session checks pass; broader statistics fidelity
  remains package-external follow-up evidence, not a physicalop completion
  claim.
- [x] (2026-09-01) Removed the stale ignored heavy-function TopN gap and
  replaced it with a running pinned-shape regression: when the heavy vector
  expression is the second by-item, the pushed projection evaluates it once
  and both local/global TopN operators use the same generated column without
  disturbing the earlier light item. Restored pinned fix-control `56318` and
  TiKV `AllowProjectionPushDown` gating; the disabled path now keeps the
  pushed heavy expression unrewritten, while MPP retains Go's independent
  projection behavior.
- [ ] (2026-09-01) Porting pinned package-wide `ResolveIndices`: common unary
  operators, aggregation/window/scan/lock, Shuffle-owned sources, hash/merge/
  index joins, Apply, and reader-owned hidden plans now compile; focused
  Shuffle and null-aware HashJoin regressions pass. Projection-neighbour
  refinement, complete generated-column fallback, DML, prefix columns, and
  remaining exact reader handle representations are still open, so this item
  is deliberately not complete.
- [x] (2026-09-01) Removed two obsolete ignored-gap tests which still claimed
  Window/stream-count cloning and ExchangeSender index resolution were absent.
  They are now running ports of pinned Go's clone and independent-schema
  resolution assertions; all four tests in that source pass with zero ignores.
- [x] (2026-09-01) Ported pinned `avoidColumnEvaluatorForProjBelowUnion` at
  the physical task boundary: direct Projection children of root and MPP
  UnionAll are marked, nested/ordinary projections remain unmarked. Removed
  the obsolete ignored gap and added a running tree-shape regression.
- [x] (2026-09-01) Restored `PhysicalTableScan.ExtractCorrelatedCols` over
  `LateMaterializationFilterCondition`, removed its now-false ignored gap,
  and added a running correlated-column regression.
- [ ] (2026-09-01) Porting pinned `PhysicalExpand`: exact Root/MPP candidate
  enumeration, `LevelExprs`/extra-name representation, index resolution,
  cloning, ordinary task attachment, and the pinned serial `ExpandExec` path
  now have focused passing regressions. Legacy nested `GroupingSets` and
  TiFlash `Expand`/`Expand2` serialization remain open, so this item is not
  complete.
- [ ] (2026-09-01, continuing) Consolidating plan cost on the wired Ver2
  dispatcher: the production coster now dispatches the available pinned
  Apply, UnionAll, IndexMergeReader, CTE, PointGet, and BatchPointGet formulas,
  with reader/scan factors and option tracing wired in the latest receipt.
  ExchangeReceiver still lacks a corresponding Rust physical-plan variant;
  selectable Ver1 and remaining session/cache semantics remain open.
- [ ] Read and map every remaining pinned production and generated file to its
  owning Rust implementation and consumer.
- [ ] Reconcile both pinned test files (`fragment_test.go` and
  `physical_utils_test.go`) with running Rust tests.
- [ ] Remove every package-owned Rust refusal, narrowing, disconnected shell,
  and behavior policy not present in pinned Go.
- [ ] Implement every package-owned Go branch absent from Rust, including its
  planner/executor integration where behavior would otherwise remain inert.
- [ ] Create `rust/testport/receipts/planner_core_operator_physicalop.md` with
  the complete artifact and behavioral inventory.
- [ ] Pass the isolated index-only test gate, the pinned Go package test, the
  Rust package/integration tests, `make lint`, formatting, and diff checks.
- [ ] Commit and push the complete package as one batch to
  `origin/hparser-integration`.

## Surprises & Discoveries

Deferred/correlated grouping (2026-09-21): Go's Constant.VecEvalXxx forwards the
requested evaluation domain to its deferred child, using the child's own
metadata for conversions. CorrelatedColumn.VecEvalXxx broadcasts one typed
binding read. A decimal child column containing an 81-digit interior value
succeeds in vector mode, but scalar evaluation pads it to the outer constant's
scale and fails with 1265. This difference is source behavior, not a reason to
normalize the modes. The oracle and native regression retain both outcomes.

Typed constant grouping (2026-09-21): generic Constant.Eval is not equivalent
to EvalXxx. It fits deferred values to the declared type, while typed decimal
evaluation preserves extra scale and ignores declared precision. String
overflow can emit a 1690 conversion warning and then fail with 1265 while
padding scale. A plain vectorized constant emits one interior warning per
batch; scalar mode emits one per row. The final receipt includes Go evidence.

- Shuffle used group_key_part's generic datum encoding despite Go using
  aggregate.GetGroupKey. For unsigned max, Rust emitted uint flag 9 and a
  ten-byte uvarint; Go emitted signed-int flag 8 and zigzag byte 1. The same
  generic path omitted GetGroupKey's enum numeric and decimal flen adjustments.
  Evidence: /tmp/tidb-shuffle-keys-go.log and /tmp/tidb-shuffle-keys-red.log.

- Native shuffle thread startup could panic after taking executors out of the
  parent, discarding unstarted executors without Close. Go goroutine creation
  has no corresponding recoverable OS-thread API. A five-position injected
  startup-failure test exposed the panic before the fix; native ownership
  restoration must accompany the existing Go cancellation/close contract.

- Observation: the current Window builder located function result types after
  the full child schema, while Go locates them after the passthrough portion
  of the output schema. This differs when columns are pruned or duplicated.
  The builder now uses `output_schema.len() - window_func_descs.len()`, and
  `fetch_child` maps each passthrough column using its bound index, as Go's
  `copyChk` does. The direct regression exercises duplicate and reordered
  columns with five child chunk sizes and reopen cycles.

- Observation: old plan checkboxes do not establish current implementation
  parity. At the current baseline, `task.rs` lacked `origin_schema`, contained
  two explicit NeedExtraProj refusals, and placed virtual-column cleanup above
  root conditions. These were corrected in the continuation, rather than
  trusting the earlier completion text.

- Observation: the original untracked Window test also made an incorrect
  assumption about Go's chunk boundary. Go `WindowExec.Next` waits until all
  rows in its first result chunk are ready (`preparedChunkAvailable`), so a
  completed partition occupying only part of that chunk is insufficient.
  The corrected test emits two separate one-row child chunks, then an error;
  Go can return the first chunk after seeing the second partition, while
  Rust's current full-child drain fails before returning it. The mock explicitly
  splits the rows because `MemTableSourceExec` emits its entire table at once.
  Evidence: `pkg/executor/windows/window.go:55-122` and the running corrected
  test in `rust/crates/tidb-executor/tests/window_executor_source.rs`.

- Observation: the old inventory is incomplete for the freshly pulled Go
  tree. The delta is two new helpers, two new tests in the existing test file,
  and Bazel metadata. The IndexMergeReader resolution source is unchanged
  between the historical and current baselines.
  Evidence: `git diff --stat e2788410d8d696605e8cb002585877a063ccc909
  be35d4c762 -- pkg/planner/core/operator/physicalop` reports four changed
  files and 239 insertions; `git ls-tree` enumerates 57 package artifacts.

- Observation: Go lint currently cannot bootstrap its required tool.
  Evidence: `make lint` fails at `go install github.com/mgechev/revive@v1.2.1`
  with "module ... found, but does not contain package ..." before linting.
  The installed `tools/bin/revive -version` reports exactly `version 1.2.1`.
  `make -o tools/bin/revive lint` subsequently passed: this skips the phony
  reinstall target and executes the unchanged revive and dashboard lint recipes.
  Plain `make lint` still has the installer failure; do not report it as passing.

- Observation: the existing Rust work is not a complete package despite broad
  MPP support. `task.rs` still explicitly refused valid `NeedExtraProj` tasks
  because `OriginSchema` was absent.
  Evidence: pinned `task_base.go:551-557,594-600` and
  `physical_indexlookup_reader.go:295-313` build a `PhysicalProjection`; the
  pre-change Rust branches returned `PlanError`.

- Observation: this package crosses native Rust crate boundaries. Physical
  representation and task attachment live primarily in `tidb-planner`, TiPB
  conversion also uses `tidb-proto` and `tidb-exec`, and direct executor
  construction lives in `tidb-executor`.
  Evidence: the current physical tree, request DAG, and executor builder all
  own parts of one Go physicalop behavior path.

- Observation: legacy testport evidence is not necessarily evidence for the
  pinned baseline. The ignored MaxCount/MinCount split test cited newer
  `master`, while a pinned-tree search finds no such aggregate name.
  Evidence: `git grep -n 'AggFuncMaxCount\|AggFuncMinCount'` at the pinned
  commit returns no match.

- Observation: the local branch can lag the shared remote while the worktree
  contains unrelated uncommitted work. Package commits must therefore be
  validated as an index-only overlay and integrated onto the current remote in
  a clean temporary checkout before push.

- Observation: the pinned `physical_utils_test.go` contains only the list and
  tree flatten tests. The ignored BatchPointGet and Max/Min MPP entries in the
  old gap catalog cite files/tests absent from this package at the pin.
  Evidence: the full pinned file is 84 lines and ends after
  `TestFlattenTreePushDownPlan`; both flatten behaviors already have running
  Rust tests.

- Observation: exchange representation alone was insufficient for Go's MPP
  fragment behavior. The closed Rust plan had neither CTE sink/source variants
  nor a fragment owner, so local CTE counts could not be serialized or consumed.
  Evidence: both pinned `fragment_test.go` tests were ignored placeholders
  before `fragment.rs` and the two physical variants were added.

- Observation: Rust had complete logical Window and UnionScan types but the
  physical dispatcher still rejected them. UnionScan also needs Go's
  projection pull-up rewrite at attachment time; merely adding an enum arm
  would create an executor-invalid `UnionScan -> Projection` tree.
  Evidence: pinned `physical_union_scan.go` and `core/task.go:82-123`; the new
  focused attachment test asserts `Projection -> UnionScan -> child`.

- Observation: physical Window now has representation, enumeration, task
  attachment, and ordinary root executor construction. Pinned Go also
  serializes it to TiPB, which remains the outstanding consumer.
  Evidence: pinned `physical_window.go:287-349`; Rust
  `tidb-executor/src/driver/physical_builder.rs::build_window`.

- Observation: Go's Shuffle plan deliberately aliases `Tails` and
  `DataSources` into its child tree and mutates the tail children once per
  worker during executor construction. Rust's owned tree cannot express that
  alias safely; cloning the head per worker and replacing nodes by stable plan
  ID preserves the same worker/source topology without an unsafe executor
  pointer.
  Evidence: pinned `physical_shuffle.go` and `executorBuilder.buildShuffle`;
  Rust `physical_builder.rs::install_shuffle_receivers` performs the native
  ownership translation.

- Observation: the initial ordinary Window executor materialized the complete
  child and recomputed aggregate frames per output row, which differed from
  Go's current-partition consumption and sliding aggregate processors both in
  error timing and asymptotic work.
  Evidence: the rewritten executor returns a completed partition before a
  later child error, advances monotone RANGE cursors, and uses moving
  COUNT/SUM/AVG/MIN/MAX/BIT_XOR state. Focused latency and moving-frame tests
  pass; the inline O(n) instrumentation test is presently blocked before
  execution by an unrelated pre-existing cfg(test) compile error in
  `kv_table.rs`. Go's high-precision session flag now reaches aggregate
  construction through the statement snapshot and disables sliding only for
  FLOAT SUM/AVG while it is ON.

- Observation: Go's reader flattenings alias the pushed-down plan nodes, while
  Rust owns flattened copies. After resolving the authoritative hidden plan,
  Rust must regenerate `IndexPlans`/`TablePlans`; resolving only the copies or
  only the root would leave executor metadata stale.
  Evidence: pinned reader `ResolveIndices` bodies mutate aliased plan objects;
  Rust `PhysicalIndexLookUpReader::resolve_indices` now rebuilds both flattened
  arrays from the resolved roots.

- Observation: although `buildExpand` initially copies projection concurrency,
  the pinned `ExpandExec.Open` unconditionally sets `numWorkers` to zero because
  its parallel evaluator is not implemented. A serial Rust executor is exact
  pinned behavior, not a performance-policy narrowing.
  Evidence: pinned `pkg/executor/builder.go:2264-2296` and
  `pkg/executor/expand.go:53-58`; the focused Rust integration test exercises
  one cached child chunk through every level projection.

- Observation: `PhysicalPlan::get_plan_cost_ver1` and its blanket `to_pb` remain
  disconnected seed APIs after the planner acquired the common Ver2 coster and
  DAG request serializer. The generic Ver2 path previously priced real
  operators as free or returned a TODO; it now delegates the recursive
  source-shaped dispatcher, while the blanket PB path still makes a table scan
  take Go's base error instead of its override.
  Evidence: repository-wide call search found only self-tests for these
  methods; `find_best_task::coster::Ver2Coster` is the production comparison
  path and `tidb-exec::dag_request` is the wired TiKV serialization path.

- Observation: Go's physical cacheability walk is followed by a distinct,
  recursive `CloneForPlanCache` gate. Rust's generic deep clone had admitted
  CTE, Expand, Window, and Shuffle-family plans that the pinned Go base clone
  refuses.
  Evidence: pinned `plan_cache.go:289` and `plan_clone_generated.go`; Rust now
  applies the exact generated/manual operator matrix only after the physical
  cacheability walk succeeds.

- Observation: old testport gap catalogs are not reliable current-state
  inventories. They still called Fragment and UnionScan attachment absent
  after both had running implementations, and `FinishIndexPlan` still named
  `StatsVersion` as unavailable after `StatsInfo` gained the field.
  Evidence: the stale ignored UnionScan function was removed in favor of the
  running task regression; `count_reads_whichever_half_the_cop_task_has_open`
  now asserts cardinality adoption and table-version retention together.

- Observation: `PhysicalProperty::CloneEssentialFields` correctly retained
  `PartialOrderInfo`, but `LogicalProjection::TryToGetChildProp` did not
  transform it through projection expressions. That could let a prefix-index
  candidate compare output-column IDs directly with child-column IDs and die
  despite Go accepting it, or survive a computed expression Go rejects.
  Evidence: pinned `logical_projection.go:524-591`; the Rust projection now
  independently transforms ordinary and partial-order sort items and the
  end-to-end prefix-index TopN regression reaches the pushed index-side Limit.

- Observation: the heavy-function TopN port had hard-coded the shipped
  `Fix56318` default even though Rust already carried the session optimizer
  fix-control map. It also omitted Go's TiKV-only
  `AllowProjectionPushDown` gate.
  Evidence: pinned `pkg/planner/core/task.go::getPushedDownTopN`; physical
  TopN candidates now retain the evaluated session gates and focused tests
  distinguish the rewrite-enabled and fix-disabled shapes.

- Observation: the package-wide audit found four P0 integration groups still
  open: general TiKV/TiFlash physical-tree protobuf lowering, selectable Ver1
  plus remaining Ver2 point costs, ordinary Lock/Show executor construction,
  and field-exact plan-cache cloning. P1 groups include missing
  task state, runtime filters, TiFlash predicate reordering/prefetch, ordinary
  clone semantics, and specialized memory/probe accounting.
  Evidence: complete pinned-symbol and Rust-consumer searches over all 55
  artifacts; these groups remain explicit completion gates below.

## Decision Log

- Decision: expose the requested typed Constant domain and implement correlated
  typed evaluation in the expression owner. Unwrap vectorized deferred chains
  only to supported constant/correlated/same-domain column leaves; retain the
  outer expression for scalar mode and scalar-function leaves.
  Rationale: vector forwarding must preserve the child's metadata and one-read
  warning semantics. Scalar-function batch behavior still requires a complete
  expression-vectorization audit and must not be inferred from scalar success.
  Date/Author: 2026-09-21, Codex.

- Decision: keep typed Constant evaluation and contextual ToDecimal in their
  expression/datatype owners, and let the checker call the typed path. Borrow
  ordinary string cells and cache one key per row; preserve NaN comparisons
  even when a vectorized constant is evaluated only once.
  Rationale: generic conversion alters Go diagnostics/decimal shape, while
  repeated Datum/key allocation is unnecessary for stable chunk storage.
  Date/Author: 2026-09-21, Codex.

- Decision: represent TestShuffleExit's delayed source panic with a test-only
  channel gate, releasing it after Next returns its injected caller error.
  Rationale: preserves the failure ordering without Go's timing-dependent
  100 ms sleep. Worker panic injection remains before its cancellation check,
  matching shuffleWorkerRun. Date/Author: 2026-09-21, Codex.

- Decision: shuffle resolves field metadata once per grouping expression,
  applies Go's EnumSetAsInt and decimal flen=0 adjustments, and appends directly
  with tidb_codec::append_hash_group_key_in_timezone into retained row buffers.
  Rationale: generic datum encoding is not Go's aggregate key encoding; reuse
  also avoids an allocated intermediate byte vector for every row/key part.
  Date/Author: 2026-09-21, Codex.

- Decision: transfer each shuffle job through a one-element startup channel
  only after std::thread::Builder::spawn succeeds. On failure, cancel/join the
  started prefix, then restore the failed job and unstarted suffix in order.
  Rationale: dropping a failed spawn closure must not drop an owned executor
  before Close can release its resources. This is native Rust error handling,
  not an additional SQL feature. Date/Author: 2026-09-21, Codex.

- Decision: normal and pipelined Window share typed function evaluation and
  frame comparison, but retain separate scheduling loops and aggregate-state
  lifetimes. Rationale: Go resets normal framed aggregate states between
  output chunks, whereas the pipeline preserves them across chunks and caches
  unchanged frame results. Pipelined CURRENT ROW bounds discover peers from
  retained rows; they must not require a whole-partition peer table.
  Date/Author: 2026-09-21 / Codex.

- Decision: retain `Arc<Chunk>` per contiguous input range and expose
  `FrameRows` to aggregate evaluators. Rationale: Go's pipelined release rule
  requires stable row indexes and the ability to drop old input chunks.
  Copying an entire partition cannot meet that rule. Typed aggregate readers
  still bind once per contiguous chunk range, rather than once per row.
  Ordinary execution now drops all frame references after materializing the
  partition's results and before returning mutable output aliases.
  Date/Author: 2026-09-21 / Codex.

- Decision: use `VecDeque<(usize, Datum)>` for the typed production MIN/MAX
  partial state. Rationale: it preserves Go's newest-equal eviction and
  constant-time front expiry while allowing statement collation and typed
  evaluation errors. The isolated `tidb-exec::minmax_deque` test helper is
  still seed evidence, not a production package-integration claim; do not
  introduce the reverse crate dependency. Date/Author: 2026-09-21 / Codex.

- Decision: numeric windows own their SUM/AVG state in
  `hash_agg/window_numeric.rs`, without changing ordinary grouped aggregates.
  Rationale: Go's decimal SUM retains a NULL counter and rounds the partial
  sum in place; both SUM and AVG add arriving rows before removing departing
  rows. Float inverse updates must obey the session precision switch.
  Date/Author: 2026-09-21 / Codex.

- Decision: implement sliding aggregate state before wiring the pipelined
  session control. Rationale: selecting the existing full-partition evaluator
  would not implement Go's pipelined behavior or its precision policy. The
  first prerequisite is COUNT; no new execution mode or completion claim is
  introduced. Date/Author: 2026-09-21 / Codex.

- Decision: continue the entire unfinished physicalop package against the
  freshly pulled Go checkout, retaining old evidence only as historical.
  Rationale: parity with current Go cannot exclude new package artifacts or
  count a successful focused regression as package completion.
  Date/Author: 2026-09-21 / Codex.

- Decision: use the complete pinned `physicalop` package as the next commit
  boundary, not a hand-picked MPP or task subset.
  Rationale: the dirty changes jointly implement physical nodes, task
  attachment, exchange, aggregation, pushdown, and serialization; none is a
  complete smaller Go package claim.
  Date/Author: 2026-09-01 / Codex.

- Decision: transcreate `OriginSchema` into the task and build the ordinary
  physical projection.
  Rationale: an executor-side trim or continued refusal would differ from Go's
  physical tree and cache semantics.
  Date/Author: 2026-09-01 / Codex.

- Decision: do not commit partial milestones from this plan.
  Rationale: repository policy and the user require whole-package parity as
  the atomic completion unit.
  Date/Author: 2026-09-01 / Codex.

## Outcomes & Retrospective

The 2026-09-21 typed-constant checkpoint corrects observable key bytes,
warnings and fatal-error order, and adds a measured stream-grouping optimization.
It does not close the complete checker, expression, types, aggregate or join
package claims. Deferred vectorization and scalar/correlated typed evaluation
still require source-oracle review; workload performance must be measured
separately from the isolated grouping benchmark.

The original shuffle scenario checkpoint adds the exact upstream range fixture
and executor-level combined failure ordering. It proves joining and reopening
under that ordering, but does not replace the original TestShuffleExit SQL
execution/failpoint harness. No production options or behavior were added.

The shuffle key checkpoint corrects captured byte and partition mismatches for
unsigned, enum and decimal values. It reuses the existing timezone-aware codec
implementation. SQL warning/error routing, all field-type variants, upstream
failure cases and performance measurements remain part of package acceptance.

The native shuffle startup-failure checkpoint now preserves executor ownership
and supports close/reopen at every launch position. It does not prove every
upstream failure branch or whole-package parity; no performance result follows
from this cleanup change. Startup adds one short-lived handoff channel per
thread, whose cost still needs workload measurement.

The package remains in progress. The 2026-09-21 work additionally corrects
IndexMergeReader generated-column binding and error traversal order, with
failing-before/passing-after regressions. No package-completion claim is valid
until the full inventory, integration, receipt, and Ready gate are complete.
The continuation restores OriginSchema conversion, enables the existing
executor test harness, and corrects the Window readiness failure it exposed.
Executor package acceptance is still not achieved: the pipelined implementation,
sliding aggregate processors, full original-test inventory and complete gates
remain pending.
No sysbench, TPC-C, TPC-H, or YCSB performance improvement has been measured in
this continuation. The COUNT evaluation regression demonstrates reduced work
for a moving frame, not a workload throughput result.

## Context and Orientation

The authoritative Go source for this audit is the exact checkout recorded
above. The complete package inventory is obtained with:

    git ls-tree -r --name-only be35d4c762f4a8252c059ab0eedc24b310270b8c pkg/planner/core/operator/physicalop

Reconcile the earlier pin before accepting any historical receipt:

    git diff e2788410d8d696605e8cb002585877a063ccc909 be35d4c762f4a8252c059ab0eedc24b310270b8c -- pkg/planner/core/operator/physicalop

The main Rust ownership surfaces are:

- `rust/crates/tidb-planner/src/physical/mod.rs` for the closed physical-plan
  tree and operator-specific enumeration;
- `rust/crates/tidb-planner/src/task.rs` for Root, coprocessor, and MPP tasks
  plus physical attachment;
- `rust/crates/tidb-planner/src/enforce.rs` for Sort and exchange enforcement;
- `rust/crates/tidb-planner/src/final_mode_agg.rs` for partial/final and MPP
  aggregation transformation;
- `rust/crates/tidb-planner/src/pushdown.rs` for store admission and TiPB
  lowering decisions;
- `rust/crates/tidb-executor/src/driver/physical_builder.rs` for ordinary
  executor construction from the physical tree;
- `rust/crates/tidb-exec/src/dag_request.rs` and
  `rust/crates/tidb-proto/proto/select.proto` for distributed request shape.

A coprocessor task is a physical subtree executed by TiKV or TiFlash. An MPP
task is a TiFlash parallel fragment with a partition contract. An enforcer is
a physical Sort or exchange inserted because a candidate does not already
satisfy the required property. A narrowing is an intentionally omitted Go
state or branch; narrowings are evidence of incompleteness for this plan.

## Plan of Work

Read each pinned Go artifact in package order. For every declared type,
function, method, generated clone branch, test, and build input, record the
owning Rust symbol or an explicit missing behavior. Validate representation
only through its consumers: an operator is not complete if it exists but is
not enumerated, attached, serialized, cache-rebuilt, or executable where Go
uses it.

For each mismatch, first add or identify a focused regression that expresses
the pinned Go behavior. Remove Rust-specific thresholds, policies, fallback
routes, and cache-only execution paths rather than preserving them beside the
Go path. Implement the Go behavior through the shared physical tree, extending
multiple Rust crates when necessary. Rerun the smallest WIP gate after each
coherent change.

After all mappings are implemented, create the package receipt with all 57
artifacts, integration decisions, exact tests, and residual representation-only
differences. Materialize the staged index over the then-current remote branch
in a clean temporary checkout and rerun the complete gate. Only then commit and
push the package.

## Concrete Steps

Run source inventory and comparison commands from repository root:

    git show be35d4c762f4a8252c059ab0eedc24b310270b8c:pkg/planner/core/operator/physicalop/<file.go>
    rg -n "TODO|REFUSED|unported|not ported|narrow" rust/crates/tidb-planner/src rust/crates/tidb-executor/src/driver/physical_builder.rs
    git diff --check

Use WIP validation while implementing:

    cd rust
    cargo fmt --all -- --check
    cargo test --locked -p tidb-planner <focused-test-filter> -- --nocapture
    cargo check --locked -p tidb-planner -p tidb-exec -p tidb-executor

Latest Shuffle integration check:

    cd rust
    cargo check --locked -p tidb-planner -p tidb-executor -p tidb-session
    cargo test --locked -p tidb-planner stream_agg_shuffle_matches_go_ndv_and_session_concurrency -- --nocapture

Latest ResolveIndices WIP checks:

    cd rust
    cargo check --locked -p tidb-planner
    cargo test --locked -p tidb-planner shuffle_resolves_by_items_against_each_owned_data_source -- --nocapture
    cargo test --locked -p tidb-planner hash_join_resolve_indices_matches_go_join_inputs_and_output_schema -- --nocapture

Latest Expand WIP checks:

    cd rust
    cargo check --locked -p tidb-executor -p tidb-planner
    cargo test --locked -p tidb-planner logical_expand_enumerates_the_pinned_go_task_matrix_and_level_exprs -- --nocapture
    cargo test --locked -p tidb-executor --test all cached_child_chunk_is_evaluated_once_per_expand_level -- --nocapture

Latest cost-path WIP checks:

    cd rust
    cargo check --locked -p tidb-planner
    cargo test --locked -p tidb-planner package_specific_ver2_operators_use_their_go_cost_bodies -- --nocapture
    cargo test --locked -p tidb-planner --lib plan_cost_ver2::golden_tests -- --nocapture

The following focused executor test was attempted but did not reach the test
because the dirty worktree's unrelated `kv_table.rs` test code does not compile
(`tidb_model::distance_metric` is not exported at that path):

    cargo test --locked -p tidb-executor physical_shuffle_builds_one_receiver_chain_per_worker -- --nocapture

The final Ready gate must also include the pinned Go package test, the two
mapped Go test families, all affected Rust integration tests, and repository
lint. The exact final list belongs in the receipt once the changed-path audit
is stable.

## Validation and Acceptance

Acceptance requires authoritative evidence for every artifact and behavior,
not merely compilation. The pinned Go package test must pass. Every Rust test
mapped from the two Go test files must run rather than remain ignored. Focused
regressions must cover every removed refusal or policy. Physical plans must be
constructible through ordinary planning and executor paths, not only by unit
test constructors. `cargo fmt --all -- --check`, required crate checks,
`make lint`, and `git diff --check` must pass on the isolated staged result.

No Ready result may be used to claim repository-wide planner parity; it proves
only this package boundary.

## Idempotence and Recovery

All source reads, formatting checks, and tests are safe to rerun. Preserve
unrelated dirty changes and stage mixed files hunk by hunk. Before committing,
clone the committed branch into an exact temporary directory, apply only the
staged diff, and validate there. If the shared remote advances, cherry-pick the
validated package commit onto its new head in a clean temporary checkout,
resolve only overlapping hunks with the package behavior authoritative, rerun
the focused gate, and push normally. Do not force-push or destructively reset
the user's dirty worktree.

## Artifacts and Notes

Current-baseline IndexMergeReader evidence (2026-09-21), from `rust/`:

    cargo test --offline --locked -j12 -p tidb-planner --lib index_merge_resolve_indices
    # Before production fix: 2 failed; after: 2 passed, zero ignored.
    cargo test --offline --locked -j12 -p tidb-planner --lib physical::
    # 61 passed; 0 failed; 0 ignored.
    cargo test --offline --locked -j12 -p tidb-executor --lib cached_physical_index_merge_builds_from_retained_partial_trees
    # 1 passed; 0 failed; 0 ignored.
    cargo fmt --all -- --check
    # Fails on pre-existing workspace formatting drift; no broad reformat applied.

Continuation checks, also from `rust/`:

    cargo test --offline --locked -j12 -p tidb-planner --lib origin_
    # Before fix: both failed; after fix: both passed.
    cargo test --offline --locked -j12 -p tidb-planner --lib task::
    # 69 passed, zero failures or ignores.
    cargo test --offline --locked -j12 -p tidb-executor --test all generated_column
    # Initially blocked by obsolete untracked test constructors; after adapting
    # those calls: 11 passed, one existing ignored DDL concurrency test.
    cargo test --offline --locked -j12 -p tidb-executor --lib virtual_dependency_expansion_preserves_reader_output
    # Passes, including root filters over virtual columns through both access paths.
    cargo test --offline --locked -j12 -p tidb-executor --test all physical_expand_source
    # One passed, no ignored tests.
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source
    # At this checkpoint, moving COUNT passed and readiness failed. The later
    # Window continuation below fixes that failure and adds two more regressions.

From repository root:

    make lint
    # Fails while installing revive, before linting starts.
    tools/bin/revive -version
    # version 1.2.1, the exact Makefile requirement.
    make -o tools/bin/revive lint
    # Passes the same lint recipes with that installed binary; skips only reinstall.
    git diff --check
    # Passes.

Only Rust sources changed; the pull also changed no Go, Bazel, or Go-module
inputs relative to the previous local HEAD, so `make bazel_prepare` is not
required for these WIP checks. Rust tests require no Go failpoint toggling.
Workload benchmarks and the complete Go/Rust package gates are still pending.

Current focused evidence:

    cd rust
    cargo test --locked -p tidb-planner need_extra_proj -- --nocapture
    # 2 passed; 0 failed

    cargo test --locked -p tidb-planner fragment::tests -- --nocapture
    # 2 passed; 0 failed; 0 ignored

    cargo test --locked -p tidb-planner 'a_union_scan_' -- --nocapture
    # 2 passed; 0 failed; 0 ignored

    cargo test --locked -p tidb-planner 'a_window_enumerates_' -- --nocapture
    # 1 passed; 0 failed; 0 ignored

The initial run failed to compile only because one new regression referenced a
helper private to a sibling test module. A local fixture fixed the test scope;
the production implementation was unchanged by that correction.

## Interfaces and Dependencies

`CopTask` retains `origin_schema: Option<tidb_expr::schema::Schema>`. When
`need_extra_proj` is true, conversion creates a `PhysicalProjection` whose
expressions are the origin schema's columns, whose schema is that origin
schema, whose stats match the reader, and whose child is the table,
index-lookup, or index-merge reader. As in Go, a pushed HashAgg or StreamAgg
suppresses this compatibility projection.

Other interfaces must be specified here as their audit decisions become
final. No new interface is acceptable solely for Rust convenience when pinned
Go has no equivalent behavior.

## Dependent whole-package unit: executor/windows

The current Go package has exactly six artifacts at the recorded checkout:
`pkg/executor/windows/BUILD.bazel`, `builder.go`, `window.go`,
`pipelined_window.go`, `window_executor_test.go`, and `window_sql_test.go`.
There is no package-level `doc.go`, generated input, platform variant, or
fixture directory. Confirm this inventory with:

    git ls-tree -r --name-only be35d4c762f4a8252c059ab0eedc24b310270b8c pkg/executor/windows

Its production ownership is `rust/crates/tidb-executor/src/window.rs` and
`src/driver/physical_builder.rs`, with aggregate state in `src/hash_agg.rs`
and session controls owned by `tidb-session`. The normal Go WindowExec and
PipelinedWindowExec are distinct algorithms; success for the normal executor
cannot stand in for the pipelined branch. `BuildOrdered` must retain its forced
pipelined behavior. `Build` must select the session-requested implementation,
and the precision setting must select the same floating-point sliding policy.

The current normal-executor milestone uses a `VecDeque<WindowResult>` where
each entry owns a chunk and its count of unfinished rows, corresponding to
Go's `resultChunks` and `remainingRowsInChunk`. Child chunks are replaced,
never reset while their output columns are aliased. `window/rows.rs` retains
owned child-chunk ranges for frame evaluation without copying partition data.
The normal executor materializes all results for a partition and releases its
frame ranges before returning output aliases. Prefix expiry preserves logical
row indexes for the future pipelined executor. Complete original-test mapping
must account for every test in both Go test files, including the ordered
builder, pipelined/concurrency matrix, data-reference reuse, sliding windows,
precision cases, and variance. The existing 48 Rust session tests are useful
coverage, not a substitute for that mapping.

Window continuation evidence, from `rust/`:

    cargo test --offline --locked -j12 -p tidb-executor --test all completed_output_chunk_precedes_later_child_error
    # Failed on the pre-fix full-child drain.
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source
    # After fix: four passed, zero failures/ignores.
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    # 48 passed, zero failures/ignores.

These are WIP gates. No window-package receipt, benchmark improvement, or
package integration commit is justified until the complete six-artifact unit
and all required validation are finished.

Sliding COUNT continuation: `hash_agg::WindowAggState` replaces the stateless
`AggFunc::window_frame_value` helper. Ordinary Window owns these states for one
partition and resets them at Go's `appendResult2Chunk` boundary. COUNT uses the
existing typed evaluator for departing and entering rows; unsupported sliding
aggregates still reset and fold the complete frame. The historical function
receipt referencing `window_frame_value` is not current package acceptance.
The correctness/evaluation-count checks are:

    cargo test --offline --locked -j12 -p tidb-executor --test all moving_count_evaluates_only_entering_and_leaving_rows
    # Before implementation: failed, actual 955 vs expected 190 evaluations.
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source
    # After implementation: six passed, no failures/ignores.
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    # After COUNT state changes: 48 passed, no failures/ignores.

From the repository root, the continuation also ran:

    make lint
    # Failed at revive v1.2.1 reinstall: module does not contain package.
    make -o tools/bin/revive lint
    # Passed using the existing required-version revive binary.
    rustfmt --edition 2024 --check rust/crates/tidb-executor/src/window.rs rust/crates/tidb-executor/tests/window_executor_source.rs
    git diff --check
    # Both passed. No whole-workspace formatting churn was applied.

Logs: `/tmp/tidb-window-count-before.log`,
`/tmp/tidb-window-count-after.log`, `/tmp/tidb-session-window-count.log`,
`/tmp/tidb-window-count-lint.log`, and
`/tmp/tidb-window-count-lint-installed.log`. This step changed
`hash_agg.rs`, `window.rs`, `tests/window_executor_source.rs`, and this plan.
The Go package tests, full-package acceptance gates, and the four workload
benchmarks were not run in this step. Floating-point sliding/precision and
pipelined execution remain compatibility risks in the unfinished package.

The remaining sliding implementations must follow their own Go update orders,
NULL counters, precision policy, and min/max deque behavior; COUNT's inverse
update must not be generalized to them without that source audit.

Numeric sliding continuation (2026-09-21): SUM/AVG now use their own state in
`rust/crates/tidb-executor/src/hash_agg/window_numeric.rs`. Decimal arithmetic
uses bounded `add_mysql`/`sub_mysql` and preserves source overflow/truncation
errors. SUM rounds the retained decimal sum; AVG divides with the statement's
division precision and rounds only its output. Float SUM/AVG refold when high
precision is ON and slide when OFF. The existing system-variable definition
is unchanged; its typed statement snapshot is now carried through
`tidb-session/src/stmt_ctx.rs`, `tidb-executor/src/stmt_context.rs`, and
`tidb-expr/src/context.rs`. Both statement-context construction branches set
the flag. The SQL test toggles ON/OFF/ON to check snapshot invalidation.

Additional changed files are `hash_agg.rs`, `window.rs`,
`tidb-executor/tests/window_executor_source.rs`, and
`tidb-session/src/tests_window/aggregates.rs`. The executor matrix now includes
SUM and AVG with an integer-arithmetic result oracle across five frames, four
chunk sizes, NULL inputs, two partitions, and reopen. A direct numeric test
checks 190 evaluations rather than 955 for a 100-row/10-row moving frame.
The maximum-width overflow fixture uses SUM: AVG can report truncation while
dividing that first full-width input, before the sliding transition of interest.

Validation commands from `rust/`:

    cargo test --offline --locked -j12 -p tidb-session --lib window_real_sum_avg_honor_precision_setting
    # Failed before production changes: OFF incorrectly returned 2 and 1.
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    # 49 passed, no failures/ignores.
    cargo test --offline --locked -j12 -p tidb-executor --lib window_numeric
    # Four passed: NULL/empty transitions, operation count, retained rounding, overflow order.
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source
    # Six passed, including the extended COUNT/SUM/AVG boundary matrix.

Additional root checks passed:

    rustfmt --edition 2024 --check rust/crates/tidb-executor/src/hash_agg/window_numeric.rs rust/crates/tidb-executor/src/window.rs rust/crates/tidb-executor/tests/window_executor_source.rs rust/crates/tidb-session/src/tests_window/aggregates.rs
    git diff --check

Logs are `/tmp/tidb-window-precision-before.log`,
`/tmp/tidb-window-numeric-after.log`, `/tmp/tidb-window-numeric-unit.log`,
and `/tmp/tidb-window-numeric-executor.log`. Root `make lint` still fails at
the revive reinstall; `make -o tools/bin/revive lint` passes with the installed
required-version binary (`/tmp/tidb-window-numeric-lint-installed.log`). Go
package tests and workload benchmarks have not run in this continuation.
The next work still includes min/max and bit-XOR sliding state, true
pipelined/ordered execution, full original-test mapping, and package gates.

Extrema/XOR continuation (2026-09-21): the MIN/MAX and BIT_XOR prerequisite
above is now implemented. `hash_agg/window_extremum.rs` owns the typed deque;
it enqueues arrivals before expiring earlier indices, evicts older equal
values, preserves Go's NaN and signed-zero ordering, and rounds a copied
decimal result without mutating its deque input. FLOAT results retain the
Float32 datum representation required by the chunk's four-byte column.
ENUM, SET, JSON, and vector MIN/MAX remain on Go's non-sliding branch.
BIT_XOR uses the existing evaluator, visiting departures before arrivals.

The six-test executor matrix now also verifies MIN/MAX and BIT_XOR across
five frame shapes, four chunk sizes, NULLs, two partitions, and reopen.
Direct state tests check disjoint/empty frames, NaNs, signed zero, and
evaluation counts: 100 for each extremum and 190 for XOR over the 100-row,
10-row frame, rather than 955 full-frame evaluations. Typed SQL regression
coverage includes all eight Go sliding MIN/MAX evaluator classes, plus BIT's
string evaluator. BIT's HEX expectation follows Go `hexFunctionClass`'s
ETInt path, so its values are `2`/`1`, not padded byte hex `02`/`01`.

This continuation changed `hash_agg.rs`, the new `hash_agg/window_extremum.rs`,
`window.rs`, `tests/window_executor_source.rs`, the session window
`aggregates.rs`/`collation.rs` tests, and this plan. Commands from `rust/`:

    cargo test --offline --locked -j12 -p tidb-session --lib window_min_max_keep_latest_collation_equal_value
    # Before production changes: failed, older A returned instead of a.
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    # Final: 51 passed, no failures/ignores.
    cargo test --offline --locked -j12 -p tidb-executor --lib window_extremum
    # Three passed before the FLOAT representation correction.
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg::window
    # Final combined sliding-state check: seven passed, no failures/ignores.
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source
    # Six passed with MIN/MAX/BIT_XOR added to the boundary matrix.

From root, `make lint` again fails at the revive reinstall, while
`make -o tools/bin/revive lint` passes. `git diff --check` and this scoped
format check pass:

    rustfmt --edition 2024 --check rust/crates/tidb-executor/src/hash_agg/window_extremum.rs rust/crates/tidb-executor/tests/window_executor_source.rs rust/crates/tidb-session/src/tests_window/aggregates.rs rust/crates/tidb-session/src/tests_window/collation.rs

Logs are `/tmp/tidb-window-extremum-before.log`,
`/tmp/tidb-window-extremum-after.log`, `/tmp/tidb-window-extremum-unit.log`,
`/tmp/tidb-window-extremum-executor.log`, and
`/tmp/tidb-window-extremum-lint-installed.log`. No Go package gate or workload
benchmark has run. Full aggfuncs integration still needs an audit, including
the internal `sumInt`/`sumUint` sliding implementations in `func_sum_int.go`,
which have no explicit AggKind counterpart. Do not interpret the implemented
common sliding states as full aggfuncs or windows package parity. Next:
implement the actual pipelined/ordered Window state machine and its selection,
then finish the complete original-test and dependent-package mapping.

Retained-row continuation (2026-09-21): `window/rows.rs` replaces the normal
executor's owned partition copy with a deque of input chunk ranges. Each
contiguous range keeps one Arc owner, and binary search resolves absolute row
indexes across ranges. Prefix expiry releases complete chunks and trims a
partially retained range without moving its row data or renumbering live rows.
The buffer honors a child chunk's selection vector. `FrameRows::visit_chunks`
lets COUNT/XOR and non-sliding folds bind typed readers once per contiguous
input range; numeric and extremum states access the same retained rows.

The ordinary executor releases its frame ranges after producing all results
for a partition. It still waits for complete partitions; this change alone
does not implement the pipelined state machine or its input-dropping policy.
The source-owned queue/result readiness behavior remains unchanged. Tests
cover resetting returned output aliases and continuing into partitions sharing
the same child chunk. Dedicated ownership tests use weak chunk references to
prove release and run COUNT/SUM/MIN/MAX while repeatedly dropping old prefixes.
No unsafe row references or partition data copies were introduced.

Files changed in this step: `tidb-executor/src/window.rs`, new
`tidb-executor/src/window/rows.rs`, `tidb-executor/src/hash_agg.rs`,
`tidb-executor/src/hash_agg/window_numeric.rs`,
`tidb-executor/src/hash_agg/window_extremum.rs`, and this plan. Commands from
`rust/`:

    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source
    # Six passed, no failures/ignores.
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    # 21 selected tests passed, including numeric/extremum and ownership checks.
    cargo test --offline --locked -j12 -p tidb-executor --lib window::rows
    # Three passed after adding sliding updates across expired prefixes.
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    # 51 passed, no failures/ignores.

Root `make lint` failed at the revive reinstall, and
`make -o tools/bin/revive lint` passed with the installed required-version
binary. Scope formatting and whitespace checks:

    rustfmt --edition 2024 --check rust/crates/tidb-executor/src/window.rs rust/crates/tidb-executor/src/window/rows.rs rust/crates/tidb-executor/src/hash_agg/window_numeric.rs rust/crates/tidb-executor/src/hash_agg/window_extremum.rs
    git diff --check

Logs: `/tmp/tidb-window-retained-executor.log`,
`/tmp/tidb-window-retained-unit.log`, `/tmp/tidb-window-retained-session.log`,
`/tmp/tidb-window-retained-expiry.log`, and
`/tmp/tidb-window-retained-lint-installed.log`. The whole-package Go/Rust gates
and workload benchmarks remain unverified. The next implementation must use
this retained-row buffer to reproduce Go's `accumulated <= dropped` output
release test, strict start/end lookahead, partition transitions, and forced
ordered-window selection. Do not select this normal executor under the
pipelined setting and count that as a pipelined implementation.

Pipelined implementation continuation (2026-09-21):
`window/pipelined.rs` now implements the scheduler described above. Input
groups are staged separately from the active partition, allowing the previous
partition to finish before a staged group is consumed. The output queue
retains cumulative input-row ends. A result can be returned only when all its
values exist and its cumulative input end is no greater than the dropped-row
counter. Prefix expiry occurs after producing into one output chunk, using
Go's minimum of current position and the previous frame bounds. Empty-frame
transitions reset partial results; unchanged frames reuse their partial
results. Rank/percent-rank/cume-dist prepare full-partition peer geometry only
when the whole partition is available, while streaming RANGE CURRENT ROW uses
the comparison cursor over incoming rows.

The default session path now selects PipelinedWindowExec when
`tidb_enable_pipelined_window_function` is ON and ordinary WindowExec when OFF.
The same snapshot reaches PlanBuilder's ROW_NUMBER default-frame rule, in all
four planner bridges. OrderedWindowExec wraps the pipelined executor and
ignores the ordinary session choice, matching Go BuildOrdered. A repository
source search finds Go BuildOrdered called only by its package test at this
checkout; no additional production ordered-plan dispatch is implied.

This step changed `tidb-executor/src/window.rs`, new
`tidb-executor/src/window/pipelined.rs`,
`tidb-executor/src/driver/physical_builder.rs`,
`tidb-executor/src/driver/planner_bridge.rs`,
`tidb-executor/src/stmt_context.rs`, `tidb-session/src/stmt_ctx.rs`,
`tidb-executor/tests/window_executor_source.rs`,
`tidb-session/src/tests_window/aggregates.rs`, and this plan. The executor
frame/chunk/partition/reopen matrices now exercise both scheduling modes.
The SQL switch test runs 150 rows through 32-row chunks, repeatedly changes
OFF/ON/OFF/ON, and covers ROWS, RANGE, ranking, position, and value functions.
The unit retention test runs 1,000 rows twice, returns before EOF, bounds live
input rows to 26 for a ten-row frame with eight-row child chunks, bounds queued
results to four chunks, and resets every returned output chunk.

Commands from `rust/`:

    cargo test --offline --locked -j12 -p tidb-executor --test all pipelined_ready_chunk_precedes_later_same_partition_error
    # Before implementation, normal Window failed with the later child error.
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    # Final: 52 passed, no failures/ignores. Initial RANGE peer-table failures fixed.
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    # 23 selected tests passed, including bounded input retention and reopen.
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source
    # Final: seven passed, including normal/pipelined matrices and forced ordered execution.

Root `make lint` still fails at revive reinstall. The identical recipes using
the installed required-version binary passed with
`make -o tools/bin/revive lint`. Scoped formatting and whitespace checks passed:

    rustfmt --edition 2024 --check rust/crates/tidb-executor/src/window.rs rust/crates/tidb-executor/src/window/pipelined.rs rust/crates/tidb-executor/tests/window_executor_source.rs rust/crates/tidb-session/src/tests_window/aggregates.rs
    git diff --check

Evidence logs: `/tmp/tidb-pipelined-before.log`,
`/tmp/tidb-pipelined-matrix.log`, `/tmp/tidb-pipelined-session.log`,
`/tmp/tidb-pipelined-unit.log`, and `/tmp/tidb-pipelined-lint-installed.log`.
These tests validate the new implementation, not whole-package parity. Next,
map and run every original Go window test/support artifact, audit evaluator
error/warning ordering and frame-less processors, audit the dependent aggfuncs
surface (including internal integer SUM), and complete required package and
workload validation. No throughput improvement or package-completion receipt
has been claimed.

Revision note (2026-09-01): created after the complete physicalop package was
selected as the next atomic parity boundary and the first confirmed task gap
was fixed.

Revision note (2026-09-21): refreshed the Go baseline after the requested pull,
recorded the inventory delta, and added verified IndexMergeReader resolution
progress. The package and overall parity/performance goal remain incomplete.

Revision note (2026-09-21, Window continuation): corrected normal Window chunk
readiness and output-index mapping, recorded the complete dependent-package
inventory, and verified four executor plus 48 session window tests. Pipelined
and sliding semantics and full-package acceptance remain open. Verified lint
using the installed required-version tool while retaining the plain-command
bootstrap failure as a tooling limitation.


### Window original-test coverage and ignored-frame notes (2026-09-21)

The complete six-artifact Go windows package remains the atomic claim unit;
this update adds source-test evidence and fixes one discovered integration
mismatch. It does not accept the package or the workload-performance goal.

`rust/scripts/generate-go-window-tests.py` extracts 198 literal actions from
five helpers in `pkg/executor/windows/window_sql_test.go`, recording source
line numbers and the source SHA-256 in generated `upstream.json`. It rejects
unrecognized MustExec/MustQuery, direct chunk-size, and nullability actions.
`--check` checks reproducibility without rewriting the fixture. Deferred
window-enable cleanup runs at helper exit, not at its lexical position.
The Rust runner preserves Go's `<nil>` rendering and sorts before comparing
only where the Go test calls Sort. Direct one/two-row MaxChunkSize assignments
use the existing internal session-variable restore path, bypassing the SQL
minimum exactly as the original test does; the runner checks the resulting
executor context's actual maximum.

Original-test mapping:

| Go test/support function | Rust evidence |
| --- | --- |
| TestWindowFunctions / doTestWindowFunctions | upstream_window_functions: all literal actions, pipelined 0/1 × concurrency 1/4 |
| TestWindowFunctionsDataReference | upstream_window_data_reference: all actions, max chunk 2, both execution modes |
| TestSlidingWindowFunctions / baseTestSlidingWindowFunctions | upstream_sliding_window_functions: all actions, pipelined 0/1 × FLOAT/DOUBLE × precision ON/OFF, including prepared reversed extents |
| TestIssue45964And46050 / testReturnColumnNullableAttribute | upstream_window_nullable_and_empty_input: all 25 function nullability assertions |
| TestVarSampAsAWindowFunction | upstream_window_nullable_and_empty_input: both empty-input statements |
| TestWindowExecutorsBasic | upstream_window_executor_basic_and_nullable: both exact SQL/results, pipelined 0/1 |
| TestWindowReturnColumnNullableAttribute | upstream_window_executor_basic_and_nullable: original four-row fixture and five flag assertions |
| TestBuildOrderedWindowExec / physicalWindowForTest | upstream_ordered_window_partitioned_row_number: same schema, partition/order, current-row frame, input/output, explicit ordered type and disabled session pipelining |

The imported SHOW WARNINGS assertion failed before the fix: no Note 3599 was
reported for ROW_NUMBER with an explicit ignored frame. PlanBuilder now emits
that source-defined note through Columns::append_note and StmtContext's
existing leveled warning collector. Named windows preserve their declared
name; function spelling is lower-case as in Go. Additional SQL checks cover
two functions sharing a mixed-case named window, note order, and absence of
notes for implicit frames or frame-sensitive functions. The seeded RAND
FIRST_VALUE/LAST_VALUE source assertion already passed before implementation:
its arguments are materialized by the planner; no speculative runtime change
was made for that case.

Files changed in this step: expr/context.rs, executor/stmt_context.rs,
planner/plan_builder/window.rs, session/tests_window.rs,
session/tests_window/specs.rs, new session/tests_window/upstream.rs and
upstream.json, executor/tests/window_executor_source.rs, the new generator,
and this living plan. All paths above are beneath rust/crates unless stated.
Rust-only changes do not trigger bazel_prepare or Go failpoint setup.

Validation commands from rust/:

    cargo test --offline --locked -j12 -p tidb-session --lib tests_window::upstream
    # Before: SHOW WARNINGS mismatch. After: all four original SQL test groups passed.
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    # Final: 58 passed, no failures/ignores.
    cargo test --offline --locked -j12 -p tidb-planner --lib plan_builder::window_tests
    # 51 passed, no failures/ignores.
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source
    # Eight passed, including the exact original ordered-window example.

Repository-root validation:

    python3 rust/scripts/generate-go-window-tests.py --check
    git diff --check
    make lint
    # Failed at revive v1.2.1 installation, before lint recipes.
    make -o tools/bin/revive lint
    # Passed actual lint recipes using the already installed required version.

Logs: /tmp/tidb-window-upstream.log (red SHOW WARNINGS case),
/tmp/tidb-window-upstream-after.log, /tmp/tidb-window-upstream-session.log,
/tmp/tidb-window-upstream-planner.log, /tmp/tidb-window-upstream-executor.log,
/tmp/tidb-window-upstream-lint.log, and
/tmp/tidb-window-upstream-lint-installed.log.

Remaining gates: this mapping runs the original assertions in Rust, not the
original Go test binaries. Concurrency variable matrices establish result
compatibility, not equivalent parallel scheduling. Go package/build/support
validation, the full dependent aggfuncs audit, frame-less processor evaluation
and error ordering, memory-accounting integration, and sysbench/TPC-C/TPC-H/
YCSB benchmark receipts remain open. No performance gain is claimed here.


### Frame-less partition evaluation (2026-09-21)

Go builder.go selects aggWindowProcessor when PhysicalWindow.Frame is nil;
that processor consumes a partition once and repeats its partial results
across result chunks. Rust previously converted nil to an explicit unbounded
frame, losing this distinction. Non-sliding aggregates then refolded the same
partition for each output row, and frame value functions reevaluated their
selected argument. Sliding aggregate state also restarted at chunk boundaries.

WindowExec now accepts an optional frame (existing explicit-frame callers
remain supported). Physical construction preserves a missing frame, and the
ordinary executor retains aggregate/value results until the partition ends.
Position-dependent functions continue advancing per row. Explicit frame
processors retain their existing chunk-reset/evaluation rules; the pipelined
executor continues using its existing unchanged-frame result retention.

The new partition_aggregates_are_evaluated_once_across_chunks regression uses
three four-row partitions crossing three-row child chunks, BIT_OR and
FIRST_VALUE instrumented arguments, and two open/drain/close cycles. Before
the fix, the no-frame case evaluated its arguments 60 times rather than Go's
15. Afterward it evaluates exactly 15 times per execution; the explicit-frame
control still evaluates 60 times and all output values/chunk sizes agree.
This is direct operation-count evidence, not a workload throughput claim.

Changed this step: rust/crates/tidb-executor/src/window.rs,
rust/crates/tidb-executor/src/driver/physical_builder.rs,
rust/crates/tidb-executor/tests/window_executor_source.rs, and this plan.
Validation from rust/:

    cargo test --offline --locked -j12 -p tidb-executor --test all partition_aggregates_are_evaluated_once_across_chunks
    # Red: 60 actual evaluations versus 15 required.
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source
    # Green: nine passed, including the explicit-frame control and reopen.
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    # 58 passed, including the complete imported Go SQL action sequences.
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    # 23 selected tests passed.

Repository-root checks:

    rustfmt --edition 2024 --check rust/crates/tidb-executor/src/window.rs rust/crates/tidb-executor/tests/window_executor_source.rs
    git diff --check
    make lint
    # Still fails installing revive v1.2.1 before lint.
    make -o tools/bin/revive lint
    # Actual lint recipes pass with installed required-version revive.

Logs: /tmp/tidb-window-partition-before.log,
/tmp/tidb-window-partition-after.log, /tmp/tidb-window-partition-session.log,
/tmp/tidb-window-partition-unit.log, /tmp/tidb-window-partition-lint.log,
and /tmp/tidb-window-partition-lint-installed.log.

This closes repeated partition aggregate/value evaluation, not all
frame-less processor semantics: cross-function UpdatePartialResult versus
AppendFinalResult error/warning ordering and LEAD/LAG argument evaluation
still need source auditing. All previously recorded whole-package and
benchmark acceptance gates remain open; no atomic package receipt is issued.


### Partition update/result ordering (2026-09-21)

Source audit of aggfuncs/func_lead_lag.go confirms that UpdatePartialResult
only retains rows; LEAD/LAG evaluates its selected argument/default during
AppendFinalResult2Chunk. Ordinary aggWindowProcessor updates every function
before appending any result, whereas PipelinedWindowExec.produce updates and
appends one function at a time. Rust previously evaluated the first LEAD result
before a later FIRST_VALUE or aggregate had consumed the partition.

The trace regression failed before the fix: ordinary execution produced
argument calls [0,1,2,2,0] instead of source order [1,2,2,0,0]. Aggregate window
states now expose separate update_frame and finish operations, including
numeric and deque extrema states. Frame-less ordinary execution consumes all
aggregate/value inputs before emitting any result. Aggregate finalization
runs at each output as in Go, so the prior step's whole-result aggregate cache
is superseded; retained partial states still eliminate repeated input scans.
Value functions retain their selected value, while LEAD/LAG remains lazy at
result append. Explicit-frame and pipelined call paths still combine update
and finish per function, preserving their source ordering.

A second regression verifies error precedence: a maximum-width decimal AVG
can consume its input but fail while dividing at finalization; a later
FIRST_VALUE argument also fails. Ordinary execution reports the later update
error first, and pipelined execution reports the earlier AVG finalization
error first. Both return zero output rows on error. The earlier operation-count
regression still passes (15 input evaluations for frame-less processing versus
60 for its explicit-frame control, including reopen and chunk boundaries).

Files changed this step: rust/crates/tidb-executor/src/window.rs,
rust/crates/tidb-executor/src/hash_agg.rs,
rust/crates/tidb-executor/src/hash_agg/window_numeric.rs,
rust/crates/tidb-executor/src/hash_agg/window_extremum.rs,
rust/crates/tidb-executor/tests/window_executor_source.rs, and this plan.
Rust-only scope still does not require bazel_prepare or failpoint setup.

Validation from rust/:

    cargo test --offline --locked -j12 -p tidb-executor --test all partition_update_precedes_lead_lag_result_evaluation
    # Red: wrong ordinary argument evaluation order.
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source
    # Final: 11 passed, including both scheduling modes and error precedence.
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    # 23 selected tests passed, including numeric/sliding state coverage.
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    # 58 passed, including original Go SQL sequences and nullable/metadata checks.

Repository-root validation:

    rustfmt --edition 2024 --check rust/crates/tidb-executor/src/window.rs rust/crates/tidb-executor/src/hash_agg/window_numeric.rs rust/crates/tidb-executor/src/hash_agg/window_extremum.rs rust/crates/tidb-executor/tests/window_executor_source.rs
    git diff --check
    make lint
    # Same revive v1.2.1 installation failure, before actual lint recipes.
    make -o tools/bin/revive lint
    # Passed actual recipes with installed required-version revive.

Evidence: /tmp/tidb-window-update-order-before.log,
/tmp/tidb-window-update-order-after.log, /tmp/tidb-window-update-order-unit.log,
/tmp/tidb-window-update-order-session.log, /tmp/tidb-window-update-order-lint.log,
and /tmp/tidb-window-update-order-lint-installed.log.

The source audit and regressions close the identified update/finalization
ordering defect. Whole-package acceptance remains open: original Go binaries
and build artifacts have not been validated; eager peer comparison, extreme
unsigned frame/LEAD offset arithmetic, dependent aggfuncs behavior and memory
accounting still need auditing. No workload benchmark was run, and no package
completion or sysbench/TPC-C/TPC-H/YCSB performance claim is made.


### Original Go package validation and LEAD unsigned offsets (2026-09-21)

Re-inventoried pkg/executor/windows: exactly BUILD.bazel, builder.go,
pipelined_window.go, window.go, window_executor_test.go, and window_sql_test.go.
The package has eight top-level tests. All eight original tests now pass on
darwin/arm64 with the cached Go 1.26.0 toolchain, using intest,deadlock tags
and count=1. This supersedes earlier entries saying that no original Go test
binary had run. No package-local platform/generated source variants exist.
This validates the original Go package through go test; it is not a Bazel
execution, Linux/race validation, or a whole Rust-package acceptance receipt.

Applied the tidb-failpoint-test-runner skill and testing-flow decision checks:
no failpoint., testfailpoint., or failpoint BUILD dependency occurs in the
windows package. No failpoint enable/disable was needed. Changes remain
Rust-only, so no bazel_prepare trigger applies. The first Go invocation was
blocked by sandbox access to the shared build cache; the authorized escalation
resolved that. The installed Go 1.27 then failed because pkg/util/hack has
checkMapABI implementations restricted to Go 1.25/1.26. Reusing the cached
Go 1.26.0 toolchain fixed the build without changing Go source or dependencies.

Exact successful command from pkg/executor/windows/:

    GOTOOLCHAIN=go1.26.0 go test -run '^(TestWindowFunctions|TestWindowFunctionsDataReference|TestSlidingWindowFunctions|TestIssue45964And46050|TestVarSampAsAWindowFunction|TestWindowExecutorsBasic|TestBuildOrderedWindowExec|TestWindowReturnColumnNullableAttribute)$' -tags=intest,deadlock -count=1

Log: /tmp/tidb-go-windows-package-go126.log (PASS, package time 4.064s after
compilation). The initial Go 1.27 failure is in
/tmp/tidb-go-windows-package.log.

Source inspection found that GetUint64FromConstant accepts the complete
unsigned range for LEAD/LAG offsets. Go lead.AppendFinalResult2Chunk adds
uint64 curIdx and offset before checking partition bounds. Rust used a checked
native-index addition, yielding defaults on overflow. The new SQL regression
failed before the fix for UINT64_MAX and UINT64_MAX-1. LEAD now explicitly
wraps in u64 and converts to usize only afterward; LAG retains checked
subtraction. There is no unchecked Rust indexing or signed overflow.

The expectation was also verified against the real Go SQL executor with a
temporary test outside the repository. For rows (id,v)=(1,10),(2,20),(3,30),
the query below returns (-1,-1,-1), (10,-1,-1), (20,10,-1) in both ordinary
and pipelined modes:

    SELECT LEAD(v,18446744073709551615,-1) OVER (ORDER BY id),
           LEAD(v,18446744073709551614,-1) OVER (ORDER BY id),
           LAG(v,18446744073709551615,-1) OVER (ORDER BY id) FROM t

Oracle command from repository root:

    GOTOOLCHAIN=go1.26.0 go test -tags=intest,deadlock -run '^TestLeadUnsignedOffsetOracle$' -count=1 /tmp/tidb_window_offset_oracle_test.go

It passed; output is /tmp/tidb-go-window-offset-oracle.log. No temporary Go
file was added to the worktree. Changed files in this step are
rust/crates/tidb-executor/src/window.rs,
rust/crates/tidb-session/src/tests_window/value_functions.rs, and this plan.

Rust validation from rust/:

    cargo test --offline --locked -j12 -p tidb-session --lib window_lead_unsigned_offset_wraps_before_partition_check
    # Red before fix: all defaults, including wrapped in-range targets.
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    # Final: 59 passed, no failures/ignores.
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source
    # Final: 11 passed.

Repository-root checks:

    rustfmt --edition 2024 --check rust/crates/tidb-executor/src/window.rs rust/crates/tidb-session/src/tests_window/value_functions.rs
    git diff --check
    make lint
    # Still fails in revive reinstall before actual lint.
    make -o tools/bin/revive lint
    # Actual lint recipes passed using installed required-version tool.

Logs: /tmp/tidb-window-offset-before.log, /tmp/tidb-window-offset-after.log,
/tmp/tidb-window-offset-executor.log, /tmp/tidb-window-offset-lint.log, and
/tmp/tidb-window-offset-lint-installed.log.

Remaining audit includes extreme unsigned ROWS frame arithmetic (ordinary and
pipelined source formulas differ), eager peer comparisons, dependent aggfuncs
and memory accounting, concurrency integration, and workload benchmarking.
The original Go test gate is now verified locally, while whole-package parity
and sysbench/TPC-C/TPC-H/YCSB performance remain unproven.


### Unsigned ROWS bounds and released-input errors (2026-09-21)

A live Go oracle confirmed that ordinary rowFrameWindowProcessor and
PipelinedWindowExec differ at UINT64_MAX FOLLOWING: ordinary execution adds
the offset, clamps to partition size, then adds the exclusive-end one;
pipelined execution adds both in uint64 before clamping. Rust saturated these
operations, producing different results. Both paths now perform the source
uint64 arithmetic, with explicit wrapping, and apply their own clamping order.
Conversion to native indexes follows the partition-size clamp. Reversed frame
bounds stay reversed so pipelined release accounting sees the original end.

For input (id,v)=(1,10),(2,20),(3,30), the oracle captured FIRST_VALUE(v) OVER
(ORDER BY id ROWS BETWEEN <bounds>):

| Bounds | Ordinary | Pipelined |
| --- | --- | --- |
| CURRENT ROW AND 18446744073709551615 FOLLOWING | 10,NULL,NULL | NULL,NULL,NULL |
| 18446744073709551615 FOLLOWING AND UNBOUNDED FOLLOWING | NULL,10,20 | NULL,10,20 |
| 18446744073709551615 PRECEDING AND CURRENT ROW | 10,10,10 | 10,10,10 |

The Rust regression failed before the arithmetic fix (first row-frame case
returned 10,20,30). The full six-case SQL matrix now passes. An additional Go
oracle run with MaxChunkSize=1 showed that the wrapped following start can
reference a released row: Go recovers a slice-bounds panic as the query error
`runtime error: slice bounds out of range [18446744073709551615:2]`.
The corresponding Rust regression initially panicked inside WindowRows.
Pipelined production now checks that range before indexing released storage
and returns the source-shaped SQL 1105 error explicitly. No unsafe indexing
or Rust panic is needed to preserve this observed behavior.

Temporary oracle source: /tmp/tidb_window_frame_oracle_test.go (not added to
the repository). Exact command from repository root, with authorized access
to the shared build cache and the source-compatible cached Go toolchain:

    GOTOOLCHAIN=go1.26.0 go test -tags=intest,deadlock -run '^TestUnsignedRowsFrameOracle$' -v -count=1 /tmp/tidb_window_frame_oracle_test.go

Both oracle iterations passed. Logs:
/tmp/tidb-go-window-frame-oracle.log (six value results) and
/tmp/tidb-go-window-frame-oracle-small.log (also the released-input error).
No Go source/build/dependency files changed, and the prior package-local
failpoint decision remains applicable.

Files changed in this step: rust/crates/tidb-executor/src/window.rs,
rust/crates/tidb-executor/src/window/pipelined.rs,
rust/crates/tidb-session/src/tests_window/frames.rs, and this plan.
Validation from rust/:

    cargo test --offline --locked -j12 -p tidb-session --lib window_unsigned_rows_bounds_match_go_execution_mode
    # Red before fix: incorrect saturating behavior.
    cargo test --offline --locked -j12 -p tidb-session --lib window_wrapped_frame_released_input_returns_go_error
    # Red before guard: panic indexing already released input.
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    # Final: 61 passed, no failures/ignores.
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    # 23 selected tests passed, including bounded retention and sliding states.
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source
    # Final: 11 passed.

Repository-root checks:

    rustfmt --edition 2024 --check rust/crates/tidb-executor/src/window.rs rust/crates/tidb-executor/src/window/pipelined.rs rust/crates/tidb-session/src/tests_window/frames.rs
    git diff --check
    make lint
    # Same revive-install failure before lint recipes.
    make -o tools/bin/revive lint
    # Actual lint recipes passed using installed required-version tool.

Rust evidence logs: /tmp/tidb-window-rows-before.log,
/tmp/tidb-window-released-before.log, /tmp/tidb-window-rows-final.log,
/tmp/tidb-window-rows-unit.log, /tmp/tidb-window-rows-executor.log,
/tmp/tidb-window-rows-lint.log, and /tmp/tidb-window-rows-lint-installed.log.

These tests cover the captured boundary cases, not every sliding aggregate's
behavior when unsigned wrap makes frames move backward. That remains part of
the whole-package audit, together with eager peer comparisons, dependent
aggfuncs, memory/concurrency integration, and workload benchmarks. No atomic
package receipt or sysbench/TPC-C/TPC-H/YCSB performance claim is issued.


### Avoid unused peer tables (2026-09-21)

Go aggfuncs/builder.go installs order-column row comparers for RANK/DENSE_RANK,
PERCENT_RANK, and CUME_DIST. COUNT and ROW_NUMBER do not compare these keys.
The ordinary Rust executor nevertheless built a full peer table for every
partition. The new two-partition, 100-row regression observed 196 ordering-key
evaluations where Go requires zero; it failed before the fix.

prepare_peers now returns after clearing old metadata unless one of those
ranking functions is present. Non-ranking execution avoids both the key scans
and the three-usize tuple per input row (24 bytes per row on this 64-bit host).
RANGE bounds use the existing monotonic cursor comparison path, including
CURRENT ROW. The SQL suite verifies its values, NULLs, interval/descending
frames, and ordinary/pipelined mode switches after this change. The new
instrumented test confirms zero extra key evaluations and correct COUNT and
ROW_NUMBER results in both modes. No benchmark throughput gain is inferred
from operation counts or allocation removal.

Changed files: rust/crates/tidb-executor/src/window.rs,
rust/crates/tidb-executor/tests/window_executor_source.rs, and this plan.
Validation from rust/:

    cargo test --offline --locked -j12 -p tidb-executor --test all nonranking_windows_do_not_compare_order_keys
    # Red: 196 actual ordering-key evaluations versus zero required.
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source
    # Final: 12 passed.
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    # 61 passed, including RANGE cursor regressions and original Go assertions.
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    # 23 selected tests passed.

Repository-root checks:

    rustfmt --edition 2024 --check rust/crates/tidb-executor/src/window.rs rust/crates/tidb-executor/tests/window_executor_source.rs
    git diff --check
    make lint
    # Unchanged revive-install failure before actual lint.
    make -o tools/bin/revive lint
    # Actual lint recipes passed with installed required-version tool.

Logs: /tmp/tidb-window-peers-before.log, /tmp/tidb-window-peers-after.log,
/tmp/tidb-window-peers-session.log, /tmp/tidb-window-peers-unit.log,
/tmp/tidb-window-peers-lint.log, and
/tmp/tidb-window-peers-lint-installed.log.

The performance reference rust/docs/perf-parity-2026-09-17.md is historical:
its different shared VM/commit/cluster measurements do not validate this
worktree. Current workload throughput remains unmeasured. Ranking's eager
peer metadata and comparison timing, extreme backward-moving sliding frames,
and remaining dependency/integration surfaces still require audit before an
atomic package receipt. The full original goal remains active.


### Current-worktree sysbench and prepared schema regression (2026-09-21)

The first current-worktree smoke benchmark used HEAD be35d4c762 plus the
uncommitted parity work. Its complete evidence directory is
/tmp/tidb-parity-sysbench.EgHa7J. worktree-sha256.json records source hashes;
rust-server-sha256.txt records the built binary. No sources changed during
that build/run. Subsequent user-requested `git pull --ff-only` advanced HEAD
to aba629bb45, changing only tidb-distsql/src/cop_paging/cop_iterator.rs.
The old release measurements therefore do not validate the newly pulled file.

Commands, from rust/ then repository root respectively:

    cargo build --offline --locked -j12 --release -p tidb-server --bin tidb-server
    bash -n rust/scripts/run-sysbench-ladder.sh
    SYSBENCH_RUST_SERVER=/Users/qiliu/projects/tidb/rust/target/release/tidb-server SYSBENCH_AUTH_USER=root SYSBENCH_AUTH_HOST='%' SYSBENCH_SAMPLES=1 SYSBENCH_OUT_DIR=/tmp/tidb-parity-sysbench.EgHa7J bash rust/scripts/run-sysbench-ladder.sh

Build passed. The ladder used 1,000 rows, one thread, ten seconds per cell,
text/binary protocols, and point-select/read-only/write-only/read-write
workloads. All eight Rust cells and eight Go cells completed without client
errors. The 24 hand-driven SQL checks passed, including Go ADMIN CHECK TABLE
following Rust index creation/deletion and matching cross-server checksums.
The owned TiUP tag sysbench-ladder-60932-1789983437 was cleaned up; its data
directory was confirmed absent after the harness exited. The harness exits
zero even when a rung fails, so exit status is not acceptance evidence.

Two correctness failures take priority over throughput optimization. Under
8-thread write load, Go's five concurrent DDL statements committed, but Rust
prepared UPDATE failed with error 1105: `a physical write child returned 4
columns, expected at least 5`. See rung8-load.log and ladder.log (five checks
passed, one failed). Independently, Go's notifier worker reported unsupported
data type 128. A direct Go SELECT of ddl_job_id/sub_job_id from
mysql.tidb_ddl_notifier reproduced the decoding error; go-notifier-probe.log
also captures the clustered composite primary key definition. The encoding
cause is not yet proven. Preserve the row-width guard while fixing the stale
plan; do not suppress the notifier error or infer correct encoding from a
Rust-only round trip.

The reference Go binary is f964e38b07cb310ff2fbd1821d92ffaf259498c8, not this
repository revision. One sample, different commits, and background notifier
errors make this compatibility smoke insufficient for a performance claim.
TPC-C/TPC-H/YCSB and statistically valid same-revision measurements remain
unverified.

After the pull, the new test
prepared_update_replans_after_another_session_adds_a_column in
rust/crates/tidb-server/src/cluster_session_node/tests/unistore_cop.rs uses two
connections to the existing embedded-store fixture. It warms a prepared
UPDATE, adds a defaulted column from the second connection, reexecutes the
same prepared handle, then intends to repeat after DROP COLUMN. The test
fails at the first execution after ADD COLUMN: two columns returned, three
required. The DROP assertion has not been reached.

    cd rust
    cargo test --offline --locked -j12 -p tidb-server --lib prepared_update_replans_after_another_session_adds_a_column

The sandboxed attempt failed earlier at fixture initialization because macOS
sysctl hw.memsize was unavailable. The approved unsandboxed rerun reaches the
actual regression and fails as described (0 passed, 1 failed, 453 filtered).
Log: /tmp/tidb-prepared-schema-before.log. This is red regression evidence,
not a passing check or whole-package acceptance. No production fix was made
in this checkpoint.

The traced cache lookup in driver/dml.rs uses Catalog::metadata_version.
cluster_session_catalog_with_templates rebuilds a Catalog from default and
increments that version while registering databases/tables; column changes
can preserve the registration count. rebuild_catalog_now replaces the old
Catalog but leaves prepared handles alive. Go plan_cache.go instead checks
InfoSchema.SchemaMetaVersion and table revisions before reuse. The next step
is to represent catalog schema identity consistently across replacement,
transaction snapshots, and temporary-table overlays, covering SELECT and
point-get cache users as well as DML. Merely relaxing the width guard or
invalidating only this one prepared statement would leave correctness gaps.

Repository-root `make lint` again failed installing revive v1.2.1 (module
found but package absent), before running lint recipes. `make -o tools/bin/revive lint` passed using the installed required-version
tool; its output is /tmp/tidb-prepared-schema-lint-installed.log.
`git diff --check` passed. Self-review confirmed the new regression is the
only change in the server test file; its existing adjacent test documentation
remains attached to the original test.
No atomic package completion or end-to-end workload parity is claimed.


### Catalog identity prevents stale prepared plans (2026-09-21)

The previous checkpoint's failing UPDATE regression now passes. The defect
was a cache identity collision: fresh catalogs counted their registrations
from zero, so rebuilding a table with different columns but the same number
of tables preserved the old cache key. Divergent clones could similarly make
different metadata changes and arrive at the same count. Go instead compares
InfoSchema versions/table revisions (pkg/planner/core/plan_cache.go).

Decision: retain the existing u64 internal metadata cache key, but allocate
its values from a process-local atomic sequence on Catalog construction and
schema metadata mutation. Clones and CatalogSnapshot retain the value;
ordinary DML does not advance it. This identifies the Rust catalog image
without changing persisted cluster schema versions or introducing a SQL
feature. The existing equality gates for DML, SELECT, point-get, row-decoder,
and statement-context caches all see the corrected identity. Allocation
uses relaxed ordering because it supplies uniqueness, not synchronization
of catalog contents; checked increment prevents silent wraparound/reuse.
No per-row or ordinary-DML atomic operation is added.

Changed production file: rust/crates/tidb-executor/src/driver/catalog.rs.
Its new regression first failed with two divergent catalogs both reporting
version 34. After the change it passes, also verifying clone/snapshot reuse,
unchanged metadata identity after INSERT, and a distinct rebuilt catalog.
The existing server regression passes through both ADD and DROP COLUMN;
an additional server test warms prepared equality and range SELECT handles
and checks the complete row shape across both changes. Tests live in the
existing catalog module and
rust/crates/tidb-server/src/cluster_session_node/tests/unistore_cop.rs.

Exact validation from rust/:

    cargo test --offline --locked -j12 -p tidb-executor --lib schema_versions_distinguish_rebuilt_and_divergent_catalogs
    # Red before fix, green after fix.
    cargo test --offline --locked -j12 -p tidb-server --lib replans_after_another_session
    # 2 passed; approved sysctl access required by embedded-store fixture.
    cargo test --offline --locked -j12 -p tidb-executor --lib prepared
    # 36 passed.
    cargo test --offline --locked -j12 -p tidb-session --lib prepared
    # 111 passed, 2 preexisting ignored cases.
    cargo test --offline --locked -j12 -p tidb-executor --lib planner_view_tests
    # 4 passed.
    cargo test --offline --locked -j12 -p tidb-session --lib temporary
    # 27 passed.

The ignored prepared cases are a process-global metrics check requiring
serial execution and a Go testing.B benchmark excluded by its original test
gate. Neither was executed. Logs are /tmp/tidb-catalog-epoch-before.log,
/tmp/tidb-catalog-epoch-after.log, /tmp/tidb-prepared-schema-after.log,
/tmp/tidb-epoch-executor-prepared.log, /tmp/tidb-epoch-session-prepared.log,
/tmp/tidb-epoch-catalog.log, and /tmp/tidb-epoch-temporary.log.

Repository-root `git diff --check` passed. `make lint` failed before lint at
the same revive v1.2.1 installation error; `make -o tools/bin/revive lint`
passed using the installed required-version tool. Logs:
/tmp/tidb-epoch-lint.log and /tmp/tidb-epoch-lint-installed.log. Self-review
kept formatting confined to new code in the large existing files.

The process-local cache identity is not a persisted schema version and must
not be serialized as one. The source audit found existing metadata_version
consumers using it for local cache identity/equality. Compatibility evidence
is the retained prepared SQL behavior, not numeric agreement with Go's
cluster version. Performance remains unmeasured after this fix. Rebuild and
rerun the owned-cluster concurrent-DDL ladder before claiming its failure is
resolved end to end; diagnose the independent Go notifier decoding failure
before accepting interoperability. No whole-package completion is claimed.


### Respect the notifier table's clustered handle (2026-09-21)

append_schema_change_mutations in tidb-exec/src/cluster_ddl.rs unconditionally
allocated a hidden row ID and called system_row_write::insert_row. That
helper explicitly writes integer row-ID keys. The smoke cluster's persisted
notifier table instead declares a clustered composite primary key on
(ddl_job_id, sub_job_id). Go expects each signed integer in that common
handle to start with codec intFlag 3 (pkg/util/codec/codec.go); the old writer
started the handle with comparable-integer byte 128, matching the observed
Go decoding failure. The prior fixture used ClusteredIndexDefMode::IntOnly
and only tested the nonclustered shape.

The new clustered_notifier_events_use_the_composite_primary_key regression
adapts the existing notifier metadata to common-handle version 1, plans CREATE
TABLE, and compares the complete record key against independently assembled
Go flag/integer bytes. It also requires no row-ID allocator mutation. Before
the fix, its actual handle was integer 1 while expected was job ID 118 and
sub-job ID -1, each datum-encoded. The regression then passed after selecting
system_row_write::store_clustered_row for clustered tables. Existing
nonclustered encoding and its allocator update remain covered by
create_table_stages_the_go_notifier_row_in_the_catalog_transaction.

This changes the record/index mutations within the existing DDL transaction;
it does not change job scheduling, state transitions, schema publication,
rollback, or notifier delivery semantics. docs/agents/ddl/README.md was read
and its job-based guidance checked against this existing mutation path; no
change to that documented framework is made. No extra feature is added.

The first aggregate test attempt failed compilation in unrelated
hash_join_v2_source.rs: ProbeStage::new now expects a mutable boxed executor
slot, next no longer accepts the source argument, and two event matches omit
FetcherError/FetcherDone. The catalog DDL suite now has its own Cargo test
target and aggregate-test: standalone marker, using the repository's existing
aggregation exclusion mechanism. All 89 tests remain enabled under ordinary
Cargo test discovery, and can be run without compiling unrelated test modules.
The aggregate hash-join compile errors are still unresolved, not waived.

Changed files: rust/crates/tidb-exec/src/cluster_ddl.rs,
rust/crates/tidb-exec/tests/cluster_ddl_source.rs,
rust/crates/tidb-exec/Cargo.toml, and this plan. Validation from rust/:

    cargo test --offline --locked -j12 -p tidb-exec --test all clustered_notifier_events_use_the_composite_primary_key
    # Blocked at unrelated hash-join test compile errors.
    cargo test --offline --locked -j12 -p tidb-exec --test cluster_ddl_source clustered_notifier_events_use_the_composite_primary_key
    # Red before the production fix: integer key versus expected common key.
    cargo test --offline --locked -j12 -p tidb-exec --test cluster_ddl_source notifier
    # 4 passed after the fix, covering both handle layouts.
    cargo test --offline --locked -j12 -p tidb-exec --test cluster_ddl_source
    # 89 passed.

Logs: /tmp/tidb-notifier-clustered-before.log,
/tmp/tidb-notifier-clustered-after.log, /tmp/tidb-notifier-ddl-suite.log.
The first attempt to name cluster_ddl_source before adding its target exited
with target-not-found; it did not execute tests. Formatting was confined to
the added test, and self-review checked handle selection and preservation of
the existing nonclustered allocator path. `git diff --check` passed.
`make lint` again failed at revive installation before actual lint; the
`make -o tools/bin/revive lint` fallback passed with the installed required-version
tool; output is /tmp/tidb-notifier-lint-installed.log.

Both failures from the live smoke now have production fixes and targeted
regressions. Neither is yet proven resolved against a live Go/TiKV cluster.
Next rebuild the release server and rerun the owned-cluster ladder, including
a direct Go notifier-table read. Existing malformed rows are not repaired by
this writer fix. No package receipt or benchmark performance claim is made.


### Live smoke after schema identity and notifier fixes (2026-09-21)

Built the release server at aba629bb45 with all current uncommitted parity
work. Evidence directory: /tmp/tidb-parity-fixed-sqhbaxlt. Source hashes in
worktree-sha256.json were rechecked after completion and were unchanged.
head.txt and rust-server-sha256.txt pin the tested tree/binary. Build command
from rust/ passed in 1m16s:

    cargo build --offline --locked -j12 --release -p tidb-server --bin tidb-server

Build log: /tmp/tidb-fixed-workload-build.log. The run used a temporary copy
of rust/scripts/run-sysbench-ladder.sh with its RUST_ROOT fixed to the actual
workspace and a final Go SELECT/ADMIN CHECK TABLE plus Go-log capture added
before the original cleanup trap. The workload/configuration itself was not
changed. The exact copy is run-with-notifier-probe.sh in the evidence directory.
Command from repository root:

    SYSBENCH_RUST_SERVER=/Users/qiliu/projects/tidb/rust/target/release/tidb-server SYSBENCH_AUTH_USER=root SYSBENCH_AUTH_HOST='%' SYSBENCH_SAMPLES=1 SYSBENCH_OUT_DIR=/tmp/tidb-parity-fixed-sqhbaxlt bash /tmp/tidb-parity-fixed-sqhbaxlt/run-with-notifier-probe.sh

All eight Rust text/prepared workload cells and all eight Go cells completed.
Prepared-dataset checksums agreed at 1000/500500/501715/1/1000; the hand-driven
transaction's final checksum agreed at 1000/500500/505171. All 24 rung-7 SQL
checks passed, including Go ADMIN CHECK TABLE for Rust-created/dropped indexes.
Under eight-thread Rust load, all five Go DDL statements committed within
one second and the workload completed without the former prepared-update
width error. Rung 8 reports six passed, zero failed.

Before cleanup, Go executed:

    SELECT ddl_job_id, sub_job_id, processed_by_flag FROM mysql.tidb_ddl_notifier LIMIT 20;
    ADMIN CHECK TABLE mysql.tidb_ddl_notifier;

Both succeeded (probe exit 0). go-notifier-probe.log is empty because no rows
remained by then; do not describe it as a captured nonempty row round trip.
The full captured go-server.log contains no unsupported-data-type or notifier
processing errors, unlike the previous run. One router context-canceled ERROR
occurred at startup, outside the notifier path. These observations plus the
independent key-byte regression support resolution of the observed defect,
without claiming exhaustive notifier interoperability.

The owned TiUP tag sysbench-ladder-68894-1789984751 was cleaned up successfully;
its data directory was confirmed absent. The harness's cleanup port checks
passed and its process completed with exit 0. No cluster/process remains
owned by this run. go-version.txt records reference Go commit
f964e38b07cb310ff2fbd1821d92ffaf259498c8; it is still different from the current
source revision.

Performance is not accepted. smoke-metrics.json preserves per-cell timings.
Prepared Rust versus Go microseconds per statement were: point read 89.44 vs
83.92, read-only 117.22 vs 156.92, write-only 1731.18 vs 139.61, and read-write
565.64 vs 161.29. This single sequential sample shows a write-heavy slowdown
worth investigating; do not infer the schema fix caused it or discard it
because correctness passed. The previous run had a broken notifier worker,
so it is not a clean performance baseline. Next establish repeatable timings
and profile write/commit time with comparable workload state and pinned Go.
TPC-C, TPC-H, YCSB, whole-package dependency audits, and the unrelated aggregate
hash-join test compilation remain outstanding. This checkpoint changed only
this living plan; it does not issue an atomic package completion receipt.


### Restore aggregate tests and original probe errors (2026-09-21)

The stale hash_join_v2_source fixture used the pre-fetcher ProbeStage API.
Migrated it to transfer Box<dyn Executor> through an Option to new(), call
next() with only the output, and capture source call/allocation/required-row
observations through shared synchronized state. Observations are read after
joining the fetcher. Drop-without-Close remains covered separately, and
repeated Close, bounded fetch counts, empty-build skipping, required-row
propagation, source/worker panic, cancellation, and build-memory release
assertions remain active. Direct-worker tests explicitly reject fetcher events
because those fixtures do not instantiate a fetcher.

The temporary standalone cluster_ddl_source target from the preceding
checkpoint broke cluster_ddl_alter_source's import of its shared helpers.
Removed the standalone marker and Cargo target, restoring the original
aggregate topology. Cargo.toml is now back to its pre-checkpoint contents.
This corrects the earlier decision; do not use the superseded standalone
command for current validation.

Running the now-compiling hash-join matrix exposed a real error-order race:
mode 3, concurrency 5, Inner join returned Internal("probe worker input
channel disconnected") instead of the injected worker panic. On unwind a
worker drops its input receiver before its wrapper reports the recovered
panic; the fetcher could win the race with a secondary disconnection error.
Go hash_join_v2.go's worker recovery (around lines 856/862) sends the recovered
error to joinResultCh. The existing Rust direct-send stage path also ignores
a disconnected input and waits for worker completion/error.

Changed ProbeFetcher::send_input in
rust/crates/tidb-executor/src/hash_join_v2/probe_stage.rs to stop feeding a
closed worker input and allow the worker's original result/error event to
reach the consumer. The panic assertion was retained, with mode/concurrency
context added; it was not relaxed to accept the wrong error. Red evidence is
/tmp/tidb-probe-migration-detail.log. The matrix passes after the fix.

Changed files in this checkpoint: the probe stage production file,
rust/crates/tidb-exec/tests/hash_join_v2_source.rs, restoration of the DDL
aggregate marker/Cargo target, and this plan. Source formatting changes were
confined to new lines. Validation commands from rust/:

    cargo test --offline --locked -j12 -p tidb-exec --test all native_probe_stage_next_close_and_error_source
    # Compiled after migration; exposed the panic/disconnection race.
    cargo test --offline --locked -j12 -p tidb-exec --test all hash_join_v2_source
    # Final: 31 passed, 776 filtered.
    cargo test --offline --locked -j12 -p tidb-exec --test all cluster_ddl
    # 102 passed, 705 filtered; includes notifier and ALTER dependencies.

Logs: /tmp/tidb-probe-migration.log, /tmp/tidb-probe-migration-after.log,
/tmp/tidb-probe-migration-detail.log, /tmp/tidb-probe-migration-final.log,
and /tmp/tidb-restored-ddl-tests.log. Repository-root `git diff --check`
passed. `make lint` hit the same revive installation failure;
`make -o tools/bin/revive lint` passed with the installed required-version
tool; its log is /tmp/tidb-probe-lint-installed.log.
The aggregate binary compiles all its modules, but only the two named suites
were executed; this is not a full 807-test run or a package completion claim.
The live workload was not repeated for this error-path change. The latest
write-performance slowdown still needs controlled profiling and optimization.


### Alternating write-only samples and native profile (2026-09-21)

The previous smoke's large write-only gap did not reproduce in an alternating
run. This checkpoint changes measurement evidence, not production behavior.
Evidence is /tmp/tidb-write-profile-am4h954u, containing run.sh, complete logs,
source-sha256.json, rust-server-sha256.txt, and rust-sample.txt. Source hashes
were unchanged throughout the build/run. Rebuilt current release, including
the latest hash-join error-path fix:

    cd rust
    cargo build --offline --locked -j12 --release -p tidb-server --bin tidb-server

Build passed; log /tmp/tidb-profile-build.log. The temporary run.sh retains
the existing sysbench ladder's owned-cluster startup, auth, preparation,
checksum validation, and cleanup. It replaces subsequent ladder phases with
four ten-second oltp_write_only cells in Rust/Go/Go/Rust order, then a separate
15-second profiled Rust cell. Each uses one thread, prepared statements,
1,000 rows, and --rand-seed=42 against the same table. Command from repo root:

    SYSBENCH_RUST_SERVER=/Users/qiliu/projects/tidb/rust/target/release/tidb-server SYSBENCH_AUTH_USER=root SYSBENCH_AUTH_HOST='%' SYSBENCH_SAMPLES=1 SYSBENCH_OUT_DIR=/tmp/tidb-write-profile-am4h954u bash /tmp/tidb-write-profile-am4h954u/run.sh

The four unprofiled means were 4.17 ms (Rust), 4.18 ms (Go), 4.12 ms (Go), and
4.19 ms (Rust) per transaction. Corresponding transactions/s were 239.56,
238.87, 242.32, 238.64. All workloads succeeded. Go ADMIN CHECK TABLE sbtest.sbtest1
succeeded after the profiled run. These two samples per implementation are
not a statistical equivalence test; data evolves between runs and the Go
reference remains the packaged f964e38b07 build rather than current source.
The previous slow Rust sample remains valid historical evidence, but is not
reproducible evidence of a specific Rust regression.

During only the separate profiled cell, the script executed:

    /usr/bin/sample "$RUST_PID" 8 1 -file "$OUT_DIR/rust-sample.txt"

Sampling succeeded. The active tidb-sql-connection-13 thread had 5,106
observations. 3,641 observations descended through commit_explicit,
RealPessimisticTransaction::commit, publish_prewrites, and BatchReply::complete;
3,640 of those ended in semaphore_timedwait_trap through wait_with_call.
That is approximately 71.3% of sampled wall-clock stacks, not 71% CPU usage.
The thread was predominantly waiting for prewrite completion. This alone
does not distinguish TiKV processing, network, or Rust response scheduling,
and does not justify removing waits, weakening durability, or changing
transaction semantics. The next useful measurements are RPC/server latency
breakdowns and a pinned Go reference, before optimizing this path.

Cleanup completed with exit 0, and the owned TiUP directory for
sysbench-ladder-73472-1789985505 was confirmed absent. The script's owned-port
checks passed. No processes remain owned by this run. No production files
changed; only this living plan was updated and `git diff --check` passed.
TPC-C/TPC-H/YCSB and atomic package acceptance remain open. No performance
improvement or full equivalence claim is made.


### Ranking comparisons happen during result append (2026-09-21)

Reviewed complete ranking implementations in Go aggfuncs/func_rank.go,
func_percent_rank.go, and func_cume_dist.go, and the result loop in
windows/window.go. Go's RANK/DENSE_RANK/PERCENT_RANK retain partition rows
without comparing during update; append of the first result needs no
comparison, and subsequent rows compare the preceding/current pair.
CUME_DIST independently compares its current row with its forward cursor,
including the initial self-comparison. Each function owns its own state.
Rust instead eagerly computed one shared peer table before updating any
function and before returning any result.

The new ranking_comparisons_follow_result_evaluation_order regression uses
instrumented ordering/value expressions to expose the schedule. With two
equal rows, RANK followed by a zero-offset LEAD requires parameter access
trace [1,0,0,1]. The old implementation produced [0,0,1,1], failing before
any production edit. CUME_DIST requires [0,0,0,0,1,1]. The test covers all four
ranking functions, ordinary/pipelined execution, and close/open reuse.
The expression hooks observe the executor schedule; actual SQL values and
RANGE behavior are independently covered by the session window suite.

Replaced prepare_peers and its three-usize tuple per row with one usize
cursor per function. window_value updates that cursor while appending each
result. Ordinary execution creates fresh function cursors per partition;
pipelined execution resets them on partition changes and open. CUME_DIST
keeps its forward boundary independently of the RANK/PERCENT_RANK state.
RANGE CURRENT ROW uses the existing monotonic bound cursors, no longer a
precomputed ranking table. This removes 24 bytes of peer metadata per row
on this host; partition row storage is still required. No workload throughput
improvement is inferred from that allocation reduction.

Changed files: rust/crates/tidb-executor/src/window.rs,
rust/crates/tidb-executor/src/window/pipelined.rs,
rust/crates/tidb-executor/tests/window_executor_source.rs, and this plan.
Exact commands from rust/:

    cargo test --offline --locked -j12 -p tidb-executor --test all ranking_comparisons_follow_result_evaluation_order
    # Red before fix: eager trace [0,0,1,1] instead of [1,0,0,1].
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source
    # Final: 13 passed, including reopen after cursor reset.
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    # 61 passed, including original Go assertions and RANGE frames.
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    # 23 selected tests passed.

Logs: /tmp/tidb-ranking-order-before.log, /tmp/tidb-ranking-order-after.log,
/tmp/tidb-ranking-final.log, /tmp/tidb-ranking-session.log,
/tmp/tidb-ranking-unit.log. Ran rustfmt --edition 2024 on the three changed
Rust files and repository-root git diff --check (passed). make lint again
failed installing revive before running lint; `make -o tools/bin/revive lint`
passed with the installed required-version tool. Log:
/tmp/tidb-ranking-lint-installed.log. Self-review checked partition/open
reset paths and independent function state. Extreme backward-moving sliding
frames, remaining aggfuncs/dependency coverage, shuffle integration, and
whole-package inventory gates remain open. This is parity progress, not an
atomic windows package receipt or a benchmark result.


### Sliding offsets preserve unsigned wrap and error order (2026-09-21)

Used a temporary Go test overlay (no repository Go edits) to execute
SUM(integer), AVG(integer), SUM(double), COUNT, MIN, MAX, and BIT_XOR over
three rows (10,20,30), with windowing_use_high_precision=0. For CURRENT ROW
through uint64::MAX FOLLOWING, ordinary execution initializes the first
nonempty frame, then wraps shiftEnd and recovers an index-out-of-range error
at row 3. Pipelined execution has three empty frames instead, producing NULL
or zero for COUNT/BIT_XOR. For uint64::MAX FOLLOWING through UNBOUNDED
FOLLOWING, both modes produce an initial empty result then whole-partition
and two-row suffix results. All 28 outcomes were captured successfully.

Go oracle command, from pkg/executor/windows, using cached Go 1.26.0:

    GOTOOLCHAIN=go1.26.0 go test -overlay=/tmp/tidb-window-sliding-overlay.json -run '^TestWrappedSlidingOracle$' -tags=intest,deadlock -count=1 -v

The overlay substitutes window_sql_test.go with a copy plus the oracle test;
its JSON and source are /tmp/tidb-window-sliding-overlay.json and
/tmp/tidb-window-sliding-oracle.go. The final oracle passed in 0.426s; log is
/tmp/tidb-window-sliding-oracle.log. An initial attempt used the wrong session
variable prefix and failed before the queries; the corrected run uses Go's
actual windowing_use_high_precision name. No failpoint setup or Bazel change
was needed for this existing package and temporary overlay.

The Rust regression first failed because ordinary SUM returned rows instead
of Go's recovered error. NumericWindowState, inverse COUNT/BIT_XOR, and the
MIN/MAX deque previously reset/recomputed when bounds moved backward. They
now preserve initialized sliding state. FrameRows exposes logical row count
and helpers to visit the still-valid suffix of a wrapped shift, then return
an explicit ExecError with Go's recovered index/length message. This avoids
iterating uint64::MAX times or panicking in Rust, while retaining expression
errors encountered before the invalid row. SUM/AVG/MIN/MAX arrivals precede
departures; COUNT/BIT_XOR departures precede arrivals as in Go. MIN/MAX expire
by the new boundary instead of evaluating departing rows, matching its deque
Slide implementation. Uninitialized empty frames remain empty.

Changed files: rust/crates/tidb-executor/src/window/rows.rs,
rust/crates/tidb-executor/src/hash_agg.rs,
rust/crates/tidb-executor/src/hash_agg/window_numeric.rs,
rust/crates/tidb-executor/src/hash_agg/window_extremum.rs,
rust/crates/tidb-session/src/tests_window/frames.rs, and this plan.
Commands from rust/:

    cargo test --offline --locked -j12 -p tidb-session --lib window_wrapped_sliding_end_preserves_go_error
    # Red before fix; final expanded 28-case regression passes.
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    # 62 passed after production fix.
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    # 23 selected tests passed.
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source
    # 13 passed.

Logs: /tmp/tidb-sliding-wrap-before.log, /tmp/tidb-sliding-wrap-after.log,
/tmp/tidb-sliding-wrap-final.log, /tmp/tidb-sliding-wrap-unit.log,
/tmp/tidb-sliding-wrap-executor.log. Ran rustfmt --edition 2024 on the four
small edited Rust files, avoiding whole-file formatting of hash_agg.rs, and
git diff --check passed. make lint hit the existing revive installation
failure; `make -o tools/bin/revive lint` passed with the installed
required-version tool. Output: /tmp/tidb-sliding-wrap-lint-installed.log. Self-review checked initialized
versus empty frames, addition/removal order, and explicit error construction.

This proves the captured default-chunk SQL cases, not every wrapped bound,
chunk boundary, released-row interaction, aggregate type, or precision mode.
Those remain part of the complete windows/aggfuncs dependency audit. No whole
package receipt or workload throughput improvement is claimed.


### Wrapped frames across chunks and precision modes (2026-09-21)

Expanded the Go oracle to eight aggregate expressions (adding AVG(double)),
two wrapped bounds, chunk sizes 1/2/1024, windowing_use_high_precision OFF/ON,
and pipelined OFF/ON: 192 cases. Go's SessionVars.MaxChunkSize was assigned
directly, matching its original tests rather than bypassing SQL validation
in production. The oracle logged one JSON result/error record per case and
passed in 0.505s. Temporary artifacts:
/tmp/tidb-window-matrix-oracle.go, /tmp/tidb-window-matrix-overlay.json,
/tmp/tidb-window-matrix-oracle.log. From pkg/executor/windows:

    GOTOOLCHAIN=go1.26.0 go test -overlay=/tmp/tidb-window-matrix-overlay.json -run '^TestWrappedSlidingMatrixOracle$' -tags=intest,deadlock -count=1 -v

The matrix establishes that ordinary one-row output chunks reset sliding
state before the wrapped shift, producing the initial whole-frame aggregate
then two empty outputs. At chunk sizes 2/1024, initialized sliding variants
return the recovered index error instead. High-precision floating SUM/AVG
are not sliding variants and return the recomputed results. With the wrapped
start and unbounded end, pipelined one-row chunks fail with Go's exact released
slice error; larger chunks preserve the initial-empty/whole/suffix sequence.
COUNT/BIT_XOR use zero for empty frames; the remaining functions use NULL.

Updated the existing Rust window_wrapped_sliding_end_preserves_go_error test
in rust/crates/tidb-session/src/tests_window/frames.rs to execute the entire
192-case matrix with exact rows, error code 1105, and messages. It passes
without any production edit. All configurations include chunk/precision/
execution context in assertion failures. Direct vars.restore_system mirrors
the Go test-only small chunk assignment, and the Session is local to the test.

Validation from rust/:

    cargo test --offline --locked -j12 -p tidb-session --lib window_wrapped_sliding_end_preserves_go_error
    # 1 matrix test passed, covering all 192 cases.

Log: /tmp/tidb-window-matrix-rust.log. Repository-root rustfmt --edition 2024
--check rust/crates/tidb-session/src/tests_window/frames.rs and git diff --check
passed. make lint again failed installing revive;
`make -o tools/bin/revive lint` passed with the installed required-version
tool. Output: /tmp/tidb-window-matrix-lint-installed.log. The broader 62-test SQL
suite was not rerun for this test-only expansion; its preceding pass remains
the production-change evidence. Only the test and this plan changed.

Remaining work is the complete package/dependency inventory and integration
audit, further untested frame/type combinations, and all requested benchmark
performance acceptance. These observations do not prove arbitrary wrapped
frames or whole-package parity, and no completion receipt is issued.

### Shuffle scheduler checkpoint — 2026-09-21

Current source: `rust/crates/tidb-executor/src/shuffle.rs`; upstream behavior
reference: `pkg/executor/shuffle.go`. The prior scheduler fetched every source
into unbounded queues before driving any worker. A counting-source regression
observed 101 fetches (100 input chunks plus EOF) before the first returned chunk.

Each source and worker now owns its executor on a thread. Each source/worker
pair has one recycled input buffer; each worker has one recycled output buffer.
The shared output channel is sized to workers plus sources, as in Go. Receiver
reads block on input or cancellation. Close broadcasts cancellation, joins all
threads, recovers executor ownership in original order, then closes workers and
sources while retaining the first close error. Drop also cancels and joins to
avoid detached execution. Source and worker panic payloads reach Next as errors.
Expression evaluation uses one mutex-protected context because Rust's Columns
trait permits non-Sync implementations; this preserves shared warning state
without unsafe sharing. This serialization needs evaluation when production
multi-source shuffle is connected.

Focused tests cover bounded fetching, input cancellation, panic recovery while
sources may be waiting on buffers, early drop, multi-source delivery, partition
membership, per-source order, first close error, and reopen. Inter-worker output
order is intentionally unspecified. No claim is made that these tests replace
original upstream SQL/failpoint coverage or discharge the enclosing Go package.

Validation from repository root:

- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-executor --lib shuffle_returns_output_before_draining_its_source`
  failed before implementation: `fetched 101 chunks before first output`.
  Log: `/tmp/tidb-shuffle-red.log`.
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-executor --lib shuffle::tests`
  passed all 20 tests after implementation. Log: `/tmp/tidb-shuffle-green.log`.
- `rustfmt --edition 2021 --check rust/crates/tidb-executor/src/shuffle.rs`
  and `git diff --check` passed.
- `make lint` failed before recipes while installing revive v1.2.1: module
  found but does not contain package. Log: `/tmp/tidb-shuffle-lint.log`.
- `make -o tools/bin/revive lint` passed using the existing installed binary.
  Log: `/tmp/tidb-shuffle-lint-existing.log`. This is not a claim that the
  unmodified make invocation passed.

No Go/Bazel files or dependency manifests changed in this checkpoint. No
production SQL path invokes ShuffleExec yet; PhysicalShuffle/receiver planning,
optimizeByShuffle, builder substitution, upstream error/failpoint validation,
and workload performance verification remain open. Native thread startup failure
and runtime statistics integration also need explicit audit before acceptance.
The source inventory was updated to distinguish the repaired standalone
scheduler from the still-missing production integration.

### Shuffle physical node and builder checkpoint — 2026-09-21

Added `physical/shuffle.rs` and the `PhysicalPlan::Shuffle` variant. Go's
Tails/DataSources pointers are represented by plan IDs into the owned child
tree, avoiding detached copies that could keep stale schemas after rewrites.
By-item expressions resolve against each referenced source after child
resolution. The node carries explain metadata, clone metadata, native owned
memory accounting, and base root-task attachment. The cacheability checker
returns Go's exact `get a Shuffle plan` refusal.

The executor builder constructs each data source once, then builds each worker
with owned receiver substitutions keyed by source plan ID. This replaces Go's
unsafe receiver pointers and mutation of tail children during executor build.
Existing source IDs are used for receiver metadata for now; Go allocates stub
IDs, so runtime-statistics/EXPLAIN ANALYZE identity remains an explicit audit
obligation. Tail-reference validation and native memory-accounting behavior
also remain to be compared as part of the complete physicalop package audit.

Validation from repository root:

- `cargo check --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-executor`
  passed (`/tmp/tidb-shuffle-builder-check.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-planner --lib shuffle_resolves`
  passed. The test distinguishes source column offset 1 from worker output
  offset 0, checks boundary lookup after deep copy, explain IDs, and cache
  refusal (`/tmp/tidb-shuffle-plan-tests.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-planner --lib physical::tests`
  passed all 51 tests (`/tmp/tidb-shuffle-physical-tests.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-executor --lib physical_shuffle_builds`
  passed: three worker copies receive two shared inputs without duplicate
  rows, including reopen (`/tmp/tidb-shuffle-builder-test.log`). The initial
  fixture incorrectly requested more than one row from TableDual and was
  corrected to its zero/one-row contract before accepting the result.
- `rustfmt --edition 2021 --check rust/crates/tidb-planner/src/physical/shuffle.rs`
  and `git diff --check` passed. Only newly added snippets were formatted in
  large files with pre-existing formatting differences.
- `make lint` again failed installing revive v1.2.1 before recipes;
  `make -o tools/bin/revive lint` passed using the existing binary. Logs:
  `/tmp/tidb-shuffle-plan-lint.log` and
  `/tmp/tidb-shuffle-plan-lint-existing.log`.

Files changed in this checkpoint: planner physical module, new shuffle module,
resolve_indices, physical tests, physical_plan_cache, task attachment;
executor physical_builder and explain; this plan and the source inventory.
No Go sources, generated Go outputs, Bazel metadata, or dependencies changed.

Next: wire Window/StreamAgg/MergeJoin concurrency from session state through
StmtContext and DispatchContext; implement the three Go optimizeByShuffle
branches with their sort/NDV gates at the same task-selection point. Then
validate actual SQL plan selection, receiver identity/runtime stats, upstream
error/failpoint cases, and all four requested workloads. This is WIP within
whole-package work, not acceptance of a node/file as a transcreated package.

### Shuffle automatic selection checkpoint — 2026-09-21

Added `physical/shuffle_optimize.rs`, implementing Go's window, stream aggregate,
and merge join rewrite from `core/plan.go`. All branches require concurrency
above one and sorted child inputs. Window/stream aggregation use the existing
multi-column NDV estimator with the resolved group-NDV skew ratio, skip NDV <= 1,
and cap concurrency to integer NDV. Merge join retains the requested concurrency.
The rewrite is called after property enforcement for unordered non-MPP tasks.
Session options flow through CostSessionOpts/StmtContext to DispatchContext;
window's unset value inherits executor concurrency, while stream aggregation and
merge join preserve their session defaults of one.

The first active SQL run exposed nine ordering regressions. A column-only
NominalSort returns its child task directly in Rust. Re-running shuffle selection
at this point wrapped that already-ordered child and lost the discharged ORDER
BY requirement. The dispatcher now excludes this nominal-sort return path. This
is an explicit native integration adjustment, supported by the pinned Go oracle:
`SELECT g,v,SUM(v) OVER(PARTITION BY g ORDER BY v) FROM t` uses Shuffle at
concurrency 4; adding `ORDER BY g,v` produces Window/Sort without Shuffle and
ordered rows. All original ordering assertions remain unchanged and pass after
the adjustment; no output was sorted merely to conceal those failures.
The new unordered SQL regression compares row multisets, which is appropriate
because the query deliberately has no final ORDER BY, and asserts plan selection
at concurrency 1, 4, then 1 again.

Validation from repository root:

- `GOTOOLCHAIN=go1.26.0 go test -overlay=/tmp/tidb-shuffle-order-overlay.json -run '^TestShuffleOrderOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows`
  passed. Temporary Go overlay only; repository Go sources were unchanged.
  `/tmp/tidb-shuffle-order-go.log` records both Go plans and rows.
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-session --lib tests_window`
  initially failed 9 ordering assertions (`/tmp/tidb-shuffle-sql.log`), then
  passed all 63 tests including the new selection check
  (`/tmp/tidb-shuffle-sql-fixed.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-planner --lib shuffle_selection_preserves`
  passed the 12-case window/stream/merge sort and NDV matrix
  (`/tmp/tidb-shuffle-selection-test.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-planner --lib find_best_task::dispatch::tests`
  passed all 29 dispatcher tests (`/tmp/tidb-shuffle-dispatch-tests.log`).
- `rustfmt --edition 2021 --check rust/crates/tidb-planner/src/physical/shuffle_optimize.rs`
  and `git diff --check` passed.
- `make lint` failed at the revive installation step as before;
  `make -o tools/bin/revive lint` passed. Logs:
  `/tmp/tidb-shuffle-selection-lint.log` and
  `/tmp/tidb-shuffle-selection-lint-existing.log`.

Files: new shuffle optimizer module, physical module export, dispatcher,
CostSessionOpts, executor planner bridge, session statement context, window spec
regression, source inventory, and this ExecPlan. No generated or Go/Bazel files
changed. Tests select shuffle in actual window SQL now; merge-join/stream-agg SQL,
receiver IDs/runtime-statistics/EXPLAIN ANALYZE, tail references after physical
rewrites, original failpoint surfaces, and all four workload measurements remain
required. No package or performance acceptance is claimed.

### Shuffled join/aggregation and retained input ownership — 2026-09-21

Captured pinned Go plans and rows for inner, left, and right merge joins,
column-grouped stream aggregation, and expression-grouped aggregation. The
first four use Shuffle at concurrency 4; the fifth uses HashAgg in both Go and
Rust despite STREAM_AGG(), so its fallback is asserted instead of forcing a
non-Go feature. New Rust tests compare serial and parallel results to independent
captured Go row sets, including duplicate keys and NULLs.

Projection elimination initially removed an identity projection referenced as
a shuffle DataSource, leaving its ID unresolved. Go's pointer retains that node
even after removal from ordinary child links. Rust now retains shuffle boundary
nodes in the owned tree until receiver substitution, while still rewriting
descendants and preserving Go's child-projection schema update. The regression
failed with `shuffle boundary plan 101 is absent`, then passed; a builder test
also executes/reopens a source projection after elimination.

A subsequent window-suite run exposed a TopN panic in key_at with shuffled
chunks. Go chunk.List.AppendRow starts a new chunk when the previous chunk was
accepted by Add (`chkIdx == consumedIdx`). Rust incorrectly appended directly
to that accepted chunk when it still had capacity. Accepted chunks can have
aliased columns: appending then increases physical row counts more than once
while only one sort key is added. TopN now tracks whether its tail was allocated
for appending, clears that state on Add/Clear/compaction, and allocates a fresh
buffer as Go does. The deterministic aliased-input regression failed before
with pointer `(0,2)` instead of `(1,0)` and passes after. This fixes the input
ownership contract, not the symptom by skipping invalid pointers.

Validation from repository root:

- `GOTOOLCHAIN=go1.26.0 go test -overlay=/tmp/tidb-shuffle-joinagg-overlay.json -run '^TestShuffleJoinAggOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows`
  passed; five captured plans/row sets in `/tmp/tidb-shuffle-joinagg-go.log`.
  Go inputs remained in a temporary overlay, not repository source changes.
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-planner --lib shuffle_keeps_source_projection`
  failed before fix (`/tmp/tidb-shuffle-source-red.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-planner --lib physical::tests`
  passed all 52 tests (`/tmp/tidb-shuffle-source-green.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-executor --lib physical_shuffle_builds`
  passed (`/tmp/tidb-shuffle-source-builder.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join`
  passed all 14 tests (`/tmp/tidb-shuffle-source-join.log`), including the five
  new Go comparisons. The focused `shuffled_join_and_stream` run also passed.
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-executor --lib append_after_added_chunk`
  failed before the TopN fix (`/tmp/tidb-topn-add-red.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-executor --lib topn`
  passed all 51 tests, including spill and parallel workers
  (`/tmp/tidb-topn-add-green.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-session --lib tests_window`
  exposed the TopN panic (`/tmp/tidb-shuffle-source-window.log`), then passed
  all 63 tests after the fix (`/tmp/tidb-shuffle-topn-window.log`).
- `make lint` still fails installing revive; `make -o tools/bin/revive lint`
  passes. Final logs: `/tmp/tidb-topn-add-lint.log` and
  `/tmp/tidb-topn-add-lint-existing.log`. `git diff --check` passed.

Files changed this checkpoint: physical/mod.rs, physical/tests.rs, executor
physical_builder.rs test, topn_chunk_heap.rs, session tests_explain_merge_join.rs,
and this ExecPlan. No Go/Bazel or generated files changed. Retained-boundary
behavior for other physical rewrites, receiver identity/runtime statistics,
full original failpoint coverage and workload measurements remain unverified.
The fixes/tests are ongoing whole-package evidence, not partial-package acceptance.

### Shuffle runtime counter aggregation — 2026-09-21

EXPLAIN ANALYZE for an eight-row partitioned window returned Shuffle actRows=8
but Window/Sort actRows=0. BuildState::meter inserted new counters for every
worker at the same physical-node key, discarding earlier workers' counters.
Go's BasicRuntimeStats are shared by executor ID (runtime_stats.go explicitly
prohibits cloning them), and its merge contract sums loop/time/row counts.
The Rust builder now reuses the existing accumulator. The shared calls mutex
also serializes RowCount's read/modify/write, avoiding lost updates between
workers. This affects instrumented execution only; ordinary execution installs
no metering wrapper.

The EXPLAIN renderer now includes `ShuffleConcurrency:N` when the executed
Shuffle has runtime statistics, matching ShuffleExec.Close's registered
RuntimeStatsWithConcurrencyInfo. The row-count and concurrency-field assertions
were independently red before their respective changes.

Validation from repository root:

- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-session --lib shuffled_window_runtime_rows`
  failed on Window actRows=0 before counter reuse
  (`/tmp/tidb-shuffle-stats-red.log`), then passed
  (`/tmp/tidb-shuffle-stats-green.log`). After extending the same regression,
  it failed on the missing ShuffleConcurrency field
  (`/tmp/tidb-shuffle-concurrency-red.log`) and passed after rendering it
  (`/tmp/tidb-shuffle-concurrency-green.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-session --lib tests_explain`
  passed all 60 EXPLAIN tests (`/tmp/tidb-shuffle-stats-explain.log`).
- `make lint` failed at the unchanged revive installation step;
  `make -o tools/bin/revive lint` passed with the installed binary. Logs:
  `/tmp/tidb-shuffle-stats-lint.log` and
  `/tmp/tidb-shuffle-stats-lint-existing.log`.
- `git diff --check` passed; the new test snippet was formatted separately to
  avoid pre-existing formatting churn in large files.

Files changed: executor driver/physical_builder.rs, executor explain.rs,
session tests_window/specs.rs, and this plan. No Go/Bazel/dependency changes.
Still missing: actual ShuffleReceiver node IDs and their EXPLAIN/statistics
rows, comprehensive original failure coverage, full-package acceptance and
all requested workload performance verification. No timing equality or
performance improvement is inferred from EXPLAIN's elapsed-time values.

### Shuffle receiver identity and source ownership — 2026-09-21

Go buildShuffle creates one PhysicalShuffleReceiverStub per data source, reuses
its ID across workers, and replaces the tail child with that stub. DataSource
is a separate plan reference, not an ordinary physical child. Rust now models
that boundary with PhysicalShuffleReceiverStub owning a boxed data source and
an empty normal-child list. The builder supplies each worker's receiver through
its existing scoped receiver map; no unsafe executor pointer is needed.

Final physical-plan preparation installs these stubs using the statement's
PlanIdAllocator after projection rewrites. Repeating preparation allocates no
new IDs. Source lookup traverses receiver-owned sources; execution preparation
and EXPLAIN's CTE, scan, process-field and statistics walks also visit them.
This preserves source ownership, execution once per source, and receiver
runtime counters shared across workers. EXPLAIN renders the source beneath
its receiver, matching the captured Go plan. Preparation timing and all nested
plan variants still need the broader package audit; this is not acceptance.

Validation from repository root:

- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-session --lib shuffled_window_runtime_rows`
  failed because ShuffleReceiver was absent before the change
  (`/tmp/tidb-shuffle-receiver-red.log`) and passed afterward
  (`/tmp/tidb-shuffle-receiver-green.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-session --lib tests_window`
  passed all 64 tests with the final separate-source ownership model
  (`/tmp/tidb-shuffle-receiver-window.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-session --lib tests_explain`
  passed all 60 tests, including shuffled join/aggregate comparisons
  (`/tmp/tidb-shuffle-receiver-explain.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-planner --lib physical::tests`
  passed all 52 tests, including source preservation, receiver empty children,
  ID uniqueness and preparation idempotence
  (`/tmp/tidb-shuffle-receiver-physical-final.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-executor --lib physical_shuffle_builds`
  passed the multiple-source builder/reopen test
  (`/tmp/tidb-shuffle-receiver-builder-final.log`).
- `make lint` failed before lint recipes while installing revive v1.2.1
  (`/tmp/tidb-shuffle-receiver-lint-final.log`).
  `make -o tools/bin/revive lint` passed all lint recipes with the installed
  binary (`/tmp/tidb-shuffle-receiver-lint-existing-final.log`).
- `rustfmt --edition 2024 --check rust/crates/tidb-planner/src/physical/shuffle.rs`
  and `git diff --check` passed. New snippets in large files were formatted
  separately to avoid unrelated baseline churn.

Files changed this checkpoint: planner physical/shuffle.rs, physical/mod.rs,
physical/resolve_indices.rs, physical/tests.rs, physical_plan_cache.rs, task.rs;
executor driver/planner_bridge.rs, driver/physical_builder.rs, explain.rs;
session tests_window/specs.rs; this plan and the physicalop inventory. No Go,
Bazel, dependency or generated-code changes. Native thread-spawn failure,
original failpoint coverage, all physicalop methods/variants, and the requested
sysbench/TPC-C/TPC-H/YCSB performance comparison remain unverified. No speedup
or completed-package claim is made.

### Native shuffle startup failure — 2026-09-21

Inspected current shuffle.rs and pinned Go shuffle.go. std::thread::spawn
panicked on native thread-creation failure while sources/workers were moved
into closures and temporary iterators. Fallible Builder::spawn now starts the
thread before ownership transfers through a bounded startup channel. Failure
restores the failed job and remaining suffix after canceling/joining the started
prefix. Next reports an execution error; Close keeps its existing first-error
and close-all behavior. Test-only injection does not add a session option or
production failpoint. SQL scheduling and partitioning are unchanged.

The deterministic regression injects creation failure at each of two source and
three worker positions. It checks source/worker object identity and order,
empty join-handle lists, Close, successful Open and all six rows on replay.
The pre-fix run reproduced the panic using the injected OS-error boundary and
the existing panic-on-spawn-failure behavior, without exhausting host threads.
Actual host resource exhaustion was not induced.

Validation from repository root:

- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-executor --lib shuffle_thread_start_failure`
  failed before the fix (`/tmp/tidb-shuffle-spawn-red.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-executor --lib shuffle::tests`
  passed all 21 tests (`/tmp/tidb-shuffle-spawn-green.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-session --lib tests_window`
  passed all 64 tests (`/tmp/tidb-shuffle-spawn-window.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join`
  passed all 14 tests (`/tmp/tidb-shuffle-spawn-join.log`).
- `make lint` failed at the unchanged revive installation step
  (`/tmp/tidb-shuffle-spawn-lint.log`); `make -o tools/bin/revive lint`
  passed the lint recipes (`/tmp/tidb-shuffle-spawn-lint-existing.log`).
- `rustfmt --edition 2024 --check rust/crates/tidb-executor/src/shuffle.rs`
  and `git diff --check` passed.

Files changed this checkpoint: rust/crates/tidb-executor/src/shuffle.rs and this
ExecPlan. No Go/Bazel/dependency/generated-code changes. Complete physicalop
method/variant coverage, upstream shuffle failure/failpoint cases, and the
requested four-workload performance measurements remain open. This native
cleanup correction is evidence within the ongoing whole-package work, not an
independent transcreated-package acceptance.

### Shuffle aggregate key bytes — 2026-09-21

Pinned Go partitionHashSplitter calls aggregate.GetGroupKey, which uses declared
field types, numeric enum values, flen=0 decimal precision and the statement
zone. Rust instead called group_key_part, a generic datum hash helper. A local
Go oracle produced independent expected bytes and murmur3 worker numbers:
unsigned [MAX_UINT64,1] -> [[8,1],[8,2]], workers [5,5]; enum values [0,1] with
both names empty -> [[8,0],[8,2]], workers [6,5]; decimal(2,2) input [1.20,12.30]
-> [[6,3,2,129,20],[6,4,2,140,30]], workers [6,2], at concurrency 7. The decimal
inputs specifically verify GetGroupKey resets flen instead of rejecting values
that exceed the declared precision. These fixtures are retained in shuffle.rs.

The splitter now appends through the existing timezone-aware group-key codec
with Go's enum/decimal field adjustments. It resolves zone once per chunk and
field metadata once per expression. Row-key buffers retain capacity, and no
temporary group-key Vec is allocated per value. Expression evaluation remains
row-based; vectorized evaluation parity and error-context warning routing need
separate coverage. No benchmark improvement is inferred from this change.

Validation from repository root:

- `GOTOOLCHAIN=go1.26.0 GOPROXY=off go run /tmp/tidb-shuffle-keys.go`
  captured the pinned Go implementation's keys and assignments
  (`/tmp/tidb-shuffle-keys-go.log`). No repository Go files were changed.
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-executor --lib hash_splitter_uses_go`
  failed before the fix: generic unsigned key bytes differed from Go
  (`/tmp/tidb-shuffle-keys-red.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-executor --lib shuffle::tests`
  passed all 22 tests including the expanded three-type Go fixture
  (`/tmp/tidb-shuffle-keys-green.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-session --lib tests_window`
  passed all 64 tests (`/tmp/tidb-shuffle-keys-window.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join`
  passed all 14 tests (`/tmp/tidb-shuffle-keys-join.log`).
- `make lint` failed installing revive (`/tmp/tidb-shuffle-keys-lint.log`);
  `make -o tools/bin/revive lint` passed the lint recipes
  (`/tmp/tidb-shuffle-keys-lint-existing.log`).
- `rustfmt --edition 2024 --check rust/crates/tidb-executor/src/shuffle.rs`
  and `git diff --check` passed.

Files changed: rust/crates/tidb-executor/src/shuffle.rs and this ExecPlan. No
Go/Bazel/generated/dependency changes. Remaining scope includes complete
physicalop and executor package artifacts, all original failure scenarios,
field-type/warning parity, and sysbench/TPC-C/TPC-H/YCSB measurements. This is
ongoing whole-package evidence, not acceptance of a partial package.

Next original-test audit targets found during this checkpoint:
pkg/executor/shuffle_test.go::TestPartitionRangeSplitter uses a 13-row VARCHAR
fixture absent from the current four-integer Rust analogue;
pkg/executor/executor_failpoint_test.go::TestShuffleExit combines an immediate
Next error, delayed source panic and worker panic during a window SQL query.
The existing isolated Rust panic tests do not establish that combined original
scenario. Preserve these as explicit outstanding validation, not covered claims.

### Original shuffle fixtures and combined failure ordering — 2026-09-21

Reviewed pkg/executor/shuffle_test.go::TestPartitionRangeSplitter and
pkg/executor/executor_failpoint_test.go::TestShuffleExit against current Rust.
The original 13 VARCHAR values and 13 worker-index expectations now appear
verbatim as data in range_splitter_matches_original_varchar_fixture.

For TestShuffleExit, unit-only injection points now match Go's positions:
Shuffle.Next errors immediately after preparation, the source panics inside
its recovery boundary after a channel gate opens, and each worker panics inside
its recovery boundary before consulting cancellation. The test releases the
source after observing the original Next error, calls Close, verifies every
join handle was consumed and every executor restored, disables injection and
verifies all four rows after reopening. This covers concurrent recovery during
early caller failure, which isolated source/worker tests did not prove. No bug
was newly observed: both original-scenario assertions pass with the existing
scheduler. This is additional validation, not a red/green bug fix.

Validation from repository root:

- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-executor --lib shuffle::tests`
  passed all 24 tests (`/tmp/tidb-shuffle-original-tests.log`).
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-session --lib shuffled_window_runtime_rows`
  passed with the executor compiled without test-only injection fields
  (`/tmp/tidb-shuffle-original-session.log`).
- `make lint` failed at revive installation (`/tmp/tidb-shuffle-original-lint.log`);
  `make -o tools/bin/revive lint` passed lint recipes
  (`/tmp/tidb-shuffle-original-lint-existing.log`).
- `rustfmt --edition 2024 --check rust/crates/tidb-executor/src/shuffle.rs`
  and `git diff --check` passed.

Files changed: rust/crates/tidb-executor/src/shuffle.rs and this plan. No Go,
Bazel, generated, dependency or production runtime-option changes. Go tests
were inspected but not rerun this checkpoint. The combined-failure test uses
fixture executors, not the original SQL query and global failpoint framework;
that end-to-end route remains unverified. Full physicalop/executor inventories,
warning and type variants, and four-workload performance acceptance remain
open. The prior checkpoint's outstanding exact range-fixture item is now
covered; the original SQL-level TestShuffleExit item remains open.

### Current release preparation — started 2026-09-21

The release binary predates the accumulated window and shuffle fixes, so new
workload measurements require rebuilding it. Started from rust/:

    cargo build --offline --locked -j12 --release -p tidb-server --bin tidb-server

Build passed in 1m14s; log /tmp/tidb-parity-current-release.log. Command session
11744 is terminal. Source/binary hashes were captured in
/tmp/tidb-parity-current-source-sha256.json. No benchmark cluster
has been started in this checkpoint. Benchmark harness review found
rust/scripts/compare-tpcc.py and existing seed patches; the installed bench
binary path in older notes, ~/.tiup/components/bench/v1.12.0/go-tpc,
is absent on this host. The installed components contain no bench package.
The tiup wrapper attempts to write execution history even for --help and was
sandbox-denied; no cluster was started. Discover the installed playground
executable directly or request the required sandbox access for cluster use. The sysbench ladder defaults to a packaged Go build, which is not a
matching-source parity baseline. A current-source Go build and explicit harness
binary selection are needed before claiming current-revision performance.

### Matching-source benchmark preparation — 2026-09-21

The previous turn was progress: added original shuffle coverage and produced
the current Rust release binary. This checkpoint builds an unmodified Go
reference at aba629bb455 and prepares a current TPC-C correctness smoke.

Commands from repository root:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go build -o /tmp/tidb-go-aba629bb455 ./cmd/tidb-server
    tiup install bench:v1.12.0

Both passed. The first build attempt needed sandbox access to the existing Go
cache and was rerun with approved escalation. Logs: /tmp/tidb-go-current-build.log
and /tmp/tidb-bench-install.log. `go version -m` records vcs.revision
aba629bb455dc09d6a5d98b3c39a542bb1189b9d; no Go/mod/sum files are modified.
The plain Go build lacks Makefile's codes tag and version link flags, so its
-V output has placeholder release metadata. This is an exact-source
correctness reference, not yet a standard production-build performance baseline.
Do not misreport the packaged nightly as this build.

Temporary harnesses reuse the existing sysbench ladder's startup, fixture auth,
TLS/client checks and mandatory owned-cluster cleanup, selecting Go through
playground --db.binpath /tmp/tidb-go-aba629bb455. TPC-C uses one warehouse,
stock go-tpc v1.12.0, Go preparation, --check-all on both implementations,
then four 1000-transaction, one-thread samples in Rust/Go/Go/Rust order only
if correctness checks pass. Inputs are unseeded and data evolves; these samples
cannot prove performance equivalence. Binary hashes and source revision are
stored alongside each run's logs and exact run.sh.

Two initial attempts failed in harness setup and cleaned up their clusters:
/tmp/tidb-current-tpcc-zjo96ret omitted the Rust fixture password;
/tmp/tidb-current-tpcc-91x5mbre used an empty array rejected by macOS Bash under
set -u. Neither is a Rust TPC-C behavior failure. Fixed explicit per-engine
password arguments and tested the nonempty --password= array under Bash.

Third run: /tmp/tidb-current-tpcc-21joa4dp, session 39871 is terminal. Invocation:

    SYSBENCH_RUST_SERVER=/Users/qiliu/projects/tidb/rust/target/release/tidb-server SYSBENCH_AUTH_USER=root SYSBENCH_AUTH_HOST='%' SYSBENCH_SAMPLES=1 SYSBENCH_OUT_DIR=/tmp/tidb-current-tpcc-21joa4dp bash /tmp/tidb-current-tpcc-21joa4dp/run.sh

The harness traps exit to stop every owned server and verify its ports are closed.
No repository production code changed in this benchmark checkpoint.

The third run passed initial Go/Rust --check-all and completed four complete
1000-transaction samples, but is NOT accepted: the final --check-all reported
condition 3.3.2.11 failure with exit status 0. The harness incorrectly printed
success because its last check inspected only exit status. Raw logs remain
preserved and summary.json explicitly marks post-check-failed-do-not-accept.

Fetched the exact installed go-tpc source revision from binary Go build info:
d05fdf8aaddcd5ae30e02333eba3760c37219c05 (go-tpc 1.0.9 packaged as TiUP bench
v1.12.0). /tmp/tidb-gotpc-check.go is the pinned tpcc/check.go. CheckPrepare
passes checkAll=true; ordinary Check defaults to false and omits condition
3.3.2.11. That condition enforces initial order_count - new_order_count = 2100;
delivery changes it. The fourth harness records a Go-only transaction/control
check before any Rust transactions, retains all 12 initial checks, then uses
ordinary 11-condition post-run checks through both engines. Every required
check inspects output for reported errors as well as process status.

Fourth run is /tmp/tidb-current-tpcc-m6_ho_2c, terminal session 31487, using the same
invocation above with SYSBENCH_OUT_DIR and script path changed to that directory.
It adds a Go-only 1000-transaction control, captures the diagnostic --check-all
output, and retains the four alternating samples. Its pinned checker source,
exact script, binary hashes, source revision, logs and eventual outcome are
kept in that directory. Session 31487 completed with exit 0; validation below
checks output contents in addition to shell status.

Fourth-run outcome: passed the correctness smoke. Initial --check-all covered
12 distinct conditions through each engine. After the Go-only 1000-transaction
control, the diagnostic --check-all reproduced 3.3.2.11 failure without any
Rust transaction, confirming it is the preparation-only invariant. Four
subsequent samples each completed exactly 1000 transactions, all five
transaction categories, no reported error/panic. Normal post-run checks covered
all 11 intended conditions through each engine and reported no failures.
summary.json contains exact counts, raw timing fields, check identities and
performance_acceptance=false. The Go control reported 24 deliveries; their
removal of NEW_ORDER rows explains the preparation-invariant failure.

Validation additionally rehashed all three binaries against binaries.json,
verified complete transaction counts, scanned required check outputs for
reported failures, and confirmed the owned TiUP directory
sysbench-ladder-23720-1789990979 no longer exists. The harness cleanup also
verified its five owned ports were closed. No benchmark processes remain.
The first summary-extraction attempt referenced an absent go-server.log;
corrected it to the retained playground.log, then all assertions passed.

Files changed this checkpoint: this ExecPlan only. Exact build/install/run
commands and test artifacts are above. `bash -n` passed for each corrected
script; `git diff --check` passed. No production edits, new Go files, imports,
Bazel changes or dependency changes were made, so no new lint/Bazel run was
required. The latest implementation's targeted tests and lint fallback remain
recorded in preceding checkpoints. Remaining measurement obligations include
standard Go production build flags, seeded/repeated controls, current-source
sysbench, TPC-H and YCSB, and optimization justified by observed profiles.
Whole-package acceptance remains open.

### TPC-H current-source answer-check run — 2026-09-21

The previous goal turn was progress: current-source TPC-C correctness smoke
passed after fixing benchmark setup and validating the checker's intended
post-run conditions. Next, built the Go reference with the repository's normal
production tags and link flags, rather than the earlier plain go build:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off make server TARGET=/tmp/tidb-go-aba629bb455-production

Build passed; /tmp/tidb-go-production-build.log contains the expanded command
with -tags codes and version metadata. Binary -V reports revision aba629bb455,
branch hparser-integration and the expected git-describe dirty version (Rust
WIP only; no Go/go.mod/go.sum changes). The cached Go1.26 toolchain remains the
native-build workaround noted in prior oracle receipts.

TPC-H harness /tmp/tidb-current-tpch-kdjf441z/run.sh reuses the validated cluster
startup, fixture credentials, standard Go binary selection and cleanup.
It prepares SF1 through Go using stock go-tpc 1.0.9 from bench v1.12.0, with
--analyze and four load workers, then runs all 22 queries once per engine with
one query thread, --count 22 --check, Go first then Rust. Preparation is bounded
by --time 15m and each query sequence by --time 20m. Checks inspect output as
well as exit status. It retains Go server logs before owned-cluster cleanup.
Raw timings are exploratory; one run cannot prove performance equivalence.

Command from repository root:

    SYSBENCH_RUST_SERVER=/Users/qiliu/projects/tidb/rust/target/release/tidb-server SYSBENCH_AUTH_USER=root SYSBENCH_AUTH_HOST='%' SYSBENCH_SAMPLES=1 SYSBENCH_OUT_DIR=/tmp/tidb-current-tpch-kdjf441z bash /tmp/tidb-current-tpch-kdjf441z/run.sh

Session 3780 completed with exit 0 after mandatory cluster cleanup.
Binary hashes, Go metadata, source revision, full
commands and copied pinned go-tpc workload.go are in the run directory.
Inspected pinned go-tpc Run: QueryContext is followed by scanQueryResult; its
reported operation latency is measured before scanning all rows, so it is not
necessarily end-to-end query latency. Count/result coverage must be verified
after execution. No production sources changed this checkpoint.

TPC-H outcome: preparation generated SF1 and successfully analyzed all eight
tables. Each run log contains exactly one summary for each Q1 through Q22,
Finished, and no error/failed/panic text. Required query coverage and unchanged
binary SHA256 values were independently asserted after the run. summary.json
records the two 22-query results, exact_parity_proven=false and
performance_acceptance=false. The owned TiUP directory
sysbench-ladder-25826-1789991214 is absent; the harness also verified all five
owned ports closed. Go and Rust server logs were retained before cleanup.

Additional checker inspection: fetched pinned tpch/check.go into the run
directory. It compares row count/order and selected fields exactly, permits
$100 differences for SUM fields and its own numeric tolerance for averages/
ratios, and does not explicitly call rows.Err after iteration. Passing this
checker is therefore benchmark-reference coverage, not the user's strict
no-gaps result parity. A future exact differential harness must collect full
rows and stream errors, with explicit numeric semantics. Tool latency is taken
before scanQueryResult consumes the full stream; no end-to-end speedup is
inferred from these reported times.

Files changed this checkpoint: this ExecPlan only. The repository's Go and
Rust production sources are unchanged. `bash -n` on the temporary harness and
`git diff --check` passed; prior code-validation gates still apply. No new
lint/Bazel preparation was required. Remaining work includes exact Go/Rust
result comparisons, YCSB, repeated seeded and balanced performance controls,
profile-justified optimization, and the full package-by-package acceptance
inventory. The standard current-source Go binary is now available for those
follow-ups at /tmp/tidb-go-aba629bb455-production.

### YCSB current-source integrity smoke — 2026-09-21

The preceding goal turn was progress: TPC-H SF1 completed all 22 pinned answer
checks through both engines. This turn runs YCSB A–F on current-source Go
production and Rust release binaries. Installed bench v1.12.0 provides go-ycsb;
its exact SHA256 is recorded even though Go build info has no source revision.
Inspected official current MySQL driver code only for usage guidance, not as a
pinned behavioral oracle. Both mysql.db and mysql.dbname name the same fresh
database; actual tables and complete rows in that database are independently
queried, so an ignored property cannot silently redirect accepted results.

Temporary harness: /tmp/tidb-current-ycsb-gwl6r81b/run.sh. It reuses the validated
owned-cluster lifecycle with /tmp/tidb-go-aba629bb455-production and the current
Rust binary. For each A–F mix and engine, it creates an independent database,
loads 1000 rows with ten 100-byte fields, and executes 1000 operations using
four clients. dataintegrity=true, constant field length, readallfields=true;
A/B/C/E/F use uniform requests, D uses latest. Mixes: A half reads/updates;
B 95% reads/5% updates; C reads; D 95% reads/5% inserts; E 95% scans/5% inserts;
F half reads/read-modify-writes. maxscanlength=100. Random inputs are unseeded,
so cross-engine operation mixes need not be identical and timing equivalence
is not inferred.

After both load and run, full sorted table contents are read through BOTH
servers and compared byte-for-byte. Each load must contain exactly 1000 rows.
Go ADMIN CHECK TABLE verifies each completed fixture. Commands and all output
are retained; every benchmark command is checked for reported errors as well
as exit status. Required final operation counts and insert-induced row growth
must still be independently verified after completion.

Command from repository root:

    SYSBENCH_RUST_SERVER=/Users/qiliu/projects/tidb/rust/target/release/tidb-server SYSBENCH_AUTH_USER=root SYSBENCH_AUTH_HOST='%' SYSBENCH_SAMPLES=1 SYSBENCH_OUT_DIR=/tmp/tidb-current-ycsb-gwl6r81b bash /tmp/tidb-current-ycsb-gwl6r81b/run.sh

Session 62489 completed with exit 0. All A–F fixtures completed, and the
harness cleaned its owned processes/data and checked all five ports on exit.
No repository production edits.

Performance caveats established from actual evidence: SHOW FULL PROCESSLIST
on the owned cluster captured YCSB loading blocked in ANALYZE TABLE usertable;
Go load time includes about 40 seconds after inserts complete. That load OPS
is not insert throughput. Complete before/after table files for both engines'
A/B fixtures are identical despite successful updates: integrity mode rewrites
deterministic values. It does not validate general value-changing update
performance. Use ordinary random payloads and explicit persisted-write checks
for the subsequent write benchmark. This smoke remains scoped integrity and
operation-path evidence, not whole-package or workload optimization acceptance.

YCSB final validation: parsed each final histogram, requiring 1000 INSERTs on
every load and 1000 logical operations on every run. F instrumentation counts
READ for both standalone reads and RMW operations; READ must equal 1000 and
UPDATE must equal READ_MODIFY_WRITE, avoiding double-counting. Full sorted table
outputs match through both engines after all 24 load/run phases, with eleven
columns per row. Post-run row counts equal 1000 + successful INSERT count.
Across twelve fixtures: 12,000 loaded rows, 12,000 logical run operations and
194 additional inserted rows. All 12 Go ADMIN CHECK TABLE commands passed.
summary.json records per-fixture counts, row hashes and explicit performance
limitations. Binary SHA256 values were rechecked unchanged. Owned TiUP directory
sysbench-ladder-30801-1789992002 is absent, and no owned benchmark process remains.

Files changed this checkpoint: this ExecPlan only. `bash -n` and
`git diff --check` passed; no production edit required new compilation, lint
or Bazel preparation. This is live workload-path and stored-data evidence,
not exact operation-sequence differential testing or package completion.
The next meaningful YCSB performance run must disable deterministic integrity
payloads, prove persisted values actually change, balance engine order, run
longer samples and use profiles to justify any optimization. Existing current
TPC-C/TPC-H/YCSB smoke coverage does not remove the whole-package inventory,
warning/type, full original-test and exact-result obligations.

### YCSB changing-value comparison and profiles — 2026-09-21

The prior goal turn made progress: A–F integrity smoke validated all twelve
fixtures and cleanup. This checkpoint addresses the observed no-op-update
limitation before drawing performance conclusions. No production code changed.

Temporary harness: /tmp/tidb-ycsb-changing-2j8ohvcn/run.sh. It reuses current
production-tagged Go and current Rust release binaries on a fresh owned cluster.
It loads one shared 1000-row table (ten 100-byte fields), dataintegrity=false,
and uses a 50/50 read/update mix, uniform keys, four clients and 100,000 logical
operations per sample. The first four samples run Rust/Go/Go/Rust; two separate
profiled samples follow, Rust then Go. Inputs are unseeded, data evolves and
there are only two unprofiled samples per engine, so formal performance
acceptance is not implied. Reuse existing binary hashes to verify identity.

Each sample must finish without reported errors, yield exactly 1000 rows,
produce identical full-table output through both servers, change persisted
values compared with the prior snapshot, and pass Go ADMIN CHECK TABLE.
The harness records monotonic wall times separately from tool histograms.
macOS sample records Rust stacks for eight seconds during sample 5; Go's
pprof endpoint records an eight-second CPU profile during sample 6. Samples 5/6
are excluded from the timing comparison. Wall-clock stack samples and Go CPU
profile samples are different measurements; do not compare their percentages
as if they were the same denominator.

Command from repository root:

    SYSBENCH_RUST_SERVER=/Users/qiliu/projects/tidb/rust/target/release/tidb-server SYSBENCH_AUTH_USER=root SYSBENCH_AUTH_HOST='%' SYSBENCH_SAMPLES=1 SYSBENCH_OUT_DIR=/tmp/tidb-ycsb-changing-2j8ohvcn bash /tmp/tidb-ycsb-changing-2j8ohvcn/run.sh

Session 35560 is terminal with exit 0 after all six samples and cleanup.
Owned workload PID is included in cleanup; the existing cluster trap stops
servers, removes owned data and checks its ports. Exact scripts, logs, binary
hashes, snapshots and profiles will remain in the run directory. `bash -n`
passed before execution. The full package-parity objective remains unchanged.

Measurement correction during live run: wall-times.tsv is INVALID and must not
be used. This host's Python3.9 macOS time.monotonic_ns uses process-relative
origins: a single-process probe increased from ~5ms to ~55ms across a 50ms
sleep, while a new process restarted around ~4ms. The harness erroneously
subtracted readings from two processes, producing even negative differences.
Use only go-ycsb's within-process `Run finished, takes ...` duration for this
exploratory comparison. It is a different defined metric, not a repaired
end-to-end wall-time value. Future wall timing needs one persistent clock
owner. This does not invalidate operation counts, stored-data checks or the
separate profiles. Sample 1 Rust and sample 2 Go both completed with changed
stored values, cross-engine reads matching, and storage checks passing;
sample 3 Go is running in the same live session 35560.

Final changing-value run: all six samples completed exactly 100,000 operations
(READ + UPDATE), every one of the 1000 stored rows changed in each sample,
full sorted table output through both servers matched, and all six Go ADMIN
CHECK TABLE checks passed. Binary hashes remain unchanged. summary.json contains
counts, within-process tool durations, row hashes, explicit rejection of
wall-times.tsv and performance_acceptance=false. Unprofiled order/durations:
Rust 54.534845208s, Go 64.157735875s, Go 60.715194708s, Rust 62.988015417s.
These do not establish a reliable Rust performance gain or regression: two
samples per engine, unseeded mixes, evolving shared data and no variance model.
No production optimization was made on the basis of this small comparison.

Rust profile evidence: four active SQL threads each had 5673 observations.
Direct publish_prewrites totals were 5300, 5280, 5280 and 5288; their largest
semaphore wait branches were 5298, 5277, 5278 and 5274. Thus 93.1–93.4% of
sampled wall observations were in the prewrite path, mostly awaiting batch
completion. rust-profile-summary.json retains these counts. This is not CPU
percentage and does not distinguish TiKV service time, network delay or client
response scheduling. It does not justify relaxing durability or transaction
semantics. The next useful performance evidence is client/server RPC latency
breakdown, not speculative expression/planner micro-optimization.

Go CPU profile was captured separately for 7.84s with 2.94s reported samples.
The top report is dominated by runtime/kernel functions (rawsyscalln, kevent,
pthread_cond_signal/wait); it does not identify a SQL-specific hotspot.
Commands/evidence:

    /usr/bin/sample "$RUST_PID" 8 1 -file "$OUT_DIR/rust-profile.txt"
    curl -fsS --max-time 15 "http://127.0.0.1:${GO_STATUS_PORT}/debug/pprof/profile?seconds=8"

The Go profile body was saved as go-profile.pb.gz. Cached Go1.26 has no pprof
tool. Running the repository-pinned google/pprof CLI from the repo first hit
unrelated offline module resolution, then its own module lacked two cached
dependencies. After fetching only its pinned dependencies, this command passed
from /Users/qiliu/go/pkg/mod/github.com/google/pprof@v0.0.0-20250903194437-c28834ac2320:

    GOTOOLCHAIN=go1.26.0 GOPROXY=https://proxy.golang.org go run -mod=readonly . -top -nodecount=20 /tmp/tidb-go-aba629bb455-production /tmp/tidb-ycsb-changing-2j8ohvcn/go-profile.pb.gz

Output: go-profile-top.txt in the run directory. No go.mod/go.sum changes.
The profiled samples are excluded from timings above. Owned TiUP directory
sysbench-ladder-34407-1789992514 is absent and cleanup checked all five ports.
No benchmark or profile process remains.

Files changed: this ExecPlan only. `bash -n`, output/count/data/hash validation
and `git diff --check` passed. No new compile/lint/Bazel gate was needed for
unchanged production sources. Whole-package parity, original test/variant
coverage, exact TPC-H differential results and validated performance
optimizations remain open; the goal is active.


### 2026-09-21: existing RPC metrics narrow YCSB write latency

No production code changed in this checkpoint. On the same current-source
production Go and release Rust binaries, ran four clients against 1000 rows,
10 changing 100-byte fields, uniform keys, 50% reads / 50% updates, 50,000
operations per sample in Rust/Go/Go/Rust order. Exact command from repo root:

    SYSBENCH_RUST_SERVER=/Users/qiliu/projects/tidb/rust/target/release/tidb-server SYSBENCH_AUTH_USER=root SYSBENCH_AUTH_HOST='%' SYSBENCH_SAMPLES=1 SYSBENCH_OUT_DIR=/tmp/tidb-ycsb-rpc-wc1la0bu bash /tmp/tidb-ycsb-rpc-wc1la0bu/run.sh

Session 31977 exited 0. All four samples completed exactly 50,000 READ+UPDATE
operations with no reported errors. All 1000 rows changed after each sample,
sorted full-table output from Go and Rust matched byte for byte, and Go ADMIN
CHECK TABLE passed after every sample. Binary SHA256 values are unchanged.
The cleanup trap checked all five ports; owned TiUP data directory
sysbench-ladder-40957-1789993285 is absent. No cluster remains from this run.

The harness read the single owned store's status address from PD, and scraped
existing TiKV and Go HTTP metrics immediately before/after each isolated
sample. It added no Rust instrumentation. `summary.json`, raw .prom snapshots,
logs, table snapshots, source revision and hashes remain in the run directory.
Validation and aggregation command:

    python3 /tmp/tidb-ycsb-rpc-wc1la0bu/summarize.py

| Sample | Tool duration (s) | UPDATE count | UPDATE mean (ms) | TiKV prewrite count | TiKV prewrite mean (ms) | Go external_Update prewrite mean (ms) |
| --- | --- | --- | --- | --- | --- | --- |
| Rust 1 | 33.0889 | 24998 | 4.864 | 24707 | 4.198 | unavailable |
| Go 2 | 35.4477 | 24850 | 5.236 | 24722 | 4.446 | 4.613 |
| Go 3 | 29.6267 | 24918 | 4.322 | 24943 | 3.593 | 3.737 |
| Rust 4 | 31.1557 | 24941 | 4.551 | 24874 | 3.841 | unavailable |

TiKV mean is the delta sum/count of tikv_grpc_msg_duration_seconds with
`type="kv_prewrite"` across priorities. Go uses
`tidb_tikvclient_source_request_seconds` with `type="Prewrite"` and
`source="external_Update"`. Scheduler prewrite means were 4.170, 4.410,
3.586, 3.797ms; latch-wait means only 0.0025–0.0039ms. Thus aggregate evidence
places most update latency in TiKV prewrite service/completion, consistent
with the previous Rust wall profile. It does not prove all Rust client wait
is TiKV service time: these are unpaired means, background writes/retries
exist, and buffered metric flushes cross scrape boundaries (counts differ
from UPDATE totals). Go's external_Update prewrite counts 24868/24936 also
exceed logical updates. Do not subtract these means as exact per-request
network or scheduling costs.

A diagnostic difference remains: Rust samples primarily increment TiKV's
priority="unknown" RPC series, Go priority="medium". Both checked-in Rust
and pinned Go protobuf definitions encode CommandPri Normal=0, Low=1, High=2;
that label alone is not evidence of wrong protobuf priority or a safe tuning
opportunity. Its attribution still needs TiKV's exact metric implementation.
No protocol or priority change was made from this observation.

Run duration uses only go-ycsb's within-process timer; no invalid cross-process
monotonic subtraction was reused. Two samples per engine, unseeded mixes and
evolving shared data do not establish a speedup. Do not trade durability or
transaction semantics for benchmark throughput. Continue whole-package parity
and use stronger workload/profile evidence for any optimization.

Files changed in this checkpoint: this ExecPlan only. `bash -n` before run,
the aggregation checks above and `git diff --check` passed. Production source
is unchanged, so no new compile/lint/Bazel run was necessary. Exact TPC-H
numeric differentials, original test/variant coverage, package acceptance and
validated performance optimization remain open; goal remains active.


### 2026-09-21: physical row-bound arithmetic and MPP warning audit

Previous goal turn was progress: collected RPC metrics changed the next
performance action, showing most update time within TiKV prewrite. This turn
returned to package source auditing; package completion remains atomic/open.
Current tree and Go sources were re-read before editing.

Five source-exact additions used Rust's checked-in-debug `+` on u64 instead of
Go's wrapping uint64 operation. Updated physical/mod.rs candidate generation
for LogicalLimit and getPhysLimits, plan_cost_ver2.rs top_n_cost, and task.rs
LIMIT/TopN cop pushdown to wrapping_add. No saturation or new limits were
introduced. Root operators retain the original offset/count, pushed operators
use offset zero and the wrapped sum. Source anchors: physical_limit.go
ExhaustPhysicalPlans4LogicalLimit/getPhysLimits, core/plan_cost_ver2.go line563,
and core/task.go LIMIT/TopN attachment helpers.

New tests in physical/tests.rs, task.rs and plan_cost_ver2/golden_tests.rs
cover normal and boundary values, candidate count/properties, preserved root
bounds, pushed bounds and cost-floor behavior. Red command (from rust/):

    cargo test --offline --locked -j12 -p tidb-planner --lib uint64_row

/tmp/tidb-limit-overflow-red.log records three failing tests with arithmetic
overflow panics (candidate, costing, LIMIT pushdown). After the LIMIT fix,
TopN pushdown independently reproduced its overflow with:

    cargo test --offline --locked -j12 -p tidb-planner --lib cop_pushdown_wraps_uint64

/tmp/tidb-topn-push-overflow-red.log records that failure. The first TopN test
fixture lacked a column ordering expression and therefore intentionally did
not push; it was corrected to exercise Go's actual pushdown gate. After all
five changes the first command passed all three tests; evidence is
/tmp/tidb-limit-overflow-green.log. Broader required checks, from rust/:

    cargo test --offline --locked -j12 -p tidb-planner --lib physical::tests
    cargo test --offline --locked -j12 -p tidb-planner --lib task::tests
    cargo test --offline --locked -j12 -p tidb-planner --lib plan_cost_ver2::golden_tests

Results: 53, 18 and 36 passing tests respectively, no failures/ignores in
these selections. Logs /tmp/tidb-overflow-{physical-tests,task-tests,
plan_cost_ver2-golden_tests}.log. Only new test snippets were rustfmt-ed;
large existing files were not broadly reformatted. `git diff --check` passed.

The ordinary SQL builder in pinned logical_plan_builder.go:2586 clips count
to MaxUint64-offset before producing LogicalLimit. Thus these native API tests
must not be presented as an ordinary SQL crash reproduction. Other unchecked
row-bound arithmetic in logical UNION/join/CTE rewrites remains to audit with
its own source and reachability conditions. No performance improvement claimed;
normal-range arithmetic results are unchanged. Existing release benchmark
binary predates these new code edits and is not current validation evidence.

Warning audit: physical_max_one_row.go calls RaiseWarningWhenMPPEnforced on
sorted or MPP property rejection. Rust physical/mod.rs documents its omission.
Go SessionVars.IsMPPEnforced checks allowMPP && enforceMPP; RaiseWarningWhenMPPEnforced
uses AppendWarning for InExplainStmt and AppendExtraWarning otherwise. Rust's
statement warning buffer and physical dispatch have no equivalent complete
routing path; planner_bridge currently does not pass allowMPP to dispatch,
even though dispatch offers with_mpp_allowed and session sysvars exist.
These are explicit follow-up integration gaps. No warning-only callback or
incorrect SHOW WARNINGS behavior was added. Existing ignored MPP integration
tests cannot be used as coverage; their stale descriptions need reconciliation
as full MPP integration is completed.

Files changed in this checkpoint: the five Rust files above (including tests
in task.rs) and this ExecPlan. No Go/Bazel/module changes; bazel_prepare and
Go failpoint mutation are not required for these Rust unit tests. `make lint`
was attempted and failed installing revive v1.2.1 (module does not contain
package). Existing-tool lint result is recorded below after completion.
No real TiFlash, full SQL integration, full Go package tests, workload rerun,
or whole-package acceptance was established by these scoped regressions.

Final lint result: `make -o tools/bin/revive lint` completed exit 0, including
revive and dashboard recipes, using the existing required-version binary.
Log /tmp/tidb-overflow-lint-existing.log. This does not make the separate
`make lint` installation failure a pass. Final `git diff --check` passed.
Whole-package parity and validated workload optimizations remain active work.


### 2026-09-21: preserve Go MPP session gates through physical planning

Previous goal turn was progress (five native arithmetic fixes plus red/green
regressions). This turn re-read current source and fixed the concrete session
integration gap found during that audit; no package completion is claimed.

Go SessionVars.IsMPPEnforced (pkg/sessionctx/variable/session.go:2099) returns
allowMPPExecution && enforceMPPExecution. Its setter permits disabling allow
while the stored enforce value remains ON. Rust's session optimizer_cost_env
previously copied only tidb_enforce_mpp, so it continued applying enforcement
cost discounts after MPP was disabled. The regression uses SQL SET to enable
both, capture a context, disable allow, and check the new context. It failed
at the expected assertion before the fix:

    cd rust
    cargo test --offline --locked -j12 -p tidb-session --lib mpp_enforcement_requires_both_session_switches

Red evidence /tmp/tidb-mpp-settings-red.log; green evidence
/tmp/tidb-mpp-settings-green.log. The final test covers all four switch pairs,
re-enabling allow with the retained enforce value, and immutability of the
previously captured statement context. No session-variable validation or
stored value is changed.

Implementation: CostSessionOpts now includes mpp_allowed (default true, as
Go). Session::optimizer_cost_env resolves both flags and stores effective
mpp_enforced as their conjunction. physical_plan_for_logical passes captured
mpp_allowed through DispatchContext::with_mpp_allowed, so existing TopN and
Expand candidate gates receive the actual session value rather than default
true. The boolean lives in the same owned statement snapshot as costing and
other optimizer settings; no new feature or alternate policy was added.

Files changed in this checkpoint:

- rust/crates/tidb-planner/src/plan_cost_ver2.rs
- rust/crates/tidb-session/src/stmt_ctx.rs (implementation and regression)
- rust/crates/tidb-executor/src/driver/planner_bridge.rs
- this ExecPlan

Broader validation, from rust/:

    cargo test --offline --locked -j12 -p tidb-session --lib stmt_ctx::tests
    cargo test --offline --locked -j12 -p tidb-planner --lib find_best_task::dispatch::tests
    cargo test --offline --locked -j12 -p tidb-planner --lib plan_cost_ver2::golden_tests

All 12, 29 and 36 tests passed respectively (77 total). Logs are
/tmp/tidb-mpp-{statement,dispatch,cost}-tests.log. This verifies statement
capture, existing dispatch behavior and cost goldens, not live TiFlash
execution. `git diff --check` passed. No Go/Bazel/dependency changes; no
bazel_prepare or Go failpoint mutation required. `make lint` again fails
installing revive1.2.1 before its recipes; /tmp/tidb-mpp-lint.log records it.
The existing-binary lint command is being completed separately.

Remaining acceptance risks: complete enforced-MPP normal/extra-warning routing,
other physical operator MPP gates and tasks, original integration fixtures,
and whole-package coverage. No workload speedup is claimed. The prior release
benchmark binary predates this and the arithmetic checkpoint; it must be
rebuilt before new performance measurements. Full goal remains active.

Existing-tool lint completed: `make -o tools/bin/revive lint` exited 0, log
/tmp/tidb-mpp-lint-existing.log. This bypasses only the failed installation
prerequisite, not the lint recipes. Final diff whitespace check passed.


### 2026-09-21: MPP dependency audit and direct TPC-H comparison

Previous turn was progress: session MPP gates now retain Go semantics and
77 scoped tests pass. At that audit point LIMIT's MPP candidate and the direct
MPP-to-root reader conversion were both missing. The latter is now closed by
the focused `MppTask::into_root_task` receipt recorded below; LIMIT's MPP
attachment, multi-fragment ExchangeReceiver, per-scan partition metadata,
and the remaining MPP task fields remain open. No package completion is
claimed from the direct conversion alone.

To strengthen outstanding workload correctness evidence meanwhile, rebuilt:

    cd rust
    cargo build --offline --locked -j12 --release -p tidb-server --bin tidb-server

Passed in 56.36s; /tmp/tidb-tpch-exact-build.log. Retrieved query.go from pinned
go-tpc d05fdf8aaddcd5ae30e02333eba3760c37219c05. Initial guessed queries.go path
returned 404; then the pinned GitHub directory listing located query.go.
All 22 qN raw SQL definitions match that revision's mysql map. Source is
retained as /tmp/tidb-tpch-exact-mocjafvl/query-source.go, with q1.sql–q22.sql.
Q15 retains the benchmark's CREATE VIEW/query/DROP VIEW sequence.

Fresh-cluster exact-output harness uses the existing owned TiUP lifecycle and
current-source production Go binary, prepares/analyzes SF1, then runs each
query through Go and Rust using the same stock MySQL client in batch mode.
Headers and escaped field text are retained, as are stderr and failure markers.
Query errors/differences do not stop coverage of later queries. Comparison is
byte-for-byte first; differing decimals, formatting or ties must be inspected
before claiming semantic failure. This is stronger than go-tpc's tolerance
checker but is not automatic proof of type/warning parity. No timed speedup
is claimed from this Go-first diagnostic order.

Launch (repo root, approved local cluster execution):

    SYSBENCH_RUST_SERVER=/Users/qiliu/projects/tidb/rust/target/release/tidb-server SYSBENCH_AUTH_USER=root SYSBENCH_AUTH_HOST='%' SYSBENCH_SAMPLES=1 SYSBENCH_OUT_DIR=/tmp/tidb-tpch-exact-mocjafvl bash /tmp/tidb-tpch-exact-mocjafvl/run.sh

Live unified-exec session: 98737, confirmed running by write_stdin. Startup,
handshake and fixture creation passed; SF1 prepare/analyze is in progress.
Do not restart on an observation timeout. Poll this handle, inspect console.log
and prepare.log, then classify all 22 outputs/errors, verify pinned binary
hashes in binaries.json, and verify terminal cleanup before reporting results.
The cleanup trap owns Rust/playground servers and data and checks its ports.

Files changed this checkpoint: this ExecPlan only; no production changes.
`bash -n /tmp/tidb-tpch-exact-mocjafvl/run.sh` passed. Build passed. Full result
comparison and cleanup are pending. Whole-package and workload goals remain
active without narrowing their acceptance criteria.


### 2026-09-21: verified live TPC-H wait and original fragment coverage

Resumed existing session 98737 (no duplicate cluster). write_stdin confirms
it remains live. SF1 loading completed; analysis progressed through lineitem,
partsupp and supplier, and is currently on part. No query output is available
yet. This is a verified wait, not completion or a blocker. The existing
cleanup trap remains responsible for owned processes/data. Continue polling
this same session; /tmp/tidb-tpch-exact-mocjafvl is the authoritative run.

Prepared /tmp/tidb-tpch-exact-mocjafvl/summarize.py, which records all 22
queries as pending until outputs exist, separates execution failures, header
differences, row-order differences and value differences, and checks binary
hashes. It records exact text matches only, never automatically promotes
numeric formatting or reordered rows to semantic parity. Because files may
be partial while a query runs, final classification must wait for the terminal
session and completed-query markers. Current summary is 22 pending and
unchanged binaries. No result acceptance yet.

Independent source audit checked both original fragment_test.go tests against
the pre-existing untracked rust/crates/tidb-planner/src/fragment.rs draft.
It mirrors the singleton exchange matrix and local CTE task-count scenario,
but lib.rs has no fragment module declaration. The draft refers to CTESink,
CTESource and ExchangeSender.tasks, absent from the actual physical enum and
sender. Thus these bodies were never included in the scoped test binaries.
They remain incomplete seed evidence, not tests passed or package integration.
This confirms an original-test coverage gap that must be closed together with
the package's real MPP/CTE representation and fragment-generation integration.
No draft source was silently integrated or deleted.

Only this ExecPlan changed. `git diff --check` passes. No production change
or new compilation was performed in this waiting/audit checkpoint. Whole
package acceptance, exact TPC-H comparison and workload optimization stay open.


### 2026-09-21: exact SF1 TPC-H differential completed

Previous turn was a verified wait plus an original-test coverage audit. This
turn resumed confirmed-live session 98737, observed analysis completing and
all queries executing, then verified terminal exit 0 and cleanup. No restart,
new cluster or production source change was made.

All 22 SQL definitions from pinned go-tpc commit
 d05fdf8aaddcd5ae30e02333eba3760c37219c05 completed on both servers. Each pair's
full stock-MySQL batch output is byte-identical, including column headers,
decimal digit strings and row ordering. No tolerance, numeric normalization,
sorting or dropped columns were used to obtain equality. Q15's view lifecycle
also completed through both engines. Per-query row counts (excluding headers):

    Q1 4; Q2 100; Q3 10; Q4 5; Q5 5; Q6 1; Q7 4; Q8 2;
    Q9 175; Q10 20; Q11 838; Q12 2; Q13 42; Q14 1; Q15 1;
    Q16 18333; Q17 1; Q18 9; Q19 1; Q20 184; Q21 100; Q22 4.

Validation command from repo root:

    python3 /tmp/tidb-tpch-exact-mocjafvl/summarize.py
    git diff --check

Follow-up assertions independently verified 22 EXACT completion markers in
console.log, no .failed files, no ERROR/failed/panic in any of 44 query stderr
files, all binary SHA256 values still matching binaries.json, terminal exit 0,
and absence of owned TiUP directory sysbench-ladder-51469-1789994537. The
harness cleanup checks all five ports after stopping its own servers. The
final summary.json includes these assertions, query row counts and full-output
hashes. It deliberately keeps full_parity_proven=false and
performance_acceptance=false. No benchmark process remains from this run.

Artifacts: /tmp/tidb-tpch-exact-mocjafvl/{query-source.go,qN.sql,qN-go.tsv,
qN-rust.tsv,qN-go.err,qN-rust.err,summary.json,binaries.json,run.sh,console.log}.
The preceding build and launch commands remain in the previous checkpoint.
This result supersedes the earlier tolerance-only SF1 check for this dataset,
but not every TPC-H data scale, parameter set or session mode. MySQL batch
output does not prove complete wire field metadata or warnings. This Go-first
correctness run was not a controlled performance comparison. No speedup claim.

Only this ExecPlan changed in this checkpoint, so no additional compilation,
lint or Bazel gate was needed. The current release was rebuilt immediately
before the run and includes the arithmetic and MPP-setting fixes. Whole
physicalop/original-test/variant acceptance and validated sysbench/TPC-C/
TPC-H/YCSB performance optimization remain outstanding. Goal stays active.


### 2026-09-21: windows package closure audit finds approximate OVER gap

Previous turn was progress: exact SF1 TPC-H output verification completed.
This turn returned to the complete six-artifact windows package at aba629bb455.
Re-read builder.go and normal/pipelined scheduling, row-bound, partial-result,
chunk-alias and reset paths. No build tags/platform-generated variants or
failpoint/testfailpoint calls/dependencies occur in the six package artifacts.
The failpoint-test-runner workflow therefore selects an ordinary tagged run;
no Go/Bazel/module source changed, so bazel_prepare is not triggered.

Full original Go package test set, from repo root:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -run '^(TestWindowFunctions|TestWindowFunctionsDataReference|TestSlidingWindowFunctions|TestIssue45964And46050|TestVarSampAsAWindowFunction|TestWindowExecutorsBasic|TestBuildOrderedWindowExec|TestWindowReturnColumnNullableAttribute)$' -tags=intest,deadlock -count=1 ./pkg/executor/windows

All eight original tests passed in 3.433s; log
/tmp/tidb-windows-acceptance-go.log. Rust validation before new changes (rust/):

    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    cargo test --offline --locked -j12 -p tidb-executor --lib window::
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source

64, 4 and 13 tests passed respectively. The four `window::` tests cover retained
row storage; they must not be described as the whole executor window surface.
Logs /tmp/tidb-windows-acceptance-{session,unit,boundary}.log. From repo root:

    python3 rust/scripts/generate-go-window-tests.py --check

Verified all 198 original extracted actions without fixture edits.

Additional dependency audit exercised JSON_ARRAYAGG, JSON_OBJECTAGG,
APPROX_COUNT_DISTINCT, BIT_AND/OR/XOR over three moving ROWS frames and both
pipeline modes (36 queries, four output rows each). A temporary Go test
overlay preserved the tracked Go package and printed exact JSON-encoded rows:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-window-closure-overlay.json -run '^TestWindowAggregateClosureOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

Passed; /tmp/tidb-window-closure-oracle.log retains 36 oracle records, with
identical results between both scheduler modes. Rows include NULL input,
duplicate JSON object keys, empty leading/trailing frames and bitwise defaults.
A matching Rust SQL matrix failed before executing approximate-count windows:
/tmp/tidb-window-closure-rust.log records a parse error at OVER. JSON cases had
already passed. This is an actual SQL integration gap, not an invented window
feature or a scheduler failure.

Pinned Go pkg/parser/expr_func_parser.go parseAggregateFuncCall uses the same
argument modifiers for approximate aggregates and parseFuncCall wraps OVER.
Rust still had an older SumExpr-specific branch forbidding modifiers and OVER.
Removed that branch and admitted multiple args in the shared arity switch.
Both APPROX_COUNT_DISTINCT and APPROX_PERCENTILE now follow the current Go
parser. Type/argument/window legality remains the existing planner's decision.
An old session test expecting a syntax error was replaced with the current
DISTINCT-window planner refusal; the successful SQL matrix covers OVER.

Independent Go parser oracle:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go run /tmp/tidb-window-approx-parser-oracle.go

Confirmed 36 forms across both names, DISTINCT, DISTINCTROW, ALL, star,
per-argument ALL, DISTINCT ALL, and absent/inline/named OVER; both empty-arg
forms rejected. /tmp/tidb-window-approx-parser-oracle.log. Matching Rust parser
matrix failed red (/tmp/tidb-window-approx-parser-red.log), then passed green.
Final parser command (rust/):

    cargo test --offline --locked -j12 -p tidb-parser --lib

734 passed; /tmp/tidb-window-closure-parser-all.log. The new session matrix
passed all 36 oracle comparisons after the fix. It was renamed from
window_non_sliding_aggregate_frames_match_go to
window_aggregate_frame_matrix_matches_go because BIT_XOR uses inverse sliding.
The test retains 18 expected result sets and exercises each under both modes;
no tolerance or unordered comparison. The broader 65-test session run first
found the obsolete syntax-error expectation above; its final rerun is recorded
below. No Go fixture was rewritten to accommodate Rust.

Files changed: rust/crates/tidb-parser/src/{expr/window.rs,tests/expr.rs},
rust/crates/tidb-session/src/tests_window/{aggregates.rs,specs.rs}, and this
ExecPlan. Only new/modified test snippets were formatted. make lint still fails
installing revive1.2.1; the existing-binary lint recipe result is recorded below.
The Rust parser change does not require Go parser generation Make targets.

This closes a real windows integration gap but does not accept the entire
parser or windows package. Remaining dependency/type/error/warning contracts
and package validation must be audited as a whole, with full source/support
inventory. Approximate percentile execution and all wire metadata were not
proved by this count/JSON/bitwise matrix. No performance change is claimed;
release binaries and previous workload results predate this parser fix.

Final broader window validation passed: `cargo test --offline --locked -j12
-p tidb-session --lib tests_window` selected 65 tests, all passing, no ignores
or failures (/tmp/tidb-window-closure-session-all.log). Existing-tool lint
`make -o tools/bin/revive lint` exited 0; log
/tmp/tidb-window-closure-lint-existing.log. This does not convert the separate
make lint bootstrap failure into a pass. Final `git diff --check` passed.
No test or cluster process remains from this checkpoint. Goal remains active.


### 2026-09-21: approximate percentile DISTINCT and reachable window results


Continued the windows dependency audit at aba629bb455. The parser correction
made ordinary approximate DISTINCT and approximate OVER queries reachable, but
rust/crates/tidb-session/src/tests_json.rs still asserted the older syntax
rejection. Checked current Go pkg/executor/aggfuncs/builder.go:
buildApproxPercentile never consults HasDistinct, and its supported numeric and
temporal accumulators rank every input row. Unsupported input evaluation types
return NULL. This behavior is preserved even though DISTINCT syntax might
suggest otherwise; changing Go semantics would violate this goal.

Used a temporary overlay of the existing windows test file, without changing
tracked Go/Bazel sources. From repository root:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-approx-reachability-overlay.json -run '^TestApproximateAggregateReachabilityOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

Passed in 0.417s. /tmp/tidb-approx-reachability-oracle.log retains 20
REACHABILITY_ORACLE records (10 queries under both pipeline settings), and
/tmp/tidb-approx-reachability-oracle.go retains the source. Results include
case-insensitive approximate distinct counts, ordinary DISTINCT percentile,
cumulative and moving approximate count/percentile frames, empty trailing
frames, and string percentile NULLs. Existing failpoint audit still applies;
no Go/Bazel dependency or file change requires bazel_prepare.

On [1,1,1,9], Go returns [1,1] for ordinary and DISTINCT percentiles at 75%.
New Rust SQL test approximate_aggregate_distinct_modifier_matches_go_builder
failed with [1,9]; /tmp/tidb-approx-distinct-red.log. A first local change to
AggFunc::from_descriptor alone did not fix SQL: production physical_builder
constructs AggFunc separately. Its intermediate failure also occupied
/tmp/tidb-approx-distinct-green.log; that file now contains the final pass.
The original red log is retained. Both builders now clear effective DISTINCT
only for ApproxPercentile. Approximate count keeps its existing correct
collation behavior. No generic aggregate, spill or partial-state policy was
changed.

Changed rust/crates/tidb-executor/src/hash_agg/builder.rs and
rust/crates/tidb-executor/src/driver/physical_builder.rs. Extended
hash_agg.rs::approx_percentile_uses_ordinal_selection to build descriptors
with/without DISTINCT and execute real HashAggExec rows, proving the helper
path independently. It is compiled but has no production callers today; do
not describe it as another wired SQL path. Updated tests_json.rs obsolete
syntax expectations to exact Go results and the actual DISTINCT-window
planner refusal, added the duplicate regression, and exercised six window
queries under both schedulers. Only these modified test functions were
formatted, avoiding unrelated file churn.

Validation, from rust/ (all passed):

    cargo test --offline --locked -j12 -p tidb-session --lib approximate_aggregate_distinct_modifier_matches_go_builder
    cargo test --offline --locked -j12 -p tidb-session --lib tests_json::
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg::
    cargo test --offline --locked -j12 -p tidb-executor --lib approx_percentile_uses_ordinal_selection

Counts were 1, 15, 65, 70, and 1 respectively. The final descriptor extension
was added after the 70-test run and passed its selected test. Logs:
/tmp/tidb-approx-distinct-green.log, /tmp/tidb-approx-json-tests.log,
/tmp/tidb-approx-window-tests.log, /tmp/tidb-approx-hash-agg-tests.log,
/tmp/tidb-approx-descriptor-tests.log. The session test formatting happened
after the JSON run; no executable test behavior changed during formatting.

The complete windows/aggfuncs/physicalop package acceptance remains open:
this work closes a concrete compatibility gap and supplies dependency
regressions, not a new package-completion claim. Release binaries and all
previous workload samples predate these approximate-parser/executor changes.
No benchmark speedup, wire metadata equivalence, or full partial/distributed
aggregate execution is claimed. Goal remains active.

Final repository validation (repository root):

    make lint
    make -o tools/bin/revive lint
    git diff --check

make lint exited 2 at the known revive1.2.1 installation error (module found
but package absent), /tmp/tidb-approx-reachability-lint.log. The existing-tool
lint recipe exited 0, /tmp/tidb-approx-reachability-lint-existing.log; this is
reported separately and does not turn the bootstrap failure into a pass.
git diff --check passed. Self-review confirmed only percentile changes its
effective modifier, preserving existing DISTINCT handling for other aggregates.
No cluster or test process remains from this checkpoint.


### 2026-09-21: RANGE cursor evaluation and temporal overflow diagnostics


Previous checkpoint was verified progress: approximate percentile DISTINCT
now matches Go. Re-audited windows/window.go, pipelined_window.go and builder.go
at aba629bb455, together with Rust window.rs, window/pipelined.rs, and the
physical builder. Go getStartOffset/getEndOffset and getStart/getEnd evaluate
comparison operands only inside the candidate-row loop. Rust range_bound
instead eagerly evaluated a Vec of every target, even after its monotonic
cursor reached the partition end. It also evaluated all keys before the
first unequal-key short circuit.

Reproduced with BIGINT keys [1,9223372036854775807] and upper RANGE offset
9223372036854775806 FOLLOWING. The first row consumes the entire upper-bound
search; Go skips the next overflowing addition. Rust incorrectly returned
1690. New test window_range_exhausted_cursor_skips_boundary_evaluation failed
red (/tmp/tidb-range-exhausted-red.log), then passed green
(/tmp/tidb-range-exhausted-green.log). Evaluation now happens per candidate,
left operand first as in Go CompareInt/CompareTime and other comparison
functions; end bounds evaluate CalcFuncs first, start bounds CompareCols first.
The eager temporary target vector is gone, but no performance gain is claimed.

A temporary Go test overlay of window_sql_test.go supplies three independent
window oracles. Tracked Go/Bazel/module files were unchanged. Source:
/tmp/tidb-range-exhausted-oracle.go; overlay:
/tmp/tidb-range-exhausted-overlay.json. From repository root:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-range-exhausted-overlay.json -run '^TestRange(ExhaustedBound|BoundDirections)Oracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-range-exhausted-overlay.json -run '^TestRangeBoundaryWarningsOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

Both passed; /tmp/tidb-range-directions-oracle.log (0.762s) and
/tmp/tidb-range-warnings-oracle.log (0.419s). The first has twelve exact result
cases across both schedulers: exhausted lower/upper bounds, signed/unsigned
keys and descending order. Two controls still raise 1690 for a needed bound.
An initial unsigned fixture used an offset above MaxInt64; Go rejected it at
planning with 3586, so the final execution fixture uses unsigned keys near
MaxUint64 with legal offset 1. No Rust expectation was invented for the
rejected query. Another initial testkit helper checked compile-time error
rather than draining runtime rows; QueryToErr captured the actual 1690.

For DATETIME keys [9999-12-30,9999-12-31,9999-12-31], one-day FOLLOWING reaches
the upper cursor end before overflow: counts [3,2,2], no warnings. Two-day
FOLLOWING yields [0,0,0], three warnings in normal mode and six in pipelined
mode. Rust's counts were correct but warnings were absent, reproduced in
/tmp/tidb-range-warnings-rust.log. The calendar helper returned NULL on actual
arithmetic overflow without consulting a warning context.

Threaded the existing Columns context from scalar_function.rs into the
shared calendar evaluator and composite interval evaluator. Arithmetic
out-of-range results now use Go baseDateArithmetical.addDate's 1441 diagnostic
and truncation-group policy: Warn records the diagnostic and returns NULL,
Error preserves the registered datatype error, Ignore suppresses it. SQL NULL
operands and operand-parsing failures do not automatically become overflow
warnings. The sessionless public date_add_interval helper retains its read
policy via NoColumns. No new SQL function or statement setting was added.
Other operand-parsing diagnostics remain an expression-package audit surface;
this is not whole DATE_ADD or whole expression-package acceptance.

Independent Go policy oracle, same overlay (repository root):

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-range-exhausted-overlay.json -run '^TestDateArithmeticOverflowPolicyOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

Passed; /tmp/tidb-date-overflow-policy-oracle.log. Fourteen interval cases
cover DAY/WEEK/MONTH/QUARTER/YEAR/HOUR/MINUTE/SECOND/MICROSECOND, composite
YEAR_MONTH/DAY_HOUR, and MaxInt64 DAY/HOUR/YEAR. All produce NULL with one 1441
warning. Strict INSERT SELECT returns 1441; INSERT IGNORE stores NULL with
one warning. Rust test date_arithmetic_overflow_uses_statement_error_policy
matches all cases and checks 22008 SQLSTATE and NULL-operand non-warning
controls; /tmp/tidb-date-overflow-policy-rust.log. Window warning regression
now passes (/tmp/tidb-range-warnings-green.log) including exact warning counts.

Files changed this checkpoint:
rust/crates/tidb-executor/src/window.rs,
rust/crates/tidb-expr/src/{scalar_function.rs,time_fn/calendar.rs},
rust/crates/tidb-session/src/tests_window/frames.rs,
rust/crates/tidb-session/src/tests_core/temporal_types.rs, and this ExecPlan.
Modified function regions were formatted without whole-file churn. Existing
unrelated worktree changes were preserved.

Final Rust validation from rust/:

    cargo test --offline --locked -j12 -p tidb-session --lib window_range_exhausted_cursor_skips_boundary_evaluation
    cargo test --offline --locked -j12 -p tidb-session --lib window_range_boundary_warning_evaluation_matches_go
    cargo test --offline --locked -j12 -p tidb-session --lib date_arithmetic_overflow_uses_statement_error_policy
    cargo test --offline --locked -j12 -p tidb-expr --lib time
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    cargo test --offline --locked -j12 -p tidb-session --lib tests_core::temporal_types
    cargo test --offline --locked -j12 -p tidb-session --lib tests_read_cast
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source

All three focused regressions passed. Broader counts: 202 expression passed
with 15 ignored, 67 window passed, 9 temporal session passed, 4 read-cast
passed, 13 executor boundary passed. Logs /tmp/tidb-range-temporal-expr-tests.log,
/tmp/tidb-range-window-suite.log, /tmp/tidb-range-temporal-session-tests.log,
/tmp/tidb-range-read-cast-tests.log, /tmp/tidb-range-window-boundary-tests.log.
The ignored expression entries include benchmark exclusions and explicit
parity gaps (cache/vectorization/etc.); they are not passing evidence and
were not silently enabled or removed.

Repository-root gates:

    python3 rust/scripts/generate-go-window-tests.py --check
    make lint
    make -o tools/bin/revive lint
    git diff --check

All 198 extracted original window actions remain unchanged. make lint failed
at the known revive1.2.1 installation bootstrap (/tmp/tidb-range-lint.log);
existing-tool lint exited 0 (/tmp/tidb-range-lint-existing.log). The two lint
outcomes remain separate. git diff --check passed. No Go/Bazel/module changes
triggered bazel_prepare, and the windows failpoint audit still requires no
failpoint toggling. Self-review checked evaluation ordering, NULL-vs-overflow
classification, existing statement policy use and the scoped diff.

No package integration/acceptance claim, release rebuild, wire-metadata gate,
or workload performance comparison was performed here. Original windows
source remains the same six-artifact atomic unit; required dependency and
whole-package acceptance is still open. No cluster was started and every test
and lint process from this checkpoint is terminal. Goal remains active.


### 2026-09-21: window partition identity and group-checker evaluation


Previous turn made verified progress on RANGE evaluation and temporal warning
policy. Current checkout remains aba629bb455. Audited windows package
lifecycle, group consumption, and the complete four-artifact dependent package
pkg/executor/internal/vecgroupchecker. Its production source, original test
file, main test harness and BUILD.bazel are now inventoried with SHA-256 hashes
in physicalop-source-inventory.md. No doc.go, platform/build tags, generated
inputs or extra fixtures exist in that package at this pin. The whole package
remains the minimum acceptance unit, and is not yet accepted.

Go VecGroupChecker distinguishes encoded first/last chunk keys from adjacent
row comparison. Numeric JSON values 1 and 1.0 compare equal within a chunk but
retain different encoded keys across chunks. Windows used its own generic
comparison everywhere and lost this Go behavior. Independent SQL oracle
created 32 JSON integer 1 rows followed by JSON floating 1.0, ordered by id.
With tidb_max_chunk_size=32, Go returns partition counts 32 for rows 1..32 and
1 for row 33. With size=64, all counts are 33. Both ordinary and pipelined
windows agree on each scenario. The new Rust SQL regression failed red with
33 for every row in the size=32 case; /tmp/tidb-window-json-boundary-red.log.

Go oracle source /tmp/tidb-window-json-boundary-oracle.go, overlay
/tmp/tidb-window-json-boundary-overlay.json. Repository-root command:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-window-json-boundary-overlay.json -run '^TestWindowJSONPartitionBoundaryOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

All four scenarios passed; /tmp/tidb-window-json-boundary-oracle-final.log.
Normal Window now retains a VecGroupChecker and collects whole adjacent ranges,
using encoded-key continuation when it fetches another child chunk. Pipelined
Window uses the same checker and Go's retained-row/new-partition decision.
Removed its separate partition-key vector and per-row partition comparison.
Ranking peer comparisons remain their own Go contract. Open recreates the
checker for native executor reuse; retained rows and queued output aliases
retain their existing ownership rules.

The shared checker itself still evaluated every row before considering Go's
single-group shortcut. This changes observable warnings, not just performance.
Casting 32 copies of '1bad' to integer produces two warnings in Go (first and
last rows), versus 32 in the previous Rust implementation. A new native
regression reproduced the 32-versus-2 failure; /tmp/tidb-group-boundary-red.log.
An initial test compile failed because ScalarFunction needed its module path;
this was corrected before capturing the behavioral red result.

Checker split_into_groups now evaluates first/last operands in Go's per-item
order, encodes them, and returns early if they are equal. Otherwise it evaluates
one key column at a time and reuses a boolean row mask. This removes the
Vec allocation per input row and the clone of the collation array. Appending
a final '2bad' row requires the full pass and produces 35 warnings (two
endpoints plus 33 rows) in both Go vectorization modes and Rust. No measured
sysbench/TPC-C/TPC-H/YCSB speedup is claimed from this structural reduction.

Go package oracle uses a temporary overlay preserving tracked sources:
/tmp/tidb-group-boundary-oracle.go and
/tmp/tidb-group-boundary-overlay.json. Repository-root command:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-group-boundary-overlay.json -run '^(TestGroupCheckerBoundaryEvaluationOracle|TestVecGroupChecker.*|TestIssue53867)$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker

Passed all four original tests plus the new oracle in 0.034s;
/tmp/tidb-group-boundary-oracle-final.log. Read the failpoint-runner skill and
its decision playbook for this newly audited package: no failpoint. or
testfailpoint. calls or Bazel failpoint dependency occur. main_test.go's
client failpoint registration is part of the original harness, not a test
requiring injected failpoints. No toggling was needed. No Go/Bazel/module
source edit triggers bazel_prepare.

Original source ports in tests_executor_internal_source.rs previously bypassed
production expression/chunk evaluation through split_evaluated. Updated datum
ownership, collation/padding and reset tests to exercise real chunks and
split_into_groups. Their native owned-data and fixed-plan-metadata choices
are documented in the inventory. The six count-matrix scenarios already
exercise production chunks. The older pre-evaluated fixtures are now compiled
only for tests; they are supplementary evidence, not acceptance of the runtime
entrypoint.

Files changed this checkpoint:
rust/crates/tidb-executor/src/{window.rs,window/pipelined.rs,vec_group_checker.rs,tests_executor_internal_source.rs},
rust/crates/tidb-session/src/tests_window/collation.rs,
and both physicalop ExecPlan/inventory documents. Existing unrelated worktree
changes were preserved. Only modified function regions were formatted.

Rust commands, from rust/:

    cargo test --offline --locked -j12 -p tidb-session --lib window_json_partition_boundary_uses_encoded_identity
    cargo test --offline --locked -j12 -p tidb-executor --lib equal_boundary_keys_skip_interior_evaluation_warnings
    cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    cargo test --offline --locked -j12 -p tidb-executor --lib issue_53867
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    cargo test --offline --locked -j12 -p tidb-executor --lib shuffle::
    cargo test --offline --locked -j12 -p tidb-executor --lib window::
    cargo test --offline --locked -j12 -p tidb-executor --test all window_executor_source

Final results: four-scenario JSON regression passes; 13 checker-selected tests
and the separately selected original reset test pass; 68 session window,
24 shuffle, four retained-row/pipeline unit, and 13 window executor boundary
tests pass. No selected tests ignored. Logs:
/tmp/tidb-window-json-boundary-final.log,
/tmp/tidb-window-grouping-checker-final.log,
/tmp/tidb-group-checker-reset-tests.log,
/tmp/tidb-window-grouping-session-final.log,
/tmp/tidb-window-grouping-shuffle.log,
/tmp/tidb-window-grouping-unit-final.log,
/tmp/tidb-window-grouping-boundary-final.log.
An initial shell invocation used the repository root instead of rust/ and
failed to locate Cargo.toml; no test ran until the corrected command above.
The final source-port and test-only-helper edits were followed by the checker,
reset and JSON regression selections; the final warning-count extension was
followed by checker/window unit/boundary selections. Broader window/shuffle
results correspond to the same production algorithm before formatting.

Repository gates:

    python3 rust/scripts/generate-go-window-tests.py --check
    make lint
    make -o tools/bin/revive lint
    git diff --check

Fixture check confirms 198 actions unchanged. make lint still fails at the
revive1.2.1 install bootstrap; /tmp/tidb-window-grouping-lint.log. Existing-tool
lint passes separately; /tmp/tidb-window-grouping-lint-existing.log. Diff check
passes. Self-review checked the group-checker reset, prior encoded key lifetime,
first/last shortcut, pending group ownership and constructor/reopen behavior.

No cluster was started; all oracle/test/lint processes are terminal. Neither
windows nor vecgroupchecker receives whole-package acceptance: grouped stream
aggregation retains its own checker algorithm, and remaining complete-package
integration and validation decisions are open. No release rebuild or workload
performance result was produced. Goal remains active.


## Stream aggregation shared checker follow-up (2026-09-21)

The whole vecgroupchecker package remains unaccepted while auditing its stream
aggregation caller. Go StreamAggExec calls the shared checker; Rust's
GroupedStreamAggExec duplicates it using hash grouping encoding. A temporary
Go windows-package SQL oracle confirms JSON 1 and 1.0 group together within
one 64-row chunk but split into counts 32 and 1 across a 32-row boundary.
The plan contains root StreamAgg in both cases. The session regression must
fail before replacing the duplicate implementation with VecGroupChecker.
Preserve the native integer-column fast path by moving it into the shared
checker, including NULL and selected-row behavior. Validate stream aggregation,
all checker consumers, and repository lint. No whole aggregate-package claim
is implied; memory, error ordering, failpoints and other package artifacts
remain subject to the atomic inventory and acceptance gates.


### Stream follow-up results and decisions

Go oracle command, repository root:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-stream-json-overlay.json -run '^TestStreamJSONBoundaryOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

Passed in 0.433s. /tmp/tidb-stream-json-oracle.log records root StreamAgg
plans and exact result rows [[1,32],[33,1]] at chunk size 32, [[1,33]] at 64.
The temporary overlay appends the oracle to the unchanged Go windows test
source. No tracked Go/build/module artifacts changed and no bazel_prepare
trigger was introduced. The windows package's previously audited failpoint
prerequisite remains unchanged; no aggregate-package injected-failure test
was run or counted.

Regression stream_aggregate_json_boundary_uses_encoded_identity failed
before implementation: Rust [[1,33]] versus Go [[1,32],[33,1]], in
/tmp/tidb-stream-json-red.log. It passes after GroupedStreamAggExec calls the
shared checker. Both chunk sizes are permanent SQL regression scenarios and
assert the physical plan actually contains StreamAgg.

A second regression, grouped_stream_agg_stops_at_required_rows_at_a_chunk_boundary,
failed with req.num_rows() > required_rows using input chunks [1,1] and [2,3]
and requested output size one; /tmp/tidb-stream-required-red.log. Go's
StreamAggExec.Next consumes/emits one group per loop and checks req.IsFull.
Rust previously emitted the previous chunk's group and the next chunk's first
group in the same iteration. It now returns when the prior group fills the
request, preserving the fetched chunk and deferring the next group's state
initialization. The final counts are 2,1,1 in three one-row requests.

Decision: remove the independent stream group algorithm rather than maintain
two encodings. Keep integer direct-column comparison by moving it into the
shared checker. It compares physical selected-row indices and NULL bits, avoids
per-row datum allocation, and uses the generic evaluator for other expressions.
The new selected-row/null fixture exercises all six existing integer type
codes and continuation into the next chunk. No new operator or SQL feature.
This preserves the existing stream integer optimization and makes it available
to window/shuffle; no measured workload speedup is claimed.

Changed this checkpoint: rust/crates/tidb-executor/src/hash_agg.rs,
rust/crates/tidb-executor/src/vec_group_checker.rs,
rust/crates/tidb-session/src/tests_explain_merge_join.rs, this ExecPlan and
physicalop-source-inventory.md. Unrelated preexisting work remains intact.
The inventory now lists all ten artifacts of dependent package executor/aggregate
with explicit open audit/validation decisions, not partial-package acceptance.

Validation commands, from rust/ unless stated:

    cargo test --offline --locked -j12 -p tidb-session --lib stream_aggregate_json_boundary_uses_encoded_identity
    cargo test --offline --locked -j12 -p tidb-executor --lib grouped_stream_agg_stops_at_required_rows_at_a_chunk_boundary
    cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    cargo test --offline --locked -j12 -p tidb-executor --lib shuffle::

After the requested-row fix, the following also passed from repository root:

    cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-executor --lib grouped_stream_agg

Four stream unit tests passed there. The broader final aggregate selection
passed all 76 tests, including that same regression, from rust/. Final suite
logs: /tmp/tidb-stream-{checker,session,hashagg,window,shuffle}-final.log,
with 14,15,76,68,24 tests respectively; none ignored. Separate green logs are
/tmp/tidb-stream-json-green.log and /tmp/tidb-stream-grouped-final.log.
An initial green command accidentally ran without a manifest from the repo
root and found no Cargo.toml; no test was counted until the corrected run.

Repository gates:

    make lint
    make -o tools/bin/revive lint
    git diff --check

make lint exits 2 in the preexisting revive1.2.1 installation step (module
found but missing the requested package); /tmp/tidb-stream-lint.log. Lint
recipes pass with the existing binary via the second command;
/tmp/tidb-stream-lint-existing.log. Diff check passes. Self-review covered
shared mask reset, selected-row integer comparison, group continuation,
required-row suspension, final group flush, and preserving unrelated changes.

Not verified here: complete aggregate/vecgroupchecker/physicalop package
acceptance, aggregate memory/failure/error ordering, full merge-join group
integration, release rebuilds, or new sysbench/TPC-C/TPC-H/YCSB measurements.
All test/lint processes are terminal and no cluster was started. Goal active.


## User-requested publication checkpoint (2026-09-21)

The user explicitly instructed: "do not forget to commit and push code".
This authorizes a progress checkpoint on origin/hparser-integration, superseding
the earlier plan decision to defer every commit until package acceptance.
Whole-package completion criteria remain unchanged and unmet. Preserve the
existing unconnected fragment.rs and vs_helper.rs drafts locally; neither is
declared by its crate and neither belongs in the validated checkpoint.
The checkpoint includes the accumulated connected Rust parity work, its
regression tests, generated window fixtures and generation script, and both
package audit documents. Validate the staged-only tree independently before
pushing normally. Do not force-push. Existing lint bootstrap and package gaps
must be disclosed in the commit and final report. The goal remains active.


Publication validation passed on the isolated staged-only checkout at
/private/tmp/tidb-parity-publish-aba629bb, based on aba629bb45. All 56 staged
files were verified byte-for-byte against that checkout before testing.
Excluded fragment.rs and vs_helper.rs are absent there. Commands:

    # Isolated repository root:
    python3 rust/scripts/generate-go-window-tests.py --check
    git diff --check
    # Isolated rust/ directory, reusing only build artifacts:
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo check --offline --locked -j12 -p tidb-exec -p tidb-executor -p tidb-expr -p tidb-parser -p tidb-planner -p tidb-server -p tidb-session
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib stream_aggregate_json_boundary_uses_encoded_identity
    # Original repository, staged tree:
    git diff --cached --check

All seven affected crates compile (31.82s); the isolated regression passes
(1m02s build, 0.04s test); 198 generated actions match. Logs are
/tmp/tidb-parity-publish-check.log and /tmp/tidb-parity-publish-test.log.
Existing warnings were emitted. This publication step adds no production
changes, package-acceptance claims, or benchmark claims. The only post-test
staged edit is this validation receipt. Earlier 197-test evidence and the
make lint bootstrap limitation remain applicable to the same source tree.


## Merge-join shared group checker follow-up (2026-09-21)

Previous turn made progress by committing/pushing 7c79401941. The remaining
merge-join consumer still has independent datum-based chunk continuation.
A pinned Go SQL oracle confirms JSON 1/1.0 inner input gives COUNT/SUM 32/528
at chunk size 32 and 33/561 at 64 for both left and right outer joins.
The plans contain MergeJoin. Reproduce this mismatch in a permanent Rust SQL
regression before replacing the duplicate grouping with VecGroupChecker.
Retain existing native spill ownership, NULL inner-key skipping, outer filters,
required-row output and opposite-side typed comparison. Audit/check all nearby
merge tests and shared consumers; package acceptance remains whole and open.
Commit/push verified progress as the user requested, without folding the two
unconnected drafts into the work.


### Merge-join validation receipt and sorting discovery

The initial SQL regression failed before the fix: Rust COUNT/SUM 33/561,
Go 32/528 at chunk size 32; /tmp/tidb-merge-json-red.log. The fix removes the
independent merge-row grouping and copied last-datum comparison. Both native
MergeSide instances own VecGroupChecker with the side's typed column keys.
Outer filters still run before splitting; inner NULL groups are skipped;
completed inner ranges transfer to the existing spillable RowContainer.
A new chunk's first group remains unconsumed when its encoded key differs
from the retained prior group. Cross-side join comparison and pending output
logic remain unchanged. Shared integer direct-column comparisons are reused.

An expanded Go run exposed nondeterministic equal-JSON-key order from parallel
Sort workers: one 32-row left-join case returned 33/561 instead of 32/528.
Thus the original SQL capture alone did not establish a deterministic fixture.
Set tidb_executor_concurrency=1 in both oracle and regression to make sorting
reproducible, while independently testing tidb_merge_join_concurrency=1 and 4.
Both Go and Rust plans show Shuffle only at merge concurrency 4, asserted by
the permanent Rust regression. This does not change production sort policy.
The earlier stream JSON fixture has the same equal-key sort sensitivity; its
test now also sets executor concurrency to one and its Go oracle was repeated.
The window fixture orders by id as well and does not have that equal-key tie.

Five Go repetitions produce 40 merge records: both join directions at chunk
size 32 give 32/528, and both at size 64 give 33/561, for merge concurrency
1 and 4. Five stream repetitions give [[1,32],[33,1]] at 32 and [[1,33]] at 64.
These are grouping-contract fixtures for the controlled input order, not a
claim of one universal result across unspecified equal-key sort orders.

Go commands from repository root, using temporary overlays (tracked Go files
are unchanged):

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-merge-json-overlay.json -run '^TestMergeJSONBoundaryOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-merge-json-overlay.json -run '^TestMergeJSONBoundaryOracle$' -tags=intest,deadlock -count=5 -v ./pkg/executor/windows
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-stream-json-overlay.json -run '^TestStreamJSONBoundaryOracle$' -tags=intest,deadlock -count=5 -v ./pkg/executor/windows

The first command ran before and after adding the shuffled scenarios; its
logs are /tmp/tidb-merge-json-oracle.log and
/tmp/tidb-merge-json-oracle-final.log (the latter exposes sort nondeterminism).
Authoritative controlled runs: /tmp/tidb-merge-json-oracle-stable.log,
/tmp/tidb-stream-json-oracle-stable.log. Overlay source files are
/tmp/tidb-merge-json-oracle.go and /tmp/tidb-stream-json-oracle.go.
These use the existing windows harness without package failpoint calls;
join's failpoint-dependent original package tests were not run or counted.
No Go/Bazel/module change requires bazel_prepare.

Rust commands from rust/:

    cargo test --offline --locked -j12 -p tidb-session --lib merge_join_json_boundary_uses_encoded_identity
    cargo test --offline --locked -j12 -p tidb-executor --lib merge
    cargo test --offline --locked -j12 -p tidb-session --lib merge
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join

The final targeted executor run passes 76 tests, including both spill
orientations, large duplicate groups, outer NULL/filter behavior, chunk
retention and required-row output: /tmp/tidb-merge-checker-executor-final.log.
The final 16-test session suite passes, including all eight new scenarios and
existing shuffled joins/stream aggregation:
/tmp/tidb-merge-checker-session-final.log. Earlier wider session selector
passes 24 and ignores four explicitly unported bootstrap/index-merge tests;
those four are not coverage of this change (/tmp/tidb-merge-checker-session.log).
Red/green and shuffled-only logs: /tmp/tidb-merge-json-red.log,
/tmp/tidb-merge-json-green.log, /tmp/tidb-merge-json-parallel.log.
Only changed code regions were formatted; self-review checked checker range
consumption, fetching after final groups, NULL skipping, container ownership
and unchanged cross-side comparison. No background executor was introduced.

Repository-root gates:

    make lint
    make -o tools/bin/revive lint
    git diff --check

First command fails the existing revive1.2.1 bootstrap with exit 2;
/tmp/tidb-merge-checker-lint.log. Existing-tool lint passes separately;
/tmp/tidb-merge-checker-lint-existing.log. Diff check passes. No workload
benchmark/release build was run and no performance gain is asserted.

Changed files: rust/crates/tidb-executor/src/join.rs,
rust/crates/tidb-session/src/tests_explain_merge_join.rs, this ExecPlan and
physicalop-source-inventory.md. The inventory now includes all 47 artifacts
of the complete join package and explicitly leaves its full validation open.
The shared checker now serves window, shuffle, grouped stream aggregation and
merge join. Remaining typed-expression/encoding/error-policy decisions and
whole-package gates still preclude accepting vecgroupchecker or join.


The merge publication gate also passed from the isolated checkout at
/private/tmp/tidb-parity-publish-aba629bb, refreshed to 7c79401941 only after
verifying it held exactly our previously published files and no other edits.
All four staged files matched that checkout byte-for-byte. From its rust/:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib merge_join_json_boundary_uses_encoded_identity

The eight-case regression passed (22.15s build, 0.06s test), recorded in
/tmp/tidb-merge-publish-test.log. Only this validation receipt changed after
the check. git diff --cached --check passes; origin/hparser-integration was
fetched and matched local HEAD before committing. The two unconnected local
drafts are still excluded. All test/lint processes are terminal; no cluster
was started. Goal active, next work is the complete typed/error-path audit
and validation of the shared checker and the remaining whole-package gates.


## Complete checker typed-domain audit in progress (2026-09-21)

Previous checkpoint 01936d8cca integrated the final existing checker consumer.
Read the complete four-artifact Go checker package again. A temporary Go oracle
covers 16 field-type/flag combinations in both vectorization modes, including
NULLs, interior equality, boundary bytes and next-chunk continuation. All four
original Go tests and the matrix pass. The native regression fails with seven
mismatches: unsigned/BIT/numeric-ENUM tags, decimal boundary encoding, timestamp
timezone, and ENUM/SET collated equality. Default duration FSP -1 also panics
through generic scalar comparison; a temporary explicit FSP zero allowed the
matrix to expose its other discrepancies. Restore the original -1 case when
replacing generic comparison with the typed Go duration comparison.

Implement typed grouping values and same-domain comparison; retain integer
cell fast path. Boundary decimals must mirror Go's copy through ToString and
FromString (which discards column shape), and timestamps must use session zone.
Verify binary-literal warning/error handling instead of dropping conversion
errors. The attempted SQL COLLATE-on-ENUM oracle is rejected by pinned Go with
1235; do not introduce that unsupported SQL feature to make a test pass.
Package acceptance remains open until all source contracts and gates have
sufficient evidence. No workload speedup is inferred from these changes.


### Typed-domain implementation and validation checkpoint

The checker now normalizes unsigned, BIT, float32 and hybrid string values
into Go's declared evaluation domain before grouping. Interior comparisons
use typed decimal/time/duration/JSON/vector equality; duration compares raw
nanoseconds and therefore accepts Go's unspecified FSP -1 without a panic.
Boundary decimal copies clear source field shape, and timestamp keys use the
statement timezone. The existing integer-cell fast path remains in place.
This removes unnecessary generic comparison work but establishes no measured
workload performance gain.

The permanent 16-case matrix checks NULL/interior groups, exact boundary
bytes and next-chunk continuation against Go output in both vector modes.
The BIT regression checks error 1292 and its exact message, two endpoint
warnings in warning mode, zero warnings in ignore mode, and empty grouping
state on a strict error. It does not introduce the unsupported SQL COLLATE
clause on ENUM/SET. The failed SQL probe remains only in /tmp.

Successful Go command from repository root (temporary source overlay only):

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-group-typed-overlay.json -run '^(TestGroupChecker.*Oracle|TestVecGroupChecker.*|TestIssue53867)$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker

All four original tests and two oracle tests passed; the new fixtures run
both vectorization modes. Output: /tmp/tidb-group-typed-oracle-final.log.
No tracked Go/import/module/Bazel changes were made, so bazel_prepare is not
triggered. This package has no injected failpoint calls requiring toggling.

Successful native commands from rust/:

    cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    cargo test --offline --locked -j12 -p tidb-executor --lib issue_53867
    cargo test --offline --locked -j12 -p tidb-executor --lib merge
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg
    cargo test --offline --locked -j12 -p tidb-executor --lib shuffle
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window

Counts respectively: 16, 1, 76, 76, 26, 23, 16 and 68 passed, zero failures or
ignored tests in these filters. These are scoped run counts, not a deduplicated
package-acceptance total. The merge-join session module includes the stream
JSON-boundary SQL regression. An additional stream_agg_json filter matched
zero tests and is not evidence. Logs: /tmp/tidb-group-typed-*.log.

From repository root:

    make lint
    make -o tools/bin/revive lint
    git diff --check

Standard lint failed installing revive 1.2.1 (module does not contain package,
exit 2); /tmp/tidb-group-typed-lint.log. The actual lint recipes passed using
the existing binary; /tmp/tidb-group-typed-lint-existing.log. Diff check passed.

Changed files: checker production/tests, this ExecPlan and source inventory.
The package remains unaccepted: codec error-context handling, legacy collation
mode and complete expression evaluation contracts need further audit. Whole
consumer-package gates, release workloads, race checks and a current controlled
performance comparison were not run in this checkpoint. Compatibility risk is
concentrated at typed grouping and cross-chunk equality; the Go matrix and
shared-consumer regressions cover the concrete changes, not every SQL input.


Isolated red/green validation used /private/tmp/tidb-parity-publish-aba629bb.
With production restored to 01936d8cca and only the current test module copied,
the typed-domain regression failed on the unspecified-duration-FSP panic and
the BIT regression failed because the old code returned generic Unsupported
instead of Go error 1292. Commands from that checkout's rust/ (both exit 101):

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-executor --lib typed_group_boundaries_match_go_evaluation_domains
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-executor --lib bit_boundary_overflow_preserves_go_error_policy

Logs: /tmp/tidb-group-typed-isolated-red-*.log. Restoring the production patch
made the complete vec_group_checker filter pass (16 tests), recorded in
/tmp/tidb-group-typed-isolated-green.log.

Before publication, fetch revealed remote commit 90f1fe4db4, an independent
ranger borrowing optimization. Both the main and isolated checkout were
fast-forwarded to it without conflicts. The three modified files matched
byte-for-byte between checkouts before the final validation receipt.

The combined isolated checkout passed these final gates from rust/:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-planner --lib ranger::detacher
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib tests_window

Counts: 13, 16, 16 and 68 passed, no failures/ignored tests; logs are
/tmp/tidb-group-typed-combined-*.log. Existing-tool lint was rerun on the
combined main checkout and passed with make -o tools/bin/revive lint.
Formatting check passed with rustfmt --check --edition 2021
rust/crates/tidb-executor/src/vec_group_checker.rs. All test/lint handles are
terminal. No cluster was started. Only this receipt changed after validation;
the two unrelated unconnected draft files remain excluded from publication.
Goal active; this is validated progress, not whole-package acceptance.

## Checker runtime collation and encoding-error audit (2026-09-21)

Checkpoint e94c515913 is pushed. Continue the complete checker package audit,
with the four-artifact inventory unchanged. The Go source uses the process
collation switch and exact FieldType collation spelling for both boundaries
and interior keys. Native boundary encoding forces new mode and caches a
normalized collation. A temporary oracle covers both modes, four names and
both vectorization modes; legacy mode retains all byte differences, unknown
and uppercase names fall back to the new PAD SPACE binary collator, and only
the exact lowercase CI name merges the case variants.

A second oracle covers invalid timestamp month zero in UTC and +08:00,
strict/warn/ignore policies and both vectorization modes. UTC packs raw fields.
Non-UTC strict mode returns 1292 with CoreTime's original field representation.
Warn/ignore drops the whole encoded boundary and warning mode emits one error
for each endpoint. Repeating a chunk with empty encoded keys does not continue
the previous chunk. The initial oracle incorrectly used SetErrLevels for
truncation; Go explicitly excludes truncation there. Its corrected version
uses SetTypeFlags, the authoritative truncation-policy setter.

Add red regressions before changing the checker and codec seam. Preserve the
original timestamp in codec errors, apply the checker statement policy, and
respect Go's empty-key continuation sentinel. A subprocess isolates the Rust
process-global collation switch from concurrently running tests. No public
SQL behavior absent from Go is introduced. Codec remains a whole-package
validation unit, using its existing inventory and test mappings; this audit
must not silently reuse its older zero-findings receipt as current evidence.

### Runtime audit implementation and validation receipt

Both new checker regressions failed on the previous production code: legacy
CI strings incorrectly merged, and strict timestamp conversion returned
Unsupported instead of the source diagnostic. Logs:
/tmp/tidb-group-runtime-red-runtime_collation_mode_and_exact_names_match_go.log
and /tmp/tidb-group-runtime-red-timestamp_boundary_errors_follow_statement_policy.log.
A separate codec prefix-clearing regression failed before its fix:
/tmp/tidb-group-codec-prefix-red.log. Initial test compilation used the wrong
SessionTimeZone constructor; only the subsequent behavioral failures above
count as red evidence.

The checker now resolves exact FieldType collation names, honors the runtime
mode, compares string keys in the source collation domain, handles timestamp
encoding errors through statement truncation policy and excludes empty prior
keys from continuation. Codec timestamp failures preserve the original Time
and clear the caller's prefix; generic codec Display text is unchanged.
Binary/padding-only comparison borrows keys. CI key generation can allocate;
no performance improvement is claimed without workload measurements.

Successful Go validations from repository root:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-group-runtime-overlay.json -run '^TestGroupChecker(CollationMode|TimestampError)Oracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-group-runtime-overlay.json -run '^TestGroupCheckerTimestampErrorOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-group-runtime-overlay.json -run '^(TestGroupChecker.*Oracle|TestVecGroupChecker.*|TestIssue53867)$' -tags=intest,deadlock -count=1 ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off ./tools/check/failpoint-go-test.sh pkg/util/codec -count=1

The timestamp oracle's final repeated-chunk evidence is in
/tmp/tidb-group-time-error-oracle-final.log; the initial combined log used the
incorrect policy setter noted above and is only collation evidence. The race
run uses the corrected overlay and passes all four original checker tests and
four oracle tests. Codec original tests pass with failpoints enabled and
cleanup confirmed at refcount zero in /tmp/tidb-group-runtime-go-codec-failpoints.log.
A preceding plain codec go test passed but did not satisfy failpoint policy;
it is excluded from gate evidence. No tracked Go changes remain, and no
Bazel/module/source-list edits require bazel_prepare.

Successful native validations from rust/:

    cargo test --offline --locked -j12 -p tidb-codec --test all encode_timestamp_in_utc_skips_timezone_validation
    cargo test --offline --locked -j12 -p tidb-codec
    cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    cargo test --offline --locked -j12 -p tidb-executor --lib merge
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg
    cargo test --offline --locked -j12 -p tidb-executor --lib shuffle
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window

Codec: 46 library, 167 integration tests passed. Checker: 18; executor merge:
76; hash_agg: 76; shuffle: 26; window: 23; session merge/stream module: 16;
session windows: 68. No failures or ignored tests in those runs. An early
command after a test-writing path error matched zero tests and is not evidence.
Logs: /tmp/tidb-group-runtime-*.log. Repeated runs are not additional distinct
coverage. Shared-consumer gates cover the grouping change; they do not replace
complete aggregate/join/window package acceptance.

From repository root, make lint again failed the existing revive bootstrap
(exit 2). make -o tools/bin/revive lint passed the actual recipes with the
existing binary. git diff --check and rustfmt --check --edition 2021 on
rust/crates/tidb-codec/src/error.rs and
rust/crates/tidb-executor/src/vec_group_checker.rs pass. The touched regions in
the larger codec source/test files retain local formatting without whole-file
churn. No release workload or controlled performance comparison was run.

Changed production/test files: tidb-codec/src/{error,package}.rs,
tidb-codec/tests/codec_package_source.rs, and tidb-executor/src/vec_group_checker.rs.
Updated records: this plan, physicalop-source-inventory.md,
codec-divergence-inventory.md and operations/util-codec-audit-execplan.md.
The historical codec zero-findings entry is qualified rather than treated as
current acceptance. Remaining checker expression-domain and codec-error
contracts are still open. The full goal remains active.


Publication validation used /private/tmp/tidb-parity-publish-aba629bb at
pushed HEAD e94c515913 with the same eight changed files. The prior temporary
checkout was reset only after verifying its contents matched the published
checkpoint (except its missing final receipt). From isolated rust/:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-codec
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join

All passed (codec 46+167, checker 18, session 16); logs:
/tmp/tidb-group-runtime-isolated-{codec,checker,merge-session}.log. Seven files
remain byte-identical across checkouts; only this plan's final receipt was
appended after validation. Fetch confirmed origin/hparser-integration matched
local HEAD before publication. All test/lint processes are terminal; failpoint
cleanup is complete and no cluster was started. The two unconnected draft
files remain excluded. This is progress evidence, not a completion claim.

## Typed constant and string-path checkpoint (2026-09-21)

Previous checkpoint b49c1a7e4b is pushed. The remaining checker expression-domain
review compared all eight Constant typed entrypoints against generic Eval.
Go's typed calls consume raw lazy values, derive parameter types at execution,
convert numeric strings, stringify non-string values, and pad decimal scale
without fitting declared precision or narrowing an existing fraction. A
27-scenario literal/deferred/parameter oracle, repeated in both vector modes,
exposes native mismatches in conversion, warnings, decimal shape and parameter
collation. The permanent matrix failed before changes; its red log is
/tmp/tidb-group-constant-red.log. The Go oracle is a temporary overlay; no
tracked Go files or Bazel inputs change.

Implement typed Constant evaluation at its expression owner and contextual
ToDecimal at the datatype owner, then use them from the complete checker
package. Keep generic Constant.Eval behavior intact. Parameter collation must
be refreshed from the current inferred type instead of saved planning metadata.
Add explicit strict/warn/ignore diagnostic evidence, validate the affected
owners and consumers, and inspect the string-key allocation path. None of
these partial dependency corrections constitutes acceptance of the complete
expression or types Go packages. Existing whole-package inventories/receipts
remain the required eventual acceptance unit. Performance claims require
measurement rather than source inspection alone.


The implementation now uses `Constant::eval_typed_on_row` and
`Datum::to_decimal_with_context`. Generic Constant evaluation is unchanged.
A parameter's `get_type` returns an owned, freshly inferred FieldType; the
original `TestGetTypeThreadSafe` contract is now active in
`constant_test_go_tables_source.rs` and proves independent mutation. Ordinary
string/blob columns borrow `raw_cells` and retain only the previous key;
case-insensitive keys are generated once per row, while binary keys borrow.
Selection vectors and NULL boundaries are covered for seven field codes.
The statement's existing vectorized-expression flag controls plain constant
batch evaluation. NaN still splits adjacent rows because Go compares it unequal.

Regression evidence preceded fixes: `/tmp/tidb-group-constant-red.log` contains
17 domain/warning mismatches; `/tmp/tidb-group-constant_interior_warnings-red.log`
shows six warnings instead of three; and
`/tmp/tidb-group-constant_decimal_conversion-red.log` shows the lost overflow
warning/rounding-error contract. Expanding diagnostics to NaN additionally
exposed a missing error argument, corrected before final validation. These
are incremental repairs within open whole-package acceptance units.

The temporary Go overlay `/tmp/tidb-group-constant-overlay.json` maps the
original checker test file to `/tmp/tidb-group-constant-oracle.go`, retaining
original tests and earlier oracles. Exact new oracle commands from root:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-group-constant-overlay.json -run '^TestGroupCheckerConstantDomainOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-group-constant-overlay.json -run '^TestGroupChecker(ConstantInterior|DecimalError)Oracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-group-constant-overlay.json -run '^TestGroupCheckerDecimalErrorOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-group-constant-overlay.json -run '^(TestGroupChecker.*Oracle|TestVecGroupChecker.*|TestIssue53867)$' -tags=intest,deadlock -count=1 ./pkg/executor/internal/vecgroupchecker

All passed. The domain oracle covers nine scenarios in literal/deferred/parameter
forms and both modes (54 records). Interior warnings are six scalar/three
vectorized. Eight decimal scenarios across strict/warn/ignore cover prefixes,
invalid strings, string/float overflow, JSON objects/strings, NaN and infinity.
The accumulated race command runs 11 test functions, including four originals.
Logs are `/tmp/tidb-group-constant-{oracle,errors-oracle-final,go-race}.log`.
An initial probe used unsupported Go `int` instead of `int64` for CreateBinaryJSON;
that fixture error was corrected and is not product evidence. This checker
package has no injected failpoint calls. No tracked Go/module/Bazel edits.

Successful final Rust commands from rust/:

    cargo test --offline --locked -j12 -p tidb-datatype --lib datum::
    cargo test --offline --locked -j12 -p tidb-expr --lib constant
    cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    cargo test --offline --locked -j12 -p tidb-executor --lib issue_53867
    cargo test --offline --locked -j12 -p tidb-executor --lib merge
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg
    cargo test --offline --locked -j12 -p tidb-executor --lib shuffle
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window

Counts in order: 32, 66, 23, 1, 76, 76, 26, 23, 16, 68 passed; overlapping
filters are not additional coverage. Expression constant selection has five
ignored tests: deferred error forwarding, separate typed parameter entrypoints,
vectorized deferred forms, upstream-self-skipped Constant2PB, and vectorized
ILIKE. They are excluded from parity evidence. Other listed selections have
no failures or ignores. Logs: `/tmp/tidb-group-constant-validation-*.log`.

From root, `make lint` failed again at the revive1.2.1 bootstrap (exit 2,
module found but does not contain package). `make -o tools/bin/revive lint`
passed the actual lint recipes with the existing binary. `git diff --check`
and `rustfmt --check --edition 2021 rust/crates/tidb-executor/src/vec_group_checker.rs`
pass. Changed regions in larger files were formatted without unrelated churn.

Remaining correctness risks: Go deferred constants delegate vector evaluation
to their child, while the native checker still evaluates that form row by row;
scalar/correlated typed domains and remaining codec-error contracts need audit.
Lossy UTF-8 diagnostic conversion also needs separate evidence for raw invalid
bytes. Whole types/expression/consumer packages remain unaccepted. No current
sysbench/TPC-C/TPC-H/YCSB end-to-end benchmark was run at this checkpoint.


Final isolated stream-grouping benchmark (same host, no other build/test process
running during samples): baseline is b49c1a7e4b plus the identical new benchmark;
final is this code including NaN error arguments. From each checkout's rust/:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo bench --offline --locked -j12 -p tidb-executor --bench pipeline --no-run

The main checkout used the same command without the redundant target override.
The printed executable was copied to `/tmp/tidb-group-pipeline-before` before
rebuilding, and `/tmp/tidb-group-pipeline-final` afterward. Each version ran
three times in order before/final/final/before/before/final:

    BENCH_ONLY=stream_group /tmp/tidb-group-pipeline-before
    BENCH_ONLY=stream_group /tmp/tidb-group-pipeline-final

Each sample uses five calibrated 250ms blocks, 1024 sorted rows, 32 rows/group,
and asserts 32 output groups through production GroupedStreamAggExec. The bench
profile uses jemallocator, no LTO and 16 codegen units; macOS ASLR remains on.
This isolates stream grouping, not the production server or workload runtime.

| Scenario | Before ns/row (3 runs) | Final ns/row (3 runs) | Median reduction | Calibrated median reduction |
| --- | --- | --- | --- | --- |
| utf8mb4_bin | 60.2, 61.6, 62.2 | 22.1, 22.6, 23.2 | 63.3% | 63.2% |
| utf8mb4_general_ci | 123.1, 126.8, 128.5 | 55.5, 57.0, 58.1 | 55.0% | 54.8% |
| utf8mb4_bin + constant | 85.2, 88.2, 87.9 | 22.2, 22.9, 23.3 | 73.9% | 73.7% |
| utf8mb4_general_ci + constant | 148.9, 153.1, 153.6 | 55.6, 57.1, 58.2 | 62.7% | 62.2% |

Calibration self-ratios span 0.9928–0.9971. Final logs are
`/tmp/tidb-group-final-bench-{1..6}-{before|final}.log`; build logs are
`/tmp/tidb-group-bench-{before,final}-build.log`. A preliminary six-run comparison
showed a similar range but predates the final NaN diagnostic fix and is excluded
from the final table. Binary SHA-256 values:

    before c01b95222da10ffa234f4a966f378c2d3fcad6f4297770ca183bd0d5f3b1ec70
    final  5a1c092cb48552fa043096093cab917344981c7bfc9a12bed0983734c7d06671

These measurements justify retaining the allocation optimization. They do not
claim end-to-end improvement for sysbench, TPC-C, TPC-H or YCSB, and do not
substitute for any whole-package acceptance gate.


Publication validation: `/private/tmp/tidb-parity-publish-aba629bb` at b49c1a7e4b
contains the same eight changed code/test/benchmark files, copied from the main
checkout; the two untracked drafts are absent and excluded. Commands from its
rust/ directory:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-datatype --lib datum::
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-expr --lib constant
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join

All passed: 32, 66 (five explicit ignores), 23 and 16 respectively. Logs:
`/tmp/tidb-group-constant-isolated-*.log`. Byte comparison confirms all eight
code files match; documentation receipts were finalized afterward. The actual
lint recipes also passed again after activating the original GetType test.
All test/lint/benchmark processes are terminal. No cluster was started and no
failpoints were enabled for these checker tests. Fetch confirmed the remote
hparser-integration branch still matched b49c1a7e4b before publication.

Changed files: `tidb-datatype/src/datum/convert.rs`;
`tidb-expr/src/{constant,context}.rs` and
`tidb-expr/src/tests/constant_test_go_tables_source.rs`;
`tidb-executor/src/{vec_group_checker,stmt_context,shuffle}.rs` and
`tidb-executor/benches/pipeline.rs`; this plan, physicalop-source-inventory.md
and types-datatype-divergence-audit.md. This checkpoint records progress and
measured local improvement, not completion of the user's parity/performance goal.


## Deferred and correlated grouping checkpoint (2026-09-21)

Previous checkpoint b9d2314d3e is pushed. The prior turn was concrete progress:
typed constants, diagnostics and measured string-key allocation changes are
committed. This checkpoint continues the same complete checker-package audit;
it does not redefine acceptance around an expression subset.

Read the entire checker production file and the Constant/CorrelatedColumn
scalar/vectorized entrypoints at the unchanged Go pin. The temporary overlay
`/tmp/tidb-group-lazy-overlay.json` retains all original checker tests and prior
oracles, adding deferred/nested/correlated warning/type cases, correlated
coverage across the 16-field matrix, and a deferred decimal-column case.
The first native regression failed on five mismatches: two correlated string
key encodings and three vectorized warning counts (six instead of three).
Red evidence: `/tmp/tidb-group-lazy-red.log`; the earlier root-directory Cargo
invocation did not run a test and is excluded. Go evidence is in
`/tmp/tidb-group-lazy-go.log` and `-go-expanded.log`.

`Constant::eval_typed_as_on_row` accepts the requested Go EvalXxx domain while
retaining the child's own FieldType for conversion and decimal padding.
`CorrelatedColumn::eval_typed` snapshots the live binding, applies hybrid
integer conversion and ToString, and preserves native temporal/decimal/JSON/
vector payloads. It does not borrow Constant's decimal padding semantics.
The conversion warning adapter is shared inside tidb-expr. The checker follows
Go deferred wrappers in vector mode, broadcasting literal/correlated leaves
once and using matching-domain child columns directly. Scalar mode retains
outer constant conversion on each row. Repeated NaN still compares unequal.

The expanded source oracle asserts exact correlated keys, offsets, continuation
and rebinding to/from NULL for 16 fields in both modes (32 records, all eight
evaluation domains). Ten lazy-batch records assert source-observed warnings
and keys. The deferred decimal fixture succeeds with three groups in vector
mode and returns fatal1265/no warnings in scalar mode. An initial hand-written
Go fixture accidentally had 87 digits and failed in the fixture constructor;
it was corrected to 81 digits before the successful oracle run. No product
claim is based on that fixture error. Native tests use repeat(81).

Exact Go commands from root:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-group-lazy-overlay.json -run '^TestGroupCheckerLazyBatchOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-group-lazy-overlay.json -run '^TestGroupChecker(TypedBoundary|LazyBatch|DeferredColumn)Oracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-group-lazy-overlay.json -run '^(TestGroupChecker.*Oracle|TestVecGroupChecker.*|TestIssue53867)$' -tags=intest,deadlock -count=1 ./pkg/executor/internal/vecgroupchecker

The checker package source and BUILD.bazel have no failpoint injection/dependency;
no failpoint enable/disable is needed for this package. The overlay does not
change tracked Go or Bazel/module inputs; bazel_prepare is not required.

Remaining acceptance work includes scalar-function typed/vectorized evaluation,
deferred domain-mismatched column contracts, remaining codec errors and complete
dependency/consumer package receipts. These changes do not prove full expression
parity, and the five existing ignored constant-related tests remain excluded.
No end-to-end workload or new performance comparison was run at this checkpoint;
the previous commit's grouping microbenchmark remains historical evidence.


Validation scope covers the changed expression owners, the complete checker
module and its shared stream aggregation, merge join, shuffle and window
consumers. Final main-checkout commands from rust/:

    cargo test --offline --locked -j12 -p tidb-expr --lib constant
    cargo test --offline --locked -j12 -p tidb-expr --lib column::tests
    cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    cargo test --offline --locked -j12 -p tidb-executor --lib issue_53867
    cargo test --offline --locked -j12 -p tidb-executor --lib merge
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg
    cargo test --offline --locked -j12 -p tidb-executor --lib shuffle
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    cargo test --offline --locked -j12 -p tidb-session --lib correlated

All passed, respectively: 66, 15, 25, 1, 76, 76, 26, 23, 16, 68 and 23.
The constant filter has five ignored tests, as detailed in the previous receipt;
all other listed runs have none. Overlapping filters do not add distinct
coverage. An initial session filter `tests_correlated` matched zero tests;
it is excluded and replaced by `correlated`, which ran 23 tests. Logs:
`/tmp/tidb-group-lazy-validation-*.log`. The red and initial focused green test
used `cargo test --offline --locked -j12 -p tidb-executor --lib lazy_batch_grouping`.
The final accumulated Go race gate passed 13 test functions (four original
checker tests plus nine differential oracles), log
`/tmp/tidb-group-lazy-go-race.log`.

From root, `make lint` failed the existing revive1.2.1 bootstrap (exit 2).
`make -o tools/bin/revive lint` passed the actual recipes with the existing
binary. `git diff --check` and
`rustfmt --check --edition 2021 rust/crates/tidb-executor/src/vec_group_checker.rs`
pass; the new expression-owner region was formatted without whole-file churn.
Logs: `/tmp/tidb-group-lazy-lint{,-existing}.log`.


Publication validation used `/private/tmp/tidb-parity-publish-aba629bb` at
published b9d2314d3e. Its prior changes were compared against that commit before
resetting the disposable worktree; only the old final plan receipt differed.
Copied the five current tracked changes, excluding both unconnected drafts.
From isolated rust/:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-expr --lib constant
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-expr --lib column::tests
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib correlated

All passed: 66 (five explicit ignores), 15, 25 and 23. Logs:
`/tmp/tidb-group-lazy-isolated-*.log`. Byte comparison verifies the three code
files match the isolated checkout; this final receipt was appended afterward.
Fetch confirmed origin/hparser-integration still matched b9d2314d3e. All
processes are terminal; no cluster or failpoint lifecycle was started.

Files changed: `tidb-expr/src/{constant,column}.rs`,
`tidb-executor/src/vec_group_checker.rs`, this plan and the physicalop source
inventory. Correctness evidence is scoped to the typed/batch contracts above;
full package acceptance, scalar-function vectorization and workload performance
remain open. The user's goal remains active.


## Arithmetic batch audit checkpoint (2026-09-21)

The prior deferred/correlated checkpoint bb4c672a3d is committed and pushed.
The next remaining checker contract is scalar-function evaluation. Source
inspection distinguishes scalar numeric +, -, * NULL behavior from their
vectorized operand-batch order: scalar integer/decimal addition and every
numeric subtraction/multiplication skip the right operand after a NULL left;
real addition evaluates both. Vectorized signatures evaluate both operand
batches before applying the operator. Native evaluation currently evaluates
all arithmetic children per row. A pinned Go oracle and native regression
will establish error order before edits.

Implement shared numeric batch evaluation in the expression owner, with a
side-effect-free shape eligibility check before evaluating any input. Reuse
existing arithmetic value/error kernels and maintain argument metadata. Wire
the checker and projection evaluator to the shared path, preserving the
statement vectorization flag. Shape gaps stay explicit, and no new builtin or
package acceptance is claimed. Validate scalar/batch differences, warnings,
selection, signedness, and dependent executor/session behavior before publishing.


### Evidence, decisions, and remaining scope


Pinned Go `builtin_arithmetic.go` and its vector implementations establish
scalar integer/decimal addition and numeric subtraction/multiplication stop
on a NULL left operand, while real addition evaluates the right operand.
Vector signatures evaluate the full left batch, then the full right batch,
then arithmetic. The checker oracle exercises 18 domain/operator/mode cases;
a second oracle verifies six warning-count cases. A third oracle confirms
unnamed column diagnostics use `Column#N`. Native red evidence is in
`/tmp/tidb-group-arithmetic-red.log`; initial fixture-construction/compilation
errors are excluded. Checker regressions now assert exact overflow domains,
expressions, warning codes/messages/counts, and successful scalar grouping.

The native shared evaluator first checks the complete expression shape without
evaluating an operand. Unsupported trees keep their existing evaluation path;
empty batches read no prepared parameters. Numeric +, -, * trees support typed
constant/correlated/ordinary-column leaves and implicit Int-to-Real,
Int-to-Decimal and Decimal-to-Real widening. Go inserts those casts during
function construction; native trees can retain mixed numeric FieldTypes.
Unsigned integer bits are reinterpreted before widening, as in Go
`builtinCastIntAsRealSig` and `builtinCastIntAsDecimalSig`. A regression checks
UINT64_MAX through all three operators in both widening domains, including
selected NULL rows and scalar results.

A SQL oracle exposed the initially missed Real * Int child under subtraction:
scalar evaluation correctly returned NULL, but vector evaluation also returned
NULL instead of Go's overflow. `/tmp/tidb-arithmetic-sql-rust.log` records this
red result; `/tmp/tidb-arithmetic-sql-rust-final.log` and the final numeric-domain
suite record the fix. The pinned Go SQL oracle passes all 12 Int/Real,
operator and mode scenarios. No Go source/build/module file was changed;
Bazel regeneration and failpoint toggling were not required for these checker
and windows oracle packages.

The first benchmark found a 19–24% integer projection regression with Datum
intermediates. The final implementation instead carries nullable i64 bits,
reads selected chunk cells directly, and reuses the existing integer operator
kernel. It preserves the existing decimal projection fast path. Six final
alternating process runs (before, after, after, before, before, after) used
identical benchmark code at bb4c672a3d and current code, 1024 rows, five timed
250ms blocks per scenario, jemalloc, and the repository bench profile. No
other build or test ran during measurement. Median ns/row was 74.1 -> 16.9
for simple integer projections and 132.1 -> 23.5 for nested integer
projections (77.2% and 82.2% reductions). Decimal was 29.9 -> 30.9 and nested
decimal 44.4 -> 45.6; calibrated changes were +0.3% and -1.6%, respectively.
Integer calibrated reductions were 78.0% and 82.6%. These are component
measurements, not sysbench/TPC-C/TPC-H/YCSB results. Logs are
`/tmp/tidb-arithmetic-final-bench-{1..6}-{before,final}.log`; the baseline
binary remains `/tmp/tidb-arithmetic-pipeline-before`.

This is progress within the inventoried complete packages, not acceptance.
Other scalar signatures, explicit cast nodes, /, DIV, MOD, unsupported child
shapes, deferred mismatched-domain columns, remaining codec failures and
full diagnostic rendering remain open. The Go package remains the atomic
acceptance unit. Workload performance and complete package validation are
still required. The two unconnected user drafts remain untouched/untracked.

### Validation receipt


From root (cached Go 1.26.0, no module downloads), the following passed:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-group-arithmetic-overlay.json -run '^TestGroupCheckerArithmetic(Batch|Warning)Oracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-group-arithmetic-overlay.json -run '^TestGroupCheckerUnnamedOverflowOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-group-arithmetic-overlay.json -run '^(TestGroupChecker.*Oracle|TestVecGroupChecker.*|TestIssue53867)$' -tags=intest,deadlock -count=1 ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-arithmetic-sql-overlay.json -run '^TestArithmeticNullBatchSQLOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

The accumulated race gate contains 16 test functions, including four originals.
Go oracle sources are `/tmp/tidb-group-arithmetic-oracle.go` and
`/tmp/tidb-arithmetic-sql-oracle.go`; overlays retain original package tests.
Logs are `/tmp/tidb-group-arithmetic-go-{final,race}.log`,
`/tmp/tidb-group-arithmetic-unnamed-go.log` and
`/tmp/tidb-arithmetic-sql-go.log`.

From rust/, final expression/consumer checks:

    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    cargo test --offline --locked -j12 -p tidb-executor --lib merge
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg
    cargo test --offline --locked -j12 -p tidb-executor --lib shuffle
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    cargo bench --offline --locked -j12 -p tidb-executor --bench pipeline --no-run

Expression tests pass 1,188, with 97 preexisting ignored tests; the existing
HTTP test needed sandbox escalation to bind localhost. Initial fixture compile
errors and sandbox failures are excluded from regression evidence. Final
consumer counts in command order are 27, 76, 76, 26, 23, 7, 16, and 68
passed, with no failures or ignored tests in those scoped filters. Earlier
broad/narrow filters `merge_join` (executor), `merge` (session) and `windows`
(session) were also run; those overlap the scoped gates and are not added to
coverage counts. Logs: `/tmp/tidb-arithmetic-final-*.log`.

From root, `make lint` failed the existing revive1.2.1 bootstrap (exit 2,
module does not contain the requested package). `make -o tools/bin/revive lint`
passed the actual lint recipes using the existing binary. Logs:
`/tmp/tidb-group-arithmetic-lint.log` and
`/tmp/tidb-arithmetic-final-lint-existing.log`. Changed Rust regions were
formatted without unrelated whole-file changes. `git diff --check` passes.
No cluster was started. Full workspace tests, all platform/build variants,
package acceptance and end-to-end workload performance were not verified.


The baseline bench build ran from the isolated rust/ checkout at bb4c672a3d,
with only the current benchmark source copied in:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo bench --offline --locked -j12 -p tidb-executor --bench pipeline --no-run

The resulting baseline and final executables were copied to the paths below.
Each invocation was repeated three times in the alternating order above:

    BENCH_ONLY=numeric_projection /tmp/tidb-arithmetic-pipeline-before
    BENCH_ONLY=numeric_projection /tmp/tidb-arithmetic-pipeline-final


Publication validation used the disposable checkout at bb4c672a3d. Its only
prior change (the benchmark source) was byte-compared with the current file
before copying all ten tracked changes. Both unconnected drafts were excluded.
The following passed from isolated rust/:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-expr --lib numeric_batch
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain

Counts: 3, 27 and 7 passed; no failures/ignores. Logs are
`/tmp/tidb-arithmetic-isolated-*.log`. Byte comparison confirms all eight
tracked Rust files match the isolated validation checkout; final documentation
receipts were appended afterward. Both checkout diffs pass whitespace checks.
`git fetch origin --prune` confirmed the publication parent bb4c672a3d remains
the remote branch head. All validation processes have terminated.

Changed files: expression scalar_function/evaluator/operator dispatch and
integer-kernel visibility, checker plus regression tests, projection benchmarks,
session numeric-domain SQL regression, this ExecPlan and source inventory.
The correctness risk is concentrated in typed operand dispatch and evaluation
order; scoped differential and consumer gates cover the audited paths. No
full-package or whole-workload claim is made, and the user's goal stays active.


## Remaining numeric signatures audit (2026-09-21)


The pushed 203183ab0b checkpoint is verified progress. Pulled the existing
branch with --ff-only; remote is unchanged. Continue the inventoried checker
package audit with /, DIV and MOD, including scalar NULL behavior, vector
operand order, argument versus result domains and diagnostic differences.
Go source specifies eager scalar Int/Real MOD, short-circuit Decimal MOD and
all division signatures, and eager full argument batches in vector mode.
Extend the prior differential regression before edits. Other expression
signatures remain required; this audit does not redefine package acceptance.


### Division audit findings and implementation


The first native regression reproduced seven scalar NULL short-circuit errors:
Int/Real/Decimal / and DIV, plus Decimal MOD. Go Int/Real MOD are eager even in
scalar mode. The checker now tests all six numeric operators over three input
domains in both modes (36 scenarios). DIV separates its integer result type
from its argument type: both Int arguments use the Int signature; otherwise
Go consumes Decimal. Native Real DIV previously used float division followed
by truncation, so 0.3 DIV 0.1 incorrectly produced 2 instead of Go's 3.

The direct Go value oracle inspected nine boundary pairs across /, DIV and MOD
in both modes (54 observations). Its first version omitted Vectorized(), which
initializes Go's child buffer allocators; that harness panic is excluded from
regression evidence. The corrected oracle and accumulated race gate pass.
Native assertions cover the 18 DIV cases: signed minimum, mixed signedness,
decimal integer overflow, unsigned negative fraction, decimal conversion of
real operands, oversized real/decimal inputs and declared real scale.

The bounded decimal kernel now performs DecimalDiv before ToInt/ToUint, retaining
1292 warnings that the old unbounded integer quotient skipped. Its warning
rendering respects the nine-word decimal buffer after fractional truncation.
The Go negative unsigned fraction exception returns zero. Integer-signature
DIV overflow names the concrete numeric pair; Decimal-signature scalar errors
name the source arguments (including implicit cast wrappers), while vector
errors name the evaluated decimal arguments. Real-to-Decimal conversion keeps
FromFloat64's overflow alias, ignores its truncation as Go does, and preserves
the cast target precision/scale. A DOUBLE(10,1) source holding 0.39 therefore
rounds to 0.4 before DIV 0.1, yielding 4 and warning 1292; the old result was 3
without a warning. The existing UINT64_MAX widening test was corrected to use
a LongLong field rather than a 32-bit Long field after the proper cast-width
gate rejected that invalid fixture.

Red logs: `/tmp/tidb-group-division-red.log`,
`/tmp/tidb-division-values-red.log`, and `/tmp/tidb-division-scaled-red.log`.
Initial fixture compilation errors are excluded. Focused green logs:
`/tmp/tidb-group-division-green.log`, `/tmp/tidb-division-values-green.log`,
and `/tmp/tidb-division-scaled-green.log`. The last two green runs were followed
by final expression/consumer gates after all production edits.

This remains a checkpoint within the complete inventoried packages. Explicit
casts, nonnumeric argument shapes, other scalar signatures, deferred mismatched
domains, remaining codec errors, full diagnostic rendering and complete
platform/build/test artifact validation remain required. No whole-package or
whole-workload completion is claimed. Source-level exception-policy and broader
cast audits still need full coverage beyond the fixtures here.

### Division validation receipt


From root, cached Go 1.26.0 with no dependency downloads:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-group-division-overlay.json -run '^TestGroupCheckerDivisionBatchOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-group-division-overlay.json -run '^TestGroupCheckerDivisionValueOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-group-division-overlay.json -run '^(TestGroupChecker.*Oracle|TestVecGroupChecker.*|TestIssue53867)$' -tags=intest,deadlock -count=1 ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-division-sql-overlay.json -run '^TestArithmeticNullBatchSQLOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

The race gate contains 18 test functions (four originals and fourteen oracles).
Both overlays retain original package tests. Logs: `/tmp/tidb-group-division-go.log`,
`/tmp/tidb-division-values-go.log`, `/tmp/tidb-division-go-race.log`, and
`/tmp/tidb-division-sql-go.log`. No tracked Go/module/Bazel input changed;
these Go packages have no failpoint prerequisite. No cluster was started.

From rust/, final gates:

    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    cargo test --offline --locked -j12 -p tidb-executor --lib merge
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg
    cargo test --offline --locked -j12 -p tidb-executor --lib shuffle
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window

Full expression validation passes 1,188 with 97 existing ignored tests;
localhost access is required by its existing HTTP test. Consumer terminal
counts and performance results follow below. Logs:
`/tmp/tidb-division-final-*.log`. `make lint` again fails the existing revive
bootstrap (exit 2); `make -o tools/bin/revive lint` passes the actual recipes
using the existing binary (`/tmp/tidb-division-lint{,-existing}.log`). Changed
Rust regions are formatted and `git diff --check` passes.


The SQL consumer gate exposed an additional batch fallback through the
rewriter's integer-to-decimal wrappers: `SELECT a / (b*2)` with NULL a and
BIGINT_MAX b returned NULL in vector mode instead of Go's overflow. The
shared batch evaluator now recognizes those cast nodes, evaluates the whole
integer child batch before applying target precision/scale, and preserves
source/target unsigned interpretation. The 24-case SQL NULL gate passes after
this fix. Other cast signatures remain open. The failing consumer output was
observed before the log was replaced by its successful rerun.

An additional SQL regression verifies real operands through column projection
and constant folding (`SELECT a DIV b` and `SELECT 0.3e0 DIV 0.1e0`) in both
modes. Go returns 3 in all four cases. The test copied into the isolated
203183ab0b baseline fails with actual Int(2), expected Int(3), before production
edits are copied; current code passes. Logs:
`/tmp/tidb-division-sql-value-{baseline,final}.log`. Exact native commands:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib real_integer_division_uses_decimal
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain

The first command ran in isolated rust/ at 203183ab0b with only the current
benchmark and SQL test file copied; the second ran in main rust/. Go's final
SQL gate (28 scenarios across two functions) was:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-division-sql-overlay.json -run '^(TestArithmeticNullBatchSQLOracle|TestRealIntegerDivisionSQLOracle)$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

Final consumer counts are 28 checker, 76 merge, 76 aggregate, 26 shuffle,
23 executor window, 8 session numeric-domain, 16 session merge and 68 session
window tests, all passing without ignores. Full expression tests passed 1,188
with 97 existing ignores after the cast integration; a subsequent eligibility
guard rejects impossible integer-output / nodes before the integer batch
kernel. Scoped publication tests cover the final tree. Final lint recipes also
passed after cast integration (`/tmp/tidb-division-final-lint-existing.log`).


### Conversion performance and publication checks


The first six alternating benchmark runs showed integer DIV/MOD and decimal
DIV improvements, but Real DIV rose from about 90 to 520 ns/row because the
previous path incorrectly divided floats directly. A subsequent optimization
preserves the corrected decimal domain: exact integral doubles within
[-2^53, 2^53] construct the same decimal directly, and Go's no-op
ProduceDecWithSpecifiedTp for unspecified scale skips redundant conversion.
Negative zero, fractional values, out-of-range values and declared scales keep
the checked path. A new test compares values and stored/visible scales against
MyDecimal.FromFloat64 across the fast boundary and fallbacks. The initial test
incorrectly demanded identical physical leading-zero layouts across Go and
Rust decimal representations; it was corrected to compare the represented
value and both scales. No datatype representation change was needed.

After optimization, the complete expression suite passes 1,189 with 97 existing
ignores; checker and numeric SQL suites pass 28 and 8. Commands repeat the
scoped gates above; logs are `/tmp/tidb-division-optimized-expr.log` and
`/tmp/tidb-division-optimized-tidb-{executor-vec_group_checker,session-numeric_domain}.log`.
The final lint recipes pass (`/tmp/tidb-division-optimized-lint-existing.log`).

For isolated validation, prior Rust changes in the disposable checkout were
byte-compared with published 203183ab0b before resetting that checkout to the
publication parent. Only current benchmark and SQL regression files were
used for baseline measurements/red evidence. Those two files were compared
with current sources (allowing formatting whitespace) before copying the eight
tracked changes. Both unconnected drafts remained excluded. From isolated rust/:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-expr --lib numeric_
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain

All pass: 22, 28 and 8; no ignores. Logs:
`/tmp/tidb-division-isolated-*.log`. The six changed Rust files are byte-identical
between main and isolated checkouts; final documentation receipts were appended
afterward. `git diff --check` passes. Fetch confirms the branch parent remains
203183ab0b. No full workspace/platform matrix or complete package acceptance is
claimed; no end-to-end workload measurement was run.

The expanded benchmark exercises ten projection cases with the same 1024-row,
five 250ms-block harness. Baseline build from isolated rust/ at 203183ab0b,
with only the benchmark changed:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo bench --offline --locked -j12 -p tidb-executor --bench pipeline --no-run

Final build from main rust/:

    cargo bench --offline --locked -j12 -p tidb-executor --bench pipeline --no-run

The corresponding executables are copied before measuring three runs each in
before/after/after/before/before/after order. No build or test runs during timing:

    BENCH_ONLY=numeric_projection /tmp/tidb-division-pipeline-before
    BENCH_ONLY=numeric_projection /tmp/tidb-division-pipeline-optimized

Final logs: `/tmp/tidb-division-optimized-bench-{1..6}-{before,optimized}.log`.
The first pre-optimization measurements remain `/tmp/tidb-division-bench-{1..6}-{before,final}.log`.


Final paired medians (negative changes mean faster):

| Projection | Before ns/row | Final ns/row | Time change | Calibrated change |
| --- | ---: | ---: | ---: | ---: |
| numeric_decimal | 32.3 | 31.8 | -1.5% | -1.0% |
| numeric_decimal_div | 228.5 | 225.4 | -1.4% | -0.5% |
| numeric_decimal_intdiv | 1064.5 | 186.6 | -82.5% | -82.5% |
| numeric_decimal_mod | 1051.9 | 1054.8 | +0.3% | +0.2% |
| numeric_decimal_nested | 47.2 | 47.4 | +0.4% | +0.1% |
| numeric_int | 17.1 | 17.7 | +3.5% | +3.0% |
| numeric_int_div | 82.9 | 17.1 | -79.4% | -79.5% |
| numeric_int_mod | 84.2 | 16.5 | -80.4% | -80.3% |
| numeric_int_nested | 24.3 | 25.1 | +3.3% | +4.4% |
| numeric_real_intdiv | 89.0 | 219.3 | +146.4% | +144.0% |

Integer DIV/MOD reduce measured time by 79–80%; bounded decimal DIV reduces
it by 82.5%. Existing integer addition projections rise by 0.6–0.8 ns/row
(3–4%); decimal addition, division and modulo remain within about 1.5%.
Real DIV is 219.3 ns/row versus the incorrect old float path's 89.0, a
remaining 146.4% cost increase despite reducing the first correct version's
roughly 520 ns/row by about 58%. This is an explicit remaining performance
risk, not a claimed workload improvement. Further performance work must keep
Go's decimal conversion, precision, warnings and error order. Sysbench,
TPC-C, TPC-H and YCSB still require end-to-end comparison.

All benchmark, lint and validation processes are terminal. Files changed:
expression scalar_function/ops and one existing evaluator fixture; executor
checker regressions and pipeline benchmark; session numeric-domain SQL tests;
this ExecPlan and source inventory. Correctness/compatibility validation is
scoped to the source contracts and gates above. Full expression/cast/codec
coverage, package acceptance and workload performance remain open. The active
goal is unchanged; commit/push publishes a progress checkpoint only.


## Value-level division audit (2026-09-21)


The prior fe4e828bd1 checkpoint is pushed and constitutes verified progress.
Pulled the existing branch with --ff-only; no remote changes. The shared value
operator still dispatches Real DIV through float division followed by truncation,
while the compiled scalar function now correctly casts through Decimal. Audit
that remaining route with fractional and mixed Decimal/Real/UInt inputs,
reuse the typed conversion owner, preserve field precision and warning/error
policy, and revalidate consumers. Complete package acceptance remains open.


### Implementation and correctness evidence

The shared value dispatcher now converts Real/FLOAT DIV operands separately
through the same typed decimal conversion as compiled expressions. Decimal and
UInt partners retain their exact value instead of passing through f64. Original
FieldTypes still control the Real-to-Decimal precision and scale. The obsolete
floating-point quotient path and its now-unused unsigned conversion are removed.
NULL handling preserves left conversion before a right NULL and avoids converting
a right operand after a left NULL. The first full expression run caught the
missing NULL guard; the original Go-derived arithmetic test and explicit value
cases now cover it.

The bounded decimal DIV kernel has an exact coefficient fast path only when
both scales match and are <= 3, both coefficients fit i128, and the effective
division precision increment is <= 30. At most five integer words plus four
fraction words fit Go's nine-word buffer, so this branch cannot skip a DecimalDiv
truncation or overflow warning. Integer conversion still checks signed/unsigned
bounds, including negative fractional quotients truncating to unsigned zero.
Every other input keeps the existing bounded decimal kernel.

Red evidence: `/tmp/tidb-value-division-red.log` records 0.3 DIV 0.1 returning 2
instead of Go's 3. The expanded existing value test also checks 0.29/0.01,
Decimal 9007199254740993 and UInt MAX paired with Real, mixed signedness, NULLs,
and declared decimal scale. The new coefficient test compares 288 combinations
against the bounded general kernel and passed before and after optimization:
`/tmp/tidb-value-division-fast-{baseline,green}.log`. Go independently checks
576 scalar/vector combinations across scales, increments, signs and coefficient
bounds with zero warnings. The mixed SQL oracle checks six exact results.

Commands from repository root (temporary overlays preserve original Go tests):

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-value-division-sql-overlay.json -run '^TestMixedIntegerDivisionSQLOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-value-division-checker-overlay.json -run '^(TestGroupChecker.*Oracle|TestVecGroupChecker.*|TestIssue53867)$' -tags=intest,deadlock -count=1 ./pkg/executor/internal/vecgroupchecker

Both pass. Logs: `/tmp/tidb-value-division-go.log` and
`/tmp/tidb-value-division-go-race.log`. Checker race gate now includes 19 test
functions. Go source baseline is unchanged, no Bazel preparation is triggered,
and these Go test packages have no failpoint requirement.

Rust validation, from rust/:

    cargo test --offline --locked -j12 -p tidb-expr --lib go_test_arithmetic_int_divide
    cargo test --offline --locked -j12 -p tidb-expr --lib integer_division_matches_go_signedness_helpers
    cargo test --offline --locked -j12 -p tidb-expr --lib decimal_integer_division_small_coefficients_match_bounded_kernel
    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    cargo test --offline --locked -j12 -p tidb-executor --lib merge
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg
    cargo test --offline --locked -j12 -p tidb-executor --lib shuffle
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window

Repository lint commands:

    make lint
    make -o tools/bin/revive lint

The standard target again fails bootstrapping revive 1.2.1 (module does not
contain the package), exit 2. The existing-tool invocation passes the actual
lint recipes. Logs: `/tmp/tidb-value-division-lint{,-existing}.log`.


Final native results: expression 1190 passed / 97 ignored; executor checker 28,
merge 76, hash aggregate 76, shuffle 26 and window 23 passed; session numeric
domain 8, merge SQL 16 and window SQL 68 passed. Logs use
`/tmp/tidb-value-division-final-<crate>-<filter-or-all>.log`.
The full expression suite required localhost permission for its existing HTTP
test. Existing ignored tests are not counted as verified. Changed Rust regions
are rustfmt-clean; `git diff --check` passes. Self-review removed the obsolete
unsigned float conversion and corrected the new test's source attribution.

Remaining correctness/compatibility scope is explicit: the raw AST evaluator
still eagerly evaluates both children, and general string, NULL-typed, JSON,
temporal and hybrid conversion/evaluation-order contracts need the ongoing
whole-expression audit. This checkpoint does not accept that package or any
physicalop/checker/aggregate/join package. It changes three expression source
files plus this plan and its source inventory. No upstream Go source, fixtures,
generated files or package boundaries changed. Workload-level sysbench, TPC-C,
TPC-H and YCSB validation remains open.

### Performance comparison with fe4e828bd1

Build the unchanged ten-case numeric projection benchmark from the clean
isolated fe4e828bd1 checkout, copying the executable before building current
sources. Commands from each checkout's rust/:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo bench --offline --locked -j12 -p tidb-executor --bench pipeline --no-run
    cargo bench --offline --locked -j12 -p tidb-executor --bench pipeline --no-run

Build logs: `/tmp/tidb-value-division-bench-{before,after}-build.log`.
Run three measurements per executable in before/after/after/before/before/after
order, with no concurrent test or build:

    BENCH_ONLY=numeric_projection /tmp/tidb-value-division-pipeline-before
    BENCH_ONLY=numeric_projection /tmp/tidb-value-division-pipeline-after

Each case uses 1024 rows and five 250ms blocks. Results are component benchmarks,
not end-to-end workload throughput claims. Run logs use
`/tmp/tidb-value-division-bench-{1..6}-{before,after}.log`.


Final medians across three runs per binary:

| Projection | Before ns/row | After ns/row | Time change | Calibrated change |
| --- | ---: | ---: | ---: | ---: |
| numeric_decimal | 31.0 | 30.9 | -0.3% | +3.0% |
| numeric_decimal_div | 223.2 | 225.5 | +1.0% | +2.0% |
| numeric_decimal_intdiv | 174.8 | 100.3 | -42.6% | -42.4% |
| numeric_decimal_mod | 1018.5 | 987.5 | -3.0% | -0.1% |
| numeric_decimal_nested | 44.9 | 44.9 | +0.0% | +1.9% |
| numeric_int | 17.2 | 17.1 | -0.6% | +2.3% |
| numeric_int_div | 16.6 | 16.4 | -1.2% | +0.4% |
| numeric_int_mod | 16.0 | 15.7 | -1.9% | +1.0% |
| numeric_int_nested | 24.3 | 24.5 | +0.8% | +4.2% |
| numeric_real_intdiv | 209.4 | 130.4 | -37.7% | -36.8% |


Decimal DIV measured 42.6% less time and Real DIV 37.7% less time than the
previous correct checkpoint. Other elapsed-time controls range from -3.0% to
+1.0%; calibrated controls range from -0.1% to +4.2%, so no improvement is
claimed for those unchanged kernels. Real DIV still has the cost of Go's
required decimal conversion; comparison with the older incorrect float kernel
is not a correctness-equivalent benchmark. Workload performance is unverified.

Publication validation uses an isolated checkout at fe4e828bd1 with only the
five tracked changed files copied and byte-compared; the two pre-existing
untracked drafts remain excluded. Additional commands from isolated rust/:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-expr --lib integer_division
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain

Logs: `/tmp/tidb-value-division-isolated-{expression,sql}.log`.

Isolated gates passed: two focused expression tests and eight numeric-domain
SQL tests. All test and benchmark processes are terminal. Remote refresh found
no divergence from fe4e828bd1. This is a verified progress checkpoint; the active
whole-package parity and workload optimization goal remains open.


## Implicit string arithmetic audit (2026-09-21)


Pulled b969970ba9 with no changes. Previous turn is verified progress. Continue
the whole-expression dependency audit: String/NULL argument domains bypass the
shared typed arithmetic path, losing scalar short-circuiting and operand-batch
warning/error order. A temporary Go oracle covers six arithmetic signatures,
warning/NULL/strict-error inputs and both modes (36 scenarios). Port these
observations into the existing expression suite before changing dispatch, then
validate SQL consumers and retained numeric performance. Full package acceptance
and the four workload optimization gates remain open.


### Decisions and source findings


The scalar arithmetic dispatcher now admits non-hybrid String operands into
its Real/Decimal argument domains; the same batch evaluator casts an entire
left operand before the right. It still declines binary literal and hybrid
signatures that require their own numeric domain rules. String-to-Real reuses
the existing StrToFloat owner. String-to-Decimal uses MyDecimal::from_string,
retains declared precision/scale, and routes parser errors through the active
statement policy. Source `builtin_cast.go::builtinCastStringAsDecimalSig.evalDecimal`
rewrites ErrTruncated to named DECIMAL error 1292; source
`builtin_cast_vec.go::vecEvalDecimal` passes raw error 1265 through instead.
Empty/digitless input keeps 1292 in both paths; overflow 1690 and Bad Number
8029 retain their identities under error, warning and ignore policies.

The SQL builder folds strict string literal casts using its existing live
statement context. Go BuildCastFunction/FoldConstant raises the cast warning at
construction and retains a typed constant; repeating the cast during row or
batch evaluation was observably wrong. The native fold preserves unsigned/
nullability metadata and subquery reference, refines decimal precision/scale,
and keeps unsuccessful conversions for runtime evaluation. It does not cache
parameter values or deferred constants. Reuse of the resolver's existing
comparison_context supplies the same statement context without adding a new
warning store or statement lifetime.

Red-before-green evidence:
`/tmp/tidb-string-arithmetic-red.log` records scalar NULL and vector diagnostic
mismatches; `/tmp/tidb-string-constant-sql-red.log` records four warnings instead
of one for `SELECT a + '2tail'` over four rows. Green evidence is in
`/tmp/tidb-string-arithmetic-{green,selected}.log`,
`/tmp/tidb-string-division-diagnostics.log`,
`/tmp/tidb-string-arithmetic-sql.log`, and
`/tmp/tidb-string-constant-sql-green.log`.

Go oracle coverage is 72 ordinary/reversed-selection scalar/vector cases, 42
malformed decimal diagnostic cases, and 12 selected-row constant construction/
grouping cases. SQL oracles add 12 NULL/warning cases and 12 constant-warning
cases. Go constant construction emits one warning, while selected-row grouping
then emits none. Native SQL regressions assert the one warning and exact rows.
No Go source or build manifest changed; temporary overlays retain original
tests. Checker and windows packages contain neither failpoint injection nor a
failpoint build dependency, so failpoint toggling is unnecessary.

### Validation receipt


Commands from repository root:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-string-arithmetic-overlay.json -run '^TestGroupCheckerStringArithmeticOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-string-arithmetic-overlay.json -run '^TestGroupCheckerStringDivisionDiagnosticsOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-string-arithmetic-overlay.json -run '^TestGroupCheckerStringConstantOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-string-arithmetic-overlay.json -run '^(TestGroupChecker.*Oracle|TestVecGroupChecker.*|TestIssue53867)$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-string-arithmetic-sql-overlay.json -run '^TestStringArithmeticSQLOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-string-arithmetic-sql-overlay.json -run '^TestStringArithmeticConstantSQLOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    make lint
    make -o tools/bin/revive lint

All final Go runs pass, including the accumulated 22-function checker race
selection. Logs: `/tmp/tidb-string-arithmetic-go-race.log`,
`/tmp/tidb-string-arithmetic-diagnostics-go.log`,
`/tmp/tidb-string-constant-go.log`,
`/tmp/tidb-string-arithmetic-sql-go.log`, and
`/tmp/tidb-string-constant-sql-go.log`. Unlike the prior checkpoint, the standard
make lint target now completes successfully, exit 0. The existing-tool command
also exits 0. Logs: `/tmp/tidb-string-arithmetic-lint-{standard,existing}.log`.
No bazel_prepare trigger applies to these Rust/test/benchmark/doc changes.

Commands from rust/:

    cargo test --offline --locked -j12 -p tidb-expr --lib string_arithmetic_preserves_go_cast_and_batch_order
    cargo test --offline --locked -j12 -p tidb-expr --lib string_division_preserves_go_decimal_parser_diagnostics
    cargo test --offline --locked -j12 -p tidb-session --lib string_arithmetic_nulls_and_warnings_follow_go_vectorization
    cargo test --offline --locked -j12 -p tidb-session --lib implicit_arithmetic_string_literal_cast_warns_once_at_build_time
    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    cargo test --offline --locked -j12 -p tidb-executor --lib merge
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg
    cargo test --offline --locked -j12 -p tidb-executor --lib shuffle
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window

Final native results: expression 1192 passed / 97 ignored; checker 28, merge 76,
hash aggregate 76, shuffle 26, executor window 23, session numeric domain 10,
merge SQL 16, window SQL 68 passed. The existing localhost HTTP test requires
permission outside the network sandbox. Logs:
`/tmp/tidb-string-arithmetic-final-<crate>-<filter-or-all>.log`.


### Performance and implementation refinement


The first six-run comparison found string DIV 36.1% faster (209.7 to 134.0
ns/row) but string addition 2.5% slower (141.7 to 145.2 ns/row). Inspecting the
new path identified a per-cell String/Datum copy between the source column and
implicit cast. The final path borrows source string bytes directly and casts
that complete operand batch before evaluating the right argument. It reuses
the same Real/Decimal conversion helper as scalar/constant callers; only the
vector diagnostic mode differs where Go does. Logical selection indices, NULL
bits, and invalid column-index errors retain the original checks. Source helper
bytes_to_f64 becomes crate-visible so both paths use the same StrToFloat policy.
The 72 selection/order and 42 decimal diagnostic scenarios pass after this
change. Repeat final native gates are justified by this additional code change.

Build the unchanged numeric cases plus two string cases from isolated
b969970ba9 with only the benchmark file updated; copy that executable before
building main. Both fixtures use Go's already-folded typed literal operand.
Commands from each checkout's rust/:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo bench --offline --locked -j12 -p tidb-executor --bench pipeline --no-run
    cargo bench --offline --locked -j12 -p tidb-executor --bench pipeline --no-run

Run each pair three times, before/after/after/before/before/after, without
concurrent builds or tests. Each of twelve cases measures 1024 rows using five
250ms blocks. Baseline and first-corrected logs:
`/tmp/tidb-string-arithmetic-bench-{1..6}-{before,after}.log`. Build logs:
`/tmp/tidb-string-arithmetic-bench-{before,after,optimized}-build.log`.
Final pair commands:

    BENCH_ONLY=numeric_projection /tmp/tidb-string-arithmetic-pipeline-before
    BENCH_ONLY=numeric_projection /tmp/tidb-string-arithmetic-pipeline-optimized

The prior standard make lint result remains valid for its Go recipes; later
changes only move/extract Rust conversion logic and reuse borrowed column bytes.
All changed Rust regions are formatted; no unrelated file-wide formatting is
introduced. Both pre-existing untracked drafts remain excluded. Publication
will include seven tracked files: expression scalar_function, rewriter and
ops/real_coerce; session numeric_domain; executor pipeline benchmark; this
ExecPlan and source inventory.

Remaining scope: hybrid/binary literal numeric signatures, parameter/deferred
cast construction, vectorized computed string arguments, raw-AST child order,
non-UTF-8 diagnostic byte fidelity and other expression/cast/codec contracts
need complete source-to-Rust audit. These are open requirements, not accepted
exceptions. Whole physicalop/checker/aggregate/join/expression packages and
sysbench/TPC-C/TPC-H/YCSB performance are not accepted by these scoped results.


Final three-run medians (negative means less time):

| Projection | Before ns/row | Final ns/row | Time change | Calibrated change |
| --- | ---: | ---: | ---: | ---: |
| numeric_decimal | 30.7 | 31.0 | +1.0% | +2.4% |
| numeric_decimal_div | 216.2 | 213.6 | -1.2% | -1.9% |
| numeric_decimal_intdiv | 99.8 | 99.2 | -0.6% | -1.5% |
| numeric_decimal_mod | 972.3 | 1000.5 | +2.9% | +3.1% |
| numeric_decimal_nested | 44.7 | 45.9 | +2.7% | +1.4% |
| numeric_int | 16.9 | 16.4 | -3.0% | -3.0% |
| numeric_int_div | 16.4 | 16.1 | -1.8% | -0.9% |
| numeric_int_mod | 15.5 | 15.5 | +0.0% | -0.8% |
| numeric_int_nested | 24.1 | 23.3 | -3.3% | -3.1% |
| numeric_real_intdiv | 129.2 | 131.4 | +1.7% | +1.9% |
| numeric_string | 142.2 | 94.7 | -33.4% | -32.3% |
| numeric_string_intdiv | 209.8 | 97.4 | -53.6% | -53.2% |

Final logs: `/tmp/tidb-string-arithmetic-optimized-bench-{1..6}-{before,optimized}.log`.
All timing processes completed successfully before isolated validation.


Borrowing the source string column removes the first version's addition
regression: final string addition measures 33.4% less time and string DIV 53.6%
less time than b969970ba9. Unchanged numeric controls range from -3.3% to +2.9%
in elapsed time (-3.1% to +3.1% calibrated); no gain is claimed for those kernels.
This is a component comparison, not a sysbench/TPC-C/TPC-H/YCSB throughput claim.

Publication checks use an isolated b969970ba9 checkout with only the seven
tracked changed files copied and byte-verified. Commands from its rust/:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-expr --lib string_arithmetic
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-expr --lib string_division
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain

Logs: `/tmp/tidb-string-arithmetic-isolated-{order,diagnostics,sql}.log`.
Self-review checked source warning modes, literal-fold timing/type metadata,
borrowed-byte lifetime and selection order, and exclusion of unrelated drafts.
No new SQL functionality was introduced. Correctness/compatibility evidence is
limited to the source contracts and tests recorded above; the full goal remains
active and package completion is not claimed.

Isolated publication gates passed: one 72-case ordering test, one 42-case
diagnostic test, and all ten numeric-domain SQL tests. All validation, build
and benchmark processes are terminal. The seven-file checkpoint is ready for
the requested commit/push; complete package and workload gates remain open.


## Hybrid and binary arithmetic audit (2026-09-21)


Previous 92bbac3838 checkpoint is verified progress; pulled again with no remote
changes. Continue the entire expression dependency audit at numeric signatures
for ENUM, SET, BIT and binary literals. Go evaluates hybrid operands through
EvalInt before Real/Decimal casts, including SET's floating integer conversion
and overflow. Binary constants use direct typed evaluation instead, preserving
unsigned decimal precision even for signed bit literals. A 108-scenario Go
matrix covers nine input families, six operators and scalar/vector modes.
Port the observations as regressions before changing dispatch, then extend
mixed-type, NULL/error-order and SQL coverage. Whole-package acceptance and
workload performance requirements remain open.


### Hybrid arithmetic discoveries and implementation decisions


The initial 108-scenario native regression failed on SET rounding above 2^53,
SET overflow identity, signed BIT arithmetic and oversized binary literal
conversion (log `/tmp/tidb-hybrid-arithmetic-red.log`). The shared evaluator now
selects Go's typed argument entrypoint: EvalInt for hybrids; direct EvalReal or
EvalDecimal for binary constants. DIV selects its input domain using the same
numeric-context rule as return-type inference. Batch eligibility follows those
entrypoints, and hybrid columns respect NULLs and logical selection before
conversion. `tidb-datatype/src/datum_convert.rs` now reports the existing
ENUM/SET floating signed-conversion overflow rather than replacing it with an
unsupported diagnostic error. Ordinary numeric and string paths are retained.

Go comparison tests are overlays on the existing vecgroupchecker test harness:
`/tmp/tidb-hybrid-arithmetic-oracle.go` and
`/tmp/tidb-hybrid-arithmetic-overlay.json`. Additional captures cover 96 mixed
Real/Decimal scenarios, 12 signed-boundary scenarios, 24 NULL/selection cases,
and two real-overflow cases. Native fixtures live in the existing
`go_arithmetic_values.rs`. Scalar decimal DIV overflow displays expressions;
vector DIV displays evaluated decimal operands. That source difference is
intentional. Binary integer diagnostics render the signed folded carrier when
appropriate; unsigned decimal/real conversions preserve the literal value.

The Go SQL overlay `/tmp/tidb-hybrid-arithmetic-sql-overlay.json` runs
`TestHybridArithmeticSQLOracle` in `pkg/executor/windows` and captures 60
queries. The native 42-successful-query fixture initially failed because the
wide binary literal warned three times (once per row), while Go warns once.
The existing implicit string-literal fold now also evaluates binary literals
in their typed domain, preserving unsigned flags and folded decimal shape.
The session fixture passes after the fix. Logs:
`/tmp/tidb-hybrid-arithmetic-sql-{go,red,green}.log`.

All Go production, original test/build artifacts and pinned package inventories
are unchanged. These fixes add no SQL operators or new behavior beyond the Go
reference. No package is accepted by this checkpoint. Remaining obligations
include complete expression and datatype source/test mapping, temporal/JSON
numeric paths, original package test gates and sysbench/TPC-C/TPC-H/YCSB runs.


### Hybrid arithmetic validation receipt


From repository `rust/`, the following gates passed (offline locked Cargo,
12 build jobs). Expression tests used localhost permission for the pre-existing
HTTP test. The final binary real-overflow diagnostic was separately reproduced
red and then verified green by the focused `hybrid_arithmetic` command.

    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-expr --lib hybrid_arithmetic
    cargo test --offline --locked -j12 -p tidb-datatype --lib
    cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    cargo test --offline --locked -j12 -p tidb-executor --lib merge
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg
    cargo test --offline --locked -j12 -p tidb-executor --lib shuffle
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    cargo bench --offline --locked -j12 -p tidb-executor --bench pipeline --no-run

Results: expression 1,194 passed/97 ignored; datatype 424 passed; checker 28,
merge 76, aggregate 76, shuffle 26 and window 23 passed; session numeric 11,
merge 16 and window 68 passed. Logs use `/tmp/tidb-hybrid-arithmetic-` followed
by `expr-full.log`, `final-focused.log`, `datatype.log`,
`executor-<filter>.log`, `session-<filter>.log` and `bench-build.log`.
The two new expression fixtures pin 242 source scenarios (218 numeric/error
observations plus 24 NULL/selection scenarios). The SQL fixture pins 42 queries.
The real-overflow red log is `/tmp/tidb-hybrid-arithmetic-realmax-red.log`.

Repository-root `make lint` passed, logged to
`/tmp/tidb-hybrid-arithmetic-lint.log`. No tracked Go/import/Bazel/module
changes require bazel_prepare. Go overlays are external reference probes, not
tracked package edits. The reference harnesses do not require failpoint toggles.
Their existing original test artifacts remain part of the package acceptance
obligations rather than being replaced by these probes.

The disposable publication checkout was byte-verified against published
92bbac3838 before resetting it to that commit. Only the seven changed tracked
files were then copied and byte-verified. Existing unrelated untracked
vs_helper.rs and fragment.rs remain excluded from implementation and validation.
The remote branch was fetched again and had zero divergence before publication.


### Hybrid arithmetic performance and isolated publication checks


The final benchmark compares the saved published-92bbac3838 binary
`/tmp/tidb-string-arithmetic-pipeline-optimized` with
`/tmp/tidb-hybrid-arithmetic-pipeline`, built from the final code. Four pairs
alternate before/after execution order, with no concurrent tests or builds.
Each invocation uses `BENCH_ONLY=numeric_projection`; logs are
`/tmp/tidb-hybrid-arithmetic-bench-{1..4}-{before,after}.log` and the median
summary is `/tmp/tidb-hybrid-arithmetic-bench-summary.json`.

| Projection | Before ns/row | After ns/row | Time change | Calibrated change |
| --- | ---: | ---: | ---: | ---: |
| numeric_decimal | 32.70 | 32.20 | -1.5% | -2.1% |
| numeric_decimal_div | 227.65 | 226.10 | -0.7% | -0.6% |
| numeric_decimal_intdiv | 104.75 | 106.25 | +1.4% | +0.9% |
| numeric_decimal_mod | 1065.80 | 1047.80 | -1.7% | -2.4% |
| numeric_decimal_nested | 48.15 | 47.25 | -1.9% | -2.0% |
| numeric_int | 17.30 | 17.70 | +2.3% | +1.5% |
| numeric_int_div | 17.05 | 17.15 | +0.6% | -0.1% |
| numeric_int_mod | 16.40 | 16.40 | +0.0% | +0.4% |
| numeric_int_nested | 24.30 | 25.20 | +3.7% | +1.9% |
| numeric_real_intdiv | 138.90 | 140.90 | +1.4% | +0.8% |
| numeric_string | 100.60 | 98.75 | -1.8% | -2.3% |
| numeric_string_intdiv | 103.50 | 104.20 | +0.7% | -0.0% |

Existing numeric controls vary from -1.9% to +3.7% in time and -2.4% to +1.9%
after calibration. No speedup is claimed for this correctness checkpoint.
Hybrid-specific performance and full sysbench/TPC-C/TPC-H/YCSB throughput
remain unmeasured by this component comparison.

The final source reference gate includes 23 overlay oracle functions and the
four original checker tests. From repository root:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-hybrid-arithmetic-overlay.json -run '^(TestGroupChecker.*Oracle|TestVecGroupChecker.*|TestIssue53867)$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-hybrid-arithmetic-sql-overlay.json -run '^TestHybridArithmeticSQLOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

Isolated checks, from `/private/tmp/tidb-parity-publish-aba629bb/rust`:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-expr --lib
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain

Logs are `/tmp/tidb-hybrid-arithmetic-isolated-{expr,sql}.log`.
Changed tracked files are `rust/crates/tidb-datatype/src/datum_convert.rs`,
`rust/crates/tidb-expr/src/builtin_arithmetic.rs`,
`rust/crates/tidb-expr/src/scalar_function.rs`,
`rust/crates/tidb-expr/src/tests/go_arithmetic_values.rs`,
`rust/crates/tidb-session/src/tests_core/numeric_domain.rs`, this ExecPlan and
`rust/docs/planner/physicalop-source-inventory.md`.


Final isolated checks passed: 1,194 expression tests (97 existing ignored) and
all 11 numeric SQL tests. All 27 Go reference/original checker test functions
passed with the race detector, and the Go SQL probe passed. All builds, tests
and benchmark processes are terminal. Final self-review checked typed domain
selection, signed carriers, conversion error identity, scalar versus vector
NULL/error ordering, binary cast fold timing and exclusion of unrelated drafts.
`git diff --check` passes. The seven-file checkpoint is ready for the requested
commit and push; the overall goal and whole-package acceptance remain open.


## Temporal and JSON arithmetic audit (2026-09-21)


Published hybrid checkpoint 93c0593c20 is verified progress. Pulled again with
no remote changes. Continue the expression dependency audit with temporal and
JSON numeric signatures: source precision selects Int versus Decimal temporal
arguments, and JSON DIV must retain exact integer/decimal conversion while
other operators evaluate Real. Capture Go outcomes before changing Rust and
preserve warning/error ordering. All whole-package and workload gates remain
open; this stage does not redefine acceptance around these signatures.


### Temporal and JSON source decisions and regressions


The 144-case warning-mode native regression failed before implementation
(`/tmp/tidb-temporal-json-arithmetic-red.log`), with JSON operands rejected as
unsupported. Temporal values previously used the scalar fallback; batch
eligibility did not cover their typed source domains. The final matrix extends
to 288 scenarios with strict error mode, and explicitly requires the numeric
batch evaluator to accept every vector case.

`scalar_function.rs` now admits Datetime/Timestamp/Duration source types into
Int/Real/Decimal argument conversion and JSON into Real/Decimal. Int batches
whose operands require temporal casts run the full ordered argument passes
before integer arithmetic, avoiding reads of temporal storage as integer chunk
cells. The existing compact integer path remains for actual integer operands.
Temporal decimal conversion uses ToNumber before precision/scale fitting.
JSON real conversion uses the existing source numeric converter and reports
FLOAT for nonnumeric JSON versus DOUBLE for a truncated JSON string. JSON DIV
uses the datatype contextful decimal converter, preserving exact integers above
2^53 and the source's raw 1265 decimal-string truncation diagnostic.

Go overlays are `/tmp/tidb-temporal-json-arithmetic-overlay.json` and
`/tmp/tidb-temporal-json-arithmetic-sql-overlay.json`. The first includes
TestGroupCheckerTemporalJSONArithmeticOracle in the unchanged checker harness;
the second includes TestTemporalJSONArithmeticSQLOracle and
TestTemporalJSONNullSQLOracle in the unchanged windows SQL harness. Go captures
are `/tmp/tidb-temporal-json-arithmetic-{go,sql-go,null-sql-go}.log`.
Native focused gates passed in `strict.log` and `sql-native.log` under the same
prefix. SQL fixtures cover fractional DATETIME/TIME, JSON integers and JSON
null, and the distinct scalar/vector short-circuit rules for NULL-left operands.

This audit does not accept the whole expression package. Full original
source/test mapping, computed-argument vectorization, temporal edge/error and
platform/timezone coverage, and all four workload performance gates remain
open. No new SQL behavior beyond the source implementation was introduced.


### Temporal and JSON validation receipt


The change touches only `rust/crates/tidb-expr/src/scalar_function.rs`, its
existing `tests/go_arithmetic_values.rs`, the existing session
`tests_core/numeric_domain.rs`, this plan and `physicalop-source-inventory.md`.
No Go/import/Bazel/module files changed; bazel_prepare is not required. The Go
checker/windows probe harnesses do not require failpoint toggles.

From `rust/`, these native commands passed:

    cargo test --offline --locked -j12 -p tidb-expr --lib temporal_json_arithmetic_uses_go_numeric_casts
    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-session --lib temporal_and_json_arithmetic_match_go_sql
    cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    cargo test --offline --locked -j12 -p tidb-executor --lib merge
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg
    cargo test --offline --locked -j12 -p tidb-executor --lib shuffle
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window
    cargo bench --offline --locked -j12 -p tidb-executor --bench pipeline --no-run

Full expression: 1,195 passed/97 existing ignored. Executor checker 28, merge
76, aggregate 76, shuffle 26 and window 23 passed. Session numeric 12, merge 16
and window 68 passed. Logs use prefix `/tmp/tidb-temporal-json-arithmetic-`
and suffixes `expr-full.log`, `executor-<filter>.log`, `session-<filter>.log`.
The full expression suite's existing HTTP test required localhost access.

From repository root, the accumulated 28-function source gate and SQL probes
passed, as did the required `make lint`:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-temporal-json-arithmetic-overlay.json -run '^(TestGroupChecker.*Oracle|TestVecGroupChecker.*|TestIssue53867)$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-temporal-json-arithmetic-sql-overlay.json -run '^TestTemporalJSONArithmeticSQLOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-temporal-json-arithmetic-sql-overlay.json -run '^TestTemporalJSONNullSQLOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    make lint

The final Go race log is `go-race.log` and lint log `lint.log` under that same
prefix. Whole-package gates, ignored original tests and workload measurements
are not discharged by these targeted integration checks.

The disposable checkout was first verified byte-for-byte against published
93c0593c20, then reset to that commit. Only the five changed tracked files were
copied and byte-verified for isolated validation. The pre-existing untracked
vs_helper.rs and fragment.rs drafts remain untouched and excluded.


### Temporal and JSON performance controls and isolated gates


Four alternating benchmark pairs compare the published 93c0593c20 binary
`/tmp/tidb-hybrid-arithmetic-pipeline` with the final
`/tmp/tidb-temporal-json-arithmetic-pipeline`, using the same existing twelve
numeric projections. Every invocation sets `BENCH_ONLY=numeric_projection`.
No tests/builds ran concurrently. Logs:
`/tmp/tidb-temporal-json-arithmetic-bench-{1..4}-{before,after}.log`;
medians: `/tmp/tidb-temporal-json-arithmetic-bench-summary.json`.

| Projection | Before ns/row | After ns/row | Time change | Calibrated change |
| --- | ---: | ---: | ---: | ---: |
| numeric_decimal | 31.75 | 32.55 | +2.5% | +2.2% |
| numeric_decimal_div | 230.60 | 237.95 | +3.2% | +2.4% |
| numeric_decimal_intdiv | 106.30 | 108.00 | +1.6% | +0.0% |
| numeric_decimal_mod | 1042.10 | 1082.80 | +3.9% | +3.0% |
| numeric_decimal_nested | 47.45 | 47.55 | +0.2% | +1.5% |
| numeric_int | 17.85 | 17.70 | -0.8% | -0.6% |
| numeric_int_div | 17.35 | 17.50 | +0.9% | +1.0% |
| numeric_int_mod | 16.45 | 16.90 | +2.7% | +1.4% |
| numeric_int_nested | 25.35 | 24.95 | -1.6% | -2.3% |
| numeric_real_intdiv | 139.70 | 143.20 | +2.5% | +1.7% |
| numeric_string | 98.85 | 100.70 | +1.9% | +2.4% |
| numeric_string_intdiv | 103.90 | 102.80 | -1.1% | -0.5% |

The controls vary from -1.6% to +3.9% in time and -2.3% to +3.0% after
calibration; no performance gain is claimed. This comparison does not benchmark
the newly admitted JSON/temporal shapes or establish workload performance.
Those measurements and full sysbench/TPC-C/TPC-H/YCSB comparisons remain open.

From `/private/tmp/tidb-parity-publish-aba629bb/rust`, isolated validation uses:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-expr --lib
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain

The logs are `/tmp/tidb-temporal-json-arithmetic-isolated-{expr,sql}.log`.
The remote was fetched again with zero divergence from the published parent.
Additional audit work remains for temporal constant overflow rendering and
precision/error/timezone extremes, JSON extremes/constant casts, and computed
argument vectorization. These are explicit remaining whole-expression work,
not waived acceptance criteria.


Final isolated gates passed: 1,195 expression tests (97 existing ignored) and
all 12 numeric SQL tests. All build, test and timing processes are terminal.
Self-review verified source domain selection, exact JSON decimal conversion,
warning/error identities, ordered argument batches, NULL handling and source
precision propagation. Final code bytes match the isolated validated patch;
`git diff --check` passes. The five-file checkpoint is ready for the requested
commit/push. No package-completion or workload-performance claim is made.


## Arithmetic constant casts and whole-expression inventory (2026-09-21)


The preceding 5427042cea checkpoint is verified progress; pulled again with no
changes. The older expression divergence sweep contains dated blanket "done"
claims that recent executed counterexamples contradict. A complete pinned
expression inventory now enumerates all 133 direct artifacts and 11 generator/
module inputs, with every artifact unaccepted pending atomic package evidence.
Continue construction-time numeric constant casts through both the AST builder
and direct NewFunction family, including warnings, operand/return metadata and
overflow diagnostics. Go remains the source of values and observable behavior.


### Constant-cast source decisions and regression evidence

The direct NewFunction family now shares implicit numeric argument preparation
with the AST arithmetic builder. Only strict Constant nodes are folded; parameter
and deferred constants retain their context dependence. Successful casts retain
source unsignedness and refine decimal precision. Failed folds leave evaluation
for runtime. Result-domain selection is unchanged. Go's +, -, *, and / classes
read the cast argument types when deriving return precision; MOD snapshots the
original types before casts and must keep that separate behavior.

The 144-case direct-construction regression failed on the published parent
(`/tmp/tidb-arithmetic-constant-red.log`) and passes after the fix. It covers
12 temporal/JSON families across six operators and scalar/vector execution,
checks argument and result metadata, construction-time warnings, two-row values,
and absence of repeated runtime warnings. The existing temporal/JSON fixture
matrix is reused. A SQL regression covers 24 DATE and fractional TIMESTAMP
literal cases, including NULL rows, values and protocol-visible precision.

The Go reference confirms explicit CAST(... AS JSON) is not a strict Constant
node: those casts remain runtime expressions. Do not recursively fold arbitrary
constant-looking trees. The SQL probe also records an additional DIV warning
for explicit JSON casts; its full native construction/evaluation correspondence
is still open. Temporal overflow rendering and precision/timezone extremes,
computed-argument vectorization, all original expression artifacts and 97
ignored native tests remain open package obligations.

The complete expression manifest verifies 133 direct artifacts and 11 generator/
module inputs against both checkout bytes and pinned Git objects. Its status
is unaccepted. Historical blanket arithmetic completion/skip claims in the old
divergence inventory are explicitly superseded by this whole-package audit.

### Constant-cast validation receipt

Changed production files: tidb-expr's builtin_arithmetic.rs, new_function.rs,
rewriter.rs and scalar_function.rs. Tests: existing go_arithmetic_values.rs and
session tests_core/numeric_domain.rs. Documentation: this plan, the historical
expr-builtin-divergence-inventory.md and new expression-package-source-inventory.md.
No Go/import/Bazel/module changes were made; bazel_prepare is not required.
The Go checker/windows probe packages do not require failpoint toggles; they
are not substitutes for the original full expression package gate.

Commands from rust/:

    cargo test --offline --locked -j12 -p tidb-expr --lib arithmetic_constant_casts_fold_before_evaluation_and_shape_result_metadata
    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-session --lib temporal_constant_arithmetic_metadata_matches_go_sql
    cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    cargo test --offline --locked -j12 -p tidb-executor --lib merge
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg
    cargo test --offline --locked -j12 -p tidb-executor --lib shuffle
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window

Commands from repository root:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-arithmetic-constant-overlay.json -run '^TestGroupCheckerArithmeticConstantCastOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-arithmetic-constant-overlay.json -run '^(TestGroupChecker.*Oracle|TestVecGroupChecker.*|TestIssue53867)$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-arithmetic-constant-sql-overlay.json -run '^TestArithmeticConstantCastSQLOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    make lint
    git diff --check

The first full native expression run passed 1,196 tests, with 97 existing
ignored. Focused SQL passed; all 29 accumulated Go checker/reference functions
passed with the race detector, and the 48-query Go SQL probe passed. make lint
passed. Logs use /tmp/tidb-arithmetic-constant- with suffixes expr-full.log,
sql-native.log, go.log, go-race.log, sql-go.log and lint.log. A first native SQL
command was accidentally invoked from the repository root (no Cargo.toml) and
was rerun successfully from rust/.

Consumer and isolated results are recorded below when complete. The disposable
checkout was verified to contain only bytes from published 5427042cea before
resetting to that commit and copying the nine intended files. The pre-existing
untracked vs_helper.rs and fragment.rs drafts are excluded and untouched.

This patch changes construction and literal-folding behavior; the existing
projection microbenchmark constructs ScalarFunction directly and does not
measure the changed builders. No performance improvement is claimed from that
unrepresentative control. Equivalent sysbench/TPC-C/TPC-H/YCSB measurements,
original whole-package gates and generated-input regeneration remain open.


Consumer gates passed: executor checker 28, merge 76, aggregate 76, shuffle 26,
and window 23; session numeric 13, merge 16 and window 68. Logs are
/tmp/tidb-arithmetic-constant-tidb-<executor|session>-<filter>.log.
Isolated validation runs from /private/tmp/tidb-parity-publish-aba629bb/rust:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-expr --lib
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain

Logs are /tmp/tidb-arithmetic-constant-isolated-{expr,sql}.log. Review checked
construction timing, source-type preservation, MOD's distinct metadata rule,
context-bound constant exclusions and untouched unrelated drafts.


Final isolated gates passed: 1,196 expression tests (97 existing ignored) and
13 SQL numeric tests. All changed Rust bytes match the isolated checkout, all
validation processes are terminal, and git diff --check passes. The remote
fetch confirmed zero divergence from the published parent. This nine-file
checkpoint is ready for the requested commit and push; no whole-package or
workload-performance acceptance is claimed.


## Strict decimal-cast precision probes (2026-09-21)


Published 43ae62aa26 is verified progress. Pulled again; branch was current.
Continue the whole-expression audit at WrapWithCastAsDecimal's ConstStrict
precision probe. Go evaluates a retained scalar expression during construction
without replacing it with a Constant; explicit JSON casts expose this through
one extra DIV warning before per-row execution. Add regressions against the
captured Go SQL oracle, check direct construction and metadata, then run native
consumers, Go references and lint before committing/pushing. This remains
whole-package audit progress, not acceptance of an arithmetic subset.


### Precision-probe source decisions and discovered JSON metadata


Go builtin_cast.go WrapWithCastAsDecimal invokes EvalDecimal for every
ConstStrict cast, independently of FoldConstant. Successful strict literal
folds already supply the value. Retained scalar trees and failed literal folds
must still be probed, with warnings preserved and errors left for runtime.
Native arithmetic preparation now performs that probe and uses the resulting
precision for return metadata without replacing the source scalar tree.
Context-bound expressions are excluded. MOD retains original argument metadata.

The SQL regression first failed because explicit JSON DIV emitted three
runtime warnings instead of Go's construction warning plus three runtime
warnings. After the probe fix, it exposed JSON MOD's wrong return metadata.
Pinned pkg/parser/expr_cast_parser.go applies mysql's JSON CAST defaults,
flen 4,194,304 and decimal 0; Rust's cast target had left both unspecified.
The explicit JSON target now carries the source defaults. This also fixes
fractional DIV: CAST('1.9' AS JSON) DIV 1 returns 2 and reports the decimal
rounding warning, exactly as Go. The defaults belong to explicit SQL casts;
low-level JSON fields retain their existing unspecified metadata.

The 48-query native SQL matrix checks both execution modes, NULL rows, result
metadata, exact JSON integer conversion, positive/negative fractions and
truncated JSON strings with ordered parse/rounding warnings. The 36-case direct
builder regression covers retained JSON, REAL arithmetic, string concat and
temporal IFNULL trees in warning/error modes; it checks metadata, build
warnings, deferred error identity and repeated runtime evaluation. The existing
cast-target metadata test now checks the JSON width and scale explicitly.

Red logs: /tmp/tidb-decimal-probe-sql-red.log (missing probe); the first
sql-green.log attempt exposed MOD metadata, then was rerun successfully after
the JSON default correction. Final fractional SQL evidence is sql-fraction.log.
Direct source/native evidence: go.log and native.log under the same prefix.
Go probes use /tmp/tidb-decimal-probe-{overlay,sql-overlay}.json. The first
fractional oracle treated expected overflow as an unexpected error; the harness
was corrected to capture errors as reference data and the full probe passed.

The 1e60 JSON SQL case establishes an additional open obligation: Go's scalar
DIV overflow contains retained nested casts with refined decimal(61,0), while
its vector overflow renders the evaluated numeric operands. Native computed
argument vectorization and diagnostic structure still need their complete
source audit. Internal cast-node identity and retained argument metadata are
also not accepted by these value/return-metadata checks. No whole expression
package acceptance is claimed.

### Precision-probe validation receipt


Changed files: rust/crates/tidb-expr/src/scalar_function.rs,
rewriter/result_type.rs, rewriter/result_type_tests.rs,
tests/go_arithmetic_values.rs, rust/crates/tidb-session/src/tests_core/numeric_domain.rs
and this plan. No Go/import/Bazel/module files changed; bazel_prepare is not
required. Rechecked the Go checker/windows package sources and BUILD.bazel:
no failpoint/testfailpoint/dependency markers, so those harnesses need no
failpoint toggles. Original whole-expression tests still require their gate.

Commands from rust/:

    cargo test --offline --locked -j12 -p tidb-session --lib json_constant_cast_arithmetic_warnings_match_go_sql
    cargo test --offline --locked -j12 -p tidb-expr --lib strict_scalar_decimal_precision_probes_preserve_runtime_evaluation
    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    cargo test --offline --locked -j12 -p tidb-executor --lib merge
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg
    cargo test --offline --locked -j12 -p tidb-executor --lib shuffle
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain
    cargo test --offline --locked -j12 -p tidb-session --lib json
    cargo test --offline --locked -j12 -p tidb-session --lib cast
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window

Commands from repository root:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-decimal-probe-overlay.json -run '^TestGroupCheckerStrictDecimalPrecisionOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-decimal-probe-overlay.json -run '^(TestGroupChecker.*Oracle|TestVecGroupChecker.*|TestIssue53867)$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-decimal-probe-sql-overlay.json -run '^(TestArithmeticConstantCastSQLOracle|TestStrictDecimalJSONFractionSQLOracle)$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    make lint
    git diff --check

Full expression validation passed 1,197 tests with 97 existing ignored. The
accumulated 30 Go checker/reference functions passed with the race detector.
The Go SQL capture covers 78 queries. make lint passed. Logs use prefix
/tmp/tidb-decimal-probe-; native broad logs are tidb-<crate>-<filter>.log,
Go logs go.log, go-race.log and sql-go.log, lint log lint.log.

Runtime kernels are unchanged by the precision probe; the explicit JSON scale
correction deliberately changes the previous incorrect fractional DIV behavior.
No microbenchmark speedup or workload acceptance is claimed. Comparable full
sysbench/TPC-C/TPC-H/YCSB runs, original expression tests, generated artifact
regeneration, and ignored native cases remain open. The two unrelated untracked
drafts are untouched and excluded from isolated validation and commit.


Consumer results: executor checker 28, merge 76, aggregate 76, shuffle 26,
window 23; session numeric 14, JSON 23, cast 33, merge 16 and window 68, all
passed. Self-review checked source JSON CAST defaults, warning/error sequencing,
strict-versus-context constant handling, retained runtime trees and decimal
result precision. Remote fetch confirmed no divergence from 43ae62aa26.

The disposable checkout's prior files were verified against published
43ae62aa26 (only the final documentation receipt was absent), then reset to
that commit. Exactly six intended files were copied and byte-checked. Commands
from /private/tmp/tidb-parity-publish-aba629bb/rust:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-expr --lib
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib json
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib cast

Isolated logs: /tmp/tidb-decimal-probe-isolated-{expr,numeric_domain,json,cast}.log.
The first isolated gate passed all 1,197 expression tests (97 existing ignored).


Final isolated SQL gates passed: numeric 14, JSON 23 and cast 33. All validation
processes are terminal, final Rust bytes match the isolated tested patch, and
git diff --check passes. The six-file checkpoint is ready for the requested
commit/push. Whole-package acceptance and workload performance remain open.


## Computed JSON arithmetic batches and diagnostics (2026-09-21)


Published 478bf5706c is verified progress; pulled again without remote changes.
Continue the whole-expression audit at computed operands. Go's cast-to-JSON
vector signatures evaluate the source batch first, then JSON conversion, then
the outer numeric conversion. Rust's arithmetic preflight currently declines
that tree and falls back to row evaluation. Capture NULL, selection and error/
warning ordering against Go, add red regressions, and admit only source trees
whose ordered evaluation is implemented. Also match scalar DIV's retained-cast
overflow text and vector DIV's value-based text. Preserve native build/runtime
boundaries and all whole-package/workload gates; no subset acceptance claim.


### Computed JSON source decisions and regression evidence


The 72-case checker probe covers string-to-JSON operands across six arithmetic
operators, scalar/vector execution, selected rows, valid strings, malformed
left JSON and NULL-left/invalid-right rows. Go parses the entire left source
batch before numeric conversion, then evaluates the right batch. Therefore an
invalid second left row precedes the first numeric warning; an invalid right
row follows all left numeric warnings but precedes right numeric warnings.
A further 16 cases check binary opaque JSON and disabled ParseToJSONFlag.
Both source probes passed. Their first vector invocation required the normal
Vectorized() initialization; the temporary harness was corrected after its
uninitialized buffer allocator panic. No Go implementation was changed.

Native preflight now admits the existing string-to-JSON cast signature only
when its source tree already has an ordered batch evaluator and is not a hybrid
string field. Numeric, temporal and hybrid-source JSON cast vector signatures
remain open audit work. JSON parsing and typed opaque/value conversion reuse
the existing scalar conversion helpers after the source batch completes.
The full preflight still runs before any evaluation; a declined tree emits no
warnings. The 88 native scenarios match the Go reference values/error class,
warning order, NULL short-circuit distinction, selection and parse flags.
The pre-implementation test failed because the batch path declined JSON casts
(/tmp/tidb-json-batch-red.log); the final focused test passed (green.log).

Scalar decimal DIV now calls the same bounded decimal quotient primitive
without consuming its operands first, so overflow rendering can use the
already-evaluated decimal precision. Retained JSON casts render their source
text and FieldType.String form; strict computed decimal casts show the refined
precision. This never re-evaluates a cast to construct the error. Vector DIV
continues to render evaluated decimal values. The SQL regression failed with
an unqualified BIGINT overflow before the fix (sql-red.log), then passed the
exact scalar/vector messages for CAST('1e60' AS JSON) DIV a (sql-green.log).
Other computed function renderings and cast-node structural identity remain
explicit open expression obligations.

### Computed JSON validation and performance plan


Changed files: rust/crates/tidb-expr/src/scalar_function.rs and its existing
tests/go_arithmetic_values.rs, session tests_core/numeric_domain.rs, executor
benches/pipeline.rs and this plan. No Go/import/Bazel/module files changed;
bazel_prepare is not required. The unchanged checker/windows Go harnesses
have no failpoint imports/calls/build dependency, as verified in the preceding
stage. The full original expression package still needs failpoint-aware gates.

The new numeric_json_real and numeric_json_intdiv controls extend the existing
pipeline benchmark through EvaluatorSuite with valid string columns, explicit
JSON casts and arithmetic. They measure parsing and conversion without warning
collection. The baseline uses published 478bf5706c plus only the identical
benchmark source; its disposable checkout was byte-verified before reset.
The saved baseline executable is /tmp/tidb-json-batch-before-pipeline, built
with CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target and:

    cargo bench --offline --locked -j12 -p tidb-executor --bench pipeline --no-run --message-format=json

Do not time benchmarks concurrently with builds/tests. Compare alternating
before/after processes with BENCH_ONLY=numeric_projection and retain the
existing twelve numeric controls. Record medians and calibrated ratios; these
component measurements cannot establish sysbench/TPC-C/TPC-H/YCSB parity.


### Computed JSON executed validation receipt


From rust/, the focused regressions and consumer gates passed:

    cargo test --offline --locked -j12 -p tidb-expr --lib json_cast_arithmetic_batches_preserve_go_operand_passes
    cargo test --offline --locked -j12 -p tidb-session --lib computed_json_div_overflow_matches_go_scalar_and_vector_diagnostics
    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-executor --lib vec_group_checker
    cargo test --offline --locked -j12 -p tidb-executor --lib merge
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg
    cargo test --offline --locked -j12 -p tidb-executor --lib shuffle
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain
    cargo test --offline --locked -j12 -p tidb-session --lib json
    cargo test --offline --locked -j12 -p tidb-session --lib cast
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window

Expression: 1,198 passed, 97 existing ignored. Executor checker 28, merge 76,
aggregate 76, shuffle 26 and window 23 passed. Session numeric 15, JSON 24,
cast 33, merge 16 and window 68 passed. Logs are
/tmp/tidb-json-batch-tidb-<crate>-<filter>.log. Full expression's existing HTTP
test requires localhost access. No unresolved validation process was restarted.

From repository root, source probes, the accumulated 32-function Go race gate
and required lint passed:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-json-batch-overlay.json -run '^TestGroupCheckerJSONCastBatchOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-json-batch-overlay.json -run '^TestGroupCheckerJSONCastVariantOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-json-batch-overlay.json -run '^(TestGroupChecker.*Oracle|TestVecGroupChecker.*|TestIssue53867)$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker
    make lint
    git diff --check

Go logs use prefix /tmp/tidb-json-batch- and suffixes go.log, variant-go.log
and go-race.log; lint.log records make lint. The SQL overflow reference is
the preceding stage's /tmp/tidb-decimal-probe-sql-go.log, captured from the
same unchanged Go revision. Original package-wide/source-generation gates,
remaining computed signatures, ignored native tests and all workload-level
measurements remain open.


### Computed JSON measured performance and isolated gate


Both benchmark binaries use the identical pipeline.rs source. The final
executable is /tmp/tidb-json-batch-after-pipeline, built from the working patch
with the same release command as the baseline. Four pairs alternate order:
before/after, after/before, before/after, after/before. Each process uses:

    BENCH_ONLY=numeric_projection /tmp/tidb-json-batch-before-pipeline
    BENCH_ONLY=numeric_projection /tmp/tidb-json-batch-after-pipeline

No build/test processes ran concurrently with timing. Logs:
/tmp/tidb-json-batch-bench-{1..4}-{before,after}.log. Machine-readable medians:
/tmp/tidb-json-batch-bench-summary.json.

| Projection | Before ns/row | After ns/row | Time change | Calibrated change |
| --- | ---: | ---: | ---: | ---: |
| numeric_decimal | 32.10 | 32.15 | +0.2% | -0.5% |
| numeric_decimal_div | 252.35 | 230.75 | -8.6% | -7.5% |
| numeric_decimal_intdiv | 108.55 | 107.90 | -0.6% | -0.5% |
| numeric_decimal_mod | 1126.90 | 1049.55 | -6.9% | -6.2% |
| numeric_decimal_nested | 47.15 | 47.55 | +0.8% | -0.0% |
| numeric_int | 19.25 | 17.80 | -7.5% | -7.6% |
| numeric_int_div | 18.65 | 17.45 | -6.4% | -6.8% |
| numeric_int_mod | 17.90 | 16.80 | -6.1% | -6.1% |
| numeric_int_nested | 26.85 | 25.65 | -4.5% | -5.1% |
| numeric_json_intdiv | 562.25 | 453.95 | -19.3% | -19.8% |
| numeric_json_real | 299.85 | 232.85 | -22.3% | -22.7% |
| numeric_real_intdiv | 146.75 | 142.15 | -3.1% | -2.3% |
| numeric_string | 102.70 | 100.95 | -1.7% | -1.4% |
| numeric_string_intdiv | 103.75 | 103.95 | +0.2% | +0.3% |

The measured JSON addition and DIV projections use 22.3% and 19.3% less time
per row respectively (22.7% and 19.8% calibrated). Existing controls range
from -8.6% to +0.8% in elapsed time; do not attribute their incidental changes
to specific optimizations. This is component evidence only and does not prove
performance on any complete benchmark workload or every JSON expression.

The disposable checkout was reset to published 478bf5706c after byte checks,
used for the baseline with only the benchmark change, then received exactly
the five intended changed files. The unrelated untracked drafts are absent.
Isolated validation commands from its rust/ directory:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-expr --lib
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib json
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib cast

Logs: /tmp/tidb-json-batch-isolated-{expr,numeric_domain,json,cast}.log.
The final remote fetch found zero divergence. Self-review checked preflight
before side effects, full operand passes, source JSON parse/opaque semantics,
selection vectors, exact quotient reuse and no extra evaluation in overflow
rendering. No generated files or Go sources were edited.


Final isolated gates passed: 1,198 expression tests (97 existing ignored),
15 numeric SQL tests, 24 JSON tests and 33 cast tests. All build/test/timing
processes are terminal. Final Rust bytes match the isolated tested patch and
git diff --check passes. The five-file checkpoint is ready for the requested
commit and push. Remaining computed signatures/diagnostics, whole-package
acceptance and all four workload performance gates remain open.


## JSON cast source signatures (2026-09-21)


Published 55d4a1caf6 is verified progress. Pulled again, no upstream changes.
Continue the complete expression package audit across the remaining JSON cast
source signatures: integer signedness/boolean/YEAR, REAL and FLOAT, DECIMAL,
DATE/DATETIME/TIMESTAMP/DURATION, hybrid BIT/ENUM/SET and JSON identity. Source
JSON type codes must match, not merely formatted text. Capture Go scalar/vector
results before changes, add red regressions, use source-typed evaluation and
native binary JSON constructors, then validate consumers and commit/push.
Package-level inventory and all original workload/validation gates remain open.


### Source-signature evidence and implementation

Go `builtin_cast.go` dispatches by source EvalType before constructing JSON.
The new reference matrix covers 17 source families in both scalar/vector modes
and with ParseToJSONFlag set/cleared (68 cases, each with a NULL row). Native
regression additionally reverses selection order, for 136 scenarios. It checks
binary JSON type codes and text, not text alone. The independent published-parent
regression fails on unsigned/decimal/hybrid identity, missing typed batches and
the vector NULL cast; log /tmp/tidb-json-source-published-red.log. Initial focused
red and green logs are /tmp/tidb-json-source-{red,green}.log.

Integer signatures preserve unsigned carriers and YEAR's unsigned JSON result;
boolean metadata takes precedence. REAL/FLOAT remain JSON DOUBLE even for an
integral value. DECIMAL uses Go's DOUBLE conversion, including rounding
9007199254740993.0 to 9.007199254740992e15. JSON identity preserves its binary
unsigned code. BIT evaluates the integer signature; ENUM/SET evaluate string
names, parsed as documents only when ParseToJSONFlag is set. Temporal signatures
retain DATE/DATETIME/TIMESTAMP/TIME binary identity and Go's FSP rules in both
parse modes. Binary scalar construction avoids the lossy text round trip.

The scalar cast and supported batch path share source conversion. Batches finish
the complete typed input pass before JSON conversion. Unsupported vector-to-JSON
casts return Go's error before evaluating the argument, including NULL. This
signature is not vectorized in Go: an initial oracle incorrectly called its
unsupported VecEvalJSON method directly and failed with the base-method error.
The corrected oracle asserts Vectorized=false and tests the scalar fallback;
the real SQL path confirms the same error with vectorization enabled/disabled.

Native SQL regression checks six source columns plus vector and NULL-vector
errors in both modes (16 queries). Captured Go SQL reference command:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-json-source-sql-overlay.json -run '^TestJSONSourceSignatureSQLOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

Result: PASS; /tmp/tidb-json-source-sql-go.log. Source oracle and accumulated
34-function race gate use unchanged pinned Go sources with temporary overlays:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-json-source-overlay.json -run '^(TestGroupChecker.*Oracle|TestVecGroupChecker.*|TestIssue53867)$' -tags=intest,deadlock -count=1 -v ./pkg/executor/internal/vecgroupchecker

Result: PASS; /tmp/tidb-json-source-go-race.log. These harness packages have no
failpoint dependency. This does not replace the full expression package's
original failpoint/generator/platform validation gates.

Changed files are the existing JSON value helper, scalar-function evaluator,
Go arithmetic regression module, session numeric SQL regression module and this
plan. No Go, module, Bazel or generated artifact changes; bazel_prepare is not
required. Existing unrelated untracked drafts remain excluded. All expression
inventory rows remain unaccepted; this is dependency audit progress only.


### Validation and review

The JSON consumer run found one historical test explicitly expecting the old
unsigned-identity loss. Go and the new source matrix both prove UNSIGNED INTEGER;
updated that assertion and removed its obsolete exception comment in the existing
session tests_json.rs. The other 24 JSON tests passed before this test correction.
No production change was needed for that failure. This makes six intended files.

From rust/, the full expression test suite and focused consumer gates use:

    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-executor --lib checker
    cargo test --offline --locked -j12 -p tidb-executor --lib merge_join
    cargo test --offline --locked -j12 -p tidb-executor --lib hash_agg
    cargo test --offline --locked -j12 -p tidb-executor --lib shuffle
    cargo test --offline --locked -j12 -p tidb-executor --lib window
    cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain
    cargo test --offline --locked -j12 -p tidb-session --lib json
    cargo test --offline --locked -j12 -p tidb-session --lib cast
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain_merge_join
    cargo test --offline --locked -j12 -p tidb-session --lib tests_window

Expression: 1,199 passed and 97 existing ignored, log
/tmp/tidb-json-source-expr.log. Consumer logs use
/tmp/tidb-json-source-tidb-<crate>-<filter>.log. Root make lint passed
(/tmp/tidb-json-source-lint.log); git diff --check passed. The original package-wide
suite, original source generators/platform gates, ignored tests and all complete
sysbench/TPC-C/TPC-H/YCSB performance gates remain open. This checkpoint makes no
new performance claim; direct binary construction removes a lossy conversion,
but elapsed-time gains must be measured separately. Existing component benchmark
evidence in the preceding stage is unchanged.


The clean checkout independently passed the full expression suite (1,199 passed,
97 ignored), executor merge tests (76), session numeric tests (16), JSON tests
(25) and cast tests (34), with only the six intended files copied. Commands from
/private/tmp/tidb-parity-publish-aba629bb/rust/:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-expr --lib
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-executor --lib merge
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib json
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib cast

Logs /tmp/tidb-json-source-isolated-tidb-<crate>-<filter>.log (expression filter
is all). Other working-tree gates passed: checker 28, hash aggregation 76,
shuffle 26, executor windows 23, session merge 16 and session windows 68.

A final fetch found upstream dc389f42a3, a non-overlapping EXPLAIN-rendering
change. Both working and isolated checkouts fast-forwarded to it. Go source
pin is unchanged. The post-integration gates use the same CARGO_TARGET_DIR
commands with executor filters explain/merge and session filters
 tests_explain_merge_join/numeric_domain/json/cast. Logs use prefix
/tmp/tidb-json-source-integrated-. No upstream code was overwritten.


### Upstream integration regression: integer-handle EXPLAIN

Post-integration cast tests failed only in the existing injected-join-key range
assertion. Upstream dc389f42a3 changed every table probe to eq(inner, outer),
including integer handles. The pinned Go source instead uses
indexJoinIntPKRangeInfo for integer handles (exhaust_physical_plans.go), and the
original join_key_type_cast.result fixture requires `decided by [Column#12]`.
A fresh SQL reference confirmed that exact range for BOTH INL_JOIN and
INL_HASH_JOIN, including the injected cast column:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-json-integration-explain-overlay.json -run '^TestJSONIntegrationIndexJoinExplainOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

PASS, /tmp/tidb-json-integration-explain-go.log. This source evidence supersedes
the upstream comment's generalization from a live-master workload sample.
The correction uses catalog table handle metadata, not key count or join variant,
to select the existing integer-handle renderer. Common-handle rendering and the
other upstream changes remain as integrated. Complete common-handle range/access
condition parity is still an open package obligation; no new acceptance claim.

Extended the existing tests_join_key_cast regression to both join variants.
It failed before the production correction and passed afterward:

    cargo test --offline --locked -j12 -p tidb-session --lib the_hinted_index_join_ranges_over_the_injected_cast_column

Logs /tmp/tidb-json-integration-explain-{red,green}.log. Additional files:
rust/crates/tidb-executor/src/explain.rs and
rust/crates/tidb-session/src/tests_join_key_cast.rs, for eight intended files total.
Required make lint passed again (/tmp/tidb-json-integration-lint.log).
Final isolated gates use the same shared CARGO_TARGET_DIR command prefix with
executor explain and session index_join/tests_explain/cast filters. Logs use
/tmp/tidb-json-source-final-tidb-<crate>-<filter>.log. The production expression
code is unchanged from its successful complete isolated suite.


Final integrated isolated checks passed: executor EXPLAIN 19; session index joins
11 (one existing ignored), EXPLAIN 62 and casts 34. Exact final commands:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-executor --lib explain
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib index_join
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib tests_explain
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 -p tidb-session --lib cast

All validation processes are terminal. Final production/test Rust bytes match the
isolated tested patch. Self-review checked static source dispatch, JSON identity,
parse-flag modes, NULL/error boundaries, selection vectors, complete source passes
and catalog handle-kind selection. git diff --check passed. The eight-file
checkpoint is ready for the user's requested commit and push. No complete package
or workload is marked accepted by this receipt.


## Index-probe access versus residual EXPLAIN audit (2026-09-21)

Published ed32149a2b is verified progress. Pulled again; no upstream changes.
Continue the complete physicalop package audit through its index-probe rendering
dependencies. The secondary-index context currently labels all join residuals as
chosenAccess, while the common-handle renderer drops them all. Neither is source
evidence for the Go distinction between actual range bounds and residual filters.
Capture both handle forms and both index-join variants against pinned Go, then
add red regressions before correcting demonstrated mismatches. Keep whole-package
inventories unaccepted and commit/push validated progress.


### Findings, implementation and validation

The 24-case initial Go matrix covered common handles and secondary indexes,
INL_JOIN and INL_HASH_JOIN, plain equality, runtime bounds, runtime inequality,
non-key residuals, static bounds and two lookup keys. Native red regression
(/tmp/tidb-probe-range-red.log) proved common-handle bound omission, secondary-index
residual leakage, and static-bound omission in both access forms. The focused
24-case green log is /tmp/tidb-probe-range-green.log.

Expanded to 48 Go/native cases with reversed operands, two runtime bounds,
simultaneous static/runtime bounds, static inequality, static non-key residuals,
and a comparison on an already equality-bound key. Go retains the original
comparison expression in its range description, including lt(outer, inner) when
that is the input order; its runtime manager separately normalizes the operator.
When that manager exists, its bounds take precedence over static bounds on the
same trailing key. Native expanded gate passed in
/tmp/tidb-probe-range-expanded-green.log.

Retain the manager's original access expressions during its existing selection
pass; do not re-evaluate them or reconstruct them from all residuals. Retain the
admitted static equality prefix and first trailing range-column predicates using
the existing ranger condition checker. Carry these descriptive snapshots through
IndexJoinInfo and PhysicalIndexJoin, including the explicit physical-plan clone.
The executor consumes that access list for both index and common-handle range
text. Integer handles continue to use the outer-key-only form. This metadata is
not a second set of execution conditions and does not change probe filtering.
No new expression evaluation, warning handling or range-building pass is added.

This remains source audit progress inside the complete physicalop package claim.
The existing runtime access builder's full Eq/IN-prefix selection, memory-quota
fallback, range construction, key-order mapping and cache/generator/platform
obligations still require complete source validation. The admitted static
predicate metadata is not evidence that those unaccepted builder paths are
complete. No package inventory row is marked accepted and no workload performance
claim is made. Additional metadata allocation has not been benchmarked.

Go reference commands from repository root (same pinned Go revision):

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-probe-range-overlay.json -run '^TestProbeRangeAccessExplainOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-probe-range-overlay.json -run '^TestProbeRangeAccessExplainOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

Both passed. Logs /tmp/tidb-probe-range-go.log (initial),
/tmp/tidb-probe-range-go-expanded.log and /tmp/tidb-probe-range-go-race.log.
The unchanged windows harness has no failpoint dependency. No tracked Go,
Bazel, module or generated files changed, so bazel_prepare is not required.
Required make lint passed (/tmp/tidb-probe-range-lint.log).

Native working-tree commands from rust/:

    cargo test --offline --locked -j12 -p tidb-planner --lib index_join
    cargo test --offline --locked -j12 -p tidb-planner --lib physical::
    cargo test --offline --locked -j12 -p tidb-planner --lib task::attach_tests
    cargo test --offline --locked -j12 -p tidb-executor --lib index_join
    cargo test --offline --locked -j12 -p tidb-executor --lib explain
    cargo test --offline --locked -j12 -p tidb-session --lib index_join
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain
    cargo test --offline --locked -j12 -p tidb-session --lib probe_explain

Passed: planner index-join 19, physical 65, attachment 17; executor index-join 19
and EXPLAIN 19; session index-join 12 (one existing ignored), EXPLAIN 62 and
new matrix 1. Logs use /tmp/tidb-probe-range-tidb-<crate>-<filter>.log;
physical/attachment logs use /tmp/tidb-probe-range-planner-physical__.log and
/tmp/tidb-probe-range-planner-task__attach_tests.log.

Six intended files: planner find_best_task/dispatch.rs, physical/mod.rs and
task.rs; executor explain.rs; session tests_index_join_inner_pattern.rs; this
plan. The two unrelated untracked drafts remain excluded. The disposable
checkout's old changes were verified against published ed32149a2b before it
was advanced, then exactly these six files were copied for the isolated gate.


Final isolated gates passed with the same counts. From the disposable rust/
directory, each native command above except the redundant probe_explain filter
was run with CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target. Logs use
/tmp/tidb-probe-range-isolated-tidb-<crate>-<filter>.log, with colons replaced by
underscores. The index_join filter includes the full new 48-case regression.
All processes are terminal, Rust bytes match the isolated tested patch, and
git diff --check passes. Fetch confirmed origin/hparser-integration still at
ed32149a2b. Self-review checked original comparison retention, no repeated
expression evaluation, static/runtime precedence, propagation and explicit clone.
Ready for the requested progress commit and push. Whole-package acceptance,
the ignored session test, and sysbench/TPC-C/TPC-H/YCSB performance gates remain
open.


## Index-probe key ordering and equality-prefix audit (2026-09-21)

Previous turn is verified progress, published as b497d04188. Pulled the branch;
no new upstream changes. Continue the complete physicalop/core dependency audit
with three-column indexes, reordered join equalities, fixed-prefix gaps and IN
prefixes. Compare both EXPLAIN and result rows against Go before deciding changes;
lookup key mapping and execution must agree. Preserve the full package inventory
and outstanding workload gates. No narrower package acceptance claim is made.


### Three-column source evidence and execution correction

Captured 28 Go cases: seven predicates across common handles/secondary indexes
and INL_JOIN/INL_HASH_JOIN. Each case checks range text and sorted result rows.
The predicates reorder three equality keys, interleave one fixed equality,
interleave/lead with IN, leave an unfixed/pruned key gap, or add an outer-derived
trailing bound. The native red log (/tmp/tidb-probe-keys-red.log) proves wrong
EXPLAIN key order, rejected fixed gaps, missing IN lookup keys, and a pruned-column
secondary-index case that incorrectly returned no rows.

The selected-key map now follows the original index positions without collapsing
pruned gaps. EXPLAIN sorts range equality pairs by that map. Index access admission
and feedback recognize constant IN prefixes separately from single-valued equality;
IN is not treated as a single value for ORDER BY or unique-lookup costing. The
existing ranger builds static EQ/IN tuples over the static prefix columns. These
are interleaved with runtime-key placeholders, preserving each complete range
tuple. Invalid/incomplete static templates decline that access candidate instead
of manufacturing missing values. Integer-handle descriptions remain unchanged.

The lookup reader expands each dynamic probe over retained range alternatives.
Alternative columns share a range ordinal, preserving tuple correlation instead
of creating a new Cartesian product of column values. An execution regression
reads a three-column common handle containing all cross combinations and proves
that only the two retained tuples are read. The converted bound-value row remains
indexed by its dynamic-probe ordinal during expansion. Existing cursor reset,
remote fallback and fork-template clone paths keep that flattened cursor position.
Go source: pkg/executor/builder.go buildRangesForIndexJoin iterates lookup content,
then complete ranger templates, and substitutes keys through keyOff2IdxOff.

### Prepared-range reconstruction

The first prepared regression exposed a real stale-range result: executing the
same fixed-prefix statement with 7 then 8 returned no rows for 8. Log
/tmp/tidb-probe-keys-cache-rust.log is red; /tmp/tidb-probe-keys-cache-green.log
is green after retaining static template rebuild inputs. The existing range
rebuild enum now also represents Go mutableIndexJoinRange. Rebuilding rebinds the
static predicates, uses the ranger without a cache-rebuild quota, reconstructs
runtime placeholders and rejects changed range count/width as Go does. The
EXPLAIN access snapshots are rebound too. The task consumes this metadata before
falling back to existing scan-derived point metadata.

Expanded prepared reference has ten executions: fixed equality values 7/8/7,
interleaved IN values (7,8)/(8,9)/(7,8), and leading IN values
(1,2)/(2,3)/(1,1)/(1,2). Rows and @@last_plan_from_cache both match Go, including
cache rejection when duplicates shrink the range count and when it grows again.
The native expanded gate is /tmp/tidb-probe-keys-cache-expanded-rust.log.

### Commands and evidence

Go reference commands from repository root, unchanged source pin:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-probe-keys-overlay.json -run '^TestProbeKeyOrderPrefixOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-probe-keys-overlay.json -run '^TestProbeKeyPrefixPreparedOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-probe-keys-overlay.json -run '^(TestProbeKeyOrderPrefixOracle|TestProbeKeyPrefixPreparedOracle|TestProbeRangeAccessExplainOracle)$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

All passed. Go logs: /tmp/tidb-probe-keys-go.log,
/tmp/tidb-probe-keys-cache-go.log, /tmp/tidb-probe-keys-cache-expanded-go.log,
/tmp/tidb-probe-keys-go-race.log. The race run includes the previous 48-case
access-versus-residual matrix as well as the new 28-case plan/row and ten-execution
prepared matrices. The unchanged windows harness has no failpoint dependency.
Tracked Go/Bazel/module/generated files are unchanged; bazel_prepare is not
required. Required make lint passed (/tmp/tidb-probe-keys-lint.log).

Native working-tree commands from rust/:

    cargo test --offline --locked -j12 -p tidb-session --lib index_probe_key_order_and_fixed_prefix_match_go_plans_and_rows
    cargo test --offline --locked -j12 -p tidb-session --lib prepared_index_probe_fixed_prefix_changes_match_go
    cargo test --offline --locked -j12 -p tidb-planner --lib index_join
    cargo test --offline --locked -j12 -p tidb-planner --lib physical::
    cargo test --offline --locked -j12 -p tidb-planner --lib task::attach_tests
    cargo test --offline --locked -j12 -p tidb-executor --lib index_join
    cargo test --offline --locked -j12 -p tidb-executor --lib explain
    cargo test --offline --locked -j12 -p tidb-executor --lib access_path::
    cargo test --offline --locked -j12 -p tidb-executor --lib physical_builder
    cargo test --offline --locked -j12 -p tidb-executor --lib index_join_static_templates_preserve_tuple_identity
    cargo test --offline --locked -j12 -p tidb-session --lib index_join
    cargo test --offline --locked -j12 -p tidb-session --lib index_probe
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain
    cargo test --offline --locked -j12 -p tidb-session --lib tests_prepared_plan_cache
    cargo test --offline --locked -j12 -p tidb-session --lib tests_sysbench_access

All passed. Planner index join 19, physical 65, attachment 17; executor index join
19 before adding the tuple regression, EXPLAIN 19, access path 39, physical builder
13 and tuple regression 1. Session index join 12 (one existing ignored), new probe
matrices 2, EXPLAIN 62, prepared cache 45 and sysbench access 15. Logs use
/tmp/tidb-probe-keys-tidb-<crate>-<filter>.log and
/tmp/tidb-probe-keys-consumer-tidb-<crate>-<filter>.log, colons replaced by
underscores. Tuple gate: /tmp/tidb-probe-keys-tuples.log.

Ten intended files: planner dispatch, physical_plan_cache, physical/tests and task;
executor access_path, driver/physical_builder, explain and tests_index_join;
session tests_index_join_inner_pattern; this plan. This is package dependency progress.
Remaining full builder quota/error/warning/cost branches, source package-wide
validation, the ignored session test, and all four workload performance gates are
open. Sysbench tests here verify correctness, not throughput. No performance gain
is claimed without measurement. In particular, static template construction and
expansion need workload measurement; source range-memory fallback still requires
its full original gate. No source inventory row is marked accepted.


### Final review: descriptive deferred expressions


Self-review found that rebinding inner_access_conditions with the ordinary
execution binder would evaluate deferred expressions again. Go
pkg/expression/constant.go Constant.StringWithCtx renders DeferredExpr instead
of evaluating it. A new physical-plan regression first failed with one evaluator
call instead of zero (/tmp/tidb-probe-keys-deferred-red.log). The metadata binder
now recursively updates parameter markers without evaluating deferred constants.
The test checks direct and nested parameters, preserves the cached deferred value,
and proves no evaluator is required for descriptions alone.

Commands from rust/:

    cargo test --offline --locked -j12 -p tidb-planner --lib cached_index_join_description
    cargo test --offline --locked -j12 -p tidb-planner --lib physical::

The first command failed before the fix; the second passed 66 tests afterward
(/tmp/tidb-probe-keys-deferred-green.log). This does not change the evaluator used
for execution predicates or actual access-range construction. The existing
plan_trace renderer still rejects parameter/deferred constants; auditing that
renderer against Go remains open, and this change does not claim prepared
EXPLAIN parity.

The isolated checkout /private/tmp/tidb-parity-publish-aba629bb contains only the
intended tracked patch over b497d04188; neither unrelated untracked draft is
copied. The previously listed targeted gates passed there, with executor index
join now 20 tests and session index join 14 tests plus one existing ignored.
After final review, physical (66), index_probe (2), prepared cache (45), and
EXPLAIN (62) passed again with the same cargo commands and this environment prefix from its
rust/ directory:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target

Logs: /tmp/tidb-probe-keys-isolated-deferred-physical.log and
/tmp/tidb-probe-keys-isolated-deferred-<filter>.log. The final make lint retry
initially hit sandbox DNS restrictions fetching revive; rerunning with network
and cache permissions passed, with log /tmp/tidb-probe-keys-final-lint.log.
No additional source-package acceptance or workload performance claim follows
from these targeted gates.


## Continuing expression/physicalop audit: execution-context EXPLAIN


Previous checkpoint 403c269c66 was committed and remote-verified progress. The
next pull was already current. Whole-package inventories and acceptance gates
remain unchanged. Audit current parameter and deferred constants in physical
EXPLAIN, including retained prepared process plans. Go Constant.ExplainInfo
(explain.go) evaluates a constant with the current execution context, whereas
Constant.StringWithCtx (constant.go) reads current parameters and renders a
deferred expression without evaluating it. IndexJoin range descriptions use
StringWithCtx too (index_join_path.go), including unquoted string constants.
Rust plan_trace currently rejects both dynamic constant kinds and receives no
execution context, so reusing the cached planning value would also be incorrect.

Implementation plan: reproduce missing prepared expression text against Go;
thread the existing statement evaluation context through the physical renderer
and process-plan publication; use it to evaluate predicate constants and fetch
projection parameters, while rendering deferred projection/range expressions.
Extend the closest renderer and session suites with current-vs-stale parameter,
deferred evaluation/error, and quoted-vs-unquoted range cases. Preserve Go's
error text and avoid extra evaluations for descriptive StringWithCtx paths.
Validate targeted renderer/EXPLAIN/prepared/index-join consumers, the Go oracle
under race, required lint, then self-review and publish the checkpoint. No
source inventory row is accepted by these dependency fixes alone.

Go baseline: unchanged windows package with temporary overlay
/tmp/tidb-explain-params-overlay.json; log /tmp/tidb-explain-params-go.log.
Its tests and BUILD.bazel have no failpoint/testfailpoint dependency. No tracked
Go/Bazel/module files change, so bazel_prepare is not required.


### Findings and implementation decisions


The original prepared SELECT regression failed with empty projection text
(/tmp/tidb-explain-params-red.log). A direct renderer test failed on a parameter
with stale saved value (/tmp/tidb-explain-dynamic-red.log). Context now flows from
ordinary EXPLAIN and retained process-plan publication to each expression
renderer. ExplainInfo evaluates dynamic constants and preserves Go's error text;
StringWithCtx fetches parameters and renders deferred expressions, preserving
the source's early return before a subquery label. Literal datums remain borrowed
rather than cloned for display. The direct Go oracle verifies parameters,
deferred arithmetic, deferred overflow, conversion to string and parameter
precedence over a deferred expression; the native test additionally checks the
source's release-build missing-parameter fallback.

The prepared index-probe matrix failed because range descriptions quoted strings
(/tmp/tidb-explain-probe-strings-red.log). Source indexJoinPathRangeInfo uses
StringWithCtx, while Selection uses ExplainInfo. Both variants now match Go for
one/two string prefix executions, including the second-execution cache hit.

Context-aware rendering exposed duplicate constant evaluation in the previous
binary-plan renderer, which rendered full and brief trees indiscriminately. The
new counted regression failed with two reads instead of one
(/tmp/tidb-explain-double-red.log). Go binaryOpTreeFromFlatOps rerenders operator
metadata only for table/index readers and the four join names. The native brief
pass now uses that same set, avoiding extra Selection/Projection evaluations.

Expanding the prepared oracle found a construction bug rather than a display
bug: integer column > string parameter retained a mutable marker after integer
refinement, and a heterogeneous IN retained an IN signature where Go builds OR
of typed equalities. The session regression remained red until both underlying
rules were corrected. Comparison construction now applies Go's plan-cache guard
and RemoveMutableConst before value-dependent refinement. A statement-context
seam exposes cache use and records the source-shaped refinement rejection via
the existing PlanCacheTracker (including its force-cache behavior). Integer IN
candidates are refined before the homogeneous-comparison check; heterogeneous
and singleton IN lists use the existing typed binary comparison builder and DNF
composer. Full collation and package-wide rewriter acceptance remain subject to
the original atomic package audit, not this regression matrix.

The final prepared matrix checks six ordinary executions and four index-probe
executions: exact relevant operator expressions, rows and cache-hit values. Go
integer/string refinement has cache hits 0/0; the other pairs have 0/1. Source
race oracle passed all three test functions, log
/tmp/tidb-explain-params-go-race.log. The native expanded matrix passes in
/tmp/tidb-explain-refinement-green.log. Broader expression/consumer validation
and final isolated publication checks are in progress.


### Final validation and remaining scope


The broader IN suite initially failed three old literal-list assertions marked
PREDICTION/UNRUN. Go race recordings of the exact statements confirm two 1292
warnings for each invalid string, ordered by candidate, from conversion and
comparison during RefineComparedConstant. Updated those predictions and their
wire-warning counts to the measured source results; the unchanged subquery
warning checks still pass. Source log /tmp/tidb-explain-in-warnings-go.log.

Final Go commands (repository root):

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-explain-params-overlay.json -run '^TestPreparedExplainConstantsOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-explain-params-overlay.json -run '^TestDynamicExplainConstantsOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -overlay=/tmp/tidb-explain-params-overlay.json -run '^TestIndexProbeStringExplainOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-explain-params-overlay.json -run '^(TestPreparedExplainConstantsOracle|TestDynamicExplainConstantsOracle|TestIndexProbeStringExplainOracle)$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-explain-params-overlay.json -run '^TestInRefinementWarningsOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

All passed. The first draft oracle had a duplicate fmt import, corrected before
behavioral capture. TopSQL decoding intentionally omits operator text, so the
final oracle decodes the ordinary connection format instead. No production Go,
Bazel, module or generated artifact changed. Windows has no failpoint dependency;
no bazel_prepare or failpoint mutation was required.

Native commands from rust/ (also run in the isolated rust/ directory with
CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target):

    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-executor --lib explain
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain
    cargo test --offline --locked -j12 -p tidb-session --lib tests_prepared_plan_cache
    cargo test --offline --locked -j12 -p tidb-session --lib tests_compare_refinement
    cargo test --offline --locked -j12 -p tidb-session --lib index_join
    cargo test --offline --locked -j12 -p tidb-session --lib index_probe
    cargo test --offline --locked -j12 -p tidb-session --lib tests_sysbench_access
    cargo test --offline --locked -j12 -p tidb-session --lib tests_in_list_full_evaluation
    cargo test --offline --locked -j12 -p tidb-session --lib tests_collation
    cargo test --offline --locked -j12 -p tidb-session --lib tests_datetime_year_compare
    cargo test --offline --locked -j12 -p tidb-session --lib tests_json

The isolated checkout is /private/tmp/tidb-parity-publish-aba629bb over published
403c269c66, with only the intended patch copied. All listed gates passed there.
Expression: 1199 passed, 97 existing ignored; executor EXPLAIN: 21; session
EXPLAIN: 64; prepared cache: 45; comparison refinement: 8; index join: 14 plus
one existing ignored; index probe: 3; sysbench access: 15; IN evaluation: 6;
collation: 13; datetime/year: 3; JSON: 15. Logs are
/tmp/tidb-explain-isolated-<crate>-<filter>.log (expression filter is all).
The full expression suite initially encountered a sandbox denial binding its
existing JSON-schema HTTP fixture; it passed with localhost permission.

Additional red/green commands from rust/:

    cargo test --offline --locked -j12 -p tidb-session --lib prepared_explain_constants_follow_current_parameters
    cargo test --offline --locked -j12 -p tidb-executor --lib dynamic_constants_use_go
    cargo test --offline --locked -j12 -p tidb-executor --lib brief_binary_plan_does_not_render
    cargo test --offline --locked -j12 -p tidb-session --lib prepared_explain_index_probe_strings
    cargo test --offline --locked -j12 -p tidb-session --lib prepared_explain
    cargo test --offline --locked -j12 -p tidb-executor --lib plan_trace::

Required make lint and git diff --check pass. Lint logs:
/tmp/tidb-explain-lint.log and /tmp/tidb-explain-final-lint.log. Final files are
executor explain, plan_trace and stmt_context; expression builtin_compare,
context, expr_util/predicates and rewriter; session tests_explain and
tests_in_list_full_evaluation; this plan. The two unrelated untracked drafts
remain untouched and outside validation/publication.

This checkpoint changes real expression construction and cache admission, not
only text. The targeted suites cover those consumers, but source package-wide
original artifact/generator/platform gates, ignored native tests, remaining
collation/rewriter/unsigned refinement branches, and full sysbench/TPC-C/TPC-H/
YCSB performance measurements are still open. The brief pass does less redundant
expression work; no throughput claim is made without workload measurements.
No whole Go package is marked accepted by this checkpoint.


## Continuing comparison audit: unsigned argument refinement


Previous checkpoint 3561624397 is published and remote-verified; the next pull
was current. It made authoritative progress without accepting a Go package.
The next remaining comparison-construction branch is refineArgsByUnsignedFlag
in pkg/expression/builtin_compare.go. It preserves nullable SQL semantics,
interprets a Uint64 constant through Go's signed EvalInt carrier, handles
correlated columns, mirrors operators when the constant is on the left, and
replaces only source-proven comparisons with NewOne/NewZero arguments. This is
part of the existing whole pkg/expression and dependent physicalop audit.

First capture a Go matrix over integer widths, flags, signed/unsigned/NULL
constants, all seven comparisons, operand order and correlated columns. Add a
native regression against the measured rewrite decisions before implementing
the missing rule. Verify SQL rows/plans/cache behavior and broader comparison
consumers, then measure an appropriate execution control without claiming
whole-workload performance from a component benchmark. Required lint, isolated
validation, self-review and the requested commit/push remain mandatory. Whole
package inventories and all four workload acceptance gates stay open.

The source matrix now passes: five integer widths, four flag combinations,
eight signed/unsigned/NULL constants, ordinary/correlated columns, both operand
orders and seven operators (4,480 decisions). The new native regression failed
before the implementation at signed nullable TINYINT <=> UINT64(1<<63), then
passed with the source rewrite. Logs: /tmp/tidb-unsigned-red.log and
/tmp/tidb-unsigned-green.log. The rule uses the existing signed EvalInt carrier,
keeps Go's early positive/NULL/error exits, respects nullable columns and
mirrored zero-boundary operators, and uses the existing NewOne/NewZero metadata.
It executes after integer/YEAR refinement and inside the existing cache guard.

The Go overlay /tmp/tidb-unsigned-overlay.json injects only temporary oracle
tests into the existing windows test harness. It changes no tracked Go source,
imports, Bazel metadata or modules; bazel_prepare is therefore not required.
The harness has no failpoint dependency. The source matrix and five SQL cases
plus four prepared executions pass with the race detector:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-unsigned-overlay.json -run '^(TestUnsignedRefinementOracle|TestUnsignedRefinementSQLOracle)$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

Log: /tmp/tidb-unsigned-go-race.log. Native SQL assertions preserve nullable
results for ordinary equality, fold impossible null-safe equality, eliminate
unsigned predicates only at source-proven bounds, and retain prepared-cache
hits while parameters change -1, 0, 1, -1. EXPLAIN checks the empty TableDual
and absence of redundant Selection for the two constant predicates.

The pipeline benchmark adds two construction-and-folding projection cases:
unsigned NOT NULL > -1 and its nullable control. Both validate all output rows
before timing. Only execution is timed; construction is outside the loop.
Baseline is published 3561624397 with the identical new benchmark source.
Build commands from each checkout's rust directory:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo bench --offline --locked -j12 -p tidb-executor --bench pipeline --no-run

The baseline and patched executables are copied to
/tmp/tidb-unsigned-before-pipeline and /tmp/tidb-unsigned-after-pipeline, so the
shared Cargo target cannot replace a measured binary. Build logs are
/tmp/tidb-unsigned-{before,after}-build.log. Timing results and final validation
receipt follow after all background checks finish. This is component evidence;
full sysbench/TPC-C/TPC-H/YCSB acceptance remains open, as do the whole-package
original test/support/build/platform/generated artifact gates. No package
inventory row is accepted by this dependency-progress checkpoint.

Three alternating runs (before/after, after/before, before/after), with builds
and tests stopped during measurement, all passed output assertions. Commands:

    BENCH_ONLY=unsigned_comparison /tmp/tidb-unsigned-before-pipeline
    BENCH_ONLY=unsigned_comparison /tmp/tidb-unsigned-after-pipeline

Logs: /tmp/tidb-unsigned-bench-{1,2,3}-{before,after}.log. Median NOT NULL
projection cost was 73.1 -> 8.1 ns/row; calibrated cost 148.3886 -> 16.7686,
8.85x lower. Nullable control was 73.2 -> 73.1 ns/row and calibrated
148.8405 -> 148.0540 (0.5%, within observed spread). This measures source-proven
constant folding in a projection, not throughput of any complete workload.

Final isolated validation over published 3561624397 plus only the intended
four files passed. Commands from /private/tmp/tidb-parity-publish-aba629bb/rust,
with CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target:

    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-session --lib tests_compare_refinement
    cargo test --offline --locked -j12 -p tidb-session --lib tests_prepared_plan_cache
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain
    cargo test --offline --locked -j12 -p tidb-session --lib tests_sysbench_access
    cargo test --offline --locked -j12 -p tidb-session --lib tests_in_list_full_evaluation
    cargo test --offline --locked -j12 -p tidb-session --lib tests_datetime_year_compare

Expression: 1,200 passed, 97 existing ignored. Session: comparison 9, prepared
cache 45, EXPLAIN 64, sysbench access 15, IN evaluation 6, datetime/YEAR 3.
Logs: /tmp/tidb-unsigned-isolated-<crate>-<filter>.log (expression filter all).
The SQL comparison suite was repeated after improving fixture readability.

Required lint and whitespace checks:

    GOTOOLCHAIN=go1.26.0 make lint
    git diff --check

Lint passed (/tmp/tidb-unsigned-lint.log). Initial sandboxed/default and
offline attempts could not satisfy make's mandatory revive installation;
the authorized network/cache-enabled retry passed. No lint rule was skipped.
Changed files: expression builtin_compare.rs, session tests_compare_refinement.rs,
executor benches/pipeline.rs and this plan. The two unrelated untracked drafts
remain untouched and outside the isolated build and publication. Whole-package
acceptance, ignored native tests and the four complete workload runs are not
verified by these checks. This remains dependency progress with no acceptance
claim for pkg/expression or pkg/planner/core/operator/physicalop.

## Continuing expression construction audit


Checkpoint 19a0cfced7 is committed, pushed and remote-verified. The next pull
was current. Source review found that new_function_impl folds comparisons
without running compareFunctionClass refinement; RealFunctionBuilder then
refines after folding. Go instead refines in getFunction, before its callback
and fold mode. This can leave an otherwise constant predicate unfurled and
makes direct NewFunction/Base/TryFold callers bypass source semantics.

Move comparison refinement into shared construction before collation,
callback and folding; remove the duplicate builder pass. First record Go
construction results for folding modes, callback-visible arguments and warning
counts, and add a failing native regression. Validate all native expression
and relevant rewrite/planner/session consumers and lint; record any uncovered
differences instead of weakening assertions. This continues the existing
atomic pkg/expression audit, not acceptance of a partial package. Commit and
push the validated progress as requested. Complete four-workload performance
and source package artifact gates remain required for eventual acceptance.

The complete native planner suite exposed one failure already present on
published 19a0cfced7: union_unsigned_widening_uses_the_in_union_cast_signature.
The disposable checkout reproduced it with unchanged published expression
construction, then restored its patched files. Log:
/tmp/tidb-construction-planner-baseline.log. The earlier Rust shortcut treated
equal EvalType as sufficient to skip UNION casts, although Go
buildProjection4Union compares full FieldType equality and BuildCastFunction4Union
retains a nonconstant integer cast for differing display widths. Remove that
shortcut and extend the existing failing test to pin source width 5 and cast
width 20. This is a discovered dependency correction within the existing
expression/core package audit, not a package acceptance claim.

The corrected UNION constructor exposed the missing follow-on source phase:
Go deriveStats4DataSource calls EliminateNoPrecisionLossCast on PushedDownConds
before range/selectivity derivation. Rust's existing helper was unwired and
recognized only a literal `cast` name, whereas the native builder emits
cast_signed/cast_unsigned/cast_unsigned_in_union/cast_char/cast_binary.
Keep the full UNION cast, admit those signatures to the existing guarded
helper, and pass the live RuleContext FunctionBuilder into the statistics fold.
Do not remove casts from arbitrary projections or incompatible/narrowing
conversions. New native helper coverage first fails for Long -> LongLong GT;
its controls retain signedness changes, narrowing casts and NE predicates.

Go's SQL oracle confirms that the eliminated constant UNION term disappears,
the survivor's projection retains cast(b AS bigint(11)), the scan filter is
GT(b,0), and pseudo estimated rows are 3333.33. Extend the session regression
to check the retained projection, unwrapped filter, estimate and returned row
[3,1]. This avoids restoring the old shortcut merely to make EXPLAIN pass.

Final construction/cast validation receipt

Go source remains unchanged from aba629bb455dc09d6a5d98b3c39a542bb1189b9d in
pkg/expression and the owning core stats/UNION builder files. The oracle tests
run through a temporary windows-package overlay with SELECT-style truncation
warnings; the strict mock default is separately observed and is not confused
with that session policy. All four Go oracles pass under the race detector:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-construction-overlay.json -run '^Test(ComparisonConstructionOracle|ConstructionCallbackReplacementOracle|UnionCastWidthOracle|UnionPredicateCastOracle)$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

Log: /tmp/tidb-construction-go.log. Comparison construction covers normal,
base, try-fold and callback entry points with unsigned, truncated-string and
decimal-string operands. Both callback observations and final warning counts
match. Callback replacement retains binary metadata after replacing CONCAT
with integer addition. The UNION oracle retains a cast from unsigned BIGINT
width 5 to width 20, and the SQL oracle pins the surviving projection/filter,
pseudo estimate and result rows.

Native red evidence: /tmp/tidb-construction-red.log (callback saw -1 instead
of refined zero); /tmp/tidb-construction-callback-red.log (replacement acquired
the old CONCAT charset); /tmp/tidb-construction-cast-red.log (native signed
cast was not eliminated). The existing UNION widening regression also failed
on the published baseline, as recorded above. An attempted temporary baseline
replacement in the main checkout was rejected by automatic approval review;
the callback baseline test instead ran in the disposable checkout, preserving
all primary working changes. No blocked action remains.

Final native validation in /private/tmp/tidb-parity-publish-aba629bb/rust,
with CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target:

    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-planner --lib
    cargo test --offline --locked -j12 -p tidb-session --lib tests_compare_refinement
    cargo test --offline --locked -j12 -p tidb-session --lib tests_prepared_plan_cache
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain
    cargo test --offline --locked -j12 -p tidb-session --lib tests_sysbench_access
    cargo test --offline --locked -j12 -p tidb-session --lib tests_in_list_full_evaluation
    cargo test --offline --locked -j12 -p tidb-session --lib tests_datetime_year_compare
    cargo test --offline --locked -j12 -p tidb-session --lib tests_collation
    cargo test --offline --locked -j12 -p tidb-session --lib tests_json
    cargo test --offline --locked -j12 -p tidb-session --lib index_join
    cargo test --offline --locked -j12 -p tidb-session --lib index_probe
    cargo test --offline --locked -j12 -p tidb-session --lib union
    cargo test --offline --locked -j12 -p tidb-session --lib set_op
    cargo test --offline --locked -j12 -p tidb-session --lib tests_recursive_cte

All pass: expression 1,203 with 97 existing ignored; planner 959; session
comparison 9, prepared cache 45, EXPLAIN 64, sysbench access 15, IN 6,
datetime/YEAR 3, collation 13, JSON 15, index join 14 with one existing ignored,
index probe 3, UNION 27, set operations 8, recursive CTE 13. Filters overlap;
these counts are not a distinct-test total. Logs:
/tmp/tidb-construction-final-<crate>-<filter>.log. The native lossless-cast test
also adds VARCHAR width/collation and binary-string controls, rerun with:

    cargo test --offline --locked -j12 -p tidb-expr --lib lossless_cast_elimination_recognizes_native

Required repository checks:

    GOTOOLCHAIN=go1.26.0 make lint
    git diff --check

Lint passes (/tmp/tidb-construction-final-lint.log). No Go source/import,
Bazel or module changes require bazel_prepare. Changed files are expression
new_function and expr_util builder/push_not; planner logical/rewrite and
plan_builder set_opr/set_opr_tests; session tests_union_all_predicate_push_down;
and this plan. Isolated tracked content is checked against the primary tree;
the two unrelated untracked drafts are excluded and remain untouched.

This fixes construction semantics and permits source-defined predicate cast
removal before range estimation; it does not claim measured throughput gains.
Complete sysbench/TPC-C/TPC-H/YCSB performance runs, ignored native tests,
original whole-Go-package tests and build/generated/platform/support artifact
gates remain open. Whole pkg/expression and dependent core/physicalop package
inventories are still unaccepted. This checkpoint is progress, not completion
of the user's goal.

## Continuing native cast substitution audit


Checkpoint edad9e9b15 is committed, pushed and remote-verified; the next pull
was current. Continue the whole expression/core package audit at cast
substitution. Native cast_* nodes currently bypass ColumnSubstituteImpl's
cast-specific metadata preservation, while RealFunctionBuilder.build_cast
ignores explicit charset and returns an unfolded cast even for constants.
Compare ordinary/correlated substitution, flags/coercibility, charset metadata,
folding and JSON's deliberate no-fold behavior directly with Go before fixing
these construction boundaries. Add red regressions, validate consumers and
required lint, then commit/push the verified progress. This does not accept a
partial package or replace any of the four workload performance gates.

Cast substitution implementation and receipt

The shared native cast-name predicate now routes cast_* through the same
ColumnSubstituteImpl special branch as Go ast.Cast. Rebuilds preserve the
original flags and coercibility after construction, including NULL replacements.
RealFunctionBuilder.build_cast derives signature collation in the live context,
honors explicit charset/repertoire, and folds non-JSON casts at construction.
It keeps the requested target FieldType charset distinct from signature
collation, matching Go newBaseBuiltinFunc. JSON remains unfolded because its
parse flags may change later. SubstituteCorCol2Constant deliberately uses the
non-explicit cast builder, as the Go source does, rather than inheriting the
ordinary substitution branch's explicit-charset option.

The temporary Go overlay captures fifteen substitutions across signed,
unsigned, CHAR, explicit ASCII and JSON casts, including negative, positive
and NULL constants. Every original flag and coercibility is preserved. It also
checks nonconstant explicit ASCII reconstruction and the correlated-rewrite
entry point (explicit false, signature utf8mb4/utf8mb4_bin, numeric coercibility,
ASCII repertoire, target FieldType still ASCII). Command from repository root:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-cast-substitution-overlay.json -run '^TestCastSubstitution(SQL)?Oracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

All pass (/tmp/tidb-cast-substitution-go.log). The SQL oracle confirms unsigned
wrapping, explicit-charset derived-table predicates and result coercibility.
It disproves the tempting assumption that folding should always reduce the
warning count: this derived-table truncating cast warns per row, three warnings
for three rows and six for six. The native SQL regression preserves both
counts and all result rows. No warning deduplication or alternate behavior is
introduced to obtain a performance improvement.

Native regressions fail before the fix: signed cast substitution drops the
original NOT NULL flag, and explicit ASCII reconstruction drops the explicit
charset bit. Log /tmp/tidb-cast-substitution-red.log. They pass after the fix
(/tmp/tidb-cast-substitution-green.log), with further correlated-rewrite metadata
assertions passing in /tmp/tidb-cast-substitution-final-unit.log.

Commands from /private/tmp/tidb-parity-publish-aba629bb/rust, using
CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target:

    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-planner --lib
    cargo test --offline --locked -j12 -p tidb-session --lib tests_collation
    cargo test --offline --locked -j12 -p tidb-session --lib tests_compare_refinement
    cargo test --offline --locked -j12 -p tidb-session --lib tests_prepared_plan_cache
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain
    cargo test --offline --locked -j12 -p tidb-session --lib tests_subquery
    cargo test --offline --locked -j12 -p tidb-session --lib tests_json
    cargo test --offline --locked -j12 -p tidb-session --lib union
    cargo test --offline --locked -j12 -p tidb-session --lib tests_sysbench_access
    cargo test --offline --locked -j12 -p tidb-session --lib tests_datetime_year_compare
    cargo test --offline --locked -j12 -p tidb-expr --lib native_cast_substitution

Expression 1,205 pass (97 existing ignored), planner 959 pass; session
collation 13, comparison 10, prepared cache 45, EXPLAIN 64, subquery 23,
JSON 15, UNION 27, sysbench access 15, datetime/YEAR 3 all pass. The comparison
suite was repeated after adding the SQL oracle cases. Logs:
/tmp/tidb-cast-substitution-isolated-<crate>-<filter>.log, with all as the
unfiltered crate suffix. Final metadata assertions use the final-unit log above.

Required checks from repository root:

    GOTOOLCHAIN=go1.26.0 make lint
    git diff --check

Lint passes (/tmp/tidb-cast-substitution-lint.log). No tracked Go, imports,
Bazel or module files changed, so bazel_prepare is not required. The temporary
Go harness has no failpoint dependency. Changed files: expression collation_derive.rs, expr_util/builder.rs,
expr_util/substitute.rs, session tests_compare_refinement.rs and this plan.
All intended tracked files are compared against isolated validation content
before publication; unrelated untracked vs_helper.rs and fragment.rs remain
untouched and excluded.

This continues whole pkg/expression and dependent planner package work.
Whole-package original tests, platform/build/generated/support artifacts,
ignored native tests and complete sysbench/TPC-C/TPC-H/YCSB performance runs
remain open. No package inventory is accepted and no throughput gain is claimed.
One still-observed utility audit item is RemoveMutableConst's deferred-error
state: Rust currently takes DeferredExpr before evaluation whereas Go clears
it only after successful evaluation. That contract needs its own source-backed
error-state regression; it is not claimed fixed by this cast checkpoint.

The final coercibility assertion exposed another source mismatch before
publication: native deriveCollation("cast") used generic string aggregation,
changing NUMERIC (5) to COERCIBLE (4), and used connection charset even for
non-string targets. Go's cast arm directly preserves the argument's
coercibility/repertoire and uses connection charset only for string targets.
Correct that shared arm; include collation_derive.rs in this checkpoint and
repeat affected validation. The initial final-unit failure is retained as
/tmp/tidb-cast-substitution-collation-red.log before replacing the pass log.

After the shared cast-collation correction, every command in the receipt above
was repeated and passed, including the final metadata assertions, both full
native crate suites, all listed session filters and make lint. The pass counts
are unchanged. The source oracle and changed-hunk review confirm no Go source
or package inventory acceptance state changed. The checkpoint is ready for the
user-requested commit/push; all remaining acceptance gates above stay open.


## Continuing mutable-constant error-state audit


Published d825ef058b is pulled and current. Audit RemoveMutableConst against
Go util.go and Constant.Eval before altering error handling. Go clears the
parameter marker immediately, stores the deferred evaluation's returned Datum
even on error, retains DeferredExpr on error, and stops before later siblings.
ScalarFunction.Eval returns NULL on error; Constant.Eval returns its saved value
for lazy evaluation errors and the unconverted input for conversion errors.
Preserve that pair internally in Rust without changing public Result APIs or
reevaluating a deferred function. Add source-oracle and red regressions for
nested constants, traversal, successful removal, and retry, then validate
expression and its comparison/cache consumers, lint, commit and push. This
remains work toward the whole expression package, not package acceptance.

Mutable-constant implementation and validation receipt

Constant's private evaluator now retains Go's error-associated Datum: its
saved Value for lazy evaluation failures and the original input for conversion
failures. Expression exposes this pair only to internal callers needing it;
ordinary scalar evaluation keeps its existing Result path. Both paths share
the existing ENUM/SET integer-flag conversion. RemoveMutableConst clears the
marker first, evaluates once, stores the returned value even on failure, and
clears DeferredExpr only after success. Errors stop traversal before later
siblings. No public API, SQL feature or warning policy is added.

The temporary source oracle verifies scalar overflow (NULL), nested constant
overflow (saved 99), and deferred conversion of '12ab' (unconverted '12ab'),
plus prior/later siblings, retry, NULL success and marker-only saved values.
From repository root:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-mutable-constant-overlay.json -run '^TestRemoveMutableConstantOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

Pass: /tmp/tidb-mutable-constant-go.log. The temporary window harness needs no
failpoint enabling. Go expression production sources remain identical to the
pinned aba629bb455dc09d6a5d98b3c39a542bb1189b9d tree.

The native regression fails before the fix, returning stored Int(99) instead
of NULL; /tmp/tidb-mutable-constant-red.log retains the failure. Red command
from repository root (after correcting test-construction spelling):

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --offline --locked -j12 --manifest-path /private/tmp/tidb-parity-publish-aba629bb/rust/Cargo.toml -p tidb-expr --lib remove_mutable_const

After the fix both new tests pass. The final tests also assert the error
classes, use a valid two-argument scalar parent, and preserve later top-level
siblings. Full final validation from the isolated checkout's rust directory,
with CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target:

    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-planner --lib
    cargo test --offline --locked -j12 -p tidb-session --lib tests_compare_refinement
    cargo test --offline --locked -j12 -p tidb-session --lib tests_prepared_plan_cache
    cargo test --offline --locked -j12 -p tidb-session --lib tests_in_list_full_evaluation
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain
    cargo test --offline --locked -j12 -p tidb-session --lib tests_sysbench_access
    cargo test --offline --locked -j12 -p tidb-session --lib tests_datetime_year_compare

All pass: expression 1,207 (97 existing ignored), planner 959; session
comparison 10, prepared cache 45, IN 6, EXPLAIN 64, sysbench access 15 and
YEAR 3. Logs /tmp/tidb-mutable-final-<crate>-<filter or all>.log. The first
unprivileged expression run failed only because the JSON schema fixture could
not bind localhost; the complete suite passed with localhost access and was
repeated after the final evaluation-path adjustment.

Required checks from repository root:

    GOTOOLCHAIN=go1.26.0 make lint
    git diff --check
    git diff --numstat aba629bb455dc09d6a5d98b3c39a542bb1189b9d -- pkg/expression

Lint passes (/tmp/tidb-mutable-constant-lint.log), diff whitespace is clean,
and the source comparison is empty. No Go/import/Bazel/module changes require
bazel_prepare. Changed files are expression constant.rs, expression.rs,
expr_util/predicates.rs and this plan. All intended tracked content is compared
with the isolated validation checkout before publication; the unrelated
untracked vs_helper.rs and fragment.rs drafts remain untouched and excluded.

This corrects failure-state compatibility within the ongoing whole-expression
package audit. No throughput improvement is claimed. Full sysbench/TPC-C/TPC-H/
YCSB performance runs, original whole-Go-package tests, ignored native tests,
and platform/build/generated/support artifact gates remain unverified; package
inventories remain unaccepted. The overall goal is still active.


## Continuing typed integer-comparison refinement audit


Checkpoint 60bdd00f3e is pushed and the next pull is current. The current
RefineComparedConstant shortcut rounds every input through f64, losing DECIMAL
precision above 2^53 and ignoring Go's typed CEIL/FLOOR result and subsequent
ETInt short circuit. Audit the complete helper's evaluation, conversion,
overflow, metadata and warning paths against Go, including narrow integer
targets and large signed/unsigned values. Add red unit and SQL regressions,
reuse native typed construction, validate affected consumers and lint, then
commit/push. This is work inside the whole expression package audit; all
package and full workload acceptance gates remain open.

Typed refinement implementation findings

RefineComparedConstant now evaluates the current constant and uses the live
conversion flags/location/warning sink, with negative-to-unsigned wrapping
disabled only for the initial conversion as Go does. CEIL/FLOOR construction
uses new_function with its normal typed result and fold, then the same ETInt
short circuit or conversion as tryToConvertConstantInt. NewFunction's shared
entry points accept a context trait object so planner rewrites can use this
construction path. Converted constants preserve deferred/parameter fields;
overflow drops SubqueryRefID, successful conversion retains it. Constant
folding now preserves the first positive input subquery reference for immutable
results, matching the ordinary Go foldConstant arm.

The exact boundary work exposes two datatype dependencies. Decimal.round_to_i64
must parse an unsigned magnitude to admit i64::MIN, including half-up rounding
onto it. Contextual integer conversions must report numeric overflow instead
of treating its diagnostic as unported. The unsigned scanner now retains
prefix, parser and width error precedence and warnings; an out-of-range negative
exponent string continues through unsigned validation to produce zero, matching
Go. Signed decimal overflow retains the source's unformatted ErrOverflow when
MyDecimal.ToInt itself overflows. Temporal/JSON/binary-literal diagnostic arms
not covered by this change remain explicitly unported; no whole types package
acceptance is claimed.

Source receipts: /tmp/tidb-refine-precision-go-final.log contains 77 refinement
cases, five SQL queries, three subquery-reference folds and eleven signed-limit
conversions; /tmp/tidb-refine-unsigned-go.log contains 32 unsigned conversions
across TINYINT/BIGINT and strict/warning modes. Commands from repository root:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-refine-precision-overlay.json -run '^TestRefine(Precision(SQL)?|FoldReference|DecimalLimit)Oracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-refine-precision-overlay.json -run '^TestRefineUnsignedDiagnosticsOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

Both pass, with no failpoint dependency in the temporary harness. No original
Go expression or types source changed relative to the pinned aba629bb tree.
Native red evidence is in /tmp/tidb-refine-precision-red.log (LE boundary rounded
one integer too high), /tmp/tidb-refine-precision-sql-red.log (missing the row
9007199254740994), /tmp/tidb-refine-fold-reference-red.log (lost reference 77),
/tmp/tidb-refine-decimal-limit-red.log (valid i64::MIN rejected), and
/tmp/tidb-refine-context-red.log (unsigned overflow returned Unsupported).
Each corresponding regression passes after the implementation. The SQL test
checks five predicates with automatic, forced-index and table-scan access.

Typed refinement final validation and performance receipt

Run native commands from /private/tmp/tidb-parity-publish-aba629bb/rust with
CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target. Red commands use the
same directory/target and the relevant test filter:

    cargo test --offline --locked -j12 -p tidb-expr --lib decimal_refinement_preserves_exact
    cargo test --offline --locked -j12 -p tidb-session --lib decimal_comparison_refinement_keeps_large
    cargo test --offline --locked -j12 -p tidb-expr --lib folding_preserves_first_positive
    cargo test --offline --locked -j12 -p tidb-datatype --lib decimal_round_to_i64_accepts_negative_limit
    cargo test --offline --locked -j12 -p tidb-datatype --lib contextual_integer_conversion_reports_numeric_overflow

Final complete and targeted commands:

    cargo test --offline --locked -j12 -p tidb-datatype --lib
    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-planner --lib
    cargo test --offline --locked -j12 -p tidb-session --lib tests_compare_refinement
    cargo test --offline --locked -j12 -p tidb-session --lib tests_prepared_plan_cache
    cargo test --offline --locked -j12 -p tidb-session --lib tests_in_list_full_evaluation
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain
    cargo test --offline --locked -j12 -p tidb-session --lib tests_sysbench_access
    cargo test --offline --locked -j12 -p tidb-session --lib tests_datetime_year_compare
    cargo test --offline --locked -j12 -p tidb-session --lib tests_collation
    cargo test --offline --locked -j12 -p tidb-session --lib numeric_domain
    cargo test --offline --locked -j12 -p tidb-session --lib unsigned
    cargo test --offline --locked -j12 -p tidb-session --lib decimal
    cargo test --offline --locked -j12 -p tidb-session --lib tests_subquery

All pass: datatype 426, expression 1,209 (97 existing ignored), planner 959;
session comparison 11, prepared cache 45, IN 6, EXPLAIN 64, sysbench access 15,
YEAR 3, collation 13, numeric domain 16, unsigned 23, decimal 7 (2 existing
ignored), subquery 23. These overlapping filters are not a distinct-test total.
Initial session filters tests_arithmetic and tests_unsigned each selected zero
tests; they are not validation evidence and were replaced by the actual numeric
module/name filters above. Logs: /tmp/tidb-refine-precision-datatype.log and
/tmp/tidb-refine-final-<crate>-<filter or all>.log.

The new pipeline benchmark checks TINYINT-column < 127.1 through
EvaluatorSuite, with every expected result and NULL checked before timing.
Go's CEIL result 128 is already ETInt, so preserving it avoids the prior
per-row decimal comparison. TINYINT-column < 126.1 is the in-range control.
Both baseline and changed binaries use the identical new benchmark code and
bench profile. The before production sources are exactly 60bdd00f3e; they were
installed only in the isolated checkout during its build and restored from the
primary tree in a finally block before the changed build. Primary source files
were never replaced. Build command for each snapshot, in the directory above:

    cargo bench --offline --locked -j12 -p tidb-executor --bench pipeline --no-run --message-format=json

Three alternating pairs then run:

    BENCH_ONLY=integer_refinement /tmp/tidb-refine-before-pipeline
    BENCH_ONLY=integer_refinement /tmp/tidb-refine-after-pipeline

All result assertions pass. Median outside-range timing changes from 211.9 to
74.1 ns/row, and calibrated units from 426.8157 to 148.9068: 2.866x faster.
The control changes from 74.3 to 74.1 ns/row and 150.8311 to 149.1130 calibrated
units (about 1.2%, near noise). Logs /tmp/tidb-refine-bench-{before,after}-[0-2].log;
build logs use the same prefix without the run number. This is a component
improvement, not a claim about complete sysbench/TPC-C/TPC-H/YCSB throughput.

Required root checks:

    GOTOOLCHAIN=go1.26.0 make lint
    git diff --check
    git diff --numstat aba629bb455dc09d6a5d98b3c39a542bb1189b9d -- pkg/expression pkg/types

Lint passes (/tmp/tidb-refine-precision-lint.log), whitespace is clean, and the
Go source comparison is empty. No Go/import/Bazel/module edits require
bazel_prepare. Changed files: datatype convert.rs, datum_convert.rs,
datum_convert/diagnostics.rs, decimal/mod.rs, decimal_tests.rs; expression
builtin_compare.rs, constant_fold.rs, new_function.rs; session
 tests_compare_refinement.rs; executor benches/pipeline.rs; and this plan.
The tests' stale description of the '10ab' warning path is corrected to match
Go's early exact-value return. Isolated tracked validation content is compared
byte-for-byte with primary content before publication. The two unrelated
untracked drafts remain excluded and untouched.

Whole expression, types and dependent planner package inventories remain
unaccepted. Original whole-Go-package tests, ignored native tests, remaining
platform/build/generated/support artifacts, and complete four-workload
performance gates are still open. Another visible audit candidate remains
numeric-constant-to-datetime refinement: its native reads_column check and
context-free conversion are narrower than the source's non-Constant and live
context rules. It needs source evidence and regressions before any claim that
those paths match. This checkpoint does not mark the user's goal complete.

Publication integration note

The first push was rejected because origin advanced to 3cc8829218, adding only
one planner joinorder test. Fetch and rebase integrated it without conflicts.
No production code changed in that upstream commit, so the component benchmark
and expression/datatype evidence remain applicable. The full planner suite was
rerun against the combined isolated tree and passes 960 tests:

    cargo test --offline --locked -j12 -p tidb-planner --lib

Log /tmp/tidb-refine-rebase-planner.log. GOTOOLCHAIN=go1.26.0 make lint also
passes after rebase (/tmp/tidb-refine-rebase-lint.log). All 23 tracked validation
paths match the combined primary tree byte-for-byte. Publication uses an
ordinary (non-force) push.


## Continuing numeric-to-datetime comparison refinement


Published 3241a43f86 is current after pulling. Audit the source rule that
refines a numeric constant against any non-Constant DATETIME/TIMESTAMP
expression. Native reads_column is narrower and the conversion uses fixed
flags rather than the live statement context. Verify scalar/column/constant
shapes, both operand orders, numeric domains and date modes against Go before
replacing these shortcuts; add red regressions, validate consumers and lint,
and commit/push. Full package and workload acceptance remain open.


### Surprises, decisions and current evidence


Go rule 3 matches every non-Constant DATETIME/TIMESTAMP expression, including
column-free scalar functions. The old reads_column predicate predated native
constant folding and is no longer the right discriminator. The numeric
Constant must be evaluated with the live context; failed evaluation or
conversion claims the rule but leaves its argument unchanged. Successful
conversion replaces the constant and drops its mutable/reference metadata as
Go does. DATE and Constant temporal operands stay outside this rule.

The dependency audit found that Datum.ConvertTo for DECIMAL uses
ParseTimeFromFloatString, whereas the native conversion used the distinct
ParseTimeFromDecimal helper. Parse the decimal text directly at target FSP
with the session zone. This preserves Go's prefix-0.0 special case, validation
before applying the fraction, rounding using all fractional digits, and
fractional carries across DST. A private parser entry accepts complete date
flags so IgnoreZeroDateErr stays independent of IgnoreZeroInDate. Existing
public parser callers retain their prior default zero-date policy; the
separate ParseTimeFromDecimal helper is unchanged.

The initial expression and SQL regressions failed before the refinement
change (/tmp/tidb-datetime-refine-red.log and
/tmp/tidb-datetime-refine-sql-red.log). The datatype regression failed before
the decimal/parser change: strict DATE conversion incorrectly accepted 0.1
(/tmp/tidb-datetime-decimal-red.log). All corresponding regressions pass after
the fixes. Native tests assert 960 shape/date-mode combinations, eight zone
comparisons, 96 decimal temporal conversions, and 24 SQL mode/access-path
cases including an unsupported REAL control. Source overlay oracles are
read-only Go evidence and are not added to upstream Go packages.

Go commands, from repository root, using the temporary overlay that replaces
only the existing executor/windows test harness:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-datetime-refine-overlay.json -run '^TestDatetimeRefinement(SQL)?Oracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-datetime-refine-overlay.json -run '^TestDecimalTemporalConversionOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows
    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-datetime-refine-overlay.json -run '^TestDatetimeRefinementZoneOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

All three pass. Logs: /tmp/tidb-datetime-refine-go-final.log,
/tmp/tidb-datetime-decimal-go.log and /tmp/tidb-datetime-zone-go.log. A local
checker verifies all 960 Go shape rows against native expected outcomes,
subquery markers and zero warning counts. SQL and zone source results match
the checked-in regression expectations. No Go source/imports/module/Bazel
inputs change, so bazel_prepare is not required. The overlay harness has no
failpoint dependency. This evidence is not a full original expression/types
package test run or a whole-package acceptance receipt.


### Consumer validation and remaining risks


In the isolated validation checkout /private/tmp/tidb-parity-publish-aba629bb,
run Rust commands from rust/ with
CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target. This excludes the two
unrelated untracked drafts from validation and publication.

    cargo test --offline --locked -j12 -p tidb-datatype --lib
    cargo test --offline --locked -j12 -p tidb-expr --lib
    cargo test --offline --locked -j12 -p tidb-planner --lib

These pass: datatype 427; expression 1210 with 97 pre-existing ignored;
planner 960. Logs are /tmp/tidb-datetime-{datatype,expr,planner}-final.log.
The expression run permits its existing localhost JSON-schema HTTP fixture.
The required root command GOTOOLCHAIN=go1.26.0 make lint passes
(/tmp/tidb-datetime-lint.log). git diff --check also passes.

Session commands use the same Rust directory/target setup:

    cargo test --offline --locked -j12 -p tidb-session --lib tests_compare_refinement
    cargo test --offline --locked -j12 -p tidb-session --lib tests_datetime_year_compare
    cargo test --offline --locked -j12 -p tidb-session --lib tests_timestamp_range
    cargo test --offline --locked -j12 -p tidb-session --lib tests_timezone_storage
    cargo test --offline --locked -j12 -p tidb-session --lib tests_zero_date
    cargo test --offline --locked -j12 -p tidb-session --lib tests_read_cast
    cargo test --offline --locked -j12 -p tidb-session --lib tests_prepared_plan_cache
    cargo test --offline --locked -j12 -p tidb-session --lib tests_non_prepared_plan_cache
    cargo test --offline --locked -j12 -p tidb-session --lib tests_explain
    cargo test --offline --locked -j12 -p tidb-session --lib tests_sysbench_access
    cargo test --offline --locked -j12 -p tidb-session --lib tests_in

The first seven filters pass 12, 3, 5, 15, 11, 4 and 45 tests respectively.
The non-prepared cache filter exposes two existing failures:
`go_admits_custom_restore_func_call_shapes` at line 398 and
`literals_of_different_kinds_do_not_share_an_entry` at line 159. Both expect
cache hit 1 but observe 0. Its other 24 tests pass and one is ignored.
Replacing only the three production files changed in this checkpoint with
published 3241a43f86 versions in the isolated checkout reproduces exactly the
same failures and counts (/tmp/tidb-datetime-nonprepared-baseline.log).
A finally block restores the current isolated files. Main sources are never
replaced. This confirms the failures predate the temporal changes; it does
not classify their behavior as source-correct. Compare Go cache eligibility,
constant metadata and runtime values before fixing code or expectations in
the continuing whole-package audit. Current session logs follow
/tmp/tidb-datetime-session-<filter>.log.

This checkpoint changes four Rust source/test files and this plan. It corrects
source behavior and makes no performance improvement claim. Complete original
Go-package validation, ignored native cases, all platform/generated/support
artifacts, the two cache failures, and full sysbench/TPC-C/TPC-H/YCSB runs remain
open. There is no live cluster or complete workload result in this checkpoint.
Whole expression/types/planner package inventories remain unaccepted and the
user's goal remains active.


The remaining session filters pass: EXPLAIN 64, sysbench access 15, and the
`tests_in` prefix 33 (including other module names with that prefix). All
selected filters execute tests; no zero-test filter is counted. The measured
SQL correctness set covers relevant conversion, date-mode, timezone,
prepared-cache and access-path consumers without claiming the full session
package. Upstream fetch confirms the branch is current at 3241a43f86 before
commit. Publication is an ordinary commit and push after reviewing the diff
and comparing all tracked isolated validation paths byte-for-byte with main.


## Continuing non-prepared comparison-cache evidence


The previous goal turn was progress: commit 91312d129e was pushed and verified.
A fresh pull is already current. The two non-prepared-cache failures recorded
above reproduce on the published baseline; investigate their source contract
before changing runtime behavior. Go allowCmpArgsRefining4PlanCache explicitly
sets SkipPlanCache for an integer expression versus a STRING/REAL/DECIMAL
constant. This includes a deferred folded TRIM result. The temporary Go
TestNonPreparedComparisonCacheOracle verifies rows, hit status and plan-cache
explanation warnings: the decimal/string/trim cases miss on repeated values,
whereas ordinary integers and POSITION hit. A VARCHAR-column control verifies
that INT/DECIMAL/STRING parameters still own distinct entries and then hit
only their own types. Correct the stale tests using those source outcomes,
retain typed-entry coverage on the control, run the cache suites and required
lint, then commit/push. These are tests inside the continuing whole-package
audit, not a new partial-package acceptance unit.


### Cache validation outcome and original-package gate


Source outcomes are recorded in /tmp/tidb-cache-parity-go-final.log. The exact
Go command (repository root) passes:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off go test -race -overlay=/tmp/tidb-cache-parity-overlay.json -run '^TestNonPreparedComparisonCacheOracle$' -tags=intest,deadlock -count=1 -v ./pkg/executor/windows

The overlay preserves the existing windows harness and adds only a temporary
oracle; it is not a tracked Go change. Its sixteen queries include integer,
decimal, string, POSITION and TRIM cases and six VARCHAR controls. Source
warnings explicitly say the decimal/string/TRIM constants may be converted
to INT. The original native tests failed as recorded in the previous section;
the correction changes expectations, not cache production logic. The
VARCHAR control retains the earlier test's separate-typed-entry intent.

From isolated rust/, with the shared CARGO_TARGET_DIR used above:

    cargo test --offline --locked -j12 -p tidb-session --lib tests_non_prepared_plan_cache -- --include-ignored --test-threads=1
    cargo test --offline --locked -j12 -p tidb-session --lib tests_prepared_plan_cache

Both pass: 28 and 45 tests, no failures or ignored cases in these selections.
Logs /tmp/tidb-cache-parity-{nonprepared,prepared}-final.log. An earlier run
from repository root using --manifest-path to the isolated rust/Cargo.toml
also passed 27 cases with the serial case ignored
(/tmp/tidb-cache-parity-native.log); it rebuilt without rust/.cargo config,
so the final runs above use the normal Rust working directory. Required root
GOTOOLCHAIN=go1.26.0 make lint passes (/tmp/tidb-cache-parity-lint.log).
No Go/import/module/Bazel inputs changed, so bazel_prepare is unnecessary.

The cache failures are now classified and resolved. The next gate runs all
original Go expression tests, using the repository failpoint runner because
source expression tests call failpoints (JSON schema, safe timestamps and PB
pushdown). The runner serializes activation and disables failpoints on exit.
It runs in the isolated checkout, without modifying main Go sources:

    GOTOOLCHAIN=go1.26.0 GOPROXY=off ./tools/check/failpoint-go-test.sh pkg/expression -race -count=1

Log /tmp/tidb-expression-original-go-package.log. This reference baseline alone
cannot accept the native package: the native ignored cases and production,
build/generated/platform/support mapping plus four workload gates remain.


Reference-gate toolchain discovery: the initial GOPROXY=off invocation could
not bootstrap failpoint-ctl. Retrying with network enabled installed the
pinned tool and ran the full build, but Go 1.26.0's external ARM64 linker
panicked in gensymlate/SetSymSect before any tests executed. Its internal
linker failed on macOS C-framework symbols. Both failpoint-enabled attempts
cleaned up to refcount zero. These are build failures, not passing tests.
Logs /tmp/tidb-expression-original-go-package.log and
/tmp/tidb-expression-original-go-internal.log. The exact retries were:

    GOTOOLCHAIN=go1.26.0 ./tools/check/failpoint-go-test.sh pkg/expression -race -count=1
    GOTOOLCHAIN=go1.26.0 GOPROXY=off ./tools/check/failpoint-go-test.sh pkg/expression -race -ldflags=-linkmode=internal -count=1

Use the go.mod minimum version instead of altering code: GOTOOLCHAIN=go1.25.12
go version successfully installs and reports go1.25.12 darwin/arm64. The
source has explicit Go 1.25 ABI support. The ongoing reference gate is now:

    GOTOOLCHAIN=go1.25.12 GOPROXY=off ./tools/check/failpoint-go-test.sh pkg/expression -race -count=1

Log /tmp/tidb-expression-original-go12512.log. A fresh independent check
verifies that all 133 direct expression-package artifacts are enumerated and
that their SHA-256 hashes match both main's files and pinned aba629bb Git
objects. Failpoint instrumentation remains confined to the isolated checkout.
The native ignored FIND_IN_SET cache-lifecycle test is still a real gap:
Go builtin_string.go uses builtinFuncCache keyed by EvalContext.CtxID, memoizes
NULL and first-match collation keys, does not cache constructor errors, and
resets on context change and signature clone. Native repeated value tests
alone do not establish those lifecycle/performance properties. This remains
part of the continuing whole-expression audit.


The original Go expression package gate PASSES with Go 1.25.12 on darwin/arm64:
PASS; ok github.com/pingcap/tidb/pkg/expression 109.022s. Failpoint cleanup
reports new_refcount=0, and git diff --stat -- pkg in the isolated checkout
is empty afterward. This is the complete default unit-test selection with
-race, -tags=intest,deadlock and -count=1, not a -run-filtered oracle. It does
not execute benchmarks or establish Linux/Bazel/native Rust equivalence.

The dependent Go types package has no failpoint., testfailpoint. or failpoint
BUILD dependency (checked with rg), and no doc.go. Its full reference gate is
now running from the isolated repository root after expression cleanup:

    GOTOOLCHAIN=go1.25.12 GOPROXY=off go test -race -tags=intest,deadlock -count=1 ./pkg/types

Log /tmp/tidb-types-original-go12512.log.


The original types gate PASSES: ok github.com/pingcap/tidb/pkg/types 1.106s.
A new rust/docs/types-package-source-inventory.md records every direct source,
original test/benchmark/support and build artifact with pinned/current SHA-256
checks. It keeps the types package explicitly unaccepted and separates the
parser_driver child package. The expression inventory now includes its full
original-Go reference receipt, without promoting any native mapping row.
Both original reference gates are now evidenced for this platform; original
benchmarks, other platforms/Bazel, complete native mappings and ignored cases,
and all four full workload acceptance gates remain open.


Publication scope: tests_non_prepared_plan_cache.rs, expression and types
package source inventories, and this ExecPlan. No runtime implementation or
Go source changes. Targeted native cache tests and original Go reference suites
were selected to validate the changed expectations and close the observed
reference-test gaps. make lint and git diff --check pass. Self-review confirms
each changed hit expectation follows the Go oracle and each typed-entry check
also asserts its own rows. The unrelated vs_helper.rs and fragment.rs drafts
remain untouched, uncompiled and unstaged. All isolated tracked validation
paths and the new inventory are compared with main before publication.


Before publication, origin advanced to e1119df5f7, adding only the TPC-H Q15
revenue0 SQL fixture. Fast-forward integrated it without conflicts. No Go or
Rust production/test source changed upstream, so the recorded cache and
reference-suite evidence still applies. The fixture is also copied into the
isolated tree for content consistency. Publication uses an ordinary commit
and non-force push.


## Continuing FIND_IN_SET cache parity

The next source gap is Go `pkg/expression/builtinFuncCache[T]` and its
`builtinFindInSetSig.constStrlistLookupCache` consumer. The Rust expression
layer now has `builtin_ext/cache.rs`: a context-keyed lazy value with a
read-lock hit path, serialized construction, context replacement, empty
clone state, and no caching of constructor errors. `FIND_IN_SET` builds the
same first-position `KeyWithoutTrimRightSpace` map as Go, memoizes a NULL list,
returns zero for an empty list, and uses the cache only for
`ConstLevel::ONLY_IN_CONTEXT`; ordinary row-dependent lists still evaluate
per row. Statement contexts expose a monotonically allocated `CtxID` through
`Columns`, while expression-only contexts retain their zero/default seam.

Native source tests now cover the eight-way concurrent constructor race,
miss/get/context-change/error-not-cached/clone lifecycle, PAD SPACE first
match, duplicate entries, empty and NULL lists, and the concrete lookup-cache
identity. The focused commands from `rust/` pass:

    cargo test --offline --locked -j12 -p tidb-expr --lib find_in_set_lookup_source -- --test-threads=1
    cargo test --offline --locked -j12 -p tidb-expr --lib builtin_func_cache -- --include-ignored --test-threads=1
    cargo test --offline --locked -j12 -p tidb-session --lib tests_non_prepared_plan_cache -- --include-ignored --test-threads=1
    cargo test --offline --locked -j12 -p tidb-session --lib tests_prepared_plan_cache -- --include-ignored --test-threads=1

The selections pass 4, 2, 28 and 45 tests respectively. A full native
expression run after the final scope correction reaches 1213 passing tests
with one sandbox-blocked localhost JSON-schema fixture; the existing AST
`COLLATE` passthrough and weight-string boundary remain unchanged. The focused
reruns above pass after that scope correction. Formatting checks for all
changed Rust files and `git diff --check` pass. The lint body also passes; the
normal `make lint` bootstrap is blocked locally because the offline module
cache cannot resolve the pinned `github.com/mgechev/revive` module package, so
the exact target was run with a wrapper that skips only that already-installed
tool bootstrap.

This closes the concrete FIND_IN_SET cache gap but does not accept the whole
expression, types, planner, executor, or workload packages. Native ignored
cases, complete production/build/generated/support inventories, and full
sysbench/TPC-C/TPC-H/YCSB measurements remain open under the original goal.


## Continuing regexp cache parity

Go's `regexpBaseFuncSig.memorizedRegexp` and
`builtinRegexpReplaceFuncSig.instCache` now use the Rust context-keyed cache.
`REGEXP`, `REGEXP_LIKE`, `REGEXP_SUBSTR`, `REGEXP_INSTR`, and
`REGEXP_REPLACE` admit a pattern cache only when Go's corresponding pattern
and match-type arguments are `ConstOnlyInContext`; replacement instructions
follow the independent replacement-constness rule. Cached compile results keep
errors as well as successful `Regex` values, so an invalid constant pattern is
reported repeatedly without recompilation. The cache resets on expression
argument invalidation and clone, and a new statement context replaces the old
entry. Lazy argument and NULL checks remain before compilation, matching the
Go evaluation order.

The former ignored `regexp_cache_identity_by_statement_context` source test is
now active. It covers non-constant bypass, same-context reuse, context
replacement, cached compile errors, clone reset, and the concrete
`REGEXP_REPLACE` pattern/instruction dispatch. The focused command passes:

    cargo test --offline --locked -j12 -p tidb-expr --lib regexp_vec_cache_source -- --test-threads=1

All six enabled regexp source-cache tests pass. The full native expression
run reaches 1214 passing tests with one sandbox-blocked localhost JSON-schema
fixture; no regexp failure remains. The benchmark-only regexp test remains
ignored because the Go `testing.B` harness has no equivalent native gate.
The changed Rust files pass rustfmt and `git diff --check`; the exact lint
target also passes with the repository's already-installed revive binary while
its offline bootstrap step is bypassed.


## Continuing LIKE/ILIKE pattern cache parity

Go's `patternCache` compiles wildcard tokens once when both the pattern and
escape arguments are constant in the statement context. Rust now keeps the
same context-keyed lifetime for `LIKE` and `ILIKE`: the existing binary fast
path, collation-aware matcher, escape handling, and ILIKE lower-casing remain
the matching implementation, while only the compiled pattern is reused. Row-
dependent patterns or escapes bypass the cache, and argument invalidation
resets it through the same scalar-function cache lifecycle used by the other
expression caches.

The active native source test covers same-context reuse, changed-pattern
isolation, context replacement, and ILIKE matching. The focused commands from
`rust/` pass:

    cargo test --offline --locked -j12 -p tidb-expr --lib like -- --test-threads=1
    cargo test --offline --locked -j12 -p tidb-expr --lib like_pattern_cache_reuses_only_within_context -- --test-threads=1

The selections pass 52 and 1 tests respectively. A full native expression run
after the cache change reaches 1215 passing tests with the same one
sandbox-blocked localhost JSON-schema fixture; no LIKE/ILIKE failure remains.
Both prepared and non-prepared session-plan-cache selections continue to pass
(45 and 28 tests). Changed Rust files pass rustfmt and `git diff --check`.
`make lint` was attempted with the repository's installed revive binary, but
the local sandbox blocks the Go build-cache stat and the dashboard-linter's
uncached module lookup; the normal offline bootstrap also cannot resolve the
pinned revive module. The previous exact lint receipt remains valid because
this checkpoint changes only Rust and documentation.

This closes the concrete LIKE/ILIKE compiled-pattern cache gap but does not
accept the whole expression, types, planner, executor, or workload packages.
Native ignored cases, complete production/build/generated/support inventories,
and full sysbench/TPC-C/TPC-H/YCSB measurements remain open under the original
goal.


## Continuing JSON_SCHEMA_VALID cache parity

Go's `builtinJSONSchemaValidSig.schemaCache` admits every schema argument whose
`ConstLevel` is `ConstOnlyInContext`, keyed by the statement `CtxID`; the
earlier Rust port admitted only strict literals because its cache predated the
context-id seam. `JsonSchemaCache` now uses the shared context-keyed cache, so
prepared/context-only schemas are compiled once per statement, constructor
errors are not retained, clones start empty, and a new statement context
replaces the previous schema. The existing document NULL short-circuit and
validator lifetime are unchanged.

The native source tests now cover clone reset and a prepared schema that stays
cached within one context but is replaced in the next. From `rust/`:

    cargo test --offline --locked -j12 -p tidb-expr --lib json_schema_valid_cache -- --test-threads=1
    cargo test --offline --locked -j12 -p tidb-expr --lib json_schema_valid -- --test-threads=1

The cache selection passes 2 tests. The broader JSON-schema selection passes 4
tests; its fifth test is the existing sandbox-blocked localhost HTTP fixture.
The complete expression run after this cache change reaches 1216 passed with
that one same permission-denied bind. The package and workload acceptance
gates remain open, including the Go failpoint-only cache-refresh oracle and
the full sysbench/TPC-C/TPC-H/YCSB measurements.


## Continuing JSON modification path parity

The Go `jsonModify` source marks its path-expression loop as a hot-path cache
opportunity when path arguments are constants. Rust now gives
`JSON_SET`/`JSON_INSERT`/`JSON_REPLACE` a context-keyed parsed-path cache when
all path arguments are `ConstOnlyInContext`; row-dependent paths still parse
normally. The cached path path uses the existing exact-leg validation and
mutation semantics, keeps value arguments per-row, and parses the document
before cached paths so NULL and document errors retain Go's ordering. Cache
errors are not retained and clone/invalidation reset the entry with the shared
cache lifecycle.

The active source test changes a prepared path within one context (the cached
path remains) and then uses a new context (the path is replaced). The focused
receipts from `rust/` are:

    cargo test --offline --locked -j12 -p tidb-expr --lib json_modify_path_cache_replaces_context_only_paths -- --test-threads=1
    cargo test --offline --locked -j12 -p tidb-expr --lib 'builtin_ext::json::tests::json_set_insert_replace_go_vectors' -- --test-threads=1

Both pass. The complete expression gate is rerun before publication; the
latest full run reaches 1217 passed with the same sandbox-blocked localhost
JSON-schema bind (one failure). The broader package and
sysbench/TPC-C/TPC-H/YCSB acceptance gates remain open. The session plan-cache
regressions selections remain green: 28 non-prepared and 45 prepared tests.


## Continuing MaxOneRow enforced-MPP warning parity

Go's `findBestTask` first applies the operator-self task-type check. A
`LogicalMaxOneRow` therefore refuses an MPP property before physical
enumeration and raises `RaiseWarningWhenMPPEnforced`; its root ordered path
reaches `ExhaustPhysicalPlans4LogicalMaxOneRow`, which raises the same source
message when order is refused. Rust previously returned the generic invalid
task before either side effect, and its statement context had no
`InExplainStmt`/extra-warning split.

Rust dispatch now accepts an optional statement-context warning sink and
preserves the MaxOneRow MPP refusal warning immediately before the generic
non-root return. The root physical arm retains the source condition for an
ordered property. `StmtContext::append_mpp_warning` applies the captured Go
`allowMPP && enforceMPP` gate, stores code 1105 in ordinary warnings for
EXPLAIN/EXPLAIN ANALYZE, and stores it in a bounded extra-warning handler for
ordinary statements. EXPLAIN contexts are marked at the session boundary;
ordinary warning retrieval remains separate from extra-warning retrieval.

The initial red planner command failed because the MPP early return produced
no sink entry. After the early-return side effect was added, the focused
commands from `rust/` pass:

    cargo test --offline --locked -j12 -p tidb-planner --lib max_one_row_refusal_raises_the_source_warning_once_per_refusal -- --test-threads=1
    cargo test --offline --locked -j12 -p tidb-executor --lib enforced_mpp_warning_uses_go_explain_and_extra_handlers -- --test-threads=1

The tests cover root ordered refusal, MPP refusal, supported root planning,
EXPLAIN versus ordinary warning storage, and the unenforced no-op. Rust-only
production/test/docs changes require no `make bazel_prepare`; the complete
physicalop package, live TiFlash behavior, other MPP operator/task gaps, and
the sysbench/TPC-C/TPC-H/YCSB performance gates remain open. The red result,
green results, formatting, lint, and publication receipts are recorded with
this checkpoint.


## Continuing MPP-to-root conversion parity

Go's `MppTask.ConvertToRootTaskImpl` creates a pass-through
`PhysicalExchangeSender` around the MPP fragment and exposes it through a
TiFlash `PhysicalTableReader`. It expands virtual-column dependencies before
building that boundary, copies task warnings to the new root task, and turns
root-only conditions into a source Selection. Conditions attached to any
other MPP shape return Go's invalid task.

Rust now performs that same owned conversion in `MppTask::into_root_task` and
uses it from `Task::into_root_task`; the sender and reader receive fresh plan
IDs from the caller's allocator and retain the fragment schema/statistics.
The focused source-shape regression passes:

    cargo test --offline --locked -j12 -p tidb-planner --lib \
      task::tests::mpp_task_converts_to_tiflash_reader_with_passthrough_sender \
      -- --test-threads=1

`cargo check --offline --locked -j12 -p tidb-planner` also passes. This closes
the direct MPP-to-root construction refusal but does not yet add Go's
multi-fragment `PhysicalExchangeReceiver`, task metadata, or the remaining
MPP enforcement/attachment branches. No TiFlash cluster execution or
sysbench/TPC-C/TPC-H/YCSB performance result is inferred from this unit test.
