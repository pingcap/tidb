# Preserve and lock the rows selected by a locking query

This living ExecPlan follows repository-root `PLANS.md`.

## Purpose / Big Picture


Concurrent Rust SQL clients must not receive the same queue item from a
`SELECT ... ORDER BY ... LIMIT 1 FOR UPDATE` while one client owns its lock.
This is a correctness prerequisite for measuring generic sysbench and TPC-C
performance. A waiting reader must repeat its query after a conflicting commit
and return the next eligible row. No workload-specific branch is permitted.

## Progress


- [x] Add opt-in TPC-C input seeding to the pinned local driver, controlling
  both process-level nonuniform constants and per-worker random streams.
  Runtime red/green verifies fresh-process repeatability, distinct worker
  streams, zero seeds and invalid inputs; driver package checks and release
  build pass. Preserve unseeded defaults and transaction logic. The patch,
  binary hashes and exact commands are in tpcc-input-seed-baseline.json.
- [ ] Run the seeded before/after/repeated-before/Go comparison. The runner
  enforces exact transaction-type counts across equal-input samples; database
  state and scheduling still evolve, so this is not full replay. Auto-review
  rejected the 320000 measured plus 8000 warmup fixture writes before process
  creation; explicit approval is pending. No benchmark handle exists. Do not
  bypass the rejection or treat approval-option preselection as consent.
  The same boundary persisted for three consecutive goal turns. After the
  unaffected diagnostics completed, the third-turn audit verified no active
  benchmark/build and no seeded-run output. The goal is now blocked pending
  explicit approval, not complete. Preserve the full optimization objective.
- [x] Measure statement-context construction in the existing in-memory
  lifecycle suite while fixture-write approval is pending. Go ResetContextOfStmt
  reuses eligible state; Rust constructs defaults then replaces session-owned
  channels. Ignored diagnostic statement_context_construction_cost_probe
  compares query/DML context construction and SELECT 1 with five rotating
  rounds. It changes no production path and touches no shared SQL database.
  Session 36770 is terminal: one ignored diagnostic passes, five 10000-operation
  samples per case. Context cost is about 1.7-1.8 microseconds versus about
  27 microseconds for in-memory SELECT 1. This does not establish the workload
  bottleneck or justify another production micro-optimization. Exact samples
  are in statement-context-cost-baseline.json.
- [x] Complete the data-free packet comparison with repeated-baseline control:
  PING and SELECT 1, explicit plaintext/TLS, four rotating before/after/
  repeated-before/Go rounds, 50000 commands per sample. Session 44551 is
  terminal: 64 samples, 3.2 million measured commands, zero errors. PING
  medians improve 8.29% plaintext and 2.73% TLS. Plaintext SELECT 1 is -1.22%
  with mixed pairs; TLS SELECT 1 is -0.74% and slower in every pair against
  both baseline observations. Output is /private/tmp/tidb-frame-write-command-control;
  exact commands, results and interpretations are in
  frame-write-command-control-baseline.json.
  This is an allowed read-only diagnostic, not a replacement for the rejected
  mixed-workload writes or evidence of a sysbench/TPC-C improvement.
- [x] Check current candidate state without persistent writes. Six connections
  (before/after/repeated-before over plaintext/TLS) have identical 948 session
  variables, 948 global variables, SELECT 1 plans and same-mode client socket
  settings. TLS 1.3 and AES-256-GCM match. All 30 SQL comparison records agree;
  variable values were not saved. Binary hashes are unchanged. This snapshot
  does not explain the regression or prove state was unchanged during timing.
  Exact command and evidence are in frame-write-command-control-baseline.json.
  Mixed fixture-write approval remains pending; no benchmark retry was made.
- [ ] Write each uncompressed frame through one native vectored operation.
  Go supplies header and body together to PacketIO's writer. Rust splits
  them, and ClientStream does not forward vectored writes. Installed Rustls
  Stream implements vectored writes, but StreamOwned only forwards scalar
  writes. Use the borrowed Stream over the same existing TLS connection and
  socket, not a second buffer or connection. Add transport-count, partial
  write, sequence, full-frame and live TLS regressions; preserve compression.
  Then run protocol/TLS checks, live Go comparisons and both mixed workloads.
  Before binary is 4f081c5f on 53051 and is saved at
  /private/tmp/tidb-frame-write-before-TnXtCM/tidb-server. Both transport-count
  regressions failed at runtime before the patch: two scalar frame writes,
  and TLS accepting only the first 7 of 16 vectored bytes. Corrected the
  post-change ErrorKind-to-PacketError compile conversion; 23 packet I/O
  checks now pass. TLS suite 1957 and compression/timeout suite 37632 are
  terminal with six and five passes, respectively (34 scoped checks total).
  Candidate 68730b1d runs on 53052/session 59794. Live comparison 46418 is
  terminal: plaintext/TLS packet cases across Go/before/after, eleven overlays
  and 48 DISTINCT/HAVING cases match Go. Sysbench 53954 completes with zero
  errors/reconnects: median +1.55%, pairs -4.25%, +1.55%, -6.95%; no reliable
  gain. TPC-C 37984 is terminal: 180,000 measured transactions plus 6,000
  warmups, zero errors, two/eight-client -8.70%/-1.69%, all pairs slower.
  Postcheck 96704 and profile/postcheck 18209 both pass eleven conditions and
  30000/0/0 balance/delivery checks. Fixed PING/SELECT diagnostic 77415 has
  mixed paired directions and does not explain the workload regression.
  All handles except server 59794 are terminal. Evidence and exact commands
  are in rust/benchmarks/frame-write-baseline.json; no speedup claim.
- [ ] Borrow expression type metadata for read-only collation queries. Go
  Column/ScalarFunction.GetType returns the retained type; Rust ret_type_of
  clones both charset strings on every call, including sort comparisons.
  Preserve the existing owned NULL placeholder when no static type exists.
  First add allocation-sharing and fallback regression coverage for all four
  expression variants, then run original collation/sort checks and real Go
  comparisons before paired mixed workloads. CPU diagnostic 81701 is terminal:
  20,000 TPC-C transactions without errors, 33.65-second cumulative interval.
  Current Rust used 174.77% of one core, TiKV 389.99%, and the other 34 Rust
  servers together 15.27%; this does not support the earlier suggestion of
  sustained 16-18% per older server. Sysbench diagnostic 50759 also completed
  without error: current Rust 84.29%, TiKV 142.35%, other Rust total 12.70%.
  Sysbench flags match the timing helper, including --mysql-ssl=off, but TLS
  frames remain in the sample; do not infer plaintext transport from the flag.
  No server was stopped; sampled runs are not throughput baselines. The
  ownership regression fails at runtime before borrowing and passes after;
  63 scoped collation, metadata-function, sort and TopN checks pass. Release
  4f081c5f built in 1m39s and runs on 53051/session 25228. Live checks 60894
  are terminal: 27 collation cases across three connection collations, eleven
  overlays and 48 DISTINCT/HAVING cases match Go. Sysbench comparison 55965
  completed three rotating 30-second rounds with zero errors/reconnects:
  median -2.99%, pairs -3.07%, -2.99%, +2.75%. TPC-C 71636 completed
  180,000 measured transactions and 6,000 warmups without errors: two/eight
  client medians +3.28%/-7.35%, all eight-client pairs slower. Postcheck
  77525 and profile/postcheck 80234 are terminal; all eleven conditions and
  30000/0/0 balance/delivery checks pass. No throughput-gain claim; see the
  current handoff for the follow-up profile and unresolved regression.
  Before 701d7abb remains on 53050 and is saved in
  /private/tmp/tidb-type-read-before-ymJqKj/tidb-server. Evidence is in
  rust/benchmarks/type-read-baseline.json.
- [ ] Share catalog schema snapshots without cloning unrelated database maps.
  Separate mixed-workload profiles of 8e153009 identify database-map cloning
  in transaction and statement snapshots (260 non-wait-classified Catalog
  clone frames in TPC-C, 29 in sysbench; not CPU percentages). Go
  `pkg/session/session.go:GetInfoSchema` retains the selected schema snapshot.
  Rust `Catalog::clone` currently clones every database's table-name map,
  even when a statement changes only one database. Keep the existing table
  copy-on-write boundary and extend sharing through the database map and
  database entries. Add allocation-sharing and mutation-isolation proof;
  preserve rollback, temporary tables, names, versions and shared allocators.
  Before binary 8e153009 is saved at
  `/private/tmp/tidb-catalog-snapshot-before-ci5NOO/tidb-server` and remains on
  port 53049. Regression session 66084 fails at runtime on old map copying;
  session 64345 passes after two-level copy-on-write. Scoped validation
  session 90730 is terminal; DDL/statistics checks bring the total to 141
  passes with only the existing manual benchmark ignored. Release build
  13635 completes in 1m35s; 701d7abb runs on port 53050/PID 28926/session
  12701. Live Go affected-row, eleven overlay and 48 DISTINCT/HAVING cases
  pass. Sysbench 97574 completes: median -2.29%, pairs -2.13%, +5.85%,
  -6.72%, zero errors/reconnects. TPC-C 41244 completes 180,000 measured
  transactions plus 6,000 warmups: +1.25%/+3.20% medians at two/eight clients,
  both with mixed paired directions. Postcheck 5727 passes all eleven
  conditions and reports 30000/0/0. No broad speedup claim. Baseline:
  `rust/benchmarks/catalog-snapshot-baseline.json`. All handles are terminal.
  The first compile attempt used wrong fixture type names, not runtime red.
- [ ] Reuse canonical store metadata during epoch recovery. A strict-error
  90-second mixed sysbench diagnostic plus three explicit benchmark-table
  splits completed 36,324 transactions, zero errors/reconnects and maximum
  latency 33.57ms; the 20-second stall did not recur. Logs use
  `/private/tmp/tidb-range-point-split-stall`. Go `newRegion`/`Store.initResolve`
  reuse resolved stores; Rust re-fetches each store for each split child.
  Add a real-PD-fixture request-count regression, then carry the canonical
  store snapshot in the existing epoch recovery plan and resolve only absent
  stores once across its children. Keep refresh/invalidation owned by the
  existing store cache and reject stale recovery publications as before.
  Runtime red is `/private/tmp/tidb-epoch-store-reuse-red-v4.log`: four
  GetStore requests versus zero. Earlier attempts used the wrong aggregated
  test target, a private helper, or hit sandbox socket denial; they are not
  behavioral red evidence. The native loader now shares recovery-scoped
  resolved stores supplied by the canonical cache. Known stores require no
  RPCs; one new shared store requires one. An additional runtime regression
  caught cached metadata falsely resetting NeedCheck to Resolved. Epoch
  publication now preserves existing store state and inserts only new store
  observations. The 14 PD-loader tests pass, plus 3 unlocked-cache, 2 bucket,
  7 store-failure, 8 maintenance, 25 recovery and 31 lock tests. Distributed
  SQL/executor/unistore adapters bring the total to 106 scoped passes, and
  transaction differential wrappers compile. Release 8e153009 runs on port
  53049; live Go result/affected-row checks pass. A new strict-error 90-second
  split diagnostic completes 39,360 transactions with maximum latency 25.78ms,
  zero errors/reconnects and three successful splits. No stall recurs in either
  diagnostic, so neither is a throughput or stall-resolution receipt. Evidence
  is in `rust/benchmarks/epoch-store-reuse-baseline.json`.
  Mixed sysbench is terminal: candidate median 432.10 TPS versus 442.18
  before, -2.28%, with all three paired rounds slower and zero errors.
  TPC-C comparison 22409 completed 180,000 measured transactions plus 6,000
  warmups, zero errors: two/eight-client median -2.97%/+12.70%, with all
  eight-client pairs faster and two-client pairs mixed. Separate profile
  workloads also complete without error. Postcheck 35375 passes all eleven
  conditions and reports 30000/0/0 customer balances/delivery counts. All
  related handles are terminal. No timeout change or rare-stall explanation.
- [ ] Convert chosen complete-key handle ranges into actual point readers.
  Before is 53acb9f4 on port 53047, saved in
  `/private/tmp/tidb-point-conversion-before-vYY819/tidb-server`. A strengthened
  remote-scan regression fails on the old code with one iterator and one
  coprocessor scan instead of one batch get (`/private/tmp/tidb-range-point-red.log`).
  The new shared range-to-handle helper is used by SELECT and DML, preserving
  full-key/non-null, prefix-index, fix-control and partition eligibility.
  SELECT installs the existing HandleSourceExec; DML selects Point/Batch.
  Removed the DML EXPLAIN-only conversion of ranges to points. The first
  conversion exposed that DML inferred predicate consumption from reader kind:
  a new runtime regression updated two rows where its residual admitted none.
  WriteReadPath now carries the consumption proof beside WriteReader; the
  rejected UPDATE/DELETE and existing residual Selection test pass. Added
  integer, absent-row, ORDER/LIMIT, fix-control, common-key/NULL and partition
  eligibility tests. KvTable also retains raw column publication state for
  all point builders; the loader must inspect TableInfo.columns, since cols()
  already removes non-public columns. Scoped checks: 160 passes and the two
  known estimate/extra-Sort failures. The 26 cluster-session tests pass.
  Final collation coverage passes. The initial release built as b129bc8e;
  a final source audit also applied column publication eligibility to the
  index-point EXPLAIN path. Properties pass again; final release build completed
  in 1m40s, log `/private/tmp/tidb-range-point-build-final.log`. Candidate
  7592762e runs on port 53048. Live affected-row counts (including zero-row
  residual rejections), eleven overlay results, four stream-favoring cases,
  48 DISTINCT/HAVING cases and clean/dirty aggregates match Go. Actual point
  execution has scoped storage-operation evidence and live Batch_Point_Get
  plans. All 486,000 fixed-data queries match Go. Full-key SUM improves 8.54%
  at eight clients with all three pairs positive; one-client timing is mixed.
  Neighboring clean/dirty aggregates have near-flat medians. Mixed sysbench
  median regresses 9.68% and includes a 20-second stall; TPC-C +2.15%/-1.17%
  at two/eight clients is inconsistent across pairs. All eleven consistency
  conditions and 30,000 customer checks pass. See Current handoff below.
  No readiness or consistent mixed-workload gain claim.
- [ ] Restore shared composite range extraction inside conjunctions. The
  unordered-overlay live test exposed an existing range UPDATE failure on
  both 84903e6c and 9760a17f (8175, while Go succeeds). Read Go ranger
  `extractBestCNFItemRanges` and its suffix-range append: preserve individual
  point prefixes, select the deepest conjunct prefix, and append ranges from
  remaining conditions over the suffix columns. A new generic ranger test
  covers tuple-IN/OR, adjacent points, suffix comparisons, NULLs, duplicates,
  contradictions and residuals. The original test fails with no derived range;
  the implementation passes it. A residual-mapping control also fails at
  runtime, proving prefix indexes retain the written row-IN filter. A bounded
  UPDATE/DELETE fixture reads only two matching keys instead of sixteen.
  Ranger, cost, handle, partition and remote-scan checks pass. The point suite
  has one row-estimate mismatch that also fails with the new behavior disabled;
  do not claim all tests pass. Release 53acb9f4 built successfully and runs on
  port 53047/session 15449. The exact live UPDATE and DELETE each affect ten
  rows, matching Go; eleven follow-up result cases match and all transactions
  roll back. Rust now uses ten complete-key ranges, while Go uses Batch_Point_Get.
  No DML shortcut or memory-limit increase. The 324,000-operation clean/dirty
  SUM regression run passed Go result checks with mixed timing directions;
  this does not establish a speedup for those unchanged predicate shapes.
  The newly supported suffixed SUM completed three rotating rounds: median
  4046.97ms before and 0.935ms after, all 270 results matching Go. Fast samples
  are short, so a separate 15,000-result candidate repeat verifies 0.832ms;
  it does not establish Rust-versus-Go equivalence or a mixed-workload gain.
- [ ] Preserve unordered table scans through staged-row merging. Live request
  tracing on release 84903e6c confirms that dirty SUM forces `keep_order=true`,
  making the source-shaped simple-scan rule reduce concurrency from 15 to 2.
  Go's same query keeps order false and executes nine region tasks concurrently.
  Unordered overlays now suppress shadowed snapshot identities and append
  live staged rows, while ordered/descending consumers retain their merge.
  Original code fails the request-order regression; dropping ordering alone
  produces SUM=340 instead of 280. Both controls are restored. Final scoped
  tests pass (23 remote scans, 15 cursors, 15 storage); release 9760a17f runs
  on port 53046. Its live trace dispatches all nine tasks before completion.
  Seven integer and four composite overlay cases match Go, plus the 48-case
  DISTINCT/HAVING matrix. Isolated timing session 60612 completed 324,000
  Go-matching operations: dirty SUM +54.19%/+45.29% at one/eight clients,
  all six paired directions positive. Clean SUM was -2.13%/+0.37%; a longer
  45,000-operation one-client repeat was +0.41% with mixed directions, so
  no consistent clean-path improvement/regression is established. Eleven
  TPC-C conditions and 30000/0/0 balance checks pass. Mixed sysbench session
  4968 completed error-free, median +1.07% with mixed paired directions;
  no consistent whole-workload win. Mixed TPC-C session 99370 completed:
  180,000 measured transactions, zero errors, median +11.89%/+1.19% at
  two/eight clients with mixed paired directions. Postchecks passed all eleven
  conditions and 30000/0/0 customer balances/delivery counts.
  Separate range UPDATE with a tuple-IN predicate plus ol_number residual
  fails with 8175 on both old and new binaries; keep that DML defect open.
- [ ] Remove the coprocessor admission receipt wait from normal query execution.
  The grouped-SUM profile points to this wait, not statistics checkpoint copies.
  Route identity now comes from the sole in-flight table via existing request
  progress before sending. Normal begin enqueues; completion and non-blocking
  publication reads do not collect submission receipts. Explicit pre-response
  observers retain their barrier. Two pending tests, 65 distributed-SQL tests
  and 74 batch-related integration tests pass. Both route-without-receipt and
  accidental admission-barrier regressions have runtime red/green evidence.
  Release 84903e6c runs on port 53044. In 324,000 Go-checked fixed-data
  operations, clean SUM improves 3.85%/11.49% at one/eight clients; dirty SUM
  is -1.22%/+2.86%. A longer 45,000-operation one-client dirty repeat is
  +0.29% with mixed directions, so that case is flat/inconsistent. Eleven
  TPC-C conditions and all 30,000 customer checks pass. Mixed sysbench median
  is +0.79%, with pairs -12.86%, +0.79%, -0.75% and zero errors/reconnects;
  no consistent whole-transaction improvement or readiness claim.
- [ ] Retain costed plain DISTINCT tasks. The existing range regression failed
  with three actual pipeline builds, then passed with two after publishing
  partial/final aggregation and ordering/window candidates. The expanded
  test covers computed and multi-column fields, empty input, LIMIT and TopN.
  The aggregate suite has 34 passes plus its unchanged extra-Sort failure.
  Eight candidate-cost, 42 plan-selection and 14 trace tests pass; NULL and
  dirty/clean-input cases pass too. Release f2ff9c88 runs on port 53041.
  All 27 live results match Go in both Rust binaries; both retain the same
  three EXPLAIN errors and eight matching operator trees. Fixed-data results
  improve DISTINCT 10.16%/5.68% and DISTINCT TopN 15.60%/5.73% at one/eight
  clients, across 324,000 Go-checked operations. Eleven of twelve paired
  directions improve; the negative sample remains. Mixed sysbench +0.45%,
  TPC-C two/eight-client -0.97%/-3.25% are not a broad win. All workload and
  consistency checks exited; 11 TPC-C conditions and 30,000 customer checks
  pass. Evidence: `rust/benchmarks/retained-distinct-baseline.json`.
  The follow-up audit reproduced a missing HAVING cost, then a correlated
  Apply's incomplete cost being published after its filter. Both have runtime
  red/green evidence. Source task ownership now follows plain SELECT's
  transformations: HAVING composes Selection cost; unrepresented Apply and
  materialized windows keep the complete task unpriced instead of reviving
  a leaf estimate. Expanded live checks then exposed a source-schema panic:
  partial DISTINCT passed through Selection and discarded columns that its
  bound HAVING predicate still read. The range-qualified regression reproduces
  the chunk index panic before the Selection negotiation fix and passes after.
  Selection now owns TableAccess, forwarding predicate/range/order negotiation
  but refusing schema changes and pre-filter row cuts. Removed the transient
  partial-cost traversal that assumed Selection was transparent. The suite now
  passes 35 tests plus the unchanged extra-Sort failure. SELECT clauses pass
  12 tests, through-projection 15 and join-reorder 26. Subqueries pass 15 with
  two plan-shape failures; reversing only the HAVING/Apply/window cost audit
  reproduces both at the same assertions. That control was restored, and the
  focused HAVING regression passes again. Release 4b29b04c on port 53043 passes
  45 live query results and three Go-error cases with no lost connections.
  Fixed-data DELIVERY completed 16,200 measured rollback transactions: one/eight
  client medians -0.49%/+0.81%, mixed paired directions, no consistent gain.
  Eleven consistency conditions and all 30,000 customer checks pass.
  Remaining: profile grouped SUM and point-update work while locks are held,
  existing plan mismatches, and complete Apply/window costing.
- [x] Measure the consolidated grouped SUM path without claiming broad success. Temporary live probes proved
  that the old single-SUM partial path omitted its costed task and forced
  three pipeline builds. A statistics-backed regression reproduced three
  builds versus the expected two; the general grouped path passes it.
  Removed the duplicate descriptor, state remapping, evaluator, wire lowering
  and trace helpers, following Go `base_physical_agg.go:278-313,594-660`.
  Temporary probes are removed from source. Release 45c21a6d built successfully
  and runs on port 53039/session 37148, compared with 03420212/53037.
  Tests: 34 aggregate passes plus the known extra-Sort failure, 14 trace,
  23 remote-scan, five partial-predicate and eight DAG-lowering passes.
  The strengthened regression verifies decimal/NULL/empty results as well as
  actual build count. The initial FromStr test compilation errors were
  corrected using Decimal::from_literal before these results were obtained.
  Live results match Go in 39 standard and nine actual-query cases per
  endpoint. The actual-query matrix has two pre-existing operator mismatches
  at expensive HashAgg factors in both Rust binaries; not complete plan parity.
  Fixed-data timing completed: 36 samples / 324,000 Go-matching operations.
  Clean SUM medians improve 18.18%/11.40%, dirty SUM 10.63%/7.16% at one/eight
  clients, with all twelve paired directions positive. The same-increment
  baseline is `rust/benchmarks/general-grouped-aggregate-baseline.json`.
  Sequential mixed sysbench/TPC-C validation completed (session 71534, exit 0).
  Median changes: sysbench +0.05%, TPC-C two-client +8.70%, eight-client -2.97%.
  Paired directions vary, so no stable mixed-workload improvement is proven.
  All 180,000 measured plus 6,000 warmup TPC-C transactions completed; the
  independent consistency checks completed (session 84875, exit 0): eleven
  conditions and 30,000 customer balance/delivery-count checks pass.
  Sysbench retains a 20,055-ms transaction in the first after sample; its
  49.06 TPS is not discarded. TiKV logged EpochNotMatch during a split at
  15:39:07.741-742, but this is correlation, not yet a causal explanation.
- [x] Verify the lock-recovery metadata-I/O ownership gap. Unlike transaction
  recovery, `lock/resolver.rs:recover_lock_region_error` calls synchronous
  cache recovery inside `with_region_cache`, holding the canonical mutex
  through loader I/O. Go `region_cache.go:2886-2941` releases its mutex while
  creating replacement regions; the existing Rust background-cache API does
  this too. Added a source regression that blocks metadata loading until an
  independent cache operation completes, and verifies retry against the new
  epoch. The initial regression also called the metadata loader via
  `cache.cluster_id()`, so both old and new implementations failed it. That
  is not valid before/after proof. It now inspects `cache.is_empty()` only;
  the corrected v2 negative control failed at runtime after the release build
  ended, and the final v2 lock suite passes all 31 tests. The source uses the
  existing `region_cache_handle().on_region_error` owner; 25 region-error and
  four DistSQL lock-recovery tests pass. Release 4d60fa05 built successfully.
  Do not claim this explains the observed 20-second transaction without live
  causal evidence; do not shorten timeouts or add a retry workaround.
- [ ] Close the stale-region selector creation race exposed by the retained
  aggregate benchmark. Session 90936 exited 1 at 2026-09-07 14:55:58 with
  `partial aggregate response failed: Backend("region route failed: MissingLeader")`.
  TiKV logged auto-splits at 14:55:57.320-332; PD health remained true.
  `request_selector` errors if the versioned entry vanished before creation,
  whereas `select_request` already returns `ReloadRegion` if it vanished
  afterward. Go `internal/locate/region_request.go:974-987` re-splits a request
  when its cached region is gone. Add a before/after-creation eviction test
  across replica policies, then let selection own that existing reload path.
  No server restart or benchmark retry workaround; the partial timing run is
  failed evidence, not a throughput result.
  Cache and direct-unary regressions both fail at runtime with MissingLeader.
  The source change removes the early error in `request_selector`; existing
  `select_request` now owns validity for both eviction timings. Four replica
  policy tests, sixteen request-selector tests and all sixty-four direct-unary
  tests pass. The transport fixture proves no RPC to the obsolete parent,
  exact delivery of both split children, and the existing bounded backoff.
  Release build 32305 exited zero (03420212), running on port 53037/session
  11606. Fifteen Go plan trees and 39 result cases match. The fresh rotating
  isolation completes 36 samples / 324,000 operations without errors; all
  results match Go. This does not establish stable throughput improvement.
- [ ] Retain the selected aggregate pipeline instead of rebuilding it. Go
  `find_best_task.go:605-745` returns/stores the chosen task; Rust currently
  builds Stream and Hash alternatives and reconstructs the winner. First
  add a runtime build-count regression, then retain the winning executor,
  output receipt, statistics checkpoint and forked trace. Separate derived
  output mode from requesting a cost receipt so top-level projections remain
  correct. Validate both cost-factor choices, deferred execution, plain
  EXPLAIN and ANALYZE, then compare release performance against a39ab966.
  The build-count regression fails with three builds before the ownership
  change and passes with two afterward; aggregate tests retain 32 passes and
  the known extra-Sort failure. Expanded ownership checks exposed a separate
  pre-existing DISTINCT gap: the plain DISTINCT path publishes no complete
  aggregate cost receipt and falls back to StreamAgg under both cost-factor
  choices. Live Go versus prior port 53035 confirms this on DISTINCT
  order_line warehouse/district columns. Do not claim that path cost-correct;
  the retained-task test checks actual costed GROUP BY choices and output
  permutations instead of assuming every DISTINCT alternative was priced.
  All 48 ownership combinations pass (two queries, two cost choices, three
  trace modes, direct/derived output, immediate/deferred execution). Trace
  tests pass 14; aggregate tests 33+1 prior failure; point tests 21+1 prior
  failure. Release 4134703e built and passes fifteen Go operator-tree and 39
  result checks; it runs as PID 10048/session 21226 on port 53036. Preserve
  a39ab966 on port 53035 and at
  `/private/tmp/tidb-retain-aggregate-before-QiFabL/tidb-server`.
- [ ] Estimate fallback selectivity only for predicates not already covered
  by greedy statistics nodes. A 15-second sample of 160,000 prepared dirty
  SUM queries completes on fd96103d; selectivity/range planning is prominent,
  and source comparison finds eager recursive DNF estimates whose results are
  discarded for covered conditions. Go `cardinality/selectivity.go:212-245`
  selects the cover before classifying leftovers. The new callback regression
  fails at runtime with evaluations `[0,1,2]` instead of `[]` under full cover.
  The callback API initially retains eager behavior for that red control;
  the implementation now evaluates it after the mask test, without keeping
  the old eager path. Selectivity tests: 11 passed; row-count fixtures: 15
  passed; access-cost tests: 23 passed. Aggregate/point suites retain only the
  prior failures (32+1 and 21+1 respectively). Release a39ab966 built and
  live checks match all fifteen Go operator trees and 39 result cases.
  Prepared-query isolation has mixed medians: clean SUM +4.20%/+0.41% and
  dirty SUM +2.09%/-4.90% at one/eight clients. The repeated dirty profile
  still shows prominent selectivity planning. No broad gain is established;
  mixed performance validation remains required. Raw evidence is under
  `/private/tmp/tidb-lazy-selectivity-*`.
- [ ] Publish pessimistic rollback region requests as one transport round,
  following pinned client-go `twoPhaseCommitter.doActionOnBatches`. Fixed-data
  DELIVERY isolation found after-version rollback at 5.13 ms versus Go 1.15 ms;
  the live tables span many regions and Rust waited for each scalar rollback
  response. Two new runtime-red tests prove serialized publication and early
  exit before successful sibling cleanup. Batched publication and complete
  per-region bookkeeping now pass all 29 pessimistic-lock source tests,
  including a new region-recovery retry test. Release fd96103d runs on port
  53034; before 459ff441 remains on 53033 and is preserved at
  `/private/tmp/tidb-rollback-before-gDMQzA/tidb-server`. Live fixed-data rollback
  comparison completes eighteen samples with all results matching Go: rollback
  latency drops about 78-79% to 0.98 ms, near Go's 0.95-1.10 ms. Whole rolled-back
  DELIVERY throughput is +19.08% at one client but -2.37% at eight. Mixed
  comparison session 5500 completed: sysbench median +0.98%, TPC-C two-client
  +6.94%, eight-client -0.09%; paired directions vary in all workloads. All nine
  sysbench samples and 180,000 measured/6,000 warmup TPC-C transactions complete.
  Postcheck 21650 passes eleven conditions and all 30,000 customer balances and
  delivery counts; final source suite 31502 passes 29 tests. WIP: no stable
  overall throughput improvement or Ready/lint claim. Evidence is recorded in
  `rust/benchmarks/pessimistic-rollback-batch-baseline.json`.
  This is a generic cleanup optimization, not a claim that rollback explains
  the original committed-workload regression.
- [ ] Replace aggregate chooser row-count cutoffs with valid whole-candidate
  costing. Live Go/Rust probe completes fifteen cases per endpoint: Go switches
  with cost factors while both Rust versions ignore them. Added multi-function
  aggregate regression; runtime-red 87722 exits 101 (HashAgg instead of
  StreamAgg at hash factor 1000); the cost-based chooser now passes that test.
  Production candidate selection and hash/stream wire lowering are implemented
  in WIP form. Aggregate suite v9: 32 passed, one prior failure (condition six's
  extra Sort). Preserving index-leaf reader structure resolves conditions two
  and eleven. New point/batch-factor and HashAgg order-advertisement regressions
  have runtime red/green evidence. Whole-child comparison now reads statement
  factors too. DAG suite: fourteen passed; focused wire suite: five passed.
  New release 459ff441 on port 53033 matches Go's operator trees in all fifteen
  live cases (before: eight); all 39 result cases per endpoint match Go.
  Comparison 76441 completed: sysbench median -0.78%, TPC-C two-client +3.74%,
  eight-client -6.54%; every eight-client pair is slower. All 180,000 measured
  and 6,000 warmup transactions complete; eleven conditions and 30,000 customer
  balances/delivery counts pass. Paired profiles and fixed-data prepared-query
  isolation now complete; 324,000 measured query/locking operations and 2,430
  measured rolled-back DELIVERY transactions all match Go results. Eight-client
  isolated SUM +1.98%, dirty SUM +0.18%, disjoint locking -0.29%, shared locking
  +0.66%; rolled-back DELIVERY -1.10%. These do not reproduce or explain the
  mixed-workload -6.54% result. Eleven consistency conditions and all 30,000
  balances/delivery counts still pass. Full diagnostic evidence is in
  `rust/benchmarks/aggregate-choice-baseline.json`.
  No overall speedup or Ready claim; retain corrected semantics.
- [x] Share the immutable optimizer-cost environment across query/DML/cloned
  contexts. Runtime red fails sharing; scoped and live checks pass. Native
  context setup drops 26-40%. Comparison 92153 exits zero: sysbench -0.31%,
  TPC-C two-client +1.50% and eight-client -3.12% medians, with mixed paired
  directions. All 180,000 measured and 6,000 warmup transactions complete;
  postcheck 5656 passes eleven conditions and all 30,000 balances/delivery
  counts. Six aggregate plan failures reproduce under prior ownership code.
  No broad stable speedup or readiness claim. Exact evidence:
  `rust/benchmarks/cost-env-owner-baseline.json`.
- [x] Derive statement SQL-mode flags once per variable generation. The
  regression fails before with two repeated scanner parses and passes after
  with zero, while query/DML flags and SET invalidation stay correct. Move
  only the existing derivation into StatementVarSnapshot; no new parser or
  workload path. All 66 scoped tests pass. Context setup falls 17-20% for
  default modes and 28-32% for expanded modes; empty modes stay roughly flat.
  Native benchmark samples include a test-only scanner counter and are not
  SQL throughput. Release d886a36d on 53031 matches Go in all 30 live cases.
  Serial comparison 71688 exited zero: sysbench +1.51%, TPC-C two-client
  -3.35% and eight-client -4.15% median TPS. All 180,000 measured and 6,000
  warmup transactions completed. Postchecks 67894 and 69921 pass eleven
  conditions and all 30,000 customer balances/delivery counts; the latter
  includes 40,000 additional profiling transactions. Preflight host is noisy (Chrome88.7%,
  mediaanalysisd86.8%, WindowServer44.8%); preserve those processes and do not
  claim a quiescent or stable broad measurement. Exact commands, hashes,
  samples and limitations: `rust/benchmarks/sql-mode-cache-baseline.json`.
- [x] Stop an empty SET_VAR restore from mutating SessionVars. Both regressions
  fail before: ordinary SELECT changes generation without a mutation, and a
  new server identity leaves the statement version stale. The owner now
  leaves empty restores untouched and invalidates on identity installation.
  Actual overlays still restore and invalidate. All 83 scoped tests pass;
  release 23174d49 on 53030 matches Go in 19 session-state and 11 global-read
  cases. Full in-memory SELECT median falls 40532 -> 39594 ns (2.31%),
  independently measured before/after, not a mixed-throughput claim.
  Comparison 99577 completes sysbench: 398.95 -> 394.15 median TPS (-1.20%),
  with paired rounds -2.53%, -1.20%, +0.39%; no sysbench gain is established.
  Full-mix TPC-C completes all 180,000 measured and 6,000 warmup transactions:
  two-client median -1.63%, eight-client +11.30%; paired directions remain
  mixed. Postcheck 1125 passes eleven conditions and all 30,000 customer
  balance/delivery counts. Separate 20,000-transaction profiling and postcheck
  47834 also finish successfully. Repeated optimizer-cost parsing disappears;
  SQL-mode scans remain. Exact
  commands and samples: `rust/benchmarks/empty-restore-baseline.json`.
- [x] Replace eager global-variable snapshots with the statement-selected live
  reader. Go `builtinValidatePasswordStrengthSig.evalInt` reads its accessor
  at evaluation; Rust copies eight globals for every context. The regression
  evaluates query/DML/cloned contexts after a peer update and verifies that
  scratch account-validation contexts keep their separate selected owner.
  Measure full context setup with empty, 1 KiB and 64 KiB dictionaries before
  and after; ordinary statements must stop copying unrelated dictionary data.
  Red log `/private/tmp/tidb-global-reader-red.log` returns Int(0) rather
  than Int(100) after enabling validation on the peer. The live-reader
  implementation passes, including scratch-owner lifetime. An executor
  counter test verifies ordinary expressions and NULL/short password inputs
  never call the global reader. All 69 scoped context/global/variable/account
  tests pass, and release e4e6384e runs on 53029 (session 16362). Eleven
  read-only text/prepared cases match Go without changing shared security
  settings. Context setup drops 4607 -> 3392 ns (query) and 4211 -> 3311 ns
  (DML) with an empty dictionary; with 64 KiB, 6681 -> 3313 and 4814 -> 3301
  ns. These isolated independent-before/after samples are not SQL throughput.
  Serial sysbench/TPC-C comparison 23698 exited zero: sysbench +0.56%,
  TPC-C two-client +2.33%, eight-client -9.88% median TPS. All three
  eight-client pairs regress; investigate rather than claim a broad win.
  Postcheck 28131 passes eleven conditions and all 30,000 customer
  balance/delivery counts after 180,000 measured and 6,000 warmup transactions.
  All commands and raw
  samples are in `rust/benchmarks/global-reader-baseline.json`. Six additional
  pairs have four gains and two losses (-5.25% to +6.29%), with substantial
  within-run drift on both endpoints. A consistent release-specific slowdown
  is not reproduced, but neither is a stable broad gain. Both profile runs,
  120,000 recheck transactions and 2,000 warmups complete; postcheck 45189
  passes eleven conditions and all 30,000 customer balance/delivery counts.
  Prior release
  528adaa7 remains on 53028 and is preserved at
  `/private/tmp/tidb-global-reader-before-boIuKi/tidb-server`.
- [x] Construct statement contexts with their real memory owner. The current
  full-mix profile records 685 heuristic non-wait inclusive observations in
  `statement_context_ignoring` and 213 in StatementMemory teardown, versus
  only sixteen in the staged delta copy. Go `ResetContextOfStmt` initializes
  the statement trackers under session roots directly. Rust first allocates
  standalone roots in `StmtContext::new`, then drops them when replacing
  memory. Query/DML constructors now take the real memory handle at creation;
  standalone constructors still supply the shipped quota. Removed the
  replacement builder and migrated all three callers. Fifty-three scoped
  context, memory, lifecycle, SQL quota and cancellation tests pass. The
  isolated five-pair constructor benchmark falls from 2271 to 1150 ns for
  queries and 2264 to 1153 ns for DML; its before arm reconstructs the
  removed builder, and no timing is used as a correctness assertion.
  Release 528adaa7 on 53028 matches Go in eleven live conflict cases;
  all 24 committed-write stages pass Go ADMIN CHECK TABLE. Serial mixed
  workload comparison 24546 exited zero: sysbench 357.45 -> 364.56 TPS
  (+1.99%), TPC-C two-client 362.58 -> 373.67 (+3.06%), eight-client
  414.64 -> 443.46 (+6.95%). Paired rounds include regressions on every
  workload, so these noisy medians do not prove stable broad improvement.
  All 180,000 measured transactions and 6,000 warmups complete. Postcheck
  34956 passes eleven conditions and all 30,000 customer balance/delivery
  counts. Full evidence is in `context-memory-owner-baseline.json`.
  Prior release is preserved at
  `/private/tmp/tidb-context-owner-before-NEEZz7/tidb-server` (7797d8fe).
- [x] Make deferred duplicate assertions safe for already-held locks before
  extending lazy checking to UPDATE and secondary indexes. Go KVTxn checks
  a new NeedCheckExists even on a held key; Rust currently filters the key
  first. The new embedded-store test covers existing point/range locks,
  an absent point lock, and an INSERT followed by UPDATE in the same
  transaction. The before run (session 82816) failed with INSERT succeeding
  instead of reporting 1062 (`/private/tmp/tidb-held-assertion-red.log`). The
  new check passes. Assertions newly added by the
  current statement must be distinguished from retained commit flags, so an
  earlier INSERT's mark is not rechecked against its own later UPDATE.
  The statement now reads only presumption marks since its checkpoint, not
  all transaction marks. The existing lazy INSERT policy also reaches its
  secondary unique indexes through one shared Get/GetLocal check. Its expanded
  regression fails before with an extra snapshot read and passes after,
  including local duplicates and deleted entries. A negative control restoring
  the old eager error-as-absence behavior fails on an injected storage error;
  the corrected implementation propagates it without staging a write. Fifteen
  storage and ten DML tests pass, along with the held-assertion and prior
  handoff regressions. Before 9fc77176 is preserved at
  `/private/tmp/tidb-index-lazy-before-4uujyq/tidb-server`, still on 53026.
  Release/lifecycle validation completes 50 distinct passing scoped tests.
  Release 7797d8fe on 53027 matches Go in all eleven assertion/conflict cases
  and all fifteen metadata cases; all 24 committed-write stages pass Go
  ADMIN CHECK TABLE. Serial comparison 92828 exited zero. Batched INSERTs
  with two unique indexes improve 121.82 -> 579.38 statements/s (4.7559x),
  with 45,000 rows checked. Sysbench is flat (+0.23%); TPC-C medians rise
  13.58% at two clients and 19.54% at eight, but inconsistent paired rounds
  and major endpoint/host drift prevent a stable broad-speedup claim. All
  180,000 measured transactions and 6,000 warmups complete. Postcheck 98316
  passes eleven conditions and all 30,000 customer balance/delivery counts.
  Raw samples, commands and limits are in
  `rust/benchmarks/index-lazy-baseline.json`.
  UPDATE's lazy policy and fair-to-normal lock lifetime are not
  enabled by this increment; those need the coordinated transition described
  under Surprises & Discoveries.
- [x] Eliminate duplicate post-execution lock handoffs for keys already
  acquired by the current statement. The embedded-store dispatch regression
  fails before with a second record-key request and passes after. The existing
  failure-cleanup ownership is now a BTreeSet; post-execution lock selection
  excludes those same keys, while new unique-index keys still reach the worker.
  Previous statements' locks remain the transaction worker's responsibility.
  Red/green logs: `/private/tmp/tidb-statement-lock-handoff-{red,green}.log`.
  Embedded-store suite: 37 passed, four failed at explicit unsupported index
  coprocessor paths. All four fail identically with this production change
  removed; the baseline records exact negative-control logs. All 23 lifecycle
  tests and the release build pass. Before 6c4ae765 is preserved at
  `/private/tmp/tidb-statement-lock-before-az4pd3/tidb-server` and runs on 53025.
  New release 9fc77176 runs on 53026 (session 5054). Eight lock scenarios per
  endpoint preserve Rust before/after behavior; all 24 stages pass Go ADMIN
  CHECK TABLE and the separate 15-case SQL matrix matches Go. The lock probe
  exposes a pre-existing mismatch: Go retains a newly acquired row lock after
  a duplicate-key UPDATE fails, while both Rust releases release it. This is
  an open parity gap, not a passed Go-equivalence check. Serial rotating
  comparison session 46454 completed: sysbench 441.44 -> 443.49 TPS (+0.47%),
  TPC-C two-client 519.01 -> 519.69 (+0.13%), eight-client 649.49 -> 707.88
  (+8.99%). Eight-client pairs are mixed and all endpoints drift downward
  over time; this is not proof of a broad speedup. All 180,000 measured
  TPC-C transactions plus 6,000 warmups completed. Postcheck session 35819
  passes all eleven conditions and 30,000 customer balance/delivery counts.
  Commands and evidence are
  in `rust/benchmarks/statement-lock-handoff-baseline.json`.
- [x] Restore loaded hidden-column metadata. The new loader regression
  `loaded_hidden_columns_preserve_native_layout_and_index_values` fails
  before with visible width four instead of three (`/private/tmp/tidb-loaded-hidden-red.log`).
  The loader now establishes visible-first native order once, keeps column
  IDs unchanged, binds generated/partition expressions to that order, and
  installs hidden columns through the existing executor API. Index loading
  and DDL backfill resolve offsets against the actual loaded KvColumns,
  removing their separate Go-order mapping. The regression passes, including
  interleaved stored columns, wildcard width, generated values, an index over
  the hidden expression, and an UPDATE followed by a forced index read.
  All 25 loader tests, the explicit backfill rerun, and release build pass.
  Release 6c4ae765 on 53025 matches Go in all 15 live cases. Six stages of
  cross-engine writes, Go ADD COLUMN and Rust index backfill pass Go
  ADMIN CHECK TABLE. No performance measurement is attributed to this binary.
  Before 13b8d343 is preserved at
  `/private/tmp/tidb-loaded-hidden-before-W6dZKN/tidb-server` and remains on 53024.
- [ ] Remove repeated planning metadata copies. The mixed TPC-C profile
  identified `predicate_push_down::bindings` and `physical_column_origin`.
  Go `ExtractOnCondition` reads bound column `RetType`; Rust rebuilt full
  column lists for relation names and for each base column's nullability.
  `TableEntry::columns` now borrows visible metadata; owned consumers clone
  explicitly. Predicate binding resolves once, with view origin handling
  retained, and relation-side classification reads only names. Five-sample
  isolated medians for 16/64/256 columns fall from 70,449/604,925/9,392,411 ns
  to 8,479/13,633/32,335 ns. This is not yet mixed-workload evidence.
  Origin validation found a separate existing omission: view/derived output
  tracing skipped the top-level outer join. A failing test (Some(false)
  versus Some(true), `/private/tmp/tidb-predicate-metadata-origin-red.log`)
  now passes with root and nested joins using the same null-extension
  boundary. Thirteen predicate, three column, sixteen join, fifteen
  projection, and twenty session-view tests pass. A `view::` executor filter
  selected zero tests and is not counted. Release 13b8d343 is on port 53024
  (session 32233); before 3e8014a3 remains on 53023 and is preserved at
  `/private/tmp/tidb-predicate-metadata-before-ymdfD9/tidb-server`.
  Historical comparison had fourteen of fifteen cases matching Go on each Rust
  release; both Rust releases expose the same hidden expression-index column
  through SELECT *. Expanded qualified/unqualified/ambiguous origin cases
  pass, as do all nine origin-module tests (76 distinct scoped tests total).
  Full serial comparison session 63913 completed: sysbench median TPS
  437.71 -> 444.03 (Go 476.97); TPC-C two-client 520.38 -> 551.09
  (Go 608.53); eight-client 709.07 -> 702.95 (Go 786.43).
  The medians are +1.44%, +5.90%, and -0.86%, respectively. All 180,000
  measured transactions and 6,000 warmups completed; this is mixed evidence,
  not a broad performance win. The separate 20,000-transaction profile and
  postchecks completed: all eleven conditions and all 30,000 customer
  balance/delivery checks pass. The later hidden-column follow-up below
  closes that correctness gap; the broader performance investigation remains.
  Exact benchmark command and samples are in
  `rust/benchmarks/predicate-metadata-baseline.json`.
- [x] Generalize the profiled dirty composite-key reader. The regression
  `dirty_common_handle_reads_share_the_remote_staged_merge` reproduces zero
  cop scans after staged updates/deletes. The shared cursor now carries all
  common-key parts independently of the visible projection, encodes canonical
  identity with prefix/collation/short-key rules, and reuses the integer path's
  merge. TopN and narrowing projections stay above UnionScan. All 23 remote,
  15 cursor and ten DML tests pass; release build e2b1853d runs on 53021
  (PID 21309, session 58800). All 30 live statement/count cases and 96 delivery
  samples match Go on both Rust releases. Dirty SUM median falls from
  2.746729 to 1.894896 ms (-31.0%); range UPDATE is approximately flat.
  Full isolated rotating comparison completed: sysbench -0.85%, TPC-C
  two-client +2.55%, eight-client +1.84% median throughput. All eleven
  consistency checks and all 30,000 customer balance/delivery checks pass.
  These modest mixed-workload changes are not statistical proof of a win.
  Commands, samples and limitations are
  in `rust/benchmarks/common-handle-reader-baseline.json`. Before binary is preserved at
  `/private/tmp/tidb-common-merge-before-i7BQfU/tidb-server` (b9c8b69d),
  still running on 53020. Red log: `/private/tmp/tidb-common-merge-before.log`.
- [ ] Move global-binding lookup to its proper node-owned cache. Completed:
  after-profile and real-TiKV reproduction show `has_global_binding_rows`
  reads transaction-wide staged-key count, not committed binding existence.
  Go applies a committed binding before/after unrelated writes; Rust ignores
  it before and applies it after three staged updates. A session-only on/off
  diagnostic attributes approximately 0.24 ms of dirty SUM latency to Rust's
  lookup path; this switch is not a proposed optimization. The embedded
  regression `global_bindings_do_not_depend_on_unrelated_staged_writes`
  reproduces the intended failure: dirty lookup passes, then clean lookup
  reports 0 instead of 1. Log: `/private/tmp/tidb-binding-visibility-table-red.log`
  (session 77863 terminal 101). Node cache integration now passes the expanded
  regression with real transaction-control routing: clean/dirty reads,
  uncommitted disable, rollback, committed disable/enable/delete and three
  repeated lookups per state (one user-table coprocessor scan, no binding SQL
  scan). Log: `/private/tmp/tidb-binding-cache-lifecycle-routed.log`.
  Nine cache unit tests pass in `/private/tmp/tidb-binding-cache-unit.log`,
  including latest-update tombstones, tied live versions, invalid SQL,
  oversize rejection and immutable pinned images. Cluster startup loads the
  image; a joined three-second worker refreshes it with an independent
  transaction; successful local record commits reload it. Standalone memory
  sessions retain their catalog path. Independent global-command writes now
  use a fresh internal pessimistic session, reusing existing SQL mutation,
  retry and commit machinery; caller validation remains on the caller.
  The original transaction-ownership regression is green in
  `/private/tmp/tidb-binding-command-third.log`. Stronger verification found
  two dependencies: missing bootstrap builtin lock row (red in
  `/private/tmp/tidb-binding-lock-bootstrap-red.log`) and unchanged UPDATEs
  dropping matched-row locks (live Go returns 1205 while original Rust
  a7eb7f20 lets a competing UPDATE through; tool session 15778). Bootstrap now seeds
  Go's row; UPDATE distinguishes filtered, unchanged and changed outcomes,
  passing unchanged row identities to the existing transaction lock owner.
  Expanded CREATE/SET/DROP ownership and lock/bootstrap checks pass:
  `/private/tmp/tidb-binding-writer-scoped.log` (two tests),
  `/private/tmp/tidb-unchanged-update-lock-routed.log` (one), and
  `/private/tmp/tidb-binding-bootstrap-green2.log` (seven).
  Candidate 3e8014a3 on 53023 passes real-TiKV independent CREATE persistence,
  bidirectional Go/Rust builtin-row contention, generic unchanged UPDATE
  contention, and all thirty existing SQL result/count cases. Logs:
  `/private/tmp/tidb-binding-writer-live-ownership.log`,
  `/private/tmp/tidb-binding-writer-live-lock.log`, and
  `/private/tmp/tidb-binding-writer-live-correctness.log`.
  This candidate has now completed the rotating comparison below.
  The added internal-writer failure test passes: a real duplicate-key error
  after an earlier write leaves zero committed rows and preserves SQLSTATE;
  an injected post-commit refresh failure leaves one committed operation,
  one callback invocation and no replay. Log:
  `/private/tmp/tidb-binding-writer-failure.log` (one test, session 90588 zero).
  Full rotating sysbench/TPC-C remeasurement completed in session 92600
  (exit zero): before e2b1853d/53021, after 3e8014a3/53023, Go/45000.
  Sysbench median TPS is 442.89/445.99/485.68; TPC-C two-client TPS is
  484.55/503.49/563.01 and eight-client TPS is 739.44/751.59/829.78.
  All 180,000 measured TPC-C transactions and 6,000 warmups completed.
  No build or profile overlapped it. Logs `/private/tmp/tidb-binding-writer-sysbench.log` and
  `/private/tmp/tidb-binding-writer-tpcc.log`; artifact directories have the
  same prefixes plus `-comparison`. Media analysis and indexing are active,
  so small timing differences remain noisy observations rather than proof.
  After a separate 20,000-transaction profile, all eleven TPC-C consistency
  checks and all 30,000 customer balance/delivery-count checks passed
  (session 33777, exit zero). The full record is in
  `rust/benchmarks/binding-cache-baseline.json` under `writer_followup`.
  Remaining: truthful cache/table status counts, peer refresh and
  prepared/failure/shutdown coverage, and remaining workload validation.
  Release a7eb7f20 on 53022 passes real-TiKV peer binding visibility (Go's
  binding appears after 1.01 seconds, both clean/dirty reads match) and all
  thirty existing result/count cases. Ninety-six rolled-back delivery
  samples match Go: SUM 1.6776875 -> 1.400375 ms (-16.5%); dirty order read
  0.4802085 -> 0.1717705 ms (-64.2%). Sysbench median 442.906 -> 450.723 TPS
  (+1.8%) is small compared with spread and observed host indexing activity.
  Evidence and raw samples are in `rust/benchmarks/binding-cache-baseline.json`.
  Fixed-count TPC-C completed all 180,000 measured transactions and 6,000
  warmups without reported errors. Medians: two clients 518.330 -> 546.631
  TPS (+5.5%); eight clients 700.959 -> 764.964 TPS (+9.1%). All eleven
  consistency conditions pass; 30,000 customers have zero incorrect balances
  and delivery counts. Host activity/data drift limits these measurements.
  The independent-command probe now confirms the next root gap from committed
  records: Go preserves one binding during/after caller rollback, Rust zero.
  Log: `/private/tmp/tidb-binding-cache-command-storage.log`. Its point-get
  binding flag is zero on both and is not used as persistence evidence.
- [x] Remove the auto-ID counter's arbitrary 64-byte transaction bound.
  The binding regression initially stopped before matching: system metadata
  allocation needed 68 bytes. The bound now includes the actual counter-key
  bytes and maximum signed decimal encoding, preserving allocator retries.
  `system_table_hidden_ids_use_the_full_counter_key` fails before and passes
  after (logs `/private/tmp/tidb-system-counter-before.log` and `-after.log`).
  The measured server 53021 predates this small source change; do not attribute
  its benchmark results to the allocator correction. The existing embedded
  rebase test and all five allocator unit tests pass (session 84376 terminal
  zero; `/private/tmp/tidb-system-counter-rebase.log` and `-unit.log`).
  Related hardcoded bounds in `cluster_sequence.rs` remain unaudited beyond
  source discovery and must be checked with sequence-specific regressions.
- [x] Reproduce over real TiKV with Go and Rust using two connections.
- [x] Identify missing executor locking and inspect Go `SelectLockExec.Next`.
- [x] Add the regression to the existing coprocessor-backed session suite.
- [x] Run and record the in-tree failing regression: actual row 1, expected row 2;
  one test failed in 0.26 seconds after compilation.
- [x] Add `select_lock.rs`: a pass-through executor extracts selected physical
  identities and publishes deduplicated keys at EOF; ordinary readers are
  unaffected because the operator is not installed on them.
- [ ] Preserve record identities through the locking query's executor plan.
  Completed: bind record-key expressions from base-table references and source
  column offsets; install on the non-aggregate source above its SQL window,
  including the embedded index-lookup projection boundary; add handle demand
  to leaf/output pruning and keep hidden handles outside wildcard output.
  Completed: filtered aggregate inputs now pass through SelectLock before
  aggregate input projections and pushdown; SUM contention has red/green proof.
  Remaining: verify pruning across all plan shapes, grouped/derived aggregates,
  derived relations, and the full validation matrix.
- [ ] Collect selected keys and connect them to statement locking and replay.
  Completed: optional channel in StmtContext/Session, owner-scoped lifetime in
  `with_bound_statement`, drain into `lock_pessimistic_statement_keys`, and
  attempt reset, and fresh locking-read timestamps. Remaining: complete
  planner producer wiring across all shapes and explicit lock-wait modes.
- [ ] Verify filters, LIMIT, joins, partition identities, lock wait modes,
  prepared execution, rollback, and unchanged ordinary-read behavior.
- [x] Repeat real TiKV concurrency checks on the six core shapes and TPC-C
  consistency checks after 30,000 transactions at two/eight-client contention.
  This closes the observed corruption reproduction, not the full locking matrix.
- [ ] Resume controlled performance comparisons; finish the remaining locking
  query shapes and Ready validation before broad completion claims.
- [x] Attribute coprocessor waiting with TiKV execution details and an identical
  Go/Rust locking read. The dominant delay is shared TiKV version scanning,
  not evidence of a Rust transport stall.
- [x] Remove serial point requests from batch UPDATE/DELETE record fetching.
  Source comparison, profile, red/green storage-count test, ten DML checks,
  uninstrumented comparisons and post-run consistency checks are recorded.
  Mixed-workload gains are modest; this does not complete the broad goal.
- [x] Route write-range record fetching through the shared table reader.
  Completed: profile ten-range, 95-row rolled-back updates; 1,271 SQL-worker
  samples wait in local snapshot scans. Compare Go buildUpdate's SelectPlan
  with Rust's local-only range fetch. Reproduce zero coprocessor scans in
  `write_range_reader_preserves_record_identity_and_staged_rows`, then pass
  that regression for hidden, integer and composite handles using the remote
  reader and shared keyed staged merge. Completed: virtual-column regression,
  21 remote-scan, 14 cursor and 10 DML tests; uninstrumented server build.
  The real TiKV delivery diagnostic matches affected counts and rows across
  96 samples; median range UPDATE falls from 1.9058335 to 1.3778335 ms (27.7%).
  Three rotating rounds: sysbench 447.11 -> 446.61 TPS (-0.11%); TPC-C
  two-client 516.34 -> 552.35 (+6.97%), eight-client 711.41 -> 736.13
  (+3.47%). All 180,000 measured transactions plus 6,000 warmups completed
  without rejected runs. All eleven standard consistency conditions pass;
  30,000 customers have zero balance or delivery-count mismatches. Samples
  vary and share evolving data, so this is not statistical proof or broad
  goal completion. Remaining: the discovered correctness gaps and Ready.
  Evidence:
  `rust/benchmarks/write-range-reader-baseline.json`.
- [x] Avoid aggregate worker setup for exhausted one-batch inputs, retaining
  the same state/output implementation and multi-batch parallel execution.
  Unit/server checks pass; controlled performance and broad validation remain.
- [x] Replace macOS subprocess RSS sampling with native resident-byte reads;
  reproduce before, pass scoped tests, compare byte units/cost, verify live
  removal, and record both workload gains and losses without a broad speedup claim.
- [ ] Reduce the remaining SQL-path overhead using current profiles and
  source comparisons; repeat controlled sysbench/TPC-C at both concurrencies.
- [x] Reproduce and correct the unsigned staged-merge and generated-column
  failures in the range-reader matrix. The shared merge now uses the reader's
  unsigned value order. The wide loader rebuilds generated metadata; table
  readers and lookup probes share physical dependency projection and virtual
  evaluation through RowDecoder. Stored values, synthetic heap identity and
  the direct chunk path for ordinary rows remain intact. Forced-index LIMIT
  coverage also exposed stale handle positions after column pruning; remap
  those positions with the projected scope instead of adding a virtual-only
  exception. Red logs and exact commands are in the baseline's
  `correctness_followup` section. Passed: 22 remote, six planner-property,
  14 cursor and 24 catalog tests. The b9c8b69d release on 53020 matches Go
  on all 26 live statement results/counts and all 96 rolled-back delivery
  samples. This is scoped correctness evidence, not broad Ready completion.
- [x] Verify the corrected release's mixed-workload performance. Session
  58549 completed nine sysbench and eighteen TPC-C samples against 53015
  and Go 45000, with zero rejected runs. Before/after/Go median TPS:
  sysbench 436.04/435.94/477.75; two-client TPC-C 523.37/539.39/623.42;
  eight-client TPC-C 727.13/728.54/843.03. The changes are -0.02%, +3.06%
  and +0.19%, with mixed individual rounds, not strong incremental speedup
  evidence. Session 69237 passed all eleven consistency checks and all
  30,000 customer balance/delivery-count checks. Full samples, commands and
  postchecks are in `correctness_followup.full_workloads` in the baseline.
- [x] Profile the remaining dirty aggregate on the corrected release,
  after all mixed workloads finished. Stage the upstream order-line write,
  repeat its grouped read, and roll back. The 20-second probe completed
  8,391 equal-result reads over 107 staged rows. A five-second thread-state
  sample records 1,826 of 3,485 SQL-thread observations waiting under
  `TableScanExec.open -> open_local_cursor -> row_cursor_with_decoder ->
  SessionSnapshot.scan`; only 59 observations enter HashAgg execution.
  This is wait attribution, not CPU-time measurement. Next extend the shared
  keyed merge to composite handles, with projected identity, collation,
  prefix, descending, tombstone and staged-replacement regression coverage
  before removing the dirty-common-handle refusal.
- [x] Repair transaction-boundary dirty-table lifetime after reproducing the
  direct-control BEGIN path; verify clean coprocessor reads resume without
  losing read-your-own-writes or changing another live transaction's marks.

## Surprises & Discoveries


TPC-C randomness has two independent time-seeded owners. Fixing only worker
seeds would leave process-level nonuniform constants different. The new
fresh-process test fails before the opt-in driver patch and passes after.
The mixed comparison itself has not started: safety review requires explicit
approval for persistent writes to the shared fixture. Sources and exact
evidence are in rust/benchmarks/tpcc-input-seed-baseline.json.

Rustls 0.23.42 Stream implements vectored writes but StreamOwned forwards only
scalar writes. A borrowed Stream preserves the same connection/socket owner
while admitting header and body before I/O. The old implementation accepting
only the first slice is legal Write behavior, not lost data; the performance
problem is the extra transport/record boundary. Both old-code regressions
failed at runtime. Correcting the post-change ErrorKind conversion preceded
34 passing scoped checks.

The later whole-workload CPU intervals do not support sustained high CPU from
each older benchmark server: 34 retained servers together used 12.70% of one
core during sysbench and 15.27% during TPC-C. These are complete-run process
counters, unlike the earlier instantaneous ps observations. Matching the
sysbench flag --mysql-ssl=off still produces rustls stack frames, so transport
mode must not be inferred from the flag. Neither sampled run is a timing
baseline. Sources: /private/tmp/tidb-catalog-cpu-{sysbench,tpcc}.json.
The catalog comparison's host CPU snapshot showed TiKV at 830.3% and several
retained older Rust servers at 16-18% each while TPC-C ran. After timing ended,
a 15-second baseline-server sample was mostly waits; a subsequent 66.451-second
cumulative-CPU interval measured about 1.2% per observed Rust server and
65.6% TiKV. This is not evidence of a sustained idle busy loop or proof that
retained-server load caused the varying results. Inspect under-load interval
counters before attributing it. User approval to stop older task-owned servers
was requested asynchronously; no server has been stopped.

The separately launched sampler missed its workload because approval latency
exceeded the workload duration. The corrected capture launches workload and
sampler in one shell and verifies timestamps before interpreting stacks.


The statement-lock live probe initially assumed a failed duplicate-key UPDATE
would release its newly acquired row lock. Go disproved that assumption before
Rust ran. The corrected observational probe confirms the mismatch predates
handoff elision on both integer and clustered composite handles. Go's source
provides two concrete investigation points: `optimizeDupKeyCheckForUpdate`
(`pkg/executor/update.go:714`) postpones pessimistic UPDATE assertions until
locking, and pinned client-go `KVTxn.exitAggressiveLockingIfInapplicable`
(`txnkv/transaction/txn.go:1355`) promotes current fair locks to normal locks
when the input request contains multiple keys, BEFORE held-key filtering.
Rust's worker filters held keys before acquisition and the coordinator chooses
ForceLock using the remaining key count; the session releases every newly
acquired statement key on failure. Trace assertion timing and lock-mode
transitions together before correcting this gap. Do not infer equivalence
from the two Rust binaries matching each other.

The metadata optimization's live Go/before/after comparison matches fourteen
of fifteen result cases, and before/after are identical. Both Rust releases
expose a Go-created expression-index hidden column in `SELECT *`; the probe
remains failing rather than excluding that case. Source tracing points to
`cluster_session.rs`: loaded columns are all passed to `KvTable::with_storage`,
whose hidden-column count starts at zero. No correction is claimed yet.
The probe dropped only its own temporary databases. Its full report is
`/private/tmp/tidb-predicate-metadata-live-all.log`. View-origin null extension
has a separate red/green regression; this does not claim all view field
metadata is correct. In particular, stored field flags and origin-derived
nullability still need a broader comparison.

The independent binding writer exposed two real prerequisites: cluster
bootstrap omitted Go's builtin lock row, and generic UPDATE discarded the
distinction between a rejected row and an unchanged matched row. Go
`executor/write.go` locks the latter without writing it. Live comparison
confirmed Go error 1205 versus old Rust success for a competing writer;
the new release holds the lock against Go and releases it on rollback.
An initial NOWAIT-based probe was invalid evidence because that separate
lock mode currently returns rows without enforcing its lock; the replacement
uses ordinary competing UPDATEs. The embedded timeout regression lasts about
20 seconds despite setting innodb_lock_wait_timeout=1, so per-session timeout
propagation and NOWAIT remain explicit gaps, not claimed fixes.

The common-handle after profile has no `SessionSnapshot.scan` in the grouped
read. Of 3,482 SQL-thread observations, 956 wait in its coprocessor stream;
474 separately enter `load_global_bindings`, including 334 waiting on that
table's coprocessor read. Source tracing reveals a correctness error behind
the repeated read: `has_global_binding_rows -> KvTable.len ->
ClusterTableStorage.key_count -> buffer.len` counts the transaction-wide
write buffer. The real-cluster regression in
`/private/tmp/tidb-binding-visibility-multiwrite-probe.log` proves matching
changes when three unrelated rows are updated, without changing bind_info.
The diagnostic creates and removes only its isolated database and binding;
DROP leaves Go's normal binding tombstone, which its GC owns.

The new real-TiKV range-write matrix found two failures in BOTH the preserved
before binary (53014) and candidate (53015), with identical Rust outputs.
After staged writes, an unsigned primary-key SELECT returns five rows instead
of Go's three; high-key snapshot rows are not hidden by the staged records.
The remote cursor concatenated the unsigned value-order groups but compared
raw signed-order record keys across their boundary. Its red/green regression
now covers ascending, descending and capped reads. A Go-created virtual-column table also lost its
virtual values in both Rust binaries (UPDATE matches zero rather than one).
`cluster_session.rs::cluster_table` is the wide loader and previously assigned
`generated: None`, citing a rejection in the separate bounded reader. Its own
entrypoint explicitly bypasses that bounded reader. Go
`pkg/table/tables/tables.go::TableFromMetaWithCollate` rebuilds each generated expression
from metadata with `buildGeneratedExpr`. The Rust loader now follows that
boundary, and shared row decoding evaluates virtual values above both remote
table reads and index probes. The final forced-index LIMIT regression also
requires `IndexAccessOrder.remap_columns` to remap handle coverage: physical
slot zero otherwise falsely covers a non-key column projected into slot zero.
Go resolves handles against each reader's current schema and refuses to sink
LIMIT beneath a table-side Selection. Heap, collated common
primary key and partition cases match Go. All test DML rolls back; the seed
database is `perf_range_reader_check_20260907`. Full evidence remains in
`/private/tmp/tidb-write-range-correctness-all.log`; final 26-statement Go
equivalence is `/private/tmp/tidb-index-coverage-correctness.log`. These are
scoped regressions, not proof of all SQL or locking semantics.

Go on local port 45000 blocks the competing prefix-key locking read and returns
row 2 after row 1 is deleted. Rust on port 53005 returns row 1 before the first
transaction commits. The standalone reproduction is
`/private/tmp/tidb-locking-prefix-probe.py`. It uses isolated tables in
`perf_sysbench`; the inconsistent `perf_tpcc` dataset remains untouched.

Rust's `driver.rs` emits a SelectLock plan label but no executor. Point-key
prelocking in `access_path.rs` hides this gap for some equality queries.
`StatementReadKeys` in `cluster_storage.rs` records storage reads and disables
remote scans when enabled. It does not identify rows surviving SQL operators.
It is not a correct replacement for the missing executor.

After installing the operator on the ordered range, the selected-key handoff
was observed and the contender replayed, yet the concurrency regression still
returned row 1. `SessionSnapshot::start_ts()` returned the original transaction
timestamp even when `read_ts` held the advanced retry timestamp. Byte-level
Get/BatchGet/Scan requests already carry the override; coprocessor scans obtain
their timestamp through this trait method and therefore replayed an old view.
The correction belongs in the snapshot abstraction, independently of locking
operator placement. Temporary handoff diagnostics were removed after tracing.

The focused `coprocessor_snapshot_timestamp_follows_the_statement_retry` test
failed with timestamp 10 instead of 20 before changing
`SessionSnapshot::start_ts` to return `read_ts.unwrap_or(start_ts)`, then passed.
The transaction's stored original timestamp is asserted unchanged.

The unprojected-handle regression failed on a heap table with
`locking plan discarded record identity column lock_heap._tidb_rowid`.
LeafDemand now adds clustered-key columns or the heap handle as required
operator inputs. The existing wildcard identity-projection shortcut ignored
hidden scope columns; it now requires visible and physical widths to agree.
The first test fixture also reached the embedded store's unimplemented TopN
processor. The focused fixture now has one row and no sort, preserving the
implicit-handle requirement; production pushdown was not disabled. Sorted
locking queries still require real TiKV validation.

Live Go EXPLAIN confirmed the ordered prefix-range shape is Projection over
SelectLock over root Limit over TableReader over cop Limit/RangeScan. This
holds for both ascending and descending order. The selected-key operator must
therefore sit above the SQL window and below the visible projection.

Go's `SUM(id) ... FOR UPDATE` plan is StreamAgg over Projection over SelectLock
over TableReader: aggregate queries lock their underlying rows. An outer-join
query with ORDER BY/LIMIT has SelectLock above TopN above HashJoin, with both
inputs' handle columns retained. Do not bypass aggregate locking or place one
unconditional wrapper around every final result.

## Decision Log


- Decision: Tighten input reproducibility and include a repeated unchanged
  baseline before attributing the packet-write regression. Stop the mixed
  run at the explicit fixture-write approval boundary, and use a local
  construction-cost probe for unaffected diagnostic work.
  Rationale: All six earlier TPC-C pairs were slower. Their changing inputs
  limit attribution but do not justify ignoring negative evidence. A local
  probe can size the next source-backed lead without mutating shared data.
  Date/Author: 2026-09-07 / Codex.

- Decision: Coalesce one uncompressed frame through borrowed IoSlices and
  forward ClientStream's native TCP/TLS vectored operation.
  Rationale: Go supplies a contiguous header/body frame to its writer. Routing
  a fragmented logical packet through write_packets would change completed-
  frame sequence state after a later failure. Shared frame writing removes
  duplicate scalar calls without a new buffer owner or compression rewrite.
  Rustls still owns record buffering; this is not a zero-allocation claim.
  Date/Author: 2026-09-07 / Codex.

- Decision: Borrow static FieldType metadata inside collation derivation;
  retain the existing owned NULL fallback for nodes without a static type.
  Rationale: Every caller only reads the type; Go column/scalar type access
  returns existing metadata. Cow removes two string copies per typed read
  without introducing a cache, changed collation rules or stale ownership.
  Parameter-marker context-dependent typing is unchanged.
  Date/Author: 2026-09-07 / Codex.
- Decision: extend catalog snapshot sharing through the schema-name map and
  individual database entries, retaining the existing table-entry boundary.
  Rationale: profiles locate repeated table-name-map cloning during snapshots;
  Go retains infoschema for a transaction and copies only the database being
  changed. A runtime allocation-identity regression fails before and passes
  after; 141 scoped tests and live Go comparisons preserve snapshot isolation.
  Whole-workload timing remains mixed, so the structural change alone is not
  a throughput verdict. Date/Author: 2026-09-07, Codex.

- Decision: replace aggregate row-count cutoffs with whole-candidate costing,
  not new thresholds or fixture-specific plan exceptions.
  Rationale: `driver/agg_select.rs` uses 10,000 rows to select global stream
  aggregation, 10 rows to admit grouped partial aggregation, and default
  cost settings rather than the statement environment. Go enumerates root
  and cop alternatives in `physical_stream_agg.go:getStreamAggs` and
  `physical_hash_agg.go:getHashAggs`, splits admitted aggregates in
  `base_physical_agg.go:NewPartialAggregate`, and costs the complete trees.
  The existing Rust Candidate coster already models readers, stream/hash
  operators, task factors and schema widths. Select before mutating executor
  schemas; keep descriptor admission, ordering, DISTINCT/AVG decomposition,
  dirty-row merging and SelectLock boundaries intact. Validate real Go plans
  under changed session cost factors and cardinalities, including multiple
  global functions, before changing expectations of the six existing failures.
  Date/Author: 2026-09-07, Codex.

- Decision: share the cached immutable CostEnv across statement contexts.
  Rationale: eliminate deep copies and replacement defaults at their owner,
  without changing cost-factor representation or formulas. Copy-on-write
  preserves retained snapshots across settings changes.
  Date/Author: 2026-09-07, Codex.


Decision: inject the real statement memory into the common context initializer
instead of constructing standalone trackers and immediately replacing them.
Rationale: the current profile identifies context setup/teardown; the injected
owner preserves session quota, cancellation, arbitration and retained-result
lifetimes without pooling or reusing a prior statement's trackers. Standalone
constructors retain the shipped quota. Remove the obsolete replacement API.
Date/Author: 2026-09-07, Codex.

Decision: optimize column metadata ownership rather than adding plan-cache
gates. Go's logical data source carries column metadata and predicate
derivation reads its bound types. Rust's repeated full-list clones disappear
by borrowing read-only schemas and separating namespace collection from type
resolution. Retain one owned binding image where predicates need mutable
view-derived type flags. Share the same join-origin/null-extension boundary
between root FROM and nested joins, with failing/passing evidence.
Rationale: the mixed-workload profile identifies this path, and isolated
width measurements show quadratic copying. Full-workload improvement remains
to be measured; the unrelated hidden-column mismatch remains visible.
Date/Author: 2026-09-07, Codex.

Decision: remove per-statement global binding storage reads through a real
node-owned binding cache, following Go `Domain.InitBindingHandle`,
`globalBindHandleWorkerLoop`, and `bindingCacheUpdater.LoadFromStorageToCache`.
Reuse the existing binding matching/index/cost machinery where possible.
The cache must read committed storage with its own transaction, be shared
across sessions, refresh peer changes on the binding lease, and publish
local CREATE/DROP/status changes at the successful commit boundary. Wire
startup and shutdown through existing node reload ownership in
`cluster_session_node/boot.rs` and the unistore stack. Do not memoize the
current user's snapshot, change the row-count threshold, disable bindings,
or add a benchmark-specific branch. Verify clean/dirty/rollback visibility,
session precedence, disabled/deleted rows, fuzzy matching, peer updates,
prepared planning, failed refresh retention, and loader shutdown. This is
an optimization/correctness increment, not a whole pkg/bindinfo completion.
Date/Author: 2026-09-07, Codex.

Decision: global binding command persistence needs its own pessimistic
transaction, serialized against Go peers by the shared builtin binding row
(`pkg/bindinfo/binding_handle.go: LockBindInfoSQL`), followed by cache reload.
Keep caller-side statement/hint validation and user transaction state, but
move storage mutation ownership out of the user's buffer. A process-local
mutex or committing the caller's transaction cannot implement this contract.
The live storage probe proves the gap independently of planner hit flags.
Existing system-row encoding and auto-ID ownership should be reused rather
than introducing a second SQL/value encoding implementation.
Date/Author: 2026-09-07, Codex.

Decision: execute the existing Go-equivalent `LOCK_BIND_INFO_SQL`, not a
binding-specific substitute. Go `executor/write.go` locks a matched unchanged
row even when `LockUnchangedKeys` is off (that setting controls additional
unique-key locks). Rust's UPDATE previously conflated filtered and unchanged
rows as `None`; explicit outcomes let the normal transaction owner lock a
matched no-op without producing fake writes or locking rejected rows.
Date/Author: 2026-09-07, Codex.

Decision: use the existing record-handle order normalization for the remote
staged merge, with an allocation-free byte iterator. Keep the raw physical
record identity unchanged. This matches Go UnionScan's typed HandleCols
comparison even when unsigned groups cross 2^63. Do not disable coprocessor
reads or sort/deduplicate the final output to hide old versions.
Date/Author: 2026-09-07, Codex.

Decision: reconstruct generated expressions at `cluster_table`, the wide
catalog's metadata boundary, preserving virtual/stored metadata and named
errors. Reuse the expression builder; do not synthesize query-specific values
or rely on admission in the unrelated bounded reader.
Date/Author: 2026-09-07, Codex.

Decision: retain record identities in the existing remote cursor's staged
merge and use that reader for full-row range fetches, as Go UPDATE builds its
source SelectPlan. Project physical columns and evaluate virtual columns with
the existing expression evaluator. Keep the backend's existing unsupported
shape admission; do not add a parallel snapshot-RPC scheduler or retry after
partial output. Date/Author: 2026-09-07, Codex.

Decision: implement selected-row identity and locking at the executor/session
boundary, not by collecting all storage reads. Rationale: filters and LIMIT
discard fetched rows, and joins need identities from multiple base tables.
Preserve remote scan capability and the existing point-lock optimization.
Date/Author: 2026-09-07, Codex.

Decision: use the existing pessimistic statement retry loop and held-lock
cleanup. Rationale: a new independent retry loop would duplicate timestamp,
savepoint, conflict, and rollback ownership. Date/Author: 2026-09-07, Codex.

## Context and Orientation


`pkg/executor/select.go` implements Go's SelectLockExec: retain base-table row
handles from produced chunks, encode physical record keys, then call
`doLockKeys`. A handle identifies a row; common handles encode composite primary
keys, and partitioned rows require their physical table identifier.

Rust `rust/crates/tidb-executor/src/driver.rs` builds the query operators.
`driver/from.rs::FromScope` tracks column positions, and `driver/leaf_demand.rs`
controls which columns scans retain. `cluster_session_node/mod.rs` in
`tidb-server` owns `attempt_statement_inner`, the statement savepoint, and
`lock_pessimistic_statement_keys`. The latter currently sees only writes.
`tidb-exec/src/cluster_table_storage.rs` provides `LockKeysOutcome`, including
the new timestamp needed to replay a statement after a lock conflict.

## Plan of Work


Milestone 1 is the executable regression in
`rust/crates/tidb-server/src/cluster_session_node/tests/unistore_cop.rs` named
`a_locking_range_reselects_after_the_first_row_is_deleted`. Its two sessions
select the first composite-key row; the first deletes it and commits. The
second must return row 2. Record red evidence before production changes.

Milestone 2 adds an actual SelectLock executor and a statement-scoped selected
key collector. Add hidden handle and physical-table columns to leaf demand
where required, propagate them through the operator plan, and exclude them
from visible projection/wildcards. Follow Go's placement relative to LIMIT,
sorting, projection, joins, and aggregation rather than inferring placement
from the current trace label. Locking must not require a second SQL scan.

Milestone 3 binds the collector to each statement attempt in the session and
passes its keys to the existing transaction lock operation. Determine locking
read timestamps from the statement, not from whether point prelocking produced
keys. Clear attempt-local keys on retry; retain and release acquired locks by
the existing statement-success/failure contract. Honor NOWAIT and WAIT modes
through the transaction request rather than only the AST classifier.

Milestone 4 validates the complete behavior and removes stale claims about
storage-key collection. Keep earlier uncommitted performance changes intact.

## Concrete Steps


From `rust/`, run Cargo commands serially with 12 jobs:

    cargo test --offline --locked --release -j12 -p tidb-server --lib a_locking_range_reselects_after_the_first_row_is_deleted
    cargo test --offline --locked --release -j12 -p tidb-server --lib cluster_session_node::tests::unistore_cop

Use the WIP verification profile while implementing. Run affected executor
tests as added, then the Ready profile including `make -j12 lint` from repository
root before completion. Do not claim the overall optimization goal complete
from this locking regression alone.

## Validation and Acceptance


The regression must fail before and pass after implementation. Verify that
filtered-out and LIMIT-discarded rows remain writable by a competitor, selected
rows block until commit/rollback, and a conflict causes a fresh query result.
Cover integer, composite, nonclustered, and partitioned keys; joins must lock
their real input rows without inventing keys for null-extended rows. Ordinary
reads must retain repeatable-read behavior and remote scans.

Repeat the live two-client check against a rebuilt Rust server and Go. Use a
fresh isolated TPC-C database for post-fix consistency/performance evidence,
while preserving the old inconsistent database for diagnosis. Inspect the
checker's reported failures, not just its exit code.

## Idempotence and Recovery


Tests create their own embedded store. Live probes use dedicated table names;
roll back open diagnostic transactions. Do not reset the worktree or overwrite
other contributors' changes. A failed statement must release only locks it
acquired, not locks retained by earlier successful statements.

## Interfaces and Dependencies


Use existing `Executor` chunks and the statement context to publish selected
record keys. The session remains the owner of transaction RPCs and retries;
the executor must not depend on `tidb-server`. Reuse `LockKeysOutcome` rather
than representing lock conflict as successful partial query output.

`tidb-executor::select_lock` now defines `SelectedLockKeys::take()` for draining
an attempt, `SelectedRecordKey` for a physical record-key expression, and
`SelectLockExec::new(child, expressions, selected)` for the pass-through
operator. A key expression returns `None` only for a null-extended joined row;
missing/pruned identity is a planning error, not a reason to skip a lock.

`StmtContext::{with_selected_lock_keys, selected_lock_keys}` carries the
optional channel into both query and DML contexts. `Session` exposes
`set_selected_lock_keys` and `take_selected_lock_keys` to its transaction owner.
The wide server creates the channel for explicit pessimistic statements,
clears it on teardown, and merges drained keys with the staged-write lock set.
Query results are already materialized inside `with_prelocked_statement` for
both wire protocols, so publication remains after the lock/retry decision.

## Outcomes & Retrospective


The input-seed implementation and its driver checks are verified, not a
throughput improvement. The new comparison is awaiting write approval and
has no live process. The prior packet/TLS regressions remain unresolved;
scoped correctness and deterministic input streams do not establish the
goal's required generic sysbench/TPC-C speedup.

The longer data-free repeated-baseline control completed 3.2 million commands
without errors. Single-frame PING improves (+8.29% plaintext, +2.73% TLS),
but TLS SELECT 1 remains slower in all four pairs against both unchanged
baseline observations. Its uncompressed result-batching and scalar TLS write
path were not changed by frame coalescing. This supports the local single-
frame mechanism while leaving wider regression attribution unresolved.
The in-memory context probe measures about 1.7-1.8 microseconds per context;
no new production refactor follows from that result alone.

Vectored frame writing passes 34 scoped tests and live Go comparisons over
both transport modes. Sysbench +1.55% has two slower pairs; TPC-C -8.70%/-1.69%
has all pairs slower. Fixed-command PING/SELECT probes remain mixed. All
consistency checks pass, including after separate profiling; the workload
does traverse the vectored path, but structural write counts and unpaired CPU
counters do not establish a throughput gain or regression cause. See
frame-write-baseline.json and the current handoff; the goal is not achieved.

Static expression type borrowing has a runtime red/green allocation test,
63 scoped checks and live Go collation/overlay/DISTINCT comparisons. Mixed
workload timing completed with sysbench -2.99% and two/eight-client TPC-C
+3.28%/-7.35%; all eight-client pairs are slower. All correctness postchecks
pass, including after a separate 20,000-transaction profile. The profile does
not establish the regression's cause. The CPU follow-up corrected the
retained-server hypothesis without stopping any server. No whole-workload
gain or readiness claim; see the current handoff and type-read-baseline.json.

Earlier release 701d7abb on 53050 extends catalog copy-on-write through schema
maps. Runtime regression, 141 scoped tests and live Go comparisons pass.
Mixed sysbench median is -2.29%; TPC-C +1.25%/+3.20% at two/eight clients,
all with mixed paired directions. Eleven consistency conditions and 30,000
customer checks pass. The preceding epoch-store change measured sysbench
-2.28% and TPC-C -2.97%/+12.70%; its eight-client pairs were all faster but
that does not establish a broad gain. The full goal remains active, with no
Ready/lint, whole-package completion, commit or push. See the current handoff
for retained binaries and the next controlled measurement boundary.

Previous release 9fc92d67 on 53032 shares immutable optimizer-cost snapshots.
Its native setup improves 26-40%, but completed mixed medians are sysbench
-0.31%, TPC-C two-client +1.50% and eight-client -3.12%; paired directions
remain mixed. Postcheck 5656 passes all eleven conditions and 30,000 customer
balances/delivery counts. Whole goal remains open. The next root gap is the
aggregate chooser's cutoff/default-policy logic, confirmed against live Go.

Previous release d886a36d on 53031 derives SQL-mode flags in the existing
variable snapshot, with 66 scoped tests and 30 live cases passing.
Native default-mode context setup falls 17-20%; expanded-mode setup falls
28-32%. Mixed comparison 71688 finished on a noisy host: sysbench +1.51%,
TPC-C two-client -3.35% and eight-client -4.15% medians. No broad speedup,
package-completion or readiness claim follows from these isolated results.

Release 23174d49 on 53030 corrects empty-restoration cache invalidation and
server-identity invalidation. Both regressions fail before and pass after;
83 scoped tests and 30 live cases pass. SELECT microbenchmark improves 2.31%.
Comparison 99577 is complete: sysbench -1.20%, TPC-C two-client -1.63% and
eight-client +11.30% medians, with mixed paired directions. All workload and
profile consistency checks pass. No Ready/lint, commit
or push. The preceding e4e6384e loop is complete in its baseline: initial
eight-client slowdown is retained, six-pair recheck is mixed, and all initial
and additional workload consistency checks pass. Do not call either release
a stable broad performance win from these measurements.

Earlier measured release 528adaa7 on 53028 uses direct memory ownership and
halves isolated context construction/drop cost. Fifty-three scoped tests and
the eleven-case live Go matrix pass, with all 24 Go index checks passing.
Mixed medians rise 1.99%/3.06%/6.95% for sysbench/TPC-C two/eight clients,
but inconsistent paired rounds and drift prevent a stable broad-speedup
claim. All 180,000 measured transactions and 6,000 warmups complete, followed
by eleven passing consistency conditions and zero balance/delivery-count
errors across 30,000 customers. All current build/test/probe/measurement and
postcheck handles are terminal. WIP; no Ready/lint, commit or push.

Earlier measured release 13b8d343 on 53024 eliminates quadratic
metadata copying in predicate binding and preserves outer-join null extension
when tracing projected origins. The isolated metadata path improves strongly,
but full-workload medians are mixed: sysbench +1.44%, TPC-C two-client +5.90%,
eight-client -0.86%. Do not equate a planning microbenchmark with workload
success. Seventy-six distinct scoped tests pass. The live matrix remains
14/15 against Go because both old and new Rust expose a hidden expression
index column through SELECT *. WIP only; no Ready/lint, commit or push.

The preceding release is 3e8014a3 on 53023 (session 74908).
It owns global binding mutations in an internal pessimistic session and uses
Go's shared-row UPDATE lock, now supported by generic unchanged-row locking.
Real Go/Rust contention works in both directions; independent persistence and
thirty SQL result/count cases pass. This candidate's measured median changes
against e2b1853d are +0.70% sysbench, +3.91% TPC-C two-client, and +1.64%
TPC-C eight-client. Host noise and evolving data prevent a strong performance
claim; Rust still trails Go. The previous a7eb7f20 observations remain
separately identified. WIP only.

Earlier measured release e2b1853d is on 53021 (PID 21309, session 58800);
preserve before b9c8b69d on 53020, earlier baseline 53015, and shared
Go/PD/TiKV. The dirty common-handle reader now shares the keyed staged merge.
The isolated grouped read improves 31.0% with equal results; full rotating
sysbench changes -0.85%, TPC-C +2.55%/+1.84% at two/eight clients. All
eleven post-run conditions and 30,000 customer balance/delivery checks pass.
These modest, drifting samples do not establish the broad performance goal.
Full reports and after-profile are in
`rust/benchmarks/common-handle-reader-baseline.json`; previous range-read
results remain separately identified in `write-range-reader-baseline.json`.

The after-profile completed 18,335 equal-result grouped reads in 30 seconds
over 101 staged rows, always rolling back. Reproduce with
`python3 /private/tmp/tidb-dirty-sum-profile.py --port 53021 --seconds 30`;
capture a validated PID with the Instruments `sample-quick.sh` script.
Workload 30735 and sample 37549 are terminal. The next source-backed root
fix is binding-cache ownership, not further unmeasured cursor micro-tuning.
Go owns refreshed bindings outside user transactions. Rust uses a misleading
write-buffer-size gate and then rereads the binding table during planning.
Live Go/old-Rust clean/dirty matching differs. The new node cache passes the
expanded embedded regression. Its first rollback extension incorrectly used
the query-only helper for transaction control; the corrected fixture routes
BEGIN/COMMIT/ROLLBACK through `QuerySession::control_transaction`, just as the
wire protocol does. No production rollback change was needed. Ready/lint and the remaining broad locking matrix are unverified;
no commit or push. The goal remains active and this is WIP evidence.

Reproduction and source comparison completed; implementation and validation
remain open. No locking fix or TPC-C performance improvement is claimed.

The new operator's three unit tests pass with
`cargo test --offline --locked --release -j12 -p tidb-executor --lib select_lock::tests`.
They cover LIMIT output/deduplication/EOF publication, failed extraction without
partial publication, and multiple record identities with absent joined rows
and replay. These are operator tests, not proof of planner or session wiring;
the operator tests alone do not establish end-to-end correctness.

Session-channel validation passed:
`cargo test --offline --locked --release -j12 -p tidb-session --lib selected_record_keys_reach_the_owner`.
The server compiled with
`cargo check --offline --locked --release -j12 -p tidb-server`, and the existing
`a_plain_read_is_not_answered_from_the_pessimistic_lock_cache` server unit test
passed. These establish the channel and unchanged plain-read behavior, not the
missing planner producer. For fresh locking-read timestamps, the next source
anchor is `pkg/sessiontxn/isolation/repeatable_read.go::getForUpdateTs` and
`updateForUpdateTS`; the Rust transaction worker currently exposes timestamp
advancement only as part of lock-conflict recovery.

With the non-aggregate planner producer installed and the snapshot timestamp
corrected, `cargo test --offline --locked --release -j12 -p tidb-server --lib
a_locking_range_reselects_after_the_first_row_is_deleted` passes (1 test,
0.27 seconds). The waiter now returns row 2. The current binary on port 53005
has not been rebuilt with these changes. Hidden handle retention, aggregates,
lock-wait modes, fresh first-attempt timestamps, and the broader live correctness
matrix remain required before TPC-C performance results are trustworthy.

The hidden-handle matrix now passes for heap, integer-key, and common-key
tables, including wildcard output, using
`cargo test --offline --locked --release -j12 -p tidb-server --lib
locking_queries_retain_unprojected_record_handles`. The original contention
test also passes after the demand changes.

Built with `cargo build --offline --locked --release -j12 -p tidb-server --bin
tidb-server` and started the new binary on port 53006 against PD 43379. Its
SHA256 is `ea8a1670e9d39a102c3154e32743125ae8c173b167f5ebda9f0a26990d43176a`.
`python3 /private/tmp/tidb-locking-prefix-probe.py 53006` now reports
`contender blocked True` and `second after deletion ((2,),)` for both Go 45000
and Rust 53006. Thus the original real-TiKV concurrency failure is corrected
on this query shape, not merely in the embedded-store test.

On the real TiKV heap table `perf_sysbench.lock_projection_live`, the sorted
locking read returns only `v=10`; its wildcard counterpart returns only
`id=1,v=10`. Both ran inside a rolled-back transaction. This verifies the
TopN/hidden-handle output path which the embedded backend cannot execute.
It does not yet verify every locking query shape or TPC-C consistency.

Fresh first-attempt locking reads now have an explicit `LockingRead` statement
shape. Range reads request `fresh_locking_snapshot` through their transaction
worker, which advances `for_update_ts`; successful point prelocking keeps its
existing timestamp optimization. Ordinary reads remain at BEGIN's snapshot.
The new `a_locking_range_sees_rows_committed_after_begin` regression was red
(empty locking result instead of the newly committed row), then passed. The
same test verifies that a later plain SELECT remains empty. Contention replay,
plain-read lock-cache isolation, and point-prelock regressions also pass in the
built server test harness. Port 53006 does not yet contain this timestamp change.

The shared contention fixture now covers ranges without ORDER BY/LIMIT too.
That new test failed with rows 1,2,3 instead of 2,3: `range_can_return_direct`
allowed the result to bypass the required root SelectLock operator. It now
requires the absence of a lock clause. This changes direct-return eligibility,
not remote scan capability or transaction retry policy.

`cargo test --offline --locked --release -j12 -p tidb-server --lib a_locking_range`
now passes all three range regressions. The built test harness also passes
the plain-read cache-isolation and point-prelock tests. Live port 53006 is
still the preceding build; fresh-first-read and direct-range changes have not
yet been exercised there. Projection-only cop paths, aggregates, explicit wait
modes, and remaining join/partition cases still require completion.

The ordered projection regression initially failed because column `w` of its
composite handle had been removed. `range_order_projection` now appends the
required handle columns to the existing SQL output prefix, retaining projection
pushdown and letting the root trim the extra columns. This exposed a separate
cursor bug: `TableScanExec::open_local_cursor` treated every full-width
projection as identity. Its focused regression requested [c,a,b] but received
[a,b,c]. The identity predicate now compares the actual offsets against the
identity sequence; the test also covers repeated offsets and true identity.
Projection changes now remap the scan's ordering metadata alongside its scope.

The duplicate projection case exposed a second decoder bug: moving the same
datum twice returned NULL on its second use. RowDecoder now computes last-use
metadata once per cursor; earlier duplicate outputs clone and the last output
moves the datum. Unique projections still move their values. The focused
`a_full_width_projection_is_not_necessarily_identity` executor test passes,
as do all four server `a_locking_range` tests and the existing hidden-handle,
plain-read cache-isolation, and folded point-lock tests. Commands remain WIP
(`cargo test --offline --locked --release -j12` with the relevant package and
test filter); no live server or performance baseline was rebuilt this turn.

The regression `a_locking_aggregate_reselects_its_input_rows` reuses the
two-session contention fixture with SUM(id): 6 before the deletion and 5 after
the blocked reader replays. Go's logical builder installs SelectLock after
WHERE and before aggregation (`logical_plan_builder.go`); Rust's aggregate
pipeline had no such operator. It failed with 6 instead of 5 before the change.
`build_aggregation` now installs SelectLock after filtering and before input
projections and aggregate pushdown negotiation, matching Go's ownership
boundary. The default executor table-access contract prevents aggregation
from being pushed through this required operator; ordinary nonlocking
aggregates retain their existing pushdown. The regression passes with
`cargo test --offline --locked --release -j12 -p tidb-server --lib
a_locking_aggregate_reselects_its_input_rows` (1 test, 0.26 seconds).
The stale plan-trace comment claiming storage-read key collection was removed.
Grouped/derived aggregate shapes and live aggregate contention remain unverified.

The broader WIP command `cargo test --offline --locked --release -j12 -p
tidb-server --lib cluster_session_node::tests::unistore_cop` reports 30 passed
and four failed. All locking tests pass. The failures are
`a_negative_bound_on_an_unsigned_column_follows_gos_rewrite`,
`an_index_read_and_a_table_scan_agree_over_unsigned_keys`,
`analyze_changes_the_plan_and_never_the_answer`, and
`every_shape_the_ddl_admits_the_loader_loads`. They reach explicit unsupported
bare-index or index Selection/Limit branches in `tidb-unistore/src/cophandler.rs`;
those branches are unchanged from HEAD. The negative-bound test also fails
when run alone. A pristine baseline suite was not rebuilt, so this evidence
identifies the rejecting backend boundary, not the history of every failure.
The suite is not green; do not hide these failures or disable index pushdown.

Rebuilt the current server with `cargo build --offline --locked --release -j12
-p tidb-server --bin tidb-server`. SHA256 is
`0cf5656920906e05c35985d48246e40a998cd5501d36d317674703bb18db306d`;
the binary runs on port 53007 against the existing PD 43379/TiKV cluster.
`python3 /private/tmp/tidb-locking-matrix.py 53007` passes all six checks on both
Go 45000 and Rust 53007: limited common-key range, unrestricted range, ordered
projection, SUM input contention, sorted heap row selection, and fresh locking
reads while plain reads remain repeatable. Each contention check asserts the
waiter blocks and replays to exclude a concurrently deleted row. Results are
in `/private/tmp/tidb-locking-matrix-results.jsonl`.

A fresh isolated `perf_tpcc_locking_v1` database was prepared through Go with
the pinned `/private/tmp/tidb-tpcc-tool-OY7S5G/go-tpc` binary:

    go-tpc tpcc prepare -H 127.0.0.1 -P 45000 -U root -D perf_tpcc_locking_v1 --warehouses 1 -T 12
    go-tpc tpcc run -H 127.0.0.1 -P 53007 -U root -D perf_tpcc_locking_v1 --warehouses 1 -T 2 --count 10000 --interval 10s
    go-tpc tpcc check -H 127.0.0.1 -P 45000 -U root -D perf_tpcc_locking_v1 --warehouses 1 -T 1

Preparation ran all 12 initial-state checks. The Rust run completed 10,000
transactions (393 Delivery, 4,510 NewOrder, 418 OrderStatus, 4,279 Payment,
400 StockLevel), with no reported errors. All 11 default post-run checks
passed through Go, including condition 3.3.2.10 which failed on the earlier
Rust build. Condition 3.3.2.11 is an initial-state count invariant and is
intentionally not part of the tool's default post-run checker. Logs are
`/private/tmp/tidb-tpcc-locking-{prepare,run,check}.log`. This is correctness
stress evidence, not a controlled performance comparison or broad completion.
The older inconsistent `perf_tpcc` database remains unchanged for diagnosis.

The subsequent eight-client run used the same `run` command with `-T 8
--count 20000`, and completed all 20,000 transactions without reported errors
(803 Delivery, 8,948 NewOrder, 790 OrderStatus, 8,642 Payment, 817 StockLevel).
The independent Go checker again passed all 11 post-run conditions. Logs are
`/private/tmp/tidb-tpcc-locking-run-eight.log` and
`/private/tmp/tidb-tpcc-locking-check-eight.log`.

Source inspection found that checkCondition10's scalar SUM can be NULL for a
customer with no delivered orders, making its inequality skip that customer.
An additional read-only Go query in
`/private/tmp/tidb-tpcc-locking-balance-check.sql` uses LEFT JOIN/COALESCE and
checks every customer balance and delivery counter. The delivery-count check
excludes initially delivered orders below 2101, matching the tool's load.go
initialization (those orders have zero amount; initial customer delivery count
is zero). Result: 30,000 customers, zero bad balances, zero bad delivery counts,
recorded in `/private/tmp/tidb-tpcc-locking-balance-check.tsv`. Run with:

    mysql --protocol=tcp -h127.0.0.1 -P45000 -uroot --plugin-dir=/opt/homebrew/opt/mysql-client/lib/plugin perf_tpcc_locking_v1 < /private/tmp/tidb-tpcc-locking-balance-check.sql

This establishes a useful live correctness baseline for resumed performance
comparisons. It does not prove explicit wait modes, every join/derived/partition
shape, or the requested overall performance improvement. Ready lint remains
unrun. This validation increment changes only this living plan, not production
source or the checked-in performance baselines.

Performance comparison resumed with the corrected binary. The initial
`perf_sysbench_locking_v1` timing batch is excluded in full: manual ANALYZE
overlapped its first measured sample. Its logs remain at
`/private/tmp/tidb-sysbench-locking-comparison`. A suspected extra-write issue
was ruled out with a Go-only preparation: before any Rust workload, the new
`perf_sysbench_locking_v2` table contained 100,000 rows spanning IDs 1..100278,
including 278 IDs above 100000. Sysbench's explicit DELETE/INSERT pairs can
fill these original auto-increment gaps and increase the row count normally.
No production change was made on that suspicion.

After preparation and ANALYZE completed, both servers received five seconds
of warm-up, followed by three rotating 20-second samples each. The corrected
Rust median is 413.49 TPS; Go is 481.61 TPS (Rust about 14% behind), with zero
errors/reconnects in all six samples. The reference is recorded in
`rust/benchmarks/sysbench-locking-baseline.json`; it is not a new speedup claim.
No build, profiler, or other workload overlapped this replacement batch.

A separate active sysbench profile was captured with the Instruments skill's
`sample-quick.sh 65048 5 1 /private/tmp/tidb-sysbench-locking-profile-active.txt`,
starting the profiler and its 20-second workload from the same shell so they
overlap. An earlier profile began after the workload ended due to tool latency
and is not used. The active profile has HashAgg partial/final workers, map
hashing, and channel waits prominently on the SQL execution path. This is an
optimization lead, not yet proof of a particular improvement.

Live EXPLAIN for `SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 50000 AND 50099`
shows Rust root HashAgg over a one-row cop partial HashAgg; Go uses StreamAgg
at both stages. The DISTINCT-range query uses partial/final HashAgg in both
engines. Rust's `hash_agg/spill.rs::execute_impl` enters the general parallel
pipeline before reading any input, and `hash_agg/parallel.rs` allocates all
partial/final channels and submits worker sets even for one input chunk.
Next: test bounded small-input aggregation admission or an equivalent generic
scalar execution improvement, preserving large-input parallelism, all rows,
memory/error behavior, and lock-operator EOF semantics. Do not ship a session
concurrency override or a workload-name/SQL-specific branch as the fix.

The next implementation increment is bounded aggregate input admission in
`hash_agg/parallel.rs`: fetch at most two child batches before submitting workers.
If EOF follows the first batch (or the source is empty), fold inline using the
existing pipeline state and common finalization, with no channels or worker
tasks. Otherwise dispatch both prefetched batches exactly once and continue
the existing pipeline. Never infer EOF from a short batch. Account memory
while buffering, propagate source/fold errors, preserve first-seen group order,
and consume EOF so selected-row locks are still published. Tests must cover
empty/global defaults, grouped results, one/multiple batches, errors, reopen,
and actual worker admission. This is based on executor input, not SQL spelling
or sysbench row-count constants; large-input parallelism remains required.

While preparing that comparison, source inspection found `HashAggExec::open`
clears its cfg(test) concurrency override before eligibility, invalidating
serial reference executions. First add the red/green regression
`serial_reference_keeps_its_concurrency_override_when_opened`, preserve the
override across Open, and run the existing parallel-versus-serial suite before
using it as evidence. Then add the exhausted-batch worker-admission regression,
implement shared finalization and bounded prefetch, and run focused aggregate
and locking regressions. Rebuild, measure against the saved corrected binary,
and update the performance baseline in the same increment as any verified win.

The serial-reference regression failed before removing Open's test-override
reset, then passed. The main parallel-versus-serial test also lacked the
override promised by its comment; it now explicitly sets 1/1. With real serial
references restored, the exhausted-input regression was red: even empty input
started five partial workers. Bounded two-batch lookahead now folds exhausted
input using the existing `fold_chunk` and shared `finish_pipeline`, without
creating worker channels or cloning the plan into worker tasks. Non-exhausted
inputs dispatch both prefetched batches exactly once before continuing.

`cargo test --offline --locked --release -j12 -p tidb-executor --lib hash_agg`
passes 52 tests. Added cases cover grouped/global empty and one-batch results,
reopen, short non-final batches, errors on input calls 1..4, memory cancellation,
and actual multi-worker admission. Existing large-input parallel and spill tests
pass. `cargo test --offline --locked --release -j12 -p tidb-server --lib a_locking`
passes six tests, including aggregate input contention and locking timestamps.
The previous corrected binary was preserved at
`/private/tmp/tidb-agg-admission-before-EC2Skm/tidb-server` with SHA256
`0cf5656920906e05c35985d48246e40a998cd5501d36d317674703bb18db306d`;
its still-running port 53007 is the before endpoint for the next comparison.

The admission build is running on port 53008 with SHA256
`29ac2bc4c8c055a8d341e33212faa2b1fc5cdea6eb856c1d0a8c57b3023a8c26`.
The six live locking checks pass against it and Go. A three-round rotating
comparison (five-second warm-up each, then 20-second samples, two clients)
measured before/after/Go medians of 400.96/416.44/461.46 TPS. The scoped
before-to-after gain is 3.86%; each round improved and all nine samples had
zero errors/reconnects. The gain is modest, and Rust still trails Go.
`rust/benchmarks/aggregate-admission-baseline.json` records exact counts,
durations, hashes, scope, and the reproduction command in this same increment.
No builds, profiling, analysis, or other workload runs overlapped the samples;
the dataset's last ANALYZE ended at 01:37:27, before measurement at 01:50:55.

After measuring, the new build completed another 20,000 TPC-C transactions at
eight clients on `perf_tpcc_locking_v1` without reported errors. The run command
is the preceding TPC-C command with `-P 53008 -T 8 --count 20000`. All 11
post-run checks through Go passed, and the stronger all-customer check reported
30,000 checked, zero bad balances, zero bad delivery counters. Artifacts are
`/private/tmp/tidb-agg-admission-tpcc-{run,check}.log` and
`/private/tmp/tidb-agg-admission-tpcc-balances.tsv`.

Remaining: controlled before/after TPC-C performance, a large-input aggregation
performance guard, remaining locking semantics, and Ready validation including
lint. Do not infer a TPC-C speedup from the single correctness-stress run or
declare the overall goal complete from this mixed-sysbench improvement.

The large-input performance guard now runs a full-table grouped MIN/MAX plus
outer SUM/COUNT on `perf_sysbench_locking_v2`; 100,001 groups are retained by
the inner root HashAgg. Source inspection confirms MIN/MAX do not qualify for
the separate direct-string COUNT/SUM implementation. Live EXPLAIN confirms
partial/final grouped aggregation, not a metadata-count shortcut. Six rotating
pairs return exactly Go's result (5009502923, 5009502923, 100001), with medians
80.99 ms before and 81.13 ms after. The 0.17% difference is not a meaningful
regression or speedup; broader large-input coverage remains open. Exact SQL
and all samples were added to `aggregate-admission-baseline.json`.

The paired TPC-C comparison is `/private/tmp/tidb-agg-tpcc-compare.py`: three
rotating before/after rounds at each of two and eight clients, 10,000
transactions per run, on the existing correctness-checked TPC-C database.
It rejects errors and incomplete transaction counts, and records each
transaction-type count. The tool's non-uniform random constants are time-seeded
(`tpcc/rand.go`), so samples do not have identical input mixes. Retain all
samples and treat small differences cautiously; do not retrofit a seed or
concurrency setting into production as a performance workaround.

All 12 paired TPC-C samples completed (120,000 transactions, no reported
errors). Median TPS before/after was 470.05/471.72 at two clients and
689.91/704.72 at eight. Paired directions are inconsistent, including two
slower after samples at eight clients; these small median changes do not
establish a TPC-C gain. All samples remain in the baseline and runtime report.
After completion, `go-tpc tpcc check -H 127.0.0.1 -P 45000 -U root
-D perf_tpcc_locking_v1 --warehouses 1 -T 1` passed all 11 post-run conditions.
The all-customer SQL at `/private/tmp/tidb-tpcc-locking-balance-check.sql`,
executed through Go with the previously documented mysql command, checked
30,000 customers with zero bad balances and zero bad delivery counters.
Next is a separate 24,000-transaction, eight-client profile against port
53008 using `sample-quick.sh 38796 5 1
/private/tmp/tidb-agg-tpcc-profile.txt`, started in the same shell as the
workload so the sampling interval overlaps execution. This is diagnostic
evidence only, excluded from all performance comparisons. WIP validation
remains in force; Ready lint and the broader acceptance gaps remain open.

The profile completed during the 24,000-transaction workload, which also
completed without reported errors. All-thread waits are not CPU time; a
derived ranking that excludes common wait leaves still identified
`foreign_key::referring` and metadata cloning among active SQL work. Inspection
found that every lookup allocated and sorted all catalog table paths, resolved
them again, and cloned each table's column names through `declared`, even if
it had no foreign keys. Go's `infoschema.GetTableReferredForeignKeys` instead
reads the schema's reference map (`pkg/infoschema/infoschema.go:1107`), consumed
by `pkg/planner/core/operator/physicalop/foreign_key.go:225`.

This increment removes the unnecessary ownership transformations: a borrowed
`Catalog::table_entries` iterator replaces the obsolete owned `table_paths`;
`referring` inspects live constraints, sorts only matching borrowed entries,
then copies precisely what the later mutable cascade needs. It preserves the
existing stable schema/table order and per-child declaration order. No foreign
key checks are skipped. This eliminates unrelated table/column copies, but
still visits catalog entries; it does not claim Go's O(1) reference-map lookup.
A cached index was not introduced because this catalog also exposes in-place
constraint edits, and its existing schema stamp does not cover those edits.

Targeted `cargo test --offline --locked --release -j12 -p tidb-executor --lib
foreign_key` passes four tests (one manual benchmark ignored), including new
case/order and metadata-change/snapshot-isolation coverage. The metadata-only
benchmark retains the old algorithm as a test oracle, adds 100 unrelated
32-column tables, and alternates six pairs for both matching and absent
references, 5,000 lookups per sample. Results are not end-to-end throughput;
run the session's existing foreign-key SQL tests and compare corrected live
builds before claiming workload gains.

The metadata microbenchmark completed all 24 samples; every borrowed sample
was faster. `rust/benchmarks/foreign-key-metadata-baseline.json` records all
timings in this increment, scoped strictly to lookup cost. Existing session
SQL coverage passes all 50 tests with `cargo test --offline --locked --release
-j12 -p tidb-session --lib tests_foreign_key` (log
`/private/tmp/tidb-fk-metadata-session-tests.log`). The server release build
also passed, logged at `/private/tmp/tidb-fk-metadata-build.log`.
Before binary `29ac2bc4c8c055a8d341e33212faa2b1fc5cdea6eb856c1d0a8c57b3023a8c26`
is preserved at `/private/tmp/tidb-fk-metadata-before-a27mbl/tidb-server`
and still runs on 53008. New binary
`d23d15774a6f70690d233ee5881cde36768672c67c0b25cadafcb07b7be2313c`
starts on 53009 with the same cluster and server arguments, logging to
`/private/tmp/tidb-fk-metadata-server-53009.log`. Re-run the live locking
matrix, then compare both endpoints and Go with rotating fixed-count TPC-C
samples. The comparison driver now accepts repeated `--server name:port`
and `--output` arguments so the original completed report is preserved.

All six live locking cases pass against the new build and Go, captured in
`/private/tmp/tidb-fk-metadata-locking-matrix.jsonl`. The running comparison
command is `python3 /private/tmp/tidb-agg-tpcc-compare.py --server before:53008
--server after:53009 --server go:45000 --output
/private/tmp/tidb-fk-metadata-tpcc-comparison`. It warms each endpoint with
1,000 validated transactions at each concurrency before three rotating
10,000-transaction samples; warmups are excluded from summaries. Do not run
builds, profiles, or another workload until it finishes.

The next source-backed candidate is batched locking reads. Go
`pkg/executor/batch_point_get.go:434` locks all finite record keys before
BatchGet under repeatable read, and `LockKeys` at line 516 requests returned
values for the transaction lock cache. Rust's
`pessimistic_read_lock_point_keys` currently calls the equality-only
`point_write_prelock_keys`; a full-primary-key tuple IN therefore takes the
ordinary read-then-lock path. Its transaction worker already serves
`TransactionRequest::BatchGet` from returned lock values when `locking=true`
(`cluster_table_storage.rs:672`). Investigate reusing existing
`try_batch_point_get` key-domain binding instead of a workload-specific key
parser. Before extending admission, verify missing keys, collation, duplicate
tuples, result ordering, isolation level (Go read-committed locks only existing
keys AFTER reading), wait policy, hints, and partition identity. Do not
broaden the fold by guessing that a partial-key range is a finite key set.

The metadata TPC-C comparison completed all 180,000 measured transactions and
6,000 warmup transactions. All post-run conditions and all 30,000 customer
balances/counters pass (`/private/tmp/tidb-fk-metadata-tpcc-check.log` and
`/private/tmp/tidb-fk-metadata-tpcc-balances.tsv`). At two clients the
before/after/Go medians are 409.34/441.47/563.23 TPS; at eight they are
559.32/570.81/665.70 TPS. The last paired sample is slower after the change
at both concurrencies, so these mixed results do not establish a stable
end-to-end improvement despite the higher medians. All timings are retained
in `foreign-key-metadata-baseline.json`, with per-kind counts in the runtime
report. The remaining Go gap is explicit, not hidden by the lookup benchmark.

A separate mixed-sysbench comparison now runs on the same before/after/Go
ports, existing `perf_sysbench_locking_v2` database and 100,000-row workload:
`python3 rust/scripts/compare-sysbench.py --server before:53008 --server
after:53009 --server go:45000 --database perf_sysbench_locking_v2 --workload
oltp_read_write --threads 2 --seconds 20 --rounds 3 --table-size 100000
--output /private/tmp/tidb-fk-metadata-sysbench-comparison`. It follows a
successful five-second-per-endpoint warmup with the same command using
`--seconds 5 --rounds 1 --output /private/tmp/tidb-fk-metadata-sysbench-warmup`.
Keep this measurement isolated from builds, profiles, and other workloads.

The mixed-sysbench comparison completed all nine samples, with no errors or
reconnects. Before/after/Go median TPS is 428.76/434.23/478.60. Small paired
differences have mixed signs; no stable end-to-end gain is established.
All counts and exact elapsed times are recorded in
`foreign-key-metadata-baseline.json`. The metadata ownership improvement is
verified in isolation, but it is not enough to close the workload gap.

The next candidate now has a live correctness reproduction as well:
`python3 /private/tmp/tidb-batch-missing-lock-probe.py` creates separate
single-integer-primary-key tables through each server. With only id=1 present,
session A explicitly selects repeatable-read isolation, begins pessimistic,
and executes `SELECT id FROM t WHERE id IN (1,2) FOR UPDATE`; both servers
return id=1. Session B's `INSERT INTO t VALUES (2)` blocks under Go but
completes immediately under Rust 53009, before A rolls back. After rollback
both inserts succeed. Exact output is
`/private/tmp/tidb-batch-missing-lock-probe.jsonl`: Go `true`, Rust `false`
for `missing_key_insert_blocked`. No earlier measurement overlapped this
probe. This is not fixed yet: a generic finite-primary-key lock/value fold
must address absent keys as well as eliminating the redundant read RPC.
Next work should add a failing server regression for this case, then extend
the shared batch-key path and verify the previously listed compatibility
conditions. Ready validation and the full performance goal remain open.

The missing-key regression now exists as
`a_locking_batch_locks_missing_primary_keys` in the server's `unistore_cop`
suite. The original source fails because the insert completes before rollback
(`/private/tmp/tidb-batch-lock-red.log`). The prototype shares a pure
`primary_batch_point_lookup` between read planning and pre-lock classification;
it binds complete integer or common primary keys without storage reads and
uses the table's common-handle encoder. Session transaction state retains the
read-committed setting to exclude absent-key prelocks in that isolation mode.
The server binds prelock keys after catalog refresh and lazy transaction open,
so the first statement under autocommit=0 follows the same ownership path.
Composite keys, key-domain refusals, and transaction isolation need targeted
tests before a live comparison or performance claim.

The first rerun reached a pre-existing pessimistic-prewrite recovery switch
and failed with `pessimistic lock type 5 is outside bounded recovery`
(`/private/tmp/tidb-batch-lock-green.log`; despite its filename this FAILED).
Pinned client-go `txnkv/transaction/prewrite.go:541-593` admits these locks and
resolves them normally. Before removing the switch, the existing real-TiKV
tests were run against verified PD 43379 with the switch enabled only in the
test process. All three passed: expired lock recovered, live owner survived
and committed, orphaned secondary recovered. Evidence is
`/private/tmp/tidb-prewrite-recovery-before-removal.log`. The switch and its
gate-only test/runner pass are now removed, retaining the protocol safety
tests without opt-in. This is not yet a finished recovery claim.

Removing that refusal exposed a second failure: the contender sleeps for the
entire old lock TTL and reaches its 20-second deadline despite rollback after
200 ms (`/private/tmp/tidb-batch-lock-recovery-green.log`, also FAILED).
Go's Prewrite uses `BoTxnLock` with TTL as a cap, not as the sleep duration.
The prototype now draws `TxnLock` delays from the transaction's existing
forward backoff budget, retaining deadline/cancellation handling. This is a
generic contention fix, not a batch-query-specific retry. The immediate
verification command from `rust/` is `cargo test --offline --locked --release
-j12 -p tidb-server --lib a_locking_batch_locks_missing_primary_keys`, with
output in `/private/tmp/tidb-batch-lock-backoff-green.log`. Next: prove that
test, then isolation/implicit/prepared cases, repeat the real-TiKV safety tests
with no switch, rebuild a preserved before/after pair, and measure.

The original absent-key regression passes after the backoff change (0.29s).
Its matrix now covers repeatable read, read committed, both mid-transaction
changes to the session isolation default, the first autocommit=0 statement,
and binary-prepared reordered/duplicated common-primary-key tuples. All six
cases and six sibling locking tests pass (7 test functions total,
`/private/tmp/tidb-batch-lock-matrix.log`). Session isolation retention passes
in `/private/tmp/tidb-batch-isolation-unit.log`; two new executor binding/
encoding tests pass in `/private/tmp/tidb-primary-batch-unit.log`; all 13
existing batch-point tests pass in `/private/tmp/tidb-batch-existing-unit.log`.
These are WIP scoped checks, not Ready validation or full SQL parity.

Default-path real-TiKV recovery initially passed all three checks, but review
found that Prewrite still stopped after four retries. The live-owner fixture
now uses an explicit five-second deadline and requires at least 4.5 seconds
of waiting before the lock diagnostic. This fails before removing the cap:
2.3337 seconds (`/private/tmp/tidb-prewrite-attempt-cap-red.log`). Prewrite
now relies on its shared time backoff and absolute deadline, not an attempt
count. The unrelated four-attempt commit timestamp limit is unchanged and
renamed to describe its actual scope. The real fixtures now use a PD-timestamp
key prefix, so reruns never overwrite or depend on earlier fixture rows.
The current verification logs are `/private/tmp/tidb-prewrite-attempt-cap-green.log`
and `/private/tmp/tidb-batch-lock-final-matrix.log`; confirm their terminal
results before relying on them. The old runnable server is preserved at
`/private/tmp/tidb-batch-before-KLx6ef/tidb-server`, SHA-256
`d23d15774a6f70690d233ee5881cde36768672c67c0b25cadafcb07b7be2313c`.
Live SQL and performance comparisons against a new server remain pending.

The deadline wait helper also had an early-exit race: a requested delay longer
than the remaining deadline was refused immediately, including when a caller
had just clamped it against an earlier reading of that deadline. The new
30-ms unit regression fails before the change (0-ms elapsed,
`/private/tmp/tidb-prewrite-deadline-wait-red.log`). `wait_with_call` now owns
the clamp and waits for cancellation or the actual deadline. All six scoped
coordinator tests pass, all three real-TiKV safety tests pass with no switch
(`/private/tmp/tidb-prewrite-final-realtikv.log`), and all seven server locking
tests pass (`/private/tmp/tidb-batch-final-server-tests.log`). The standalone
runner passes `bash -n`; its owned three-node playground lifecycle was not
rerun, because the test binaries used the verified existing cluster.

The new release server is live on port 53010, PID 63758 (verify before use),
tool session 54164, log `/private/tmp/tidb-batch-server-53010.log`. SHA-256 is
`6d46115d6502712cb626775f2e9315d57d7197ede611f8408f22130526b326e2`.
The build command was `cargo build --offline --locked --release -j12 -p
tidb-server --bin tidb-server` from `rust/`; log
`/private/tmp/tidb-batch-build.log`. Running
`python3 /private/tmp/tidb-batch-locking-verified.py --ports 45000` and then
`--ports 53010` proves the same seven cases on Go and Rust: integer/composite
missing-key RR locks, RC existing-only admission, both session-default
changes within a transaction, and the first autocommit=0 statement. Both
commands exit zero; all selected/final rows and lock-blocking assertions
match. Outputs are `/private/tmp/tidb-batch-locking-go.jsonl` and
`/private/tmp/tidb-batch-locking-rust.jsonl`. This does not claim full
read-committed snapshot or partition/wait-policy coverage.

The current isolated measurement is tool session 31594. It runs a five-second
warmup on before 53009, after 53010, and Go 45000, followed by three rotating
20-second mixed-sysbench samples per endpoint with two clients, seed 1,
prepared protocol, and the existing 100,000-row `perf_sysbench_locking_v2`
dataset. Exact invocation is `python3 rust/scripts/compare-sysbench.py
--server before:53009 --server after:53010 --server go:45000 --database
perf_sysbench_locking_v2 --workload oltp_read_write --threads 2 --seconds 20
--rounds 3 --table-size 100000 --output
/private/tmp/tidb-batch-sysbench-comparison`; warmup uses `--seconds 5
--rounds 1 --output /private/tmp/tidb-batch-sysbench-warmup`. Do not overlap
builds, profiling, or other workloads. Read the session's terminal result and
`results.json` before claiming any performance outcome. TPC-C timing and
post-run invariants remain pending on this build, as do Ready and lint.

The sysbench batch completed successfully: all nine samples have zero errors
and reconnects. Before/after/Go median TPS is 443.61/434.87/488.73; after is
slower in every paired round (roughly 1.97% lower median). This is a possible
regression to investigate, not a speedup or a reason to undo required locking
semantics. Exact counts, elapsed times and binary identities are checked into
`rust/benchmarks/batch-locking-baseline.json`. No other workload or build
overlapped this measurement.

TPC-C is now running as tool session 55122: `python3
/private/tmp/tidb-agg-tpcc-compare.py --server before:53009 --server after:53010
--server go:45000 --output /private/tmp/tidb-batch-tpcc-comparison`. Log:
`/private/tmp/tidb-batch-tpcc-comparison.log`. The driver warms each endpoint
with 1,000 transactions at each concurrency, then runs three rotating 10,000
transaction samples per endpoint at two and eight clients. It rejects any
reported error or missing transaction count. Source
`/private/tmp/tidb-tpcc-tool-OY7S5G/source/tpcc/new_order.go:47-54` confirms
that its stock read is a full-composite-key IN batch with FOR UPDATE, so it
exercises this optimization. Wait on the confirmed live session; do not start
another workload/build/profile while it runs. After it finishes, require all
11 Go post-run checks and the all-customer balance/delivery-counter SQL
check, record all timings, and investigate the sysbench regression before a
broader performance claim. The active performance objective remains open.

The TPC-C comparison has terminated with exit 1, not a live wait. All three
two-client pairs improve: before/after/Go median TPS is
469.145/515.570/641.424 (9.90% Rust improvement, still behind Go). Both
completed eight-client pairs are slower after the change. The third
eight-client before sample reports error 1213 during stock UPDATE; the
driver correctly rejects it and stops. Do not claim an eight-client win or
silently replace that sample. All eleven `go-tpc tpcc check` conditions pass
afterward; the independent balance/delivery check covers 30,000 customers
with zero mismatches. Evidence is in
`/private/tmp/tidb-batch-tpcc-comparison/results.json`,
`/private/tmp/tidb-batch-tpcc-check.log`, and
`/private/tmp/tidb-batch-tpcc-balances.tsv`; the checked-in
`rust/benchmarks/batch-locking-baseline.json` records the limited result.

Synchronized two-client sysbench CPU samples of before and after are
`/private/tmp/tidb-batch-profile-before.sample.txt` and
`/private/tmp/tidb-batch-profile-after2.sample.txt`. Both show the memory
arbitrator spawning `ps` every 100 ms for RSS. This is a generic overhead
candidate, not evidence explaining the sysbench delta between those builds.
The first after profile missed the load and is excluded from diagnosis.

The next iteration replaces only macOS RSS discovery with the safe
`libproc` 0.14.11 wrapper around `proc_pidinfo(PROC_PIDTASKINFO)`, matching
gopsutil v3.24.5 `process_darwin_cgo.go`'s `pti_resident_size` byte counter.
Keep the sampling interval, memory admission checks, Linux behavior, and
workspace unsafe-code prohibition unchanged. The PATH-isolated child test
failed before production changes with NotFound, recorded in
`/private/tmp/tidb-native-rss-red.log`. New checks cover touched resident
pages, no external command dependency, byte equivalence against `ps`, and
an ignored alternating sampler microbenchmark. The old release binary is
preserved at `/private/tmp/tidb-native-rss-before-rOeZVd/tidb-server` with
SHA-256 `6d46115d6502712cb626775f2e9315d57d7197ede611f8408f22130526b326e2`.
Native sampling tests, a new server build, and end-to-end measurements are
pending; no speedup from this iteration is yet proven.

Native RSS verification now passes: 12 cgroup tests plus the separately run
ignored sampler comparison. The original command-dependent regression failed
before the change; touched resident pages grow RSS and a PATH-empty child
works afterward. `ps` and native measurements differ by 96 KiB after a
16-MiB allocation. Across five alternating 100-call rounds, median cost is
1,213,772 ns versus 259 ns per call. This proves only sampler overhead, not
SQL throughput. Exact samples and commands are in
`rust/benchmarks/native-rss-baseline.json`. The first microbenchmark attempt
was denied process inspection by the sandbox; its authorized rerun passes.

The release build succeeds (`/private/tmp/tidb-native-rss-build.log`) with
SHA-256 `08f5f2597f221e5e7a33a1d547084817a7b84686780c5104736d988ba7258981`.
New server port 53011 is PID 48843, session 4912, log
`/private/tmp/tidb-native-rss-server-53011.log` (verify before use). All seven
live locking cases pass there (`/private/tmp/tidb-native-rss-locking.log`).
Isolated sysbench comparison is running as session 68056: preceding Rust
53010, native RSS 53011, and Go 45000, five-second warmups then three rotating
20-second samples with two clients, prepared statements, seed 1, and
100,000-row `perf_sysbench_locking_v2`. Invocation uses
`rust/scripts/compare-sysbench.py`; outputs are
`/private/tmp/tidb-native-rss-sysbench-warmup` and
`/private/tmp/tidb-native-rss-sysbench-comparison`. Do not overlap another
workload, build, or profile. TPC-C and Ready/lint remain pending.

Session 68056 terminated with exit 1: the first Go measured sample has one
ignored SQL error. Go's log at 03:25:36 and `INFORMATION_SCHEMA.DEADLOCKS`
confirm a real two-transaction cycle on `UPDATE sbtest1 SET c=? WHERE id=?`;
transaction IDs are 468902933068513322 and 468902933068513323. The first
Rust pair is error-free (419.63 vs 430.18 TPS), but insufficient for a
throughput claim. Preserve the failed sample and report. The next declared
experiment is a separate three-round paired Rust comparison, not a
replacement Go comparison: session 40076, same workload/configuration,
before 53010 and after 53011, output
`/private/tmp/tidb-native-rss-sysbench-paired`. It isolates this increment's
effect only and cannot prove Rust exceeds Go. No benchmark acceptance rule
or locking behavior was weakened to bypass the observed deadlock.

The separate paired run (40076) completed with all six samples error-free.
Before/after median TPS is 426.94/422.28, approximately 1.09% lower after,
with one pair faster and two slower. No sysbench gain is proven. The sampler
overhead reduction is real but not a dominant end-to-end improvement here.
Exact counts and durations are in `rust/benchmarks/native-rss-baseline.json`.

TPC-C comparison session 19572 is running on before 53010, after 53011, and
Go 45000. Command: `python3 /private/tmp/tidb-agg-tpcc-compare.py --server
before:53010 --server after:53011 --server go:45000 --output
/private/tmp/tidb-native-rss-tpcc-comparison`. Log:
`/private/tmp/tidb-native-rss-tpcc-comparison.log`. It uses the same fixed
10,000 transaction, two/eight-client, three-round design described above.
After terminal status, inspect all samples and run the eleven standard
postchecks plus all-customer balances. Then take a separate synchronized
load profile to verify removal of the sampler subprocess stack and locate
the next SQL-path cost. Do not overlap profiling with measurement. The
whole performance objective, Linux execution, and Ready/lint remain open.

TPC-C session 19572 completed successfully: 18 accepted samples (180,000
measured transactions) and 6,000 warmup transactions, no reported errors.
Before/after/Go median TPS is 492.01/497.45/612.09 at two clients and
681.92/644.38/814.50 at eight. Both concurrencies have mixed paired results:
the median rises 1.11% at two clients and falls 5.50% at eight. No broad
throughput gain is established, and Rust remains behind Go. All eleven
standard postchecks pass, and all 30,000 customers have correct balances
and delivery counts. Exact commands/results are recorded in
`rust/benchmarks/native-rss-baseline.json`. Postcheck logs are
`/private/tmp/tidb-native-rss-tpcc-check.log` and
`/private/tmp/tidb-native-rss-tpcc-balances.tsv`.

A separate synchronized 18-second sysbench load and ten-second native
profile finished successfully (session 11725, now terminal). Profile:
`/private/tmp/tidb-native-rss-profile.sample.txt`; load log:
`/private/tmp/tidb-native-rss-profile-workload.log` (zero errors/reconnects).
The sampled memory thread still calls `handle_runtime_stats`, but has no
`Command::output`/`posix_spawn` frames. Its non-sleep samples drop from 175
of 6302 in the earlier profile to 4 of 6321 here; these are thread-state
samples, not CPU-time or SQL throughput measurements. Production source
and the PATH regression independently confirm no subprocess dependency.
Go's reference binary has CGO_ENABLED=1, consistent with the inspected
gopsutil native Darwin implementation.

The new profile also shows repeated AST clone/bind/drop in
`Session::prepared_statement_prelock_keys` even for ordinary SELECTs,
plus existing coprocessor waits. Source inspection confirms the write
classifier already accepts parameters without binding a whole statement.
Next measure and remove redundant classification work at its owning layer,
preserving locking/read-committed guards and key-encoding semantics; do not
assume this smaller cost explains the whole Go gap. No additional production
change is made yet. All commands in this iteration used WIP scope; Linux
runtime checks and Ready/lint are not run. The goal remains active, not
blocked or achieved. No commit or push was made.

The next source-backed loop is prepared pre-lock classification. Go's
`SetParameterValuesIntoSCtx` in `pkg/planner/core/plan_cache.go:77` installs
execute values on retained markers; the point-plan matcher reads those
values rather than copying unrelated SQL expressions. Rust's session helper
instead cloned and bound the entire statement, even an ordinary SELECT whose
lock set is empty. A new ignored benchmark measures five 10,000-call rounds
for narrow/wide ordinary reads, narrow/wide writes, and batch locking reads;
`/private/tmp/tidb-prelock-classification-before.log` records the unchanged
implementation's baseline. A 26-case equivalence matrix spans both isolation
levels, absolute parameter positions outside the predicate, integer and
composite keys, duplicate IN members, unary refusals, and non-default waits.

The implementation now uses `PessimisticPrelock` as a borrowed statement
description: classify shape before catalog access, borrow the table, bind
only the key predicate, and reuse existing point/batch key conversion.
`bind_prelock_predicate` shares the statement marker visitor and literal
conversion, preserving parameter positions and expression semantics. The
wire front end validates parameter count before this helper
(`mysql_connection.rs`, `split_prepared_statement_execute`). Read-committed
exclusion and default-wait/single-table guards are unchanged. Primary batch
binding and point-write conversion borrow authoritative column types from
the table rather than cloning metadata. Removed the redundant old pre-lock
entrypoints and their stale documentation; no whole-statement copy remains
in the session pre-lock helper.

The independent count assertion initially incorrectly expected unary marker
predicates to get a fast plan. The original equivalence test passed before
and after because both decline those predicates. Go's point/batch matcher
also declines unary AST nodes (`point_get_plan.go:200-223,925-984`), so the
test count was corrected to preserve that behavior, not broaden production
admission. Final test/microbenchmark session is 3442, log
`/private/tmp/tidb-prelock-classification-final.log`; inspect terminal status
before claiming it passes. The preceding intermediate benchmark already
shows non-locking reads avoid binding, but no SQL-level gain is claimed.
The old native-RSS server binary is preserved at
`/private/tmp/tidb-prelock-before-9dAt5e/tidb-server`, SHA-256
`08f5f2597f221e5e7a33a1d547084817a7b84686780c5104736d988ba7258981`.
Next run scoped executor/session/server tests, build with 12 jobs, then
compare live workloads without overlapping compilation or profiling.

Session 3442 completed successfully. Median before/after classification ns
per call: ordinary narrow read 592/4, wide read 8779/4, narrow write 2940/567,
wide write 6093/456, batch locking read 3505/849. All 26 equivalence cases
pass, including independent count assertions. Results are checked into
`rust/benchmarks/prelock-classification-baseline.json`.

The 22-test executor point suite has 21 passes and one failure:
`residual_selection_uses_logical_rows_over_access_rows` reports selection
rows 9.49 versus 53.35 expected. The preserved pre-change executor test
binary built at 02:53:27 also fails identically, proving this did not start
with pre-lock classification. Reproduction: run
`rust/target/release/build/tidb-executor/665958758b881ef6/out/tidb_executor-665958758b881ef6
--exact driver::tests::point_get::residual_selection_uses_logical_rows_over_access_rows
--nocapture`; log `/private/tmp/tidb-prelock-estimate-old-binary.log`.
Current suite log is `/private/tmp/tidb-prelock-point-tests.log`. Keep this
unresolved estimate mismatch in the completion audit; do not label the suite
green or alter its expectation merely to pass. It is a candidate for the
next source-level investigation after this increment's live measurement.
Parameter and server locking tests continue as session 65830.

Scoped validation completed: two parameter tests, seven locking server
tests, two point-write server tests, and failed-prelock rollback all pass.
The release build passes, new binary SHA-256
`955a227109e71e0160d553947adad3e9f5351e351eb0d642a328d4064dbfd9f9`.
New server is port 53012, PID 73238, session 8951, log
`/private/tmp/tidb-prelock-server-53012.log`. Seven live locking cases pass
on this server too. Before is native-RSS port 53011, PID 48843, session 4912.

Stopped only five verified superseded owned benchmark servers, with no
connected clients: PID/port 14428/53000, 65048/53007, 38796/53008,
31788/53009, and 63758/53010. Their binaries/logs remain preserved; no data
was deleted. Keep only current before/after and Go running for the next
comparison. Do not compare absolute performance across the old and new
server topologies as a code-only gain.

Sysbench warmup plus comparison is session 76066: before 53011, after
53012, Go 45000, five-second warmups then three rotating 20-second runs,
two clients, prepared protocol, seed 1, 100,000 rows in
`perf_sysbench_locking_v2`. Commands use `rust/scripts/compare-sysbench.py`;
outputs `/private/tmp/tidb-prelock-sysbench-warmup` and
`/private/tmp/tidb-prelock-sysbench-comparison`. No concurrent build or
profile. Inspect the terminal result before proceeding to TPC-C. All
validation remains WIP; the estimate failure and Ready/lint remain open.

Sysbench session 76066 completed: nine error-free samples, zero reconnects.
Before/after/Go median TPS is 438.02/441.25/473.88. The after median is 0.74%
higher, but paired results are mixed (two improve, one declines), so this
does not establish a strong or Go-relative throughput win. Exact samples
are in `rust/benchmarks/prelock-classification-baseline.json`.

TPC-C is running as session 97724: `python3
/private/tmp/tidb-agg-tpcc-compare.py --server before:53011 --server after:53012
--server go:45000 --output /private/tmp/tidb-prelock-tpcc-comparison`, log
`/private/tmp/tidb-prelock-tpcc-comparison.log`. The same fixed-count two/eight
client protocol applies; inspect terminal status and all samples, then run
the eleven standard checks and the independent all-customer balance check.
Do not overlap another build or profile. A stale module comment saying
locking SELECTs are not implemented was corrected in `transactions.rs` to
describe the verified pre-lock/selected-row paths and retained scope limits;
this comment-only edit does not change the measured binary's behavior.

TPC-C session 97724 is terminal with exit 0: all 18 samples and 180,000
measured transactions accepted, no reported errors. Before/after/Go median
TPS is 494.68/508.59/573.52 at two clients and 676.76/582.90/866.39 at eight.
Two-client median rises 2.81% with mixed pairs; eight-client median falls
13.87%, with two pairs slower. This is not a successful broad performance
result. All eleven standard consistency checks pass, and 30,000 customer
balances/delivery counts have zero mismatches. Logs, exact timing arrays,
commands, and limitations are in
`rust/benchmarks/prelock-classification-baseline.json`. Postcheck session
74992 is also terminal with exit 0.

The per-transaction distributions give a stronger next probe than more
small CPU edits. After eight-client round 0 has NEW_ORDER max 4,831.8 ms and
PAYMENT max 4,160.7 ms, while their medians are only 5.2/4.7 ms. The before
build's round 1 also has a 4,160.7-ms NEW_ORDER maximum, and Go round 1 has
PAYMENT max 1,946.2 ms. This is evidence of long latency tails, not proof of
their root cause or that this classification edit created them. DELIVERY
also consumes much of aggregate client time: round 2 after averages 173.7 ms
versus Go 123.1 ms. Keep the regression and all samples; do not rerun until a
favorable median appears. Next instrument statement/transaction latency and
lock/commit waits, comparing Go and Rust source at the responsible boundary.
Also investigate the independently reproduced row-estimate mismatch before
Ready. Do not assume allocation microbenchmarks resolve either issue.

The current safe stopping point has no active workload/build/profile. Only
before 53011, after 53012, Go 45000, TiKV and PD are intentionally retained;
verify PIDs before reuse. No commit or push. WIP validation and reduced
classification overhead are proven; full performance, the estimate failure,
Linux execution and Ready/lint remain unresolved. The original goal stays
active, neither complete nor blocked.

2026-09-07 latency diagnosis: temporary statement/phase timers were built
and run on port 53013, then removed from source and the diagnostic server
stopped. Two accepted eight-client runs completed 10,000 and 30,000
transactions with no reported errors. The multi-second tail did not recur;
do not claim it resolved. Logs are `/private/tmp/tidb-latency-probe-tpcc.log`,
`/private/tmp/tidb-latency-probe-tpcc2.log`, and corresponding
`tidb-latency-probe-server[2].log`. One 318.510416-ms ordered locking read
(connection 2097154, end UNIX ms 1788725901623) retried eight times; its eight
logged postlock waits total 292.719915 ms. This proves contention
amplification, not the cause of the earlier seconds-long outliers.

The fair-locking hypothesis was narrowed rather than implemented: both Go
45000 and Rust 53012 report `@@tidb_pessimistic_txn_fair_locking = 1`, but
`TransactionThread::prepare_with` never calls `set_fair_locking`, and
`RealPessimisticTransaction::from_transaction` starts with false. Go's
`RetryAggressiveLocking`/`DoneAggressiveLocking` release redundant locks and
reset a primary assigned during the retry; enabling Rust's mode without
that lifecycle is not a root fix. A three-connection live test
(`/private/tmp/tidb-fair-retry-lock-lifetime.py` and `.log`) showed neither
current Go nor Rust retains the discarded key, ruling that out as this
path's immediate bottleneck. Four Go-only session-setting contrasts,
ON/OFF/OFF/ON, completed 40,000 transactions without errors. Their timings
are 9.6801/9.6875/10.1104/10.2472 seconds, with mixed delivery differences;
they do not establish that the missing setting explains the workload gap.
Exact commands/results: `/private/tmp/tidb-fair-setting-comparison/results.json`.

A single-client DELIVERY-only diagnostic then isolated lock-owner work.
The first command used weights summing to one and the driver refused it
before running; its idle sample is invalid evidence. Corrected command:
`go-tpc tpcc run -H 127.0.0.1 -P 53012 -U root -D perf_tpcc_locking_v1
--warehouses 1 -T 1 --count 1000 --weight 0,0,0,100,0 --interval 10s`.
All 1,000 deliveries completed, average 26.9 ms (profiled, not a comparison).
Profile `/private/tmp/tidb-delivery-isolated2.sample.txt` captures the active
workload: 1,667 of the SQL worker's 3,538 thread-state observations wait in
TableScan open -> local MergedIterator -> snapshot Scan. These are sampled
thread states, not CPU-time percentages. The local cursor fetches 256 rows
even for LIMIT 1, but why coprocessor admission was lost is the first fix.

Source tracing found that direct protocol transaction control bypasses
`dispatch.rs`'s dirty-table reset. COMMIT publishes `txn.working` with its
old dirty marks; direct BEGIN then cloned those marks into a fresh
transaction. `pushdown_row_cursor_with_context` refuses a dirty
common-handle table. The new regression in `tests_union_scan.rs` failed
with `a fresh transaction inherited the committed transaction's
staged-write mark` before the fix; log
`/private/tmp/tidb-dirty-boundary-before2.log`. The earlier compile-only
attempt used a private accessor and is not red behavioral evidence.

Reset marks when constructing ordinary and historical transaction
snapshots, preserving the existing autocommit boundary. This is independent
of SQL text, schema shape and workload. All eleven union-scan tests pass,
including the new direct-control regression and proof that opening a peer
transaction does not clear another writer's private marks or expose its
rows. Log `/private/tmp/tidb-dirty-boundary-after.log`. Historical-read and
server-locking tests plus a release build completed serially as session 87549.
Preserve the uninstrumented before
binary `/private/tmp/tidb-prelock-current-OTBUWh/tidb-server`, SHA-256
955a227109e71e0160d553947adad3e9f5351e351eb0d642a328d4064dbfd9f9, live on
53012. The older 53011 remains untouched. No throughput gain is yet claimed.

Session 87549 completed: two historical-read tests and nine server-locking
tests pass; the release build passes. New binary SHA-256
2814764b8feacdce212eb854b3e9a47706d832c5156787fd518f5ef6b1bf1580 runs
uninstrumented on 53013 (server session 73963), log
`/private/tmp/tidb-dirty-boundary-server.log`. Seven finite-key live cases
and the Go/Rust ordered-prefix contender check pass; logs
`tidb-dirty-boundary-live-batch.log` and `tidb-dirty-boundary-live-prefix.log`
under `/private/tmp`. The updated comments describe committed snapshots'
retained dirty cells and the fresh transaction boundary, not the old
incorrect assertion that shared cells can only be false.

Sysbench comparison session 78041 completed before 53012, after 53013, Go 45000:
five-second warmups, then three rotating 20-second prepared read/write runs
per server, two clients, seed 1, 100,000 rows. Output prefix
`/private/tmp/tidb-dirty-boundary-sysbench-`. No concurrent build/profile.
All nine samples have zero errors/reconnects. Median before/after/Go TPS is
452.687/450.683/483.791, a -0.44% after change with mixed pairs. TPC-C session
18112 also completed: 180,000 measured transactions, all 18 samples accepted,
no reported errors. Two-client median before/after/Go TPS is
469.279/475.389/570.697 (+1.30%); eight-client is
671.797/663.867/831.416 (-1.18%). This is not a broad throughput win.
Exact samples, commands, binary hashes, limits and validation are preserved in
`rust/benchmarks/dirty-table-boundary-baseline.json`. WIP only; prior
row-estimate mismatch, fair-locking lifecycle gap, seconds-long outliers,
full locking-shape matrix and Ready/lint remain unresolved.

Live SHOW CREATE confirms new_order and order_line really have clustered
composite keys, rather than inferring that from the driver. The post-change
isolated profile confirms clean scans now use coprocessor execution:
`/private/tmp/tidb-dirty-boundary-delivery.sample.txt` has 1,723 of the active
SQL worker's 3,583 thread-state observations in coprocessor completion, via
TableScan.open -> pushdown_row_cursor_with_context -> CopScanSource.open ->
serve_scan_task -> DirectUnaryQueryResponse.pull ->
BatchCoprocessorPending.complete. The formerly dominant local-scan open
path is replaced, but waiting still dominates. This is the actual evidence
for restored admission, not EXPLAIN labels alone. A 1,000-delivery after run
finished before PID lookup completed (no sample); a coordinated 500-delivery
run captured the valid five-second profile. Their 35.1/37.3-ms averages
are retained, not compared as matched speed measurements to the earlier
26.9-ms run over a different evolving dataset. Do not claim the slower
isolated values are improvements or that these waits are necessarily network
time; queued transport work and TiKV execution are still unseparated.

Final postchecks, session 84452, completed after all measured/profiled runs:
eleven standard TPC-C consistency conditions pass; all 30,000 customers have
zero balance or delivery-count mismatches. Logs
`/private/tmp/tidb-dirty-boundary-tpcc-check.log` and
`/private/tmp/tidb-dirty-boundary-balances.tsv`. No temporary phase probes
remain in source. No active build/workload/profile remains. The current
after server is PID 37241 on 53013, session 73963; before remains 73238 on
53012 and older 48843 on 53011, plus Go/PD/TiKV. Verify before reuse.

Next measure the coprocessor request lifecycle at publication, completion,
and TiKV execution-detail boundaries against Go. The new evidence changes
the next action from speculative CPU or fairness changes to a specific
blocking request path. Keep the original goal active; no commit or push was
performed and neither broad performance completion nor Ready is established.

### Batch write record-fetch iteration (2026-09-07)


The coprocessor attribution loop captured 3,600 requests during 200 delivery
transactions: 4,314.48 ms summed client RPC time, 3,737.97 ms TiKV RPC time,
3,594.65 ms TiKV processing time. The same rolled-back locking read on Go and
uninstrumented Rust had 2.462/2.463 ms medians. Go EXPLAIN ANALYZE reported one
processed row but 16,070 scanned versions. This rejects a Rust transport stall
as the explanation for that shared delay. All temporary timing probes were
removed; the trace server was stopped. Raw trace is
`/private/tmp/tidb-cop-timing-server.log`.

Upstream `tpcc/delivery.go` statement-by-statement comparison instead exposed
the finite-key write gap. A bounded rollback-loop profile captured 975 SQL
worker observations in `fetch_write_rows -> read_row`, including 960 waiting
in `SessionSnapshot.get`; these are thread-state observations, not CPU time.
Go `pkg/planner/core/point_get_plan.go:1194` builds a batch reader for writes,
and `pkg/executor/batch_point_get.go:444` fetches all values with BatchGet.
Rust's Batch write arm still looped over point reads. It now reuses
`stored_records_batched` with the unchanged DML decoding context. Source
change is limited to `driver/dml.rs`; the storage-count regression extends
the existing CountingStorage tests in `access_path.rs`.

The regression fails before the change (three requests, expected one), then
passes for integer/common handles, UPDATE/DELETE, duplicate and absent keys.
Ten `driver::tests::dml::` tests pass. Commands, run serially from `rust/`,
are `cargo test --offline --locked --release -j12 -p tidb-executor --lib
batch_writes_fetch_records_in_one_storage_request`, the same command with
filter `driver::tests::dml::`, and `cargo build --offline --locked --release
-j12 -p tidb-server --bin tidb-server`. Logs use the
`/private/tmp/tidb-batch-write-` prefix.

The uninstrumented after build runs on 53014, PID 52153, session 7384; before
remains on 53013 and is preserved at
`/private/tmp/tidb-dirty-boundary-preserved-Rs5tE4/tidb-server`.
`python3 /private/tmp/tidb-delivery-phase-compare.py --ports 45000 53013 53014`
checks identical row/affected-row results over 96 rolled-back transactions.
After two warmups per server, batch DELETE medians before/after are
1.396/0.384 ms and batch UPDATE 1.586/0.577 ms. This is a text-protocol
statement diagnostic, not a mixed-workload throughput claim. Full samples
and binary hashes are in `rust/benchmarks/batch-write-read-baseline.json`.

Session 16591 completed isolated mixed comparisons: five-second sysbench
warmups, three rotating 20-second two-client read/write samples per server,
then three rotating 10,000-transaction TPC-C samples at two/eight clients.
Endpoints are before:53013, after:53014, go:45000. Output prefixes are
`/private/tmp/tidb-batch-write-sysbench-` and
`/private/tmp/tidb-batch-write-tpcc-comparison`. There were no overlapping
builds, profiles or other workloads. Sysbench's nine samples had zero errors
or reconnects: before/after/Go median TPS 450.899/447.248/484.991 (-0.81%).
TPC-C accepted all eighteen 10,000-transaction samples without reported
errors. Two-client medians were 474.822/494.714/555.950 (+4.19%); eight-client
medians were 717.187/723.923/871.279 (+0.94%, mixed individual pairs).
All samples are in the baseline; no statistical-significance or broad-goal
claim follows from these modest changes over a shared evolving dataset.

Postchecks session 34876 completed: all eleven standard TPC-C conditions
pass, and the independent balance/delivery-count query reports 30,000
customers, zero bad balances and zero bad delivery counts. Logs:
`/private/tmp/tidb-batch-write-tpcc-check.log` and
`/private/tmp/tidb-batch-write-balances.tsv`. Commands are the previous
iteration's exact checks with these output names. No active workload,
build or profile remains. Benchmark servers and cluster remain available.

Preceding binding handoff (2026-09-07): candidate 3e8014a3 is live on 53023, server
session 74908, log `/private/tmp/tidb-binding-writer-server.log`. It includes
the independent binding writer, bootstrap lock row, and generic no-op UPDATE
record locks. Seven bootstrap and two binding lifecycle/ownership tests passed
in build chain 16343; the ordinary contention regression passed separately
in `/private/tmp/tidb-unchanged-update-lock-routed.log`. Live chain 26724 is
terminal zero: independent CREATE persistence, bidirectional Go/Rust binding
locks, no-op UPDATE contention, and thirty SQL comparisons all pass. Temporary
probe databases were removed; binding drops use normal tombstones. The
session-binding rerun is recorded in `/private/tmp/tidb-binding-writer-session.log`.
All commands and evidence are in the baseline's `writer_followup` object.

Keep a7eb7f20 on 53022 (session 90163, PID 63618), preserved at
`/private/tmp/tidb-binding-writer-before-oluwQI/tidb-server`. The remeasurement
of 3e8014a3 versus e2b1853d is complete; its modest gains and full postchecks
are recorded above and in the baseline, not inferred from a7eb7f20.
The writer failure/rollback test also passes. Preserve older baseline servers
and shared Go/PD/TiKV. The separate mixed-workload sample has no binding-table
reads in active SQL workers. It contains lock RPC waits as well as planning
work; waiting samples do not prove channel overhead. macOS sample completed,
but its wrapper exited 141 while printing a summary through head; the complete
raw report is `/private/tmp/tidb-binding-writer-mixed.sample.txt`.
Next remove redundant column metadata copying in predicate pushdown and
physical origin lookup, then compare scoped planning and full mixed workloads.
Refresh retention, prepared planning/status counts, shutdown, NOWAIT,
per-session lock timeout propagation, additional unchanged unique-index locks,
the full locking matrix, prior row-estimate mismatch, sequence-counter bounds,
Ready/lint and the broad performance objective remain open. No commit or push.

Preceding metadata handoff (2026-09-07): production release 13b8d343 runs on
53024, PID 32096, session 32233. It includes borrowed visible-column metadata,
single-pass predicate bindings, name-only relation classification, and shared
root/nested join-origin null extension. No production edits followed that
build; expanded tests were compiled separately after throughput timing ended.
The preceding release 3e8014a3 remains on 53023 and is preserved at
`/private/tmp/tidb-predicate-metadata-before-ymdfD9/tidb-server`.

All measurement, test, profile, and postcheck sessions are terminal; no active
workload or compiler remains. Seventy-six distinct scoped tests pass; origin
coverage includes left/right, preserved/null-extended sides, derived outputs,
unqualified names, and ambiguity. Mixed medians and every sample are in
`rust/benchmarks/predicate-metadata-baseline.json`. All eleven standard
TPC-C checks and 30,000 customer balances/delivery counts pass after the
180,000 measured transactions, 6,000 warmups, and separate 20,000-transaction
profile. The live SQL matrix then was 14/15 against Go: old and new Rust
both expose one hidden expression-index column; the probe remains failing.

The after-profile is `/private/tmp/tidb-predicate-metadata-mixed.sample.txt`,
with demangled and inclusive summaries beside it. Active SQL workers are
28/29, each with 10,023 observations. Compared to the preceding full-mix
profile, predicate bindings fall from 380 to 7 inclusive observations and
physical origins from 396 to 31. Lock requests (4,622), remote scan waits
(3,030), and commits (1,928) remain prominent; these overlapping all-state
observations are not CPU percentages or proof of local channel overhead.
macOS sample completed; its summary wrapper exited 141 after writing the
complete report, as before. No timing run overlaps the profile.

The next step then was to address the loaded hidden-column mismatch, preserving physical
column identity and index/generated-expression offsets; do not simply omit
the failing case or drop the column from storage. Then quantify local lock
handoffs versus real TiKV waits. The isolated metadata gain is real evidence
for this path, but the eight-client median declined 0.86% and the full goal
is not achieved. Ready/lint and broader compatibility remain open. No commit
or push. Revision note: replaced stale binding measurement status, recorded
the metadata optimization and origin regression, and closed their measurement
and postcheck loop without hiding the remaining correctness gap.

Current hidden-column handoff (2026-09-07): release 6c4ae765 on port 53025
supersedes that 14/15 correctness result: all 15 original cases match Go.
The loader preserves stored column IDs but establishes the executor's visible
prefix and hidden tail once; DDL backfill uses the same native columns.
This handles Go ADD COLUMN after an existing hidden expression column,
not just the case where hidden columns happen to be last in metadata.
All 25 loader tests pass (the separate backfill rerun is included in that
count), and the release build succeeds. The live write probe runs INSERT,
UPDATE, DELETE and CREATE INDEX after Go ADD COLUMN; all six stages match
Go and pass Go ADMIN CHECK TABLE. Exact commands and logs are recorded in
the baseline's hidden_column_followup object. This is correctness evidence,
not a new performance measurement. Next quantify redundant local lock
handoffs versus real TiKV RPC waits. WIP, no Ready/lint, commit or push.
Revision note: closed the hidden-column validation loop and retained the
preceding release's failed evidence and throughput attribution.

Current lock-dispatch handoff (2026-09-07): release 9fc77176 runs on
53026 in server session 5054, with before 6c4ae765 preserved and still on
53025. It excludes keys already acquired by this statement from post-run
lock dispatch using the same ownership set needed for failure cleanup.
The dispatch regression fails before and passes after; it verifies the
pre-execution lock still happens and new unique-index locks still dispatch.
Sixty distinct scoped tests pass. Four other embedded-store tests fail at
explicit unsupported index coprocessor paths and reproduce identically with
the handoff production change removed; they remain open, not waived.

All builds, probes, measurements and postchecks are terminal. Eight live
lock scenarios per endpoint preserve Rust before/after behavior; all 24
stages pass Go ADMIN CHECK TABLE. The separate 15-case SQL matrix matches
Go. Failed duplicate UPDATE lock lifetime differs from Go on both releases,
as recorded under Surprises & Discoveries and in the baseline. Investigate
pessimistic UPDATE assertion timing together with fair-to-normal lock
promotion next. The lock request's unfiltered keys carry semantic effects in
Go even when its eventual transport has fewer keys; preserve that distinction.
Performance medians and limits are recorded above, with every sample in
`rust/benchmarks/statement-lock-handoff-baseline.json`. All eleven TPC-C
conditions and all 30,000 balance/delivery-count checks pass after the runs.
The broad optimization objective remains active; no Ready/lint, commit or
push. Revision note: closed the handoff measurement loop without presenting
noisy medians or pre-existing correctness gaps as proof of completion.

## Earlier checkpoint: cost-based aggregate selection without workload cutoffs


Release d886a36db0b1fc29844a5a619f64981abed4ef19dad8cec80616288ef3a15f43
runs on port 53031, server session 65403. Before 23174d49 remains on 53030,
preserved at `/private/tmp/tidb-sql-mode-cache-before-x4cfTe/tidb-server`.
No production change follows this release build. Existing servers and
shared cluster processes are not cleanup targets.

The prior empty-restore loop is complete in
`rust/benchmarks/empty-restore-baseline.json`: both regressions, 83 scoped
tests and 30 live cases pass; all measured and profile workload counts and
consistency checks pass. Mixed medians are sysbench -1.20%, TPC-C two-client
-1.63%, eight-client +11.30%, with mixed paired directions. Its fresh profile
no longer shows repeated optimizer-cost parsing, but still shows repeated
SQL-mode scans. All those workload/test/profile handles are terminal.

The new increment stores scanner, temporal, strict, division, grouping,
unsigned-subtraction and auto-zero flags in StatementVarSnapshot. The
existing uppercase/token/trim derivation and generation invalidation are
unchanged; no parser, runtime cache layer or workload-specific branch is
added. The counter regression fails before with two helper calls and passes
after with zero, including the query/DML semantics and SET-to-empty checks.
All 66 scanner/date/cast/null/generated-expression/auto-zero/context tests
pass, as do 19 live session-state and 11 global-read cases against Go.
The default-mode context benchmark falls 3382 -> 2791 ns (query) and
3444 -> 2753 ns (DML); expanded modes fall 3866 -> 2795 and 3986 -> 2730.
Independent before/after timing and test-only counter overhead limit precise
attribution. Empty-mode results are roughly flat, and this is not SQL TPS.

Serial sysbench/TPC-C comparison 71688 completed successfully. All nine
sysbench samples have zero errors/reconnects; all eighteen measured TPC-C
runs complete 10,000 transactions of all five types, plus six 1,000-transaction
warmups. Sysbench median changes +1.51%; TPC-C two/eight-client medians change
-3.35%/-4.15%. Paired directions are mixed. Exact raw reports and limits are
in `rust/benchmarks/sql-mode-cache-baseline.json`. Both postchecks pass all
eleven conditions and 30,000 balances/delivery counts. Preserve the noisy-host
limitation; no stable broad gain is established.

The first subsequent sample missed the workload (sample began 12:11:00,
workload ended 12:10:58); reject it as idle-only evidence. Corrected combined
workload/sampler session 25283 exited zero. The sample begins 12:12:30 inside
the 12:12:29-12:13:03 workload, and cost-factor construction/teardown remains
visible. Postcheck 69921 covers both profiling workloads (40,000 transactions).

Next root change: share CostEnv itself, not individual factor-name strings.
Session's generation cache owns Arc<CostEnv>; query/DML and context clones
share it. Changed global spill policy or quota uses copy-on-write so retained
statements keep their old values. Standalone contexts lazily borrow one
immutable default rather than allocating a default that session builders
immediately discard. Existing owned-CostEnv callers remain accepted by the
builder. Go's process-default factors and typed session settings motivate
the ownership boundary; no cost formula or workload-specific path changes.

Runtime red 11381 fails the sharing assertion. Green 98304 completes six
session-context, fourteen executor-context, four join-cost, eleven memory-quota
and four fix-control tests plus release build. Fifteen projection tests pass.
Aggregate tests have 25 passes and six plan mismatches. Negative control 95327
restores the previous owned CostEnv/default construction and reproduces all
six with identical actual/expected plans; these are not introduced by this
increment and remain open. Restored implementation passes fourteen context
tests; its final rebuild 4266 exited zero and the binary hash matches.

Release 9fc92d67 is running on 53032 (server session 1477); its 19 session-local
and eleven read-only global cases match Go and before d886a36d on 53031.
Native context medians (before -> after ns) are empty query 2718 -> 1673,
empty DML 2662 -> 1874, default query 2703 -> 1636, default DML 2618 -> 1775,
expanded query 2756 -> 1670 and expanded DML 2581 -> 1909. Independent
before/after samples, noisy host, not SQL throughput. Full raw samples,
negative-control failures and exact commands are recorded in
`rust/benchmarks/cost-env-owner-baseline.json`.

Comparison 92153 is terminal (exit zero). Sysbench median 447.39 -> 446.02
TPS (-0.31%); TPC-C two-client 508.49 -> 516.14 (+1.50%) and eight-client
705.12 -> 683.11 (-3.12%). Paired two-client changes are -1.77%, +7.25%,
+3.26%; eight-client +0.79%, -3.64%, +17.96%. This drift does not establish
a stable broad gain. All eighteen 10,000-transaction runs plus six 1,000
warmups complete with all five transaction types. Postcheck 5656 exits zero:
eleven conditions, 30,000 customers, zero incorrect balances/delivery counts.
No benchmark/compiler/profiler remains active from that comparison.

Next root loop: aggregate selection. The read-only session-local probe
`python3 /private/tmp/tidb-aggregate-cost-policy-probe.py --ports 45000 53031 53032`
exits zero; full plans are in
`/private/tmp/tidb-aggregate-cost-policy-probe.jsonl`. It compares five
aggregate queries at default factors, hash factor 1000, and stream factor 1000.
Go selects StreamAgg for small SUM/COUNT ranges at defaults and changes all
five families when hash becomes expensive; Rust keeps its original hash
choices. This predates immutable sharing. No shared settings/data changed.

Source causes: `agg_select.rs:prefer_stream_agg_for_global_count` uses default
settings and zero child cost with a 10,000-row cutoff;
`prefer_stream_agg_for_small_global` is only a cutoff;
`prefer_partial_agg_for_input` uses ten rows. The global stream path also
admits only one aggregate function. Go's actual stream executor and Rust's
StreamAggExec support multiple aggregate states. The Rust Candidate evaluator
already supplies whole-tree reader/task factors, row widths and aggregation
costs. The table scan also refuses partial aggregation at estimates <=1,
while the index source does not; separate feasibility from cost instead of
moving thresholds between layers.

Regression `aggregate_choice_reads_statement_cost_factors_for_multiple_functions`
in `driver/tests/aggregates.rs` compares high hash/stream factors and checks
SUM/COUNT rows. Command 87722:
`cd rust && cargo test --offline --locked --release -j12 -p tidb-executor --lib aggregate_choice_reads_statement_cost_factors_for_multiple_functions`,
log `/private/tmp/tidb-aggregate-cost-policy-red.log`, exited 101 with a runtime
assertion: actual HashAgg, expected StreamAgg at hash factor 1000. The same
regression passes after implementing `choose_aggregate` (green log
`/private/tmp/tidb-aggregate-cost-policy-green.log`).

Implementation must compare eligible root hash/stream and split cop+reader+
final candidates with actual statement environment and input/output types
before mutating source schemas. Go shares final/partial statistics
(`base_physical_agg.go:NewPartialAggregate`); root and partial function
counts differ for AVG and eliminated FIRST_ROW carriers. Preserve those
descriptors and output remapping. Admission must still honor pushdown
blacklist, residual predicates, prefix-index whole-value needs, ordering,
dirty-row merges, and SelectLock boundaries. No replacement row cutoff,
workload identifier or relaxed golden. Extend row/empty/NULL/AVG/DISTINCT,
session-factor and cardinality coverage; then compare real Go plans, run
relevant suites (six existing failures are explicit baseline gaps), and
measure isolated and mixed workloads with consistency postchecks.

UPDATE lazy assertions, fair-to-normal lock lifetime and other recorded
correctness gaps remain open. Broad goal active; WIP, no Ready/lint, commit
or push.

Revision note: closed cost-owner workload and consistency evidence; verified
aggregate cost-policy divergence on live Go and both Rust releases; added a
runtime-red multi-function regression and the source-backed root design.

Revision 2026-09-07: aggregate implementation now replaces the cutoffs and
default-factor comparison with actual statement candidate costing. Source
admission is separate from installation; global descriptors carry the chosen
hash/stream mode through coprocessor DAG lowering. Empty global grouping is
accepted without weakening the rejection of an entirely empty aggregate.
`cargo test --offline --locked --release -j12 -p tidb-exec --test all cop_scan_partial_predicate_limit_source::`
passes five tests (`/private/tmp/tidb-aggregate-cost-policy-wire-tests-v4.log`).
`cargo test --offline --locked --release -j12 -p tidb-executor --lib driver::tests::aggregates::`
reports 29 passed and three failed in the v3 aggregate log. Condition eleven
still returns the expected query result but fails its synthetic COUNT plan
assertion; investigate complete versus split state rendering and candidate
ownership, not a replacement cardinality threshold. Conditions two and six
remain prior plan gaps. Temporary test-only cost diagnostics remain; remove
them after diagnosis. Point/batch-point candidate coverage and grouped ordered
access alternatives need review. No aggregate throughput measurement or Ready
claim is supported yet.

Follow-up 2026-09-07: the new condition-eleven failure came from
`driver/leaf_access.rs` replacing an index leaf's `planner_candidate` with an
opaque `Fixed` root cost. Preserve its existing reader tree, as table leaves
already do. This also resolves the prior condition-two failure. The extended
SUM/COUNT regression fails for `id IN (1,2)` before batch point-task costing
(`/private/tmp/tidb-aggregate-point-task-red.log`) and passes afterward
(`...-green-v2.log`), including NULL and missing rows. Single, batch, converted,
and join-leaf point candidates share the root-cost constructor in `access_cost`.

The new `hash_aggregate_does_not_advertise_its_ordered_inputs_order` regression
first fails because HashAgg advertises `[0,1]` from its input
(`/private/tmp/tidb-aggregate-delivery-red.log`). Publish only the selected
executor's output orders, after projection; equality-fixed group keys retain
both equivalent orders proved by `aggregation_order`. An extended test then
finds default factors in the outer whole-child comparison; `driver.rs` now
uses the statement environment there as well. Aggregate v9 reports 32 passed,
only prior condition six fails. Point suite v9 reports 21 passed and the
previously reproduced 9.49-versus-53.35 estimate failure; merge-decision suite
passes nine tests. Temporary aggregate diagnostics and the unreferenced legacy
partial-stream trace path are deleted. New DAG coverage accepts function-only,
group-only and grouped functions in both modes, rejects a wholly empty
aggregate, and verifies `Aggregation.streamed` remains absent in list DAGs.
Release 459ff441bff6d8a5dc97c11602ae91a0837b8399d54a72f21c58b426369a9df0
builds successfully and runs on port 53033 in server session 60196. The
preserved before binary 9fc92d67 remains on port 53032. Live plan probe v9
matches Go operator trees in fifteen of fifteen cases (before: eight), including
the grouped ordered-index alternative under expensive hash aggregation.
`/private/tmp/tidb-aggregate-choice-live-rows-v9.jsonl` records all 117 passing
result checks: 39 each for Go, before and after; AVG, DISTINCT, NULL/empty input,
point/batch and grouped cases. This is scoped parity, not full EXPLAIN equality.

Comparison session 76441 completed these commands sequentially from repo root:

    python3 rust/scripts/compare-sysbench.py --server before:53032 --server after:53033 --server go:45000 --database perf_sysbench_locking_v2 --workload oltp_read_write --threads 2 --seconds 20 --rounds 3 --table-size 100000 --output /private/tmp/tidb-aggregate-choice-sysbench-comparison-v9
    python3 /private/tmp/tidb-agg-tpcc-compare.py --server before:53032 --server after:53033 --server go:45000 --output /private/tmp/tidb-aggregate-choice-tpcc-comparison-v9

Session 76441 exited zero; no workload remains running from this comparison.
All nine sysbench samples have zero errors/reconnects. All eighteen measured
TPC-C runs complete 10,000 transactions each, and six warmups complete 1,000
each. The Go checker passes eleven conditions. The first mysql balance-check
invocation failed to load native authentication; retry 83356 with
`--plugin-dir=/opt/homebrew/opt/mysql-client/lib/plugin` exits zero and reports
30,000 customers, zero bad balances and zero bad delivery counts.
Host preflight: Chrome renderer 84.4%,
mediaanalysisd 78.1%, mediaanalysisd-access 18.4%, mds_stores 15.4%, syspolicyd
10.3%; no processes were stopped. No builds, profiler or SQL probes may overlap
timed samples. Full raw samples, commands and postchecks are recorded in
`rust/benchmarks/aggregate-choice-baseline.json`. Sysbench median 447.38 ->
443.88 TPS (-0.78%). TPC-C two-client 479.83 -> 497.76 (+3.74%), eight-client
626.78 -> 585.81 (-6.54%). Eight-client paired changes are -1.71%, -8.18%,
-4.04%; two-client -3.28%, +4.73%, +0.06%. This is not an overall performance
improvement. Next isolate and profile the eight-client slowdown with the
preserved before binary and the current after release; do not restore
cardinality cutoffs or bypass candidate correctness. Broader whole-package
and Ready verification remain incomplete. The broad goal stays active.

## Earlier checkpoint: concurrent region rollback


The paired sampling runs and fixed-data isolation above are now complete. The
query helper `/private/tmp/tidb-aggregate-isolation.go` uses binary prepared
statements, rotating before/after/Go endpoints and fixed pending orders. Its
clean/dirty SUM and shared/disjoint locking comparisons do not reproduce the
full mixed-workload aggregate regression. Full DELIVERY statement sequences,
rolled back rather than committed, reveal a separate causal cleanup cost:
Rust waits for one region's rollback response before sending the next, while
pinned client-go `txnkv/transaction/2pc.go:doActionOnBatches` overlaps regions.
The live region map is `/private/tmp/tidb-delivery-isolation-regions.tsv`.

`tidb-txnkv/src/transaction/command_client.rs` now retains every rollback
publication before completing responses; `transaction/pessimistic.rs` consumes
the entire response round, forgets successful keys, preserves failed/ambiguous
keys and retries only region-error batches. The existing transport fixture in
`tests/pessimistic_lock_source.rs` withholds both responses until both region
requests arrive. Two tests fail at runtime before this change (publication
timeout and unprocessed sibling cleanup); the final suite passes 29 tests,
including region retry and existing ForceLock timestamp constraints. Exact
commands and logs are in `rust/benchmarks/pessimistic-rollback-batch-baseline.json`.

The release build (57029), final source tests (31502), sequential mixed
comparison (5500), and consistency checks (21650) all exited zero. No build,
benchmark or postcheck from this increment remains active. Keep server session
36268 on port 53034 (fd96103d) and the prior port 53033 (459ff441) available;
the prior binary is `/private/tmp/tidb-rollback-before-gDMQzA/tidb-server`.
No servers or unrelated source changes were removed, and no Git writes occurred.

Measured rollback latency falls about 78-79% to 0.98 ms, near Go. This does not
prove an overall workload win: fixed-data rolled-back DELIVERY throughput is
+19.08% at one client and -2.37% at eight; mixed medians are sysbench +0.98%,
TPC-C two-client +6.94%, eight-client -0.09%, with varying paired directions.
All 180,000 measured and 6,000 warmup TPC-C transactions complete; eleven
consistency conditions and 30,000 customer balance/delivery-count checks pass.

Next profile statement execution inside contended lock-holding intervals.
The fixed-data dirty grouped read still takes about twice Go's latency.
Rust `driver.rs:run_select_traced_with_delivery_choice_inner` builds two
aggregation alternatives and then reconstructs the winner; Go retains its
chosen task in `find_best_task.go`. That is a source-backed candidate for the
next probe, not yet proof of the dominant CPU cost. Preserve the selected
candidate's statistics residency, output-order receipt and EXPLAIN behavior
if changing ownership. Large-fanout transport saturation and full Ready/lint
validation remain unverified, and all previously recorded correctness gaps
remain open. Broad goal active, WIP only.

Revision note: closed aggregate diagnostic evidence, implemented and measured
concurrent region rollback, recorded mixed results and postchecks without
promoting the scoped cleanup win to a broad throughput claim.

## Previous handoff: retained aggregate tasks and stale-region recovery


The previous status-only turn was no progress; this loop adds runtime-red
regressions, implements source changes, and yields new live evidence. No
build, test, probe or benchmark started by this loop remains active.

`driver.rs` now keeps a cost-selected aggregate executor instead of rebuilding
it. Each branch uses the caller's explicit derived-output mode, an independent
trace fork and statistics-load snapshot. The winner transfers those owners;
the loser is dropped before execution. The new ownership test covers 48
combinations and trace tests cover independent logical-column allocation and
shared counters belonging to an existing prefix. Aggregate/point suites retain
only their known failures (33+1 and 21+1). Plain DISTINCT still lacks a complete
cost receipt; Go versus the prior release confirms its cost-factor mismatch.

The first release 4134703e on port 53036 passes live parity, but timing session
90936 fails at 14:55:58 with MissingLeader after TiKV's 14:55:57 auto-splits.
The routing source had an inconsistent validity boundary: eviction before
request-selector creation was terminal, eviction afterward already rebuilt
ranges. `region/cache/replica_routing.rs` now leaves validity to selection for
both timings. Both cache-level and real transport-state tests reproduce the
old failure; the transport test now rebuilds into two children with no RPC to
the obsolete parent. Four replica-policy, sixteen request-selector and all
sixty-four direct-unary tests pass. Initial transport-test compilation used a
private module path; the v2 red log is the runtime reproduction, not that
compile error.

Release 03420212 on port 53037 contains both changes. Build 32305 and parity
78876 exited zero. Isolation 17935 exited zero: 36 rotating samples / 324,000
operations all match Go, no ignored errors. Median clean SUM changes are
-0.21%/-1.43% and dirty SUM +0.42%/-0.48% at one/eight clients. These are not
a speedup. Do not run a broad mixed sweep simply to seek a positive result;
tighten the source/profile explanation first. No profiler or build overlapped
timing; the host still had ordinary application/background load.

Next verify whether these actual reads produce a cost receipt or fall through
the unpriced path. A concrete remaining source hole is the old single-grouped
SUM path in `agg_select.rs`: `partial_grouped_sum` expressly prevents candidate
publication, while general grouped HashAgg already publishes its candidate.
That can defeat retained-task ownership and repeat planning. Prove the actual
path with a representative regression/profile before deleting the duplicate
single-SUM path in favor of general grouped aggregation. Also note that the
test counter currently counts explicit alternatives, not a subsequent Auto
fallback body; a fallback regression must count actual pipeline builds.
The DISTINCT missing-receipt path is another related gap. Do not fix either
with invented costs, workload branches, or arbitrary retry loops.

Preserve all servers: a39ab966 port 53035; 4134703e port 53036/PID 10048/session
21226; 03420212 port 53037/session 11606. Saved binaries are
`/private/tmp/tidb-retain-aggregate-before-QiFabL/tidb-server` (a39ab966) and
`/private/tmp/tidb-region-eviction-before-zxQW1e/tidb-server` (4134703e).
Evidence and exact commands are in `rust/benchmarks/retained-aggregate-baseline.json`
and `rust/benchmarks/lazy-selectivity-baseline.json`. No commits, pushes,
server restarts, unrelated deletions, Ready/lint or package-completion claims.
Broad performance goal remains active.

## Selection boundary and fixed-data DELIVERY evidence


The grouped-SUM loop completed regression, release, live parity,
isolated/mixed performance, and consistency evidence. Its tests and timed
workloads exited. Server session 37148 remains on port 53039 with the measured
45c21a6d release. Its before endpoint is 03420212 on port 53037/PID 29625;
the preserved binary is `/private/tmp/tidb-general-grouped-before-KlDY79/tidb-server`.
Diagnostic-only port 53038 still contains old temporary logging and must not
be used for timing; those probes are absent from current source.

General grouped aggregation now owns single SUM too. Removed `GroupBySum`,
its descriptor/schema remapping, local evaluator, TiKV lowering, and trace
helpers. A statistics-backed decimal/NULL/empty-input regression verifies
two actual pipeline builds rather than three. The aggregate suite passes
34 tests and retains its known extra-Sort failure; trace/remote-scan suites
pass 14/23 and partial-predicate/DAG suites 5/8. Standard live Go comparison
matches 15 operator trees and 39 result cases; nine actual-query result
cases also match, with two existing extreme-factor operator mismatches.

The measured 45c21a6d binary improves fixed-data grouped SUM throughput by
18.18%/11.40% clean and 10.63%/7.16% dirty at one/eight clients. All twelve
paired directions improve and 324,000 measured results match Go. Mixed
medians are sysbench +0.05%, TPC-C two-client +8.70%, eight-client -2.97%;
paired directions vary, so broad improvement remains unproven. Nine sysbench
samples contain no errors/reconnects. All 180,000 measured and 6,000 warmup
TPC-C transactions complete; eleven consistency conditions and 30,000
customer balance/delivery-count checks pass. Full samples, commands, binary
identities and limitations are in `rust/benchmarks/general-grouped-aggregate-baseline.json`.

The first after sysbench sample contains a 20,055-ms transaction and 49.06
TPS; retain it. TiKV logged a split of region 1241 plus two CheckTxnStatus
and one detached Commit EpochNotMatch responses at 15:39:07.741-742. This
correlation is not yet a causal explanation. Do not claim the next change
resolves this observed stall and do not shorten timeouts as a workaround.

Source review found a related cache ownership defect: lock recovery invoked
`RegionCache::on_region_error` inside `with_region_cache`, holding its mutex
over PD store loading. Go `internal/locate/region_cache.go:2886-2941` unlocks
around replacement construction. Transaction recovery already uses the
existing Rust shared-cache method that does the same. `lock/resolver.rs` now
uses that owner too; no new retry policy, cache, or timeout was introduced.

The initial test incorrectly accessed the loader through `cache.cluster_id()`
and failed before and after; it is not valid differential evidence. The
corrected observer reads only cached state. Repeating the old source produces
the expected runtime failure after the two-second metadata hold, while the
fixed source passes all 31 lock-resolver tests in 0.01 seconds. It also proves
the status RPC retries with the replacement epoch and preserves the committed
result. Logs and exact commands (cwd `rust`):

    cargo test --offline --locked --release -j12 -p tidb-txnkv --test lock_resolver_source lock_epoch_recovery_leaves_cache_available_during_metadata_loading
    # runtime-red: /private/tmp/tidb-lock-recovery-cache-red-v2.log
    cargo test --offline --locked --release -j12 -p tidb-txnkv --test lock_resolver_source
    # 31 passed: /private/tmp/tidb-lock-recovery-cache-green-v2.log
    cargo test --offline --locked --release -j12 -p tidb-txnkv --test region_error_recovery_source
    # 25 passed: /private/tmp/tidb-lock-recovery-region-tests.log
    cargo test --offline --locked --release -j12 -p tidb-distsql --test all lock_recovery_source::
    # 4 passed: /private/tmp/tidb-lock-recovery-distsql-tests.log
    cargo build --offline --locked --release -j12 -p tidb-server
    # success: /private/tmp/tidb-lock-recovery-cache-build.log

The cache-fix release before the current DISTINCT work is
`4d60fa059272c0b2e642145a3d69aa1c80ac7a9d3edfbbc0c4ee1d50ff728084`;
it includes the cache ownership fix but is not the running/timed port 53039
binary. It now runs on port 53040/PID 22178/session 97922 and is preserved at
`/private/tmp/tidb-distinct-before-YXEcCO/tidb-server`. The temporary
negative-control source was restored exactly afterward.

The 20,000-transaction eight-client TPC-C diagnostic finished without the
20-second stall. A macOS sample of PID 22178 shows pessimistic lock waits
dominating active SQL execution; these include sleeping stacks, not CPU-only
time. The attach-mode scripts and samples are `/private/tmp/tidb-contention-profile*`.
Sequential sampled sysbench before/after-cache runs also completed with no
errors, reconnects, or reproduced stall: 19,744/19,031 transactions, maximum
latency 26.88/22.30 ms. They are diagnostics, not throughput evidence.
Artifacts are `/private/tmp/tidb-sysbench-tail-{before,after}*`; session 42759
exited zero. Eleven TPC-C conditions and all 30,000 customer balance/delivery
checks passed after these diagnostic writes (session 82964 exited zero).

The next source-backed loop addresses DISTINCT's third pipeline build,
visible in sysbench's real range-query path. Go `buildDistinct` constructs a
normal FIRST_ROW aggregation; Rust built the alternatives but returned no
costed task. Reuse the grouped aggregate cost builders, include actual cop
partial stages, and price Sort/TopN/Limit wrappers rather than inventing an
aggregate-only score. The range ownership test fails at runtime with three
builds (`/private/tmp/tidb-distinct-retained-red.log`) and passes after the
change; the expanded aggregate suite retains only the known extra Sort
failure (`/private/tmp/tidb-distinct-retained-aggregates.log`). New TopN and
Limit candidates use Go's existing cost formulas; root Limit inherits child
cost, unlike pushed reader limits. Eight candidate tests, 42 plan-selection
tests and 14 trace tests pass. The strengthened NULL/dirty/clean-input
regression passes too. The release build exited zero; f2ff9c88 runs on
port 53041/PID 68185/session 89578. Preserve it and the before endpoint
53040. `/private/tmp/tidb-distinct-live.py` is a read-only
Go/result/operator matrix. All 27 result cases match on both Rust endpoints.
Both retain three EXPLAIN failures for out-of-range point DISTINCT and only
eight matching operator trees. Three indexed cases change aggregation family
in the new binary; their results still match Go, but full operator parity is
not claimed. The original probe stopped on the old binary's EXPLAIN failure;
v2 records that error separately and completes the result matrix.

The fixed-data prepared helper now supports
`distinct` and `distinct-topn`, each inside a read-only transaction rolled
back after the sample. Its 36 samples completed 324,000 Go-checked operations:
DISTINCT median throughput +10.16%/+5.68%, DISTINCT TopN +15.60%/+5.73% at
one/eight clients. Samples are short and one of twelve paired directions
regresses; all are retained. No timing overlapped builds, probes or profiling.

The sequential mixed process 58492 exited zero: nine sysbench samples with
no errors/reconnects, then all 180,000 measured and 6,000 warmup TPC-C
transactions. Mixed medians: sysbench +0.45%; TPC-C two-client -0.97%,
eight-client -3.25%. Paired directions vary widely, especially TPC-C eight
clients (-20.65%, +8.90%, +24.01%). The shared evolving dataset and time-seeded
workload do not isolate a broad win. Postcheck session 78871 exited zero;
11 conditions and all 30,000 customer balances/delivery counts pass.
Raw samples, exact commands, hashes and limitations are checked into
`rust/benchmarks/retained-distinct-baseline.json`. Selected new lines were
formatted afterward; no semantic change or rebuild/timing overlap.

The whole-task audit now has runtime red/green evidence: HAVING's Selection
was absent from the DISTINCT cost, and correlated Apply could publish a leaf
cost that omitted its inner work. `plain_candidate` now follows the executor
through these transformations. HAVING composes its filter; unrepresented
Apply and materialized windows retain no whole-task cost. This does not
implement Apply/window costs.

Expanded live validation revealed that the original Selection forwarding was
not a valid operator contract: `SELECT DISTINCT k FROM sbtest1 WHERE id BETWEEN
1 AND 100 HAVING k > 50000 ORDER BY k` changed a two-column source into a
one-column partial aggregate under a predicate bound to column one. Release
71c6edf1 on port 53042 loses its query connection with the same chunk-index
panic at all three aggregate factor settings. The unqualified unit fixture
had already pruned its source to one column, so its earlier passing result did
not cover this live shape. The expanded unit regression now reproduces the
actual panic before the fix and passes after it.

`SelectionExec` now owns its TableAccess implementation instead of exposing
the unfiltered child's capability. Predicate, handle-range, partition, order,
scan-estimate and lookup-batch negotiation still forward. Schema-changing
offers and Limit/TopN retain the trait's default refusal because moving them
below a residual filter requires an explicit semantics-preserving rewrite.
This matches Go LogicalSelection's predicate-column preservation and its base
TopN attachment above the filter. The temporary cost-helper traversal through
Selection was removed; it had priced an invalid execution shape.

The independent-operation live helper now records errors and lost connections
without retrying them or poisoning subsequent cases. Go rejects the intentionally
unprojected correlated HAVING column in three cases; those are error-parity
checks, not successful query-result claims. The pre-fix 48-case matrix contains
42 matching result sets, three matching Go errors and three connection losses.
Raw evidence is `/private/tmp/tidb-selection-boundary-live-red.jsonl`.

The broader subquery run stops at two assertions: non-unique IN chooses
IndexJoin rather than the expected HashJoin (`subqueries.rs:1100`), and the
TPC-C condition-ten history lookup has no cop HashAgg (`:1824`). A temporary
control removed only the latest cost-audit changes and reproduced both
failures, with 15 passes, at exactly those assertions. All control edits were
restored before subsequent tests. Source inspection finds that a non-covering
index lookup is a `Candidate::Fixed` in `access_cost::index_path`, while
`choose_aggregate` admits a partial cost only under `Candidate::Reader`.
The general execution path also falls back to local partial aggregation when
the index cannot supply its inputs. Merely changing the trace or admitting
an unmodelled partial would not solve this missing physical ownership.

Commands run from `rust`, all WIP:

    cargo test --offline --locked --release -j12 -p tidb-executor --lib driver::tests::select_clauses::
    cargo test --offline --locked --release -j12 -p tidb-executor --lib driver::tests::subqueries::
    cargo test --offline --locked --release -j12 -p tidb-executor --lib driver::tests::through_proj::
    cargo test --offline --locked --release -j12 -p tidb-executor --lib driver::tests::join_reorder::
    cargo test --offline --locked --release -j12 -p tidb-executor --lib distinct_candidate_includes_plain_having
    cargo build --offline --locked --release -j12 -p tidb-server

Logs are `/private/tmp/tidb-whole-task-{clauses,subqueries,subqueries-control,
through-proj,joins,restored-regression,build}.log`. The corresponding completed
test counts are 12, 15 plus two failures, the identical control count, 15,
26 and one. Release 46812 completed as 71c6edf1d61861a9117e6983442eb45837f8defc6afe97ef774f077c66b2da84,
running on 53042 (session 84323, PID 44348). The Selection-boundary follow-up
passes five Selection tests, 35 aggregates plus the known Sort failure, 15
through-projection and 26 join-reorder tests. Predicate tests pass 11 with two
existing global-StreamAgg trace refusals and one ignored; restoring the original
Selection forwarding reproduces both refusals at exactly the same assertions.
That control is restored and the final expanded regression passes. Logs are
`/private/tmp/tidb-selection-boundary-{red,green,final-regression,selection-tests,
aggregate-tests,predicate-tests,predicate-control,through-proj-tests,join-tests}.log`.
Release build 85895 completed as
4b29b04cfd7f90463f8386e467121f90d4756e6da153c042305a735b106f2ed6,
running on 53043 (server session 14847). The final live matrix completed with
45 matching successful result sets and three matching Go errors, no connection
losses. It still has only eight matching successful operator trees and three
old empty point-query EXPLAIN refusals. Fourteen trace unit tests pass.

Timing session 14709 completed all 18 samples and 16,200 measured transactions:

    /private/tmp/tidb-aggregate-isolation -before-port 53041 -after-port 53043 -modes delivery-rollback -count 200 -rounds 3

Each transaction follows DELIVERY's statements but rolls back, preserving the
pending-order fixture for every endpoint. Returned district SUMs match Go in
every sample. Builds, tests, probes and profilers did not overlap timing.
One-client median TPS before/after/Go is 58.769/58.480/69.193 (-0.49%);
eight-client is 59.310/59.790/74.599 (+0.81%). Paired directions are mixed at
both concurrencies, so this is not an incremental performance win.
Eight-client after/Go median locking-read time is 125.33/100.97 ms, including
waits. The grouped SUM after staging takes 1.47/0.83 ms and the ten customer
updates take 3.14/2.10 ms. These are concrete critical-section paths to profile
next, not proof yet of which internal operation causes the gap. Source already
folds point UPDATE reads into their pessimistic lock response; do not add a
duplicate read/lock shortcut. Aggregate planning still builds both physical
alternatives and copies every catalog table's mutable statistics residency
for its checkpoints; quantify that work before changing the ownership model.

Postcheck session 11287 completed all eleven TPC-C consistency conditions and
the all-customer check reports 30000/0/0 (customers/bad balances/bad delivery
counts). Logs are `/private/tmp/tidb-selection-boundary-{delivery.jsonl,
delivery.err,tpcc-check.log,balances.tsv}`. Exact commands, binary hashes,
validation counts, every timing sample and phase medians are recorded under
`whole_task_followup` in `rust/benchmarks/retained-distinct-baseline.json`.
The original DISTINCT performance samples above remain attributed to f2ff9c88,
not this new release. All build/test/benchmark/postcheck sessions are terminal;
the preserved benchmark SQL servers and Go/PD/TiKV cluster remain running.
Do not attribute the rare 20-second event to the cache fix without reproducing
its causal path.

The cache fix still has no live throughput or stall-resolution claim.
WIP only: no Ready/lint, whole-package completion,
commit, push, server restart, or unrelated deletion. The full goal stays active.

Revision note: completed the whole-task audit, reproduced and corrected a live
Selection schema-boundary panic, preserved negative/control evidence and closed
the fixed-data DELIVERY phase loop. Next work is measured critical-section
SUM/update optimization, with no broad performance or readiness claim.

## Previous increment: asynchronous coprocessor admission


The diagnostic grouped-SUM run completed 60,000 Go-matching operations while
sampling port 53043 for 20 seconds. This is not throughput evidence: all thread
states, including blocked waits and idle workers, were sampled. The sample is
`/private/tmp/tidb-critical-sum.sample.txt`; the workload and parser outputs are
`/private/tmp/tidb-critical-sum-{workload.jsonl,sql.json,producer.json}`. Sampling
completed successfully, but the sampling wrapper exited 141 in its subsequent
text-summary pipeline. Statistics checkpoint copying accounts for little of the
active execution path; the producer instead includes a synchronous
`DeferredReceipts::wait` in coprocessor begin, before the response wait.

Read the pinned Go source at
`/Users/qiliu/go/pkg/mod/github.com/tikv/client-go/v2@v2.0.8-0.20260708122311-01bd8f99f4da/internal/client/client_batch.go`,
`sendBatchRequest` around lines 1435-1510. It queues an entry and waits for its
response, without an admission receipt round trip. Rust's ordinary coprocessor
begin now uses the existing deferred transport submission. Immutable physical
route identity is recorded in the existing request progress by the sole
in-flight table before sending; completion never waits for a separate receipt,
including when a rejection completes before the worker acknowledges admission.
Removed the three unused call-scoped synchronous submission wrappers.

Publication evidence remains separate from policy. `try_publication` reads
only already-assigned identity; normal distributed-SQL dispatch records early
or late identity once without an admission barrier. An explicitly installed
pre-response observer still requests `publication`'s receipt barrier and runs
before completion. The existing withheld-header and query-observer contracts
remain intact. Snapshot publication order is observation order, not a claim
that asynchronously completing requests were published in that same order.

The route-without-receipt regression failed at runtime with missing publication
identity, then passed. A negative control reinstating only the unconditional
publication read failed the unobserved-query regression; that control was
restored. Two pending unit tests, 65 distributed-SQL tests and 74 batch-related
integration tests pass. Two deadline fixtures previously assumed the worker had
finished before begin returned; they now explicitly observe admission before
inspecting the worker error and still prove no connection or wire request.
Logs are `/private/tmp/tidb-cop-handoff-{red,green,pending,barrier-red,distsql,
batch,batch-final,build}.log`. Commands from `rust`:

    cargo test --offline --locked --release -j12 -p tidb-txnkv --lib rpc::batch::coprocessor::tests::
    cargo test --offline --locked --release -j12 -p tidb-distsql --test all direct_unary_
    cargo test --offline --locked --release -j12 -p tidb-txnkv --test all batch_
    cargo build --offline --locked --release -j12 -p tidb-server

Release 84903e6c14c7b3131cb23be74e6780a5f6e02c614083b9c554d1ce8687058aa3
runs on port 53044, PID 34242, server session 54250. The previous 4b29b04c
release remains on port 53043 and is saved at
`/private/tmp/tidb-cop-handoff-before-XmUyDI/tidb-server`. The live matrix at
`/private/tmp/tidb-cop-handoff-live.jsonl` contains 45 matching successful Go
results and three matching errors; existing EXPLAIN limitations remain.

Timing session 44509 completed all 36 samples and 324,000 measured operations
with no overlapping builds, tests or profiling:

    /private/tmp/tidb-aggregate-isolation -before-port 53043 -after-port 53044 -modes sum,dirty-sum -count 2000 -rounds 3

Outputs are `/private/tmp/tidb-cop-handoff-sum.{jsonl,err}`. Clean SUM median
throughput improves 3.85%/11.49% at one/eight clients, with all six paired
directions positive. Dirty SUM is -1.22%/+2.86%, with all three pairs negative
at one client and positive at eight. A longer one-client repetition completed
45,000 operations at +0.29%, with pairs -0.40%, +1.17%, +0.16%. Thus one-client
dirty SUM has no consistent improvement or regression. Every result matched
Go. The repeat command and complete samples are recorded in
`rust/benchmarks/coprocessor-admission-baseline.json`. Its raw output is
`/private/tmp/tidb-cop-handoff-dirty-repeat.{jsonl,err}`.

Postcheck session 66956 completed all eleven TPC-C consistency conditions and
reported 30000/0/0 customers/bad balances/bad delivery counts. Commands and
logs are recorded in the same artifact. Mixed sysbench session 4367 completed:

    python3 rust/scripts/compare-sysbench.py --server before:53043 --server after:53044 --server go:45000 --database perf_sysbench_locking_v2 --workload oltp_read_write --threads 2 --seconds 20 --rounds 3 --table-size 100000 --output /private/tmp/tidb-cop-handoff-sysbench

All nine samples completed without errors or reconnects. Before/after/Go
median TPS is 438.04/441.51/434.82, nominally +0.79%; paired differences are
-12.86%, +0.79%, -0.75%. Round zero's baseline is 516.00 TPS versus about
438 TPS later. Keep that sample: there is no source-backed reason to discard
it, and these directions do not establish a consistent transaction-level win.
Observed sample maximum latencies span 17.06-32.99 ms; this does not resolve
the historical rare 20-second event. Raw samples and commands are in
`/private/tmp/tidb-cop-handoff-sysbench/{results.json,w0-r*-*.log}` and the
checked-in-format baseline artifact contains their complete parsed metrics.

All prior build/test/live-comparison/benchmark/postcheck handles are terminal.
Retained SQL servers and Go/PD/TiKV remain running. No control edits remain.
Mixed TPC-C session 19621 completed against ports 53043/53044/45000:

    python3 /private/tmp/tidb-agg-tpcc-compare.py --server before:53043 --server after:53044 --server go:45000 --output /private/tmp/tidb-cop-handoff-tpcc

Logs are `/private/tmp/tidb-cop-handoff-tpcc.log` and the output directory's
per-sample logs and `results.json`: 180,000 measured transactions plus 6,000
warmups, all five transaction kinds, zero errors. Median changes at two/eight
clients are -1.84%/+8.22%; paired directions are mixed, so neither is a
consistent whole-workload win. The data evolves and the RNG is time-seeded.
Postcheck session 39700 passed all eleven conditions and reported
30000/0/0 customers/bad balances/bad delivery counts. Exact commands and
complete samples are in `rust/benchmarks/coprocessor-admission-baseline.json`.

Static inspection found that `RemoteRowCursor::next_remote` and `next_staged`
clone buffered rows for merge inspection. Go's `UnionScanExec::getOneRow` in
`pkg/executor/union_scan.go` uses its buffered row and advances a cursor.
The completed 20-second diagnostic sample at
`/private/tmp/tidb-cop-handoff-dirty.sample.txt` shows row cloning is a minor
cost: `CopRowStream::next_row` has 9628 inclusive observations, channel receive
9430, and Datum cloning 64. These are inclusive diagnostic observations, not
throughput evidence or percentages of active CPU. Do not prioritize cloning
as the root bottleneck.

The actual Go/Rust clean/dirty plan comparison is recorded in
`/private/tmp/tidb-dirty-order-diagnostic-plan.jsonl`; all results match.
Separate diagnostic server port 53045, session 36172, runs 84903e6c with
`TIKV_QUERY_TRACE=1` (never a benchmark endpoint). Its log is
`/private/tmp/tidb-dirty-order-diagnostic-server.log`. Requests prove dirty SUM
has no remote aggregation and sets keep_order=true, despite Rust EXPLAIN
claiming cop HashAgg and keep order false. Nine tasks dispatch in pairs; clean
SUM dispatches all nine before receiving results. Go `request_builder.go`
lines 91-105 explains the two-worker rule; do not bypass that policy. The root
fix is removing the unordered reader's dependence on ordered overlay merging.
Keep ordered consumers unchanged and suppress snapshot keys shadowed by any
staged value or tombstone, as Go `UnionScanExec::getSnapshotRow` does.
WIP only; no Ready/lint, package completion, commit or push. The goal remains
active.

Revision note: moved normal coprocessor admission off the receipt wait,
preserved explicit observation semantics, verified the transport boundary,
and completed fixed-data, repeat, mixed-sysbench and consistency checks.

## Previous increment: unordered staged overlay


The candidate at this checkpoint was release
9760a17f8483f0eb2da3e160bac6ddb33fd7097720260f5b26b3285045e928ed,
port 53046, PID 17917, server session 36994. Before is 84903e6c on port 53044,
saved at `/private/tmp/tidb-dirty-order-before-SAMx5L/tidb-server`. Both
diagnostic servers created in this increment on port 53045 were stopped after
capture; no existing server was stopped. All controls were restored before
the final tests and release build. Three production/test files changed in
this increment: `kv_table/table_scan.rs`, `cluster_storage.rs`, and
`remote_scan.rs` under `rust/crates/tidb-executor/src`.

The complete command/log/sample receipt is
`rust/benchmarks/unordered-overlay-baseline.json`. Fifty-three scoped tests
pass, both negative controls fail at runtime, and all 324,000 fixed-data plus
45,000 clean-repeat query results match Go. Eleven integer/composite overlay
cases and four additional streaming-favoring cases match Go; 48 existing
DISTINCT/HAVING cases match Go results/errors. This remains WIP, not Ready,
package completion, or proof of overall TiDB semantic parity.

Mixed sysbench completed all nine samples, zero errors/reconnects: before,
after, Go median TPS 411.47/415.86/408.39. The nominal +1.07% has pairs
-5.85%, +2.79%, -8.40%; no consistent transaction-level improvement.
Mixed TPC-C session 99370 completed successfully:

    python3 /private/tmp/tidb-agg-tpcc-compare.py --server before:53044 --server after:53046 --server go:45000 --output /private/tmp/tidb-dirty-order-tpcc

Log: `/private/tmp/tidb-dirty-order-tpcc.log`; all eighteen samples are in the
output directory's `results.json`. All 180,000 measured transactions completed
without errors. Median changes are +11.89%/+1.19% at two/eight clients, but
paired directions are mixed; no consistent whole-workload win. The subsequent
eleven consistency conditions passed, with 30,000 customers and zero balance
or delivery-count mismatches. Data evolves and the tool RNG is time-seeded.

The range UPDATE in `/private/tmp/tidb-dirty-order-live-control.jsonl` exposed
an independent defect: error 8175 on both 84903e6c and 9760a17f while Go
succeeded. Both transactions rolled back; no memory limit was raised. The
separate point-write setup verified only overlay behavior. The ranger then
lowered tuple-IN only as the sole conjunct; its write consumer scanned the
whole table before filtering and memory accounting. The next increment below
addresses this shared range boundary using Go's composite-prefix/suffix rules.
The dirty aggregate EXPLAIN still misrepresents its local partial fallback
as remote aggregation; it is not evidence of actual operator placement.

Revision note: removed unnecessary ordering from staged table reads without
changing Go's concurrency policy; verified the new merge and isolated gain,
retained mixed-workload uncertainty and recorded the independent DML gap.

## Previous increment: composite predicates within conjunctions


The shared ranger change in `index_range.rs` selects correlated tuple-IN/OR
point prefixes and appends suffix ranges, following Go
`pkg/util/ranger/detacher.go::extractBestCNFItemRanges`. Regression coverage in
that file and `remote_scan.rs` proves bounded reads and retained prefix-index
residuals. The candidate is release
53acb9f453cc0f1632bf0eef002854c9557a158cc2e366b75be134aea118b4ed on port
53047, PID 30944, server session 15449, log `/private/tmp/tidb-cnf-range-server.log`.
The same binary is saved at
`/private/tmp/tidb-point-conversion-before-vYY819/tidb-server` for the next
before/after comparison; its SHA was verified after copying.
Before is 9760a17f on port 53046, retained at
`/private/tmp/tidb-cnf-range-before-7YsQrg/tidb-server`.

The exact previously failing UPDATE now succeeds without memory-limit changes,
and its DELETE and eleven result cases match Go. Evidence and exact commands:
`rust/benchmarks/composite-cnf-range-baseline.json`. The 99 scoped passes,
one existing estimate failure and two ignored ranger cases remain the WIP
test verdict. Release completed in 1m39s with warnings; no Ready/lint claim.
All benchmark and postcheck handles for this increment are terminal. The
eleven TPC-C consistency conditions completed, and the all-customer check
reports 30000/0/0. No server was stopped, and no fixture mutation was committed.

The isolation helper's `suffixed-sum` mode completed all nine samples, 270
Go-matching results. Baseline median latency is 4046.97ms/query; candidate
0.935ms/query. The candidate's three short samples span only 27-33ms and
cannot establish precise Rust-versus-Go performance. A separate candidate-only
repeat checks 15,000 results over three 5000-query samples and confirms median
0.832ms/query. Commands and every sample are recorded in the baseline JSON.
The unrelated clean/dirty SUM run completed 324,000 Go-matching results with
mixed timing directions. Do not conflate these measurements or claim a mixed
workload speedup from the isolated full-scan removal.

The next increment below implements actual point-task conversion and uses this
candidate as its before control. Go's
`find_best_task.go:2193-2260,3089-3200` converts eligible chosen complete-key
ranges into a root BatchPointGet task with capped access count and retained
residuals. Rust's chosen handle-range arm in `driver/access.rs` only publishes
single-point root metadata while leaving the table scan installed;
`write_read_path` also falls back to ranges when its AST fast paths decline.
`TableScanExec::accept_handle_ranges` stores ranges, while `HandleSourceExec`
performs `stored_records_batched` at open. Add an execution-boundary regression
that distinguishes them. Address this at the shared access-path boundary, with actual
batched execution and proper cost/task identity, not an EXPLAIN-only rename.
Preserve Go's fix-control, schema, full-key, prefix-index, partition and ordering
eligibility rules. Dirty aggregate EXPLAIN and the existing estimate mismatch
remain separate open issues. No commit or push; the overall goal stays active.

Revision note: completed the prior mixed TPC-C receipt, verified the composite
range failure against real TiKV, and identified the remaining source-owned
point-task conversion gap.

## Previous increment: range-derived point execution


Candidate SHA256 is
7592762e6ef218bfeca8d1520748c841a8a6da0373e10fe602d19c09bb24f79b,
port 53048, PID 42413, server session 31788. The server log is
`/private/tmp/tidb-range-point-server.log`. Before is the retained 53acb9f4
on port 53047. Keep both servers and the Go/PD/TiKV cluster running.

The final build completed; live correctness checks are terminal and pass.
`/private/tmp/tidb-range-point-live.jsonl` contains 16 mutation receipts and
22 result receipts: Go and Rust each reject both residual writes with zero
affected rows, accept UPDATE/DELETE with ten rows each, and match all eleven
queries. All writes roll back. Stream-favoring checks, 48 DISTINCT/HAVING
cases per endpoint, and clean/dirty aggregate comparisons also match Go.
The three exact-key live plans use Batch_Point_Get for SELECT, UPDATE and SUM.
This is not a claim of complete operator, cost, or partition-routing parity.

Fixed-data benchmark session 76068 completed this repository-root command:

    /private/tmp/tidb-aggregate-isolation -before-port 53047 -after-port 53048 -modes sum,dirty-sum,suffixed-sum -count 2000 -rounds 3

Output is `/private/tmp/tidb-range-point-sum.jsonl`, with empty adjacent
`.err` file. All 54 samples and 486,000 measured queries match Go. Full-key
SUM improves 8.54% at eight clients; paired gains are 5.17%, 8.54%, 12.79%.
One-client median gain is 5.75% but directions are mixed. Clean SUM medians
are +0.44%/+0.01% at one/eight clients; dirty SUM -0.71%/-0.03%. All three
one-client dirty samples are slower (-4.93%, -0.03%, -0.71%); retain this
evidence rather than declaring all unchanged paths unaffected.

Mixed sysbench session 23843 completed the following command:

    python3 rust/scripts/compare-sysbench.py --server before:53047 --server after:53048 --server go:45000 --database perf_sysbench_locking_v2 --workload oltp_read_write --threads 2 --seconds 20 --rounds 3 --table-size 100000 --output /private/tmp/tidb-range-point-sysbench

All nine samples report zero errors/reconnects, but candidate median TPS
regresses 9.68%. The first candidate sample has a 20024.34ms transaction and
31.11 TPS. Preserve it. TiKV logs a region 1393 split and two CheckTxnStatus
EpochNotMatch responses at 19:48:11.426 +08:00 within that sample. This is
correlation, not a root-cause claim; the older cache repair did not establish
resolution of this rare stall.

Diagnostic session 40466 completed `python3 /private/tmp/tidb-range-point-stall-probe.py`:
120 seconds of the original mixed sysbench shape on port 53048, with a six-second
macOS stack sample triggered only if the one-second report shows zero TPS.
Artifacts use `/private/tmp/tidb-range-point-stall`. No stall/capture occurred;
44,340 transactions completed with maximum latency 117.09ms. One default
sysbench ignored error occurred at second 89 (identity not printed), so this
is not a clean correctness receipt or performance baseline. Do not infer
the earlier stall is resolved.

Mixed TPC-C session 90675 completed:

    python3 /private/tmp/tidb-agg-tpcc-compare.py --server before:53047 --server after:53048 --server go:45000 --output /private/tmp/tidb-range-point-tpcc

Log: `/private/tmp/tidb-range-point-tpcc.log`; all receipts are in the output
directory's `results.json`. All 180,000 measured transactions plus 6,000
warmups completed with zero errors. Median changes are +2.15%/-1.17% at
two/eight clients; both have mixed paired directions. No consistent mixed
workload gain. All benchmark, diagnostic and postcheck handles are terminal.
Postcheck session 49009 passed all eleven TPC-C conditions and reports
30,000 customers, zero bad balances and zero bad delivery counts. Logs are
`/private/tmp/tidb-range-point-tpcc-check.log` and
`/private/tmp/tidb-range-point-balances.tsv`. No servers were stopped; no
source changes, commits or pushes occurred during this validation increment.
Do not overlap builds/live SQL/profiling with timed benchmarks.
For the next stall investigation, compare Go `internal/locate/region_cache.go`
`newRegion` and `store_cache.go` `Store.initResolve` against Rust
`tidb-txnkv/src/pd_loader.rs` `hydrate_region`: Go reuses a resolved store,
while Rust issues GetStore for each replacement region's distinct peer stores.
Rust `BackgroundRegionCache::on_region_error` already hydrates outside the
canonical cache mutex; do not repeat the earlier fix or add a second store
cache. First establish a request-count/ownership regression with the existing
loader fixtures and capture the live blocked stacks if reproducible. The
extra PD requests are source evidence, not proof of the 20-second event.
Update `rust/benchmarks/range-point-conversion-baseline.json` with measured
evidence, not expectations. Existing estimate/extra-Sort failures, dirty
aggregate EXPLAIN, and broader cost/routing fidelity remain open. WIP only;
no Ready/lint, package completion, commit or push.

## Previous increment: canonical store reuse during epoch recovery


The source now carries canonical store metadata through the existing recovery
plan. Absent stores are resolved once across split children; existing stores
retain health, generation and pending-refresh state at epoch publication.
The runtime request-count and false-refresh regressions have red/green proof.
106 scoped tests pass; differential transaction wrappers compile. Exact
commands and limitations are in `rust/benchmarks/epoch-store-reuse-baseline.json`.

The release build completed in 1m31s, log `/private/tmp/tidb-epoch-store-build.log`.
Candidate SHA256 is 8e153009837e963a8fd89a6847eb640f3a04eeb57ba573e2852b0da866d9ba92,
port 53049, PID 43140, server session 32065. Live affected-row/eleven overlay
queries and 48 DISTINCT/HAVING cases match Go. Strict-error split diagnostic
session 70165 completed: 39,360 transactions, zero errors/reconnects, maximum
latency 25.78ms. All three splits at keys 50137, 50197 and 50256 succeeded.
Events are in `/private/tmp/tidb-epoch-store-split-live.events.log`. The postcheck
COUNT/MIN/MAX/SUM aggregates match Go exactly; this evolving fixture contains
100,013 rows spanning 1..100278 (the initial Go preparation's gaps can be
filled by normal sysbench DELETE/INSERT pairs). All test/build/diagnostic
handles are terminal. Mixed sysbench session 71596 also completed; all nine
samples report zero errors/reconnects, but candidate median 432.10 TPS is
2.28% below before 442.18 TPS (Go 436.80). All three paired directions are
negative: -3.40%, -1.49%, -0.28%. No rare stall occurred. Results are in
`/private/tmp/tidb-epoch-store-sysbench/results.json`; the checked-in baseline
records the exact command. This is not speedup evidence or proof that epoch
recovery caused the observed timing difference.

TPC-C comparison session 22409 completed this repository-root command:

    python3 /private/tmp/tidb-agg-tpcc-compare.py --server before:53048 --server after:53049 --server go:45000 --output /private/tmp/tidb-epoch-store-tpcc

Output log: `/private/tmp/tidb-epoch-store-tpcc.log`; per-sample logs and the
`results.json` are in the output directory. All 180,000 measured transactions
plus 6,000 warmups complete without error. Two-client median is -2.97%, with
paired directions +3.85%, -5.57%, +10.41%; eight-client median +12.70%, with
all pairs faster (+10.28%, +37.40%, +4.55%). The evolving shared fixture and
time-seeded transaction mix limit attribution; do not claim broad speedup.
Separate profile sessions 52862/84398 complete without errors after timing.
Both use 15-second macOS all-thread-state samples; TPC-C runs 20,000 extra
transactions, sysbench runs 45 seconds with ignored errors disabled.
Postcheck 35375 completes the eleven TPC-C conditions and all-customer check
(30000/0/0). Exact commands and artifact paths are in the baseline JSON.
The before server remains 7592762e on port 53048/PID 42413/session 31788; its
verified binary is saved in `/private/tmp/tidb-epoch-store-before-I7hBr7/tidb-server`.
Keep all retained servers and the Go/PD/TiKV cluster running. No mixed-workload
gain or stall-resolution claim.

The before strict-error diagnostic added three physical split boundaries at record
keys 50167, 50226 and 50286 in `perf_sysbench_locking_v2.sbtest1`. All three
splits completed, no rows were deleted by splitting, and 36,324 mixed-write
transactions completed without error. The rare 20-second stall did not recur;
its root cause remains open. Diagnostic throughput is not a baseline.

Continue causal investigation of the rare 20-second event. Do not reduce
timeouts, discard slow samples or claim the extra GetStore calls explained
that event. The shared metadata-loader lock and its callers remain a source
lead if a future blocked-stack capture points there. No Ready/lint, package
completion, commit or push; the whole-workload optimization goal stays active.

## Previous handoff: copy-on-write catalog schema snapshots


The preceding measurement/profile loop is terminal. The new regression in
`tidb-executor/src/driver/catalog.rs` requires a cloned snapshot to share
table-name allocations until mutation, preserve an untouched database's map
when another database changes, and keep row writes, cross-database rename,
drop and version changes private. Session 66084 fails at runtime on old map
copying (`/private/tmp/tidb-catalog-snapshot-red-v2.log`); session 64345 passes.
The first compile attempt used incorrect fixture API names, not runtime red.
The implementation extends existing table-entry copy-on-write through
database entries and the outer schema map; all mutable database access uses
one `database_mut` helper. No second mutable catalog, new lock or workload
special case. Go's `infoschema/builder.go:getSchemaAndCopyIfNecessary` also
copies only the changed database, while `Session.GetInfoSchema` retains the
selected snapshot. Scoped validation 90730 is terminal: final regression,
catalog versions, transactions, savepoints, statement rollback, temporary
tables, sequences, prepared plan cache, views and union scans. Per-scope
logs are `/private/tmp/tidb-catalog-snapshot-*.log`. DDL/statistics tests
complete too: 141 scoped passes total, one existing manual benchmark ignored.
Release build 13635 completes in 1m35s. Candidate SHA256 is
701d7abb16bd8b88feffa1014d0fbec78c1049bab007ec52d00836196bbd1d7a,
port 53050/PID 28926/server session 12701. Live session 28799 is terminal:
16 affected-row receipts, 22 overlay result receipts and 96 DISTINCT/HAVING
records all match Go; both error logs are empty, all mutations rolled back.
Before is 8e153009 on port 53049/PID 43140/session 32065, preserved at
`/private/tmp/tidb-catalog-snapshot-before-ci5NOO/tidb-server` with verified SHA.
Paired sysbench session 97574 completed this repository-root command:

    python3 rust/scripts/compare-sysbench.py --server before:53049 --server after:53050 --server go:45000 --database perf_sysbench_locking_v2 --workload oltp_read_write --threads 2 --seconds 20 --rounds 3 --table-size 100000 --output /private/tmp/tidb-catalog-snapshot-sysbench

All nine samples report zero errors/reconnects; before/after/Go medians are
396.89/387.80/389.95 TPS. Candidate median is -2.29%, with mixed pairs
(-2.13%, +5.85%, -6.72%); no sysbench speedup. TPC-C session 41244 completed:

    python3 /private/tmp/tidb-agg-tpcc-compare.py --server before:53049 --server after:53050 --server go:45000 --output /private/tmp/tidb-catalog-snapshot-tpcc

Log: `/private/tmp/tidb-catalog-snapshot-tpcc.log`. All 180,000 measured
transactions plus 6,000 warmups completed without error. Median changes are
+1.25%/+3.20% at two/eight clients, but paired directions are mixed:
[-2.55%, +1.25%, +9.75%] and [-2.13%, +3.20%, -11.97%]. Postcheck session
5727 passes all eleven TPC-C conditions and reports 30000/0/0 customer checks.
No broad speedup claim. The earlier sysbench diagnostic used default
TLS while the timing helper explicitly disables it; do not treat that
profile's transport proportions as representative. The diagnostic helper
has been corrected. The TPC-C profile used matching command arguments.
Do not overlap builds, live probes or profiling with benchmarks.

All test/build/live/benchmark/postcheck and profile handles are terminal.
An idle baseline sample (session 28733, 15 seconds, PID 43140) completed after
timing; `/private/tmp/tidb-retained-idle-53049.sample.txt` is mostly waits.
Keep the observed under-load CPU spike separate from this quiet sample.
The baseline JSON records the subsequent cumulative CPU interval too.
No server was stopped; user approval to stop older task-owned benchmark
servers is pending. If approved, resolve the exact owned PIDs before stopping
them, preserving Go/PD/TiKV and ports 53049/53050. Do not blanket-kill servers.
Next close the measurement-environment loop with controlled under-load CPU
accounting. The source survey also found that `tidb-expr/collation_derive.rs`
`ret_type_of` clones `Expression::static_type()` for read-only consumers,
including every sort comparison. Go Column/ScalarFunction.GetType returns
the existing type pointer. No expression change has been made; verify the
fallback and original tests before selecting that next optimization. Rare
20-second stalls and prior planner estimate/EXPLAIN gaps remain open.

Revision note: completed epoch-store measurements, implemented and validated
catalog snapshot ownership, retained all mixed timing results, and separated
the under-load CPU observation from quiet-state evidence. No readiness claim.

## Current handoff: controlled performance diagnosis


The goal remains a generic sysbench/TPC-C speedup, not a local packet or
construction-time microbenchmark result. Its status is BLOCKED pending
explicit fixture-write approval, not complete. The same approval boundary
persisted for three consecutive goal turns; unaffected diagnostics are done
and the third-turn process/output audit confirms nothing remains running
except the preserved test servers. No production Rust optimization was added
in the current diagnostic increment, and no commit or push occurred.

Candidate 68730b1de1185340661edf516676c8002ce8e1818cf037a247fbc77aa99efee5
remains on port 53052/PID 24677/server session 59794. Before 4f081c5f remains
on port 53051/PID 17058/session 25228, saved at
/private/tmp/tidb-frame-write-before-TnXtCM/tidb-server. Preserve these and
Go 45000 / PD 43379 / TiKV 61160. No server was stopped. HEAD and the last
live remote check both equal d04d200a0e on hparser-integration; the shared
working tree is dirty.

The packet/TLS increment's 34 scoped checks, live Go comparisons, mixed timing,
profiles and consistency checks are terminal and recorded with exact commands
in rust/benchmarks/frame-write-baseline.json. Sysbench median +1.55% had two
slower pairs; TPC-C two/eight-client -8.70%/-1.69% had all six pairs slower.
Do not dismiss those results as noise. Plans and statistics matched in the
subsequent read-only snapshot, not necessarily throughout the timings.

The pinned go-tpc driver now accepts optional GOTPC_TPCC_INPUT_SEED for both
process-level constants and per-worker random streams. Unseeded defaults and
transaction logic are unchanged. The fresh-process regression failed before
the patch and passed afterward; package checks, build and patch round-trip
check passed. Exact source revision, binary/patch hashes and commands are in
rust/benchmarks/tpcc-input-seed-baseline.json. The original tool is preserved.

rust/scripts/compare-tpcc.py rotates before/after/repeated-before/Go, checking
identical transaction-type counts for equal seeds. It controls inputs, not
database state, timestamps or scheduling. The intended 320000 measured plus
8000 warmup run did NOT start: auto-review rejected persistent shared-fixture
writes before process creation. There is no benchmark handle or output yet.
Explicit user approval was requested asynchronously; preselection is not
approval. Do not retry or substitute another mutating command without consent.
After approval, use the exact recorded command and repeat both consistency
checks from frame-write-baseline.json; do not reset or drop the fixture.

Unaffected work: the ignored statement_context_construction_cost_probe in the
existing lifecycle suite ran to terminal exit 0 in session 36770. Five samples
of 10000 operations give medians 1694.64 ns query context, 1812.63 ns DML
context and 27167.18 ns in-memory SELECT 1. Go reuses eligible context state;
Rust constructs some defaults subsequently replaced by session-owned channels.
The local timing does not prove this is the mixed-workload bottleneck or
justify stacking another production micro-optimization. Exact evidence is in
rust/benchmarks/statement-context-cost-baseline.json.

The separate approved data-free packet diagnostic is TERMINAL in session 44551:
python3 /private/tmp/tidb-frame-write-command-compare.py --server before:53051
--server after:53052 --server repeat_before:53051 --server go:45000 --count
50000 --rounds 4 --output /private/tmp/tidb-frame-write-command-control.
It sends only PING and SELECT 1, across explicit plaintext/TLS, with no table
access or persistent data writes. Logs are the output directory/results.json
and /private/tmp/tidb-frame-write-command-control.log. All 64 samples and
3.2 million measured commands completed without error. PING medians improve
8.29% plaintext and 2.73% TLS; all plaintext PING pairs improve against both
baseline observations, and TLS improves against all repeated baselines.
Plaintext SELECT 1 is -1.22% with mixed signs; TLS SELECT 1 is -0.74% and
slower against both baseline observations in all four rounds. That result
batch still uses unchanged scalar writes; a vectored-write cause is not
isolated by the evidence. Full samples, control deltas and interpretation are
in rust/benchmarks/frame-write-command-control-baseline.json. No performance
job remains running, only the preserved servers. Do not launch mixed writes
until the requested user approval arrives.

The follow-up read-only state probe is terminal: python3
/private/tmp/tidb-frame-control-state.py writes 36 records to
/private/tmp/tidb-frame-control-state.jsonl. Every connection exposes the same
948 session and 948 global variables; all 30 SQL comparisons agree. SELECT 1
plans, TLS cipher/version and same-mode client socket settings match. Variable
values are compared in memory, not saved. Both binary hashes were rechecked.
The scalar result-batching path remains unchanged. These observations narrow
current-state explanations, not the historical regression's causal boundary.
The second goal turn at the unapproved fixture-write boundary completed these
unaffected read-only checks; the third confirmed the same blocker and marked
the goal blocked. Explicit consent, not automatic continuation, is needed
before the mixed benchmark can resume. No production edit,
timed benchmark, test/build, server stop, or persistent database write occurred.

The prior type-read regressions, planner estimate/EXPLAIN gaps and rare
20-second stalls remain open. Validation is WIP: no Ready/lint, whole-package
completion or broad performance claim.

Revision note: record the tested seed driver and the explicit fixture-write
approval boundary; replace stale claims that no driver change exists; retain
negative throughput evidence and close both diagnostic handles with measured
results, without promoting isolated PING gains to mixed-workload success.
Record the terminal state-equality probe and retain the explicit pending
fixture-write boundary instead of inventing another production optimization.
Record the third-turn blocked audit and actual goal status; no work is claimed
complete and no rejected mutation has been retried.
