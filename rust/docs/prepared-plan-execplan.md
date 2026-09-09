# Reuse parameter-aware physical plans

This living ExecPlan follows root `PLANS.md`.

## Purpose / Big Picture

Improve Rust sysbench/TPC-C throughput and latency through generic Go-aligned ownership,
planning and batching changes. Go source defines semantics, evaluation order,
diagnostics and lifecycle. No query-specific fast paths or throughput claims
based only on CPU samples or fewer tests.

The current cleanup removes tests invented for Rust implementation details,
including protection gates. Retain original Go test ports and their fixtures;
test names and Go production-source citations alone do not establish lineage.

## Progress

- [x] Foreground batch completion WIP: five transaction batch paths now share
  `complete_published_batch`, admitting every region before response waits.
  The success path no longer forces an Inspect/publication barrier. Failures
  retain the ordered observation so an unpublished error is distinguished from
  a possibly applied request. The invalid-endpoint witness reproduced the old
  publication `expect` panic and now covers BatchGet, Prewrite, Commit,
  PessimisticLock, PessimisticRollback and single Commit. Consolidated validation
  passes 155 unit and 417 topology-independent tests (11 ignored) plus the
  release server build; no live speedup or package-completion claim. Recovery,
  candidate binary and logs:
  `/private/tmp/tidb-foreground-completion.ujPWR6/`.
  The live follow-up executes 528000 read-only transactions across 1/8/32
  clients, with identical Go/control/candidate/drain check-query outputs and no
  errors or reconnects. Severe same-binary drift prevents acceptance: the
  eight-client saved control moves from 1603 to 788 TPS. Separate PID-filtered
  CPU profiles and source review identify an obsolete transaction actor
  boundary. Evidence: `rust/benchmarks/foreground-completion-observations.json`.
  All owned services are stopped; fixture data, binaries and raw traces remain.

- [x] Direct transaction ownership WIP: remove the request enum, reply channels,
  pinned transaction worker and detached statement-finish message together.
  `SessionSnapshot::get` now calls the existing coordinator directly, matching
  Go's `KVTxn.Get -> union-store -> KVSnapshot.Get` ownership. One synchronized
  state preserves snapshot sharing; only its owner can take the state for
  commit/rollback. Get/BatchGet/Scan timestamps, lock-cache eligibility,
  pessimistic lock/retry logic, heartbeat retention and commit decisions remain
  shared with the existing implementation. Read-only finish is a local state
  transition, so it needs no worker. The follow-up consumer-decoding migration
  removes the coprocessor scan pool; see `cop-scan-worker-execplan.md` for its
  current validation and remaining lower-layer scheduling gap.
  Two tests asserting the removed worker design are deleted, without replacement
  protection gates. Consolidated checks pass 330 exec unit and 722 exec target
  tests (one ignored); server checks have 156 passes and five failures identical
  to the saved pre-migration source. Release server build passes. Live Go/Rust
  cases match for optimistic/pessimistic commit, rollback, repeatable reads,
  blocking locks and disconnect cleanup. All 28 equal-work trials complete
  (168000 measured read-only transactions, no SQL errors/reconnects). Median
  throughput improves 3.75/5.83/5.30 percent at 1/8/32 clients; eight-client SQL
  CPU falls 18.35 percent. Every candidate TPS sample exceeds every control
  sample at its tier. Record this scoped local gain in
  `rust/benchmarks/direct-transaction-baseline.json`; TPC-C, whole-package
  completion and Ready validation remain open. The last 32-client p95 sample
  regresses even though the tier's median p95 improves; no universal tail claim.
  Recovery, binaries, lifecycle scripts and logs:
  `/private/tmp/tidb-direct-transaction.Tz3RgL/`.

- [ ] Detached secondary progress WIP: Go `spawnWithStorePool` runs independent
  continuations. Rust's blocking process-global FIFO and client-mutex-held
  response wait are replaced by retained runtime tasks, using the existing
  completion state and native task wakeups. Submission clones only the request
  capability and retains the owning authority until terminal completion. The
  deadline, failure metric, identity and primary-committed outcome are unchanged.
  The held-response wire witness failed before implementation: after two seconds
  the second transaction had not reached TiKV and the first authority was locked.
  Afterward the second arrived in 1278 us while the first response remained held,
  with the first authority available. The historical OS-thread-count protection
  test is removed. Final WIP validation passes 20 RPC unit and 417 topology-
  independent tests (11 ignored) plus the release build. Existing completion
  tests cover response-before-poll, response-after-poll, cancellation wake and
  release of the task waker. This proves scheduling independence, not a workload
  speedup; mixed-write TPC-C and full performance acceptance remain open.
  Recovery and logs: `/private/tmp/tidb-detached-progress.cAMaPl/`; checked-in
  evidence: `rust/benchmarks/detached-secondary-progress-baseline.json`.

- [x] Live workload refresh WIP: rebuild current Rust and compare Go/Rust over
  one new 100000-row TiKV fixture. Six query outputs match (307 lines), as do
  cache/chunk/scan settings and TLS cipher. One-client median TPS is Go 477.49,
  Rust 474.74; eight-client Go 2032.95, Rust 2071.64. Ranges overlap; no accepted
  Rust speedup. Equal-work trials and separate profiles are retained in
  `rust/benchmarks/live-sysbench-refresh-observations.json`. Go is revision
  23bff313186b8ceb61fcbe9faac43ae700e7fb14, not checkout HEAD.
- [ ] Transport follow-up: equal 30000-event trials show Go 2.11 versus Rust
  1.54 requests/batch, about 22% versus 65% singleton batches. Live Go uses
  standard adaptive policy; Rust only drains ready work. Same-binary Go
  standard/basic controls show 2.07% higher median TPS and 6.66% less SQL CPU
  with standard. The prior blocking Rust adaptive collector already regressed
  eight-client traffic; do not repeat that implementation. Reconcile the
  per-store collection/timer ownership and lifecycle path before a candidate,
  then compare sparse and concurrent traffic, deadlines and cancellation.
  General physical-plan reuse is still a separate open boundary. Owned data,
  binary, supervisor and measurement scripts are retained under
  `/private/tmp/tidb-live-workload.nIrZBe/`; services are stopped after measurement.
  Implementation direction: retain adaptive collection per physical address
  in the existing transport event loop. A pending batch owns its source-policy
  target and deadline; it does not own a blocking collector loop. Other store,
  retirement and lifecycle commands remain serviceable while collection waits.
  Reaching the target posts one queue turn before publication (Go's one
  cooperative yield); expiration publishes without another yield. Preserve
  caller-local validation/receipts and flush preceding work before close or
  inspection. Reuse source request-arrival timestamps and the existing
  BatchTrigger, with no SQL/workload knobs. Validate source-policy decisions,
  lifecycle/cancellation/expiry/forwarding and live sparse/concurrent traffic
  together, using the saved immutable Rust binary as immediate control.
  Recovery: `/private/tmp/tidb-batch-owner.Lf2c27/before-source.tar.gz`.
  WIP result: per-store collection is implemented, including timer cancellation
  at the target and a final post-yield drain. The source-policy timer regression
  failed before refinement. Consolidated validation passes 10 transport unit,
  75 batch, 2 concurrency, 1 cancellation and 2 unary tests (one overlap), plus
  the release server build. No Ready lint or package-completion claim.
  The refined eight-client run reduces SQL/TiKV CPU by 14.87/20.26 percent but
  lowers throughput 3.07 percent and increases mean latency 4.185 to 4.315 ms.
  The 32-client control drifts substantially; neither candidate is accepted.
  Six query outputs match Go/control/candidate before and after both windows.
  Separate Running-only CPU captures confirm lower transport-owner CPU weight,
  but do not isolate per-request waiting latency. The installed sysbench's
  zero p95 was traced to dropped atomic instructions in bundled AArch64 inline
  assembly: macOS treats semicolons as comments. Replacing instruction
  separators with newline escapes in an isolated pinned build restores the
  increment and histogram; a controlled-delay probe proves the difference.
  Fresh equal-work trials with that tool still lose 3.51 percent throughput
  and add 3.66/3.72 percent mean/p95 latency at eight clients. The 32-client
  direction improves, but does not override the regression. The comparison
  script now records latency and executable identity, and rejects zero p95.
  A separate nine-run Go/control/candidate comparison validates that runner.
  Evidence: `rust/benchmarks/sysbench-latency-validation.json`. A temporary
  collector probe now measures target completion at about 33 us and deadline
  completion at 119 us for eight clients; it has been removed byte-for-byte
  from production source. Go's extra collection mean is 31 us across all
  batches (wait-more minus wait-head); these populations are not identical.
  Source inspection finds that Rust's submit/flush/send/recreate chain has no
  asynchronous leaf, yet publication drove it with Runtime::block_on. The
  entire chain is now synchronous, matching Go getClientAndSend; only the
  stream I/O task is spawned on the explicitly supplied runtime. The lazy
  connection constructor keeps its own necessary runtime context. Consolidated
  WIP validation passes 89 distinct transport checks and the release server
  build. Equal-work before/after/after/before trials show median throughput
  changes of +1.94/+0.51/+1.28 percent at 1/8/32 clients; mean latency falls
  at each tier, while eight-client p95 is unchanged and SQL CPU rises 0.25
  percent. Six query outputs match Go and both Rust controls before and after.
  Two samples per binary per tier remain modest evidence, not acceptance of
  the earlier adaptive-collector regression or the overall performance goal.
  No Ready lint, complete-package claim or TPC-C validation in this increment.
  Results and exact commands: `rust/benchmarks/synchronous-publication-baseline.json`.
  Probe and source recovery: `/private/tmp/tidb-collector-timing.7OPyE8/`;
  live comparison: `/private/tmp/tidb-sync-publish.h3O1nL/`.
  Entry-publication WIP removes the second receipt channel and the owner's
  per-caller packet-ID filtering from ordinary RPCs. Go keeps identity on the
  entry and waits only for the response. Transaction and coprocessor completion
  now use the existing entry publication route; an on-demand owner barrier
  remains only for explicit observers before publication. The public synchronous
  receipt API preserves exact packet/caller boundaries. A valid completed Get
  formerly failed if its separate receipt channel closed; the entry-identity
  witness failed before this change and now passes. Final WIP validation passes
  20 RPC unit and 416 topology-independent tests (11 ignored), plus release
  build. The broad test run exposed one test's process-global failure-counter
  synchronization race; waiting for its own fixture's secondary Commit resolves
  that failure without changing production transaction behavior. Initial paired
  throughput changes are +2.34/+7.09/+2.75 percent at 1/8/32 clients, but the
  eight-client control varies 7.4 percent. The final cleaned binary's separate
  eight-client window has an even larger immediate-control outlier (1714 to
  1075 TPS), so it cannot establish a reliable gain magnitude. The candidate
  remains 1.37 percent slower than the stable drain-only control, with mean/p95
  latency 1.47/2.80 percent higher despite SQL/TiKV CPU reductions of 15.92/21.76
  percent. Go/result comparisons pass before and after both windows. Keep the
  overall performance goal open and diagnose control variability before
  interpreting another effect size. Checked-in evidence:
  `rust/benchmarks/entry-publication-baseline.json`.
  Recovery, immutable binaries and raw logs:
  `/private/tmp/tidb-entry-publication.z1hwgp/`.
  General prepared-plan reuse remains open.
  Raw measurements, binaries and profiles are retained; observations are in
  `rust/benchmarks/batch-owner-observations.json`, not an accepted baseline.

- [ ] Explain construction ownership WIP: the current isolated prepared-range
  profile spends 220 of 6751 samples below base-table physical display-name
  construction even without a PlanTrace. Go formats expressions and physical
  names through ExplainInfo, separately from executor construction. Move base
  and derived display metadata into their trace consumers; retain join display
  conditions only for tracing, and install IndexJoin runtime fields before
  optional display construction. Cost estimates and executable null filters
  remain independent of tracing. Initial consolidated checks pass 375 tests,
  with seven previously recorded failures and an additional prepared POW
  metadata failure. The latter passes on retained pre-fold-change binaries:
  an implicit real cast was never folded at its own construction boundary.
  Go WrapWithCastAsReal calls BuildCastFunction, whose normal fold is independent
  of the parent mode. Correct that constructor without restoring recursive
  descendant folding. Final checks pass 376 executor and 164 expression tests;
  seven recorded failures remain. All 21 Go fixture reports are byte-identical
  after timing normalization. Fresh paired profiles remove the display-name
  construction path (244 of 6137 before samples); local paired timings reduce
  range/index/aggregate elapsed time by 3.7/2.5/2.4 percent. Sort's 1.9 percent
  reduction remains qualified against the 1.3 percent point-control shift.
  Baseline: `rust/benchmarks/explain-construction-baseline.json`. Recovery and
  evidence: `/private/tmp/tidb-trace-ownership.flFD9Q/`. Live workload throughput,
  whole-package parity and native prepared-plan reuse remain open.

- [ ] Construction-fold ownership WIP: current prepared-range CPU samples show
  expression rewrite/folding work on the live Session path. Compare Go
  `newFunctionImpl` and `foldConstant`, remove repeated descendant folding and
  unconditional normal-mode warning bookmarks, and defer try-fold replacement
  until warning acceptance instead of cloning the original tree. Validate
  existing expression/SQL fixtures and paired prepared-query timings as one
  batch. Evidence and pre-edit source: `/private/tmp/tidb-live-profile.RItNBU/`.
  The first timing run does not establish a speedup (its first before run is
  consistently faster than both after runs and the final before run). Native
  fold diagnostics remove try-fold tree allocations and the original Go
  `interval(1,0,1,2,1/0)` fixture loses a spurious warning. The next connected
  edit makes column resolution return the complete column in one lookup;
  identity-only statistics callers no longer build expression/name metadata.
  Final validation: 143 expression and 77 executor tests pass, with two
  previously documented executor failures. Twenty Go fixture reports improve
  the INTERVAL warning and are otherwise unchanged. Final paired medians are
  0.5–1.3% lower, including a 0.7% point-control reduction: no independent
  throughput win is accepted. Record:
  `rust/benchmarks/expression-construction-observations.json`. Broader live
  performance optimization and native cache integration remain open.

- [ ] Native IndexJoin inner-table task WIP: unify integer and common-handle
  construction, connect existing dynamic range definitions, retain remaining
  filter selectivity and apply Go's full-key uniqueness cap. Move oversized
  residual IN predicates to root like Go, without moving range predicates.
  The connected flow passes 139 existing scoped tests, 38 row-count/selectivity
  diagnostic cases plus key/filter/order boundaries, and 24 stored-row native
  executions including parameter rebinding and outer-dependent ranges. Sixteen
  complete Go fixture reports are unchanged. Secondary path statistics,
  selection and full ordinary order matching remain required before live
  native SQL/cache activation. Current evidence:
  `/private/tmp/tidb-inner-table.hXS9cU/` (not a before-source recovery archive).

- [x] Static metadata ownership WIP: retain built-in charset/collation names
  directly from static registry data and keep arbitrary spellings shared.
  Update FieldType construction/builders/setters and expression collation state
  together, preserving exact bytes, clone behavior and serialization. The
  remaining prepared-range profile shows construction-time metadata allocation.
  All 272 consolidated tests pass; sixteen Go fixture reports are unchanged.
  Nine isolated metadata construction cases allocate no heap memory. Paired
  query timings improve index/range/aggregate by 2.6–4.9%; sort is inconclusive,
  and a longer point control does not reproduce its initial slowdown.
  Recovery: `/private/tmp/tidb-static-metadata.4I1mRd/`; exact commands, sources
  and measurements: `rust/benchmarks/static-metadata-ownership-baseline.json`.

- [x] Chosen-range ownership WIP: retain each table path's complete ranger result through costing, chosen-
  path conversion, join-leaf construction and write reads. Remove subsequent
  range rebuilds used only to recover residual conditions. All 94 targeted tests
  pass; sixteen complete Go fixture reports are unchanged. Paired isolated
  timings reduce range time by 6.0% and aggregate/sort time by 4.2%; point/index
  controls are unchanged within noise. The after sample retains only the
  enumeration-time ranger call chain. Recovery:
  `/private/tmp/tidb-live-planning.MFcV5P/`; exact commands and raw measurements:
  `rust/benchmarks/chosen-range-ownership-baseline.json`.

- [x] Native IndexJoin range-cache WIP: retain immutable definitions and rebuild current parameter
  ranges before native executor construction. Follow Go's empty/range-count/
  width rejection and unlimited rebuild quota. Carry the definition through
  path feedback and physical-plan cloning; preserve the unchanged definition
  across executions. The connected batch passes 93 targeted tests, 30 changing-
  parameter stored-row executions and the combined native diagnostics. Fourteen
  original Go fixture reports are unchanged. Correct byte-datum quota accounting
  after the original Go 1260-byte test exposed the missing binary collation size.
  Secondary-path
  statistics/selection and live SQL cache activation remain separate gaps.
  Recovery: `/private/tmp/tidb-index-cache-ranges.5PpNvt/`.

- [x] Borrowed range-condition WIP: remove extraction/result deep copies from
  table/column range construction; borrow in live statistics and move owned
  conditions into native table/IndexJoin plans. All 117 distinct targeted tests
  pass; fourteen full Go fixture reports are unchanged. Paired local timings
  reduce range/index/sort elapsed time by 1.7–2.9%; aggregate and short point
  control differences are not accepted independently. Recovery and raw logs:
  `/private/tmp/tidb-range-ownership.lcqtXE/`; exact commands and measurements:
  `rust/benchmarks/range-condition-ownership-baseline.json`.

- [x] Borrowed SELECT rewrite WIP: remove unconditional whole-tree copies from
  derived-wildcard expansion, correlated-aggregate decorrelation and outer-join
  elimination; share derived-child traversal. Preserve rule order, eligibility
  and source metadata. All 14 Go fixture reports are unchanged. Consolidated
  driver checks pass 254 tests; all seven failures reproduce before the change.
  Isolated paired timings improve four non-point prepared-query classes by
  2.9–3.5%, not a live-workload claim. Recovery:
  `/private/tmp/tidb-select-rewrite.8wXe7T/`; raw timings and exact commands:
  `rust/benchmarks/select-rewrite-ownership-baseline.json`.

- [x] Shared type/expression string ownership WIP: an isolated prepared-range
  profile shows metadata clone/drop allocation churn. Match Go's immutable
  string-header copies in FieldType and CollationInfo while preserving exact
  names, setter independence and existing ENUM/SET slice semantics. Both
  owners and serialization are updated; 35 targeted tests pass. Seven prior
  Go topics are unchanged. Clean before/after runs reduce elapsed time by
  9.7–17.7% in four non-point prepared-query classes. This is isolated local
  evidence, not sysbench/TPC-C throughput acceptance. Recovery and profiles:
  `/private/tmp/tidb-prepared-owner.ixmsOs/`; reproducible source and raw
  measurements: `rust/benchmarks/type-metadata-sharing-baseline.json`.

- [x] Exclusive access dispatch WIP: retain Go's statement-level fast-plan
  boundary and ordinary DataSource statistics. Select batch/single/range
  readers through one exclusive chain, retaining the point lookup rather
  than resolving it twice. Reuse one partition-pruning result. Nine targeted
  tests pass; seven original Go SQL topics retain identical output and
  divergence details. Isolated timing is mixed; no speedup is accepted.
  Recovery: `/private/tmp/tidb-fast-plan-order.v0tVwE/before.tar.gz`.

- [x] Live skyline consolidation WIP: move comparison into shared planner
  `find_best_task/candidate.rs`; ordinary access paths and native IndexJoin
  now use it. Preserve actual prefix lengths in skyline and point-path
  heuristics. Remove the retired comparator, debug hook, stale explanations
  and 13 Rust-only tests; retain original Go fixtures. Baseline reproduces
  incorrect prefix-path pruning. Consolidated validation passes 29 access-cost
  tests, two Go lookup suites, 16,384 shared-comparator cases and 13,107 live
  skyline pairs against extracted Go, plus the combined native diagnostics.
  Five original fixture topics retain identical results/divergence details.
  Recovery:
  `/private/tmp/tidb-skyline-unify.gWLnzl/before.tar.gz`.

- [x] IndexJoin path-selection prerequisites WIP: source comparison found that
  ordinary same-typed columns incorrectly match through absent virtual
  expressions, and native IndexReader lacks Go's separate DataSourceSchema.
  Reproduce and correct borrowed identity/resolution; carry immutable reader
  output separately from physical index/handle columns. Add Go skyline and
  IndexJoin NDV/key-coverage comparison. 16,384 normalized candidate comparisons
  match an oracle extracted from Go; combined native diagnostics pass. Recovery:
  `/private/tmp/tidb-index-path-task.8LCYwX/`. Secondary/common-handle candidate
  selection still needs original-table path statistics and scan-task construction.

- [x] IndexJoin path-range WIP batch: port Go's continuous join/static prefix,
  final constant or outer-dependent range, prefix residuals, memory fallback
  and rebuild-mode quota behavior. Reuse canonical ranger evaluation. Both
  ported Go lookup-filter/fallback suites pass, as do 40 parameter/quota/rebuild
  configurations and 126 twice-opened native IndexJoin configurations (84 use
  newly built range templates). Recovery: `/private/tmp/tidb-index-ranges.DWEwIZ/`.
  Skyline/statistics path selection and live cache activation remain open.

- [x] Native IndexJoin property WIP batch: source-derived memo key and explicit
  inheritance, inner-pattern admission, static regular-family construction and
  integer-PK inner scan selection. Preserve unfinished secondary/common-handle
  path analysis and full family enumeration as explicit gaps. Recovery:
  `/private/tmp/tidb-index-property.onXVBe/`. Consolidated diagnostics pass 30
  property/task cases, six selectivity cases, eight static candidates and 126
  twice-opened IndexJoin configurations. Correct duplicate-PK lookup handling
  after reproducing the old one-key-only rejection.

- [x] IndexJoin task handoff WIP batch: carry chosen access-path feedback
  through cop/root tasks, complete lookup/hash keys at attachment, preserve
  per-outer statistics, dispatch the existing Go cost formula, and move owned
  tasks instead of cloning their plan trees at attachment, candidate conversion
  and sort enforcement. Consolidated diagnostics pass 56 completion and 48 cost
  configurations plus the 105 twice-opened stored-row IndexJoin configurations.
  Recovery: `/private/tmp/tidb-index-join-task.H7Wuu4/`.

- [x] Native IndexJoin WIP batch: execution-bearing physical metadata,
  key/output resolution, current-transaction batched readers and per-row
  comparison ranges. Consolidated diagnostics pass 105 configurations opened
  twice; original fixture details are unchanged. Recovery and evidence:
  `/private/tmp/tidb-native-index-join.3Ru38H/`.
- [ ] Finish native IndexJoin candidate enumeration and histogram-backed costs,
  cache admission/rebuild diagnostics, partition routing, inner HashJoin/UnionScan,
  IndexHash/IndexMerge variants and parallel worker lifecycle. The native SQL
  and cache owners are not activated by this executor-building increment.
  Attachment also needs the statement-context decorrelated-alternative marker.

- [x] Aggregate binding WIP batch: use current values/types for aggregate admission,
  bind mutable leaves in the existing request-owned copy for table and covering
  index readers, and consume the partial candidates already built for costing.
  Consolidated diagnostics pass 144 aggregate configurations opened twice;
  original fixture details are unchanged. Recovery snapshot, probe and logs:
  `/private/tmp/tidb-aggregate-binding.Rx6wTh/`.

- [x] Shared scan-lowering WIP batch: remove the live index-join adapter that
  read saved Constant.Value and cloned expressions for column remapping. Native
  readers and all three lookup request paths use context-aware lowering with
  Go's derived collation. Reopen refreshes descriptors without cloning executable
  filters or replacing scratch/immutable fast paths. Context-free scalar
  admission no longer serializes saved mutable values. Recovery and combined
  evidence: `/private/tmp/tidb-scan-lowering.KViq9v/`.

- [x] Native reader WIP batch: connect the physical builder to transaction-owned
  table, covering-index and lookup cursors. Retain ranges/filter definitions,
  refresh mutable values on Open, preserve index/table predicate ownership,
  physical column IDs and output offsets. Combined diagnostic passes 266
  stored-byte reader configurations, including actual native Apply with/without
  its cache, current parameters and outgoing request inspection. Reproduce and
  correct lost zero-column cardinality and a discarded filter on remote-to-local
  reopen. Recovery/logs: `/private/tmp/tidb-native-readers.StFw1N/`.
- [ ] Finish cop operators beyond scan/Selection, filtered/dirty embedded lookup
  limits, native SQL/cache activation and remote workload verification. Reader
  output pruning currently uses the existing column-evaluator Projection when
  filter inputs must survive until local/staged-row evaluation; complete the
  corresponding remote OutputOffsets lowering before performance acceptance.

- [x] Native scan WIP batch: detach access predicates from retained filters,
  preserve table-order index identity after pruning, and retain/rebuild
  correlated table/index access expressions using the existing ranger.
  Promote correlated equality/range prefixes using Go's rules. Combined
  evidence: `/private/tmp/tidb-native-ranges.g7ojup/`. Its initial archive
  covers physical sources, dispatch and this plan, not all later touched files.
- [ ] Connect these native readers to live SQL. Whole-package native scan
  metadata, real histogram costing and statement-context integration remain open.

- [x] Native serial Apply WIP batch: shared correlated datum cells, native
  Apply candidates, statement-bound extraction, Go NoCopPushDown properties,
  and executor construction using the existing tuple Joiner and chunk List.
  Consolidated diagnostics pass; this is seed evidence, not whole-package
  completion. Recovery and logs: `/private/tmp/tidb-native-apply.8dFnea/`.
- [ ] Finish parallel Apply and native SQL/cache integration. The builder
  explicitly rejects parallel Apply; serial correlated readers now execute
  against stored table/index bytes.

- [x] Connected FD/merge-search WIP batch: implement Go dependency projection and
  expression IDs in the existing graph; derive FDs from native logical
  operators; enumerate natural/enforced merge orders with source-derived
  child properties and costing inputs. Consolidate validation after this
  connected batch, not after individual edits. Recovery snapshot and control
  replay: `/private/tmp/tidb-native-fd.hrkUNL/`. Full SQL/cache integration and
  Go's cached per-node FD lifecycle remain separate open work.

- [x] Trace Go plan-cache lookup/range rebuild against Rust planning and
  execution. Prepared constants retain marker identity and current statement
  parameters; immutable physical plans must own reusable definitions while
  executors own per-run state.
- [x] Route expression diagnostics through the existing statement context;
  reuse compiled predicates for statistics, execution and trace where already
  owned. Native ranger integration remains partial.
- [x] Share DataSource StatsInfo with single-table aggregate/DISTINCT parents,
  and use the same table-statistics construction for join cost models.
  Preserve resolver UniqueIDs and Go's linear NDV scaling default.
- [x] Remove confirmed Rust-only source-string guards, refusal-set pins,
  optimizer call-count tests/counters, source-size/doctest-count gates and
  their obsolete tooling/documentation: 85 named tests, two extra cases and
  the shell source-layout guards. Recovery copies are outside the repo.
- [ ] Finish test lineage reconciliation across the workspace. A missing Go
  citation is not evidence that a renamed port has no Go original.
- [x] Remove another 108 Rust-authored tests: 48 planner builder/FROM seam
  tests and 60 expression utility additions, plus four appended cases.
  Keep the 11 expression utility Go-port tests and remove orphaned suite
  registrations and the reference to the deleted test module.
- [ ] Finish compiled predicate/range ownership and share logical planning
  across physical alternatives.
- [x] Bind physical statistics offsets to resolver UniqueIDs once per
  derivation; share that mapping with DataSource NDVs, recursive DNF,
  pseudo predicates and string-match selectivity. Do not retain it across
  pruning or prepared executions.
- [x] WIP-validate that binding change: six Go fixture reports unchanged
  (1095 matches, four pre-existing divergences, 23 skips), 30 live prepared
  result sets match Go. Fixed-work and eight-client throughput differences
  remain below acceptance; no performance win claimed.
- [x] WIP-validate consuming CNF splitting at expression/planner/statistics builders:
  move owned conjunct trees instead of cloning them immediately before dropping
  the originals, preserving Go's left-to-right order.
- [ ] Replace general AST/access-path pin replay with parameter-aware physical
  plan reuse through the existing session cache lifecycle.
- [x] Audit the migration boundary against current Go: Rust's physical
  `resolve_indices` is a traversal-only body; PhysicalHashJoin lacks keys
  and predicates; PhysicalSort carries order identities rather than full
  expressions. The executor does not consume this typed physical tree.
- [x] User approved the cross-module planner/executor/session replacement
  and requested aggressive Go alignment without further permission pauses.
- [x] Batch the typed physical-plan prerequisites: retain sort expressions,
  join predicates/keys, partition and prefix columns, reader output columns;
  implement child-first index binding, duplicate-slot handling, virtual-column
  fallback, aggregate binding and Apply's joined-row equality binding.
  Share the column-bearing order type with logical windows.
- [x] Remove the 22 explicitly Rust-authored physical-tree tests and six
  enforcer tests, including the traversal-only 40,000-node protection test.
- [x] WIP batch validation: four-package release test-target build, a temporary
  combined binding probe, and six unchanged original Go fixture reports
  (410 matches, zero divergences, 16 skips). No per-edit test loop.
- [x] Replace the unused Selection/Projection/Limit plan wrappers with direct
  runtime ownership. Add `physical_builder.rs` for typed Selection, Projection,
  Limit, Sort, TopN, Dual, UnionAll and MaxOneRow, with transaction-owned reader
  construction. Move UnionAll out of AST planning without duplicating it.
- [x] Follow Go's resolved-index LIMIT mapping and TopN's output-column mapping
  in both memory and spill paths. Remove four invented wrapper tests and the
  orphaned `projection/tests/programs.rs` registration.
- [x] WIP runtime batch evidence: the release test-target build and Go TopN
  single-spill port pass; a temporary combined probe checks changed prepared
  values, fresh readers, reopen, cardinality, and projected TopN over 32 runs.
  Eight Go fixture topics have unchanged reports: 642 matches, two pre-existing
  divergences and 28 skips. Reviewed-source release rebuild also passes.
- [x] Lower native aggregate descriptors into the existing live hash/global
  stream/grouped stream operators, preserving final COUNT/AVG argument modes,
  DISTINCT, ordering, separators and per-execution parameters. Do not activate
  the orphaned `stream_agg.rs`; the live stream operators are in `hash_agg.rs`.
- [x] Carry Go DefaultVal eligibility through serial, stream and worker output:
  partial and FIRST_ROW-only empty inputs produce no synthetic row.
- [x] Reproduce COUNT's saved-marker bug (NULL execution returned 3 instead of
  0) and the original Go aggregate fixture's BIT(1) GROUP BY crash. Replace
  saved-payload shortcut eligibility with literal identity; distinguish chunk
  storage layout from integer evaluation type across all direct aggregate reads.
- [x] WIP aggregate-batch validation: release build and expanded runtime probe
  pass, including the COUNT(NULL) regression and BIT/ENUM/SET grouping. The Go
  aggregate fixture completes after the crash correction: 808 matches, 20
  unresolved divergences, 64 skips. Four other topic reports are unchanged.
- [ ] Reconcile those 20 aggregate-fixture divergences: GROUP_CONCAT ordinal
  and parameter ordering, enum/set ordering, join residual evaluation,
  HAVING/GROUP BY alias resolution, DISTINCT variance precision, functional
  dependency validation and constants over empty aggregate input. The saved
  control crashes before finishing this topic; do not label all differences
  pre-existing without separate reproduction.
- [x] Build typed ordinary hash joins from bound keys, residuals, outer filters,
  defaults, build orientation and concurrency. Share inline output mapping
  across live serial/parallel/hash/merge/index paths; retain complete input
  rows for predicates. Resume pure-equality candidates at chunk boundaries,
  support empty hash keys without materializing a Cartesian result, and close
  the uniquely owned hash table without first cloning its Arc.
- [x] Combined native probe: 720 cases pass for pruning, zero-column rows,
  both build orientations, filters, NULL-safe keys, residual parameters,
  defaults, semi/anti, reopen, bounded output and actual spill cleanup.
- [x] Final release rebuild passes (2m29s); all eight rebuilt fixture reports
  retain control counts: 2727 matches, 46 existing divergences, 167 skips.
- [x] Port native `getHashJoins` / `getHashJoin`: shared build shapes,
  current concurrency/version inputs, build/probe hint precedence and warnings,
  parent-limit scaling of logical outer-child requirements, and CTE propagation.
  Connect forced hash and null-aware candidates to the general task dispatcher.
- [x] Follow Go's hint-driven enforcer retry and task-map property identity;
  remove formatted string keys and 15 explicitly Rust-authored dispatcher tests.
- [x] Combined search probe passes 2016 configurations and planning-to-executor
  checks for costed build choice, hint sorting, CTE memo identity, reopen and
  current parameter binding. Scoped planner-library and replay-target builds pass.
- [x] Seven rebuilt Go fixture reports retain control counts: 1292 matches,
  one pre-existing divergence and 55 skips; scoped formatting and diff checks pass.
- [ ] Finish native index enumeration and Apply integration before allowing unhinted
  joins through the general dispatcher. Do not substitute a hash-only search
  for the live reduced-tree search's wider candidate set.
- [x] Share BasePhysicalJoin across hash/merge, bind native merge
  keys and predicates, attach both child tasks and build fresh merge executors.
  Align the live group cursor with Go's key collations, NULL inner-key skipping,
  once-per-chunk outer filters, inner-first fetching, outer EOF and semi/anti
  output. Preserve unused equalities as residuals in the legacy AST adapter.
- [x] Combined WIP probe: 720 hash and 886 merge configurations, reopen and
  actual spill cleanup. Four SQL collation witnesses fail on the saved
  pre-change binary and match their expected rows on the rebuilt binary.
  Six original Go fixture reports remain identical: 1717 matches, 24 existing
  divergences and 92 skips.
- [x] Remove eight Rust-authored merge-order helper tests and stale historical
  commentary. Original Go fixtures are unchanged; temporary witness fixtures
  were removed after copying them outside the checkout.
- [ ] Complete native nullable-marker/null-aware/anti-residual join families,
  missing key comparison domains, window/lock builders and reader metadata,
  then connect the native optimizer output through the SQL driver. The new
  builder is not yet a prepared SQL execution or cache-hit path.
- [x] Restore live Go/PD/TiKV services and previous/current Rust servers,
  preserving retained data; no benchmark fixture reset.
- [x] Compare 30 ordered prepared and 10 text result sets with live Go,
  verify point-query cache miss/hit/hit and unchanged fixture count/SUM(k).
- [x] Run three rotating read-only trials at 8 clients. The awake rerun's
  candidate median is 1770.21 TPS versus control 1813.95 TPS (-2.41%);
  no accepted performance win. Reject the first set because macOS slept.
- [x] Capture and inspect matched candidate/Go/control profiles. Rewriting and
  cloning sample weights fell, but access commitment increased. Five text
  workload plans match control; the throughput regression is not yet explained.
- [x] Run nine single-client, fixed-seed 3000-transaction read-only trials.
  Median SQL CPU falls 4.27s -> 4.18s (-2.11%); median TPS is 406.01 -> 403.70.
  Paging/chunk settings match all three servers. No throughput acceptance.
- [ ] Resolve storage/request cost with statement-level evidence before attributing
  the eight-client regression. Aggregate TiKV counters have publication lag and
  background traffic; the fixed-work harness did not measure TiKV process CPU.
- [ ] Verify changed-value results, errors, warnings, invalidation and resource
  lifecycles against Go; measure alternating unprofiled workloads and separate
  matched profiles. Mixed-write TPC-C needs explicit authority.

## Context and Orientation

`tidb-session::PreparedAst` is the prepared handle shared by the protocol
front ends. `Session::run_bound_prepared_internal` owns replay, replan,
access-path pins, result-authority capture and publication.
`driver.rs` still interleaves optimization with fresh executor construction;
general prepared execution does not yet retain a complete physical tree.

`access_cost::data_source_statistics` derives the profile passed through
`AccessPathCommit`. Aggregate/DISTINCT parents consume it without constructing
a separate RowSource estimate. `join_reorder::emit` uses
`table_stats_profile`; the private legacy cost model now stores StatsInfo
directly. This does not remove all AST rebuilding or candidate replanning.

Go authorities to reread before changing each owner:

- `pkg/planner/core/plan_cache.go` and `plan_cache_rebuild.go`: eligibility,
  cache lookup and rebuilding current ranges without rerunning the optimizer.
- `pkg/planner/core/stats.go`: one DataSource profile derived from
  PushedDownConds and retained for parent consumers.
- `pkg/planner/property/stats_info.go` and
  `operator/logicalop/logical_selection.go`: scaling and Selection GroupNDVs.
- `pkg/expression/constant.go`, `constant_fold.go` and the native ranger:
  marker/deferred evaluation, expression ownership and endpoint diagnostics.
- `pkg/expression/expression.go:922–946`: normal-form splitting forwards
  existing expression interfaces rather than cloning expression trees.
- `pkg/sessionctx/vardef/tidb_vars.go`: scale-NDV default is 1.0; group-NDV
  default is 0.0. They are not interchangeable.

## Plan of Work and Milestones

The approved runtime milestone is a cross-module replacement. Further caches
around AST replay would preserve the wrong owner.
Use the existing `tidb_planner::physical::PhysicalPlan` as the authoritative
tree; do not add a third independent cached-plan representation. The first
batch supplies execution-bearing sort/join/partition fields and per-operator
index resolution. Finish scan and reader execution metadata as their executor
builders are connected. The
existing metadata-only `physical_*::Physical*Plan` wrappers are not executable
plans. Go sources are `operator/physicalop/*`, `core/resolve_indices.go`, and
`expression/column.go`.

The typed executor builder now follows Go `pkg/executor/builder.go` for
Selection, Projection, Limit, Sort, TopN, Dual, UnionAll, MaxOneRow and the
existing hash/stream aggregate families and ordinary hash joins: fresh
operators, contexts, buffers and trackers on every execution. Its
`PhysicalReaderBuilder` callback owns current-transaction reader construction.
The unused Selection/Projection/Limit wrapper definitions are removed; the
native physical tree is the reusable definition owner. `AggFunc::from_descriptor`
lowers native aggregate descriptors without type re-inference. SUM_INT,
DISTINCT merge-set state, approximate-count partial sketches and binary
GROUP_CONCAT separators remain explicitly unported; do not reinterpret them
as ordinary scalar inputs. `join/builder.rs` consumes bound plan keys directly;
`join/output.rs` owns Go's inline output mapping, leaving hashing and residual
evaluation on full child rows. Nullable semi markers, null-aware keys,
anti-semi nullable residuals and additional hash comparison domains remain
unported. Finish those join families, window and lock construction and reader metadata before connecting native
optimizer output through the SQL driver. TopN prefix-rank truncation remains
explicitly unsupported. Keep column UniqueIDs stable and resolve runtime
indexes against each operator's child schema after pruning. Do not widen
`FromScope` into another parallel physical-plan framework.

The SQL adapter must feed `plan_builder::catalog::TableSource` from the current
catalog, run `logical::rule::logical_optimize`, and use the general
`find_best_task::dispatch` tree search. Native forced hash/merge joins,
null-aware joins and serial Apply are connected there. Unhinted joins still
need native index alternatives before replacing the live search space;
do not bypass this dispatcher with a second SQL-only plan representation.

Then replace session access-path pins and simulated cache hits with actual
plan retention and `RebuildPlan4CachedPlan`-style range rebinding. Reconcile
the entire current Go `NewPlanCacheKey` and parameter-type matching before
reuse: Rust's five-field `PreparedPlanKey` is not the full Go key. Go also
accounts for charset/collation, pruning mode, read engines, effective binding,
parameterized LIMIT values, fresh-statistics policy, read-only and foreign-key
settings, transaction status and dirty-table/UnionScan state, among others.
Preserve Go's admission decisions and invalidation ordering; an invalid entry
replans normally. No stored execution context, cursor, transaction handle or
statement-memory owner may outlive its execution.

Retire the replaced AST planning, access-path pins and duplicate metadata
paths as each integrated part becomes authoritative. Partial wiring is WIP,
not a whole Go package completion claim. Validate cold and warm executions,
changed parameter values/types, errors/warnings, DDL and session changes,
transaction-local writes and operator lifecycle against original Go fixtures
and the live Go reference. Finally measure alternating unprofiled sysbench
and authorized TPC-C workloads and use separate matched profiles to verify
that cache hits no longer optimize or derive statistics. Preserve Go expected
outputs; do not count skipped checks as parity or CPU samples as throughput.

Continue test lineage cleanup alongside the affected code: delete confirmed
Rust-authored tests and dead scaffolding, retain Go originals and fixtures,
and introduce no replacement ledgers or protection gates.

## Concrete Steps

Cleanup validation commands, run from `rust/` with 12 build jobs:

    cargo metadata --offline --locked --no-deps --format-version 1
    cargo test --offline --locked --release -j12 --no-run -p tidb-executor -p tidb-distsql -p tidb-txnkv -p tidb-planner -p tidb-exec -p tidb-server -p tidb-metadef -p tidb-proto -p difftest-result-tests
    cargo test --offline --locked --release -j12 -p tidb-executor --lib common_dnf_filter_extraction_matches_go_expression_cases
    cargo test --offline --locked --release -j12 -p tidb-metadef --lib tests_metadef
    cargo test --offline --locked --release -j12 -p tidb-expr --lib test_null_reject_builtin_registry_snapshot

Repository-root checks:

    python3 rust/scripts/generate-parser-charset.py
    git diff --check

Use WIP checks during the cleanup. Before claiming Ready, run the required
scoped checks and repository-root lint:

    GOMAXPROCS=12 GOFLAGS='-p=12' make -j12 lint

No Go/Bazel inputs changed in this increment. Reassess prerequisites if they do.

Owned-CNF WIP validation uses the existing original Go integration topics
`expression/constant_fold`, `agg_predicate_pushdown`, and
`planner/core/join_reorder_through_projection`. Preserve a pre-change replay
binary, then build the same replay target:

    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run

Run each binary with `INTEGRATION_TOPIC=<topic>` and
`INTEGRATION_SHOW_DIVERGENCES=1`, selecting `replay_one_topic_from_env --ignored
--nocapture`. Compare the reports, not just exit status: this harness reports
divergences and unsupported topics without failing. Logs and the control binary
are in `/private/tmp/tidb-owned-cnf.DUWHTC/`.

Candidate replay command (repeat with each topic listed above):

    INTEGRATION_TOPIC=expression/constant_fold INTEGRATION_SHOW_DIVERGENCES=1 target/release/build/difftest-result-tests/bcafd1d1788d0b66/out/integration_diff-bcafd1d1788d0b66 replay_one_topic_from_env --ignored --nocapture

## Validation and Acceptance

Owned-CNF WIP: the release replay target compiled in 43.42 seconds. Control and
candidate both reported 365 matches, zero divergences and 14 skipped checks
across the three original Go topics: 81 Rows, 188 SideEffect and 96 PlanProperty
matches. PlanProperty compares selected table-access properties, not full plans;
ten unsupported statements and four recorder-rewritten outputs remain skipped.
No expected outputs or test assertions changed. `git diff --check` passed.
The replay logs are `control.log` and `candidate-current.log` under
`/private/tmp/tidb-owned-cnf.DUWHTC/`; `build.log` identifies the candidate binary.
The first post-build invocation accidentally repeated the old binary; its log
is `control-repeat.log` and is not candidate evidence. Candidate SHA256 is
`efb53aaf80d14f54c43705d558845bfb0bc6ef7e0c6f209707cfd85484676ede`;
control SHA256 is
`59b90f1ecc195e2f7bbebe159c9bd7fced429cf8c22ca861c0f26e701ca0c367`.
Subsequent live evidence is recorded below; the fixture replay itself is not
a throughput measurement.

WIP cleanup validation passed: Cargo target metadata, compilation of all test
targets in the nine affected packages, six retained Go-derived tests, and
`git diff --check`. The charset generator produced only the intended header
comment change. Retained cases come from `TestFilterExtractFromDNF`, the four
`pkg/meta/metadef` original tests, and `TestNullRejectBuiltinRegistrySnapshot`.

The first compile used a generated list made before the last test deletion
and failed on that missing file. The completed rerun regenerated registrations
and passed without changing the generator. Logs:
`/private/tmp/tidb-go-test-cleanup-build-final.log` and
`/private/tmp/tidb-go-test-cleanup-retained-go-tests.log`.

The full behavioral suite and root lint/Ready have not run. Subsequent live
checks are scoped below. Existing compiler warnings remain. Deletions reduce
Rust-specific coverage; they do not prove the remaining implementation correct.

The next cleanup removes the explicitly authored builder/FROM seam suites and
expression utility additions after comparing the Go expression utility,
constant-folding, DNF and planner tests. The parameter-inference matrix is also
Rust-authored, not Go's `TestDefaultTypeForValue` or `TestDeferredParamNotNull`.
No production behavior or Go fixture changes in this cleanup. Recovery archive:
`/private/tmp/tidb-go-test-lineage.aCKWSi/before.tar.gz`. WIP checks passed:
all test targets in both crates compiled; all 11 retained expression utility
Go-port tests and the Go `TestTableRange` case table passed. Commands from `rust/`:

    cargo test --offline --locked --release -j12 --no-run -p tidb-expr -p tidb-planner
    cargo test --offline --locked --release -j12 -p tidb-expr --lib expr_util::tests::go_
    cargo test --offline --locked --release -j12 -p tidb-planner --lib table_ranges_match_gos_case_table

Logs in the recovery directory are `build-final.log`, `expr-go-tests.log` and
`planner-go-tests.log`. The initial invocation from the repository root found
no Cargo manifest and was corrected to `rust/`; it is not build evidence.
`git diff --check` and removed-module reference checks passed. No root lint,
full behavioral run, live workload measurement or push in this cleanup.

Known discrepancies include duplicated range-conversion warnings, residual
Selection row estimates, a TPC-C Sort, unsupported StreamAgg child shapes and
correlated scalar subqueries. They remain open independently of test deletion.

Current live candidate SHA256:
`da424a99c6a45b833240b4341f8f36dab5ad27d80ece02dbfca393d35796430d`.
The release server built in 41.68 seconds. Retained services were restarted
without resetting their data. Current manifest:
`/private/tmp/tidb-batching-candidate-kkk_ng4s/manifest.json`.
All 30 prepared and 10 text result sets match the live Go reference. The fixture
still contains 100013 rows with SUM(k)=5011271921. No workload writes ran.

The first sysbench set was invalidated by confirmed macOS sleep: two nominal
15-second trials lasted 246.8977 and 743.1123 seconds. The complete awake rerun
used 8 clients, three rotating 15-second trials per server, four gRPC connections
and no concurrent build/profile. Median TPS: control 1813.95, candidate 1770.21,
Go 1754.84. This accumulated change regressed against control by 2.41%; it does
not isolate CNF splitting from the StatsInfo changes. No rollback or performance
acceptance follows from these short shared-laptop observations alone.
Raw commands, hashes and results are in the `owned_cnf_followup` entry of
`rust/benchmarks/prepared-program-observations.json`.

Earlier live results and throughput/profile observations are in
`rust/benchmarks/prepared-program-observations.json`. The preceding candidate
regressed median throughput at 1 and 8 clients; its noisy 32-client result is
not an accepted win. No accepted performance baseline has been changed.

## Idempotence and Recovery

The shared worktree contains substantial unrelated edits; preserve them.
Only remote branch `hparser-integration` is allowed. No push is requested.

During the inner-table increment the earlier temporary directories disappeared,
including `tidb-inner-table.5cZgM0`, `tidb-static-metadata.4I1mRd`,
`tidb-index-cache-ranges.5PpNvt` and `tidb-live-planning.MFcV5P`. Their recovery
paths below are historical, not available archives. Checked-in baseline data
remain available. The retained pre-inner-table replay binary was verified by
SHA256 `1ced3a1b013b63d1d1050a47d3fbfb2748943bf241fdd34bab710030f3f966f8`
before rebuilding; fresh logs and both replay binaries are in the current
evidence directory above. No source restoration from missing archives occurred.

Keep retained TiUP data at
`/Users/qiliu/.tiup/data/tidb-perf-paired-nIF6JX` unchanged. Revalidate service
and fixture state before use. Cleanup recovery archives, including dirty
test contents, are in `/private/tmp/tidb-go-test-cleanup.bYb1gx/`.

## Decision Log

Remove the entire transaction actor rather than retaining a second read-only
path. The sole owner takes the synchronized state before terminal work, making
outstanding snapshots observe a finished transaction. The pessimistic heartbeat
and opener remain alive through commit/rollback. Snapshot reads serialize only
within their own transaction, as the old actor did; no global lock or additional
transport is introduced. Fatal lock errors finish cleanup before returning.
The Go/Rust live lifecycle comparison uses two new owned databases and does not
reset or write the retained sysbench fixture.

Treat the current live window as diagnostic only: repeated unchanged controls
vary too much to establish a throughput/latency gain. Stop narrowing the work
to transport receipt overhead. The next source-backed boundary is direct
transaction ownership: the extra pinned actor persists after its !Send premise
ceased to hold. Keep the transaction state and lifecycle unified rather than
adding a second direct-read implementation beside the actor.

Foreground multi-region requests use the same response-first completion as
single requests. Collect admissions before mapping completions, preserving
input order and independent sibling results without parallel index/Option
bookkeeping. Keep publication synchronization only on failures: removing that
ordering could misclassify a cancellation racing with physical publication.
This follows pinned client-go `sendBatchRequest` and the concurrent region
branch of `doActionOnBatches`; it does not change retry or concurrency policy.

Use suspendable runtime tasks for detached secondary commits, matching Go's
independent store-pool continuations. A single thread may drive ready tasks,
but must not synchronously wait for one transaction before admitting another.
Retain the store/client owner across each task without locking that authority
across network I/O. Native task wakeups and the existing completion notifier
share one observer slot on the original completion state; no second response
queue or periodic polling is introduced. Keep foreground completion APIs intact.

Ordinary BatchCommands completion reads the existing request's OnceLock route
and assigned ID. Do not build or send a second acknowledgement solely to
reconstruct this identity. Keep a lazy address-local Inspect barrier for the
explicit pre-response publication API, and preserve the separate synchronous
receipt API. Test detached secondary work against fixture-local requests, not
another transaction's update to a process-global metric. These changes leave
batching policy, transaction decisions and wire identity unchanged.

For throughput-and-latency comparisons, bind observations to the benchmark
binary as well as the SQL binaries. Repair the demonstrated macOS AArch64
assembly-separator defect in an isolated sysbench 1.0.20 build; do not replace
the user's installed tool or mix old zero-percentile results with valid tails.
Keep the collection candidate unaccepted after the fresh eight-client regression.

Keep EXPLAIN-only names, condition copies and key text inside existing trace
consumers, following Go's ExplainInfo/executor-builder ownership separation.
Do not suppress estimates used for candidate selection or the executable
IndexJoin null filters. The broader check also exposed a previous folding
regression: implicit real casts must fold at construction, as Go's
WrapWithCastAsReal/BuildCastFunction does. Correct the cast owner, preserving
deferred parameter rebinding and the non-recursive ordinary parent fold.

Go `newFunctionImpl` bookmarks warnings only in try-fold mode, and the ordinary
`foldConstant` scalar arm inspects its already-built arguments rather than
recursively re-folding them. Make construction folding produce a candidate
without mutating its input, check try-fold warnings, then move the accepted
value and (for deferred constants) original expression into the replacement.
Remove the eager expression snapshot and obsolete recursive-fold exception
list. This also preserves an earlier try-fold rejection: the original Go
`interval(1,0,1,2,1/0)` fixture no longer acquires a division-by-zero warning.
The separate metadata-only recursive fold is not changed by this increment.

Go `expressionRewriter.toColumn` looks up a name and returns its complete
schema column. The Rust ScopeResolver instead resolved the column twice and
cloned its type again just to build OrigName. Resolve a borrowed binding once,
then construct either its complete expression column or its identity/type
view. Move default name completion into `ColumnResolver::resolve_column`;
complete-column overrides now own their metadata. Statistics/group-NDV callers
use the identity view. Preserve qualification, ambiguity, USING/NATURAL
coalescing, scalar-subquery IDs and existing name casing. No statement/global
cache or workload-specific branch is introduced.

Go `buildDataSource2TableScanByIndexJoinProp` and `constructDS2TableScanTask`
use one table-task construction flow for integer and common handles. Replace
the native integer-only branch with that shared flow and use the existing
IndexJoin range analyzer for composite primary keys. Estimate residual filters
before restoring inner-schema access predicates as explicit selections. Cap
scan cardinality at one only for a full unique-key equality match; composite
prefixes retain their estimated fanout. Split oversized residual IN trees to
root execution, preserve mutable range definitions and outer-bound managers
through task feedback, and reject ordered partition probes. Ranger inputs are
statement-scoped; rejected-path errors and quota fallbacks remain observable.
The current order matcher handles direct full-length common-key prefixes;
Go's constant-column skipping/merge-sort metadata is not yet represented here.
Secondary-index statistics/selection and native family enumeration remain
explicit gaps; this increment does not activate a reduced live SQL plan cache.

Go's FieldType and expression collation constructors retain empty and built-in
string headers without allocating string backing. Rust Arc strings had fixed
clone costs but still allocated for every initial construction. Keep static
registry spellings directly and Arc backing for other exact names in a narrow
charset-name representation shared by FieldType and expression collation state.
Only byte-identical registry spellings use static backing: aliases, uppercase,
unknown names and JSON spellings remain unchanged. There is no dynamic string
interner or cached semantic lookup. Setters/builders accept borrowed string
views, and column metadata, cast, IFNULL folding and IndexJoin forwarding no
longer require eager owned-string copies. Runtime collator lookup, Go memory
accounting, flags, clone mutation independence and ENUM/SET slices are unchanged.

Go's physical table scan retains the selected path's ranges, access conditions
and table filters. Retain the complete Rust handle ranger result rather than
discarding its residual conditions and rebuilding it at each consumer. Borrow
residual AST expressions through path selection; only execution-owned leaf
filters clone them. Synthetic IndexMerge branch ASTs live through selection.
When root-only conditions were excluded before ranging, preserve the original
WHERE instead of treating the narrowed residual as the whole predicate. Keep
the existing exact-range guard for approximate predicates. UPDATE/DELETE now
consume the selected table or index path and its real-statistics estimate,
without a second table-path rebuild or separate estimate.

Go `mutableIndexJoinRange` shares immutable path/join definitions between cache
clones, rebuilding execution-owned ranges through `indexJoinPathBuild` with no
quota. Retain that definition once, carry it through `IndexJoinInfo` and physical
cloning, and rebuild before native executor construction. Reader metadata uses
the rebound ranges; static plans still borrow their original ranges. Reject
empty or changed count/width like Go, without adding an executor-Open rebuild.
This does not activate the live SQL cache or secondary-index candidate selection.

Go `Datum.SetBytes` retains `binary`, and `Datum.MemUsage` counts its six bytes.
Rust omitted that collation in ranger quota accounting. The original
`TestPlanCacheForIndexJoinRangeFallback` boundary (1260 bytes) now passes:
short strings fit, longer strings fall back during planning, and cached rebuild
retains the full ranges. Do not imitate Go allocator capacity to correct this.

Go `ExtractAccessConditionsForColumn`/`expression.Filter` retain expression
interfaces and `buildColumnRange` returns its original condition slice, either
as access or residual conditions. The Rust result now borrows the input slice;
standard `Borrow<Expression>` lets the same builder consume owned conditions
or extracted references without another adapter/copy. Live statistics keeps
those references through coverage matching. Native scan and IndexJoin callers
consume their built ranges before moving the original owned access vector into
the access or fallback-filter owner. Range algorithms, quotas, evaluation and
error order are unchanged. This removes an ownership mismatch in the live
planning flow without substituting for the unfinished physical-plan cache.

Go's decorrelation and join-elimination rules retain unchanged plan nodes;
the corresponding Rust AST adapters cloned complete SELECTs on every no-op.
Use one borrowed derived-child walk, constructing an owned replacement only
when a child changes. Preserve left-to-right traversal and existing rule order.
Join elimination proves parent-column demand and uniqueness before allocating;
`LeafDemand::of_select_parent_clauses` does not inspect FROM. Explicit envelope
construction avoids cloning the obsolete FROM tree. Derived `NodeBox` copies
retain original bytes, encoding, SQL mode, source offset and decoded-text cache.
This improves existing live ownership without claiming Go's full logical-plan
representation or prepared physical-plan cache is integrated.

The prepared-cache audit still finds AST/path-pin replay rather than Go's
physical-plan reuse. A fresh isolated range profile additionally identifies
metadata copies as a live cost throughout that planning flow. Go
`parser/types.FieldType.Clone` and `DeepCopy` retain immutable string bytes;
Rust's owned String fields allocated again at each clone. Use `Arc<str>` for
FieldType and expression CollationInfo names, with replacement-only setters.
This changes ownership, not charset lookup, canonicalization or SQL behavior.
Keep public string-taking APIs and source memory-accounting rules unchanged.
The full prepared physical-plan/cache migration remains required.

Go `planner/optimize.go` returns TryFastPlan before logical optimization,
but that is a statement-level boundary, not a rule for every DataSource.
Rust already has that entry in `try_fast_point_select`. An attempted second
early-return owner inside `commit_fast_path_source` was discarded after
tracing that boundary and measuring unchanged standalone point performance
plus slower ordinary controls. Keep DataSource statistics for parent planning.
The retained change makes batch, single-point and range dispatch exclusive:
point metadata is resolved once and moved to its reader; a later matcher
cannot repeat unique-index lookup or supersede an already committed source.
Partition pruning is derived once and reused for the table and reader.

The live executor already owns table statistics and a second skyline
implementation. Consolidate comparison before adding native lookup-task
statistics plumbing: the shared comparator lives in the planner, and both
callers store its metrics once and borrow them for comparisons. Table-local
physical offsets identify the live maps; native maps retain expression
UniqueIDs. Do not clone maps or reconstruct expressions per comparison.
The live chooser's existing default-only fix-45132 policy and DNF/full-index
match derivation remain separate unfinished metadata boundaries.

The 2026-09-09 candidate/identity batch first resolves source-discovered
prerequisites rather than feeding incorrect column matches into index coverage.
Go `Column.EqualByExprAndID` allows expression fallback only when the source
virtual expression is a ScalarFunction. Ordinary columns use UniqueID, and
schema resolution borrows the target instead of cloning it for every schema
entry. IndexScan retains a shared immutable DataSourceSchema; IndexReader
conversion exposes it, while pushed aggregates/projections own their output.
Native IndexScans missing this metadata do not silently expose scan columns.

Candidate comparison consumes explicit, already-derived path/statistics facts.
It ports Go's risk, coverage, pseudo-statistics, global-index, property and
row-count-ratio comparisons, then applies IndexJoin's original-table equality
NDV, used-width and join-key coverage tie breakers. The native statistics
connection is still missing; this batch does not invent its inputs or activate a
reduced secondary-index search. The temporary Go oracle preserves the actual
comparison function bodies, replacing only external planner/statistics inputs
with normalized values. This is not a whole Go planner package claim.

The path-range batch follows `indexJoinPathBuild` through the existing ranger
APIs, exposing the current-constant evaluator instead of introducing another
range implementation. The statement supplies the range-fallback callback, so
warnings are not lost when a quota rejects the entire path. Go's original
`TestIndexJoinAnalyzeLookUpFilters` and `TestRangeFallbackForAnalyzeLookUpFilters`
provide the two checked-in tests; broader execution/parameter diagnostics stay
outside the repository. This is a range-analysis increment, not full native
path selection or cache activation. Date: 2026-09-09.

The native lookup property follows Go PhysicalProperty.HashCode and
CloneEssentialFields: cache the immutable lookup suffix once, share its
definition, and inherit it explicitly only across admitted operators. General
search/enforcer working copies retain it; ordinary child-property clones do
not. Hash joins try each lookup child separately. Integer-PK scan selection
feeds all matching key offsets upward through IndexJoinInfo. Filtered scans
require a caller-supplied table-statistics selectivity function; Go's selection
factor applies only after that function fails or returns a nonpositive value.
Do not fabricate statistics merely to make an incomplete caller succeed.
The regular static candidate builder is available, but the unhinted search
still requires secondary/common-handle path analysis and the other families.

The task handoff uses Go `IndexJoinInfo`, `completePhysicalIndexJoin` and
`inheritStatsFromBottomTaskForIndexJoinInner`. Feedback is immutable and shared
across candidate task copies; attachment consumes its own reference and moves
the fields when uniquely owned. Non-lookup equalities are moved to residuals,
then eligible cross-input bare-column EQs are extracted in reverse residual
order into hash keys. IN operands retain residual NULL handling. Null-safe
equalities are not promoted by this EQ-only loop. The upstream implementation
removes a bare-column EQ after its schema-side checks even when neither side
matches; this port follows the code, not the contradictory nearby comment.

Go copies task headers while sharing plans. Since Rust attachment already owns
its candidate tasks, it now moves them through conversion, attachment and sort
enforcement; the borrowed conversion still copies for callers retaining a
candidate. CopTask::copy no longer clones and then reclones its warning buffer.
The native IndexJoin cost dispatcher calls the existing formula, including
range seeking, semi-join over-read and session factors. Its hash-row widths use
the existing chunk codec's Go type-width function through a direct workspace
dependency, not a duplicate implementation. Histograms/native SQL activation
remain open; this is not a measured workload performance win.

The native IndexJoin batch follows Go `buildIndexLookUpJoin`,
`ColWithCmpFuncManager.BuildRangesByRow` and `buildRangesForIndexJoin`.
Construct executor-owned key types, joiners and comparison functions; bind
lookup keys and last-column bounds into request-owned ranges. Go's
`cloneForIndexJoinBuild` shares its plan, so the Rust batch builder borrows the
retained inner definition instead of cloning the plan per batch. Fresh reader
state receives the ranges separately and uses the current transaction's table
owner. Keep residual predicates after unioning bounds from different outer
rows. The existing sequential IndexLookUpJoin runtime is reused; this does not
claim Go's parallel worker lifecycle or complete physicalop package parity.

The aggregate batch follows Go `AggFuncToPBExpr` and
`PbConverter.conOrCorColToPBExpr`: retain the aggregate argument, read current
constant/correlated values when constructing the request, and infer a marker's
type from its current datum (`Constant.GetType`). The storage seam already owns
a clone of the aggregate description; bind that copy in place instead of adding
another expression clone or retaining session state in the request. Evaluation
refusal leaves the original local partial path available. Consume the candidates
already built for costing instead of running partial planning again after
selection. No plan-cache or performance-completion claim follows from this batch.

The shared-lowering batch follows `pkg/expression/expr_to_pb.go`'s
`conOrCorColToPBExpr`: evaluate constant/correlated leaves in the converter's
current context, not by rewriting a clone or reading a saved payload.
`rewrite_inner_filters` in the live index-join decision builder resolves its
predicates against a single inner-table scope; outer probe bounds are separate.
All three lookup request paths now use the shared converter and remap descriptor
offsets only. Unsupported descriptions remain complete local filters. Native
reader reopen replaces only descriptions, retaining the executable expressions,
immutable fast paths and scratch chunk. This does not widen the scalar catalog
to unimplemented datum/function families or complete native SQL integration.

The reader batch uses the existing transaction storage owners and streaming
executors, not an AST rebuild on every Open. Immutable ranges and descriptions
are initialized once; only mutable bindings are refreshed, and filter scratch
chunks survive reopen. Retain the local filter even when the current remote
stream proves it applied every predicate: whether evaluation can be skipped
belongs to that stream, not to the reusable executor definition. Optional native
bindings are boxed so unused definitions do not inflate legacy reader structs.
Unsupported cop operators remain explicit errors; do not substitute root
execution for a cop DAG or activate a partial SQL/cache path.

The scan batch repairs the access/filter boundary before live activation.
Only predicates detached for the chosen key become ranges; reserved and
non-key conditions remain Selections. Index offsets address retained table
columns after pruning. Scan definitions retain access expressions and key
types, and the existing ranger rebuilds from current correlated/parameter
values. Correlated-prefix promotion preserves prefix/range residuals and the
source plan-cache exclusion reason. Do not reconstruct SQL/AST to bind a range.

The Apply batch follows Go's shared Data pointer, with fresh cells bound into
each execution's cloned inner tree. The inner executor is built once and
reopened after rebinding; the existing Joiner owns all tuple/NULL semantics.
An iterator cursor crosses Rust borrow boundaries without rescanning the List.
Go's NoCopPushDown bit travels through child properties and task memo identity;
Apply's outer child sets it, and aggregation suppresses cop candidates when set.
Native parallel Apply remains an explicit unsupported path, not serial fallback.

The merge batch shares Go's BasePhysicalJoin ownership rather than encoding a
merge as a fake hash plan or reconstructing an ON tree. The legacy AST caller
removes only the equalities covered by its chosen merge keys, matching Go's
moveEqualToOtherConditions. Native merge candidates now read real logical
ExtractFD().ConstantCols(), including dependency projection and expression
identities in the existing graph. The combined probe exposed a missing
DataSource enforcer branch: children requiring a Sort returned invalid tasks.
Port that branch at its source owner, including Fix46177's default-true
comparison of natural and enforced scans. The SQL/cache switch continues to
wait for native index alternatives and the remaining statement bindings.

The user approved the broad replacement, disabled the change-instruction
critic skill, and requested larger implementation batches followed by
consolidated validation. The batches change the physical model, binding and
executor construction together; SQL execution is not yet routed through the
typed builder. Go builds fresh evaluator suites from physical expressions,
so unused executor-local plan wrappers are deleted instead of adding another
cache authority. LIMIT's column helper lives inside its projected variant,
eliminating a separate optional helper and its impossible-state assertion.

Go original tests are the reference; Rust-only source shape, call count,
refusal-set, source-size and doctest-count assertions are not compatibility
requirements. Delete their scaffolding, not production behavior.

Reuse StatsInfo at the existing DataSource owner instead of introducing another
row/NDV representation or plan framework. Preserve root Selection scaling
separately from physical access estimates.

Go `initStats` in `pkg/planner/core/stats.go` binds table statistics to
`TblCols` with `ID2UniqueID`; `cardinality.Selectivity` reuses the same HistColl
in recursive DNF estimates. Rust now shares a local `(physical offset, UniqueID)`
mapping along those calls. Name resolution uses the current resolver once;
expression columns still supply their original type/flags to ranger. This
removes repeated binding work but does not implement Go's retained physical
plan cache or establish a throughput improvement.

Use an owned CNF adapter only where callers consume freshly built expressions.
Keep the borrowed splitter for callers retaining their trees. An iterative
stack preserves Go's conjunct order without recursively moving large Rust
expression values. This does not introduce a cache or change evaluation order.

## Surprises & Discoveries

The September 9 live follow-up shows an unchanged drain control falling from
1796 to 971 TPS at eight clients, while its request count and batching remain
similar. Candidate and saved control also slow; the drift cause is unproven.
CPU-only profiles show transaction and scan worker paths alongside transport
work. The source still justifies the transaction actor with Rc/RefCell, but
current shared runtime fields are Arc/Mutex. A metadata-only compiler probe
using the current production types establishes that both are Send, without
editing implementation code or creating a permanent protection test.

Admission into the Rust owner queue is not physical publication. An invalid
TiKV URI fails later with no identity; the five batch paths and single-command
error helper incorrectly expected one and panicked. The pre-edit regression
records `Stage A binds a nonzero publication before pending escapes` in
`/private/tmp/tidb-foreground-completion.ujPWR6/red-unit.log`. The regression
lives beside the private command implementation.

The installed sysbench histogram function returns without incrementing its
bucket. Its bundled CK_PR_UNARY macro emits `mov;stadd`; on this assembler
the second instruction is a comment. The isolated probe prints
`semicolon=0 newline=1`. A 200-event usleep(5000) probe has mean 6.11 ms in both
builds, but only the corrected build has 200 histogram entries and p95 6.32 ms.

`tidb-executor/src/skyline.rs` duplicated the new native comparator and
discarded index-prefix lengths. A covering prefix candidate could therefore
prune a full-length lookup candidate even though Go reports their access
maps incomparable. `/private/tmp/tidb-skyline-unify.gWLnzl/before.log` records
the old `(1, false)` verdict where the Go comparison requires `(0, false)`.
The old code also ORed table-pseudo onto an analyzed index's own status;
Go `isCandidatesPseudo` allows that index to remain non-pseudo. The shared
comparator consumes the actual per-path status and recognizes MV paths.

`Column.equal_by_expr_and_id` previously compared optional virtual expressions
without first requiring a scalar virtual expression. Two None values therefore
made unrelated same-typed ordinary columns equal. The temporary baseline
`identity-before.log` fails on columns 1 and 2 before the change. The resulting
wrong fallback also affects `PhysicalIndexReader.resolve_indices`. Correcting
the shared identity method removes that failure class; the resolver's loop no
longer allocates a cloned target per schema entry. Existing column tests and
the combined native probe pass after the change.

Go `getIndexJoinIntPKPathInfo` can map several join equalities to the same PK.
`dedupHandles` checks their converted values agree, skipping contradictory
lookups. The Rust native reader rejected more than one key instead.
`duplicate-before.log` reproduces `integer handle lookup requires one key`;
the final probe passes matching and conflicting values through all seven join
families after replacing that restriction with Go's equality check.

Adding the direct local chunk dependency changed Cargo's integration executable
hash. The first replay command still named the older artifact; inspection of
the build's reported executable caught this, and the final candidate replay
uses `integration_diff-fc2a7b145e4187e6`. Its detailed results match the saved
pre-batch executable. No stale-artifact run is used as post-change evidence.

The IndexJoin probe initially created its composite index after inserting
fixture rows without backfilling it, then omitted the residual inequality
required after per-outer ranges are combined. Correcting those two diagnostic
setup errors produces the expected stored-byte results. Neither was treated as
a production bug or bypassed with a production special case.

The pre-batch native DataSource witness for `value = 7` produces an integer
primary-handle range `[7,7]`; it should produce a full handle range plus a
Selection on `value`. The dispatcher passed every predicate to BuildTableRange
without DetachCondsForColumn, then dropped residuals. `before.log` under the
scan evidence directory proves the failure; the final combined probe passes.

The pre-batch ownership witness fails: cloning a correlated column bound to 3
then updating the original to 9 still evaluates the clone as 3. Shared cells
make that witness pass. The diagnostic initially used StmtContext::default(),
whose cache quota is zero, and therefore could not expect cache hits; the final
probe supplies the session quota explicitly. Production defaults were unchanged.

The live merge cursor compared keys using the derivation-free collation, while
its child sorts used each column's collation. With utf8mb4_general_ci keys,
the saved binary returned zero rows for case-varied inner-join matches and
NULL-extended all outer rows. Passing the key collation through group and
cross-side comparisons makes all four temporary SQL witnesses match.
These expected rows were written as a diagnostic from Go's comparison rules,
not recorded from a live Go server. The initial spill probe's 250 KiB quota
did not trigger spilling; the final 96 KiB probe measured a positive disk
peak and zero live disk bytes after both closes.

Some source-inspection tests are actual Go ports:
`TestNullRejectBuiltinRegistrySnapshot` exists in
`pkg/planner/util/null_misc_test.go`. Keep its Rust counterpart.

Using group-NDV's 0.0 default for StatsInfo scaling changed TPC-H Q1 plan shape.
Reading Go's two defaults identified the mistake; the implementation now uses
the canonical scale-NDV value 1.0.

## Outcomes & Retrospective

Direct-ownership WIP changes `cluster_table_storage.rs`. The subsequent scan
migration removes the remaining pinned pool (see `cop-scan-worker-execplan.md`).
The old transaction actor's Rc/RefCell premise no
longer applies to the synchronized store. Go transaction Get, Commit and
Rollback and the Rust coordinator's local read-only finish were read before
the migration. Two obsolete worker-design tests were removed; behavior tests
and the original locking implementation remain. The test-placement skill keeps
the cleanup scoped to those obsolete tests, not a new protection suite.

Exact consolidated commands from `rust/` (logs under
`/private/tmp/tidb-direct-transaction.Tz3RgL/`):

    cargo test --offline --locked --release -j12 -p tidb-exec --lib
    cargo test --offline --locked --release -j12 -p tidb-exec --test all
    cargo test --offline --locked --release -j12 -p tidb-server --lib cluster_session_node::tests
    cargo build --offline --locked --release -j12 -p tidb-server --bin tidb-server
    git diff --check

Exec results: 330 unit and 722 integration-target passes, one ignored. The
server command was also run against the saved pre-change main file: both
versions report the same 156 passes and five failures (one lazy uniqueness-check
expectation and four unistore index-plan cases). These failures are not masked
or repaired in this lifecycle migration. The migrated source was restored
before the successful final release build. Live lifecycle output hashes match
Go, including repeatable reads, rollback release and disconnect release.
Full TPC-C, failure-injected live cluster behavior, Ready/root lint and whole
Go-package parity remain unverified. This is WIP validation.

The live comparison commands are `ruby
/private/tmp/tidb-direct-transaction.Tz3RgL/candidate/lifecycle.rb` and `ruby
/private/tmp/tidb-direct-transaction.Tz3RgL/candidate/measure.rb`. The lifecycle
script is intentionally not idempotent: it created and retains two named
databases, and must not be rerun as a reset. The measurement script is read-only.
Across 28 fixed-work trials, eight-client median TPS rises 1800.37 -> 1905.31,
mean latency falls 4.44 -> 4.19 ms, median trial p95 falls 4.82 -> 4.57 ms, and
SQL CPU falls 11.415 -> 9.32 seconds per 6000 transactions. The before TPS
range is 1781.37-1810.83, and the after range is 1878.70-1918.34. All six
query outputs match Go/control/candidate before and after. This supports a
local read-only performance gain, not mixed-write performance or full parity.
The baseline record includes all tier samples, binary identities and limitations.
All three owned supervisors then exited successfully; the six owned PIDs and
twelve ports are absent. Fixture data, the two new lifecycle databases, logs
and binaries remain for recovery. No Git mutation or push occurred.

Foreground completion WIP removes five pre-response publication waits and
their duplicated indexed result vectors. A real asynchronous admission failure
now returns BeforePublication rather than panicking. Ordered error observation
still preserves potentially applied writes. All 155 unit tests and 417 tests
in the topology-independent target pass, with 11 ignored; release server build
passes. The first broader run failed because sandbox policy denied localhost
fixture binds (326 passed, 91 failed); the permitted rerun passed. No shared
cluster was started or modified. Live sysbench/TPC-C and Ready lint remain
unverified; no accepted performance baseline changes.

Exact WIP commands from `rust/` (logs and pre-edit recovery source are under
`/private/tmp/tidb-foreground-completion.ujPWR6/`):

    cargo test --offline --locked --release -j12 -p tidb-txnkv --lib transaction_admission_errors_remain_before_publication -- --nocapture
    cargo test --offline --locked --release -j12 -p tidb-txnkv --lib
    cargo test --offline --locked --release -j12 -p tidb-txnkv --test all
    cargo build --offline --locked --release -j12 -p tidb-server --bin tidb-server
    git diff --check

The first command demonstrates the pre-edit panic; the full unit run includes
the passing witness afterward. The test-placement skill keeps the regression
beside its private owning implementation. No other Rust/Go test was deleted.

Detached-secondary WIP removes the global serial flush queue, mutex-held I/O
wait and the non-Go worker-count protection test. A held-response wire witness
now allows independent transaction progress and leaves the authority available.
20 RPC unit and 417 topology-independent tests plus release build pass; 11
tests remain ignored. The measured 1278 us second-request admission is fixture
evidence, not sysbench/TPC-C throughput. No cluster was started in this increment.

Entry-publication WIP replaces redundant receipt transport for ordinary reads
and transactional commands. The source witness failed before implementation;
20 RPC unit and 416 topology-independent tests pass after cleanup (11 ignored),
and the release build passes. A previously hidden parallel test race was exposed
by the broader run and repaired at the fixture synchronization boundary. Both
windows' Go/result comparisons pass. The final cleaned binary remains 1.37
percent slower than the stable drain-only control at eight clients; the immediate
control's large late outlier prevents a reliable gain estimate in that window.
No Ready lint, whole-package parity, TPC-C or overall performance acceptance is
claimed. Artifacts: `/private/tmp/tidb-entry-publication.z1hwgp/`.

Latency-measurement WIP changes `rust/scripts/compare-sysbench.py`, this plan
and `rust/benchmarks/sysbench-latency-validation.json`; Rust production source
is unchanged in that increment. The script parses and summarizes latency,
rejects the actual broken histogram output, accepts corrected output, and
completes nine live Go/control/candidate comparisons. The isolated dependency
build uses 12 jobs and a temporary installation prefix. No Ready lint, TPC-C
completion, package parity or accepted performance improvement is claimed.

Explain-construction WIP batch changes `tidb-executor/src/driver/from.rs` and
`tidb-expr/src/rewriter.rs`, this ExecPlan and the new local benchmark record
`rust/benchmarks/explain-construction-baseline.json`. The profile shows no
physical-column display-name construction during ordinary prepared ranges
after the change. Local median elapsed times: range 38.91 -> 37.45 us, index
61.17 -> 59.67 us, aggregate 58.81 -> 57.38 us. Both invocation pairs improve.
Sort is 53.79 -> 52.76 us; point control is 1.555 -> 1.535 us. Those latter
changes are retained without an independent performance-acceptance claim.
This is isolated in-process storage, not live sysbench/TPC-C acceptance.

Consolidated checks pass 540 tests (376 executor, 164 expression). Seven
executor failures remain, all listed in the earlier
`select-rewrite-ownership-baseline.json`; they were not freshly reproduced on
the immediate pre-edit unit-test binary. A newly exposed prepared POW metadata
failure was reproduced before its correction, and passes afterward. It also
passes on retained older executor binaries; removal of recursive folding had
exposed the uninitialized implicit cast. The existing parameter-rebinding
test passes for results 8, 9 and 1 from one compiled expression. No test
expectations, permanent tests, Go source or original fixtures were changed.
All 21 complete Go fixture reports compare identically apart from elapsed
time: 4955 matches, 141 existing differences, 606 skips. Skips and existing
differences remain explicit gaps, not parity evidence.

Commands from `rust/` (logs and recovery archives in
`/private/tmp/tidb-trace-ownership.flFD9Q/`):

    cargo test --offline --locked --release -j12 -p tidb-expr -p tidb-executor --lib --no-run
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    cargo build --offline --locked --release -j12 --target-dir /Users/qiliu/projects/tidb/rust/target --manifest-path /private/tmp/tidb-live-profile.RItNBU/Cargo.toml
    target/release/build/tidb-executor/25c7702b011f2820/out/tidb_executor-25c7702b011f2820 driver:: access_cost:: --test-threads=12
    target/release/build/tidb-expr/c547695f5d95432a/out/tidb_expr-c547695f5d95432a rewriter:: simple_expr:: constant:: new_function:: expr_util:: builtin_arithmetic:: builtin_compare:: math:: pow --test-threads=12
    ruby /private/tmp/tidb-trace-ownership.flFD9Q/replay.rb before /private/tmp/tidb-trace-ownership.flFD9Q/replay-before
    ruby /private/tmp/tidb-trace-ownership.flFD9Q/replay.rb after /private/tmp/tidb-trace-ownership.flFD9Q/replay-final
    bash /Users/qiliu/.codex/skills/instruments/scripts/sample-quick.sh --launch -d 8 -o /private/tmp/tidb-trace-ownership.flFD9Q/range-before.sample -- /private/tmp/tidb-trace-ownership.flFD9Q/before 200000 range
    bash /Users/qiliu/.codex/skills/instruments/scripts/sample-quick.sh --launch -d 8 -o /private/tmp/tidb-trace-ownership.flFD9Q/range-after.sample -- /private/tmp/tidb-trace-ownership.flFD9Q/after 200000 range
    ruby /private/tmp/tidb-trace-ownership.flFD9Q/timings.rb
    git diff --check -- crates/tidb-executor/src/driver/from.rs crates/tidb-expr/src/rewriter.rs docs/prepared-plan-execplan.md benchmarks/explain-construction-baseline.json

The final source differs from the tested/measured build only in comments and
line wrapping in the implicit-cast helper. Both fresh sample commands exit
successfully and own only their temporary benchmark process. Timings ran
after all builds, tests and profiles finished; host-wide quiescence was not
guaranteed. Ready lint, a fresh Go CPU profile, whole-package completion,
native physical-plan reuse and live workload gains remain unverified. No
live services, workload data, Git state or remote branches were changed.

The expression-construction WIP batch changes expression `constant_fold.rs`
and `rewriter.rs`, executor `driver/from.rs`, `access_cost.rs` and
`driver/access.rs`. The final consolidated checks pass 143 expression tests
and 77 executor tests; two executor predicate-pushdown tests fail with
`global StreamAgg child is not a point get or bare scan`, and one benchmark
is ignored. Both failures are documented in
`rust/benchmarks/select-rewrite-ownership-baseline.json`; their before-state
was not rebuilt in this increment. Twenty original Go fixture reports retain
every result and divergence except the resolved INTERVAL warning: 4,938
matches, 141 remaining divergences and 606 skips. No tests were added,
removed, or changed. The isolated construction diagnostic compares the saved
pre-edit fold with the new fold: normal folding no longer reads warning state,
try-fold nonconstant trees allocate zero times (before: 3/24/96/384 allocations
at depths 1/8/32/128), and normal/try/disabled warning behavior preserves
preexisting warnings and expression results.

Validation profile is WIP. Commands run from `rust/`:

    cargo test --offline --locked --release -j12 -p tidb-expr -p tidb-executor --lib --no-run
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    cargo build --offline --locked --release -j12 --target-dir /Users/qiliu/projects/tidb/rust/target --manifest-path /private/tmp/tidb-live-profile.RItNBU/Cargo.toml
    cargo run --offline --locked --release -j12 --target-dir /Users/qiliu/projects/tidb/rust/target --manifest-path /private/tmp/tidb-live-profile.RItNBU/diagnostic/Cargo.toml
    target/release/build/tidb-expr/c547695f5d95432a/out/tidb_expr-c547695f5d95432a rewriter:: simple_expr:: constant:: new_function:: expr_util:: builtin_arithmetic:: builtin_compare:: --test-threads=12
    target/release/build/tidb-executor/25c7702b011f2820/out/tidb_executor-25c7702b011f2820 access_cost:: driver::tests::predicate_pushdown:: driver::tests::column_type_flags:: driver::tests::joins:: driver::tests::index_ranges:: driver::tests::from:: driver::tests::group_concat:: --test-threads=12
    ruby /private/tmp/tidb-live-profile.RItNBU/replay.rb before /private/tmp/tidb-live-profile.RItNBU/replay-before
    ruby /private/tmp/tidb-live-profile.RItNBU/replay.rb after /private/tmp/tidb-live-profile.RItNBU/replay-final
    ruby /private/tmp/tidb-live-profile.RItNBU/timings.rb
    ruby /private/tmp/tidb-live-profile.RItNBU/timings-final.rb

The first full-workload ABBA timing run has startup drift and establishes no
speedup. The final run pairs binaries adjacently within each query class and
extends the point control to one million operations per round. Its median
elapsed reductions are point 0.65%, range 1.01%, index 0.54%, aggregate 0.52%
and sort 1.25%. These modest changes do not establish an independent throughput
win; accepted performance baselines are unchanged. Raw observations, scripts,
binary hashes and the saved fold diagnostic are retained in
`rust/benchmarks/expression-construction-observations.json`. CPU sampling
uses the profiling skill's `sample-quick.sh --launch -d 8` on the sealed local
benchmark processes. The initial sandbox sample failed; outside-sandbox
sampling produced complete before/fold-after/final traces (the helper's summary
pipeline exits 141 after capture). No live database, fresh Go profile,
sysbench/TPC-C run, Ready lint or Git/remote writes occurred. Only formatting
and status documentation changed after the final build.

The inner-table WIP increment changes planner `find_best_task/dispatch.rs`,
`find_best_task/index_join.rs` and new `find_best_task/index_join/table.rs`.
All 112 scoped planner and 27 scoped executor tests pass. The isolated diagnostic
passes 38 row-count/residual-selectivity cases, additional mapping/unique-key/
10,000-vs-10,001-IN/order-partition boundaries, and 24 native stored-row
executions. Those executions construct inner tasks through `find_best_task`,
attach their feedback to a physical IndexJoin, resolve indices and execute via
native readers at chunk sizes 1/2/32: composite prefixes, static full-key
filters, repeated parameter values 1/2/3/2/1 on an unchanged cached physical
plan, and outer-dependent final-column bounds. The diagnostic uses an isolated
in-process Session, not the user's live databases. No permanent tests were
added or removed. Sixteen complete fixture reports are unchanged at 3,384
matches, 126 existing divergences and 500 skips. No performance improvement is
claimed, so no new performance baseline is recorded.

Validation profile is WIP; commands run from `rust/` (logs and the standalone
diagnostic manifest/source are in `/private/tmp/tidb-inner-table.hXS9cU/`):

```sh
cargo test --offline --locked --release -j12 -p tidb-planner --lib --no-run
cargo test --offline --locked --release -j12 -p tidb-planner -p tidb-executor --lib --no-run
cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
cargo generate-lockfile --offline --manifest-path /private/tmp/tidb-inner-table.hXS9cU/Cargo.toml
cargo run --offline --locked --release -j12 --target-dir /Users/qiliu/projects/tidb/rust/target --manifest-path /private/tmp/tidb-inner-table.hXS9cU/Cargo.toml
target/release/build/tidb-planner/3d1b2bbdef61703f/out/tidb_planner-3d1b2bbdef61703f ranger:: find_best_task:: task:: physical::index_join:: --test-threads=12
target/release/build/tidb-executor/25c7702b011f2820/out/tidb_executor-25c7702b011f2820 tests_index_join:: index_join physical_builder:: --test-threads=12
ruby /private/tmp/tidb-inner-table.hXS9cU/replay.rb before /private/tmp/tidb-inner-table.hXS9cU/replay-before
ruby /private/tmp/tidb-inner-table.hXS9cU/replay.rb after /private/tmp/tidb-inner-table.hXS9cU/replay-after
```

The replay script compares full fixture output including divergence details,
normalizing elapsed times only. Final source edits after this validation are
comments/formatting and this status record. Compatibility risk remains in the
native planner's incomplete secondary/order/statistics integration, not a claim
of package parity. No live TiKV, sysbench/TPC-C, fresh Go profile, Ready lint or
Git/remote writes occurred.

The static metadata batch changes datatype charset/name storage, its public
export and FieldType/builders, model column forwarding, expression collation,
cast/folding forwarding and native IndexJoin forwarding. Existing Go-derived
spelling, hash, clone, memory and JSON checks remain unchanged. All 272 scoped
tests pass, and sixteen complete Go fixture reports retain 3,384 matches,
126 existing divergences and 500 skips. No permanent tests were added/removed.
An isolated allocator diagnostic confirms zero heap allocations across nine
construction cases (10,000 constructions each). Paired local elapsed changes
are index -4.9%, range -3.6% and aggregate -2.6%; sort is inconclusive. Initial
point +3.2% was not reproduced by a longer million-operation control (-1.8%);
neither point result is accepted as an independent improvement. The baseline
retains both raw runs, exact commands and allocator source. Native name layout
and setter trait bounds change; Go accounting and SQL spellings do not. No live
TiKV, sysbench/TPC-C, fresh Go profile, Ready lint or Git/remote writes occurred.
Full native plan-cache activation and whole-package parity remain open.

The chosen-range ownership batch changes executor `access_cost.rs`,
`driver/access.rs`, `driver/leaf_access.rs`, one stale comment in `explain.rs`,
this plan and `rust/benchmarks/chosen-range-ownership-baseline.json`. Consolidated
WIP validation passes 94 tests; sixteen full Go fixture reports remain identical
with 3,384 matches, 126 existing divergences and 500 skips. No tests were added
or removed. Paired local measurements improve range by 6.0% and aggregate/sort
by 4.2%; point/index differences are not accepted as changes. The four-second
CPU samples corroborate removal of two subsequent range-build call chains.
Exact commands, binary hashes, raw timings, source references and limitations
are in the baseline. Only whitespace/comments changed after the measured build.
Loaded statistics can now change ordinary write path selection consistently;
analyzed-statistics workload coverage is not exhaustive. No live workload,
fresh Go profile, Ready lint, service mutation, commit or push was performed.
Whole-package parity and live prepared-plan cache activation remain open.

The native IndexJoin range-cache batch changes planner
`find_best_task/index_join/path.rs`, its `path/cached.rs` and Go-derived
`path/tests.rs`, `physical/index_join.rs`, `ranger/types.rs`, executor
`physical_builder.rs`, `physical_builder/index_join.rs`,
`physical_builder/readers.rs`, and this plan. WIP checks pass 61 ranger, three
IndexJoin and 29 access-cost tests. The original 1260-byte quota test failed
before the accounting correction; no expected quota was loosened. Native
diagnostics pass 30 executions with changing parameters, two reader shapes and
three chunk sizes, immutable definition/range clone isolation, and Go's
empty/count/width reuse rejection. The combined reader/Apply/aggregate/task
diagnostics also pass. All fourteen original Go SQL reports retain 2,888
matches, 109 existing divergences and 387 skips with identical normalized
details. No performance claim or baseline change belongs to this capability
increment. Quota fallback intentionally changes at the corrected byte boundary;
live cache admission, EXPLAIN rebuild information, secondary/common-handle path
selection and full package completion remain open. No live TiKV, sysbench/TPC-C,
fresh Go profile or Ready lint was run. No service or Git/remote writes.

Exact batch commands (from `rust/`), recovery source snapshots, complete
diagnostic source and raw reports are in
`/private/tmp/tidb-index-cache-ranges.5PpNvt/`:

    cargo test --offline --locked --release -j12 -p tidb-planner --lib ranger::
    cargo test --offline --locked --release -j12 -p tidb-planner --lib find_best_task::index_join::
    cargo test --offline --locked --release -j12 -p tidb-executor --lib access_cost::
    cargo run --offline --locked --release -j12 --target-dir /Users/qiliu/projects/tidb/rust/target --manifest-path /private/tmp/tidb-index-cache-ranges.5PpNvt/probe/Cargo.toml
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    ruby /private/tmp/tidb-index-cache-ranges.5PpNvt/replay.rb before /private/tmp/tidb-index-cache-ranges.5PpNvt/replay-before
    ruby /private/tmp/tidb-index-cache-ranges.5PpNvt/replay.rb after /private/tmp/tidb-index-cache-ranges.5PpNvt/replay-after

The replay driver runs each original topic with `INTEGRATION_TOPIC=<topic>` and
`INTEGRATION_SHOW_DIVERGENCES=1`, selecting `--ignored --nocapture
replay_one_topic_from_env --test-threads=12`, and compares complete reports after
elapsed-time normalization. The before/after SHA-256 values are
`a3e46b80f9086d0f968fabd2c35b1898e4cf0a3aa88b3dd5d706177893c29f4d` and
`c0a267ee86a33e627838dac2f2bce8679088a1c61a52d75cea800b83123bf95f`.
Final changes after compilation only format the new code and update this plan.

The range-condition ownership batch changes ranger `detacher.rs`/`ranger.rs`,
planner `find_best_task/dispatch.rs`/`index_join/path.rs`, executor
`access_cost.rs`, this plan and its measurement baseline. WIP checks pass 61
ranger, 27 planner-caller and 29 access-cost tests. The two Go IndexJoin lookup
and fallback suites also pass separately and are included in the 27, not
counted twice. The only test edit supplies the expression type for an empty
generic input; no test expectations or fixtures changed. All fourteen Go SQL
reports retain exactly 2,888 matches, 109 divergences and 387 skips.

Sealed paired timings after compilation and replay: range 26.506→25.869 µs,
secondary index 41.289→40.591 µs, sort 35.652→34.630 µs, with identical rows and
metadata. The roughly 1% aggregate difference and noisy cached-point control
are not independent accepted wins. Exact commands, raw timing rounds, hashes
and reproduction pointers are in
`rust/benchmarks/range-condition-ownership-baseline.json`. Ready lint, fresh Go
CPU profiling, live TiKV/sysbench/TPC-C and full-package completion remain
unverified. No services, datasets or remotes were changed; the full goal remains.

The SELECT ownership batch changes `driver.rs`, adds `driver/ast_rewrite.rs`,
and updates the three adapters named in Progress. WIP validation uses one
consolidated driver check plus fourteen original Go fixture topics. The final
driver result is 254 passing, seven failing and one ignored. All seven failure
details reproduce in the isolated pre-change source snapshot (253 passing,
eight failing, one ignored); its additional TPC-H Q14 failure is not treated as
an ownership fix. No tests or fixtures were removed or adjusted to hide failures.
Full fixture reports retain 2,888 matches, 109 divergences and 387 skips.

After builds and replay completed, sealed before/after/after/before timings
retain identical rows and metadata: range 27.732→26.772 µs, secondary index
42.445→41.196 µs, aggregate 40.588→39.396 µs, sort 36.566→35.475 µs. Point is a
short noisy control. Absolute timings differ from the previous increment even
for its unchanged sealed binary; only the paired comparison is accepted.
`rust/benchmarks/select-rewrite-ownership-baseline.json` records raw rounds,
binary hashes, source reproduction and exact validation commands. Ready lint,
live TiKV/sysbench/TPC-C, a fresh Go profile and full-package completion remain
unverified. No external services, datasets, Git history or remotes were changed.

Type-metadata sharing changes datatype `field_type/mod.rs`,
`field_type/json.rs`, expression `expr_collation.rs`, this plan and the new
measurement baseline. The profiling skill selected the measured allocation
owner. WIP commands from `rust/` were:

    cargo test --offline --locked --release -j12 -p tidb-datatype --lib field_type::
    cargo test --offline --locked --release -j12 -p tidb-expr --lib expr_collation::
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo build --offline --locked --release -j12 --manifest-path /private/tmp/tidb-prepared-owner.ixmsOs/probe/Cargo.toml
    git diff --check

The first two commands pass 29 and six tests. Eleven Go fixture topics report
2,482 matches, 109 divergences and 371 skips. The seven topics from the prior
increment have identical normalized reports, including their 67 divergences.
The four additional prepared/collation topics have 42 divergences without an
exact pre-change replay; do not classify those as unchanged or new. The
baseline lists all topics, the replay command and raw measurements.

After all compilation, tests and sampling completed, before/after/after/before
runs of the sealed in-process benchmark retained identical result rows and
metadata. Medians over ten rounds of 10,000 executions improve range 9.7%,
secondary index 17.7%, aggregate 11.5% and sort 9.7%. Do not claim a separate
cached-point win from its short control case. Earlier overlapping timing runs
are excluded. No live services or data were changed. Ready lint, live TiKV,
sysbench/TPC-C throughput, multiclient reference-count contention and a fresh
Go CPU profile remain unverified. Retain the full optimization goal.

Exclusive access dispatch changes only executor `driver/access.rs` and this
plan in this increment, preserving the pre-existing shared worktree edits.
Validation is WIP, not whole-package completion. From `rust/`, commands were:

    cargo test --offline --locked --release -j12 -p tidb-executor --lib driver::access::find_best_task_property_tests::
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo build --offline --locked --release -j12 --manifest-path /private/tmp/tidb-fast-plan-order.v0tVwE/probe/Cargo.toml
    git diff --check

The first filter `driver::access::tests::` selected zero tests; the corrected
command above passes nine. The freshly built replay executable listed below
was run with `INTEGRATION_SHOW_DIVERGENCES=1`,
`INTEGRATION_TOPIC=<topic>` and arguments
`--ignored --nocapture replay_one_topic_from_env --test-threads=12` for
`table/index`, `explain_easy`, `util/ranger`, `index_join`, `index_merge`,
`executor/aggregate` and `session/clustered_index`. All normalized output
matches the pre-change replay: 1,496 matches, 67 existing divergences and
316 skips. No fixture was rewritten.

Evidence is in `/private/tmp/tidb-fast-plan-order.v0tVwE/`. Sealed
`probe-final 3000` and `probe-before 3000` runs have identical results across
nine isolated in-process Session cases. Five-round median timings vary from
2.5% faster to 3.0% slower: no accepted performance win or baseline update.
This does not verify live TiKV, concurrency, sysbench or TPC-C performance.
No Ready lint, live profile or push was performed. The principal remaining
compatibility risk is broader planner integration beyond these fixture topics.

Live skyline consolidation changes executor `access_cost.rs`, `skyline.rs`,
`driver/access.rs`, planner `find_best_task.rs`, moves shared comparison out
of `find_best_task/index_join/candidate.rs` into `find_best_task/candidate.rs`,
and updates this plan. The retired implementation, environment debug hook,
13 Rust-only tests and their historical explanations are removed; the
pre-turn versions are recoverable from `before.tar.gz` under
`/private/tmp/tidb-skyline-unify.gWLnzl/`. No original Go tests were removed.

Validation is WIP because native secondary/common-handle lookup selection,
complete statistics metadata and native SQL/cache activation remain open.
From `rust/`, commands were:

    cargo test --offline --locked --release -j12 -p tidb-executor --lib access_cost::
    cargo test --offline --locked --release -j12 -p tidb-planner --lib find_best_task::index_join::
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo run --offline --locked --release -j12 --manifest-path /private/tmp/tidb-skyline-unify.gWLnzl/probe/Cargo.toml
    GOMAXPROCS=12 go run -p=12 /private/tmp/tidb-skyline-unify.gWLnzl/oracle.go
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    rustfmt --edition 2021 --check crates/tidb-planner/src/find_best_task/candidate.rs crates/tidb-planner/src/find_best_task/index_join/candidate.rs crates/tidb-executor/src/skyline.rs
    git diff --check

`access-tests-final.log` passes 29 tests, `planner-tests.log` passes the two
original Go lookup-filter suites, and `probe.log` passes the combined native
diagnostics plus 29,491 comparator/live-pruning cases. The current Go source
was re-extracted and its output byte-compared with the probe oracle. The
initial compilation found one remaining point-heuristic use of index width;
that metadata is retained on Candidate and the consolidated rerun passes.

The replay executable reported by the fresh build is
`target/release/build/difftest-result-tests/fc2a7b145e4187e6/out/integration_diff-fc2a7b145e4187e6`.
For each topic in `table/index explain_easy util/ranger index_join index_merge`,
run it with `INTEGRATION_TOPIC=<topic> INTEGRATION_SHOW_DIVERGENCES=1` and
arguments `--ignored --nocapture replay_one_topic_from_env --test-threads=12`.
The five topics report 441 matches, 38 existing divergences and 125 skips.
Normalized output, including every divergence detail, is identical to the
cached pre-change executable's replay; this is not full SQL parity. Logs are
`before-<topic>.log` and `after-<topic>.log` in the evidence directory, with
slashes replaced by underscores. No Ready lint, live TiKV, throughput/profile
comparison or push was performed. The performance baseline is unchanged.

Candidate/identity WIP changes expression `column.rs`, planner
`find_best_task/index_join.rs`, new `find_best_task/index_join/candidate.rs`,
`find_best_task/dispatch.rs`, `physical/mod.rs`, `task.rs`, and this plan.
No Go production or checked-in test files changed. The reader schema is an
Arc shared across scan-definition clones; execution-local output columns are
still independently resolved. The dispatcher initializes this source metadata.

Evidence: `/private/tmp/tidb-index-path-task.8LCYwX/`. `column-tests.log` passes
14 existing checks; `task-tests-final.log` passes 50, including the existing
lookup-range suites. `probe-final.log` passes all 16,384 candidate comparisons
against `skyline-go.txt`, produced by extracted Go comparison functions in
`oracle.go` (normalization script: `extract_oracle.rb`). It also checks NDV
boundaries, skyline-before-NDV precedence, key/width tie breakers, borrowed
column maps, ordinary/generated column identity, 16 IndexReader output
conversions, absent-column rejection, projection output and metadata sharing.
Existing native covering-reader probes now construct their readers through
CopTask conversion instead of manually setting reader output. The 126
twice-opened IndexJoin configurations, 144 aggregate configurations, 266 reader
configurations and earlier Apply/merge/property/cost diagnostics pass.

WIP commands from `rust/`, except the absolute Go oracle command:

    GOMAXPROCS=12 go run -p=12 /private/tmp/tidb-index-path-task.8LCYwX/oracle.go
    cargo test --offline --locked --release -j12 -p tidb-expr --lib column::
    cargo test --offline --locked --release -j12 -p tidb-planner --lib task::
    cargo test --offline --locked --release -j12 -p tidb-planner --lib find_best_task::index_join::path::
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo run --offline --locked --release -j12 --manifest-path /private/tmp/tidb-index-path-task.8LCYwX/probe/Cargo.toml
    rustfmt --edition 2021 --check crates/tidb-planner/src/find_best_task/index_join/candidate.rs
    git diff --check

The whole `column.rs` rustfmt check also reports a preexisting CorrelatedDatum
formatting difference, preserved rather than mixed into this source fix.
No sysbench/TPC-C timing or live Go/Rust profile was measured, no accepted
performance baseline changed, and Ready lint/live TiKV/SQL-cache activation
remain unverified. The next integration must supply real original-table path
statistics to these comparison rules and construct the selected common-handle,
covering-index or double-read task using Go's filter/cardinality flow. No push
or whole-package completion claim.

IndexJoin path-range WIP adds `find_best_task/index_join/path.rs` and its Go
test-port module, registers it in `find_best_task/index_join.rs`, and exposes
the existing evaluator-aware equality extractor in `ranger/detacher.rs`.
No executor production code was changed in this increment. The temporary
native-reader diagnostic replaces hand-authored secondary-index templates
with the new path builder's output before normal task attachment/execution.

Evidence: `/private/tmp/tidb-index-ranges.DWEwIZ/`. `path-tests-final.log` passes
both Go-derived suites (11 lookup-filter cases plus quota/rebuild cases).
`probe-final.log` passes 40 parameter/quota/rebuild configurations, contradictory
parameter extraction and outer-only comparison ownership. All 126 twice-opened
IndexJoin configurations pass; 84 covering/index-lookup configurations consume
newly built templates. Prior property, cost, completion, aggregate, reader,
Apply and merge diagnostics also pass. The first temporary quota assertion
incorrectly expected a full two-point, three-column range below its 1,120-byte
Go-shaped estimate; the corrected diagnostic expects the retained two-column
prefix at quota 1,000. Production quota behavior was not weakened.

WIP commands from `rust/` (12 jobs, offline and locked):

    cargo test --offline --locked --release -j12 -p tidb-planner --lib find_best_task::index_join::path::
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo run --offline --locked --release -j12 --manifest-path /private/tmp/tidb-index-ranges.DWEwIZ/probe/Cargo.toml
    rustfmt --edition 2021 --check crates/tidb-planner/src/find_best_task/index_join/path.rs crates/tidb-planner/src/find_best_task/index_join/path/tests.rs
    git diff --check

Remaining: secondary/common-handle candidate selection with original-table
NDV/skyline comparison, native scan-task construction, mutable-template/cache
ownership and range information, memory-tracker accounting, full family and
operator support, native SQL/cache activation, Ready lint and live Go/Rust
profiles/workloads. Original SQL fixture replay was not repeated for this
unactivated native path. No service changes, push, performance baseline change,
whole-package completion claim or measured speedup.

Native IndexJoin property WIP changes planner `physical_property.rs`,
`physical/mod.rs`, `physical/hash_join.rs`, `find_best_task.rs`,
`find_best_task/dispatch.rs`, new `find_best_task/index_join.rs`, `enforce.rs`,
the task-layer boundary comment, executor `physical_builder/readers.rs`, and
this plan. The first three property constructors in the legacy join enumerator
explicitly retain their existing no-native-lookup behavior.

Evidence: `/private/tmp/tidb-index-property.onXVBe/`. `probe-final.log` passes
30 property/task cases, six current selectivity input cases, eight static
candidates, and the previous cost/key-completion checks. It passes 126
twice-opened IndexJoin configurations, including 42 with property-driven
integer-PK scan selection and 21 with two distinct outer columns constrained
to the same inner PK. The earlier aggregate/reader/Apply/merge probes pass too.
Four existing property tests pass. The five original fixture reports match the
saved executable in every detail after timing normalization: 1,137 matches,
47 existing divergences, 97 skips. WIP commands from `rust/`:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo run --offline --locked --release -j12 --manifest-path /private/tmp/tidb-index-property.onXVBe/probe/Cargo.toml
    cargo test --offline --locked --release -j12 -p tidb-planner --lib --no-run
    cargo test --offline --locked --release -j12 -p tidb-planner --lib physical_property::
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    for topic in index_join executor/index_lookup_join executor/index_lookup_merge_join executor/parallel_apply executor/aggregate; do INTEGRATION_SHOW_DIVERGENCES=1 INTEGRATION_TOPIC="$topic" target/release/build/difftest-result-tests/fc2a7b145e4187e6/out/integration_diff-fc2a7b145e4187e6 --exact replay_one_topic_from_env --ignored --nocapture --test-threads=12; done
    git diff --check

Remaining: full family/path enumeration, mutable templates, histogram ownership
and scan RangeInfo/cost metadata, native inner HashJoin execution, complete
cop-predicate admission, native SQL/cache activation, Ready lint and live
Go/Rust profiles/workloads. No service reset, push, performance baseline change
or whole-package completion claim. Temporary diagnostics are not permanent gates.

The IndexJoin task handoff WIP batch changes planner `physical/index_join.rs`,
`physical/mod.rs`, `task.rs`, `find_best_task/coster.rs`,
`find_best_task/dispatch.rs`, `enforce.rs`, the planner Cargo manifest and
workspace lockfile, plus this plan. The lockfile increment is only the local
`tidb-chunk` dependency under `tidb-planner`; existing unrelated changes remain.
It replaces the attachment refusal with source-derived key completion and
feedback consumption, and removes unnecessary owned-task plan copies.

Evidence is `/private/tmp/tidb-index-join-task.H7Wuu4/`. `probe-final.log`
passes 56 key-completion configurations, all three reader feedback conversions,
hash feedback precedence, warning order, per-outer statistics, owned allocation
identity and borrowed-copy isolation. It checks 48 native cost configurations
against the existing Go formula and its independently supplied inputs. The 105
stored-row IndexJoin configurations now construct executors through actual task
attachment; each opens twice. Prior reader/aggregate/Apply/merge diagnostics
also pass. These are external diagnostics, not added permanent protection tests.
`task-tests.log` records 48 existing planner task-related checks passing.
Original fixture details are unchanged: 1,137 matches, 47 existing divergences,
97 skips. The current native SQL path is not exercised by the original replay.
WIP commands from `rust/`:

    cargo update --offline -p tidb-planner
    cargo update --offline -p tidb-planner --manifest-path /private/tmp/tidb-index-join-task.H7Wuu4/probe/Cargo.toml
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo run --offline --locked --release -j12 --manifest-path /private/tmp/tidb-index-join-task.H7Wuu4/probe/Cargo.toml
    cargo test --offline --locked --release -j12 -p tidb-planner --lib task::
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    for topic in index_join executor/index_lookup_join executor/index_lookup_merge_join executor/parallel_apply executor/aggregate; do INTEGRATION_SHOW_DIVERGENCES=1 INTEGRATION_TOPIC="$topic" target/release/build/difftest-result-tests/fc2a7b145e4187e6/out/integration_diff-fc2a7b145e4187e6 --exact replay_one_topic_from_env --ignored --nocapture --test-threads=12; done
    git diff --check

No live service or retained workload was changed. Ready/root lint, live TiKV,
Go/Rust workload profiles and throughput remain unverified. This batch neither
activates native SQL/cache reuse nor claims whole-package parity, a performance
baseline change, or delivery to the remote branch.

Current native IndexJoin WIP changes planner physical metadata/dispatch,
resolution and correlated-expression traversal, executor physical building and
transaction-reader range ownership. Task attachment explicitly identifies the
still-missing IndexJoinInfo propagation instead of attaching as a HashJoin.
The obsolete comment saying no native reader implementation exists is removed.

Evidence is `/private/tmp/tidb-native-index-join.3Ru38H/`. `probe.log` passes
105 IndexJoin configurations, each opened twice, spanning integer-handle,
covering-index and index-lookup readers, all seven join families, three chunk
sizes, current transaction bytes and per-outer comparison bounds. It also
passes the prior 144 aggregate, 266 reader, 26 range/filter, 360 Apply candidate,
504 twice-opened Apply runtime, 36 NULL-channel and 120 merge configurations.
The in-process backend captures requests and declines remote execution; this
is not live TiKV evidence. No permanent test hook or service reset was added.

Planner/executor library test targets and the integration replay target compile.
`control-details.log` and `candidate-details.log` match after timing normalization:
1,137 matches, 47 existing divergences and 97 skips across the five original
fixture topics. These reports do not establish native SQL activation. WIP
commands, from `rust/`:

    cargo check --offline --locked --release -j12 -p tidb-executor
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo run --offline --locked --release -j12 --manifest-path /private/tmp/tidb-native-index-join.3Ru38H/probe/Cargo.toml
    cargo test --offline --locked --release -j12 -p tidb-planner -p tidb-executor --lib --no-run
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    for topic in index_join executor/index_lookup_join executor/index_lookup_merge_join executor/parallel_apply executor/aggregate; do INTEGRATION_SHOW_DIVERGENCES=1 INTEGRATION_TOPIC="$topic" target/release/build/difftest-result-tests/050430c182ecd301/out/integration_diff-050430c182ecd301 --exact replay_one_topic_from_env --ignored --nocapture --test-threads=12; done
    git diff --check

Ready/root lint, live Go/Rust profiles, throughput and remote execution are not
verified in this batch. No performance baseline or push is justified. Full
native SQL/cache integration and whole-package parity remain open.

Current aggregate-binding WIP changes `tidb-expr/src/pushdown_catalog.rs`,
`tidb-executor/src/remote_scan.rs`, `driver/agg_select.rs`, `access_path.rs`,
`kv_table/table_scan.rs`, the existing partial-aggregate request test call site,
and this plan. Grouped COUNT no longer turns a non-column argument into COUNT(*)
based on its saved value; its expression follows the same path as other
aggregate arguments. The table Open no longer clones its aggregate and column
list before the request builder's existing clone. A duplicated dirty-table
check in the covering-index request builder is removed.

Evidence is `/private/tmp/tidb-aggregate-binding.Rx6wTh/`. `combined-probe.log`
passes 144 table/index configurations, each opened twice, spanning grouped/global
COUNT/SUM/MIN/MAX with parameter, deferred and correlated leaves, including NULL.
It checks the outgoing owned literal, current marker type, numeric TiPB payload,
local aggregate result and unchanged retained definition. Its in-process backend
declines remote execution: this is storage-request/local-result evidence, not
TiKV execution. The same run also passes the existing 266 reader, 26 range,
360 Apply candidate, 504 twice-opened Apply runtime, 36 NULL-channel and 120 merge
configurations. Initial probe setup omitted clearing its in-memory fixture's
dirty mark, correctly preventing remote aggregate requests; the final probe
models a committed fixture. No live service or retained workload data was changed.

`go-pushdown-tests.log` records two passes and one existing ignored test.
`control-details.log` and `candidate-details.log` are identical after normalizing
timing: 1,137 matches, 47 existing divergences and 97 skips across five original
fixture topics. Source comparison establishes the old missing binding and saved
COUNT check; this batch did not execute a separate pre-change failing diagnostic.
No new permanent test or hook was added. WIP commands, from `rust/`:

    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo run --offline --locked --release -j12 --manifest-path /private/tmp/tidb-aggregate-binding.Rx6wTh/probe/Cargo.toml
    cargo test --offline --locked --release -j12 -p tidb-executor --lib predicate_pushdown::tests_push_down_verdict::tikv_
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    for topic in index_join executor/index_lookup_join executor/index_lookup_merge_join executor/parallel_apply executor/aggregate; do INTEGRATION_SHOW_DIVERGENCES=1 INTEGRATION_TOPIC="$topic" target/release/build/difftest-result-tests/050430c182ecd301/out/integration_diff-050430c182ecd301 --exact replay_one_topic_from_env --ignored --nocapture --test-threads=12; done
    git diff --check

Repeat the replay command with the saved `control-replay` executable for the
control. The scalar catalog's unsupported datum/function families still refuse
pushdown, including a NULL scalar leaf; those are not advertised as implemented.
Root Ready lint, live Go/TiKV profiles, throughput and native SQL/cache activation
remain unverified. No accepted baseline change, whole-package claim or push.

Current shared-lowering WIP changed `tidb-expr/src/pushdown_catalog.rs`,
`tidb-executor/src/predicate_pushdown.rs`, `access_path.rs`,
`physical_builder/readers.rs`, `kv_table/table_scan.rs` and this plan.
`/private/tmp/tidb-scan-lowering.KViq9v/before.log` reproduces a live index-join
request containing saved literal 99 when its current parameter is -9.
`final-lowering-probe.log` passes six parameter/deferred request cases and 41
comparison/boolean/builtin cases. `final-readers-probe.log` passes all 266 native
reader cases plus the previous ranger/Apply/FD/merge checks. Neither request
capture nor stored-byte execution is a real TiKV workload performance result.

Commands from `rust/`:

    cargo test --offline --locked --release -j12 -p tidb-executor --lib execution_lowering_probe -- --nocapture
    cargo run --offline --locked --release -j12 --manifest-path /private/tmp/tidb-scan-lowering.KViq9v/probe/Cargo.toml --target-dir /Users/qiliu/projects/tidb/rust/target
    cargo test --offline --locked --release -j12 -p tidb-executor --lib predicate_pushdown::tests_push_down_verdict::tikv_
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    for topic in index_join executor/index_lookup_join executor/index_lookup_merge_join executor/parallel_apply executor/aggregate; do INTEGRATION_SHOW_DIVERGENCES=1 INTEGRATION_TOPIC="$topic" target/release/build/difftest-result-tests/050430c182ecd301/out/integration_diff-050430c182ecd301 --exact replay_one_topic_from_env --ignored --nocapture --test-threads=12; done
    git diff --check
    rustfmt --check --edition 2021 --config skip_children=true crates/tidb-executor/src/physical_builder/readers.rs

The first command ran with a temporary `execution_lowering_probe` test module
in `access_path.rs`, pointing at the external `scan_lowering_diag.rs` in the
evidence directory. That registration is removed; no permanent test or fixture
was added. To reproduce those private-boundary diagnostics, temporarily attach
`#[cfg(test)] #[path = "/private/tmp/tidb-scan-lowering.KViq9v/scan_lowering_diag.rs"] mod execution_lowering_probe;`
to `access_path.rs`, run the command, then remove that registration again.

After removal, the retained Go pushdown tests pass (two pass, one existing
unsupported-family suite ignored), and the replay target rebuilds. All five
control/candidate fixture reports and divergence details match after removing
elapsed times: 1137 matches, 47 existing divergences, 97 skips. No Ready/lint,
live profile, throughput acceptance or push. No baseline change or package-
completion claim; native SQL/cache integration and the full goal remain open.

Current native reader WIP changes are `tidb-executor/src/physical_builder.rs`,
its new `physical_builder/readers.rs`, `access_path.rs`, `kv_table/table_scan.rs`,
`predicate_pushdown.rs` and this plan. No Go fixtures or permanent protection
tests were added. Evidence in `/private/tmp/tidb-native-readers.StFw1N/`:
`expanded-probe-final.log` shows zero-column readers returning 0 instead of 25;
`before-retained-filter-final.log` shows the second Open returning all 25 rows
instead of row 25. `verified-probe.log` passes all 266 reader configurations
plus the previous combined ranger/Apply/FD/merge diagnostics. Request-capture
and simulated remote-to-local transition evidence is not real TiKV execution.

WIP commands from `rust/`:

    cargo run --offline --locked --release -j12 --manifest-path /private/tmp/tidb-native-readers.StFw1N/probe/Cargo.toml --target-dir /Users/qiliu/projects/tidb/rust/target
    cargo test --offline --locked --release -j12 -p tidb-executor --lib predicate_pushdown::tests_push_down_verdict::tikv_
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    for topic in subquery executor/parallel_apply executor/merge_join executor/aggregate; do INTEGRATION_SHOW_DIVERGENCES=1 INTEGRATION_TOPIC="$topic" target/release/build/difftest-result-tests/050430c182ecd301/out/integration_diff-050430c182ecd301 --exact replay_one_topic_from_env --ignored --nocapture --test-threads=12; done
    git diff --check
    rustfmt --check --edition 2021 --config skip_children=true crates/tidb-executor/src/physical_builder/readers.rs crates/tidb-executor/src/physical_builder.rs

Go pushdown verdicts: two pass, one existing unsupported-family suite ignored.
Control/candidate fixture details are identical after removing elapsed times:
1178 matches, 20 existing divergences, 81 skips. The SQL replay still exercises
the legacy entrypoint, so it establishes non-regression there, not native SQL
activation. No Ready/root lint, live benchmark, remote execution acceptance or
push; the performance baseline is unchanged and the full goal remains active.

Current native scan WIP: changed planner `access_path.rs`, physical
`mod.rs`, `correlated.rs`, new `scan_ranges.rs`, task conversion, native
dispatch and ranger binary-comparison evaluation. The physical executor
builder's reader contract now states the required Open-time rebinding.
The root-fix loop selected the access/filter ownership correction; the WIP
profile consolidates checks without claiming Ready or whole-package parity.

Evidence `/private/tmp/tidb-native-ranges.g7ojup/`: wrong-column before/after
witness, 26 range/filter/pruning configurations, common-handle rebinding,
correlated-prefix termination, the preceding 900 Apply configurations and
FD/120 merge diagnostics pass (`probe.log`). All 17 Go ranger case suites
pass (`ranger-tests.log`). Rebuilt and control replay details are identical
after removing elapsed times: 1178 matches, 20 existing aggregate divergences,
81 skipped checks. These fixtures still execute the legacy SQL driver.

Exact commands, from `rust/`:

    cargo run --offline --locked --release -j12 --manifest-path /private/tmp/tidb-native-ranges.g7ojup/probe/Cargo.toml --target-dir /Users/qiliu/projects/tidb/rust/target
    cargo test --offline --locked --release -j12 -p tidb-planner --lib ranger::go_cases
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    for topic in subquery executor/parallel_apply executor/merge_join executor/aggregate; do INTEGRATION_SHOW_DIVERGENCES=1 INTEGRATION_TOPIC="$topic" target/release/build/difftest-result-tests/050430c182ecd301/out/integration_diff-050430c182ecd301 --exact replay_one_topic_from_env --ignored --nocapture --test-threads=12; done
    git diff --check

Control used the same replay arguments with the evidence directory's
`control-replay`. `control-details.log` and `candidate-details.log` retain
full reports. Source self-review and scoped rustfmt covered the changed path.
No live workload/performance, transaction-backed native reader, native SQL/
cache entry, real-histogram scan costing or Ready/root lint was verified.
No baseline changed, live service was reset, permanent guard added or code pushed.

Current native Apply WIP (2026-09-08): changes cover tidb-expr column ownership
and constructor/substitution callers, tidb-planner physical Apply/correlation
modules, physical properties and dispatcher, tidb-executor physical_builder,
apply/native and the shared Joiner filter visibility, tidb-chunk iterator cursor,
and the planner vardef dependency/lockfile. Existing unrelated edits are retained.

Combined evidence in `/private/tmp/tidb-native-apply.8dFnea/`: 360 Apply candidate
configurations, 504 serial runtime configurations each opened twice, 36 merged
EQ/NA/residual UNKNOWN cases, shared-cell isolation, and the preceding FD/120
merge diagnostics pass. Column tests: 14 pass; original chunk iterator tests:
3 pass. Control and rebuilt SQL replay agree on 1178 matches, 20 existing
aggregate divergences and 81 skipped checks. Replay wrapper success alone is
not a parity verdict. The fixtures still use the legacy live SQL driver, while
the temporary runtime probe directly exercises the native builder.

Exact WIP validation, from `rust/` (all builds use 12 jobs):

    cargo run --offline --locked --release -j12 --manifest-path /private/tmp/tidb-native-apply.8dFnea/probe/Cargo.toml --target-dir /Users/qiliu/projects/tidb/rust/target
    cargo test --offline --locked --release -j12 -p tidb-expr --lib column::tests
    cargo test --offline --locked --release -j12 -p tidb-chunk --lib iterator::tests::go_test
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    for topic in subquery executor/parallel_apply executor/merge_join executor/aggregate; do INTEGRATION_TOPIC="$topic" target/release/build/difftest-result-tests/050430c182ecd301/out/integration_diff-050430c182ecd301 --exact replay_one_topic_from_env --ignored --nocapture --test-threads=12; done
    git diff --check

Control used the same replay loop with `/private/tmp/tidb-native-apply.8dFnea/control-replay`.
No live workload, throughput, parallel Apply, correlated scan range rebuild,
native SQL driver/cache path or Ready/root lint was verified. No performance
baseline changed, permanent guard was added, service was reset, or code pushed.

Current native FD/merge-search WIP (2026-09-08): the existing dependency
graph now supports Go AddFrom/ProjectCols and expression/grouping identities.
Native DataSource, Selection, Projection, Aggregation, Join, Apply and Union
derive dependencies from their resolved trees. Complete table-order columns
survive pruning. Latest-index checks consume connection/isolation and each
source's locking-read flag, not a global precomputed permission bit.

Natural/enforced merge candidates now use those dependencies, Go's key-order
permutation and collation rules, hint precedence, and child requirements.
Forced merge candidates enter the native task dispatcher. The combined probe
exposed DataSource's missing enforcer branch: a valid merge request returned
an invalid task. The source-owned branch now compares naturally ordered and
enforced scans, with Go Fix46177's default-true setting. Before/after evidence
is in probe-before-source-enforcer.log and probe-final.log under
/private/tmp/tidb-native-fd.hrkUNL/.

Changed files: rust/Cargo.lock; tidb-planner/Cargo.toml and src/plan_builder.rs,
logical/{mod.rs,data_source.rs,functional_dependencies.rs},
physical/{mod.rs,merge_join.rs}, find_best_task/dispatch.rs;
tidb-funcdep/src/{fd_graph.rs,fd_graph/projection.rs,lib.rs,tests_extract_fd.rs};
tidb-executor/src/driver/funcdep.rs; this plan. Go source and fixture files
were not changed. Rust-only diagnostic programs stay outside the checkout.

WIP validation uses one connected batch, not per-edit tests. Commands from
rust/ (logs and recovery archive are in /private/tmp/tidb-native-fd.hrkUNL/):

    cargo metadata --offline --filter-platform aarch64-apple-darwin --format-version 1
    cargo test --offline --locked --release -j12 -p tidb-funcdep --lib
    cargo test --offline --locked --release -j12 -p tidb-planner --lib --no-run
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    cargo run --offline --locked --release -j12 --target-dir /Users/qiliu/projects/tidb/rust/target --manifest-path /private/tmp/tidb-native-fd.hrkUNL/probe/Cargo.toml
    cargo run --offline --locked --release -j12 --target-dir /Users/qiliu/projects/tidb/rust/target --manifest-path /private/tmp/tidb-native-merge.yOBLZN/probe/Cargo.toml -- /private/tmp/tidb-native-merge.yOBLZN/spill
    for topic in executor/merge_join planner/funcdep/only_full_group_by executor/aggregate; do INTEGRATION_TOPIC="$topic" INTEGRATION_SHOW_DIVERGENCES=1 target/release/build/difftest-result-tests/d47cccb06c146097/out/integration_diff-d47cccb06c146097 replay_one_topic_from_env --ignored --nocapture; done
    git diff --check

Funcdep tests: 12 passed, three original SQL-to-FD tests remain ignored.
The temporary native probe checks five upstream expected FD graphs on
hand-built resolved trees, dependency projection, metadata/outer/union and
aggregation behavior, 120 merge configurations, collation/ENUM/SET/null-safe
exclusions and native Sort retry. This is not the full original SQL pipeline.
The existing combined runtime probe passes 720 hash and 886 merge cases,
including reopen, current parameters and actual spill cleanup. Fixture counts
match control: merge_join 247/0/12 skipped, only_full_group_by 121/0/27,
aggregate 808/20/64; total 1176 matches, 20 existing divergences, 103 skips.
The replay does not prove the new native tree is active in live SQL.

Remaining risks/gaps: per-node FD cache lifecycle; generated-column catalog
expression binding; session extended-expression IDs and metadata adapters;
native index/Apply task alternatives; complete plan-cache key and SQL entry
integration. No live Go comparison, throughput gain, baseline update, root
lint/Ready claim, whole-package completion or push is claimed.

Merge runtime WIP changes planner physical definitions, hash construction,
resolve_indices and task attachment, plus executor join.rs, join/builder.rs,
join/output.rs, hash_join.rs, physical_builder.rs and merge_join_plan.rs.
The root-fix skill drove the shared ownership and removal of stale helpers;
the WIP profile kept validation at the connected batch boundary. No Go source,
permanent fixture, remote branch, service or accepted baseline changed.
Recovery source and logs: /private/tmp/tidb-native-merge.yOBLZN/.

Commands from rust/ for this batch:

    cargo test --offline --locked --release -j12 -p tidb-planner --lib --no-run
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    cargo run --offline --locked --release -j12 --target-dir /Users/qiliu/projects/tidb/rust/target --manifest-path /private/tmp/tidb-native-merge.yOBLZN/probe/Cargo.toml -- /private/tmp/tidb-native-merge.yOBLZN/spill
    for topic in executor/merge_join executor/jointest/join executor/jointest/hash_join executor/index_lookup_join planner/core/join_reorder_through_projection subquery; do INTEGRATION_TOPIC="$topic" INTEGRATION_SHOW_DIVERGENCES=1 target/release/build/difftest-result-tests/bcafd1d1788d0b66/out/integration_diff-bcafd1d1788d0b66 replay_one_topic_from_env --ignored --nocapture; done
    INTEGRATION_TOPIC=executor/zz_native_merge_batch INTEGRATION_SHOW_DIVERGENCES=1 /private/tmp/tidb-native-merge.yOBLZN/control-replay replay_one_topic_from_env --ignored --nocapture
    INTEGRATION_TOPIC=executor/zz_native_merge_batch INTEGRATION_SHOW_DIVERGENCES=1 target/release/build/difftest-result-tests/bcafd1d1788d0b66/out/integration_diff-bcafd1d1788d0b66 replay_one_topic_from_env --ignored --nocapture
    git diff --check

The witness commands require restoring witness.test and witness.result from
the evidence directory to their temporary executor/zz_native_merge_batch
locations. They are deliberately not permanent Rust-invented tests.
Native candidate enumeration, full comparison-domain parity, nullable-marker
and null-aware joins, nullable anti residuals, SQL/cache integration, live
TiKV behavior, throughput and Ready/root lint remain unverified. No package
completion or performance win is claimed.

Current search WIP adds `physical/hash_join.rs` and changes
`find_best_task.rs`, `find_best_task/dispatch.rs`, and the physical module
registration. Native hash candidates retain full expressions and statistics;
they no longer require another reduced logical join tree. The shared build-side
rule is also used by the live reduced-tree search. General dispatch currently
admits only Go's early-return forced-hash family and null-aware joins, whose
search excludes merge/index candidates. Unhinted joins remain explicitly
unfinished, not silently narrowed to hash. This is not SQL/cache integration.

Evidence/recovery: `/private/tmp/tidb-join-search.dDXPEk/`. The combined
`probe-final.log` records 2016 candidate configurations plus native planning,
physical binding and execution. Its first run reused a search context after
mutating a logical plan with the same ID; the diagnostic now uses a fresh
context, as required by Go's per-search task map. No production cache bypass
or plan fingerprint was added. The planner library test target builds in 16s,
and the final replay target builds in 38s. All seven rebuilt fixture reports
match control counts: 1292 matches, one pre-existing divergence, 55 skips
(`control-fixtures.log`, `candidate-fixtures.log`). Scoped formatting and
`git diff --check` pass.

Commands from `rust/` for this batch:

    cargo test --offline --locked --release -j12 -p tidb-planner --lib --no-run
    cargo test --offline --locked --release -j12 -p difftest-result-tests --test integration_diff --no-run
    cargo run --offline --locked --release -j12 --target-dir /Users/qiliu/projects/tidb/rust/target --manifest-path /private/tmp/tidb-join-search.dDXPEk/probe/Cargo.toml
    for topic in planner/core/join_reorder_through_projection planner/core/rule_join_reorder planner/core/join_reorder2 executor/jointest/join explain_join_stats agg_predicate_pushdown subquery; do INTEGRATION_TOPIC="$topic" INTEGRATION_SHOW_DIVERGENCES=1 target/release/build/difftest-result-tests/bcafd1d1788d0b66/out/integration_diff-bcafd1d1788d0b66 replay_one_topic_from_env --ignored --nocapture; done
    rustfmt --check --edition 2021 --config skip_children=true crates/tidb-planner/src/physical/hash_join.rs crates/tidb-planner/src/find_best_task/dispatch.rs
    git diff --check

Validation remains WIP. Ready/root lint, live native SQL/cache execution and
sysbench/TPC-C are not verified. No speedup, baseline update or push is claimed.

Current join WIP updates `join.rs`, `hash_join.rs`, `physical_builder.rs` and
adds `join/builder.rs` / `join/output.rs`. It does not connect native SQL/cache
execution or establish a throughput improvement. Recovery and evidence live
in `/private/tmp/tidb-native-join.6Ve6Rv/`. `probe-final.log` records 720 passing
cases after correcting parallel output allocation; the probe asserts bounded
chunks, reopens executors and verifies positive spill high-water usage followed
by zero disk usage after Close. Go `HashJoinV1Exec.Close` closes its row container;
the old Rust `try_unwrap(Arc::clone(...))` could never obtain unique ownership.
The first eight fixture reports retain the same counts as the saved control:
2727 matches, 46 known divergences and 167 skips. The final release rebuild
passes in 2m29s (`build-final.log`), and every rebuilt topic retains those
control counts (`candidate-final-fixtures.log`). These are WIP comparisons,
not package-completion evidence.

Commands from `rust/` for this batch:

    cargo test --offline --locked --release -j12 --no-run -p tidb-executor -p difftest-result-tests
    cargo run --offline --locked --release -j12 --target-dir /Users/qiliu/projects/tidb/rust/target --manifest-path /private/tmp/tidb-native-join.6Ve6Rv/probe/Cargo.toml -- /private/tmp/tidb-native-join.6Ve6Rv/spill
    for topic in executor/aggregate executor/jointest/join planner/core/join_reorder_through_projection subquery select executor/jointest/hash_join executor/merge_join executor/index_lookup_join; do INTEGRATION_TOPIC="$topic" INTEGRATION_SHOW_DIVERGENCES=1 target/release/build/difftest-result-tests/bcafd1d1788d0b66/out/integration_diff-bcafd1d1788d0b66 replay_one_topic_from_env --ignored --nocapture; done
    rustfmt --check --edition 2021 --config skip_children=true crates/tidb-executor/src/join/builder.rs crates/tidb-executor/src/join/output.rs crates/tidb-executor/src/physical_builder.rs
    git diff --check

Ready/root lint, live SQL native-plan execution and sysbench/TPC-C remain
unverified. No service or benchmark fixture changed; no baseline update or push.

Current aggregate WIP adds `rust/crates/tidb-executor/src/hash_agg/builder.rs`
and updates `physical_builder.rs`, `hash_agg.rs` and `hash_agg/parallel.rs`.
The physical builder consumes Go-shaped descriptors with the existing runtime
operators; there is no additional aggregate evaluator. The standalone
`stream_agg.rs` is not registered in the crate and was left unchanged.
Native aggregate construction is still outside SQL-driver/cache execution.
The live runtime also receives two source-backed corrections: COUNT's literal
shortcut cannot consume a parameter marker's saved payload, and integer
column fast paths require the chunk owner's fixed-width layout. Go
`expression.Column.EvalInt` decodes hybrid types rather than reading them as
eight-byte cells. The common layout rule covers grouping and partial-count
reads in serial/direct/worker paths; it does not disable aggregation workers.

Recovery and evidence are in `/private/tmp/tidb-native-aggregation.odyNCh/`.
The combined temporary probe initially passed hash/global/grouped stream
construction, nine folds, final count/AVG, empty-input modes, DISTINCT,
parameters, concat ordering/separator, approximate aggregates and multi-chunk
inputs (`probe.log`). The additional marker case failed before the correction
(`count-before.log`: expected 0, got 3). The Go `executor/aggregate` fixture
crashed in the saved control binary on `select a from t group by 1` with a
BIT(1) primary key (`aggregate-sql-before.log`): the worker attempted an
eight-byte read from two variable-width cells. This is a pre-existing failure,
not a passing fixture comparison. The expanded probe passes after the root
corrections (`probe-final.log`), including BIT/ENUM/SET keys across chunks and
the current-NULL marker case. The aggregate fixture now completes: 808 matches,
20 unresolved differences and 64 skips (`candidate-final-fixtures.log`).
The four other reports remain identical to control: 581 matches, two existing
differences and 26 skips. The newly reachable aggregate report is not a full
parity pass; its unresolved classes are listed in Progress. No new permanent
tests, Go files or workload data changes.

Exact aggregate-batch commands from `rust/`:

    cargo test --offline --locked --release -j12 --no-run -p tidb-executor -p difftest-result-tests
    cargo run --offline --locked --release -j12 --target-dir /Users/qiliu/projects/tidb/rust/target --manifest-path /private/tmp/tidb-native-aggregation.odyNCh/probe/Cargo.toml -- /private/tmp/tidb-native-aggregation.odyNCh/spill
    INTEGRATION_TOPIC=executor/aggregate INTEGRATION_TRACE_SQL=1 RUST_BACKTRACE=1 /private/tmp/tidb-native-aggregation.odyNCh/control-integration-diff replay_one_topic_from_env --ignored --nocapture
    for topic in executor/aggregate agg_predicate_pushdown select subquery planner/core/join_reorder_through_projection; do INTEGRATION_TOPIC="$topic" INTEGRATION_SHOW_DIVERGENCES=1 INTEGRATION_TRACE_SQL=1 target/release/build/difftest-result-tests/bcafd1d1788d0b66/out/integration_diff-bcafd1d1788d0b66 replay_one_topic_from_env --ignored --nocapture; done
    git diff --check

The release build passed after the root corrections in 2m28s
(`build-final.log`). Formatting-only changes to existing dirty source were
removed using the pre-edit archive; unrelated contributor edits are retained.
This remains WIP validation. Ready lint, live prepared-cache integration,
sysbench/TPC-C measurements and full Go-package completion remain unproven.

Current executor-builder WIP changes, under `rust/crates/tidb-executor/src/`:
new `physical_builder.rs`, `max_one_row.rs` and `union_all.rs`; updated
`lib.rs`, `projection.rs`, `selection.rs`, `limit.rs`, `topn.rs`,
`topn_spill.rs` and `driver/set_opr.rs`; deleted invented
`projection/tests/programs.rs`. The existing SQL driver uses the same moved
UnionAll, fresh projection/filter/limit constructors and TopN output handling;
it does not yet consume native physical trees. No new permanent tests or
protection gates were introduced. Deleted dirty files can be recovered from
`/private/tmp/tidb-executor-builder.koHmnZ/before.tar.gz`; the related pre-edit
files are in `related-before.tar.gz` beside it.

Batch commands from `rust/` (logs in that same temporary directory):

    cargo test --offline --locked --release -j12 --no-run -p tidb-executor -p difftest-result-tests
    cargo run --offline --locked --release -j12 --target-dir /Users/qiliu/projects/tidb/rust/target --manifest-path /private/tmp/tidb-executor-builder.koHmnZ/probe/Cargo.toml -- /private/tmp/tidb-executor-builder.koHmnZ/spill
    target/release/build/tidb-executor/42a5b97ce3d3b987/out/tidb_executor-42a5b97ce3d3b987 test_generate_topn_results_when_spill_only_once --nocapture
    for topic in expression/constant_fold agg_predicate_pushdown planner/core/join_reorder_through_projection window_function topn_pushdown topn_push_down select subquery; do INTEGRATION_TOPIC="$topic" INTEGRATION_SHOW_DIVERGENCES=1 target/release/build/difftest-result-tests/bcafd1d1788d0b66/out/integration_diff-bcafd1d1788d0b66 replay_one_topic_from_env --ignored --nocapture; done

The corrected batch build passed in 2m25s (`build-final.log`); its initial
failure was missing test-only imports after removing the projection wrapper.
The temporary probe passed (`probe-final.log`), including 32 projected TopN
spill runs; its initial build failure was a probe-only `Vec::repeat` use and
did not require a production change. The retained Go single-spill test passes
(`go-topn-spill.log`). Control/candidate fixture reports and divergence details
are identical after removing elapsed times: 642 matches, two existing
divergences and 28 skips. The existing differences are float rendering and
binary-to-UTF8 error handling in `select`. Skips are not parity evidence.
The probe validates runtime construction, not real transaction readers or
live prepared-cache reuse. The reviewed-source release rebuild passed in
2m32s (`build-reviewed.log`). The final combined probe, Go single-spill test
and eight-topic fixture replay pass with the same results
(`probe-reviewed.log`, `go-topn-spill-reviewed.log`,
`candidate-reviewed-fixtures.log`). Scoped rustfmt and `git diff --check`
also pass. No live service, workload fixture,
benchmark baseline, remote branch or Go file was changed. No speedup or whole
Go package completion is claimed; validation remains WIP, not Ready/root lint.

Current physical-plan WIP: `physical/resolve_indices.rs` replaces the empty
traversal with Go-derived binding for the represented operators. Stable
UniqueIDs are preserved while runtime indexes follow child schemas. HashJoin
uses separate input-local key indexes and joined-row residual/output indexes;
Apply rebinds equality predicates against the joined row. Projection and
Limit/TopN handle repeated column identities using Go's distinct algorithms.
The shared `physical_property::ColumnSortItem` replaces `WindowSortItem` and
is also used by physical partition orders. Sorts retain full expressions.
Reader conversion now supplies output schemas and index-reader columns.

Changed files in this batch, under `rust/crates/tidb-planner/src/`:
`physical/mod.rs`, new `physical/resolve_indices.rs`, deleted
`physical/tests.rs`, `physical_property.rs`, `enforce.rs`, `task.rs`,
`find_best_task/dispatch.rs`, `logical/window.rs`, `plan_builder/window.rs`,
`logical/operator_tests.rs`, and `logical/rule_tail_tests.rs`. The last two
only update consumers of the shared order type. This ExecPlan is also updated.
No Go files, fixtures, session cache behavior or accepted performance baseline
were changed. Recovery archives and validation logs are in
`/private/tmp/tidb-physical-plan-migration.TjMaJG/`.

WIP commands from `rust/`:

    cargo test --offline --locked --release -j12 --no-run -p tidb-planner -p tidb-executor -p tidb-session -p difftest-result-tests
    cargo run --offline --locked --release -j12 --target-dir /Users/qiliu/projects/tidb/rust/target --manifest-path /private/tmp/tidb-physical-plan-migration.TjMaJG/probe/Cargo.toml
    for plan_topic in expression/constant_fold agg_predicate_pushdown planner/core/join_reorder_through_projection window_function topn_pushdown topn_push_down; do INTEGRATION_TOPIC="$plan_topic" INTEGRATION_SHOW_DIVERGENCES=1 target/release/build/difftest-result-tests/bcafd1d1788d0b66/out/integration_diff-bcafd1d1788d0b66 replay_one_topic_from_env --ignored --nocapture; done
    git diff --check

The initial build found one outdated Apply constructor; the corrected
consolidated build passed in 3m24s (`build-final.log`). The temporary probe
checks binding, not live SQL execution or Go package completeness, and adds no
permanent test or gate. It passed (`probe.log`). Its temporary lockfile was
resolved offline from the workspace lockfile with host-platform filtering;
an unfiltered metadata query initially requested an unavailable Android-only
dependency and was not used as validation evidence.
The fixture reports (`control-fixtures.log`, `candidate-fixtures.log`) are
identical apart from elapsed time: 81 Rows, 208 SideEffect and 121 PlanProperty
matches. Ten unsupported checks, four recorder rewrites and two plans without
compared properties remain skipped; `topn_pushdown` has zero compared checks.
These replays guard existing SQL behavior but do not exercise the new typed
tree through the SQL driver. Runtime builder/cache integration, live reuse and
invalidation, performance measurements and Ready/root lint remain unverified.
No benchmark fixture or live service was changed, and nothing was pushed.

Current WIP: statistics binding is shared within each derivation. Release replay,
executor test target and server builds passed. The six fixture reports are
identical to the prior replay after excluding times, including the four
`explain_easy_stats` discrepancies. Live prepared results match Go. One-client
fixed-work medians: 410.79 -> 412.98 TPS, SQL CPU 4.14 -> 4.12 seconds per 3000
transactions. Eight-client medians: 1851.26 -> 1854.78 TPS (+0.19%). These small
differences are not an accepted speedup. No accepted baseline changed.
Exact commands, hashes and result paths are under
`statistics_column_binding_followup` in the existing observations JSON.
Artifacts and the pre-edit source snapshot are in
`/private/tmp/tidb-statistics-columns.dDczpg/`; current candidate manifest is
`/private/tmp/tidb-batching-candidate-f58diddf/manifest.json` (port 53074).
The candidate supervisor owns only its new server; retained Go/PD/TiKV and
prior Rust supervisors remain untouched. No workload writes or fixture reset.

The preceding ownership changes had selected live Go parity evidence, but their
8-client timing regressed against control; no performance acceptance. General
prepared execution still reconstructs plans behind access-path pins, unlike
Go's retained physical plan and range rebuild. Fresh matched profiles show
rewriting sample weight falling from 0.951s to 0.366s and expression cloning
from 0.236s to 0.122s, but access commitment rising from 1.941s to 2.118s.
The candidate's 1.117s DataSource-statistics path includes repeated histogram
selectivity and expression rewriting under general prepared execution.
Go cached-range rebuilding accounts for 0.302s. These are inclusive sample
weights from separate captures, not additive CPU costs or proof of the full
regression cause. Five representative text plans are identical to control;
prepared request/storage work still needs a fixed-work comparison.
The subsequent fixed-work run confirms lower candidate SQL CPU, but does not
establish extra requests: aggregate batch-command medians are 66306 control
and 66336 candidate per 3000 transactions, and asynchronous coprocessor metrics
have inconsistent snapshot boundaries. Single-client median TPS is 406.01
control, 403.70 candidate, 395.61 Go. Settings match at paging 128/50000,
chunks 32/1024, and DistSQL concurrency 15. The bounded EXPLAIN ANALYZE request
did not execute because automatic approval review failed with a connection
error; do not treat that as a SQL failure or bypass the rejection.
Confirmed invented guards are removed; whole-workspace
test lineage, full implementation parity and physical-plan reuse remain open.

Revision: native scans now retain detached access conditions, residual filters
and stable key metadata; correlated range rebuilding uses the existing ranger.
Consolidated WIP checks pass. Transaction-owned reader wiring, full native
index alternatives/projections/costing, parallel Apply, FD cache lifecycle and
SQL/cache integration remain open; no live performance claim.

Revision: ordinary RPC publication now uses entry identity without a receipt
channel. Consolidated validation and both live comparison windows are recorded
in `rust/benchmarks/entry-publication-baseline.json`; the residual eight-client
regression and immediate-control variability remain open.

Revision: detached secondaries now suspend independent tasks during response
waits instead of serializing transactions. Scheduling evidence and consolidated
validation are recorded; mixed-workload performance remains unverified.

Revision: foreground transaction batches now share response-first completion.
Only failure classification retains a publication barrier; admissions, caller
order and sibling results are preserved. Consolidated WIP validation is recorded.

Revision: the obsolete transaction actor is removed in one ownership migration.
Consolidated checks distinguish five pre-existing server failures, and live
transaction lifecycle cases match Go. Workload measurement remains separate
from implementation and package-completion evidence.
