# Remove unnecessary runtime crossings while preserving Go request ownership

This living ExecPlan follows root PLANS.md. Preserve the full sysbench/TPC-C
throughput and latency objective. Earlier increments restore pessimistic row
locking, Go-shaped index/statistics planning and per-session process publication.
The current increment preserves statistics-loading services through catalog
refresh. Live request logging proved a refreshed catalog has histogram demand
but no service; Go's domain owns the loading handle independently of InfoSchema.
The preceding increments reconcile column-statistics validity and restore the
current execution context for prepared range costing.
Full performance and whole-Go-package acceptance remain open.


## Purpose / Big Picture

Make generic Rust SQL execution faster without workload-specific shortcuts,
different SQL results or weaker cancellation/lifecycle semantics. Go source is
the authority for task concurrency, batching, paging, retries and results.
Acceptance requires beating the immutable faster Rust control on matched work,
then validating TPC-C and mixed writes. CPU savings alone are insufficient.


## Progress

- [x] Trace the remaining TPC-C SortExec to the actual prepared non-covering
  customer query: instrumented 0a61b84af5 chooses a table scan with 30,000-row
  access estimate. The 6,000-transaction cold probe and all 11 consistency
  checks pass. Temporary instrumentation is removed; evidence is retained in
  /private/tmp/tidb-prepared-order.Y13Xya. Ordinary bound EXPLAIN uses the index.
- [x] Locate the generic costing cause: InitStats constructs a name-only
  FromScope and copies only its time zone. index_range::constant_value asks
  resolver.param_value for EXECUTE markers, which fails without the statement
  context; range costing falls back to the table row count. Go stats.go builds
  ranges through ds.SCtx().GetRangerCtx(). Execution has the real context, so
  row correctness does not expose the costing error.
- [x] Extend retained prepared-cache coverage: analyzed composite-index EXECUTE
  estimates 20 rows before the fix versus literal EXPLAIN's 1 row. Carry the
  current statement context through InitStats and reuse one per-source scope
  for predicate extraction and costing. The paired session batch improves from
  119 passed / 9 failed to 121 passed / 7 failed; the range-estimate regression
  and existing range-quota case pass. The other seven failure bodies are identical.
- [x] Remove the obsolete Rust-only access-cost test that embeds marker values
  but supplies no parameter context. Go Constant.GetUserVar reads current
  context parameters; the retained end-to-end cache test covers that path.
  Do not restore a stale-AST-value fallback to satisfy the removed test.
- [x] Final range batch: 65 passed; all-target check, release build, Ready lint
  and changed-source formatting pass. Eight prepared result comparisons match
  Go. Complete 72,000 measured sysbench events and 50,000 uninstrumented
  candidate TPC-C transactions with all 11 consistency checks. The paired
  session batch still has seven unchanged failures; this is not a green release.
- [x] Re-profile the original TPC-C chain: SortExec::next remains 627ms versus
  baseline 643ms, about 4% of Running samples in both. A separately instrumented
  6,000-transaction candidate run proves the actual cached customer scan now
  estimates 1 row rather than 30,000, yet retains scan-plus-sort. The parameter
  context bug is fixed; the remaining costing discrepancy is not. Separate
  bound EXPLAIN uses the same ordered lookup as Go. No speedup acceptance or
  baseline promotion: timing overlaps, mediaanalysisd uses roughly 90% CPU,
  Go auto-analyze runs during the pair and the fixture grows.
- [x] Remove temporary instrumentation; verify all 11 owned PIDs absent and
  ten ports closed, retaining the fixture. Receipt and exact commands:
  benchmarks/prepared-range-context-validation.json.
- [x] Reproduce Go ColumnStatsIsInvalid mismatches: a retained empty histogram
  estimates 1 instead of the source pseudo estimate 0.127; an evicted nullable
  common-handle column with positive NDV estimates 1 instead of 0.01. The latter
  retains a nonzero NULL count, so checking TotalRowCount alone is insufficient.
  Red logs: /private/tmp/tidb-column-validity.EXqPAR/column-red-verified.log and
  handle-red-verified.log. Preserve the valid evicted all-NULL/zero-NDV case.
- [x] Centralize the collection's pseudo/payload/essential-load validity gate
  and apply it to integer/common handles, partial-index costing, exponential
  backoff inputs and column selectivity. The public reduced-column estimator
  rejects zero-row payloads too. Remove the misplaced ordinary-column index
  fallback: Go builds column and index nodes separately. Retire its two
  helper-only tests and the obsolete public-entry empty-histogram assertion;
  source pseudo fixtures and live execution remain the verification path.
- [x] Column-validity batch: 14 planner and 63 executor tests pass; workspace
  all-target check, release build, Ready lint and changed-source formatting pass.
  Paired session results are 106 passed / 6 failed / 1 ignored on both revisions.
  Four failing estimate values change; two failure bodies are unchanged. Retain
  all six unresolved fixtures. This is not a fully green release.
- [x] Eight prepared result comparisons match Go. Complete 72,000 measured
  sysbench events and 50,000 uninstrumented candidate TPC-C transactions, with
  all 11 consistency checks passing. SortExec::next remains 614ms versus 627ms,
  about 4% of Running samples. No accepted speedup or baseline promotion.
- [x] Trace the remaining customer statistics gap through a separate request
  probe: columns 2/3/6 and both index payloads are requested with a 100ms wait,
  but the planning catalog has no load service. Session construction installs
  it; ClusterServerSession::rebuild_catalog_now replaces the catalog without
  installing it. Go's Domain.StatsHandle survives schema refresh. The two
  diagnostic runs each complete 6,000 TPC-C transactions and 11 consistency
  checks. Remove all instrumentation; verify 11 owned PIDs absent and ten ports
  closed, retaining the fixture. Exact evidence and commands are in
  benchmarks/column-statistics-validity-validation.json.
- [x] Preserve domain statistics-loading ownership across catalog refresh.
  Reproduce through the existing refresh/load test surface and verify actual
  prepared execution plus matched workloads. The lifecycle gap is repaired;
  some scan-plus-sort choices remain and the full performance goal stays open.
- [x] Extend the existing Go-derived DDL-after-load case with evicted-column
  and index demand after stats-only and schema refresh. It fails before the
  fix because the column payload is not reloaded. Retain the storage loader
  and domain pool on the connection, attaching both through one helper to
  every rebuilt catalog. All 16 selected statistics tests pass afterward.
- [x] All-target workspace compilation, release build, Ready lint and scoped
  changed-line formatting pass. Eight prepared results match Go. Artifacts:
  /private/tmp/tidb-stats-load-lifetime.PIJjEK. Original sandbox attempts fail
  on sysctl hw.memsize (test harness) and Go module download (lint); authorized
  reruns pass. Neither is a semantic regression result.
- [x] Diagnostic TPC-C: 6,000 transactions and 11 consistency checks pass.
  All 33 captured nonempty requests have a load service. Actual customer
  execution now includes an ordered IndexLookUp (6.30 rows); another execution
  still uses scan-plus-sort (3,000 access rows, 19.90 output rows). Remove probes.
- [x] Finish 72,000 measured Rust and 24,000 Go sysbench events, 50,000
  uninstrumented candidate TPC-C transactions and fresh Go/Rust CPU profiles.
  All consistency checks pass. TPC-C SortExec::next is 343ms / 15.136s Running
  versus prior 614ms / 15.757s; fresh Go has no SortExec::Next sample. Sysbench
  SortExec::next remains 2.565s versus Go's 170ms; inclusive call boundaries
  differ, so these are not normalized per-transaction cost comparisons.
  End-to-end timings overlap; no performance acceptance or baseline promotion.
  Verify ten owned PIDs absent and ten ports closed, retaining the fixture.
  Receipt: benchmarks/statistics-loading-lifetime-validation.json.
- [x] Reproduce cross-catalog statistics publication in the retained Go-derived
  ddl_after_loaded_statistics_matches_go test. A peer opened with evicted
  statistics still sees an evicted column after another session completes its
  domain load. Red log: /private/tmp/tidb-stats-publication.2G2PqG/peer-red.log.
- [x] Replace independent cluster planner copies with a shared live conversion
  view over SharedStats. Every read must resolve the canonical table; memoized
  conversions must include schema metadata and retain weak source identity,
  so a load, eviction, ANALYZE or DDL cannot reuse stale values. Catalog schema
  metadata includes partition and temporary-table IDs. Remove StatsTemplates
  and the loader's session-only conversion publication. Remove statistics-only
  schema rebuilds and temporary-statistics reinstall. ANALYZE returns computed
  results directly instead of reading back through a planner cache.
- [x] Candidate statistics batch: 17 pass / 4 fail. All four remaining failures
  reproduce with the same assertions on unmodified 15a5025ac9: add-column DDL,
  partition truncation, global partition plan expectation, and global-temporary
  statistics publication. Baseline logs are baseline-tests.log and
  baseline-temporary.log under /private/tmp/tidb-stats-publication.2G2PqG.
  These are retained failures, not waived parity claims.
- [x] Nine loader/concurrency tests pass. Native ANALYZE: 14 pass / 5 fail;
  baseline-native.log reproduces all five failures on unmodified 15a5025ac9.
  All-target workspace compilation, release build, Ready lint and changed-line
  formatting pass. The initial candidate is candidate-server under
  /private/tmp/tidb-stats-publication.2G2PqG, SHA-256
  02c82ba8119a426283f82676dd391471a5c7c85f217f2d4add030b33b49a7c55.
  It passes prepared results and an ABBA trial but does not establish a speedup.
  Final source review shares the initially empty schema memo itself across
  transaction clones (Arc<OnceLock<_>>), avoiding a new build per transaction.
  Final combined tests retain 40 passes and the same nine baseline failures.
  Workspace compilation, lint and release build pass again. The final frozen
  binary is publication-server, SHA-256
  18340f03da2ebb0d15d62bdc024df525dc7a202fd9c233a6263806c8dd0f44e9.
  No whole-package or all-tests-pass claim.
- [x] Finish live timings and Go/Rust profiles for this increment. The former
  session-only publication is removed from cluster loading. The peer regression
  verifies shared loaded values and eviction without rebuilding its schema.
  Establish the performance effect on remaining scan-plus-sort in live trials;
  the reproduced visibility failure establishes correctness, not a speedup.
- [x] Final eight-client TPC-C: 6.549147 / 6.516980 seconds per 6,000
  transactions versus 6.554072 / 6.550132 before; Go 6.197479 / 6.257330.
  Final sysbench 1/8/32-client before/after values overlap. Eight final prepared
  comparisons match Go; all live consistency checks pass, including 32,000
  profiled transactions each on final Rust and Go. No speedup acceptance or
  baseline promotion: background macOS CPU and Go auto-ANALYZE are recorded.
  Final Rust SortExec::next is 2.546s sysbench / 409ms TPC-C; Go 177ms / 1ms.
  These inclusive frames have different caller boundaries: Go fetchChunksParallel
  starts fetch/worker/result goroutines, while Rust fetches under next.
  Go also has substantial condition-variable signaling; the traces do not prove
  excess Rust wakeups. Full traces and caller chains are retained in the artifact
  root. Verify 15 owned PIDs absent and 10 ports closed; retain the data fixture.
  Remove the clean, recreatable baseline checkout after retaining its logs.
  Receipt: benchmarks/statistics-publication-validation.json records final
  source/binary hashes, exact commands, known failures and raw timing samples.
- [ ] Continue causal latency attribution using comparable work/request/poll
  counts and Go/Rust call chains, not inclusive SortExec::next totals. Remaining
  scan-plus-sort choices and the nine reproduced baseline failures remain open.
- [x] Inspect final 6ef4aea7aa source/profile and Go GetGlobalConfig/UpdateGlobal.
  Statement context's instance-variable read deep-copies the full Rust Config;
  Go reads a published pointer and clones only in UpdateGlobal.
- [x] Publish shared configuration snapshots and reconcile consumers: readers
  borrow fields, writers explicitly clone, restore guards retain the old snapshot.
  Keep publication side effects and serialization semantics unchanged.
- [x] Validate config/load/restore/error-extension/session variable consumers as
  one batch, compile all targets, and remeasure sysbench/TPC-C with exact binaries.
  Profile the original configuration-copy chain before publishing the increment.
- [x] Configuration-only candidate passes 125 retained tests, all-target
  compilation and Ready lint. Its 72,000 measured sysbench events and 50,000
  candidate TPC-C transactions pass result/history and 11 consistency checks.
  Profiled global-config read cost falls from 103/57ms to 1/1ms (sysbench/TPC-C).
  Timings remain diagnostic: shared CPU, growing data and live auto-analyze.
- [x] Prepared-query probe returns matching customer/order rows but fails on
  Rust's EXPLAIN of the delivery locking read: domain not found for ctx. Go's
  bound EXPLAIN succeeds. The wrapper misses the conditionally attached schema;
  statement_has_lock also clones/walks every ordinary AST just to decide attachment.
- [x] Reuse immutable latest-index metadata per catalog version and attach it to
  every statement context, like Go's domain availability. Delete the AST lock
  probe and duplicate RC/locking/DML attachment branches. Extend the retained
  Go-derived EXPLAIN fixture, prove failure before, and validate after alongside
  DDL, temporary-table and prepared-plan behavior before the final live rerun.
- [x] The exact delivery EXPLAIN fails with domain-not-found before the index
  change and passes afterward; the enclosing fixture then hits its existing
  USE INDEX() assertion. The paired broad batch has identical 18 failing test
  names and identical other 17 failure bodies, with 149 baseline / 148 candidate
  passes (one obsolete Rust-only domain-absence test removed). Final focused
  session batch passes 160 cases with the existing index-hint and range-quota
  failures retained. Historical reads, temporary tables and prepared statements
  are included. All-target compilation, changed-source format and Ready lint pass.
- [x] Freeze the combined binary; compare bound locking EXPLAIN and prepared
  results against Go, rerun matched sysbench/TPC-C, export CPU profiles and record
  limitations. Stop owned services and verify cleanup before normal publication
  to origin/hparser-integration. No accepted speedup or whole-package claim yet.
- [x] Combined binary e008264dca0c passes eight prepared result comparisons;
  delivery locking EXPLAIN matches Go exactly. Complete 72,000 fixed sysbench
  events without errors and 50,000 candidate TPC-C transactions across all five
  types with all 11 consistency checks. Profiles reduce global configuration
  lookup from 103ms to 3ms in sysbench and index-schema rebuild from 354ms to a
  1ms initialization sample in TPC-C. Samples are not normalized speedup proof.
  Eight-client sysbench TPS rises 1.73%, but 32-client falls 1.25%; active Go
  auto-analyze and mediaanalysisd at 69.6-93.7% CPU confound timing. No performance
  baseline promotion. Receipt: benchmarks/shared-statement-metadata-validation.json.
  All 17 owned PIDs are absent and ten ports closed; retain the fixture.
- [x] Profile published 5d48c8e9c4 and Go on both workloads and the saved
  control on read-only sysbench. All 84,000 fixed-work sysbench transactions
  have zero errors/reconnects; each Go/Rust TPC-C run completes the same 32,000
  seeded transaction mix and passes all 11 consistency checks. Artifact root:
  /private/tmp/tidb-after-index-profile.V83erd. Shared-host timings are diagnostic.
- [x] Attribute ProcessRegistry.statement_started to 1.041 seconds of 15.817
  sampled running CPU-seconds in Rust TPC-C. Go SetProcessInfo reads a session
  SQLDigest memo; Rust normalizes the same SQL twice under the global registry
  mutex. A live transaction shows each SELECT digest once in Go and twice in Rust.
- [x] Extend the retained transaction-history fixture: the original code records
  seven digests instead of five. Separate publication from execution history,
  including repeated identical executions inside one retained command. Keep entry
  mutation/normalization outside the registry directory lock.
- [x] Final session scope passes 91 cases with the same unindexed prepared-cache
  range-quota failure as the unchanged baseline. All three final-source TCP
  process-list/KILL/transaction-control cases, release build and Ready lint pass.
  Final real-TiKV runs complete 72,000 measured read-only sysbench events with no
  errors/reconnects and 50,000 candidate TPC-C transactions, with all 11 consistency
  checks passing after every run. Exact binary, commands and measurements are in
  benchmarks/process-publication-validation.json. All owned cluster PIDs are gone
  and ports closed; full performance and whole-package acceptance remain open.
- [x] Profile the corrected 2f292578af binary and Go on both sysbench and TPC-C,
  with fixed-work timing separated from CPU/native captures. Artifact root:
  /private/tmp/tidb-post-lock-profile.VGAM1f. Treat shared-host timings as
  diagnostic while mediaanalysisd is active; select edits from measured call chains.
- [x] Compare cold and warm plans with Go: the customer secondary index should
  cover the common handle; Rust instead requires a table lookup. Cold metadata-only
  primary histograms also reach estimation despite Go's TotalRowCount()==0 gate.
  Both source-backed regression checks fail before the two edits and pass after.
- [x] Validate the generic histogram-validity and common-handle coverage fixes
  with cold/warm real-TiKV plans, exact Go result comparisons, fixed-work sysbench
  and TPC-C consistency checks. Do not accept shared-host timing as a speedup.
- [x] Cold real-TiKV plans expose stale pre-load access estimates after the
  first two fixes. Initialize from the loaded snapshot at SyncWaitStatsLoadPoint,
  before join reorder, clear obsolete estimates, and remove the partition-only
  refresh. Resolve index NDV columns against the pruned schema by identity.
- [x] Candidate and unmodified 01a0087a34 both pass 178 executor and 36 planner
  cases, with the same five executor failures. Release build, Ready lint and
  formatting pass. Two cold/warm candidate runs return Go-equivalent query rows,
  complete 24,000 TPC-C transactions with all 11 postchecks, and keep sysbench
  error-free. Record diagnostic before/after timings and limits in
  benchmarks/index-path-statistics-validation.json. Stop all owned services and
  verify PIDs absent and ports closed before publication.
- [x] Attribute the remaining merged-code profile to source: every statement
  rebuilds all catalog names for sequence resolution (1.877s sampled CPU in an
  eight-second trace), absent from the faster control's hot paths.
- [x] Read Go SequenceOperatorProvider -> GetSequenceByName and SequenceState.
  The lookup is per requested name; LASTVAL is keyed by sequence ID.
- [x] Reproduce Go TestSequenceFunction's drop/recreate LASTVAL case: Rust
  returns 1 instead of NULL. Preserve the failing log before implementation.
- [x] Replace eager sequence/name maps with a shared immutable schema view,
  preserve sequence IDs from creation/catalog loading, and validate retained
  sequence SQL/allocator/error/transaction fixtures as a batch.
- [x] Rebuild and measure this statement-context increment at 1/8/32 clients;
  verify result parity and removal of the name-enumeration profile chain.
- [x] Run 74 scoped retained tests, release build, formatting and Ready lint.
  The Go-derived DROP/CREATE LASTVAL regression fails before and passes after.
- [x] Prepare a fresh one-warehouse TPC-C fixture with all 12 prepare checks.
  Go/before/after each complete 100 seeded transactions with identical type
  counts and the driver's 11 standard post-run consistency checks.
- [x] Broaden to 6,000-event one/eight-client TPC-C trials. Stop when the
  pre-change Rust binary corrupts 377 customer balance relationships; retain
  the failed fixture and reject the incomplete performance comparison.
- [x] Reproduce the locking gap with two prepared connections: Go waits and
  reads row 2 after row 1 is deleted; both Rust binaries immediately return
  row 1. Find the uncalled build_select_lock and missing PhysicalLock
  executor construction. This gap predates the sequence/context increment.
- [x] Wire Go's physical locking semantics through the real planner/executor,
  then prove the two-connection case and rerun TPC-C on a fresh valid fixture.
- [x] Reproduce four existing range/aggregate lock regressions before edits:
  `locking-red.log` in `/private/tmp/tidb-physical-lock.5cG54c` (four pass,
  four fail). The losing transaction returns the deleted row or stale sum.
- [x] Build SelectLock after WHERE and before projection/aggregation, retain
  physical row identities, construct the executor from the physical plan,
  and delete the uncalled AST wrapper. Verify text and prepared execution,
  including hidden handles, LIMIT, retry, and lock wait policy as one batch.
- [x] Nine scoped locking tests pass, including prepared replay, OF targets,
  NOWAIT and WAIT. The same 104-case integration scope improves from 77/27
  pass/fail on committed 70bafc8 to 81/23, with no newly failing cases.
- [x] Fast-forward collaborator commits through c13931936c without conflicts,
  including the final two-line stats-reload revert. The locking diff is identical.
- [x] Validate the 1da08c426a-based binary on fresh real TiKV: prepared contender
  waits and reselects row 2 like Go; Rust completes 6,000 TPC-C transactions at
  each of one/eight clients and passes all 11 checks after each run. Preserve
  evidence in benchmarks/physical-select-lock-validation.json; no timing claim.
- [x] Stop owned Rust/Go/PD/TiKV nodes and verify PIDs absent and ports closed.
- [x] Prepare the scoped locking checkpoint after post-revert validation: nine
  locking tests, release build, Ready lint and formatting pass. Publish only to
  origin/hparser-integration, retaining the collaborator revert.
  Partition-ID propagation, shared-lock promotion, nested locking-read timestamps,
  optimistic locking and online DDL coordination remain open. Live TPC-C evidence
  precedes the collaborator-only stats revert; do not relabel it as a final-binary run.
- [x] Rebuild and measure merged 6d26dab06d against the immutable control:
  fourteen fixed-work trials, 84,000 measured transactions, equal SQL hashes.
- [x] Profile current Rust, control and Go separately. Current Rust has 1,044
  statistics-load workers among 1,164 native threads; control has none.
- [x] Move statistics-load worker ownership from session catalog creation to
  the domain-shaped factory, and replace mutex-held timed receives with Go's
  channel-select shutdown/task lifecycle. Retained catalog/statistics tests pass.
- [x] Reproduce the commit-history ownership cycle, then store data-only catalog
  snapshots and reattach live owners when serving stale reads. All 52 scoped
  catalog, statistics and transaction tests pass; Ready lint passes.
- [x] Measure the combined worker/history lifecycle fix against immutable current
  and faster-control binaries: 20 trials, 120,000 measured transactions and matching
  SQL hashes. At 32 clients, throughput +6.87%, SQL CPU -12.74%, p95 -6.94% versus
  pre-fix. One/eight-client throughput remains effectively flat; control still wins.
- [x] Profile after repeated connection churn: six named statistics workers,
  126 total native threads. No per-session worker growth or receiver-mutex polling.
- [x] Stop this increment's owned services: verify 19 PIDs exited and 16 ports
  closed. No compared server required forced termination; preserve fixture data.
- [x] Read pinned Go batch send/receive and cop-worker ownership.
- [x] Preserve native result replies, notification and absolute-deadline parking
  from the earlier increment.
- [x] Push checkpoint 431d637dec to hparser-integration and verify remote SHA.
- [x] Co-locate the existing command owner and its connection tasks on one
  transport-owned event loop; remove per-connection runtime allocation.
- [x] Run one consolidated release validation: 2,056 passed, zero failed,
  16 ignored; server build passed with twelve jobs.
- [x] Match seven live SQL comparisons and before/after workload hashes.
- [x] Complete 40 fixed-work trials: 240,000 measured plus 40,000 warmup
  transactions, zero SQL errors/reconnects.
- [x] Capture native and CPU profiles separately from timing. Verify the named
  send-to-driver kernel-wake path disappears from the sampled candidate chains.
- [x] Compare actual Go/Rust range plans and region-task counts. Both need four
  tasks; do not bypass the required workers.
- [x] Stop five supervisors; verify eight owned PIDs gone and sixteen ports
  closed. Preserve fixture data and update the current receipt.
- [x] Rerun the 2,056-test release scope, server build, formatting and Ready lint
  for the user-requested checkpoint. Confirm all 32 measured source/config files
  and the measured server SHA256 match current bytes.
- [ ] Attribute remaining four-region request latency before the next connected
  optimization: collection, I/O, cop-worker/SQL rendezvous and join.
- [ ] Beat the faster control on throughput/latency and validate TPC-C/mixed writes.
- [ ] Complete required whole-package/platform validation before parity claims.
- [x] Compile the merged executor/session/server and all their test targets.
- [x] Remove 56 identified Rust-only tests and obsolete helper scaffolding.
- [x] Run retained Go memory-cleanup, sort-spill and session-variable tests:
  three passed, zero failed. Ready lint passed.
- [ ] Complete the repository-wide Go-test provenance audit.
- [x] Reconcile the upstream merge through b77c90cde6 with no unresolved entries.
- [x] Build release server/smoke binaries and run the 81-case live SQL harness.
- [x] Identify and remove PD's Go-incompatible reference-count shutdown gate.
- [x] Verify retained-handle/in-flight cancellation and live shutdown.
- [x] Repaired SQL assertions expose 11 real row divergences, all returning
  unfiltered rows when physical predicates are not fully described remotely.
- [x] Keep residual conditions authoritative across chunk/row reads,
  projection, TopN/Limit, partial aggregation and scan reopening; rerun live SQL.
- [x] Live result equality: 78 row, two error and one projection assertion pass,
  plus five related LIMIT/TopN/COUNT/SUM checks. Missing receipts now fail loudly.
- [ ] Repair receipt validation: removed diagnostic output and an early return
  concealed both missing receipts and unperformed SQL equality assertions.
- [x] Pass 78 scoped socket/executor/planner tests, workspace all-target compilation,
  formatting, shell syntax and Ready lint.


## Context and Orientation

rust/crates/tidb-txnkv/src/rpc/execution.rs::TransportIo owns one native thread
running a Tokio current-thread event loop. transport_runtime.rs spawns the
existing command task on that loop and supplies the same Handle to all configured
connection slots. Connection count still controls sockets. ConnectionTasks keeps
each physical channel generation's independent task scope; shutdown closes those
scopes before stopping and joining TransportIo, including panic reporting.

Cop workers continue on the shared multi-thread execution_runtime. No SQL or
blocking recovery is moved onto the transport event loop. Batch policy,
identities, retries and deadlines are unchanged. The earlier transport-only increment changed two
production files. Current statistics/history ownership changes are described below.

rust/crates/tidb-distsql/src/cop_paging/cop_iterator.rs owns independent workers,
ordered two-response buffers, unordered producer/consumer rendezvous and join.
rust/crates/tidb-exec/src/cop_scan.rs decodes responses on the SQL consumer.
These are real Go responsibilities, not optional synchronization to remove.

The current statistics increment is owned by executor catalog/sync_load.rs and
server ClusterSessionFactory. The factory shares one worker pool across catalogs;
commit history stores CatalogSnapshot data without back-references to its ring.


## Decision Log

Decision (2026-09-10, cross-session statistics visibility): make
executor driver/catalog/statistics.rs a shared conversion view over the domain's
canonical SharedStats, not another publication owner. A catalog caches only the
schema tuples used by conversion, invalidating them with metadata changes.
Every statistics lookup resolves the current raw table; converted values are
reused only when both source identity and schema tuples match. Weak source
identity prevents address reuse without retaining obsolete raw payloads.
Initial sessions, rebuilt catalogs and transaction snapshots retain this view.
Statistics publication no longer rebuilds schema catalogs. The Go-derived peer
test verifies both eviction and completed-load visibility without schema refresh.
Native ANALYZE returns its result to the cluster publisher directly; reading its
unpublished output through the live planner view would read the prior generation.

Decision (2026-09-10, statistics-loading lifetime): retain an immutable pair of
the cluster storage loader and domain-owned worker pool on ClusterServerSession.
ClusterStatisticsLoading::attach is the only path that installs those resources
into both the initial and rebuilt catalogs. Do not put schema-version state in
the loader or make refresh depend on query shape. This follows Go's separation
of Domain.StatsHandle from InfoSchema and avoids a worker-to-loader reference
cycle: load tasks own the loader, not the connection's pair of resources.

Decision (2026-09-10, column-statistics validity): keep eviction metadata with
the owning TableStatistics and expose one validated column lookup. Match Go's
nonzero-row and essential-load/NDV conditions, including valid all-NULL headers.
The reduced-column public estimator independently rejects zero-row payloads.
Do not replace a column range node with an index estimate: Go Selectivity builds
those nodes separately. Retain the distinct index fallback used for expression
evaluation, where Go findAvailableStatsForCol actually calls for it.

Decision (2026-09-10, prepared range costing): InitStats borrows StmtContext
instead of only SessionTimeZone. Use FromScope::for_statement once per filtered
data source for handle ranges, index ranges and selectivity, matching Go's
stats.go ranger-context ownership. No query-specific plan hint, forced index,
extra batching limit or cached-marker fallback is introduced. Removing the
duplicate scope also removes a duplicate catalog lookup and column-name copy.

Decision (2026-09-10, global configuration ownership): use a shared Arc<Config>
published under the existing short-lived RwLock. GetGlobalConfig clones only the
handle; UpdateGlobal and file loading clone the config before mutation. Store and
restore accept published snapshots without copying their contents. Consumers must
borrow substructures or copy only fields they actually own, not reintroduce full
tree clones. Do not change SQL variable precedence or add a stale per-session
configuration cache. Existing config atomic-boolean semantics and eager latest
index-schema construction were separate source gaps; the live prepared-plan
probe now ties index metadata ownership to a concrete EXPLAIN failure below.

Decision (2026-09-10, index metadata ownership): Catalog already centralizes
metadata-version changes, including local temporary-table attachment/detachment.
Retain one lazily built Arc<LatestIndexSchema> in that catalog version and clear
it at the central metadata mutation boundary. Clones and historical snapshots
retain their own version's immutable value. Statement contexts always have their
catalog's domain view; planner predicates still decide when to consult it.
This removes AST cloning and special attachment rules instead of adding an
EXPLAIN-only exception. Overlay local temporary indexes through copy-on-write.
Replace the obsolete Rust-only test asserting absent standalone domain metadata
with real Go locking-EXPLAIN behavior in the existing SQL fixture.

Decision (2026-09-10, process publication): mirror Go's per-session process
state. The shared registry is only a connection directory, not the lock covering
SQL normalization and every statement update. A retained command owns its entry
directly; publication during that command reuses its digest but never appends
transaction history. Session execution records history independently, including
repeated identical executions within a retained command. Do not cache by workload text or
discard process-list/transaction observability to reduce CPU.

Decision (2026-09-10, cold access paths): follow Go IndexStatsIsInvalid's
missing-or-zero-payload rule, not a stricter load-status gate. Common-handle
columns can cover a secondary index subject to Go's length/collation rules.
Refresh source estimates at the existing statistics-wait rule, before join
reorder; reuse that initializer for static partitions and invalidate old costs.
Do not force indexes or change workload SQL to obtain the desired plans.

Decision (2026-09-10, physical locking): restore the missing Go planner and
executor connection, not a TPC-C query special case or a storage scan heuristic.
Go buildSelectLock runs before projection/aggregation; optimizer TopN pushdown
determines the locked rows. Existing session statement replay already owns
fresh for-update snapshots and lock conflicts. Partition identities, explicit
table targets and wait policy must be preserved, not silently guessed.

Decision (2026-09-10, statement-context profile): remove eager materialization,
not add a cache or SQL-shape detection. SequenceSnapshot pins the catalog's
copy-on-write schema map and resolves only the requested table name. The
snapshot stays independent of catalog/service/history owners. Go's provider
uses its infoschema table lookup; a standalone sequence-map plus all-object
HashSet adds a full schema walk to every SELECT and DML even without sequences.
Carry the actual sequence TableInfo.ID and key session LASTVAL by ID, so a
DROP/CREATE naturally selects a different identity without name-specific cleanup.
Decision (2026-09-10, initial merged-code profile): fix statistics worker ownership before
batch-timer experiments. ClusterSessionFactory.open_storage_session creates a
fresh statistics service/pool per catalog, including internal pooled sessions.
Go Domain.StartLoadStatsSubWorkers starts one pool for its StatsHandle. The
Rust workers also serialize recv_timeout(10ms) behind a shared receiver mutex;
Go selects tasks or domain shutdown without idle polling. Make the worker pool
an explicit shared domain-owned argument and use native multi-consumer channels.
Preserve per-request cache publication, urgent/expired priority, queue limits,
singleflight, retry and worker join semantics.
Decision (2026-09-10, catalog lifecycle): committed snapshots held the same
Arc<CommitHistory> that contained them. This retained session catalogs and their
statistics services indefinitely. Store only committed data in the ring; attach
statistics, analyze-options and history owners to returned live views, matching
Go's distinction between snapshot infoschema and domain services. Do not clear
history at session shutdown or add reference-count gates.

Decision (2026-09-10, user request): remove tests absent from the Go suites,
including Rust-only source-text protection checks and retired implementation
scaffolding. Keep tests ported from Go even when their Rust names differ.

Decision (2026-09-10, user approval): retire the unused crate-root physical
builder/readers and join builder, preserving the active driver builder. Use one
JoinOutput for serial and parallel output/defaults, one concurrency value, and
checked child access at the merged ownership boundaries. Compile after the
coherent integration batch; do not treat compilation as semantic acceptance.

Decision (2026-09-10): move publication to its I/O owner, not a different batch
policy. Pinned client-go conn_batch.go:193-339 collects requests, chooses a
connection and sends from the batch owner. Rust's prior separate runtimes added
a kernel wake at packet publication. Co-location removes that boundary while
retaining asynchronous connection tasks and their distinct lifetimes.

Decision (2026-09-10): retain the measured CPU reduction as WIP, not throughput
success. Against immediate prior, SQL CPU is -8.07 percent, throughput -0.39
percent and p95 unchanged. Do not promote a slower candidate over the faster
control or claim the remaining latency is solved.

Decision (2026-09-10): preserve source-required worker behavior. Go creates an
unbuffered unordered result channel. Rust removes terminal active attempts at
response acceptance, so terminal state does not itself cause false fallback.
Live Go EXPLAIN ANALYZE and Rust traces both show four cop tasks for the same
bounded range. Region boundaries explain those tasks; they are not redundant.


## Surprises & Discoveries

The final column-validity load probe narrows the next failure to catalog
replacement, not estimator arithmetic or predicate collection. It records 92
unique requests with service=false and 22 with service=true. Customer's ordered
query has the correct column/index demand but no service to execute it. Initial
session construction in cluster_session_node/mod.rs installs the loader;
rebuild_catalog_now assigns a fresh catalog and loses that attachment. Go's
domain/domain.go StatsHandle accessor reads a domain-owned handle. A fix must
preserve that lifetime through both statistics and schema refresh; forcing an
index or preloading the benchmark's customer table would hide the cause.

The post-lock profile exposes substantial Sort/TableScan CPU in Rust TPC-C.
Go uses a covering idx_customer range for customer-name lookup and an ordered
idx_order lookup for order status. On a fresh Rust server the customer table
prefix estimates one row despite containing 3,000. A later query after histogram
loading estimates 3,000 correctly. Fixing coverage and empty payloads alone
leaves order status on the wrong cold plan: InitStats had copied its access costs
before the load completed. Evidence: plans-repeat.log and root-live.log under
/private/tmp/tidb-post-lock-profile.VGAM1f.

The current statement-context probe is in
/private/tmp/tidb-execution-attribution.blT99K. It reuses the immutable final/control
CPU traces, attributes copy/allocation/hash/synchronization stacks to their nearest
TiDB owner, and demangles Rust symbols with the pinned toolchain. The final trace
contains 2.648s of sampled Running weight in statement_context_ignoring, including
1.877s in Catalog.object_names; these are overlapping inclusive weights, not
latency percentages. Go sequenceOperatorProp/infoschema.GetSequenceByName does one
table lookup. Go TestSequenceFunction also reuses a session after dropping and
recreating a sequence: sequence-red.log proves Rust leaked the old LASTVAL by name.

The eef7d1b0a6-based context increment completes 20 read-only trials:
120,000 measured plus 20,000 warmup transactions, zero errors/reconnects.
At 1/8/32 clients, throughput improves 6.135/2.300/1.433 percent, fixed-work SQL
CPU falls 9.178/6.375/3.992 percent, and p95 falls 5.350/3.475/0.894 percent.
The faster Rust control still leads by 7.115/14.281/8.203 percent throughput.
The parity hash covers selected point/range/aggregate queries plus whole-table
COUNT/SUM, not a row-by-row comparison of all 100,000 rows.
The separate candidate trace has 28,168 Running samples and no object_names
enumeration chain. Measurements, source patch, binary identities and logs are
under /private/tmp/tidb-execution-attribution.blT99K.

TPC-C uses pinned go-tpc a9ca4818625deef91ff80f6c395a575ccae22b7c with the
existing input-seed patch and a new perf_context_tpcc_blt99k database.
Prepare passes all 12 checks. The driver's runtime default deliberately omits
3.3.2.11: delivery deletes new_order rows, so its prepare-only count is no longer
an invariant (the Go run demonstrates this too). The standard 11 checks pass
after the short probes and longer Go run, but condition 3.3.2.12 finds 377
inconsistent customers after the longer pre-change Rust run. No after-binary
TPC-C throughput comparison was completed. A customer with one delivered order
has two delivery credits; this is not decimal rounding. locking-probe.log
records the deterministic Go/Rust locking divergence. The physical builder has
no Lock arm; PlanBuilder::build_select_lock has only a unit-test caller, and
the old AST select_lock::wrap has no callers. Repair the physical path rather
than reviving that obsolete AST path.

Exact increment commands (Cargo from rust; lint from repository root):

    cargo test --offline --locked --release -j12 -p tidb-executor -p tidb-session -p tidb-server --lib --test all -- sequence --test-threads=12
    cargo build --offline --locked --release -j12 -p tidb-server --bin tidb-server
    GOMAXPROCS=12 GOFLAGS='-p=12' make -j12 lint
    ruby /private/tmp/tidb-execution-attribution.blT99K/measure.rb
    ruby /private/tmp/tidb-execution-attribution.blT99K/profile.rb
    ruby /private/tmp/tidb-execution-attribution.blT99K/tpcc-measure.rb
    ruby /private/tmp/tidb-execution-attribution.blT99K/locking-probe.rb

The last two commands intentionally expose non-green pre-existing behavior;
their exit/result fields must not be reported as successful acceptance.

Live sequence scope is narrower than the local-session tests. Go completes
all nine CREATE/DROP/CREATE/ALTER values; Rust passes the initial three values
from a Go-created cluster sequence. Rust-owned cluster sequence DDL returns
1105 (unsupported), and Go-owned DROP with the retained Rust connection times
out, so live DROP/CREATE identity refresh is not verified. Preserve that failure
in sequence-catalog.log rather than treating the local test as cluster parity.
All 16 owned server PIDs exited and ten ports closed; cleanup.json verifies
this. The failed TPC-C database and probe artifacts remain available for the
locking investigation. Current receipt: statement-sequence-lookup-baseline.json.

Before publication, fast-forwarded collaborator commit 785ea28a61 (PD bootstrap
readiness retries) without conflicts. All 45 PD-client tests, the combined
release server build and Ready lint pass. The eight measured Rust diffs remain
identical to the frozen source patch. The combined binary was not rebenchmarked;
the sysbench numbers describe the immutable pre-integration candidate.
Seven changed Rust files and the edited sequence-fixture helpers are formatted;
the fixture's untouched tail retains pre-existing rustfmt differences.

    cargo test --offline --locked --release -j12 -p tidb-pd-client --test all

Fresh post-merge evidence is in /private/tmp/tidb-current-throughput.6qK3A7.
At eight clients Rust averages 1,654 TPS versus the faster control's 1,963 TPS;
SQL CPU is 14.20 versus 8.62 seconds for equal 6,000-transaction trials.
The separate CPU trace attributes 11.472 seconds of sampled Running weight to
statistics-loader synchronization chains, out of 43.211 seconds total. This
is not a latency percentage. The native sample confirms 1,044 such workers,
created per session/internal catalog, whereas Go owns its pool per domain.
No current throughput acceptance is claimed; the older baseline stays intact.

The worker-only fresh-process ABBA run is in /private/tmp/tidb-stats-workers.WDt2ZF:
20 trials, 120,000 measured transactions, zero errors/reconnects and identical
full SQL hashes. A separate native sample shows six statistics workers, rather
than 1,044. Throughput changes are small and do not establish a performance win.
The original simultaneous-server profile is diagnostic, not comparable fixed-work
CPU evidence; observing thousands of native threads can itself be expensive.

A second fail-before test proves the catalog history cycle. After data-only
snapshots, the last stale view releases both history and statistics workers.
The original stale-read transaction fixtures still pass. Evidence:
catalog-cycle-red.log and catalog-cycle-green.log under the worker artifact root;
Ready lint and the final binary are tracked under
/private/tmp/tidb-stats-lifecycle.BPa0w4. The final binary SHA256 is
27c9b05fe9476821aea654a7e9578faa6c679667e9a4abd674a754b078902fa3.

The final matched-work run uses fresh compared servers in before/after/after/before
order, and brackets the run with faster-control and Go trials. All 120,000 measured
plus 20,000 warmup transactions finish without errors or reconnects; every full SQL
hash matches. At 1/8/32 clients, throughput changes versus pre-fix are
-0.69/-0.38/+6.87 percent; SQL CPU changes are -0.05/-1.61/-12.74 percent; p95 changes
are +0.78/0/-6.94 percent. The candidate still trails the faster control by
11.81/15.26/10.03 percent throughput. These are shared-host measurements with two
samples per server/concurrency, not general performance acceptance. TPC-C/mixed
writes and full Go-package parity remain open. The current receipt contains exact
commands, binary identities and aggregate measurements; raw traces remain separate.

Catalog/statistics/transaction validation used:

    cargo test --offline --locked --release -j12 -p tidb-executor -p tidb-session -p tidb-server --lib -- driver::catalog tests_core::transactions cluster_session_node::tests::statistics --test-threads=12
    cargo build --offline --locked --release -j12 -p tidb-server --bin tidb-server
    GOMAXPROCS=12 GOFLAGS='-p=12' make -j12 lint
    ruby /private/tmp/tidb-stats-lifecycle.BPa0w4/measure.rb
    ruby /private/tmp/tidb-stats-lifecycle.BPa0w4/profile.rb


Tokio 1.53.0 current_thread::Handle.schedule pushes same-runtime wakes into its
local task queue. External wakes enter the remote queue and unpark the driver.
The driver calls mio 1.2.2; macOS kqueue wake issues kevent. The prior source
crossed that boundary from publish_batch/send_group for every packet.

Two separately captured eight-client Time Profiler traces contain 28,727 prior
and 25,593 candidate Running samples. The predecessor's named
send_group -> wake_by_val -> I/O Handle.unpark -> kevent chain accounts for
193ms sampled CPU among the top-65 synchronization chains; it is absent from the
candidate list. Source independently confirms local publication. This does not
prove every remote wake or kevent is gone, nor quantify the entire latency gap.

Against the faster control at 1/8/32 clients, throughput is -2.63/-10.03/-6.11
percent, SQL CPU +20.06/+29.81/+15.95 percent, and p95 +0.99/+11.00/+7.45 percent.
All seven throughput ABBA blocks regress. ABBA runs control, candidate,
candidate, control to reduce simple ordering bias.

Against immediate prior at eight clients, three ABBA blocks give throughput
-0.39 percent, SQL CPU -8.07 percent and identical mean p95. Shared cop-worker
condition-variable waits/signals, native SQL/worker rendezvous and reply parking
remain prominent. Named profile categories are not complete critical-path
attribution. Native all-thread sample counts include waiting and are not CPU
percentages or interchangeable with Go pprof units.

The diagnostic range id BETWEEN 50000 AND 50099 has matching Go/Rust
TableReader -> Projection -> TableRangeScan plans. Go EXPLAIN ANALYZE reports
four tasks/four RPCs, and Rust traces four tasks. SHOW TABLE REGIONS confirms
four real regions intersect that range. No task-count shortcut is justified.


## Plan of Work and Milestones

Current configuration milestone: change tidb-config/src/config_tree/config.rs and
its consumers so read ownership matches Go's shared GetGlobalConfig pointer.
Keep config parsing/update and error-extension publication behavior. Run retained
tidb-config, tidb-errmsg and scoped session-variable fixtures, then workspace
all-target compilation with twelve jobs. Build the release server and compare
fixed-work sysbench/TPC-C and Running-state profiles against frozen 6ef4aea7aa;
keep shared-host limitations explicit and stop all owned services afterward.

Completed process milestone: change tidb-session/src/process.rs so the registry
contains independently synchronized entries, and process/result guards retain
their own entry. Snapshot the directory before reading entries; release entry
locks before invoking KILL targets. Consolidate statement finish/release cleanup.
Normalize only once for an already-held statement with the same text; append
history at execution start, not result retention. Every execution must append
again, even when an outer command retains the same process entry. Extend the
existing tests_system_schemas transaction-history case, then run retained
process-list, KILL, transaction, prepared and result-lifecycle tests as a batch.
Use the immutable 5d48 binary for before/after live comparisons and reprofile
ProcessRegistry/normalization, with builds separated from measurements.

Current correctness increment: follow pinned PD client afa43111d149
client.go::Close -> inner_client.go::close (cancel, wait, close services), and
client-go e4905600583b tikv/kv.go::Close (stop background tasks, region cache,
TiKV transport, then PD). Remove only PD's reference-count prerequisite, not
cancellation, worker joining or actual error reporting. Existing retained-handle
and in-flight fixtures must fail before the production change and pass after.
Make missing receipts fail the differential, and compare SQL independently.

The corrected live run exposes a second production root cause. TableScanExec
trusts a backend's receipt for a subset of the original conditions, and its
Open removes retained filters. Use one complete-description decision for remote
post-Selection operations; keep the executable filter across Open/Close.
Go PhysicalSelection.ToPB encodes the complete Conditions list, never treating
an empty encoded subset as proof of the whole Selection. Existing index lookup
paths already combine their receipt with fully_described; table scans must too.

The transport co-location milestone is implemented and measured; preserve its
evidence before subsequent edits. Do not revisit the removed publication
boundary as though it still existed.

The next milestone measures elapsed stages along the real four-region query:
batch collection, socket completion, cop worker, SQL response consumption and
worker join. Use existing request timestamps where possible and reconcile each
stage with pinned Go. The asynchronous batch collector still owns a native timer;
Go reuses and stops its timer, while Rust currently discards its Delay when idle.
That is a source difference to measure, not an established cause or permission
to relax deadlines.

Implement one connected, source-backed change, run the affected release
validation once, rebuild and repeat matched work. Do not tune arbitrary batch
sizes/thread counts, bypass independent workers or reconstruct the Go runtime.


## Concrete Steps and Validation

From /Users/qiliu/projects/tidb/rust, both commands exited zero:

    cargo test --offline --locked --release -j12 --no-fail-fast -p tidb-distsql -p tidb-txnkv -p tidb-unistore -p tidb-exec --lib --tests
    cargo build --offline --locked --release -j12 -p tidb-server --bin tidb-server

From repository root:

    rustfmt --check --edition 2021 --config skip_children=true rust/crates/tidb-txnkv/src/rpc/execution.rs rust/crates/tidb-txnkv/src/rpc/transport_runtime.rs
    GOMAXPROCS=12 GOFLAGS='-p=12' make -j12 lint
    git diff --check -- rust
    ruby /private/tmp/tidb-transport-owner.gHQJhr/verify-cleanup.rb

Validation reused existing transport, cancellation, deadline, paging and
shutdown suites. No Go/Bazel changes require bazel_prepare. Ready checks passed
again for the requested commit and push, with fresh logs in
/private/tmp/tidb-push-checkpoint.rAQYpO. This remains a WIP performance increment,
not release readiness. Full workspace tests, seven known executor
failures, live transaction difftests, TPC-C/mixed-write performance and complete
platform/package semantics remain unverified.

Live measurement commands:

    ruby /private/tmp/tidb-transport-owner.gHQJhr/candidate/read_parity.rb
    ruby /private/tmp/tidb-transport-owner.gHQJhr/prior-comparison/measure.rb
    ruby /private/tmp/tidb-transport-owner.gHQJhr/candidate/measure.rb

Each trial uses 6,000 measured plus 1,000 warmup transactions. The immediate-prior
comparison has twelve trials at eight clients; the faster-control comparison
has 28 at one/eight/32 clients. Build and profiling never overlap timing.
The desktop is shared, not an isolated performance host. Live Go differs from
checkout; selected SQL equality does not establish full source parity.


## Outcomes & Retrospective

Statistics loading now survives catalog refresh through retained loading
resources and one attachment path. The existing Go-derived regression fails
before and passes afterward; 16 selected tests, all-target compilation and
Ready lint pass. The live request probe no longer loses its service and actual
customer executions include ordered lookups. Sampled TPC-C sort time falls but
is not eliminated. Eight-client fixed-work timings overlap (after 6.28/6.51s,
before 6.43/6.41s; Go 6.05/6.23s), with background CPU and auto-analyze present.
The performance goal remains open. All owned services are stopped. The next
source-backed gap is publication to concurrent catalog waiters, not request
collection or lifetime; see statistics-loading-lifetime-validation.json.

Column validity now follows Go's loaded-distribution gate, including the valid
evicted all-NULL case. Three obsolete Rust-only tests and their misplaced index
fallback are removed; Go fixtures remain. Focused checks pass, but six existing
session failures and the workload optimization goal remain open. Paired timing
does not support a speedup claim. The live request probe locates the next root
cause: catalog replacement discards its statistics-loading service. Receipt:
benchmarks/column-statistics-validity-validation.json. All owned services are
stopped and the benchmark fixture is retained.

The process-publication increment removes repeated normalization under the global
registry lock and duplicate execution-history entries, following Go's separation
between SetProcessInfo and LazyTxn.onStmtStart. The final live query records its
SELECT digest once, matching Go, while repeated real executions remain separate.
The cluster-front BEGIN-history gap remains open; this is not full transaction
history or package parity. The extended existing fixture fails before and passes
after; the separate range-quota failure is unchanged on the original baseline.

Final after/before/before/after fixed-work trials show +2.34% sysbench throughput,
-6.05% SQL CPU and -3.54% p95 at 32 clients; one client is effectively flat. These
are shared-host diagnostics, not an accepted performance baseline: mediaanalysisd
uses 65.5-87.2% CPU at round boundaries, only two samples exist per variant/client,
and the TPC-C fixture grows. Separate Running-state profiles attribute 1.041s to
the original statement-start path and 0.511s to final process publication; these
windowed inclusive weights are not normalized performance comparisons. Final
artifacts and verified cleanup are under /private/tmp/tidb-process-publication.JIYx20.
The prepared customer-detail Sort investigation and broader optimization goal
remain open. See benchmarks/process-publication-validation.json for exact evidence.

The index-path increment restores Go's customer covering read and ordered
order-status lookup on the first query, not just after statistics have warmed.
Both fresh servers estimate the customer index at Go's 8.85 rows. Existing
prefix/key/join/aggregation tests show no new failures against unmodified upstream;
five existing failures remain listed in the receipt. Diagnostic TPC-C eight-client
trials take 6.18/6.69 seconds versus the prior binary's 11.12 seconds for 6,000
transactions. This is not an accepted baseline change: the host is shared, the
fixture grows, and the candidate also contains collaborator changes. Full workload
performance and package parity remain open. Exact commands and hashes are in
benchmarks/index-path-statistics-validation.json.

Post-merge root-fix evidence is under /private/tmp/tidb-counter-window.jbkXVN.
PD close tests fail twice before the production change (SharedOwners) and pass
after: 70 passed, one ignored across the PD library and aggregate test target.
The actual smoke path closes cleanly. Removing a reference-count gate is not
removing worker cancellation/join/error checks; those remain tested.

The corrected live harness initially found 11 wrong-result cases: omitted
conditions made the table-scan fast path return all 2,000 rows. A single
complete-description decision now protects remote post-Selection operations;
Open no longer erases the executable filter. The subsequent run has 78 row,
two error and one projection equality assertion passing. Five additional SQL
comparisons against the same Go node also pass:

    SELECT id FROM t WHERE sbig + 1 > 999 ORDER BY id LIMIT 1
    SELECT id FROM t WHERE abs(sbig) ORDER BY id DESC LIMIT 3
    SELECT COUNT(*) FROM t WHERE sbig + 1 > 999
    SELECT SUM(sbig) FROM t WHERE NOT mod(sbig, 100)
    SELECT id FROM t WHERE mod(sbig, 2.5) ORDER BY id LIMIT 3

The live harness still exits 1 for 78 unavailable coprocessor receipts, not SQL
differences. The removed smoke diagnostics must be replaced with current
runtime statistics before claiming pushdown/batching acceptance. This is not
performance evidence. Index partial-aggregation receipt completeness also needs
follow-up source/live verification; the table-only fixture does not prove it.

Commands for this increment (Cargo and script from the isolated rust directory):

    cargo test --offline --locked --release -j12 -p tidb-pd-client --lib --test all -- close_cancels
    cargo test --offline --locked --release -j12 -p tidb-pd-client --lib --test all --no-fail-fast
    cargo test --offline --locked --release -j12 -p tidb-executor -p tidb-exec --lib --test all --no-fail-fast -- kv_table::table_scan predicate_pushdown cop_scan_
    KEEP_LOGS=1 SCAN_PUSHDOWN_PORT_OFFSET=43820 CARGO_BUILD_JOBS=12 bash scripts/run-realtikv-scan-pushdown.sh
    GOMAXPROCS=12 GOFLAGS='-p=12' make -j12 lint

The script builds release server/smoke with twelve jobs. Ready lint passed;
formatting, bash syntax and git diff checks pass. The final scan/predicate/cop
suite passed 42 tests with zero failures (112 passes including PD). One obsolete
Rust-only refusal-message protection test and unused helpers were removed;
the existing residual SQL fixture now covers NOT, projection and TopN/Limit.
Logs are pd-close-before.log, pd-close-after.log, pd-close-live.log,
scan-filter-after-live.log, scan-filter-scoped-verified.log and scan-filter-lint.log. Both isolated playgrounds
are stopped and their PD/Go/Rust ports 46199/47820/47920 are verified closed.
No Go or Bazel source changed, so no new bazel_prepare run is required.

Changed production files: tidb-pd-client/src/client/mod.rs and src/error.rs,
tidb-executor/src/kv_table/table_scan.rs, and lifecycle comments in
tidb-exec/src/real_tikv_read.rs and tidb-server/src/bin/cluster-session-smoke.rs.
Changed fixtures: tidb-pd-client/tests/pd_worker_lifecycle_source.rs,
tidb-exec/tests/cop_scan_narrowed_output_source.rs and
cop_scan_partial_predicate_limit_source.rs. The differential script, this living
plan and baseline validation metadata carry the corrected evidence. Historical
performance measurements are unchanged. Full runtime/package/platform parity
and throughput remain unverified; the earlier broad-suite failures remain open.


Candidate SHA256 e61d432de6c71681b9868d7df9a0e81786c56551093742ed95f1793c5c130c8c
reduces fixed-work SQL CPU relative to immediate prior 76f6f08fa without improving
throughput or p95. It still trails faster control 84c39b216. Co-location removes
a verified cost, but not the full bottleneck. The full objective remains open.


## Idempotence and Recovery

Current evidence is /private/tmp/tidb-transport-owner.gHQJhr. before-source.tar.gz
preserves pre-edit RPC source and the preceding baseline; candidate-source.tar.gz
holds the measured source/configuration. Frozen binaries, raw trial logs,
SQL hashes, native samples and CPU traces remain local. The current summary is
rust/benchmarks/cop-progress-baseline.json.

All five owned supervisors are stopped. Preserve data at
/private/tmp/tidb-live-workload.nIrZBe; archive its logs/manifest before reuse.
Never restore source archives over unrelated work. Publish only to PingCAP's
origin/hparser-integration with a normal push, preserving unrelated local work
and concurrent remote commits. The measured base commit is 431d637dec; verify
the published merge commit on the remote before reporting delivery.

The authorized merge is being assembled in /private/tmp/tidb-upstream-merge.nzY42N,
preserving the original checkout and its uncommitted files. Local merge
cf5e822e99 combines 59700b3180 and cf6990b012. The newer remote history through
b77c90cde6 is also integrated; there are no unresolved index entries.

The active executor owns physical construction and ResolveIndices. The unused
crate-root builder/readers and duplicate join state are retired. Scan pruning
preserves stored column identities; projection offsets resolve against the
actual child schema. Preserved-side outer-join predicates remain join conditions.
Prepared scope resolvers forward the execution parameter snapshot. Window
execution includes ranking, value/relative functions and RANGE comparison columns.
TiKV select-response errors retain typed code, SQLSTATE and message through
storage, executor, recordset, text/binary writer and cursor materialization.
No string marker or substring-based error-code parsing remains.

Client identity, authentication, privilege attachment and connection kill
handling remain in public open_session. Both boot paths configure the factory
before sharing it; the binding writer borrows a weak factory reference.
The cleanup removes 56 identified Rust-only tests and obsolete scaffolding,
not every test lacking a completed provenance audit. Retained shared fixtures
must still compile against the current public interfaces.

Merged validation is separate from earlier measured performance evidence.
Bazel preparation passed with twelve jobs after matching the WORKSPACE Go pin
to go.mod 1.25.12. Earlier foundation runs passed 1,188 expression, 1,192 planner
and 641 transaction tests (99, 1,071 and 10 ignored respectively). Retained Go
memory-cleanup, sort-spill and session-variable tests passed. The latest broad
runtime run (merge-latest-runtime.log) reports executor 1,278 passed / 13 failed
and session 1,522 passed / 157 failed / 210 ignored. These failures include
plan/receipt expectations, unsupported SQL shapes and result/metadata differences;
they have not all been established as pre-existing and are not waived or deleted.
The merged code is not complete Go parity or a fully green release.

Release server and smoke builds passed. The real TiKV scan differential ran
81 cases, including exact COT(0) code 1690, SQLSTATE 22003 and message. A subsequent
source audit found that missing receipts returned before SQL equality assertions;
the earlier claim of 81 proven comparisons was incorrect. Two independent issues
exist: PD shutdown rejects retained handles, and the smoke binary no longer emits
the receipt lines parsed by the script. Neither full SQL equality nor pushdown
acceptance is established by that earlier run.
Playground cleanup completed and ports 46199/47820/47920 are closed.
Evidence: merge-live-scan-pushdown.log under /private/tmp/tidb-counter-window.jbkXVN.

Scoped Ready verification passed: 71 client/executor/recordset tests, seven
Go-derived planner fixtures, workspace compilation with all targets, formatting,
shell syntax and lint. Logs are merge-final-verified.log,
merge-planner-fixtures-final.log, merge-workspace-ready-2.log and
merge-final-lint-2.log. The final live rerun is merge-live-scan-final.log;
it compares full Go error text and explicitly reports incomplete receipts.
The earlier 31 socket failures were PermissionDenied at bind and all passed
with authorized localhost access.

The final workspace check also reconciled explicit callback completion types,
native expression-backed sort items and reader explain arguments in retained
fixtures. Four tests of the retired Rust-only ProjectionInlineExpr model were
removed; real-plan join-reorder tests remain. Production merge da0bf5ab86
preserves both local history and the shared upstream head b77c90cde6.
Publication target is origin/hparser-integration, normal push only.

Exact final commands (Cargo commands from the isolated worktree's rust directory;
lint and formatting from its root):

    cargo check --offline --locked --release -j12 --workspace --all-targets
    cargo test --offline --locked --release -j12 -p tidb-executor -p tidb-exec -p tidb-server --no-fail-fast --lib --tests -- coprocessor_cot_error_matches_go select_response_errno_survives_recordset_mapping resultset_writer_source mysql_client_lifecycle_source initial_database_tcp_source concurrent_mysql_sessions_source mysql_native_auth_lifecycle_source prepared_typed_markers tests_executor_part19_source shared_physical
    cargo test --offline --locked --release -j12 -p difftest-planner-tests --test all -- physical_sort physical_table_reader
    KEEP_LOGS=1 SCAN_PUSHDOWN_PORT_OFFSET=43820 CARGO_BUILD_JOBS=12 bash scripts/run-realtikv-scan-pushdown.sh
    GOMAXPROCS=12 GOFLAGS='-p=12' make -j12 lint
    git diff --name-only --diff-filter=ACM -- '*.rs' | xargs rustfmt --check --edition 2021 --config skip_children=true
    bash -n rust/scripts/run-realtikv-scan-pushdown.sh rust/scripts/run-realtikv-lock-recovery.sh
    git diff --check

Remaining semantic work includes statement-context-aware cached-plan rebuilding,
partitioned/nested selected-row locking, unsupported aggregate/window/subquery shapes,
and the broad failures listed above. CachedSelectPlan::bind and CachedDmlPlan::bind
still use parameter-only rebuild context, whereas Go plan_cache.go::adjustCachedPlan
uses the current session's ranger context. One current statement context must own
cache rebuilding and execution. No complete package or performance acceptance
is claimed by this merge.

Counter diagnostics at /private/tmp/tidb-counter-window.jbkXVN/probe.json show
that TiKV metrics continue publishing buffered work after a benchmark ends.
Settled eight-client windows show roughly 10 gets and 400 coprocessor keys per
transaction for both Go and Rust. Immediate snapshots cannot support a claim
that Rust reads more rows. Rust returned about 3.8% more coprocessor bytes in
these diagnostic windows; attribution remains open. These shared-host trials
are not accepted performance evidence. All probe services are stopped.


## Interfaces and Dependencies

The measured co-location increment changed internal scheduling ownership, not
protocol rules. The subsequent full upstream merge changes public Rust
interfaces and dependencies; its API reconciliation and validation are tracked
separately above. TransportIo remains the native transport scheduling owner.

Updated 2026-09-10 after the co-location measurement: replaced the earlier
per-connection experiment's active status with current source, evidence,
rejected hypotheses and next latency-attribution work.

Updated 2026-09-10 during the authorized full upstream merge: corrected the
publication remote, recorded source-backed resolutions and counter visibility,
and separated prior-binary evidence from pending merged-code validation.
