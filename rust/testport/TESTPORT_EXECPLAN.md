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
No Go source or Bazel metadata is expected to change.

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

- 2026-09-01: audited the complete Go `pkg/util/dbterror/exeerrors` package at
  `origin/master` `db35d47066648fe73abce6318d53fc625df51490` against the Rust
  owner on `origin/hparser-integration`. The package has exactly `errors.go`
  and `BUILD.bazel`, no functions or tests, and an 82-prototype public error
  catalog. Go master's three dual-password additions were already present in
  the Rust owner and its complete 82-row Go-generated fixture, so no duplicate
  production or test behavior was added. Inventory and Ready evidence are in
  `receipts/util_dbterror_exeerrors.md`.

- 2026-09-01: audited the complete Go `pkg/util/plancodec` package at
  `origin/master` `db35d47066648fe73abce6318d53fc625df51490` against the Rust
  owner on `origin/hparser-integration`. The seven-artifact package has no
  generated, platform, fixture, or benchmark variant. Go master appends the
  stable `Analyze` plan type at physical ID 64; Rust previously returned zero
  for the name and `UnknownPlanID64` for the ID. Added the table entry and a
  focused two-way regression, with the complete file/function/test inventory
  and validation gates in `receipts/util_plancodec.md`.

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

- 2026-09-01: completed the unclaimed pinned Go `pkg/util/cgroup` package.
  Read and mapped all nine production, test, platform, and Bazel artifacts;
  corrected raw controller-count and mount-separator parsing, preserved the
  pinned hybrid memory-usage fallback, and changed CPU quota conversion to
  retain Go's signed `-1` unsupported sentinel. Moved host-memory/process-RSS
  helpers out of the cgroup owner into `tidb-util::memory::process`, removed
  Rust-only scheduler recommendation wrappers, and added complete source
  memory/CPU fixture matrices plus public unsupported-platform checks. The
  inventory, integration boundary, and host validation limits are recorded in
  `receipts/util_cgroup.md` and
  `docs/operations/cgroup-audit-execplan.md`; Ready validation and push remain
  the final step of this batch.

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
  Inventory and WIP gates are in
  `receipts/statistics_handle_usage_indexusage.md`.
- 2026-08-29: completed the pinned Go
  `pkg/statistics/handle/usage/collector` package in the distinct
  `tidb-stats-handle-usage-collector` owner. Moved its three source tests out
  of the aggregate statistics crate, removed a source-absent capacity test,
  preserved repeated worker starts and the source channel behavior after
  close, and rewired index usage to consume the package directly. Inventory
  and WIP gates are in `receipts/statistics_handle_usage_collector.md`.
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
- 2026-08-29: re-audited the complete pinned Go `pkg/util/sli` package and
  every production integration point used by its sole external source test.
  Restored Go's signed duration/native-int representation and failpoint,
  attached one accumulator to each ordinary session, added exact final
  mutation bytes/keys and remote processed-key details to the cluster and
  real-TiKV executor paths, invalidated INSERT/REPLACE SELECT, and finalized
  the accumulator from the common text/prepared command path. Ported
  `TestTxnWriteThroughputSLI` as one source-derived cluster executor test.
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
- 2026-08-29: re-audited the complete pinned Go `pkg/util/ppcpuusage`
  package, which has no test artifact. Removed its remaining Rust-only
  signed-overflow regression and `must_use` diagnostic; the production owner
  and statement-summary consumers retain Go's signed wrapping durations.
- 2026-08-29: re-audited the complete pinned Go `pkg/util/texttree` package.
  Removed Rust-only `must_use` diagnostics, its arbitrary-byte supplemental
  regression, and the corresponding temporary-probe narrative; exactly the
  two Go test identities and both ordinary consumers remain.
- 2026-08-29: re-audited the complete pinned Go `pkg/util/arena` package.
  Removed the remaining Rust-only reset-reuse regression and restored exactly
  the two Go test identities; shared backing and reset reuse remain production
  behavior rather than an extra test surface.
- 2026-08-29: re-audited the complete pinned Go `pkg/util/bitmap` package.
  Removed its remaining Rust-only signed-boundary regression and restored the
  exact three Go test identities while retaining the source-derived signed
  length behavior in production.
- 2026-08-29: re-audited the complete pinned Go `pkg/util/encrypt` package
  against its existing Rust owner. Removed three Rust-only regression cases
  and the extra alias assertion from the source-named suite; the 17 Go test
  identities and source benchmark remain the complete package surface.
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
- 2026-08-29: completed the pinned Go `pkg/util/resourcegrouptag` package
  (one production file, one source test, one test harness, and `BUILD.bazel`).
  Split its decoder, label classifier, and first-key extraction out of the
  mixed Rust `pkg/kv` builder module; the builder and decode hook remain with
  their actual Go owner. Reduced the package suite to the three source test
  identities and gave it an independent Cargo target matching Go's package
  test artifact. Complete inventory and WIP gates are recorded in
  `receipts/util_resourcegrouptag.md`.
- 2026-08-29: completed the pinned Go `pkg/util/deadlockhistory` package
  (one production file, one source test, one test harness, and `BUILD.bazel`).
  Removed the Rust-only package row renderer, key decoder, server policy, and
  recording entry point; executor code now owns retryable admission and live
  recording while the ordinary information-schema reader owns key decoding
  and statement-summary digest-text lookup, matching Go's boundaries. Reduced
  the package tests to the four source identities and deleted an unregistered
  duplicate DEADLOCKS test file. Complete inventory and WIP gates are recorded
  in `receipts/util_deadlockhistory.md`.
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
- 2026-08-29: audited all five pinned Go `pkg/util/generic` artifacts. Restored
  signed capacity, nullable signed comparators, constructor panic order, and
  wrapping sort semantics; removed `is_empty`, four supplemental tests, their
  semantic manifest, and a stale audit plan. The stats TopN consumer uses the
  corrected owner. Complete inventory and WIP gates are recorded in
  `receipts/util_generic.md`.
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
- 2026-08-29: audited every production and build artifact in pinned Go
  `pkg/util/domainutil` plus its startup publication. Removed the second
  independent Rust repair registry and all package-only Rust tests, retained
  one `tidb-domain` owner, restored Go hash-map and simple-lowercase behavior,
  and wired the effective startup repair config into that single global.
  Complete inventory, integration boundaries, and WIP gates are recorded in
  `receipts/util_domainutil.md`.
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
- 2026-08-29: audited every production and build-tag artifact in pinned Go
  `pkg/util/israce`. The production mapping already matched both source build
  variants. Removed the two Rust-only unit tests, retired semantic-gate
  manifest, and stale standalone audit plan; the ordinary printer remains the
  source-shaped consumer. Complete inventory and WIP gates are recorded in
  `receipts/util_israce.md`.
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
- 2026-08-28: audited every production, test, test-harness, and build file in
  pinned Go `pkg/util/slice`. The owner already preserves all three production
  functions and their nil/empty, ordering, formatting, and short-circuit
  contracts. Removed the duplicate `b029` test module, receipt, and manifest
  claim while retaining the complete owner test surface.
- 2026-08-28: audited every production, test, test-harness, and build file in
  pinned Go `pkg/util/disjointset`. Removed Rust-only public `len`/`is_empty`
  conveniences and deep `Clone` implementations whose semantics are absent
  from (and differ from copying) the Go slice/map-backed structs. Repinned the
  complete package evidence; all Go operations and downstream chunk usage
  remain intact.
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
- [x] Re-audit the complete pinned `pkg/util/kvcache` package, remove an
      invented `Peek` gap absent from Go, remove the duplicate semantic test
      carrier and Rust-only public cache APIs, repin its semantic receipt, and
      make its translated tests platform-neutral; all 8 Go tests pass.
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
- [x] Complete the pinned `pkg/util/queue` package in its `tidb-util` owner and
      remove the unused executor duplicate whose `Clear` eagerly dropped
      backing values and whose public head/tail accessors existed only for its
      duplicate external tests.
- [x] Complete the pinned `pkg/util/sli` package in its `tidb-util` owner,
      replace the executor-local observation simulator with direct Go-shaped
      Prometheus reporting, and remove synthetic failpoint, fixture, and
      inspection APIs that had no production consumer.
- [x] Complete the pinned `pkg/util/set` package in its `tidb-util` owner:
      restore all five concrete memory-aware constructors and tracker rules,
      retain exactly seven source tests and three benchmarks, remove public
      generic wrapper and ordered-tree policy, restore the free keyed-set API
      and current-key clone/order behavior, pre-size memory-aware constructors,
      and wire HashAgg to Go's concrete string set.
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
      atomic inventory and WIP gates are in `receipts/util_tiflash.md`.
- [x] Complete the pinned `pkg/util/disk` package in `tidb-util`: restore the
      exact temp-directory lifecycle and both tracker constructors, route its
      real server/chunk/memory-alarm consumers through them, and remove the
      Rust-only spill-policy surface from the package. The five-artifact
      inventory and WIP gates are in `receipts/util_disk.md`.
- [x] Complete the pinned root `pkg/util/sem` package in its `tidb-util`
      owner, verify its full policy and cross-crate sysvar wiring, retain its
      five source tests, and remove supplementary Rust-only assertions.
- [x] Complete the pinned root `pkg/util/traceevent` package across
      `tidb-util`, the vendored `tikv-client`, and server initialization:
      replace the disconnected fake client registry with live hooks, restore
      ordinary startup registration, preserve structured fields and context,
      remove Rust-only public/test surfaces, and port both source benchmarks.
      The atomic inventory and WIP gates are in `receipts/util_traceevent.md`.
- [x] Complete the pinned root `pkg/util/tracing` package in `tidb-util`:
      preserve shared span-handle semantics, restore its open string phase and
      pointer-preserving CE deduplication, add the four source benchmarks and
      empty `OptimizeTracer`, and remove supplementary Rust-only APIs/tests.
      The atomic inventory and WIP gates are in `receipts/util_tracing.md`.
- [x] Complete the pinned `pkg/session/syssession` package in the
      `tidb-syssession` owner: replace the executor-local policy fragments
      with the full owner/operation/pool lifecycle, remove ignored empty
      carriers, and migrate timer storage off its local session/pool
      imitation. The atomic inventory and WIP gates are in
      `receipts/session_syssession.md`.
- [x] Complete the pinned `pkg/util/sqlexec/mock` support package in a
      distinct `tidb-sqlexec-mock` owner: preserve the context-key identity
      and the generated restricted-executor mock's full three-method
      contract without introducing a second SQL interface. The atomic
      inventory and WIP gates are in `receipts/util_sqlexec_mock.md`.
- [x] Complete the pinned `pkg/statistics/handle/util` package in a distinct
      `tidb-stats-handle-util` owner: remove five partial policy modules,
      implement every production artifact over shared model/executor/session
      contracts, and replace its four ignored source tests with executable
      behavior. The atomic inventory and WIP gates are in
      `receipts/statistics_handle_util.md`.
- [x] Complete the pinned `pkg/statistics/handle/util/test` support package in
      `tidb-stats-handle-util-test`: match a typed request context, preserve
      the wrong-type panic and exact description, remove the string-only
      predicate and its two supplemental tests, and make ordinary `StatsCtx`
      carry the matching request source. The atomic inventory and WIP gates
      are in `receipts/statistics_handle_util_test.md`.
- [x] Complete the pinned `pkg/statistics/handle/logutil` package in
      `tidb-stats-handle-logutil`: preserve all four category-tagged logger
      constructors and both source sampling policies over the shared logutil
      owner, without adding tests to the source-test-free package. The atomic
      inventory and WIP gates are in `receipts/statistics_handle_logutil.md`.
- [x] Complete the pinned `pkg/statistics/handle/internal` support package in
      `tidb-stats-handle-internal`: replace the opaque snapshot workaround
      with `AssertTableEqual` over actual statistics tables, and remove its
      three non-Go tests. The atomic inventory and WIP gates are in
      `receipts/statistics_handle_internal.md`.
- [x] Complete the pinned `pkg/statistics/handle/usage/collector` package in
      `tidb-stats-handle-usage-collector`: preserve both bounded channels,
      synchronous timeout escalation, worker priority/drain/close behavior,
      and all three source tests while removing the supplemental capacity
      assertion. The atomic inventory and WIP gates are in
      `receipts/statistics_handle_usage_collector.md`.
- [x] Complete the pinned `pkg/statistics/handle/usage/indexusage` package in
      `tidb-stats-handle-usage-indexusage`: own the real model-driven GC,
      samples, global/session/statement collectors, four source tests, and
      parallel benchmark; remove narrowed and duplicate aggregate surfaces.
      The atomic inventory and WIP gates are in
      `receipts/statistics_handle_usage_indexusage.md`.
- [x] Audit the pinned parent `pkg/statistics/handle/usage` package as one
      atomic unit and remove its disconnected key, pending-ID,
      transaction-mode, SQL-string, empty-test, and function-batch carriers.
      The package remains explicitly unclaimed until the ordinary stats
      handle, session, schema, transaction, persistence, and integration paths
      exist. The complete inventory is in
      `receipts/statistics_handle_usage_audit.md`.
- [x] Complete the pinned `pkg/statistics/handle/cache/internal` interface
      package in `tidb-stats-handle-cache-internal`: bind it to shared actual
      statistics tables, preserve all eleven pointer-receiver methods,
      and remove the generic value, extra method, and source-absent mock tests.
      The atomic inventory and WIP gates are in
      `receipts/statistics_handle_cache_internal.md`.
- [x] Complete the pinned `pkg/statistics/handle/cache/internal/mapcache`
      package in `tidb-stats-handle-cache-internal-mapcache`: derive cost from
      actual statistics tables, retain shared pointers across independent map
      copies, implement the complete cache contract, and remove the generic
      caller-cost surface and source-absent tests. The atomic inventory and WIP
      gates are in `receipts/statistics_handle_cache_internal_mapcache.md`.
- [x] Audit the pinned `pkg/statistics/handle/cache/internal/lfu` package as
      one five-artifact unit and remove its table-free key-set, shard,
      caller-memory, source-absent test, and stale function-batch carriers.
      The package remains explicitly unclaimed until complete pinned
      Ristretto behavior is available. The inventory is in
      `receipts/statistics_handle_cache_internal_lfu_audit.md`.
- [x] Complete the pinned `pkg/statistics/handle/cache/internal/testutil`
      support package in `tidb-stats-handle-cache-internal-testutil`: construct
      actual statistics tables, optional production payloads, full-load
      status, native memory accounting, and both append helpers; remove the
      shape-only carrier and source-absent tests. The atomic inventory and WIP
      gates are in `receipts/statistics_handle_cache_internal_testutil.md`.
- [x] Audit the complete pinned `pkg/statistics/handle/cache/metrics` package.
      Remove the label-only Rust carrier and its two source-absent tests: the
      Go package exposes eight initialized Prometheus handles, not label
      metadata, and completing it requires the absent atomic `pkg/metrics`
      owner. The inventory is in
      `receipts/statistics_handle_cache_metrics_audit.md`.
- [x] Audit the complete pinned root `pkg/statistics/handle/cache` package.
      Remove three private-helper carriers and eight supplemental tests: Go's
      package behavior is the integrated atomic cache, SQL row cache, metrics,
      LFU/map selection, publication, update, and benchmark surface. The root
      stays unclaimed while its LFU and metric dependencies are absent. The
      full inventory is in `receipts/statistics_handle_cache_audit.md`.
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
      six public scalar/SQL carriers, their 19 tests, and 28 ignored empty test
      functions. Go owns one transactional session/storage/cache read-writer
      package with 11 artifacts and 28 integrated tests; its handle and type
      dependencies remain incomplete. Inventory is in
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
- [ ] Audit the next bounded package cluster by reading the requested Go
      `origin/master` first, then fill executable gaps and remove false
      carriers.
- [ ] Run Ready validation and self-review only when the requested parity scope
      is genuinely complete enough for a final-status claim.

## Decision Log

- Decision: Go master's three dual-password executor errors require no Rust
  edit because the requested hparser branch already owns and fixture-checks
  them. Preserve the complete generated fixture as source evidence and avoid a
  second partial test that cannot fail independently of the all-entry guard.
  Date/Author: 2026-09-01, Codex.
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
- Decision: `pkg/statistics/handle/autoanalyze/exec` is defined by current
  session execution and its process, metrics, warning, cache, and interruption
  effects. Private ratio/window parsing cannot stand alone as the package, and
  Go has no public parser API or parser-only tests. Rust's detached helpers and
  aliases are removed until the full execution owner can land.
  Date/Author: 2026-08-29, Codex.

## Surprises & Discoveries

- Rust retained raw ANALYZE samples but skipped prefix-index statistics in the
  cluster path; Go cuts raw index values before histogram construction.
- Rust had functional-dependency machinery but the needed equivalence closure
  was private, which led to a false planner-property gap outside the owner.
- Several testport batches consisted entirely of ignored empty functions or
  comments. They increased apparent coverage without testing Go behavior.
- The pinned `pkg/util/kvcache` package has no `Peek` method or Peek assertion;
  the Rust gap was derived from a different source state. Its translated tests
  also assumed Linux `/proc/meminfo`, unlike the platform-neutral Go tests.
- Go's nil receiver/interface and concrete builtin-signature identity tests
  describe implementation shapes Rust cannot invoke after adopting non-null
  references and name-keyed dispatch. Empty Rust functions for those shapes
  test nothing and must not count as parity.
- The current workspace still contains many `go-parity-gap` markers. Their
  presence is an audit queue, not evidence that every carrier should survive.
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

## Outcomes & Retrospective

Work remains in progress. Current validated behavior includes ANALYZE prefix
indexes, MPP equivalence comparison, retained runnable b103 DDL final-state
tests, lexer tests, funcdep graph tests, and the complete pinned kvcache test
surface. The 2026-09-01 rolling Go-master plancodec batch also restores
Analyze physical ID 64 and passes its Go/Rust owner and consumer gates. The
following `pkg/util/dbterror/exeerrors` audit certifies the already-aligned
82-entry catalog without changing execution behavior. The final outcome must
list exact files and commands, remaining unverified packages, and correctness,
compatibility, and performance risks without claiming repository-wide parity.
