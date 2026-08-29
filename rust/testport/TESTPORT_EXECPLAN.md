# Go-parity audit for the Rust SQL path

This ExecPlan is a living document per `PLANS.md`. Keep `Progress`,
`Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective`
current while the audit proceeds.

## Purpose / Big Picture

Make the Rust SQL implementation behaviorally match TiDB at pinned Go commit
`e2788410d8d696605e8cb002585877a063ccc909`. The comparison source is the Go
code itself. Rust-only execution policy, cache-specific pipelines, documentary
gap tests, and receipts that imply parity without executable behavior are not
part of the target. A completed transcreation claim is package-atomic as
required by root `AGENTS.md`; partial work remains implementation progress and
must not be presented as a completed Go-package port.

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
  its complete package receipt; all 17 source tests and three source-derived
  regressions pass with downstream encrypted-spill and expression consumers.
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
- [x] Complete the pinned root `pkg/util/sem` package in its `tidb-util`
      owner, verify its full policy and cross-crate sysvar wiring, retain its
      five source tests, and remove supplementary Rust-only assertions.
- [ ] Audit the next bounded package cluster by reading pinned Go first, then
      fill executable gaps and remove false carriers.
- [ ] Run Ready validation and self-review only when the requested parity scope
      is genuinely complete enough for a final-status claim.

## Decision Log

- Decision: the sole behavioral reference is pinned Go commit
  `e2788410d8d696605e8cb002585877a063ccc909`, not current `origin/master` and
  not Rust parity comments. Date/Author: 2026-08-28, Codex.
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
- The full planner test target currently encounters unrelated pre-existing
  compile errors in CTE/TopN and memory-trace test sources; scoped planner tests
  for the changed MPP property behavior pass.

## Outcomes & Retrospective

Work remains in progress. Current validated behavior includes ANALYZE prefix
indexes, MPP equivalence comparison, retained runnable b103 DDL final-state
tests, lexer tests, funcdep graph tests, and the complete pinned kvcache test
surface. The final outcome must list exact files and commands, remaining
unverified packages, and correctness, compatibility, and performance risks
without claiming repository-wide parity.
