# `pkg/util/stmtsummary` — Go-master parity audit

Comparison source: Go `origin/master` at commit
`febee17ec716d86b1e355e5400ef9e4f4f190bad` (2026-09-02).

This receipt records the current Go-master delta implemented by the native
`tidb-stmtsummary` owner. It does not claim that the whole Go package is
transcreated: the v2 reader/logger/table-test artifacts and the executor,
infoschema, and planner integration remain an explicit dependency boundary.

## Complete Go package inventory

The package has exactly 22 tracked artifacts and 11,214 Go lines. Every
production file, test, benchmark, nested table-test harness, and Bazel target
was read and enumerated before editing. There is no `doc.go`, fixture tree,
generated source, or platform-specific Go variant.

| Artifact group | Files | Lines | Inventory |
| --- | ---: | ---: | --- |
| v1 production | `evicted.go`, `reader.go`, `statement_summary.go` | 2,665 | eviction rollups, column factories, summary map/statistics, RU and network summaries |
| v1 tests/harness | `evicted_test.go`, `main_test.go`, `statement_summary_test.go` | 2,842 | eviction, row/column, concurrency, metrics, history, and test bootstrap |
| v1 build | `BUILD.bazel` | 66 | library/test targets and dependencies |
| v2 production | `v2/column.go`, `v2/logger.go`, `v2/reader.go`, `v2/record.go`, `v2/stmtsummary.go` | 3,366 | persistent records, columns, rotation, logging, and history reader |
| v2 tests/harness | `v2/column_test.go`, `v2/main_test.go`, `v2/reader_test.go`, `v2/record_test.go`, `v2/stmtsummary_benchmark_test.go`, `v2/stmtsummary_test.go` | 1,398 | column/record/reader/rotation tests and three benchmarks |
| v2 table harness | `v2/tests/main_test.go`, `v2/tests/table_test.go` | 778 | information-schema table integration tests |
| v2 build | `v2/BUILD.bazel`, `v2/tests/BUILD.bazel` | 99 | production, test, and table-test targets |

The Go source contains 240 non-test declarations, 76 test/benchmark
declarations (including three benchmarks). Rust ownership is split across `src/statement_summary.rs`,
`src/evicted.rs`, `src/reader.rs`, and the v2 `column.rs`, `record.rs`, and
`stmtsummary.rs` modules. Rust has no v2 `reader.rs`, `logger.rs`, or
`v2/tests` equivalent yet.

## Go-master delta and parity decisions

Three Go-master commits changed this owner since the Rust extraction point:

* `8bab3c26d7` prevents an evicted “other” row from leaking into a later
  current interval. The Rust reader already carries the same begin-time guard.
* `655769534b` snapshots evicted-count fields under the evicted mutex and uses
  `Peek` plus record locking during internal-query cleanup. Rust's evicted
  aggregate is always accessed through its owning mutex, and the v2 window and
  record are separate mutexes with the same lock order; no additional adapter
  path was needed.
* `381ac705f9` adds `IAExecCountStr = "IA_REMOTE_EXEC_COUNT"` to both
  reader/column surfaces and tracks executions with at least one IA remote-read
  segment. IA counts are incremented only when
  `GetIARemoteReadSegmentStats(...).Count > 0`; segment count/bytes/wait-time
  statistics continue to sum every execution. The value is included in v1
  eviction rollups, v2 record merges, and v2 persisted JSON as
  `ia_remote_exec_count`.

The same Go-master source also replaces the old v1 plan-error nil path with
`PlanDiscardedEncoded` and initializes a newly-created summary's
`isInternal` from its first statement before applying logical AND on later
  statements. Rust now mirrors those two behaviors as well as IA tracking in
  v1 `StmtSummaryStats` and v2 `StmtRecord`, registers the new column factory,
  preserves Go column order, and emits the JSON field. Focused source-derived
  regressions cover one IA execution plus one ordinary execution, current and
history rows, chunk round-trips, eviction aggregation, v2 merges and JSON,
plan-encoding failure fallback, and internal-only cleanup.

The 2026-09-02 Go package batch restores all of the above root-package
behavior in `pkg/util/stmtsummary` from the current Go master source,
including the eviction lock snapshot, stale-interval filter, IA
execution-count column, internal-query LRU cleanup, plan-error fallback, and
associated BUILD shard metadata. No Rust owner source was changed in this
batch; the v2 and executor/infoschema/planner boundaries remain explicit
below.

The same package boundary includes the 2026-09-02 `pkg/util/stmtsummary/v2`
batch. Its complete direct inventory (five production files, six test/
benchmark files, and one BUILD target) was re-read before editing. Go-master
behavior now records IA execution counts in v2 records and JSON, exposes the
column factory and history/memory readers, snapshots the evicted window begin
time, and serializes internal-query cleanup under the record mutex. The
nested `v2/tests` table package was audited but unchanged; Rust's missing v2
reader/logger/table ownership and SQL integration remain explicit boundaries.

## Latest Go-master follow-up (`78cac443a4f46c13bfe27eb247b5c80657952547`)

The fetched Go `origin/master` is now `78cac443a4f46c13bfe27eb247b5c80657952547`.
This package-scoped batch applies that commit's 15-file delta (569 insertions,
135 deletions) as one unit across the already inventoried 22 artifacts. v1
history collection now returns the newest retained intervals in chronological
order, average KV/PD/backoff/write-response columns divide by execution count,
table-name serialization skips empty table entries, and history reset keeps
the newest element. The v2 record uses the same table-name filtering and
normalized SQL formatting. The v2 history reader now enumerates paths before
opening files, preserves a rotating current file by inode, bounds open file
descriptors through worker-side close, and handles metadata lookup failures;
the v2 table harness covers open-ended time ranges. Focused regressions were
added for each of these source behaviors, including the pre-fix evicted-history
failure reproduced locally.

The v2 table test could not compile in this shared workspace because an
unrelated in-progress `pkg/statistics/handle/util` edit references the absent
`vardef.TiDBAnalyzeStoreBatchSize` symbol. That edit is outside this package
and was preserved. The Rust owner and executor/infoschema/planner integration
remain explicit boundaries; no speculative Rust changes were made.

## Rust owner follow-up: current-master history and execution averages

The complete 22-artifact Go inventory above remains the package boundary for
this Rust-only follow-up. The relevant current-master source is
`78cac443a4f46c13bfe27eb247b5c80657952547` (`planner, util: fix statement
summary history and display correctness`), already recorded in the preceding
Go-master follow-up. Before editing, the Rust owner was read across all seven
production modules (`statement_summary.rs`, `evicted.rs`, `reader.rs`, and
`v2/{column,record,stmtsummary}.rs`) and its existing unit fixtures/tests.

The old Rust owner still diverged in four source-visible paths:

* v1 and evicted history collection took the oldest `historySize` entries,
  while Go takes the newest entries and restores chronological order;
* `clear_history` retained the front entry instead of the current back entry;
* v1 and v2 KV/PD/backoff/write-response averages divided by `commitCount`
  instead of `execCount`;
* v1/v2 table-name builders inserted delimiters using the original index, so
  skipped empty table entries left a trailing comma, and v2 records retained
  unformatted normalized SQL.

Focused Rust regressions were added for latest-interval ordering and reset,
evicted history selection, all four execution-average columns in both
surfaces, empty-table filtering, and v2 normalized-SQL formatting. Each
pre-fix probe failed with the old result (oldest intervals, commit-based
averages, or `db2.tb2,`); the corrected probes pass.

Validation for this Rust follow-up:

* `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-stmtsummary --lib` — complete owner suite passes after the fix.
* `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-stmtsummary --all-targets` — passes.
* pinned workspace formatting, `make lint`, and `git diff --check` — pass.

No Go source was edited. The v2 logger/history-reader/table harness and the
executor/infoschema/planner integration remain explicit boundaries as stated
above; this batch only aligns behavior represented by the native Rust owner.

## Rust v1 return-contract follow-up (2026-09-07)

This Rust-only follow-up reuses the complete 22-artifact, 11,214-line Go
package inventory above. The atomic implementation unit is the direct v1 Go
package: three production files (`evicted.go`, `reader.go`, and
`statement_summary.go`), three test/harness files, and `BUILD.bazel`. Every v1
production function, test, build target, corresponding Rust module and test,
caller, manifest, and workspace/lock entry was read before editing. The direct
package has no fixture, generated source, or platform variant. The nested
`pkg/util/stmtsummary/v2` directory remains a separate Go package and was not
edited in this batch.

The v1 Rust owner imposed `#[must_use]` on 39 constructors, accessors,
formatters, averages, collections, and scalar/struct returns whose direct Go
counterparts may be discarded. Those annotations were removed without
changing runtime logic. Eight annotations remain on native Rust `Option`
boundaries: the reader column factory; statement-stat creation; optional
summary construction, lookup, eviction, and normalized-SQL access; backoff
formatting; and empty-byte conversion.

One `#[deny(unused_must_use)]` regression was added to each v1 owner module.
Temporarily restoring the 39 annotations made the focused suite fail to compile
with exactly 39 `unused return value` diagnostics. With the correction in
place, all three focused tests pass, the complete crate owner suite passes 67
tests, and all-target compilation succeeds. No statement aggregation,
eviction, locking, SQL formatting, averaging, row materialization, storage, or
concurrency behavior changed. No Go, v2, Bazel, Cargo, generated, fixture, or
platform-specific artifact changed, so neither Go execution nor
`make bazel_prepare` applies.

The living implementation plan is
`rust/docs/operations/util-stmtsummary-v1-return-contract-audit-execplan.md`.

Return-contract follow-up evidence:

- Pre-fix focused command, after temporarily restoring the 39 annotations:
  `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-stmtsummary --lib go_v1_ -- --nocapture --test-threads=1`
  — failed as expected with exactly 39 `unused return value` diagnostics.
- The same focused command with the correction in place — three tests passed.
- Complete owner command:
  `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-stmtsummary --lib --no-fail-fast --test-threads=1`
  — 67 tests passed.
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-stmtsummary --all-targets`
  with the same OpenSSL environment — passed; existing warnings belong to the
  separately owned v2 surface.
- Scoped `rustfmt +nightly-2026-08-22 --edition 2021 --check` over the three
  edited v1 source files, `make lint` with the pinned Go environment, and
  `git diff --check` — passed.

## Rust v2 corrective alignment follow-up (2026-09-07)

This Rust-only follow-up reuses the complete 22-artifact, 11,214-line Go
inventory above without reopening Go source, per the user's explicit scope.
The direct `pkg/util/stmtsummary/v2` implementation unit is five production
files, six test/benchmark files, and `v2/BUILD.bazel`; it has no `doc.go`,
fixture, generated source, or platform variant. The nested `v2/tests` directory
is a separate Go package and remains outside this batch.

Before editing, all five Rust v2 modules were read line by line: `column.rs`
(821 lines), `mod.rs` (49), `reader.rs` (2,174), `record.rs` (1,709), and
`stmtsummary.rs` (2,122), for 6,875 lines, 255 functions or methods, 27 inline
tests, and 38 explicit `#[must_use]` annotations. The crate root and manifest,
workspace and lock registration, `tidb-session` proxy callers and tests,
`tidb-workloadrepo` dependency edge, every repository caller, and all build
surfaces were also read. There is no owner `build.rs`, fixture, generated
source, platform variant, or custom build output.

The audit found four independently testable Rust-only gaps:

- thirty-six ordinary Go-shaped constructors, accessors, formatters,
  collections, and scalar/struct returns imposed Rust-only discard
  diagnostics; those annotations are removed, while the native
  `column_factory` and `global_stmt_summary` `Option` boundaries retain theirs;
- `reader::send_with_cancel` recursively retried every full channel slot and
  could exhaust a worker stack; it now carries the returned value in one loop
  frame while preserving the 20 ms cancellation poll;
- normal rotating-file rename and pruning printed unconditional `DEBUG` lines
  to process stderr; those writes are removed without changing best-effort
  file error handling;
- public `HistoryReader::new` accepted a private `CancelToken`; the token and
  its construction/state/cancel operations are now public and covered from an
  external integration test.

Six focused inline regressions cover the four return-owner modules, bounded
stack behavior, and silent rotation. One integration regression covers the
public cancellation API. Before the fixes, the return suite failed with
exactly 36 diagnostics, the retry child aborted with a stack overflow, the
writer child captured both debug lines, and the integration test failed with
`E0603`. After the fixes, all seven regressions pass, the complete owner gate
passes 74 tests, and both `tidb-stmtsummary` and its production caller
`tidb-session` compile across all targets. Owner warning cleanup and the stale
crate-level claim that `v2/reader.go` was absent were corrected in the same
package batch.

No Go, Bazel, Cargo manifest, lockfile, generated, fixture, or platform
artifact changed, so Go execution and `make bazel_prepare` do not apply. This
does not make v2 package-complete: most of `logger.go` and the separate
`v2/tests` Go package remain explicit unported boundaries. The living plan is
`rust/docs/operations/util-stmtsummary-v2-alignment-execplan.md`.

Corrective follow-up evidence:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline
  --locked -p tidb-stmtsummary --lib go_v2_alignment_ -- --nocapture
  --test-threads=1` with the standard OpenSSL environment — six tests passed.
- The same Cargo command selecting `--test v2_public_api` — one test passed.
- `cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml
  --offline --locked -p tidb-stmtsummary --no-fail-fast --test-threads=1` — 74
  tests passed.
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline
  --locked -p tidb-stmtsummary --all-targets` — passed with no owner-path
  diagnostic.
- The same all-target check for `-p tidb-session` — passed; its existing
  warnings are outside this package batch.
- Scoped `rustfmt +nightly-2026-08-22 --edition 2021 --check` over all six
  edited/added Rust files, `git diff --check`, and Ready-profile `make lint`
  with the pinned Go environment — passed.
- After the single package commit rebased cleanly over nine incoming
  `origin/hparser-integration` commits, all seven focused regressions, the
  74-test owner suite, both all-target checks, scoped formatting, diff hygiene,
  and Ready-profile `make lint` passed again.

## Validation (Ready profile)

- Failpoint-enabled Go root targeted run:
  `./tools/check/failpoint-go-test.sh pkg/util/stmtsummary -run 'Test(ToDatumIAColumns|ToDatumIAColumnsChunkRoundTrip|AddStatementPlanEncodeError|ToEvictedCountDatumConcurrent|CurrentRowsExcludePreviousIntervalEvictedOther|DisablingInternalQueryPreservesLRUOrder)$' -count=1`
  — passed.
- Failpoint-enabled Go root full package run:
  `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh ./pkg/util/stmtsummary -count=1 -vet=off`
  — passed (0.553s).
- Failpoint-enabled Go v2 targeted run:
  `./tools/check/failpoint-go-test.sh pkg/util/stmtsummary/v2 -run 'Test(IAAvgColumns|IAAvgColumnsChunkRoundTrip|HistoryReader|StmtRecord|StmtWindow)' -count=1`
  — passed.
- Pre-fix v2 regression run (before restoring the Go fields/column): the
  failpoint runner failed to compile with the expected missing
  `IAExecCountStr` and `StmtRecord.IAExecCount` symbols.
- Post-fix failpoint-enabled v2 focused run:
  `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh ./pkg/util/stmtsummary/v2 -run '^(TestIAAvgColumns|TestIAAvgColumnsChunkRoundTrip|TestMemReader|TestHistoryReader|TestStmtRecord|TestStmtWindow|TestEvictedConcurrentWithRotate)$' -count=1 -vet=off`
  — passed (0.856s).
- Post-fix failpoint-enabled v2 full package run:
  `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh ./pkg/util/stmtsummary/v2 -count=1 -vet=off`
  — passed (0.806s).
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-stmtsummary --lib -- --test-threads=1` — 46 passed, including the two new v1 regressions.
- `cargo +nightly-2026-08-22 check --offline --locked -p tidb-stmtsummary --all-targets` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest ./pkg/executor -run '^TestAdminShowSlowIARemoteReadStats$' -count=1` — passed against Go master.
- `rustup run nightly-2026-08-22 rustfmt --edition 2021 --check` over all five edited Rust owner files — passed.
- `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- The required `make bazel_prepare` was attempted for the BUILD shard and new
  top-level tests, but is blocked locally because no `bazel` executable is
  installed (`make: bazel: No such file or directory`).

The broad `pkg/util/stmtsummary/v2/tests` table suite was started with the
failpoint runner but did not complete within the local run window; no table
fixture was changed in this batch.

OpenSSL-dependent Rust commands use the bundled Poppler root as `OPENSSL_DIR`
and its `lib` directory in `DYLD_LIBRARY_PATH`.

Latest `78cac443a4` follow-up evidence:

- Pre-fix failpoint-aware run:
  `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh ./pkg/util/stmtsummary -run '^TestEvictedHistoryCollectionKeepsLatestIntervals$' -count=1 -vet=off`
  — failed as expected (`actual: 1`, `expected: 2`).
- Root focused run for latest regressions with the same failpoint wrapper and
  environment, `-run '^(TestEvictedHistoryCollectionKeepsLatestIntervals|TestExecutionAverageColumnsUseExecCount|TestTableNamesSkipEmptyTables)$'`
  — passed.
- Root full package run with the same wrapper and environment — passed
  (0.479s).
- v2 focused run with the same wrapper and environment,
  `-run '^(TestStmtFiles|TestHistoryReader|TestStmtRecordTableNamesSkipEmptyTables|TestStmtRecordFormatsDigestText)$'`
  — passed.
- v2 full package run with the same wrapper and environment — passed
  (0.811s).
- Focused `pkg/util/stmtsummary/v2/tests`
  `TestStmtSummaryHistoryOpenEndedTimeRange` — blocked at compile time by the
  unrelated in-progress statistics edit's undefined
  `vardef.TiDBAnalyzeStoreBatchSize` reference.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed; no Rust source changed in this follow-up.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make bazel_prepare`
  — required for the test/BUILD changes and blocked because the local `bazel`
  executable is unavailable.

## Risks and unverified surfaces

- Correctness risk is concentrated in the IA count predicate and the stable
  persisted JSON field name/type, plus the first-statement internal flag and
  plan-error fallback; all four are covered by source-derived tests.
- Compatibility risk remains at the integration boundary: Rust does not yet
  implement Go's v2 history reader/logger/table tests or the executor
  `SHOW SLOW` and infoschema/planner changes from the same Go commit.
- Performance impact is one conditional increment per summary update and one
  additional persisted integer; no new allocation or hot-path scan is added.
- The Go executor regression passes with the required `-tags=intest`; the
  broader executor, infoschema, and planner integration remains outside this
  crate's current validation surface.
