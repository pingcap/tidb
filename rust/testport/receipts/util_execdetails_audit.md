# `pkg/util/execdetails` — Go-master parity boundary receipt

Status: audited, but unclaimed as a package-complete transcreation. The
package is a cross-cutting execution-details owner whose current Rust pieces
are explicitly `SEED`s; the missing context, client-go, protobuf, Prometheus,
zap, and ordinary executor integration cannot be repaired safely as a
partial leaf. No production or test file was changed in this audit.

Go source: `origin/master`
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.

Rust comparison branch: `origin/hparser-integration`
`9319f10e99679b997063c7f0699787cc2cbe7b5f` at the time of this refresh.

## Complete Go inventory

The package has exactly eight direct artifacts, all read in full from the
Go-master tree:

- `BUILD.bazel` — 50 lines; one `go_library` over the five production files
  and one flaky/race-enabled `go_test` over `execdetails_test.go` and
  `main_test.go`.
- `execdetails.go` — 699 lines; execution-detail value types, string/zap
  formatting, percentile summaries, synchronized merge/reset accessors, and
  cop-task summaries.
- `runtime_stats.go` — 1,458 lines; runtime-stat implementations,
  hash-state and RU evidence, root/cop row snapshots, analyze scan-byte
  estimation, summary coverage tracking, and the runtime-stat collection.
- `ruv2_metrics.go` — 1,095 lines; context-independent and context-bound RU
  v2 counters, executor metric recorder resolution, clone/merge, RU
  calculation, formatting, and ingestion from TiKV details/commit details.
- `tiflash_stats.go` — 918 lines; TiFlash scan/columnar/wait/network
  summaries, merge/clone/format helpers, and TiFlash RU-consumption decode.
- `util.go` — 313 lines; execution-detail and RUV2 context plumbing,
  TiKV-detail snapshot loading, generic percentiles, and duration formatting.
- `execdetails_test.go` — 1,354 lines; 31 helper/test functions covering
  strings, cop/runtime summaries, RUV2 calculation and bypass paths,
  TiFlash/vector/columnar summaries, commit/root stats, duration formatting,
  RU-version clone/merge behavior, and IA remote-read extraction.
- `main_test.go` — 32 lines; `TestMain` common-test setup and goleak
  configuration only.

There is no `doc.go`, README, fixture or `testdata` directory, benchmark,
fuzz test, generated input/output, platform-specific file, or build-tag
variant in the Go package. The checkout source is the older branch copy; the
Go-master delta is 33 additions/deletions in `execdetails.go`, 156 test lines,
and 408 additions/deletions in `runtime_stats.go` (the current package source
itself was not edited here).

Against the current Go-master authority, the complete package remains eight
artifacts, 5,919 lines, 333 declarations, and 30 top-level test/benchmark/fuzz
entries. The current branch is missing the same three-file delta: 32 added and
one removed line in `execdetails.go`, 152 added and four removed lines in
`execdetails_test.go`, and 396 added and 12 removed lines in
`runtime_stats.go`. These additions cover read-pool details, checked cop/root
row snapshots and summary coverage, analyze scan-byte estimates, hash-state
state transitions, and Explain-RU output; they are not dependency-closed
leaves.

### Production function inventory

`execdetails.go`: `P90Summary.Reset`, `P90Summary.Merge`,
`StmtExecDetails.ensureRUV2Metrics`, `getRUV2Metrics`, `setRUV2Metrics`,
`GetIARemoteReadSegmentStats`, `ExecDetails.String`, `ExecDetails.ToZapFields`,
`SyncExecDetails.MergeExecDetails`, `MergeCopExecDetails`, `mergeScanDetail`,
`mergeTimeDetail`, `MergeReadPoolTaskDetails`, `mergeReadPoolTaskDetails`,
`MergeLockKeysExecDetails`, `MergeSharedLockKeysExecDetails`, `Reset`,
`GetExecDetails`, `CopTasksDetails`, `CopTasksSummary`,
`TaskTimeStats.String`, `TaskTimeStats.FormatFloatFields`, and
`CopTasksDetails.ToZapFields`.

`runtime_stats.go`: `HashStateRowsSnapshot.Complete`, `Invalid`,
`NewHashStateRuntimeStats`, `HashStateRuntimeStats.AddRows`, `Complete`,
`Invalidate`, `HashStateRowsSnapshot`, `String`, `Tp`, `Clone`, `Merge`,
`merge`; all `basicCopRuntimeStats` string/clone/merge/summary methods;
`StmtCopRuntimeStats.mergeExecSummary`; `CopRuntimeStats.GetActRows`,
`GetTasks`, `recordSummaryEvidence`, `String`; all `BasicRuntimeStats`
accessors/clone/merge; `NewRootRuntimeStats`, root accessors and merge/string;
basic record/open/close/row/time methods; `NewRuntimeStatsColl`,
`EstimateScanBytes`, analyze-scan-byte record/get methods, registration and
basic/stmt/root/coprocessor accessors, `RootRowsSnapshot` and `CopRowsSnapshot`
observers, expected/invalidate summary tracking, hash-state snapshot lookup,
cop recording, plan-ID decoding, and existence checks; concurrency-info
construction/set/clone/string/merge; commit-stat merge/clone/string and
format helpers; RU runtime-stat string/clone/merge/type methods; and
`ExplainRURuntimeStats` string/clone/merge/type methods.

`ruv2_metrics.go`: context lookup, raw-RUV2 and RUDetails/commit ingestion,
RUV2 construction/bypass/adders, executor-recorder resolution, all fixed and
label-counter snapshot/sum/clone helpers, metrics clone/merge/getters,
zero-test, RU calculation, and summary/total/detail formatting.

`tiflash_stats.go`: scan and columnar clone/string/merge/summary methods,
wait-summary clone/string/merge/ignore check, network update/clone/empty/
string/merge/inter-zone getter, and `MergeTiFlashRUConsumption`.

`util.go`: all context constructors/inheritance/synchronization and
`GetExecDetailsFromContext`/`LoadTiKVExecDetails`, plus `Int64`, `Duration`,
`DurationWithAddr`, every `Percentile` method, `FormatDuration`, and `getUnit`.

## Rust owner inventory and comparison

The four direct Rust owners are exported by `rust/crates/tidb-exec/src/lib.rs`
and contain:

- `exec_details.rs` — 991 lines, three source-shaped formatting/IA tests;
- `runtime_stats.rs` — 2,620 lines, sixteen focused runtime/RU tests;
- `ruv2_metrics.rs` — a re-export of `tidb_util::ruv2_metrics`;
- `tiflash_stats.rs` — 1,753 lines, three TiFlash/vector/columnar tests.

The owners' headers explicitly identify them as `SEED`s. `exec_details.rs`
does not own Go's `ReadPoolTaskDetails`, `ToZapFields`, P90 summaries,
`SyncExecDetails`, or cop-task aggregation. `runtime_stats.rs` stops at 20
type IDs while Go master has 22, and has no `HashStateRuntimeStats`,
`ExplainRURuntimeStats`, analyze scan-byte estimate, root/cop row snapshots,
summary-coverage state, or read-pool propagation. `ruv2_metrics.rs` omits
Go's `context.Context` plumbing and process-global Prometheus side effects;
its RUDetails and recorder paths use Rust stand-ins. `tiflash_stats.rs`
explicitly leaves `MergeTiFlashRUConsumption` unported because the resource
manager protobuf decode/client-go RUDetails owner is absent.

Those gaps are not isolated formatting defects: they cross the ordinary
executor, client-go, protobuf, resource-manager, metrics, and context
boundaries. Removing the Rust-only seed API or adding just the recent Go
master fields would either strand current consumers or create a second,
non-Go execution path. Per `AGENTS.md`, this package therefore remains an
explicit unclaimed inventory until its complete dependency-closed owner can
land atomically. No focused regression was added because there is no safe
atomic production fix in this batch; the existing source-shaped tests remain
the executable evidence for the implemented subset.

## Validation

Profile: Ready for this docs-only authority refresh; the package itself
remains explicitly unclaimed and is not presented as a completed transcreation.

Commands run from the repository root:

- `git ls-tree -r --name-only origin/master -- pkg/util/execdetails`, full
  source reads, declaration inventories, and the Go-master diff — passed;
  confirmed the eight-artifact inventory and the listed recent additions.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test
  ./pkg/util/execdetails -count=1` — passed on the checkout source.
- Exact detached Go-master checkout at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: the same package test passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler
  DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib
  cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked
  -p tidb-exec --lib exec_details` — passed, 3 tests.
- The same locked command for `runtime_stats` and `tiflash_stats` — passed,
  respectively 16 and 3 tests.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked
  -p tidb-util --lib ruv2_metrics::tests` — passed, 9 tests.
- The same OpenSSL/toolchain environment with
  `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --locked
  -p tidb-exec --lib` — passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all --
  --check` — passed.

No Go source, Go import block, test function, Bazel file, or module metadata
changed, so `make bazel_prepare` is not required. The pinned `make lint` was
run successfully on a clean committed worktree; the concurrent working tree
has unrelated uncommitted failpoint-generated edits that produce unrelated
lint findings. `git diff --check` is run after the receipt/plan documentation
batch is staged.

## Risks and unverified scope

- Correctness: the implemented Rust subset passes its focused tests, but the
  package as a whole cannot be claimed until the listed context, client-go,
  protobuf, metrics, and ordinary executor seams are integrated.
- Compatibility: adding only the 22-type or read-pool fragments would expose
  APIs without their Go consumers; removing the seed wrappers now could break
  existing Rust callers. Both actions are deferred with the boundary recorded.
- Performance: no production behavior changed and no performance claim is
  made; Prometheus and concurrent context paths remain unverified.
- Not verified locally: non-host Go/Rust platform selections, unavailable
  resource-manager/client-go integrations, and higher-level executor/session
  callers that consume this package.
