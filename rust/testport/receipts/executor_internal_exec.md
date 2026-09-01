# `pkg/executor/internal/exec` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains five tracked artifacts and 1,610 lines. Every production
file, source test, and eight-shard Bazel target was read line by line. There
are no generated files, fixtures, benchmarks, fuzz targets, or platform
variants.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 61 | `c258a46e1eb71dc324c7453228c7470679784ba3` | `4926ff0475814ba3b79738a7744a3247ff87728bb14add161efdd27fc87769fa` | internal executor library and eight-shard flaky test target |
| `executor.go` | 734 | `d0a1efe910bb2f44c4343f9327a13790c7faafba` | `5b3f2d390853c3ac4be69d76b29193f9ef7f6bf15556dac5a2c2b2ed56537985` | Open/Next/Close wrappers, chunk metadata, panic handling, killer, tracing, and RU-v2 accounting |
| `executor_test.go` | 118 | `9d67c38a468f83673464c6b04533c1c1b0ae40c9` | `2f4a53092efd2df468011d2deffaa155808d224b3a9f431d1d49213386cf2b08` | accumulator reuse/counting and executor metric mapping tests |
| `indexusage.go` | 149 | `7a245cfa2169499295ef1fdc411b708129a745fc` | `a0a92ece9753ebe39c423d8a1e4f485af76e56238f56c4e0fb84b7ae46f03060` | clustered-index lookup and cop/point usage reporting |
| `indexusage_test.go` | 548 | `9b6c4d2d62038f998025e00985127c88ab84426a` | `213c354485efb0dbec3066526977f2f48a1fc8a5c9011a95deb0b9a09e579851` | mock-domain, prepared-plan, partition/global-index, disablement, and clustered-index coverage |

`executor.go` defines the internal `Executor` contract, `BaseExecutorV2`/
`BaseExecutor` metadata and chunk allocation, panic-safe Open/Next/Close
wrappers, SQL-killer checks, tracing, and RUV2 input/output accounting. The
tests cover zero-column row accounting, accumulator reuse/allocation, and the
complete concrete executor metric map. `indexusage.go` maps integer/common
clustered handles, skips pseudo statistics for cop reads, uses the smallest
non-zero point-get bucket when stats are missing, and reports logical versus
physical table IDs. Its tests exercise direct reporting, real SQL plans,
prepared statements, partitions, global indexes, disabled collection, and
clustered-index metadata. `BUILD.bazel` keeps the package visible only to
executor subpackages.

## Rust ownership and parity fix

The dependency-closed Rust owners are `tidb-executor::executor` for the
pull-based trait and `ExecutorMeta`, and
`tidb-executor::driver::index_usage_reporter` for usage reporting. The trait
is intentionally re-exported because concrete Rust executors and integration
tests in other crates construct it; that cross-crate API is not Rust-only
behavior. The Rust wrappers and source-derived reporter tests preserve the
Go-derived collector rules.

One behavior diverged: Go's `getClusterIndexID` returns index ID `0` for a
common-handle table even when no primary index entry is present (the zero
value is the source contract). Rust's live reporter returned `None` and
silently skipped usage. `cluster_index_id` now returns `Some(0)` for that
case. The focused regression
`common_handle_without_primary_uses_zero_index_id` failed before the change
and passes after it; no new public API or Rust-only execution path was added.

## Validation and risk

Profile: **Ready** for the index-usage behavior fix. No Go or Bazel source
changed, so `make bazel_prepare` is not required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/executor/internal/exec -run '^TestIndexUsageReporter$' -count=1
# passed; focused Go reporter coverage; failpoints enabled and disabled by wrapper

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-executor --lib driver::index_usage_reporter::tests --offline --locked
# passed; 3 Rust reporter tests, including the new regression

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
make lint
# passed
```

The pre-fix regression run failed as expected because the reporter skipped
index 0. A broader failpoint-safe Go package run was attempted but its
existing `TestIndexUsageReporterWithClusterIndex` timed out waiting for mock
index-usage publication; the focused reporter test and Rust suite pass. Not
verified: Bazel execution, that flaky integration path, and full workspace
tests. Existing Rust warnings and unrelated dirty files remain.
