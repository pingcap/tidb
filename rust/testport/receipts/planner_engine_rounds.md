# `pkg/planner` — alternative storage-engine rounds parity receipt

Status: complete direct-package inventory; restored the Go optimizer's
engine-restricted alternative rounds and per-invocation cleanup behavior. The
nested `pkg/planner/core/casetest/mpp` fixture/test package is covered by the
companion receipt `planner_core_casetest_mpp_engine_rounds.md`; the surrounding
planner core packages and Rust planner runtime remain separate boundaries.

Comparison source: Go `origin/master` at
`78cac443a4f46c13bfe27eb247b5c80657952547` (2026-09-02).

## Complete Go inventory

The direct `pkg/planner` package contains three pre-existing artifacts and
1,109 lines: `BUILD.bazel` (39 lines), `OWNERS` (7 lines), and `optimize.go`
(1,063 lines). All three were read before editing, including every function in
`optimize.go`; the package has no `doc.go`, test, fixture/testdata directory,
generated output, platform-specific variant, or other build input. This batch
adds `optimize_engine_rounds_test.go` (a focused regression) and the matching
`go_test` target in `BUILD.bazel`.

## Go behavior restored

The optimizer now recognizes when the first physical plan actually mixes TiKV
and TiFlash reads, then considers complete TiKV-only and TiFlash-only rebuilds
when alternative plans are enabled. Explicit storage hints, enforced MPP, a
missing TiFlash path, or disallowed MPP keep the corresponding rounds disabled.
The round driver now captures setup state in a closure per invocation, so
concurrent optimizations cannot overwrite process-global saved flags; isolation
read-engine maps are restored after each round. The Explain/binding hint path
also counts hints nested under `EXPLAIN` when producing the binding warning.

The existing `physicalop.StorageEngineUsage` and
`physicalop.HasSingleScanIndexJoin` helpers are the direct planner boundary for
mixed-plan detection and the single-scan index-join carve-out. They remain
owned by the separate `pkg/planner/core/operator/physicalop` receipt.

## Regression and validation

The focused regression was first run before the source restoration under the
failpoint-aware wrapper and failed to compile because the new round gate
functions were absent. After restoration:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/planner \
  -run '^TestEngineRestrictedRoundGatesAndCleanup$' -count=1
# PASS

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/planner -count=1
# PASS

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
make lint
# PASS

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
# PASS

git diff --check
# PASS

make bazel_prepare
# BLOCKED: make: bazel: No such file or directory
```

The Go package has no Rust owner crate for the live optimizer, so no Rust test
command is claimed for this boundary. The separate Rust `tidb-exec` owner run
used while validating the preceding `pkg/meta/autoid` batch remains blocked by
the unrelated `auto_pre_split.rs` `FieldType::default()` compile errors.

## Risks and unverified surfaces

- Correctness risk is concentrated in mixed-engine detection and preserving
  the single-scan index-join exception; the new gate and map-restoration
  regression covers the state transitions, while full plan-shape behavior
  still belongs to the nested MPP integration suite.
- The complete Go-master MPP fixture is restored and inventoried in the
  companion receipt. Live mock TiFlash plan selection and execution still
  depend on the Go testkit validation described there.
- Bazel analysis is unverified because the local `bazel` executable is absent;
  Windows builds and full-workspace tests were not run.
