# `pkg/planner/core` — alternative-plan storage signal receipt

Status: complete direct-package inventory for the bounded storage-signal fix.
This receipt covers the root `pkg/planner/core` Go package only; every nested
directory (`access`, `base`, `casetest`, `cost`, `operator`, `partitionpruning`,
`resolve`, `rule`, and others) remains a separate package boundary.

Comparison source: Go `origin/master` at
`78cac443a4f46c13bfe27eb247b5c80657952547` (2026-09-02).

## Complete Go inventory

The direct package contains 116 artifacts and 76,570 lines: 106 Go
production/test files, `BUILD.bazel`, and nine direct `testdata` fixtures
(`explain_analyze_ru_suite`, `fts_resolve_index_suite`,
`plan_suite_unexported`, and `runtime_filter_generator_suite` input/output
books). The complete direct inventory was read before editing, including all
1,854 top-level function declarations and 231 test/benchmark/example
declarations. There is no direct `doc.go`, generated source, or
platform-specific variant. Nested BUILD files, fixtures, generated inputs, and
platform variants were treated as their owning nested-package boundaries.

## Go behavior restored

`setPreferredStoreType` now marks the statement context whenever a
`READ_FROM_STORAGE` hint selects a real access path. `buildDataSource` now
marks the missing-TiFlash-path signal when alternative logical plans are
enabled and the table has no TiFlash path (including explicit-transaction
`FOR UPDATE` reads whose TiFlash path is removed later). These signals gate
the optimizer's TiKV-only and TiFlash-only alternative rounds without
overriding explicit storage hints or attempting impossible TiFlash plans.

Focused regressions in `logical_plans_test.go` cover both hint marking and
missing-path marking. The restored nested MPP package then verifies the full
round-selection, hint, replica, enforced-MPP, and index-join behavior against
the Go-master fixtures; see
`receipts/planner_core_casetest_mpp_engine_rounds.md`.

No dependency-closed Rust owner for the Go plan-builder/testkit storage-signal
path was changed in this batch. The Rust planner signal model is documented by
the existing `planner_engine_rounds` and `planner_physicalop_engine_usage`
receipts; no speculative Rust facade was added.

## Regression and validation

Before the fix, `TestReadFromStorageHintMarksAlternativePlanSignal` failed
because the statement-context flag remained false. After the fix, the focused
core signal tests pass, and the nested MPP engine-round tests pass with the
restored Go-master fixture. The Ready profile additionally requires lint, Rust
formatting, and diff checks. `make bazel_prepare` is required by the changed Go
sources and is blocked in this environment because the `bazel` executable is
unavailable.

```text
./tools/check/failpoint-go-test.sh pkg/planner/core \
  -run '^Test(ReadFromStorageHintMarksAlternativePlanSignal|AlternativePlanMissingTiFlashPathSignal)$' \
  -count=1 -vet=off
# PASS

./tools/check/failpoint-go-test.sh pkg/planner/core/casetest/mpp \
  -run '^TestAlternativeEngine(RestrictedRounds|RestrictedRoundGates|RoundsSkipSingleScanIndexJoin)$' \
  -count=1 -vet=off
# PASS

make lint
# PASS

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
# PASS

git diff --check
# PASS

make bazel_prepare
# BLOCKED: make: bazel: No such file or directory
```

## Risks and unverified surfaces

- Correctness risk is limited to statement-local optimizer gating and the
  explicit-transaction TiFlash-path edge; the focused tests and MPP failpoint
  gates exercise both signals.
- Rust planner execution, Bazel analysis, Windows/platform builds, and
  full-workspace tests were not run locally.
