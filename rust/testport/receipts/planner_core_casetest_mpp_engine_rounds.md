# `pkg/planner/core/casetest/mpp` — alternative-engine round fixture receipt

Status: complete direct-package inventory and Go-master fixture/test
restoration. This receipt covers the nested MPP casetest package as one Go
package boundary; it does not claim parity for the surrounding
`pkg/planner/core` package or for the Rust planner runtime.

Comparison source: Go `origin/master` at
`78cac443a4f46c13bfe27eb247b5c80657952547` (2026-09-02).

## Complete Go inventory

The direct package contains seven tracked artifacts and 7,369 lines:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 32 | Go test target, data glob, shard/dependency inputs |
| `engine_rounds_test.go` | 329 | mock TiFlash fixtures and three engine-round regressions |
| `main_test.go` | 60 | test bootstrap and suite registration |
| `mpp_test.go` | 955 | existing MPP plan-shape casetests |
| `testdata/integration_suite_in.json` | 291 | input SQL/EXPLAIN cases |
| `testdata/integration_suite_out.json` | 2,887 | recorded plan output |
| `testdata/integration_suite_xut.json` | 2,815 | xUnit/recording companion data |

All three Go files, the BUILD target, and all three fixture files were read
before editing. The Go sources contain 34 top-level function declarations,
including 27 test/benchmark/example declarations (the three restored engine
round tests and the existing `TestMain`/MPP suite). There is no `doc.go`,
generated source, platform-specific variant, or additional build input in
this direct package. The nested casetest directories are separate package
boundaries.

## Go behavior restored

The package now contains the Go-master mock TiFlash fixtures and tests for
alternative logical-plan engine rounds. The tests cover mixed TiKV/TiFlash
round-one plans, cheaper TiFlash-only replacements, missing-replica and
`READ_FROM_STORAGE` gates, enforced-MPP and feature-off gates, isolation-engine
state restoration, and the single-scan versus double-read index-join carve-out.
The integration suite input and both recorded output companions are restored
as the exact Go-master fixtures. BUILD metadata includes the test source,
mock-store dependencies, and the 26-shard target.

No Rust production owner is dependency-closed for this live Go testkit/MPP
fixture package. The Rust planner's engine-signal helpers are covered by the
separate `planner_physicalop_engine_usage` and `planner_engine_rounds` receipts;
this package receipt records the remaining cross-runtime integration boundary
without inventing a Rust testkit facade.

## Regression and validation

Before restoration, the focused wrapper invocation failed at compile time
because the Go package lacked `engine_rounds_test.go` and its BUILD/fixture
inputs. After restoration, the focused engine-round tests and the package
suite were run through the failpoint-aware wrapper. The Ready profile also
requires lint, Rust formatting, and diff checks. `make bazel_prepare` remains
required by the restored Go test source and BUILD metadata but is blocked in
this environment because the `bazel` executable is unavailable.

```text
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

- Correctness risk is concentrated in mock TiFlash plan selection and the
  engine-round gate predicates; the restored tests assert both plan shape and
  execution results, but they depend on the full Go testkit.
- No Rust runtime behavior was changed in this batch; the Rust MPP planner
  implementation and cross-runtime integration remain unverified here.
- Bazel analysis, Windows/platform builds, and full-workspace tests were not
  run locally; the local Bazel executable is absent.
