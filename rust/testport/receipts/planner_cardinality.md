# `pkg/planner/cardinality` — Go-master parity receipt

Comparison source: Go `origin/master` at commit
`94eb995357f34b7bab4889a82f0405797046447d` (2026-09-02).

## Complete inventory

The package contains 18 tracked artifacts and 12,070 Go/Bazel/fixture lines.
Every production file, test, fixture, and BUILD target was read in full before
editing. There is no generated source, platform-specific variant, benchmark
corpus, or nested Go package.

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 101 | library and 50-shard flaky test target |
| `cross_estimation.go` | 287 | cross-column and cross-index estimates |
| `exponential.go` | 54 | exponential-backoff estimator |
| `exponential_test.go` | 84 | backoff and zero-repeat regression tests |
| `join.go` | 51 | join cardinality helpers |
| `main_test.go` | 66 | test setup, fixture loading, and goleak harness |
| `ndv.go` | 262 | NDV estimation and stats-node graph |
| `ndv_test.go` | 241 | NDV and property tests |
| `pseudo.go` | 242 | pseudo-statistics estimates |
| `row_count_column.go` | 315 | column range and equality estimates |
| `row_count_index.go` | 786 | index range, recursive, and handle estimates |
| `row_size.go` | 189 | row-size estimates |
| `row_size_test.go` | 76 | row-size tests |
| `selectivity.go` | 1,264 | predicate selectivity and stats status |
| `selectivity_test.go` | 3,118 | integration and selectivity tests |
| `testdata/cardinality_suite_in.json` | 425 | recorded input cases |
| `testdata/cardinality_suite_out.json` | 4,419 | recorded expected results |
| `trace.go` | 90 | cardinality trace helpers |

The production files contain 74 function/method declarations. Test and
fixture-support files contain 71 declarations and 57 top-level test/benchmark
entries at the comparison revision. The fixtures are checked-in JSON inputs
and outputs, not generated production code.

## Go-master delta and implementation

The restored production guard is the paired `histCnt > 0` condition in
`equalRowCountOnColumn` and `equalRowCountOnIndex`. A histogram bucket's
`Repeat == 0` means that no point frequency was recorded for its upper bound;
it must not be returned as an exact zero-row estimate. Go master therefore
falls through to the uniform estimate before applying the stale-last-bucket
heuristic. The focused `TestEqualRowCountZeroRepeatFallsBackToUniformEstimate`
constructs a version-2 column histogram with a zero-repeat matching bucket and
asserts the uniform estimate. Before the guard it failed with `0`; afterward
it returns `5` as expected.

Go master also contains larger, dependency-sensitive cardinality additions
that remain explicit follow-up boundaries in this batch: appended-handle
selectivity damping and point caps in `row_count_index.go`, recursive index
estimation error continuation, JSON-column selectivity filtering, appended
handle range-length handling, and their integration/golden test updates in
`selectivity_test.go` and the fixture outputs. This commit does not claim those
unimplemented deltas as package-complete parity.

## Rust ownership and parity result

Rust's `tidb-planner` has partial cardinality and order-planning carriers, but
no dependency-closed owner for Go's statistics histograms, TopN/CMSketch
loading, pseudo-statistics, session risk variables, and testkit integration.
No Rust-only cardinality behavior was found to remove. Adding a detached Rust
estimate would bypass the ordinary Go statistics pipeline, so the remaining
Rust boundary and the unimplemented Go-master deltas stay explicit.

## Validation and risk

Profile: **Ready** for this focused behavior restoration. The package uses
failpoints, so the canonical wrapper enabled and disabled them around both
focused and full package tests.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex \
./tools/check/failpoint-go-test.sh ./pkg/planner/cardinality \
  -run '^TestEqualRowCountZeroRepeatFallsBackToUniformEstimate$' -count=1
# PASS; failpoints enabled and disabled

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex \
./tools/check/failpoint-go-test.sh ./pkg/planner/cardinality -count=1
# PASS; 35.203s; failpoints enabled and disabled

make bazel_prepare
# not yet rerun after this edit; required before final staging because the
# existing top-level test file was extended and this checkout has no bazel binary

git diff --check
# PASS before staging
```

The final package batch will run `make bazel_prepare` and `make lint` before
commit. No Rust source changed, so no Rust cargo gate is applicable. Not
verified here: Bazel analysis/sharding, detached Go-master execution, full
planner integration beyond the package suite, live correlated statistics
workloads, Windows execution, and full-workspace tests. Correctness risk is
limited to the zero-repeat fallback; the focused regression and full package
suite cover the changed path. Compatibility/performance risk is low because
the guard only changes an exact-zero histogram fallback and adds no new
runtime work.
