# `pkg/planner/util` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`94eb995357f34b7bab4889a82f0405797046447d` (2026-09-02).

## Complete inventory

The root package contains 16 direct artifacts and 3,581 Go/Bazel lines. Every
production source, test source, package harness, and BUILD target was read in
full before editing. The subdirectories (`coretestsdk`, `coreusage`,
`costusage`, `domainmisc`, `fixcontrol`, `partitionpruning`, `tablesampler`,
and `utilfuncp`) are separate Go package boundaries and are not folded into
this receipt. The root package has no `doc.go`, generated source, platform
variant, fixture tree, benchmark corpus, or nested package.

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 79 | planner utility library and six-shard flaky test target |
| `byitem.go` | 94 | order-item comparison and cloning helpers |
| `column.go` | 129 | index-column conversion and column utilities |
| `column_test.go` | 80 | index-column utility tests |
| `explain_misc.go` | 38 | explain-plan utility helpers |
| `expression.go` | 35 | expression cloning and extraction helpers |
| `funcdep_misc.go` | 125 | functional-dependency helpers |
| `handle_cols.go` | 484 | integer/common-handle column utilities |
| `main_test.go` | 34 | package test setup and goleak harness |
| `misc.go` | 360 | planner utility and protobuf conversion helpers |
| `null_misc.go` | 472 | null-rejection and expression analysis |
| `null_misc_builtins.go` | 234 | null-rejection builtin registry |
| `null_misc_test.go` | 566 | registry snapshot and null-rejection tests |
| `path.go` | 598 | access-path model, correlated predicate split, and range/order helpers |
| `path_test.go` | 123 | access-path and range tests |
| `slice_recursive_flatten_iter_test.go` | 130 | recursive slice iterator tests |

The production files contain 109 function/method declarations. The test
files contain 21 helper/test declarations, including `TestMain` and seven
source tests at the comparison revision. The package's complete BUILD
dependency closure was checked; the regression below uses dependencies that
were already present in `BUILD.bazel`.

## Go-master delta and implementation

Go commit `684ced8facbdba11086a5a5fa9b102aa5808f061` adds the missing
`AccessPath.markConstCol` behavior in `SplitCorColAccessCondFromFilters`.
When a full-length index column is matched by an equality access condition,
including `index_col = correlated_col`, the path now allocates `ConstCols`
when needed and marks that index position. Prefix columns remain unmarked
because their stored value is truncated and does not identify one value.
This allows the later index columns to provide scan order for correlated
subqueries without changing range construction.

`TestSplitCorColAccessCondMarksConstColumn` builds a correlated equality,
splits it into an access condition, and asserts that the full-length index
column is marked and no filter remains. Before the production edit the test
failed with `ConstCols` equal to nil; it passes with the restored behavior.

## Rust ownership and parity result

Rust's `tidb-planner` has a partial index-order matcher in
`find_best_task/dispatch.rs` and an ignored source-derived integration
contract for correlated equality order. It does not have a dependency-closed
owner for Go's `AccessPath`, correlated range rebuilding, and complete
`matchProperty` pipeline. No Rust-only behavior was found to remove, and no
standalone Rust facade was added; the missing Go behavior is restored in its
own package while the Rust execution boundary remains explicit.

## Validation and risk

Profile: **Ready** for this package behavior restoration. The package does
not import failpoint APIs, so the ordinary Go test command is sufficient.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex \
go test ./pkg/planner/util -run '^TestSplitCorColAccessCondMarksConstColumn$' -count=1
# pre-fix: FAIL (expected []bool{true}, got nil)
# post-fix: PASS

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex \
go test ./pkg/planner/util -count=1
# PASS (0.972s)

make bazel_prepare
# blocked: `make: bazel: No such file or directory`

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
make lint
# PASS

git diff --check
# PASS
```

No Rust source changed, so no Rust cargo gate was applicable. Not verified
here: Bazel analysis/sharding, full planner integration tests, live TiDB
correlated-subquery execution, Windows execution, and full-workspace tests.
Correctness risk is limited to recognizing execution-time equality as a
single full-length index value; the focused regression covers the new state
transition and the existing package suite covers neighboring path behavior.
Compatibility and performance risk are low: only the path metadata is
updated, and no range or scan operation is added.
