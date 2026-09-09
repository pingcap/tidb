# `pkg/expression/test/constantpropagation` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains three tracked artifacts and 148 lines. Every test body,
test harness, and three-shard/flaky Bazel target was read before this receipt
was written. There is no `doc.go`, fixture/testdata directory, generated or
platform-specific variant, or generator input.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 20 | `14aa4f424b52569c292582c6810777fbae1065fb` | `fcff654eb7672a7f14e394a2353bf98ce075a96753b7050701f7e3661324c45f` | short flaky test target and testmain dependencies |
| `constant_propagation_test.go` | 71 | `0d96361e97acc29dfc36041d66a5aa39745001e9` | `e0af9c646b46fce670bda101817ecba5076af2629fae1d9aa1aadcfcee95a1f9` | one SQL test covering cast inference, pushdown skipping, and repeated merge-join planning |
| `main_test.go` | 57 | `3f623f036bee2adc34b82850d1c1d94d23188b17` | `b62386cf5c816bd57a8e9709b2c1b807683a22075698d6735e6bdb1c71f8306f` | common setup, failpoints, timezone, and goleak harness |

The sole test creates decimal/JSON/binary fixtures, checks a CTE with repeated
constant predicates, pins a `plan_tree` pushdown shape, and repeats a
STRAIGHT_JOIN/LEFT JOIN merge-join plan twenty times. `TestMain` configures
common test state, expression indexes, failpoints, system timezone, and
goleak exclusions. The Go master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` is empty for all three artifacts.

## Rust ownership and parity status

Rust expression constant propagation is implemented in
`rust/crates/tidb-expr/src/constant_propagation.rs`, with source tables in
`tests/constant_test_go_tables_source.rs`; planner propagation is owned by
`tidb-planner`. Those carriers exercise the expression-level substitutions and
outer-join rules, but they do not execute Go's full SQL plan-tree fixture with
mock storage, STRAIGHT_JOIN routing, or MergeJoin selection. The package test
therefore remains an explicit integration gap, not evidence that the Rust
constant-propagation implementation is missing a leaf function.

No Rust-only behavior was found to remove. Extending the expression crate with
planner/storage policy solely to make this one SQL fixture pass would violate
the package boundary; the next implementation unit is the dependency-closed
planner + executor plan-tree pipeline.

## Validation and risk

Profile: **WIP** for this documentation-only boundary audit; no production or
test file changed, so no new regression test or Ready claim is made. Exact
Go-master test (detached worktree, required `intest,deadlock` tags) passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test -tags=intest,deadlock ./pkg/expression/test/constantpropagation \
  -run '^TestConstantPropagation$' -count=1                 # passed
```

The package has no failpoint references in its source/build metadata, so no
failpoint enable/disable wrapper was required. Rust source, Bazel, and module
files were unchanged; `make bazel_prepare` was not required. Not verified:
Bazel execution, planner/executor integration in Rust, and full workspace
suites. Runtime behavior is unchanged by this receipt.

This receipt certifies the bounded package inventory and explicit SQL-plan
boundary; it is not a repository-wide parity claim.
