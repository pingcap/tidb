# `pkg/planner/funcdep` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go package inventory

The pinned package contains exactly these six artifacts:

- `pkg/planner/funcdep/BUILD.bazel`
- `pkg/planner/funcdep/doc.go`
- `pkg/planner/funcdep/fd_graph.go`
- `pkg/planner/funcdep/fd_graph_test.go`
- `pkg/planner/funcdep/extract_fd_test.go`
- `pkg/planner/funcdep/main_test.go`

There are no generated or platform-specific variants, fixtures, benchmarks,
README files, or other support artifacts in the package. The current Go copy
is byte-identical to the pinned revision.

`BUILD.bazel`'s production library maps to `tidb-funcdep` plus the logical-plan
integration in `tidb-planner`; Cargo owns the corresponding build graph.
`main_test.go` installs Go leak checking around the Go package tests. Rust has
no goroutine test process and therefore no equivalent support hook; all mapped
Rust tests run under Cargo's normal test harness.

## Rust ownership and behavior

- `crates/tidb-funcdep/src/fd_graph.rs` owns the complete FD graph operations:
  strict, lax, equivalence, constant, and conditional edges; closure and
  implication; nullable/not-null transformations; Cartesian and outer joins;
  projection; one-row relations; expression-id registration; aggregation
  metadata; and deterministic display/equivalence-class behavior.
- `crates/tidb-funcdep/src/lib.rs` exposes the graph and the existing
  null-rejection expression analysis used by logical extraction.
- `crates/tidb-planner/src/logical/functional_dependencies.rs` owns the pinned
  `ExtractFD` behavior for DataSource, Selection, Projection, Join, Apply,
  Aggregation, UnionAll, Expand, and pass-through logical operators.
- Logical builders preserve Go's statement-wide column-id allocation,
  `tidb_enable_new_only_full_group_by_check` expression-id gate, and
  read-committed/locking-read latest-index handling.
The package tests cover every production operation directly, while parsed SQL
regressions cover the projection/aggregation, UnionAll, Join, and Apply source
families from `extract_fd_test.go`. Planner-level tests also cover DataSource
unique keys and null rejection, outer-join conditional rules, correlated
Apply, expression-id gating, and latest-index failure. No ignored or
documentation-only test substitutes are counted.

## Validation evidence (Ready profile)

Failpoint decision: no `failpoint.`, `testfailpoint.`, or Bazel failpoint
dependency occurs in `pkg/planner/funcdep`, so no enable/disable cycle is
required.

Commands used for the package gate:

```text
GOTOOLCHAIN=go1.25.10 go test -tags=intest,deadlock -count=1 ./pkg/planner/funcdep
cd rust
cargo test --locked -p tidb-funcdep -- --nocapture
cargo test --locked -p tidb-planner logical::functional_dependencies::tests:: -- --nocapture
cargo test --locked -p tidb-planner extract_fd_source -- --nocapture
cargo check --locked -p tidb-funcdep -p tidb-planner -p tidb-executor
cargo fmt --all -- --check
cd ..
make lint
git diff --check
```

The original package checkpoint above passed with 18/18 graph tests, 16/16
logical extraction tests, 3/3 parsed-source tests, the pinned Go package test,
`make lint`, formatting, the three-crate type check, and whitespace validation.

This follow-up removes six explicit Rust-only `#[must_use]` diagnostics from
the four `FdSet` getters and the two null-rejection predicates. A detached
pre-fix owner with the new deny-on-discard regressions failed with exactly six
`unused_must_use` diagnostics. After the fix:

- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/332dd69a952932bb/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-funcdep --lib 'fd_graph::tests::return_values_may_be_ignored_like_go' -- --exact --nocapture` — passed;
- the same command with `null_reject::tests::return_values_may_be_ignored_like_go` — passed;
- the same locked toolchain with `-p tidb-funcdep --lib -- --test-threads=1` — 20 owner tests passed;
- the same locked toolchain with `-p tidb-planner 'logical::functional_dependencies::tests::' -- --nocapture --test-threads=1` — 16 planner consumer tests passed;
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml -p tidb-funcdep -- --check` and `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10
  TMPDIR=/tmp/tidb-codex make lint` — passed (Ready repository lint).

The current checkout's Go probe
`PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10
TMPDIR=/tmp/tidb-codex go test ./pkg/planner/funcdep -count=1` is blocked before
package execution by the existing `pkg/session` references to missing
`metrics.GlobalMemArbitratorSubTasks.CancelWaitAversePlan` and
`CancelStandardModePlan` fields. No Go file changed. The final repository
Ready lint is recorded after this follow-up; `make bazel_prepare` is not
required because no Go/Bazel/module file changed; Ready lint passed above.

## Residual risk

The transcreation preserves SQL/planner behavior rather than Go pointer or map
aliasing, which Rust's ownership model does not expose. Internal allocated
column-id numbers can differ when an earlier discarded Go subtree consumed an
id; live schemas and registered expression ids remain collision-free and have
the same dependency semantics. No repository-wide planner parity claim is made
by this package receipt.

## Follow-up: the session's ONLY_FULL_GROUP_BY flag reaches the planner (2026-09-09)

Go's `checkOnlyFullGroupBy` runs only when `sql_mode` has
`ONLY_FULL_GROUP_BY` (`logical_plan_builder.go:3731`). The executor bridge set
`builder.new_only_full_group_by_check` (the `tidb_enable_new_only_full_group_by_check`
experiment flag) but never `builder.only_full_group_by`, so the builder kept
its default of `true` and the check fired even when the session's mode omitted
the flag. `SELECT 1 + c, COUNT(*) FROM t` therefore failed under a context with
the mode off.

The bridge now forwards `ctx.only_full_group_by()` at every `PlanBuilder`
construction alongside the experiment flag.

Regression:
`tests_executor_suite_statements_source::the_session_only_full_group_by_flag_reaches_the_planner`
asserts the query is accepted with the flag off and rejected with 8123 when
the flag is on. It failed before the forwarding (the flag-off arm raised
`Unsupported("In aggregated query without GROUP BY ... only_full_group_by")`)
and passes after.

Ready validation from `rust/`:

```text
cargo test -p tidb-executor --lib the_session_only_full_group_by_flag_reaches_the_planner
# passed: 1
cargo test -p tidb-executor --lib statistics_request_tests
# passed: 16 (their full-suite failures are the documented global async-load
# flake; they pass in isolation)
cargo test -p tidb-executor --lib -- --test-threads=1
# 1,150 passed / 81 failed against the 1,149 / 81 baseline: the new regression
# is the only delta, plus the documented hash-agg spill flake in one run
cargo fmt -p tidb-executor -- --check
# pre-existing drift only (ddl.rs, ddl/alter_table.rs, access_cost.rs,
# index_range.rs)
git diff --check -- rust
# passed
```
