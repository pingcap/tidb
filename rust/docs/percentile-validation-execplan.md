# Preserve Go percentile argument validation

This living plan follows root PLANS.md. After this change, APPROX_PERCENTILE
accepts Go's constant percentage forms and returns the same validation errors
through planning and execution. This is one failure category, not completion
of a Go package or of the broader Rust quality goal.

## Progress


- [x] Inspect Go master fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85 and capture oracle SQL.
- [x] Identify rejection of string percentages and loss of aggregate error identity.
- [x] Run new expression regression red, restore evaluation and error propagation.
- [x] Run expression (4), diagnostic (1), integration (310), and lint gates; update report.
- [ ] Push the independently reviewed failure-category commit.

## Context and Milestones


Work in /tmp/tidb-hparser-current. Aggregate descriptors in
rust/crates/tidb-expr/src/aggregation/base_func.rs validate arguments before
the executor is constructed. Go accepts any non-NONE ConstLevel (constant
within an execution or across executions), evaluates it with EvalInt, and
checks the range 1 through 100. Rust currently accepts only integer literals.
The planner's aggregation builder erases AggDescError into a string, so
session tests also cannot observe the original error category.

First prove the literal mismatch with the added expression regression. Then
evaluate constant expressions using the current Columns context, preserve
Go's raw integer-field reads for non-string literals, and preserve descriptor
errors across PlanError into DriverError. Keep the existing session assertions
and add strict client error checks. Finally run the original aggregate suite
and integration gates. A later failure in that suite is new evidence, not a
reason to weaken its assertion.

## Concrete Validation


All cargo commands run from /tmp/tidb-hparser-current with
RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432.

    cargo test --manifest-path rust/Cargo.toml -p tidb-expr --lib approx_percentile
    cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_json
    cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
    make lint
    git diff --check

The oracle is /tmp/tidb-go-master-oracle/bin/tidb-server, started with unistore
at /tmp/percentile-go-data, SQL port 14841 and status port 14842. Replay
/tmp/percentile-oracle.sql using mysql --force on standard input. Evidence is
/tmp/percentile-go.out. Stop that exact oracle process after validation.
These tests may be rerun without changing goldens. Do not reset other changes.

## Surprises & Discoveries


Go reads a decimal 50.5 as integer field 0 and real 50e0 as its bit pattern
4632233691727265792. Unsigned max becomes -1. String '50' and folded 25+25
both return percentile result 2 over rows 1 through 4. PREPARE with an unbound
percentage fails as NULL in Go; it must not silently freeze a parameter.

## Decision Log


Preserve typed descriptor errors at the existing planner boundary rather than
recognizing error text in the executor. Retain the original session assertions;
the MySQL diagnostic is checked in addition. Use existing expression evaluation
and conversion APIs, not a second percentage parser. Scope is argument
validation; percentile accumulation already reached correct results in the
original test before its validation assertion.

## Outcomes & Retrospective


Implementation and focused checks pass. Original JSON aggregate test now
reaches its final assertion: APPROX_COUNT_DISTINCT(DISTINCT i) OVER (). Go
rejects that syntax with 1064; Rust accepts parsing and emits 1235. This is a
separate parser discrepancy, not a percentile validation failure. Evidence:
/tmp/percentile-expr-final.log (4 passed), /tmp/percentile-wire-green.log
(1 passed), /tmp/percentile-integration.log (310 passed),
/tmp/percentile-lint.log (exit 0), /tmp/window-distinct-go.out (1064).
The full objective still
includes other session failures, catalog, RealTiKV, source-size, and Go/Bazel
gates enumerated in BLOCKER_RESOLUTION.md and LOCAL_TEST_REPORT.md.
