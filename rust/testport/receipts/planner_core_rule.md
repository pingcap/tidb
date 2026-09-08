# `pkg/planner/core/rule` current-master audit receipt

Go authority: `f5cf8f6337612c6ae51fb6e384e4bb3469dde680` (fetched master).
Rust batch parent: `feb796539f`.

## Package inventory

All nineteen direct artifacts, totaling 7460 lines, were read before editing:
fourteen production Go files, four Go test files, and BUILD.bazel. There is no
direct doc.go, fixture directory, generated/platform variant, or other tracked
build artifact. Nested `util` is a separate package. The complete path, blob,
line-count and declaration inventory is
`rust/docs/planner/core-rule-reading-inventory-20260908.md`.

The four original test artifacts are collect_column_stats_usage_test.go,
rule_max_min_eliminate_test.go, rule_partition_pruning_test.go and
rule_prune_indexes_internal_test.go. Their source was read; this batch does not
claim that all their cases have been ported or executed. The disabled issue12028
assertion is not passing coverage.

## Rust behavior repaired

Owner: `rust/crates/tidb-planner/src/logical/rule_predicate_simplification.rs`.
Shared conversion adapter: `rust/crates/tidb-planner/src/constraint.rs`, whose
complete two-artifact Go package audit is recorded separately.

Go logicalConstant calls Datum.ToBool with statement TypeContext and classifies
only successful conversions. Rust previously ignored Converted.event. Strict
`1garbage`/`0garbage` now remain Other; warning/ignore policies retain the numeric
truth value, and warning mode emits code 1292 with the DOUBLE diagnostic.
Parameter constants still bypass conversion when plan cache is enabled.

Go processCondition classifies leaves both before and after processing. Rust
now preserves both calls and their warnings. Unchanged AND/OR expressions are
retained rather than rebuilt. The shared conversion helper preserves existing
constraint behavior, including BINARY diagnostics.

## Regression and Ready evidence

Three added tests failed before the fix: logical_constant_obeys_statement_truncation_policy
(True instead of Other), strict_short_circuit_retains_truncated_operands
(AND operand deleted), and short_circuit_preserves_go_conversion_order_and_cache_guard
(no warnings instead of two). Evidence: `/tmp/core-rule-red-20260908.log`.

Run from repository root:

    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner --lib logical::rule_predicate_simplification::tests -- --nocapture
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner --lib constraint::tests
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner --lib logical::rule_tests
    make lint
    rustfmt +nightly-2026-08-22 --edition 2021 --check rust/crates/tidb-planner/src/constraint.rs rust/crates/tidb-planner/src/logical/rule_predicate_simplification.rs
    git diff --check

Results: 5/5 predicate tests, 6/6 constraint tests, 57/57 consumer tests; make lint
passed. Logs: `/tmp/core-rule-green-20260908.log`,
`/tmp/core-rule-constraint-20260908.log`, `/tmp/core-rule-consumers-20260908.log`,
`/tmp/core-rule-ready-lint-20260908.log`. Existing compiler warnings remain.

## Scope still open

This is a validated package-scoped repair, not whole-package or optimizer parity.
The original Go test mapping, join final-deletion path, constant propagation,
partition processing and statistics consumers still require comparison.
The datatype conversion layer's merged overflow/truncation events and invalid
UTF-8 behavior are not repaired here. No Go source or fixtures were changed.

## Follow-up: join propagation validity filter

Parent `35433b5de2`; same Go authority and complete package inventory.
Go applyPredicateSimplificationHelper passes its validity callback to the
ordinary propagator even when full propagation is disabled. The Rust join
wrapper passed None and could retain rejected derived DNF predicates.
It now forwards valid, matching the ordinary wrapper and the Go call.

Regression `join_simplification_preserves_filter_without_full_propagation`
uses `(a=b AND a>7) OR c=9` and a rejecting callback. It verifies both callback
invocation and expression equality with the ordinary simplifier, for which Go
uses the same helper. Before the fix, independent runs failed on zero versus
one callback and on the extra derived predicate. Red logs:
`/tmp/core-rule-join-red-20260908.log` and
`/tmp/core-rule-join-shape-red-20260908.log`.

Ready commands use the same toolchain and flags above: predicate owner filter
`logical::rule_predicate_simplification::tests` passed 6/6 and consumer filter
`logical::rule_tests` passed 57/57. `make lint` passed. Logs:
`/tmp/core-rule-join-green-20260908.log`,
`/tmp/core-rule-join-consumers-20260908.log`, and
`/tmp/core-rule-join-ready-lint-20260908.log`. Formatting and diff checks are
performed before commit. This does not close the remaining package audit.

## Follow-up: preserve conjunctions during OR cleanup

Parent `63a4d3cda7`; same full Go inventory and pinned authority.
Rust recursive OR cleanup applied its hash deduplication to AND branches as
well as leaves. Go appends recursively cleaned AND branches directly and only
deduplicates other branches. Rust now follows that branch boundary.

Regression `redundant_or_retains_conjunction_branches_after_recursive_cleanup`
uses `((a OR a) AND b) OR c OR ((a OR a) AND b) OR c`. It verifies three outer
branches in order, both conjunctions reduced to the ordered terms a,b, and one
ordinary c leaf. Before the fix, branch count was two; see
`/tmp/core-rule-or-red-20260908.log`. A later test-only assertion correction
avoids comparing convenience-function Tiny metadata with the composer's
inferred result type; the failing branch-count assertion is unchanged.

Ready commands are the same predicate/consumer test commands above: 7/7
predicate tests and 57/57 consumer tests passed. `make lint` passed, along with
rustfmt and diff checks. Logs: `/tmp/core-rule-or-green-20260908.log`,
`/tmp/core-rule-or-consumers-20260908.log`, and
`/tmp/core-rule-or-ready-lint-20260908.log`. No Go files were changed.
