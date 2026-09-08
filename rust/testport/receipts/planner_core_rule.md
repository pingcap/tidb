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

## Follow-up: reconstruct reduced IN through the builder

Parent `ddf4f77c1d`; same complete nineteen-artifact Go inventory and authority.
Go updateInPredicate invokes NewFunctionInternal after removing matching IN
members. Rust instead created a bare ScalarFunction with old result metadata.
The rule now passes the remaining arguments through its statement builder.

Regression `in_ne_rebuild_preserves_derived_string_collation` starts with a
general-ci column and literal a, plus an explicitly binary-collated literal b.
After NE removes b, construction from the remaining arguments derives
general-ci. The old Rust node kept binary collation and returned Int(0) for
the row A, while the rebuilt expression returns Int(1). The final regression
failed on that row result before the production fix and additionally checks
the reconstructed expression's metadata. `/tmp/core-rule-in-red-20260908.log`
contains the failing 0-versus-1 assertion. An initial ordinary-string probe
passed and was not used as evidence of the bug.

Ready: the same predicate owner command passed 8/8; logical::rule_tests passed
57/57; make lint passed. Logs: `/tmp/core-rule-in-green-20260908.log`,
`/tmp/core-rule-in-consumers-20260908.log`, and
`/tmp/core-rule-in-ready-lint-20260908.log`. rustfmt and diff checks pass.
No Go or expression-crate source changed. The Rust construction adapter is
fallible; a rejected reconstruction retains both original predicates. Its
error contract and expression-layer constant equality still require separate
comparison; this receipt does not claim those boundaries fully match Go.

## Follow-up: bound false OR branches and plan-cache marking

Parent `783d3f9e53`; same complete core/rule inventory and Go authority.
Go unsatisfiableExpression delegates to logicalop.IsConstFalse, which checks
the bound constant without a plan-cache guard. pruneEmptyORBranches marks
the plan uncacheable if that check leads to a mutable branch being pruned.
Rust reused the guarded logical_constant classifier, retaining such branches
and never reaching the cache marker. The consumer now uses NULL or successful
statement-aware conversion to zero directly.

Regression `false_parameter_or_branch_is_pruned_and_disables_plan_cache`
verifies parameters bound to zero and NULL: the generic classifier remains
Other, OR pruning removes the branch, and the exact cache reason is emitted
for each rewrite. Before the fix it failed because the branch remained;
`/tmp/core-rule-false-red-20260908.log` records that failure.

Ready predicate tests passed 9/9; logical::rule_tests passed 57/57; make lint,
rustfmt and diff checks passed. Logs: `/tmp/core-rule-false-green-20260908.log`,
`/tmp/core-rule-false-consumers-20260908.log`, and
`/tmp/core-rule-false-ready-lint-20260908.log`. The logicalop dependency body
was read to resolve this call contract; its package was not edited or claimed
complete. No Go source changed, and the broader package audit remains open.

## Follow-up: MAX/MIN split source candidates

Parent `568aa00db6`; same Go authority and complete core/rule inventory.
The complete Go MAX/MIN file (267 lines), its sole Go test file (37 lines),
and the complete Rust owner were re-read. Rust owner:
`rust/crates/tidb-planner/src/logical/rule_max_min_elimination.rs`.

Go cloneSubPlans restores PossibleAccessPaths from AllPossibleAccessPaths for
each cloned source. Rust retained the original pruned subset through its
generic clone. The rule clone now restores the full path list before its
split aggregate is pruned.

Regression split_source_clone_restores_all_access_paths failed before the fix
with only index 22 instead of indices 11 and 22. It now verifies both independent
clones restore path order and that modifying one clone leaves its sibling and
the original unchanged. These are structural path candidates, not execution
admission proofs. Red evidence: `/tmp/core-rule-maxmin-red-20260908.log`.

Ready commands:

    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner --lib logical::rule_max_min_elimination::tests
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner --lib logical::rule_tests
    make lint
    rustfmt +nightly-2026-08-22 --edition 2021 --check rust/crates/tidb-planner/src/logical/rule_max_min_elimination.rs
    git diff --check

Results: 3/3 MAX/MIN tests and 57/57 consumer tests; lint and formatting/diff
checks pass. The owner suite includes the original Go empty scalar aggregate
regression's Rust counterpart and nullable single-MAX transformation coverage.
Logs: `/tmp/core-rule-maxmin-green-20260908.log`,
`/tmp/core-rule-maxmin-consumers-20260908.log`, and
`/tmp/core-rule-maxmin-ready-lint-20260908.log`. Handle-path control flow and
range-size policy remain pending comparisons; no whole-rule/package completion
is claimed. No Go files changed.

## Follow-up: integer-handle early return

Parent `09166c4a45`; unchanged Go authority and complete package inventory.
Go checkColCanUseIndex returns false immediately if its matching integer
handle has residual filter conditions. Rust's any closure kept searching
later paths. The ordered loop now distinguishes this whole-search return from
unrelated-path continuation.

Regression integer_handle_residual_stops_before_later_covering_index uses
integer handle a, index (b,a), and b=1. It checks index-only success,
table-before-index failure, and index-before-table success. The original
implementation failed the table-before-index assertion; behavioral red
evidence is `/tmp/core-rule-handle-red-20260908.log` (after fixing a test import).

Ready: the MAX/MIN owner command above passes 4/4 and logical::rule_tests passes
57/57. make lint, rustfmt and diff checks pass. Logs:
`/tmp/core-rule-handle-green-20260908.log`,
`/tmp/core-rule-handle-consumers-20260908.log`, and
`/tmp/core-rule-handle-ready-lint-20260908.log`. Range-size policy and other
unverified package behavior remain open. No Go source was edited.

## Audit checkpoint: range context and order-aware traversal

At `69b5be8c52`, MAX/MIN still passes zero to ranger despite Go passing
SessionVars.RangeMaxSize. Rust ranger already has a memory-limit input and
DNF fallback; the missing value spans session statement snapshots, executor
StmtContext, RuleContext, and the rule call. The sysvar registration alone
does not make it effective. Full-chain correction and regression remain open.

The complete 245-line Go order-aware owner and complete Rust counterpart
were compared function by function. The ExecPlan records all eight Go
declarations and their Rust mappings, including the separate vertex traversal
needed by Rust ownership. No confirmed new owner mismatch in this pass;
joinorder choice/annotation and ordering dependencies remain unproven.
The two existing owner tests pass via the usual cargo command with filter
logical::rule_order_aware_join_reorder::tests; log
`/tmp/core-rule-order-audit-20260908.log`. No production changes or Ready
behavior claim are part of this checkpoint.

## Statement-context prerequisite reading

At parent cb54e38fc5, all four Go stmtctx artifacts (2455 lines) were read,
including seventeen tests and one benchmark. The separate inventory records
every blob and function declaration. Constructor/reset/build-state restoration
rebind range fallback handling to warnings and plan-cache state; this expands
the required evidence for the pending range-limit transmission repair.
No Rust behavior changed, no tests were claimed run for this reading step,
and the range-limit gap remains open pending variable/session inventories.

Variable prerequisite checkpoint at f252a064aa: the new variable reading
inventory records every tracked direct/nested artifact and eight fully read
direct files, including nextgen_test.go, which is absent from the ordinary
BUILD test list. Remaining files are pending; this is source-reading evidence
only, with no production fix or new validation claim.

At 398b4c16af, another seven variable artifacts were read completely, bringing
direct reading coverage to fifteen. The inventory records accessor, removed
variable, status-variable and registered-hook findings. Nine direct files and
nested tests remain pending. No production change or test execution is claimed
for this source-reading checkpoint.
