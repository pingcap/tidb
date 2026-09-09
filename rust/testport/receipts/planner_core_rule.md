# `pkg/planner/core/rule` current-master audit receipt

## Current range-quota integration batch (2026-09-08)

The Rust MAX/MIN rule now uses the session range quota and Go's hint-filtered
logical access paths. Ranger quota events share statement warning/cache state,
including recursive/DNF candidates; DNF memory accounting includes actual range
payloads. Prepared SELECT/DML preserve planning warning order and execute rejected
cache candidates once without inserting or rebuilding them. Cache hits do not
replay planning warnings; fix-control 49736 retains the forced-cache behavior.

Go authority: `f5cf8f6337612c6ae51fb6e384e4bb3469dde680`. Complete package reading
inventories and individual red/green evidence remain below and in the ranger,
variable and session inventories. This batch fixes these integration gaps; it
**does not claim complete planner/statistics/optimizer or core-rule parity**.

Ready checks executed: `make lint` passed; 64 ranger, 5 MAX/MIN, 2 shared-context,
6 SQL regression and 2 logical integration tests passed. The statement snapshot
regression passed within the expanded range filter. Expanded range tests report
36 passed, 11 failed and 2 ignored; all 11 failures have identical panic payloads
on HEAD (29 passed, 11 failed, 2 ignored). Prepared-cache module has 30 passes and
one pre-existing HashAgg panic, separately reproduced on HEAD. These failures
are limitations, not passing checks. Exact commands/logs appear in the checkpoints.

Remaining audit work includes other ranger callers, physical cacheability refusal
paths, legacy skip-cache state, and the full remaining planner/statistics packages.
The chronological checkpoints below document intermediate states; this summary
supersedes their pending statements for the changes and checks listed above.


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

At 2d9fdf901d, completed the generic variable.go implementation, bringing
direct reading coverage to sixteen files. Integer parse-overflow errors,
bound-clamping warnings, relaxed validation warning restoration and hook/alias
ordering are recorded as pending range-limit integration requirements. Eight
direct files and nested tests remain unread; no behavior change is claimed.

At 9f99bd62f1, completed the hint allowlist and conversion helper production/test
files (1443 lines), bringing direct coverage to nineteen. The range-size option
is hint-updatable in Go, so the eventual repair must test SET_VAR propagation.
Five direct files and all nested test packages remain pending. This checkpoint
records reading evidence only; no Rust behavior fix or Ready validation claim.

At 620d61522f, read the entire noop catalog and slow_log.go (1865 lines),
including warning collection, used-statistics output and all rule accessors and
parsers. Direct coverage is twenty-one artifacts; session.go, sysvar.go,
sysvar_test.go and nested tests remain pending. No Rust edit or test execution
is claimed for this prerequisite reading batch.

Local checkpoint after fc53443c27: completed all 4013 lines and 228 function
declarations of session.go. Direct coverage is twenty-two artifacts, with
sysvar.go, sysvar_test.go and nested test packages still pending. Raw session
construction leaves RangeMaxSize zero; later variable initialization and old
state restoration remain essential to the pending range-context fix. No Rust
behavior or Ready claim. Per the user's clarification, this documentation-only
checkpoint remains local and will accompany a future validated Rust batch.

Local continuation: sysvar.go lines 1–1300 read with all inline hooks; resume
at 1301. Strict-mode isolation-engine filtering, previous-statement cache
flags and auto-analyze validation are recorded in the reading inventory.
The file is partial; no Rust edit, test run or push is claimed.

Local reading advanced sysvar.go through line 2350, including all hooks in
1301–2350. Resume at 2351. Auto-analyze dependencies, statistics cache updates,
plan-cache aliases and platform-specific GROUP_CONCAT limits are recorded in
the inventory. The package remains incomplete; no behavior/test/push claim.

Local continuation read sysvar.go 2351–3370, reaching and inspecting the full
RangeMaxSize registration/setter. Remaining source starts at 3371. Fixed-value
index-join-build and merge-statistics-concurrency behavior, partition warnings
and distinct global/session stats-load storage are recorded in the inventory.
No Rust edit or Ready claim; documentation remains local.

Completed sysvar.go through line 4404, including the final catalog hooks and
default-initialization/dispatch/custom-DML helpers. Direct coverage is 23/24;
sysvar_test.go and nested tests remain pending. Fix-control error atomicity and
the absence of a range-limit new-install override are recorded. No Rust change,
test execution or push in this local reading checkpoint.

Local test-reading continuation covers sysvar_test.go lines 1–1200; resume
at 1201 inside TestTiDBServerMemoryLimit2. Direct complete count remains 23/24,
with nested tests pending. This records source inspection only, not test
execution or Ready validation. Documentation-only updates remain local and
uncommitted; publication waits for a verified Rust behavior fix.

Next contiguous test segment (1201–1600) is read. Resume at 1601 inside
TestTiDBAutoAnalyzeRatio; the file and package remain incomplete. Invalid
ratio values preserve prior state. No Rust change, test run, commit or push.

Completed sysvar_test.go (2422 lines) and nested tests/slowlog BUILD/TestMain
pairs. All 24 direct artifacts are read; three nested test source files remain.
The tracked inventory still contains 31 artifacts. Selectivity NotEqual
assertions comparing different types are not numerical parity evidence.
SkipInit initialization constraints are recorded for the range-budget repair.
No Rust implementation or Ready validation claim; documentation stays local.

Nested tests/session_test.go is now fully read (1083 lines). Only variable_test.go
and slowlog/slow_log_test.go remain pending in the inventoried tree. Verified
source expectations for optimizer setting inheritance, retry resets, savepoint
restoration and slow-log formatting; weak hook/field-comparison coverage is
identified in the inventory. No runtime verification or Rust change; local only.

Completed the final nested variable (743 lines) and slowlog (707 lines) tests.
All 31 inventoried variable-tree artifacts are now read. Generic/relaxed
validation, dependency ordering and slowlog snapshot/matching contracts are
recorded with helper coverage limits. Source reading is complete; the Rust
range-budget fix and package parity are not. No Ready run, commit or push.

Revalidated Rust implementation: MAX/MIN passes zero at both index range
call sites; session snapshot/executor/rule contexts lack the range budget.
Ranger's public wrapper drops the internal skip-plan-cache reason, and DNF
cumulative quota fallback has no observable fallback event for warning handling.
The existing integration range fallback cases are ignored empty gap tests.
These findings require context and fallback-side-effect work together; merely
changing the numeric argument would not establish parity. No code/test/push claim.

Read all eight artifacts in Go pkg/util/context and pkg/util/ranger/context
(898 lines); exact blob/line inventory is in the ExecPlan. Every fallback
attempts cache invalidation before the once-only capacity warning, and detached
ranger contexts share handler/tracker state. Rust already has the utility
handlers, so the pending integration should reuse them. This is prerequisite
source evidence only; no Rust change, Ready run, commit or push.

Checked the older ranger receipt against the pinned Go authority: four files
changed, requiring refreshed reading before ranger edits. Full changed-file
coverage is not claimed from the truncated diff. Query and DML cache admission
both inspect executor StmtContext.skip_plan_cache(); any utility-handler bridge
must preserve that consumer path and the statement warning sink. Local records
only, with no Rust implementation, validation or publication claim.

Fully refreshed ranger bench_test.go and ranger.go at the pinned authority;
their exact blobs and source observations are recorded in the ExecPlan.
Quota accounting follows conversion/interval compaction. CNF fallback preserves
the built prefix and records a handler event; empty BuildColumnRange bypasses
quota. The two new long-IN benchmarks are not in TestBenchDaily's invocation.
detacher.go and points.go refreshed reading remains pending. No Rust/test/push claim.

Completed full pinned detacher.go reading, including every recursive CNF/DNF
and shard-index helper. Range quota events occur before union and remain
observable even when candidate ranges are discarded. Ordinary unextractable
conditions may yield the same final shape without a quota event, ruling out
inference from final ranges alone. points.go is the remaining changed-file
reading prerequisite. No Rust behavior change, validation, commit or push.

points.go refreshed reading is complete (1054 lines). All four changed ranger
files are now read, with unchanged artifact identities checked against the prior
receipt. util_ranger.md records the refresh and remaining side-effect gap.
Utility handler baseline testing started under WIP; no integration/Ready claim.
The command completed successfully: four handler/tracker unit tests passed,
including warning-once and callback-panic behavior. This verifies the reusable
utility baseline only; statement/ranger integration remains unimplemented.

Started Rust implementation: handler-aware ranger entry now records quota
events through recursive detachers. A focused CNF regression failed before
wiring and passes afterward (WIP). MAX/MIN/session callers remain unwired;
full integration, broader regressions and Ready are still required. No push.


2026-09-08 WIP MAX/MIN quota propagation: RuleContext now carries range_max_size
and an optional shared RangeFallbackHandler; join-reorder's context clone retains
both. check_column_can_use_index passes the same context through selections and
uses its budget/handler for common-handle and secondary-index range detachment.
Go authority: rule_max_min_eliminate.go:88 explicitly supplies SessionVars.RangeMaxSize.
The existing integer-handle/index-order test now also exercises the covering-index
path at quota 1. It failed before the call-site change with 'range quota fallback
must prevent MAX/MIN index admission' (/tmp/maxmin-quota-red-20260908.log), then
passed. Handler assertions additionally require disabled prepared-cache admission,
'in-list is too long', and two warnings. All four MAX/MIN unit tests pass:
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner rule_max_min_elimination --lib -- --test-threads=1
(/tmp/maxmin-quota-handler-20260908.log). git diff --check passes.
The executor RuleContext initializer deliberately remains a WIP scaffold with
zero budget/None handler; session snapshot, actual warning sink, and cache
admission integration are still REQUIRED before Ready/publication. No end-to-end
fix or package completion is claimed. No commit/push; keep documentary changes
local until the verified Rust batch is complete.


2026-09-08 session prerequisite: enumerated all 92 tracked artifacts under
pkg/session at pinned master, including 26 direct package artifacts and nested
packages (34,341 total lines). Exact blobs/line counts/read status are recorded
in rust/docs/planner/session-reading-inventory-20260908.md. Fully read BUILD.bazel,
OWNERS, contextimpl.go, global_init.go and main_test.go (5/26 direct). No session
source/test edit yet: complete source reading remains required before wiring
StatementVarSnapshot. This is local reading progress only, not Ready or parity.


2026-09-08 session prerequisite continuation: fully read session_nextgen_test.go
(180), testutil.go (111), advisory_locks.go (111), mock_bootstrap.go (221),
sync_upgrade.go (155), upgrade_run.go (122). Direct package reading is now 11/26.
Recorded nextgen-only coverage, prepared execution helper lifetime, advisory lock
rollback/reference counting and upgrade retry/MDL ordering in the reading inventory.
No Rust edit/test/Ready run in this reading checkpoint; no commit or push.


2026-09-08 session reading advances to 15/26 direct artifacts: session_test.go,
upgrade_test.go, txnmanager.go and tidb.go completely read (1,262 lines). Recorded
provider publication/retry ordering, parse warning behavior, LOAD DATA LOCAL
retry exclusion, transaction-abort distinctions and bootstrap test coverage in
the inventory. No new Rust edit or runtime validation; prior code remains WIP.
Receipt/ExecPlan updates remain local and unpushed.


2026-09-08 session reading checkpoint: txn.go fully read (766 lines, contiguous
1–390 and 391–EOF); direct package count 16/26. Recorded LazyTxn state transitions,
statement staging/cleanup, memory-hook reset, commit timestamp preservation,
classic/nextgen lock decisions and TSO failure behavior in the reading inventory.
No additional Rust edit, test execution, Ready claim, commit or push.


2026-09-08 reading checkpoint: tidb_test.go and bootstrap.go fully read (1,212
lines), direct session package progress 18/26. Inventory records global initial
value seeding, classic/nextgen schema differences, initialization SQL error policy
and per-statement versus nested-context test contracts. No new Rust edit or
validation in this checkpoint; documentation remains local, uncommitted/unpushed.


2026-09-08 source-reading checkpoint: upgrade_backfill_test.go completely read
(500 lines); direct session package count 19/26. Inventory records persisted-row
versus SQL getter assertions, nextgen exclusions, old binding digest fixture
construction and duplicate/unparsable-row handling. No Rust edit, Ready run,
commit or push; documentary updates remain local.


Session prerequisite continuation: nontransactional.go fully read (873 lines),
bringing direct package reading to 20/26. Inventory records temporary session
overrides, collated shard boundaries, first-job error precedence, dry-run examples
and index eligibility. Existing Rust changes remain WIP; no code edited, tests
run, commit or push in this reading checkpoint.


Session reading checkpoint: starter_bootstrap_file.go fully read (694 lines);
direct count 21/26. Inventory records restricted-SQL restoration, bootstrap versus
upgrade transaction boundaries, two version stores and conditional reset-marker
publication. No additional Rust edit, test, Ready claim, commit or push.


Session prerequisite: starter_bootstrap_file_test.go completely read (928 lines);
direct count 22/26. Recorded rollback versus partial-commit evidence, stale codec
refresh, transient PD publication retry, bounded reset and mock-CAS coverage
limits. No new Rust edit, validation, commit or push; docs stay local.


Session prerequisite cursor: bench_test.go read through line 760, covering
helpers, scan/lookup/point-get/insert/sort/join benchmarks and the beginning of
partition-pruning DDL. Resume at 761; direct completion remains 22/26. Recorded
fixed-query plan-check and row-count helper limitations. Local documentation
only; no commit, push or Ready claim.


Session reading: bench_test.go completed through EOF (2,143 lines total), direct
package completion 23/26. Recorded empty-table partition benchmark limits,
prepared compiler path, daily benchmark exclusions and unchecked pipelined
execution errors. No Ready claim, commit or push; documentation remains local.


Session source-reading cursor: bootstrap_test.go through 650, resume 651; direct
completion stays 23/26. Recorded reserved-ID validation, new-cluster optimizer
default tests and legacy test setup limitations in the inventory. Documentation
remains local; no commit, push or Ready claim.


Session prerequisite reading: bootstrap_test.go 651–1050 complete, resume 1051.
Recorded historical-default replacement versus custom-value preservation and
nonprepared-cache upgrade defaults. Direct completion 23/26; local documents
only, no Ready claim, commit or push.


Session prerequisite reading: bootstrap_test.go completed through line 2011;
direct artifacts now 24/26. session.go and upgrade_def.go remain unread in this
audit; nested packages retain their independent pending status. Inventory records
statistics/cache default migrations and distinguishes index recreation from
preservation and schema precision from timestamp-value assertions. No new Rust
edit or runtime validation in this checkpoint. Receipt and ExecPlan updates stay
local: documentation-only work is not committed or pushed. A Rust batch still
requires focused regression evidence and the Ready validation profile before
commit and push.


Session prerequisite: upgrade_def.go read contiguously through 1400, resume
1401. Recorded the ordered registry and conditional/default-rewrite distinctions,
plus historical binding rewrite behavior. Version97 range-quota compatibility
is currently comment evidence only; implementation reading remains pending.
Direct artifacts remain 24/26 complete. Local documentation only; no new Rust
edit, Ready claim, commit or push.


Session prerequisite: upgrade_def.go fully read (2345 lines), direct completion
25/26; session.go remains. Confirmed v97 backfills zero only for absent range
quota, distinct from fresh-cluster defaults. Recorded statistics-default and
binding-digest migration behavior and nextgen transaction-file inversion in the
inventory. No new Rust edit or Ready validation; documentation remains local,
uncommitted and unpushed.


Session prerequisite cursor: session.go read through 1250; resume inside retry
at 1251. Recorded cache allocation gates, transaction commit/retry exclusions,
staged temporary-table publication and replay context resets. Direct completion
25/26. Local receipt/inventory/ExecPlan updates only; no commit, push or Ready
claim.


Session source reading advanced to 1950; resume 1951. Recorded global-variable
fallback/type-validation boundaries, internal parser mode and execution error
propagation in the inventory. Direct completion remains 25/26. Documentation
only and local; no new Rust edit or Ready claim.


Session reading reached 2650; resume 2651 after compilation timing. Recorded
precompile reset/cancellation ordering and restricted-session warning cleanup.
Noted unnamed-return deferred Close-error limitation from actual code. Direct
completion remains 25/26; documentation only, kept local without commit or push.


Session reading reached 3450; resume DropPreparedStmt at 3451. Inventory records
result finishing/error precedence and prepare-dedup AST/context isolation.
Direct artifacts remain 25/26 complete. Local documentation only; no Ready
claim, commit or push.


Session reading reached 4250, resume 4251. Confirmed GetRangerCtx shares the
statement's PlanCacheTracker and RangeFallbackHandler by pointer, providing
production evidence for the pending Rust integration. Recorded close/prepare
cleanup and cached DistSQL refresh behavior. Direct completion remains 25/26;
local documentation only, no Ready claim, commit or push.


Session source reading reached 5050, resume 5051. Recorded bootstrap ordering,
post-lock version checks and core session context construction. Common-global
loading still pending; direct completion remains 25/26. Documentation is local
only with no new Ready claim, commit or push.


Session prerequisite direct-package reading complete: all 26 direct artifacts,
including session.go 6051 lines. Nested packages remain separately pending.
Confirmed common globals invoke relaxed setters only for absent session values,
with CommonGlobalLoaded set before cache fetch; recorded error and migration
ordering. Ready is not claimed and Rust integration remains unfinished. Local
documentation updates are uncommitted and unpushed per user instruction.


Rust integration WIP: added signed range quota to StatementVarSnapshot and both
statement-context construction paths, using the registered vardef default when
absent. Executor context exposes the value and planner_bridge passes it into
RuleContext instead of hardcoded zero. Shared fallback handler integration is
still unfinished. This scaffold is not validated or ready for publication;
next add focused session regressions with fail-before/pass-after evidence, wire
actual statement warning/cache consumers, then run Ready. No commit or push.


Range quota snapshot regression passes (1 test) for default 67108864, SET to 1,
0 and i64::MAX on both read/DML contexts, and preservation of the previously
built context. Green log: /tmp/session-range-quota-green-20260908.log. Removing
only the two with_range_max_size builder calls reproduces failure; red log:
/tmp/session-range-quota-red-20260908.log. Builder calls restored afterward.
Shared warning/cache tracker wiring and Ready remain pending; no publication.


Shared range fallback WIP: StmtContext owns an Arc<OnceLock> handler/tracker
bundle and a warning adapter sharing its real warning buffer. Query and DML
prepared-plan builders initialize tracking; repeated initialization does not
reenable a rejected cache. RuleContext now receives the handler; cache admission
observes tracker rejection alongside the legacy reason field. Executor check and
one clone/dedup/admission/fresh-context regression pass; logs:
/tmp/range-shared-state-check-20260908.log and
/tmp/range-shared-state-test-20260908.log. Full SQL integration regression,
fail-before proof for this wiring, force/nonprepared behavior reconciliation and
Ready remain pending. This is not complete parity and is not published.


SQL integration regression added in tests_prepared_plan_cache.rs and currently
FAILS: prepared SELECT MAX(b), MIN(b) WHERE a=? with index(a,b), quota1 returns
correct values but SHOW WARNINGS is empty. Log /tmp/range-quota-sql-test-20260908.log.
Initial single-MAX probe also lacked warnings but does not exercise the split
aggregate index check; do not count it as proof of a regression. Multiaggregate
probe still requires path tracing. bind_cached_prepared_select_for_statement
constructs a separate local statement context on cache miss and returns binding
without draining its warnings; inspect alongside actual rule triggering before
fixing. Test currently checks results/warnings; cache flag assertion remains to
be added. No Ready claim or publication.


SQL failure diagnosis: temporary probes confirm MAX/MIN sees two aggregate
functions and a Selection over DataSource, but DataSource.all_possible_access_paths
and possible_access_paths are both empty although indexes contains public visible
ab(a,b). Thus the index loop never reaches range detachment. Probe log:
/tmp/range-quota-sql-probe-20260908.log. Probes removed. Next trace production
access-path initialization/order before treating empty warnings as loss at the
session boundary. Separate cache-miss context remains a potential propagation
gap, not the established cause of this test. No Ready or publication.


Access-path diagnosis refined: Rust plan_builder.rs deliberately initializes
hint-filtered enumerated_paths and unfiltered public_enumerated_paths while
leaving costed DataSourceAccessPath lists empty. MAX/MIN reads only the latter,
so the missing connection is representation consumption, not absent enumeration.
Pinned Go logical_plan_builder.go:5220 onward copies hint-filtered possiblePaths
into AllPossibleAccessPaths before constructing DataSource. Therefore the logical
rule must consume the corresponding filtered newborn paths, preserving order;
using unfiltered public_enumerated_paths would violate hints. Do not fabricate
costed path admissions to satisfy this logical check. Rust fix remains pending;
SQL regression stays failing, no Ready or publication.


MAX/MIN now consumes filtered enumerated_paths when costed paths are absent,
retaining table/index order and integer-handle early rejection. SQL regression
now passes: repeated prepared MAX/MIN executes return correct rows, emit range
quota warnings and never report found_in_plan_cache. Previous failure log:
/tmp/range-quota-sql-test-20260908.log; green:
/tmp/range-quota-sql-path-green-20260908.log. This establishes the early-path
representation gap as the cause of the missing warning in that query. Separate
cache-miss warning propagation, hint/order regressions, force-cache behavior and
Ready still need review. No commit or push.


Added SQL hint/quota regression: IGNORE INDEX(ab) and USE INDEX() suppress the
MAX/MIN range-quota event; FORCE INDEX(ab) emits exactly one capacity warning;
quota zero preserves results and emits none. All three range_quota session tests
pass in /tmp/range-quota-hints-20260908.log. This validates use of filtered paths
and unlimited quota through SQL. Force plan-cache (distinct from FORCE INDEX),
warning propagation across cache-miss contexts and full Ready remain pending.
No commit or push.


Force-cache wiring regression: executor context with fix49736 ON previously
rejected cache on range fallback (runtime red /tmp/range-force-red.log).
start_prepared_range_tracking now transfers that fix-control value to the shared
tracker, matching pinned Go executor/select.go:1263. Both range_fallback executor
tests pass (/tmp/range-force-green.log), including two forced fallback attempts
retaining cache eligibility with three warnings (two risk warnings, one capacity
warning by count). SQL warning propagation and Ready still pending; no push.


Forced-cache SQL regression added and fails at runtime: fix49736 ON, quota1,
prepared MAX/MIN returns correct rows but SHOW WARNINGS lacks the required risk
warning (empty buffer). Evidence /tmp/forced-range-sql-red.log. This is separate
from the now-fixed early-path enumeration gap. Prepared cache miss creates a
local StmtContext in prepared_ast.rs; PreparedSelectExecution carries plan/key
lease only, while dispatch.execute_prepared_select begins another statement
boundary before execution. Planning warnings need ownership across that boundary,
including errors; simply appending before the reset would lose them again.
Current SQL test intentionally remains red pending the integration fix. No Ready
claim, commit or push.


Prepared SELECT execution lease now owns cache-miss planning warnings (separate
from retained cache entry), drained once after nested statement execution even
when it returns an error. Cache hits receive an empty warning list. Forced range
SQL regression now passes (/tmp/forced-range-sql-green.log), against prior runtime
failure /tmp/forced-range-sql-red.log. Remaining: validate repeated cache hits,
warning order/error boundaries and analogous DML/rejected-plan handling; Ready
has not run and no publication is authorized by partial test success alone.


2026-09-08 WIP warning-boundary verification: SELECT and INSERT SELECT execution
leases now carry cache-miss planning warnings independently of cache entries.
DML regression failed with an empty warning buffer before the lease transfer
(/tmp/forced-range-dml-red.log), then passed. Both SELECT and DML regressions
also execute a second time, assert a cache hit, and require no replayed planning
warnings. A separate INSERT IGNORE regression exposed reversed warning order:
execution overflow 1264 preceded planning warnings in
/tmp/forced-range-warning-order-red.log. Dispatch now drains planning warnings
immediately after begin_prepared_statement_boundary and before execution, which
preserves Go's append order and retains them on an execution error. All three
forced_range session regressions pass in
/tmp/forced-range-warning-order-green.log. Command:
`cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-session --lib forced_range`.
`git diff --check` passes. This supersedes the earlier after-execution drain.
Rejected-cache-plan warning transfer still needs investigation: bind's Option
failure discards its local planner context and ordinary execution replans. Ready
has not run; no batch commit or push. Documentation-only updates stay local.


2026-09-08 rejected SELECT candidate regression: strengthening the existing
range_quota_fallback_is_visible_to_sql_and_prepared_cache test to require
`skip prepared plan-cache: in-list is too long` failed at runtime with only the
capacity warning (/tmp/rejected-range-warning-red.log). Pinned Go
pkg/planner/core/plan_cache.go generateNewPlan keeps the newly optimized plan for
execution even when UseCache becomes false; it only skips insertion. Rust's
cached_physical_query_plan previously returned None and lost both plan and
warnings. It now returns the plan plus admission state when StmtContext rejects
caching. The SELECT binder retains a private execution lease without inserting
it into cached_plans or rebuilding its already-bound ranges. The same SQL test
passes twice with correct results, no cache hit, and the required warnings
(/tmp/rejected-range-warning-green.log). Other physical cacheability refusals
still use the old fallback and need separate review. DML's analogous rejection
path remains to fix. No Ready validation, commit or push yet; local WIP only.


2026-09-08 DML rejection continuation: INSERT SELECT regression failed before
retaining the rejected candidate (/tmp/rejected-range-dml-red.log), and passes
afterward (/tmp/rejected-range-dml-green.log). Like SELECT, DML now executes its
already-bound plan once when StmtContext rejects caching, without cache insertion
or range rebuild. Two executions verify correct inserted rows, no cache hits,
the skip-cache warning and exactly one range-capacity warning each.
Expanded prepared-plan-cache module: 30 passed, 1 failed both concurrently and
serially (/tmp/prepared-cache-batch-suite.log and
/tmp/prepared-cache-batch-serial.log). The failing existing test is
expression_and_aggregate_parameters_rebuild_on_cache_hits: parallel HashAgg reads
an empty null bitmap at tidb-chunk/src/column.rs:262. Isolated backtrace is in
/tmp/prepared-aggregate-isolated.log. A controlled HEAD baseline replacing all
modified Rust files temporarily reproduced the identical panic
(/tmp/prepared-aggregate-head-baseline.log); all WIP bytes were restored in a
finally block, and git diff --check passes. This is a verified pre-existing
failure, not a passing module result. Ready/lint and further batch review remain
pending; no commit or push. Documentation stays local until a Rust batch is ready.


2026-09-08 Ready profile execution (not a completed batch): make lint passed
(/tmp/range-batch-ready-lint.log). Targeted cargo offline/locked tests passed:
-p tidb-planner --lib ranger:: (64), --lib max_min (5),
-p tidb-executor --lib range_fallback (2),
-p tidb-session --lib tests_prepared_plan_cache::range_quota (2),
--lib forced_range (3), --lib rejected_range_dml (1).
Logs: /tmp/ranger-batch-ready.log, /tmp/maxmin-batch-ready.log,
/tmp/range-context-batch-ready.log, /tmp/quota-sql-ready.log,
/tmp/forced-sql-ready.log, /tmp/rejected-sql-ready.log.
Overbroad session --lib range_ ran unrelated surfaces: 36 passed, 11 failed,
2 ignored (/tmp/range-session-batch-ready.log). These failures are not yet
individually attributed; do not claim the suite passes. The standalone planner
test-target name was invalid; correct target is --test all with module filter.
That compilation exposed a missing pre-existing RuleContext allow_agg_push_down
initializer in core_logical_cte_topn_prune_source.rs. Added false, matching the
unit test context; the corrected integration command now passes
(/tmp/core-rule-integration-ready-green.log). Final review and failure triage
remain before batch publication. No commit/push; doc-only changes remain local.


2026-09-08 expanded-range failure attribution completed: controlled HEAD run
/tmp/range-session-head-baseline.log reports 29 passed, 11 failed, 2 ignored;
current WIP /tmp/range-session-batch-ready.log reports 36 passed, 11 failed,
2 ignored. All 11 failing names AND panic locations/payloads match exactly after
removing thread IDs. Existing failures cover join range derivation (2), partition
pruning (2), sysbench access (3), and window ranges (4). The added seven passing
tests account for the count difference. All temporary HEAD source replacements
were restored byte-for-byte by finally; git diff --check passed afterward.
These are documented baseline limitations, not new failures or passing tests.
Targeted batch checks and make lint passed as recorded above. Final source/diff
review and package-scoped commit construction remain; no publication yet.


2026-09-08 publication checkpoint: Rust integration batch dbe2add614 was committed
and pushed normally to origin/hparser-integration (fc53443c27..dbe2add614).
Rustfmt --check passed on every changed Rust file; Ready evidence and baseline
limitations are recorded above. Next iteration reviews remaining ranger callers
and physical cacheability refusal behavior before changing further Rust code.
This post-publication checkpoint is documentation-only and remains local under
the user's instruction; no separate documentation commit or push.


Next batch (2026-09-08): ranger callsite audit recorded in
ranger-callsite-audit-20260908.md. Go partition_processor uses session RangeMaxSize
for HASH/KEY and LIST/RANGE pruning; Rust partition bridge hardcoded zero.
static_partition_pruning_respects_range_quota failed with empty warnings before
wiring (/tmp/partition-quota-red.log) and passes after wiring
(/tmp/partition-quota-green.log). Added handler-aware partition entry preserving
convert_to_sort_key=false/merge_consecutive=false, and threaded StmtContext through
LIST COLUMNS CNF/DNF helpers. Cache rebuild zero-quota calls remain unchanged.
This is WIP: expand partition variants, cache interaction and unlimited-budget
coverage; then run Ready before a separate meaningful Rust commit/push.


2026-09-08 partition batch coverage: static_list_columns_quota_preserves_recursive_predicates
passes for nested AND/OR predicates over LIST COLUMNS(a,b), checking all three
expected rows, one deduplicated warning at quota1, and no warning at quota0
(/tmp/list-columns-quota-test.log). Prepared HASH regression changes both IN
parameters between executions, verifies the two correct rows each time, no cache
hit and one capacity warning (/tmp/partition-quota-prepared.log). This validates
results and admission but does not yet attribute the admission refusal to quota
rather than the static-partition cacheability gate. No such stronger claim is
made. rustfmt applied and git diff --check passes. Remaining: other partition
variants, partition wrapper option contracts, Ready tests/lint and final review.
No new commit/push; all current documentation is local.


2026-09-08 partition quota batch Ready evidence: four SQL regressions cover HASH,
KEY, RANGE, RANGE COLUMNS, LIST and recursive LIST COLUMNS; quota1 preserves rows
and emits one warning, quota0 emits none. Prepared HASH checks changing parameters
and no hit. All four passed in /tmp/partition-sql-ready.log. The original HASH
regression failed before the fix with empty warnings (/tmp/partition-quota-red.log).
All 64 ranger tests passed (/tmp/partition-ranger-ready.log), and both partition
rule tests passed (/tmp/partition-rule-ready.log). Commands use
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked
with -p tidb-session --lib tests_prepared_plan_cache::static_,
-p tidb-planner --lib ranger::, and -p tidb-planner --lib rule_partition_processor.
make lint passed (/tmp/partition-batch-ready-lint.log). rustfmt and diff checks
passed. Final review confirms partition wrapper keeps sort-key conversion and
consecutive-range merging disabled, recursive calls share one statement handler,
and cache rebuild zero-quota paths are unchanged. This batch addresses the
pkg/planner/core/rule partition quota integration; whole-package parity remains
incomplete. It includes Rust changes, so accompanying receipt updates can ship
with the meaningful fix commit under the user's publication instruction.
