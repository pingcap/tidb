# Retire disconnected read-planning adapters

This living ExecPlan follows root PLANS.md. Git preserves removed source and
historical audit receipts remain indexed under parity/current-audit.

## Purpose / Big Picture


Remove the unused predicate metadata/binding chain, duplicate lock classifier
and fixed two-reader adapter together. The live logical/expression/scan owners
and four prepared-read tests remain intact. Verify absent registrations and
unchanged retained code, then run grouped checks. Fewer inputs do not prove a
measured speedup or complete Go package parity.

## Context and Orientation


Cloud base 0cb2af0f2cd90f591048faa6be5bc2d0ded439b9, hparser-integration.
Go master remains b36c940a4332c866d8b0e2afde88f5e7c2fd7fed; native master
cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1 is unchanged. No incoming commits.
The removed tidb-planner modules condition_binding/residual_condition describe
syntax without any live executor consumer. logical_lock is a second classifier,
separate from retained logical/lock.rs and its operator tests. tidb-exec's
real_tikv_multi_read is used only by a private test harness; its struct/opener
and forwarding methods in real_tikv_read.rs are also unused outside that group.
Go keeps residual expressions with the logical join and shared expression
rewriter, locks with logicalop, and snapshots with the executor/transaction owner.

## Progress


- [x] Refresh refs and trace module, type and opener callers against Go ownership.
- [x] Remove four modules/four harnesses and private methods; retain four mixed-harness tests.
- [x] Verify retained bytes, aggregate registration, grouped all-target check and lint.
- [ ] Commit through actual hook; fresh pre-push build; verify remote and save startup.

## Milestones and Plan of Work


Remove the three planner modules and corresponding difftests/planner-tests files,
plus the multi-reader module and real_tikv_multi_relation_scan_source harness.
Delete only its struct, opener method and trailing impl in real_tikv_read.rs.
Trim only the residual-model test and imports from prepared_param_marker_source.
Verify every retained single-reader byte and the four retained test bodies.
Finally update the audit receipt and latest cleanup links without changing
finding dispositions. Keep all original Go obligations and meaningful tests.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh in every build shell. From rust/:

    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-planner -p difftest-planner-tests -p tidb-exec -p tidb-server --all-targets

Run root make lint and git diff --check. Fresh aggregate registrations must omit
all four removed harnesses and retain prepared_param_marker_source. Existing
logical/operator_tests and all remaining prepared tests must be byte-identical.
No broad runtime sweep for unreachable deletion. Normal commit must execute the
actual hook's cd rust && cargo build --locked -p tidb-server. Repeat immediately
before push, verify remote SHA and preserve concurrent changes; never bypass
hooks or force push.

## Surprises & Discoveries


Module-name scans alone are insufficient: several live modules are reexports or
impl blocks. The two-reader type therefore required separate type/opener tracing.
The mixed prepared harness contains four useful tests and one disconnected
residual-model test; only the latter and its imports are retired.

## Decision Log


Delete the complete unused adapter groups, preserving real shared owners and
original Go obligations. No new dependency, script or replacement test framework.
Date: 2026-10-05 America/Los_Angeles.

## Outcomes & Retrospective


Evidence and before-images: /workspace/.cloud-setup/read-plan-cleanup. Recover
individual files with git show 0cb2af0f2c:<path> into a temporary file for review.
Post-commit gates, remote verification and setup persistence belong in external
final-handoff.json, avoiding self-referential evidence commits. Counts remain
86 tracked / 30 repaired / 56 unresolved. No full Go/live TiKV suite, benchmark
or complete package acceptance is claimed.

Removed 2025 source/test lines and 24 private tests. Grouped all-target check,
root lint and diff checks pass. Fresh aggregates omit all four retired harnesses
and retain the mixed prepared harness. All 3537 other inputs are byte-identical.
