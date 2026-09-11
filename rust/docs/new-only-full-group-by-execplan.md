# Connect the new ONLY_FULL_GROUP_BY checker

This living plan follows repository `PLANS.md`. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current.

## Purpose / Big Picture


When `tidb_enable_new_only_full_group_by_check=ON`, SQL must use the Go master's functional-dependency checker. A functional dependency means one set of column values uniquely determines another. The old default checker remains unchanged. In particular, default mode rejects SELECT l.pk,r.b FROM l JOIN r USING(pk) GROUP BY l.pk with 1055, while new mode accepts it when both pk columns are primary keys. LEFT JOIN must preserve unmatched rows with NULL values.

## Progress


- [x] (2026-09-11) Verify default OFF and new ON behavior against Go master binary fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85.
- [x] Identify missing mode setup in the harvested JOIN test and missing new-checker call in Rust.
- [x] (2026-09-11) Establish and run mode-specific Rust red regressions: default-mode assertion passes; ON still returns 1055, /tmp/join-fd-mode-red.log.
- [x] (2026-09-11) Connect projection FD validation, auxiliary ORDER BY exemption, scope completion, and delayed view validation.
- [x] (2026-09-11) Verify original JOIN cases, correlated scalar subqueries, strict/lax keys, outer joins, constant grouping, nested queries and windows: eight session regressions pass.
- [x] (2026-09-11) Ready checks: 16 planner FD tests, aggregation_tests, 310 session integration tests and make lint pass.
- [x] (2026-09-11) Independently committed and pushed as d23f87264c to origin/hparser-integration, rebased without conflict over concurrent statistics collector commit 673e47ccf6 (no overlapping files).

## Surprises & Discoveries


The original test cites `tests/integrationtest/t/planner/funcdep/only_full_group_by.test`, whose second line enables the new checker. Rust omitted this SET. Go's default old checker rejects the same USING query that Rust rejects. Adding USING dependencies to the old checker would therefore diverge from the source of truth.

`PlanBuilder.new_only_full_group_by_check` already receives the session setting but only controls projection expression-ID registration. `check_only_full_group_by` always executes the old AST checks. Rust already has bottom-up `LogicalPlan::extract_fd()` and `tidb_funcdep::FdSet`; reuse these, do not construct a second graph.

The first implementation exposed a second FD bug: GROUP BY '' had an empty column set and was mistaken for absent grouping. Go inserts the zero sentinel only when GroupByItems itself is empty. The original nested-join regression caught this with 8123 instead of 1055; after correcting that condition all original JOIN cases pass. CREATE VIEW also required delayed new-checker validation: /tmp/group-check-view-go.out proves CREATE succeeds and SELECT returns 1055; /tmp/group-check-scope-red.log captures Rust rejecting CREATE before the fix.

## Decision Log


Decision: preserve default-mode behavior and implement the new checker at the projection boundary. Rationale: Go explicitly selects the old checker before expression rewriting and the new checker after projection construction. Author/date: Codex, 2026-09-11.

Decision: keep failing cases and full row/error assertions. Correct only the missing upstream mode setup, with a separate regression proving the default still rejects the USING query. Do not accept all queries when the new flag is enabled.

Decision: use existing order_by_range for auxiliary ORDER BY fields. Rust rewrites aggregate arguments below aggregation, so it does not append Go's auxiliary aggregate-argument fields. HAVING SUM(b) and ORDER BY SUM(b) regression coverage verifies this mapping without adding unused flags. Persist consumed aggregate scope in LogicalProjection because Rust recomputes FD graphs; both projection construction stages call the checker. Resolve new-mode view definitions with a cloned context that defers grouping validation, while querying the stored definition uses the normal session context.

## Context and Orientation


Work in `/tmp/tidb-hparser-current`, target branch origin/hparser-integration. Oracle source and binary are in `/tmp/tidb-go-master-oracle` at fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85. The overall all-failures goal remains governed by BLOCKER_RESOLUTION.md in the user's hparser-integration-tidb directory.

Go `pkg/planner/core/logical_plan_builder.go:4407` selects the old checker only when the new flag is false. At `:1899`, buildProjection calls ExtractFD, tests aggregate state, constants, group columns and strict closure, and emits 1055 or 8123. It skips auxiliary aggregate columns and, without grouping keys, auxiliary ORDER BY columns. It handles ANY_VALUE and registered scalar expression IDs. It finally applies MaxOneRow for ungrouped aggregation and clears HasAggBuilt to avoid checking an inner aggregation again in an outer projection.

Rust owners: `rust/crates/tidb-planner/src/plan_builder/only_full_group_by.rs`, `plan_builder/projection_group_check.rs`, both projection stages in `plan_builder.rs`, and `logical/functional_dependencies.rs`. `tidb-funcdep/src/fd_graph.rs` supplies has_agg_built, group_by_cols, registered_unique_id, constant_cols and closure_of_strict. PlanError carries typed 1055/8123/3029 errors. `rust/crates/tidb-executor/src/view.rs` delays the check while resolving a new-mode view definition.

## Plan of Work


Milestone 1: add mode-specific session regressions to tests_harvested_relation_engine.rs, with nonempty tables. In default mode require 1055; after ON require matching INNER/LEFT results, including unmatched NULLs. Set ON explicitly in the original new-checker JOIN test. Run and retain red logs before implementation.

Milestone 2: add a sibling planner module for projection FD checking. Skip the old check only when ON and invoke the new validation after the projection schema and child are installed. Port all Go projection rules and auxiliary-field distinctions. Ensure FD state consumed by the new check does not leak into outer query blocks. Existing extract_fd recomputes graphs, so investigate how to encode completion on projection rather than mutating a discarded FdSet. Preserve view deferred-validation behavior. Do not merely disable validation or silently skip unregistered expressions without checking the Go branch.

Milestone 3: run original tests and fix proven gaps in FD extraction, one category at a time. Test nullable unique keys, multi-column keys, USING/NATURAL joins, outer-join direction, NULL-rejecting WHERE, nested joins and correlated scalar subqueries. Verify old-mode behavior independently. Add data where empty-result tests would hide wrong-row behavior.

## Concrete Steps


From /tmp/tidb-hparser-current, prefix every Rust command with RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432:

    cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib only_full_group_by
    cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib functional_dependencies
    cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib aggregation_tests
    cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
    make lint
    git diff --check

Run focused red tests first, then the full relevant checks after implementation. Capture logs in uniquely named /tmp files. Full session --lib may include unrelated known failures; compare names and messages, not just totals. Do not mark the full goal complete using only these scoped gates.

## Validation and Acceptance


Both modes must match Go. Given l rows (1,10),(2,20) and r row (1,30), default USING grouping returns 1055; new-mode INNER returns (1,30); new-mode LEFT returns (1,30),(2,NULL). Invalid non-key grouping must still reject. Original JOIN regression and new correlated-subquery source cases must pass without weakening error or row assertions. All Ready checks required by AGENTS.md must finish before claiming this category fixed.

## Idempotence and Recovery


Use fresh Session instances for tests. Go differential instances use dedicated unistore directories and ports; stop only the process started for this work. Keep unrelated remote commits, rebase the local category on a fetched tip and retest overlap before a normal push. Never force-push. Temporary instrumentation must be uniquely tagged and removed.

## Artifacts and Notes


`/tmp/join-fd-go.out`: default flag 0 then ERROR 1055 for USING. `/tmp/join-fd-go-new.out`: ON mode INNER 1/30 and LEFT 1/30,2/NULL. `/tmp/join-fd-oracle.log`: dedicated Go server log.

## Interfaces and Dependencies


Keep FdSet as the canonical graph. A projection validator should take the built projection, original ProjectionField metadata and query context, and return Result<(), PlanError>. Add persistent projection metadata only where required to reproduce Go's consumed aggregate scope; do not add a new optimizer mode or duplicate dependency algorithms. Preserve typed error fields through executor conversion.

## Outcomes & Retrospective


The mode-specific checker is connected and the original JOIN and correlated-subquery tests pass. /tmp/group-check-final-focused.log records eight passing SQL regressions; /tmp/group-check-planner-fd.log records 16 passing FD tests; /tmp/group-check-planner-aggregation.log records the aggregation suite; /tmp/group-check-session-integration.log records 310 passing integrations; /tmp/group-check-ready-lint.log records make lint success. This is a scoped failure-category fix, not completion of the whole Go planner package or the overall Rust quality goal. Readiness was independently rechecked at all four runner call sites and remains resolved; the previously measured access-path estimator mismatch is still outstanding.
