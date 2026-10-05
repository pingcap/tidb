# Share typed user-variable ownership

## Purpose / Big Picture


User variables must retain their declared SQL types even before an inline assignment executes and across session migration. Remove the value-only session map and make SET, planning, expression execution, prepared statements and migration use the existing UserVars owner. This connected S03/X01 maintenance batch is not complete upstream package acceptance.

## Progress


- [x] Confirmed clean Cloud checkouts and refreshed integration/master. Located all live callers and Go source owners.
- [x] Capture fail-before regressions in the existing user-variable suite.
- [x] Migrate the owner, typed planning and session-state transfer together.
- [x] Run grouped behavioral checks, all-target checking, lint and self-review; update both registers.
- [ ] Commit with actual locked-build hook, rebuild immediately before authorized push and verify remote; save Cloud checkpoint.

## Context and Orientation


The editable checkout is /workspace/tidb, hparser-integration at 2b7793fa87cf14960afc26de0abcbb68e00bbb48. Go master b36c940a4332c866d8b0e2afde88f5e7c2fd7fed is exported in /workspace/.cloud-setup/go-master. Native client cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1 is unchanged. Go pkg/sessionctx/variable/session.go owns independent value and type maps. pkg/planner/core/expression_rewriter.go publishes inline RHS types at planning time; pkg/executor/set.go stores the planned expression type. pkg/expression/builtin_other.go selects typed read and assignment signatures.

Rust tidb-expr/src/user_vars.rs already supplies the shared owner. Session and StmtContext instead use a second value-only map; migration infers types from values and ignores incoming types. Session's AST variable binder also chooses read signatures from values before shared expression planning sees inline assignments.

## Plan of Work


First extend existing tests to show independent migration maps, inline types with no input rows and SET declared width/scale. Replace the duplicate map and migrate every caller. Thread UserVars through existing resolver wrappers; select typed reads and publish inline types during expression rewriting. Keep SET's plan-before-execute ordering, retaining compiled expressions/plans rather than rebinding against values changed by earlier assignments. Snapshot both maps under one lock when encoding and restore them independently. Remove obsolete value-based binder logic and update stale ownership comments.

## Concrete Steps


Source /workspace/.cloud-setup/env.sh before builds; work in rust/ with CARGO_BUILD_JOBS=1. Run cargo test --locked -p tidb-session --lib user_variable_batch before edits, preserving failure evidence in /workspace/.cloud-setup/user-variable-batch. After repair group the user-variable, migration, prepared-statement and relevant expression/executor tests into one validation phase. Run cargo check --locked -p tidb-session -p tidb-executor -p tidb-expr --all-targets, make lint from repository root, and git diff --check. No Go/Bazel/dependency changes are planned.

## Validation and Acceptance


Tests must prove independent values/types survive migration, SET retains declared metadata, empty-result inline assignment still publishes type without a value, multi-assignment SET reads have pre-execution types, and ordinary DML/prepared/SELECT INTO callers share values. Preserve existing Go-obligation tests; no empty harness or duplicate model is required. Actual hooks/pre-commit must pass cd rust && cargo build --locked -p tidb-server. Repeat that locked build immediately before every push and verify remote SHA.

## Idempotence and Recovery


Preserve concurrent work and never force-push. Keep the validated commit on authentication failure. Reuse build outputs; only verified inactive regenerable executables may be pruned under disk pressure. No native change or dependency synchronization is required. Cloud draft saving is separate from publication and fresh restore.

## Interfaces and Dependencies


Reuse UserVars/UserVarsReader, ColumnResolver, StmtContext and existing physical query planning/execution APIs. Values and declared types are independent: neither implies that the other exists. No new dependency or public wire format is introduced.

## Surprises & Discoveries


Go publishes inline assignment's RHS type during rewriting, even when no row executes. SET NULL removes both maps; inline NULL leaves the value alone. Session migration must not reconstruct types from values.

## Decision Log


- 2026-10-05: repair the shared ownership segment across S03/X01, retaining broad findings as partial until their other obligations are validated. Preserve meaningful tests in existing suites.

## Outcomes & Retrospective


Three original-source regressions fail as expected. The first grouped run exposed missing UserVar dispatch and missing PlanScopeResolver forwarding; the repair now routes both through the shared owner. The duplicate integration test file is consolidated into the existing session suite, preserving its running-total obligation. Integer reads use the Go carrier, decimal reads avoid CAST scale rounding, and timestamp/duration/JSON assignment follows the source string family. Behavioral validation is complete; publication evidence follows in the external final handoff. Other unresolved roots have not been freshly reproduced in this batch.

Final validation: 138 targeted Rust tests, eight real MySQL/unistore checks, all-target checking, make lint, formatting and diff checks pass. Both S03/X01 remain partial; their other obligations remain explicit. Actual hook/pre-push results follow in the external final handoff; no full Go suite, multi-node, performance or fresh restore is claimed.
