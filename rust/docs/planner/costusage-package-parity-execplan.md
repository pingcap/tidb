# Complete the pinned planner costusage package

This ExecPlan is a living document maintained under repository `PLANS.md`. The pinned Go revision is `e2788410d8d696605e8cb002585877a063ccc909`.

## Purpose / Big Picture

`pkg/planner/util/costusage` owns cost-model flags, version-two cost values, optional cost traces, arithmetic over traced costs, and plan-cost options. Rust already has a translation, but it exposes a Rust-only fixed-cost constructor and differs in option mutation and floating-point formula spelling. Completion means callers observe the pinned Go behavior without an additional integration wrapper.

## Progress

- [x] (2026-08-30) Inventoried and read the complete pinned package: `cost_misc.go` and `BUILD.bazel`; no original tests, fixtures, generated/platform variants, benchmarks, fuzz targets, or examples.
- [x] (2026-08-30) Removed Rust-only package APIs and tests and corrected source behavior.
- [x] (2026-08-30) Ran WIP and Ready validation and recorded the atomic package receipt.

## Surprises & Discoveries

- Rust's `fixed_cost_ver2` has no callers and no pinned Go equivalent.
- `CostVer2Factor.String` uses Go `%v`, equivalent to `strconv.FormatFloat` with the general shortest format. Rust's ordinary `Display` chooses different notation for large values, while `tidb-datatype` already owns the compatible formatter.
- Go's `PlanCostOption.WithCostFlag` mutates the receiver. Rust's consuming builder returned a modified copy, which differs when the original option is subsequently observed.

## Decision Log

- Decision: Preserve native Rust names where they only translate exported Go fields or getters, but remove APIs with behavior absent from Go.
  Rationale: Field privacy is a Rust representation choice; a fixed-cost injection path is a separate behavior.
  Date/Author: 2026-08-30 / Codex
- Decision: Remove the package-local Rust unit module because the pinned Go package has no original tests; validate through the existing source-backed `pkg/planner/core/plan_cost_ver2` tests.
  Rationale: The user requires tests as well as production code to follow the pinned package inventory.
  Date/Author: 2026-08-30 / Codex

## Outcomes & Retrospective

The Rust package now follows every production behavior in the pinned Go leaf. Cost factors use Go's general shortest float spelling, traced division and multiplication preserve Go's fixed float spelling including special values, plan-cost flags mutate the existing option, trace factor costs use an unordered map, and the predefined zero is a value instead of a helper function. The unused fixed-cost injection API, raw-cost getter, boundary documentation, and package-local tests absent from Go were removed.

Atomic receipt: pinned inventory `cost_misc.go` plus `BUILD.bazel`; no original tests/support/fixtures/generated/platform variants. Native production owner `tidb-planner::cost_usage`; source-backed behavioral coverage comes from the translated `pkg/planner/core/plan_cost_ver2` tests. All 35 targeted cost tests passed, executor/server consumers compiled, formatting passed, Ready `make lint` passed, and diff checks passed.

## Context and Orientation

The Rust translation is `rust/crates/tidb-planner/src/cost_usage.rs`. Its production consumers are `rust/crates/tidb-planner/src/plan_cost_ver2.rs`, `rust/crates/tidb-planner/src/find_best_task/coster.rs`, and physical-plan cost entry points. Go's `CostTrace.factorCosts` is a map from factor name to accumulated cost; formulas retain function argument order even though map iteration order is unspecified.

## Plan of Work

Change the trace map to Rust's native unordered map, format factors with `tidb_datatype::format_float_g_shortest`, make `PlanCostOption::with_cost_flag` mutate its receiver, expose the zero cost as a constant value, and remove `fixed_cost_ver2`, `raw_value`, and the package-local test module. Update affected source-backed consumer tests to use the mutating option method.

## Concrete Steps

From `rust/`, run:

    cargo test --locked -p tidb-planner --lib plan_cost_ver2 -- --nocapture
    cargo check --locked -p tidb-executor
    cargo check --locked -p tidb-server
    cargo fmt --all -- --check

From the repository root, run:

    make lint
    git diff --check

All commands succeeded. The first sandboxed `make lint` attempt could not resolve `proxy.golang.org`; rerunning the same Ready gate with network permission downloaded the pinned tool and passed.

## Validation and Acceptance

The source-backed version-two cost tests must retain Go formula strings and values. Compilation of executor and server proves the public cost API has no stale consumers. Formatting, lint, and diff checks form the Ready gate.

## Idempotence and Recovery

All validation commands are safe to rerun. Edits are isolated to Rust sources and this plan; no generated files or external state are involved.

## Artifacts and Notes

The atomic source inventory is exactly `pkg/planner/util/costusage/cost_misc.go` and `pkg/planner/util/costusage/BUILD.bazel` at the pinned revision.

## Interfaces and Dependencies

`tidb-planner::cost_usage` depends on the already-declared `tidb-datatype` crate only for Go-compatible general float formatting. It supplies `CostVer2`, `CostTrace`, `CostVer2Factor`, `PlanCostOption`, flags, constructors, and arithmetic to the planner's version-two cost implementation.
