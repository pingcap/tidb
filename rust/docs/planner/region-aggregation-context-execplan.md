# Preserve Go aggregate descriptors through region execution

This living ExecPlan follows `PLANS.md` at the repository root.

## Purpose / Big Picture

Region aggregation must evaluate the same typed arguments, modes and comparisons
as Go's `pkg/expression/aggregation.NewDistAggFunc`. COUNT over partial counts
must sum counts, computed MIN/MAX must retain the argument collation, and numeric
and temporal extrema must not fail after the second row. The region loop should
own groups; the expression aggregation layer should own aggregate semantics.

The user asked to find more mismatches, then instructed “follow go” and
“continue”. This authorizes implementation of the reproduced boundary defects.
The owning Go package remains the atomic inventory/completion unit. This work
is integration repair and seed evidence, not a transcreated-package claim.
Unsupported factory paths and descriptor/planner paths remain explicit.

## Progress

- [x] Fetch master and integration branch; verify relevant Go archive sources match.
- [x] Capture 1,932 aggregate cases; baseline has 513 result/warning-count differences.
- [x] Refine the regression domain to valid partial-count inputs and typed rows.
- [x] Add failing regression fixture and package inventory.
- [x] Introduce typed aggregate descriptors/states and integrate region execution.
- [x] Preserve aggregate mode in generated protobuf bindings from source schema.
- [x] Run focused tests and required lint.
- [x] Run locked server builds in the commit hook and immediately before push.
- [x] Commit and push 17ad39d246 to hparser-integration; report remaining package gaps.
- [x] Incorporate remote 5c875eae66 before publication (unrelated executor table-scan change).

## Surprises & Discoveries

The local Expr schema omits aggFuncMode, so valid FinalMode/Partial2Mode COUNT
requests silently count input rows. Region extrema inspect a restricted list
of Datum variants and reject real/time/JSON pairs. Their collation is recovered
only from direct scan columns, so MAX(LOWER(column)) loses its CI collation.
Direct SimpleExpr columns bypass typed row extraction, losing duration FSP.
The initial oracle constructed a Decimal from the marker NULL before replacing
it with a null leaf; the harness was corrected before collecting the baseline.

## Decision Log

- Decision: Use decoded expression FieldTypes and the shared Datum conversion and
  comparison APIs, with per-request context, instead of expanding RegionAggKind
  and its ad hoc runtime type switch.
  Rationale: Runtime variants alone cannot preserve declared collation, modes,
  unsigned carriers or temporal precision. This follows Go's factory/state split.
  Date/Author: 2026-09-29, Codex.
- Decision: Keep grouping/output ordering outside this change unless required for
  integration; do not remove Rust's collation-aware group keys merely because
  Go's legacy mock hash grouping has separate limitations.
  Rationale: Preserve SQL correctness and bound the independently tested change.
  Date/Author: 2026-09-29, Codex.

## Context and Orientation

`rust/crates/tidb-unistore/src/cophandler.rs` currently implements both group
management and aggregate semantics through RegionAggregator/RegionSum.
`rust/crates/tidb-expr/src/distsql_builtin.rs` supplies typed PB expression trees.
`rust/crates/tidb-datatype` supplies context-aware conversion/comparison and
bounded decimal arithmetic. `rust/crates/tidb-proto/proto/select.proto` is the
source input compiled by build.rs; generated outputs must not be edited.
Go reference is master 12b639a116; the execution archive is 8936d7bdcb and has no
diff for expression/aggregation, types, collate or the Go cophandler sources.

## Plan of Work

First save the package inventory and differential fixture under rust/docs/planner
and add a regression replay in the existing cophandler tests. Exclude malformed
partial-count input domains from parity assertions, documenting those exclusions.
Then add the shared DistAggregate descriptor/state lifecycle to tidb-expr with
build/update/partial-result methods. Decode and retain expression metadata and
aggregate mode. RegionAggregator retains group lookup but delegates argument
evaluation and state updates, using one typed row view for each scanned row.
Generate aggregate-mode bindings by editing select.proto and building normally.
Preserve explicit errors for unimplemented or invalid upstream shapes.

## Milestones

The first milestone is failing regression evidence with the old runtime. The
second is a descriptor/state implementation wired into real region aggregation,
with the same regression passing. The third is validation and publication,
including review of changes to public/generated protobuf structures.

## Concrete Steps

From the repository root, run the temporary oracle replay while capturing the
baseline, then run the permanent regression and targeted crate checks:

    cargo test --locked -p tidb-unistore --manifest-path rust/Cargo.toml --lib region_aggregate -- --test-threads=1
    cargo test --locked -p tidb-expr --manifest-path rust/Cargo.toml --lib aggregation -- --test-threads=1
    python3 rust/scripts/check-tipb-proto-projection.py
    make lint
    cd rust && cargo build --locked -p tidb-server

The Go oracle uses the repository failpoint wrapper in the external reference
archive. Runners restore temporary source probes in finally blocks. Do not run
concurrent writers to probed source files. No Go imports/files or Bazel targets
in this repository are changed, so bazel_prepare is not triggered by this plan.

## Validation and Acceptance

A valid COUNT partial input [2,3] yields 5 in Final/Partial2 mode and 2 in
Complete/Partial1 mode. MAX over doubles [1.25,2.5] yields 2.5. MAX(LOWER(column))
for á,z under utf8mb4_general_ci yields z. Typed temporal values retain source
precision. Test all preserved fixture cases and record unsupported capabilities
separately. New tests must fail before implementation and pass after it.
Build success alone does not establish parity or throughput improvement.

## Idempotence and Recovery

All data collection is repeatable and offline against pinned captures. Temporary
probes are restored after use. Keep edits minimal and revert only this task's
changes if a design proves invalid; preserve unrelated work. Rebase rather than
force-push if the shared branch advances.

## Artifacts and Notes

Temporary oracle/probe files use the region-aggregate prefix in /private/tmp.
Permanent evidence will include exact versions, all package source/support
identities, compressed wire/result rows, validation logs and a comparison script.

## Interfaces and Dependencies

The shared descriptor accepts tipb::Expr plus input FieldTypes and timezone,
retains typed argument Expressions, and produces per-group states. Updates take
the existing Columns context and a typed chunk Row; partial results are Datums.
No new external dependency is needed. COUNT must support its Go multi-argument
NULL semantics and partial-count modes. Each signature retains its own argument
order; there must be no universal eager child evaluation loop.

## Outcomes & Retrospective

The initial 1,764-case valid regression failed with 387 differences. The final
expanded 3,129-case regression passes, including result bits, error code/message,
warning order and lazy argument evaluation. Shared conversion checks passed 67
tests, expression aggregation passed 86 (18 existing ignored), wire tests passed
4 and producer integration passed 8. Unistore passed 189 (13 existing ignored)
when excluding one selection failure reproduced on untouched b415a65bd8.
`make lint` passed. See `region-aggregation-context-20260929/validation.txt` for
exact commands and the complete inventory/evidence directory for exclusions.

The typed distributed evaluator reuses the existing canonical AggFuncDesc, and
the region loop reuses a typed row buffer. Expanded cases also required sharing
source-only ToFloat64 conversion and preserving StrToInt's parse-error precedence.
Go's GROUP_CONCAT factory path, local aggregate lifecycle, DISTINCT, computed
group keys and remaining package tests are not completed. No benchmark gain or
full Go package parity is claimed. Commit-hook and pre-push builds are the final
publication steps; their outcome belongs to the commit/task delivery record.
