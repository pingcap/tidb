# Complete the pinned planner cost package

This ExecPlan is a living document maintained under repository `PLANS.md`. The pinned Go revision is `e2788410d8d696605e8cb002585877a063ccc909`.

## Purpose / Big Picture

`pkg/planner/core/cost` owns the planner's shared selectivity, distinct, floating-point tolerance, aggregation-function factors, and small-scan threshold. Rust already translated the values, but logical selection retained a duplicate of this package's selection constant. Completing the package means one owner for that source value and source-named aggregation keys.

## Progress

- [x] (2026-08-30) Inventoried and read the complete pinned package: `factors_thresholds.go` and `BUILD.bazel`; no tests, fixtures, generated/platform variants, benchmarks, fuzz targets, or examples.
- [x] (2026-08-30) Compared every constant and every aggregation-map entry with `tidb-planner::cost_factors`.
- [x] (2026-08-30) Removed the duplicate logical-selection constant definition and used the translated Go aggregate-name catalog as the map keys.
- [x] (2026-08-30) Ran WIP and Ready validation and recorded the atomic package receipt.

## Surprises & Discoveries

- Rust already had every pinned value and a branch-complete source-backed test, so this package needs consolidation rather than a second implementation.
- `SmallScanThreshold` and aggregation weighting are consumed by Go's version-one physical cost methods. Their full production integration belongs to the atomic `physicalop`/version-one-cost package; this package supplies exactly the shared values those consumers read.

## Decision Log

- Keep `logical::selection::SELECTION_FACTOR` as a public re-export, not a definition. This preserves native call sites while making `cost_factors` the single owner matching Go's package boundary. Leave `cardinality::derive_stats::DISTINCT_FACTOR` alone: it translates Go's separate unexported `cardinality.distinctFactor`, despite having the same numeric value.
- Use `tidb_expr::aggregation::names`, the existing translation of Go `ast.AggFunc*`, instead of repeating literal function names in the cost map.

## Validation

Run from `rust/`:

    cargo test --locked -p tidb-planner --lib cost_factors -- --nocapture
    cargo check --locked -p tidb-executor
    cargo check --locked -p tidb-server
    cargo fmt --all -- --check

Run `make lint` and `git diff --check` from the repository root for Ready.

## Outcomes & Retrospective

The package now has one Rust owner for all pinned constants and aggregation factors. Logical selection retains a compatibility re-export instead of a competing definition, and aggregation factors use the translated Go `AggFunc*` name catalog.

Atomic receipt: pinned inventory `factors_thresholds.go` plus `BUILD.bazel`; no original tests/support/fixtures/generated/platform variants. Native production owner `tidb-planner::cost_factors`; exact source-backed tests `cost_factors_source.rs`. The aggregate test target was repaired to use the current Go-shaped `ByItems` type in its pre-existing physical-sort memory test, then both cost tests and that repaired test passed. Executor/server consumer checks, formatting, Ready `make lint`, and diff checks passed.
