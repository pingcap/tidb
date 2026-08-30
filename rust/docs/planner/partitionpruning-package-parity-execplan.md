# Complete the pinned planner partitionpruning package

This ExecPlan is a living document maintained under repository `PLANS.md`. The pinned Go revision is `e2788410d8d696605e8cb002585877a063ccc909`.

## Purpose / Big Picture

`pkg/planner/util/partitionpruning` is the common entry point that dispatches HASH/KEY, RANGE, and LIST partition pruning and then makes online DROP PARTITION results safe to read. Rust integrates the same behavior across `tidb-planner`, `tidb-executor`, and `tidb-model`, but two full-range fallback branches bypass explicit partition-name filtering and dropping-partition remapping. Completion makes every fallback pass through the same source behavior.

## Progress

- [x] (2026-08-30) Inventoried and read the complete pinned package: `partition_prune.go` and `BUILD.bazel`; no package-local tests, fixtures, generated/platform variants, benchmarks, fuzz targets, or examples.
- [x] (2026-08-30) Audited the integrated Rust dispatch, range/list/hash/key pruners, model overlap mapping, and static-partition planner adapter.
- [x] (2026-08-30) Centralized the integrated adapter and fixed both full-range fallback branches.
- [x] (2026-08-30) Added a regression test proving explicit partition names survive an unresolvable pruning dependency.
- [x] (2026-08-30) Ran WIP and Ready validation and recorded the atomic package receipt.

## Surprises & Discoveries

- Rust deliberately represents Go's `FullRange` sentinel as the concrete definition ordinals required by its static partition child builder. This is a native representation difference, not a pruning-policy difference.
- The normal, LIST COLUMNS, and empty-condition paths call `remap_partition_indices`, but missing dependency columns and ranger-detachment refusal return raw definition ordinals. That can scan partitions excluded by `PARTITION (...)` and can retain a dropping definition with no readable overlap.
- Partition names are stored as original strings in Rust. Go compares `ast.CIStr.L`, so Rust must use its existing Go-compatible `CiString`, not ASCII-only comparison.

## Decision Log

- Decision: Extract the catalog-independent portion of the live planner adapter into one function and test it directly.
  Rationale: This proves the actual fallback branch rather than merely retesting the remap helper in isolation.
  Date/Author: 2026-08-30 / Codex
- Decision: Keep concrete all-partition ordinals instead of introducing Go's `FullRange` integer sentinel.
  Rationale: The next Rust stage immediately enumerates physical children; the concrete set has identical planner behavior and avoids a second sentinel path.
  Date/Author: 2026-08-30 / Codex

## Outcomes & Retrospective

The live planner now sends every Go full-range outcome through one remapping path, including unresolved dependency columns and ranger detachment refusal. Explicit partition names and online-drop overlap mapping therefore apply consistently. Name matching now uses the existing Go-compatible `CiString`. The focused regression, all 20 executor partition-pruning tests, both logical partition-processor tests, formatting, the server check, `make lint`, and `git diff --check` pass. This is the atomic completion receipt for pinned Go package `pkg/planner/util/partitionpruning`.

## Context and Orientation

`rust/crates/tidb-planner/src/logical/rule_partition_processor.rs` owns the tree rewrite. `rust/crates/tidb-executor/src/driver/planner_bridge.rs` owns catalog-backed pruning and converts surviving definitions to planner ordinals. `rust/crates/tidb-executor/src/partition_pruning.rs` owns HASH/KEY/RANGE/LIST calculations. `rust/crates/tidb-model/src/partition.rs` and executor `PartitionSpec` capture online-drop overlap mapping.

## Plan of Work

Extract the live adapter body into `partition_indices_for_spec`, route every full-range return through `remap_partition_indices`, and use `CiString` for partition/dependency names. Add a direct regression test with a non-empty predicate, an unresolved dependency, and an explicit partition name; the correct result is only that named definition.

## Concrete Steps

From `rust/`, run:

    cargo test --locked -p tidb-executor --lib partition_pruning_fallback -- --nocapture
    cargo test --locked -p tidb-executor --lib partition_pruning -- --nocapture
    cargo test --locked -p tidb-planner --lib logical::rule_partition_processor -- --nocapture
    cargo check --locked -p tidb-server
    cargo fmt --all -- --check

From the repository root, run:

    make lint
    git diff --check

## Validation and Acceptance

The regression must return only the explicitly named partition even though its dependency cannot be resolved. Existing partition pruning and logical partition processor tests must remain green, and the server consumer must compile.

## Idempotence and Recovery

All commands are safe to rerun. No generated files or external state are changed.

## Artifacts and Notes

Atomic pinned inventory: `pkg/planner/util/partitionpruning/partition_prune.go` and `BUILD.bazel`.

## Interfaces and Dependencies

The integrated package depends on planner logical data sources, executor partition specifications and ranger detachment, model overlap metadata, expression evaluation, and statement context. The public planner seam remains `PartitionPruning::partition_indices`.
