# Complete the pinned planner access-object package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. The pinned Go revision is `e2788410d8d696605e8cb002585877a063ccc909`.

## Purpose / Big Picture

The pinned `pkg/planner/core/access` package is the single typed owner for table, index, partition, CTE, and other access descriptions used by textual and binary EXPLAIN. Rust currently hand-assembles a subset of those strings in the executor and only decodes the protobuf forms. Completing the package removes that duplicate producer and makes normalized, display, and protobuf output follow the same values.

## Progress

- [x] (2026-08-30) Read and inventoried the complete pinned package: production `access_obj.go` and `BUILD.bazel`; no tests, fixtures, generated artifacts, build/platform variants, benchmarks, fuzz targets, or examples.
- [x] (2026-08-30) Added one native Rust owner for every production type and method with branch-complete tests.
- [x] (2026-08-30) Replaced executor scan/index/point/CTE string assembly with typed objects retained by the explain tree; table database names now come from the owning catalog schema.
- [x] (2026-08-30) Audited every pinned dynamic-partition call site and recorded its owning-package integration boundary.
- [x] (2026-08-30) Ran the Ready validation gate and recorded the atomic package receipt.

## Surprises & Discoveries

- Observation: Rust already owns matching `tipb` protobuf messages and a complete decoder in `tidb-util::plancodec`, but no producer constructs those access objects.
  Evidence: `rust/crates/tidb-proto/proto/explain.proto` defines all four messages; `rust/crates/tidb-util/src/plancodec.rs` prints them; `rust/crates/tidb-executor/src/explain.rs` returns only hand-built strings.
- Observation: Go's dynamic-partition objects are produced by reader methods in `pkg/planner/core/operator/physicalop`, not by the access package, and depend on `PhysPlanPartInfo`, statement-context pruning mode, InfoSchema, and a second pruning pass.
  Evidence: pinned `physical_table_reader.go`, `physical_index_reader.go`, `physical_indexlookup_reader.go`, `physical_indexmerge_reader.go`, and `physical_utils.go`. Rust readers explicitly omit `PhysPlanPartInfo`, so inventing dynamic objects from static scan strings would change behavior.
- Observation: the aggregate planner integration-test target currently fails before the explain test runs because `physicalop_memory_trace_clone_stream_count_source.rs:47` passes `SortItem` where the current physical plan requires `ByItems`.
  Evidence: `cargo test --locked -p tidb-planner --test all plan_tree_renderer_leaf -- --nocapture`; this file was unchanged by this package.

## Decision Log

- Decision: Map the Go package to `tidb-planner::access` and use a closed `AccessObject` enum at Rust call sites.
  Rationale: Go uses a small interface implemented only by the types in this package. A closed enum preserves that ownership, supports exhaustive dispatch, and avoids a second string-only representation.
  Date/Author: 2026-08-30 / Codex
- Decision: Retain `Option<AccessObject>` in `ExplainOperator` rather than rendering at executor construction time.
  Rationale: Go retains the interface value until its ordinary row, normalized, or protobuf consumer chooses the appropriate method. Keeping a Rust string here would preserve the legacy split pipeline the parity work is removing.
  Date/Author: 2026-08-30 / Codex
- Decision: Do not synthesize dynamic-reader access objects in this package.
  Rationale: the pinned behavior belongs to the complete `physicalop` package and requires source pruning facts Rust does not yet retain. The access package now supplies its exact value type; the reader integration must arrive with that owning package and its tests.
  Date/Author: 2026-08-30 / Codex

## Outcomes & Retrospective

The pinned access package's complete production surface is implemented: all value types, exact display and normalized branches, and protobuf population including empty and error-placeholder behavior. Existing scan/index/point/CTE explain producers now construct those values, and the explain tree retains the typed object instead of a cache- or renderer-specific string. Dynamic reader production remains an explicit `physicalop` package dependency rather than a partial access-package claim.

Atomic receipt: pinned inventory `access_obj.go` plus `BUILD.bazel`; no original tests/support/fixtures/generated/platform variants. Native production owner `tidb-planner::access`; consumers `tidb-planner::explain` and `tidb-executor::explain`; protobuf dependency `tidb-proto`. WIP checks passed for the access unit branch set and executor/server consumers. Ready `make lint`, formatting, and diff checks passed. The aggregate planner test target remains unavailable because of the unrelated pre-existing `SortItem`/`ByItems` compile error recorded above.

## Context and Orientation

Pinned `access_obj.go` defines `ScanAccessObject`, `IndexAccess`, `OtherAccessObject`, `DynamicPartitionAccessObject`, and the list `DynamicPartitionAccessObjects`. Their strings feed ordinary EXPLAIN, normalized strings feed plan normalization, and `SetIntoPB` populates `tipb.ExplainOperator.AccessObjects`. Rust's current physical explain producer is `rust/crates/tidb-executor/src/explain.rs`; protobuf definitions are generated by `tidb-proto` from `rust/crates/tidb-proto/proto/explain.proto`.

## Plan of Work

Add `rust/crates/tidb-planner/src/access.rs`, export it, and add the existing `tidb-proto` workspace dependency. Implement exact display and normalized formatting plus protobuf population, including nil/empty equivalents and error placeholders. Refactor the executor's table/index/point/CTE access helpers to construct these types and render through the shared owner. Audit reader-level dynamic partition metadata and integrate it only where the physical plan carries the same source facts.

## Concrete Steps

Run from `rust/` unless stated otherwise:

    cargo test --locked -p tidb-planner --lib access -- --nocapture
    cargo check --locked -p tidb-executor
    cargo check --locked -p tidb-server
    cargo fmt --all -- --check

Run `make lint` from repository root for the Ready gate, followed by `git diff --check`.

## Validation and Acceptance

Acceptance requires exact punctuation and spacing for empty/table/partition/ordinary-index/clustered-index cases, normalized partition elision, empty-other protobuf refusal, dynamic all/dual/error/multiple-table formatting, error-slot protobuf placeholders, and a production explain path that uses the shared owner.

## Idempotence and Recovery

All edits and checks are safe to repeat. If protobuf output differs, inspect generated field and oneof names rather than adding an adapter format. If a dynamic reader lacks the source facts Go reads, record and fill that owning physical-plan gap; do not infer partitions from display strings.

## Milestones

Milestone one implements and tests the complete package surface. Milestone two removes the executor's duplicate string producer. Milestone three validates all consuming crates and records the package receipt.

## Interfaces and Dependencies

`tidb-planner::access` will expose the four source value types plus an `AccessObject` enum, `Display`, `normalized_string`, and `set_into_pb(&mut tidb_proto::tipb::ExplainOperator)`. `tidb-proto` is the native equivalent of Go's direct `go-tipb` dependency.
