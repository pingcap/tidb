# Complete `pkg/planner/util/tablesampler` and wire ordinary TABLESAMPLE execution

This ExecPlan is a living document maintained under `PLANS.md`.

## Purpose / Big Picture

Pinned TiDB commit `e2788410d8d696605e8cb002585877a063ccc909` contains exactly `sample.go` and `BUILD.bazel` in `pkg/planner/util/tablesampler`. The package retains a `TABLESAMPLE` AST node, a cloned full table schema, and the selected partition tables so the physical `TableSample` operator can decode one row from each storage region. Rust parses the clause but currently refuses every method before ordinary physical planning, including TiDB's supported `REGIONS` method.

After this work, the Rust planner will carry the same source-shaped sample metadata into a dedicated physical table-sample leaf and the ordinary physical executor builder will instantiate it. Rust's current in-memory `TableStorage` has no TiKV region cache; this is not a reason to invent region splitting. Go's exact non-TiKV fallback treats the complete physical table (or each physical partition) as one key range, so the matching Rust backend behavior is to return the first record of each selected physical table range. `BERNOULLI` and `SYSTEM` remain rejected, as Go's preprocess phase requires the TiDB `REGIONS` method.

## Progress

- [x] (2026-08-30) Read the complete pinned `tablesampler` package and its Bazel target.
- [x] (2026-08-30) Read the pinned logical builder, best-task conversion, physical operator, executor builder, region sampler, and executor tests that consume the package.
- [x] (2026-08-30) Locate Rust's parsed AST, logical datasource, physical-plan enum, ordinary executor builder, and in-memory `TableStorage` fallback semantics.
- [x] (2026-08-30) Add source-shaped sample metadata and constructor/memory accounting in `tidb-planner`.
- [x] (2026-08-30) Wire supported-method validation, logical metadata, root physical sample planning, explain, clone/cache traversal, and ordinary executor construction.
- [x] (2026-08-30) Restore Go's `isSampling`/zero optimizer-flag behavior and remove the obsolete Rust-only sampled-access-path rejection model.
- [x] (2026-08-30) Replace the refusal test with Go-shaped unsplit, empty, predicate, aggregate, generated/default/handle, partition, and unsupported-method regressions.
- [x] (2026-08-30) Run focused tests, planner/executor checks, all-target clippy, format verification, `make lint`, and self-review.
- [x] (2026-08-30) Prepare the atomic package-parity commit for `hparser-integration`; synchronize and push immediately after creation.

## Surprises & Discoveries

- Observation: Go's `splitIntoMultiRanges` returns one full key range when storage does not implement `tikv.Storage`; only real TiKV storage consults a region cache.
  Evidence: pinned `pkg/executor/sample.go::splitIntoMultiRanges`.
- Observation: Go rejects `BERNOULLI` and `SYSTEM` during preprocessing; only `SampleMethodTypeTiDBRegion` is admitted to planning.
  Evidence: pinned `pkg/planner/core/preprocess.go:416-419` and `preprocess_test.go:280-283`.
- Observation: Rust already parses all three method spellings and has a real TiKV-format sorted table store, but its AST documentation and executor regression deliberately encode unconditional refusal.
  Evidence: `rust/crates/tidb-ast/src/select/table_ref.rs::TableSample` and `rust/crates/tidb-executor/src/driver/tests/select_clauses.rs::a_table_sample_clause_is_refused_rather_than_answered_in_full`.
- Observation: Go disables every logical optimizer rule when a query contains sampling, preserving Selection, Limit, and Sort above the sample leaf.
  Evidence: pinned `pkg/planner/core/planbuilder.go::GetOptFlag`; the Rust predicate regression failed until the same builder state was restored.
- Observation: the previous Rust `TableAccessPath.sampled` bit represented sampling as a rejected table-scan property, which has no Go access-path equivalent and became redundant once `DataSource.sample_info` owned the real branch.
  Evidence: pinned `findBestTask4LogicalDataSource` branches on `SampleInfo` before ordinary path conversion.

## Decision Log

- Decision: implement the Go non-TiKV fallback exactly over Rust's current storage rather than manufacture synthetic region boundaries.
  Rationale: one first record per complete physical table range is the behavior Go itself uses for every storage backend without a TiKV region cache. Adding arbitrary row-count or byte-count regions would be Rust-only policy.
  Date/Author: 2026-08-30 / Codex.
- Decision: add a dedicated physical `TableSample` leaf and build it through the ordinary physical executor switch.
  Rationale: Go never lowers a supported sample clause to an ordinary full scan, and the user's parity requirement explicitly forbids cache-only or syntax-specific execution workarounds.
  Date/Author: 2026-08-30 / Codex.
- Decision: keep the package metadata in `tidb-planner` instead of creating a one-type crate.
  Rationale: its only Rust consumers are the logical and physical planner nodes; `tidb-ast` and `tidb-expr` are already dependencies, and executor depends on planner in the existing direction.
  Date/Author: 2026-08-30 / Codex.
- Decision: open one directed row cursor per physical range and decode only its first record.
  Rationale: Go issues one bounded snapshot scan per range. Materializing a whole table to select its first row would preserve results but violate the implementation and performance behavior being ported.
  Date/Author: 2026-08-30 / Codex.

## Outcomes & Retrospective

Implementation and Ready validation are complete pending commit/push. The live planner now carries the cloned AST/full schema/selected partitions, returns zero logical optimizer flags for sampling queries, constructs only a root `TableSample` leaf, and builds it through the common physical executor switch. The local executor follows Go's non-TiKV one-range fallback and reads only the first key-ordered record per selected physical table. The prior blanket-refusal docs/test and sampled access-path shim were removed. Focused regressions, checks, clippy, formatting, and repository lint passed; clippy reports only the workspace's pre-existing warnings.

## Context and Orientation

`rust/crates/tidb-ast/src/select/table_ref.rs` owns the parsed `TableSample`. `rust/crates/tidb-planner/src/plan_builder.rs::build_data_source` constructs `logical::DataSource`, and `rust/crates/tidb-planner/src/find_best_task/dispatch.rs::find_best_task_4_logical_data_source_without_enforcer` chooses a physical access path. `rust/crates/tidb-planner/src/physical/mod.rs` is the single physical-plan tree used by both fresh and cached execution. `rust/crates/tidb-executor/src/driver/physical_builder.rs::build_with_state` is the common executor constructor. `rust/crates/tidb-executor/src/kv_table.rs::KvTable` stores rows in TiKV key order and exposes the decoding context needed to return the first record.

The package completion unit contains no tests, fixtures, generated files, generation inputs, platform variants, benchmarks, fuzz targets, or examples. Its `BUILD.bazel` declares one public Go library over `sample.go`.

## Plan of Work

Add `tidb_planner::table_sampler::TableSampleInfo` with the cloned AST, cloned full schema, selected physical partition IDs, a nil-equivalent constructor, and source-shaped memory accounting over Rust-owned data. Add `sample_info` to logical `DataSource`; build it only after validating the method is `REGIONS`, using the complete pre-pruning schema and the table's selected partition IDs.

Add `PhysicalTableSample` to the physical enum as a childless root operator with row-count one, cloned sample metadata, physical table ID, and descending flag. In datasource best-task enumeration, a sample clause must consider only the table path, refuse ordered/COP properties exactly as Go does, and return the physical sample directly in a root task. Do not construct an index, point-get, or table-reader alternative.

Add a small `TableSampleExec` that uses the catalog's physical `KvTable`, decodes rows with the statement context, and emits the first row in key order for each selected physical table range. Route it through `physical_builder::build_with_state`, and add explain/memory/cache-tree handling wherever exhaustive physical matches require the new leaf.

Replace the old refusal regression with tests proving: empty table returns no rows; a nonempty unsplit table returns its first key-ordered row; projection, generated/default columns, predicates, aggregates, and limits operate above the sample leaf; `BERNOULLI` and `SYSTEM` return the same invalid-table-sample class rather than scanning; and EXPLAIN contains `TableSample`. Add planner structural tests for constructor cloning, method validation, root-only/no-order admission, and index-path exclusion.

## Concrete Steps

Run from `/Users/qiliu/projects/tidb`:

    cargo test --manifest-path rust/Cargo.toml -p tidb-planner table_sample --no-fail-fast
    cargo test --manifest-path rust/Cargo.toml -p tidb-executor table_sample --no-fail-fast
    cargo check --manifest-path rust/Cargo.toml -p tidb-planner -p tidb-executor
    cargo clippy --manifest-path rust/Cargo.toml -p tidb-planner -p tidb-executor --all-targets
    cargo fmt --manifest-path rust/Cargo.toml --all -- --check
    make lint
    git diff --check

Expected: focused tests pass, checks exit zero, and no touched line adds a clippy diagnostic. Existing workspace warnings may remain.

## Validation and Acceptance

The acceptance query is `SELECT a FROM t TABLESAMPLE REGIONS()` against the current in-memory backend. With rows whose storage-key order begins at `a = 1`, it must return only that first record, matching Go's one-range non-TiKV fallback. The physical plan and EXPLAIN must contain `TableSample`; it must not contain a table scan chosen as a substitute. Unsupported sampling methods must fail before execution.

No Go source, imports, Bazel metadata, or module dependencies are changed, so `make bazel_prepare` is not required.

## Idempotence and Recovery

All inspection, tests, formatting, and checks are safe to rerun. Changes use `apply_patch`. If the physical enum exposes an unanticipated owner, extend that owner's exhaustive leaf handling; do not add a parallel AST executor. Preserve unrelated user changes and rebase normally before pushing.

## Artifacts and Notes

Pinned package inventory:

    pkg/planner/util/tablesampler/BUILD.bazel
    pkg/planner/util/tablesampler/sample.go

Pinned integration references include `pkg/planner/core/logical_plan_builder.go::buildDataSource`, `pkg/planner/core/find_best_task.go::convertToSampleTable`, `pkg/planner/core/operator/physicalop/physical_table_sample.go`, `pkg/executor/builder.go::buildTableSample`, and `pkg/executor/sample.go`.

## Interfaces and Dependencies

The intended planner interface is:

    pub struct TableSampleInfo {
        pub ast_node: tidb_ast::TableSample,
        pub full_schema: tidb_expr::schema::Schema,
        pub partition_ids: Vec<i64>,
    }

    pub fn new_table_sample_info(
        node: Option<&tidb_ast::TableSample>,
        full_schema: &tidb_expr::schema::Schema,
        partition_ids: Vec<i64>,
    ) -> Option<TableSampleInfo>

The physical node remains part of `tidb_planner::physical::PhysicalPlan`; no alternate public execution API is introduced.

Revision note: this initial version records the complete package inventory, pinned consumer behavior, and the chosen exact non-TiKV fallback before implementation.
