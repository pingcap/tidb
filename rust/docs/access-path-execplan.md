# One `AccessPath`, as Go has it

This ExecPlan is a living document. Keep Progress, Surprises & Discoveries,
Decision Log and Outcomes & Retrospective current as work proceeds. It follows
`PLANS.md` at the repository root.

## Purpose / Big Picture

Go's planner carries every candidate way of reading a `DataSource` as one
struct, `util.AccessPath` (`pkg/planner/util/path.go`), held in
`DataSource.PossibleAccessPaths`. A path is born nearly empty during
enumeration (`getPossibleAccessPaths`: store type, index, handle kind,
forced flags) and grows its ranges, conditions and counts in place during
statistics derivation (`deriveStatsByFilter`, `fillIndexPath`,
`deriveTablePathStats`), and index-merge paths are `AccessPath`s too.

The Rust port (`crates/tidb-planner-util/src/path.rs`) splits that one value
into several:

- `PossiblePath` (enum: `Table`, `TiFlashTable`, `Index`) is the newborn path
  (about 250 references).
- `IndexPathState` (+ `FilledIndexPath`) is an index path's grown state,
  kept apart in `DataSource.derived_index_paths`, keyed by index id.
- `FilledTablePath` is the table path's grown state.
- `DerivedAccessPaths` / `DerivedAccessPath` (`Ordinary`, `Union`,
  `IndexMerge`) with `UnionIndexMergePath`, `IndexMergePath` and
  `IndexMergePartial` are the post-derivation collection.
- `ResolvedIndexPaths` carries the forced / keep-order flags as id sets.

After this plan, one `AccessPath` struct with Go's fields replaces all of
them, `DataSource` holds Go's `PossibleAccessPaths: Vec<AccessPath>`, and
every rule reads Go's fields. The observable outcome is unchanged: the
integration corpus gate still reports `157 topics, 3 divergences (3 stale
recordings), 0 refused`, and the planner and executor suites pass.

## Field mapping (Go field <- today's Rust home)

- `Index` <- `PossiblePath::Index { index }`, `PossiblePath::Table {
  primary_index }`, `IndexMergePartial.index`.
- `StoreType` <- `PossiblePath::TiFlashTable` (TiFlash) vs the rest (TiKV).
- `IsIntHandlePath` / `IsCommonHandlePath` <- `PossiblePath::Table {
  is_int_handle }`.
- `FullIdxCols` / `FullIdxColLens` <- `IndexPathState.declared_columns`,
  `FilledIndexPath.full_columns`.
- `IdxCols` / `IdxColLens` <- `FilledIndexPath.columns`,
  `FilledTablePath.common_columns`.
- `Ranges`, `AccessConds`, `EqCondCount`, `EqOrInCondCount` <-
  `FilledIndexPath.detached` / `FilledTablePath.detached`
  (`DetachRangeResult`).
- `CountAfterAccess` / `MinCountAfterAccess` / `MaxCountAfterAccess` <-
  `IndexPathState.row_estimate`, `FilledTablePath` counts, merge counts.
- `CountAfterIndex` <- `FilledIndexPath.count_after_index`.
- `IndexFilters` / `TableFilters` <- `FilledIndexPath`, merge paths.
- `PartialIndexPaths` <- `IndexMergePath.partials`.
- `PartialAlternativeIndexPaths` / `IndexMergeORSourceFilter` /
  `KeepIndexMergeORSourceFilter` <- `UnionIndexMergePath`.
- `IndexMergeIsIntersection` / `IndexMergeAccessMVIndex` <-
  `IndexMergePath` flags.
- `Forced` / `ForceKeepOrder` / `ForceNoKeepOrder` <- `ResolvedIndexPaths`
  id sets.
- `IsSingleScan`, `IsUkShardIndexPath` <- `IndexPathState`.
- `NoncacheableReason` <- `IndexMergePath.noncacheable_reason`.
- Rust-only: `correlated_access_count`, `FilledTablePath.handle_column` /
  `handle_type` (Go reads `ds.HandleCols`); each is checked against Go when
  its consumer moves.

## Plan of Work

1. Define `AccessPath` in `tidb-planner-util` with Go's fields; build it
   from `PossiblePath` at enumeration (`getPossibleAccessPaths`), stored as
   `DataSource.possible_access_paths`, side by side with the old fields.
2. Statistics derivation fills the `AccessPath` in place (Go
   `fillIndexPath` / `deriveTablePathStats`) instead of
   `derived_index_paths` / `derived_access_paths.table_path`.
3. Index-merge generation produces `AccessPath`s with `PartialIndexPaths`
   / `PartialAlternativeIndexPaths`.
4. Move every consumer (logical rules, physical task builders, EXPLAIN,
   plan cache) onto `AccessPath`, one crate at a time, each step gated.
5. Delete `PossiblePath`, `IndexPathState`, `FilledIndexPath`,
   `FilledTablePath`, `DerivedAccessPath(s)`, `UnionIndexMergePath`,
   `IndexMergePath`, `IndexMergePartial`, `ResolvedIndexPaths`.

## Validation and Acceptance

Per step: `cargo check --tests` of touched crates; `cargo test -p
tidb-planner --lib`, `-p tidb-planner --test all`, the logical and
physical operator crates, `-p tidb-executor --lib` / `--test all`; the
corpus gate at each milestone. Build with `CARGO_PROFILE_DEV_DEBUG=0
CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0`.

## Progress

- [ ] 1. `AccessPath` defined and built at enumeration.
- [ ] 2. Statistics derivation fills it.
- [ ] 3. Index merge produces it.
- [ ] 4. Consumers moved.
- [ ] 5. Old types deleted.

## Surprises & Discoveries

(none yet)

## Decision Log

- 2026-11-02: one struct with Go's fields, filled in place, rather than an
  enum per lifecycle stage: Go's rules read and write the same path value
  across enumeration, derivation and physical planning, and a per-stage type
  forces every rule that spans stages to join two structures by index id.

## Outcomes & Retrospective

(none yet)
