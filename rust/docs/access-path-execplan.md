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

1. Define `AccessPath` in `tidb-planner-util` with Go's enumeration fields
   (`Index`, `StoreType`, `IsIntHandlePath`, `IsCommonHandlePath`) and Go's
   predicates (`IsTablePath`, `IsTiKVTablePath`, `IsTiFlashSimpleTablePath`,
   `IsUndetermined`); it replaces `PossiblePath` in place, since both are the
   newborn path. Later steps add the derived fields to the same struct.
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

- [x] 1. `AccessPath` defined and built at enumeration (2026-11-02):
  `PossiblePath` is gone; `genTiFlashPath` now fills the handle kind and the
  PRIMARY index as Go does; the hint tail ports `getPathByIndexName`,
  `isTiKVIndexByName`, `removeIgnoredPaths`, `removeGlobalIndexPaths` and
  `IsUndetermined`; the isolation filter and `prefer_tikv` key on
  `StoreType`.
- [ ] 2. Statistics derivation fills it.
  - [x] 2a (2026-11-02): an index path's state lives on its `AccessPath`
    (`declared_columns` = Go `FullIdxCols`/`FullIdxColLens`,
    `is_uk_shard_index_path`, `is_single_scan`, `row_estimate` = Go
    `CountAfterAccess` and bounds, `filled`); `IndexPathState` and the
    id-keyed `DataSource.derived_index_paths` are gone, and readers find the
    live path with `DataSource::path_by_index_id`.
  - [x] 2b (2026-11-02): the TiKV table path carries its
    `deriveTablePathStats` result (then `filled_table`, flattened in 2c-4);
    `DerivedAccessPaths.table_path` is gone. Go also fills the TiFlash table
    path; Rust's TiFlash readers still read the TiKV path's result.
  - [x] 2d (2026-11-02): Go's `Forced`, `ForceKeepOrder`,
    `ForceNoKeepOrder` and `IndexLookUpPushDownBy` are fields of the path.
    Hint resolution sets them on a working copy of `publicPaths` and
    `available` lists positions into it, as Go's pointers share one path;
    the id sets on `DataSource` and `ResolvedIndexPaths` are gone.
    `USE INDEX ()` now marks the table path `Forced` and `SetIndexForce`
    follows `getGeneralAttributesFromPaths` (any forced path), as in Go.
  - [x] 2c-1 (2026-11-02): `FullIdxCols` is one path field
    (`full_idx_cols`, was `declared_columns` + `FilledIndexPath.full_columns`),
    replaced by the fill as Go's `fillIndexPath` does.
  - [x] 2c-2 (2026-11-02): an index path's `IdxCols` / `IdxColLens` are a
    path field (`idx_cols`); the merge helpers take the `AccessPath`.
  - [x] 2c-3 (2026-11-02): an index path's fill is Go's fields on the path
    (`ranges`, `access_conds`, `eq_cond_count`, `eq_or_in_cond_count`,
    `is_dnf_cond`, `min_access_conds_for_dnf_cond`, `index_filters`,
    `table_filters` -- one field with the merges' -- and `count_after_index`);
    `fill_index_path` fills the `AccessPath` in place as Go's `fillIndexPath`
    does, and `FilledIndexPath` is gone. A path derivation filled carries its
    `CountAfterIndex` (Go leaves 0 on a path it never fills: Rust still skips
    multi-valued indexes there). A point get's residual is `IndexFilters`
    then `TableFilters`, as Go appends them.
  - [x] 2c-4 (2026-11-02): the table path's fill is the same fields
    (`idx_cols` = a common handle's `IdxCols`, the ranges and conditions,
    `table_filters`, and `row_estimate` = `CountAfterAccess` and bounds);
    `fill_table_path` / `detach_table_path` fill the `AccessPath` in place,
    the handle column and type are read from the `DataSource` where used
    (Go `GetPKIsHandleCol`), and `FilledTablePath` and
    `DataSource::filled_table_path` are gone. Merge partials tell a table
    partial by `IsTablePath`, as Go does.
- [ ] 3. Index merge produces it.
  - [x] 3a (2026-11-02): a finished merge's partials are `AccessPath`s (Go
    `PartialIndexPaths`); `IndexMergePartial` is gone. Go's per-path
    `NoncacheableReason` replaces `DataSource.partial_index_noncacheable_ids`.
  - [x] 3b (2026-11-02): the union alternatives are Go's
    `PartialAlternativeIndexPaths` (`[][][]*AccessPath`: a table or index
    alternative is one path, a multi-valued one a path per value, and
    `KeepIndexMergeORSourceFilter` sits on the first path); the `Partial`
    enum is gone.
  - [x] 3c (2026-11-02): union and finished merges are `AccessPath`s with
    Go's `PartialAlternativeIndexPaths` / `PartialIndexPaths`,
    `IndexMergeORSourceFilter`, `TableFilters`, `IndexMergeIsIntersection`
    and `IndexMergeAccessMVIndex`; `UnionIndexMergePath`, `IndexMergePath`
    and the `DerivedAccessPath` enum are gone. The union's plan-cache check
    reads the statement flag at physical time (Go `UseCache()`), not a copy.
- [x] 3d-1 (2026-11-02): derivation writes Go's
  `DataSource.AccessPathMinSelectivity`; `DerivedAccessPaths.min_selectivity`
  (a second copy every reader had to choose between) is gone.
- [x] 3d-2 (2026-11-02): `apply_table_index_hints` returns Go's `available`
  paths and appends its warnings through a sink in Go's order (Go
  `StmtCtx.AppendWarning`); `ResolvedIndexPaths` is gone.
  `DataSource.enumerated_paths` takes Go's name, `all_possible_access_paths`.
- [x] 3d (2026-11-02): `DerivedAccessPaths` is gone. `DataSource` holds Go's
  `PossibleAccessPaths` after derivation (`possible_access_paths`, None until
  derived) beside `all_possible_access_paths`; derivation returns
  `generateIndexMergePath`'s `maxRowCount` for the caller to cap the rows.
  2026-11-02: the lists share paths as Go's pointers do --
  `possible_access_paths` entries are `PossibleAccessPath::Shared(position)`
  into `all_possible_access_paths` or an `IndexMerge` path, resolved by
  `DataSource::possible_paths`.
- [ ] 4. Consumers moved.
- [ ] 5. Old types deleted.

## Surprises & Discoveries

- Go's TiFlash path is a table path (`IsTablePath` includes
  `Index != nil && StoreType == TiFlash`) and, on a clustered table, carries
  the PRIMARY index. Rust's `TiFlashTable` carried nothing, so every rule
  keyed by index id skipped it; Go's rules that walk `PossibleAccessPaths`
  without a store check (`checkColCanUseIndex`, `Selectivity`'s
  `filledPaths`, the join-elimination unique-key check) see it as a second
  table path. Those sites now follow Go.
- Go builds a columnar (vector) index path as a TiFlash table path with
  `Index` set; Rust still skips columnar indexes at enumeration.
- Go's `deriveStats4DataSource` fills every non-table path, multi-valued
  index paths included (`fillIndexPath`, then `deriveIndexPathStats` in the
  heuristics loop, whose `len(path.Ranges) == 0` rule can select it, and
  `getGeneralAttributesFromPaths` reads its `CountAfterIndex`). Rust's
  ordinary fill skipped multi-valued indexes until 2026-11-02; it now
  fills them as Go does (the physical conversion still reads them only
  through index merge, as Go's `convertToIndexScan` does).
- The physical task builders (`dispatch.rs`, `candidate_preparation.rs`)
  keep their three-way TiKV-table / TiFlash / index split for now; step 4
  moves them onto Go's per-path fields.

- `crates/tidb-planner/src/plan_builder/tests.rs` had not been a module
  since `431d637dec5` dropped its `mod tests;`. It is declared again
  (2026-11-02): stale API uses updated; the unknown-database test now
  expects Go's `Table 'nosuchdb.t' doesn't exist` (checked with gorun); the
  unported-clause test was removed because the clause is ported.

## Decision Log

- 2026-11-02: one struct with Go's fields, filled in place, rather than an
  enum per lifecycle stage: Go's rules read and write the same path value
  across enumeration, derivation and physical planning, and a per-stage type
  forces every rule that spans stages to join two structures by index id.

## Outcomes & Retrospective

(none yet)
