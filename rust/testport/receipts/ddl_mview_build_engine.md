# Batch 24 — MV initial-build data-movement engine (pure tier)

## Go reference (pinned master `94a9cbedab`)

* `pkg/ddl/mview_worker.go` — `onCreateMaterializedView` case
  `model.StateWriteReorganization`:
  * `hasCreateMaterializedViewBuildRows` probe + `ErrInvalidDDLJob`
    "create materialized view: detected residual build rows on retry";
  * `runReorgJob(buildCreateMaterializedViewData)` →
    `buildCreateMaterializedViewDataByInsert` (`buildCreateMaterializedViewInsertSQL`
    = `REPLACE INTO s.v <SQLContent>`) on non-TiKV stores,
    `...ByImport` on TiKV;
  * `job.SnapshotVer == 0` refusal "invalid build read tso";
  * `upsertCreateMaterializedViewRefreshInfo`, `InitBuildState = Ready`,
    `updateSchemaVersion`, `FinishMultipleTableJob(Done, Public, [bases…, view])`;
  * build error ⇒ `job.State = Rollingback` (non-terminal; next tick runs
    `rollbackCreateMaterializedView`).

## Rust deliverables

* `rust/crates/tidb-exec/src/mview_build_engine.rs` (new):
  * `derive_materialized_view_build(snapshot, schema_name, schema_id, view,
    bases, start_ts) -> MviewBuildPlan { read_ts, row_count, mutations }`;
  * residual probe over the view's record range (`scan_range`), Go's
    ErrInvalidDDLJob text on refusal;
  * base rows pre-loaded from `snapshot.scan_range` of each base's record
    range into `MemTableStorage`; the handle layout (`pk_is_handle` /
    common handle offsets) is wired so key-carried columns decode;
  * definition SELECT `SELECT * FROM (<SQLContent>) AS tidb_mv_query` via
    `run_select_meta_in`, context carrying the definition's persisted SQL
    mode (strictness = Go `ModeStrict{Trans,All}Tables` bits);
  * per output row: positional column-id mapping onto the view's columns,
    then `store_clustered_row` (clustered views) or `insert_row` under
    sequentially allocated row ids from the persisted allocator watermark
    (row-id views; watermark advance rides the same mutations);
  * Go's REPLACE-over-empty-destination ≡ INSERT is documented; the IMPORT
    INTO arm stays a real-store seam.
* `rust/crates/tidb-exec/src/cluster_ddl.rs`:
  * the `StateWriteReorganization` arm's recorded seam is CLOSED: with no
    caller-supplied outcome the tick runs the build itself and merges the
    row mutations ahead of the completion bookkeeping (build + completion
    are now ATOMIC here; Go commits them separately, which is why its probe
    exists — kept for fidelity);
  * a caller that executed the build out of band still supplies
    `MviewBuildOutcome { read_ts }`; that path is unchanged;
  * new `rolling_back_step` helper: build failure ⇒ job `ROLLINGBACK`,
    non-terminal, Go's error text as the step warning, next tick rolls back.
* `rust/crates/tidb-exec/src/lib.rs`: module registration.
* Tests (`rust/crates/tidb-exec/tests/cluster_ddl_source.rs`):
  * `persisted_materialized_view_create_step_runs_phase_one_and_rolls_back`
    rewritten: seeds real base record bytes, phase 2 with `None` builds +
    completes in one tick, and the view rows are read back through the
    driver (`read_view_rows` helper mirrors the engine's read path) and
    equal the aggregation of the seeded rows;
  * new
    `persisted_materialized_view_build_refuses_residual_rows_then_rolls_back`:
    residual row ⇒ Rollingback transition (non-terminal, warning text) ⇒
    next tick drops the phase-1 view and ends `ROLLBACK_DONE`.

## Validation

```
cargo +nightly-2026-08-22 nextest run -p tidb-exec \
  -E 'test(materialized_view) + test(persisted_materialized_view) +
      test(derives_the_purge_schedule) + test(preserves_text)'
# 12 passed (11 pre-existing + 1 new)
cargo clippy -p tidb-exec --all-targets   # no new warnings
cargo fmt --all -- --check                # clean
```
