# `pkg/ddl/jobsubmit` — materialized-view table-ID reporting receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
`job2TableIDs` arms added by the materialized-view DDL commit `94a9cbedab`
in `pkg/ddl/jobsubmit/submit.go`.

## Drift content and Rust alignment

- `ActionCreateMaterializedView`: reports the view's own id plus every
  created log id (`makeStringForIDs([TableID, MLogTableIDs...])`);
- `ActionCreateMaterializedViewLog`: reports the log id plus the recorded
  base table id when `TableInfo.MaterializedViewLog.BaseTableID > 0`.

→ `tidb-exec/src/ddl_job_submit.rs::job_table_ids`, matching Go's
`makeStringForIDs` semantics exactly: dedupe into a set, sort the decimal
strings lexicographically (`"100" < "50" < "99"`), join with commas. Nil
args fall through to the id-only default, and a log whose `TableInfo` lacks
log metadata (or records base id 0) reports only its own id.

## Known gaps recorded (not fixed in this batch)

Go's `getRequiredGIDCount` / `assignGIDsForJobs` MV arms and
`SetSchemaDiffForCreateTable`'s rollback/reorg arms have no Rust owner (the
job-submission ID-allocation infrastructure and the DDL-job commit
`SetSchemaDiff` writer are not yet transcreated); they belong to those
future owners. `delete_range` / `rollingback` / `sanity_check` / `reorg` MV
arms belong to the DDL worker infrastructure (next batch).

## Regression tests

`ddl_job_submit::tests::job_table_ids_cover_materialized_view_creates` —
view with log ids (`100,50,99`, Go's lexicographic set ordering), view
without ids (`50`), log with base id (`51,88`), log without metadata (`51`).

Fail-before evidence: the arms do not exist pre-batch; the tests bind to the
new match arms (pre-batch, the MV actions fell to the id-only default and
the with-ids assertions fail).

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline \
  -p tidb-exec -E 'test(ddl_job_submit)'
# 10/10 passed (including the new regression)
```

No Go source changed in this batch.
