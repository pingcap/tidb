# Migrate mysql.stats_buckets into mysql.stats_data

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.


## Purpose / Big Picture

Today, histogram buckets for every column and index of every (partitioned) table are stored in `mysql.stats_buckets`, which defines its row identity with a non-clustered `UNIQUE INDEX (table_id, is_index, hist_id, bucket_id)` and no clustered primary key. On heavily partitioned tables this layout is the dominant cost of `ANALYZE TABLE`: prior work (ref #66751) showed that simply turning that unique key into a clustered PK cuts `ANALYZE` in half (for example, ~1h down to ~30min on a table with 8000 partitions and 50 columns).

PR #66303 added a new consolidated system table `mysql.stats_data` with a clustered PK `(table_id, type, hist_id)` and a `LONGBLOB value` column, designed to absorb payloads from several stats tables that lack a proper clustered PK. The `type` discriminator has already been reserved (see `pkg/meta/metadef/system.go`): `1=col bucket`, `2=idx bucket`, `3=col fmsketch`, `4=idx fmsketch`. This plan only addresses type `1` and `2`.

After this plan is implemented, bucket data for every analyzed column and index will live as one blob row per histogram in `stats_data`, keyed by the clustered PK. On a heavily partitioned cluster the expected observable outcome is an `ANALYZE TABLE` wall-clock reduction of roughly 2× on the same tables used in #66751, with no change in plan quality for range predicates and no user-visible loss of statistics immediately after upgrade (because readers fall back to the legacy table for any histograms that have not yet been re-analyzed).

You can observe the change concretely by:

1. Running `ANALYZE TABLE t` against a partitioned table with many columns, and confirming that `SELECT count(*) FROM mysql.stats_data WHERE type IN (1,2)` grows while `SELECT count(*) FROM mysql.stats_buckets WHERE table_id = <t.table_id>` drops to zero for that table.

2. Running `EXPLAIN` on a range query against `t` before and after `ANALYZE` and confirming the same histogram-driven row-count estimates (proving the data round-trip preserves semantics).

3. On an upgraded cluster that has bucket rows in `stats_buckets` but nothing in `stats_data` for that histogram yet, observing that `SHOW STATS_BUCKETS` and range-query plans still work because readers fall back to the legacy table.


## Progress

Use a checkbox list for granular progress. Every stopping point must be reflected.

- [x] (2026-04-19) Reserve `StatsDataTypeColBucket=1`, `StatsDataTypeIdxBucket=2`, `StatsDataTypeColFMSketch=3`, `StatsDataTypeIdxFMSketch=4` in `pkg/meta/metadef/system.go` and update the SQL comment on the `type` column of `CreateStatsDataTable`. Landed in commit `f4a4d6aa86` on branch `stats-tables-with-clustered-pk`.
- [ ] M1: Switch the bucket-row codec to `proto.Marshal(statistics.HistogramToProto(hg))`, no wrapper file. Add a round-trip unit test that explicitly covers a v2 column histogram (first time those get proto-round-tripped). Expected: random histograms encode and decode losslessly; one row per histogram.
- [ ] M2: Switch writers and readers. Rewrite `saveBucketsToStorage` to `INSERT … ON DUPLICATE KEY UPDATE value = VALUES(value)` against `stats_data`, then unconditionally `DELETE` the matching `stats_buckets` row in the same session. Rewrite `InsertColStats2KV` to use `INSERT IGNORE` against `stats_data` (preserves the legacy "create-if-absent" semantics). Rewrite `HistogramFromStorageWithPriority` to read `stats_data` first and fall back to `stats_buckets` on miss, logging the fallback at INFO with rate limit.
- [ ] M3: Bootstrap loader. Rewrite `getTablesWithBucketsInRange`, `genInitStatsBucketsSQLForIndexes`, `initStatsBuckets4Chunk` in `pkg/statistics/handle/bootstrap.go` so that initial stats loading queries `stats_data` first and falls back to `stats_buckets` for indexes that have no `stats_data` row. Two separate queries (skip the legacy one if the new query already covers the table-id range). New `initStatsBuckets4Chunk` variant for the blob payload. No data migration during bootstrap.
- [ ] M4: GC and ID migration. Add a per-histogram `stats_data` DELETE in `deleteHistStatsFromKV` scoped by `(table_id, type, hist_id)` (same session as the existing legacy DELETE). The whole-table `GCStats` path already deletes `stats_data` by `table_id` (added by #66303). `changeGlobalStatsTables` already includes `stats_data`, no change. BR systable list already includes both tables.
- [ ] M5: Tests. Add unit tests covering write, read, GC, ID migration, bootstrap with mixed legacy/new data, and the `INSERT IGNORE` default-bucket path. Update existing bucket-counting tests that reference `mysql.stats_buckets`.
- [ ] M6: Integration and realtikv tests. Update `tests/integrationtest/t/executor/kv.test` (and `.result`), `tests/realtikvtest/statisticstest/statistics_test.go`, `br/tests/br_restore_physical/run.sh`. Confirm `ANALYZE` + `SHOW STATS_BUCKETS` round-trip passes end-to-end.
- [ ] M7: Performance check. Reproduce the #66751 workload (partitioned table with many columns, run `ANALYZE TABLE`) on a local cluster and confirm the wall-clock improvement. Record numbers in `Artifacts and Notes`.
- [ ] M8: Open the PR. Title: `statistics: migrate stats_buckets data into stats_data`. Issue references: `ref #66220, ref #66751`. Run `Ready` verification profile from `.agents/skills/tidb-verify-profile` before marking the PR ready.


## Surprises & Discoveries

Document unexpected behavior and concise evidence.

- Observation: PR #66303 was locally merged into branch `stats-tables-with-clustered-pk` but is still `OPEN` on GitHub. The table has already been renamed from `stats_table_data` to `stats_data` (commit `f5afdafdef`).
  Evidence: `git log --oneline | grep stats_data` shows the rename commit on this branch. `pkg/meta/metadef/system_tables_def.go:299-309` contains `CreateStatsDataTable`.

- Observation: Before this plan landed, there were no Go constants for the `type` discriminator. Only a SQL comment documented the values.
  Evidence: `grep -r "StatsDataType" pkg/` returned zero results prior to commit `f4a4d6aa86`.

- Observation: FM-sketch writers for `stats_data` were prototyped and then deliberately reverted in #66303 so that the PR would only add the table.
  Evidence: commit `2bcfee08d9 Reverted code changes, we want a small PR only for adding the new sys table` on this branch.


## Decision Log

Record every key decision in this format:

- Decision: Store all buckets of one histogram as a single opaque blob in `stats_data.value`, keyed by `(table_id, type, hist_id)`. Do not add a `sub_id` or `bucket_id` column to `stats_data`.
  Rationale: Adding `sub_id` to the PK reintroduces the wide-PK, many-rows-per-histogram pattern that PR #66751 showed to be the slow path. One-blob-per-histogram makes the clustered PK lookup return exactly one row per read and one write per `ANALYZE` per histogram, which is the tightest shape possible.
  Date/Author: 2026-04-19 / mattias, claude.

- Decision: Split bucket storage into two `type` values (`1=col bucket`, `2=idx bucket`) rather than encoding `is_index` inside the blob.
  Rationale: The existing bootstrap path uses `is_index=1` as a selective filter (`getTablesWithBucketsInRange`, `genInitStatsBucketsSQLForIndexes`). Using two `type` values lets those queries stay selective on the clustered PK without decoding blobs. Matches the FM-sketch precedent.
  Date/Author: 2026-04-19 / mattias, claude.

- Decision: Use dual-read fallback (Strategy A), not "repopulate on next ANALYZE" (Strategy B).
  Rationale: Histogram data is refreshed only on `ANALYZE`, which can be hours or days apart on large tables. Orphaning pre-upgrade buckets would cause range-query plan regressions until the next `ANALYZE` fires. Dual-read makes upgrade behavior observationally identical.
  Date/Author: 2026-04-19 / mattias, claude.

- Decision: No user-facing feature flag for the migration.
  Rationale: A `tidb_enable_*` gate was initially proposed but rejected. Codec correctness is fully covered by round-trip unit tests; dual-read already keeps upgrade smooth; once wrong bytes are written, flipping a gate off does not repair them. The cost of a sysvar (documentation, default-change rollout risk, doubled test matrix, removal churn one release later) outweighs the narrow "stop further damage" value, which a binary rollback also provides. If a test-only escape hatch is needed for fault injection, use a `failpoint` in the write path.
  Date/Author: 2026-04-19 / mattias, claude.

- Decision: Value `0` in the `type` column is left undefined and reserved for future use (for example a potential replacement for `stats_meta`).
  Rationale: The user explicitly does not want to pre-commit to an unused slot. Leaving a gap is fine; a comment in the `CREATE TABLE` documents it as reserved.
  Date/Author: 2026-04-19 / mattias, claude.

- Decision: Codec is `proto.Marshal(statistics.HistogramToProto(hg))` reusing the existing helpers; no new wrapper file, no version byte, no bespoke format.
  Rationale: `HistogramToProto`/`HistogramFromProto` already exist at `pkg/statistics/histogram.go:906,933` and preserve every field that `stats_buckets` carries (per-histogram NDV, per-bucket Count/Repeat/LowerBound/UpperBound/NDV). The proto round-trip is exercised in production for every region of every index analyze and for v1 column analyze, so it is the most-tested histogram codec in the system. Per-histogram fields not in the proto (`NullCount`, `LastUpdateVersion`, `TotColSize`, `Correlation`, `ID`) live in `stats_histograms`, not `stats_buckets`, so their absence is correct. NULL bucket bounds are not a real case (NULL counts are tracked in `stats_histograms.null_count`), so the proto's non-nullable `bytes` for bound values matches reality.
  Date/Author: 2026-04-20 / mattias, claude.

- Decision: ANALYZE writes use `INSERT … ON DUPLICATE KEY UPDATE value = VALUES(value)`, not `REPLACE INTO`.
  Rationale: For a clustered-PK table with no secondary indexes (which `stats_data` is), ODKU compiles to a single KV Put on update, while REPLACE compiles to a Delete + Put. Saving the Delete adds up across thousands of histograms × thousands of partitions per ANALYZE. Affected-rows convention is identical for our purposes (1 = INSERTed, 2 = UPDATEd), so the legacy-purge optimization (next decision) still works.
  Date/Author: 2026-04-20 / mattias, claude.

- Decision: Unconditional legacy purge after every new-path write (no affected-rows optimization for now).
  Rationale: We considered using ODKU's `affected_rows` to skip a no-op DELETE in the steady state. Two unresolved concerns: (1) TiDB's ODKU affected-rows semantics for our exact case (clustered PK, single non-key column update) have not been verified to match MySQL — needs a deliberate test; (2) when the new blob equals the existing blob, MySQL returns `affected_rows = 0` because the UPDATE is elided entirely — and `ON UPDATE CURRENT_TIMESTAMP` does NOT fire in that case (the timestamp's auto-update is gated on at least one user column actually changing, so it gives no independent signal). A naïve `if affected == 1 { DELETE }` would then skip the purge on the first new-path write to a histogram whose new blob happens to be identical to a pre-existing row's blob (rare in practice due to sampling variance, but possible on small/static tables). The corrected check `affected != 2` re-introduces the no-op DELETE on `0`, eroding most of the savings.

  A bulletproof variant would be to force a real UPDATE by including `updated_at = NOW(6)` in the SET clause (microsecond precision means two ANALYZE writes in the same microsecond are essentially impossible) and switching to `affected != 2`. That closes the `0` hole but still depends on TiDB matching MySQL's affected-rows convention.

  Cost of the always-DELETE path: one DELETE on a non-existent key per ANALYZE per histogram. In TiDB MVCC this resolves to a transaction-buffer entry with no key to delete at commit, so the KV-level cost is ~zero; the only real cost is the session/network roundtrip we're already paying for the ODKU. Defer the optimization until a benchmark shows the no-op DELETE is meaningful in aggregate; until then, always DELETE.
  Date/Author: 2026-04-20 / mattias, claude.

- Decision: `InsertColStats2KV` uses `INSERT IGNORE`, not REPLACE/ODKU.
  Rationale: This path creates a default single-bucket histogram when a new column is added. The intent is "create only if absent; never overwrite a real ANALYZE result." REPLACE/ODKU would clobber an existing histogram; INSERT IGNORE is a no-op on PK collision, which is exactly the legacy `INSERT IGNORE INTO stats_buckets` semantics. Minimally different from the legacy code.
  Date/Author: 2026-04-20 / mattias, claude.

- Decision: Dual-read fallback logs at INFO with rate limiting.
  Rationale: On a fresh upgrade, every histogram read for an unmigrated table will trigger a fallback until the next ANALYZE — a per-read log line would spam. INFO level matches the existing log volume in this area; rate limiting (e.g. one summary line per stats handle init, or one per (table_id, is_index, hist_id) per minute) keeps signal high. A future `stats_meta` successor with a clustered PK could carry a per-table flag indicating where bucket data lives, removing the need for fallback probing — out of scope here.
  Date/Author: 2026-04-20 / mattias, claude.

- Decision: `ChangeGlobalStatsID` needs no change.
  Rationale: `pkg/statistics/handle/storage/update.go:154-156` already lists `stats_data` in `changeGlobalStatsTables` (added by #66303). The single UPDATE template `update mysql.<table> set table_id = %? where table_id = %?` works for `stats_data` because `table_id` is its first column.
  Date/Author: 2026-04-20 / mattias, claude.

- Decision: Bootstrap loader uses two separate queries (stats_data first, then stats_buckets), not UNION; per-index fallback in Go.
  Rationale: Two queries are easier to unit-test than a single UNION, and they let us skip the legacy probe entirely once the new query already covers every `table_id` in the requested range. Per-index fallback in Go (skip legacy rows whose `(table_id, hist_id)` was already returned by the new scan) avoids any SQL `NOT IN` gymnastics and keeps the legacy parse path (`initStatsBuckets4Chunk`) untouched. A new variant — `initStatsBuckets4ChunkFromStatsData` — handles the blob payload via `proto.Unmarshal` + `statistics.HistogramFromProto` in one call per histogram.
  Date/Author: 2026-04-20 / mattias, claude.

- Decision: No data migration during bootstrap; an upgrade-gate that controls when writes start landing in `stats_data` is deferred to a separate follow-up PR modeled on PR #66847's pattern.
  Rationale: Migrating data at bootstrap would slow startup, complicate failure recovery, and conflate two concerns. The natural migration trigger is the next ANALYZE — which writes a fresh blob and lazily purges the legacy row (M2). The remaining hazard is rolling-upgrade mixed-version clusters: a newly-upgraded node would write to `stats_data` while older nodes still read only from `stats_buckets`, causing temporary plan regressions on the old nodes. PR #66847 addresses the analogous problem for global index V1 keys with a cluster-version-gated atomic flag (`globalIndexV1Supported`) updated by a background loop polling all server-info entries; the same pattern applied here would gate the new write path behind "all nodes ≥ first-version-with-stats_data-bucket-support". Out of scope for this PR; tracked as a follow-up so this PR can ship with read-side coverage in place but write-side gated by a future flag.
  Date/Author: 2026-04-20 / mattias, claude.

- Decision: Per-histogram GC adds a `stats_data` DELETE keyed on `(table_id, type, hist_id)`, in the same session as the existing legacy DELETE. Whole-table GC and `ChangeGlobalStatsID` need no change; both already handle `stats_data` via wiring added in #66303.
  Rationale: `deleteHistStatsFromKV` (gc.go:259) keys the legacy DELETE on `(table_id, hist_id, is_index)`; `stats_data` carries the same identity but with `type` instead of `is_index` (1 = col bucket, 2 = idx bucket). Same-session DELETE keeps the reader's "exactly one of the two tables holds this histogram" invariant intact through the GC. Order between the two DELETEs is unimportant because the migration invariant guarantees only one row ever exists across both tables for any given histogram (M2's unconditional purge enforces this on write). Whole-table GC at `gc.go:170` already does `DELETE FROM stats_data WHERE table_id=?`, which correctly purges all bucket rows (and any future row types) for the dropped table.
  Date/Author: 2026-04-20 / mattias, claude.

- Decision: BR needs no code change; both tables are already in the systable restore map. Only `br/tests/br_restore_physical/run.sh` needs an assertion update (covered under M6).
  Rationale: #66303 already added `stats_data` to `br/pkg/restore/snap_client/systable_restore.go:44`. BR backs up and restores rows of each system table verbatim, so the existing wiring covers the new bucket payload without modification.
  Date/Author: 2026-04-20 / mattias, claude.

- Decision: No bootstrap version bump is added by this PR.
  Rationale: `stats_data` already exists (created by #66303 at version 255 and in NextGen boot table version 2). The migration only changes which table a new write targets; no schema change, no upgrade step, no bootstrap entry change.
  Date/Author: 2026-04-19 / mattias, claude.


## Outcomes & Retrospective

(To be filled in after M7 and M8. Compare achieved `ANALYZE` wall-clock against the expected ~2× improvement; note any residual cost that is not explained by the removed write-path overhead; record any codec-format surprises and confirm the dual-read path was exercised in the realtikv run.)


## Context and Orientation

Relevant code lives in two packages. The **metadata layer** at `pkg/meta/metadef/` defines the system tables. `pkg/meta/metadef/system_tables_def.go:299-309` has the `CREATE TABLE` for `stats_data`; `pkg/meta/metadef/system.go:157-170` has its table ID and the four `StatsDataType*` constants. The **stats handle** at `pkg/statistics/handle/` is what reads and writes bucket data at runtime. The specific files this plan touches:

- `pkg/statistics/handle/storage/save.go` — writes buckets during `ANALYZE`. Functions: `saveBucketsToStorage` (line 83), `SaveAnalyzeResultToStorage` (around line 296), `SaveColOrIdxStatsToStorage` (around line 361), `InsertColStats2KV` (around line 486, for default-bucket-on-add-column).
- `pkg/statistics/handle/storage/read.go` — on-demand reader: `HistogramFromStorageWithPriority` (line 132), called when the optimizer needs a histogram that was not preloaded.
- `pkg/statistics/handle/bootstrap.go` — initial stats load at domain startup. Functions: `getTablesWithBucketsInRange` (line 586), `genInitStatsBucketsSQLForIndexes` (line 726), `initStatsBuckets4Chunk` (line 681).
- `pkg/statistics/handle/storage/gc.go` — background purge: `GCStats` (around line 161, per-table), `deleteHistStatsFromKV` (around line 259, per-histogram).
- `pkg/statistics/handle/storage/update.go` — cross-table stats-ID migration: `changeGlobalStatsTables` slice at line 154, used by `ChangeGlobalStatsID`.

**Non-obvious terms defined once here, grounded in this repository:**

- **Histogram**: in TiDB an equal-height histogram with up to `statistics.MaxBucketNumber` buckets (default 256, see `pkg/statistics/histogram.go`). Each bucket has `count`, `repeats`, `lower_bound` (LONGBLOB of the boundary datum), `upper_bound`, and `ndv`. Used for selectivity estimation of range predicates.
- **hist_id**: the numeric identifier of a column (or index) within a table, used as the lookup key together with `table_id` and `is_index`.
- **Clustered PK**: a primary key whose rows are physically stored by PK order, so a PK lookup returns data without an extra index probe. The opposite of the non-clustered unique key used by `mysql.stats_buckets` today.
- **Bootstrap stats load**: the code path run during domain startup that preloads a subset of histogram metadata (typically indexes) into memory so common queries do not stall on first use. Lives in `pkg/statistics/handle/bootstrap.go`.
- **`ANALYZE TABLE`**: the SQL command that samples rows, computes histograms/TopN/FM-sketches, and persists them to the `stats_*` system tables. Write hot path relevant to this plan.
- **Dual-read**: the reader checks the new table first; if it finds a row, it returns it and ignores the legacy table. If not, it reads from the legacy table. Used to keep upgrade behavior seamless.
- **NextGen**: the bootstrap mode used by the next-generation TiDB deployment; has a separate boot-table versioning scheme (see `pkg/meta/meta.go` references to `BaseNextGenBootTableVersion` and `NextGenBootTableVersion2`). Relevant only because `stats_data` is already registered there; this plan does not change that.

**Legacy `stats_buckets` schema** (verbatim from `pkg/meta/metadef/system_tables_def.go:198-210`, quoted here to avoid requiring the reader to open the file):

    CREATE TABLE IF NOT EXISTS mysql.stats_buckets (
        table_id    BIGINT(64) NOT NULL,
        is_index    TINYINT(2) NOT NULL,
        hist_id     BIGINT(64) NOT NULL,
        bucket_id   BIGINT(64) NOT NULL,
        count       BIGINT(64) NOT NULL,
        repeats     BIGINT(64) NOT NULL,
        upper_bound LONGBLOB NOT NULL,
        lower_bound LONGBLOB ,
        ndv         BIGINT NOT NULL DEFAULT 0,
        UNIQUE INDEX tbl(table_id, is_index, hist_id, bucket_id)
    );

**New `stats_data` schema** (verbatim from `pkg/meta/metadef/system_tables_def.go:302-309`):

    CREATE TABLE IF NOT EXISTS mysql.stats_data (
        table_id   BIGINT(64) NOT NULL COMMENT 'physical partition ID (or table ID for non-partitioned tables)',
        type       INT NOT NULL COMMENT '0=undefined (reserved), 1=col bucket, 2=idx bucket, 3=col fmsketch, 4=idx fmsketch',
        hist_id    BIGINT(64) NOT NULL COMMENT 'column or index ID',
        value      LONGBLOB NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (table_id, type, hist_id) CLUSTERED
    );


## Plan of Work

The plan lives entirely on branch `stats-tables-with-clustered-pk` (which includes #66303 locally). All edits are repository-relative paths.

The high-level sequence is: first introduce a pure data codec (no behavior change), then switch writers to `stats_data` and add dual-read fallback for upgrade smoothness, then convert the bootstrap loader, then handle GC/BR/migration, then tests and performance verification. Each milestone is individually reviewable inside one PR or can be split along the dashed lines below if review feedback asks.

**M1 — Bucket codec.** Reuse the existing `statistics.HistogramToProto` and `statistics.HistogramFromProto` (`pkg/statistics/histogram.go:906,933`) plus `proto.Marshal` / `proto.Unmarshal` from `github.com/pingcap/tipb/go-tipb`. No new wrapper file or new exported functions. The blob written to `stats_data.value` is exactly the bytes of `proto.Marshal(statistics.HistogramToProto(hg))`. The reader does `proto.Unmarshal` into a `tipb.Histogram`, then `statistics.HistogramFromProto` to land in a `statistics.Histogram` whose bounds are bytes-datums; the existing post-decode step (`hg.DecodeTo(tp, tz)` already done by callers today) handles type conversion.

This works because the proto round-trip is already exercised in production for index analyze (every region, every analyze) and v1 column analyze. The only path that has never been proto-round-tripped is **v2 column histograms** (built locally from row samples in `analyze_col_sampling.go`), so the M1 test must explicitly cover that case.

Add a round-trip unit test colocated near the existing proto helpers (e.g. extend `pkg/statistics/statistics_test.go` rather than create a new file) covering: an empty histogram, one bucket, `MaxBucketNumber` buckets, wide upper/lower bounds (up to 64 KB each), a v2 column histogram built via the column sampling path, and a malformed blob (expected `proto.Unmarshal` error).

NULL handling is not a concern: bucket bounds carry only non-null values; NULL counts live in `stats_histograms.null_count` (not in any bucket). The proto's non-nullable `bytes` for bound matches reality.

**M2 — Switch writers and readers.** No feature flag (see Decision Log). Rewrite the write paths:

- `saveBucketsToStorage` (save.go:83): instead of the loop that INSERTs one row per bucket, do `blob, err := proto.Marshal(statistics.HistogramToProto(hg))` then

        INSERT INTO mysql.stats_data (table_id, type, hist_id, value)
        VALUES (?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE value = VALUES(value)

  with `type = StatsDataTypeColBucket` if `is_index == 0` else `StatsDataTypeIdxBucket`. Then unconditionally:

        DELETE FROM mysql.stats_buckets
          WHERE table_id=? AND is_index=? AND hist_id=?

  Both statements run in the same session/transaction so a concurrent reader never sees both rows simultaneously.

  We considered an "affected-rows == 1 → purge, == 2 → skip purge" optimization to avoid a no-op DELETE on the steady-state common case. **Two unresolved concerns** make us default to unconditional DELETE for now:

  1. **TiDB ODKU affected-rows semantics are unverified for this case.** MySQL returns 1 for INSERT, 2 for UPDATE, 0 if the UPDATE would not change any column. TiDB usually matches but should not be assumed. Needs a unit test or a deliberate `EXPLAIN ANALYZE`-style verification before relying on it for correctness.

  2. **The `affected_rows = 0` case breaks the optimization.** If a new ANALYZE produces a blob byte-for-byte identical to the existing row's value, both MySQL and TiDB ODKU return 0 — not 2. The naïve `if affected == 1 { DELETE }` check would then skip the purge even when it is the first new-path write on a histogram that still has legacy data. Sampling variance makes identical blobs rare in practice (per-bucket counts and NDV depend on the sample) but not impossible (very small / static tables, deterministic sample order). The corrected check would be `if affected != 2 { DELETE }`, but that re-introduces the no-op DELETE on `affected = 0`, eroding the savings.

  If either concern is settled by a follow-up benchmark showing the steady-state DELETE is meaningful, we can re-enable the optimization with `affected != 2` and a clear comment. Until then: always purge.

- `SaveAnalyzeResultToStorage` (save.go:296): the pre-write `DELETE FROM mysql.stats_buckets …` becomes redundant once `saveBucketsToStorage` handles purge; remove it. The `INSERT` path is subsumed into the new `saveBucketsToStorage`.
- `SaveColOrIdxStatsToStorage` (save.go:361): same pattern as above.
- `InsertColStats2KV` (save.go:486): the `insert ignore into mysql.stats_buckets … values (…, 0, …)` path becomes the equivalent against `stats_data`. We need INSERT IGNORE semantics here (create default bucket only if no histogram exists yet — never overwrite a real ANALYZE result), not REPLACE/ODKU which would clobber existing data:

        hg := &statistics.Histogram{ … default single-bucket … }
        blob, _ := proto.Marshal(statistics.HistogramToProto(hg))
        _, err = util.Exec(sctx,
            "INSERT IGNORE INTO mysql.stats_data (table_id, type, hist_id, value) VALUES (?, ?, ?, ?)",
            tableID, StatsDataTypeColBucket, colInfo.ID, blob)

  No legacy purge here: this path runs only when a column is added, before any ANALYZE has populated either table for that hist_id. The dual-read fallback handles the rare case of a pre-upgrade legacy row sharing the hist_id.

Rewrite the read path:

- `HistogramFromStorageWithPriority` (read.go:132): first query `SELECT value FROM mysql.stats_data WHERE table_id=? AND type=? AND hist_id=?`. If a row is returned, `proto.Unmarshal` it into a `tipb.Histogram`, then `statistics.HistogramFromProto` and proceed with the existing `hg.DecodeTo(tp, tz)` step. If no row, fall back to the existing `SELECT … FROM mysql.stats_buckets WHERE …` and log at INFO once per (table_id, is_index, hist_id) with a rate limit so the message does not flood. Future direction: a `stats_meta` successor with a clustered PK could carry a per-table flag indicating whether bucket data lives in `stats_data` vs. legacy, removing the need for fallback probing — out of scope for this PR.

**M3 — Bootstrap loader.** In `pkg/statistics/handle/bootstrap.go`. Read-only at bootstrap; no data migration.

- `getTablesWithBucketsInRange` (line 586): two separate queries, not a UNION. First:

        SELECT DISTINCT table_id
        FROM   mysql.stats_data
        WHERE  type = 2 AND table_id IN (<range>)

  Then, only if the first query did not return every `table_id` in the requested range, run the legacy:

        SELECT DISTINCT table_id
        FROM   mysql.stats_buckets
        WHERE  is_index = 1 AND table_id IN (<range>)

  Merge the two `table_id` sets in Go. Two queries are easier to unit-test than a UNION and let us skip the legacy probe entirely once the migration is steady-state on a given cluster.

- `genInitStatsBucketsSQLForIndexes` (line 726) + `initStatsBuckets4Chunk` (line 681): straight-forward fallback per index, no SQL `NOT IN` gymnastics.

  1. Run the new-format scan first:

         SELECT table_id, hist_id, value
         FROM   mysql.stats_data
         WHERE  type = 2 AND table_id IN (<range>)
         ORDER BY table_id

     Parse with a new `initStatsBuckets4ChunkFromStatsData` (or similarly-named variant) that does `proto.Unmarshal` + `statistics.HistogramFromProto` to populate all buckets of the histogram in one call. Track the `(table_id, hist_id)` pairs that appeared in this scan.

  2. Then run the legacy scan exactly as today, hitting `mysql.stats_buckets` with `ORDER BY table_id`, parsed by the existing `initStatsBuckets4Chunk` unchanged.

  3. In the result loop for the legacy scan, skip rows whose `(table_id, hist_id)` was already seen in step 1 — simple Go-side filter against the set built above. No need to push the filter into SQL.

  This keeps the existing legacy path intact (low risk of breaking what works today) and bolts the new path on top.

**M4 — GC, BR, ID migration.**

- **Whole-table GC (`GCStats`, gc.go:170)**: no change needed. The DELETE against `stats_data` by `table_id` was already added by #66303 and correctly purges every row for the dropped table regardless of `type`.

- **Per-histogram GC (`deleteHistStatsFromKV`, gc.go:259)**: add one new DELETE next to the existing legacy DELETE, in the same session. The legacy DELETE keys on `(table_id, hist_id, is_index)`; the new DELETE keys on `(table_id, type, hist_id)` where `type = StatsDataTypeColBucket` if `is_index == 0` else `StatsDataTypeIdxBucket`:

        DELETE FROM mysql.stats_data
        WHERE  table_id = ? AND type = ? AND hist_id = ?

  Both DELETEs in the same transaction so a reader never sees one purge land without the other. Order between them does not matter — the read path only ever returns one of the two rows (`stats_data` first, fall back to `stats_buckets`), and a properly maintained migration invariant is that **bucket data for the same histogram must never exist in both tables simultaneously** (M2's unconditional purge enforces that on every write).

- **`ChangeGlobalStatsID` (`update.go:154`)**: no change. `stats_data` is already in `changeGlobalStatsTables` (added by #66303) and the single UPDATE template `update mysql.<table> set table_id = %? where table_id = %?` works for `stats_data` because `table_id` is its first column.

- **BR (`br/pkg/restore/snap_client/systable_restore.go:44`)**: no code change. Both `stats_buckets` and `stats_data` are already in the map (#66303). The only test asset to update is `br/tests/br_restore_physical/run.sh:74` (post-restore assertion, see M6).

**M5 — Unit tests.** Add to `pkg/statistics/handle/storage/`:

- Extension to `pkg/statistics/statistics_test.go` — round-trip coverage including a v2-built column histogram (see M1).
- `save_test.go` additions — `TestSaveBucketsWritesStatsData` (ANALYZE writes `type=1` for a column, `type=2` for an index, and the matching `stats_buckets` row is purged).
- `read_test.go` additions — `TestReadBucketsPrefersStatsData` (seed both, new wins), `TestReadBucketsFallsBackToLegacy` (seed only legacy, still works).
- `gc_test.go` updates — existing row-count assertions at lines 41/46/52/58/77/83/88/94 must now also check `stats_data`. Add `TestGCStatsPurgesBothBucketTables`.
- `update_test.go` — `TestChangeGlobalStatsIDMigratesBothBucketTables`.
- `save.go`/`InsertColStats2KV` — `TestInsertColStats2KVWritesStatsData`.

Bootstrap tests:

- `pkg/statistics/handle/bootstrap_test.go` (or wherever `TestInitStats*` lives — confirm while editing) — `TestBootstrapInitStatsBucketsFromStatsData`, `TestBootstrapInitStatsBucketsMixedLegacyAndNew`.

**M6 — Integration and realtikv tests.**

- `tests/integrationtest/t/executor/kv.test` and `.../r/executor/kv.result`: the two existing bucket queries (lines 11, 16) must either be updated to read from `stats_data`, or rewritten as a `UNION` so the test is codec-agnostic. Use the TiDB integration-test recording flow (`.agents/skills/tidb-integrationtest-recorder`).
- `tests/realtikvtest/statisticstest/statistics_test.go:56` — `delete from mysql.stats_buckets` prep becomes `delete from both`.
- `br/tests/br_restore_physical/run.sh:74` — expectation becomes "rows exist in either table".
- `pkg/statistics/handle/handletest/statstest/stats_test.go:843`, `.../handle_test.go:1047`, `.../ddl/ddl_test.go:385`, `.../asyncload/async_load_test.go:241`: update each to count/update the right table. Prefer a helper `bucketRowCount(tableID)` that sums rows in both tables so existing assertions stay format-agnostic during the transition.

**M7 — Performance check.** Using a local cluster (TiUP playground via `.agents/skills/tidb-realtikv-runner`), reproduce the #66751 workload at a reduced scale (e.g. 200 partitions × 50 cols is enough to see a signal without burning hours). Measure `ANALYZE TABLE t` wall-clock on the merge base (master, pre-migration) and on this branch. Record both numbers in `Artifacts and Notes`. Expect the new branch to be noticeably faster; if it is not, that is a blocker, not a pass.

**M8 — Open the PR.** Use the template: `gh pr create -T .github/pull_request_template.md`. Title: `statistics: migrate stats_buckets data into stats_data`. Issue line: `Issue Number: ref #66220, ref #66751`. Before flipping the PR to "ready for review", run the `Ready` profile from `.agents/skills/tidb-verify-profile` (which includes `make lint`).


## Concrete Steps

All commands are run from `/Users/mattias/repos/worktrees/stats-tables-with-clustered-pk`.

Step 1 — confirm starting point:

    git rev-parse HEAD
    # expected: f4a4d6aa86 or later on branch stats-tables-with-clustered-pk
    git log --oneline -1
    # expected: statistics: reserve stats_data type values …

Step 2 — add round-trip test (M1):

    # extend pkg/statistics/statistics_test.go with a Test*HistogramProtoRoundtrip
    # covering: empty / 1 bucket / MaxBucketNumber / wide bounds / v2 column hist / malformed blob
    go test -count=1 -run "HistogramProto" ./pkg/statistics/...
    # expected: PASS

Step 3 — Bazel prepare if files were added (per AGENTS.md Quick Decision Matrix):

    make bazel_prepare

Step 4 — implement M2 write/read changes, run targeted tests:

    go test -count=1 -run "TestSaveBuckets|TestReadBuckets|TestInsertColStats2KV" ./pkg/statistics/handle/storage/...
    # expected: PASS for all new tests

Step 5 — implement M3 bootstrap changes:

    go test -count=1 -run "TestBootstrapInitStats|TestInitStatsBuckets" ./pkg/statistics/handle/...
    go test -count=1 -run "TestBootstrap" ./pkg/session/...
    # expected: PASS (pkg/session tests guard bootstrap version invariants, they must stay green because this plan does not bump versions)

Step 6 — implement M4:

    go test -count=1 -run "TestGCStats|TestChangeGlobalStatsID" ./pkg/statistics/handle/storage/...

Step 7 — record integration tests (M6, per `.agents/skills/tidb-integrationtest-recorder`):

    # only after editing t/executor/kv.test
    # use the recording command documented in docs/agents/testing-flow.md -> Integration tests
    # verify the regenerated .result is minimal (no churn beyond the bucket queries)

Step 8 — realtikv M7 (per `.agents/skills/tidb-realtikv-runner`):

    # baseline: check out master (or the merge base) and build a tidb-server binary
    git worktree add ../baseline-master master
    (cd ../baseline-master && make)
    # run playground using the baseline binary, ANALYZE the test table, record wall-clock, stop playground
    # then repeat with this branch's binary
    (make)
    # run playground using this branch's binary, re-ANALYZE, record wall-clock
    # record both in Artifacts and Notes below
    tiup playground stop   # mandatory cleanup
    git worktree remove ../baseline-master

Step 9 — Ready profile before PR:

    # per .agents/skills/tidb-verify-profile "Ready" definition
    make lint
    # plus the targeted test surface from steps 4-6
    # do not run make bazel_lint_changed unless the user requests it


## Validation and Acceptance

The plan is accepted when the following are all true:

- **Unit**: `go test ./pkg/statistics/handle/storage/...` and `go test ./pkg/statistics/handle/...` are green.
- **Bootstrap**: `go test -run TestBootstrap ./pkg/session/...` is green. (This confirms no bootstrap version invariant was broken.)
- **Integration**: `tests/integrationtest/t/executor/kv.test` runs green against the new code. The regenerated `.result` differs only in the bucket queries, with no unrelated churn.
- **Realtikv round-trip**: on a playground cluster running this branch, `ANALYZE TABLE t` then `EXPLAIN SELECT … FROM t WHERE col BETWEEN ? AND ?` produces the same row-count estimate as the same query on a baseline (master) build (± 1 due to rounding).
- **Upgrade-seamlessness**: seed `mysql.stats_buckets` with a pre-migration row, restart the cluster on this branch, and run a range query — the histogram still drives the estimate. (Dual-read works.)
- **Performance**: on the reduced-scale #66751-style workload, `ANALYZE TABLE` on this branch is at least 30% faster wall-clock than on master. (Target is ~2×; 30% is the minimum acceptance bar.)

Failures: if any acceptance item fails, stop and record in `Surprises & Discoveries` with the failing command and the observed vs. expected output. Do not continue to the next milestone.


## Idempotence and Recovery

All `go test` commands are safe to rerun. The realtikv playground must be cleaned up (`tiup playground stop`, delete the data dir) after every run; failure to do so leaves stale stats that will pollute the next baseline.

If M2/M3 introduce a codec bug that is caught after release, the recovery path is a binary rollback. Affected histograms can be repaired by `ANALYZE TABLE t` once the bug is fixed; the dual-read path means readers continue to find the legacy `stats_buckets` row for any histogram that has not been re-analyzed since the bad release.

If PR #66303 is re-requested-changes and the `StatsDataType*` constants or the SQL comment on the `type` column get renumbered/renamed upstream before this plan's PR merges, merge the upstream change in and re-run M1 round-trip tests to confirm the codec still targets the right `type` values. Commit `f4a4d6aa86` on the local branch is the reference.


## Artifacts and Notes

(To be populated during execution. Expected entries:)

- M1 — confirmation that the proto round-trip is lossless for v2-built column histograms; any surprises around `Tp` being `TypeBlob` after `HistogramFromProto`.
- M7 — `ANALYZE` wall-clock before/after on the reproduction workload, with `EXPLAIN ANALYZE` or server-side timing.
- M8 — PR URL and CI run URL.


## Interfaces and Dependencies

No new exported API. Storage call sites use:

    // write
    blob, err := proto.Marshal(statistics.HistogramToProto(hg))
    // read
    var p tipb.Histogram
    if err := proto.Unmarshal(blob, &p); err != nil { … }
    hg := statistics.HistogramFromProto(&p)
    // caller does hg.DecodeTo(tp, tz) afterward, same pattern as today

`statistics.HistogramToProto` and `statistics.HistogramFromProto` already exist at `pkg/statistics/histogram.go:906,933`. They preserve `NDV` and per-bucket `Count`/`Repeats`/`LowerBound`/`UpperBound`/`NDV`. Per-histogram fields not in the proto (`NullCount`, `LastUpdateVersion`, `TotColSize`, `Correlation`, `ID`) are correctly outside scope: they live in `mysql.stats_histograms`, not `stats_buckets`/`stats_data`.

No new system variable is added.

Downstream consumers:

- `pkg/statistics/handle/storage/save.go` — `saveBucketsToStorage`, `SaveAnalyzeResultToStorage`, `SaveColOrIdxStatsToStorage`, `InsertColStats2KV`.
- `pkg/statistics/handle/storage/read.go` — `HistogramFromStorageWithPriority`.
- `pkg/statistics/handle/bootstrap.go` — `getTablesWithBucketsInRange`, `genInitStatsBucketsSQLForIndexes`, `initStatsBuckets4Chunk`.
- `pkg/statistics/handle/storage/gc.go` — `GCStats`, `deleteHistStatsFromKV`.
- `pkg/statistics/handle/storage/update.go` — `ChangeGlobalStatsID` (via `changeGlobalStatsTables`).

No new dependency is added to `go.mod` if `tipb.Histogram` is reused (already vendored via `github.com/pingcap/tipb`). If a bespoke format is chosen, encoding is pure stdlib `encoding/binary` + `bytes`.
