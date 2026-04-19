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
- [ ] M1: Decide and prototype the bucket payload codec. Add `pkg/statistics/handle/storage/bucket_codec.go` plus table-driven round-trip tests. Expected: random histograms encode and decode losslessly; one row per histogram.
- [ ] M2: Switch writers and readers. Rewrite `saveBucketsToStorage` to write to `stats_data` and purge the matching `stats_buckets` row. Rewrite `HistogramFromStorageWithPriority` to read `stats_data` first and fall back to `stats_buckets` on miss.
- [ ] M3: Bootstrap loader. Rewrite `getTablesWithBucketsInRange`, `genInitStatsBucketsSQLForIndexes`, `initStatsBuckets4Chunk` in `pkg/statistics/handle/bootstrap.go` so that initial stats loading reads `stats_data` first and then reads the remaining (table_id, is_index) pairs from `stats_buckets`.
- [ ] M4: GC and ID migration. Route `gc.go` deletes to both tables; keep `stats_buckets` in `changeGlobalStatsTables` so restore/ID-migration continues to work.
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

**M1 — Bucket codec.** Create `pkg/statistics/handle/storage/bucket_codec.go` with two exported functions:

    func EncodeBuckets(hg *statistics.Histogram) ([]byte, error)
    func DecodeBuckets(blob []byte, hg *statistics.Histogram) error

Prefer reusing `tipb.Histogram` from `github.com/pingcap/tipb/go-tipb` if a quick grep (`grep -rn "tipb\.Histogram" pkg/statistics`) confirms that the coprocessor protocol's histogram shape already carries `count`, `repeats`, `lower_bound`, `upper_bound`, `ndv` per bucket. If it is a clean fit, wrap `proto.Marshal`/`proto.Unmarshal`. Otherwise, add a minimal versioned format: one leading byte (format version), then repeated bucket records. Document the choice in `Decision Log`.

Add `bucket_codec_test.go` with a table-driven round-trip test for: empty histogram, one bucket, `MaxBucketNumber` buckets, wide upper/lower bounds (up to 64 KB each), and a malformed blob (expected decode error).

**M2 — Switch writers and readers.** No feature flag (see Decision Log). Rewrite the write paths:

- `saveBucketsToStorage` (save.go:83): instead of the loop that INSERTs one row per bucket, encode the `statistics.Histogram` with `EncodeBuckets`, then `REPLACE INTO mysql.stats_data (table_id, type, hist_id, value) VALUES (?, ?, ?, ?)` with `type = StatsDataTypeColBucket` if `is_index == 0` else `StatsDataTypeIdxBucket`. Also `DELETE FROM mysql.stats_buckets WHERE table_id=? AND is_index=? AND hist_id=?` so the legacy row does not shadow the new one.
- `SaveAnalyzeResultToStorage` (save.go:296): the pre-write `DELETE FROM mysql.stats_buckets …` stays; the `INSERT` path is subsumed into the new `saveBucketsToStorage`.
- `SaveColOrIdxStatsToStorage` (save.go:361): same pattern as above.
- `InsertColStats2KV` (save.go:486): the `insert ignore into mysql.stats_buckets … values (…, 0, …)` path becomes a read-modify-write against `stats_data`. Pseudocode:

        existing, err := readStatsDataBucketBlob(sctx, tableID, colTypeValue, histID)
        if err != nil { return err }
        if existing != nil { return nil } // equivalent of INSERT IGNORE no-op
        hg := &statistics.Histogram{ … default single-bucket … }
        blob, _ := EncodeBuckets(hg)
        _, err = util.Exec(sctx, "INSERT INTO mysql.stats_data (table_id, type, hist_id, value) VALUES (?, ?, ?, ?)", …)

Rewrite the read path:

- `HistogramFromStorageWithPriority` (read.go:132): first query `SELECT value FROM mysql.stats_data WHERE table_id=? AND type=? AND hist_id=?`. If a row is returned, `DecodeBuckets` into the histogram and return. If no row, fall back to the existing `SELECT … FROM mysql.stats_buckets WHERE …` and log at INFO once per (table_id, is_index, hist_id) with a rate limit so the message does not flood.

**M3 — Bootstrap loader.** In `pkg/statistics/handle/bootstrap.go`:

- `getTablesWithBucketsInRange` (line 586): instead of `SELECT DISTINCT table_id FROM mysql.stats_buckets WHERE is_index = 1`, union with `SELECT DISTINCT table_id FROM mysql.stats_data WHERE type = 2`. Either a single `UNION` query or two queries merged in Go — two queries are easier to unit-test.
- `genInitStatsBucketsSQLForIndexes` (line 726): run the new-format scan first (`SELECT table_id, hist_id, value FROM mysql.stats_data WHERE type = 2 AND table_id IN (…) ORDER BY table_id`) and then the old-format scan (`SELECT … FROM mysql.stats_buckets WHERE is_index = 1 AND table_id IN (…) AND (table_id, hist_id) NOT IN (new-covered set) ORDER BY table_id`). The "NOT IN" is the tricky bit: collect `(table_id, hist_id)` seen in the new scan into a set, then filter the legacy scan in Go.
- `initStatsBuckets4Chunk` (line 681): today it parses 5 columns per row into one bucket. Split into two helpers: `initStatsBucketsFromLegacyChunk` (current code unchanged) and `initStatsBucketsFromBlobChunk` (parse blob via `DecodeBuckets` into all buckets of the histogram in one call). The caller picks the helper based on which scan the chunk came from.

**M4 — GC, BR, ID migration.**

- `gc.go:161` and `gc.go:259`: issue the DELETE against both tables. Order does not matter; run the `stats_data` delete first so a crash between them leaves the legacy row behind (which is harmless — it will be ignored on next read because the new table is checked first, and it will be deleted on the next GC pass).
- `update.go:154` (`changeGlobalStatsTables`): keep `stats_buckets` in the list; add `stats_data` if it is not already there (check while implementing). The migration SQL for `stats_data` has a different shape (`type` column, no `is_index`), so it may need its own branch.
- BR (`br/pkg/restore/snap_client/systable_restore.go:44`): no code change needed. BR already handles `stats_buckets`; `stats_data` is already in the map via #66303.

**M5 — Unit tests.** Add to `pkg/statistics/handle/storage/`:

- `bucket_codec_test.go` — round-trip coverage (see M1).
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

Step 2 — scaffold codec (M1):

    # create pkg/statistics/handle/storage/bucket_codec.go with EncodeBuckets/DecodeBuckets
    # create pkg/statistics/handle/storage/bucket_codec_test.go with round-trip tests
    go test -count=1 -run TestEncodeBuckets ./pkg/statistics/handle/storage/...
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

- M1 — which codec was chosen (tipb vs. bespoke) and why.
- M7 — `ANALYZE` wall-clock before/after on the reproduction workload, with `EXPLAIN ANALYZE` or server-side timing.
- M8 — PR URL and CI run URL.


## Interfaces and Dependencies

In `pkg/statistics/handle/storage/bucket_codec.go`, define:

    func EncodeBuckets(hg *statistics.Histogram) ([]byte, error)
    func DecodeBuckets(blob []byte, hg *statistics.Histogram) error

`hg` is `*github.com/pingcap/tidb/pkg/statistics.Histogram`. Encoder reads `hg.Buckets[i].{Count, Repeat, LowerBound, UpperBound}` and `hg.NDV`; decoder populates the same fields. Implementation may delegate to `github.com/pingcap/tipb/go-tipb.Histogram` if that proto carries the required fields (decide and record in `Decision Log`).

No new system variable is added.

Downstream consumers:

- `pkg/statistics/handle/storage/save.go` — `saveBucketsToStorage`, `SaveAnalyzeResultToStorage`, `SaveColOrIdxStatsToStorage`, `InsertColStats2KV`.
- `pkg/statistics/handle/storage/read.go` — `HistogramFromStorageWithPriority`.
- `pkg/statistics/handle/bootstrap.go` — `getTablesWithBucketsInRange`, `genInitStatsBucketsSQLForIndexes`, `initStatsBuckets4Chunk`.
- `pkg/statistics/handle/storage/gc.go` — `GCStats`, `deleteHistStatsFromKV`.
- `pkg/statistics/handle/storage/update.go` — `ChangeGlobalStatsID` (via `changeGlobalStatsTables`).

No new dependency is added to `go.mod` if `tipb.Histogram` is reused (already vendored via `github.com/pingcap/tipb`). If a bespoke format is chosen, encoding is pure stdlib `encoding/binary` + `bytes`.
