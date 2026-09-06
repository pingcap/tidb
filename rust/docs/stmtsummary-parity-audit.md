# pkg/util/stmtsummary parity audit (baseline a85e0fd5df)

Full-package audit of `pkg/util/stmtsummary` (+ `v2/`) against
`rust/crates/tidb-stmtsummary`, read file-by-file at the baseline commit.

## Fixed this batch (behavior-breaking)

- `reader.rs`: the current `STATEMENTS_SUMMARY_EVICTED` rollup row is now
  hidden once its interval is stale (`seElement.beginTime <
  beginTimeForCurInterval` -> no row), matching Go reader.go:214-220 --
  evicted summaries are lazily expired like regular ones. Pre-fix failing
  baseline: the Go regression test
  `TestCurrentRowsExcludePreviousIntervalEvictedOther` (issue coverage for
  the stale rollup) failed with the previous interval's rollup exposed;
  after the fix it passes as a ported test.

## Open items (feature-sized ports, not yet claimed)

1. `v2/reader.go` (951 lines) is not ported: `MemReader`,
   `HistoryReader`, `stmtFile`/`stmtScanWorker`/`stmtParseWorker` persisted
   log scanning, `StmtTimeRange`. Under persistent mode
   (`tidb_stmt_summary_enable_persistent=ON`) there is no
   statements_summary/history read path. Documented in
   `src/lib.rs` and `src/v2/mod.rs` headers.
2. `v2/logger.go` rotation is unported: `FileStmtLogWriter` is append-only;
   `Config.file_max_size/days/backups` are carried but unused, and the
   lumberjack/zap sink plus `StmtSummaryEvictedLogCounter` wiring are
   trait-narrowed behind `EvictedLogMetricsSink` (Noop default). Persistent
   mode should stay gated off until this lands.

## Accepted narrowings / cosmetics (documented)

- Evicted-count row BEGIN/END render in UTC where Go renders in the
  process-local zone (documented in evicted.rs header).
- `formatSQL` truncation backs off to the nearest UTF-8 boundary (Go can
  split a character); byte length reported matches Go.
- v1/v2 proxy drops the `SetMaxStmtCount` capacity error and saturates
  int conversions where Go wraps; both unreachable via sysvar validation.
- `StmtSummary::evicted()` reads window begin under the same lock (strictly
  more atomic than Go's post-unlock read).
- Rust-only test seams (`normalized_sql_for_digest`, `set_mock_now`,
  `with_sinks`); no TiDB-observable surface added.

## Verified matching (one line each)

- Digest key composition; StmtExecInfo -> stats mapping (every counter
  incl. commit/backoff/IA/RU v1+v2/network/CPU/storage); v2 StmtRecord::add
  and Merge; history buffer trim/lazy-expire/collect ordering; averages
  (truncating division); interval rotation and default options;
  SetGroupByUser flip; Clear/ClearInternal; eviction rollup map->record->
  evicted order and window reset; AddEvicted interval matching/trim cursor;
  LRU order semantics; v1 reader ~150 columnValueFactory arms; v2 record
  JSON field order/omitempty/formats; v2 column factories; v2 lifecycle
  (rotate loop, async persist, evictedCh cap 1024/batch 64/flush 100ms/
  report 30s, two evicted aggregates, all 11 proxy dispatch rules);
  tidb_stmt_summary_* sysvar defaults/ranges/validation.

## Validation

- `cargo test -p tidb-stmtsummary --all-targets`: 52 tests pass (51 prior +
  the ported stale-evicted regression).
- `cargo fmt -p tidb-stmtsummary`, `git diff --check`.
