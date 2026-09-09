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

## Ported in a follow-up batch

`v2/reader.go` (951 lines) is now ported whole in
`src/v2/reader.rs`: `MemReader`, `HistoryReader` with the
scan/parse worker pipeline (unbuffered file dispatch, concurrent/2 scan
workers that then join parsing, monitor over the inner error channel),
`stmtChecker` with time-range filtering, `stmtFile`/`stmtFiles` with the
pinned-active-inode rotation deduplication, the persisted-record JSON
parser mirroring `record`'s field names and `encoding/json` leniency, and
the ported `v2/reader_test.go` regressions (61 crate tests total).

Port notes (each commented at its site):
- Go closes channels explicitly (`close(linesCh)`); Rust closes by
  dropping senders, so a scan worker must drop its lines sender before it
  becomes a parse worker -- the exact deadlock the ported Go tests
  exercise.
- `os.SameFile` deduplication resolves metadata at walk time (Go's lazy
  `os.DirEntry.Info`), with an injectable failure for the ported test.
- `parseEndTs` keeps Go's quirk of matching the config prefix against the
  file's BASE name, so only a relative config filename yields a usable
  rotated-file end timestamp.
- The KV worker in `tidb-pd-client` is unrelated here; all etcd notes in
  the earlier schemaver receipt.

## Ported in the follow-up rotation batch

`v2/logger.go`'s file sink is now mirrored by
`RotatingFileLogWriter` (`v2/stmtsummary.rs`), wired into
`new_stmt_summary` where Go's `newStmtLogStorage` builds its logger:
size-based rotation past `Config.file_max_size` (MB), backups named
`<base>-<local timestamp><ext>` in the exact format the v2 reader's
`parseEndTs` parses (verified by a cross-module test), and pruning by
`file_max_backups` count and `file_max_days` age (zero disables each
dimension), matching lumberjack's `removeOldBackups`. Regression tests
cover size rotation with content recovery, reader interop, count pruning,
and age pruning. The zap-core plumbing (no-op encoder, `WrapCore`) remains
an ecosystem boundary absorbed by the `StmtLogWriter` trait; the evicted
log metrics sink stays behind `EvictedLogMetricsSink`.

Persistent mode remains gated off at the server seam, as before.

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

- `cargo test -p tidb-stmtsummary --all-targets`: 64 lib tests pass
  (61 prior + the 4 rotation regressions, re-stabilized).
- `cargo fmt -p tidb-stmtsummary`, `git diff --check`, `make lint`.
