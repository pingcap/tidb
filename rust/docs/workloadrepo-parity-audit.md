# tidb-workloadrepo parity audit

Go source: `pkg/util/workloadrepo` @ a85e0fd5df (`worker.go` 416, `snapshot.go`
300, `table.go` 192, `housekeeper.go` 175, `sampling.go` 109, `utils.go` 105,
`const.go` 52). Rust: `rust/crates/tidb-workloadrepo` (`lib.rs`, `src/tests.rs`
mirroring Go's test names).

The port expresses Go's package-external authorities as seams: `owner.Manager`
from `tidb-owner`, an etcd `RepositoryStore` (get/create/CAS/watch) over
`EtcdClient`, a `SessionPool`/`RepositorySession` internal-SQL seam, and the
instance ID passed at construction in place of `infosync.GetServerInfo`.

## Fixed divergences (this batch)

1. **`%?` string escaping was not Go's.** `buildCreateQuery` renders column
   comments through `MustFormatSQL("%?")` -> `appendSQLArgString` ->
   `escapeBytesBackslash` (`pkg/util/sqlescape/utils.go:40-80`): NUL->`\0`,
   LF->`\n`, CR->`\r`, ctrl-Z->`\Z`, `'`->`\'`, `"`->`\"`, `\`->`\\`. Rust
   `quote_string` only backslash-escaped `\` and doubled `'`, so comments
   containing newlines/control chars produced different DDL bytes and a
   different (still-valid) quoting style. Fixed by mirroring the Go escape
   table; regression `test_comment_string_escaping_matches_go` pins both
   vectors through the public `build_create_query`.
2. **Numeric-variable rejection message class.** Go's
   `setRetentionDays`/`changeSamplingInterval`/`changeSnapshotInterval`
   reject unparseable input with `errWrongValueForVar.GenWithStackByArgs(name,
   value)` (errno 1231, "Variable '...' can't be set to the value of '...'");
   Rust used the 1232 "Incorrect argument type" wording and dropped the value.
   Both paths are defensively unreachable behind int-type validation, but the
   in-function messages are now byte-identical; regression
   `test_interval_rejection_messages_match_go`.

## Verified equal

- Constants: owner/prompt/snap-id key paths, `snapshotRetries` 5, defaults
  5/3600/7, `HIST_SNAPSHOTS`, `WORKLOAD_SCHEMA`, the three error texts
  ("etcd client required for workload repository", "Workload repository is not
  enabled", "Snapshot initiation failed"), `errKeyNotFound` modeled through
  the absent/empty key arm (Go's `etcdGet` returns "" for both).
- Partition machinery: `generatePartitionName`/`parsePartitionName`
  (`p20060102`), `generatePartitionRanges` (last-partition max, today+1 and
  today+2, `VALUES LESS THAN (TO_DAYS('...'))`, allExisted), day-overflow
  normalization, `calcNextTick` 2am-local with Go's skipped-wall-clock
  resolution documented in `date_at_hour`.
- SQL builders: `buildCreateQuery`/`buildInsertQuery` byte-shaped (snapshot
  vs sampling SELECT arms, the no-space `WHERE` quirk, `%n` backtick doubling
  == `quote_identifier`), the metadata `HIST_SNAPSHOTS` CREATE text,
  `queryMaxSnapID`/`upsertHistSnapshot`/`updateHistSnapshot` statements
  (UPSERT and `COALESCE(CONCAT(ERROR, %?), ERROR, %?)`) byte-identical.
- Worker lifecycle: `start` double-enable guard, enabled-before-sesspool
  early return, etcd-required refusal, `SetHistoryEnabled(false)` on start
  and `true` on stop, `setRepositoryDest` table/else switch, package-level
  `takeSnapshot` mutex + error masking, `stop` ordering (cancel, join,
  re-enable history, close owner).
- `startRepository` loop: campaign-then-1s-poll, owner-gated
  `createAllTables`, `checkTablesExists` (last partition after tomorrow),
  then the three loops and exit.
- Snapshot path: watch coalescing to the last event (Go's documented
  "take a snap for the last one"), parallel per-table capture, join errors
  into the ERROR column, `takeSnapshot` 5-retry loop with the
  empty-key recovery from `queryMaxSnapID`, create-vs-CAS on `snapIDKey`,
  error wrappings ("cannot get current snapid", "could not insert into
  hist_snapshots", "cannot update current snapid to N").
- Sampling path: interval 0 stops the loop (Go `Ticker.Stop`), parallel
  capture with instance ID only.
- Housekeeper: next-tick at 2am, owner-only, and Go's quirk that a non-owner
  or failed tick leaves the timer unarmed forever (`next = None`) -- ported
  deliberately with a doc comment.
- Sysvar surface: the four `tidb_workload_repository_*` catalog entries match
  Go's `RegisterSysVar` scope/type/default/min/max exactly; `dest` validation
  routes to `validate_dest` (lowercase, ''/table, 1231 text), and the four
  SetGlobal hooks route to the worker methods (`variables.rs`), with
  `take_snapshot` wired at the executor seam (`show.rs`) as Go's
  `executor.TakeSnapshot`.

## Documented narrowings (intentional, structural)

- Prometheus/logging: Go logs every transition and the failpoints
  (`FastRunawayGC`) are test injection; none ported.
- Panic isolation: Go's `wg.RunWithRecover` keeps the other two loops alive
  when one panics; a Rust loop panic unwinds the scoped threads and ends the
  prestart thread (worker stays enabled until an explicit stop/start).
- `setRepositoryDest` holds Go's worker lock across compare+start/stop; Rust
  matches on the value outside the lock, so two racing SETs from different
  sessions can execute start/stop in inverted order (final state converges
  on the last lock winner, not the last SET).
- Go's housekeeper grabs one session for its lifetime; Rust checks out a
  session per tick. `runQuery`'s `DrainRecordSet(256)` batch lives inside the
  session seam. `StopRepository`'s pool-nil "never restart" guard is a
  server-wiring concern pending the connection-loop tier.
- `etcdCreate`/`etcdCAS` false outcomes lose Go's
  "failed to create/update etcd" wrapping inside the retry loop (both retry;
  the wrapped text is not observable at this tier).

## Validation

- `cargo test -p tidb-workloadrepo` --lib: 17 passed / 0 failed (15
  pre-existing + 2 new regressions; message and escaping pins verified
  against the pre-fix code paths).
- fmt, clippy (no warnings in-crate), `git diff --check`, `make lint`.
