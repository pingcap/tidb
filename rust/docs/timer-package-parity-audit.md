# pkg/timer parity audit (baseline a85e0fd5df)

Full-file audit of the Go package `pkg/timer` (api, tablestore, runtime,
metrics) against `rust/crates/tidb-timer`. Three parallel read-only passes
read every production Go file at the baseline commit and every Rust file in
full; behavior divergences found were fixed in the same batch.

## Fixed this batch (behavior-breaking)

1. `runtime/worker.rs` — after a `VersionNotMatch` update failure the
   post-failure `GetByID` error now reassigns the pending error (Go
   worker.go:349-359), so an update=VersionNotMatch + fetch=NotExist sequence
   returns `TimerMetaChangedResponse(nil)` (timer dropped from cache) instead
 of retrying forever.
2. `runtime/mod.rs` — `start()` now holds the ctx-state lock across the
   running check and the ctx init (Go holds `rt.mu` across check + `initCtx` +
   `wg.Run`), so two concurrent `Start` calls cannot both spawn the loop.
3. `cron.rs` — robfig v3.0.1 parity fixes:
   - a stepped star (`*/n`, n>1) clears the star bit, restoring Go's
     dom/dow OR-vs-AND day-match rule (`0 0 */2 * 1` = odd days OR Mondays);
   - descriptors are case-sensitive (`@YEARLY`, `@EVERY 1h` are rejected with
     `unrecognized descriptor: ...`, matching robfig's raw-text switch);
   - `@every` truncates sub-second nanos for delays >= 1s and rounds sub-second
     delays up to 1s (robfig `Every`);
   - empty comma segments are skipped (`strings.FieldsFunc` semantics);
   - `getRange` mirrors robfig's validation order and messages (bounds after
     endpoint parsing, `too many hyphens`, `negative number (%d) not allowed`
     via the step, exact wording with the expression context).
4. `mem_store.rs` — overflow senders now observe a notifier-level closed flag
   (stands in for Go's notifier context cancellation), so `close()` cannot
   block forever on a backed-up watcher whose receiver stopped draining.
5. `table_store/sql.rs` — `TimerExt::unmarshal` is strict like Go's
   `json.Unmarshal`: a mistyped `tags`/`manual`/`event` member errors the whole
   decode (Go `UnmarshalTypeError` fails the enclosing `List`) instead of
   yielding a silently partial record. Explicit JSON nulls still read as
   absent, matching Go pointer semantics.

## Verified matching (one line per surface)

- `api/timer.go` ↔ `timer.rs`: policy types, interval/cron next-event,
  ManualRequest state, EventExtra, TimerSpec.Validate branch order and error
  strings, NextEventTime.
- `api/store.go` ↔ `store.rs`: OptionalVal, TimerCond/TimerUpdate/Operator
  semantics, field-set order, watch event bits, And/Or/Not.
- `api/mem_store.go` ↔ `mem_store.rs`: create guard order, uuid-hex ids,
  versioning, normalizeTimeFields, duplicate-key errors, notify fan-out.
- `api/client.go` ↔ `client.rs`: all With* options, namespace defaulting,
  manual trigger retry (5x, 1000ms backoff, 2-minute timeout), CloseTimerEvent
  field-forbidden errors, watermark defaulting.
- `api/error.go`/`hook.go` ↔ `error.rs`/`hook.rs`: sentinels and hook shapes.
- `tablestore/sql.go` ↔ `table_store/sql.rs`: INSERT (16 columns,
  FROM_UNIXTIME watermark/event handling, JSON_MERGE_PATCH), SELECT (19
  columns), condition/UPDATE builders, TIMER_EXT marshal order and omitempty
  semantics.
- `tablestore/store.go` ↔ `table_store/store.rs`: row->record mapping incl.
  session-timezone resolution and `TIDB` fallback, txn update constraints
  (EventID/Version/timezone/sched-policy), session tz hygiene
  (ROLLBACK-first, SET UTC, restore with AvoidReuse), delete/create flows.
- `tablestore/notifier.go` ↔ `notifier.rs`: etcd key prefix, payload shape
  with Go-compatible HTML escaping, 20s timeout / 1s min-interval / 60s lease,
  drain-then-put batching without dedup, keep-alive reset.
- `runtime/cache.go` ↔ `runtime/cache.rs`: version/location guards, 2999
  sentinel, IDLE/TRIGGER next-try rules, resort index math.
- `runtime/runtime.go`/`worker.go` ↔ `runtime/mod.rs`/`worker.rs`: all
  intervals/batch sizes/backoffs equal (1m/60s/1s/5s/10s, 128-slot channels,
  10s default retry, 1m chan-block), full loop mechanics, manual-request
  state machine, retry loop, shutdown ordering.

## Accepted narrowings (documented, no observable in-package effect)

- `TZ=`/`CRON_TZ=` spec prefix (robfig-accepted) is rejected; the timer zone
  always comes from `TimerSpec.TimeZone`, which TiDB itself writes.
- `pkg/timer/metrics` prometheus counters (`tidb_server_timer_event_count`)
  are replaced by in-process atomics; no prometheus registry surface exists in
  the Rust workspace for this package.
- Log fields (retryTimerIDs/retryAfter/requestID/...) are narrower than Go's
  zap fields; messages and severities match where present.
- json.rs does not escape U+2028/U+2029 and is slightly more lenient on number
  forms than `encoding/json`; both only affect byte-level equality of encoded
  documents, not values.
- notifier instance key uses 32-hex uuids (Go: 36-char hyphenated);
  uniqueness contract unchanged.
- Go panic-path robustness narrowings (nil guards, closed-channel sends,
  nil HookFactory) are unrepresentable states in the Rust shapes.

## Validation

- `cargo test -p tidb-timer`: 16 lib + 49 integration tests pass (8 cron unit
  tests incl. the new robfig-parity regressions; new
  `test_timer_ext_unmarshal_strict_like_go`).
- `cargo fmt -p tidb-timer`, `git diff --check` clean.
- `make lint` (go toolchain 1.25.10 cache) passes.
