# pd-client TSO parity audit (pinned client-go afa43111)

Audit of `rust/crates/tidb-pd-client/src/tso.rs` + `client/worker.rs`
against the pinned external module
`github.com/tikv/pd/client@v0.0.0-20260805103528-afa43111d149`
(clients/tso/dispatcher.go, stream.go; source resolved from the module
cache — this is the external-pin decision recorded in the journal).

## Fixed this batch (behavior)

1. Batch arithmetic is PLAIN (dispatcher.go:461,483): the proto's
   suffix_bits field is never read by the pinned client-go, so the
   port's `count << suffix_bits` shift is dropped
   (`tso_fallback` stays the terminal monotonicity error).
2. Retry semantics mirror `handleProcessRequestError`
   (dispatcher.go:356-398): EVERY failure is retried until the wait
   deadline expires -- the previous 20-attempt cap and the narrow
   retryable-error set are gone; the terminal error is the deadline
   miss (Go's ctx.Err() analog).
3. Retry interval is Go's `constants.RetryInterval` = 500ms uniformly
   (was 100ms with a free first retry).

The monotonicity violation (`tso_fallback`) stays TERMINAL: Go panics
inside the dispatcher (dispatcher.go:522-536); this crate surfaces it
as an error and keeps the process alive -- a documented narrowing, not
a retryable failure. Regression updates: the three suffix-shift tests
were rewritten to the pinned plain arithmetic, and the
malformed/fallback integration test now pins terminal-fallback +
retry-until-deadline + terminal-timeout.

## Verified matching

TS composition `physical<<18 | logical`; batch last-timestamp recovery
(`logical - count + 1`); per-waiter distribution; count echo
validation; request shape (Header{ClusterId}, Count, empty dc_location);
single-in-flight default; stream re-establishment and endpoint-change
handling; shutdown plumbing.

## Documented narrowings (no action)

- Rust validates responses the pinned Go reads blindly (cluster-id
  match, negative physical, logical range, batch underflow, zero TSO).
- Go's optional >1 RPC concurrency with batching delay is unported
  (default 1 matches).
- Leader-change handling uses one shared membership-refresh path.
- Go panics the process on fallback; Rust errors (above).

## Validation

- `cargo test -p tidb-pd-client` (26 lib + 44 integration),
  `cargo fmt`, `git diff --check`, `make lint`.
