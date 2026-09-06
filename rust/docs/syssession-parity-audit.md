# pkg/session/syssession parity audit (baseline a85e0fd5df)

Full-file audit of Go `pkg/session/syssession` (pool.go, session.go) against
`rust/crates/tidb-syssession/src/lib.rs`.

## Fixed this batch

- The `WithForceBlockGCSession` internal-session registration loop now breaks
  under `tidb_util::intest::IN_TEST` (both the trait default and the
  `SqlExecError` impl), mirroring Go's `intest.InTest` break in pool.go:304-
  312. Go's forcing failpoint (ForceBlockGCInTest) has no hook here, so the
  forced-retry arm is always false; production semantics (retry until
  cancelled) are unchanged.
- Owner-check error messages carry the identity suffix Go appends via
  `objectStr` (`caller: Owner(<id>), owner: Owner(<id>)` / `<nil>`), at both
  the TransferOwner and EnterOperation sites; the owner id is this crate's
  identity analog of Go's `%p`.
- The `txn_valid` source-error stringify is annotated as the crate's error
  boundary (Go returns the raw error; `SysSessionError` is message-only).

## Verified matching (one line each)

- PoolMaxSize 1024^3; capacity normalization; "session pool closed" text.
- Get reuse-then-factory incl. onBecameOwner failure/panic close; owner
  transfer with close-on-failure; Put's not-owner no-op, avoidReuse close,
  pending-txn close, reset-then-close, closed/full-pool close and their
  ordering; CloseUnlessReturned as Go's `returned` defer.
- WithSession (close on error/panic) and WithForceBlockGCSession (contains
  check, 100 ms retry, cancel error, same lease logic).
- Pool Close idempotent + drain; IsClosed.
- Session: owner-scoped idempotent Close, AvoidReuse, WithSessionContext,
  six executor methods entering/exiting the same threadUnsafe operation.
- TransferOwner: closed/owner/early-return/inUse rejection/noop temp owner/
  close-on-hook-failure with the "TransferOwner error, opSeq: N, " prefix.
- EnterOperation/ExitOperation: unsafe-race rejection, inUse accounting,
  panic -> markAvoidReuse + re-panic, deferred close when closed-in-use.
- CheckNoPendingTxn texts; owner hooks (session registers internal, pool
  owner noop); no separate Retain/Release API on either side (ownership
  transfer + inUse is the model); no metrics on either side.
- Test hooks incl. Go's accidental "ResetSctxForTestcaller" concatenation.

## Accepted narrowings (documented at sites)

- Rust `Session::clone()` duplicates the owner id, so two clones could both
  act as owner where Go's uncopyable `*Session` forbids a second proxy;
  Go-shaped call patterns never hit this.
- Invalid pool capacity normalizes silently (Go's `intest.Assert` panics in
  test builds); Put-time anomalies close without a test-time assert; the
  exit-path diagnostics (owner-transferred, race-at-exit, resign errors)
  are dropped rather than logged; no `Inuse()` accessor.

## Validation

- `cargo test -p tidb-syssession --lib` (3 consecutive runs), `cargo fmt`,
  `git diff --check`, `make lint`.
