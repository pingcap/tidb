# `pkg/sessionctx/sysproctrack` parity audit ExecPlan

## Objective

Inventory the process-tracking interface package, compare every method with
Rust's session/executor/server callback seams, and record the boundary without
inventing a concrete tracker implementation.

## Completed this batch

1. Read both Go-master artifacts (48 lines): the BUILD target and all
   `TrackProc`/`Tracker` methods. No tests, fixtures, generated, benchmark,
   fuzz, platform, or generator artifacts exist.
2. Ran the exact Go-master package compile/test command; it passed with no test
   files.
3. Compared the interfaces with Rust's `TrackSysProc`, `UnTrackSysProc`,
   `ExecOptionWithSysProcTrack`, server guard, and auto-analyze callback
   owners. Rust preserves the callback contract while process-map operations
   remain outside this interface package.
4. Found no Rust-only behavior to remove and no safe package-local behavior to
   implement. Recorded exact hashes, the empty Go-master delta, and the
   explicit boundary in `rust/testport/receipts/sessionctx_sysproctrack.md`.

## Validation gate

- [x] Complete Go-master production/build inventory and Rust comparison.
- [x] Exact Go-master package compile/test passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch latest refs, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify remote `rev-list` is `0 0`.

## Remaining boundaries

Faithful process-list parity requires the session-manager concrete tracker,
concurrent map ownership, process cancellation, and SQL `KILL` integration.
Those owners belong to `pkg/session/sessmgr` and server runtime; this plan
does not claim whole-repository parity.
