# `pkg/session/cursor` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for the session cursor tracker and
record a safe package-atomic Rust ownership boundary. Read every Go source,
test, and build artifact before editing; do not substitute the prepared
protocol cursor for the session result-set tracker.

## Completed this batch

1. Inventoried all four artifacts (247 lines): cursor state, concurrent
   tracker/handle implementation, five tests including the create/delete
   stress test, and the five-shard flaky Bazel target. No fixtures, generated
   outputs, benchmarks, fuzz inputs, or platform variants were omitted.
2. Ran the exact Go-master package suite; all five tests passed in 4.827s.
3. Compared the complete package with Rust. Rust's prepared-protocol
   `cursor_state` is a different owner and does not cover session-wide
   `RangeCursor`, handle closure, `StartTS`, or static-recordset/infosync
   integration.
4. Found no safe missing behavior to implement and no Rust-only behavior to
   remove. Recorded the complete inventory, hashes, validation evidence, and
   explicit SEED boundary in `rust/testport/receipts/session_cursor.md`.

## Validation gate

- [x] Complete Go source/test/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master package suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

Session-owned result-set lifetimes, static recordsets, infosync publication,
and prepared protocol cursors remain explicit boundaries in their owning
packages. The repository package loop continues after this receipt; this plan
does not claim whole-repository completion.
