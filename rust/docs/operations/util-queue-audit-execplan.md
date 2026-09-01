# `pkg/util/queue` parity audit ExecPlan

## Objective

Keep the complete Go-master queue package aligned with its native Rust owner,
including the circular-buffer retention/growth contract and Go's discardable
return-value surface.

## Completed

- Read all three current Go-master artifacts in full: `BUILD.bazel`,
  `queue.go`, and `queue_test.go` (198 lines total, nine declarations, and
  four ordered source subtests). Confirmed no fixtures, generated/platform
  variants, benchmarks, fuzz targets, examples, or nested packages exist.
- Refreshed the authority to `origin/master`
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`; the three artifacts remain
  byte-identical to the prior package pin and exact detached worktree.
- Revalidated current and exact detached Go-master package tests; both pass.
- Preserved the four Go-named queue tests and the source-derived retained-slot
  regression. The Rust owner keeps `Clear` constant-time like Go, avoids
  dropping backing slots eagerly, and preserves zero-value versus
  `NewQueue(0)` behavior and wrapped growth.
- Removed the unused divergent executor duplicate and Rust-only head/tail
  inspection surface in the earlier atomic implementation batch.
- Removed Rust-only `#[must_use]` diagnostics from `Queue::new`, `len`,
  `is_empty`, and `cap`. The focused `#[deny(unused_must_use)]` regression
  failed with four errors before the edit and passes afterward.

## Validation gate

- [x] Complete inventory and current-authority hashes recorded in
      `rust/testport/receipts/util_queue.md`.
- [x] Current and exact Go-master package tests pass.
- [x] Focused Rust queue tests and owner checks pass.
- [x] Ready formatting, clean-tree repository lint, and diff checks pass.
- [ ] Push this receipt/ExecPlan batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Next boundary

Any future queue change must preserve FIFO order, zero-value initialization,
strict growth behavior, panic-on-empty-pop, and retained-slot semantics. Keep
the queue owner dependency-closed; do not reintroduce a consumer-specific
duplicate.

Plan revision note (2026-09-02): refreshed the complete package at current Go
master, recorded the four return-diagnostic removals and their
fail-before/fail-after regression, and updated Ready evidence.
