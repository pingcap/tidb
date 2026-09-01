# Restore signed-length parity for `pkg/util/bitmap`

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

The Rust concurrent bitmap matches ordinary Go calls but deliberately replaces
Go's signed length domain with `usize` and rejects overflow. Strict package
parity requires Go `int` to remain `isize`, source wrapping during segment-count
calculation, and the same constructor and Reset outcomes at negative and
maximum lengths. This change restores that behavior while retaining the exact
three Go concurrency/reset tests.

## Progress

- [x] (2026-08-29) Read all four pinned Go package artifacts and the complete
  Rust owner, prior plan, manifest, tests, exports, and consumer search.
- [x] (2026-08-29) Identified the signed-length sanitization as a behavior gap;
  no Rust production consumer exists.
- [x] (2026-08-29) Added the signed-boundary regression; it failed to compile
  because construction and Reset required `usize`.
- [x] (2026-08-29) Implemented source-width arithmetic and removed two non-Go
  tests, diagnostics, and the retired manifest.
- [x] (2026-08-29) Passed the Go package and direct probe, focused debug and
  release suites, all-target compilation, formatting, and diff checks.
- [x] (2026-08-29) Self-reviewed the final diff and pushed its atomic
  implementation batch.
- [x] (2026-09-02) Revalidated the complete package against current Go master
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`; the exact detached Go package
  test passes and source remains unchanged.

## Surprises & Discoveries

- Observation: the old plan explicitly preferred deterministic rejection over
  Go behavior.
  Evidence: Rust's `segment_len` rejects values above `isize::MAX - 31`, while
  Go wrapping makes `NewConcurrentBitmap(-1)` inert, makes `-32` and `MaxInt`
  construction panic, and lets `Reset(MaxInt)` create a malformed logical
  length over retained storage.

- Observation: there is no Rust production consumer to migrate.
  Evidence: repository search finds `ConcurrentBitmap` only in its owner,
  manifest, and audit documentation.

## Decision Log

- Decision: Represent Go `int` as `isize` for bitmap lengths and indexes and
  use `wrapping_add(31) >> 5` exactly.
  Rationale: this reproduces source arithmetic and branches on every platform
  width instead of substituting a safer but different contract.
  Date/Author: 2026-08-29 / Codex

- Decision: Retain exclusive access for the methods named `unsafe_*`.
  Rationale: it encodes the source's documented no-concurrent-access
  precondition without changing valid single-owner results.
  Date/Author: 2026-08-29 / Codex

## Outcomes & Retrospective

Signed-length parity is restored and the implementation batch is published on
`hparser-integration`; this refresh keeps the current Go-master authority
explicit.

## Context and Orientation

The pinned package consists of `concurrent.go`, `concurrent_test.go`,
`main_test.go`, and `BUILD.bazel`. It stores 32 bits per segment, numbers the
most-significant bit first, uses sequentially consistent atomic load/CAS for
`Set`, clears or grows on Reset, clones independently, and reports struct plus
segment-capacity bytes. Rust owns it in
`rust/crates/tidb-util/src/bitmap.rs`.

## Plan of Work

First replace the old oversized-rejection test with a source signed-boundary
regression and capture its type mismatch against the `usize` constructor.
Then change length and index APIs to `isize`, reproduce source wrapping and
negative segment allocation panics, and preserve Reset's negative-segment
reuse branch. Remove the other two supplemental tests, Rust-only diagnostics,
the retired semantic manifest, and stale audit claims. Retain the three exact
Go tests plus the required signed-boundary regression.

## Concrete Steps

From repository root:

    go test ./pkg/util/bitmap

From `rust`:

    cargo test -q -p tidb-util bitmap::tests --lib --locked -- --test-threads=1
    cargo test -q -p tidb-util bitmap::tests --lib --release --locked -- --test-threads=1
    cargo check -p tidb-util --all-targets --locked
    cargo fmt --all --check

From repository root:

    git diff --check

## Validation and Acceptance

The three Go tests and their Rust translations pass. The regression proves
`new(-1)` is inert, `new(-32)` and `new(isize::MAX)` panic, and Reset to
`isize::MAX` retains existing storage and later panics when an apparently
in-range bit addresses a missing segment. Debug and release behavior agree.

## Idempotence and Recovery

All commands are safe to rerun. No production consumer exists, so signature
changes are confined to owner tests. Do not restore deterministic overflow
rejection as a parity claim.

## Artifacts and Notes

Pre-fix regression: E0308 showed `new` and `reset` required `usize`.

Go probe: `new(-1)` inert; `new(-32)` and `new(MaxInt)` panic;
`reset(MaxInt)` succeeds and the following `set(32)` panics.

Post-fix focused suites: 4 passed in debug and 4 passed in release.

## Interfaces and Dependencies

`ConcurrentBitmap::new`, `reset`, `set`, `unsafe_set`, and `unsafe_is_set` use
`isize`. The existing atomic representation, clone behavior, and memory
accounting remain. No dependency or unsafe-code exception is added.

Plan revision note: replaced the prior plan because its explicit invalid-input
narrowing conflicts with the strict Go-behavior goal.
