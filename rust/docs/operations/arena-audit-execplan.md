# Restore complete `pkg/util/arena` behavior in Rust

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.
Current Go authority: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02).

## Purpose / Big Picture

Go `pkg/util/arena` reduces allocation cost by returning byte slices backed by
one reusable allocation. Rust currently advances an arena offset but returns a
fresh `Vec<u8>` every time, so it preserves superficial lengths and capacities
without the package's actual reuse behavior. This change introduces a safe
byte-slice handle that shares the allocator's backing storage and proves reset
reuses the same bytes.

## Progress

- [x] (2026-08-29) Read all four pinned Go artifacts and the complete Rust
  owner, manifest, prior plan, export, and consumer surface.
- [x] (2026-08-29) Confirmed there are no Rust production consumers and the
  workspace forbids unsafe code.
- [x] (2026-08-29) Selected a safe shared-storage slice handle rather than the
  non-reusing `Vec<u8>` approximation or package deletion.
- [x] (2026-08-29) Added the reset-reuse regression; it failed with actual
  `[0]`, expected `[9]` against the owned-`Vec` implementation.
- [x] (2026-08-29) Implemented shared backing, removed three non-Go tests and
  the stale semantic manifest, and added the package receipt.
- [x] (2026-08-29) Passed the Go package, focused three-test Rust owner slice,
  complete `tidb-util` suite, all-target check, formatting, and diff checks.
- [x] (2026-08-29) Self-reviewed the final package diff and pushed its atomic
  implementation batch.
- [x] (2026-09-02) Revalidated the package against current Go master
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`; source is unchanged and the
  exact detached Go package probe passes.

## Surprises & Discoveries

- Observation: the previous plan knowingly accepted the package's defining
  allocation behavior as an integration difference.
  Evidence: `arena.rs` says returned buffers do not share storage and its final
  test expects zero after reset where Go returns the prior byte.

- Observation: no Rust production file consumes this API.
  Evidence: repository search finds the symbols only in `arena.rs`, its stale
  semantic manifest, and audit documentation.

- Observation: warnings-as-errors Clippy has unrelated current-branch findings.
  Evidence: all-target Clippy stops in `tidb-mysql/src/consts.rs`; owner-only
  Clippy additionally reports existing encrypt, master-key, and table-selector
  lints. Allowing only those named lints makes the arena owner check pass.

## Decision Log

- Decision: Replace `Vec<u8>` results with a cloneable `ArenaBytes` descriptor
  over `Rc<Vec<Cell<u8>>>`.
  Rationale: `Cell<u8>` permits safe byte mutation; `Rc` preserves backing
  lifetime and copied-slice sharing without unsafe code. Independent start,
  length, and capacity fields reproduce a Go slice descriptor.
  Date/Author: 2026-08-29 / Codex

- Decision: Keep Rust inputs as `usize`.
  Rationale: negative Go `int` inputs are unrepresentable at this native Rust
  boundary; all valid nonnegative calls retain source behavior.
  Date/Author: 2026-08-29 / Codex

## Outcomes & Retrospective

The defining shared-backing gap is fixed. The Go oracle, complete owning-crate
suite, all-target compilation, formatting, and diff checks pass. The atomic
implementation batch is published on `hparser-integration`.

## Context and Orientation

The pinned package contains `arena.go`, `arena_test.go`, `main_test.go`, and
`BUILD.bazel`. A fitting allocation returns `arena[off:off:off+capacity]` and
advances `off`; fallback returns fresh storage. `AllocWithLen` allocates before
reslicing. `Reset` changes only `off`, leaving bytes untouched. `StdAllocator`
always returns fresh zeroed storage. Rust ownership is
`rust/crates/tidb-util/src/arena.rs`, exported by `src/lib.rs`.

## Plan of Work

First change the reset test to expect stale-byte reuse and capture its failure
against `Vec`. Then define `ArenaBytes`, change `Allocator` to return it, and
store the simple arena as shared cells. Preserve strict-fit fallback,
allocate-before-length-check order, zeroed standard allocation, and descriptor
cloning. Retain the two exact Go test translations plus the reset regression;
remove the other supplemental tests and stale semantic manifest.

## Concrete Steps

From repository root:

    go test ./pkg/util/arena

From `rust`:

    cargo test -q -p tidb-util arena::tests --lib --locked -- --test-threads=1
    cargo check -p tidb-util --all-targets --locked
    cargo fmt --all --check

From repository root:

    git diff --check

## Validation and Acceptance

Go and Rust owner tests must pass. Rust must retain the two source tests and
prove a byte written into a fitting allocation is read from the same range after
`Reset`. Exact-fit and oversize requests remain independent and zeroed;
invalid length still panics after offset advance; all targets compile without
unsafe code.

## Idempotence and Recovery

All commands are safe to rerun. There is no production consumer, so compile
failures are confined to the owner tests. Do not restore the knowingly
non-reusing implementation as a parity claim.

## Artifacts and Notes

Pre-fix regression: failed with actual `[0]`, expected `[9]`.

Post-fix focused owner tests: 3 passed.

Complete owner evidence: 575 passed, 3 ignored; all integration and
documentation tests passed. The Go package and all-target check passed.

Scoped Clippy passed with allowances only for six pre-existing findings outside
`arena.rs`; unrestricted Clippy remains blocked by unrelated branch warnings.

## Interfaces and Dependencies

`Allocator::alloc` and `alloc_with_len` return `ArenaBytes`. `ArenaBytes`
provides `len`, `is_empty`, `capacity`, `set_len`, `get`, `set`, and `to_vec`.
`SimpleAllocator::new` and `StdAllocator` remain. No dependency or unsafe-code
exception is added.

Plan revision note: replaced the prior plan because it explicitly accepted
missing shared-backing behavior, contrary to the strict Go-parity goal.
