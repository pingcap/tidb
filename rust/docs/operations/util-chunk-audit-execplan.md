# `pkg/util/chunk` parity audit ExecPlan

This living ExecPlan records the complete Go-package audit, the
`UsedMemoryUsage` restoration, and the bounded Rust-only return-contract
correction. The repository-wide rolling audit continues after this package.

## Purpose / Big Picture

Keep the Rust `tidb-chunk` owner and the Go `pkg/util/chunk` package aligned at
the requested Go `master` authority. `Chunk.MemoryUsage` reports retained
capacity; `Chunk.UsedMemoryUsage` reports currently occupied buffer lengths.
Both contracts are needed by memory-accounting consumers and must remain
distinct after reset/reuse.

## Progress

- [x] (2026-09-02) Read and inventoried all 29 package artifacts at
      `origin/master` `c6054025ed4c32ab3672a2a24ea46892714d21ec`: 11,342 Go
      lines across production files, tests, `main_test.go`, and `BUILD.bazel`.
      There are no package docs, fixtures, generated inputs/outputs, platform
      variants, benchmarks, or additional harnesses.
- [x] (2026-09-02) Compared every Go-master artifact with the hparser branch;
      the only delta is `Chunk.UsedMemoryUsage` and its three assertions in
      `TestChunkMemoryUsage`. The Rust `tidb-chunk` owner already implements
      the equivalent aggregate and per-column length accounting.
- [x] (2026-09-02) Added the missing Go method and restored the source-shaped
      memory regression covering initial capacity, growth, and reset.
- [x] (2026-09-02) Demonstrated the regression failed before the production
      method existed (undefined method in a detached pre-fix worktree), then
      passed with the fix under the canonical failpoint wrapper.
- [x] (2026-09-07) Re-read the complete Rust owner inventory (46 tracked
      artifacts, 25,231 lines) and removed fourteen Rust-only `#[must_use]`
      diagnostics from Go-shaped allocator and iterator constructors/accessors.
      The source regression failed before the edit with exactly fourteen
      diagnostics and passes after; ownership adapters remain unchanged.
- [ ] Commit this Rust-only correction once, rebase onto the latest fetched
      `origin/hparser-integration`, push without force, verify the remote SHA,
      and fetch again before the next package.

## Surprises & Discoveries

- The Rust owner already had `used_memory_usage` and its reset/growth tests,
  while the Go hparser branch lacked the corresponding method despite current
  Go `master` exposing it. The parity fix therefore belongs in Go source and
  does not require speculative Rust changes.
- `pkg/util/chunk` contains failpoint-backed spill tests. The canonical
  failpoint wrapper was used for both focused and full package runs; shared
  runner reference counting left an existing enablement in place and restored
  the prior state afterward.

## Decision Log

- Decision: restore the exact Go-master method and assertions in the Go
  package, preserving the existing Rust length-versus-capacity implementation.
  Rationale: this is the only package delta, and adding a second Rust path or
  changing memory consumers would exceed the dependency-closed fix. Date:
  2026-09-02, Codex.
- Decision: remove `#[must_use]` only from direct Go allocator and iterator
  returns; retain diagnostics on Rust lifetime/ownership wrappers and private
  storage helpers. Date: 2026-09-07, Codex.

## Validation

Run from the repository root:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh pkg/util/chunk -run '^TestChunkMemoryUsage$' -count=1
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh pkg/util/chunk -count=1
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
    git diff --check
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make bazel_prepare

Expected results are passing focused/full failpoint-wrapped Go tests, passing
lint, and clean diff hygiene. `make bazel_prepare` is required by the
conservative Ready gate for restored Go source but is blocked locally because
the `bazel` executable is unavailable.

## Risks

- Correctness: low; the method mirrors Go's exact length-based accounting and
  leaves capacity-based `MemoryUsage` unchanged.
- Compatibility: additive exported Go API; no existing call sites change.
- Performance: one linear pass over columns with no allocations; no impact on
  retained allocation tracking.

## Outcomes & Retrospective

The Go chunk package now exposes the same used-versus-retained memory split as
current `master`, and the source regression proves growth and reset semantics.
Rust owner parity and earlier complete test-port receipts remain unchanged;
broader chunk spill failures are outside this focused delta.

## 2026-09-07 Rust-only validation

The allocator/iterator source regression was first run against the pre-fix
owner and failed with exactly fourteen `unused_must_use` diagnostics. The
post-fix focused command passes, and the complete owner suite reports 329
passed and 4 skipped. The all-target owner check passes with only existing
warnings. Pinned nightly rustfmt, `git diff --check`, and the pinned
repository Ready `make lint` command all pass (lint exit 0).

No Go, Bazel, Cargo metadata, generated/platform artifact, or fixture changed,
so `make bazel_prepare` is not required for this Rust-only batch. The next
required state transition is one package-level commit, a non-forced rebase and
push to `hparser-integration`, remote SHA verification, and a fresh fetch.
