# Complete `pkg/util/sys/storage` parity

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` current while the package claim
is implemented and validated. It follows `PLANS.md` at the repository root.

## Purpose / Big Picture

The Rust utility layer must answer the same question as Go's
`pkg/util/sys/storage`: how many bytes are available to the caller in the
filesystem containing a directory. After this work, the Rust spill-storage
consumer and direct callers use the same platform-selected implementation,
including the Go fallback on unsupported platforms and the same error boundary
when the operating system cannot inspect a path. A user can observe the
behavior with the package test and the spill quota check without enabling a
Rust-only test policy.

The comparison source is the pinned Go package at commit
`e2788410d8d696605e8cb002585877a063ccc909`. The package is independent of the
larger `pkg/util/disk` package: only the filesystem-capacity helper and its
platform variants are in this claim.

## Progress

- [x] (2026-09-01) Reconciled the current remote branch and confirmed no
      `pkg/util/sys/storage` parity receipt or ExecPlan claim exists. The
      earlier Rust seed (`061813f122`) is not a package claim.
- [x] (2026-09-01) Read and inventoried every pinned Go production, test,
      harness, platform, and build artifact before editing.
- [x] Move the source test into an explicit Rust package-test carrier and
      remove Rust-only test contracts from the production module.
- [x] Add focused regressions for the exact `statfs` arithmetic and the
      supported-platform OS error boundary.
- [x] Record the exact host-toolchain result and create one batch commit.
- [x] Complete the Ready validation with command-local bundled Go and OpenSSL
      tooling; Windows and unsupported-target execution remain unrun.

## Surprises & Discoveries

- Observation: the Rust filesystem helper already existed, but its previous
  semantic test description was removed by commit `3353b29fb4` when the old
  gate machinery was removed. No receipt or living plan currently claims the
  package.
  Evidence: `git log --all -- rust/crates/tidb-util/src/sys/storage.rs`
  contains `061813f122`, while current `rust/testport/receipts/` and
  `rust/docs/operations/` contain no storage-package claim.

- Observation: Go's source test only asserts that the current directory has a
  positive capacity. A missing path is an additional boundary regression, not
  a replacement for that source assertion: Go's `syscall.Statfs` and Rust's
  `statfs` must both return an error rather than inventing a capacity on the
  supported POSIX platforms.

- Observation: the existing Rust implementation used `statvfs`, which is not
  behaviorally interchangeable with Go's `syscall.Statfs` on macOS. On the
  host filesystem, `statvfs` reported a 1 MiB block size while `statfs`
  reported 4 KiB, changing the returned byte capacity by 256x.
  Decision: use `rustix::fs::statfs` and lock the exact multiplication in a
  focused test.
  Evidence: direct C `statvfs`/`statfs` comparison and
  `uses_statfs_available_bytes` in the external package test.

## Decision Log

- Decision: retain `Path` input at the Rust boundary even though Go accepts a
  `string`.
  Rationale: the ordinary Rust spill-storage owner already holds a `PathBuf`,
  and `impl AsRef<Path>` preserves Go's arbitrary-byte Unix path capability
  without adding a second filesystem operation or changing the result.
  Date/Author: 2026-09-01 / Codex

- Decision: remove the inline fallback and missing-directory tests from the
  production module and keep one external source-shaped carrier plus one
  focused regression.
  Rationale: Go's package has one functional test; fallback compilation is
  covered by the platform-selected implementation, while the invalid-path
  case protects a real error contract. This avoids presenting Rust-only test
  policy as Go parity evidence.
  Date/Author: 2026-09-01 / Codex

- Decision: treat `cmd/tidb-server/main.go::checkTempStorageQuota` as the Go
  integration boundary even though the Rust call is owned by
  `tidb_util::disk::SpillStorage::open`.
  Rationale: repository-wide search shows the Go startup caller and the Rust
  startup path (`tidb-server::open_spill_storage` and its node runners) both
  perform the capacity admission before the server becomes usable. The
  shared helper remains the only capacity implementation; the existing Rust
  spill error type is outside this package claim and is intentionally left
  unchanged.
  Date/Author: 2026-09-01 / Codex

## Outcomes & Retrospective

The implementation now states the exact three-file Go production inventory,
the two test artifacts, BUILD metadata, Rust owner, and spill-storage startup
integration. The POSIX adapter uses `statfs`, matching Go's block arithmetic;
the focused source-module and repository package tests pass. The Ready gate
passes with command-local bundled Go and OpenSSL dependencies. Any unavailable
cross-platform execution (for example, Windows or an unsupported target) is
reported as not run rather than implied by a host-platform test.

## Context and Orientation

The pinned Go package contains:

* `pkg/util/sys/storage/sys_posix.go`, selected for Linux and macOS; it calls
  `syscall.Statfs` and returns `Bavail * Bsize` bytes.
* `pkg/util/sys/storage/sys_windows.go`, selected for Windows; it calls
  `windows.GetDiskFreeSpaceEx` and returns caller-available bytes.
* `pkg/util/sys/storage/sys_other.go`, selected for other targets; it returns
  `math.MaxInt64` as a `uint64` and never inspects the path.
* `pkg/util/sys/storage/sys_test.go`, the one functional test.
* `pkg/util/sys/storage/main_test.go`, test setup/goleak harness only.
* `pkg/util/sys/storage/BUILD.bazel`, the Go library/test target and platform
  dependency selection.

There is no `doc.go`, generated file, fixture, benchmark, or additional
testdata in the package. The Rust owner is
`rust/crates/tidb-util/src/sys/storage.rs`, exported through
`rust/crates/tidb-util/src/sys/mod.rs` and `src/lib.rs`. The Go startup caller
is `cmd/tidb-server/main.go::checkTempStorageQuota`; Rust reaches the same
capacity admission through
`tidb-server::open_spill_storage -> tidb_util::disk::SpillStorage::open`,
where available bytes are compared with the configured quota.

## Plan of Work

First align the POSIX implementation with Go's `syscall.Statfs` rather than
the behaviorally different `statvfs`, while preserving the Windows and
unsupported-platform variants and the spill-storage call site. Remove the
module's fallback constant test and missing-directory test, which are Rust-only
test surfaces, and make the unsupported-platform constant compile only on the
unsupported target where the Go fallback exists.

Next add `rust/crates/tidb-util/tests/sys_storage_source.rs`. It asserts the
exact source test (`.` has capacity), the `f_bavail * f_bsize` arithmetic, and
a deterministic supported-POSIX regression (a newly-created directory's
missing child returns an error). The test is external so it exercises the
public package boundary.

Finally keep the package receipt and this plan's completion evidence honest,
append the implementation entry to `rust/testport/TESTPORT_EXECPLAN.md`, run
formatting, the focused Rust tests, `make lint` for the Rust code change, and
commit the atomic package batch.

## Concrete Steps

Run from the repository root unless a command begins with `cd rust`:

    git ls-tree -r --name-only e2788410d8d696605e8cb002585877a063ccc909 pkg/util/sys/storage
    rg -n '^func |^//go:build' pkg/util/sys/storage
    cargo fmt --manifest-path rust/Cargo.toml --all -- --check
    cargo test --manifest-path rust/Cargo.toml --locked -p tidb-util --test sys_storage_source
    cargo test --manifest-path rust/Cargo.toml --locked -p tidb-util --lib sys::storage
    make lint

The first command must list exactly the package artifacts above. The focused
tests must pass on the host platform; the POSIX error regression is compiled
only where the Go package calls `Statfs`. `make lint` is required for the Ready
profile because Rust production code and tests change.

## Validation and Acceptance

Acceptance requires all of the following:

1. On Linux/macOS, `get_target_directory_capacity(".")` is positive and a
   missing child returns an I/O error; the result is computed from available
   blocks, not total blocks.
2. On Windows, the selected implementation calls the caller-scoped free-space
   API, matching `GetDiskFreeSpaceEx` rather than a volume-wide approximation.
3. On other targets, the selected implementation returns exactly
   `9_223_372_036_854_775_807` as Go's `math.MaxInt64` converted to `uint64`.
4. The Go startup caller `cmd/tidb-server/main.go::checkTempStorageQuota` is
   represented by the Rust startup path
   `tidb-server::open_spill_storage -> SpillStorage::open`, which still uses
   this helper for quota admission and carries the original operating-system
   error through its existing error wrapper.
5. Every Go artifact is mapped in the receipt to Rust code, a test, a harness
   omission, or an explicit platform decision. No Rust-only test contract is
   described as a source test.

## Idempotence and Recovery

The edits are safe to rerun. Tests create their own temporary directory and do
not modify repository or cluster state. If a Cargo build fails because of an
unavailable host dependency, retain the source changes, record the exact
failure in the receipt, and do not claim Ready until an equivalent validation
can run. Revert only the package batch if a review finds the source inventory
or platform mapping incomplete.

## Artifacts and Notes

The final receipt will include the pinned source tree listing, a Go-to-Rust
mapping table, exact commands and concise pass/fail output, and the host-target
limitations. The receipt is
`rust/testport/receipts/util_sys_storage.md`.

## Interfaces and Dependencies

`tidb_util::sys::storage::get_target_directory_capacity` remains the public
Rust API. It returns `std::io::Result<u64>` and delegates by target selection
to `rustix::fs::statfs` (Linux/macOS), `fs4::available_space` (Windows), or
the `math::MaxInt64` fallback (other targets). `tidb-util` keeps its existing
`rustix`, `fs4`, and `tempfile` dependencies; no Go source or Bazel metadata is
changed by this Rust-only claim.
