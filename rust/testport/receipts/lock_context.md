# `pkg/lock/context` — Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The nested package contains exactly two tracked artifacts and 55 lines. Both
the public Go interface and its Bazel target were read in full; there is no
`doc.go`, test, fixture, generated file, benchmark, platform variant, or
additional build input.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 12 | `67c6107f8f25d7f65c8f5b8fd5c8ab9d95382271` | `f87cc6482536e245e0e8e383a567db0d7c9c0f91ddd989086f90e3b08d97356f` | public context library target |
| `lockcontext.go` | 43 | `55d4e74685bff3243a569ff6b998ce4a601eae1e` | `bdc94ad76f0322aaf757e0873a6a91ffc86d86965ef50948d5d29829a185efad` | read/write table-lock context interfaces |

`TableLockReadContext` declares `CheckTableLocked`, `GetAllTableLocks`, and
`HasLockedTables`; `TableLockContext` adds lock insertion and release methods.
These are interfaces only and have no package-level production functions or
tests.

## Rust ownership and decision

No Rust crate exposes a dependency-closed equivalent of these session-owned
interfaces. Rust model types represent persisted `TableLockInfo` and AST
types represent lock syntax, while the executor retains explicit tests for
the missing session lock registry. Inventing a Rust trait here without the
session implementation would be an uncallable Rust-only API, so this package
is recorded as an honest boundary with no source edit or regression test.

## Validation and risk

Profile: **WIP** for this docs-only boundary audit. `git diff --check` passed;
no Go/Bazel file changed and `make bazel_prepare` is not required.

- Correctness and compatibility remain bounded by the unported session lock
  registry and checker in the parent package.
- Performance is unchanged.
- Not verified locally: Bazel analysis and end-to-end table-lock SQL flows.
