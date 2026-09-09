# `pkg/lock` — Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

`pkg/lock` has two package-owned tracked artifacts and 172 lines when the
non-source owner policy file is included. Both Go/Bazel artifacts and the
`OWNERS` support file were read in full before comparing Rust owners.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 17 | `4305959ea0c1707657b8cb22e4928b4db56bca6d` | `7a775f0f41d2791cda80bbdec288326cf640833660661ca1456e3633047f49d8` | public `Checker` library target |
| `lock.go` | 155 | `bb50f3b63496f97b1df4ed8f221fe5a6ddc01072` | `777f0369c2f60a9948bd0673338f66b0055f3d26d3c2dbb51e1748bd1c2bcb5e` | table-lock checker and privilege decision tree |
| `OWNERS` | 5 | `a70e8d7189b998dee4b5d28e5f6b836412a6c6bf` | `abe5177d00f796537931cfed0ff0d843d357739d49388c87956d07596bb2744c` | repository ownership metadata |

There are no Go tests, fixtures, generated outputs, benchmarks, build-tag
variants, or package documentation. The production surface is
`NewChecker`, `Checker.CheckTableLock`, `Checker.CheckLockInDB`, the private
`checkLockTpMeetPrivilege` decision helper, and `ErrLockedTableDropped`.
The checker depends on the full infoschema/table-lock session context, table
metadata, and privilege/error surfaces.

## Rust ownership and decision

No dependency-closed Rust owner exists for `pkg/lock`. Rust contains the AST
and model representations of table locks, parser support, and executor tests,
but not the Go `Checker` decision tree or the session lock registry. The
executor's source-derived table-lock tests explicitly mark the missing
`HasLockedTables`/`CheckTableLocked`/`HandleLockTables*` integration as a
`go-parity-gap`; adding a standalone checker would create a second, unused
authorization path and could disagree with infoschema state.

The package is therefore recorded as an explicit boundary with no speculative
Rust behavior and no source edit. The absence of Go tests means no focused
regression is applicable to this docs-only inventory.

## Validation and risk

Profile: **WIP** for this boundary audit; no executable code changed and the
repository-wide loop remains in progress. `git diff --check` passed for the
receipt/ExecPlan batch. No Go or Bazel source changed, so `make bazel_prepare`
is not required.

- Correctness: the unported checker remains a known integration gap; existing
  Rust AST/model lock types do not claim SQL enforcement parity.
- Compatibility: future implementation must move the checker, session lock
  context, infoschema lock listing, and error paths together.
- Performance: unchanged.
- Not verified locally: full Go package tests (none exist), Bazel analysis,
  and end-to-end SQL table-lock enforcement.
