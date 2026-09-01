# `pkg/sessionctx/sysproctrack` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains two tracked artifacts and 48 lines. The complete
inventory includes the public BUILD target and every method in the
`TrackProc` and `Tracker` interfaces. It has no Go tests, fixtures or
`testdata`, generated output, platform-specific variant, benchmark, fuzz
target, or generator input.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 12 | `7db6090ac4daaf0e812dc1808003d3563cedeb99` | `6efeb2f63c731f0b3bd036f01013e6aa2d1aaddea79eec79f553703da25383fe` | public Go library target and session/sessmgr dependency closure |
| `track.go` | 36 | `259585a4d0607a8230579007e0fdc1ca29d6281c` | `9598baf2ab6d05c5c43d43285ca0805fe963de2f9097ac7a65d4a8b6af1ebd8a` | `TrackProc` process view and `Tracker` lifecycle interfaces |

`TrackProc` exposes `GetSessionVars` and `ShowProcess`; `Tracker` exposes
`Track`, `UnTrack`, `GetSysProcessList`, and `KillSysProcess`. These are
contracts only: the package defines no concrete state, locking, process-map
ownership, or error policy. The Go master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` is empty for both artifacts.

## Rust ownership and explicit boundary

Rust carries the same callback seam through `tidb-sqlexec`'s
`TrackSysProc`/`UnTrackSysProc` types and `ExecOptionWithSysProcTrack`; the
server owns the lifecycle guard and auto-analyze consumers pass those
callbacks explicitly. Rust does not expose a direct `GetSysProcessList`/
`KillSysProcess` map interface because no concrete Go tracker implementation
belongs to this package. No Rust-only behavior was found to remove, and no
safe package-local behavior can be implemented without inventing process-map
ownership and session-manager integration.

## Validation and risk

Profile: **WIP** for this documentation-only boundary audit; no production,
test, or Bazel file changed, so no regression test or package-complete Ready
claim is made.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/sessionctx/sysproctrack -count=1                         # passed (no test files)
```

The exact detached Go-master worktree was used. Rust source, Bazel, and
module files were unchanged; `make bazel_prepare` and Ready lint were not
required. Not verified: concrete Go session-manager implementations, live
process kill paths, or full server integration. Compatibility and performance
risk are unchanged because this batch modifies documentation only.

This receipt certifies the bounded sysproctrack interface-package inventory
and explicit ownership boundary; it is not a repository-wide parity claim.
