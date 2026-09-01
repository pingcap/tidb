# `pkg/util/gcutil` — Go-master package boundary receipt

Go source: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package is
byte-for-byte unchanged from the earlier extraction pin
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

Both Go artifacts were read in full before deciding the Rust ownership
boundary:

| Artifact | Lines | SHA-256 | Inventory |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 18 | `a041f1ecd7679237387e14028ec87db97b2df2d98fec10415a371c9602c0f74c` | public library target and six session/metadata/Oracle utility dependencies |
| `gcutil.go` | 91 | `403d73bea7e1b2cb092dffcabf3fb412f42f54e2aa4ce2f869c00bacbc14490c` | GC enable/disable toggles, snapshot-vs-safe-point validation, restricted SQL safe-point load, TiKV time parsing, and TSO conversion |

There is no `doc.go`, test file, fixture, generated or platform variant,
benchmark, fuzz target, example, nested package, or additional build artifact.
The package has six exported functions and one SQL query constant; its source
has no current-master delta.

## Rust ownership and decision

The behavior is intentionally cross-cutting. `tidb-vardef` owns the
`tidb_gc_enable` literal; `tidb-session` owns mutable global sysvar access and
restricted SQL execution; `tidb-model` owns TSO-to-time conversion and
`tidb-error` owns `ErrSnapshotTooOld`; `tidb-txnkv` owns PD/GC safe-point state.
No crate currently owns the complete session-context helper, compatible
TiKV-GC time parser, or global-variable mutation path as one dependency-closed
`gcutil` package.

The existing transaction and recovery tests exercise GC state and safe-point
errors through their ordinary owners, but a new standalone Rust helper would
duplicate session/sysvar/SQL policy and could disagree on internal-source
classification, `HIGH_PRIORITY` lookup, time parsing, or error formatting. No
Rust-only adapter or partial validator was added; this complete Go package
remains explicitly unclaimed until those owners can move together.

## Validation

Profile: WIP for the continuing repository audit; no Rust code changed.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/gcutil -count=1` — package has no Go test files; command reports the expected no-test result.
- `git diff --stat e2788410d8d696605e8cb002585877a063ccc909..origin/master -- pkg/util/gcutil` — empty; source is unchanged at Go master.
- Rust search of the session, vardef, model, error, and txnkv crates — confirmed split ownership and no dependency-closed `gcutil` implementation.

No Go or Bazel file changed, so `make bazel_prepare` is not required. Full
session integration, restricted-SQL, and live TiKV GC tests were not run for
this explicitly unclaimed boundary.

## Risks and unverified scope

- Correctness: the Go package's six functions remain source-of-truth; no Rust
  implementation currently claims their combined behavior.
- Compatibility: any future port must retain the `InternalTxnGC` source type,
  exact `tikv_gc_safe_point` query, `CompatibleParseGCTime` acceptance rules,
  TSO physical-time conversion, and `ErrSnapshotTooOld` timestamp text.
- Performance: no additional SQL query or session wrapper was introduced.
- Not verified locally: live GC enable/disable mutation, malformed safe-point
  rows, and snapshot validation through a running TiDB/TiKV cluster.
