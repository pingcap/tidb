# `pkg/util/syncutil` — Go-master package boundary receipt

Go source: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package is
byte-for-byte unchanged from extraction pin
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

All three Go-master artifacts were read in full, including both build-tag
variants:

| Artifact | Lines | Blob | SHA-256 | Inventory |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 12 | `0d534a7a9118ea79590b2de89b05d13ba9084228` | `db1a7299fe7d6f62f6a9bd4f7db3a5c929260609ed1389612e744976938821ac` | public library target with the deadlock-tagged and default sources plus go-deadlock dependency |
| `mutex_deadlock.go` | 40 | `8879ff51381047751e5118e4f8fd4b0f410b3622` | `359eefa4e65c9c1383143b54b618ea0b0be9c357bd95ded7fd106dd28c666871` | `deadlock` build variant, 20-second detector timeout, and embedded deadlock Mutex/RWMutex |
| `mutex_sync.go` | 32 | `c6bdc55ccf87b21f7b8cd710a4108c8fae9a67cb` | `f37f34907fd7ae4b44c8f7f0c933b0098c3688bde674e2f7c90617ec9787ceac` | default `!deadlock` variant, false flag, and embedded standard sync Mutex/RWMutex |

There is no `doc.go`, source test, fixture/testdata tree, generated output,
benchmark/fuzz target, or nested package. The package has 84 Go lines and two
platform-by-build-tag implementations. Dozens of session, executor, planner,
statistics, import, and domain files consume the exported wrapper types, so a
future port must preserve the package-wide type identity and method promotion.

## Rust ownership and decision

Rust's crates use `std::sync::{Mutex,RwLock}`, parking-lot-like guards, and
crate-local lock wrappers. They do not expose a single public `syncutil`
owner, nor can Rust reproduce Go's compile-time `deadlock` build tag and
`go-deadlock` runtime detector for the Go consumers. Introducing a Rust lock
facade would not replace those imports and could alter poisoning, guard, or
deadlock-diagnostic semantics. No Rust-only behavior was found and no safe
missing Go behavior can be added without moving all consumers together. This
package remains explicitly unclaimed; its lock policy is a portability and
tooling boundary rather than a detached runtime feature.

## Validation

Profile: WIP for the continuing repository audit; no source or build artifact
changed.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/syncutil -count=1` — passed (`[no test files]`).
- `git diff --stat e2788410d8d696605e8cb002585877a063ccc909..origin/master -- pkg/util/syncutil` — empty; source is unchanged at Go master.
- Rust search across all crates and Go call sites — found only crate-local standard locks and no dependency-closed public replacement.

No Go or Bazel file changed, so `make bazel_prepare` is not required. Deadlock
build-tag execution and detector diagnostics were not run beyond the default
package compile because they are external Go tooling behavior.

## Risks and unverified scope

- Correctness: any future native owner must preserve lock method promotion,
  zero-value behavior, and the deadlock detector's 20-second timeout.
- Compatibility: all current Go consumers rely on `syncutil.Mutex` and
  `syncutil.RWMutex` being assignment-compatible wrappers; changing their
  identity is a broad source break.
- Performance: standard-lock behavior is unchanged; enabling deadlock mode has
  intentional diagnostic overhead.
- Not verified locally: `-tags=deadlock` package/test builds and detector
  timeout reporting under CI contention.
