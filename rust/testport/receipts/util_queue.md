# `pkg/util/queue` — complete package transcreation

Go source: `origin/master` at
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01). The package is
byte-for-byte unchanged from the earlier implementation; this receipt updates
the authority and complete artifact hashes.

## Complete inventory

The package has exactly three artifacts, all read in full before this refresh:

| Artifact | Lines | Git blob | SHA-256 | Inventory |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 17 | `db8f01ebf757ad4a2f20600f9534066b89340b60` | `0ab3a64e1d621b678f056fcf58e3fed825efd36685609945db2e9e2eaf3c97a7` | public library plus flaky short test target |
| `queue.go` | 94 | `0a13551814d3c0797ff8d0fd9a8510bcc2dda2c5` | `4bdbdc1f9a50aa149673d4e80612fcdc893f6fbfd426cb452b2f9db9afe46f93` | generic circular buffer, zero value, growth, pop, clear, expansion, length, emptiness, and capacity |
| `queue_test.go` | 87 | `678247705ff6484a0c3e90df2c9a679e2e60bf1b` | `76dbc2ccc2a43f41c5f305e8d961f8389883453ea3e8db3c94056a13f6b5bdf8` | `TestQueue` with four ordered subtests |

There is no `doc.go`, README, ownership file, generated/platform source,
fixture, benchmark, example test, or additional harness. The checkout is
byte-identical to the current Go-master authority. The complete inventory is
198 textual lines, nine function declarations, and no current source delta.

## Rust ownership and audit result

`rust/crates/tidb-util/src/queue.rs` is the sole package owner. Its four
Go-named tests reproduce every source assertion. Its source-derived boundary
tests cover the production method omitted by the Go test, wrapped growth, the
Go zero-value versus `NewQueue(0)` distinction, and Go's retained-slot behavior
after `Clear`.

The audit removed the unused 244-line `tidb-exec::queue` duplicate and its
separate seven-test carrier. That copy diverged from Go by clearing every
backing slot during `Clear`, causing values to be dropped immediately, and it
published Rust-only head/tail accessors solely for external tests. The retained
owner resets only indices like Go, preserves values until overwrite or backing
replacement, and tests the private indices inside the owner module.

## Validation

Profile: Ready for this docs-only authority refresh; the package owner and
focused retained-slot regression were implemented in the earlier atomic batch.

- `go test ./pkg/util/queue` — passed.
- The same package test passed in an exact detached checkout of Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- `cargo test -p tidb-util --locked queue::` — passed.
- `cargo test -p tidb-util --locked` — passed.
- `cargo check -p tidb-exec --lib --locked` — passed.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: reduced; one owner preserves Go's FIFO, growth, panic, zero
  value, and retained-slot semantics.
- Compatibility: the unused executor module and its Rust-only inspection
  accessors are removed.
- Performance: unchanged in the retained owner; clear remains constant time
  like Go rather than scanning the backing array.
