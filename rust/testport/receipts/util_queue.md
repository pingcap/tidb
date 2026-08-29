# `pkg/util/queue` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full:

- `queue.go` — the generic circular buffer, zero value, growth, pop, clear,
  clear-and-expand, length, emptiness, and capacity behavior;
- `queue_test.go` — `TestQueue` with its four ordered subtests;
- `BUILD.bazel` — one library and one flaky short test target.

There is no `doc.go`, README, ownership file, generated/platform source,
fixture, benchmark, example test, or additional harness. The checkout is
byte-identical to the pin.

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

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/queue` — passed.
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
