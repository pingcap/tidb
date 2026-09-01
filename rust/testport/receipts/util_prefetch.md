# `pkg/util/prefetch` — complete Go-master package transcreation

Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02).

## Complete inventory

The package has exactly three artifacts, all read in full: `reader.go`,
`reader_test.go`, and `BUILD.bazel`. They define the background prefetch
reader, two alternating buffers and an unbuffered handoff, partial-range EOF
conversion, explicit idempotent close, and exactly four tests. There is no
package doc, README, fixture, benchmark, generated or platform variant, or
ownership file. The checkout is byte-identical to the pin.
The inventory is exactly 300 lines: 17 build lines, 126 production lines, and
157 test/support lines.

## Rust ownership and audit result

`rust/crates/tidb-util/src/prefetch.rs` is the production owner. Rust's
standard `Read` trait has no `Close`, so the one native constructor accepts the
reader plus its close callback; together they represent Go's single
`io.ReadCloser`. The audit removed the second convenience constructor, whose
no-op close could not reproduce Go, and removed the explicit `Drop`-time close
that Go does not perform. Explicit `close` retains Go's source-close, producer
cancel/join, error return, and idempotence order.

The test module now contains exactly the four Go source tests. Two
supplemental Rust close tests were removed; their extra scenarios are not
artifacts of this Go package. The current living ExecPlan is
`rust/docs/operations/util-prefetch-audit-execplan.md`.

## Validation

Profile: **Ready** for this docs-only authority refresh. No Go, Rust, Bazel,
or module source changed, so `make bazel_prepare` is not required.

- `git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/prefetch` — passed.
- Pinned-Go `go test ./pkg/util/prefetch -run '^(TestBasic|TestConvertUnexpectedEOF|TestCloseBeforeDrainRead|TestFillPrefetchBuffer)$' -count=1` — passed in the current and exact detached Go-master worktrees.
- With the pinned OpenSSL environment, `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib prefetch::tests:: --offline --locked -- --test-threads=1` — passed (4 tests).
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` and the batch diff checks — passed.

The focused Rust command emitted only existing workspace warnings. Full
workspace tests and Bazel execution remain outside this leaf receipt.

## Risk

- Correctness: the four source tests pass in both implementations.
- Compatibility: callers of the artificial no-close constructor must supply
  the source close callback; there were no repository callers.
- Performance: the background handoff and one-buffer-ahead behavior are
  unchanged.
