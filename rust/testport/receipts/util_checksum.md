# `pkg/util/checksum` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full:

- `checksum.go` — CRC framing writer, positional validating reader, pooled
  read buffer, sticky errors, cache offsets, flush, and close behavior;
- `checksum_test.go` — all ten package tests and their file/mutation helpers;
- `main_test.go` — common test setup and leak checking;
- `BUILD.bazel` — one library and one flaky short test target.

There is no `doc.go`, generated/platform source, fixture, testdata, benchmark,
fuzz target, example, or additional harness.

## Rust ownership and audit result

`rust/crates/tidb-util/src/checksum/mod.rs` owns the package and uses the
shared count-plus-error positional-read and explicit-close contracts in
`rust/crates/tidb-util/src/layered_io.rs`. The live spill consumer is
`rust/crates/tidb-chunk/src/chunk_util.rs`.

The audit removed `checksum::Writer::underlying`, a Rust-only public accessor
with no Go checksum equivalent. The chunk spill stack now retains a separate
cipher-writer handle, matching Go's `checksumWriter` and `cipherWriter`
ownership, while the checksum layer receives a shared handle to that same
writer. Reads still overlay the cipher plaintext tail before the checksum
payload tail, including before either layer is flushed.

The stale semantic manifest and historical audit plan were deleted. A strict
test-surface re-audit also removed two supplemental signed-overflow tests and
their private recording fixture. Production retains the source's explicit
wrapping arithmetic, while the suite now contains exactly the ten Go tests.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/checksum`
- `cargo test --offline --locked -p tidb-util --lib checksum::tests --no-fail-fast` — passed, exactly 10 tests.
- `cargo test --offline --locked -p tidb-chunk chunk_util`
- `cargo test --offline --locked -p tidb-util --no-run`
- `cargo test --offline --locked -p tidb-chunk`
- `cargo check --offline --locked -p tidb-util -p tidb-chunk --all-targets`
- `cargo fmt -p tidb-util -- --check`
- `git diff --check`

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: live encrypted spill reads retain the same two cache
  overlays without reaching through checksum's public API.
- Compatibility: intentionally removes one Rust-only public method; the sole
  repository consumer is migrated to Go's separate-writer ownership shape.
- Performance: plaintext behavior is unchanged. Encrypted spill writes take a
  local uncontended lock to safely represent Go's shared writer pointer across
  Rust ownership; encryption, checksum framing, and I/O geometry are unchanged.
