# `pkg/util/arena` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full: `arena.go`,
`arena_test.go`, `main_test.go`, and `BUILD.bazel`. There is no package doc,
README, fixture, generated or platform variant, benchmark, fuzz target,
example, or ownership file. The local Go package is unchanged from the pin.

Production behavior is an allocator interface, a simple allocator backed by
one reusable byte array, and a standard allocator that always allocates fresh
storage. A fitting simple allocation returns a slice descriptor over the
shared arena; `Reset` rewinds only its offset and does not clear bytes.

## Rust ownership and audit result

`rust/crates/tidb-util/src/arena.rs` owns the complete package. `ArenaBytes`
is a safe Go-slice descriptor over reference-counted `Cell<u8>` backing: cloned
descriptors share writes, reslicing exposes retained bytes, fitting allocations
reuse the simple arena, and fresh fallback and standard allocations remain
independent and zeroed. The
workspace's unsafe-code prohibition remains intact.

The audit replaced the previous owned-`Vec<u8>` approximation, which advanced
the offset but allocated every returned buffer separately. It removed three
supplemental tests, a Rust-only `must_use` diagnostic, and the retired semantic
manifest. The two exact Go tests remain, plus one required regression proving
the corrected reset reuse.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/arena` — passed.
- Pre-fix `cargo test -q -p tidb-util
  arena::tests::reset_reuses_the_go_backing_storage --lib --locked -- --exact
  --test-threads=1` — failed with actual `[0]`, expected `[9]`.
- Post-fix `cargo test -q -p tidb-util arena::tests --lib --locked --
  --test-threads=1` — passed; three tests ran.
- Complete `cargo test -q -p tidb-util --locked -- --test-threads=1`,
  `cargo check -p tidb-util --all-targets --locked`, `cargo fmt --all --check`,
  and `git diff --check` — passed; 575 library tests passed, 3 were ignored,
  and every integration and documentation test passed.
- `cargo clippy -p tidb-util --lib --no-deps --locked -- -A
  clippy::chunks-exact-to-as-chunks -A clippy::new-without-default -D warnings`
  — passed. The two allowances cover six pre-existing findings outside
  `arena.rs`; unscoped all-target Clippy also reaches pre-existing
  `tidb-mysql` findings and is not green on the current branch.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: shared storage and reset reuse now match Go; valid lengths,
  capacities, offset movement, fallback, and standard allocation remain.
- Compatibility: the unused public Rust return type changes from `Vec<u8>` to
  `ArenaBytes`; repository search found no production consumer.
- Performance: fitting allocations now reuse one backing allocation instead
  of allocating a new vector, restoring the package's purpose.
