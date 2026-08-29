# `pkg/util/disjointset` parity receipt

- Go authority: `e2788410d8d696605e8cb002585877a063ccc909`
- Go inventory: `BUILD.bazel`, `int_set.go`, `int_set_test.go`,
  `main_test.go`, `set.go`, `set_test.go`
- Rust owners: `rust/crates/tidb-util/src/disjointset/{mod,int_set,set}.rs`
- Rust consumer: `rust/crates/tidb-chunk/src/chunk_util.rs`

The dense and sparse parent indexes now use signed values like Go `int`, so
negative sizes and indexes are representable and panic instead of being
excluded by the type system. Sparse union keeps the first operand's root;
dense union keeps the second operand's root. The Rust method is `find_val`,
matching Go `FindVal`.

The two source tests and one signed-boundary regression own the package test
surface. The duplicate integration contract and retired semantic manifest
were removed.

WIP validation:

- `go test ./pkg/util/disjointset`: pass.
- `cargo test -p tidb-util --lib 'disjointset::' --locked --
  --test-threads=1`: 3 passed.
- `cargo test -q -p tidb-util --locked -- --test-threads=1`: 559 passed,
  3 ignored; all integration and doc-test targets passed.
- `cargo test -p tidb-chunk --lib
  'chunk_identity_tests::column_swap_helper_' --locked --
  --test-threads=1`: 2 passed.
- `cargo test -p tidb-chunk --test all
  column_swap_identity_and_cache_contract --locked -- --test-threads=1`: 1
  passed.
- `cargo check -p tidb-util --all-targets --locked`: pass.
- `cargo check -p tidb-chunk --lib --locked`: pass with five pre-existing
  warnings.
- `cargo clippy -p tidb-util --lib --no-deps --locked -- -A
  clippy::chunks-exact-to-as-chunks -A clippy::new-without-default -D
  warnings`: pass.
- `cargo fmt --all --check` and `git diff --check`: pass.

Direct `tidb-chunk` Clippy with `-D warnings` is blocked by seven pre-existing
diagnostics in `chunk.rs`, `codec.rs`, and `mutrow.rs`; none are in the changed
consumer path.
