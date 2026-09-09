# `pkg/util/intset` parity receipt

- Go authority: `e2788410d8d696605e8cb002585877a063ccc909`
- Go inventory: `BUILD.bazel`, `fast_int_set.go`,
  `fast_int_set_test.go`, `fast_int_set_bench_test.go`
- Rust owner: `rust/crates/tidb-util/src/intset.rs`
- Rust benchmark owner: `rust/crates/tidb-util/benches/intset.rs`
- Rust consumer updated: `rust/crates/tidb-funcdep/src/fd_graph.rs`

Rust retains the `u64` small path and ordered sparse path, including Go's
retained-large representation, `MaxInt` iteration sentinel, wrapping shift,
and `CopyFrom` behavior. The exported surface maps Go's constructor and
methods; Rust-only iterator and duplicate-constructor APIs were removed.

Validation results are recorded by the completing commit.

WIP validation:

- `go test ./pkg/util/intset`: pass.
- `cargo test -p tidb-util --lib 'intset::tests::' --locked --
  --test-threads=1`: 6 passed.
- `cargo test -q -p tidb-util --locked -- --test-threads=1`: 560 passed,
  3 ignored; all integration and doc-test targets passed.
- `cargo test -p tidb-funcdep --locked -- --test-threads=1`: 12 passed.
- `cargo bench -p tidb-util --bench intset --locked`: all six source
  workloads executed once.
- `cargo check -p tidb-util --all-targets --locked`: pass.
- `cargo check -p tidb-funcdep --all-targets --locked`: pass with five
  pre-existing `tidb-chunk` warnings.
- `cargo check -p tidb-planner --lib --locked`: pass with pre-existing
  warnings.
- `cargo clippy -p tidb-util --lib --bench intset --no-deps --locked -- -A
  clippy::chunks-exact-to-as-chunks -A clippy::new-without-default -D
  warnings`: pass.
- `cargo fmt --all --check` and `git diff --check`: pass.

`cargo check -p tidb-planner --all-targets --locked` is not a valid clean
gate at this revision because two unrelated existing integration tests do not
compile: `core_logical_cte_topn_prune_source.rs` omits
`RuleContext.column_allocator`, and
`physicalop_memory_trace_clone_stream_count_source.rs` supplies `SortItem`
where `ByItems` is required. The planner library itself compiles successfully.
