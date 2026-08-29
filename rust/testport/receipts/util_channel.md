# `pkg/util/channel` parity receipt

- Go authority: `e2788410d8d696605e8cb002585877a063ccc909`
- Go inventory: `BUILD.bazel`, `channel.go`
- Rust owner: `rust/crates/tidb-util/src/channel.rs`

Go exposes one `Clear` function and no package tests. Rust now accepts its
native receive-channel type and drains it until every sender disconnects. The
former `IntoIterator` signature also accepted vectors and arbitrary iterables,
behavior absent from Go; that surface and its two synthetic tests were
removed, together with the retired semantic manifest.

WIP validation:

- `go test ./pkg/util/channel`: pass, `[no test files]`.
- `cargo check -p tidb-util --all-targets --locked`: pass.
- `cargo clippy -p tidb-util --lib --no-deps --locked -- -A
  clippy::chunks-exact-to-as-chunks -A clippy::new-without-default -D
  warnings`: pass.
- `cargo fmt --all --check` and `git diff --check`: pass.
