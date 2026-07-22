# Rust rewrite validation

Run commands from `rust/`. Cargo always uses 12 jobs.

## Fast package loop

Read the live Go package directly and run a focused Cargo test only when it
shortens the current edit loop. Keep translating after it passes. Do not run a
workspace sweep during package translation unless a shared public API change
requires it.

After the whole package and every original test/support obligation are
translated, run:

```sh
cd .. && go test ./pkg/example
cd rust && cargo test --offline --locked -j12 -p tidb-example --all-targets
```

## Whole-package closure

Use ordinary tools once after the complete Go package and its original
test/support owners have Rust equivalents:

```sh
cargo fmt --all -- --check
cargo clippy --offline --locked -j12 --workspace --all-targets -- -D warnings
cargo test --offline --locked -j12 --workspace --all-targets
cargo test --offline --locked -j12 --workspace --doc
git -C .. diff --check -- rust
```

Large integration-test directories compile through `scripts/aggregate-tests.rs`.
This retains every test source while avoiding hundreds of tiny Cargo binaries.

Real PD/TiKV, protocol, or differential suites remain additional acceptance
requirements when the changed package reaches those boundaries.
