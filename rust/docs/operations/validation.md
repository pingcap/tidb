# Rust rewrite validation

Run commands from `rust/`. Cargo always uses 12 jobs.

## Fast package loop

Use the live Go package as the inventory and run the smallest relevant Cargo
test directly during implementation. A green cohesive edit may be committed
and pushed immediately; package completion is not a commit gate. Do not run a
workspace sweep unless the edit changes a shared public API.

After the whole package and every original test/support obligation are
translated, run:

```sh
cd .. && go test ./pkg/example
cd rust && cargo test --offline --locked -j12 -p tidb-example --all-targets
```

## Pre-push validation

Use ordinary tools once before push:

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
