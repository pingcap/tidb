# Rust rewrite validation

Run commands from `rust/`. Cargo always uses 12 jobs.
Run Cargo commands serially in this workspace; concurrent commands contend on
the same target-directory lock and are slower than one `-j12` invocation.

## Commands

A unit-test filter without `--lib` starts every integration-test binary. Select
the target explicitly for a focused run:

```sh
cargo test --offline --locked -j12 -p <owning-crate> --lib <test-name>
cargo test --offline --locked -j12 -p <owning-crate> --test all <test-name>
```

The parser package uses one unit harness and one integration harness:

```sh
cargo test --offline --locked -j12 -p tidb-parser --lib --test all
```

At package completion run:

```sh
cd .. && go test ./pkg/example
cd rust && cargo test --offline --locked -j12 -p tidb-example --all-targets
```

```sh
cargo fmt --all -- --check
cargo test --offline --locked -j12 -p <owning-crate> --all-targets
python3 <package-generator-or-differential-check> --check
git -C .. diff --check -- rust
```
