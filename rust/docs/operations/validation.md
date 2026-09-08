# Rust rewrite validation

Run commands from `rust/`. Cargo always uses 12 jobs.
Run Cargo commands serially in this workspace; concurrent commands contend on
the same target-directory lock and are slower than one `-j12` invocation.

`.cargo/config.toml` sets `RUST_MIN_STACK=33554432`. The vendored
`tikv-client-rs` mock's async lock-resolution future chain
(`third_party/tikv-client-rs/src/transaction/lock.rs`) overflows the 2 MiB
default test-thread stack in a debug build, aborting several `tidb-txnkv`
integration tests with `has overflowed its stack`. Cargo applies the variable
to the test binaries it runs, so no shell override is needed.

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

During a batch, run only affected source and Rust tests:

```sh
cd ../<go-module> && go test -p 12 ./package-one ./package-two
cd <repo>/rust && cargo test --offline --locked -j12 -p tidb-one -p tidb-two --all-targets
```

At batch completion run broad checks once, not once per package:

```sh
cargo fmt --all -- --check
cargo test --offline --locked -j12 -p <affected-crate-one> -p <affected-crate-two> --all-targets
python3 <batch-generator-or-differential-check> --check
git -C .. diff --check -- rust
```
