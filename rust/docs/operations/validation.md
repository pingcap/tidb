# Rust rewrite validation

Run commands from `rust/`. Cargo always uses 12 jobs.

## Fast package loop

Read the complete live Go inventory:

```sh
scripts/port.py inventory pkg/example --verbose
```

During implementation, run the smallest relevant Cargo test directly. After
the whole package and every original test/support obligation are translated:

```sh
scripts/port.py record pkg/example -p tidb-example
```

`record` checks dependency closure, runs every target in each named crate once,
then atomically stores the package digest and crate names in
`ported-packages.json`. A repair can omit `-p` and reuse the existing crates.

## Pre-push validation

Use ordinary tools once before push:

```sh
cargo fmt --all -- --check
cargo clippy --offline --locked -j12 --workspace --all-targets -- -D warnings
cargo test --offline --locked -j12 --workspace --all-targets
cargo test --offline --locked -j12 --workspace --doc
python3 -m unittest scripts/test_port.py
git -C .. diff --check -- rust
```

Large integration-test directories compile through `scripts/aggregate-tests.rs`.
This retains every test source while avoiding hundreds of tiny Cargo binaries.

Real PD/TiKV, protocol, or differential suites remain additional acceptance
requirements when the changed package reaches those boundaries. There is no
queue, start step, claim, campaign, gate, transfer, frozen workspace, or
receipt.
