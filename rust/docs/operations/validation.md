# Rust rewrite validation

Run commands from `rust/`. Cargo always uses 12 jobs.

## Fast package loop

Read the live Go package directly and run a focused Cargo test only when it
shortens the current edit loop. Keep translating after it passes. Do not run a
workspace sweep during package translation. If a shared public API changes,
compile or test only its direct reverse dependencies.

After the whole package and every original test/support obligation are
translated, run:

```sh
cd .. && go test ./pkg/example
cd rust && cargo test --offline --locked -j12 -p tidb-example --all-targets
```

## Whole-package closure

After the complete Go package and its original test/support owners have Rust
equivalents, run only its owning checks:

```sh
cargo fmt --all -- --check
cargo test --offline --locked -j12 -p <owning-crate> --all-targets
python3 <package-generator-or-differential-check> --check
git -C .. diff --check -- rust
```

Run workspace Clippy/tests, repository lint, and relevant live-cluster checks
once at a dependency-layer integration point or deployable milestone. They are
not leaf-package closure commands.

Large integration-test directories compile through `scripts/aggregate-tests.rs`.
This retains every test source while avoiding hundreds of tiny Cargo binaries.

Real PD/TiKV, protocol, or differential suites remain additional acceptance
requirements when the changed package reaches those boundaries.
