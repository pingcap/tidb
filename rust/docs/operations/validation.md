# Rust rewrite validation

Run commands from `rust/`. Cargo always uses 12 jobs.

## Package loop

During implementation, run the smallest relevant Cargo test directly. When one
complete Go package and all of its original tests/support are translated, run:

```sh
scripts/package-port.py finish pkg/example \
  --crate tidb-example \
  --rust-path rust/crates/tidb-example/src/lib.rs \
  --rust-path rust/crates/tidb-example/tests/package_source.rs \
  --test-target tidb-example:package_source
```

`finish` derives the live Go inventory and direct internal dependencies, then
runs formatting, strict all-target Clippy, crate library tests, and only the
declared integration-test targets. It writes one file under `ports/` after all
checks pass. A later repair can omit the mapping flags and reuse the proof.

Useful read-only commands:

```sh
scripts/package-port.py inventory pkg/example
scripts/package-port.py check pkg/example
scripts/package-port.py check
```

## Pre-push checkpoint

Run the workspace-wide sweep once before push/release or after a shared
foundation changes:

```sh
scripts/package-port.py checkpoint
```

It checks every package proof, then runs workspace formatting, all-target
Clippy, tests, tool regressions, and `git diff --check`. Real PD/TiKV, protocol,
or differential suites remain additional requirements when the changed package
reaches those boundaries.

Large integration-test directories are compiled as aggregate harnesses through
`scripts/aggregate-tests.rs`; topology-sensitive files remain standalone. The
checkpoint runs aggregate tests with 12 internal threads and standalone test
binaries 12-way. Every source file still executes, but Cargo no longer launches
one process for each tiny test file.

There is no queue, start step, claim, campaign, generated ledger, transfer,
frozen workspace, or separate receipt.
