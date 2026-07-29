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

## Test-coverage inventory

Measures which Go tests of a transcreated package have a Rust counterpart, so
`AGENTS.md` non-negotiable 6 ("a claim includes every original test artifact")
is checkable instead of assumed. Run from the repository root:

```sh
python3 rust/scripts/test-coverage-inventory.py           # rewrite the doc
python3 rust/scripts/test-coverage-inventory.py --check   # fail if stale
```

It shells out to `rust/difftests/tools/go_test_declaration_inventory` for the
Go side, so a working Go toolchain is required; pass `--cache <path>` to reuse
that TSV across runs. Output is `docs/operations/test-coverage-inventory.md`
(generated, do not hand-edit); the ranked gaps and the confidence accounting
are hand-written in `docs/operations/test-coverage-gaps.md`.

When a Go package is transcreated, add its `Mapping(...)` row to the script and
regenerate.
