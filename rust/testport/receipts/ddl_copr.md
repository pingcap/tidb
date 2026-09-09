# `pkg/ddl/copr` package receipt

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete package inventory

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 37 | `0cf654c6aaed30c4902e0dbc25740b44a73eadd8` | workspace crate `tidb-ddl-copr` over the existing model, datatype, and expression owners |
| `copr_ctx.go` | 442 | `26cab165d69e43a6e9a1b02ca8f51f89491d279f` | complete single/multi-index context construction and query surface in `tidb-ddl-copr/src/lib.rs` |
| `copr_ctx_test.go` | 216 | `80b4a676f63c9ee5f70c91b6a8bbe08afa78296c` | all three original tests in the crate-local test module |

There is no package doc, test harness, benchmark, fixture, generated
source/input, build/platform variant, or ownership artifact in the pinned
directory.

## Behavior and integration decision

The dedicated crate retains the source package boundary. Construction expands
virtual-column dependencies, preserves table column order, adds the correct
integer/common/extra-row handle, resolves index and virtual-column output
offsets, and retains the expression context so each condition request is built
on demand. Single-index lookup ignores its id; multi-index lookup returns Go
nil (`None`) for an unknown id. Multi-index conditions require every index to
have a pushable condition and otherwise compose the predicates as one balanced
DNF through the shared expression owner.

The implementation uses the existing `pkg/meta/model` pointer carriers and
the existing `pkg/expression` simple-expression, generated-column, extraction,
and DNF capabilities. It adds no alternate parser, condition evaluator,
fallback scan policy, or Rust-only crossover.

## WIP validation

Run from `rust/`:

```text
cargo fmt --all -- --check
cargo check --offline -p tidb-ddl-copr
cargo test --locked -p tidb-ddl-copr
```

The package suite passes the three tests present in the pinned inventory.

## Follow-up closure — discardable context query returns (2026-09-06)

The complete three-artifact, 695-line Go package was re-read at current
`origin/master` `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; its production,
test, and BUILD files remain byte-identical to the pinned source. The Rust
owner and all existing source tests were reviewed before editing.

Go permits callers to discard the `GetBase`, `IndexColumnOutputOffsets`,
`IndexInfo`, and `GetSchemaAndNames` results exposed by the single-index,
multi-index, and interface context types. Rust had marked ten direct methods
`#[must_use]`, adding a Rust-only return contract. The annotations were
removed without changing context construction, index lookup, or schema/name
resolution. `tests::go_context_query_returns_can_be_ignored` discards all ten
methods under `#[deny(unused_must_use)]`; with the annotations restored it
failed with exactly ten diagnostics, and after the edit it passes.

Ready validation for this Rust-only follow-up:

```text
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-ddl-copr --lib go_context_query_returns_can_be_ignored -- --test-threads=1
PASS; 1 passed, 0 failed.

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-ddl-copr --lib --test-threads=1
PASS; 4/4 owner tests passed.

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-ddl-copr --all-targets
PASS.

rustfmt +nightly-2026-08-22 --check --edition 2021 rust/crates/tidb-ddl-copr/src/lib.rs
PASS.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
PASS.

git diff --check
PASS.
```

Only Rust source/tests and parity documentation changed. No Go, Bazel, Cargo
dependency, or module file changed, so `make bazel_prepare` was not required.
