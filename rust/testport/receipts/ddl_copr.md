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
