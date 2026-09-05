# Rust/Go parity receipt: live constant-fold warning ownership

Date: 2026-09-06

## Scope and inventory

The complete `pkg/expression`, `pkg/planner/core`, and `pkg/session` Go
inventories were walked before editing: production and test Go files, fixtures,
generated/platform variants, Bazel/build inputs, and package metadata were all
included. The corresponding Rust `tidb-expr`, `tidb-planner`, and
`tidb-session` production, inline/standalone test, generated harness, fixture,
and metadata files were inventoried. No Go or generated source changed.

## Go behavior

TiDB constructs a scalar function with the live `BuildContext`. Constant casts
and closed arithmetic are folded at that boundary, so conversion diagnostics
belong to the statement warning buffer even when the final expression is a
constant. For example, `ABS('12abc')` and `'12abc' + 1` each retain one 1292,
`'abc' DIV 2` names DECIMAL conversion, and `1 / 0` retains one 1365. A
row-dependent expression still emits conversion warnings according to its
runtime signature and is not pre-evaluated.

## Rust gap and fix

The session plan resolver previously folded every closed subtree with a
zone-only `ZonedNoColumns` context. That made values correct but discarded
warnings before the real `StmtContext` existed. Warning-cast preservation only
covered integer casts, so REAL/DECIMAL and temporal signatures were also
folded too early. The resolver now defers value folding whenever a live
statement context is attached. `PlanBuilder::rewrite_scalar` then performs the
single normal fold with its concrete context after rewriting, preserving the
Go warning owner. The preservation classifier includes REAL, DECIMAL, DATE,
DATETIME, TIMESTAMP, and DURATION cast nodes so the zone-only pass cannot erase
their diagnostics.

## Focused regressions

The following Rust session tests pass in isolation:

- `tests_core::builtins::numeric_prefix_strings_warn_once_per_coercion`
- `tests_core::builtins::string_operands_take_go_s_per_operator_numeric_cast`
- `tests_core::numeric_domain::division_by_zero`
- `tests_core::numeric_domain::numeric_literals`
- `tests_compare_refinement` (all eight comparison tests)

The first two pin DOUBLE/DECIMAL warning text and prefix behavior; the numeric
domain tests pin 1365 ownership and decimal literal evaluation.

## Validation

Focused module commands passed with `--test-threads=1`. The Ready profile also
passed:

```text
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --all-targets
GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint
```

Warnings emitted by pre-existing lints are unchanged.
