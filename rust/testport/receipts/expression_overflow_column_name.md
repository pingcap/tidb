# `pkg/expression` integer-overflow column-name parity receipt

Status: fixed in the Rust `tidb-expr` owner. This is one package-scoped Rust
batch; no Go, generated, fixture, platform, or Bazel input changed.

## Go package inventory

The Go authority is `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`. Before editing, the complete
recursive `pkg/expression` tree was rechecked: 208 tracked artifacts and
146,291 lines. That inventory includes 68 production Go files, 60 Go test
files, seven generated/vectorized Go sources, the root and nested
`BUILD.bazel` metadata, `OWNERS`, the generator/helper packages, the
integration-test package and README, and the constant-propagation and
multi-valued-index fixture/test packages. There is no `doc.go`, platform-only
Go source, or additional generated input outside those 208 artifacts.

The relevant source/test contract was reread in
`pkg/expression/builtin_arithmetic.go`, `builtin_arithmetic_test.go`,
`builtin_arithmetic_vec.go`, `builtin_arithmetic_vec_test.go`,
`scalar_function.go`, `expression.go`, and `column.go`. Go's checked integer
arithmetic reports `types.ErrOverflow` with the result class and the two
operands rendered through `Expression.StringWithCtx`; a resolved column uses
its qualified `OrigName` (for example `test.t.col1`) rather than an internal
`Column#N` label.

## Rust owner and fix

The Rust owner is `rust/crates/tidb-expr`: 176 tracked artifacts (175 Rust
sources/tests plus `Cargo.toml`) and 111,612 lines at this checkout. Its
`scalar_function.rs` overflow adapter previously rendered constants only and
returned bare `EvalError::IntOverflow` whenever either operand was a column.
That was a Rust-only behavior: a row-backed `MinInt64 * -1` lost Go's
qualified expression in the client-visible 1690 message.

The adapter now renders `Column.orig_name` and the embedded name of a
`CorrelatedColumn` when Go's resolver has populated it. Unnamed/non-renderable
operands retain the prior safe fallback instead of inventing a name. The
existing source-derived regression in
`tests/aggregation_arithmetic_cast_source.rs` is active and evaluates a real
row-backed `test.t.col1` with `MinInt64`, asserting exactly:

```text
BIGINT value is out of range in '(test.t.col1 * -1)'
```

## Regression and validation

The focused test failed before the production renderer change with bare
`IntOverflow`, then passed after the change:

```text
OPENSSL_DIR=/usr/local/corplink/mdm/opt/corplink-mdm/policy \\
OPENSSL_LIB_DIR=/usr/local/corplink/mdm/opt/corplink-mdm/policy/lib \\
OPENSSL_INCLUDE_DIR=/usr/local/corplink/mdm/opt/corplink-mdm/policy/include \\
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \\
  -p tidb-expr --lib \\
  tests::aggregation_arithmetic_cast_source::test_arithmetic_overflow_error_message_with_column_name \\
  -- --exact --nocapture
# pre-fix: failed (left IntOverflow, expected the qualified DataOutOfRange text)
# post-fix: 1 passed
```

Ready validation for this package batch:

```text
OPENSSL_DIR=/usr/local/corplink/mdm/opt/corplink-mdm/policy \\
OPENSSL_LIB_DIR=/usr/local/corplink/mdm/opt/corplink-mdm/policy/lib \\
OPENSSL_INCLUDE_DIR=/usr/local/corplink/mdm/opt/corplink-mdm/policy/include \\
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \\
  -p tidb-expr --all-targets
# passed (existing warnings only)

OPENSSL_DIR=/usr/local/corplink/mdm/opt/corplink-mdm/policy \\
OPENSSL_LIB_DIR=/usr/local/corplink/mdm/opt/corplink-mdm/policy/lib \\
OPENSSL_INCLUDE_DIR=/usr/local/corplink/mdm/opt/corplink-mdm/policy/include \\
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \\
  -p tidb-expr --lib -- --test-threads=1
# complete owner suite: 1,187 passed, 99 ignored, 0 failed

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml \\
  -p tidb-expr -- --check
git diff --check
# both passed

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \\
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \\
TMPDIR=/tmp/tidb-codex-go-lint make lint
# passed
```

No Go test was changed or required by this Rust-only batch. The known full
workspace formatter drift in unrelated `tidb-session` files is outside this
owner and remains untouched.

## Risks and boundaries

The fix only uses names already carried by resolved Rust columns. It does not
change arithmetic, overflow classification, unsigned handling, or unnamed
column fallback. Expression rendering for other non-constant node kinds and
the broader Go `StringWithCtx` redaction modes remain outside this bounded
batch.
