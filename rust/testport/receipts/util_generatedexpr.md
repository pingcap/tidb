# `pkg/util/generatedexpr` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

| Artifact | Bytes | Blob |
| --- | ---: | --- |
| `pkg/util/generatedexpr/generated_expr.go` | 2,729 | `f22f6339911db6ca297f93d72fb259e7391ec355` |
| `pkg/util/generatedexpr/gen_expr_test.go` | 965 | `ea4cc590ca036e59d0b274e4e10a6e74d6cb2ea1` |
| `pkg/util/generatedexpr/main_test.go` | 1,198 | `cecfabc08b05034156e907bcdafd656af591b537` |
| `pkg/util/generatedexpr/BUILD.bazel` | 879 | `ec49ebef428a74ea8de70cf75b8f427636d89563` |

There is no `doc.go`, fixture, benchmark, generated source, or platform
variant. `main_test.go` is process-wide Go test/goleak setup and carries no
package behavior.

## Rust ownership and behavior

`rust/crates/tidb-model/src/generated_expr.rs` is the sole native owner:

- `parse_expression` prepends `select `, invokes the ordinary TiDB parser,
  and returns the first projection expression from the first statement;
- parser errors propagate through the native parser error type;
- `simple_resolve_name` walks the expression bottom-up, compares only each
  column name's case-insensitive component against `TableInfo.Columns`,
  preserves the name-based AST, and stops with Go's exact
  `can't find column <column> in <table>` text at the first miss.

The owner is used directly by generated columns, expression indexes,
partition expressions, and persisted-default loading. There is no parallel
compatibility wrapper.

The pinned package has one behavioral test, `TestParseExpression`; Rust keeps
that exact JSON_EXTRACT case. Four supplemental Rust tests for first-field,
syntax-error, and name-resolution behavior were removed because Go has no
corresponding package tests.

## Validation

Profile: WIP; this completes one pinned package inside the continuing parity
audit.

- `cargo test --quiet --offline -p tidb-model generated_expr::tests::parse_expression_matches_go_test -- --exact`
- `cargo check --quiet --offline -p tidb-model -p tidb-executor`
- `cargo fmt --all -- --check`
- `git diff --check`

No Go/Bazel source changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: production parsing and resolution are unchanged; the deleted
  code was source-absent test coverage only.
- Compatibility: no public API changed.
- Performance: no runtime path changed.
