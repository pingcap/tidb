# ALTER USER security ownership

## Purpose

Directly transition Go's `AlterUserStmt` contract into a stable user/security
AST and parser module. Preserve per-account specifications and the fixed Go
restore ordering for TLS, resource, password/lock, comment/attribute, and
resource-group options. Execution remains unsupported before mutation.

## Source boundary

- `pkg/parser/ddl_user_parser.go:parseAlterUserStmt`
- `pkg/parser/ddl_user_parser.go:parseAlterUserSpec`
- `pkg/parser/ast/misc.go:AlterUserStmt.Restore`

## Progress

- [x] Extract account/auth/password/TLS/resource payloads into `tidb-ast/src/stmt/user.rs` and restore in Go source order.
- [x] Route ALTER USER through `tidb-parser/src/user.rs`; that module owns named specs, `USER()`, TLS, resource, and resource-group parsing.
- [x] Preserve existing authentication/dual-password/password-expire/resource-group behavior and add the remaining option families.
- [x] Prove the controlled static parser delta from 49,123 to 49,165 matched rows (+42).
- [x] Keep execution rejected before mutation through the existing `Database::run_ddl` unsupported arm.
- [x] Run the exact 42-row security selector and reviewed full static snapshot gate.

## Validation

- `cargo check -j 12 -p tidb-parser --all-targets` (pass)
- `cargo test -j 12 -p tidb-parser` (216 passed)
- `cargo clippy -j 12 -p tidb-parser --all-targets -- -D warnings` (pass)
- `cargo run -j 12 -p difftest --bin integration_parser_queue -- --check` (`rust_matched=49165`, exact +42)
- `cargo test -j 12 -p difftest --test selector_security` (5 passed, including exact 42-row slice)
- `cargo test -j 12 -p difftest --test integration_parser_diff -- --nocapture` (pass: 49,165 matched; 1,520 raw parse failures)
- `cargo test -j 12 -p tidb-exec --lib alter_user_` (2 pre-mutation rejection tests passed)
- `cargo clippy -j 12 -p tidb-ast --all-targets -- -D warnings` (pass)
- `cargo clippy -j 12 -p difftest --test selector_security -- -D warnings` (pass)
- `cargo clippy -j 12 -p tidb-exec --lib -- -D warnings` (pass)
- `rustfmt --edition 2021 --config skip_children=true --check <ALTER USER owned files>` (pass)

The broader `cargo test -j 12 -p tidb-exec --lib` sweep reached 225/226;
both ALTER USER tests passed and the sole unrelated failure was the in-flight
Datum migration's `tests::expr::hex_bit_literal_eval` UTF-8 expectation.

## Source-fidelity decisions

- Bare named specifications are legal because statement-global options follow the complete account list.
- TLS and resource options retain the fixed Go order and use closed enums instead of stringly typed kinds.
- Empty `REQUIRE`/`WITH` option lists follow the hand parser and disappear during restore.
- Resource counts are strict `i64`; values above MaxInt64 are rejected.
- Password/lock counts are `i64`; positive overflow saturates to MaxInt64, matching Go's ignored `strconv.ParseInt` error value.
- SSL/X509/NONE duplicates are parse errors; duplicate string-valued TLS constraints remain parseable for later runtime validation, matching Go.
