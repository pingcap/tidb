# `ROW_COUNT()` / `LAST_INSERT_ID()` evaluation (builtin_info parity)

Go source: `origin/master` at
`d152e4b78d35cfcb771bfabc289f837c2374d4aa` (2026-09-03). The two functions
were registered and typed but had no evaluation arms — a built node answered
`Unsupported` where Go answers a number.

## Go behavior (the oracle)

- `ROW_COUNT()` (`builtin_info.go:913-923`): returns
  `vars.StmtCtx.PrevAffectedRows` — the preceding statement's affected-row
  count, never NULL, 0 when nothing preceded it.
- `LAST_INSERT_ID()` (`builtin_info.go:482-489`): returns
  `vars.StmtCtx.PrevLastInsertID` through an ETInt result the vectorized
  source pins as UNSIGNED — NULL when the context has no generated id.
- `LAST_INSERT_ID(expr)` (`builtin_info.go:508-521`): evaluates the integer
  expression (NULL propagates), records `vars.SetLastInsertID(uint64(res))`
  (`session.go:2759-2763`, which also sets `LastInsertIDSet`), and returns
  the same value.

## The Rust implementation

- `Columns::row_count()` existed (`Option<i64>`, default `None`); the
  `row_count` eval arm maps `None → NULL` following the `found_rows`
  precedent.
- The `last_insert_id` 0-arg arm maps `Some → UInt(id)`, `None → NULL` —
  the UNSIGNED kind the pre-existing `vectorized_builtin_info_func` pin
  demands (`builtinLastInsertIDSig.evalInt`'s int64 rides an ETInt result).
- The `last_insert_id(expr)` 1-arg arm evaluates the integer (cast via
  `cast_arg_as_int`, NULL propagates), records it with
  `ctx.set_last_insert_id`, and returns `Int(recorded)` — Go's evalInt
  returns the int64 expression value.
- `builtin_return_type` is re-exported `pub(crate)` from the rewriter for
  the control-function rebuild (see `cast_hybrid_push.md`).

## Regression

- `simple_expr::tests::info_functions_read_and_record_session_counts`
  — FAIL-BEFORE (pre-fix all three shapes answered
  `Unsupported("ROW_COUNT requires a session")`-class errors because no eval
  arms existed). Pins ROW_COUNT `Some(7)`/`None`, LAST_INSERT_ID `UInt(5)` /
  NULL, and the recording side effect of the 1-arg form.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-expr --no-fail-fast
# 1187 run, 1186 passed, 1 failed — only the documented network flake
# (json_schema_valid_resolves_file_and_http_references); the pre-existing
# vectorized_builtin_info_func pin stays green.
cargo +nightly-2026-08-22 clippy --offline --locked -p tidb-expr --all-targets
# clean in touched code
```

## Risk

- Correctness: low; the arms read only the context accessors that the exec
  tier already implements (`StmtContext` publishes the same session state Go
  reads from `SessionVars`).
- Compatibility: the 0-arg `LAST_INSERT_ID` keeps the UNSIGNED kind the
  vectorized source pins; `ROW_COUNT`'s None → NULL follows the house
  `found_rows` convention.
