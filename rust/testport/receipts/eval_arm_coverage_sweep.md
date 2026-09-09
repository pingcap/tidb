# Eval-arm coverage sweep — every registered builtin reaches an eval path

## Question

The registry arity receipt (`registry_arity_parity.md`) proved accept/reject
parity for all 283 Go classes. The next tier up — does every registered name
actually EVALUATE (rather than falling through to the port's final
`EvalError::Unsupported("this scalar function is not yet ported")` fallback)
— had never been swept end to end.

## Method

A temporary in-crate probe (appended to `builtin_registry.rs`, run, then
removed — the full `FUNCTION_CLASSES` table, 309 entries, is module-private)
built each registry name with `min_args` synthetic constants and called
`ScalarFunction::eval` through the real ladder. Names were retried across
INT / STRING / DOUBLE / DATETIME argument shapes. Any outcome other than the
"not yet ported" fallback marks the name covered; a name falling through for
every shape is a coverage miss.

Result: 309 registered names, 25 fell through to the fallback.

## Triage of the 25

Every miss was traced to its build path. None is an eval gap:

| name(s) | mechanism |
|---|---|
| `adddate` `date_add` `subdate` `date_sub` | rewriter normalizes onto unit-suffixed internal names (`date_add_<unit>`, `rewriter.rs:1534`); `scalar_function.rs` dispatches those to `time_fn::calendar` |
| `trim` | rewriter builds the direction-normalized form (grammar-special `TRIM(... FROM ...)` argument) |
| `extract` | grammar-special `EXTRACT(unit FROM x)`; rewrites to the unit-keyed internal call |
| `position` | parsed infix `POSITION(a IN b)`; rewrites onto `locate` (3-arg) |
| `convert` | grammar-special `CONVERT(x, type)` / `USING`; rewrites onto the cast family |
| `case` | rewriter builds the lazy `case_when` control form (`rewriter.rs:1340`) |
| `json_memberof` | rewriter renames to `json_member_of` (`rewriter.rs:1348`), which dispatches to the ported `builtin_ext/json/predicate.rs` eval; covered by Go's own `TestJSONMemberOf` vectors (`tests/builtin_info_json_math_source.rs:822`) |
| `istrue_with_null` | Go's keep-null wrapper for IS TRUE under NOT; Rust serves the same truth table through the `istrue`/`isnottrue`/... arms (`scalar_function.rs:1135-1156`), already pinned against Go |
| `row` `default_func` `match_against` `fts_match_word` | planner/session-internal pseudo-functions, never evaluated as name-dispatched builtins (same as Go, where their classes exist for build-time routing) |
| `'tidb`.(dateliteral` `'tidb`.(timeliteral` `'tidb`.(timestampliteral` | Go's literal-parsing pseudo-classes (`dateLiteralFunctionClass` family); reachable only through the literal parse path, not name dispatch |
| `uuid_short` | documented by-design refusal (see PROGRESS journal, session cumulative) |

## The genuine boundary: six `tidb_*` server-hook functions

UPDATE 2026-09-05: `tidb_is_ddl_owner` has since been closed — its expropt
plumbing was already complete, so only the eval arm was needed; see
`receipts/tidb_is_ddl_owner_arm.md`. The other five remain the boundary.

`tidb_decode_sql_digests`, `tidb_encode_index_key`, `tidb_encode_record_key`,
`tidb_is_ddl_owner`, `tidb_mvcc_info`, `tidb_row_checksum` are Go classes
whose evals consume server-level optional eval props — latest infoschema,
privilege checker, session vars, and executor-initialized function hooks
(`EncodeIndexKeyFromRow == nil` makes Go itself answer
`"EncodeIndexKeyFromRow is not initialized"`). They are Tier: server stack,
not pure expression evaluation:

- Go anchors: `pkg/expression/builtin_info.go:1104-1157`
  (`builtinTiDBEncodeIndexKeySig`), `builtin_info_vec.go:216`
  (`builtinTiDBIsDDLOwnerSig`), and the sibling classes.
- The Rust capability layer exists (`expropt/ddlowner.rs` ports
  `DDLOwnerInfoProvider`), but wiring the evals needs the session/executor
  hook initializers and KV key-encode-from-row paths — the sibling-owned
  server tier, same boundary class as `expr_to_pb`/`tipb.Expr.Eval`.

Recorded here as the sweep's boundary classification; not a code change.

## Validation

- The probe was a throwaway: removed from the tree after producing the list
  (working-tree diff clean at commit time).
- The full tidb-expr suite stays 1207/1207 (excluding the documented network
  flake) at the same tip.

## Risk

- None: documentation-only receipt.
- Value: closes the eval-coverage tier for all 309 registered names — any
  future "function parses and passes type checking but answers not-yet-ported
  at evaluation" regression now has a swept baseline to diff against.
