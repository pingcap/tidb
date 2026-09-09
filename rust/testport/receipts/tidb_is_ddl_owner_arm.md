# `tidb_is_ddl_owner`: the DDL-owner arm reads the existing optional prop

## Divergence

The eval-arm coverage sweep (`eval_arm_coverage_sweep.md`) classified six
`tidb_*` server-hook functions as the server-tier boundary. Five of them
(`mvcc_info`, `row_checksum`, `decode_sql_digests`, `encode_index_key`,
`encode_record_key`) genuinely need executor-initialized hooks or new
provider types and stay there. `tidb_is_ddl_owner` is different: its entire
plumbing already existed on this branch —

- Go: `builtinTiDBIsDDLOwnerSig.evalInt` (`pkg/expression/builtin_info.go:627`)
  answers 1/0 through `DDLOwnerPropReader.IsDDLOwner`
  (`pkg/expression/expropt/ddlowner.go:42`); a context without the provider
  fails with `optional property: 'OptPropDDLOwnerInfo' not exists in
  EvalContext` (`expropt/optional.go:111`).
- Rust: `expropt/ddlowner.rs` ports the provider, the reader, and the
  err-when-missing/value-when-provided semantics (pinned in
  `expropt/tests.rs`) — only the eval arm was missing, so the name fell
  through to the generic "not yet ported" fallback.

## Fix

- `crates/tidb-expr/src/context.rs`: new `Columns::ddl_owner_info` default
  returning Go's `getPropProvider` missing-provider error verbatim. A
  session that bridges `EvalPropContext` overrides it through
  `DdlOwnerPropReader`.
- `crates/tidb-expr/src/scalar_function.rs`: the
  `tidb_is_ddl_owner` arm — zero arguments, `Datum::Int(1/0)` from the
  provider, never NULL (`Go builtin_info.go:627` returns `res, false`),
  `WrongParameterCount` (1582) on extra arguments.

The five genuinely server-hook-bound functions keep their boundary
classification; their sweep-receipt entry now names `tidb_is_ddl_owner` as
the closed exception.

## Fail-before / pass-after

`tidb_is_ddl_owner_reports_the_missing_provider_like_go`
(`scalar_function.rs` tests): asserts Go's missing-provider error text on a
provider-less context and 1582 on an extra argument. FAILS before the arm
(the fallback answered the generic not-yet-ported error) and passes after.
Verified by temporarily disabling the arm and re-running.

## Validation

- `tidb-expr` full suite: 1209/1209 (excluding the documented network flake).
- fmt clean; no new clippy warnings in the touched regions.

## Go anchors

- `pkg/expression/builtin_info.go:607-628` (class + sig + evalInt)
- `pkg/expression/expropt/ddlowner.go:42-48`
- `pkg/expression/expropt/optional.go:109-112` (missing-provider error)
- `pkg/expression/builtin_info_vec.go:216-228` (vectorized twin)
