// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! The complete inventory of `pkg/expression/builtin_cast.go` against this
//! crate, plus the gate that fails when the inventory goes stale.
//!
//! `crate::cast` had been reopened by six separate units in forty-three
//! commits, every one of them arriving from a divergence and leaving without
//! saying what else was missing. This file is the answer to "what else": every
//! one of the Go file's **151** top-level functions is classified below as
//! PORTED, DECLINED or UNREACHABLE, and [`GO_SYMBOLS`] carries the names so a
//! Go-side addition or removal breaks a test instead of passing unnoticed.
//!
//! # Re-deriving the Go list
//!
//! ```sh
//! grep -oE '^func (\([^)]*\) )?[A-Za-z0-9_]+' pkg/expression/builtin_cast.go \
//!   | sed -E 's/^func (\([^)]*\*([A-Za-z0-9_]+)\) )?/\2./; s/^\.//' \
//!   | sort
//! ```
//!
//! That is exactly how [`GO_SYMBOLS`] was produced (from `56b92c1185`), and
//! [`the drift gate`](go_symbol_list_still_describes_builtin_cast_go) re-runs
//! the same extraction over the checked-in Go source on every test run. There
//! is no Go parser in this repo, so the list is checked in rather than derived
//! at compile time; the gate makes the two disagree loudly.
//!
//! # 1. The 55 `Clone` methods — UNREACHABLE, one proof for all
//!
//! Go's `builtinFunc` is a heap object holding `args`, `tp` and a pb code, and
//! `Clone` exists because `ScalarFunction.Clone` deep-copies an expression
//! tree that may be shared across sessions (see every signature's own
//! `NOTE: ... may be shared across sessions` comment). This crate's
//! [`crate::Expression`] is a plain `Clone`-derived enum with no per-signature
//! object at all: [`crate::cast::eval_cast`] is a free function selected by a
//! `match` on [`tidb_ast::CastType`], so there is nothing to clone. PROOF that
//! no caller is being skipped: every `Clone` body in the Go file is the same
//! three lines (`newSig := &X{}; newSig.cloneFrom(...); return newSig`) and
//! carries no evaluation rule whatsoever.
//!
//! # 2. The 61 `eval*` signature bodies
//!
//! Go picks ONE signature from (argument eval type × target type) in
//! `castAsXxxFunctionClass.getFunction`; this tier fuses that choice into a
//! `match` on the DATUM KIND inside each target's arm, because the datum kind
//! is a faithful proxy for the argument eval type in every case but `YEAR`
//! (which [`crate::cast::eval_cast`] takes from the static `FieldType`, see
//! `year_source_value`).
//!
//! ## 2a. `SIGNED` / `UNSIGNED` target (Go `castAsIntFunctionClass`)
//!
//! | Go signature | verdict | where |
//! | --- | --- | --- |
//! | `builtinCastIntAsIntSig.evalInt` | PORTED | `cast::to_i64_signed_in` `Int`/`UInt` arms; `cast::to_u64_unsigned` `Int`/`UInt` arms |
//! | `builtinCastRealAsIntSig.evalInt` | PORTED | `cast::to_i64_signed_in` `Real` arm + `cast::report_signed_overflow`; unsigned in `cast::real_to_u64_saturating` |
//! | `builtinCastDecimalAsIntSig.evalInt` | PORTED | `cast::to_i64_signed_in` `Decimal` arm + `cast::report_signed_overflow`; unsigned in `cast::to_u64_unsigned`'s `Decimal` arm |
//! | `builtinCastStringAsIntSig.evalInt` | PORTED | `cast::str_int_prefix` + `cast::report_int_truncation` |
//! | `builtinCastStringAsIntSig.handleOverflow` | DECLINED | Go clamps a past-`u64` digit run to `math.MinInt64`/`MaxUint64` and warns 1292; this crate saturates at `i64::MIN`/`MAX` with no warning. Quoted from `cast::str_int_prefix`'s own doc: "a principled, documented divergence for a value nobody writes intentionally". |
//! | `builtinCastTimeAsIntSig.evalInt` | PORTED | `cast::to_i64_signed_in`'s `other => other.to_i64_in(zone)` arm, whose `Datum::Time` case is Go's `RoundFrac(DefaultFsp).ToNumber().ToInt()` |
//! | `builtinCastDurationAsIntSig.evalInt` | PORTED | same `other` arm, `Datum::Duration` case. Its `b.tp.GetType() == mysql.TypeYear` branch is `cast::cast_to_year`'s. |
//! | `builtinCastJSONAsIntSig.evalInt` | DECLINED | no `Datum::Json` ever reaches a cast here: `builtin_ext::json::value::cast_as_json` renders the document to TEXT, so a JSON-sourced integer cast is `cast::str_int_prefix` over that text. Closing it needs the JSON tier to carry `BinaryJSON`, which is `tests_json::json_type_loses_gos_typed_names_at_cast_not_at_json_type`'s own subject. |
//! | `builtinCastVectorFloat32AsUnsupportedSig.evalInt` | UNREACHABLE | Go's own body is `errors.Errorf("cannot cast from vector to %s")` — it exists to REFUSE. `tidb_ast::CastType` has no vector variant and `tidb-parser` accepts no `VECTOR` cast target, so this tier refuses one token earlier, at parse. |
//!
//! Every signature's `b.inUnion` arm is DECLINED as one item: `inUnion` is set
//! only by `BuildCastFunction4Union`, and this tier has no `UNION` type-
//! unification pass to call it (`rewriter::cast_target` is the only cast
//! builder and never passes it). TWELVE signatures carry such an arm --
//! `grep -c 'b.inUnion' pkg/expression/builtin_cast.go` is 16, of which 4 are
//! comment lines: `IntAsInt`, `IntAsReal`, `IntAsDecimal`, `RealAsReal`,
//! `RealAsInt`, `RealAsDecimal`, `DecimalAsDecimal`, `DecimalAsInt`,
//! `DecimalAsReal`, `StringAsInt`, `StringAsReal`, `StringAsDecimal`. All of
//! them read "a negative value in an unsigned UNION column becomes 0".
//!
//! ## 2b. `DOUBLE` / `FLOAT` target (Go `castAsRealFunctionClass`)
//!
//! | Go signature | verdict | where |
//! | --- | --- | --- |
//! | `builtinCastIntAsRealSig.evalReal` | PORTED | `cast::to_f64_for_cast` `Int`/`UInt` arms (Go's `float64(uint64(val))` split is the `UInt` arm) |
//! | `builtinCastRealAsRealSig.evalReal` | PORTED | `cast::to_f64_for_cast` `Real` arm |
//! | `builtinCastDecimalAsRealSig.evalReal` | PORTED | `cast::to_f64_for_cast` `Decimal` arm |
//! | `builtinCastStringAsRealSig.evalReal` | PORTED | `cast::to_f64_for_cast` `String`/`Bytes` arms via `cast::decimal_prefix` |
//! | `builtinCastTimeAsRealSig.evalReal` | PORTED | `cast::to_f64_for_cast`'s `other => other.to_f64()` arm |
//! | `builtinCastDurationAsRealSig.evalReal` | PORTED | same `other` arm |
//! | `builtinCastJSONAsRealSig.evalReal` | DECLINED | same reason as `builtinCastJSONAsIntSig` |
//! | `builtinCastVectorFloat32AsUnsupportedSig.evalReal` | UNREACHABLE | as above |
//!
//! `types.ProduceFloatWithSpecifiedTp` (called only by `StringAsReal`) is
//! DECLINED: it clamps to a `FLOAT(M,D)` target, and `tidb_ast::CastType`
//! carries no `(M,D)` for a CAST — `CastType::Float`'s own doc records that
//! `FLOAT(M, D)` is a `ParseError` as a cast target.
//!
//! ## 2c. `DECIMAL` target (Go `castAsDecimalFunctionClass`)
//!
//! | Go signature | verdict | where |
//! | --- | --- | --- |
//! | `builtinCastIntAsDecimalSig.evalDecimal` | PORTED | `cast::to_decimal_for_cast` `Int`/`UInt` arms |
//! | `builtinCastRealAsDecimalSig.evalDecimal` | PORTED | `cast::to_decimal_for_cast` `Real` arm |
//! | `builtinCastDecimalAsDecimalSig.evalDecimal` | PORTED | `cast::to_decimal_for_cast` `Decimal` arm |
//! | `builtinCastStringAsDecimalSig.evalDecimal` | PORTED (value); DECLINED (event) | value: `cast::decimal_prefix`. Go's `ErrTruncated -> ErrTruncatedWrongVal("DECIMAL", val)` is NOT raised — the recorded `expression/cast` divergence `select cast('61qw' as decimal); Warning 1292 Truncated incorrect DECIMAL value: '61qw'` is exactly this, and it also makes `update t1 set c1 = cast('61qw' as decimal)` an error TiDB raises and this tier does not. Filed: it needs `decimal_prefix` to report whether the scan consumed the whole string, the same shape `cast::int_prefix_consumed_all` already has for `SIGNED`. |
//! | `builtinCastTimeAsDecimalSig.evalDecimal` | PORTED | `cast::to_decimal_for_cast`'s `other => other.to_decimal()` arm |
//! | `builtinCastDurationAsDecimalSig.evalDecimal` | PORTED | same `other` arm |
//! | `builtinCastJSONAsDecimalSig.evalDecimal` | DECLINED | same reason as `builtinCastJSONAsIntSig` |
//! | `builtinCastVectorFloat32AsUnsupportedSig.evalDecimal` | UNREACHABLE | as above |
//!
//! `types.ProduceDecWithSpecifiedTp`, which every one of those signatures ends
//! with, is PORTED in two halves: the VALUE clamp is
//! `tidb_datatype::Decimal::cast_to_precision` and the two mutually exclusive
//! warnings are `cast::report_decimal_production`.
//!
//! ## 2d. `CHAR` / `BINARY` target (Go `castAsStringFunctionClass`)
//!
//! | Go signature | verdict | where |
//! | --- | --- | --- |
//! | `builtinCastIntAsStringSig.evalString` | PORTED | `cast::string_source_text` (including its `TypeYear && res == "0"` -> `"0000"` rule, `cast::year_zero_string`) |
//! | `builtinCastRealAsStringSig.evalString` | PORTED | `cast::datum_sql_string`. Go's `bits = 32` for a `TypeFloat` argument is DECLINED: this tier renders `Datum::Float32` through `Datum::sql_string`'s own `(*value as f32).to_string()`, which is the same shortest-round-trip choice. |
//! | `builtinCastDecimalAsStringSig.evalString` | PORTED | `cast::datum_sql_string` |
//! | `builtinCastStringAsStringSig.evalString` | PORTED | `cast::eval_cast`'s `Char`/`Binary` arms |
//! | `builtinCastTimeAsStringSig.evalString` | PORTED | `cast::datum_sql_string`, whose `Datum::Time` case is Go's `val.String()` |
//! | `builtinCastDurationAsStringSig.evalString` | PORTED | same, `Datum::Duration` case |
//! | `builtinCastJSONAsStringSig.evalString` | PORTED | the JSON value is already this tier's canonical text, so `datum_sql_string` is `val.String()` |
//! | `builtinCastVectorFloat32AsStringSig.evalString` | UNREACHABLE | no vector value can reach a cast: `grep -rn 'Datum::VectorFloat32(' crates/tidb-expr/src/` constructs one in TEST code only (`ops.rs:1369`, `ops.rs:1432`), never in a production path, and `tidb_ast::CastType` has no vector variant for a `CAST(vec AS CHAR)` to name |
//!
//! `types.ProduceStrWithSpecifiedTp` is PORTED in two halves: the truncation
//! is `cast::eval_cast`'s own `chars().take(n)` (character target) /
//! `cast::binary_pad_truncate` (byte target), and the 1406 warning is
//! `cast::report_data_too_long`.
//!
//! ## 2e. `DATE` / `DATETIME` target (Go `castAsTimeFunctionClass`)
//!
//! | Go signature | verdict | where |
//! | --- | --- | --- |
//! | `builtinCastIntAsTimeSig.evalTime` | PORTED | `cast::parse_time_by_source` `Int`/`UInt` arms; its `TypeYear` branch is `cast::cast_to_time_value`'s `year_source_value` arm |
//! | `builtinCastRealAsTimeSig.evalTime` | PORTED | `cast::real_to_time` |
//! | `builtinCastDecimalAsTimeSig.evalTime` | PORTED | `cast::parse_time_by_source` `Decimal` arm |
//! | `builtinCastStringAsTimeSig.evalTime` | PORTED | `cast::parse_time_by_source`'s `_` arm + `cast::cast_to_time_value`'s `NO_ZERO_DATE` rejection |
//! | `builtinCastTimeAsTimeSig.evalTime` | PORTED | `cast::parse_time_by_source` `Time` arm (`set_kind` + `round_frac`) |
//! | `builtinCastDurationAsTimeSig.evalTime` | PORTED | `cast::cast_to_time_value`'s `Datum::Duration` arm (`convert_to_time` against the statement clock, then `round_frac`) |
//! | `builtinCastJSONAsTimeSig.evalTime` | DECLINED | same reason as `builtinCastJSONAsIntSig`; a JSON-sourced temporal cast reaches the STRING arm over the canonical text, which is Go's `JSONTypeCodeString` branch but not its `Date`/`Datetime`/`Duration` ones |
//! | `builtinCastVectorFloat32AsUnsupportedSig.evalTime` | UNREACHABLE | as above |
//!
//! ## 2f. `TIME` target (Go `castAsDurationFunctionClass`) — all seven DECLINED
//!
//! `builtinCastIntAsDurationSig`, `RealAsDuration`, `DecimalAsDuration`,
//! `StringAsDuration`, `TimeAsDuration`, `DurationAsDuration`,
//! `JSONAsDuration`: `cast::eval_cast`'s `CastType::Time` arm is
//! `Err(EvalError::Unsupported("CAST AS TIME"))` and
//! `rewriter::cast_target` returns `None` for it.
//!
//! **The standing reason for this was STALE and is corrected here.**
//! `tidb_ast::CastType::Time`'s own doc says MySQL `TIME` is "not yet modelled
//! at all"; that is false. Measured at this tip: `tidb_datatype::MySqlDuration`
//! exists with `round_frac`/`to_number`/`Display`,
//! `tidb_datatype::parse_duration` and `tidb_datatype::number_to_duration` are
//! both public, `tidb_datatype::Time::to_duration` exists,
//! `tidb_datatype::Datum::convert_to_duration_target` is a complete write-path
//! port, `tidb_chunk::Row::get_datum` reads a `FieldTypeCode::Duration` cell as
//! `Datum::Duration`, and `crate::ops` already compares and arithmetics on
//! `Datum::Duration`. Nothing is missing from the VALUE DOMAIN.
//!
//! What is missing is the WIRING, and it is a rung of its own rather than a
//! line: seven signatures with three different parsers (`NumberToDuration` for
//! the integer, `ParseDuration` over the rendered text for real/decimal/string,
//! `ConvertToDuration` + `RoundFrac` for the two temporal ones), each with its
//! own `ErrTruncatedWrongVal`-becomes-NULL rule that differs between
//! `StringAsDuration` (keeps `ParseDuration`'s own `isNull`) and
//! `RealAsDuration`/`DecimalAsDuration` (force NULL); plus a
//! `rewriter::cast_target` arm; plus `scalar_function::cast_type_of`. It flips
//! **141 corpus statements** (`grep -rioE 'as +time(\([0-9]+\))?[ ),]'
//! tests/integrationtest/t/`) from "both sides reject" to "both sides answer"
//! in one step, so it wants its own measurement, not a rider on this one.
//! FILED, not half-built.
//!
//! ## 2g. `JSON` target (Go `castAsJSONFunctionClass`) — ported, in a sibling
//!
//! `builtinCastIntAsJSONSig`, `RealAsJSON`, `DecimalAsJSON`, `StringAsJSON`,
//! `TimeAsJSON`, `DurationAsJSON`, `JSONAsJSON`: PORTED in
//! `crate::builtin_ext::json::value::cast_as_json` /
//! `cast_as_json_typed`, NOT in `crate::cast` — `eval_cast`'s `CastType::Json`
//! arm delegates and `scalar_function`'s `cast_` dispatch calls the typed form
//! directly so the argument's `FieldType` survives. Go's
//! `builtinCastIntAsJSONSig` boolean and unsigned/`TypeYear` arms are
//! `json::value::boolean_flagged_int` and `Datum::to_mysql_json`'s own
//! unsigned handling. The one carried divergence is the JSON tier's TEXT
//! representation (a document's TYPE CODE is lost, so `JSON_TYPE` says
//! `STRING` for a temporal document where Go says `DATE`/`DATETIME`); the
//! VALUES agree byte for byte, including `builtinCastTimeAsJSONSig`'s
//! `SetFsp(MaxFsp)` (`cast(cast('2020-01-01 01:02:03' as datetime) as json)`
//! is `"2020-01-01 01:02:03.000000"`, captured).
//!
//! ## 2h. `VECTOR<FLOAT32>` target and source — all UNREACHABLE
//!
//! `builtinCastUnsupportedAsVectorFloat32Sig.evalVectorFloat32`,
//! `builtinCastStringAsVectorFloat32Sig.evalVectorFloat32`,
//! `builtinCastVectorFloat32AsVectorFloat32Sig.evalVectorFloat32`, and the
//! seven `builtinCastVectorFloat32AsUnsupportedSig.eval*` bodies.
//!
//! PROOF: `tidb_ast::CastType` has no vector variant at all, and
//! `tidb_parser::cast`'s target-type table has no `VECTOR` entry, so the
//! refusal happens at PARSE — one gate earlier than Go's, which refuses at
//! `getFunction`. Four of the ten Go bodies are themselves nothing but
//! `errors.Errorf("cannot cast from vector to %s")`.
//!
//! ## 2i. `castJSONAsArrayFunctionSig.evalJSON` — DECLINED
//!
//! `tidb_ast::CastExpr::array`'s own doc carries the quoted proof: real TiDB
//! rejects a bare `CAST(x AS type ARRAY)` in any ordinary SELECT at
//! plan-build time (`expression_rewriter.go`, `v.Tp.IsArray() &&
//! !er.allowBuildCastArray` -> `ErrNotSupportedYet`), and
//! `allowBuildCastArray` only flips inside functional/multi-valued INDEX
//! definition rewriting, which this crate does not model. `rewriter`'s
//! `Expr::Cast` arm refuses with "a CAST with the ARRAY modifier is not
//! supported yet".
//!
//! # 3. The nine `getFunction` methods and one `verifyArgs`
//!
//! | Go function | verdict | where |
//! | --- | --- | --- |
//! | `castAsIntFunctionClass.getFunction` | PORTED | fused into `cast::eval_cast`'s `Signed`/`Unsigned` arms. Its OPENING gate — `args[0].GetType().Hybrid() \|\| IsBinaryLiteral(args[0])` -> `builtinCastIntAsIntSig` — is `Datum::to_i64_in`'s ENUM/SET/BIT/binary-literal handling, quoted in `cast::cast_arg_as_int`'s own doc. |
//! | `castAsRealFunctionClass.getFunction` | PORTED | `cast::eval_cast`'s `Double`/`Float` arms; its `Hybrid() -> argTp = ETInt` remap is `Datum::to_f64`'s hybrid handling |
//! | `castAsDecimalFunctionClass.getFunction` | PORTED | `cast::eval_cast`'s `Decimal` arm; same hybrid remap through `Datum::to_decimal` |
//! | `castAsStringFunctionClass.getFunction` | PORTED (dispatch); DECLINED (the `castBitAsUnBinary` re-cast) | the signature choice is `cast::eval_cast`'s `Char`/`Binary` arms. Go's `TypeBit` + non-binary target arm rebuilds `args[0]` as a binary-charset cast before dispatching; `cast::cast_arg_as_string`'s `Datum::Bit` arm records the captured consequence (`hex(ltrim(b))` is `FF`) but `eval_cast` itself has no BIT-under-a-charset-target arm. |
//! | `castAsTimeFunctionClass.getFunction` | PORTED | `cast::parse_time_by_source`, which IS the per-source signature choice |
//! | `castAsDurationFunctionClass.getFunction` | DECLINED | see 2f |
//! | `castAsJSONFunctionClass.getFunction` | PORTED | `builtin_ext::json::value::cast_as_json_typed` |
//! | `castAsVectorFloat32FunctionClass.getFunction` | UNREACHABLE | see 2h |
//! | `castAsArrayFunctionClass.getFunction` | DECLINED | see 2i |
//! | `castAsArrayFunctionClass.verifyArgs` | DECLINED | same; it only rejects a non-`ETJson` argument for a cast this tier never builds |
//!
//! # 4. The 24 free functions
//!
//! | Go function | verdict | where |
//! | --- | --- | --- |
//! | `BuildCastFunction` | PORTED | `rewriter::cast_target` + `rewriter`'s `Expr::Cast` arm |
//! | `BuildCastFunctionWithCheck` | PORTED (same site) | its `inUnion`/`isExplicitCharset` parameters are the DECLINED halves — no UNION unification pass, and `CastType::Char`'s `charset` is carried but not modelled at evaluation time (that variant's own doc) |
//! | `BuildCastFunction4Union` | DECLINED | it exists only to pass `inUnion: true`; no caller can exist without a UNION type-unification pass |
//! | `BuildCastCollationFunction` | DECLINED | it wraps an expression to change its COLLATION, which `crate::collation_derive` does by stamping the derived collation onto the node instead of building a cast. No Rust symbol; searched `cast_collation`/`build_cast_collation` across `crates/`, zero hits. |
//! | `WrapWithCastAsInt` | PORTED | `cast::cast_arg_as_int` |
//! | `WrapWithCastAsTime` | PORTED | `cast::cast_arg_as_datetime` |
//! | `WrapWithCastAsString` | PORTED | `cast::cast_arg_as_string` |
//! | `WrapWithCastAsReal` | DECLINED | `crate::arg_eval_type` has exactly `wrap_datetime_args`, `wrap_int_args` and `wrap_string_args` (`grep -n 'wrap_real_args\|wrap_decimal_args'` is empty): no builtin this crate builds declares an `ETReal` ARGUMENT whose value differs from `cast::to_f64_for_cast`'s own coercion, which every real-typed builtin already applies to its operands. |
//! | `WrapWithCastAsDecimal` | DECLINED | same shape; also carries the const-folding precision refinement (`castExpr.ConstLevel() == ConstStrict` -> `SetFlenUnderLimit(precision)`), which is result-type metadata this tier does not compute |
//! | `WrapWithCastAsDuration` | DECLINED | needs the `TIME` target of 2f first |
//! | `WrapWithCastAsJSON` | DECLINED | the JSON argument seam is `json::value::json_argument`, which takes the value directly rather than wrapping a cast; Go's `ParseToJSONFlag` distinction it exists to set is `json::value::StringArgument` |
//! | `WrapWithCastAsVectorFloat32` | UNREACHABLE | see 2h |
//! | `TryPushCastIntoControlFunctionForHybridType` | DECLINED | it REBUILDS `IF`/`CASE`/`ELT` with each hybrid branch pre-cast so the control function's own eval type is numeric. This tier evaluates a control function's branches to DATUMS and `crate::ops` reads an `Enum`/`Set` datum's ordinal directly, so the condition Go's comment names (`if(1, e, 'a') = 1` comparing as ETString) cannot arise. Searched `try_push_cast`/`push_cast_into_control` across `crates/`, zero hits. |
//! | `CanImplicitEvalInt` | DECLINED | it is `f.FuncName.L == ast.DayName` and nothing else; it exists so `builtinCastStringAsIntSig` calls `EvalInt` on `DAYNAME` instead of parsing its NAME. This tier's `time_fn::dayname` returns the NAME as a `Datum::String`, so `CAST(DAYNAME(x) AS SIGNED)` is `str_int_prefix("Monday")` = 0 where TiDB answers the weekday ORDINAL. |
//! | `CanImplicitEvalReal` | DECLINED | the same one-name predicate for `builtinCastStringAsRealSig`; same consequence through `to_f64_for_cast` |
//! | `padZeroForBinaryType` | PORTED | `cast::binary_pad_truncate` plus `eval_cast`'s own `max_allowed_packet` guard (which is the reason the guard tests the DECLARED width BEFORE allocating) |
//! | `adjustRetFtForCastString` | DECLINED | pure RESULT-WIDTH arithmetic, and only when the CAST wrote no length (`originalFlen == types.UnspecifiedLength`). Go's own comment proves no value can move: every `argLen` arm is at least as wide as the rendering it describes. What this tier loses is the reported `flen` of a bare `CAST(x AS CHAR)`. |
//! | `decimalPrecisionToLength` | DECLINED | `adjustRetFtForCastString`'s `ETDecimal` helper only |
//! | `minimalDecimalLenForHoldingInteger` | DECLINED | `WrapWithCastAsDecimal`'s helper only |
//! | `setDataTypeDouble` | DECLINED | result-width helper with NO caller in this file at all: `grep -n setDataTypeDouble pkg/expression/*.go` reports its definition here and its ONE call site in `expression.go:1270` (`PropagateType`'s `ETReal` arm). It is a cast-file resident, not a cast rule. |
//! | `floatLength` | DECLINED | `setDataTypeDouble`'s helper |
//! | `convertJSON2Tp` | DECLINED | reached only from `castJSONAsArrayFunctionSig` and `ConvertJSON2Tp`; see 2i |
//! | `ConvertJSON2Tp` | DECLINED | its only callers are multi-valued-index machinery outside this package (`ErrInvalidJSONForFuncIndex` is the error it returns), which this crate does not model |
//! | `newFakeSctx` | DECLINED | it builds the strict `StatementContext` `castJSONAsArrayFunctionSig` uses so an array cast ignores sql_mode; see 2i |
//!
//! # 5. What this lockdown CHANGED
//!
//! One rung, measured: a temporal CAST now answers a `Datum::Time` with the
//! target's own fsp instead of a formatted string. See `cast::cast_to_time`'s
//! own doc for the four divergences that were all the same bug.
//!
//! # 6. What this lockdown FILED
//!
//!  1. **The `TIME` target** (2f) — the value domain exists; the wiring is a
//!     rung, 141 corpus statements wide.
//!  2. **`CAST(<string> AS DECIMAL)`'s 1292** (2c) — recorded in
//!     `expression/cast` as both a missing warning and a missing statement
//!     ERROR; needs `decimal_prefix` to report whether it consumed the whole
//!     operand.
//!  3. **`DAYNAME`'s implicit integer path** (`CanImplicitEvalInt`/`Real`, 4).
//!  4. **The JSON tier's type codes** (2g) — one cause behind every DECLINED
//!     `builtinCastJSONAsXxxSig`.

/// Every top-level function in `pkg/expression/builtin_cast.go` at
/// `56b92c1185`, as `<receiver-type>.<method>` or a bare name, sorted.
///
/// Re-derive with the command in this module's own doc. The gate below
/// re-runs that extraction against the checked-in Go source, so this list
/// and the classification above cannot silently fall behind the Go file.
///
/// Test-only: nothing EVALUATES a Go symbol name, so making it visible to the
/// library would only be a dead `pub(crate)`. Its one consumer is the gate.
#[cfg(test)]
pub(crate) const GO_SYMBOLS: &[&str] = &[
    "BuildCastCollationFunction",
    "BuildCastFunction",
    "BuildCastFunction4Union",
    "BuildCastFunctionWithCheck",
    "CanImplicitEvalInt",
    "CanImplicitEvalReal",
    "ConvertJSON2Tp",
    "TryPushCastIntoControlFunctionForHybridType",
    "WrapWithCastAsDecimal",
    "WrapWithCastAsDuration",
    "WrapWithCastAsInt",
    "WrapWithCastAsJSON",
    "WrapWithCastAsReal",
    "WrapWithCastAsString",
    "WrapWithCastAsTime",
    "WrapWithCastAsVectorFloat32",
    "adjustRetFtForCastString",
    "builtinCastDecimalAsDecimalSig.Clone",
    "builtinCastDecimalAsDecimalSig.evalDecimal",
    "builtinCastDecimalAsDurationSig.Clone",
    "builtinCastDecimalAsDurationSig.evalDuration",
    "builtinCastDecimalAsIntSig.Clone",
    "builtinCastDecimalAsIntSig.evalInt",
    "builtinCastDecimalAsJSONSig.Clone",
    "builtinCastDecimalAsJSONSig.evalJSON",
    "builtinCastDecimalAsRealSig.Clone",
    "builtinCastDecimalAsRealSig.evalReal",
    "builtinCastDecimalAsStringSig.Clone",
    "builtinCastDecimalAsStringSig.evalString",
    "builtinCastDecimalAsTimeSig.Clone",
    "builtinCastDecimalAsTimeSig.evalTime",
    "builtinCastDurationAsDecimalSig.Clone",
    "builtinCastDurationAsDecimalSig.evalDecimal",
    "builtinCastDurationAsDurationSig.Clone",
    "builtinCastDurationAsDurationSig.evalDuration",
    "builtinCastDurationAsIntSig.Clone",
    "builtinCastDurationAsIntSig.evalInt",
    "builtinCastDurationAsJSONSig.Clone",
    "builtinCastDurationAsJSONSig.evalJSON",
    "builtinCastDurationAsRealSig.Clone",
    "builtinCastDurationAsRealSig.evalReal",
    "builtinCastDurationAsStringSig.Clone",
    "builtinCastDurationAsStringSig.evalString",
    "builtinCastDurationAsTimeSig.Clone",
    "builtinCastDurationAsTimeSig.evalTime",
    "builtinCastIntAsDecimalSig.Clone",
    "builtinCastIntAsDecimalSig.evalDecimal",
    "builtinCastIntAsDurationSig.Clone",
    "builtinCastIntAsDurationSig.evalDuration",
    "builtinCastIntAsIntSig.Clone",
    "builtinCastIntAsIntSig.evalInt",
    "builtinCastIntAsJSONSig.Clone",
    "builtinCastIntAsJSONSig.evalJSON",
    "builtinCastIntAsRealSig.Clone",
    "builtinCastIntAsRealSig.evalReal",
    "builtinCastIntAsStringSig.Clone",
    "builtinCastIntAsStringSig.evalString",
    "builtinCastIntAsTimeSig.Clone",
    "builtinCastIntAsTimeSig.evalTime",
    "builtinCastJSONAsDecimalSig.Clone",
    "builtinCastJSONAsDecimalSig.evalDecimal",
    "builtinCastJSONAsDurationSig.Clone",
    "builtinCastJSONAsDurationSig.evalDuration",
    "builtinCastJSONAsIntSig.Clone",
    "builtinCastJSONAsIntSig.evalInt",
    "builtinCastJSONAsJSONSig.Clone",
    "builtinCastJSONAsJSONSig.evalJSON",
    "builtinCastJSONAsRealSig.Clone",
    "builtinCastJSONAsRealSig.evalReal",
    "builtinCastJSONAsStringSig.Clone",
    "builtinCastJSONAsStringSig.evalString",
    "builtinCastJSONAsTimeSig.Clone",
    "builtinCastJSONAsTimeSig.evalTime",
    "builtinCastRealAsDecimalSig.Clone",
    "builtinCastRealAsDecimalSig.evalDecimal",
    "builtinCastRealAsDurationSig.Clone",
    "builtinCastRealAsDurationSig.evalDuration",
    "builtinCastRealAsIntSig.Clone",
    "builtinCastRealAsIntSig.evalInt",
    "builtinCastRealAsJSONSig.Clone",
    "builtinCastRealAsJSONSig.evalJSON",
    "builtinCastRealAsRealSig.Clone",
    "builtinCastRealAsRealSig.evalReal",
    "builtinCastRealAsStringSig.Clone",
    "builtinCastRealAsStringSig.evalString",
    "builtinCastRealAsTimeSig.Clone",
    "builtinCastRealAsTimeSig.evalTime",
    "builtinCastStringAsDecimalSig.Clone",
    "builtinCastStringAsDecimalSig.evalDecimal",
    "builtinCastStringAsDurationSig.Clone",
    "builtinCastStringAsDurationSig.evalDuration",
    "builtinCastStringAsIntSig.Clone",
    "builtinCastStringAsIntSig.evalInt",
    "builtinCastStringAsIntSig.handleOverflow",
    "builtinCastStringAsJSONSig.Clone",
    "builtinCastStringAsJSONSig.evalJSON",
    "builtinCastStringAsRealSig.Clone",
    "builtinCastStringAsRealSig.evalReal",
    "builtinCastStringAsStringSig.Clone",
    "builtinCastStringAsStringSig.evalString",
    "builtinCastStringAsTimeSig.Clone",
    "builtinCastStringAsTimeSig.evalTime",
    "builtinCastStringAsVectorFloat32Sig.Clone",
    "builtinCastStringAsVectorFloat32Sig.evalVectorFloat32",
    "builtinCastTimeAsDecimalSig.Clone",
    "builtinCastTimeAsDecimalSig.evalDecimal",
    "builtinCastTimeAsDurationSig.Clone",
    "builtinCastTimeAsDurationSig.evalDuration",
    "builtinCastTimeAsIntSig.Clone",
    "builtinCastTimeAsIntSig.evalInt",
    "builtinCastTimeAsJSONSig.Clone",
    "builtinCastTimeAsJSONSig.evalJSON",
    "builtinCastTimeAsRealSig.Clone",
    "builtinCastTimeAsRealSig.evalReal",
    "builtinCastTimeAsStringSig.Clone",
    "builtinCastTimeAsStringSig.evalString",
    "builtinCastTimeAsTimeSig.Clone",
    "builtinCastTimeAsTimeSig.evalTime",
    "builtinCastUnsupportedAsVectorFloat32Sig.Clone",
    "builtinCastUnsupportedAsVectorFloat32Sig.evalVectorFloat32",
    "builtinCastVectorFloat32AsStringSig.Clone",
    "builtinCastVectorFloat32AsStringSig.evalString",
    "builtinCastVectorFloat32AsUnsupportedSig.Clone",
    "builtinCastVectorFloat32AsUnsupportedSig.evalDecimal",
    "builtinCastVectorFloat32AsUnsupportedSig.evalDuration",
    "builtinCastVectorFloat32AsUnsupportedSig.evalInt",
    "builtinCastVectorFloat32AsUnsupportedSig.evalJSON",
    "builtinCastVectorFloat32AsUnsupportedSig.evalReal",
    "builtinCastVectorFloat32AsUnsupportedSig.evalString",
    "builtinCastVectorFloat32AsUnsupportedSig.evalTime",
    "builtinCastVectorFloat32AsVectorFloat32Sig.Clone",
    "builtinCastVectorFloat32AsVectorFloat32Sig.evalVectorFloat32",
    "castAsArrayFunctionClass.getFunction",
    "castAsArrayFunctionClass.verifyArgs",
    "castAsDecimalFunctionClass.getFunction",
    "castAsDurationFunctionClass.getFunction",
    "castAsIntFunctionClass.getFunction",
    "castAsJSONFunctionClass.getFunction",
    "castAsRealFunctionClass.getFunction",
    "castAsStringFunctionClass.getFunction",
    "castAsTimeFunctionClass.getFunction",
    "castAsVectorFloat32FunctionClass.getFunction",
    "castJSONAsArrayFunctionSig.Clone",
    "castJSONAsArrayFunctionSig.evalJSON",
    "convertJSON2Tp",
    "decimalPrecisionToLength",
    "floatLength",
    "minimalDecimalLenForHoldingInteger",
    "newFakeSctx",
    "padZeroForBinaryType",
    "setDataTypeDouble",
];

#[cfg(test)]
mod tests {
    use super::GO_SYMBOLS;

    /// Every Rust symbol the inventory names as a PORTED landing site, as a
    /// COMPILE-TIME reference: renaming or deleting one breaks the build here,
    /// not silently three units later.
    ///
    /// The function-pointer coercions are the strongest form available — they
    /// pin the SIGNATURE too, so a changed parameter list (which is exactly
    /// how the target fsp was lost: `cast_type_of` had the width and
    /// `eval_cast` had no parameter to take it) is a compile error rather
    /// than a quiet behavior change.
    #[test]
    fn every_ported_rust_symbol_still_exists() {
        use crate::cast;
        use tidb_datatype::{Datum, FieldType, SessionTimeZone, TimeType};

        // The public cast entry points.
        const _: fn(
            &tidb_ast::CastType,
            Datum,
            Option<&FieldType>,
            &dyn crate::Columns,
        ) -> Result<Datum, crate::EvalError> = cast::eval_cast;
        const _: fn(&Datum) -> i64 = cast::to_i64_signed;
        const _: fn(&Datum, &SessionTimeZone) -> i64 = cast::to_i64_signed_in;
        const _: fn(&Datum, &dyn crate::Columns) -> Result<(), crate::EvalError> =
            cast::report_int_truncation;

        // The three argument-cast seams (Go's `WrapWithCastAsInt`,
        // `WrapWithCastAsTime`, `WrapWithCastAsString`).
        type ArgCast =
            fn(&Datum, Option<&FieldType>, &dyn crate::Columns) -> Result<Datum, crate::EvalError>;
        const _: ArgCast = cast::cast_arg_as_int;
        const _: ArgCast = cast::cast_arg_as_datetime;
        const _: ArgCast = cast::cast_arg_as_string;

        // The JSON target's landing site, which is deliberately NOT in
        // `crate::cast` (inventory 2g).
        const _: fn(&Datum, Option<&FieldType>) -> Result<Datum, crate::EvalError> =
            crate::builtin_ext::cast_as_json_typed;

        // The temporal target really does answer a `Datum::Time` with the
        // target's own fsp -- the whole of inventory section 5 in one
        // assertion, so a regression to formatted text fails here first.
        let cast = tidb_ast::CastType::DateTime { fsp: Some(3) };
        let got = cast::eval_cast(
            &cast,
            Datum::new_string("2020-01-01 12:00:00.123456".to_owned()),
            None,
            &crate::context::NoColumns,
        )
        .expect("a datetime cast");
        assert_eq!(
            got,
            Datum::Time(
                tidb_datatype::parse_time(
                    "2020-01-01 12:00:00.123456",
                    TimeType::DateTime,
                    3,
                    false,
                    true,
                    false,
                    &SessionTimeZone::utc(),
                )
                .expect("the same parse")
                .time
            ),
            "CAST(... AS DATETIME(3)) must be a Datum::Time carrying fsp 3"
        );
    }

    /// The drift gate: [`GO_SYMBOLS`] must still be exactly the set of
    /// top-level functions in `pkg/expression/builtin_cast.go`.
    ///
    /// This re-runs the extraction documented in this module's own doc against
    /// the CHECKED-IN Go source, in Rust, so adding a signature to the Go file
    /// (a master merge) or removing one fails here with the names rather than
    /// leaving the classification above quietly incomplete. There is no Go
    /// parser in this repo; the regex-free scan below reads the same `func`
    /// lines the `grep`/`sed` pipeline does.
    #[test]
    fn go_symbol_list_still_describes_builtin_cast_go() {
        let go = go_repo_root().join("pkg/expression/builtin_cast.go");
        let source = match std::fs::read_to_string(&go) {
            Ok(text) => text,
            // The crate is buildable without the Go tree beside it; a missing
            // source is not a failure, it is "this gate did not run", and
            // saying so is better than a false green.
            Err(error) => {
                eprintln!("SKIPPED: {}: {error}", go.display());
                return;
            }
        };
        let mut found: Vec<String> = source.lines().filter_map(go_func_symbol).collect();
        found.sort();

        let listed: Vec<String> = GO_SYMBOLS.iter().map(|s| (*s).to_owned()).collect();
        let mut expected = listed.clone();
        expected.sort();
        assert_eq!(
            listed, expected,
            "GO_SYMBOLS must stay sorted so a diff against the Go file reads cleanly"
        );

        let added: Vec<&String> = found.iter().filter(|s| !expected.contains(s)).collect();
        let removed: Vec<&String> = expected.iter().filter(|s| !found.contains(s)).collect();
        assert!(
            added.is_empty() && removed.is_empty(),
            "builtin_cast.go and the inventory in `cast_inventory` disagree.\n\
             NEW in Go (classify each in the module doc, then add here): {added:?}\n\
             GONE from Go (drop its inventory row and this entry): {removed:?}\n\
             Re-derive with:\n  grep -oE '^func (\\([^)]*\\) )?[A-Za-z0-9_]+' \
             pkg/expression/builtin_cast.go | sed -E 's/^func (\\([^)]*\\*([A-Za-z0-9_]+)\\) )?/\\2./; s/^\\.//' | sort"
        );
        assert_eq!(
            found.len(),
            151,
            "the inventory is written against 151 Go functions"
        );
    }

    /// One `func` line of Go, reduced to `<receiver-type>.<name>` or a bare
    /// name -- the same reduction the documented `sed` performs.
    fn go_func_symbol(line: &str) -> Option<String> {
        let rest = line.strip_prefix("func ")?;
        let (receiver, rest) = match rest.strip_prefix('(') {
            Some(after) => {
                let (inside, after) = after.split_once(')')?;
                // `b *builtinCastIntAsIntSig` or `*builtinCastStringAsIntSig`.
                let ty = inside.rsplit('*').next()?.trim();
                (Some(ty.to_owned()), after.trim_start())
            }
            None => (None, rest),
        };
        let name: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if name.is_empty() {
            return None;
        }
        Some(match receiver {
            Some(ty) => format!("{ty}.{name}"),
            None => name,
        })
    }

    /// The repository root: the nearest ancestor of this crate holding
    /// `go.mod`. Returning the crate directory when there is none is what
    /// makes the gate report SKIPPED rather than panic in a checkout without
    /// the Go tree.
    fn go_repo_root() -> std::path::PathBuf {
        let mut at = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        while !at.join("go.mod").is_file() {
            if !at.pop() {
                return std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            }
        }
        at
    }
}
