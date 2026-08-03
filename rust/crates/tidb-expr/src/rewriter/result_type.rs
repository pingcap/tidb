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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! What TYPE a rewritten expression reports, split out of `rewriter.rs`.
//!
//! The rewriter has two jobs: build the expression tree, and stamp each node
//! with the field type Go's `getFunction` would have given it. This module is
//! the second job -- the per-builtin result-type table
//! ([`builtin_return_type`]), the literal type rules Go's
//! `types.DefaultTypeForValue` applies, and the flen/decimal arithmetic those
//! two share (`concat_flen`, `string_cast_flen`, `set_numeric_len_from_args`
//! and friends).
//!
//! It is kept apart from the tree building because a wrong type here is a
//! chunk-cell panic rather than a wrong answer, so the rules deserve to be
//! read as one table instead of scattered through the rewrite arms.

use super::{Constant, Expression};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

use crate::builtin_ext::GlCmpStringMode;

/// Go `types.SetBinChsClnFlag`: the binary charset/collation plus the binary
/// flag every non-string literal type carries.
/// The builtins whose result is a BINARY string: Go's `getFunction` for each
/// ends with `types.SetBinChsClnFlag(bf.tp)` (`unhexFunctionClass`,
/// `base64FunctionClass.fromBase64` in `pkg/expression/builtin_string.go`;
/// `inet6AtonFunctionClass` in `pkg/expression/builtin_miscellaneous.go`).
///
/// This is the single list both places that force it consult -- the result
/// type built in [`builtin_return_type`] and the re-assertion after generic
/// collation derivation -- so the two cannot drift apart.
pub(super) fn returns_binary_string(name: &str) -> bool {
    matches!(name, "unhex" | "from_base64" | "inet6_aton")
}

pub(super) fn set_binary_charset(ft: &mut FieldType) {
    ft.set_charset_name("binary");
    // `set_collation_name` resolves the cached `Collation` enum that
    // `is_binary_string` reads, so naming the collation once is enough; there
    // is no second call here to keep in sync.
    ft.set_collation_name("binary");
    ft.add_flags(tidb_datatype::FieldTypeFlags::BINARY);
}

/// Go `types.DefaultTypeForValue` for a `*MyDecimal`: the printed length plus
/// one for the decimal point, and the literal's own fractional digits.
pub(super) fn decimal_literal_type(value: &tidb_datatype::Decimal) -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::NewDecimal);
    ft.set_flen_under_limit(value.to_string().chars().count() as i64);
    ft.set_decimal_under_limit(i64::from(value.scale()));
    ft.set_flen_under_limit(ft.flen() + 1);
    set_binary_charset(&mut ft);
    ft.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    ft
}

/// Go `types.DefaultTypeForValue` for an `int64`/`uint64`: a `BIGINT` as wide
/// as the value's printed digits, with `UnsignedFlag` set exactly when the
/// literal did not fit the signed domain.
///
/// The flen counts the digits of the VALUE, not of the source text, because
/// Go measures `StrLenOfInt64Fast(x)` after the parse -- so `007` is flen 1.
pub(super) fn int_literal_type(printed_len: usize, unsigned: bool) -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::LongLong);
    ft.set_flen(printed_len as i64);
    ft.set_decimal(0);
    set_binary_charset(&mut ft);
    ft.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    if unsigned {
        ft.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
    }
    ft
}

/// Go `types.DefaultTypeForValue` for a `BitLiteral`/`HexLiteral`: a binary
/// `VarString` three bytes wide per literal byte. Only the hex form is
/// unsigned, which is what makes `0x41 + 0` read the bytes as a number.
pub(super) fn binary_literal_type(byte_len: usize, unsigned: bool) -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::VarString);
    ft.set_flen(byte_len as i64 * 3);
    ft.set_decimal(0);
    set_binary_charset(&mut ft);
    ft.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    if unsigned {
        ft.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
    }
    ft
}

/// The result type of a builtin this rewriter is willing to build.
///
/// A chunk cell is sized from its column's type, so a wrong static type is a
/// panic rather than a wrong answer -- which is why this rewriter builds ONLY
/// the functions whose result type Go fixes to one thing, and refuses the
/// rest instead of falling back to a placeholder. Go's own per-class type
/// inference (`getFunction` on each `functionClass`) is the full version of
/// this table; the deferred names are listed with it.
///
/// NOT BUILT here, and refused (each needs more than a fixed result type):
/// `CAST`/`CONVERT` take a target type, not a value, argument;
/// `GROUP_CONCAT` is an aggregate; `DATE_ADD`-family take an `Expr::Interval`
/// argument that is not an expression at all.
pub(super) fn builtin_return_type(name: &str, args: &[Expression]) -> Option<FieldType> {
    let text = || {
        let mut ft = FieldType::new(FieldTypeCode::VarString);
        ft.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
        ft
    };
    let int = || FieldType::new(FieldTypeCode::LongLong);
    // `date_add_<unit>`/`date_sub_<unit>` carry the INTERVAL unit in the name
    // (see the `Expr::Func` arm of `rewrite_expr_resolved`). Real TiDB types
    // these from the date argument (DATE in, DATE out; DATETIME in, DATETIME
    // out), which this tier renders as the same formatted string the row path
    // produces — the documented temporal-as-string divergence.
    if name.starts_with("date_add_") || name.starts_with("date_sub_") {
        return Some(text());
    }
    Some(match name {
        // String in, string out.
        "upper" | "ucase" | "lower" | "lcase" | "trim" | "ltrim" | "rtrim" | "reverse" | "left"
        | "right" | "substring" | "substr" | "mid" | "replace" | "repeat" | "lpad" | "rpad"
        | "space" | "hex" | "md5" | "elt" | "make_set" | "substring_index" | "insert_func"
        | "char_func" | "export_set" | "quote" => text(),
        // Go `translateFunctionClass.getFunction`: an `ETString` result whose
        // flen is argument 0's own, and `SetBinFlagOrBinStr(args[0], bf.tp)`
        // -- the latter being exactly what `derive_collation`'s `translate`
        // arm (first argument decides) already reproduces, so only the width
        // is set here. Both signature bodies were already ported and reachable
        // from the AST evaluator; without this arm the chunk tier refused
        // `TRANSLATE` outright, so live SQL never reached them.
        "translate" if args.len() == 3 => {
            let mut ft = text();
            if let Some(arg) = args[0].static_type() {
                ft.set_flen(arg.flen());
            }
            ft
        }
        // `CONCAT`/`CONCAT_WS` are the two of that family that SIZE their
        // result, and the size reaches the client as the column metadata's
        // `ColumnLength`. See [`concat_flen`].
        "concat" => {
            let mut ft = text();
            ft.set_flen(concat_flen(args, 0));
            ft
        }
        "concat_ws" if !args.is_empty() => {
            // The separator lands between the remaining arguments, so it is
            // counted `len(args) - 2` times rather than once.
            let separators = i64::from(u32::try_from(args.len().saturating_sub(2)).ok()?);
            let separator_flen = separators * string_cast_flen(args[0].static_type()?);
            ft_with_flen(text(), clamp_blob_width(concat_flen(args, 1) + separator_flen))
        }
        // Go `unhexFunctionClass`/`base64FunctionClass.fromBase64` (see
        // `types.SetBinChsClnFlag` in `pkg/expression/builtin_string.go`):
        // unlike the plain string-in-string-out family above, these two
        // return a BINARY-collated `VarString`, which is what makes
        // `CHAR_LENGTH(UNHEX(...))` count bytes rather than characters.
        "unhex" | "from_base64" => {
            let mut ft = text();
            set_binary_charset(&mut ft);
            ft
        }
        // The date/time family. Every value this crate produces for them is
        // a formatted string or an integer -- see `time_fn`'s own doc for
        // why there is no `Time` value domain here -- so the result types
        // are the string and integer ones rather than Go's temporal types.
        // The VALUES match TiDB; the reported column type is the documented
        // divergence, the same one the temporal casts carry.
        "now" | "current_timestamp" | "localtime" | "localtimestamp" | "utc_timestamp"
        | "curdate" | "current_date" | "utc_date"
        | "curtime" | "current_time" | "utc_time" | "monthname" | "dayname" | "last_day"
        | "sec_to_time" | "maketime" | "makedate" | "from_days" | "date_format" | "str_to_date" => {
            text()
        }
        "month" | "day" | "dayofmonth" | "dayofweek" | "dayofyear" | "weekday" | "quarter"
        | "week" | "weekofyear" | "yearweek" | "year" | "hour" | "minute" | "second"
        | "microsecond" | "time_to_sec" | "to_days" | "period_add" | "period_diff"
        | "unix_timestamp" | "datediff"
        // `EXTRACT`'s composite units (`HOUR_MINUTE`, `DAY_SECOND`, ...) are
        // sugared into these single-argument function names (see the
        // `Expr::Extract` arm below) and, like every other EXTRACT unit,
        // always return an integer (`ExtractDatetimeNum`/
        // `ExtractDurationNum` in `pkg/types/time.go`).
        | "year_month" | "day_hour" | "day_minute" | "day_second" | "day_microsecond"
        | "hour_minute" | "hour_second" | "hour_microsecond" | "minute_second"
        | "minute_microsecond" | "second_microsecond" => int(),
        // The math family. Go types these from the ARGUMENT, and the captured
        // types are what a chunk cell must be sized for: `ABS` and `MOD`
        // preserve the argument domain, `CEIL`/`FLOOR` return an integer for
        // an integer or decimal argument but stay real for a real one, and
        // `ROUND`/`TRUNCATE` keep the decimal domain.
        "abs" | "mod" => arg_numeric_type(args)?,
        "ceil" | "ceiling" | "floor" => match arg_numeric_type(args)?.eval_type() {
            tidb_datatype::EvalType::Real => FieldType::new(FieldTypeCode::Double),
            _ => int(),
        },
        "round" | "truncate" => match arg_numeric_type(args)?.eval_type() {
            tidb_datatype::EvalType::Real => FieldType::new(FieldTypeCode::Double),
            tidb_datatype::EvalType::Int if name == "round" => int(),
            _ => FieldType::new(FieldTypeCode::NewDecimal),
        },
        // Always real, whatever went in.
        "sqrt" | "pow" | "power" | "exp" | "ln" | "log" | "log2" | "log10" | "pi" | "sin"
        | "cos" | "tan" | "asin" | "acos" | "atan" | "atan2" | "cot" | "radians" | "degrees"
        | "rand" => FieldType::new(FieldTypeCode::Double),
        "sign" | "crc32" => int(),
        // The JSON family's value slice: JSON evaluated as VALUES. Go types
        // the first group `MysqlJson` and the second group as strings/ints.
        //
        // DOCUMENTED DIVERGENCE, the same one the temporal casts carry: this
        // crate has no BinaryJSON value, so a JSON-returning builtin produces
        // its canonical JSON TEXT (`format_json`, which is
        // `BinaryJSON.MarshalJSON`'s exact spelling -- byte-sorted keys,
        // `, ` / `: ` separators). The VALUE therefore matches TiDB
        // byte for byte; the reported column type is `VarString` where TiDB
        // says `JSON`. Typing it as Go does would put a string into a JSON
        // cell, which panics rather than mistyping.
        //
        // `JSON_TABLE` is deliberately NOT listed: it is a table function,
        // not a scalar, so it keeps falling through to the refusal below.
        "json_extract" | "json_object" | "json_array" | "json_keys" | "json_quote"
        | "json_unquote" | "json_type" | "json_set" | "json_insert" | "json_replace"
        | "json_remove" | "json_array_append" | "json_array_insert" | "json_merge"
        | "json_merge_preserve" | "json_merge_patch" => text(),
        "json_valid" | "json_contains" | "json_length" | "json_depth" => int(),
        "conv" | "bin" | "oct" | "format" => text(),
        // NOT the same merge a CASE uses, even though both are "combine the
        // argument types". Go `greatestFunctionClass.getFunction` takes only
        // the EVAL TYPE from `resolveType4Extremum` and then picks one
        // signature per eval type -- `builtinGreatestRealSig` returns an
        // `f64`, so a FLOAT argument comes back as DOUBLE. A CASE instead
        // keeps the merged FIELD type (`types.AggFieldType`), where FLOAT
        // beside an integer stays FLOAT. Captured from Go over a FLOAT column
        // holding 12.191: `greatest(c,0)` prints 12.190999984741211 while
        // `case ... then c else 0 end` prints 12.191.
        "greatest" | "least" => extremum_return_type(args)?,
        // `NULLIF` keeps its first argument's type; the second only decides
        // whether the result is NULL.
        "nullif" => args.first()?.static_type()?.clone(),
        // `ANY_VALUE` is the identity on both value AND type: Go
        // `anyValueFunctionClass.getFunction` clones the argument's whole
        // `FieldType` over the builder's (`*bf.tp = *ft`), so the charset,
        // collation, flen and decimal all pass through untouched.
        "any_value" if args.len() == 1 => args.first()?.static_type()?.clone(),
        // Reading a user variable: Go's `BuildGetVarFunction` picks one of its
        // typed `GETVAR` signatures from the type the session currently holds
        // for the name, so the CHOICE is made before the rewriter runs and
        // arrives encoded in the name -- the same "build-time decision lives
        // in the function name" shape `cast_*` and `date_add_*` use. The
        // declared type must agree with the value the evaluator returns,
        // because the chunk tier appends into a column of exactly this type.
        "getvar_int" if args.len() == 1 => int(),
        "getvar_uint" if args.len() == 1 => {
            let mut ft = int();
            ft.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
            ft
        }
        "getvar_real" if args.len() == 1 => FieldType::new(FieldTypeCode::Double),
        "getvar_decimal" if args.len() == 1 => FieldType::new(FieldTypeCode::NewDecimal),
        "getvar_string" if args.len() == 1 => text(),
        // `SETVAR` reports -- and stores -- its value argument's type.
        "setvar" if args.len() == 2 => args[1].static_type().cloned().unwrap_or_else(text),
        // Go reads these from `SessionVars`; each returns a string.
        "database" | "schema" | "version" | "current_user" | "current_role" | "user"
        | "session_user" | "system_user" => text(),
        // Go `connectionIDFunctionClass` fixes an unsigned `LongLong`.
        "connection_id" => {
            let mut ft = int();
            ft.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
            ft
        }
        // Go `rowCountFunctionClass` (`pkg/expression/builtin_info.go`): a
        // plain signed `LongLong` with no flen of its own -- unlike
        // `LAST_INSERT_ID` below, `ROW_COUNT()` is signed, because a failed
        // statement reports -1.
        "row_count" if args.is_empty() => int(),
        // Go `nextValFunctionClass` / `lastValFunctionClass` /
        // `setValFunctionClass` (`pkg/expression/builtin_info.go`): each fixes
        // a signed `LongLong`. The sequence NAME travels as the first argument,
        // a string constant the rewriter substitutes for the column reference
        // the parser produced -- see the `nextval` arm of
        // `rewrite_expr_resolved`.
        "nextval" | "lastval" if args.len() == 1 => int(),
        "setval" if args.len() == 2 => int(),
        // Go `lastInsertIDFunctionClass` adds `mysql.UnsignedFlag`, which is
        // what makes `LAST_INSERT_ID(-1)` report 18446744073709551615 rather
        // than -1. Both the zero-argument and one-argument forms are the same
        // class and so the same result type.
        "last_insert_id" if args.len() <= 1 => {
            let mut ft = int();
            ft.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
            ft
        }
        // Go `bitCountFunctionClass` (`pkg/expression/builtin_other.go`):
        // int in, int out, flen 2 (a 64-bit word has at most 64 set bits).
        "bit_count" if args.len() == 1 => {
            let mut ft = int();
            ft.set_flen(2);
            ft
        }
        // Go `isNullFunctionClass` (`pkg/expression/builtin_op.go`): the
        // one-digit boolean shape. This is the FUNCTION spelling `ISNULL(x)`;
        // the `x IS NULL` operator is rewritten onto the same name by the
        // `Expr::Is` arm below, so both spellings share this type.
        "isnull" if args.len() == 1 => {
            let mut ft = int();
            ft.set_flen(1);
            ft
        }
        // The hash family (`pkg/expression/builtin_encryption.go`). Each
        // returns hex TEXT in the CONNECTION charset -- explicitly NOT
        // binary, unlike `UNHEX`/`FROM_BASE64` above -- with a fixed flen:
        // `sha1FunctionClass` 40 (one SHA-1 digest hexed) and
        // `sha2FunctionClass` 128 (sized for its widest variant, SHA-512).
        "sha" | "sha1" if args.len() == 1 => {
            let mut ft = text();
            ft.set_flen(40);
            ft
        }
        "sha2" if args.len() == 2 => {
            let mut ft = text();
            ft.set_flen(128);
            ft
        }
        // Go `ordFunctionClass` (`pkg/expression/builtin_string.go`): string
        // in, int out, flen 10 -- the code point of the first CHARACTER under
        // the argument's charset, so a binary argument yields its first byte.
        "ord" if args.len() == 1 => {
            let mut ft = int();
            ft.set_flen(10);
            ft
        }
        // Go `toBase64FunctionClass`: connection-charset text whose flen is
        // derived from the argument's via `base64NeededEncodedLength`, and
        // stays unspecified when the argument's is (which is the case for
        // every expression this tier types as `text()`).
        "to_base64" if args.len() == 1 => {
            let mut ft = text();
            let arg_flen = args.first()?.static_type()?.flen();
            ft.set_flen(base64_needed_encoded_length(arg_flen));
            ft
        }
        // The IP-address family (`pkg/expression/builtin_miscellaneous.go`).
        // `INET_ATON` is UNSIGNED -- that flag is the whole reason
        // `INET_ATON('255.255.255.255')` reports 4294967295 rather than a
        // negative number once it is widened.
        "inet_aton" if args.len() == 1 => {
            let mut ft = int();
            ft.set_flen(21);
            ft.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
            ft
        }
        "inet_ntoa" if args.len() == 1 => {
            let mut ft = text();
            ft.set_flen(93);
            ft.set_decimal(0);
            ft
        }
        // `INET6_ATON` returns raw address BYTES, so Go stamps it binary
        // (`types.SetBinChsClnFlag`) exactly as it does `UNHEX`; `INET6_NTOA`
        // returns printable text in the connection charset.
        "inet6_aton" if args.len() == 1 => {
            let mut ft = text();
            set_binary_charset(&mut ft);
            ft.set_flen(16);
            ft.set_decimal(0);
            ft
        }
        "inet6_ntoa" if args.len() == 1 => {
            let mut ft = text();
            ft.set_flen(117);
            ft.set_decimal(0);
            ft
        }
        // The address/UUID predicates: one-digit boolean ints.
        "is_ipv4" | "is_ipv4_compat" | "is_ipv4_mapped" | "is_ipv6" | "is_uuid"
            if args.len() == 1 =>
        {
            let mut ft = int();
            ft.set_flen(1);
            ft
        }
        // Go `uuidVersionFunctionClass`: an int with flen 10, NOT the boolean
        // shape above -- it reports the version nibble, not a yes/no.
        "uuid_version" if args.len() == 1 => {
            let mut ft = int();
            ft.set_flen(10);
            ft
        }
        // Go `intervalFunctionClass` (`pkg/expression/builtin_compare.go`):
        // always an int (the index of the containing bucket), with no flen of
        // its own. Whether the arguments are compared as ints or as reals is
        // an ARGUMENT-side choice that does not reach the result type.
        "interval" if args.len() >= 2 => int(),
        // Go `formatBytesFunctionClass`/`formatNanoTimeFunctionClass`
        // (`pkg/expression/builtin_info.go`): real in, connection-charset
        // text out, with no fixed flen.
        "format_bytes" | "format_nano_time" if args.len() == 1 => text(),
        // Go `timeFormatFunctionClass` (`pkg/expression/builtin_time.go`):
        // a DURATION and a format string in, text out, with the flen sized
        // from the FORMAT argument's -- `(flen + 1) / 2 * 11`, an upper bound
        // on how much one format specifier can expand.
        "time_format" if args.len() == 2 => {
            let mut ft = text();
            let format_flen = args[1].static_type()?.flen();
            if format_flen != tidb_datatype::UNSPECIFIED_LENGTH {
                ft.set_flen((format_flen + 1) / 2 * 11);
            }
            ft
        }
        // Go `toSecondsFunctionClass`: datetime in, plain int out. It sits
        // beside `TO_DAYS` in the int family above but arrives here because
        // it takes the zero-date `calcDaynr` path in `time_fn::calendar`.
        "to_seconds" if args.len() == 1 => int(),
        // `JSON_SEARCH` is typed `ETJson` by Go; it carries the same
        // JSON-as-canonical-text divergence documented for the JSON family
        // above, so its result cell is a string here.
        "json_search" if args.len() >= 3 => text(),
        // String in, number out.
        "length" | "octet_length" | "char_length" | "character_length" | "bit_length" | "ascii"
        | "instr" | "locate" | "position" | "find_in_set" | "strcmp" | "field" => int(),
        // Go `likeFunctionClass`/`regexpLikeFunctionClass`: a one-digit
        // boolean.
        "like" | "ilike" | "regexp" => {
            let mut ft = int();
            ft.set_flen(1);
            ft.add_flags(tidb_datatype::FieldTypeFlags::IS_BOOLEAN);
            ft
        }
        // Go aggregates the branch types of these (`aggregateType`). Only a
        // set of branches that already agree is built here; a mixed set is
        // refused rather than guessed, because the guess sizes a chunk cell.
        // `IF`'s first argument is the condition, so only its two result
        // branches carry the type.
        "if" if args.len() == 3 => builtin_return_type("case_when", &args[1..])?,
        "case_when" | "ifnull" | "coalesce" => {
            // A NULL branch carries no type of its own -- Go's `aggregateType`
            // ignores it -- so only the typed branches have to agree.
            let branches = args
                .iter()
                .filter_map(Expression::static_type)
                .filter(|ft| ft.code() != FieldTypeCode::Null);
            let typed: Vec<&FieldType> = branches.collect();
            let first = (*typed.first()?).clone();
            // Go `types.AggFieldType` merges the string family to VarString,
            // which is what lets `IFNULL(varchar_column, 'literal')` -- a
            // Varchar branch and a VarString branch -- have one type. Other
            // mixtures are refused rather than guessed, since the result type
            // sizes a chunk cell.
            if typed
                .iter()
                .all(|ft| ft.eval_type() == tidb_datatype::EvalType::String)
            {
                if typed.iter().any(|ft| ft.code() != first.code()) {
                    text()
                } else {
                    first
                }
            } else if typed.iter().all(|ft| {
                matches!(
                    ft.eval_type(),
                    tidb_datatype::EvalType::Int
                        | tidb_datatype::EvalType::Decimal
                        | tidb_datatype::EvalType::Real
                )
            }) {
                // Go `InferType4ControlFuncs` merges the branches' FIELD types
                // with `types.AggFieldType` -- the `fieldTypeMergeRules`
                // table -- and only then reads an eval type off the result for
                // the width rules. The table is not an eval-type widening:
                // FLOAT beside a BIGINT literal merges to FLOAT, not DOUBLE,
                // and that difference is a printed VALUE (Go prints a FLOAT
                // 12.191 where a DOUBLE reads 12.190999984741211).
                let owned: Vec<FieldType> = typed.iter().map(|ft| (*ft).clone()).collect();
                let mut merged = tidb_datatype::agg_field_type(&owned);
                let mut flags = merged.flags();
                tidb_datatype::aggregate_eval_type(&owned, &mut flags);
                merged = merged.with_flags(flags);
                set_numeric_len_from_args(&mut merged, &typed);
                merged
            } else {
                if typed.iter().any(|ft| ft.code() != first.code()) {
                    return None;
                }
                first
            }
        }
        _ => return None,
    })
}

/// The flen `TO_BASE64` reports for an argument of flen `n`.
///
/// Port of `base64NeededEncodedLength` (`pkg/expression/builtin_string.go`),
/// including its two sentinels: an unspecified argument length stays
/// unspecified, and a length whose encoding would overflow a signed word
/// reports -1 (which is the same value as `UNSPECIFIED_LENGTH`, exactly as in
/// Go -- both mean "no usable bound"). The `+ (length - 1) / 76` term is the
/// newline every 76 output characters that MySQL's base64 inserts.
pub(super) fn base64_needed_encoded_length(n: i64) -> i64 {
    if n == tidb_datatype::UNSPECIFIED_LENGTH || n > 6_827_690_988_321_067_803 {
        return tidb_datatype::UNSPECIFIED_LENGTH;
    }
    let length = (n + 2) / 3 * 4;
    length + (length - 1) / 76
}

/// Go `maxlen` (`pkg/expression/builtin_control.go`): an UNKNOWN length in
/// either operand widens the result to `mysql.MaxRealWidth` rather than
/// staying unknown.
fn maxlen(lhs: i64, rhs: i64) -> i64 {
    /// Go `mysql.MaxRealWidth`.
    const MAX_REAL_WIDTH: i64 = 23;
    if lhs < 0 || rhs < 0 {
        MAX_REAL_WIDTH
    } else {
        lhs.max(rhs)
    }
}

/// Go `setDecimalFromArgs` then `setFlenFromArgs`
/// (`pkg/expression/builtin_control.go`), for a control function whose merged
/// result is NUMERIC.
///
/// This is the half of `InferType4ControlFuncs` that decides how the value
/// PRINTS: `IFNULL(0, 1.5)` is `decimal(3,1)`, so its integer branch reads
/// back as `0.0`, not `0`. Dropping it does not merely lose display width --
/// the merged scale is what the evaluated branch is converted onto, so an
/// unspecified scale here is a WRONG VALUE, not a cosmetic difference.
/// The result type of `GREATEST`/`LEAST`.
///
/// Go `greatestFunctionClass.getFunction` (`pkg/expression/builtin_compare.go`)
/// reduces `resolveType4Extremum` to an EVAL type and dispatches one signature
/// per eval type, so the result carries the eval type's canonical field type
/// rather than any argument's own: the `ETReal` arm is
/// `builtinGreatestRealSig`, whose `evalReal` is an `f64`, so a FLOAT argument
/// widens to DOUBLE here even though the same pair of branches inside a CASE
/// stays FLOAT.
/// `pkg/parser/mysql.MaxBlobWidth` and `MaxLongBlobWidth`.
const MAX_BLOB_WIDTH: i64 = 16_777_216;
const MAX_LONG_BLOB_WIDTH: i64 = 4_294_967_295;

fn ft_with_flen(mut ft: FieldType, flen: i64) -> FieldType {
    ft.set_flen(flen);
    ft
}

fn clamp_blob_width(flen: i64) -> i64 {
    if flen >= MAX_BLOB_WIDTH {
        MAX_BLOB_WIDTH
    } else {
        flen
    }
}

/// The flen an argument carries once `WrapWithCastAsString` has wrapped it,
/// which is the width `CONCAT` actually sums.
///
/// Two Go functions decide it in sequence (`pkg/expression/builtin_cast.go`).
/// `WrapWithCastAsString` asks for a width: a string argument is not wrapped
/// at all and keeps its own flen, an integer widens to `MaxIntWidth` whatever
/// its declared display width (a `BIT(n)` instead asks for its byte count),
/// a decimal gains three for the sign, the point and a leading zero, and a
/// float or double asks for nothing because `CAST(f AS CHAR)`'s printed width
/// is not predictable. `setCastFlen`-style sizing on the cast's own return
/// type then fills in every width still unasked-for, which is where the
/// remaining types get theirs.
///
/// Captured from Go, one `CONCAT` argument each: `DOUBLE` 370, `FLOAT` 87,
/// `DATE` 10, `DATETIME` 19, `DATETIME(3)` 23, `TIME(2)` 13, `TINYINT(4)` 20,
/// `BIT(8)` 1, `DECIMAL(10,2)` 13, `VARCHAR(16)` 16.
///
/// Not modeled: a DECIMAL whose own flen is unspecified, where Go computes
/// `decimalPrecisionToLength`. That leaves the unspecified path below, which
/// is Go's answer for an argument of genuinely unknown width.
fn string_cast_flen(ft: &FieldType) -> i64 {
    use tidb_datatype::EvalType;
    const UNSPECIFIED: i64 = tidb_datatype::UNSPECIFIED_LENGTH;

    let eval_type = ft.eval_type();
    let requested = match eval_type {
        // Already a string: no cast is inserted at all.
        EvalType::String => return ft.flen(),
        EvalType::Int if ft.code() == FieldTypeCode::Bit => (ft.flen() + 7) / 8,
        EvalType::Int => 20,
        EvalType::Decimal if ft.flen() != UNSPECIFIED => ft.flen() + 3,
        EvalType::Real => UNSPECIFIED,
        _ => ft.flen(),
    };
    if requested != UNSPECIFIED {
        return requested;
    }

    // The cast's return-type sizing, reached only for a width nobody asked for.
    let with_fraction = |base: i64| {
        if ft.decimal() > 0 {
            base + 1 + ft.decimal()
        } else {
            base
        }
    };
    match eval_type {
        // 87 and 370 are Go's own worst-case widths for `%f`-formatted
        // f32/f64, not MySQL's 12/22 -- TiDB never uses scientific notation.
        EvalType::Real if ft.code() == FieldTypeCode::Float => 87,
        EvalType::Real => 370,
        EvalType::Datetime | EvalType::Timestamp if ft.code() == FieldTypeCode::Date => 10,
        EvalType::Datetime | EvalType::Timestamp => with_fraction(19),
        EvalType::Duration => with_fraction(10),
        EvalType::Json => MAX_LONG_BLOB_WIDTH,
        _ => UNSPECIFIED,
    }
}

/// Go `concatFunctionClass.getFunction`, over `args[skip..]`.
///
/// The result's flen is the sum of the arguments' string-cast widths, clamped
/// at `MaxBlobWidth`. An argument of unknown width does NOT simply make the
/// whole thing unknown: Go restarts the running sum at `MaxBlobWidth` and then
/// still adds that argument's negative flen, so a single unsized argument
/// leaves `MaxBlobWidth - 1` unless a later argument pushes the sum back over
/// the clamp. That is Go's arithmetic verbatim, oddity included -- it is what
/// the client is told, so it is what we must say.
fn concat_flen(args: &[Expression], skip: usize) -> i64 {
    let mut flen = 0_i64;
    for arg in args.iter().skip(skip) {
        let arg_flen = arg
            .static_type()
            .map_or(tidb_datatype::UNSPECIFIED_LENGTH, string_cast_flen);
        if arg_flen < 0 {
            flen = MAX_BLOB_WIDTH;
        }
        flen += arg_flen;
    }
    clamp_blob_width(flen)
}

/// Go `resolveType4Extremum`'s `cmpStringMode`
/// (`pkg/expression/builtin_compare.go`): when GREATEST/LEAST's arguments
/// aggregate to a STRING kind that is not itself temporal, and at least one
/// argument IS a date or datetime, Go compares every argument as a parsed
/// time instead of as text.
///
/// `Directly` -- Go's default -- is also the answer whenever an argument has
/// no static type here, since an unknown type cannot be shown to be temporal.
pub fn gl_cmp_string_mode(args: &[Expression]) -> GlCmpStringMode {
    let typed: Vec<FieldType> = args
        .iter()
        .filter_map(|arg| arg.static_type().cloned())
        .collect();
    if typed.len() != args.len() || typed.is_empty() {
        return GlCmpStringMode::Directly;
    }
    let aggregated = tidb_datatype::agg_field_type(&typed);
    if !aggregated.eval_type().is_string_kind() || aggregated.code().is_type_temporal() {
        return GlCmpStringMode::Directly;
    }
    // Go scans for a temporal argument but PREFERS a DATETIME one, so a
    // (DATE, DATETIME, string) list compares as datetime rather than as date.
    let mut temporal: Option<FieldTypeCode> = None;
    for ft in &typed {
        if ft.code().is_type_temporal()
            && (temporal.is_none() || ft.code() == FieldTypeCode::Datetime)
        {
            temporal = Some(ft.code());
        }
    }
    match temporal {
        Some(FieldTypeCode::Date) => GlCmpStringMode::AsDate,
        Some(code) if code.is_temporal_with_date() => GlCmpStringMode::AsDatetime,
        // A DURATION is temporal but carries no date, so Go leaves the mode
        // alone and the plain string signature runs.
        _ => GlCmpStringMode::Directly,
    }
}

fn extremum_return_type(args: &[Expression]) -> Option<FieldType> {
    let typed: Vec<&FieldType> = args
        .iter()
        .filter_map(Expression::static_type)
        .filter(|ft| ft.code() != FieldTypeCode::Null)
        .collect();
    let first = (*typed.first()?).clone();
    if typed
        .iter()
        .all(|ft| ft.eval_type() == tidb_datatype::EvalType::String)
    {
        if typed.iter().any(|ft| ft.code() != first.code()) {
            let mut merged = FieldType::new(FieldTypeCode::VarString);
            merged.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
            return Some(merged);
        }
        return Some(first);
    }
    if typed.iter().all(|ft| {
        matches!(
            ft.eval_type(),
            tidb_datatype::EvalType::Int
                | tidb_datatype::EvalType::Decimal
                | tidb_datatype::EvalType::Real
        )
    }) {
        let owned: Vec<Expression> = typed
            .iter()
            .map(|ft| Expression::Constant(Constant::new(Datum::Null, (*ft).clone())))
            .collect();
        let mut merged = arg_numeric_type(&owned)?;
        set_numeric_len_from_args(&mut merged, &typed);
        return Some(merged);
    }
    // Everything left is a MIXTURE of kinds, which Go handles the same way it
    // handles the uniform cases: `resolveType4Extremum` aggregates the
    // argument FieldTypes and `getFunction` picks one signature per EVAL type.
    // A mixture whose aggregate is string-kind -- a string beside a number, a
    // DATE beside a string (which additionally selects the compare-as-time
    // signature, see [`gl_cmp_string_mode`]), or the JSON case Go also folds
    // onto `ETString` -- therefore returns a string, where this table used to
    // refuse the whole shape and no such call ran at all.
    let aggregated =
        tidb_datatype::agg_field_type(&typed.iter().map(|ft| (*ft).clone()).collect::<Vec<_>>());
    if aggregated.eval_type().is_string_kind() {
        let mut merged = FieldType::new(FieldTypeCode::VarString);
        merged.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
        return Some(merged);
    }
    if typed.iter().any(|ft| ft.code() != first.code()) {
        return None;
    }
    Some(first)
}

/// Go `setDecimalFromArgs` then `setFlenFromArgs`, which `AggFieldType` does
/// NOT do: the merge carries the FIRST argument's flen/decimal, so every
/// caller of `InferType4ControlFuncs`'s merge has to re-derive both from all
/// the arguments. `tidb_executor::window`'s `LAG`/`LEAD` inference is the
/// other caller.
pub fn set_numeric_len_from_args(result: &mut FieldType, args: &[&FieldType]) {
    use tidb_datatype::EvalType;
    let eval_type = result.eval_type();
    // setDecimalFromArgs: ETInt has no scale; otherwise the widest argument
    // scale, or unspecified as soon as one argument's is unspecified.
    if eval_type == EvalType::Int {
        result.set_decimal(0);
    } else {
        let mut max_decimal = 0;
        let mut unspecified = false;
        for arg in args {
            if arg.decimal() == tidb_datatype::UNSPECIFIED_LENGTH {
                unspecified = true;
                break;
            }
            max_decimal = max_decimal.max(arg.decimal());
        }
        if unspecified {
            result.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
        } else {
            result.set_decimal_under_limit(max_decimal);
        }
    }
    // setFlenFromArgs, the ETDecimal/ETInt arm: the widest INTEGRAL part
    // (each argument's flen less its sign digit and its own scale), with the
    // merged scale and one sign digit added back.
    if matches!(eval_type, EvalType::Decimal | EvalType::Int) {
        let mut max_arg_flen = 0;
        for arg in args {
            let sign_len = i64::from(arg.flags() & tidb_datatype::FieldTypeFlags::UNSIGNED == 0);
            let mut flen = arg.flen() - sign_len;
            if arg.decimal() != tidb_datatype::UNSPECIFIED_LENGTH {
                flen -= arg.decimal();
            }
            max_arg_flen = maxlen(max_arg_flen, flen);
        }
        result.set_flen_under_limit(max_arg_flen + result.decimal() + 1);
    } else {
        // The trailing `else` arm: the widest argument flen as-is.
        let mut max_len = 0;
        for arg in args {
            max_len = max_len.max(arg.flen());
        }
        result.set_flen(max_len);
    }
}

/// The numeric type a type-preserving math builtin reports, which Go takes
/// from its argument: an integer argument keeps the integer domain, a decimal
/// keeps the decimal one, and a real keeps the real one. A pair of arguments
/// (`MOD`, `ATAN2`) takes the wider of the two, which is the same order Go's
/// `setFlenDecimal4Int`-family builders use.
fn arg_numeric_type(args: &[Expression]) -> Option<FieldType> {
    use tidb_datatype::EvalType;
    let mut best: Option<FieldType> = None;
    for arg in args {
        let ft = arg.static_type()?;
        let rank = |ft: &FieldType| match ft.eval_type() {
            EvalType::Int => 0,
            EvalType::Decimal => 1,
            _ => 2,
        };
        if best.as_ref().is_none_or(|current| rank(ft) > rank(current)) {
            best = Some(ft.clone());
        }
    }
    let best = best?;
    Some(match best.eval_type() {
        EvalType::Int => FieldType::new(FieldTypeCode::LongLong),
        EvalType::Decimal => FieldType::new(FieldTypeCode::NewDecimal),
        // A string argument is read as a number, which Go does as a real.
        _ => FieldType::new(FieldTypeCode::Double),
    })
}

#[cfg(test)]
mod concat_flen_tests {
    use super::*;

    fn arg(code: FieldTypeCode, flen: i64, decimal: i64) -> Expression {
        let mut ft = FieldType::new(code);
        ft.set_flen(flen);
        ft.set_decimal(decimal);
        Expression::Constant(Constant::new(Datum::Null, ft))
    }

    fn flen_of(name: &str, args: &[Expression]) -> i64 {
        builtin_return_type(name, args)
            .unwrap_or_else(|| panic!("{name} has no return type"))
            .flen()
    }

    const UNSPECIFIED: i64 = tidb_datatype::UNSPECIFIED_LENGTH;

    fn varchar(flen: i64) -> Expression {
        arg(FieldTypeCode::Varchar, flen, UNSPECIFIED)
    }
    fn bigint() -> Expression {
        arg(FieldTypeCode::LongLong, 11, 0)
    }

    /// Every number below is Go's own `GetFlen()` for the built expression,
    /// captured by running `expression.NewFunction` over these exact argument
    /// FieldTypes. `CONCAT`'s width reaches the client as the result set's
    /// `ColumnLength`, so an unspecified flen here is wire-visible.
    #[test]
    fn concat_sums_the_string_cast_widths_go_sums() {
        assert_eq!(flen_of("concat", &[varchar(16)]), 16);
        assert_eq!(flen_of("concat", &[varchar(16), varchar(16)]), 32);
        // Any integer widens to MaxIntWidth, whatever its display width.
        assert_eq!(flen_of("concat", &[bigint(), bigint()]), 40);
        assert_eq!(flen_of("concat", &[arg(FieldTypeCode::Tiny, 4, 0)]), 20);
        // BIT is the exception: its BYTE count, because TiKV needs the real
        // length when it evaluates things like ASCII(bit).
        assert_eq!(flen_of("concat", &[arg(FieldTypeCode::Bit, 8, 0)]), 1);
        // Sign, decimal point and a leading zero.
        assert_eq!(
            flen_of("concat", &[arg(FieldTypeCode::NewDecimal, 10, 2)]),
            13
        );
        // TiDB prints reals in full, never scientific notation, so the
        // worst-case widths are far wider than MySQL's 12/22.
        assert_eq!(
            flen_of(
                "concat",
                &[arg(FieldTypeCode::Float, UNSPECIFIED, UNSPECIFIED)]
            ),
            87
        );
        assert_eq!(
            flen_of(
                "concat",
                &[arg(FieldTypeCode::Double, UNSPECIFIED, UNSPECIFIED)]
            ),
            370
        );
        assert_eq!(
            flen_of(
                "concat",
                &[
                    varchar(16),
                    arg(FieldTypeCode::Double, UNSPECIFIED, UNSPECIFIED)
                ]
            ),
            386
        );
        assert_eq!(
            flen_of(
                "concat",
                &[arg(FieldTypeCode::Date, UNSPECIFIED, UNSPECIFIED)]
            ),
            10
        );
        assert_eq!(
            flen_of("concat", &[arg(FieldTypeCode::Datetime, UNSPECIFIED, 0)]),
            19
        );
        assert_eq!(
            flen_of("concat", &[arg(FieldTypeCode::Datetime, UNSPECIFIED, 3)]),
            23
        );
        assert_eq!(
            flen_of("concat", &[arg(FieldTypeCode::Duration, UNSPECIFIED, 2)]),
            13
        );
    }

    /// The separator is counted `len(args) - 2` times, so a single value
    /// argument contributes no separator at all.
    #[test]
    fn concat_ws_counts_the_separator_between_values_only() {
        assert_eq!(flen_of("concat_ws", &[varchar(3), varchar(16)]), 16);
        assert_eq!(
            flen_of("concat_ws", &[varchar(3), varchar(16), varchar(16)]),
            35
        );
        assert_eq!(
            flen_of("concat_ws", &[varchar(3), bigint(), bigint(), bigint()]),
            66
        );
        assert_eq!(
            flen_of(
                "concat_ws",
                &[
                    varchar(3),
                    arg(FieldTypeCode::Double, UNSPECIFIED, UNSPECIFIED),
                    varchar(16)
                ]
            ),
            389
        );
    }

    /// Go restarts the running sum at MaxBlobWidth for an argument of unknown
    /// width and then still ADDS that argument's -1, so a lone unsized
    /// argument leaves MaxBlobWidth - 1 rather than MaxBlobWidth. Porting the
    /// arithmetic rather than the intent is what keeps us on Go's number.
    #[test]
    fn concat_clamps_at_max_blob_width_the_way_go_does() {
        let unsized_arg = arg(FieldTypeCode::Varchar, UNSPECIFIED, UNSPECIFIED);
        assert_eq!(
            flen_of("concat", std::slice::from_ref(&unsized_arg)),
            MAX_BLOB_WIDTH - 1
        );
        // A second argument pushes the sum back over the clamp.
        assert_eq!(
            flen_of("concat", &[unsized_arg, varchar(16)]),
            MAX_BLOB_WIDTH
        );
    }
}
