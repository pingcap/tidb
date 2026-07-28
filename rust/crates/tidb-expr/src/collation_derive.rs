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

//! Expression-level collation derivation: the half of
//! `pkg/expression/collation.go` that needs the `Expression` node hierarchy.
//!
//! [`crate::expr_collation`] holds the value types (`Coercibility`,
//! `Repertoire`, `ExprCollation`, `collationInfo`) and the charset predicates;
//! this module holds `deriveCoercibilityForColumn`/`ForConstant`,
//! `inferCollation`, `CheckAndDeriveCollationFromExprs`, `deriveCollation` and
//! `illegalMixCollationErr`.
//!
//! # Why this exists
//!
//! Without it, every string comparison in the evaluator ran under one
//! hard-coded `utf8mb4_bin` PAD SPACE rule, because a `Datum` carries no
//! collation. MySQL's answer is that the collation of a comparison is a
//! property of the EXPRESSION, aggregated from its operands by coercibility --
//! so it is decided once, while the expression is built, and stamped onto the
//! function's result type. That is exactly what Go does
//! (`newBaseBuiltinFuncWithTp` -> `bf.tp.SetCollate(ec.Collation)`), and what
//! [`apply_derived_collation`] does here; the collation-aware signatures then
//! read it back with [`collation_of_node`].

use crate::expr_collation::{
    get_bin_collation, is_bin_collation, is_unicode_collation, Coercibility, CollationInfo,
    ExprCollation, Repertoire,
};
use crate::expression::Expression;
use crate::EvalError;
use tidb_datatype::{Collation, EvalType, FieldType, FieldTypeCode};

const CHARSET_UTF8: &str = "utf8mb3";
const CHARSET_UTF8MB4: &str = "utf8mb4";
const CHARSET_BIN: &str = "binary";
const CHARSET_ASCII: &str = "ascii";
const COLLATION_UTF8MB4: &str = "utf8mb4_bin";
const COLLATION_BIN: &str = "binary";

/// The charset/collation a string expression with no source of its own takes:
/// Go's `ctx.GetCharsetInfo()` (`@@character_set_connection` /
/// `@@collation_connection`).
///
/// Captured from TiDB: `SELECT collation(concat('a','b'))` is `utf8mb4_bin`,
/// so this tier's connection collation is `utf8mb4_bin` -- NOT MySQL 8's
/// `utf8mb4_0900_ai_ci`.
#[must_use]
pub const fn connection_charset_info() -> (&'static str, &'static str) {
    (CHARSET_UTF8MB4, COLLATION_UTF8MB4)
}

/// The static result type of an expression, or a `Null` type placeholder.
fn ret_type_of(expr: &Expression) -> FieldType {
    expr.static_type()
        .cloned()
        .unwrap_or_else(|| FieldType::new(FieldTypeCode::Null))
}

/// Go `types.IsTypeBit`.
fn is_type_bit(ft: &FieldType) -> bool {
    ft.code() == FieldTypeCode::Bit
}

/// The embedded `collationInfo` of any expression node.
fn collation_info_of(expr: &Expression) -> &CollationInfo {
    match expr {
        Expression::Column(c) => &c.collation,
        Expression::Constant(c) => &c.collation,
        Expression::CorrelatedColumn(c) => &c.column.collation,
        Expression::ScalarFunction(f) => &f.collation,
    }
}

/// The embedded `collationInfo` of any expression node, mutably.
fn collation_info_mut(expr: &mut Expression) -> &mut CollationInfo {
    match expr {
        Expression::Column(c) => &mut c.collation,
        Expression::Constant(c) => &mut c.collation,
        Expression::CorrelatedColumn(c) => &mut c.column.collation,
        Expression::ScalarFunction(f) => &mut f.collation,
    }
}

/// The result type of any expression node, mutably.
fn ret_type_mut(expr: &mut Expression) -> Option<&mut FieldType> {
    match expr {
        Expression::Column(c) => c.ret_type.as_mut(),
        Expression::Constant(c) => c.ret_type.as_mut(),
        Expression::CorrelatedColumn(c) => c.column.ret_type.as_mut(),
        Expression::ScalarFunction(f) => f.ret_type.as_mut(),
    }
}

/// Go `Expression.Coercibility()`: the stored value once derived, otherwise
/// the per-node-kind default (`deriveCoercibilityForConstant` /
/// `deriveCoercibilityForColumn`).
///
/// Go's `deriveCoercibilityForScalarFunc` is a `panic` stub -- a function's
/// coercibility is always ASSIGNED while it is built (see
/// [`apply_derived_collation`]). A function node assembled by hand, outside
/// the rewriter, lands on the column rule here instead of panicking.
#[must_use]
pub fn coercibility_of(expr: &Expression) -> Coercibility {
    let info = collation_info_of(expr);
    if info.has_coercibility() {
        return info.coercibility();
    }
    let ft = ret_type_of(expr);
    if let Expression::Constant(c) = expr {
        return if c.value.is_null() {
            Coercibility::IGNORABLE
        } else if ft.eval_type() == EvalType::String {
            Coercibility::COERCIBLE
        } else {
            Coercibility::NUMERIC
        };
    }
    if ft.code() == FieldTypeCode::Null {
        Coercibility::IGNORABLE
    } else if is_type_bit(&ft) {
        Coercibility::IMPLICIT
    } else {
        match ft.eval_type() {
            EvalType::Json | EvalType::String => Coercibility::IMPLICIT,
            _ => Coercibility::NUMERIC,
        }
    }
}

/// Go `Column.Repertoire()` and `initConstantRepertoire`
/// (`pkg/planner/core/expression_rewriter.go`): a stored non-zero value wins;
/// otherwise a constant's repertoire comes from its BYTES (any byte >= 0x80
/// makes it UNICODE) and a column's from its charset.
#[must_use]
pub fn repertoire_of(expr: &Expression) -> Repertoire {
    let info = collation_info_of(expr);
    if info.repertoire() != Repertoire::default() {
        return info.repertoire();
    }
    let ft = ret_type_of(expr);
    if let Expression::Constant(c) = expr {
        if ft.eval_type() == EvalType::String {
            if let Some(bytes) = c.value.as_raw_bytes() {
                if bytes.iter().any(|b| *b >= 0x80) {
                    return Repertoire::UNICODE;
                }
            }
        }
        return Repertoire::ASCII;
    }
    match ft.eval_type() {
        EvalType::Json => Repertoire::UNICODE,
        EvalType::String => {
            if ft.charset_name().eq_ignore_ascii_case(CHARSET_ASCII) {
                Repertoire::ASCII
            } else {
                Repertoire::UNICODE
            }
        }
        _ => Repertoire::ASCII,
    }
}

/// The charset and collation an expression contributes to aggregation.
///
/// Go reads `arg.GetType(ctx).GetCharset()/GetCollate()` but SUBSTITUTES
/// `utf8mb4`/`utf8mb4_bin` for a JSON argument and `binary`/`binary` for a BIT
/// one, so neither drags its storage charset into the result.
fn arg_charset_collation(expr: &Expression) -> (String, String) {
    let ft = ret_type_of(expr);
    if ft.eval_type() == EvalType::Json {
        return (CHARSET_UTF8MB4.to_owned(), COLLATION_UTF8MB4.to_owned());
    }
    if is_type_bit(&ft) {
        return (CHARSET_BIN.to_owned(), COLLATION_BIN.to_owned());
    }
    (ft.charset_name().to_owned(), ft.collation_name().to_owned())
}

/// Go `GetDisplayName`: the spelling a user sees in an error. This crate's
/// rewriter names comparisons `eq`/`ne`/`lt`/..., which MySQL prints as the
/// operator itself.
fn display_name(func_name: &str) -> &str {
    match func_name {
        "eq" => "=",
        "ne" => "<>",
        "lt" => "<",
        "le" => "<=",
        "gt" => ">",
        "ge" => ">=",
        "nulleq" => "<=>",
        other => other,
    }
}

/// Go `illegalMixCollationErr`: 1267 for two or three arguments, 1271 for any
/// other arity.
///
/// Captured from TiDB, byte for byte:
/// `[expression:1267]Illegal mix of collations (utf8mb4_general_ci,EXPLICIT)
/// and (utf8mb4_unicode_ci,EXPLICIT) for operation '='`, and
/// `[expression:1271]Illegal mix of collations for operation 'concat_ws'`.
#[must_use]
pub fn illegal_mix_collation_err(func_name: &str, args: &[Expression]) -> EvalError {
    let display = display_name(func_name);
    let part = |e: &Expression| {
        format!(
            "({},{})",
            ret_type_of(e).collation_name(),
            coercibility_of(e).name().unwrap_or("EXPLICIT")
        )
    };
    match args.len() {
        2 => EvalError::IllegalMixCollation(format!(
            "Illegal mix of collations {} and {} for operation '{display}'",
            part(&args[0]),
            part(&args[1])
        )),
        3 => EvalError::IllegalMixCollation(format!(
            "Illegal mix of collations {}, {} and {} for operation '{display}'",
            part(&args[0]),
            part(&args[1]),
            part(&args[2])
        )),
        _ => EvalError::IllegalMixCollationGeneric(format!(
            "Illegal mix of collations for operation '{display}'"
        )),
    }
}

/// Go `inferCollation`: aggregates the arguments' collations left to right,
/// `agg(a, b, c) := agg(agg(a, b), c)`. `None` is the illegal mix the callers
/// turn into 1267/1271.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn infer_collation(exprs: &[Expression]) -> Option<ExprCollation> {
    let Some(first) = exprs.first() else {
        // Go returns the server default with IGNORABLE coercibility.
        return Some(ExprCollation {
            coer: Coercibility::IGNORABLE,
            repe: Repertoire::UNICODE,
            charset: CHARSET_UTF8MB4.to_owned(),
            collation: COLLATION_UTF8MB4.to_owned(),
        });
    };

    let mut repertoire = repertoire_of(first);
    let mut coercibility = coercibility_of(first);
    let (mut dst_charset, mut dst_collation) = arg_charset_collation(first);
    let mut unknown_cs = false;

    for arg in &exprs[1..] {
        let (arg_charset, arg_collation) = arg_charset_collation(arg);
        let arg_coer = coercibility_of(arg);
        let arg_repe = repertoire_of(arg);
        let merge = |r: Repertoire| Repertoire(r.0 | arg_repe.0);

        // A binary-charset operand is compatible with everything, and wins a
        // coercibility tie because `binary` has more precedence.
        if dst_collation == COLLATION_BIN || arg_collation == COLLATION_BIN {
            if coercibility > arg_coer
                || (coercibility == arg_coer && arg_collation == COLLATION_BIN)
            {
                coercibility = arg_coer;
                dst_charset = arg_charset;
                dst_collation = arg_collation;
            }
            repertoire = merge(repertoire);
            continue;
        }

        if dst_charset != arg_charset {
            // A conversion is allowed only when it cannot lose data.
            let mut take_arg = false;
            let converted = if coercibility < arg_coer {
                arg_repe == Repertoire::ASCII
                    || arg_coer >= Coercibility::SYSCONST
                    || is_unicode_collation(&dst_charset)
            } else if coercibility == arg_coer {
                if (is_unicode_collation(&dst_charset) && !is_unicode_collation(&arg_charset))
                    || (dst_charset == CHARSET_UTF8MB4 && arg_charset == CHARSET_UTF8)
                {
                    true
                } else if (is_unicode_collation(&arg_charset)
                    && !is_unicode_collation(&dst_charset))
                    || (arg_charset == CHARSET_UTF8MB4 && dst_charset == CHARSET_UTF8)
                    // Go's third same-coercibility case: an ASCII-repertoire
                    // accumulator yields to a non-ASCII argument. It reaches
                    // the same conclusion as the two above -- adopt the
                    // argument's charset -- so they share one arm here.
                    || (repertoire == Repertoire::ASCII && arg_repe != Repertoire::ASCII)
                {
                    take_arg = true;
                    true
                } else {
                    repertoire != Repertoire::ASCII && arg_repe == Repertoire::ASCII
                }
            } else {
                take_arg = repertoire == Repertoire::ASCII
                    || coercibility >= Coercibility::SYSCONST
                    || is_unicode_collation(&arg_charset);
                take_arg
            };
            if converted {
                if take_arg {
                    coercibility = arg_coer;
                    dst_charset = arg_charset;
                    dst_collation = arg_collation;
                }
                repertoire = merge(repertoire);
                continue;
            }
            // Cannot apply conversion: wait for an explicit COLLATE clause.
            repertoire = merge(repertoire);
            coercibility = Coercibility::NONE;
            dst_charset = CHARSET_BIN.to_owned();
            dst_collation = COLLATION_BIN.to_owned();
            unknown_cs = true;
        } else {
            // Same charset: the lower coercibility wins; a tie between two
            // different non-`_bin` collations is an unresolved mix.
            if coercibility == arg_coer {
                if dst_collation == arg_collation {
                    // Identical: nothing to resolve.
                } else if coercibility == Coercibility::EXPLICIT {
                    return None;
                } else if is_bin_collation(&dst_collation) {
                    // The `_bin` side already wins.
                } else if is_bin_collation(&arg_collation) {
                    dst_charset = arg_charset;
                    dst_collation = arg_collation;
                } else {
                    coercibility = Coercibility::NONE;
                    dst_collation = get_bin_collation(&arg_charset).to_owned();
                    dst_charset = arg_charset;
                }
            } else if coercibility > arg_coer {
                coercibility = arg_coer;
                dst_charset = arg_charset;
                dst_collation = arg_collation;
            }
            repertoire = merge(repertoire);
        }
    }

    if unknown_cs && coercibility != Coercibility::EXPLICIT {
        return None;
    }

    Some(ExprCollation {
        coer: coercibility,
        repe: repertoire,
        charset: dst_charset,
        collation: dst_collation,
    })
}

/// Go `CheckAndDeriveCollationFromExprs`.
///
/// `safeConvert`'s encoding-validity check is DEFERRED (documented): it only
/// rejects when the aggregated charset cannot represent an argument's bytes,
/// and every charset this tier's columns can hold is `utf8mb4` or `binary`,
/// where Go's `FindEncodingTakeUTF8AsNoop` is a no-op and the check always
/// passes. It becomes reachable when a `gbk`/`latin1` column can hold a value.
pub fn check_and_derive_collation_from_exprs(
    func_name: &str,
    eval_type: EvalType,
    args: &[Expression],
) -> Result<ExprCollation, EvalError> {
    let Some(mut ec) = infer_collation(args) else {
        return Err(illegal_mix_collation_err(func_name, args));
    };
    if eval_type != EvalType::String && ec.coer == Coercibility::NONE {
        return Err(illegal_mix_collation_err(func_name, args));
    }
    if eval_type == EvalType::String && ec.coer == Coercibility::NUMERIC {
        let (chs, coll) = connection_charset_info();
        ec.charset = chs.to_owned();
        ec.collation = coll.to_owned();
        ec.coer = Coercibility::COERCIBLE;
        ec.repe = Repertoire::ASCII;
    }
    Ok(ec)
}

/// Go `deriveCollation`: which arguments a function aggregates, and what the
/// result's own coercibility/repertoire become.
///
/// `func_name` is this crate's rewriter spelling (`eq`, `lt`, `like`, ...),
/// which maps 1:1 onto Go's `ast.EQ`/`ast.LT`/`ast.Like` constants.
///
/// DEFERRED (documented) relative to Go's full switch: `date_format`/
/// `time_format`, `cast` (the rewriter knows the target type and sets the
/// result collation itself), `case` (Go's own comment marks its aggregation
/// as incorrect), `field`, and the JSON-returning family beyond
/// `json_pretty`/`json_quote`. Every unlisted name lands in the same default
/// arm Go uses.
#[allow(clippy::too_many_lines)]
pub fn derive_collation(
    func_name: &str,
    args: &[Expression],
    ret_type: EvalType,
) -> Result<ExprCollation, EvalError> {
    match func_name {
        // Aggregate over ALL arguments, string in / string out.
        "concat" | "concat_ws" | "lower" | "lcase" | "reverse" | "upper" | "ucase" | "quote"
        | "coalesce" | "greatest" | "least" => {
            check_and_derive_collation_from_exprs(func_name, ret_type, args)
        }
        // Only the first argument decides.
        "left" | "right" | "repeat" | "trim" | "ltrim" | "rtrim" | "substr" | "substring"
        | "mid" | "substring_index" | "replace" | "translate"
            if !args.is_empty() =>
        {
            check_and_derive_collation_from_exprs(func_name, ret_type, &args[..1])
        }
        // `INSERT(str, pos, len, newstr)`: arguments 0 and 3.
        "insert_func" if args.len() == 4 => check_and_derive_collation_from_exprs(
            func_name,
            ret_type,
            &[args[0].clone(), args[3].clone()],
        ),
        // `LPAD(str, len, padstr)` / `RPAD`: arguments 0 and 2.
        "lpad" | "rpad" if args.len() == 3 => check_and_derive_collation_from_exprs(
            func_name,
            ret_type,
            &[args[0].clone(), args[2].clone()],
        ),
        // `ELT`/`EXPORT_SET`/`MAKE_SET`: every argument but the first.
        "elt" | "export_set" | "make_set" if !args.is_empty() => {
            check_and_derive_collation_from_exprs(func_name, ret_type, &args[1..])
        }
        // Int-returning, but the comparison itself needs a collation.
        "find_in_set" | "regexp" => {
            check_and_derive_collation_from_exprs(func_name, EvalType::Int, args)
        }
        "locate" | "instr" | "position" | "regexp_like" | "regexp_substr" | "regexp_instr"
            if args.len() >= 2 =>
        {
            check_and_derive_collation_from_exprs(func_name, ret_type, &args[..2])
        }
        // Comparison: aggregate when the compare type is string, then report
        // the RESULT as a NUMERIC/ASCII int (the aggregated collation stays on
        // the result type, which is where the comparer reads it from).
        "ge" | "le" | "gt" | "lt" | "eq" | "ne" | "nulleq" | "strcmp" => {
            if args.len() == 2 && compare_is_string(args) {
                let mut ec = check_and_derive_collation_from_exprs(func_name, EvalType::Int, args)?;
                ec.coer = Coercibility::NUMERIC;
                ec.repe = Repertoire::ASCII;
                return Ok(ec);
            }
            Ok(default_collation(ret_type))
        }
        // `IF(cond, a, b)` aggregates the two branches; `IFNULL(a, b)` both.
        "if" if args.len() == 3 => check_and_derive_collation_from_exprs(
            func_name,
            ret_type,
            &[args[1].clone(), args[2].clone()],
        ),
        "ifnull" if args.len() == 2 => {
            check_and_derive_collation_from_exprs(func_name, ret_type, args)
        }
        "like" | "ilike" if args.len() >= 2 => {
            let mut ec =
                check_and_derive_collation_from_exprs(func_name, EvalType::Int, &args[..2])?;
            ec.coer = Coercibility::NUMERIC;
            ec.repe = Repertoire::ASCII;
            Ok(ec)
        }
        "in" if !args.is_empty() => {
            if ret_type_of(&args[0]).eval_type() == EvalType::String {
                return check_and_derive_collation_from_exprs(func_name, EvalType::Int, args);
            }
            Ok(default_collation(ret_type))
        }
        // Go `ast.Database, ast.User, ast.Version, ...`: a system constant.
        "database"
        | "schema"
        | "user"
        | "session_user"
        | "current_user"
        | "system_user"
        | "version"
        | "current_role"
        | "tidb_version"
        | "current_resource_group" => {
            let (chs, coll) = connection_charset_info();
            Ok(ExprCollation {
                coer: Coercibility::SYSCONST,
                repe: Repertoire::UNICODE,
                charset: chs.to_owned(),
                collation: coll.to_owned(),
            })
        }
        // Pure-ASCII producers: the connection charset with ASCII repertoire.
        "format" | "space" | "to_base64" | "uuid" | "hex" | "md5" | "sha" | "sha1" | "sha2"
        | "sm3" => {
            let (chs, coll) = connection_charset_info();
            Ok(ExprCollation {
                coer: Coercibility::COERCIBLE,
                repe: Repertoire::ASCII,
                charset: chs.to_owned(),
                collation: coll.to_owned(),
            })
        }
        // JSON functions always return utf8mb4/utf8mb4_bin.
        "json_pretty" | "json_quote" => Ok(ExprCollation {
            coer: Coercibility::COERCIBLE,
            repe: Repertoire::UNICODE,
            charset: CHARSET_UTF8MB4.to_owned(),
            collation: COLLATION_UTF8MB4.to_owned(),
        }),
        _ => Ok(default_collation(ret_type)),
    }
}

/// Go `deriveCollation`'s default arm: `binary`/NUMERIC/ASCII for a
/// non-string result, the connection charset for a string one.
fn default_collation(ret_type: EvalType) -> ExprCollation {
    let mut ec = ExprCollation {
        coer: Coercibility::NUMERIC,
        repe: Repertoire::ASCII,
        charset: CHARSET_BIN.to_owned(),
        collation: COLLATION_BIN.to_owned(),
    };
    if ret_type == EvalType::String {
        let (chs, coll) = connection_charset_info();
        ec.charset = chs.to_owned();
        ec.collation = coll.to_owned();
        ec.coer = Coercibility::COERCIBLE;
        if ec.charset != CHARSET_ASCII {
            ec.repe = Repertoire::UNICODE;
        }
    }
    ec
}

/// Go `getBaseCmpType`'s question in the one form `deriveCollation` asks it:
/// is this a STRING comparison? Both operands must be string-typed -- a string
/// compared with a number promotes to REAL and consults no collation, matching
/// `builtin_compare.go`.
fn compare_is_string(args: &[Expression]) -> bool {
    ret_type_of(&args[0]).eval_type() == EvalType::String
        && ret_type_of(&args[1]).eval_type() == EvalType::String
}

/// Writes a derived [`ExprCollation`] onto a freshly built node.
///
/// Go's `newBaseBuiltinFuncWithTp` sets the function's own coercibility and
/// repertoire AND stamps the charset/collation onto its RESULT TYPE -- which
/// is where every collation-aware signature (`builtinCompareStringSig`,
/// `builtinLikeSig`, `builtinLocate*Sig`, ...) reads its collator from. Both
/// halves matter: the first feeds an enclosing function's aggregation, the
/// second feeds this function's own evaluation.
pub fn apply_derived_collation(expr: &mut Expression, ec: &ExprCollation) {
    if let Some(ft) = ret_type_mut(expr) {
        ft.set_charset_name(ec.charset.clone());
        ft.set_collation_name(ec.collation.clone());
    }
    let info = collation_info_mut(expr);
    info.set_coercibility(ec.coer);
    info.set_repertoire(ec.repe);
    info.set_charset_and_collation(&ec.charset, &ec.collation);
}

/// The collation a collation-aware signature must run under: the one the
/// derivation stamped on this node's result type.
///
/// An unknown or empty collation name falls back to `utf8mb4_bin`, this tier's
/// connection collation -- the value the pre-derivation code hard-coded.
#[must_use]
pub fn collation_of_node(expr: &Expression) -> Collation {
    Collation::from_name(ret_type_of(expr).collation_name()).unwrap_or(Collation::Utf8Mb4Bin)
}

/// Marks an expression as carrying an EXPLICIT `COLLATE` clause.
///
/// Go's `expression_rewriter` `case *ast.SetCollationExpr` writes the
/// collation onto the argument's own result type and raises its coercibility
/// to EXPLICIT. (Go wraps a COLUMN in a cast first, because a column's
/// `RetType` is shared plan metadata; here every node is owned by exactly one
/// expression tree, so the write cannot alias.)
pub fn set_explicit_collation(expr: &mut Expression, collation: Collation) {
    let charset = charset_of_collation(collation);
    if let Some(ft) = ret_type_mut(expr) {
        ft.set_charset_name(charset);
        ft.set_collation_name(collation.name());
    }
    let info = collation_info_mut(expr);
    info.set_coercibility(Coercibility::EXPLICIT);
    info.set_charset_and_collation(charset, collation.name());
    info.set_explicit_charset(true);
}

/// The charset a collation belongs to (Go `charset.GetCollationByName().CharsetName`).
#[must_use]
pub fn charset_of_collation(collation: Collation) -> &'static str {
    match collation {
        Collation::Binary => "binary",
        Collation::AsciiBin => "ascii",
        Collation::Latin1Bin => "latin1",
        Collation::Utf8Bin | Collation::Utf8GeneralCi | Collation::Utf8UnicodeCi => CHARSET_UTF8,
        Collation::GbkBin | Collation::GbkChineseCi => "gbk",
        Collation::Gb18030Bin | Collation::Gb18030ChineseCi => "gb18030",
        _ => CHARSET_UTF8MB4,
    }
}

/// Whether `collation` may follow `COLLATE` on a value of `charset` -- Go's
/// `charset.GetCollationByName` + `CheckCollation` pair, whose failure is 1253.
///
/// Captured from TiDB: `SELECT 'a' COLLATE latin1_bin` fails with
/// `[ddl:1253]COLLATION 'latin1_bin' is not valid for CHARACTER SET 'utf8mb4'`,
/// because a bare string literal is `utf8mb4`.
#[must_use]
pub fn collation_matches_charset(collation: Collation, charset: &str) -> bool {
    charset.eq_ignore_ascii_case(charset_of_collation(collation))
}
