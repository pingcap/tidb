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

//! `compare2` family builtins. Every builtin here is transcreated from its
//! implementation in `pkg/expression/builtin_*.go`, cited per function.

use std::cmp::Ordering;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

use tidb_ast::BinaryOp;
use tidb_datatype::TimeType;

use crate::coerce::{coerce_str, integer_cmp, integer_of};
use crate::ops::{to_decimal, to_f64};
use crate::{eval_binary, Datum, EvalError};

/// Dispatches this family's builtins; `None` if `name` isn't one of them.
pub(crate) fn dispatch(
    name: &str,
    vals: &[Datum],
    ctx: &dyn crate::Columns,
) -> Option<Result<Datum, EvalError>> {
    match (name, vals.len()) {
        ("LEAST", _) => Some(extremum(vals, Ordering::Less, ctx)),
        ("GREATEST", _) => Some(extremum(vals, Ordering::Greater, ctx)),
        ("INTERVAL", n) if n >= 2 => Some(interval(vals, ctx)),
        ("ISNULL", 1) => Some(Ok(Datum::Int(i64::from(matches!(vals[0], Datum::Null))))),
        ("INET_ATON", 1) => Some(inet_aton(&vals[0])),
        ("INET_NTOA", 1) => Some(inet_ntoa(&vals[0])),
        ("INET6_ATON", 1) => Some(inet6_aton(&vals[0])),
        ("INET6_NTOA", 1) => Some(inet6_ntoa(&vals[0])),
        ("IS_IPV4", 1) => Some(is_ipv4_value(&vals[0])),
        ("IS_IPV4_MAPPED", 1) => Some(is_ipv4_mapped_value(&vals[0])),
        ("IS_IPV4_COMPAT", 1) => Some(is_ipv4_compat_value(&vals[0])),
        ("IS_IPV6", 1) => Some(is_ipv6_value(&vals[0])),
        _ => None,
    }
}

/// LEAST/GREATEST: `NULL` if any argument is `NULL`, else the extreme value
/// by `want` (`Less` for LEAST, `Greater` for GREATEST) — a MIXED
/// Int/Decimal/Float argument list promotes through `eval_binary`'s own
/// comparison (confirmed via goeval: `GREATEST(1.5e2, 3.14, 2)` — Float,
/// Decimal, Int all in one call — is `FLOAT:150`, not an error), so no
/// per-type-pair matching is hand-rolled here. When a string is present,
/// Go's FieldType aggregation selects the string signature and stringifies
/// every argument before comparing it; the byte-preserving scalar path below
/// keeps that source boundary without inventing a numeric coercion. Port of
/// the signatures built by `leastFunctionClass` and
/// `greatestFunctionClass` in `pkg/expression/builtin_compare.go`.
fn extremum(vals: &[Datum], want: Ordering, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    // The AST/value evaluator has no argument `FieldType`s, so it can name
    // neither Go's signature nor a derived collation. Both are the chunk
    // evaluator's ([`extremum_with_signature`]'s other caller,
    // `ScalarFunction::eval_by_signature`) -- this tier asks for the
    // value-derived signature and the connection default collation.
    extremum_with_signature(
        vals,
        want,
        None,
        crate::ops::DERIVATION_FREE_COLLATION,
        ctx,
    )
}

/// Go `GLCmpStringMode` (`pkg/expression/builtin_compare.go`): which of the
/// three ETString GREATEST/LEAST signatures `resolveType4Extremum` selected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GlCmpStringMode {
    /// `GLCmpStringDirectly` -> `builtinGreatestStringSig`: compare the
    /// strings themselves, under the function's derived collation.
    Directly,
    /// `GLCmpStringAsDate` -> `builtinGreatestCmpStringAsTimeSig{cmpAsDate:
    /// true}`: parse every argument as a DATE and compare the re-rendered
    /// canonical text.
    AsDate,
    /// `GLCmpStringAsDatetime` -> the same signature with `cmpAsDate: false`,
    /// parsing every argument as a DATETIME.
    AsDatetime,
}

/// Go's `resolveType4Extremum` answer for one GREATEST/LEAST call: which of
/// the eight signatures `getFunction` built. Produced by
/// `crate::rewriter::result_type::gl_signature`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GlSignature {
    /// Go `argTp` -- `aggregateType(args).EvalType()`, forced to
    /// `types.ETString` by a non-`Directly` compare mode or by an ETJson
    /// aggregate. This is what Go's `switch` selects on.
    pub arg_type: tidb_datatype::EvalType,
    /// Which of the three ETString signatures.
    pub cmp_string_mode: GlCmpStringMode,
    /// `fieldTimeType == GLRetDate`, i.e. `builtinGreatestTimeSig`'s
    /// `cmpAsDate`.
    pub ret_date: bool,
}

/// [`extremum`] with the two things only the chunk evaluator knows: the
/// SIGNATURE `resolveType4Extremum` derived from the argument FieldTypes, and
/// the collation `deriveCollation` derived for the function.
///
/// The signature is the load-bearing half. Go's `argTp` is the AGGREGATE of
/// the argument types, so it fixes the comparison domain before any value
/// exists; reading the domain off the runtime datums instead answers a
/// different question whenever an argument's declared type and its datum
/// disagree about a domain. Every MySQL type whose datum is neither a string
/// nor a number is such an argument. CAPTURED from TiDB over
/// `enum('{}','[1]','x')` holding `'{}'` and `set('a','b','c')` holding
/// `'b'`:
///
/// ```text
/// greatest(e, 2) -> {}      least(e, 2) -> 2
/// greatest(s, 2) -> b       least(s, 2) -> 2
/// ```
///
/// Both aggregate to a string kind, so Go stringifies the `2` and compares
/// text. A value-derived domain sees an ENUM datum beside an integer, compares
/// them as numbers against the enum's ORDINAL, and returns the two answers
/// swapped -- and returns the enum datum itself where a string was declared,
/// which the wire encoder then renders as its raw 8-byte ordinal followed by
/// the name.
///
/// `mode != Directly` is `builtinGreatestCmpStringAsTimeSig` /
/// `builtinLeastCmpStringAsTimeSig`: EVERY argument is parsed as a time and
/// re-emitted canonically before comparison, so which argument wins changes.
/// CAPTURED from TiDB over a `DATE` column holding `2020-01-01`:
///
/// ```text
/// greatest(d, '99-1-1')  -> 2020-01-01     least(d, '99-1-1')  -> 1999-01-01
/// greatest(d, 'zzz')     -> zzz            least(d, '2019-5-5') -> 2019-05-05
/// ```
///
/// The last row is the one that pins the ERROR rule: an argument that does not
/// parse keeps its ORIGINAL text (Go `doTimeConversionForGL` leaves `strVal`
/// alone once `handleInvalidTimeError` has downgraded the error to a warning),
/// which is why `'zzz'` -- not the date -- is the greatest. Note also that this
/// signature compares with `strings.Compare`, NOT the collator: only the
/// `Directly` mode is collation-aware.
///
/// # What this selection does NOT decide
///
/// Go's three numeric arms differ only in which cast
/// `newBaseBuiltinFuncWithTp` wrapped the arguments in, so they share
/// [`extremum_numeric`] here -- and that block still reads the result's
/// promotion off the runtime datums rather than off the aggregate. One
/// measured consequence, present before this selection landed and unchanged by
/// it: over `create table g(i int, d decimal(10,3))` holding `-5` and `2.500`,
/// TiDB answers `least(i, d)` with `-5` and this tier with `-5.000`.
/// `WrapWithCastAsDecimal` takes each argument's OWN decimal
/// (`tp.SetDecimalUnderLimit(expr.GetType().GetDecimal())`), so an integer
/// COLUMN keeps scale 0 -- while the all-constant `least(1, 2.5)` really is
/// `1.0`, which is the capture the block was built on. Separating a typed
/// integer argument from a folded integer constant is the next rung, not this
/// one.
///
/// # Mutation probes
///
/// Run against `cargo test -p tidb-expr -p tidb-session -p
/// difftest-result-tests`:
///
///  * IGNORE the passed signature and always take the value-derived one --
///    killed by `greatest_least_source_vectors_compare_strings_as_time`.
///  * `ret_date` forced to `false` -- killed by
///    `an_all_temporal_greatest_returns_the_aggregated_temporal_type`.
///  * DROP the ETJson-to-ETString fold -- killed only after
///    `a_json_greatest_compares_the_rendered_text` was added; a JSON aggregate
///    needs two values that rank one way as text and the other as numbers
///    (`'10'` and `'9'`), because identical JSON arguments agree in both
///    domains.
///  * `ret_date` from the DATUM kinds instead of the aggregate -- killed only
///    after `the_temporal_greatest_result_type_follows_the_aggregate_not_the
///    _values`. A declared type and its datums differ only where an
///    expression's type is merged from branches it did not take, which is what
///    `IFNULL(d, dt)` is.
///  * DROP the ENUM/SET/JSON widening in `extremum_return_type` -- killed by a
///    PANIC, not a wrong value: the chunk column expects a name/value cell.
///  * Route ETDuration through the STRING arm (over-application) -- killed only
///    after `100:00:00`/`20:00:00` was added. The declared duration result type
///    casts a stray string answer straight back into a duration, so this hides
///    completely unless the two domains ORDER the values differently.
///  * DROP the time arm entirely -- killed by both temporal tests.
///  * DROP the temporal scan's DATETIME preference -- killed only after
///    `greatest(d, dt, '2020-01-01 05:00:00')` was added.
///  * FLATTEN the value-derived fallback to one domain -- killed by
///    `expr_eval_matches_go_engine`.
///
/// TWO SURVIVED, and both are argued rather than fixture-covered:
///
///  * DROP the `cmpStringMode != Directly => argTp = ETString` override.
///    Go writes it as the `if` arm of an `if`/`else if` whose `else` handles
///    ETJson, and `resolveType4Extremum` only leaves `Directly` when the
///    aggregate is a string KIND -- which is `ETString || ETJson`. So the
///    override can differ from the ETJson fold only for a JSON aggregate that
///    also has a temporal argument, and `mergeFieldType` sends JSON beside
///    anything else to VARCHAR. CAPTURED, closing the hole: `greatest(j, d)`
///    over a JSON and a DATE column is `[1]`, the compare-as-date answer, not
///    a JSON one. The branch is unreachable; the faithful form is kept because
///    it is Go's, and because Go's ordering DOES decide whether
///    `unsupportedJSONComparison` warns.
///  * DROP `gl_signature`'s requirement that EVERY argument be statically
///    typed, aggregating only the typed ones. No fixture reaches it because
///    the rewriter types every expression it hands to `eval_by_signature`
///    today -- but `ScalarFunction::ret_type` is an `Option`, so an inner
///    builtin the return-type table cannot name would make the aggregate a
///    claim about a partial argument list. Go always holds every type, so
///    naming a domain from a subset is a claim Go never makes.
pub(crate) fn extremum_with_signature(
    vals: &[Datum],
    want: Ordering,
    signature: Option<GlSignature>,
    collation: tidb_datatype::Collation,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    if vals.is_empty() {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    if vals.contains(&Datum::Null) {
        return Ok(Datum::Null);
    }
    let signature = signature.unwrap_or_else(|| value_derived_signature(vals));
    let mode = signature.cmp_string_mode;
    if signature.arg_type != tidb_datatype::EvalType::String {
        // Go's ETDatetime/ETTimestamp arm; every other arm -- ETInt, ETReal,
        // ETDecimal, ETDuration, ETVectorFloat32 -- shares the
        // compare-and-return block at the bottom, which is what those five
        // `evalXxx` bodies do once `newBaseBuiltinFuncWithTp` has cast the
        // arguments into the domain.
        if matches!(
            signature.arg_type,
            tidb_datatype::EvalType::Datetime | tidb_datatype::EvalType::Timestamp
        ) {
            if let Some(result) = extremum_time(vals, want, signature.ret_date) {
                return Ok(result);
            }
        }
        return extremum_numeric(vals, want);
    }
    if mode != GlCmpStringMode::Directly {
        let cmp_as_date = mode == GlCmpStringMode::AsDate;
        let mut best: Option<String> = None;
        for value in vals {
            let Some(text) = coerce_str(value)? else {
                return Ok(Datum::Null);
            };
            let text = time_conversion_for_gl(cmp_as_date, &text, ctx);
            let wins = match &best {
                None => true,
                Some(best) if want == Ordering::Greater => text > *best,
                Some(best) => text < *best,
            };
            if wins {
                best = Some(text);
            }
        }
        return Ok(Datum::new_string(best.unwrap_or_default()));
    }
    // Go `builtinGreatestStringSig.evalString`: every argument is rendered
    // with `EvalString` -- the cast `newBaseBuiltinFuncWithTp` already wrapped
    // it in -- and compared with `types.CompareString(v, maxv, b.collation)`,
    // the function's own derived collator rather than raw bytes. This covers
    // `TestGreatestLeastFunc`'s `("123a", "b", "c", 12)` row: GREATEST is
    // `"c"`, LEAST is `"12"`. CAPTURED from TiDB:
    // `greatest('a' collate utf8mb4_general_ci, 'B')` is `B` and `least(...)`
    // is `a`, the SWAP of the byte answer; and under the PAD SPACE default
    // `greatest('a', 'a ')` is `a`, because the two compare EQUAL and Go keeps
    // the earlier argument.
    let collator = tidb_datatype::get_collator(collation.name());
    let mut best = extremum_string_value(&vals[0])?;
    for v in &vals[1..] {
        let candidate = extremum_string_value(v)?;
        if collator.compare(&candidate, &best) == want {
            best = candidate;
        }
    }
    Ok(Datum::new_string(best))
}

/// Go's ETDatetime/ETTimestamp arm (`builtinGreatestTimeSig.evalTime`): every
/// argument is cast onto the AGGREGATED temporal type before it is compared,
/// and the winner is converted to that type on the way out
/// (`res.Convert(tc, getAccurateTimeTypeForGLRet(b.cmpAsDate))`). So a DATE
/// beside a DATETIME compares as midnight of that day and prints as a datetime
/// -- `LEAST(date '2020-01-01', datetime '2020-01-01 10:00:00')` is
/// `2020-01-01 00:00:00`, not `2020-01-01`.
///
/// `None` when an argument's datum is not a time after all, which sends the
/// call to [`extremum_numeric`] rather than inventing a conversion this tier
/// has not ported.
fn extremum_time(vals: &[Datum], want: Ordering, ret_date: bool) -> Option<Datum> {
    let mut best = match &vals[0] {
        Datum::Time(time) => *time,
        _ => return None,
    };
    for value in &vals[1..] {
        let Datum::Time(time) = value else {
            return None;
        };
        if time.compare(best) == want {
            best = *time;
        }
    }
    best.set_kind(if ret_date {
        TimeType::Date
    } else {
        TimeType::DateTime
    });
    Some(Datum::Time(best))
}

/// Go's ETInt, ETReal, ETDecimal, ETDuration and ETVectorFloat32 arms, which
/// all read `for i := range b.args { ... if v > maxv { maxv = v } }` over
/// arguments `newBaseBuiltinFuncWithTp` has already cast into the domain.
///
/// The domain that the aggregate named is reproduced here from the arguments
/// rather than from the winner: which argument wins must not decide the
/// result's type.
fn extremum_numeric(vals: &[Datum], want: Ordering) -> Result<Datum, EvalError> {
    let op = if want == Ordering::Greater {
        BinaryOp::Gt
    } else {
        BinaryOp::Lt
    };
    let mut best = vals[0].clone();
    for v in &vals[1..] {
        if eval_binary(op, v.clone(), best.clone())? == Datum::Int(1) {
            best = v.clone();
        }
    }
    // The RESULT promotes to the widest type among ALL arguments (Float >
    // Decimal > Int, same hierarchy `+`/`-` use) — not just whichever raw
    // value happened to win the comparison (a real bug caught by the
    // differential corpus, not assumed correct on the first attempt:
    // `LEAST(1.5e2, 3.14, 2)` is `FLOAT:2`, not `INT:2`, even though the
    // winning argument `2` was written as a bare Int literal).
    if vals
        .iter()
        .any(|v| matches!(v, Datum::Real(_) | Datum::Float32(_)))
    {
        return Ok(Datum::Real(to_f64(best)));
    }
    if vals.iter().any(|v| matches!(v, Datum::Decimal(_))) {
        // The result carries the SIGNATURE's scale, not the winning value's
        // own. Go aggregates the arguments' FieldTypes into one return type
        // whose `Decimal` is the MAX over them, then wraps every argument in
        // `WrapWithCastAsDecimal` to it -- so which argument wins cannot
        // change the scale. Reading the runtime datum instead lets an integer
        // winner keep scale 0. CAPTURED from real TiDB:
        //
        //   least(1, 2.5)      1.0     least(2.5, 1)         1.0
        //   least(1, 2.555)    1.000   least(1, 2.5, 3.25)   1.00
        //   least(1, 2.50)     1.00    greatest(3, 2.55, 1)  3.00
        //   greatest(3, 2.5)   3.0     greatest(2.5, 1.234)  2.500
        //
        // `least(2.5, 1)` and `greatest(2.5, 1.234)` are the two that pin the
        // rule: in the first the winner is an INT and still prints a
        // fraction, in the second the winner's own scale is 1 and the printed
        // one is 3.
        let scale = vals
            .iter()
            .map(|value| match value {
                Datum::Decimal(value) => value.scale(),
                _ => 0,
            })
            .max()
            .unwrap_or(0);
        return Ok(Datum::Decimal(to_decimal(best).cast_to_precision(0, scale)));
    }
    // TiDB's common numeric type for a mixed signed/unsigned integer list is
    // DECIMAL. Preserve that result domain even when the winning value is an
    // integer: GREATEST(1, CAST(2 AS UNSIGNED)) is DEC:2, not UINT:2.
    if vals.iter().any(|v| matches!(v, Datum::Int(_)))
        && vals.iter().any(|v| matches!(v, Datum::UInt(_)))
    {
        return Ok(Datum::Decimal(to_decimal(best)));
    }
    Ok(best)
}

/// The signature for a caller with no argument `FieldType`s at all.
///
/// Go always has them, so this is not a second rule but the same question
/// asked of the only evidence the AST/value tier holds: a datum's own kind.
/// The three answers it can give are the three Go arms this file implements,
/// and the compare mode is `Directly` because a temporal ARGUMENT is exactly
/// what a bare datum cannot reveal.
fn value_derived_signature(vals: &[Datum]) -> GlSignature {
    use tidb_datatype::EvalType;
    let arg_type = if vals.iter().all(|v| matches!(v, Datum::Time(_))) {
        EvalType::Datetime
    } else if vals
        .iter()
        .any(|v| matches!(v, Datum::String(_) | Datum::Bytes(_)))
    {
        EvalType::String
    } else {
        EvalType::Real
    };
    GlSignature {
        arg_type,
        cmp_string_mode: GlCmpStringMode::Directly,
        ret_date: vals
            .iter()
            .all(|v| matches!(v, Datum::Time(time) if time.kind() == TimeType::Date)),
    }
}

fn extremum_string_value(value: &Datum) -> Result<Vec<u8>, EvalError> {
    Ok(match value {
        Datum::String(value) => value.bytes().to_vec(),
        Datum::Bytes(value) => value.clone(),
        Datum::Int(value) => value.to_string().into_bytes(),
        Datum::UInt(value) => value.to_string().into_bytes(),
        Datum::Decimal(value) => value.to_string().into_bytes(),
        Datum::Real(value) => value.to_string().into_bytes(),
        Datum::Null => return Err(EvalError::Unsupported("NULL string operand")),
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel string operand"));
        }
        other => other
            .to_bytes()
            .map_err(|_| EvalError::Unsupported("datum string conversion"))?,
    })
}

/// Go `doTimeConversionForGL` (`pkg/expression/builtin_compare.go`): parse one
/// GREATEST/LEAST argument as a DATE or DATETIME and re-render it as
/// `types.Time.String()`.
///
/// A value that does not parse keeps its ORIGINAL text: Go raises the invalid
/// time through `handleInvalidTimeError`, and in the non-erroring modes that
/// leaves `strVal` untouched. The zero date is NOT rejected here -- this is not
/// the cast path's `NO_ZERO_DATE` check -- so
/// `least(<a DATE>, '0000-00-00')` is `0000-00-00`, captured from TiDB.
fn time_conversion_for_gl(cmp_as_date: bool, value: &str, ctx: &dyn crate::Columns) -> String {
    let (kind, fsp) = if cmp_as_date {
        // `types.ParseDate` asks for `MinFsp`; a DATE carries no fraction.
        (tidb_datatype::TimeType::Date, 0)
    } else {
        // `types.ParseDatetime` asks for the literal's own fraction width.
        (
            tidb_datatype::TimeType::DateTime,
            i64::from(tidb_datatype::get_fsp(value)),
        )
    };
    let parsed = tidb_datatype::parse_time(
        value,
        kind,
        fsp,
        false,
        true,
        ctx.date_modes().allow_invalid_dates,
        &ctx.time_zone(),
    );
    match parsed {
        Ok(parsed) => parsed.time.to_string(),
        Err(_) => value.to_owned(),
    }
}

/// `INTERVAL(n, n1, n2, ...)`: return the zero-based position of the first
/// boundary greater than `n`, or the number of boundaries when none is
/// greater. Port of `builtinIntervalIntSig.evalInt` and
/// `builtinIntervalRealSig.evalInt` in `pkg/expression/builtin_compare.go`.
///
/// Like TiDB, an integer-only call uses its exact integer signature; the
/// presence of any decimal, float, or string selects the lossy real
/// signature. A NULL target is `-1`; NULL boundaries participate only in the
/// nullable signature and are skipped. The binary search intentionally keeps
/// TiDB's documented precondition that non-NULL boundaries are sorted.
fn interval(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    if vals.iter().any(Datum::is_range_sentinel) {
        return Err(EvalError::Unsupported("range sentinel INTERVAL argument"));
    }
    if vals[0] == Datum::Null {
        return Ok(Datum::Int(-1));
    }
    let nullable = vals.iter().any(|v| matches!(v, Datum::Null));
    if vals
        .iter()
        .all(|v| matches!(v, Datum::Int(_) | Datum::UInt(_) | Datum::Null))
    {
        let target = match integer_of(&vals[0])? {
            Some(value) => value,
            None => unreachable!("all-int guard"),
        };
        let index = if nullable {
            vals[1..]
                .iter()
                .position(|v| {
                    integer_of(v)
                        .expect("range sentinels rejected above")
                        .is_some_and(|boundary| integer_cmp(target, boundary).is_lt())
                })
                .unwrap_or(vals.len() - 1)
        } else {
            vals[1..].partition_point(|v| {
                integer_of(v)
                    .expect("range sentinels rejected above")
                    .is_some_and(|boundary| integer_cmp(boundary, target).is_le())
            })
        };
        return Ok(Datum::Int(index as i64));
    }

    let target = interval_real(&vals[0], ctx)?;
    // Every boundary is converted UP FRONT: a boundary with no ETReal
    // reading is an error in TiDB, and an error cannot leave
    // `partition_point`'s comparator.
    let boundaries = vals[1..]
        .iter()
        .map(|boundary| match boundary {
            Datum::Null => Ok(None),
            other => interval_real(other, ctx).map(Some),
        })
        .collect::<Result<Vec<_>, EvalError>>()?;
    let index = if nullable {
        boundaries
            .iter()
            .position(|boundary| boundary.is_some_and(|value| target < value))
            .unwrap_or(vals.len() - 1)
    } else {
        boundaries.partition_point(|boundary| boundary.is_some_and(|value| value <= target))
    };
    Ok(Datum::Int(index as i64))
}

/// TiDB's real signature evaluates every argument as `ETReal`, which is the
/// same coercion `eval_binary` performs when a string meets a number -- down
/// to `INTERVAL('b', ...)` reading `'b'` as 0 (`TestIntervalFunc`). Sharing
/// that one port instead of keeping a second copy of the numeric-prefix rule
/// here is what makes an invalid-UTF-8 boundary read its prefix rather than
/// silently sort as zero.
fn interval_real(value: &Datum, ctx: &dyn crate::Columns) -> Result<f64, EvalError> {
    crate::ops::to_f64_with_mysql_string(value, ctx)
}

/// `INET_ATON(expr)`: decimal dotted IPv4 to an unsigned 32-bit integer,
/// including TiDB/MySQL's one-, two-, and three-component shorthand. Port of
/// `builtinInetAtonSig.evalInt` in `pkg/expression/builtin_miscellaneous.go`.
/// Invalid non-NULL input is an evaluation error in TiDB's strict-context
/// unit test; this family has no matching generic SQL-error variant, so it is
/// surfaced as `Unsupported` until the frozen `EvalError` domain grows one.
fn inet_aton(value: &Datum) -> Result<Datum, EvalError> {
    let Some(text) = coerce_str(value)? else {
        return Ok(Datum::Null);
    };
    if text.is_empty() || text.ends_with('.') {
        return Err(EvalError::Unsupported("invalid INET_ATON address"));
    }
    let mut result = 0_u64;
    let mut byte_result = 0_u64;
    let mut dots = 0_u8;
    for byte in text.bytes() {
        match byte {
            b'0'..=b'9' => {
                byte_result = byte_result * 10 + u64::from(byte - b'0');
                if byte_result > 255 {
                    return Err(EvalError::Unsupported("invalid INET_ATON address"));
                }
            }
            b'.' => {
                dots += 1;
                if dots > 3 {
                    return Err(EvalError::Unsupported("invalid INET_ATON address"));
                }
                result = (result << 8) + byte_result;
                byte_result = 0;
            }
            _ => return Err(EvalError::Unsupported("invalid INET_ATON address")),
        }
    }
    if dots == 1 {
        result <<= 8;
    }
    if dots <= 2 {
        result <<= 8;
    }
    Ok(Datum::UInt((result << 8) + byte_result))
}

/// `INET_NTOA(expr)`: unsigned 32-bit integer to canonical dotted IPv4.
/// Port of `builtinInetNtoaSig.evalString` in
/// `pkg/expression/builtin_miscellaneous.go`. The scalar domain has no
/// planning-time `ETInt` cast, so only its already-integer values are
/// representable faithfully; other types remain honestly unsupported.
fn inet_ntoa(value: &Datum) -> Result<Datum, EvalError> {
    let value = match value {
        Datum::Null => return Ok(Datum::Null),
        Datum::Int(value) => *value as u64,
        Datum::UInt(value) => *value,
        _ => return Err(EvalError::Unsupported("INET_NTOA non-integer argument")),
    };
    let Ok(value) = u32::try_from(value) else {
        return Ok(Datum::Null);
    };
    Ok(Datum::new_string(format!(
        "{}.{}.{}.{}",
        value >> 24,
        (value >> 16) & 0xff,
        (value >> 8) & 0xff,
        value & 0xff
    )))
}

/// `INET6_ATON(expr)`: parse an IPv4 or IPv6 spelling into the raw network
/// byte representation used by TiDB's binary `ETString` signature.  Go's
/// `net.ParseIP` always returns a 16-byte value for a colon-containing
/// spelling, including IPv4-mapped and IPv4-compatible forms; the source
/// then keeps the four-byte representation only for a plain dotted IPv4
/// input.  Port of `builtinInet6AtonSig.evalString` in
/// `pkg/expression/builtin_miscellaneous.go`.
fn inet6_aton(value: &Datum) -> Result<Datum, EvalError> {
    let text = match value {
        Datum::Null => return Ok(Datum::Null),
        // `EvalString` in the Go signature preserves raw bytes.  IP syntax is
        // ASCII, so invalid UTF-8 is simply the same parse failure rather than
        // a lossy replacement conversion.
        Datum::String(value) => std::str::from_utf8(value.bytes()),
        Datum::Bytes(value) => std::str::from_utf8(value),
        _ => return inet6_aton_text(&coerce_str(value)?.expect("non-NULL scalar")),
    }
    .map_err(|_| EvalError::Unsupported("invalid INET6_ATON address"))?;
    inet6_aton_text(text)
}

fn inet6_aton_text(text: &str) -> Result<Datum, EvalError> {
    if text.is_empty() {
        return Err(EvalError::Unsupported("invalid INET6_ATON address"));
    }
    // Keep the source's four-byte result only when the original spelling is
    // plain IPv4.  `Ipv6Addr::from_str` handles all colon-containing forms,
    // including embedded IPv4 and mapped IPv4, and its octets are exactly the
    // bytes Go copies from `ip.To16()`/`ip.To4()`.
    if !text.contains(':') {
        if let Ok(ip) = Ipv4Addr::from_str(text) {
            return Ok(Datum::new_bytes(ip.octets()));
        }
    }
    Ipv6Addr::from_str(text)
        .map(|ip| Datum::new_bytes(ip.octets()))
        .map_err(|_| EvalError::Unsupported("invalid INET6_ATON address"))
}

/// `INET6_NTOA(expr)`: render a four- or sixteen-byte binary string as the
/// canonical textual IPv4/IPv6 spelling.  Go first asks `net.IP.String()` to
/// format the bytes and then prefixes a sixteen-byte mapped IPv4 result with
/// `::ffff:`; Rust's `Ipv6Addr` formatter emits that same canonical mapped
/// spelling.  Any other byte length is SQL `NULL`, matching the source's
/// `net.ParseIP(ip) == nil` branch.  Port of
/// `builtinInet6NtoaSig.evalString` in `pkg/expression/builtin_miscellaneous.go`.
fn inet6_ntoa(value: &Datum) -> Result<Datum, EvalError> {
    let bytes = match value {
        Datum::Null => return Ok(Datum::Null),
        Datum::String(value) => value.bytes().to_vec(),
        Datum::Bytes(value) => value.clone(),
        // The Go function's argument is ETString, so numeric constants are
        // first rendered by EvalString and then interpreted as raw bytes.
        _ => coerce_str(value)?.expect("non-NULL scalar").into_bytes(),
    };
    match bytes.len() {
        4 => Ok(Datum::new_string(
            Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]).to_string(),
        )),
        16 => Ok(Datum::new_string(
            Ipv6Addr::from(<[u8; 16]>::try_from(bytes).unwrap()).to_string(),
        )),
        _ => Ok(Datum::Null),
    }
}

/// `IS_IPV4(expr)`: strict four-component decimal IPv4 predicate. Port of
/// `builtinIsIPv4Sig.evalInt` and its `isIPv4` helper in
/// `pkg/expression/builtin_miscellaneous.go`.
fn is_ipv4_value(value: &Datum) -> Result<Datum, EvalError> {
    let Some(value) = coerce_str(value)? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Int(i64::from(is_ipv4(&value))))
}

fn is_ipv4(value: &str) -> bool {
    let mut dots = 0;
    let mut component = 0_u16;
    let mut previous_dot = true;
    for byte in value.bytes() {
        match byte {
            b'0'..=b'9' => {
                // Only whether the component exceeds 255 matters. Saturate
                // rather than letting an arbitrarily long invalid component
                // overflow Rust's debug arithmetic before we reject it.
                component = component
                    .saturating_mul(10)
                    .saturating_add(u16::from(byte - b'0'));
                previous_dot = false;
            }
            b'.' => {
                dots += 1;
                if dots > 3 || component > 255 || previous_dot {
                    return false;
                }
                component = 0;
                previous_dot = true;
            }
            _ => return false,
        }
    }
    dots == 3 && component <= 255 && !previous_dot
}

/// `IS_IPV4_MAPPED(expr)`: true only for a sixteen-byte binary payload whose
/// first twelve bytes are the IPv4-mapped prefix (`::ffff:`).  The Go
/// signature receives an ETString and tests the raw bytes directly; keeping
/// this helper byte-oriented is important because arbitrary SQL strings are
/// allowed to contain invalid UTF-8.  Port of
/// `builtinIsIPv4MappedSig.evalInt` in `pkg/expression/builtin_miscellaneous.go`.
fn is_ipv4_mapped_value(value: &Datum) -> Result<Datum, EvalError> {
    let Some(bytes) = eval_string_bytes(value)? else {
        return Ok(Datum::Null);
    };
    let mapped = bytes.len() == 16 && bytes[..12] == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff];
    Ok(Datum::Int(i64::from(mapped)))
}

/// `IS_IPV4_COMPAT(expr)`: true only for a sixteen-byte binary payload whose
/// first twelve bytes are all zero (`::/96`, excluding the mapped `::ffff:`
/// prefix by construction).  Port of `builtinIsIPv4CompatSig.evalInt` in
/// `pkg/expression/builtin_miscellaneous.go`.
fn is_ipv4_compat_value(value: &Datum) -> Result<Datum, EvalError> {
    let Some(bytes) = eval_string_bytes(value)? else {
        return Ok(Datum::Null);
    };
    let compat = bytes.len() == 16 && bytes[..12] == [0; 12];
    Ok(Datum::Int(i64::from(compat)))
}

/// Evaluates the ETString argument used by the Go IPv4 binary predicates
/// without decoding or replacing arbitrary bytes.  Numeric constants still
/// follow the normal EvalString coercion used by the source signature.
fn eval_string_bytes(value: &Datum) -> Result<Option<Vec<u8>>, EvalError> {
    match value {
        Datum::Null => Ok(None),
        Datum::String(value) => Ok(Some(value.bytes().to_vec())),
        Datum::Bytes(value) => Ok(Some(value.clone())),
        _ => Ok(coerce_str(value)?.map(|text| text.into_bytes())),
    }
}

/// `IS_IPV6(expr)`: true for a parseable IPv6 address, including an IPv4
/// mapped spelling, but false for a pure IPv4 address. Port of
/// `builtinIsIPv6Sig.evalInt` in `pkg/expression/builtin_miscellaneous.go`.
fn is_ipv6_value(value: &Datum) -> Result<Datum, EvalError> {
    let Some(value) = coerce_str(value)? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Int(i64::from(
        Ipv6Addr::from_str(&value).is_ok() && !is_ipv4(&value),
    )))
}

#[cfg(test)]
mod tests {
    use super::dispatch;
    use crate::Datum;
    use crate::Decimal;

    fn call(name: &str, vals: &[Datum]) -> Datum {
        dispatch(name, vals, &crate::NoColumns)
            .expect("name/arity should dispatch to compare2")
            .expect("Go vector should evaluate")
    }

    fn s(value: &str) -> Datum {
        Datum::new_string(value.to_string())
    }

    /// `TestIntervalFunc` vectors that fit the current signed `Datum` domain.
    #[test]
    fn interval_go_vectors() {
        let cases: &[(Vec<Datum>, i64)] = &[
            (vec![Datum::Null, Datum::Int(1), Datum::Int(2)], -1),
            (vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)], 0),
            (vec![Datum::Int(2), Datum::Int(1), Datum::Int(3)], 1),
            (vec![Datum::Int(3), Datum::Int(1), Datum::Int(2)], 2),
            (vec![Datum::Int(0), s("b"), s("1"), s("2")], 1),
            (vec![s("a"), s("b"), s("1"), s("2")], 1),
            (
                vec![
                    Datum::Int(23),
                    Datum::Int(1),
                    Datum::Int(23),
                    Datum::Int(23),
                    Datum::Int(23),
                    Datum::Int(30),
                    Datum::Int(44),
                    Datum::Int(200),
                ],
                4,
            ),
            (
                vec![
                    Datum::Int(23),
                    Datum::Decimal(Decimal::from_literal("1.7")),
                    Datum::Decimal(Decimal::from_literal("15.3")),
                    Datum::Decimal(Decimal::from_literal("23.1")),
                    Datum::Int(30),
                    Datum::Int(44),
                    Datum::Int(200),
                ],
                2,
            ),
            (
                vec![
                    Datum::Int(9_007_199_254_740_992),
                    Datum::Int(9_007_199_254_740_993),
                ],
                0,
            ),
            (
                vec![
                    Datum::UInt(9_223_372_036_854_775_808),
                    Datum::UInt(9_223_372_036_854_775_809),
                ],
                0,
            ),
            (
                vec![Datum::Int(i64::MAX), Datum::UInt(9_223_372_036_854_775_808)],
                0,
            ),
            (
                vec![
                    Datum::Int(-9_223_372_036_854_775_807),
                    Datum::UInt(9_223_372_036_854_775_808),
                ],
                0,
            ),
            (
                vec![Datum::UInt(9_223_372_036_854_775_806), Datum::Int(i64::MAX)],
                0,
            ),
            (
                vec![
                    Datum::UInt(9_223_372_036_854_775_806),
                    Datum::Int(-9_223_372_036_854_775_807),
                ],
                1,
            ),
            (vec![Datum::Int(-1), Datum::Int(2333), Datum::Null], 0),
            (
                vec![Datum::Int(1), Datum::Null, Datum::Null, Datum::Null],
                3,
            ),
            (
                vec![
                    Datum::Int(1),
                    Datum::Null,
                    Datum::Null,
                    Datum::Null,
                    Datum::Int(2),
                ],
                3,
            ),
            (
                vec![Datum::Int(9_007_199_254_740_992), s("9007199254740993")],
                1,
            ),
            (
                vec![s("9007199254740992"), Datum::Int(9_007_199_254_740_993)],
                1,
            ),
            (vec![s("9007199254740992"), s("9007199254740993")], 1),
            // Go's StrToFloat saturates an overflowing real conversion to
            // MAXFLOAT (with truncation ignored by TestIntervalFunc). The
            // old Rust fallback returned zero, placing this target before a
            // 1e308 boundary instead of after it.
            (vec![s("1e999"), Datum::Real(1e308)], 1),
        ];
        for (args, want) in cases {
            assert_eq!(call("INTERVAL", args), Datum::Int(*want));
        }
    }

    /// `TestInetAton` exact valid/NULL vectors. Its malformed-input vectors
    /// assert a strict-context TiDB error, which is still an error here.
    #[test]
    fn inet_aton_go_vectors() {
        let cases = [
            (Datum::Null, Datum::Null),
            (s("255.255.255.255"), Datum::UInt(4_294_967_295)),
            (s("0.0.0.0"), Datum::UInt(0)),
            (s("127.0.0.1"), Datum::UInt(2_130_706_433)),
            (s("113.14.22.3"), Datum::UInt(1_896_748_547)),
            (s("127"), Datum::UInt(127)),
            (s("127.255"), Datum::UInt(2_130_706_687)),
            (s("127.2.1"), Datum::UInt(2_130_837_505)),
        ];
        for (arg, want) in cases {
            assert_eq!(call("INET_ATON", &[arg]), want);
        }
        for invalid in ["", "0.0.0.256", "127,256", "123.2.1.", "127.0.0.1.1"] {
            assert!(dispatch("INET_ATON", &[s(invalid)], &crate::NoColumns)
                .unwrap()
                .is_err());
        }
    }

    /// `TestInetNtoa` vectors, including values outside the IPv4 range.
    #[test]
    fn inet_ntoa_go_vectors() {
        let cases = [
            (
                Datum::Int(167_773_449),
                Datum::new_string("10.0.5.9".to_string()),
            ),
            (
                Datum::Int(2_063_728_641),
                Datum::new_string("123.2.0.1".to_string()),
            ),
            (Datum::Int(0), Datum::new_string("0.0.0.0".to_string())),
            (Datum::Int(545_460_846_593), Datum::Null),
            (Datum::Int(-1), Datum::Null),
            (
                Datum::Int(4_294_967_295),
                Datum::new_string("255.255.255.255".to_string()),
            ),
            (Datum::Null, Datum::Null),
        ];
        for (arg, want) in cases {
            assert_eq!(call("INET_NTOA", &[arg]), want);
        }
    }

    /// `TestInet6AtoN` exact source vectors.  The result is binary even when
    /// the input is ordinary dotted IPv4 text: a plain IPv4 spelling uses
    /// four bytes, while every colon-containing spelling uses sixteen.
    #[test]
    fn inet6_aton_go_vectors() {
        let cases = [
            ("0.0.0.0", Datum::new_bytes([0, 0, 0, 0])),
            ("10.0.5.9", Datum::new_bytes([0x0a, 0, 5, 9])),
            (
                "fdfe::5a55:caff:fefa:9089",
                Datum::new_bytes([
                    0xfd, 0xfe, 0, 0, 0, 0, 0, 0, 0x5a, 0x55, 0xca, 0xff, 0xfe, 0xfa, 0x90, 0x89,
                ]),
            ),
            (
                "::ffff:1.2.3.4",
                Datum::new_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4]),
            ),
            ("", Datum::Null),
            ("Not IP address", Datum::Null),
            ("1.0002.3.4", Datum::Null),
            ("1.2.256", Datum::Null),
            (
                "::ffff:255.255.255.255",
                Datum::new_bytes([
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                ]),
            ),
        ];
        for (text, want) in cases {
            match want {
                Datum::Null => assert!(dispatch("INET6_ATON", &[s(text)], &crate::NoColumns)
                    .unwrap()
                    .is_err()),
                want => assert_eq!(call("INET6_ATON", &[s(text)]), want),
            }
        }
        assert_eq!(call("INET6_ATON", &[Datum::Null]), Datum::Null);
    }

    /// `TestInet6NtoA` exact source vectors, including invalid byte lengths
    /// and the NULL input path.
    #[test]
    fn inet6_ntoa_go_vectors() {
        let cases = [
            (Datum::new_bytes([0, 0, 0, 0]), "0.0.0.0"),
            (Datum::new_bytes([0x0a, 0, 5, 9]), "10.0.5.9"),
            (
                Datum::new_bytes([
                    0xfd, 0xfe, 0, 0, 0, 0, 0, 0, 0x5a, 0x55, 0xca, 0xff, 0xfe, 0xfa, 0x90, 0x89,
                ]),
                "fdfe::5a55:caff:fefa:9089",
            ),
            (
                Datum::new_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4]),
                "::ffff:1.2.3.4",
            ),
            (
                Datum::new_bytes([
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                ]),
                "::ffff:255.255.255.255",
            ),
        ];
        for (value, want) in cases {
            assert_eq!(call("INET6_NTOA", &[value]), s(want));
        }
        for bytes in [Vec::new(), vec![0x0a, 0, 5], vec![0; 15]] {
            assert_eq!(call("INET6_NTOA", &[Datum::new_bytes(bytes)]), Datum::Null);
        }
        assert_eq!(call("INET6_NTOA", &[Datum::Null]), Datum::Null);
    }

    /// `TestIsIPv4` and `TestIsIPv6` vectors, plus their NULL checks.
    #[test]
    fn is_ip_go_vectors() {
        for (ip, want) in [
            ("192.168.1.1", 1),
            ("255.255.255.255", 1),
            ("10.t.255.255", 0),
            ("10.1.2.3.4", 0),
            ("2001:250:207:0:0:eef2::1", 0),
            ("::ffff:1.2.3.4", 0),
            ("1...1", 0),
            ("192.168.1.", 0),
            (".168.1.2", 0),
            ("168.1.2", 0),
            ("1.2.3.4.5", 0),
        ] {
            assert_eq!(call("IS_IPV4", &[s(ip)]), Datum::Int(want));
        }
        assert_eq!(call("IS_IPV4", &[Datum::Null]), Datum::Null);
        for (ip, want) in [
            ("2001:250:207:0:0:eef2::1", 1),
            ("2001:0250:0207:0001:0000:0000:0000:ff02", 1),
            ("2001:250:207::eff2::1，", 0),
            ("192.168.1.1", 0),
            ("::ffff:1.2.3.4", 1),
        ] {
            assert_eq!(call("IS_IPV6", &[s(ip)]), Datum::Int(want));
        }
        assert_eq!(call("IS_IPV6", &[Datum::Null]), Datum::Null);
    }

    /// `TestIsIPv4Mapped` and `TestIsIPv4Compat` operate on raw ETString
    /// bytes, not textual IPv6 spellings.  Keep every source row here,
    /// including the malformed lengths and the SQL NULL path.
    #[test]
    fn is_ipv4_binary_predicate_go_vectors() {
        let mapped_cases = [
            (vec![], 0),
            (vec![0x10, 0x10, 0x10, 0x10], 0),
            (
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4],
                1,
            ),
            (
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xff, 0xff, 1, 2, 3, 4],
                0,
            ),
            (vec![0, 1, 2, 3, 4, 5, 6], 0),
            // Go's EvalString preserves arbitrary bytes; invalid UTF-8 is
            // still just a non-matching binary payload.
            (vec![0xff; 16], 0),
        ];
        for (bytes, want) in mapped_cases {
            assert_eq!(
                call("IS_IPV4_MAPPED", &[Datum::new_bytes(bytes)]),
                Datum::Int(want)
            );
        }
        assert_eq!(call("IS_IPV4_MAPPED", &[Datum::Null]), Datum::Null);

        let compat_cases = [
            (vec![], 0),
            (vec![0x10, 0x10, 0x10, 0x10], 0),
            (vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4], 1),
            (vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 2, 3, 4], 0),
            (
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xff, 0xff, 1, 2, 3, 4],
                0,
            ),
            (vec![0, 1, 2, 3, 4, 5, 6], 0),
            (vec![0xff; 16], 0),
        ];
        for (bytes, want) in compat_cases {
            assert_eq!(
                call("IS_IPV4_COMPAT", &[Datum::new_bytes(bytes)]),
                Datum::Int(want)
            );
        }
        assert_eq!(call("IS_IPV4_COMPAT", &[Datum::Null]), Datum::Null);
    }

    /// `builtin*IsNullSig.evalInt` in `builtin_op.go` has the same result
    /// for every represented eval type.
    #[test]
    fn isnull_values() {
        for value in [
            Datum::Int(0),
            s(""),
            Datum::Decimal(Decimal::from_literal("0.0")),
            Datum::Real(0.0),
        ] {
            assert_eq!(call("ISNULL", &[value]), Datum::Int(0));
        }
        assert_eq!(call("ISNULL", &[Datum::Null]), Datum::Int(1));
    }

    /// LEAST/GREATEST print the SIGNATURE's scale, not the winner's own.
    ///
    /// Go aggregates the arguments' FieldTypes into one return type whose
    /// `Decimal` is the max over them and casts every argument to it, so an
    /// INTEGER argument that wins the comparison still prints a fraction.
    /// Every expectation is a verbatim capture from real TiDB.
    #[test]
    fn extremum_decimal_carries_the_aggregated_scale() {
        let d = |text: &str| Datum::Decimal(Decimal::from_literal(text));
        let text = |value: Datum| value.sql_string().expect("a decimal renders");
        for (name, args, expected) in [
            // The winner is the INT and still carries the fraction.
            ("LEAST", vec![Datum::Int(1), d("2.5")], "1.0"),
            ("LEAST", vec![d("2.5"), Datum::Int(1)], "1.0"),
            ("LEAST", vec![Datum::Int(1), d("2.555")], "1.000"),
            ("LEAST", vec![Datum::Int(1), d("2.50")], "1.00"),
            ("LEAST", vec![Datum::Int(-1), d("2.5")], "-1.0"),
            ("GREATEST", vec![Datum::Int(3), d("2.5")], "3.0"),
            ("GREATEST", vec![Datum::Int(1), d("2.0")], "2.0"),
            // The MAX scale over ALL arguments, not the winner's.
            ("LEAST", vec![Datum::Int(1), d("2.5"), d("3.25")], "1.00"),
            (
                "GREATEST",
                vec![Datum::Int(3), d("2.55"), Datum::Int(1)],
                "3.00",
            ),
            ("GREATEST", vec![d("2.5"), d("1.234")], "2.500"),
            ("LEAST", vec![d("2.5"), d("1.234")], "1.234"),
            ("GREATEST", vec![d("2.5"), d("1.2")], "2.5"),
            // No decimal argument at all keeps the integer domain.
            ("LEAST", vec![Datum::Int(1), Datum::Int(2)], "1"),
            // A signed/unsigned mix promotes to DECIMAL but to scale 0,
            // because neither argument carries a fraction.
            ("LEAST", vec![Datum::Int(1), Datum::UInt(2)], "1"),
            ("GREATEST", vec![Datum::Int(1), Datum::UInt(2)], "2"),
        ] {
            assert_eq!(text(call(name, &args)), expected, "{name}({args:?})");
        }
    }
}
