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

//! The ARGUMENT half of Go's
//! `newBaseBuiltinFuncWithTp(ctx, funcName, args, retType, argTps...)`
//! (`pkg/expression/builtin.go`).
//!
//! Go declares each builtin's argument eval-types alongside its return type,
//! and `newBaseBuiltinFuncWithTp` then REWRITES the argument list at BUILD
//! time, wrapping every argument in the cast its declared eval type asks for:
//!
//! ```go
//! switch argTps[i] {
//! case types.ETInt:      args[i] = WrapWithCastAsInt(ctx, args[i])
//! case types.ETReal:     args[i] = WrapWithCastAsReal(ctx, args[i])
//! case types.ETDecimal:  args[i] = WrapWithCastAsDecimal(ctx, args[i])
//! case types.ETString:   args[i] = WrapWithCastAsString(ctx, args[i])
//! case types.ETDatetime: args[i] = WrapWithCastAsTime(ctx, args[i], types.NewFieldType(mysql.TypeDatetime))
//! case types.ETTimestamp:args[i] = WrapWithCastAsTime(ctx, args[i], types.NewFieldType(mysql.TypeTimestamp))
//! case types.ETDuration: args[i] = WrapWithCastAsDuration(ctx, args[i])
//! case types.ETJson:     args[i] = WrapWithCastAsJSON(ctx, args[i])
//! }
//! ```
//!
//! Every `evalInt`/`evalString`/`evalTime` body downstream therefore reads an
//! argument that is ALREADY of its declared type -- which is why Go's
//! `builtinMonthSig.evalInt` is three lines with no parsing in it at all.
//!
//! This tier has no build-time expression rewrite, so each builtin used to
//! re-derive the type from the runtime [`Datum`] instead, and every one of
//! them re-derived it slightly differently. This module is that missing
//! layer, moved to the one point in each evaluator where the arguments have
//! just been evaluated: the values, not the expressions, carry the cast.
//!
//! # Scope
//!
//! The `types.ETDatetime` and `types.ETInt` columns of the switch above are
//! built here. The other six cast kinds are the remaining rungs of the same
//! structural change and are deliberately absent -- an argument at a position
//! this module does not name is passed through byte-for-byte, exactly as
//! before.
//!
//! A name whose `argTps` entry is chosen by the ARGUMENT'S OWN TYPE is not a
//! member of this layer even when the entry it chooses is one of those two,
//! because what varies then is the SIGNATURE, not the cast. `OCT` is the
//! measured example -- see [`int_arg_mask`]'s doc.

use crate::{Columns, Datum, EvalError};
use tidb_datatype::FieldType;

/// A bitmask over 0-based argument positions.
type ArgMask = u32;

/// The argument positions at which Go declares `types.ETDatetime`, for the
/// builtins routed through this layer. `0` for every other name.
///
/// Each arm quotes the `newBaseBuiltinFuncWithTp` call it transcribes, from
/// `pkg/expression/builtin_time.go` at the line given. The `retType` is the
/// first type argument and is NOT part of this mask -- only the `argTps...`
/// tail is.
///
/// # Deferred, with the reason
///
/// Go declares `types.ETDatetime` arguments for roughly two dozen more
/// builtins (`builtin_time.go:279`, `:388`, `:898`, `:1161`, `:1209`,
/// `:1325`, `:1370`, `:1525`, `:1571`, `:2013`, `:2246`, `:2496`, `:2787`,
/// `:5523`, `:6781`, `:6923`, `:7081`, and `builtin_other.go:1456`, plus the
/// `argTps...` forms at `:2164`, `:2542`, `:2640` and `:4597` that BUILD
/// their type list at run time). They are not listed here because this is
/// the first rung: the layer is proved on the two measured classes before it
/// is pointed at the rest. Adding a name here is the whole cost of routing
/// one -- that is the point of the design.
///
/// `TIMESTAMP` is a deliberate NON-member even though its first argument is
/// temporal: Go declares it `types.ETString` and then selects between two
/// PARSERS from the argument's type,
/// `switch args[0].GetType(ctx.GetEvalCtx()).GetType() { case mysql.TypeFloat,
/// mysql.TypeDouble, mysql.TypeNewDecimal, mysql.TypeLonglong: isFloat = true }`
/// (`builtin_time.go:4592-4595`), storing the answer in the SIGNATURE
/// (`builtinTimestamp1ArgSig{bf, isFloat}`). That is signature-selection
/// state, not an argument cast, and it belongs to a different rung.
const fn datetime_arg_mask(name: &str) -> ArgMask {
    match name.as_bytes() {
        // `:1116` `types.ETInt, types.ETDatetime` (monthFunctionClass).
        b"MONTH" => 1 << 0,
        // `:1284` `types.ETInt, types.ETDatetime` (dayOfMonthFunctionClass).
        b"DAY" | b"DAYOFMONTH" => 1 << 0,
        // `:5833` `types.ETInt, types.ETDatetime` (quarterFunctionClass).
        b"QUARTER" => 1 << 0,
        // `:1620` `types.ETInt, types.ETDatetime` (yearFunctionClass).
        b"YEAR" => 1 << 0,
        // `:832` `types.ETString, types.ETDatetime, types.ETString`
        // (dateFormatFunctionClass).
        b"DATE_FORMAT" => 1 << 0,
        // `:6733` `types.ETInt, types.ETDatetime` (toDaysFunctionClass).
        b"TO_DAYS" => 1 << 0,
        // `:4310` `types.ETInt, types.ETString, types.ETDatetime,
        // types.ETDatetime` (timestampDiffFunctionClass) -- the UNIT is
        // argument 0 and stays a string.
        b"TIMESTAMPDIFF" => (1 << 1) | (1 << 2),
        // `:6551` `types.ETString, types.ETString, types.ETReal,
        // types.ETDatetime` (timestampAddFunctionClass).
        b"TIMESTAMPADD" => 1 << 2,
        // `:5424` `types.ETDatetime, types.ETDatetime, types.ETString,
        // types.ETString` (convertTzFunctionClass) -- the leading
        // `types.ETDatetime` is the RETURN type; argument 0 is the second.
        b"CONVERT_TZ" => 1 << 0,
        _ => 0,
    }
}

/// Applies Go's build-time `WrapWithCastAsTime` to every argument whose
/// declared eval type is `types.ETDatetime`, returning the argument values
/// each signature body is entitled to assume it received.
///
/// `arg_types` are the arguments' static [`FieldType`]s, positionally; a
/// missing or `None` entry is an evaluator tier that does not have them (the
/// row/AST path), which costs only Go's `YEAR` distinction -- see
/// [`crate::cast::cast_arg_as_datetime`].
pub(crate) fn wrap_datetime_args(
    name: &str,
    vals: Vec<Datum>,
    arg_types: &[Option<FieldType>],
    ctx: &dyn Columns,
) -> Result<Vec<Datum>, EvalError> {
    wrap(
        datetime_arg_mask(name),
        vals,
        arg_types,
        ctx,
        crate::cast::cast_arg_as_datetime,
    )
}

/// The argument positions at which Go declares `types.ETInt`, for the builtins
/// routed through this layer. `0` for every other name.
///
/// A bit set beyond a call's actual arity costs nothing -- [`wrap`] walks the
/// VALUES -- so the two builtins whose `types.ETInt` entry is appended only at
/// the wider arity need no arity test here.
///
/// # `OCT` is NOT a member, and that is the shape of the exception
///
/// `octFunctionClass.getFunction` (`builtin_string.go:3005-3006`) reads
///
/// ```go
/// if IsBinaryLiteral(args[0]) || args[0].GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
///     bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETInt)
///     ... sig = &builtinOctIntSig{bf}
/// } else {
///     bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
///     ... sig = &builtinOctStringSig{bf}
/// }
/// ```
///
/// The `types.ETInt` entry exists only on the branch whose own condition
/// already guarantees `WrapWithCastAsInt` is a NO-OP: `EvalType() == ETInt`
/// takes that function's early return, and a binary literal reaches
/// `builtinCastIntAsIntSig` through the same `Hybrid() || IsBinaryLiteral`
/// short-circuit either way. What the condition really selects is the
/// SIGNATURE, and the two signatures disagree about the same value: captured
/// from real TiDB (`gorun`) over an `enum('x','y','z')` holding `'y'`,
/// `oct(e)` is `0` -- `builtinOctStringSig` parsing the text `y` -- while
/// `make_set(e,'p','q','r')` is `q`, the ordinal `2`. Routing `OCT` through
/// this mask would give it the ordinal and break the first of those.
/// `OCT`'s integer-vs-string split is therefore owned by `string_fn::oct`,
/// where the rest of Go's signature selection already lives.
pub(crate) const fn int_arg_mask(name: &str) -> ArgMask {
    match name.as_bytes() {
        // `builtin_math.go:276-279` `argTps := []types.EvalType{argTp}` then
        // `if len(args) > 1 { argTps = append(argTps, types.ETInt) }`
        // (roundFunctionClass) -- argument 0's type is DERIVED from the
        // argument, only the scale is declared.
        b"ROUND" => 1 << 1,
        // `builtin_math.go:2041` `newBaseBuiltinFuncWithTp(ctx, c.funcName,
        // args, argTp, argTp, types.ETInt)` (truncateFunctionClass) -- same
        // shape, and `TRUNCATE` has no one-argument form.
        b"TRUNCATE" => 1 << 1,
        // `builtin_string.go:3924` `types.ETString, types.ETString,
        // types.ETInt, types.ETInt, types.ETString` (insertFunctionClass) --
        // the leading `types.ETString` is the RETURN type, so `pos` and `len`
        // are arguments 1 and 2.
        b"INSERT" => (1 << 1) | (1 << 2),
        // `builtin_string.go:2938-2939` `argTps[0] = types.ETInt` then
        // `for i := 1; i < length; i++ { argTps[i] = types.ETString }`
        // (makeSetFunctionClass).
        b"MAKE_SET" => 1 << 0,
        // `builtin_string.go:1503-1506` `argTps := []types.EvalType{
        // types.ETString, types.ETString}` then `if hasStartPos { argTps =
        // append(argTps, types.ETInt) }` (locateFunctionClass) -- the
        // three-argument form's start position.
        b"LOCATE" => 1 << 2,
        _ => 0,
    }
}

/// Applies Go's build-time `WrapWithCastAsInt` to every argument whose
/// declared eval type is `types.ETInt`. See [`wrap_datetime_args`] for what
/// `arg_types` is worth per tier; for this kind it costs only the UNSIGNED
/// inheritance -- see [`crate::cast::cast_arg_as_int`].
pub(crate) fn wrap_int_args(
    name: &str,
    vals: Vec<Datum>,
    arg_types: &[Option<FieldType>],
    ctx: &dyn Columns,
) -> Result<Vec<Datum>, EvalError> {
    wrap(
        int_arg_mask(name),
        vals,
        arg_types,
        ctx,
        crate::cast::cast_arg_as_int,
    )
}

/// The `EvalInt` a signature body reads out of an argument this layer has
/// already cast: Go's `int64` carrier, whose 64 bits are the same ones an
/// UNSIGNED argument holds (`b.args[i].EvalInt` never re-reads the flag --
/// only `builtinTruncateIntSig` does, and it asks the TYPE). `None` is Go's
/// NULL.
///
/// This is the reader the whole rung buys: once a position is named in
/// [`int_arg_mask`], no signature body has to re-derive an integer from a
/// runtime datum, and every one of them re-derived it slightly differently.
pub(crate) fn eval_int(v: &Datum) -> Result<Option<i64>, EvalError> {
    match v {
        Datum::Null => Ok(None),
        Datum::Int(value) => Ok(Some(*value)),
        Datum::UInt(value) => Ok(Some(*value as i64)),
        // The layer named this position `types.ETInt`, so nothing else can
        // arrive -- unless an evaluator entry point reached the signature
        // without passing through [`wrap_int_args`]. Refuse loudly instead of
        // re-deriving the cast here: an uncovered entry is exactly what this
        // rung exists to make visible, and a silent NULL would hide it.
        _ => Err(EvalError::Unsupported("un-cast types.ETInt argument")),
    }
}

/// The `for i := range args` loop of `newBaseBuiltinFuncWithTp`, with the
/// switch arm passed in. Every cast kind is this loop and a different
/// `cast_arg_as_*`, which is the whole reason the mask is a mask.
fn wrap(
    mask: ArgMask,
    mut vals: Vec<Datum>,
    arg_types: &[Option<FieldType>],
    ctx: &dyn Columns,
    cast: fn(&Datum, Option<&FieldType>, &dyn Columns) -> Result<Datum, EvalError>,
) -> Result<Vec<Datum>, EvalError> {
    if mask == 0 {
        return Ok(vals);
    }
    for (index, value) in vals.iter_mut().enumerate() {
        if mask & (1 << index) == 0 {
            continue;
        }
        *value = cast(value, arg_types.get(index).and_then(Option::as_ref), ctx)?;
    }
    Ok(vals)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NoColumns;
    use tidb_datatype::{FieldTypeBuilder, FieldTypeCode};

    fn year_type() -> Option<FieldType> {
        Some(
            FieldTypeBuilder::new()
                .with_code(FieldTypeCode::Year)
                .build(),
        )
    }

    /// The mask is the `argTps...` TAIL: `CONVERT_TZ`'s leading
    /// `types.ETDatetime` is its RETURN type, and `TIMESTAMPDIFF`'s first
    /// argument is the unit string. Getting either off by one silently
    /// casts the wrong argument.
    #[test]
    fn mask_positions_follow_the_argtps_tail() {
        assert_eq!(datetime_arg_mask("CONVERT_TZ"), 1);
        assert_eq!(datetime_arg_mask("TIMESTAMPDIFF"), 0b110);
        assert_eq!(datetime_arg_mask("TIMESTAMPADD"), 0b100);
        assert_eq!(datetime_arg_mask("MONTH"), 1);
        // Not a member: Go declares `types.ETString` and branches on
        // `isFloat` instead (see the mask's doc).
        assert_eq!(datetime_arg_mask("TIMESTAMP"), 0);
        assert_eq!(datetime_arg_mask("CONCAT"), 0);
    }

    /// The `types.ETInt` mask is the `argTps...` TAIL too, and every one of
    /// these five has its integer position somewhere OTHER than 0 or has a
    /// leading return type in the same declaration. `INSERT`'s `pos`/`len`
    /// are arguments 1 and 2 because the first `types.ETString` in
    /// `types.ETString, types.ETString, types.ETInt, types.ETInt,
    /// types.ETString` is the RETURN type; shifting that mask by one would
    /// cast the SUBJECT string and leave `pos` uncast.
    #[test]
    fn the_int_mask_positions_follow_the_argtps_tail() {
        assert_eq!(int_arg_mask("ROUND"), 0b10);
        assert_eq!(int_arg_mask("TRUNCATE"), 0b10);
        assert_eq!(int_arg_mask("INSERT"), 0b110);
        assert_eq!(int_arg_mask("MAKE_SET"), 0b1);
        assert_eq!(int_arg_mask("LOCATE"), 0b100);
        // Not a member: Go picks the SIGNATURE from the argument's type and
        // the `types.ETInt` entry it then declares is a no-op (see the
        // mask's doc).
        assert_eq!(int_arg_mask("OCT"), 0);
        assert_eq!(int_arg_mask("CONCAT"), 0);
        // The two masks are independent: a name in one is not in the other.
        assert_eq!(datetime_arg_mask("ROUND"), 0);
        assert_eq!(int_arg_mask("MONTH"), 0);
    }

    /// A bit set beyond a call's arity must be inert, because `ROUND` and
    /// `LOCATE` append their `types.ETInt` entry only at the wider arity and
    /// this mask has no arity test. `ROUND(x)` must therefore reach the
    /// signature with its single argument untouched.
    #[test]
    fn a_mask_bit_past_the_arity_casts_nothing() {
        let vals = vec![Datum::new_string("3.7".to_string())];
        let out = wrap_int_args("ROUND", vals.clone(), &[], &NoColumns).unwrap();
        assert_eq!(out, vals);
    }

    /// Position 0 of `ROUND`/`TRUNCATE`/`INSERT`/`LOCATE` is NOT declared
    /// `types.ETInt` -- Go derives `ROUND`'s from the argument and gives the
    /// other two `types.ETString` -- so the value there must survive the
    /// layer verbatim. An off-by-one mask would turn `INSERT`'s subject
    /// string into an integer.
    #[test]
    fn the_uncast_positions_keep_their_own_datum() {
        let subject = Datum::new_string("abcdef".to_string());
        let out = wrap_int_args(
            "INSERT",
            vec![
                subject.clone(),
                Datum::new_string("2".to_string()),
                Datum::new_string("2".to_string()),
                Datum::new_string("X".to_string()),
            ],
            &[None, None, None, None],
            &NoColumns,
        )
        .unwrap();
        assert_eq!(out[0], subject);
        assert_eq!(out[1], Datum::Int(2));
        assert_eq!(out[2], Datum::Int(2));
        assert_eq!(out[3], Datum::new_string("X".to_string()));
    }

    /// Go's UNSIGNED inheritance: `WrapWithCastAsInt`'s `targetType` is `nil`
    /// at every `newBaseBuiltinFuncWithTp` call site, so the built cast is
    /// UNSIGNED exactly when the SOURCE type is -- and that flag is what
    /// `builtinTruncateIntSig` reads back out of `b.args[1].GetType(ctx)`
    /// (`builtin_math.go:2166`) to answer "scale is non-negative". A source
    /// type this tier does not have therefore answers SIGNED.
    #[test]
    fn the_cast_inherits_only_the_source_s_unsigned_flag() {
        let unsigned = Some(
            FieldTypeBuilder::new()
                .with_code(FieldTypeCode::NewDecimal)
                .build()
                .with_unsigned(true),
        );
        let value = Datum::Decimal(tidb_datatype::Decimal::from_literal("2"));
        let out = wrap_int_args("ROUND", vec![Datum::Int(1), value.clone()], &[None, unsigned], &NoColumns)
            .unwrap();
        assert_eq!(out[1], Datum::UInt(2));

        let out =
            wrap_int_args("ROUND", vec![Datum::Int(1), value], &[None, None], &NoColumns).unwrap();
        assert_eq!(out[1], Datum::Int(2));
    }

    /// A name outside the mask must not have its arguments touched at all --
    /// the layer is additive, and a builtin it does not name keeps the datum
    /// it was given.
    #[test]
    fn unlisted_names_pass_arguments_through() {
        let vals = vec![Datum::Int(20_240_315_123_045)];
        let out = wrap_datetime_args("CONCAT", vals.clone(), &[], &NoColumns).unwrap();
        assert_eq!(out, vals);
    }

    /// Go's `ETDatetime` wrap over an INT argument is `ParseTimeFromNum`, so
    /// `20240315123045` reaches the signature as a DATETIME. Confirmed
    /// against real TiDB (`gorun`): `select month(20240315123045)` is `3`.
    #[test]
    fn int_argument_is_parsed_as_a_packed_datetime() {
        let out = wrap_datetime_args(
            "MONTH",
            vec![Datum::Int(20_240_315_123_045)],
            &[None],
            &NoColumns,
        )
        .unwrap();
        let Datum::Time(time) = &out[0] else {
            panic!("expected a temporal value, got {:?}", out[0]);
        };
        assert_eq!(time.core_time().month(), 3);
        assert_eq!(time.core_time().day(), 15);
        assert_eq!(time.core_time().hour(), 12);
    }

    /// Go's YEAR hole: `ParseTimeFromYear(2024)` injects the value as the
    /// year FIELD, giving `2024-00-00`, where `ParseTimeFromNum(2024)` would
    /// fail outright. Confirmed against real TiDB (`gorun`, YEAR column
    /// holding 2024): `month/day/quarter` are `0` and `year` is `2024`.
    ///
    /// This is the ONE thing the static type buys that the datum cannot: an
    /// identical `Datum::Int(2024)` with no type is not a YEAR.
    #[test]
    fn a_year_typed_argument_takes_go_s_year_parser() {
        let out = wrap_datetime_args("MONTH", vec![Datum::Int(2024)], &[year_type()], &NoColumns)
            .unwrap();
        let Datum::Time(time) = &out[0] else {
            panic!("expected a temporal value, got {:?}", out[0]);
        };
        assert_eq!(time.core_time().year(), 2024);
        assert_eq!(time.core_time().month(), 0);
        assert_eq!(time.core_time().day(), 0);

        // The same integer WITHOUT the YEAR type is not a date at all.
        let untyped =
            wrap_datetime_args("MONTH", vec![Datum::Int(2024)], &[None], &NoColumns).unwrap();
        assert_eq!(untyped[0], Datum::Null);
    }
}
