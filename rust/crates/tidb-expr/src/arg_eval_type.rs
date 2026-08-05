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
//! ONLY the `types.ETDatetime` column of the switch above is built here.
//! The other seven cast kinds are the remaining rungs of the same structural
//! change and are deliberately absent -- an argument at a position this
//! module does not name is passed through byte-for-byte, exactly as before.

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
    let mask = datetime_arg_mask(name);
    if mask == 0 {
        return Ok(vals);
    }
    let mut vals = vals;
    for (index, value) in vals.iter_mut().enumerate() {
        if mask & (1 << index) == 0 {
            continue;
        }
        let source = arg_types.get(index).and_then(Option::as_ref);
        *value = crate::cast::cast_arg_as_datetime(value, source, ctx)?;
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
