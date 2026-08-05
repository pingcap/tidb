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

//! Go `InferType4ControlFuncs` (`pkg/expression/builtin_control.go`) -- the
//! RESULT half of `newBaseBuiltinFuncWithTp` for the control family.
//!
//! Go runs ONE inference for `CASE WHEN`, `IF`, `IFNULL`, `NULLIF`,
//! `COALESCE`, `LEAD` and `LAG`, and the eval type it returns is what SELECTS
//! the signature -- `case types.ETString: sig = &builtinIfStringSig{bf}` and
//! the same shape for Int/Real/Decimal/Time/Duration/JSON. Getting the type
//! wrong therefore gets the VALUE wrong, not merely the printed width:
//! `IFNULL("aaaa", bigint_col)` is a STRING signature answering `aaaa`, and an
//! Int signature there answers `0`.
//!
//! Because Go shares one function, so does this module. `IF` passes its two
//! result branches, `CASE WHEN` its THEN/ELSE branches, and `IFNULL`/
//! `COALESCE` all of their arguments -- exactly the slices Go's four
//! `getFunction`s pass (`builtin_control.go:325` `thenArgs...`,
//! `:449` `args[1], args[2]`, `:520` `args[0], args[1]`, and
//! `builtin_compare.go:130` `args...`).
//!
//! # What is deliberately NOT here
//!
//! Go's `addCollateAndCharsetAndFlagFromArgs` runs inside
//! `InferType4ControlFuncs`, between `setDecimalFromArgs` and
//! `setFlenFromArgs`. In this crate the charset/collation half of every
//! builtin is one bottom-up walk instead
//! ([`super::derive_tree_collation`] -> `collation_derive::derive_collation`,
//! whose `if`/`ifnull`/`coalesce` arms are Go's own
//! `CheckAndDeriveCollationFromExprs` calls), so re-deriving it here would be
//! a second copy of a rule that already has an owner. This module owns the
//! CODE, the FLAGS, the SCALE and the WIDTH; that walk owns the charset and
//! the collation.

use tidb_datatype::{
    agg_field_type, aggregate_eval_type, EvalType, FieldType, FieldTypeCode, FieldTypeFlags,
    UNSPECIFIED_LENGTH,
};

use crate::expression::Expression;

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

/// Go `setDecimalFromArgs` then `setFlenFromArgs`, which `AggFieldType` does
/// NOT do: the merge carries the FIRST argument's flen/decimal, so every
/// caller of `InferType4ControlFuncs`'s merge has to re-derive both from all
/// the arguments.
///
/// The `eval_type` is Go's `types.AggregateEvalType` answer, NOT
/// `resultFieldType.EvalType()`. Go passes the former to both setters
/// (`builtin_control.go:271-278`) and only reads the latter afterwards, for
/// the scale fixups.
pub fn set_len_from_args(result: &mut FieldType, eval_type: EvalType, args: &[&FieldType]) {
    // setDecimalFromArgs: ETInt has no scale; otherwise the widest argument
    // scale, or unspecified as soon as one argument's is unspecified.
    if eval_type == EvalType::Int {
        result.set_decimal(0);
    } else {
        let mut max_decimal = 0;
        let mut unspecified = false;
        for arg in args {
            if arg.decimal() == UNSPECIFIED_LENGTH {
                unspecified = true;
                break;
            }
            max_decimal = max_decimal.max(arg.decimal());
        }
        if unspecified {
            result.set_decimal(UNSPECIFIED_LENGTH);
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
            let sign_len = i64::from(!arg.is_unsigned());
            let mut flen = arg.flen() - sign_len;
            if arg.decimal() != UNSPECIFIED_LENGTH {
                flen -= arg.decimal();
            }
            max_arg_flen = maxlen(max_arg_flen, flen);
        }
        result.set_flen_under_limit(max_arg_flen + result.decimal() + 1);
    } else if eval_type == EvalType::String {
        // The ETString arm. An INTEGER branch inside a string-typed control
        // function is measured by the width its DECLARED type can print, not
        // by the display width it happens to carry: `IFNULL(varchar_col,
        // bigint_col)` is 20 wide however the column was declared, which is
        // what stops the integer branch from being truncated when the string
        // signature renders it.
        let mut max_len = 0;
        for arg in args {
            let fixed = match arg.code() {
                FieldTypeCode::Tiny => Some(4),
                FieldTypeCode::Short => Some(6),
                FieldTypeCode::Int24 => Some(9),
                FieldTypeCode::Long => Some(11),
                FieldTypeCode::LongLong => Some(20),
                _ => None,
            };
            match fixed {
                Some(width) => max_len = maxlen(width, max_len),
                None => {
                    // Go RETURNS here, leaving the whole result unsized --
                    // one branch of unknown width makes the result's width
                    // unknown, rather than silently sizing it to the others.
                    if arg.flen() == UNSPECIFIED_LENGTH {
                        result.set_flen(UNSPECIFIED_LENGTH);
                        return;
                    }
                    max_len = maxlen(arg.flen(), max_len);
                }
            }
        }
        result.set_flen(max_len);
    } else {
        // The trailing `else` arm: the widest argument flen as-is.
        let mut max_len = 0;
        for arg in args {
            max_len = max_len.max(arg.flen());
        }
        result.set_flen(max_len);
    }
}

/// [`set_len_from_args`] for a caller that has only the merged type to read
/// an eval type from.
///
/// `tidb_executor::window`'s `LAG`/`LEAD` inference is the one such caller:
/// it reaches Go's merge through `typeInfer4LeadLag` and has no
/// `AggregateEvalType` answer of its own to pass.
pub fn set_numeric_len_from_args(result: &mut FieldType, args: &[&FieldType]) {
    let eval_type = result.eval_type();
    set_len_from_args(result, eval_type, args);
}

/// Go `types.SetBinChsClnFlag`.
fn set_bin_chs_cln_flag(ft: &mut FieldType) {
    ft.set_charset_name("binary");
    ft.set_collation_name("binary");
    ft.add_flags(FieldTypeFlags::BINARY);
}

/// Go `types.TryToFixFlenOfDatetime` (`pkg/types/field_type.go`): a DATETIME's
/// width is its canonical one, never an argument's.
fn try_to_fix_flen_of_datetime(ft: &mut FieldType) {
    /// Go `mysql.MaxDatetimeWidthNoFsp`.
    const MAX_DATETIME_WIDTH_NO_FSP: i64 = 19;
    if ft.code() == FieldTypeCode::Datetime {
        let decimal = ft.decimal();
        ft.set_flen(MAX_DATETIME_WIDTH_NO_FSP + if decimal > 0 { decimal + 1 } else { 0 });
    }
}

/// The NOT NULL / BINARY tail each control `getFunction` runs on the type
/// `InferType4ControlFuncs` just handed it.
///
/// Go does NOT share this part, and each of the four spells out two lines of
/// its own -- so this is a transcription of four two-line tails, not a second
/// inference:
///
/// * `caseWhenFunctionClass` turns NOT NULL OFF unconditionally, "because if
///   all when-clauses are false, the result of case-when expr is NULL"
///   (`builtin_control.go:330-333`).
/// * `ifNullFunctionClass` puts it back when EITHER argument is NOT NULL
///   (`:526`), which is why `IFNULL(1, NULL)` is a NOT NULL column even
///   though a NULL branch just cleared the flag.
/// * `coalesceFunctionClass` does the same over ALL arguments
///   (`builtin_compare.go:124-127, 136`).
/// * `ifFunctionClass` adds `bf.tp`'s flag (`builtin_control.go:465`), and
///   `newReturnFieldTypeForBaseBuiltinFunc` gives every non-string eval type
///   `mysql.BinaryFlag` and nothing else -- no NOT NULL at all.
fn add_result_flags(name: &str, result: &mut FieldType, args: &[&FieldType]) {
    let any_not_null = args.iter().any(|ft| ft.has_flag(FieldTypeFlags::NOT_NULL));
    match name {
        "case_when" => result.del_flags(FieldTypeFlags::NOT_NULL),
        "ifnull" | "coalesce" if any_not_null => result.add_flags(FieldTypeFlags::NOT_NULL),
        "if" if result.eval_type() != EvalType::String => {
            result.add_flags(FieldTypeFlags::BINARY);
        }
        _ => {}
    }
}

/// Go `InferType4ControlFuncs`, verbatim, plus [`add_result_flags`]'s
/// per-name tail.
///
/// `None` only when an argument carries no static type at all -- Go always
/// has one, so that is this tier's "I could not type the arguments", never a
/// judgement that Go would refuse the call.
pub fn infer_type4_control_funcs(name: &str, args: &[Expression]) -> Option<FieldType> {
    let all: Vec<&FieldType> = args
        .iter()
        .map(Expression::static_type)
        .collect::<Option<Vec<_>>>()?;
    // Go panics on an empty list; every caller here passes at least one
    // branch, and a `CASE` with none does not parse.
    let (null_fields, not_null_fields): (Vec<&FieldType>, Vec<&FieldType>) =
        all.iter().partition(|ft| ft.code() == FieldTypeCode::Null);

    // Every argument is TypeNull: the result is a NULL column, and Go stops
    // here without any of the fixups below.
    let Some(&first_not_null) = not_null_fields.first() else {
        let mut result = (*null_fields.first()?).clone();
        result.del_flags(FieldTypeFlags::NOT_NULL);
        result.set_code(FieldTypeCode::Null);
        result.set_flen(0);
        result.set_decimal(0);
        set_bin_chs_cln_flag(&mut result);
        add_result_flags(name, &mut result, &all);
        return Some(result);
    };

    let mut result = if not_null_fields.len() == 1 {
        // `*resultFieldType = *notNullFields[0]`: one typed branch is copied
        // WHOLE, so `IFNULL(NULL, decimal_col)` keeps that column's own
        // precision instead of a re-derived one.
        first_not_null.clone()
    } else {
        let owned: Vec<FieldType> = not_null_fields.iter().map(|ft| (*ft).clone()).collect();
        let mut merged = agg_field_type(&owned);
        // Go declares `var tempFlag uint` -- ZERO -- lets `AggregateEvalType`
        // write UNSIGNED and BINARY into it, and then `SetFlag(tempFlag)`,
        // which REPLACES the mask. So the NOT NULL that `AggFieldType`'s
        // `mergeTypeFlag` carried through is dropped for a multi-branch
        // result and only the two flags that merge survive.
        let mut flags = 0_u32;
        let eval_type = aggregate_eval_type(&owned, &mut flags);
        merged.set_flags(flags);
        set_len_from_args(&mut merged, eval_type, &not_null_fields);
        merged
    };

    // A NULL branch anywhere means the result can be NULL.
    if !null_fields.is_empty() {
        result.del_flags(FieldTypeFlags::NOT_NULL);
    }

    let result_eval = result.eval_type();
    if result_eval == EvalType::Int {
        result.set_decimal(0);
    } else if result_eval == EvalType::String {
        result.set_decimal(UNSPECIFIED_LENGTH);
    }
    // An ENUM/SET result is not one: the signature that runs is the Int or
    // String one, so the type reported is the one that signature returns.
    //
    // This is reachable ONLY through the single-typed-branch path above --
    // `IF(c, NULL, enum_col)`. A PAIR of enums never gets here, because
    // `fieldTypeMergeRules[TypeEnum][TypeEnum]` is already `TypeVarchar`
    // (`types/field_type.go`'s table, row 19 column 19), so `AggFieldType`
    // has done the rewrite before this line runs.
    if matches!(result.code(), FieldTypeCode::Enum | FieldTypeCode::Set) {
        match result_eval {
            EvalType::Int => result.set_code(FieldTypeCode::LongLong),
            EvalType::String => result.set_code(FieldTypeCode::Varchar),
            _ => {}
        }
    }
    try_to_fix_flen_of_datetime(&mut result);
    add_result_flags(name, &mut result, &all);
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constant::Constant;
    use tidb_datatype::Datum;

    fn arg(code: FieldTypeCode, flen: i64, decimal: i64) -> Expression {
        let mut ft = FieldType::new(code);
        ft.set_flen(flen);
        ft.set_decimal(decimal);
        Expression::Constant(Constant::new(Datum::Null, ft))
    }

    fn typed(code: FieldTypeCode) -> Expression {
        arg(code, UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH)
    }

    /// The measured Rank-A case: a string branch beside an integer one is a
    /// STRING result, not an integer one. Go, via `goeval`:
    /// `IF(1,1,'a')` -> `STR:1`.
    #[test]
    fn an_integer_branch_beside_a_string_one_is_a_string_result() {
        let ft = infer_type4_control_funcs(
            "ifnull",
            &[
                arg(FieldTypeCode::LongLong, 1, 0),
                arg(FieldTypeCode::VarString, 1, UNSPECIFIED_LENGTH),
            ],
        )
        .expect("both branches are typed");
        assert_eq!(ft.eval_type(), EvalType::String);
        // The ETString arm of `setFlenFromArgs`: the LONGLONG branch is 20
        // wide whatever display width it declared.
        assert_eq!(ft.flen(), 20);
        assert_eq!(ft.decimal(), UNSPECIFIED_LENGTH);
    }

    /// Go's `len(notNullFields) == 1` shortcut copies the ONE typed branch
    /// whole -- no re-derivation at all.
    ///
    /// The width is the boundary that shows it: an UNSIGNED `int(10)` spends
    /// no digit on a sign, so running the merge over a one-element list
    /// instead would put one back (`setFlenFromArgs` re-adds a sign digit
    /// unconditionally) and report `int(11)`. A SIGNED branch, or a DECIMAL
    /// one, round-trips through the merge unchanged and would agree either
    /// way -- which is why this case is unsigned.
    #[test]
    fn one_typed_branch_beside_nulls_is_that_branch_exactly() {
        let mut unsigned = FieldType::new(FieldTypeCode::Long);
        unsigned.set_flen(10);
        unsigned.set_decimal(0);
        unsigned.add_flags(FieldTypeFlags::UNSIGNED);
        let ft = infer_type4_control_funcs(
            "ifnull",
            &[
                typed(FieldTypeCode::Null),
                Expression::Constant(Constant::new(Datum::Null, unsigned)),
            ],
        )
        .expect("typed");
        assert_eq!(ft.code(), FieldTypeCode::Long);
        assert_eq!((ft.flen(), ft.decimal()), (10, 0));
    }

    /// The ENUM/SET rewrite's ONLY reachable path: one enum branch beside a
    /// NULL one, where `AggFieldType` never runs. Recorded witness:
    /// `tests/integrationtest/r/expression/misc.result:911` runs
    /// `select if(A, null,b)=1 from t` over `b enum("b")`.
    #[test]
    fn a_lone_enum_branch_reports_the_varchar_its_signature_returns() {
        let ft = infer_type4_control_funcs(
            "if",
            &[typed(FieldTypeCode::Null), arg(FieldTypeCode::Enum, 1, 0)],
        )
        .expect("typed");
        assert_eq!(ft.code(), FieldTypeCode::Varchar);
    }

    /// `IF` never re-adds NOT NULL: `newReturnFieldTypeForBaseBuiltinFunc`
    /// gives `bf.tp` only `mysql.BinaryFlag`, so two NOT NULL branches still
    /// make a nullable result -- if all when-clauses are false there is
    /// nothing to return.
    ///
    /// This is also the boundary for Go's `var tempFlag uint`: starting that
    /// mask at the MERGED flags instead of at zero would let `AggFieldType`'s
    /// `mergeTypeFlag` carry NOT NULL through, and `IF` has no tail to undo
    /// it. `IFNULL` and `COALESCE` would both hide the change, because their
    /// own tails put NOT NULL back.
    #[test]
    fn two_not_null_branches_do_not_make_an_if_not_null() {
        let not_null = |code| {
            let mut ft = FieldType::new(code);
            ft.set_flen(1);
            ft.set_decimal(0);
            ft.add_flags(FieldTypeFlags::NOT_NULL);
            Expression::Constant(Constant::new(Datum::Null, ft))
        };
        let branches = [
            not_null(FieldTypeCode::LongLong),
            not_null(FieldTypeCode::LongLong),
        ];
        let ft = infer_type4_control_funcs("if", &branches).expect("typed");
        assert!(!ft.has_flag(FieldTypeFlags::NOT_NULL));
        // The same two branches under IFNULL DO carry it (`builtin_control.go`
        // `:526`), which is what makes `IFNULL(1, NULL)` a NOT NULL column.
        let ft = infer_type4_control_funcs("ifnull", &branches).expect("typed");
        assert!(ft.has_flag(FieldTypeFlags::NOT_NULL));
        let with_null = [
            not_null(FieldTypeCode::LongLong),
            typed(FieldTypeCode::Null),
        ];
        let ft = infer_type4_control_funcs("ifnull", &with_null).expect("typed");
        assert!(ft.has_flag(FieldTypeFlags::NOT_NULL));
        // CASE WHEN clears it unconditionally (`:330-333`).
        let ft = infer_type4_control_funcs("case_when", &branches).expect("typed");
        assert!(!ft.has_flag(FieldTypeFlags::NOT_NULL));
    }

    /// `setFlenFromArgs` branches on `AggregateEvalType`'s answer, NOT on the
    /// merged FIELD type's eval type, and the two genuinely differ: every
    /// TEMPORAL eval type is `IsStringKind`, so a DURATION pair merges to a
    /// DURATION field type whose `AggregateEvalType` is `ETString`.
    ///
    /// The ETString arm gives up on the width as soon as one branch has none;
    /// the trailing arm this would otherwise take just maxes the widths and
    /// reports the other branch's. Reading the eval type off the merged type
    /// would agree on every NUMERIC pair, which is why the boundary is
    /// temporal.
    #[test]
    fn the_width_arm_is_chosen_by_the_aggregate_eval_type() {
        let ft = infer_type4_control_funcs(
            "ifnull",
            &[
                arg(FieldTypeCode::Duration, 10, 0),
                arg(FieldTypeCode::Duration, UNSPECIFIED_LENGTH, 0),
            ],
        )
        .expect("typed");
        assert_eq!(ft.code(), FieldTypeCode::Duration);
        assert_eq!(ft.flen(), UNSPECIFIED_LENGTH);
    }

    /// Every branch NULL: a NULL column, width and scale zeroed.
    #[test]
    fn every_branch_null_is_a_null_column() {
        let ft = infer_type4_control_funcs(
            "ifnull",
            &[typed(FieldTypeCode::Null), typed(FieldTypeCode::Null)],
        )
        .expect("typed");
        assert_eq!(ft.code(), FieldTypeCode::Null);
        assert_eq!((ft.flen(), ft.decimal()), (0, 0));
        assert_eq!(ft.charset_name(), "binary");
    }

    /// An ENUM pair merges to a STRING eval type, and Go then REWRITES the
    /// reported type to VARCHAR because `builtinIfStringSig` is what runs.
    #[test]
    fn an_enum_pair_reports_the_varchar_its_signature_returns() {
        let ft = infer_type4_control_funcs(
            "ifnull",
            &[
                arg(FieldTypeCode::Enum, 1, 0),
                arg(FieldTypeCode::Enum, 1, 0),
            ],
        )
        .expect("typed");
        assert_eq!(ft.code(), FieldTypeCode::Varchar);
        assert_eq!(ft.decimal(), UNSPECIFIED_LENGTH);
    }

    /// `setDecimalFromArgs` widens the scale to the widest branch, which is
    /// what makes `IFNULL(1, 1.5)` print `1.0` and not `1`. Go, via `gorun`:
    /// `RS:1|1.0|1.0|1|1|1`.
    #[test]
    fn a_decimal_branch_widens_the_integer_branchs_scale() {
        let ft = infer_type4_control_funcs(
            "ifnull",
            &[
                arg(FieldTypeCode::LongLong, 1, 0),
                arg(FieldTypeCode::NewDecimal, 2, 1),
            ],
        )
        .expect("typed");
        assert_eq!(ft.code(), FieldTypeCode::NewDecimal);
        // `setFlenFromArgs`' decimal arm: each branch's INTEGRAL width is its
        // flen less one sign digit less its own scale, so both branches here
        // contribute 0, and the merged scale plus one sign digit are added
        // back -- `0 + 1 + 1`.
        assert_eq!((ft.flen(), ft.decimal()), (2, 1));
    }

    /// A DATETIME result takes its canonical width, never a branch's.
    #[test]
    fn a_datetime_result_takes_its_canonical_width() {
        let ft = infer_type4_control_funcs(
            "ifnull",
            &[
                arg(FieldTypeCode::Datetime, 3, 0),
                arg(FieldTypeCode::Datetime, 3, 0),
            ],
        )
        .expect("typed");
        assert_eq!((ft.code(), ft.flen()), (FieldTypeCode::Datetime, 19));
    }
}
