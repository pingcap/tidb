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

//! Tests for [`super`].
//!
//! Ports of the Go tests that cover the ported symbols
//! (`base_func_test.go`'s `TestClone`, `TestBaseFunc_InferAggRetType`,
//! `TestTypeInfer4AvgSum`; `aggregation_test.go`'s `TestAggFuncDesc` and
//! `TestCheckAggPushDownSumInt`), plus written coverage for the type
//! inference of every aggregate kind against every argument family -- the
//! thing that silently changes every downstream plan when it drifts.
//!
//! `pkg/expression/aggregation` has NO `descriptor_test.go`; its descriptor
//! coverage is the two `aggregation_test.go` cases above.

use super::names;
use super::wrap_cast::type_of;
use super::*;
use crate::column::Column;
use crate::constant::Constant;
use crate::context::NoColumns;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use tidb_ast::CiString;
use tidb_datatype::{
    Datum, FieldType, FieldTypeCode, FieldTypeFlags, MAX_DECIMAL_WIDTH, UNSPECIFIED_LENGTH,
};

fn col(unique_id: i64, code: FieldTypeCode) -> Expression {
    Expression::Column(Column::new(unique_id, FieldType::new(code)))
}

fn col_typed(unique_id: i64, ft: FieldType) -> Expression {
    Expression::Column(Column::new(unique_id, ft))
}

fn int_const(value: i64) -> Expression {
    let mut constant = Constant::default();
    constant.value = Datum::new_int(value);
    constant.ret_type = Some(FieldType::new(FieldTypeCode::LongLong));
    Expression::Constant(constant)
}

fn string_const(value: &str) -> Expression {
    let mut constant = Constant::default();
    constant.value = Datum::new_string(value.as_bytes().to_vec());
    let mut ft = FieldType::new(FieldTypeCode::VarString);
    ft.set_charset_name("utf8mb4");
    ft.set_collation_name("utf8mb4_bin");
    constant.ret_type = Some(ft);
    Expression::Constant(constant)
}

fn desc(name: &str, args: Vec<Expression>) -> AggFuncDesc {
    AggFuncDesc::new(&NoColumns, name, args, false).expect("descriptor builds")
}

// ---------------------------------------------------------------------
// Go base_func_test.go::TestClone
// ---------------------------------------------------------------------

#[test]
fn base_func_clone_is_deep() {
    let column = col(0, FieldTypeCode::LongLong);
    let original = BaseFuncDesc::new(&NoColumns, names::FIRST_ROW, vec![column]).unwrap();
    let mut cloned = original.clone();
    assert!(original.equal(&cloned));

    cloned.args[0] = col(1, FieldTypeCode::Varchar);
    // The original's argument is untouched, so the two no longer match.
    assert_eq!(original.args[0].as_column().unwrap().unique_id, 0);
    assert!(!original.equal(&cloned));
}

// ---------------------------------------------------------------------
// Go base_func_test.go::TestBaseFunc_InferAggRetType
// ---------------------------------------------------------------------

#[test]
fn max_min_keep_the_argument_type_and_drop_not_null() {
    for code in [FieldTypeCode::Double, FieldTypeCode::Bit] {
        let mut not_null = FieldType::new(code);
        not_null.add_flags(FieldTypeFlags::NOT_NULL);
        for name in [names::MAX, names::MIN] {
            let mut d =
                BaseFuncDesc::new(&NoColumns, name, vec![col_typed(0, not_null.clone())]).unwrap();
            // A second TypeInfer is idempotent, as the Go test asserts.
            d.type_infer(&NoColumns).unwrap();
            assert_eq!(d.ret_type.code(), code);
            assert_eq!(d.ret_type.flags() & FieldTypeFlags::NOT_NULL, 0);
        }
    }
}

#[test]
fn group_concat_keeps_a_non_binary_input_collation() {
    let mut col_type = FieldType::new(FieldTypeCode::VarString);
    col_type.set_charset_name("utf8mb4");
    col_type.set_collation_name("utf8mb4_0900_ai_ci");
    col_type.del_flags(FieldTypeFlags::BINARY);

    let mut sep_type = FieldType::new(FieldTypeCode::VarString);
    sep_type.set_charset_name("utf8mb4");
    sep_type.set_collation_name("utf8mb4_0900_ai_ci");
    sep_type.del_flags(FieldTypeFlags::BINARY);
    let mut separator = Constant::default();
    separator.value = Datum::new_string(" ");
    separator.ret_type = Some(sep_type);

    let d = BaseFuncDesc::new(
        &NoColumns,
        names::GROUP_CONCAT,
        vec![col_typed(1, col_type), Expression::Constant(separator)],
    )
    .unwrap();
    assert_eq!(d.ret_type.charset_name(), "utf8mb4");
    assert_eq!(d.ret_type.collation_name(), "utf8mb4_0900_ai_ci");
}

#[test]
fn group_concat_falls_back_to_the_connection_collation() {
    // An integer argument makes the derived collation NUMERIC, which
    // `check_and_derive_collation_from_exprs` rewrites to the connection's.
    let d = BaseFuncDesc::new(
        &NoColumns,
        names::GROUP_CONCAT,
        vec![col(2, FieldTypeCode::LongLong), string_const(",")],
    )
    .unwrap();
    assert_eq!(d.ret_type.charset_name(), "utf8mb4");
    assert_eq!(d.ret_type.code(), FieldTypeCode::VarString);
    assert_eq!(d.ret_type.flen(), super::wrap_cast::MAX_BLOB_WIDTH);
    assert_eq!(d.ret_type.decimal(), 0);
}

// ---------------------------------------------------------------------
// Go base_func_test.go::TestTypeInfer4AvgSum (the `sum(col)` half)
// ---------------------------------------------------------------------

#[test]
fn type_infer_4_avg_sum_widens_a_decimal_column() {
    let mut arg_type = FieldType::new(FieldTypeCode::NewDecimal);
    arg_type.set_flen(15);
    arg_type.set_decimal(2);

    let mut avg = BaseFuncDesc::new(&NoColumns, names::AVG, vec![col_typed(0, arg_type)]).unwrap();
    let avg_ret = avg.ret_type.clone();

    // Go mutates the avg descriptor into the partial sum in place.
    avg.name = names::SUM.to_owned();
    avg.type_infer_4_avg_sum(&avg_ret).unwrap();
    // A plain column re-runs typeInfer4Sum: flen + 22, scale preserved.
    assert_eq!(avg.ret_type.flen(), 15 + 22);
    assert_eq!(avg.ret_type.decimal(), 2);
}

#[test]
fn type_infer_4_avg_sum_rejects_a_non_sum_name() {
    let mut d =
        BaseFuncDesc::new(&NoColumns, names::AVG, vec![col(0, FieldTypeCode::Long)]).unwrap();
    let ret = d.ret_type.clone();
    assert_eq!(
        d.type_infer_4_avg_sum(&ret),
        Err(AggDescError::ExpectSumFunc("avg".to_owned()))
    );
}

// ---------------------------------------------------------------------
// Go aggregation_test.go::TestAggFuncDesc (identity), expressed through
// `Equals` because `Hash64` is not ported -- see the module header.
// ---------------------------------------------------------------------

#[test]
fn agg_func_desc_identity_discriminates_every_field() {
    let column = col(0, FieldTypeCode::LongLong);
    let first = desc(names::SUM, vec![column.clone()]);
    let mut second = desc(names::SUM, vec![column.clone()]);
    assert!(first.equals(&second));

    second.has_distinct = true;
    assert!(!first.equals(&second));
    second.has_distinct = false;

    second.mode = AggFunctionMode::Final;
    assert!(!first.equals(&second));
    second.mode = AggFunctionMode::Complete;

    second.base.name = "whatever".to_owned();
    assert!(!first.equals(&second));
    second.base.name = names::SUM.to_owned();

    second.base.args = Vec::new();
    assert!(!first.equals(&second));
    second.base.args = vec![column.clone()];

    second.base.ret_type = FieldType::new(FieldTypeCode::NewDecimal);
    assert!(!first.equals(&second));
    second.base.ret_type = first.base.ret_type.clone();
    assert!(first.equals(&second));

    second.order_by_items = vec![ByItems::new(column, true)];
    assert!(!first.equals(&second));
}

// ---------------------------------------------------------------------
// Go aggregation_test.go::TestCheckAggPushDownSumInt
// ---------------------------------------------------------------------

#[test]
fn sum_int_is_pushable_to_both_stores() {
    let d = desc(names::SUM_INT, vec![col(0, FieldTypeCode::LongLong)]);
    let blacklist = std::collections::HashMap::new();
    assert!(check_agg_push_down(
        &d,
        crate::infer_pushdown::PushDownStore::TiFlash,
        &blacklist
    ));
    assert!(check_agg_push_down(
        &d,
        crate::infer_pushdown::PushDownStore::TiKv,
        &blacklist
    ));
}

#[test]
fn group_concat_is_not_pushable_to_tikv_but_is_to_tiflash() {
    let d = desc(
        names::GROUP_CONCAT,
        vec![col(0, FieldTypeCode::VarString), string_const(",")],
    );
    let blacklist = std::collections::HashMap::new();
    assert!(!check_agg_push_down(
        &d,
        crate::infer_pushdown::PushDownStore::TiKv,
        &blacklist
    ));
    assert!(check_agg_push_down(
        &d,
        crate::infer_pushdown::PushDownStore::TiFlash,
        &blacklist
    ));
}

#[test]
fn approx_count_distinct_only_pushes_to_tiflash() {
    let d = desc(
        names::APPROX_COUNT_DISTINCT,
        vec![col(0, FieldTypeCode::Long)],
    );
    let blacklist = std::collections::HashMap::new();
    assert!(check_agg_push_down(
        &d,
        crate::infer_pushdown::PushDownStore::TiFlash,
        &blacklist
    ));
    assert!(!check_agg_push_down(
        &d,
        crate::infer_pushdown::PushDownStore::TiKv,
        &blacklist
    ));
}

#[test]
fn approx_percentile_and_an_ordered_non_group_concat_never_push() {
    let blacklist = std::collections::HashMap::new();
    let percentile = desc(
        names::APPROX_PERCENTILE,
        vec![col(0, FieldTypeCode::Long), int_const(50)],
    );
    assert!(!check_agg_push_down(
        &percentile,
        crate::infer_pushdown::PushDownStore::TiFlash,
        &blacklist
    ));

    let mut ordered_sum = desc(names::SUM, vec![col(0, FieldTypeCode::Long)]);
    ordered_sum.order_by_items = vec![ByItems::new(col(0, FieldTypeCode::Long), false)];
    assert!(!check_agg_push_down(
        &ordered_sum,
        crate::infer_pushdown::PushDownStore::TiKv,
        &blacklist
    ));
}

#[test]
fn a_vector_argument_blocks_all_but_the_value_agnostic_kinds() {
    let blacklist = std::collections::HashMap::new();
    let sum = desc(names::SUM, vec![col(0, FieldTypeCode::VectorFloat32)]);
    assert!(!check_agg_push_down(
        &sum,
        crate::infer_pushdown::PushDownStore::TiFlash,
        &blacklist
    ));
    let count = desc(names::COUNT, vec![col(0, FieldTypeCode::VectorFloat32)]);
    assert!(check_agg_push_down(
        &count,
        crate::infer_pushdown::PushDownStore::TiFlash,
        &blacklist
    ));
}

#[test]
fn tiflash_refuses_a_duration_argument_and_a_json_sum() {
    let duration_max = desc(names::MAX, vec![col(0, FieldTypeCode::Duration)]);
    assert!(!check_agg_push_flash(&duration_max));
    let json_sum = desc(names::SUM, vec![col(0, FieldTypeCode::Json)]);
    assert!(!check_agg_push_flash(&json_sum));
    let json_max = desc(names::MAX, vec![col(0, FieldTypeCode::Json)]);
    assert!(check_agg_push_flash(&json_max));
}

// ---------------------------------------------------------------------
// Written coverage: type inference per agg kind and per argument type.
// ---------------------------------------------------------------------

#[test]
fn count_returns_a_not_null_bigint() {
    for name in [names::COUNT, names::APPROX_COUNT_DISTINCT] {
        let d = desc(name, vec![col(0, FieldTypeCode::Varchar)]);
        assert_eq!(d.ret_type().code(), FieldTypeCode::LongLong);
        assert_eq!(d.ret_type().flen(), 21);
        assert_eq!(d.ret_type().decimal(), 0);
        assert_ne!(d.ret_type().flags() & FieldTypeFlags::NOT_NULL, 0);
        assert_eq!(d.ret_type().charset_name(), "binary");
    }
}

#[test]
fn sum_is_decimal_for_integers_and_double_otherwise() {
    // An integer column with a declared width: flen + 21, scale 0.
    let mut long = FieldType::new(FieldTypeCode::Long);
    long.set_flen(11);
    let d = desc(names::SUM, vec![col_typed(0, long)]);
    assert_eq!(d.ret_type().code(), FieldTypeCode::NewDecimal);
    assert_eq!(d.ret_type().flen(), 32);
    assert_eq!(d.ret_type().decimal(), 0);

    // An integer column with an UNSPECIFIED width saturates to the max.
    // NOTE `FieldType::new(LongLong)` in this workspace pre-fills the default
    // display width (20), unlike Go's `types.NewFieldType`, so the negative
    // flen has to be set explicitly to reach the source's clamp branch.
    let mut unspecified = FieldType::new(FieldTypeCode::LongLong);
    unspecified.set_flen(UNSPECIFIED_LENGTH);
    let d = desc(names::SUM, vec![col_typed(0, unspecified)]);
    assert_eq!(d.ret_type().flen(), MAX_DECIMAL_WIDTH);

    // The workspace default (BIGINT display width 20) takes the ordinary
    // flen + 21 path.
    let d = desc(names::SUM, vec![col(0, FieldTypeCode::LongLong)]);
    assert_eq!(d.ret_type().flen(), 41);

    // YEAR takes the integer arm too.
    let d = desc(names::SUM, vec![col(0, FieldTypeCode::Year)]);
    assert_eq!(d.ret_type().code(), FieldTypeCode::NewDecimal);

    // DECIMAL widens flen by 22 and keeps the scale.
    let mut dec = FieldType::new(FieldTypeCode::NewDecimal);
    dec.set_flen(15);
    dec.set_decimal(2);
    let d = desc(names::SUM, vec![col_typed(0, dec)]);
    assert_eq!(d.ret_type().code(), FieldTypeCode::NewDecimal);
    assert_eq!(d.ret_type().flen(), 37);
    assert_eq!(d.ret_type().decimal(), 2);

    // REAL and everything else are DOUBLE.
    for code in [
        FieldTypeCode::Double,
        FieldTypeCode::Float,
        FieldTypeCode::Varchar,
        FieldTypeCode::Datetime,
    ] {
        let d = desc(names::SUM, vec![col(0, code)]);
        assert_eq!(d.ret_type().code(), FieldTypeCode::Double, "{code:?}");
        assert_eq!(d.ret_type().flen(), 23);
    }
}

#[test]
fn sum_int_keeps_the_unsigned_flag_and_rejects_non_integers() {
    let mut unsigned = FieldType::new(FieldTypeCode::LongLong);
    unsigned.add_flags(FieldTypeFlags::UNSIGNED);
    let d = desc(names::SUM_INT, vec![col_typed(0, unsigned)]);
    assert_eq!(d.ret_type().code(), FieldTypeCode::LongLong);
    assert_ne!(d.ret_type().flags() & FieldTypeFlags::UNSIGNED, 0);

    assert_eq!(
        AggFuncDesc::new(
            &NoColumns,
            names::SUM_INT,
            vec![col(0, FieldTypeCode::Varchar)],
            false
        )
        .unwrap_err(),
        AggDescError::SumIntNonInteger
    );
    assert_eq!(
        AggFuncDesc::new(
            &NoColumns,
            names::SUM_INT,
            vec![col(0, FieldTypeCode::Long), col(1, FieldTypeCode::Long)],
            false
        )
        .unwrap_err(),
        AggDescError::SumIntArgCount
    );
}

#[test]
fn avg_scales_by_the_division_precision_increment() {
    // NoColumns reports Go's default div_precision_increment of 4.
    assert_eq!(
        crate::context::Columns::div_precision_increment(&NoColumns),
        4
    );

    // BIGINT's default display width is 20, so flen is 24 and scale 4.
    let d = desc(names::AVG, vec![col(0, FieldTypeCode::LongLong)]);
    assert_eq!(d.ret_type().code(), FieldTypeCode::NewDecimal);
    assert_eq!(d.ret_type().decimal(), 4);
    assert_eq!(d.ret_type().flen(), 24);

    // DECIMAL(15,2) becomes DECIMAL(19,6).
    let mut dec = FieldType::new(FieldTypeCode::NewDecimal);
    dec.set_flen(15);
    dec.set_decimal(2);
    let d = desc(names::AVG, vec![col_typed(0, dec)]);
    assert_eq!(d.ret_type().flen(), 19);
    assert_eq!(d.ret_type().decimal(), 6);

    // Temporal arguments are DOUBLE with scale 4; everything else DOUBLE with
    // an unspecified scale.
    for code in [
        FieldTypeCode::Date,
        FieldTypeCode::Duration,
        FieldTypeCode::Datetime,
        FieldTypeCode::Timestamp,
    ] {
        let d = desc(names::AVG, vec![col(0, code)]);
        assert_eq!(d.ret_type().code(), FieldTypeCode::Double, "{code:?}");
        assert_eq!(d.ret_type().decimal(), 4, "{code:?}");
    }
    let d = desc(names::AVG, vec![col(0, FieldTypeCode::Varchar)]);
    assert_eq!(d.ret_type().decimal(), UNSPECIFIED_LENGTH);
}

#[test]
fn bit_funcs_return_unsigned_not_null_bigint_and_cast_their_argument() {
    for name in [names::BIT_AND, names::BIT_OR, names::BIT_XOR] {
        let d = desc(name, vec![col(0, FieldTypeCode::Varchar)]);
        assert_eq!(d.ret_type().code(), FieldTypeCode::LongLong);
        assert_eq!(d.ret_type().flen(), 21);
        assert_ne!(d.ret_type().flags() & FieldTypeFlags::UNSIGNED, 0);
        assert_ne!(d.ret_type().flags() & FieldTypeFlags::NOT_NULL, 0);
        // typeInfer4BitFuncs wraps args[0] in a cast to INT.
        assert!(matches!(d.args()[0], Expression::ScalarFunction(_)));
        assert_eq!(
            type_of(&d.args()[0]).eval_type(),
            tidb_datatype::EvalType::Int
        );
    }
    // An argument that already evaluates as INT is NOT wrapped.
    let d = desc(names::BIT_OR, vec![col(0, FieldTypeCode::LongLong)]);
    assert!(matches!(d.args()[0], Expression::Column(_)));
}

#[test]
fn max_min_and_first_row_rewrite_enum_only_for_the_window_kinds() {
    // MAX/MIN/FIRST_ROW keep an ENUM argument's type.
    for name in [names::MAX, names::MIN, names::FIRST_ROW] {
        let d = desc(name, vec![col(0, FieldTypeCode::Enum)]);
        assert_eq!(d.ret_type().code(), FieldTypeCode::Enum, "{name}");
    }
    // FIRST_VALUE/LAST_VALUE/NTH_VALUE turn ENUM/SET into a CHAR(255).
    for name in [names::FIRST_VALUE, names::LAST_VALUE] {
        for code in [FieldTypeCode::Enum, FieldTypeCode::Set] {
            let d = desc(name, vec![col(0, code)]);
            assert_eq!(
                d.ret_type().code(),
                FieldTypeCode::String,
                "{name} {code:?}"
            );
            assert_eq!(d.ret_type().flen(), 255);
        }
    }
}

#[test]
fn max_of_a_float_scalar_function_is_cast_to_double() {
    let mut float_ret = FieldType::new(FieldTypeCode::Float);
    float_ret.set_flen(12);
    let inner = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("plus"),
        float_ret,
        vec![col(0, FieldTypeCode::Float), col(1, FieldTypeCode::Float)],
    ));
    let d = desc(names::MAX, vec![inner]);
    assert_eq!(d.ret_type().code(), FieldTypeCode::Double);
    assert_eq!(d.ret_type().flen(), 23);
    // A plain FLOAT COLUMN is not wrapped.
    let d = desc(names::MAX, vec![col(0, FieldTypeCode::Float)]);
    assert_eq!(d.ret_type().code(), FieldTypeCode::Float);
}

#[test]
fn json_aggregates_return_json_and_object_agg_casts_its_key() {
    let d = desc(names::JSON_ARRAYAGG, vec![col(0, FieldTypeCode::Long)]);
    assert_eq!(d.ret_type().code(), FieldTypeCode::Json);
    assert_eq!(d.ret_type().charset_name(), "binary");

    let d = desc(
        names::JSON_OBJECTAGG,
        vec![col(0, FieldTypeCode::Long), col(1, FieldTypeCode::Long)],
    );
    assert_eq!(d.ret_type().code(), FieldTypeCode::Json);
    // The KEY is cast to string; the VALUE is untouched.
    assert!(matches!(d.args()[0], Expression::ScalarFunction(_)));
    assert!(matches!(d.args()[1], Expression::Column(_)));
}

#[test]
fn window_ranking_and_distribution_types() {
    for name in [names::ROW_NUMBER, names::RANK, names::DENSE_RANK] {
        let d = desc(name, Vec::new());
        assert_eq!(d.ret_type().code(), FieldTypeCode::LongLong, "{name}");
        assert_eq!(d.ret_type().flen(), 21);
        assert_eq!(d.ret_type().charset_name(), "binary");
    }
    let d = desc(names::CUME_DIST, Vec::new());
    assert_eq!(d.ret_type().code(), FieldTypeCode::Double);
    assert_eq!(d.ret_type().flen(), 23);
    assert_eq!(d.ret_type().decimal(), 31);

    let d = desc(names::NTILE, vec![int_const(4)]);
    assert_eq!(d.ret_type().code(), FieldTypeCode::LongLong);
    assert_ne!(d.ret_type().flags() & FieldTypeFlags::UNSIGNED, 0);

    // percent_rank reproduces the source's SetFlag(MaxRealWidth) typo: the
    // flags become 23 and flen stays unset.
    let d = desc(names::PERCENT_RANK, Vec::new());
    assert_eq!(d.ret_type().code(), FieldTypeCode::Double);
    assert_eq!(d.ret_type().flags(), 23);
    assert_eq!(d.ret_type().flen(), UNSPECIFIED_LENGTH);
}

#[test]
fn the_variance_family_is_always_double() {
    for name in [
        names::VAR_POP,
        names::VAR_SAMP,
        names::STDDEV_POP,
        names::STDDEV_SAMP,
    ] {
        let d = desc(name, vec![col(0, FieldTypeCode::NewDecimal)]);
        assert_eq!(d.ret_type().code(), FieldTypeCode::Double, "{name}");
        assert_eq!(d.ret_type().flen(), 23);
        assert_eq!(d.ret_type().decimal(), UNSPECIFIED_LENGTH);
    }
}

#[test]
fn approx_percentile_types_follow_the_value_argument() {
    let d = desc(
        names::APPROX_PERCENTILE,
        vec![col(0, FieldTypeCode::Long), int_const(50)],
    );
    assert_eq!(d.ret_type().code(), FieldTypeCode::LongLong);

    let d = desc(
        names::APPROX_PERCENTILE,
        vec![col(0, FieldTypeCode::Float), int_const(50)],
    );
    assert_eq!(d.ret_type().code(), FieldTypeCode::Double);

    let mut dec = FieldType::new(FieldTypeCode::NewDecimal);
    dec.set_decimal(3);
    let d = desc(
        names::APPROX_PERCENTILE,
        vec![col_typed(0, dec), int_const(50)],
    );
    assert_eq!(d.ret_type().flen(), MAX_DECIMAL_WIDTH);
    assert_eq!(d.ret_type().decimal(), 3);

    // Every rejection path.
    let err = |args: Vec<Expression>| {
        AggFuncDesc::new(&NoColumns, names::APPROX_PERCENTILE, args, false).unwrap_err()
    };
    assert_eq!(
        err(vec![col(0, FieldTypeCode::Long)]),
        AggDescError::ApproxPercentileArgCount
    );
    assert_eq!(
        err(vec![col(0, FieldTypeCode::Long), int_const(0)]),
        AggDescError::ApproxPercentileOutOfRange(0)
    );
    assert_eq!(
        err(vec![col(0, FieldTypeCode::Long), int_const(101)]),
        AggDescError::ApproxPercentileOutOfRange(101)
    );
    assert_eq!(
        err(vec![
            col(0, FieldTypeCode::Long),
            col(1, FieldTypeCode::Long)
        ]),
        AggDescError::ApproxPercentileNotConstant
    );
}

#[test]
fn an_unknown_name_is_refused() {
    assert_eq!(
        AggFuncDesc::new(
            &NoColumns,
            "median",
            vec![col(0, FieldTypeCode::Long)],
            false
        )
        .unwrap_err(),
        AggDescError::UnsupportedAggFunction("median".to_owned())
    );
    // The name is lower-cased first, so an upper-case spelling is accepted.
    let d = desc("SUM", vec![col(0, FieldTypeCode::Long)]);
    assert_eq!(d.name(), "sum");
}

// ---------------------------------------------------------------------
// Written coverage: default values, NOT-NULL updates, split, cast wrapping.
// ---------------------------------------------------------------------

#[test]
fn default_values_match_the_empty_table_query_in_the_source_comment() {
    let long = || vec![col(0, FieldTypeCode::Long)];
    assert_eq!(
        desc(names::COUNT, long()).base.get_default_value(),
        Datum::new_int(0)
    );
    assert_eq!(
        desc(names::BIT_OR, long()).base.get_default_value(),
        Datum::new_int(0)
    );
    assert_eq!(
        desc(names::BIT_XOR, long()).base.get_default_value(),
        Datum::new_int(0)
    );
    assert_eq!(
        desc(names::BIT_AND, long()).base.get_default_value(),
        Datum::new_uint(u64::MAX)
    );
    for name in [
        names::AVG,
        names::SUM,
        names::MAX,
        names::MIN,
        names::FIRST_ROW,
    ] {
        assert_eq!(
            desc(name, long()).base.get_default_value(),
            Datum::Null,
            "{name}"
        );
    }
    assert_eq!(
        desc(names::APPROX_COUNT_DISTINCT, long())
            .base
            .get_default_value(),
        Datum::new_int(0)
    );
}

#[test]
fn update_not_null_flag_follows_the_group_by_shape() {
    // COUNT never loses NOT NULL.
    let mut d = desc(names::COUNT, vec![col(0, FieldTypeCode::Long)]);
    d.update_not_null_flag_4_ret_type(false, false).unwrap();
    assert_ne!(d.ret_type().flags() & FieldTypeFlags::NOT_NULL, 0);

    // SUM over a NOT NULL column loses it only without GROUP BY.
    let mut not_null = FieldType::new(FieldTypeCode::Long);
    not_null.add_flags(FieldTypeFlags::NOT_NULL);
    let mut d = desc(names::SUM, vec![col_typed(0, not_null.clone())]);
    d.base.ret_type.add_flags(FieldTypeFlags::NOT_NULL);
    d.update_not_null_flag_4_ret_type(true, false).unwrap();
    assert_ne!(d.ret_type().flags() & FieldTypeFlags::NOT_NULL, 0);
    d.update_not_null_flag_4_ret_type(false, false).unwrap();
    assert_eq!(d.ret_type().flags() & FieldTypeFlags::NOT_NULL, 0);

    // MAX over a BIT column keeps NOT NULL even without GROUP BY.
    let mut bit = FieldType::new(FieldTypeCode::Bit);
    bit.add_flags(FieldTypeFlags::NOT_NULL);
    let mut d = desc(names::MAX, vec![col_typed(0, bit)]);
    d.base.ret_type.add_flags(FieldTypeFlags::NOT_NULL);
    d.update_not_null_flag_4_ret_type(false, false).unwrap();
    assert_ne!(d.ret_type().flags() & FieldTypeFlags::NOT_NULL, 0);

    // FIRST_ROW keeps NOT NULL when every sibling aggregate is a FIRST_ROW.
    let mut d = desc(names::FIRST_ROW, vec![col_typed(0, not_null)]);
    d.base.ret_type.add_flags(FieldTypeFlags::NOT_NULL);
    d.update_not_null_flag_4_ret_type(false, true).unwrap();
    assert_ne!(d.ret_type().flags() & FieldTypeFlags::NOT_NULL, 0);
    d.update_not_null_flag_4_ret_type(false, false).unwrap();
    assert_eq!(d.ret_type().flags() & FieldTypeFlags::NOT_NULL, 0);

    let mut d = desc(names::SUM, vec![col(0, FieldTypeCode::Long)]);
    d.base.name = "median".to_owned();
    assert!(d.update_not_null_flag_4_ret_type(false, false).is_err());
}

#[test]
fn split_produces_a_partial_and_a_final_descriptor() {
    // AVG's final phase reads two intermediate columns: count then sum.
    let avg = desc(names::AVG, vec![col(0, FieldTypeCode::LongLong)]);
    let (partial, final_desc) = avg.split(&[3, 4]);
    assert_eq!(partial.mode, AggFunctionMode::Partial1);
    assert_eq!(final_desc.mode, AggFunctionMode::Final);
    assert_eq!(final_desc.args().len(), 2);
    assert_eq!(final_desc.args()[0].as_column().unwrap().index, 3);
    assert_eq!(
        type_of(&final_desc.args()[0]).code(),
        FieldTypeCode::LongLong
    );
    assert_eq!(final_desc.args()[1].as_column().unwrap().index, 4);
    assert_eq!(
        type_of(&final_desc.args()[1]).code(),
        FieldTypeCode::NewDecimal
    );

    // A FinalMode input splits into Partial2.
    let mut avg2 = avg.clone();
    avg2.mode = AggFunctionMode::Final;
    let (partial, _) = avg2.split(&[0, 1]);
    assert_eq!(partial.mode, AggFunctionMode::Partial2);

    // COUNT DISTINCT keeps the original arguments (Go's documented hack).
    let mut count = desc(names::COUNT, vec![col(7, FieldTypeCode::Long)]);
    count.has_distinct = true;
    let (_, final_desc) = count.split(&[2]);
    assert_eq!(final_desc.args()[0].as_column().unwrap().unique_id, 7);
    count.has_distinct = false;
    let (_, final_desc) = count.split(&[2]);
    assert_eq!(final_desc.args()[0].as_column().unwrap().index, 2);

    // GROUP_CONCAT carries the separator through to the final phase.
    let concat = desc(
        names::GROUP_CONCAT,
        vec![col(0, FieldTypeCode::VarString), string_const("-")],
    );
    let (_, final_desc) = concat.split(&[5]);
    assert_eq!(final_desc.args().len(), 2);
    assert!(matches!(final_desc.args()[1], Expression::Constant(_)));

    // APPROX_COUNT_DISTINCT's intermediate is a serialized string sketch.
    let acd = desc(
        names::APPROX_COUNT_DISTINCT,
        vec![col(0, FieldTypeCode::Long)],
    );
    let (_, final_desc) = acd.split(&[1]);
    assert_eq!(type_of(&final_desc.args()[0]).code(), FieldTypeCode::String);
}

#[test]
fn wrap_cast_for_agg_args_skips_the_no_cast_kinds_and_null_arguments() {
    // SUM over an integer column: the DECIMAL return type wraps the argument.
    let mut d = desc(names::SUM, vec![col(0, FieldTypeCode::Long)]);
    d.base.wrap_cast_for_agg_args(&NoColumns).unwrap();
    assert!(matches!(d.args()[0], Expression::ScalarFunction(_)));
    assert_eq!(
        type_of(&d.args()[0]).eval_type(),
        tidb_datatype::EvalType::Decimal
    );

    // MAX is in noNeedCastAggFuncs, so nothing is wrapped.
    let mut d = desc(names::MAX, vec![col(0, FieldTypeCode::Long)]);
    d.base.wrap_cast_for_agg_args(&NoColumns).unwrap();
    assert!(matches!(d.args()[0], Expression::Column(_)));

    // A NULL-typed argument is left alone.
    let mut d = desc(names::SUM, vec![col(0, FieldTypeCode::Long)]);
    d.base.args = vec![col(0, FieldTypeCode::Null)];
    d.base.wrap_cast_for_agg_args(&NoColumns).unwrap();
    assert!(matches!(d.args()[0], Expression::Column(_)));

    // An empty argument list returns early.
    let mut d = desc(names::ROW_NUMBER, Vec::new());
    d.base.wrap_cast_for_agg_args(&NoColumns).unwrap();
    assert!(d.args().is_empty());
}

// ---------------------------------------------------------------------
// Written coverage: modes, window descriptors, explain rendering.
// ---------------------------------------------------------------------

#[test]
fn agg_function_mode_strings_and_ordinals_round_trip() {
    let all = [
        (AggFunctionMode::Complete, 0, "complete"),
        (AggFunctionMode::Final, 1, "final"),
        (AggFunctionMode::Partial1, 2, "partial1"),
        (AggFunctionMode::Partial2, 3, "partial2"),
        (AggFunctionMode::Dedup, 4, "deduplicate"),
    ];
    for (mode, ordinal, text) in all {
        assert_eq!(mode.ordinal(), ordinal);
        assert_eq!(mode.as_str(), text);
        assert_eq!(AggFunctionMode::from_ordinal(ordinal), Some(mode));
    }
    assert_eq!(AggFunctionMode::from_ordinal(5), None);
    assert_eq!(AggFunctionMode::default(), AggFunctionMode::Complete);
}

#[test]
fn need_count_need_value_and_is_all_first_row() {
    assert!(need_count(names::COUNT));
    assert!(need_count(names::AVG));
    assert!(!need_count(names::SUM));
    assert!(need_value(names::SUM));
    assert!(need_value(names::APPROX_PERCENTILE));
    assert!(!need_value(names::COUNT));

    let first_rows = vec![
        desc(names::FIRST_ROW, vec![col(0, FieldTypeCode::Long)]),
        desc(names::FIRST_ROW, vec![col(1, FieldTypeCode::Long)]),
    ];
    assert!(is_all_first_row(&first_rows));
    let mixed = vec![
        desc(names::FIRST_ROW, vec![col(0, FieldTypeCode::Long)]),
        desc(names::COUNT, vec![col(1, FieldTypeCode::Long)]),
    ];
    assert!(!is_all_first_row(&mixed));
    assert!(is_all_first_row(&[]));
}

#[test]
fn window_func_desc_applies_the_not_null_fixups() {
    // ROW_NUMBER is forced NOT NULL.
    let d = WindowFuncDesc::new(&NoColumns, "ROW_NUMBER", Vec::new(), false)
        .unwrap()
        .unwrap();
    assert_ne!(d.base.ret_type.flags() & FieldTypeFlags::NOT_NULL, 0);

    // FIRST_VALUE over a NOT NULL column loses the flag (the default arm).
    let mut not_null = FieldType::new(FieldTypeCode::Long);
    not_null.add_flags(FieldTypeFlags::NOT_NULL);
    let d = WindowFuncDesc::new(
        &NoColumns,
        names::FIRST_VALUE,
        vec![col_typed(0, not_null.clone())],
        false,
    )
    .unwrap()
    .unwrap();
    assert_eq!(d.base.ret_type.flags() & FieldTypeFlags::NOT_NULL, 0);

    // LEAD with three NOT NULL-compatible arguments keeps the flag.
    let d = WindowFuncDesc::new(
        &NoColumns,
        names::LEAD,
        vec![
            col_typed(0, not_null.clone()),
            int_const(1),
            col_typed(1, not_null),
        ],
        false,
    )
    .unwrap()
    .unwrap();
    assert_ne!(d.base.ret_type.flags() & FieldTypeFlags::NOT_NULL, 0);

    // NTILE(0) and LEAD(x, NULL) are REJECTED, not errors.
    assert!(
        WindowFuncDesc::new(&NoColumns, names::NTILE, vec![int_const(0)], false)
            .unwrap()
            .is_none()
    );
    let mut null_const = Constant::default();
    null_const.ret_type = Some(FieldType::new(FieldTypeCode::LongLong));
    assert!(WindowFuncDesc::new(
        &NoColumns,
        names::LEAD,
        vec![
            col(0, FieldTypeCode::Long),
            Expression::Constant(null_const)
        ],
        false,
    )
    .unwrap()
    .is_none());
    // ... unless the argument check is skipped.
    assert!(
        WindowFuncDesc::new(&NoColumns, names::NTILE, vec![int_const(0)], true)
            .unwrap()
            .is_some()
    );
}

#[test]
fn agg_func_desc_for_window_func_reuses_the_inferred_signature() {
    let window = WindowFuncDesc::new(
        &NoColumns,
        names::FIRST_VALUE,
        vec![col(0, FieldTypeCode::Long)],
        false,
    )
    .unwrap()
    .unwrap();
    let agg = AggFuncDesc::new_for_window_func(&window, true);
    assert_eq!(agg.name(), names::FIRST_VALUE);
    assert!(agg.has_distinct);
    assert!(agg.ret_type().equal(&window.base.ret_type));
}

#[test]
fn frame_rules_match_the_source_tables() {
    assert!(!need_frame("RANK"));
    assert!(!need_frame(names::ROW_NUMBER));
    assert!(need_frame(names::FIRST_VALUE));
    assert!(need_frame(names::SUM));
    assert_eq!(
        use_default_frame("Row_Number"),
        Some(WindowFrameDefault {
            rows: true,
            start_is_current_row: true,
            end_is_current_row: true,
        })
    );
    assert_eq!(use_default_frame(names::RANK), None);
}

#[test]
fn explain_renders_distinct_order_by_and_the_separator() {
    let mut d = desc(
        names::GROUP_CONCAT,
        vec![col(0, FieldTypeCode::VarString), string_const(",")],
    );
    d.has_distinct = true;
    d.order_by_items = vec![
        ByItems::new(col(0, FieldTypeCode::VarString), true),
        ByItems::new(col(1, FieldTypeCode::VarString), false),
    ];
    let rendered = explain_agg_func_normalized(&d, false);
    assert!(rendered.starts_with("group_concat(distinct "), "{rendered}");
    assert!(rendered.contains(" order by "), "{rendered}");
    assert!(rendered.contains(" desc"), "{rendered}");
    assert!(rendered.contains(" separator "), "{rendered}");
    assert!(rendered.ends_with(')'), "{rendered}");

    let count = desc(names::COUNT, vec![col(0, FieldTypeCode::Long)]);
    assert!(explain_agg_func_normalized(&count, false).starts_with("count("));
    assert!(explain_agg_func_normalized(&count, true).starts_with("count(complete,"));
}
