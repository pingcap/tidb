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

//! Real port of
//! `pkg/planner/core/casetest/rule/rule_inject_extra_projection_test.go::TestWrapCastForAggFuncs`
//! (`pkg/planner.part9` item 502 on `origin/master`).
//!
//! The Go test is a pure UNIT test: it builds SUM descriptors directly with
//! `aggregation.NewAggFuncDesc` (`pkg/expression/aggregation/descriptor.go:47`,
//! ported as [`tidb_expr::aggregation::AggFuncDesc::new`] whose inference
//! mirrors `typeInfer4Sum`, `base_func.go:215-237`), then drives the
//! planner-side helper `coreusage.WrapCastForAggFuncs`
//! (`pkg/planner/util/coreusage/cast_misc.go:25`) which wraps each
//! descriptor's arguments in casts ONLY when `Mode != FinalMode &&
//! Mode != Partial2Mode`. No session, catalog, or SQL pipeline is involved,
//! so both halves of the exercised surface exist on the Rust side: the
//! descriptors come from `tidb_expr::aggregation`, and the mode gate is
//! implemented by [`tidb_planner::core_usage::wrap_cast_for_agg_funcs`].
//!
//! Per-case expectations re-derived from Go source:
//! - The argument retTypes cover the int family (`TypeLong`, `TypeLonglong`,
//!   `TypeInt24`) → `typeInfer4Sum` returns `TypeNewDecimal`;
//!   `TypeNewDecimal` stays decimal; `TypeDouble` becomes double.
//!   `WrapWithCastAsDecimal`/`...AsReal` force the argument's type byte to the
//!   descriptor's return-type byte for every non-Final/Partial2 mode —
//!   exactly what Go asserts (`base_func.go:531-533`).
//! - Final/Partial2 modes are skipped untouched: the clone taken before the
//!   call must still equal the argument afterward
//!   (`rule_inject_extra_projection_test.go:60-66`). The comparison uses the
//!   SAME type-byte check Go uses, so it inherits Go's own blind spots too (a
//!   wrongly wrapped decimal-typed argument would still share the byte;
//!   int-family arguments would not — they would flip to NewDecimal and fail).

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::aggregation::{names, AggFuncDesc, AggFunctionMode};
use tidb_expr::constant::Constant;
use tidb_expr::expression::Expression;
use tidb_expr::NoColumns;
use tidb_planner::core_usage::wrap_cast_for_agg_funcs;

/// Go `aggNames := []string{ast.AggFuncSum}`
/// (`rule_inject_extra_projection_test.go:35`).
const AGG_NAMES: [&str; 1] = [names::SUM];
/// Go `modes := []aggregation.AggFunctionMode{CompleteMode, FinalMode,
/// Partial1Mode, Partial1Mode}` (`:36`) — Partial1 appears twice on purpose.
const MODES: [AggFunctionMode; 4] = [
    AggFunctionMode::Complete,
    AggFunctionMode::Final,
    AggFunctionMode::Partial1,
    AggFunctionMode::Partial1,
];
/// Go `retTypes := []byte{mysql.TypeLong, mysql.TypeNewDecimal,
/// mysql.TypeDouble, mysql.TypeLonglong, mysql.TypeInt24}` (`:37`).
const RET_TYPES: [FieldTypeCode; 5] = [
    FieldTypeCode::Long,
    FieldTypeCode::NewDecimal,
    FieldTypeCode::Double,
    FieldTypeCode::LongLong,
    FieldTypeCode::Int24,
];
/// Go `hasDistincts := []bool{true, false}` (`:38`).
const HAS_DISTINCTS: [bool; 2] = [true, false];

/// Builds one SUM descriptor exactly like the Go table walker: a single
/// constant whose VALUE is the zero datum (NULL) but whose declared type is
/// `ret_type`, so type inference reads the declared type while the cast loop's
/// skip check sees a non-Null DECLARED type too (`base_func.go:522` compares
/// `GetType().GetType() == TypeNull`, i.e. the type, not the datum).
fn sum_desc(has_distinct: bool, ret_type: FieldTypeCode) -> AggFuncDesc {
    let mut constant = Constant::default();
    constant.value = Datum::Null;
    constant.ret_type = Some(FieldType::new(ret_type));
    AggFuncDesc::new(
        &NoColumns,
        AGG_NAMES[0],
        vec![Expression::Constant(constant)],
        has_distinct,
    )
    .expect("NewAggFuncDesc(sum) infers")
}

/// Static type-code of the first argument, standing in for Go's
/// `Args[0].GetType(ctx).GetType()` byte read.
fn arg0_code(desc: &AggFuncDesc) -> FieldTypeCode {
    desc.args()[0]
        .static_type()
        .expect("built expression carries a static type")
        .code()
}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_inject_extra_projection_test.go:31
/// TestWrapCastForAggFuncs`.
///
/// Walks distinct × name × mode × retType exactly like the Go nested loops,
/// snapshots clones before wrapping (`orgAggFuncs`, `:52`), applies the
/// mode-gated wrap, then asserts per index (`:57-64`): wrapped modes have
/// their argument's type byte equal the descriptor's return-type byte, and
/// Final/Partial2 modes keep the exact pre-call argument types.
#[test]
fn wrap_cast_for_agg_funcs_gates_final_and_partial2_modes() {
    let mut agg_funcs = Vec::with_capacity(
        HAS_DISTINCTS.len() * AGG_NAMES.len() * MODES.len() * RET_TYPES.len(),
    );
    for &has_distinct in &HAS_DISTINCTS {
        for &name in &AGG_NAMES {
            for &mode in &MODES {
                for &ret_type in &RET_TYPES {
                    let mut desc = sum_desc(has_distinct, ret_type);
                    // Go assigns `aggFunc.Mode = mode` after construction.
                    desc.mode = mode;
                    assert_eq!(desc.name(), name.to_ascii_lowercase());
                    agg_funcs.push(desc);
                }
            }
        }
    }
    assert_eq!(agg_funcs.len(), 40);

    let originals = agg_funcs.clone();
    wrap_cast_for_agg_funcs(&NoColumns, &mut agg_funcs).expect("wrap cast for agg args");

    for (index, desc) in agg_funcs.iter().enumerate() {
        let original = &originals[index];
        if !matches!(desc.mode, AggFunctionMode::Final | AggFunctionMode::Partial2) {
            assert_eq!(
                desc.ret_type().code(),
                arg0_code(desc),
                "mode {:?} must leave arg type == RetTp type",
                desc.mode
            );
        } else {
            assert_eq!(
                arg0_code(original),
                arg0_code(desc),
                "mode {:?} must keep its arguments unwrapped",
                desc.mode
            );
            // Pin the negative as well: had the gate wrongly wrapped this
            // int-family argument it would now carry the aggregate's
            // NewDecimal byte instead of staying unwrapped.
            assert_eq!(
                original.ret_type().code(),
                desc.ret_type().code(),
                "mode {:?} must not re-infer RetTp either",
                desc.mode
            );
        }
    }
}
