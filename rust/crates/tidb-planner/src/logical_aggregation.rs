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

//! LogicalAggregation identity from
//! `pkg/planner/core/operator/logicalop/logical_aggregation.go` and its
//! generated Hash64/Equals implementation.
//!
//! The source identity hashes the Agg tag, output schema, ordered aggregate
//! descriptors, ordered group-by expressions, and PossibleProperties orders.
//! This leaf keeps that order over normalized column/expression and aggregate
//! metadata adapters; expression evaluation/type inference, full FieldType and
//! ByItems metadata, plan context, statistics, optimizer rules, and runtime
//! aggregation remain explicit external boundaries. `HasTiFlash` is omitted
//! because the source deliberately excludes it from Hash64/Equals.

use crate::aggregation_descriptor::AggFuncDesc;
use crate::hash_equaler::{new_hash_equaler, Hasher, NIL_FLAG, NOT_NIL_FLAG};

/// Normalized column-shaped expression identity used by aggregation metadata.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AggregationExprIdentity {
    id: i64,
    unique_id: i64,
    index: i64,
    type_fingerprint: Option<u64>,
}

impl AggregationExprIdentity {
    /// Creates an expression identity without normalized type metadata.
    #[must_use]
    pub const fn new(id: i64, unique_id: i64, index: i64) -> Self {
        Self {
            id,
            unique_id,
            index,
            type_fingerprint: None,
        }
    }

    /// Creates an expression identity with normalized type metadata.
    #[must_use]
    pub const fn with_type_fingerprint(
        id: i64,
        unique_id: i64,
        index: i64,
        type_fingerprint: u64,
    ) -> Self {
        Self {
            id,
            unique_id,
            index,
            type_fingerprint: Some(type_fingerprint),
        }
    }
}

/// Normalized aggregate ORDER BY item.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AggregationOrderIdentity {
    expr: AggregationExprIdentity,
    desc: bool,
}

impl AggregationOrderIdentity {
    /// Creates an aggregate ORDER BY identity.
    #[must_use]
    pub const fn new(expr: AggregationExprIdentity, desc: bool) -> Self {
        Self { expr, desc }
    }
}

/// Logical-plan identity view of the canonical aggregate descriptor.
pub type AggFuncIdentity =
    AggFuncDesc<AggregationExprIdentity, Option<u64>, AggregationOrderIdentity>;

/// PossibleProperties order identities. The source HasTiFlash runtime signal
/// is intentionally not represented because it is excluded from hash/equality.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PossiblePropertiesIdentity {
    orders: Option<Vec<Vec<AggregationExprIdentity>>>,
}

impl PossiblePropertiesIdentity {
    /// Creates source PossiblePropertiesInfo order metadata.
    #[must_use]
    pub fn new(orders: Option<Vec<Vec<AggregationExprIdentity>>>) -> Self {
        Self { orders }
    }

    /// Creates order metadata while explicitly discarding source HasTiFlash.
    #[must_use]
    pub fn new_with_has_tiflash(
        orders: Option<Vec<Vec<AggregationExprIdentity>>>,
        _has_tiflash: bool,
    ) -> Self {
        Self { orders }
    }
}

/// Minimal LogicalAggregation identity and generated Hash64/Equals fields.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LogicalAggregationIdentity {
    schema: Option<Vec<AggregationExprIdentity>>,
    agg_funcs: Option<Vec<AggFuncIdentity>>,
    group_by_items: Option<Vec<AggregationExprIdentity>>,
    possible_properties: PossiblePropertiesIdentity,
}

impl LogicalAggregationIdentity {
    /// Creates an identity from normalized source fields.
    #[must_use]
    pub fn new(
        schema: Option<Vec<AggregationExprIdentity>>,
        agg_funcs: Option<Vec<AggFuncIdentity>>,
        group_by_items: Option<Vec<AggregationExprIdentity>>,
        possible_properties: PossiblePropertiesIdentity,
    ) -> Self {
        Self {
            schema,
            agg_funcs,
            group_by_items,
            possible_properties,
        }
    }

    /// Computes generated Hash64 in source field order.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_string("Aggregation");
        hash_schema(&mut hasher, self.schema.as_deref());
        hash_agg_funcs(&mut hasher, self.agg_funcs.as_deref());
        hash_exprs(&mut hasher, self.group_by_items.as_deref());
        hash_possible_properties(&mut hasher, &self.possible_properties);
        hasher.sum64()
    }

    /// Compares generated Hash64/Equals identity fields.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

fn hash_schema(hasher: &mut impl Hasher, columns: Option<&[AggregationExprIdentity]>) {
    match columns {
        Some(columns) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            for column in columns {
                hash_expr(hasher, column);
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_exprs(hasher: &mut impl Hasher, expressions: Option<&[AggregationExprIdentity]>) {
    match expressions {
        Some(expressions) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_int(expressions.len() as i64);
            for expression in expressions {
                hash_expr(hasher, expression);
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_agg_funcs(hasher: &mut impl Hasher, funcs: Option<&[AggFuncIdentity]>) {
    match funcs {
        Some(funcs) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_int(funcs.len() as i64);
            for func in funcs {
                hash_agg_func(hasher, func);
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_agg_func(hasher: &mut impl Hasher, func: &AggFuncIdentity) {
    hasher.hash_string(&func.name);
    hasher.hash_int(func.args.len() as i64);
    for arg in &func.args {
        hash_expr(hasher, arg);
    }
    match func.ret_type {
        Some(fingerprint) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_uint64(fingerprint);
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
    hasher.hash_int(func.mode.ordinal());
    hasher.hash_bool(func.has_distinct);
    hasher.hash_int(func.order_by.len() as i64);
    for item in &func.order_by {
        hash_expr(hasher, &item.expr);
        hasher.hash_bool(item.desc);
    }
}

fn hash_possible_properties(hasher: &mut impl Hasher, properties: &PossiblePropertiesIdentity) {
    // PossiblePropertiesInfo is an embedded value in LogicalAggregation, so
    // its generated method always starts with the non-nil receiver marker.
    hasher.hash_byte(NOT_NIL_FLAG);
    match &properties.orders {
        Some(orders) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_int(orders.len() as i64);
            for order in orders {
                // Go hashes inner nil and empty slices by len only.
                hasher.hash_int(order.len() as i64);
                for column in order {
                    hash_expr(hasher, column);
                }
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_expr(hasher: &mut impl Hasher, expression: &AggregationExprIdentity) {
    match expression.type_fingerprint {
        Some(fingerprint) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_uint64(fingerprint);
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
    hasher.hash_int64(expression.id);
    hasher.hash_int64(expression.unique_id);
    hasher.hash_int(expression.index);
}

#[cfg(test)]
mod tests {
    use super::{
        AggFuncIdentity, AggregationExprIdentity, AggregationOrderIdentity,
        LogicalAggregationIdentity, PossiblePropertiesIdentity,
    };
    use crate::aggregation_descriptor::AggFunctionMode;

    fn column(unique_id: i64) -> AggregationExprIdentity {
        // The Go anchor's column has ID=0, UniqueID=0, Index=0 and a concrete
        // integer return type; UniqueID is varied only by the divergence cases.
        AggregationExprIdentity::with_type_fingerprint(0, unique_id, 0, 1)
    }

    fn aggregate() -> AggFuncIdentity {
        AggFuncIdentity::new(
            "AVG",
            vec![column(0)],
            Some(2),
            AggFunctionMode::Complete,
            true,
            Vec::new(),
        )
    }

    fn properties(orders: Option<Vec<Vec<AggregationExprIdentity>>>) -> PossiblePropertiesIdentity {
        PossiblePropertiesIdentity::new(orders)
    }

    fn base() -> LogicalAggregationIdentity {
        LogicalAggregationIdentity::new(
            None,
            Some(vec![aggregate()]),
            Some(vec![column(0)]),
            properties(Some(vec![vec![column(0)]])),
        )
    }

    fn assert_differs(first: &LogicalAggregationIdentity, second: &LogicalAggregationIdentity) {
        assert_ne!(first.hash64(), second.hash64());
        assert!(!first.equals(second));
    }

    #[test]
    fn original_test_agg_func_desc_hash_fields() {
        // Exact field-mutation sequence from
        // pkg/expression/aggregation/aggregation_test.go::TestAggFuncDesc,
        // executed through the source-shaped Hash64 encoder below rather
        // than Rust's derived Hash implementation.
        let mut first = base();
        let first_desc = &mut first.agg_funcs.as_mut().unwrap()[0];
        first_desc.name = "sum".to_owned();
        first_desc.has_distinct = false;
        first_desc.mode = AggFunctionMode::Complete;
        first_desc.args = vec![column(0)];
        first_desc.ret_type = Some(2);
        first_desc.order_by.clear();
        let mut second = first.clone();
        assert_eq!(first.hash64(), second.hash64());

        let desc = &mut second.agg_funcs.as_mut().unwrap()[0];
        desc.has_distinct = true;
        assert_differs(&first, &second);
        second.agg_funcs.as_mut().unwrap()[0].has_distinct = false;

        second.agg_funcs.as_mut().unwrap()[0].mode = AggFunctionMode::Final;
        assert_differs(&first, &second);
        second.agg_funcs.as_mut().unwrap()[0].mode = AggFunctionMode::Complete;

        second.agg_funcs.as_mut().unwrap()[0].name = "whatever".to_owned();
        assert_differs(&first, &second);
        second.agg_funcs.as_mut().unwrap()[0].name = "sum".to_owned();

        second.agg_funcs.as_mut().unwrap()[0].args.clear();
        assert_differs(&first, &second);
        second.agg_funcs.as_mut().unwrap()[0].args.push(column(0));

        second.agg_funcs.as_mut().unwrap()[0].ret_type = Some(3);
        assert_differs(&first, &second);
        second.agg_funcs.as_mut().unwrap()[0].ret_type = Some(2);

        second.agg_funcs.as_mut().unwrap()[0]
            .order_by
            .push(AggregationOrderIdentity::new(column(0), true));
        assert_differs(&first, &second);
    }

    #[test]
    fn source_test_matching_aggregation_has_equal_hash_and_identity() {
        let first = base();
        let second = base();
        assert_eq!(first.hash64(), second.hash64());
        assert!(first.equals(&second));
    }

    #[test]
    fn source_test_group_by_nil_and_empty_framing_changes_hash_and_equality() {
        let first = base();
        let mut second = base();
        second.group_by_items = Some(Vec::new());
        assert_differs(&first, &second);

        second.group_by_items = Some(vec![column(0)]);
        assert_eq!(first.hash64(), second.hash64());
        assert!(first.equals(&second));
    }

    #[test]
    fn source_test_possible_properties_order_changes_hash_and_equality() {
        let first = base();
        let mut second = base();
        second.possible_properties = properties(Some(vec![Vec::new()]));
        assert_differs(&first, &second);

        second.possible_properties = properties(Some(vec![vec![column(0)]]));
        assert_eq!(first.hash64(), second.hash64());
        assert!(first.equals(&second));
    }

    #[test]
    fn source_test_has_tiflash_is_excluded_from_hash_and_equality() {
        let first = LogicalAggregationIdentity::new(
            None,
            Some(vec![aggregate()]),
            Some(vec![column(0)]),
            PossiblePropertiesIdentity::new_with_has_tiflash(Some(vec![vec![column(0)]]), false),
        );
        let second = LogicalAggregationIdentity::new(
            None,
            Some(vec![aggregate()]),
            Some(vec![column(0)]),
            PossiblePropertiesIdentity::new_with_has_tiflash(Some(vec![vec![column(0)]]), true),
        );
        assert_eq!(first.hash64(), second.hash64());
        assert!(first.equals(&second));
    }

    #[test]
    fn source_test_aggregate_descriptor_fields_change_hash_and_equality() {
        let first = base();
        let mut second = base();

        second.agg_funcs = Some(vec![AggFuncIdentity::new(
            "sum",
            vec![column(0)],
            Some(2),
            AggFunctionMode::Complete,
            true,
            Vec::new(),
        )]);
        assert_differs(&first, &second);

        second.agg_funcs = Some(vec![AggFuncIdentity::new(
            "avg",
            vec![column(0)],
            Some(2),
            AggFunctionMode::Complete,
            true,
            vec![AggregationOrderIdentity::new(column(0), true)],
        )]);
        assert_differs(&first, &second);
    }

    #[test]
    fn source_test_schema_and_type_identity_change_hash_and_equality() {
        let first = base();
        let mut second = base();
        second.schema = Some(vec![column(0)]);
        assert_differs(&first, &second);

        second.schema = None;
        second.agg_funcs = Some(vec![AggFuncIdentity::new(
            "avg",
            vec![column(0)],
            Some(3),
            AggFunctionMode::Complete,
            true,
            Vec::new(),
        )]);
        assert_differs(&first, &second);
    }
}
