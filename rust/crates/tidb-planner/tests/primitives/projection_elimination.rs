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

//! Go `canProjectionBeEliminatedLoose` over the real logical operator.

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::expression::Expression;
use tidb_planner::logical::projection::LogicalProjection;
use tidb_planner::logical::rule::RuleId;
use tidb_planner::logical::BaseLogicalPlan;

fn column(unique_id: i64) -> Expression {
    Expression::Column(Column::new(
        unique_id,
        FieldType::new(FieldTypeCode::LongLong),
    ))
}

fn projection(expressions: Vec<Expression>) -> LogicalProjection {
    LogicalProjection::new(BaseLogicalPlan::default(), expressions)
}

#[test]
fn empty_projection_is_loosely_eliminable_when_not_expand() {
    assert!(projection(Vec::new()).can_be_eliminated_loose());
}

#[test]
fn direct_column_projection_is_loosely_eliminable() {
    assert!(projection(vec![column(1), column(2), column(3)]).can_be_eliminated_loose());
}

#[test]
fn computed_expression_blocks_loose_elimination() {
    let computed = Expression::Constant(Constant::new(
        Datum::new_int(1),
        FieldType::new(FieldTypeCode::LongLong),
    ));
    assert!(!projection(vec![column(1), computed]).can_be_eliminated_loose());
}

#[test]
fn expand_projection_blocks_even_column_only_shape() {
    let mut projection = projection(vec![column(1), column(2)]);
    projection.proj4_expand = true;
    assert!(!projection.can_be_eliminated_loose());
}

#[test]
fn source_rule_name_is_stable() {
    assert_eq!(RuleId::ProjectionEliminator.name(), "projection_eliminate");
}
