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

//! Go `base.Plan` as `PlanBuilder.Build` and `planner.Optimize` return it,
//! and the plans of `pkg/planner/core/common_plans.go` that are not
//! operator trees.
//!
//! SEED: the common plans grow here as the statements that build them move
//! onto the planner; today only `ShowNextRowID`.

use tidb_datatype::FieldName;
use tidb_expr::schema::Schema;

use crate::logical::LogicalPlan;
use crate::physical::PhysicalPlan;

/// Go `ShowNextRowID` (`common_plans.go:83`): `SHOW TABLE t NEXT_ROW_ID`,
/// with `buildShowNextRowID`'s schema.
#[derive(Clone, Debug)]
pub struct ShowNextRowID {
    /// Go `TableName`, as its name path (`schema, table` once preprocessed).
    pub table_name: Vec<String>,
    /// Go `SimpleSchemaProducer`'s schema.
    pub schema: Schema,
    /// Go `SimpleSchemaProducer`'s output names.
    pub names: Vec<FieldName>,
}

/// Go `base.Plan`: an operator tree before or after optimization, or one of
/// the common plans.
#[derive(Clone, Debug)]
pub enum Plan {
    /// A logical operator tree (Go `base.LogicalPlan`).
    Logical(LogicalPlan),
    /// A physical operator tree (Go `base.PhysicalPlan`).
    Physical(PhysicalPlan),
    /// Go `ShowNextRowID`.
    ShowNextRowID(ShowNextRowID),
}
