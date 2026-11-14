// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `PlanBuilder.buildInsert`: one builder owns the target schema, source,
//! VALUES, ON DUPLICATE and generated columns for the lifetime of the build.

use tidb_ast::InsertStmt;
use tidb_expr::schema::Schema;
use tidb_exprctx::Columns;
use tidb_infoschema::table_source::TableSource;
use tidb_planner_core_base::PlanError;
use tidb_planner_core_operator_logicalop::LogicalPlan;
use tidb_planner_core_operator_physicalop::{
    BasePhysicalPlan, FkTriggerNode, InsertSelectOutput, PhysicalDmlRoot, PhysicalPlan,
};

use super::PlanBuilder;
use super::insert_values::{InsertTarget, InsertValues};

pub use tidb_planner_core_operator_physicalop::InsertExpressions;

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `PlanBuilder.buildInsert`. The callbacks supply `DoOptimize` and
    /// `Insert.BuildOnInsertFKTriggers` session services; neither builds the
    /// source AST. All expression and query building uses this receiver.
    pub fn build_insert(
        &mut self,
        insert: &InsertStmt,
        mut do_optimize: impl FnMut(&Self, LogicalPlan, u64) -> Result<PhysicalPlan, PlanError>,
        build_fk_triggers: impl FnOnce(
            &InsertTarget,
            &[String],
        ) -> Result<Vec<FkTriggerNode>, PlanError>,
    ) -> Result<PhysicalPlan, PlanError> {
        self.add_opt_flag(tidb_planner_core_rule::flags::PRUNE_COLUMNS);
        let target = self.resolve_insert_target(insert)?;
        self.table_info_2_schema_and_names(&target);
        let base = BasePhysicalPlan::new(self.plan_ids, "Insert", 0);
        self.append_insert_target_visits(insert, &target.database, &target.table);
        // Go's mockTablePlan, allocated after Insert and before its source.
        self.plan_ids.alloc();
        let (select_plan, insert_select) = if insert.source.is_some() {
            let (plan, output) =
                self.build_select_plan_of_insert(insert, &target, &mut do_optimize)?;
            (Some(Box::new(plan)), Some(output))
        } else {
            (None, None)
        };
        // Keep the existing physical FK allocation order while moving the
        // INSERT build into this receiver. FK planning is a separate owner.
        // ResolveOnDuplicate below owns assignment errors, after VALUES.
        let on_duplicate_columns = self
            .build_on_duplicate_update_columns(insert, &target)
            .unwrap_or_else(|_| {
                insert
                    .on_duplicate
                    .iter()
                    .filter_map(|assignment| assignment.col.last().cloned())
                    .collect()
            });
        let mut fk_triggers = build_fk_triggers(&target, &on_duplicate_columns)?;
        fk_triggers.sort_by_key(|node| node.operator == "Foreign_Key_Cascade");
        let plan_id = self.plan_ids.current();
        let select = select_plan
            .as_deref()
            .and_then(PhysicalPlan::schema)
            .zip(insert_select.as_ref());
        let expressions = self.build_insert_expressions(insert, &target, select)?;
        Ok(PhysicalPlan::Dml(PhysicalDmlRoot {
            base,
            go_operator: "Insert".to_owned(),
            select_plan,
            insert_select,
            fk_triggers,
            insert_expressions: Some(expressions),
            plan_id,
            ..Default::default()
        }))
    }

    /// Go `PlanBuilder.buildSelectPlanOfInsert`: extend the AST temporarily,
    /// call this builder's `Build`, check its outputs, then `DoOptimize`.
    /// Cloning only the extended AST leaves PREPARE's original untouched.
    fn build_select_plan_of_insert(
        &mut self,
        insert: &InsertStmt,
        target: &InsertTarget,
        do_optimize: &mut impl FnMut(&Self, LogicalPlan, u64) -> Result<PhysicalPlan, PlanError>,
    ) -> Result<(PhysicalPlan, InsertSelectOutput), PlanError> {
        self.is_for_update_read = true;
        let extended = self.extend_insert_select(insert, target);
        let (query, actual_col_len) = match &extended {
            Some((query, width)) => (query, Some(*width)),
            None => (
                insert.source.as_deref().expect("INSERT SELECT source"),
                None,
            ),
        };
        let node = tidb_resolve::NodeW::new(query.clone());
        let logical = self.build_query_node(&node, false)?;
        self.check_insert_select(
            target,
            actual_col_len.unwrap_or_else(|| logical.schema().map_or(0, Schema::len)),
        )?;
        let names = logical.output_names().to_vec();
        let physical = do_optimize(self, logical, self.get_opt_flag())?;
        Ok((
            physical,
            InsertSelectOutput {
                names,
                actual_col_len,
            },
        ))
    }

    /// Go `PlanBuilder.buildInsert`'s expression phase. Also used when the
    /// prepared plan cache supplies an already-built SELECT source.
    pub fn build_insert_expressions(
        &mut self,
        insert: &InsertStmt,
        target: &InsertTarget,
        select: Option<(&Schema, &InsertSelectOutput)>,
    ) -> Result<InsertExpressions, PlanError> {
        let values = if insert.source.is_none() {
            self.build_values_list_of_insert(insert, target)?
        } else {
            InsertValues::default()
        };
        if let Some((schema, output)) = select {
            self.check_insert_select(target, output.actual_col_len.unwrap_or(schema.len()))?;
        }
        let on_duplicate = self.resolve_on_duplicate(insert, target, select)?;
        let generated = self.resolve_generated_columns(target)?;
        Ok(InsertExpressions {
            lists: values.lists,
            on_duplicate,
            generated,
        })
    }
}
