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


//! Go `PlanBuilder.buildSet` (`pkg/planner/core/planbuilder.go`): a `SET`
//! statement planned as Go's `core.Set`, every value rewritten over a
//! one-row dual and a value whose rewrite is not a dual evaluated to its
//! first row while the statement is built.

use tidb_ast::{Expr, SetStmt};
use tidb_expr::constant::Constant;
use tidb_expr::expression::Expression;
use tidb_hack::go_to_lower;

use tidb_expr::Columns;

use tidb_infoschema::table_source::TableSource;
use super::visit_info::VisitAuthError;
use super::{PlanBuilder, SubqueryKind};
use crate::common_plans::{Plan, Set, VarAssignment};
use crate::logical::{LogicalPlan, LogicalTableDual};
use crate::plan_base::PlanError;

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `buildSet`.
    ///
    /// # Errors
    ///
    /// Go `ErrNotSupportedInNextGen` for a variable read-only in the next-gen
    /// kernel, and any rewrite or subquery-evaluation error of a value.
    pub fn build_set(&mut self, set: &SetStmt) -> Result<Plan, PlanError> {
        let mut plan = Set::default();
        for vars in &set.variables {
            if vars.is_global || vars.is_instance {
                self.append_dynamic_visit(
                    &["SYSTEM_VARIABLES_ADMIN"],
                    false,
                    VisitAuthError::SpecificAccessDenied {
                        privilege: "SUPER or SYSTEM_VARIABLES_ADMIN",
                    },
                );
            }
            let var_name = go_to_lower(vars.name.as_str());
            if tidb_sem_compat::is_enabled()
                && tidb_sem_compat::is_invisible_sys_var(&var_name)
            {
                self.append_dynamic_visit(
                    &["RESTRICTED_VARIABLES_ADMIN"],
                    false,
                    VisitAuthError::SpecificAccessDenied {
                        privilege: "RESTRICTED_VARIABLES_ADMIN",
                    },
                );
            }
            if tidb_config_kerneltype::is_next_gen()
                && tidb_vardef::is_read_only_var_in_next_gen(&var_name)
            {
                return Err(PlanError::eval(tidb_expr::EvalError::Conversion(
                    tidb_error::catalog::variable::ERR_NOT_SUPPORTED_IN_NEXT_GEN
                        .generate(format!("setting {var_name} is not supported in the next generation of TiDB")),
                )));
            }
            let mut assign = VarAssignment {
                name: vars.name.clone(),
                expr: None,
                is_default: false,
                is_global: vars.is_global,
                is_system: vars.is_system,
                is_instance: vars.is_instance,
                // Go `ExtendValue`: the collation literal's datum.
                extend_value: vars.extend_value.as_ref().map(|value| match value {
                    Expr::String(text) | Expr::RawString(text) => text.clone(),
                    other => other.restore(),
                }),
            };
            if matches!(vars.value, Expr::Default(None)) {
                assign.is_default = true;
            } else {
                assign.expr = Some(self.rewrite_set_value(&vars.value)?);
            }
            plan.var_assigns.push(assign);
        }
        Ok(Plan::Set(plan))
    }

    /// The value arm of Go `buildSet`: a column name without a table is its
    /// own name as a string, the value is rewritten over a one-row
    /// `LogicalTableDual`, and when the rewrite built a plan other than that
    /// dual (an IN/EXISTS lowered to an apply) the plan is optimized and
    /// evaluated to its first row (`DoOptimize` + `EvalSubqueryFirstRow`),
    /// whose first datum becomes a constant of the rewritten type.
    fn rewrite_set_value(&mut self, value: &Expr) -> Result<Expression, PlanError> {
        let mut value = match value {
            // Go: `cn.Name.Table.L == ""` converts to a string value with
            // the session's connection charset and collation.
            Expr::Column(path) if path.len() == 1 => Expr::String(path[0].clone()),
            other => other.clone(),
        };
        // The mocked plan needs one output row for the complex cases.
        let dual = LogicalTableDual::new(self.base(LogicalTableDual::TYPE), 1);
        let (possible_plan, _) =
            self.lower_scalar_subqueries(LogicalPlan::TableDual(dual), &mut value)?;
        let expr = self.rewrite_scalar_with_plan(
            &value,
            &possible_plan,
            &std::collections::BTreeMap::new(),
        )?;
        if matches!(possible_plan, LogicalPlan::TableDual(_)) {
            return Ok(expr);
        }
        let Some(evaluator) = self.subquery_evaluator else {
            return Err(PlanError::internal(
                "a SET value's subquery needs the executor's evaluation hook",
            ));
        };
        let ret_type = expr
            .static_type()
            .cloned()
            .ok_or_else(|| PlanError::internal("a SET value has no type"))?;
        let evaluated = evaluator(&possible_plan, SubqueryKind::Scalar, self.get_opt_flag())?;
        let datum = evaluated.row.into_iter().next().unwrap_or_default();
        Ok(Expression::Constant(Constant::new(datum, ret_type)))
    }
}
