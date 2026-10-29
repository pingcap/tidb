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

//! Go `buildUpdateLists`'s resolution of each `SET` column
//! (`logical_plan_builder.go:6335-6372`): the column is found among the
//! read's output names, and the source it belongs to must be one of the
//! statement's updatable tables (`updatableTableListResolver`: the base tables
//! of the outer `FROM`, never a derived table's), which names the base table
//! the write -- and its `UpdatePriv` visit -- is for.

use tidb_ast::{Assignment, JoinNode, TableRef, UpdateKind, UpdateStmt};
use tidb_datatype::{FieldNameMetadata, QualifiedColumnName};
use tidb_hack::go_to_lower;

use super::PlanBuilder;
use tidb_infoschema::table_source::TableSource;
use super::dml_source::{dml_source_select, single_table_join};
use crate::plan_base::PlanError;

/// A table column an UPDATE writes, identified by the base table (as the
/// statement spelled it) and the column's spelling in the read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateTarget {
    /// The base table's schema.
    pub database: String,
    /// The base table's name.
    pub table: String,
    /// The column's name.
    pub column: String,
}

pub(super) fn is_cte(update: &UpdateStmt, table: &TableRef) -> bool {
    let [name] = table.name.as_slice() else {
        return false;
    };
    update.with.as_ref().is_some_and(|with| {
        with.ctes
            .iter()
            .any(|cte| go_to_lower(&cte.name) == go_to_lower(name))
    })
}

pub(super) fn collect_tables<'a>(node: &'a JoinNode, tables: &mut Vec<&'a TableRef>) {
    match node {
        JoinNode::Table(table) => tables.push(table),
        JoinNode::Join(join) => {
            collect_tables(&join.left, tables);
            if let Some(right) = &join.right {
                collect_tables(right, tables);
            }
        }
        JoinNode::Derived { .. } => {}
    }
}

/// The name a column path (`c`, `t.c` or `d.t.c`) is written with.
pub(super) fn path_column(path: &[String]) -> Result<QualifiedColumnName, PlanError> {
    match path {
        [column] => Ok(QualifiedColumnName::new("", "", column)),
        [table, column] => Ok(QualifiedColumnName::new("", table, column)),
        [database, table, column] => Ok(QualifiedColumnName::new(database, table, column)),
        _ => Err(PlanError::from(
            tidb_error::plannererrors::ERR_UNKNOWN_COLUMN.generate_with_stack_by_args(&[
                tidb_hack::go_to_lower(path.join(".")).into(),
                "field list".into(),
            ]),
        )),
    }
}

/// The name a `SET` column is written with.
pub(super) fn assignment_column(assignment: &Assignment) -> Result<QualifiedColumnName, PlanError> {
    path_column(&assignment.col)
}

impl<S: TableSource, C: tidb_expr::Columns> PlanBuilder<'_, S, C> {
    /// A table reference's schema and name, the current database filling a
    /// missing schema.
    pub(super) fn update_table_path(&self, table: &TableRef) -> Result<(String, String), PlanError> {
        match table.name.as_slice() {
            [_] if self.source.current_database().is_empty() => Err(PlanError::from(
                tidb_error::plannererrors::ERR_NO_DB.generate_with_stack_by_args(&[]),
            )),
            [name] => Ok((self.source.current_database().to_owned(), name.clone())),
            [database, name] => Ok((database.clone(), name.clone())),
            _ => Err(PlanError::internal("a table reference has no name")),
        }
    }

    /// Go `buildUpdateLists`'s `foundListItem` loop: the updatable table the
    /// output name `name` belongs to, among `tables`, the outer `FROM`'s
    /// table sources.
    pub(super) fn updatable_table_of(
        &self,
        update: &UpdateStmt,
        tables: &[&TableRef],
        name: &FieldNameMetadata,
    ) -> Result<UpdateTarget, PlanError> {
        let mut target = None;
        for table in tables {
            // Go `buildUpdateLists`: `if isCTE(tlW) || IsView() ||
            // IsSequence()` is 1288. A CTE is no stored table, so it is
            // recognised before any schema is resolved for it.
            if is_cte(update, table) {
                let visible = table
                    .alias
                    .as_deref()
                    .or_else(|| table.name.last().map(String::as_str))
                    .unwrap_or_default();
                if go_to_lower(visible) == name.table.lower {
                    break;
                }
                continue;
            }
            let (database, original) = self.update_table_path(table)?;
            let visible = table.alias.as_deref().unwrap_or(&original);
            // Go `updatableTableListResolver.Leave` lists an aliased table
            // under its alias with `Schema` blanked, and `buildUpdateLists`
            // skips the schema check for a blank one.
            if go_to_lower(visible) != name.table.lower
                || (table.alias.is_none() && go_to_lower(&database) != name.database.lower)
            {
                continue;
            }
            if self.source.find_table(&database, &original).is_some() {
                target = Some(UpdateTarget {
                    database,
                    table: original,
                    column: name.column.original.clone(),
                });
            }
            break;
        }
        target.ok_or_else(|| {
            PlanError::from(
                tidb_error::plannererrors::ERR_NON_UPDATABLE_TABLE.generate_with_stack_by_args(&[
                    (name.table.original.clone()).into(),
                    "UPDATE".into(),
                ]),
            )
        })
    }

    /// The columns an UPDATE writes, each once, in assignment order.
    pub fn resolve_update_targets(
        &mut self,
        update: &UpdateStmt,
    ) -> Result<Vec<UpdateTarget>, PlanError> {
        // A fast single-table update has only one possible write owner; a CTE
        // target resolves its columns like any source before Go refuses it.
        let from = match &update.kind {
            UpdateKind::Single(table) if !is_cte(update, table) => {
                let (database, name) = self.update_table_path(table)?;
                return Ok(update
                    .assignments
                    .iter()
                    .filter_map(|assignment| assignment.col.last())
                    .map(|column| UpdateTarget {
                        database: database.clone(),
                        table: name.clone(),
                        column: column.clone(),
                    })
                    .collect());
            }
            UpdateKind::Single(table) => single_table_join(table),
            UpdateKind::Multi { from, .. } => (**from).clone(),
        };
        let select = dml_source_select(update.with.as_ref(), &[], from.clone(), None, &[], None);
        // Go `buildUpdate` sets `b.inUpdateStmt` before `buildResultSetNode`,
        // so a NATURAL / USING join keeps both sides' columns (`b.b` stays
        // nameable in SET) instead of the coalesced SELECT schema.
        let in_update_or_delete_stmt = std::mem::replace(&mut self.in_update_or_delete_stmt, true);
        let plan = self.with_dml_ctes(&select, |builder| {
            builder.build_table_refs(select.from.as_ref())
        });
        self.in_update_or_delete_stmt = in_update_or_delete_stmt;
        let plan = plan?;
        // Go's updatableTableListResolver visits only the outer table sources;
        // a derived query's base tables do not make that derived source writable.
        let mut tables = Vec::new();
        collect_tables(&from.left, &mut tables);
        if let Some(right) = &from.right {
            collect_tables(right, &mut tables);
        }
        let mut targets: Vec<UpdateTarget> = Vec::new();
        for assignment in &update.assignments {
            let column = assignment_column(assignment)?;
            let index =
                tidb_expr::find_field_name(plan.output_names(), &column)
                    .map_err(|_| {
                        PlanError::from(
                            tidb_error::plannererrors::ERR_AMBIGUOUS.generate_with_stack_by_args(
                                &[(column.display_name()).into(), "field list".into()],
                            ),
                        )
                    })?
                    .ok_or_else(|| {
                        PlanError::from(
                            tidb_error::plannererrors::ERR_UNKNOWN_COLUMN
                                .generate_with_stack_by_args(&[
                                    tidb_hack::go_to_lower(column.column.original.clone()).into(),
                                    "field list".into(),
                                ]),
                        )
                    })?;
            let target =
                self.updatable_table_of(update, &tables, &plan.output_names()[index].names)?;
            // One write target per table column, compared as Go's `.L` names.
            if !targets.iter().any(|existing| {
                go_to_lower(&existing.database) == go_to_lower(&target.database)
                    && go_to_lower(&existing.table) == go_to_lower(&target.table)
                    && go_to_lower(&existing.column) == go_to_lower(&target.column)
            }) {
                targets.push(target);
            }
        }
        Ok(targets)
    }
}
