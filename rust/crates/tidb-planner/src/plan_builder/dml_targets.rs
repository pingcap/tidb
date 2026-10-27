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

//! Go `buildDelete`'s resolution of `DELETE t1, t2 FROM ...` targets
//! (`logical_plan_builder.go:6580-6627`) over `collectTableName`
//! (`:7662`): a target names a source of the `FROM` by its alias, or, when the
//! source has none, by `schema.table`; only a source that is a base table may
//! be deleted from.

use std::collections::BTreeMap;

use tidb_ast::{DeleteKind, DeleteStmt, Join, JoinNode, TableRef};
use tidb_hack::go_to_lower;

use super::catalog::TableSource;
use super::delete_pruning::DeleteTarget;
use super::PlanBuilder;
use crate::plan_base::PlanError;

/// What Go's `collectTableName` records for the sources of a `FROM`.
#[derive(Default)]
struct CollectedNames<'a> {
    /// `updatableName`: whether a write may name the source.
    updatable: BTreeMap<String, bool>,
    /// `info`: the table a name stands for, when its source is a table name.
    tables: BTreeMap<String, &'a TableRef>,
}

fn is_cte(delete: &DeleteStmt, table: &TableRef) -> bool {
    let [name] = table.name.as_slice() else {
        return false;
    };
    delete.with.as_ref().is_some_and(|with| {
        with.ctes
            .iter()
            .any(|cte| go_to_lower(&cte.name) == go_to_lower(name))
    })
}

fn collect_table_name<'a>(
    delete: &DeleteStmt,
    current_database: &str,
    node: &'a JoinNode,
    names: &mut CollectedNames<'a>,
) {
    match node {
        JoinNode::Join(join) => collect_join(delete, current_database, join, names),
        JoinNode::Table(table) => {
            let cte = is_cte(delete, table);
            let name = match &table.alias {
                Some(alias) => go_to_lower(alias),
                None => match table.name.as_slice() {
                    // A CTE reference has no schema.
                    [name] if cte => go_to_lower(name),
                    // Go's preprocessor fills the schema of a base table; its
                    // key is `schema.name`, the current database when the
                    // statement left the schema out.
                    [name] => format!("{current_database}.{}", go_to_lower(name)),
                    path => path
                        .iter()
                        .map(|part| go_to_lower(part))
                        .collect::<Vec<_>>()
                        .join("."),
                },
            };
            names.tables.insert(name.clone(), table);
            // `canUpdate && s.Schema.L != ""`.
            names.updatable.insert(name, !cte);
        }
        JoinNode::Derived { alias, .. } => {
            if let Some(alias) = alias {
                names.updatable.insert(go_to_lower(alias), false);
            }
        }
    }
}

fn collect_join<'a>(
    delete: &DeleteStmt,
    current_database: &str,
    join: &'a Join,
    names: &mut CollectedNames<'a>,
) {
    collect_table_name(delete, current_database, &join.left, names);
    if let Some(right) = &join.right {
        collect_table_name(delete, current_database, right, names);
    }
}

impl<S: TableSource, C: tidb_expr::Columns> PlanBuilder<'_, S, C> {
    /// The tables a multi-table DELETE removes rows from, in the order its
    /// target list names them, each once.
    ///
    /// `ErrUnknownTable` ("MULTI DELETE") for a name the `FROM` does not
    /// provide, `ErrNonUpdatableTable` for one it provides as a derived
    /// table or CTE, and Go's plain refusal of a view or a sequence.
    pub fn resolve_delete_targets(
        &self,
        delete: &DeleteStmt,
    ) -> Result<Vec<DeleteTarget>, PlanError> {
        let DeleteKind::Multi { targets, from, .. } = &delete.kind else {
            return Err(PlanError::internal(
                "target resolution is for a multi-table DELETE",
            ));
        };
        let mut names = CollectedNames::default();
        let current_database = go_to_lower(self.source.current_database());
        collect_join(delete, &current_database, from, &mut names);
        let mut resolved: Vec<DeleteTarget> = Vec::new();
        for target in targets {
            let (schema, written) = match target.as_slice() {
                [name] => (String::new(), name.as_str()),
                [schema, name] => (go_to_lower(schema), name.as_str()),
                _ => {
                    return Err(PlanError::unknown_table_in_clause(
                        target.join("."),
                        "MULTI DELETE",
                    ))
                }
            };
            let name = go_to_lower(written);
            let mut found = if schema.is_empty() {
                names.updatable.get(&name).map(|can| (*can, name.clone()))
            } else {
                None
            };
            if found.is_none() {
                let key = if schema.is_empty() {
                    format!("{current_database}.{name}")
                } else {
                    format!("{schema}.{name}")
                };
                found = names.updatable.get(&key).map(|can| (*can, key));
            }
            let Some((can_update, key)) = found else {
                return Err(PlanError::unknown_table_in_clause(written, "MULTI DELETE"));
            };
            if !can_update {
                return Err(PlanError::non_updatable_table(written, "DELETE"));
            }
            let table_ref = names
                .tables
                .get(&key)
                .ok_or_else(|| PlanError::internal("an updatable name has no table"))?;
            let (database, table) = match table_ref.name.as_slice() {
                [table] => (self.source.current_database(), table.as_str()),
                [database, table] => (database.as_str(), table.as_str()),
                _ => return Err(PlanError::internal("a table reference has no name")),
            };
            if self.source.find_view(database, table).is_some() {
                return Err(PlanError::internal(format!(
                    "delete view {written} is not supported now"
                )));
            }
            if self.source.find_sequence(database, table).is_some() {
                return Err(PlanError::internal(format!(
                    "delete sequence {written} is not supported now"
                )));
            }
            let meta = self
                .source
                .find_table(database, table)
                .ok_or_else(|| PlanError::unknown_table(database, table))?;
            let target = DeleteTarget {
                database: meta.db_name.clone(),
                table: meta.table_name.clone(),
                visible: name,
            };
            if !resolved.iter().any(|existing| {
                existing.database == target.database
                    && existing.table == target.table
                    && existing.visible == target.visible
            }) {
                resolved.push(target);
            }
        }
        Ok(resolved)
    }
}
