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

//! Go `preprocessor`'s name qualification (`pkg/planner/core/preprocess.go`):
//! `handleTableName` and the CTE scope (`preprocessWith`) it consults.

use std::any::Any;

use tidb_ast::{Visitable, Visitor};
use tidb_util::stringutil::go_to_lower;

use crate::plan_base::PlanError;

/// Query-local CTE visibility shared by physical-table admission and PREPARE
/// database pinning. Go's preprocessWith publishes ordinary names after their
/// definition, recursive names before it, and restores the enclosing scope.
#[derive(Default)]
pub struct CteScope {
    visible: Vec<String>,
    offsets: Vec<usize>,
    recursive: Vec<bool>,
}

impl CteScope {
    /// Tracks the scope a visitor enters; call from `Visitor::enter`.
    pub fn enter(&mut self, node: &mut dyn Any) {
        if node.is::<tidb_ast::SelectStmt>() || node.is::<tidb_ast::SetOprStmt>() {
            self.offsets.push(self.visible.len());
        } else if let Some(with) = node.downcast_mut::<tidb_ast::WithClause>() {
            self.recursive.push(with.recursive);
        } else if let Some(cte) = node.downcast_mut::<tidb_ast::Cte>() {
            self.offsets.push(self.visible.len());
            if self.recursive.last().copied().unwrap_or(false) {
                self.visible.push(go_to_lower(cte.name.clone()));
            }
        }
    }

    /// Tracks the scope a visitor leaves; call from `Visitor::leave`.
    pub fn leave(&mut self, node: &mut dyn Any) {
        if node.is::<tidb_ast::SelectStmt>() || node.is::<tidb_ast::SetOprStmt>() {
            self.visible
                .truncate(self.offsets.pop().expect("query scope entered"));
        } else if node.is::<tidb_ast::WithClause>() {
            self.recursive.pop();
        } else if let Some(cte) = node.downcast_mut::<tidb_ast::Cte>() {
            self.visible
                .truncate(self.offsets.pop().expect("CTE scope entered"));
            self.visible.push(go_to_lower(cte.name.clone()));
        }
    }

    /// Whether `path` is an unqualified name of a CTE visible here.
    #[must_use]
    pub fn contains(&self, path: &[String]) -> bool {
        matches!(path, [name] if self.visible.contains(&go_to_lower(name.clone())))
    }
}

/// Go `preprocessor.handleTableName` (`pkg/planner/core/preprocess.go:1758`):
/// every unqualified table name that does not name a CTE takes the current
/// database. PREPARE runs it on the statement it retains, so the stored
/// `VisitInfos` and every later EXECUTE -- after any `USE` -- resolve against
/// the database current at PREPARE (`PlanCacheStmt.StmtDB`); an unqualified
/// name with no current database is Go's `ErrNoDB`.
///
/// # Errors
///
/// `ErrNoDB` when an unqualified name has no current database to take.
pub fn handle_table_name<N: Visitable>(node: &mut N, current_db: &str) -> Result<(), PlanError> {
    struct Pinner<'a> {
        current_db: &'a str,
        scope: CteScope,
        unresolved: bool,
    }
    impl Pinner<'_> {
        fn pin(&mut self, path: &mut Vec<String>) {
            if path.len() != 1 {
                return;
            }
            if self.current_db.is_empty() {
                self.unresolved = true;
                return;
            }
            path.insert(0, self.current_db.to_owned());
        }
    }
    impl Visitor for Pinner<'_> {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            self.scope.enter(node);
            if let Some(table_ref) = node.downcast_mut::<tidb_ast::TableRef>() {
                if !self.scope.contains(&table_ref.name) {
                    self.pin(&mut table_ref.name);
                }
            } else if let Some(insert) = node.downcast_mut::<tidb_ast::InsertStmt>() {
                self.pin(&mut insert.table);
            } else if let Some(import) = node.downcast_mut::<tidb_ast::ImportIntoStmt>() {
                self.pin(&mut import.table);
            } else if let Some(distribute) = node.downcast_mut::<tidb_ast::DistributeTableStmt>() {
                self.pin(&mut distribute.table);
            } else if let Some(create) = node.downcast_mut::<tidb_ast::CreateTableStmt>() {
                self.pin(&mut create.name);
            } else if let Some(create) = node.downcast_mut::<tidb_ast::CreateViewStmt>() {
                self.pin(&mut create.name);
            } else if let Some(create) = node.downcast_mut::<tidb_ast::CreateSequenceStmt>() {
                self.pin(&mut create.name);
            } else if let Some(drop) = node.downcast_mut::<tidb_ast::DropTableStmt>() {
                for name in &mut drop.names {
                    self.pin(name);
                }
            } else if let Some(alter) = node.downcast_mut::<tidb_ast::AlterTableStmt>() {
                self.pin(&mut alter.name);
            } else if let Some(create) = node.downcast_mut::<tidb_ast::CreateIndexStmt>() {
                self.pin(&mut create.table);
            } else if let Some(drop) = node.downcast_mut::<tidb_ast::DropIndexStmt>() {
                self.pin(&mut drop.table);
            } else if let Some(split) = node.downcast_mut::<tidb_ast::SplitRegionStmt>() {
                self.pin(&mut split.table);
            }
            false
        }

        fn leave(&mut self, node: &mut dyn Any) -> bool {
            self.scope.leave(node);
            true
        }
    }
    let mut pinner = Pinner {
        current_db,
        scope: CteScope::default(),
        unresolved: false,
    };
    node.accept(&mut pinner);
    if pinner.unresolved {
        return Err(PlanError::no_db());
    }
    Ok(())
}

/// Go `preprocessor.resolveShowStmt` (`pkg/planner/core/preprocess.go:2028`):
/// a SHOW without a database takes its table's schema, else the current
/// database; one with a database qualifies an unqualified table with it; and
/// `CURRENT_USER` becomes the session's authenticated account
/// (`auth_user`), when the session has one.
pub fn resolve_show_stmt(
    node: &mut tidb_ast::ShowStmt,
    current_db: &str,
    auth_user: Option<(&str, &str)>,
) {
    if node.db_name.is_empty() {
        node.db_name = match node.table.as_deref() {
            Some([schema, _]) if !schema.is_empty() => schema.clone(),
            _ => current_db.to_owned(),
        };
    } else if let Some(table) = node.table.as_mut() {
        if let [name] = table.as_slice() {
            *table = vec![node.db_name.clone(), name.clone()];
        }
    }
    if let (Some(user), Some((auth_username, auth_hostname))) = (node.user.as_mut(), auth_user) {
        if user.current_user {
            user.user = auth_username.to_owned();
            user.host = auth_hostname.to_owned();
        }
    }
}
