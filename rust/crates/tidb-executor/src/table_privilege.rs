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

//! The table-scope privileges one statement demands -- Go's `visitInfo`.
//!
//! Go collects these while planning and checks them after name resolution.
//! UPDATE and DELETE targets below come from shared resolved source metadata;
//! SQL spelling alone cannot identify the owner of an unqualified column.
//! Other statement collectors still use the AST and remain a parity boundary.

use tidb_privilege::PrivilegeTypeExt;
use tidb_ast::{DmlStmt, Stmt};

use tidb_privilege::PrivilegeType;

/// Go VisitInfo4PrivCheck keeps observation visits while adapting admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemporaryPrivilege {
    SkipLocal,
    Check,
    CreateLocal,
    Skip,
}

/// One `visitInfo` entry: a privilege demanded on one table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TablePrivilegeRequest {
    /// Schema name, already resolved against the session's current database.
    pub database: String,
    /// Table name as written.
    pub table: String,
    /// The privilege Go's `appendVisitInfo` recorded.
    pub privilege: PrivilegeType,
    /// Whether Go attached a statement-specific `authErr` naming the table
    /// (`ErrTableaccessDenied`, 1142). Where it did not, `CheckPrivilege`
    /// falls to `ErrPrivilegeCheckFail` (8121) -- which is what a denied
    /// `UPDATE`'s `SET` target reports, since
    /// `logical_plan_builder.go`'s `buildNewAssignments` passes `nil`.
    pub table_named_in_error: bool,
    /// Whether Go reports a database-scoped denial (1044) instead of the
    /// table-scoped 1142 form. `CREATE/DROP DATABASE` carry a schema name but
    /// no table, and their planner visitInfo attaches exactly that error.
    pub database_named_in_error: bool,
    /// The database Go's `ErrDBaccessDenied` names, when it differs from the
    /// checked scope (a `LOCK TABLES` visit names its schema).
    pub database_in_error: Option<String>,
    /// Adapt admission without discarding original observation table names.
    pub temporary_privilege: TemporaryPrivilege,
    /// Go authErr can name a different command/table than the checked visit.
    pub table_error_override: Option<(&'static str, String)>,
    /// Go `appendDynamicVisitInfo`: dynamic privileges any one of which
    /// grants the visit (SUPER standing in for each); empty for an ordinary
    /// static-privilege visit.
    pub dynamic_privileges: Vec<String>,
    /// Go `dynamicWithGrant`: the dynamic privilege is needed WITH GRANT
    /// OPTION.
    pub dynamic_with_grant: bool,
    /// Go's `ErrSpecificAccessDenied` authErr (1227), naming what the
    /// statement needs.
    pub specific_denial: Option<&'static str>,
    /// Further static privileges that grant the visit as well: Go checks a
    /// combined mask (`RequestVerification` asks `priv & mask > 0`).
    pub also_granted_by: Vec<PrivilegeType>,
    /// Go's `ErrViewNoExplain` authErr (1345): the SHOW VIEW visit
    /// `BuildDataSourceFromView` appends under EXPLAIN.
    pub view_no_explain: bool,
}

impl TablePrivilegeRequest {
    pub fn new(database: &str, table: &str, privilege: PrivilegeType) -> Self {
        Self {
            database: database.to_owned(),
            table: table.to_owned(),
            privilege,
            table_named_in_error: true,
            database_named_in_error: false,
            database_in_error: None,
            table_error_override: None,
            temporary_privilege: TemporaryPrivilege::Check,
            dynamic_privileges: Vec::new(),
            dynamic_with_grant: false,
            specific_denial: None,
            also_granted_by: Vec::new(),
            view_no_explain: false,
        }
    }

    /// A global visit whose denial Go reports as
    /// `ErrSpecificAccessDenied(denial)`.
    fn specific(privilege: PrivilegeType, denial: &'static str) -> Self {
        Self {
            table_named_in_error: false,
            specific_denial: Some(denial),
            ..Self::new("", "", privilege)
        }
    }
}

/// Splits a written name path into `(schema, table)`, defaulting the schema
/// to the session's current database exactly as Go's builders do
/// (`dbName == "" => CurrentDB`). `None` for a path this tier cannot read as
/// a table name.
fn split_path<'a>(path: &'a [String], current_db: &'a str) -> Option<(String, String)> {
    match path {
        // An unqualified name with NO current database is Go's `ErrNoDB`,
        // raised during name resolution -- which runs BEFORE
        // `CheckPrivilege`. Dropping the request here lets the executor
        // report 1046 rather than turning "no database selected" into an
        // access-denied.
        [table] if !current_db.is_empty() => Some((current_db.to_owned(), table.clone())),
        [schema, table] => Some((schema.clone(), table.clone())),
        _ => None,
    }
}

/// Every `TableRef` reachable from `node`, in traversal order -- the row
/// sources Go's `buildDataSource` visits, and therefore the tables it
/// demands `SELECT` on.
pub fn read_tables(stmt: &Stmt, current_db: &str) -> Vec<(String, String)> {
    tidb_bindinfo::binding::collect_physical_table_refs(stmt)
        .iter()
        .filter_map(|path| split_path(path, current_db))
        .collect()
}

/// Go's `visitInfo` for one statement, in the order its builder appends it.
///
/// An empty list means the statement demands no TABLE-scope privilege here:
/// either it genuinely needs none (`SELECT 1`, `SET`, `BEGIN`), or its
/// privileges are demanded by its own executor arm instead (every account
/// statement, `ANALYZE`, `KILL`).
pub fn required_table_privileges(
    stmt: &Stmt,
    current_db: &str,
    resolve_write: impl FnOnce(
        &tidb_ast::DmlStmt,
    ) -> Result<
        Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
        crate::DriverError,
    >,
    resolve_read: impl FnOnce(
        &tidb_ast::QueryStmt,
    ) -> Result<
        Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
        crate::DriverError,
    >,
    resolve_statement: impl FnOnce(
        &Stmt,
    ) -> Result<
        Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
        crate::DriverError,
    >,
) -> Result<Vec<TablePrivilegeRequest>, crate::DriverError> {
    let mut requests = Vec::new();
    match stmt {
        // A query's visit info is what Go's plan builder appends while it
        // resolves names (`buildDataSource`, a locking read's tables, a
        // view's body), so it is read off the plan, not off the syntax.
        Stmt::Query(query) => {
            // Go plans first and checks privileges after, so a statement whose
            // PLAN fails reports that failure, not a privilege error. This pass
            // plans with constant folding suspended and cannot reproduce a
            // failure that depends on it (`get_lock(..), no_such_fn()` folds the
            // first call before the error), so a failed pass contributes no
            // visits and the statement's own planning raises the error.
            for visit in resolve_read(query).unwrap_or_default() {
                requests.push(request_of_visit(&visit));
            }
            // Go `buildSelectInto` appends FILE after planning the query.
            if matches!(&**query, tidb_ast::QueryStmt::Select(select) if select.into_outfile.is_some())
            {
                requests.push(TablePrivilegeRequest::specific(tidb_mysql::FilePriv, "FILE"));
            }
        }
        Stmt::Dml(dml) => match &**dml {
            // `buildInsert` (`planbuilder.go` around line 4176): `InsertPriv`
            // on the target, plus `DeletePriv` for `REPLACE` or `UpdatePriv`
            // for `ON DUPLICATE KEY UPDATE`. An `INSERT ... SELECT`'s source
            // is planned as an ordinary query, so it carries its own
            // `SelectPriv` entries.
            DmlStmt::Insert(_) => {
                for visit in resolve_write(dml)? {
                    requests.push(request_of_visit(&visit));
                }
            }
            // An `UPDATE` READS its sources before it writes them, so Go's
            // `buildDataSource` demands `SelectPriv` on each, and
            // `buildNewAssignments` (`logical_plan_builder.go` around line
            // 6490) then demands `UpdatePriv` on each assignment's table --
            // with no `authErr`, which is why a denied `UPDATE` reports 8121
            // rather than 1142.
            DmlStmt::Update(_) => {
                for visit in resolve_write(dml)? {
                    requests.push(request_of_visit(&visit));
                }
            }
            // `buildDelete` (`logical_plan_builder.go` around line 6640):
            // `SelectPriv` on every source through `buildDataSource`, then
            // `DeletePriv` on each named target, this time WITH the 1142
            // `authErr`.
            DmlStmt::Delete(_) => {
                // The visits already drop the source's last one for a
                // non-qualified delete, as Go's `popVisitInfo` does.
                for visit in resolve_write(dml)? {
                    requests.push(request_of_visit(&visit));
                }
            }
            // Go `buildImportInto`: IMPORT INTO needs the full DML privileges
            // of the target (ALTER too, for add-index by SQL), each carrying
            // the table-named 1142, plus FILE for a server-disk source.
            DmlStmt::ImportInto(import) => {
                if let Some((schema, table)) = split_path(&import.table, current_db) {
                    for privilege in [
                        tidb_mysql::SelectPriv,
                        tidb_mysql::UpdatePriv,
                        tidb_mysql::InsertPriv,
                        tidb_mysql::DeletePriv,
                        tidb_mysql::AlterPriv,
                    ] {
                        requests.push(TablePrivilegeRequest::new(&schema, &table, privilege));
                    }
                }
                if let tidb_ast::ImportSource::File { path, .. } = &import.source {
                    // `objstore.IsLocal`: no scheme, or the `file` scheme.
                    let local = path
                        .split_once("://")
                        .is_none_or(|(scheme, _)| scheme.is_empty() || scheme == "file");
                    if local {
                        requests.push(TablePrivilegeRequest::specific(tidb_mysql::FilePriv, "FILE"));
                    }
                }
            }
            // Every other DML form is refused as unsupported before it
            // could touch a table.
            _ => {}
        },
        // Go `Compile` for every statement that is not a query or DML:
        // `PlanBuilder.Build`'s visit infos, after `VisitInfo4PrivCheck`.
        // SET and SHOW compile themselves (`Session::run_set_stmt`,
        // `Session::run_planned_show`), and PREPARE / EXECUTE / DEALLOCATE
        // are the session's.
        Stmt::Session(session)
            if matches!(
                session.as_ref(),
                tidb_ast::SessionStmt::Set(_)
                    | tidb_ast::SessionStmt::Prepare { .. }
                    | tidb_ast::SessionStmt::Execute { .. }
                    | tidb_ast::SessionStmt::Deallocate(_)
            ) => {}
        Stmt::Admin(admin) if matches!(admin.as_ref(), tidb_ast::AdminStmt::Show(_)) => {}
        Stmt::Ddl(_) | Stmt::Admin(_) | Stmt::Session(_) => {
            for visit in resolve_statement(stmt)? {
                requests.push(request_of_visit(&visit));
            }
        }
    }
    // Preserve original table visits for observation, but retain Go's
    // statement-specific temporary-table policy for each execution's grants.
    // Exempt only operations whose execution resolves the session overlay.
    // The temporary DDL router shares those resolved targets with execution.
    if matches!(stmt, Stmt::Query(_) | Stmt::Dml(_)) {
        for request in &mut requests {
            request.temporary_privilege = TemporaryPrivilege::SkipLocal;
        }
    }
    Ok(requests)
}

/// The static privileges a `mysql.PrivilegeType` mask names; any one of them
/// grants the visit, as `RequestVerification` asks `priv & mask > 0`.
pub fn global_privileges_of(privilege: tidb_mysql::PrivilegeType) -> Vec<PrivilegeType> {
    tidb_mysql::ALL_GLOBAL_PRIVS
        .iter()
        .copied()
        .chain([tidb_mysql::GrantPriv])
        .filter(|global| privilege.0 & global.bit() != 0)
        .collect()
}

/// One plan-time [`tidb_planner::plan_builder::visit_info::VisitInfo`] as the
/// request the session checks.
pub fn request_of_visit(
    visit: &tidb_planner::plan_builder::visit_info::VisitInfo,
) -> TablePrivilegeRequest {
    use tidb_planner::plan_builder::visit_info::VisitAuthError;
    if visit.privilege == tidb_mysql::ExtendedPriv {
        // Go `appendDynamicVisitInfo`: any one dynamic privilege (SUPER
        // standing in for each) passes.
        let mut request = TablePrivilegeRequest {
            table_named_in_error: false,
            dynamic_privileges: visit.dynamic_privs.clone(),
            dynamic_with_grant: visit.dynamic_with_grant,
            ..TablePrivilegeRequest::new("", "", tidb_mysql::SuperPriv)
        };
        if let VisitAuthError::SpecificAccessDenied { privilege } = &visit.error {
            request.specific_denial = Some(privilege);
        }
        return request;
    }
    // Go `RequestVerification` passes when the account holds ANY bit of the
    // visit's mask (a locking read's `DELETE|UPDATE|LOCK TABLES`, SHOW's
    // `AllPrivMask`); the first names the denial, DELETE for a locking read.
    let mut alternatives = global_privileges_of(visit.privilege);
    let locking = alternatives.contains(&tidb_mysql::DeletePriv)
        && alternatives.contains(&tidb_mysql::UpdatePriv)
        && alternatives.contains(&tidb_mysql::LockTablesPriv);
    if locking {
        alternatives.retain(|privilege| *privilege != tidb_mysql::DeletePriv);
        alternatives.insert(0, tidb_mysql::DeletePriv);
    }
    let primary = alternatives.first().copied().unwrap_or(tidb_mysql::SelectPriv);
    let mut request = TablePrivilegeRequest::new(&visit.database, &visit.table, primary);
    request.also_granted_by = alternatives.into_iter().skip(1).collect();
    match &visit.error {
        VisitAuthError::TableAccessDenied { command, table } => {
            request.table_error_override = Some((command, table.clone()));
        }
        VisitAuthError::DbAccessDenied { database } => {
            request.table_named_in_error = false;
            request.database_named_in_error = true;
            request.database_in_error = Some(database.clone());
        }
        VisitAuthError::SpecificAccessDenied { privilege } => {
            request.table_named_in_error = false;
            request.specific_denial = Some(privilege);
        }
        VisitAuthError::None | VisitAuthError::PrivilegeCheckFail => {
            request.table_named_in_error = false;
        }
        VisitAuthError::ViewNoExplain => request.view_no_explain = true,
    }
    request
}
