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
//! Go collects these while PLANNING (`planner/core`'s `appendVisitInfo`, one
//! entry per table the plan touches) and checks them in one pass afterwards
//! (`optimizer.go`'s `CheckPrivilege`). This tier plans inside the executor,
//! so the collection happens here instead, off the parsed statement -- but
//! the entries, their order, and the error each one reports are Go's.
//!
//! WHAT IS DELIBERATELY NOT COLLECTED. A request this module cannot resolve
//! to a concrete `(schema, table)` is DROPPED rather than guessed at: the
//! `SET` targets of a multi-table `UPDATE` whose assignments are unqualified,
//! and a multi-table `DELETE` target that matches no table in its own join.
//! Go resolves those through name resolution this module does not run.
//! Dropping is fail-open for exactly those shapes and is called out at each
//! site; inventing a table instead would REFUSE statements Go allows, which
//! is the worse error.

use tidb_ast::{DdlStmt, DmlStmt, Stmt};

use crate::privilege::GlobalPriv;

/// One `visitInfo` entry: a privilege demanded on one table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TablePrivilegeRequest {
    /// Schema name, already resolved against the session's current database.
    pub(crate) database: String,
    /// Table name as written.
    pub(crate) table: String,
    /// The privilege Go's `appendVisitInfo` recorded.
    pub(crate) privilege: GlobalPriv,
    /// Whether Go attached a statement-specific `authErr` naming the table
    /// (`ErrTableaccessDenied`, 1142). Where it did not, `CheckPrivilege`
    /// falls to `ErrPrivilegeCheckFail` (8121) -- which is what a denied
    /// `UPDATE`'s `SET` target reports, since
    /// `logical_plan_builder.go`'s `buildNewAssignments` passes `nil`.
    pub(crate) table_named_in_error: bool,
}

impl TablePrivilegeRequest {
    fn new(database: &str, table: &str, privilege: GlobalPriv) -> Self {
        Self {
            database: database.to_owned(),
            table: table.to_owned(),
            privilege,
            table_named_in_error: true,
        }
    }

    /// The form whose denial carries no statement-specific error.
    fn unnamed(database: &str, table: &str, privilege: GlobalPriv) -> Self {
        Self {
            table_named_in_error: false,
            ..Self::new(database, table, privilege)
        }
    }
}

/// Go `metadef.IsMemDB`: the three virtual schemas whose privilege answers
/// are decided by `UserPrivileges.RequestVerification`'s own early arms
/// (`privileges.go` around line 194) rather than by any stored grant.
fn is_mem_db(database: &str) -> bool {
    database.eq_ignore_ascii_case("information_schema")
        || database.eq_ignore_ascii_case("performance_schema")
        || database.eq_ignore_ascii_case("metrics_schema")
}

fn is_mem_or_sys_db(database: &str) -> bool {
    database.eq_ignore_ascii_case("mysql") || is_mem_db(database)
}

/// SEM's hard table rule, evaluated before stored grants and before the
/// ordinary virtual-schema rule below. `None` means SEM has no opinion and
/// the normal privilege path decides.
pub(crate) fn sem_verdict_mask(
    database: &str,
    table: &str,
    mask: u64,
    has_restricted_tables_admin: bool,
) -> Option<bool> {
    if !tidb_util::sem::is_enabled() || has_restricted_tables_admin {
        return None;
    }
    let database_lower = database.to_ascii_lowercase();
    let table_lower = table.to_ascii_lowercase();
    if tidb_util::sem::is_invisible_table(&database_lower, &table_lower) {
        return Some(false);
    }
    const SEM_REFUSED_WRITES: &[GlobalPriv] = &[
        GlobalPriv::Create,
        GlobalPriv::Alter,
        GlobalPriv::Drop,
        GlobalPriv::Index,
        GlobalPriv::CreateView,
        GlobalPriv::Insert,
        GlobalPriv::Update,
        GlobalPriv::Delete,
    ];
    if is_mem_or_sys_db(&database_lower)
        && SEM_REFUSED_WRITES
            .iter()
            .any(|privilege| privilege.bit() == mask)
    {
        return Some(false);
    }
    None
}

/// The fixed answer `RequestVerification` gives for a virtual schema before
/// it consults a single grant, or `None` when the stored grants decide.
///
/// `mask` is Go's `priv` argument, which is a `mysql.PrivilegeType` and may
/// carry several bits. That matters here: Go's write refusal is a
/// `switch priv { case mysql.CreatePriv, ...: }`, an EQUALITY test, so a
/// multi-bit mask -- the "any privilege" question `SHOW TABLES` and the
/// `information_schema` retrievers ask -- never enters it and falls straight
/// through to the `information_schema` admission below. Matching on the
/// whole mask rather than on a decoded privilege is what keeps that true
/// without a second rule.
///
/// Go refuses every write-shaped privilege on all three virtual schemas
/// (`privileges.go` around line 194) and then admits EVERYTHING on
/// `information_schema` (around line 201), which is what makes `SELECT ...
/// FROM information_schema.*` need no grant at all while
/// `performance_schema` and `metrics_schema` still consult stored grants.
pub(crate) fn mem_db_verdict_mask(database: &str, mask: u64) -> Option<bool> {
    if !is_mem_db(database) {
        return None;
    }
    const REFUSED: &[GlobalPriv] = &[
        GlobalPriv::Create,
        GlobalPriv::Alter,
        GlobalPriv::Drop,
        GlobalPriv::Index,
        GlobalPriv::CreateView,
        GlobalPriv::Insert,
        GlobalPriv::Update,
        GlobalPriv::Delete,
        GlobalPriv::References,
        GlobalPriv::Execute,
        GlobalPriv::ShowView,
        GlobalPriv::LockTables,
    ];
    if REFUSED.iter().any(|priv_| priv_.bit() == mask) {
        return Some(false);
    }
    database
        .eq_ignore_ascii_case("information_schema")
        .then_some(true)
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
fn read_tables(stmt: &Stmt, current_db: &str) -> Vec<(String, String)> {
    // A CTE is referenced through the table grammar but resolves to its own
    // query, so it is not a data source and Go demands nothing on it.
    let ctes = crate::binding::collect_cte_names(stmt);
    crate::binding::collect_table_paths(stmt)
        .iter()
        .filter(|path| {
            !matches!(path.as_slice(), [name]
                if ctes.iter().any(|cte| cte.eq_ignore_ascii_case(name)))
        })
        .filter_map(|path| split_path(path, current_db))
        .collect()
}

/// Resolves a multi-table `UPDATE`/`DELETE` target, which is written as a
/// bare name that may be either a table or one of the join's ALIASES.
///
/// Returns `None` when the name matches no source in the statement's own
/// join: Go resolves it through the plan's output names, and a name this
/// function cannot place is one that would need that resolution.
fn resolve_target(
    target: &[String],
    sources: &[(String, String, Option<String>)],
    current_db: &str,
) -> Option<(String, String)> {
    let (schema, name) = match target {
        [name] => (None, name.as_str()),
        [schema, name] => (Some(schema.as_str()), name.as_str()),
        _ => return None,
    };
    // An alias hides the table it renames, so it is matched first and only
    // for the unqualified spelling (`db.alias` is not a thing).
    if schema.is_none() {
        if let Some((source_db, source_table, _)) = sources.iter().find(|(_, _, alias)| {
            alias
                .as_deref()
                .is_some_and(|alias| alias.eq_ignore_ascii_case(name))
        }) {
            return Some((source_db.clone(), source_table.clone()));
        }
    }
    let schema = schema.unwrap_or(current_db);
    sources
        .iter()
        .find(|(source_db, source_table, alias)| {
            alias.is_none()
                && source_db.eq_ignore_ascii_case(schema)
                && source_table.eq_ignore_ascii_case(name)
        })
        .map(|(source_db, source_table, _)| (source_db.clone(), source_table.clone()))
}

/// The `(schema, table, alias)` of every `TableRef` in `stmt`, for target
/// resolution.
fn aliased_sources(stmt: &Stmt, current_db: &str) -> Vec<(String, String, Option<String>)> {
    crate::binding::collect_table_refs(stmt)
        .into_iter()
        .filter_map(|(path, alias)| {
            split_path(&path, current_db).map(|(schema, table)| (schema, table, alias))
        })
        .collect()
}

/// Go's `visitInfo` for one statement, in the order its builder appends it.
///
/// An empty list means the statement demands no TABLE-scope privilege here:
/// either it genuinely needs none (`SELECT 1`, `SET`, `BEGIN`), or its
/// privileges are demanded by its own executor arm instead (every account
/// statement, `ANALYZE`, `KILL`).
pub(crate) fn required_table_privileges(
    stmt: &Stmt,
    current_db: &str,
) -> Vec<TablePrivilegeRequest> {
    let mut requests = Vec::new();
    match stmt {
        // `buildDataSource` (`logical_plan_builder.go` around line 4972)
        // appends `SelectPriv` for every table the query reads.
        Stmt::Query(_) => {
            for (schema, table) in read_tables(stmt, current_db) {
                requests.push(TablePrivilegeRequest::new(
                    &schema,
                    &table,
                    GlobalPriv::Select,
                ));
            }
        }
        Stmt::Dml(dml) => match &**dml {
            // `buildInsert` (`planbuilder.go` around line 4176): `InsertPriv`
            // on the target, plus `DeletePriv` for `REPLACE` or `UpdatePriv`
            // for `ON DUPLICATE KEY UPDATE`. An `INSERT ... SELECT`'s source
            // is planned as an ordinary query, so it carries its own
            // `SelectPriv` entries.
            DmlStmt::Insert(insert) => {
                if let Some((schema, table)) = split_path(&insert.table, current_db) {
                    requests.push(TablePrivilegeRequest::new(
                        &schema,
                        &table,
                        GlobalPriv::Insert,
                    ));
                    if insert.replace {
                        requests.push(TablePrivilegeRequest::new(
                            &schema,
                            &table,
                            GlobalPriv::Delete,
                        ));
                    } else if !insert.on_duplicate.is_empty() {
                        requests.push(TablePrivilegeRequest::new(
                            &schema,
                            &table,
                            GlobalPriv::Update,
                        ));
                    }
                }
                for (schema, table) in read_tables(stmt, current_db) {
                    requests.push(TablePrivilegeRequest::new(
                        &schema,
                        &table,
                        GlobalPriv::Select,
                    ));
                }
            }
            // An `UPDATE` READS its sources before it writes them, so Go's
            // `buildDataSource` demands `SelectPriv` on each, and
            // `buildNewAssignments` (`logical_plan_builder.go` around line
            // 6490) then demands `UpdatePriv` on each assignment's table --
            // with no `authErr`, which is why a denied `UPDATE` reports 8121
            // rather than 1142.
            DmlStmt::Update(update) => {
                for (schema, table) in read_tables(stmt, current_db) {
                    requests.push(TablePrivilegeRequest::new(
                        &schema,
                        &table,
                        GlobalPriv::Select,
                    ));
                }
                match &update.kind {
                    tidb_ast::UpdateKind::Single(table_ref) => {
                        if let Some((schema, table)) = split_path(&table_ref.name, current_db) {
                            requests.push(TablePrivilegeRequest::unnamed(
                                &schema,
                                &table,
                                GlobalPriv::Update,
                            ));
                        }
                    }
                    tidb_ast::UpdateKind::Multi { .. } => {
                        // Each assignment names its own table, so only the
                        // QUALIFIED ones can be placed without running Go's
                        // column resolution. An unqualified assignment in a
                        // multi-table update is therefore not demanded here
                        // (see the module doc).
                        let sources = aliased_sources(stmt, current_db);
                        for assignment in &update.assignments {
                            let qualifier =
                                &assignment.col[..assignment.col.len().saturating_sub(1)];
                            if qualifier.is_empty() {
                                continue;
                            }
                            if let Some((schema, table)) =
                                resolve_target(qualifier, &sources, current_db)
                            {
                                let request = TablePrivilegeRequest::unnamed(
                                    &schema,
                                    &table,
                                    GlobalPriv::Update,
                                );
                                if !requests.contains(&request) {
                                    requests.push(request);
                                }
                            }
                        }
                    }
                }
            }
            // `buildDelete` (`logical_plan_builder.go` around line 6640):
            // `SelectPriv` on every source through `buildDataSource`, then
            // `DeletePriv` on each named target, this time WITH the 1142
            // `authErr`.
            DmlStmt::Delete(delete) => {
                for (schema, table) in read_tables(stmt, current_db) {
                    requests.push(TablePrivilegeRequest::new(
                        &schema,
                        &table,
                        GlobalPriv::Select,
                    ));
                }
                match &delete.kind {
                    tidb_ast::DeleteKind::Single(table_ref) => {
                        if let Some((schema, table)) = split_path(&table_ref.name, current_db) {
                            requests.push(TablePrivilegeRequest::new(
                                &schema,
                                &table,
                                GlobalPriv::Delete,
                            ));
                        }
                    }
                    tidb_ast::DeleteKind::Multi { targets, .. } => {
                        let sources = aliased_sources(stmt, current_db);
                        for target in targets {
                            if let Some((schema, table)) =
                                resolve_target(target, &sources, current_db)
                            {
                                requests.push(TablePrivilegeRequest::new(
                                    &schema,
                                    &table,
                                    GlobalPriv::Delete,
                                ));
                            }
                        }
                    }
                }
            }
            // Every other DML form is refused as unsupported before it
            // could touch a table.
            _ => {}
        },
        Stmt::Ddl(ddl) => requests.extend(ddl_table_privileges(ddl, current_db)),
        Stmt::Admin(_) | Stmt::Session(_) => {}
    }
    requests
}

/// The `visitInfo` `planbuilder.go`'s DDL arm appends, for the statements
/// this tier executes. Everything it refuses as unsupported is deliberately
/// absent rather than half-modelled.
fn ddl_table_privileges(ddl: &DdlStmt, current_db: &str) -> Vec<TablePrivilegeRequest> {
    let one = |path: &[String], privilege| {
        split_path(path, current_db)
            .map(|(schema, table)| vec![TablePrivilegeRequest::new(&schema, &table, privilege)])
            .unwrap_or_default()
    };
    match ddl {
        // `planbuilder.go` around line 5428.
        DdlStmt::CreateTable(create) => one(&create.name, GlobalPriv::Create),
        // Around line 5528. Go appends one entry per named table.
        DdlStmt::DropTable(drop) => drop
            .names
            .iter()
            .flat_map(|name| one(name, GlobalPriv::Drop))
            .collect(),
        // Around line 5321.
        DdlStmt::AlterTable(alter) => one(&alter.name, GlobalPriv::Alter),
        // Around line 5404 / 5520: an index is an `ALTER`-class change that
        // Go demands `IndexPriv` for.
        DdlStmt::CreateIndex(create) => one(&create.table, GlobalPriv::Index),
        DdlStmt::DropIndex(drop) => one(&drop.table, GlobalPriv::Index),
        // Around line 5487.
        DdlStmt::CreateView(create) => one(&create.name, GlobalPriv::CreateView),
        _ => Vec::new(),
    }
}
