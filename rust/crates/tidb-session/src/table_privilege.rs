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

use tidb_ast::{AlterPartitionAction, AlterTableAction, DdlStmt, DmlStmt, Stmt, TableConstraint};

use crate::privilege::GlobalPriv;

/// Go VisitInfo4PrivCheck keeps observation visits while adapting admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TemporaryPrivilege {
    SkipLocal,
    Check,
    CreateLocal,
    Skip,
}

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
    /// Whether Go reports a database-scoped denial (1044) instead of the
    /// table-scoped 1142 form. `CREATE/DROP DATABASE` carry a schema name but
    /// no table, and their planner visitInfo attaches exactly that error.
    pub(crate) database_named_in_error: bool,
    /// Adapt admission without discarding original observation table names.
    pub(crate) temporary_privilege: TemporaryPrivilege,
    /// Go authErr can name a different command/table than the checked visit.
    pub(crate) table_error_override: Option<(&'static str, String)>,
    /// Go `appendDynamicVisitInfo`: dynamic privileges any one of which
    /// grants the visit (SUPER standing in for each); empty for an ordinary
    /// static-privilege visit.
    pub(crate) dynamic_privileges: &'static [&'static str],
    /// Go's `ErrSpecificAccessDenied` authErr (1227), naming what the
    /// statement needs.
    pub(crate) specific_denial: Option<&'static str>,
    /// Further static privileges that grant the visit as well: Go checks a
    /// combined mask (`RequestVerification` asks `priv & mask > 0`).
    pub(crate) also_granted_by: &'static [GlobalPriv],
}

impl TablePrivilegeRequest {
    fn new(database: &str, table: &str, privilege: GlobalPriv) -> Self {
        Self {
            database: database.to_owned(),
            table: table.to_owned(),
            privilege,
            table_named_in_error: true,
            database_named_in_error: false,
            table_error_override: None,
            temporary_privilege: TemporaryPrivilege::Check,
            dynamic_privileges: &[],
            specific_denial: None,
            also_granted_by: &[],
        }
    }

    /// Go `appendDynamicVisitInfo(privileges, false,
    /// ErrSpecificAccessDenied(denial))`.
    fn dynamic(privileges: &'static [&'static str], denial: &'static str) -> Self {
        Self {
            table_named_in_error: false,
            dynamic_privileges: privileges,
            specific_denial: Some(denial),
            ..Self::new("", "", GlobalPriv::Super)
        }
    }

    /// A global visit whose denial Go reports as
    /// `ErrSpecificAccessDenied(denial)`.
    fn specific(privilege: GlobalPriv, denial: &'static str) -> Self {
        Self {
            table_named_in_error: false,
            specific_denial: Some(denial),
            ..Self::new("", "", privilege)
        }
    }

    /// The form whose denial carries no statement-specific error.
    fn unnamed(database: &str, table: &str, privilege: GlobalPriv) -> Self {
        Self {
            table_named_in_error: false,
            ..Self::new(database, table, privilege)
        }
    }

    fn database(database: &str, privilege: GlobalPriv) -> Self {
        Self {
            table_named_in_error: false,
            database_named_in_error: true,
            ..Self::new(database, "", privilege)
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
    if !tidb_util::sem_compat::is_enabled() || has_restricted_tables_admin {
        return None;
    }
    let database_lower = database.to_ascii_lowercase();
    let table_lower = table.to_ascii_lowercase();
    if tidb_util::sem_compat::is_invisible_table(&database_lower, &table_lower) {
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
    crate::binding::collect_physical_table_refs(stmt)
        .iter()
        .filter_map(|path| split_path(path, current_db))
        .collect()
}

/// The tables a locking read locks: its `FOR UPDATE OF` list, or Go
/// `ExtractTableList(sel.From)` -- every table its FROM clause names,
/// through joins and derived tables, CTE names excepted.
fn locking_read_tables(
    select: &tidb_ast::SelectStmt,
    lock: &tidb_ast::SelectLock,
    current_db: &str,
) -> Vec<(String, String)> {
    if !lock.of.is_empty() {
        return lock
            .of
            .iter()
            .filter_map(|path| split_path(path, current_db))
            .collect();
    }
    let Some(from) = &select.from else {
        return Vec::new();
    };
    // The statement's own FROM clause alone, under its WITH clause so the
    // CTE scope still hides CTE names.
    let mut from_only = select.clone();
    from_only.fields = tidb_ast::SelectFieldList::default();
    from_only.from = Some(from.clone());
    from_only.where_clause = None;
    from_only.group_by.clear();
    from_only.having = None;
    from_only.windows.clear();
    from_only.order_by.clear();
    from_only.lock = None;
    read_tables(
        &Stmt::Query(tidb_ast::NodeBox::new(tidb_ast::QueryStmt::Select(
            Box::new(from_only),
        ))),
        current_db,
    )
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
    resolve_write: impl FnOnce(
        &tidb_ast::DmlStmt,
    ) -> Result<Vec<(String, String)>, tidb_executor::DriverError>,
) -> Result<Vec<TablePrivilegeRequest>, tidb_executor::DriverError> {
    let mut requests = Vec::new();
    match stmt {
        // `buildDataSource` (`logical_plan_builder.go` around line 4972)
        // appends `SelectPriv` for every table the query reads.
        Stmt::Query(query) => {
            for (schema, table) in read_tables(stmt, current_db) {
                requests.push(TablePrivilegeRequest::new(
                    &schema,
                    &table,
                    GlobalPriv::Select,
                ));
            }
            // Go `buildSelect`: a locking read also needs DELETE, UPDATE or
            // LOCK TABLES on each table it locks -- the `OF` list, or every
            // table of its FROM clause.
            if let tidb_ast::QueryStmt::Select(select) = &**query {
                if let Some(lock) = &select.lock {
                    for (schema, table) in locking_read_tables(select, lock, current_db) {
                        let mut request =
                            TablePrivilegeRequest::new(&schema, &table, GlobalPriv::Delete);
                        request.also_granted_by = &[GlobalPriv::Update, GlobalPriv::LockTables];
                        request.table_error_override =
                            Some(("SELECT with locking clause", table.clone()));
                        requests.push(request);
                    }
                }
            }
            // Go `buildSelectInto` appends FILE after planning the query.
            if matches!(&**query, tidb_ast::QueryStmt::Select(select) if select.into_outfile.is_some())
            {
                requests.push(TablePrivilegeRequest::specific(GlobalPriv::File, "FILE"));
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
            DmlStmt::Update(_) => {
                for (schema, table) in read_tables(stmt, current_db) {
                    requests.push(TablePrivilegeRequest::new(
                        &schema,
                        &table,
                        GlobalPriv::Select,
                    ));
                }
                for (schema, table) in resolve_write(dml)? {
                    requests.push(TablePrivilegeRequest::unnamed(
                        &schema,
                        &table,
                        GlobalPriv::Update,
                    ));
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
                // Go buildDelete removes its final source SELECT visit
                // when neither a WHERE nor ORDER clause needs it.
                if delete.where_clause.is_none() && delete.order_by.is_empty() {
                    requests.pop();
                }
                for (schema, table) in resolve_write(dml)? {
                    requests.push(TablePrivilegeRequest::new(
                        &schema,
                        &table,
                        GlobalPriv::Delete,
                    ));
                }
            }
            // Every other DML form is refused as unsupported before it
            // could touch a table.
            _ => {}
        },
        Stmt::Ddl(ddl) => {
            // Go `buildDDL`'s dynamic-privilege visits.
            match ddl.as_ref() {
                DdlStmt::CreatePlacementPolicy(_)
                | DdlStmt::AlterPlacementPolicy(_)
                | DdlStmt::DropPlacementPolicy(_) => requests.push(TablePrivilegeRequest::dynamic(
                    &["PLACEMENT_ADMIN"],
                    "SUPER or PLACEMENT_ADMIN",
                )),
                DdlStmt::CreateResourceGroup(_)
                | DdlStmt::AlterResourceGroup(_)
                | DdlStmt::DropResourceGroup(_) => requests.push(TablePrivilegeRequest::dynamic(
                    &["RESOURCE_GROUP_ADMIN"],
                    "SUPER or RESOURCE_GROUP_ADMIN",
                )),
                _ => {}
            }
            if let DdlStmt::CreateView(_) = ddl.as_ref() {
                // Go builds the query before appending CREATE VIEW and DROP
                // visits. Reuse scoped read collection, including subqueries.
                for (schema, table) in read_tables(stmt, current_db) {
                    requests.push(TablePrivilegeRequest::new(
                        &schema,
                        &table,
                        GlobalPriv::Select,
                    ));
                }
            }
            requests.extend(ddl_table_privileges(ddl, current_db));
        }
        Stmt::Admin(_) | Stmt::Session(_) => {}
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
    if let Stmt::Ddl(ddl) = stmt {
        match ddl.as_ref() {
            DdlStmt::CreateTable(create) => {
                for request in &mut requests {
                    if request.privilege == GlobalPriv::Create {
                        request.temporary_privilege =
                            if create.temporary == tidb_ast::CreateTableTemporary::Local {
                                request.table_error_override = None;
                                request.database_named_in_error = true;
                                TemporaryPrivilege::CreateLocal
                            } else {
                                TemporaryPrivilege::Check
                            };
                    }
                }
            }
            DdlStmt::TruncateTable(_)
            | DdlStmt::AlterTable(_)
            | DdlStmt::CreateIndex(_)
            | DdlStmt::DropIndex(_)
            | DdlStmt::RenameTable(_) => {
                for request in &mut requests {
                    request.temporary_privilege = TemporaryPrivilege::SkipLocal;
                }
            }
            DdlStmt::DropTable(drop) => {
                for request in &mut requests {
                    request.temporary_privilege =
                        if drop.temporary == tidb_ast::DropTemporary::Local {
                            TemporaryPrivilege::Skip
                        } else {
                            TemporaryPrivilege::SkipLocal
                        };
                }
            }
            _ => {}
        }
    }
    Ok(requests)
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
    let references = |path: &[String], owner: &[String]| {
        // Go's preprocessor defaults an unqualified FK reference to the
        // child table's schema, which need not be the current database.
        let Some((schema, _)) = split_path(owner, current_db) else {
            return Vec::new();
        };
        split_path(path, &schema)
            .map(|(schema, table)| {
                vec![TablePrivilegeRequest::new(
                    &schema,
                    &table,
                    GlobalPriv::References,
                )]
            })
            .unwrap_or_default()
    };
    match ddl {
        // `planbuilder.go` around line 5392 / 5508: database DDL attaches
        // ErrDBaccessDenied (1044), with the schema name as the scope.
        DdlStmt::CreateDatabase { name, .. } => {
            vec![TablePrivilegeRequest::database(name, GlobalPriv::Create)]
        }
        DdlStmt::DropDatabase { name, .. } => {
            vec![TablePrivilegeRequest::database(name, GlobalPriv::Drop)]
        }
        // `planbuilder.go` around line 5428.
        DdlStmt::CreateTable(create) => {
            let mut visits = Vec::new();
            for constraint in &create.table_constraints {
                if let TableConstraint::ForeignKey(fk) = constraint {
                    if let Some(table) = &fk.reference.table {
                        visits.extend(references(table, &create.name));
                    }
                }
            }
            let mut target = one(&create.name, GlobalPriv::Create);
            // buildDDL retains the final REFERENCES authErr for the CREATE
            // visit too. The checked scope still belongs to the new table.
            if let Some(last) = visits.last() {
                for request in &mut target {
                    request.table_error_override = Some(("REFERENCES", last.table.clone()));
                }
            }
            visits.extend(target);
            if let Some(source) = &create.like_table {
                let mut source = one(source, GlobalPriv::Select);
                for request in &mut source {
                    request.table_error_override = Some(("CREATE", request.table.clone()));
                }
                visits.extend(source);
            }
            visits
        }
        // Around line 5528. Go appends one entry per named table.
        DdlStmt::DropTable(drop) => drop
            .names
            .iter()
            .flat_map(|name| one(name, GlobalPriv::Drop))
            .collect(),
        // Around line 5321.
        DdlStmt::AlterTable(alter) => {
            let mut visits = one(&alter.name, GlobalPriv::Alter);
            for action in &alter.actions {
                match action {
                    AlterTableAction::RenameTable { new_name } => {
                        visits.extend(one(&alter.name, GlobalPriv::Drop));
                        visits.extend(one(new_name, GlobalPriv::Create));
                        visits.extend(one(new_name, GlobalPriv::Insert));
                    }
                    AlterTableAction::Partition(AlterPartitionAction::Exchange {
                        table, ..
                    }) => {
                        // EXCHANGE mutates both tables; Go preserves this order.
                        visits.extend(one(&alter.name, GlobalPriv::Drop));
                        visits.extend(one(table, GlobalPriv::Create));
                        visits.extend(one(table, GlobalPriv::Insert));
                        visits.extend(one(&alter.name, GlobalPriv::Insert));
                        visits.extend(one(&alter.name, GlobalPriv::Create));
                        visits.extend(one(table, GlobalPriv::Alter));
                        visits.extend(one(table, GlobalPriv::Drop));
                    }
                    AlterTableAction::Partition(
                        AlterPartitionAction::Drop { .. } | AlterPartitionAction::Truncate { .. },
                    ) => {
                        visits.extend(one(&alter.name, GlobalPriv::Drop));
                    }
                    AlterTableAction::AddForeignKey(fk) => {
                        if let Some(table) = &fk.reference.table {
                            visits.extend(references(table, &alter.name));
                        }
                    }
                    AlterTableAction::AddColumns { constraints, .. } => {
                        for constraint in constraints {
                            if let TableConstraint::ForeignKey(fk) = constraint {
                                if let Some(table) = &fk.reference.table {
                                    visits.extend(references(table, &alter.name));
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            visits
        }
        DdlStmt::RenameTable(rename) => {
            let mut visits = Vec::new();
            for (old, new) in &rename.pairs {
                visits.extend(one(old, GlobalPriv::Alter));
                visits.extend(one(old, GlobalPriv::Drop));
                visits.extend(one(new, GlobalPriv::Create));
                visits.extend(one(new, GlobalPriv::Insert));
            }
            visits
        }
        DdlStmt::TruncateTable(name) => one(name, GlobalPriv::Drop),
        DdlStmt::AlterSequence(alter) => one(&alter.name, GlobalPriv::Alter),
        DdlStmt::DropSequence(drop) => drop
            .names
            .iter()
            .flat_map(|name| one(name, GlobalPriv::Drop))
            .collect(),
        DdlStmt::DropView { names, .. } => names
            .iter()
            .flat_map(|name| one(name, GlobalPriv::Drop))
            .collect(),
        DdlStmt::AlterDatabase { name, .. } => {
            let database = name.as_deref().unwrap_or(current_db);
            // Name resolution owns ErrNoDB before privilege checking.
            if database.is_empty() {
                Vec::new()
            } else {
                vec![TablePrivilegeRequest::database(database, GlobalPriv::Alter)]
            }
        }
        // Around line 5404 / 5520: an index is an `ALTER`-class change that
        // Go demands `IndexPriv` for.
        DdlStmt::CreateIndex(create) => one(&create.table, GlobalPriv::Index),
        DdlStmt::DropIndex(drop) => one(&drop.table, GlobalPriv::Index),
        // Around line 5487.
        DdlStmt::CreateView(create) => {
            let mut visits = one(&create.name, GlobalPriv::CreateView);
            if create.or_replace {
                visits.extend(one(&create.name, GlobalPriv::Drop));
            }
            visits
        }
        // `resolveCreateSequenceStmt` (`planbuilder.go` around line 5398)
        // records the same table-scoped CREATE privilege as CREATE TABLE and
        // carries the sequence name in the 1142 auth error.
        DdlStmt::CreateSequence(create) => one(&create.name, GlobalPriv::Create),
        _ => Vec::new(),
    }
}
