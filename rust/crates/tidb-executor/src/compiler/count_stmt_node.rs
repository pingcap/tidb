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

//! Go `compiler.go`'s `CountStmtNode`, `getStmtDbLabel` and
//! `getDbFromResultNode`: the statement counters `Compile` charges once a
//! statement is planned.
//!
//! Go counts after the preprocessor ran, so a table name it visited carries
//! the current database when unqualified (`handleTableName`), a CTE
//! reference resolves to no table, and `SHOW` carries its resolved
//! `DBName` (`resolveShowStmt`). `RECOVER TABLE`, `FLASHBACK TABLE` and
//! `CREATE`/`DROP BINDING` skip their children, so their names stay as
//! written; a binding's origin resolves only the tables `checkBindGrammar`
//! found.

use std::collections::BTreeSet;

use tidb_ast::{
    AdminCheckStmt, AdminStmt, BindingStatementTarget, CreateBindingSource, DdlStmt, DeleteKind,
    DmlStmt, DropBindingTarget, FlushTarget, Join, JoinNode, QueryStmt, SessionStmt,
    SetBindingTarget, ShowStmt, Stmt, TableRef, UpdateKind,
};

/// Go `CountStmtNode`. `stmt` is the statement whose label is counted: a
/// prepared statement's own node for an EXECUTE. `table_exists` is the
/// session's InfoSchema lookup by (schema, table).
pub fn count_stmt_node(
    stmt: &Stmt,
    current_db: &str,
    in_restricted_sql: bool,
    resource_group: &str,
    table_exists: &dyn Fn(&str, &str) -> bool,
) {
    if let Some(count) = stmt_node_count(stmt, current_db, in_restricted_sql, resource_group, table_exists) {
        count.record();
    }
}

/// The labels Go's `CountStmtNode` charges, taken from the statement before
/// it is planned and recorded once `Compile` succeeds.
#[derive(Debug)]
pub struct StmtNodeCount {
    type_label: &'static str,
    resource_group: String,
    /// Go `getStmtDbLabel`, when `status.record-qps-by-db` or
    /// `status.record-db-label` asks for it.
    db_labels: Option<BTreeSet<String>>,
}

/// [`count_stmt_node`]'s labels, or `None` for internal SQL.
pub fn stmt_node_count(
    stmt: &Stmt,
    current_db: &str,
    in_restricted_sql: bool,
    resource_group: &str,
    table_exists: &dyn Fn(&str, &str) -> bool,
) -> Option<StmtNodeCount> {
    if in_restricted_sql {
        return None;
    }
    let status = &tidb_config::config_tree::config::get_global_config().status;
    Some(StmtNodeCount {
        type_label: stmt.label(),
        resource_group: resource_group.to_owned(),
        db_labels: (status.record_qps_by_db || status.record_db_label)
            .then(|| get_stmt_db_label(stmt, current_db, table_exists)),
    })
}

impl StmtNodeCount {
    /// Charges Go `CountStmtNode`'s counters.
    pub fn record(self) {
        let record_qps_by_db = tidb_config::config_tree::config::get_global_config().status.record_qps_by_db;
        match &self.db_labels {
            Some(db_labels) if record_qps_by_db => {
                for db_label in db_labels {
                    tidb_metrics::executor::DB_STATEMENT_TOTAL
                        .with_label_values(&[db_label.as_str(), self.type_label])
                        .inc();
                }
            }
            Some(db_labels) => {
                for db_label in db_labels {
                    tidb_metrics::executor::STATEMENT_TOTAL
                        .with_label_values(&[db_label.as_str(), &self.resource_group, self.type_label])
                        .inc();
                }
            }
            None => {
                tidb_metrics::executor::STATEMENT_TOTAL
                    .with_label_values(&["", &self.resource_group, self.type_label])
                    .inc();
            }
        }
    }
}

/// How the preprocessor left a statement's table names.
struct Resolver<'a> {
    current_db: &'a str,
    /// Go `resolveCtx.GetTableName` finds the table: always after
    /// `handleTableName`, only for an existing table after
    /// `checkBindGrammar`.
    resolves: &'a dyn Fn(&str, &str) -> bool,
}

impl Resolver<'_> {
    /// Go `x.Schema.L` of a name `handleTableName` visited.
    fn schema(&self, name: &[String]) -> String {
        match name {
            [schema, _] => schema.to_lowercase(),
            _ => self.current_db.to_lowercase(),
        }
    }

    /// Go `resolveCtx.GetTableName(x).DBInfo.Name.L`, or no label.
    fn table(&self, name: &[String], ctes: &BTreeSet<String>) -> Vec<String> {
        if let [table] = name {
            if ctes.contains(&table.to_lowercase()) {
                return Vec::new();
            }
        }
        let schema = self.schema(name);
        let table = name.last().map(String::as_str).unwrap_or_default();
        if (self.resolves)(&schema, table) {
            vec![schema]
        } else {
            Vec::new()
        }
    }
}

/// Go `x.Schema.L` of a name the preprocessor did not visit.
fn written_schema(name: &[String]) -> String {
    match name {
        [schema, _] => schema.to_lowercase(),
        _ => String::new(),
    }
}

/// Go `getStmtDbLabel`.
pub fn get_stmt_db_label(
    stmt: &Stmt,
    current_db: &str,
    table_exists: &dyn Fn(&str, &str) -> bool,
) -> BTreeSet<String> {
    let resolved = Resolver {
        current_db,
        resolves: &|_, _| true,
    };
    let mut db_label_set = BTreeSet::new();
    let ctes = BTreeSet::new();
    match stmt {
        Stmt::Ddl(ddl) => match &**ddl {
            DdlStmt::AlterTable(x) => {
                db_label_set.insert(resolved.schema(&x.name));
            }
            DdlStmt::CreateIndex(x) => {
                db_label_set.insert(resolved.schema(&x.table));
            }
            DdlStmt::CreateTable(x) => {
                db_label_set.insert(resolved.schema(&x.name));
            }
            DdlStmt::DropIndex(x) => {
                db_label_set.insert(resolved.schema(&x.table));
            }
            DdlStmt::TruncateTable(x) => {
                db_label_set.insert(resolved.schema(x));
            }
            DdlStmt::RepairTable(x) => {
                db_label_set.insert(resolved.schema(&x.table));
            }
            DdlStmt::FlashbackTable(x) => {
                if let Some(table) = &x.table {
                    db_label_set.insert(written_schema(table));
                }
            }
            DdlStmt::RecoverTable(x) => {
                if let Some(table) = &x.table {
                    db_label_set.insert(written_schema(table));
                }
            }
            DdlStmt::CreateView(x) => {
                db_label_set.insert(resolved.schema(&x.name));
            }
            DdlStmt::RenameTable(x) => {
                for (old_table, _) in &x.pairs {
                    db_label_set.insert(resolved.schema(old_table));
                }
            }
            DdlStmt::DropTable(x) => {
                for table in &x.names {
                    db_label_set.insert(resolved.schema(table));
                }
            }
            // Go `AdminStmt` (`ADMIN CLEANUP TABLE LOCK`).
            DdlStmt::CleanupTableLock(x) => {
                for table in &x.tables {
                    db_label_set.insert(resolved.schema(table));
                }
            }
            _ => {}
        },
        Stmt::Dml(dml) => match &**dml {
            DmlStmt::Insert(x) => {
                db_label_set.extend(resolved.table(&x.table, &ctes));
                if let Some(source) = &x.source {
                    db_label_set.extend(get_db_from_query(source, &resolved, &ctes));
                }
            }
            DmlStmt::Update(x) => {
                let ctes = cte_names(x.with.as_ref());
                db_label_set.extend(get_db_from_update(&x.kind, &resolved, &ctes));
            }
            DmlStmt::Delete(x) => {
                let ctes = cte_names(x.with.as_ref());
                db_label_set.extend(get_db_from_delete(&x.kind, &resolved, &ctes));
            }
            DmlStmt::Call(x) => {
                // Go's preprocessor leaves the procedure's schema as written.
                db_label_set.insert(written_schema(&x.name));
            }
            DmlStmt::LoadData(x) => {
                db_label_set.insert(resolved.schema(&x.table));
            }
            DmlStmt::ImportInto(x) => {
                db_label_set.insert(resolved.schema(&x.table));
            }
            DmlStmt::DistributeTable(x) => {
                db_label_set.insert(resolved.schema(&x.table));
            }
            DmlStmt::Batch(x) => {
                if let Some(shard_column) = &x.shard_column {
                    // Go `ShardColumn.Schema.L`: the schema of a
                    // `db.table.column` name.
                    if let [schema, _, _] = shard_column.as_slice() {
                        db_label_set.insert(schema.to_lowercase());
                    } else {
                        db_label_set.insert(String::new());
                    }
                }
            }
            _ => {}
        },
        Stmt::Query(query) => {
            db_label_set.extend(get_db_from_query(query, &resolved, &ctes));
        }
        Stmt::Admin(admin) => match &**admin {
            AdminStmt::AnalyzeTable(x) => {
                for table in &x.tables {
                    db_label_set.insert(resolved.schema(table));
                }
            }
            AdminStmt::SplitRegion(x) => {
                db_label_set.insert(resolved.schema(&x.table));
            }
            AdminStmt::CompactTable(x) => {
                db_label_set.insert(resolved.schema(&x.table));
            }
            AdminStmt::Show(x) => db_label_set.extend(show_db_labels(x, current_db)),
            AdminStmt::DropStats(x) => {
                for table in &x.tables {
                    db_label_set.insert(resolved.schema(table));
                }
            }
            AdminStmt::Flush(x) => {
                if let FlushTarget::Tables { tables, .. } = &x.target {
                    for table in tables {
                        db_label_set.insert(resolved.schema(table));
                    }
                }
            }
            // Go `AdminStmt.Tables`.
            AdminStmt::AdminCheck(x) => match &**x {
                AdminCheckStmt::Table { tables } => {
                    for table in tables {
                        db_label_set.insert(resolved.schema(table));
                    }
                }
                AdminCheckStmt::Index { table, .. } => {
                    db_label_set.insert(resolved.schema(table));
                }
            },
            AdminStmt::AdminChecksum(x) => {
                for table in &x.tables {
                    db_label_set.insert(resolved.schema(table));
                }
            }
            AdminStmt::AdminRecoverIndex(x) | AdminStmt::AdminCleanupIndex(x) => {
                db_label_set.insert(resolved.schema(&x.table));
            }
            AdminStmt::ShowNextRowId(x) => {
                db_label_set.insert(resolved.schema(&x.table));
            }
            AdminStmt::CreateBinding(x) => {
                if let CreateBindingSource::Statement { target } = &x.source {
                    // Go `checkBindGrammar` resolves the tables that exist.
                    let checked = Resolver {
                        current_db,
                        resolves: table_exists,
                    };
                    db_label_set.extend(get_db_from_binding(target, &checked));
                }
            }
            AdminStmt::DropBinding(x) => {
                if let DropBindingTarget::Statement(target) = &x.target {
                    // Go checks (and resolves) the grammar only with a hinted
                    // statement.
                    let checked = Resolver {
                        current_db,
                        resolves: if target.hinted.is_some() {
                            table_exists
                        } else {
                            &|_, _| false
                        },
                    };
                    db_label_set.extend(get_db_from_binding(target, &checked));
                }
            }
            AdminStmt::SetBinding(x) => {
                if let SetBindingTarget::Statement(target) = &x.target {
                    db_label_set.extend(get_db_from_binding(target, &resolved));
                }
            }
            _ => {}
        },
        Stmt::Session(session) => {
            if let SessionStmt::Use(db_name) = &**session {
                db_label_set.insert(db_name.clone());
            }
        }
    }
    // Go adds the "" db label.
    if db_label_set.is_empty() {
        db_label_set.insert(String::new());
    }
    db_label_set
}

/// Go `getStmtDbLabel`'s `ShowStmt` after `resolveShowStmt` and
/// `handleTableName`: the resolved `DBName` and the table's schema.
fn show_db_labels(x: &ShowStmt, current_db: &str) -> Vec<String> {
    let db_name = match (x.db_name.as_str(), x.table.as_deref()) {
        ("", Some([schema, _])) => schema.clone(),
        ("", _) => current_db.to_owned(),
        (db_name, _) => db_name.to_owned(),
    };
    let mut labels = vec![db_name];
    if let Some(table) = &x.table {
        labels.push(match table.as_slice() {
            [schema, _] => schema.to_lowercase(),
            _ if !x.db_name.is_empty() => x.db_name.to_lowercase(),
            _ => current_db.to_lowercase(),
        });
    }
    labels
}

fn cte_names(with: Option<&tidb_ast::WithClause>) -> BTreeSet<String> {
    with.map(|with| with.ctes.iter().map(|cte| cte.name.to_lowercase()).collect())
        .unwrap_or_default()
}

/// Go `getStmtDbLabel`'s binding cases: the origin statement's table
/// references, else the hinted statement's.
fn get_db_from_binding(target: &BindingStatementTarget, resolver: &Resolver<'_>) -> Vec<String> {
    let labels = get_db_from_binding_stmt(&target.origin, resolver);
    match &target.hinted {
        Some(hinted) if labels.is_empty() => get_db_from_binding_stmt(hinted, resolver),
        _ => labels,
    }
}

/// Go's `tableRef` of a binding statement: a SELECT's `From`, an UPDATE's or
/// DELETE's `TableRefs`, an INSERT's `Table`.
fn get_db_from_binding_stmt(stmt: &Stmt, resolver: &Resolver<'_>) -> Vec<String> {
    let ctes = BTreeSet::new();
    match stmt {
        Stmt::Query(query) => get_db_from_query(query, resolver, &ctes),
        Stmt::Dml(dml) => match &**dml {
            DmlStmt::Insert(x) => resolver.table(&x.table, &ctes),
            DmlStmt::Update(x) => get_db_from_update(&x.kind, resolver, &cte_names(x.with.as_ref())),
            DmlStmt::Delete(x) => get_db_from_delete(&x.kind, resolver, &cte_names(x.with.as_ref())),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

fn get_db_from_update(kind: &UpdateKind, resolver: &Resolver<'_>, ctes: &BTreeSet<String>) -> Vec<String> {
    match kind {
        UpdateKind::Single(table) => get_db_from_table(table, resolver, ctes),
        UpdateKind::Multi { from, .. } => get_db_from_join(from, resolver, ctes),
    }
}

fn get_db_from_delete(kind: &DeleteKind, resolver: &Resolver<'_>, ctes: &BTreeSet<String>) -> Vec<String> {
    match kind {
        DeleteKind::Single(table) => get_db_from_table(table, resolver, ctes),
        DeleteKind::Multi { from, .. } => get_db_from_join(from, resolver, ctes),
    }
}

/// Go `getDbFromResultNode` of a `SelectStmt`: its `FROM`. A set operation
/// reaches no case and labels nothing.
fn get_db_from_query(query: &QueryStmt, resolver: &Resolver<'_>, outer_ctes: &BTreeSet<String>) -> Vec<String> {
    let QueryStmt::Select(select) = query else {
        return Vec::new();
    };
    let mut ctes = outer_ctes.clone();
    ctes.extend(cte_names(select.with.as_ref()));
    select
        .from
        .as_ref()
        .map(|from| get_db_from_join(from, resolver, &ctes))
        .unwrap_or_default()
}

/// Go `getDbFromResultNode` of a `TableName`.
fn get_db_from_table(table: &TableRef, resolver: &Resolver<'_>, ctes: &BTreeSet<String>) -> Vec<String> {
    resolver.table(&table.name, ctes)
}

/// Go `getDbFromResultNode` of a `Join`.
fn get_db_from_join(join: &Join, resolver: &Resolver<'_>, ctes: &BTreeSet<String>) -> Vec<String> {
    let mut db_labels = get_db_from_node(&join.left, resolver, ctes);
    if let Some(right) = &join.right {
        db_labels.extend(get_db_from_node(right, resolver, ctes));
    }
    db_labels
}

/// Go `getDbFromResultNode` of a `TableSource`'s source.
fn get_db_from_node(node: &JoinNode, resolver: &Resolver<'_>, ctes: &BTreeSet<String>) -> Vec<String> {
    match node {
        JoinNode::Table(table) => get_db_from_table(table, resolver, ctes),
        JoinNode::Derived { subquery, .. } => get_db_from_query(subquery, resolver, ctes),
        JoinNode::Join(join) => get_db_from_join(join, resolver, ctes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> Stmt {
        tidb_parser::parse(sql).expect("parses")
    }

    fn labels_with(sql: &str, table_exists: &dyn Fn(&str, &str) -> bool) -> Vec<String> {
        get_stmt_db_label(&parse(sql), "test", table_exists)
            .into_iter()
            .collect()
    }

    fn labels(sql: &str) -> Vec<String> {
        labels_with(sql, &|_, _| true)
    }

    /// Go labels after the preprocessor: an unqualified table by the
    /// current database, a CTE reference by nothing, and an empty label
    /// set by "".
    #[test]
    fn statement_db_labels_follow_gos_rules() {
        assert_eq!(labels("select * from t, db1.u"), ["db1", "test"]);
        assert_eq!(labels("with c as (select 1) select * from c"), [""]);
        assert_eq!(labels("create table t (a int)"), ["test"]);
        assert_eq!(labels("create table DB2.t (a int)"), ["db2"]);
        assert_eq!(labels("insert into t select * from db3.s"), ["db3", "test"]);
        assert_eq!(labels("use DB4"), ["DB4"]);
        assert_eq!(labels("select 1 union select 2"), [""]);
        assert_eq!(labels("begin"), [""]);
        assert_eq!(labels("drop stats t"), ["test"]);
        assert_eq!(labels("admin check table db5.t"), ["db5"]);
        assert_eq!(labels("flush tables t"), ["test"]);
        assert_eq!(labels("recover table t"), [""]);
        assert_eq!(labels("show tables"), ["test"]);
        assert_eq!(labels("show tables from DB6"), ["DB6"]);
        assert_eq!(labels("show create table db7.t"), ["db7"]);
        assert_eq!(labels("set binding disabled for select * from db8.t"), ["db8"]);
    }

    /// Go `checkBindGrammar` resolves only the tables that exist, and DROP
    /// BINDING checks the grammar only with a hinted statement.
    #[test]
    fn binding_db_labels_resolve_existing_tables_only() {
        let create = "create binding for select * from t using select * from t";
        assert_eq!(labels(create), ["test"]);
        assert_eq!(labels_with(create, &|_, _| false), [""]);
        assert_eq!(labels("drop binding for select * from t"), [""]);
        assert_eq!(labels("drop binding for select * from t using select * from t"), ["test"]);
    }
}
