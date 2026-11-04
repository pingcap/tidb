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
//! Go reads a table's database through the resolve context (`GetTableName`,
//! the table's `DBInfo`); an unqualified table there is the current
//! database, and a CTE reference resolves to no table.

use std::collections::BTreeSet;

use tidb_ast::{
    AdminStmt, DdlStmt, DeleteKind, DmlStmt, Join, JoinNode, QueryStmt, SessionStmt, Stmt, TableRef,
    UpdateKind,
};

/// Go `CountStmtNode`. `stmt` is the statement whose label is counted: a
/// prepared statement's own node for an EXECUTE.
pub fn count_stmt_node(stmt: &Stmt, current_db: &str, in_restricted_sql: bool, resource_group: &str) {
    if in_restricted_sql {
        return;
    }
    let type_label = stmt.label();
    let status = &tidb_config::config_tree::config::get_global_config().status;
    if status.record_qps_by_db || status.record_db_label {
        let db_labels = get_stmt_db_label(stmt, current_db);
        if status.record_qps_by_db {
            for db_label in &db_labels {
                tidb_metrics::executor::DB_STATEMENT_TOTAL
                    .with_label_values(&[db_label.as_str(), type_label])
                    .inc();
            }
        } else {
            for db_label in &db_labels {
                tidb_metrics::executor::STATEMENT_TOTAL
                    .with_label_values(&[db_label.as_str(), resource_group, type_label])
                    .inc();
            }
        }
    } else {
        tidb_metrics::executor::STATEMENT_TOTAL
            .with_label_values(&["", resource_group, type_label])
            .inc();
    }
}

/// Go `x.Table.Schema.L`: the schema the statement names, empty when
/// unqualified.
fn named_schema(name: &[String]) -> String {
    match name {
        [schema, _] => schema.to_lowercase(),
        _ => String::new(),
    }
}

/// Go `resolveCtx.GetTableName(x).DBInfo.Name.L`.
fn resolved_schema(name: &[String], current_db: &str) -> String {
    match name {
        [schema, _] => schema.to_lowercase(),
        _ => current_db.to_lowercase(),
    }
}

/// Go `getStmtDbLabel`.
pub fn get_stmt_db_label(stmt: &Stmt, current_db: &str) -> BTreeSet<String> {
    let mut db_label_set = BTreeSet::new();
    let ctes = BTreeSet::new();
    match stmt {
        Stmt::Ddl(ddl) => match &**ddl {
            DdlStmt::AlterTable(x) => {
                db_label_set.insert(named_schema(&x.name));
            }
            DdlStmt::CreateIndex(x) => {
                db_label_set.insert(named_schema(&x.table));
            }
            DdlStmt::CreateTable(x) => {
                db_label_set.insert(named_schema(&x.name));
            }
            DdlStmt::DropIndex(x) => {
                db_label_set.insert(named_schema(&x.table));
            }
            DdlStmt::TruncateTable(x) => {
                db_label_set.insert(named_schema(x));
            }
            DdlStmt::FlashbackTable(x) => {
                if let Some(table) = &x.table {
                    db_label_set.insert(named_schema(table));
                }
            }
            DdlStmt::RecoverTable(x) => {
                if let Some(table) = &x.table {
                    db_label_set.insert(named_schema(table));
                }
            }
            DdlStmt::CreateView(x) => {
                db_label_set.insert(named_schema(&x.name));
            }
            DdlStmt::RenameTable(x) => {
                for (old_table, _) in &x.pairs {
                    db_label_set.insert(named_schema(old_table));
                }
            }
            DdlStmt::DropTable(x) => {
                for table in &x.names {
                    db_label_set.insert(named_schema(table));
                }
            }
            _ => {}
        },
        Stmt::Dml(dml) => match &**dml {
            DmlStmt::Insert(x) => {
                db_label_set.insert(resolved_schema(&x.table, current_db));
                if let Some(source) = &x.source {
                    db_label_set.extend(get_db_from_query(source, current_db, &ctes));
                }
            }
            DmlStmt::Update(x) => {
                let ctes = cte_names(x.with.as_ref());
                match &x.kind {
                    UpdateKind::Single(table) => db_label_set.extend(get_db_from_table(table, current_db, &ctes)),
                    UpdateKind::Multi { from, .. } => {
                        db_label_set.extend(get_db_from_join(from, current_db, &ctes));
                    }
                }
            }
            DmlStmt::Delete(x) => {
                let ctes = cte_names(x.with.as_ref());
                match &x.kind {
                    DeleteKind::Single(table) => db_label_set.extend(get_db_from_table(table, current_db, &ctes)),
                    DeleteKind::Multi { from, .. } => {
                        db_label_set.extend(get_db_from_join(from, current_db, &ctes));
                    }
                }
            }
            DmlStmt::LoadData(x) => {
                db_label_set.insert(named_schema(&x.table));
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
            if let QueryStmt::Select(_) = &**query {
                db_label_set.extend(get_db_from_query(query, current_db, &ctes));
            }
        }
        Stmt::Admin(admin) => match &**admin {
            AdminStmt::AnalyzeTable(x) => {
                for table in &x.tables {
                    db_label_set.insert(named_schema(table));
                }
            }
            AdminStmt::SplitRegion(x) => {
                db_label_set.insert(named_schema(&x.table));
            }
            AdminStmt::CompactTable(x) => {
                db_label_set.insert(named_schema(&x.table));
            }
            _ => {}
        },
        Stmt::Session(session) => {
            if let SessionStmt::Use(db_name) = &**session {
                db_label_set.insert(db_name.clone());
            }
        }
        _ => {}
    }
    db_label_set
}

fn cte_names(with: Option<&tidb_ast::WithClause>) -> BTreeSet<String> {
    with.map(|with| with.ctes.iter().map(|cte| cte.name.to_lowercase()).collect())
        .unwrap_or_default()
}

/// Go `getDbFromResultNode` of a `SelectStmt`: its `FROM`. A set operation
/// reaches no case and labels nothing.
fn get_db_from_query(query: &QueryStmt, current_db: &str, outer_ctes: &BTreeSet<String>) -> Vec<String> {
    let QueryStmt::Select(select) = query else {
        return Vec::new();
    };
    let mut ctes = outer_ctes.clone();
    ctes.extend(cte_names(select.with.as_ref()));
    select
        .from
        .as_ref()
        .map(|from| get_db_from_join(from, current_db, &ctes))
        .unwrap_or_default()
}

/// Go `getDbFromResultNode` of a `TableName`.
fn get_db_from_table(table: &TableRef, current_db: &str, ctes: &BTreeSet<String>) -> Vec<String> {
    if let [name] = table.name.as_slice() {
        if ctes.contains(&name.to_lowercase()) {
            return Vec::new();
        }
    }
    vec![resolved_schema(&table.name, current_db)]
}

/// Go `getDbFromResultNode` of a `Join`.
fn get_db_from_join(join: &Join, current_db: &str, ctes: &BTreeSet<String>) -> Vec<String> {
    let mut db_labels = get_db_from_node(&join.left, current_db, ctes);
    if let Some(right) = &join.right {
        db_labels.extend(get_db_from_node(right, current_db, ctes));
    }
    db_labels
}

/// Go `getDbFromResultNode` of a `TableSource`'s source.
fn get_db_from_node(node: &JoinNode, current_db: &str, ctes: &BTreeSet<String>) -> Vec<String> {
    match node {
        JoinNode::Table(table) => get_db_from_table(table, current_db, ctes),
        JoinNode::Derived { subquery, .. } => get_db_from_query(subquery, current_db, ctes),
        JoinNode::Join(join) => get_db_from_join(join, current_db, ctes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> Stmt {
        tidb_parser::parse(sql).expect("parses")
    }

    /// Go labels a table by its resolved database, a DDL by the schema it
    /// names, and a CTE reference by nothing.
    #[test]
    fn statement_db_labels_follow_gos_rules() {
        let labels = |sql: &str| get_stmt_db_label(&parse(sql), "test").into_iter().collect::<Vec<_>>();
        assert_eq!(labels("select * from t, db1.u"), ["db1", "test"]);
        assert_eq!(labels("with c as (select 1) select * from c"), Vec::<String>::new());
        assert_eq!(labels("create table t (a int)"), [""]);
        assert_eq!(labels("create table DB2.t (a int)"), ["db2"]);
        assert_eq!(labels("insert into t select * from db3.s"), ["db3", "test"]);
        assert_eq!(labels("use DB4"), ["DB4"]);
        assert_eq!(labels("select 1 union select 2"), Vec::<String>::new());
    }
}
