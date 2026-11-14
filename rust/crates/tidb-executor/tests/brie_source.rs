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

//! Go `br/pkg/task/show/cmd_test.go` through the SQL executor.

use tidb_datatype::Datum;
use tidb_executor::StmtOutput;
use tidb_session::Session;

/// Go `TestShowViaSQL`: full Go backupmeta, both session time zones.
#[test]
fn test_show_via_sql() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("backupmeta"),
        include_bytes!("../../../../br/pkg/task/show/testdata/full-schema.meta"),
    )
    .unwrap();
    let mut session = Session::new();
    let sql = format!(
        "SHOW BACKUP METADATA FROM 'local://{}'",
        dir.path().display()
    );
    for (zone, expected) in [
        ("+08:00", "2023-04-10 11:18:21"),
        ("-08:00", "2023-04-09 19:18:21"),
    ] {
        session.run(&format!("set @@time_zone='{zone}'")).unwrap();
        let StmtOutput::Rows { columns, mut rows } = session.run_with_columns(&sql).unwrap() else {
            panic!("SHOW BACKUP METADATA must return rows");
        };
        assert_eq!(
            columns.iter().map(|c| c.0.as_str()).collect::<Vec<_>>(),
            [
                "Database",
                "Table",
                "Total_kvs",
                "Total_bytes",
                "Time_range_start",
                "Time_range_end"
            ]
        );
        rows.sort_by_key(|r| tidb_datatype::datum_text::sysvar_datum_text(&r[1]).unwrap());
        let tables = [
            "customer",
            "district",
            "history",
            "item",
            "new_order",
            "order_line",
            "orders",
            "stock",
            "warehouse",
        ];
        assert_eq!(rows.len(), tables.len());
        for (row, table) in rows.iter().zip(tables) {
            assert_eq!(row[0], Datum::new_string_datum("tpcc"));
            assert_eq!(row[1], Datum::new_string_datum(table));
            assert_eq!(row[2], Datum::Int(0));
            assert_eq!(row[3], Datum::Int(0));
            assert_eq!(row[4], Datum::Null);
            let Datum::Time(end) = row[5] else {
                panic!("datetime result")
            };
            assert_eq!(end.to_string(), expected);
        }
    }
}

/// Go `TestShowBackupQueryRedact`: query inspection over the registered job.
/// Registration is direct here; the physical BACKUP runner is not ported.
#[test]
fn test_show_backup_query_redact() {
    use tidb_ast::{AdminStmt, Stmt};
    use tidb_executor::brie::{BrieTaskInfo, GLOBAL_BRIE_QUEUE};
    let Stmt::Admin(admin) = tidb_parser::parse("backup database * to 's3://nonexist/real?endpoint=http://127.0.0.1&access-key=notleaked&secret-access-key=notleaked'").unwrap() else { panic!() };
    let AdminStmt::Brie(stmt) = &*admin else {
        panic!()
    };
    let (_, id) = GLOBAL_BRIE_QUEUE.register_task(
        &tidb_gocontext::Context::background(),
        BrieTaskInfo::new(&stmt),
    );
    let mut session = Session::new();
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns(&format!("show br job query {id}"))
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        rows,
        vec![vec![Datum::new_string_datum(
            "BACKUP DATABASE * TO 's3://nonexist/real'"
        )]]
    );
}

/// Go `TestCancel`: the CANCEL executor cancels a registered job's context,
/// and a missing job produces Go's warning. The task runner is not exercised.
#[test]
fn test_cancel() {
    use tidb_ast::{AdminStmt, Stmt};
    use tidb_executor::brie::{BrieTaskInfo, GLOBAL_BRIE_QUEUE};
    let Stmt::Admin(admin) = tidb_parser::parse("backup database * to 'noop://'").unwrap() else {
        panic!()
    };
    let AdminStmt::Brie(stmt) = &*admin else {
        panic!()
    };
    let (ctx, id) = GLOBAL_BRIE_QUEUE.register_task(
        &tidb_gocontext::Context::background(),
        BrieTaskInfo::new(&stmt),
    );
    let mut session = Session::new();
    session.run(&format!("cancel br job {id}")).unwrap();
    assert_eq!(ctx.err(), Some(tidb_gocontext::CtxError::Canceled));
    // Go does not erase the task on cancellation.
    let StmtOutput::Rows { rows, .. } = session.run_with_columns("show backups").unwrap() else {
        panic!()
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][2], Datum::new_string_datum("Wait Canceled"));
    assert_eq!(rows[0][3], Datum::Real(100.0));
    session.run("cancel br job 1844674407370955161").unwrap();
    let StmtOutput::Rows { rows, .. } = session.run_with_columns("show warnings").unwrap() else {
        panic!()
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        tidb_datatype::datum_text::sysvar_datum_text(&rows[0][2]).unwrap(),
        "Job ID 1844674407370955161 doesn't exist"
    );
}

/// Go `TestVisitInfo`'s BACKUP/RESTORE privileges, exercised at execution.
#[test]
fn test_visit_info_brie() {
    let registry = tidb_privilege::PrivilegeRegistry::default();
    let mut root = Session::new();
    root.attach_privileges(registry.clone());
    root.run("create user 'brie'@'%'").unwrap();
    let mut session = Session::with_catalog(root.shared_catalog());
    session.set_user("brie@%".into(), "brie@%".into());
    session.attach_privileges(registry.clone());
    for (sql, privilege) in [
        ("SHOW BACKUPS", "BACKUP_ADMIN"),
        ("SHOW RESTORES", "RESTORE_ADMIN"),
        ("SHOW BR JOB QUERY 1", "BACKUP_ADMIN"),
        ("CANCEL BR JOB 1", "BACKUP_ADMIN"),
        ("BACKUP DATABASE test TO 'local:///tmp/a'", "BACKUP_ADMIN"),
        (
            "RESTORE DATABASE test FROM 'local:///tmp/a'",
            "RESTORE_ADMIN",
        ),
    ] {
        let err = session.run(sql).unwrap_err().to_mysql_error();
        assert_eq!(err.code, 1227);
        assert_eq!(err.message, format!("Access denied; you need (at least one of) the SUPER or {privilege} privilege(s) for this operation"));
    }
    root.run("grant BACKUP_ADMIN, RESTORE_ADMIN on *.* to 'brie'@'%'")
        .unwrap();
    session.run("show backups").unwrap();
    session.run("show restores").unwrap();
}

/// Go `TestSecurityEnhancedLocalBackupRestore`, the builder errors on
/// UniStore and SEM v1's local/hdfs restrictions, in Go's order.
#[test]
fn test_security_enhanced_local_backup_restore() {
    let mut session = Session::new();
    for (sql, expected) in [
        (
            "BACKUP DATABASE * TO 'Local:///tmp/test'",
            "BACKUP requires tikv store, not unistore",
        ),
        (
            "RESTORE DATABASE * FROM 'LOCAl:///tmp/test'",
            "RESTORE requires tikv store, not unistore",
        ),
    ] {
        assert_eq!(
            session.run(sql).unwrap_err().to_mysql_error().message,
            expected
        );
    }
    tidb_sem::enable();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            tidb_sem::disable();
        }
    }
    let _reset = Reset;
    for (sql, backend) in [
        ("BACKUP DATABASE * TO 'local:///tmp/test'", "local"),
        ("BACKUP DATABASE * TO 'file:///tmp/test'", "local"),
        ("BACKUP DATABASE * TO '/tmp/test'", "local"),
        ("RESTORE DATABASE * FROM 'LOCAl:///tmp/test'", "local"),
        ("BACKUP DATABASE * TO 'hdfs:///tmp/test'", "hdfs"),
        ("RESTORE DATABASE * FROM 'HDFS:///tmp/test'", "hdfs"),
    ] {
        let err = session.run(sql).unwrap_err().to_mysql_error();
        assert_eq!(err.code, 8132);
        assert_eq!(err.message, format!("Feature '{backend} storage' is not supported when security enhanced mode is enabled"));
    }
}

/// Go `buildBRIESchema` (planbuilder.go), the wire result-set boundary.
#[test]
fn test_brie_result_schema() {
    let session = Session::new();
    assert_eq!(
        session.statement_kind("CANCEL BR JOB 1").unwrap(),
        tidb_session::StmtKind::Write
    );
    for sql in [
        "SHOW BR JOB QUERY 1",
        "SHOW BACKUP METADATA FROM 'local:///tmp/a'",
        "BACKUP DATABASE * TO 'local:///tmp/a'",
        "RESTORE DATABASE * FROM 'local:///tmp/a'",
    ] {
        assert_eq!(
            session.statement_kind(sql).unwrap(),
            tidb_session::StmtKind::Query
        );
    }
}
