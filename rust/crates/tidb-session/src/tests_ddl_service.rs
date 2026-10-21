#![cfg(test)]

//! The session's shared DDL orchestration, driven by a fake service so the
//! behavior around the call (not the publish itself) is pinned.

use crate::ddl_service::DdlService;
use crate::tests_support::*;
use crate::*;
use tidb_exec::cluster_ddl::DdlStatement;
use tidb_exec::real_tikv_ddl::{ClusterDdlReport, DdlWarningLevel};

struct Fake(Result<ClusterDdlReport, (u16, &'static str)>);

impl DdlService for Fake {
    fn execute(
        &self,
        _statement: &DdlStatement,
        _job_context: &tidb_executor::StmtContext,
    ) -> Result<ClusterDdlReport, DriverError> {
        self.0.clone().map_err(|(code, message)| {
            DriverError::Mysql(tidb_executor::MysqlError::sql_error(code, message))
        })
    }
}

fn drop_database(session: &mut Session, name: &str) -> (tidb_ast::Stmt, DdlStatement) {
    let sql = format!("DROP DATABASE {name}");
    let parsed = session.parse_statement(&sql).unwrap();
    (
        parsed,
        DdlStatement::DropDatabase {
            name: name.to_owned(),
            if_exists: false,
        },
    )
}

#[test]
fn a_dropped_current_database_is_deselected_and_job_warnings_reach_the_session() {
    let mut session = Session::new();
    session.run("CREATE DATABASE d_svc").unwrap();
    session.run("USE d_svc").unwrap();
    let (parsed, statement) = drop_database(&mut session, "d_svc");
    let service = Fake(Ok(ClusterDdlReport::Applied {
        schema_version: 7,
        created_id: None,
        warnings: vec![
            (DdlWarningLevel::Note, 1007, "a note".to_owned()),
            (DdlWarningLevel::Warning, 1105, "a warning".to_owned()),
        ],
    }));
    session.begin_ddl_statement(&parsed).unwrap();
    session
        .run_ddl_statement("DROP DATABASE d_svc", &parsed, &statement, &service)
        .unwrap();
    // Go `dropSchema`: the next unqualified statement answers ErrNoDB.
    assert_eq!(session.current_database(), "");
    let warnings = row_text(session.run("SHOW WARNINGS"));
    assert_eq!(
        warnings,
        [
            ["Note", "1007", "a note"],
            ["Warning", "1105", "a warning"],
        ]
    );
}

#[test]
fn a_failed_job_leaves_the_session_untouched_and_surfaces_the_service_error() {
    let mut session = Session::new();
    session.run("CREATE DATABASE d_svc2").unwrap();
    session.run("USE d_svc2").unwrap();
    let (parsed, statement) = drop_database(&mut session, "d_svc2");
    let service = Fake(Err((1105, "the owner is gone")));
    let error = session
        .run_ddl_statement("DROP DATABASE d_svc2", &parsed, &statement, &service)
        .unwrap_err();
    let mysql = error.to_mysql_error();
    assert_eq!((mysql.code, mysql.message.as_str()), (1105, "the owner is gone"));
    assert_eq!(session.current_database(), "d_svc2");
}
