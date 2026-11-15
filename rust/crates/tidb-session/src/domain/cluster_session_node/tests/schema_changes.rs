//! The session's side of DDL on a mock node: how it carries the DDL
//! executor's reports, warnings and refusals to the client. DDL that runs
//! is tested on the in-process cluster (`unistore_cop/session_schema_changes.rs`).

use tidb_ddl::real_tikv_ddl::ClusterDdlReport;
use super::super::*;
use super::node_fixture::*;
use crate::domain::configured_user_store::ConfiguredUserStore;
use tidb_ddl::real_tikv_ddl::cluster_ddl_error;
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_datatype::Datum;
use tidb_store_driver_error::pessimistic_lock_error::commit_outcome_to_sql_error;
use tidb_ddl::real_tikv_ddl::classify_session_ddl_commit_error;
use tidb_txnkv::transaction::{OptimisticCommitOutcome, OptimisticTransactionReceipt, RolledBackTransaction, TransactionCause, UndeterminedTransaction};



/// Supplies the authority's report without reimplementing its DDL planner.
/// The session must preserve every warning, irrespective of report variant.
struct ReportDdl {
    expected: Expected,
    report: ClusterDdlReport,
}

/// The one statement a [`ReportDdl`] answers.
enum Expected {
    /// `DROP TABLE IF EXISTS` over these `(schema, table)` names.
    DropTables(Vec<(String, String)>),
    /// `ALTER TABLE` on this name path.
    AlterTable(Vec<String>),
}

impl ClusterDdl for ReportDdl {
    fn create_schema_with_info(
        &self,
        _: tidb_model::DBInfo,
        _: tidb_ddl::OnExist,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute persisted jobs"))
    }

    fn create_table_with_info(
        &self,
        _: &str,
        _: tidb_model::TableInfo,
        _: &[tidb_model::InvolvingSchemaInfo],
        _: tidb_ddl::CreateTableConfig,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute persisted jobs"))
    }

    fn batch_create_table_with_info(
        &self,
        _: &str,
        _: Vec<tidb_model::TableInfo>,
        _: tidb_ddl::CreateTableConfig,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute persisted jobs"))
    }

    fn create_placement_policy_with_info(
        &self,
        _: tidb_model::PolicyInfo,
        _: tidb_ddl::OnExist,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute persisted jobs"))
    }

    fn update_table_replica_info(
        &self,
        _: i64,
        _: bool,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn alter_table(
        &self,
        statement: &tidb_ast::AlterTableStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let Expected::AlterTable(expected) = &self.expected else {
            panic!("unexpected ALTER TABLE: {statement:?}");
        };
        assert_eq!(&statement.name, expected);
        Ok(self.report.clone())
    }

    fn recover_schema(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn recover_table(
        &self,
        _: &Option<Vec<String>>,
        _: i64,
        _: &str,
        _: bool,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn drop_index(
        &self,
        _: &tidb_ast::DropIndexStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn create_index(
        &self,
        _: &tidb_ast::CreateIndexStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn alter_table_set_tiflash_replica(
        &self,
        _: &str,
        _: &str,
        _: u64,
        _: &[String],
        _: bool,
        _: bool,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn alter_schema(
        &self,
        _: &str,
        _: &[tidb_ast::DatabaseOption],
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn drop_placement_policy(
        &self,
        _: &tidb_ast::DropPlacementPolicyStmt,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn alter_placement_policy(
        &self,
        _: &tidb_ast::AlterPlacementPolicyStmt,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn create_placement_policy(
        &self,
        _: &tidb_ast::CreatePlacementPolicyStmt,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn refresh_meta(
        &self,
        _: i64,
        _: i64,
        _: &str,
        _: &str,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn alter_table_mode(
        &self,
        _: tidb_model::TableMode,
        _: i64,
        _: i64,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn lock_tables(
        &self,
        _: &[tidb_ast::TableLock],
        _: &str,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        unreachable!("these tests take no table lock")
    }

    fn unlock_tables(
        &self,
        unlock_tables: Vec<tidb_model::TableLockTpInfo>,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        assert!(unlock_tables.is_empty(), "these tests take no table lock");
        Ok(ClusterDdlReport::AlreadySatisfied { detail: "no table is locked".into(), warnings: Vec::new() })
    }

    fn cleanup_table_lock(
        &self,
        _: &[Vec<String>],
        _: &str,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        unreachable!("these tests take no table lock")
    }

    fn create_masking_policy(
        &self,
        _: &tidb_ast::CreateMaskingPolicyStmt,
        _: &str,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        unreachable!("these tests create no masking policy")
    }

    fn create_view(
        &self,
        _: &str,
        _: &str,
        _: bool,
        _: &tidb_model::TableInfo,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn do_ddl_job_wrapper(
        &self,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
        _: tidb_ddl::JobWrapper,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown(
            "the mock catalog writer does not execute persisted jobs",
        ))
    }

    fn drop_table(
        &self,
        statement: &tidb_ast::DropTableStmt,
        current_db: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let Expected::DropTables(expected) = &self.expected else {
            panic!("unexpected DROP TABLE: {statement:?}");
        };
        let names: Vec<_> = statement
            .names
            .iter()
            .map(|path| match path.as_slice() {
                [schema, table] => (schema.clone(), table.clone()),
                [table] => (current_db.to_owned(), table.clone()),
                _ => panic!("unexpected name path {path:?}"),
            })
            .collect();
        assert_eq!((&names, statement.if_exists), (expected, true));
        Ok(self.report.clone())
    }

    fn drop_sequence(
        &self,
        _: &tidb_ast::DropSequenceStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn rename_table(
        &self,
        _: &tidb_ast::RenameTableStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn drop_view(
        &self,
        _: &[Vec<String>],
        _: bool,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn drop_schema(
        &self,
        _: &str,
        _: bool,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        panic!("this fixture reports DROP TABLE and option-only ALTER TABLE only")
    }

    fn create_table(
        &self,
        statement: &tidb_ast::CreateTableStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, current_db, context);
        panic!("this fixture reports DROP statements only")
    }

    fn create_sequence(
        &self,
        statement: &tidb_ast::CreateSequenceStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, current_db, context);
        panic!("this fixture reports DROP statements only")
    }

    fn alter_sequence(
        &self,
        statement: &tidb_ast::AlterSequenceStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, current_db, context);
        panic!("this fixture reports DROP statements only")
    }

    fn repair_table(
        &self,
        statement: &tidb_ast::RepairTableStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, current_db, context);
        panic!("this fixture reports DROP statements only")
    }

    fn truncate_table(
        &self,
        statement: &[String],
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, current_db, context);
        panic!("this fixture reports DROP statements only")
    }

    fn create_schema(
        &self,
        name: &str,
        if_not_exists: bool,
        options: &[tidb_ast::DatabaseOption],
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (name, if_not_exists, options, context);
        panic!("this fixture reports DROP statements only")
    }

    // A cluster without TiFlash stores.
    fn tiflash_store_count(&self) -> Result<u64, SqlQueryError> {
        Ok(0)
    }
}

#[test]
fn ddl_reports_preserve_warning_order_levels_and_statement_lifetime() {
    use tidb_context::WarningLevel::{Note, Warning};

    let node = MockNode::start();
    let notes = vec![
        tidb_context::SqlWarn::with_code(Note, 1051, "Unknown table 'app.absent_a'"),
        tidb_context::SqlWarn::with_code(Note, 1051, "Unknown table 'app.absent_b'"),
    ];
    // An applied multi-drop and an entirely suppressed multi-drop carry the
    // same ordered notes. Neither report variant determines the level.
    for applied in [true, false] {
        let report = if applied {
            ClusterDdlReport::Applied {
                schema_version: 12,
                created_id: None,
                warnings: notes.clone(),
            }
        } else {
            ClusterDdlReport::AlreadySatisfied {
                detail: "missing tables".to_owned(),
                warnings: notes.clone(),
            }
        };
        let mut names = vec![
            ("app".to_owned(), "absent_a".to_owned()),
            ("app".to_owned(), "absent_b".to_owned()),
        ];
        let sql = if applied {
            names.insert(0, ("app".to_owned(), "t".to_owned()));
            "DROP TABLE IF EXISTS t, absent_a, absent_b"
        } else {
            "DROP TABLE IF EXISTS absent_a, absent_b"
        };
        let mut session = open_session_on_with_ddl(
            &node,
            Arc::new(ReportDdl {
                expected: Expected::DropTables(names),
                report,
            }),
        );
        session
            .node_conn()
            .execute_write(sql)
            .expect("route multi-table DROP");
        let expected: Vec<_> = notes
            .iter()
            .map(|note| {
                vec![
                    Datum::new_string_datum("Note"),
                    Datum::UInt(u64::from(note.code())),
                    Datum::new_string_datum(note.message()),
                ]
            })
            .collect();
        assert_eq!(rows(&mut session, "SHOW WARNINGS"), expected);
        assert_eq!(rows(&mut session, "SHOW WARNINGS"), expected);
        rows(&mut session, "SELECT 1");
        assert!(rows(&mut session, "SHOW WARNINGS").is_empty());
    }

    let message = "ALTER TABLE WITH VALIDATION is currently unsupported".to_owned();
    let mut session = open_session_on_with_ddl(
        &node,
        Arc::new(ReportDdl {
            expected: Expected::AlterTable(vec!["t".to_owned()]),
            report: ClusterDdlReport::AlreadySatisfied {
                detail: "no change".to_owned(),
                warnings: vec![tidb_context::SqlWarn::with_code(Warning, 8200, message.clone())],
            },
        }),
    );
    session
        .node_conn()
        .execute_write("ALTER TABLE t WITH VALIDATION")
        .expect("route option-only ALTER");
    assert_eq!(
        rows(&mut session, "SHOW WARNINGS"),
        vec![vec![
            Datum::new_string_datum("Warning"),
            Datum::UInt(8200),
            Datum::new_string_datum(message),
        ]]
    );
    assert_eq!(node.catalog.get_latest().schema_meta_version(), 11);
}

struct UndeterminedDdl(SqlQueryError);

impl ClusterDdl for UndeterminedDdl {
    fn create_schema_with_info(
        &self,
        _: tidb_model::DBInfo,
        _: tidb_ddl::OnExist,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(self.0.clone())
    }

    fn create_table_with_info(
        &self,
        _: &str,
        _: tidb_model::TableInfo,
        _: &[tidb_model::InvolvingSchemaInfo],
        _: tidb_ddl::CreateTableConfig,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(self.0.clone())
    }

    fn batch_create_table_with_info(
        &self,
        _: &str,
        _: Vec<tidb_model::TableInfo>,
        _: tidb_ddl::CreateTableConfig,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(self.0.clone())
    }

    fn create_placement_policy_with_info(
        &self,
        _: tidb_model::PolicyInfo,
        _: tidb_ddl::OnExist,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(self.0.clone())
    }

    fn update_table_replica_info(
        &self,
        _: i64,
        _: bool,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(self.0.clone())
    }

    fn alter_table(
        &self,
        statement: &tidb_ast::AlterTableStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, current_db, context);
        Err(self.0.clone())
    }

    fn recover_schema(
        &self,
        name: &str,
        new_name: &str,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (name, new_name, current_db, context);
        Err(self.0.clone())
    }

    fn recover_table(
        &self,
        table: &Option<Vec<String>>,
        job_id: i64,
        new_name: &str,
        flashback: bool,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (table, job_id, new_name, flashback, current_db, context);
        Err(self.0.clone())
    }

    fn drop_index(
        &self,
        statement: &tidb_ast::DropIndexStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, current_db, context);
        Err(self.0.clone())
    }

    fn create_index(
        &self,
        statement: &tidb_ast::CreateIndexStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, current_db, context);
        Err(self.0.clone())
    }

    fn alter_table_set_tiflash_replica(
        &self,
        schema: &str,
        table: &str,
        count: u64,
        labels: &[String],
        reset_available: bool,
        hypo: bool,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (schema, table, count, labels, reset_available, hypo, context);
        Err(self.0.clone())
    }

    fn alter_schema(
        &self,
        name: &str,
        options: &[tidb_ast::DatabaseOption],
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (name, options, context);
        Err(self.0.clone())
    }

    fn drop_placement_policy(
        &self,
        statement: &tidb_ast::DropPlacementPolicyStmt,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, context);
        Err(self.0.clone())
    }

    fn alter_placement_policy(
        &self,
        statement: &tidb_ast::AlterPlacementPolicyStmt,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, context);
        Err(self.0.clone())
    }

    fn create_placement_policy(
        &self,
        statement: &tidb_ast::CreatePlacementPolicyStmt,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, context);
        Err(self.0.clone())
    }

    fn refresh_meta(
        &self,
        schema_id: i64,
        table_id: i64,
        involved_db: &str,
        involved_table: &str,
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (schema_id, table_id, involved_db, involved_table, context);
        Err(self.0.clone())
    }

    fn alter_table_mode(
        &self,
        table_mode: tidb_model::TableMode,
        schema_id: i64,
        table_id: i64,
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (table_mode, schema_id, table_id, context);
        Err(self.0.clone())
    }

    fn lock_tables(
        &self,
        _: &[tidb_ast::TableLock],
        _: &str,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        unreachable!("these tests take no table lock")
    }

    fn unlock_tables(
        &self,
        unlock_tables: Vec<tidb_model::TableLockTpInfo>,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        assert!(unlock_tables.is_empty(), "these tests take no table lock");
        Ok(ClusterDdlReport::AlreadySatisfied { detail: "no table is locked".into(), warnings: Vec::new() })
    }

    fn cleanup_table_lock(
        &self,
        _: &[Vec<String>],
        _: &str,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        unreachable!("these tests take no table lock")
    }

    fn create_masking_policy(
        &self,
        _: &tidb_ast::CreateMaskingPolicyStmt,
        _: &str,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        unreachable!("these tests create no masking policy")
    }

    fn create_view(
        &self,
        schema: &str,
        name: &str,
        or_replace: bool,
        info: &tidb_model::TableInfo,
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (schema, name, or_replace, info, context);
        Err(self.0.clone())
    }

    fn do_ddl_job_wrapper(
        &self,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
        _: tidb_ddl::JobWrapper,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(self.0.clone())
    }

    fn drop_table(
        &self,
        statement: &tidb_ast::DropTableStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, current_db, context);
        Err(self.0.clone())
    }
    fn drop_sequence(
        &self,
        statement: &tidb_ast::DropSequenceStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, current_db, context);
        Err(self.0.clone())
    }
    fn rename_table(
        &self,
        statement: &tidb_ast::RenameTableStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, current_db, context);
        Err(self.0.clone())
    }
    fn drop_view(
        &self,
        names: &[Vec<String>],
        if_exists: bool,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (names, if_exists, current_db, context);
        Err(self.0.clone())
    }
    fn drop_schema(
        &self,
        name: &str,
        if_exists: bool,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (name, if_exists, context);
        Err(self.0.clone())
    }

    fn create_table(
        &self,
        statement: &tidb_ast::CreateTableStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, current_db, context);
        Err(self.0.clone())
    }

    fn create_sequence(
        &self,
        statement: &tidb_ast::CreateSequenceStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, current_db, context);
        Err(self.0.clone())
    }

    fn alter_sequence(
        &self,
        statement: &tidb_ast::AlterSequenceStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, current_db, context);
        Err(self.0.clone())
    }

    fn repair_table(
        &self,
        statement: &tidb_ast::RepairTableStmt,
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, current_db, context);
        Err(self.0.clone())
    }

    fn truncate_table(
        &self,
        statement: &[String],
        current_db: &str,
        context: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (statement, current_db, context);
        Err(self.0.clone())
    }

    fn create_schema(
        &self,
        name: &str,
        if_not_exists: bool,
        options: &[tidb_ast::DatabaseOption],
        context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let _ = (name, if_not_exists, options, context);
        Err(self.0.clone())
    }

    // A cluster without TiFlash stores.
    fn tiflash_store_count(&self) -> Result<u64, SqlQueryError> {
        Ok(0)
    }
}

#[test]
fn full_cluster_ddl_keeps_an_undetermined_verdict_connection_fatal() {
    let outcome = OptimisticCommitOutcome::Undetermined(UndeterminedTransaction {
        receipt: OptimisticTransactionReceipt::new(1, 2, b"key".to_vec(), 1),
        cause: TransactionCause::Transport {
            detail: "commit response lost".to_owned(),
        },
    });
    let lock_error = commit_outcome_to_sql_error(&outcome)
        .expect_err("an unknown commit verdict must not answer success");
    let ddl_error = classify_session_ddl_commit_error(61, lock_error);
    let query_error = cluster_ddl_error(ddl_error);
    let node = MockNode::start();
    let mut session = open_session_on_with_ddl(&node, Arc::new(UndeterminedDdl(query_error)));
    let query_error = session
        .node_conn()
        .execute_write("CREATE DATABASE uncertain")
        .expect_err("the DDL cannot answer success when its commit response was lost");
    assert_undetermined_closes_without_packet(&query_error);
}

#[test]
fn full_cluster_ddl_keeps_a_region_backoff_error_coded_on_the_wire() {
    let outcome = OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
        receipt: OptimisticTransactionReceipt::new(1, 2, b"key".to_vec(), 1),
        cause: TransactionCause::BackoffExhausted {
            terminal: tikv_client::retry::BO_REGION_SCHEDULING.terminal_error,
            detail: "regionScheduling backoffer exhausted".to_owned(),
        },
    });
    let lock_error = commit_outcome_to_sql_error(&outcome)
        .expect_err("an exhausted region backoff cannot answer success");
    let query_error = cluster_ddl_error(classify_session_ddl_commit_error(61, lock_error));
    assert_eq!(
        query_error.code,
        tidb_errno::errcode::ErrRegionUnavailable
    );
    assert_query_error_packet(
        &query_error,
        tidb_errno::errcode::ErrRegionUnavailable,
        "Region is unavailable",
    );
}

#[test]
fn auto_random_rebase_errors_keep_their_tidb_codes_on_the_cluster_wire() {
    let invalid = cluster_ddl_error(tidb_ddl::real_tikv_ddl::ClusterDdlError::Plan(
        tidb_ddl_build::plan_error::DdlPlanError::from(
            tidb_dbterror::ERR_INVALID_AUTO_RANDOM.gen_with_stack_by_args(&[
                ("alter auto_random_base of a non auto_random table".to_owned()).into(),
            ]),
        ),
    ));
    assert_query_error_packet(
        &invalid,
        8216,
        "Invalid auto random: alter auto_random_base of a non auto_random table",
    );

    let zero = cluster_ddl_error(tidb_ddl::real_tikv_ddl::ClusterDdlError::Plan(
        tidb_ddl_build::plan_error::DdlPlanError::from(
            tidb_autoid::autoid_errors::ERR_AUTOINC_READ_FAILED.gen_with_stack_by_args(&[]),
        ),
    ));
    assert_query_error_packet(
        &zero,
        1467,
        "Failed to read auto-increment value from storage engine",
    );
}

/// Every catalog refusal already rendered Go's own message, but only two
/// variants carried a CODE, so the rest reached a client as 1105 -- the code
/// a MySQL client reads as "unknown error" rather than the specific one it
/// switches on. `pkg/errno` names each; these pin the pairing.
#[test]
fn a_catalog_refusal_carries_the_code_mysql_clients_switch_on() {
    use tidb_ddl_build::plan_error::DdlPlanError;

    let coded = |error: DdlPlanError| {
        let query_error = cluster_ddl_error(tidb_ddl::real_tikv_ddl::ClusterDdlError::Plan(error));
        (query_error.code, query_error.message)
    };

    let (code, message) = coded(DdlPlanError::from(
        tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
            .gen_with_stack_by_args(&[("nope".to_owned()).into()]),
    ));
    assert_eq!(code, 1049);
    assert_eq!(message, "Unknown database 'nope'");

    let (code, message) = coded(DdlPlanError::from(
        tidb_infoschema::infoschema_errors::ERR_DATABASE_EXISTS
            .gen_with_stack_by_args(&[("dup".to_owned()).into()]),
    ));
    assert_eq!(code, 1007);
    assert_eq!(message, "Can't create database 'dup'; database exists");

    let (code, message) = coded(DdlPlanError::from(
        tidb_infoschema::infoschema_errors::ERR_TABLE_DROP_EXISTS.gen_with_stack_by_args(&[
            format!("{}.{}", ("s".to_owned()), ("t".to_owned())).into(),
        ]),
    ));
    assert_eq!(code, 1051);
    assert_eq!(message, "Unknown table 's.t'");

    let (code, message) =
        coded(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS.gen_with_stack_by_args(&[
                format!("{}.{}", ("s".to_owned()), ("t".to_owned())).into(),
            ]),
        ));
    assert_eq!(code, 1050);
    assert_eq!(message, "Table 's.t' already exists");

    let (code, message) = coded(DdlPlanError::from(
        tidb_infoschema::infoschema_errors::ERR_COLUMN_EXISTS
            .gen_with_stack_by_args(&[("a".to_owned()).into()]),
    ));
    assert_eq!(code, 1060);
    assert_eq!(message, "Duplicate column name 'a'");

    let (code, message) = coded(DdlPlanError::from(
        tidb_dbterror::ERR_DUP_KEY_NAME
            .gen_with_stack_by_args(&[("ix".to_owned()).into()]),
    ));
    assert_eq!(code, 1061);
    assert_eq!(message, "Duplicate key name 'ix'");

    let (code, message) = coded(DdlPlanError::from(
        tidb_dbterror::ERR_KEY_COLUMN_DOES_NOT_EXITS
            .gen_with_stack(format!("column does not exist: {}", ("c".to_owned()))),
    ));
    assert_eq!((code, message.as_str()), (1072, "column does not exist: c"));
}

/// REPRODUCTION PROBE (rung 8's 9007-at-COMMIT): a schema move landing under
/// an open transaction must not fail its COMMIT with the narrow guard's
/// "Write conflict, please retry the transaction" -- with MDL on (the
/// default), Go's `validator.Check` skips the delta comparison entirely
/// (`pkg/infoschema/isvalidator/validator.go:236-241`), because the DDL
/// waits for the transaction instead.
#[test]
fn commit_survives_a_schema_move_under_the_transaction() {
    let (mut session, node) = open_session();
    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    let statement = session
        .node_conn().prepare_general("UPDATE t SET v = 90 WHERE id = 9")
        .expect("prepare");
    session
        .node_conn().execute_general(&statement, &[])
        .expect("update inside the transaction");

    // The reloader's shape: a Go peer's DDL bumps the cluster catalog while
    // this transaction is open. The catalog CONTENT is unchanged -- rung 8's
    // failing workload never touches the DDL'd table.
    let mut moved = (*node.catalog.get_latest()).clone();
    moved.set_schema_meta_version(moved.schema_meta_version() + 1);
    node.catalog.upsert(moved);

    let outcome = session.node_conn().control_transaction("COMMIT");
    assert!(
        outcome.is_ok(),
        "an unrelated schema move must not fail COMMIT: {:?}",
        outcome.err()
    );
}

/// Go resolves the table before the planner's privilege check: an
/// unsupported ALTER specification on a missing table answers 1146 even to
/// a user without ALTER, and 1142 on a table that exists (captured from Go
/// on this branch).
#[test]
fn an_unsupported_alter_spec_checks_the_table_before_privileges() {
    let node = MockNode::start();
    node.accounts.create_user("low");
    node.accounts.grant_db("low", "app", &["SELECT"]);
    // The mock node's `app` schema already holds a table `t`.
    let store = ConfiguredUserStore::from_accounts(node.accounts.live.clone());
    let identity = store
        .authenticate_native("low", "127.0.0.1", &SALT, &[])
        .expect("the SELECT-only account authenticates");
    let mut low = open_session_on_with_context(&node, session_context_with_identity(22, identity));
    low.node_conn().execute_write("USE app").unwrap();
    let missing = low
        .node_conn()
        .execute_write("ALTER TABLE nosuch REBUILD PARTITION p0")
        .unwrap_err();
    assert_eq!(
        (missing.code, missing.message.as_str()),
        (1146, "Table 'app.nosuch' doesn't exist")
    );
    let denied = low
        .node_conn()
        .execute_write("ALTER TABLE t REBUILD PARTITION p0")
        .unwrap_err();
    assert_eq!(
        (denied.code, denied.message.as_str()),
        (1142, "ALTER command denied to user 'low'@'%' for table 't'")
    );
}

/// Go's preprocessor resolves the table before the planner's privilege
/// check: a missing table is 1146 to an account holding any privilege on it,
/// and a 1142 naming the statement type (and the table as written) to one
/// holding none (captured from Go on this branch).
#[test]
fn a_missing_ddl_table_is_masked_only_for_an_account_holding_nothing() {
    let node = MockNode::start();
    node.accounts.create_user("low");
    node.accounts.grant_db("low", "app", &["SELECT"]);
    node.accounts.create_user("none");
    let store = ConfiguredUserStore::from_accounts(node.accounts.live.clone());
    let mut low = open_session_on_with_context(
        &node,
        session_context_with_identity(
            23,
            store
                .authenticate_native("low", "127.0.0.1", &SALT, &[])
                .unwrap(),
        ),
    );
    low.node_conn().execute_write("USE app").unwrap();
    for sql in [
        "ALTER TABLE missing ADD COLUMN c INT",
        "CREATE INDEX i ON missing (a)",
        "DROP INDEX i ON missing",
        "TRUNCATE TABLE missing",
        "CREATE TABLE t3 LIKE missing",
    ] {
        let error = low.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (1146, "Table 'app.missing' doesn't exist"),
            "{sql}"
        );
    }
    let mut none = open_session_on_with_context(
        &node,
        session_context_with_identity(
            24,
            store
                .authenticate_native("none", "127.0.0.1", &SALT, &[])
                .unwrap(),
        ),
    );
    for (sql, verb) in [
        ("ALTER TABLE app.Missing ADD COLUMN c INT", "ALTER"),
        ("CREATE INDEX i ON app.Missing (a)", "CREATE"),
        ("DROP INDEX i ON app.Missing", "SELECT"),
        ("TRUNCATE TABLE app.Missing", "SELECT"),
    ] {
        let error = none.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message),
            (
                1142,
                format!("{verb} command denied to user 'none'@'%' for table 'Missing'")
            ),
            "{sql}"
        );
    }
    let error = none
        .node_conn()
        .execute_write("CREATE TABLE app.t9 LIKE app.Missing")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1146, "Table 'app.Missing' doesn't exist")
    );
}
