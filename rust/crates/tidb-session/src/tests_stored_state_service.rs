#![cfg(test)]

//! The session's account and `SET GLOBAL` orchestration, driven by fake
//! services so what happens around the persistence call is pinned: the
//! statement runs against the scratch copy, a failure commits nothing, and
//! the live copy is put back either way.

use std::cell::Cell;
use std::rc::Rc;

use crate::privilege::PrivilegeRegistry;
use crate::stored_state_service::{
    AccountService, PendingAccountChange, PendingSysvarChange, SysvarService,
};
use crate::tests_support::*;
use crate::*;

struct FakeAccounts {
    scratch: PrivilegeRegistry,
    committed: Rc<Cell<bool>>,
}

struct FakePendingAccounts {
    scratch: PrivilegeRegistry,
    committed: Rc<Cell<bool>>,
}

impl AccountService for FakeAccounts {
    fn begin(&self) -> Result<Box<dyn PendingAccountChange>, DriverError> {
        Ok(Box::new(FakePendingAccounts {
            scratch: self.scratch.clone(),
            committed: Rc::clone(&self.committed),
        }))
    }
}

impl PendingAccountChange for FakePendingAccounts {
    fn registry(&self) -> PrivilegeRegistry {
        self.scratch.clone()
    }

    fn set_write_hints(&mut self, _hints: tidb_exec::cluster_account_write::AccountWriteHints) {}

    fn commit(self: Box<Self>) -> Result<Vec<String>, DriverError> {
        self.committed.set(true);
        Ok(self
            .scratch
            .accounts()
            .into_iter()
            .map(|(user, host)| format!("{user}@{host}"))
            .collect())
    }
}

struct FakeSysvars {
    scratch: GlobalSysvars,
    committed: Rc<Cell<bool>>,
}

struct FakePendingSysvars {
    scratch: GlobalSysvars,
    committed: Rc<Cell<bool>>,
}

impl SysvarService for FakeSysvars {
    fn begin(&self) -> Result<Box<dyn PendingSysvarChange>, DriverError> {
        Ok(Box::new(FakePendingSysvars {
            scratch: self.scratch.clone(),
            committed: Rc::clone(&self.committed),
        }))
    }
}

impl PendingSysvarChange for FakePendingSysvars {
    fn table(&self) -> GlobalSysvars {
        self.scratch.clone()
    }

    fn commit(self: Box<Self>) -> Result<Vec<String>, DriverError> {
        self.committed.set(true);
        Ok(vec!["tidb_mem_quota_query".to_owned()])
    }
}

#[test]
fn account_statements_run_on_the_scratch_registry_and_commit_only_on_success() {
    let mut session = Session::new();
    session.attach_privileges(PrivilegeRegistry::default());
    let committed = Rc::new(Cell::new(false));
    let service = FakeAccounts {
        scratch: PrivilegeRegistry::default(),
        committed: Rc::clone(&committed),
    };
    let changed = session
        .run_account_statement_with("CREATE USER svc_u1", &service)
        .unwrap();
    assert!(committed.get());
    assert!(
        changed.iter().any(|account| account == "svc_u1@%"),
        "{changed:?}"
    );
    // The live registry was put back: the user exists only in the change.
    assert!(!session
        .swap_privileges(PrivilegeRegistry::default())
        .unwrap()
        .user_exists("svc_u1", "%"));

    let committed = Rc::new(Cell::new(false));
    let service = FakeAccounts {
        scratch: PrivilegeRegistry::default(),
        committed: Rc::clone(&committed),
    };
    session
        .run_account_statement_with("DROP USER svc_missing", &service)
        .expect_err("dropping a missing user fails");
    assert!(!committed.get());
}

#[test]
fn set_global_runs_on_the_scratch_table_and_a_failure_counts_as_an_error() {
    let mut session = Session::new();
    let committed = Rc::new(Cell::new(false));
    let scratch = GlobalSysvars::new();
    let service = FakeSysvars {
        scratch: scratch.clone(),
        committed: Rc::clone(&committed),
    };
    session
        .run_global_var_statement_with("SET GLOBAL tidb_mem_quota_query = 123456789", &service)
        .unwrap();
    assert!(committed.get());
    assert_eq!(scratch.get("tidb_mem_quota_query").unwrap(), "123456789");
    assert_ne!(
        row_text(session.run("SELECT @@global.tidb_mem_quota_query")),
        [["123456789"]]
    );

    let committed = Rc::new(Cell::new(false));
    let service = FakeSysvars {
        scratch: GlobalSysvars::new(),
        committed: Rc::clone(&committed),
    };
    session
        .run_global_var_statement_with("SET GLOBAL svc_no_such_variable = 1", &service)
        .expect_err("an unknown variable is refused");
    assert!(!committed.get());
    // Go's SetExecutor appends the failure to the statement context.
    assert_eq!(row_text(session.run("SELECT @@error_count")), [["1"]]);
}

mod analyze {
    use std::cell::RefCell;
    use std::sync::Arc;

    use tidb_exec::cluster_analyze::AnalyzeStatement;
    use tidb_exec::real_tikv_analyze::ClusterAnalyzeReport;

    use crate::analyze_service::AnalyzeService;
    use crate::*;

    #[derive(Default)]
    struct Recording(RefCell<Vec<String>>);

    impl AnalyzeService for Recording {
        fn flush_stats_delta(&self, target_ids: &[i64]) -> Result<(), DriverError> {
            self.0
                .borrow_mut()
                .push(format!("flush {}", target_ids.len()));
            Ok(())
        }

        fn execute(
            &self,
            statement: &AnalyzeStatement,
            _historical_stats_enabled: bool,
        ) -> Result<ClusterAnalyzeReport, DriverError> {
            self.0
                .borrow_mut()
                .push(format!("execute {}", statement.table));
            Err(DriverError::Mysql(tidb_executor::MysqlError::new(
                1105,
                "sampling failed",
            )))
        }

        fn update_stats_cache(&self, physical_ids: &[i64]) -> Result<(), DriverError> {
            self.0
                .borrow_mut()
                .push(format!("update {}", physical_ids.len()));
            Ok(())
        }

        fn publish_temporary_table_stats(
            &self,
            _table_id: i64,
            _statistics: Arc<tidb_stats::Table>,
        ) {
            self.0.borrow_mut().push("publish temporary".to_owned());
        }

        fn dump_historical_stats(&self, table_id: i64) {
            self.0.borrow_mut().push(format!("history {table_id}"));
        }
    }

    fn statements(sql: &str) -> Vec<AnalyzeStatement> {
        tidb_exec::real_tikv_analyze::prepare_cluster_analyze(sql, "test")
            .unwrap()
            .unwrap()
    }

    /// Deltas are flushed before sampling, and the first table that fails
    /// ends the statement: Go stores nothing for the tables after it.
    #[test]
    fn analyze_flushes_first_and_stops_at_the_first_failure() {
        let mut session = Session::new();
        session.run("CREATE TABLE test.an1 (a INT)").unwrap();
        session.run("CREATE TABLE test.an2 (a INT)").unwrap();
        let service = Recording::default();
        let error = session
            .run_analyze_statements(&statements("ANALYZE TABLE an1, an2"), &service)
            .expect_err("the service's failure reaches the client");
        assert_eq!(error.to_mysql_error().message, "sampling failed");
        assert_eq!(*service.0.borrow(), ["flush 2", "execute an1"]);
    }

    /// A temporary table's rows live in the session's overlay, so the
    /// session analyzes it and the service only publishes the result.
    #[test]
    fn a_temporary_table_is_analyzed_by_the_session() {
        let mut session = Session::new();
        session
            .run("CREATE TEMPORARY TABLE test.an_tmp (a INT)")
            .unwrap();
        session
            .run("INSERT INTO test.an_tmp VALUES (1), (2)")
            .unwrap();
        let service = Recording::default();
        session
            .run_analyze_statements(&statements("ANALYZE TABLE an_tmp"), &service)
            .unwrap();
        // Nothing to flush: an empty target list would mean every table.
        assert_eq!(*service.0.borrow(), ["publish temporary"]);
    }

    /// Go `collectStatsDeltaFlushObjectsForAnalyze` flushes the whole table
    /// -- itself and every partition -- even when one partition is analyzed.
    #[test]
    fn analyzing_a_partition_flushes_the_whole_table() {
        let mut session = Session::new();
        session
            .run(
                "CREATE TABLE test.an_pt (a INT) PARTITION BY RANGE (a) \
                 (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
            )
            .unwrap();
        let service = Recording::default();
        let _ = session
            .run_analyze_statements(&statements("ANALYZE TABLE an_pt PARTITION p0"), &service);
        assert_eq!(*service.0.borrow(), ["flush 3", "execute an_pt"]);
    }
}
