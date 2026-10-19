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
