//! CHECK constraints in the write path: `tidb_enable_check_constraint` gates
//! ATTACHMENT at CREATE time (OFF drops the constraint with one warning,
//! create_table.go:1470); once attached, a violating INSERT fails with Go's
//! `ErrCheckConstraintViolated` (3819, "Check constraint '<name>' is
//! violated.") and conforming rows pass (Go `pkg/ddl/constraint_test.go`,
//! `tests/integrationtest/t/ddl/constraint.test` TestCheckConstraintSwitch /
//! TestAlterTableDropCheckConstraints).
//!
//! `tidb_enable_check_constraint` is a process-wide sysvar, and these tests
//! turn it both ON and OFF, so they run in the `serial` binary holding the
//! serial lock.

use crate::support::testkit::{TestKit, serial};

fn setup(checks_on: bool) -> TestKit {
    let mut tk = TestKit::new();
    tk.must_exec(if checks_on {
        "set global tidb_enable_check_constraint = on"
    } else {
        "set global tidb_enable_check_constraint = off"
    });
    tk.must_exec("create table t (a int check (a > 0))");
    tk
}

#[test]
fn attached_check_rejects_violating_inserts_with_3819() {
    let _serial = serial();
    let mut tk = setup(true);
    let error = tk.must_get_err_code("insert into t values (-5)", 3819);
    assert_eq!(
        error.message, "Check constraint 't_chk_1' is violated.",
        "Go: ErrCheckConstraintViolated (3819)"
    );
    // A conforming row still passes.
    tk.must_exec("insert into t values (3)");
}

#[test]
fn checks_off_drops_the_constraint_at_create() {
    let _serial = serial();
    let mut tk = setup(false);
    // With the variable OFF the constraint never attached.
    tk.must_exec("insert into t values (-5)");
}
