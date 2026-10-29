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

//! Production-consumer coverage for `pkg/util/sem` policy enforcement.

use tidb_session::{Session, StmtResult};

#[test]
fn sem_policy_requires_explicit_restricted_privileges() {
    struct DisableSemOnDrop;

    impl Drop for DisableSemOnDrop {
        fn drop(&mut self) {
            tidb_sem::disable();
        }
    }

    tidb_sem::disable();
    let _reset = DisableSemOnDrop;

    // The grants are made over SQL with SEM off, as an administrator
    // provisions them before the restriction applies; Go keeps them in the
    // privilege tables the session's domain reads.
    let mut session = Session::new();
    session.run("CREATE ROLE 'sem_role'@'%'").unwrap();
    session.run("GRANT 'sem_role'@'%' TO 'root'@'%'").unwrap();
    session
        .run("GRANT RESTRICTED_TABLES_ADMIN ON *.* TO 'sem_role'@'%'")
        .unwrap();
    session.set_user("root@%".to_owned(), "root@%".to_owned());
    tidb_sem::enable();

    // The same explicit RESTRICTED_TABLES_ADMIN gate owns schema/table
    // visibility and the SEM read-only rule for system schemas.
    assert!(!session.database_is_visible("METRICS_ScHEma"));
    assert!(!session.has_scoped_privilege("mysql", "tidb", tidb_mysql::SelectPriv));
    assert!(!session.has_scoped_privilege("mysql", "user", tidb_mysql::InsertPriv));
    session.run("SET ROLE 'sem_role'@'%'").unwrap();
    assert!(session.database_is_visible("METRICS_ScHEma"));
    assert!(session.has_scoped_privilege("mysql", "tidb", tidb_mysql::SelectPriv));
    assert!(session.has_scoped_privilege("mysql", "user", tidb_mysql::InsertPriv));
    session.run("SET ROLE NONE").unwrap();
    assert!(!session.database_is_visible("METRICS_ScHEma"));
    assert!(!session.has_scoped_privilege("mysql", "tidb", tidb_mysql::SelectPriv));
    assert!(!session.has_scoped_privilege("mysql", "user", tidb_mysql::InsertPriv));

    // Invisible system variables disappear from SHOW and require the
    // explicit RESTRICTED_VARIABLES_ADMIN privilege for every direct scope.
    for sql in [
        "SHOW VARIABLES LIKE 'tidb_general_log'",
        "SHOW GLOBAL VARIABLES LIKE 'tidb_general_log'",
    ] {
        let StmtResult::Rows(hidden) = session.run(sql).unwrap() else {
            panic!("SHOW VARIABLES must return rows");
        };
        assert!(hidden.is_empty(), "{sql}");
    }
    for sql in [
        "SELECT @@TIDB_GENERAL_LOG",
        "SELECT @@global.tidb_general_log",
        "SET tidb_general_log = 1",
        "SET GLOBAL tidb_general_log = 1",
    ] {
        // Go `ErrSpecificAccessDenied` (1227) naming the privilege.
        let error = session.run(sql).expect_err(sql).to_mysql_error();
        assert_eq!(error.code, 1227, "{sql}: {error:?}");
        assert!(
            error.message.contains("RESTRICTED_VARIABLES_ADMIN"),
            "{sql}: {error:?}"
        );
    }

    tidb_sem::disable();
    session
        .run("GRANT RESTRICTED_TABLES_ADMIN, RESTRICTED_VARIABLES_ADMIN ON *.* TO 'root'@'%'")
        .unwrap();
    tidb_sem::enable();
    assert!(session.database_is_visible("METRICS_ScHEma"));
    assert!(session.has_scoped_privilege("mysql", "tidb", tidb_mysql::SelectPriv));
    assert!(session.has_scoped_privilege("mysql", "user", tidb_mysql::InsertPriv));
    for sql in [
        "SHOW VARIABLES LIKE 'tidb_general_log'",
        "SHOW GLOBAL VARIABLES LIKE 'tidb_general_log'",
    ] {
        let StmtResult::Rows(visible) = session.run(sql).unwrap() else {
            panic!("SHOW VARIABLES must return rows");
        };
        assert_eq!(visible.len(), 1, "{sql}");
    }
    session.run("SELECT @@tidb_general_log").unwrap();
    session.run("SELECT @@global.tidb_general_log").unwrap();
    session.run("SET tidb_general_log = 1").unwrap();
    session.run("SET GLOBAL tidb_general_log = 1").unwrap();
}
