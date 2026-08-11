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

use tidb_session::{privilege, Session, StmtResult};

#[test]
fn sem_policy_requires_explicit_restricted_privileges() {
    struct DisableSemOnDrop;

    impl Drop for DisableSemOnDrop {
        fn drop(&mut self) {
            tidb_util::sem::disable();
        }
    }

    tidb_util::sem::disable();
    let _reset = DisableSemOnDrop;
    tidb_util::sem::enable();

    let registry = privilege::PrivilegeRegistry::default();
    let mut session = Session::new();
    session.set_user("root@%".to_owned(), "root@%".to_owned());
    session.attach_privileges(registry.clone());

    // SUPER remains the compatibility fallback for ordinary dynamic
    // privileges, but SEM requires every RESTRICTED_* privilege to be an
    // explicit grant.
    assert!(registry.has_dynamic_priv("root", "%", "BACKUP_ADMIN", false));
    assert!(!registry.has_dynamic_priv("root", "%", "RESTRICTED_TABLES_ADMIN", false));

    // An explicit restricted privilege on an active role is just as valid as
    // one on the account itself; only SUPER fallback is removed.
    let sem_role = ("sem_role".to_owned(), "%".to_owned());
    assert!(registry.create_role(&sem_role.0, &sem_role.1));
    registry.grant_role(&sem_role, &("root".to_owned(), "%".to_owned()));
    registry.grant_dynamic(&sem_role.0, &sem_role.1, "RESTRICTED_TABLES_ADMIN", false);
    assert!(registry.has_dynamic_priv_with_roles(
        "root",
        "%",
        std::slice::from_ref(&sem_role),
        "RESTRICTED_TABLES_ADMIN",
        false,
    ));

    // The same explicit RESTRICTED_TABLES_ADMIN gate owns schema/table
    // visibility and the SEM read-only rule for system schemas.
    assert!(!session.database_is_visible("METRICS_ScHEma"));
    assert!(!session.has_scoped_privilege("mysql", "tidb", privilege::GlobalPriv::Select));
    assert!(!session.has_scoped_privilege("mysql", "user", privilege::GlobalPriv::Insert));
    session.run("SET ROLE 'sem_role'@'%'").unwrap();
    assert!(session.database_is_visible("METRICS_ScHEma"));
    assert!(session.has_scoped_privilege("mysql", "tidb", privilege::GlobalPriv::Select));
    assert!(session.has_scoped_privilege("mysql", "user", privilege::GlobalPriv::Insert));
    session.run("SET ROLE NONE").unwrap();
    assert!(!session.database_is_visible("METRICS_ScHEma"));
    assert!(!session.has_scoped_privilege("mysql", "tidb", privilege::GlobalPriv::Select));
    assert!(!session.has_scoped_privilege("mysql", "user", privilege::GlobalPriv::Insert));

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
        assert!(
            matches!(
                session.run(sql),
                Err(tidb_executor::DriverError::SpecificAccessDenied(privilege))
                    if privilege == "RESTRICTED_VARIABLES_ADMIN"
            ),
            "{sql}"
        );
    }

    registry.grant_dynamic("root", "%", "RESTRICTED_TABLES_ADMIN", false);
    registry.grant_dynamic("root", "%", "RESTRICTED_VARIABLES_ADMIN", false);
    assert!(session.database_is_visible("METRICS_ScHEma"));
    assert!(session.has_scoped_privilege("mysql", "tidb", privilege::GlobalPriv::Select));
    assert!(session.has_scoped_privilege("mysql", "user", privilege::GlobalPriv::Insert));
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
