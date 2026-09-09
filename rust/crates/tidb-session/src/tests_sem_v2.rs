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

use crate::tests_support::*;
use crate::*;

// Go `pkg/util/sem/compat/sem_integration_test.go::TestRestrictedSQL`, plus
// the integrations exercised by `restricted_hint.go` and the GRANT/REVOKE
// visit-info collectors.
#[test]
fn configured_sem_v2_policy_reaches_statement_hint_and_privilege_gates() {
    let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--ignored",
            "--exact",
            "tests_sem_v2::configured_sem_v2_policy_child",
        ])
        .output()
        .expect("run isolated SEM v2 policy test");
    assert!(
        output.status.success(),
        "isolated SEM v2 policy test failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[test]
#[ignore = "subprocess helper"]
fn configured_sem_v2_policy_child() {
    tidb_util::sem::disable();
    tidb_util::sem_v2::disable();
    crate::sysvar::install_sem_v2_sysvar_registry();

    let config = tidb_util::sem_v2::Config {
        version: "1.0".to_owned(),
        tidb_version: tidb_util::sem_v2::tidb_release_version(),
        restricted_privileges: vec!["FILE".to_owned()],
        restricted_variables: vec![tidb_util::sem_v2::VariableRestriction {
            name: "autocommit".to_owned(),
            hidden: false,
            readonly: true,
            value: String::new(),
        }],
        restricted_sql: tidb_util::sem_v2::SQLRestriction {
            sql: vec!["ALTER RESOURCE GROUP".to_owned()],
            rule: Vec::new(),
        },
        restricted_hints: vec!["hash_agg".to_owned()],
        ..tidb_util::sem_v2::Config::default()
    };
    tidb_util::sem_v2::enable_by(&config).unwrap();

    let privileges = privilege::PrivilegeRegistry::default();
    let mut bootstrap = bootstrap_session(&privileges);
    bootstrap.run("CREATE USER 'nobody'@'%'").unwrap();
    bootstrap.run("CREATE USER 'recipient'@'%'").unwrap();
    privileges.grant(
        "nobody",
        "%",
        privilege::GlobalPriv::File.bit() | privilege::GlobalPriv::GrantOption.bit(),
    );

    let mut nobody = authenticated_session(&privileges, "nobody", "%");
    let restricted = nobody
        .run("ALTER RESOURCE GROUP rg RU_PER_SEC=500")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(restricted.code, 8132);
    assert!(restricted
        .message
        .contains("is not supported when security enhanced mode is enabled"));

    let grant_denial = nobody
        .run("GRANT FILE ON *.* TO 'recipient'@'%'")
        .unwrap_err();
    assert!(matches!(
        grant_denial,
        DriverError::SpecificAccessDenied(privilege) if privilege == "RESTRICTED_PRIV_ADMIN"
    ));
    let variable_denial = nobody.run("SET autocommit=0").unwrap_err();
    assert!(matches!(
        variable_denial,
        DriverError::SpecificAccessDenied(privilege)
            if privilege == "RESTRICTED_VARIABLES_ADMIN"
    ));

    privileges.grant_dynamic("nobody", "%", "RESTRICTED_SQL_ADMIN", false);
    privileges.grant_dynamic("nobody", "%", "RESTRICTED_PRIV_ADMIN", false);
    privileges.grant_dynamic("nobody", "%", "RESTRICTED_VARIABLES_ADMIN", false);
    nobody.run("GRANT FILE ON *.* TO 'recipient'@'%'").unwrap();
    assert!(privileges.has_global_priv("recipient", "%", privilege::GlobalPriv::File));
    nobody.run("SET autocommit=0").unwrap();

    nobody.run("SELECT /*+ HASH_AGG() */ SUM(1)").unwrap();
    assert!(nobody.warnings().iter().any(|warning| {
        warning.message
            == "the HASH_AGG() optimizer hint is restricted under the current security policy and is ignored"
    }));
}
