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

//! MySQL 8.0 dual passwords (`RETAIN CURRENT PASSWORD` / `DISCARD OLD
//! PASSWORD`) and the `mysql.user` rows they maintain -- Go
//! `executor/simple.go` `executeAlterUser`/`executeSetPwd` and the corpus
//! topic `executor/dual_password`.

use crate::tests_support::*;
use crate::*;

fn root_session() -> (privilege::PrivilegeRegistry, Session) {
    let registry = privilege::PrivilegeRegistry::default();
    let mut session = Session::new();
    session.attach_privileges(registry.clone());
    session.set_user("root@%".to_owned(), "root@127.0.0.1".to_owned());
    (registry, session)
}

fn one_cell(session: &mut Session, sql: &str) -> String {
    let rows = row_text(session.run(sql));
    assert_eq!(rows.len(), 1, "{sql}");
    assert_eq!(rows[0].len(), 1, "{sql}");
    rows[0][0].clone()
}

/// CAPTURED (`executor/dual_password`): `ALTER USER ... IDENTIFIED BY
/// RETAIN CURRENT PASSWORD` promotes the pre-change primary hash into
/// `mysql.user.User_attributes -> '$.additional_password'`; `DISCARD OLD
/// PASSWORD` removes it and collapses an otherwise-empty attributes object
/// back to NULL (not `'{}'`); a plugin change drops the secondary silently.
#[test]
fn retain_and_discard_maintain_the_additional_password_attribute() {
    let (registry, mut session) = root_session();
    session.run("create user dpu1 identified by 'old'").unwrap();
    let old_hash = registry.auth_string("dpu1", "%").unwrap();
    session
        .run("alter user dpu1 identified by 'new' retain current password")
        .unwrap();
    assert_eq!(
        one_cell(
            &mut session,
            "select json_extract(user_attributes, '$.additional_password') from mysql.user \
             where user = 'dpu1' and host = '%'"
        ),
        format!("\"{old_hash}\"")
    );
    // The primary moved to the new password in the same statement.
    assert_eq!(
        registry.auth_string("dpu1", "%").as_deref(),
        Some(privilege::encode_password("new").as_str())
    );

    session.run("alter user dpu1 discard old password").unwrap();
    assert_eq!(
        one_cell(
            &mut session,
            "select json_extract(user_attributes, '$.additional_password') from mysql.user \
             where user = 'dpu1' and host = '%'"
        ),
        "NULL"
    );
    assert_eq!(
        one_cell(
            &mut session,
            "select user_attributes is null from mysql.user where user = 'dpu1' and host = '%'"
        ),
        "1"
    );

    // Plugin change without RETAIN silently drops the secondary.
    session
        .run("alter user dpu1 identified by 'a' retain current password")
        .unwrap();
    assert_eq!(
        one_cell(
            &mut session,
            "select json_extract(user_attributes, '$.additional_password') is not null from \
             mysql.user where user = 'dpu1' and host = '%'"
        ),
        "1"
    );
    session
        .run("alter user dpu1 identified with caching_sha2_password by 'b'")
        .unwrap();
    assert_eq!(
        one_cell(
            &mut session,
            "select json_extract(user_attributes, '$.additional_password') from mysql.user \
             where user = 'dpu1' and host = '%'"
        ),
        "NULL"
    );
}

/// CAPTURED error set (`executor/dual_password`): 3894 for RETAIN with a
/// plugin change, 3895 for RETAIN with a missing/empty new password, 3878
/// when the CURRENT primary is empty -- for both ALTER USER and SET
/// PASSWORD.
#[test]
fn retain_validation_errors_match_go() {
    let (_registry, mut session) = root_session();
    session.run("create user dpu1 identified by 'old'").unwrap();
    session
        .run("alter user dpu1 identified with mysql_native_password by 'c'")
        .unwrap();
    assert!(matches!(
        session.run(
            "alter user dpu1 identified with caching_sha2_password by 'd' retain current password"
        ),
        Err(DriverError::PasswordCannotBeRetainedOnPluginChange { .. })
    ));
    assert!(matches!(
        session.run("alter user dpu1 identified by '' retain current password"),
        Err(DriverError::CurrentPasswordCannotBeRetained { .. })
    ));
    // No new password at all: Go's HAND PARSER refuses RETAIN without a
    // BY-form auth option (`parseAlterUserSpec`: "RETAIN attaches only to
    // BY-form auth options"), so this is 1064 at parse time -- the
    // executor's own AuthOpt-nil 3895 arm is defensive, unreachable from
    // SQL text.
    assert!(session
        .run("alter user dpu1 retain current password")
        .is_err());

    session
        .run("create user dpemptycur identified by ''")
        .unwrap();
    assert!(matches!(
        session.run("alter user dpemptycur identified by 'new' retain current password"),
        Err(DriverError::SecondPasswordCannotBeEmpty { .. })
    ));
    assert!(matches!(
        session.run("set password for dpemptycur = 'new' retain current password"),
        Err(DriverError::SecondPasswordCannotBeEmpty { .. })
    ));
}

/// CAPTURED (`executor/dual_password`): `SET PASSWORD ... RETAIN CURRENT
/// PASSWORD` stores the secondary too, and the multi-user ALTER USER
/// applies each dual-password clause only to the spec it follows.
#[test]
fn set_password_retain_and_per_spec_dual_clauses() {
    let (registry, mut session) = root_session();
    session.run("create user dpu1 identified by 'p1'").unwrap();
    let p1_hash = registry.auth_string("dpu1", "%").unwrap();
    session
        .run("set password for dpu1 = 'p2' retain current password")
        .unwrap();
    assert_eq!(
        one_cell(
            &mut session,
            "select json_extract(user_attributes, '$.additional_password') from mysql.user \
             where user = 'dpu1' and host = '%'"
        ),
        format!("\"{p1_hash}\"")
    );
    assert_eq!(
        registry.auth_string("dpu1", "%").as_deref(),
        Some(privilege::encode_password("p2").as_str())
    );

    session
        .run(
            "create user dpm1 identified by 'p1', dpm2 identified by 'q1', dpm3 identified by 'r1'",
        )
        .unwrap();
    session
        .run("alter user dpm1 identified by 'p2', dpm3 identified by 'r2' retain current password")
        .unwrap();
    let has_secondary = |session: &mut Session, user: &str| {
        one_cell(
            session,
            &format!(
                "select json_extract(user_attributes, '$.additional_password') is not null \
                 from mysql.user where user = '{user}' and host = '%'"
            ),
        )
    };
    assert_eq!(has_secondary(&mut session, "dpm1"), "0");
    assert_eq!(has_secondary(&mut session, "dpm3"), "1");
    // The clause applies only to the spec it follows; a spec with neither
    // auth nor clause is a silent no-op (Go composes no fields for it).
    session
        .run("alter user dpm1, dpm3 discard old password")
        .unwrap();
    assert_eq!(has_secondary(&mut session, "dpm1"), "0");
    assert_eq!(has_secondary(&mut session, "dpm3"), "0");
}

/// CAPTURED (`executor/dual_password`): COMMENT rides the same
/// `user_attributes` JSON -- RETAIN + COMMENT merge together, DISCARD +
/// COMMENT is Go's single JSON_REMOVE(JSON_MERGE_PATCH(...)) expression.
#[test]
fn dual_password_combines_with_comment_atomically() {
    let (_registry, mut session) = root_session();
    session
        .run("create user dpcomm identified by 'p1'")
        .unwrap();
    session
        .run(
            "alter user dpcomm identified by 'p2' retain current password comment \
             'rotation in progress'",
        )
        .unwrap();
    assert_eq!(
        row_text(session.run(
            "select json_extract(user_attributes, '$.additional_password') is not null as \
             has_secondary, json_unquote(json_extract(user_attributes, '$.metadata.comment')) \
             as comment from mysql.user where user = 'dpcomm' and host = '%'",
        )),
        vec![vec!["1".to_owned(), "rotation in progress".to_owned()]]
    );
    session
        .run("alter user dpcomm discard old password comment 'rotation finished'")
        .unwrap();
    assert_eq!(
        row_text(session.run(
            "select json_extract(user_attributes, '$.additional_password') as secondary, \
             json_unquote(json_extract(user_attributes, '$.metadata.comment')) as comment \
             from mysql.user where user = 'dpcomm' and host = '%'",
        )),
        vec![vec!["NULL".to_owned(), "rotation finished".to_owned()]]
    );
}

/// The `mysql.user` MIRROR the account statements maintain (see
/// `crate::user_table`): CREATE USER inserts the row Go's
/// `executeCreateUser` inserts (`'{}'` attributes, priv columns at their
/// `'N'` defaults), a GLOBAL GRANT/REVOKE flips the same columns Go's
/// `composeGlobalPrivUpdate` names, RENAME moves the row, DROP deletes it,
/// and the bootstrap root row is the `doDMLWorks` one (NULL attributes).
#[test]
fn account_statements_keep_the_user_table_rows_written() {
    let (_registry, mut session) = root_session();
    assert_eq!(
        row_text(session.run(
            "select Host, User, plugin, authentication_string, user_attributes, \
             Select_priv, Super_priv, Account_locked from mysql.user order by user"
        )),
        [[
            "%",
            "root",
            "mysql_native_password",
            "",
            "NULL",
            "Y",
            "Y",
            "N"
        ]]
    );
    session.run("create user mu1 identified by 'pw'").unwrap();
    assert_eq!(
        row_text(session.run(
            "select plugin, authentication_string, user_attributes, Select_priv, Grant_priv \
             from mysql.user where user = 'mu1'"
        )),
        [[
            "mysql_native_password",
            privilege::encode_password("pw").as_str(),
            "{}",
            "N",
            "N"
        ]]
    );
    session
        .run("grant select, process on *.* to mu1 with grant option")
        .unwrap();
    assert_eq!(
        row_text(session.run(
            "select Select_priv, Process_priv, Grant_priv, Super_priv from mysql.user \
             where user = 'mu1'"
        )),
        [["Y", "Y", "Y", "N"]]
    );
    session.run("revoke process on *.* from mu1").unwrap();
    assert_eq!(
        row_text(
            session.run("select Select_priv, Process_priv from mysql.user where user = 'mu1'")
        ),
        [["Y", "N"]]
    );
    session.run("rename user mu1 to mu2").unwrap();
    assert_eq!(
        row_text(session.run("select count(*) from mysql.user where user = 'mu1'")),
        [["0"]]
    );
    assert_eq!(
        row_text(session.run("select Select_priv from mysql.user where user = 'mu2'")),
        [["Y"]]
    );
    session.run("drop user mu2").unwrap();
    assert_eq!(
        row_text(session.run("select count(*) from mysql.user where user = 'mu2'")),
        [["0"]]
    );
    // A ROLE is a locked, password-expired row (Go: `IsCreateRole` flips
    // `Account_locked` and `Password_expired` to 'Y').
    session.run("create role mr1").unwrap();
    assert_eq!(
        row_text(
            session
                .run("select Account_locked, Password_expired from mysql.user where user = 'mr1'")
        ),
        [["Y", "Y"]]
    );
    session.run("drop role mr1").unwrap();
}

/// CAPTURED (`executor/dual_password`): the dual-password privilege model.
/// Self-account RETAIN/DISCARD needs APPLICATION_PASSWORD_ADMIN (1227
/// naming it); cross-account needs the ordinary ALTER USER authority
/// (CREATE USER; 1227 naming CREATE USER), and APPLICATION_PASSWORD_ADMIN
/// is NOT authority over other accounts. Cross-user SET PASSWORD keeps its
/// SUPER-only 1044.
#[test]
fn dual_password_privilege_gates_match_go() {
    let (registry, mut session) = root_session();
    session
        .run("create user dpvictim identified by 'v1'")
        .unwrap();
    session
        .run("create user dpaponly identified by 'a1'")
        .unwrap();
    session
        .run("grant application_password_admin on *.* to dpaponly")
        .unwrap();
    session
        .run("create user dpself identified by 's1'")
        .unwrap();
    session
        .run("create user dpselfadmin identified by 's1'")
        .unwrap();
    session
        .run("grant application_password_admin on *.* to dpselfadmin")
        .unwrap();

    let shared = session.shared_catalog();
    let open = |identity: &str| {
        let mut peer = Session::with_catalog(SharedCatalog::clone(&shared));
        peer.attach_privileges(registry.clone());
        peer.set_user(identity.to_owned(), identity.to_owned());
        peer
    };

    // APPLICATION_PASSWORD_ADMIN alone is not authority over OTHERS.
    let mut aponly = open("dpaponly@%");
    assert!(matches!(
        aponly.run("alter user dpvictim identified by 'v2' retain current password"),
        Err(DriverError::SpecificAccessDenied(privilege)) if privilege == "CREATE USER"
    ));
    assert!(matches!(
        aponly.run("alter user dpvictim discard old password"),
        Err(DriverError::SpecificAccessDenied(privilege)) if privilege == "CREATE USER"
    ));
    // Cross-user SET PASSWORD ... RETAIN keeps the SUPER-only 1044.
    assert!(matches!(
        aponly.run("set password for dpvictim = 'v2' retain current password"),
        Err(DriverError::DbAccessDenied { .. })
    ));

    // Self-service without APPLICATION_PASSWORD_ADMIN: denied, naming it.
    let mut selfish = open("dpself@%");
    assert!(matches!(
        selfish.run("set password = 's2' retain current password"),
        Err(DriverError::SpecificAccessDenied(privilege))
            if privilege == "APPLICATION_PASSWORD_ADMIN"
    ));
    assert!(matches!(
        selfish.run("alter user 'dpself'@'%' identified by 's2' retain current password"),
        Err(DriverError::SpecificAccessDenied(privilege))
            if privilege == "APPLICATION_PASSWORD_ADMIN"
    ));
    // ... and ALTER USER USER() goes through the same gate.
    assert!(matches!(
        selfish.run("alter user user() identified by 's2' retain current password"),
        Err(DriverError::SpecificAccessDenied(privilege))
            if privilege == "APPLICATION_PASSWORD_ADMIN"
    ));

    // With it: allowed, secondary set; DISCARD via USER() clears it.
    let mut selfadmin = open("dpselfadmin@%");
    selfadmin
        .run("set password = 's2' retain current password")
        .unwrap();
    assert_eq!(
        one_cell(
            &mut session,
            "select json_extract(user_attributes, '$.additional_password') is not null from \
             mysql.user where user = 'dpselfadmin' and host = '%'"
        ),
        "1"
    );
    selfadmin
        .run("alter user user() discard old password")
        .unwrap();
    assert_eq!(
        one_cell(
            &mut session,
            "select json_extract(user_attributes, '$.additional_password') from mysql.user \
             where user = 'dpselfadmin' and host = '%'"
        ),
        "NULL"
    );

    // Self-service dual-password must NOT piggy-back COMMENT: the extra
    // statement-level option routes it through the CREATE USER admin check.
    let mut selfadmin2 = open("dpselfadmin@%");
    assert!(matches!(
        selfadmin2.run("alter user 'dpselfadmin'@'%' discard old password comment 'x'"),
        Err(DriverError::SpecificAccessDenied(privilege)) if privilege == "CREATE USER"
    ));
}
