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

//! Go `pkg/extension/auth_test.go`: executable authentication and authorization.
use crate::domain::{configured_user_store::ConfiguredUserStore, mock_store::MockStoreDomain};
use crate::tests_support::query_text;
use std::sync::{Arc, Mutex};
use tidb_extension::AuthPlugin;

const PLUGIN: &str = "authentication_test_plugin";
#[derive(Default)]
struct Calls {
    authenticate: usize,
    reject_select: bool,
    generate: usize,
    validate: usize,
    static_privs: Vec<tidb_mysql::PrivilegeType>,
    static_contexts: Vec<(String, String, String, String, String)>,
    dynamic_privs: Vec<(String, bool)>,
}
struct Registered;
impl Drop for Registered {
    fn drop(&mut self) {
        tidb_extension::reset();
    }
}
fn register(allow: bool) -> (Registered, Arc<Mutex<Calls>>) {
    crate::init_extension_hooks();
    tidb_extension::reset();
    let calls = Arc::new(Mutex::new(Calls::default()));
    let auth = calls.clone();
    let generate = calls.clone();
    let validate = calls.clone();
    let static_calls = calls.clone();
    let dynamic_calls = calls.clone();
    tidb_extension::register(
        "extension_authentication_plugin",
        vec![tidb_extension::with_custom_auth_plugins(vec![AuthPlugin {
            name: PLUGIN.into(),
            required_client_side_plugin: tidb_mysql::AuthNativePassword.into(),
            authenticate_user: Some(Arc::new(move |request| {
                auth.lock().unwrap().authenticate += 1;
                assert!(!request.user.is_empty());
                assert!(!request.stored_auth_string.is_empty());
                if request.input_auth_string == b"1" {
                    Ok(())
                } else {
                    Err("authentication failed".into())
                }
            })),
            generate_auth_string: Some(Arc::new(move |password| {
                generate.lock().unwrap().generate += 1;
                (password.replace("raw", "encoded"), true)
            })),
            validate_auth_string: Some(Arc::new(move |_| {
                validate.lock().unwrap().validate += 1;
                true
            })),
            verify_privilege: Some(Arc::new(move |request| {
                assert!(request.conn_state.is_none());
                assert!(request.active_roles.is_empty());
                static_calls.lock().unwrap().static_contexts.push((
                    request.user.into(),
                    request.host.into(),
                    request.db.into(),
                    request.table.into(),
                    request.column.into(),
                ));
                static_calls
                    .lock()
                    .unwrap()
                    .static_privs
                    .push(request.static_priv);
                if request.static_priv == tidb_mysql::SelectPriv
                    && static_calls.lock().unwrap().reject_select
                {
                    return false;
                }
                allow || request.static_priv != tidb_mysql::InsertPriv
            })),
            verify_dynamic_privilege: Some(Arc::new(move |request| {
                dynamic_calls
                    .lock()
                    .unwrap()
                    .dynamic_privs
                    .push((request.dynamic_priv.into(), request.with_grant));
                allow
            })),
        }])],
    )
    .unwrap();
    tidb_extension::setup().unwrap();
    (Registered, calls)
}
fn login(
    store: &Arc<MockStoreDomain>,
    users: &ConfiguredUserStore,
    user: &str,
    response: &[u8],
) -> crate::Session {
    let identity = users
        .authenticate_native(user, "localhost", &[], response)
        .unwrap();
    let mut session = store.new_session().unwrap();
    session.set_user(
        format!("{}@{}", identity.username(), identity.host()),
        format!("{user}@localhost"),
    );
    session.attach_privileges(users.accounts());
    session.set_auth_plugin(identity.auth_plugin());
    session
}
fn fixture() -> (Arc<MockStoreDomain>, ConfiguredUserStore, crate::Session) {
    let store = MockStoreDomain::create().unwrap();
    let users = ConfiguredUserStore::from_accounts(
        store.new_session().unwrap().privilege_manager().unwrap(),
    );
    let mut root = store.new_session().unwrap();
    root.run("use test").unwrap();
    (store, users, root)
}

/// Go `TestAuthPlugin`.
#[test]
fn test_auth_plugin() {
    let (_registered, calls) = register(false);
    let (store, users, mut root) = fixture();
    let invalid = "create user 'u2'@'localhost' identified with 'bad_plugin' by 'rawpassword'";
    assert!(root
        .run(invalid)
        .unwrap_err()
        .to_string()
        .contains("Plugin 'bad_plugin' is not loaded"));
    root.run("create user 'u2'@'localhost' identified with 'authentication_test_plugin' as 'encodedpassword'").unwrap();
    assert_eq!(calls.lock().unwrap().generate, 0);
    assert_eq!(calls.lock().unwrap().validate, 1);
    assert!(users
        .authenticate_native("u2", "localhost", &[], b"2")
        .is_err());
    let mut user = login(&store, &users, "u2", b"1");
    assert_eq!(calls.lock().unwrap().authenticate, 2);
    assert_eq!(calls.lock().unwrap().validate, 3);
    for (sql, expected) in [
        ("alter user 'u2'@'localhost' identified with 'authentication_test_plugin' by 'anotherrawpassword'", "anotherencodedpassword"),
        ("set password for 'u2'@'localhost'='yetanotherrawpassword'", "yetanotherencodedpassword"),
    ] {
        root.run(sql).unwrap();
        assert_eq!(users.accounts().auth_string("u2", "localhost").as_deref(), Some(expected));
        user = login(&store, &users, "u2", b"1");
    }
    user.run("set password='yetanotherrawpassword2'").unwrap();
    assert_eq!(
        users.accounts().auth_string("u2", "localhost").as_deref(),
        Some("yetanotherencodedpassword2")
    );
    user = login(&store, &users, "u2", b"1");
    assert_eq!(calls.lock().unwrap().authenticate, 5);
    root.run("create table t1(id int primary key, v int)")
        .unwrap();
    root.run("grant select, insert on test.t1 to u2@localhost")
        .unwrap();
    root.run("insert into t1 values (1,10),(2,20)").unwrap();
    query_text(&mut root, "select * from t1");
    user.run("use test").unwrap();
    assert!(calls.lock().unwrap().static_privs.is_empty());
    assert_eq!(
        query_text(&mut user, "select * from t1 where id=1").1,
        vec![vec!["1", "10"]]
    );
    assert_eq!(query_text(&mut user, "select * from t1").1.len(), 2);
    assert!(user
        .run("insert into t1 values (3,30)")
        .unwrap_err()
        .to_string()
        .contains("INSERT command denied"));
    assert_eq!(
        calls.lock().unwrap().static_privs,
        [
            tidb_mysql::SelectPriv,
            tidb_mysql::SelectPriv,
            tidb_mysql::InsertPriv
        ]
    );
    assert_eq!(
        calls.lock().unwrap().static_contexts,
        vec![
            (
                "u2".into(),
                "localhost".into(),
                "test".into(),
                "t1".into(),
                "".into()
            );
            3
        ]
    );
    assert!(user.run("delete from t1 where id=1").is_err());
    assert!(!calls
        .lock()
        .unwrap()
        .static_privs
        .contains(&tidb_mysql::DeletePriv));
    assert!(user
        .run("set global sql_mode='STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER'")
        .is_err());
    assert!(calls.lock().unwrap().dynamic_privs.is_empty());
    root.run("grant SYSTEM_VARIABLES_ADMIN on *.* to u2@localhost")
        .unwrap();
    assert!(user
        .run("set global sql_mode='STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER'")
        .is_err());
    assert!(calls
        .lock()
        .unwrap()
        .dynamic_privs
        .contains(&("SYSTEM_VARIABLES_ADMIN".into(), false)));
}

/// Go `TestAuthPluginSwitchPlugins`.
#[test]
fn test_auth_plugin_switch_plugins() {
    let (_registered, calls) = register(false);
    let (store, users, mut root) = fixture();
    root.run(
        "create user u2@localhost identified with 'authentication_test_plugin' as 'rawpassword'",
    )
    .unwrap();
    root.run("create table t1(id int primary key, v int)")
        .unwrap();
    root.run("grant select, insert on test.t1 to u2@localhost")
        .unwrap();
    root.run("insert into t1 values(1,10),(2,20)").unwrap();
    let mut old = login(&store, &users, "u2", b"1");
    old.run("use test").unwrap();
    query_text(&mut old, "select * from t1");
    assert!(old.run("insert into t1 values(3,30)").is_err());
    root.run("alter user u2@localhost identified with 'mysql_native_password' by ''")
        .unwrap();
    query_text(&mut old, "select * from t1");
    assert!(old.run("insert into t1 values(3,30)").is_err());
    let count = calls.lock().unwrap().static_privs.len();
    let mut new = login(&store, &users, "u2", b"");
    new.run("use test").unwrap();
    new.run("insert into t1 values(3,30),(4,40)").unwrap();
    query_text(&mut new, "select * from t1");
    assert_eq!(calls.lock().unwrap().static_privs.len(), count);
    root.run(
        "alter user u2@localhost identified with 'authentication_test_plugin' by 'rawpassword'",
    )
    .unwrap();
    let mut new = login(&store, &users, "u2", b"1");
    new.run("use test").unwrap();
    query_text(&mut new, "select * from t1");
    assert!(new.run("insert into t1 values(5,50)").is_err());
    assert_eq!(calls.lock().unwrap().static_privs.len(), count + 2);
}

/// Go `TestCreateUserWhenGrant`.
#[test]
fn test_create_user_when_grant() {
    let (_registered, calls) = register(true);
    let (_, users, mut root) = fixture();
    root.run("set sql_mode=''").unwrap();
    root.run("grant all privileges on *.* to 'test'@'%' identified with 'authentication_test_plugin' as 'xxx'").unwrap();
    assert_eq!(
        users.accounts().plugin("test", "%").as_deref(),
        Some(PLUGIN)
    );
    assert_eq!(calls.lock().unwrap().validate, 1);
    root.run("drop user 'test'@'%'").unwrap();
    root.run("grant all privileges on *.* to 'test'@'%'")
        .unwrap();
    assert_eq!(
        users.accounts().plugin("test", "%").as_deref(),
        Some(tidb_mysql::AuthNativePassword)
    );
}

/// Go `TestCreateViewWithPluginUser`: definer checks must not invoke a plugin out of session.
#[test]
fn test_create_view_with_plugin_user() {
    let (_registered, calls) = register(true);
    let (store, users, mut root) = fixture();
    for sql in [
        "create table t1(id int primary key,v int)",
        "insert into t1 values(1,10),(2,20)",
        "create user u1 identified with 'authentication_test_plugin' as 'rawpassword'",
        "grant select, insert, create view on test.* to u1",
        "create user u2",
        "grant select on test.* to u2",
    ] {
        root.run(sql).unwrap();
    }
    let mut first = login(&store, &users, "u1", b"1");
    first.run("use test").unwrap();
    calls.lock().unwrap().reject_select = true;
    assert!(first
        .run("create view v1 as select * from t1")
        .unwrap_err()
        .to_string()
        .contains("SELECT command denied"));
    let mut second = login(&store, &users, "u1", b"1");
    second.run("use test").unwrap();
    calls.lock().unwrap().reject_select = false;
    second.run("create view v1 as select * from t1").unwrap();
    assert_eq!(
        query_text(&mut second, "select * from v1").1,
        vec![vec!["1", "10"], vec!["2", "20"]]
    );
    assert_eq!(query_text(&mut first, "select * from v1").1.len(), 2);
    let count = calls.lock().unwrap().static_privs.len();
    let mut other = login(&store, &users, "u2", b"");
    other.run("use test").unwrap();
    calls.lock().unwrap().reject_select = true;
    assert_eq!(query_text(&mut other, "select * from v1").1.len(), 2);
    assert_eq!(calls.lock().unwrap().static_privs.len(), count);
}

/// Go `TestPluginUserModification`: checks on other accounts use grants, not their plugins.
#[test]
fn test_plugin_user_modification() {
    let (_registered, calls) = register(true);
    let (store, users, mut root) = fixture();
    for sql in [
        "create user u1 identified with 'authentication_test_plugin' as 'rawpassword'",
        "grant super on *.* to u1",
        "create user u2",
        "grant create user on *.* to u2",
        "create user u3",
        "grant create user, system_user, super on *.* to u3",
        "create user u4 identified with 'authentication_test_plugin' as 'rawpassword'",
        "grant create user, super on *.* to u4",
    ] {
        root.run(sql).unwrap();
    }
    let mut u2 = login(&store, &users, "u2", b"");
    for sql in [
        "drop user u1",
        "alter user u1 identified with 'authentication_test_plugin' as 'randompassword'",
    ] {
        assert!(u2
            .run(sql)
            .unwrap_err()
            .to_string()
            .contains("SYSTEM_USER or SUPER"));
    }
    assert!(calls.lock().unwrap().static_privs.is_empty());
    assert!(calls.lock().unwrap().dynamic_privs.is_empty());
    let mut u3 = login(&store, &users, "u3", b"");
    for sql in [
        "alter user u1 identified with 'authentication_test_plugin' as 'randompassword'",
        "set password for 'u1'='random2password'",
        "drop user u1",
    ] {
        u3.run(sql).unwrap();
    }
    assert!(calls.lock().unwrap().static_privs.is_empty());
    assert!(calls.lock().unwrap().dynamic_privs.is_empty());
    let mut u4 = login(&store, &users, "u4", b"1");
    for sql in [
        "alter user u3 identified by 'blah'",
        "set password for 'u3'='blahblah'",
        "drop user u3",
    ] {
        u4.run(sql).unwrap();
    }
    assert_eq!(calls.lock().unwrap().static_privs.len(), 3);
    assert_eq!(calls.lock().unwrap().dynamic_privs.len(), 4);
}

/// Go `zzprobe/x01_extension.TestX01ExtensionAuthEdges` (captured in testdata).
#[test]
fn test_x01_extension_auth_edges() {
    crate::init_extension_hooks();
    tidb_extension::reset();
    let _registered = Registered;
    let validations = Arc::new(Mutex::new(Vec::<String>::new()));
    let validated = validations.clone();
    let output = Arc::new(Mutex::new(String::new()));
    let authenticated = output.clone();
    tidb_extension::register(
        "test",
        vec![tidb_extension::with_custom_auth_plugins(vec![AuthPlugin {
            name: "probe_auth".into(),
            authenticate_user: Some(Arc::new(move |r| {
                let state = r.conn_state.unwrap();
                let cert = state.peer_certificates[0]
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                *authenticated.lock().unwrap() = format!(
                    "X01 TLS version={:x} cipher={:x} certificate={cert}\n",
                    state.version, state.cipher_suite
                );
                Ok(())
            })),
            generate_auth_string: Some(Arc::new(|s| (s.into(), true))),
            validate_auth_string: Some(Arc::new(move |s| {
                validated.lock().unwrap().push(s.into());
                s != "reject"
            })),
            ..Default::default()
        }])],
    )
    .unwrap();
    tidb_extension::setup().unwrap();
    let (_store, users, mut root) = fixture();
    root.run("create user u identified with 'probe_auth'")
        .unwrap();
    let mut captured = format!(
        "X01 plugin-only validations={:?}\n",
        validations.lock().unwrap()
    );
    let error = root
        .run("create user rejected identified with 'probe_auth' as 'reject'")
        .unwrap_err();
    captured.push_str(&format!(
        "X01 rejected hash={}\n",
        error.to_mysql_error().error_string()
    ));
    root.run("create user tlsuser identified with 'probe_auth' as 'encoded'")
        .unwrap();
    let state = tidb_variable::vars::TlsConnectionState {
        version: 0x0304,
        cipher_suite: 0x1301,
        peer_certificates: vec![vec![1, 2, 3]],
    };
    let admission = users
        .admit_transport(crate::domain::secure_transport::TransportKind::DirectTls)
        .unwrap();
    users
        .authenticate_with_plugin(
            "tlsuser",
            "localhost",
            &[],
            b"1",
            "probe_auth",
            admission,
            Some(&state),
            None,
        )
        .unwrap();
    captured.push_str(&output.lock().unwrap());
    assert_eq!(
        captured,
        include_str!("../testdata/extension_auth_edges_go.txt")
    );
}
