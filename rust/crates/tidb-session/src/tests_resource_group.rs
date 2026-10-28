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

//! Go's resource groups end to end on one store: the DDL
//! (`ddl/resource_group.go`), `information_schema.RESOURCE_GROUPS`
//! (`setDataFromResourceGroups`), `SET RESOURCE GROUP`, and the account's
//! `RESOURCE GROUP` with the login that applies it (`simple.go`, `Auth`).
//! A fresh store has resource control ON (Go's default, published by the
//! sysvar-cache rebuild at bootstrap) without any `SET GLOBAL`.

use crate::privilege;
use crate::tests_support::{bootstrap_session, row_text, session_as};
use crate::Session;

fn error(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session.run(sql).expect_err(sql).to_mysql_error();
    (error.code, error.message)
}

fn run(session: &mut Session, sql: &str) {
    session
        .run(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
}

#[test]
fn resource_groups_follow_go() {
    let privs = privilege::PrivilegeRegistry::default();
    let mut session = bootstrap_session(&privs);
    session.set_user("root@%".into(), "root@%".into());
    let groups = "select * from information_schema.resource_groups order by name";
    assert_eq!(
        row_text(session.run(groups)),
        [[
            "default",
            "UNLIMITED",
            "MEDIUM",
            "UNLIMITED",
            "NULL",
            "TASK_TYPES='stats'"
        ]]
    );

    run(&mut session, "create resource group rg1 ru_per_sec = 100");
    run(
        &mut session,
        "create resource group rg2 ru_per_sec = 200 priority = high burstable = moderated \
         query_limit = (exec_elapsed = '15s' action = kill watch = exact duration = '10m')",
    );
    assert_eq!(
        row_text(session.run(groups)),
        [
            vec![
                "default",
                "UNLIMITED",
                "MEDIUM",
                "UNLIMITED",
                "NULL",
                "TASK_TYPES='stats'"
            ],
            vec!["rg1", "100", "MEDIUM", "OFF", "NULL", "NULL"],
            vec![
                "rg2",
                "200",
                "HIGH",
                "MODERATED",
                "EXEC_ELAPSED='15s', ACTION=KILL, WATCH=EXACT DURATION='10m0s'",
                "NULL"
            ],
        ]
    );
    assert_eq!(
        error(&mut session, "create resource group rg1 ru_per_sec = 1"),
        (8248, "Resource group 'rg1' already exists".to_owned())
    );
    run(
        &mut session,
        "create resource group if not exists rg1 ru_per_sec = 1",
    );
    assert_eq!(
        error(&mut session, "drop resource group default"),
        (1105, "can't drop reserved resource group".to_owned())
    );
    assert_eq!(
        error(
            &mut session,
            "alter resource group rg1 background = (task_types = 'stats')"
        )
        .1,
        "unsupported operation. Currently, only the default resource group support change \
         background settings"
    );
    assert_eq!(
        error(
            &mut session,
            "alter resource group default background = (task_types = 'nosuch')"
        ),
        (8255, "Unknown background task name 'nosuch'".to_owned())
    );
    assert_eq!(
        error(
            &mut session,
            "alter resource group rg1 query_limit = (action = kill)"
        )
        .1,
        "please set at least one field(exec_elapsed_time_ms, processed_keys, ru)"
    );

    assert_eq!(
        error(&mut session, "set resource group nosuch"),
        (8249, "Unknown resource group 'nosuch'".to_owned())
    );
    run(&mut session, "set resource group rg1");
    assert_eq!(
        row_text(session.run("select current_resource_group()")),
        [["rg1"]]
    );
    run(&mut session, "set resource group ``");

    // An account's group, recorded in User_attributes and applied at login.
    run(&mut session, "create user u1");
    run(&mut session, "alter user u1 resource group rg1");
    assert_eq!(
        row_text(session.run("select user_attributes from mysql.user where user = 'u1'")),
        [[r#"{"resource_group": "rg1"}"#]]
    );
    run(&mut session, "create role r1");
    assert_eq!(
        error(&mut session, "alter user r1 resource group rg1"),
        (8257, "Cannot set resource group for a role".to_owned())
    );
    assert_eq!(
        error(&mut session, "create user u2 resource group nosuch"),
        (8249, "Unknown resource group 'nosuch'".to_owned())
    );
    let mut login = session_as(&privs, crate::SharedCatalog::clone(&session.catalog), "u1", "%");
    assert_eq!(
        row_text(login.run("select current_resource_group()")),
        [["rg1"]]
    );

    assert_eq!(
        error(&mut session, "drop resource group rg1"),
        (
            1105,
            "user [u1] depends on the resource group to drop".to_owned()
        )
    );
    run(&mut session, "drop resource group rg2");
    run(&mut session, "drop resource group if exists rg2");
}
