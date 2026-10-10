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

//! Go's `ALTER DATABASE` (`executor.AlterSchema`) and the schema placement
//! `CREATE DATABASE` records: SHOW CREATE DATABASE, schemata, the default a
//! new table inherits, and the policy-in-use refusal.

use crate::tests_support::row_text;
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
fn alter_schema_and_schema_placement_follow_go() {
    let mut session = Session::new();
    run(&mut session, "create placement policy p1 followers=4");
    run(&mut session, "create database d1 placement policy p1");
    assert_eq!(
        row_text(session.run("show create database d1"))[0][1],
        "CREATE DATABASE `d1` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ \
         /*T![placement] PLACEMENT POLICY=`p1` */"
    );
    assert_eq!(
        row_text(session.run(
            "select tidb_placement_policy_name from information_schema.schemata \
             where schema_name = 'd1'"
        )),
        [["p1"]]
    );
    // A table with no placement of its own takes the schema's.
    run(&mut session, "create table d1.t (a int)");
    assert!(
        row_text(session.run("show create table d1.t"))[0][1]
            .contains("/*T![placement] PLACEMENT POLICY=`p1` */"),
        "the table inherits the schema placement"
    );
    assert_eq!(error(&mut session, "drop placement policy p1").0, 8241);
    assert_eq!(
        error(&mut session, "create database d2 placement policy nosuch"),
        (8239, "Unknown placement policy 'nosuch'".to_owned())
    );

    run(&mut session, "alter database d1 placement policy = default");
    assert_eq!(
        row_text(session.run("show create database d1"))[0][1],
        "CREATE DATABASE `d1` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"
    );
    run(
        &mut session,
        "alter database d1 charset utf8mb4 collate utf8mb4_general_ci",
    );
    assert_eq!(
        row_text(session.run(
            "select default_collation_name from information_schema.schemata \
             where schema_name = 'd1'"
        )),
        [["utf8mb4_general_ci"]]
    );
    assert_eq!(
        error(
            &mut session,
            "alter database d1 charset utf8 collate utf8mb4_bin"
        ),
        (
            1302,
            "Conflicting declarations: 'CHARACTER SET utf8' and 'CHARACTER SET utf8mb4'".to_owned()
        )
    );
    assert_eq!(
        error(&mut session, "alter database nosuch placement policy p1").0,
        1049
    );
    run(&mut session, "use d1");
    run(&mut session, "alter database placement policy p1");
    assert!(row_text(session.run("show create database d1"))[0][1]
        .ends_with("/*T![placement] PLACEMENT POLICY=`p1` */"));
}
