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

#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

#[test]
fn auto_random_metadata_round_trips_through_show_create_table() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ar (id BIGINT AUTO_RANDOM(5, 53) PRIMARY KEY, v INT)")
        .unwrap();

    let shown = rows(&mut session, "SHOW CREATE TABLE ar")[0][1].clone();
    assert!(shown.contains("/*T![auto_rand] AUTO_RANDOM(5, 53) */"));
}

#[test]
fn auto_random_allocates_rebases_and_guards_explicit_values() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ar (id BIGINT AUTO_RANDOM(3) PRIMARY KEY, v INT)")
        .unwrap();
    session.run("INSERT INTO ar (v) VALUES (10), (20)").unwrap();

    let ids = rows(&mut session, "SELECT id FROM ar ORDER BY v")
        .into_iter()
        .map(|row| row[0].parse::<i64>().unwrap())
        .collect::<Vec<_>>();
    let incremental_mask = (1_i64 << 60) - 1;
    assert_eq!(
        ids.iter()
            .map(|id| id & incremental_mask)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(ids[0] >> 60, ids[1] >> 60);

    let shown = rows(&mut session, "SHOW CREATE TABLE ar")[0][1].clone();
    assert!(shown.contains("/*T![auto_rand] AUTO_RANDOM(3) */"));

    let error = session
        .run("INSERT INTO ar VALUES (7, 70)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 8216);

    session
        .run("SET @@allow_auto_random_explicit_insert = ON")
        .unwrap();
    session.run("INSERT INTO ar VALUES (7, 70)").unwrap();
    session.run("INSERT INTO ar (v) VALUES (80)").unwrap();
    let generated = rows(&mut session, "SELECT id FROM ar WHERE v = 80")[0][0]
        .parse::<i64>()
        .unwrap();
    assert_eq!(generated & incremental_mask, 8);
}

#[test]
fn auto_random_is_the_first_column_of_a_clustered_common_handle() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE ar_common (\
                 id BIGINT AUTO_RANDOM(3), tenant BIGINT, v INT, \
                 PRIMARY KEY (id, tenant) CLUSTERED)",
        )
        .unwrap();
    session
        .run("INSERT INTO ar_common (tenant, v) VALUES (9, 10), (9, 20)")
        .unwrap();
    let ids = rows(&mut session, "SELECT id FROM ar_common ORDER BY v")
        .into_iter()
        .map(|row| row[0].parse::<i64>().unwrap())
        .collect::<Vec<_>>();
    let incremental_mask = (1_i64 << 60) - 1;
    assert_eq!(ids[0] & incremental_mask, 1);
    assert_eq!(ids[1] & incremental_mask, 2);
}
