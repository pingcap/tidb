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

#[test]
fn auto_random_base_seeds_rebases_reports_and_validates_the_counter() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE ar_base (id BIGINT AUTO_RANDOM(5) PRIMARY KEY, v INT) \
             AUTO_RANDOM_BASE=100",
        )
        .unwrap();
    let shown = rows(&mut session, "SHOW CREATE TABLE ar_base")[0][1].clone();
    assert!(shown.contains("/*T![auto_rand_base] AUTO_RANDOM_BASE=100 */"));

    session.run("INSERT INTO ar_base (v) VALUES (1)").unwrap();
    let first = rows(&mut session, "SELECT id FROM ar_base")[0][0]
        .parse::<i64>()
        .unwrap();
    assert_eq!(first & ((1_i64 << 58) - 1), 100);

    session
        .run("ALTER TABLE ar_base AUTO_RANDOM_BASE=500")
        .unwrap();
    session.run("INSERT INTO ar_base (v) VALUES (2)").unwrap();
    let second = rows(&mut session, "SELECT id FROM ar_base WHERE v=2")[0][0]
        .parse::<i64>()
        .unwrap();
    assert_eq!(second & ((1_i64 << 58) - 1), 500);

    session
        .run("ALTER TABLE ar_base AUTO_RANDOM_BASE=10")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SHOW WARNINGS"),
        vec![vec![
            "Warning".to_owned(),
            "1105".to_owned(),
            "Can't reset AUTO_INCREMENT to 10 without FORCE option, using 501 instead".to_owned(),
        ]]
    );
    session.run("INSERT INTO ar_base (v) VALUES (3)").unwrap();
    let third = rows(&mut session, "SELECT id FROM ar_base WHERE v=3")[0][0]
        .parse::<i64>()
        .unwrap();
    assert_eq!(third & ((1_i64 << 58) - 1), 501);

    session
        .run("ALTER TABLE ar_base FORCE AUTO_RANDOM_BASE=2")
        .unwrap();
    session.run("INSERT INTO ar_base (v) VALUES (4)").unwrap();
    let fourth = rows(&mut session, "SELECT id FROM ar_base WHERE v=4")[0][0]
        .parse::<i64>()
        .unwrap();
    assert_eq!(fourth & ((1_i64 << 58) - 1), 2);

    let error = session
        .run("ALTER TABLE ar_base FORCE AUTO_RANDOM_BASE=0")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1467);

    let error = session
        .run(&format!(
            "ALTER TABLE ar_base AUTO_RANDOM_BASE={}",
            1_u64 << 58
        ))
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 8216);

    session.run("CREATE TABLE plain (id BIGINT)").unwrap();
    let error = session
        .run("ALTER TABLE plain AUTO_RANDOM_BASE=10")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 8216);
}

#[test]
fn modify_column_increases_auto_random_bits_and_converts_auto_increment() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ar_bits (id BIGINT AUTO_RANDOM(5) PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("ALTER TABLE ar_bits MODIFY COLUMN id BIGINT AUTO_RANDOM(8)")
        .unwrap();
    let shown = rows(&mut session, "SHOW CREATE TABLE ar_bits")[0][1].clone();
    assert!(shown.contains("AUTO_RANDOM(8)"));
    session.run("INSERT INTO ar_bits (v) VALUES (1)").unwrap();
    let first = rows(&mut session, "SELECT id FROM ar_bits")[0][0]
        .parse::<i64>()
        .unwrap();
    assert_eq!(first & ((1_i64 << 55) - 1), 2);

    let error = session
        .run("ALTER TABLE ar_bits MODIFY COLUMN id BIGINT AUTO_RANDOM(7)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 8216);
    assert_eq!(
        error.message,
        "Invalid auto random: decreasing auto_random shard bits is not supported"
    );

    let error = session
        .run("ALTER TABLE ar_bits MODIFY COLUMN id BIGINT")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 8216);
    assert_eq!(
        error.message,
        "Invalid auto random: adding/dropping/modifying auto_random is not supported"
    );

    let error = session
        .run("ALTER TABLE ar_bits MODIFY COLUMN id BIGINT AUTO_RANDOM(9, 32)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 8216);
    assert_eq!(
        error.message,
        "Invalid auto random: alter the range bits of auto_random column is not supported"
    );

    session
        .run("CREATE TABLE ar_overflow (id BIGINT AUTO_RANDOM(5) PRIMARY KEY)")
        .unwrap();
    session
        .run(&format!(
            "ALTER TABLE ar_overflow FORCE AUTO_RANDOM_BASE={}",
            1_u64 << 55
        ))
        .unwrap();
    let error = session
        .run("ALTER TABLE ar_overflow MODIFY COLUMN id BIGINT AUTO_RANDOM(8)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 8216);
    assert_eq!(
        error.message,
        "Invalid auto random: max allowed auto_random shard bits is 7, but got 8 on column `id`"
    );

    session
        .run("CREATE TABLE plain_to_ar (id BIGINT PRIMARY KEY)")
        .unwrap();
    let error = session
        .run("ALTER TABLE plain_to_ar MODIFY COLUMN id BIGINT AUTO_RANDOM(5)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 8216);
    assert_eq!(
        error.message,
        "Invalid auto random: auto_random can only be converted from auto_increment clustered primary key"
    );

    session
        .run("CREATE TABLE ai_to_ar (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)")
        .unwrap();
    session.run("INSERT INTO ai_to_ar (v) VALUES (1)").unwrap();
    session
        .run("ALTER TABLE ai_to_ar MODIFY COLUMN id BIGINT AUTO_RANDOM(5)")
        .unwrap();
    let shown = rows(&mut session, "SHOW CREATE TABLE ai_to_ar")[0][1].clone();
    assert!(shown.contains("AUTO_RANDOM(5)"));
    assert!(!shown.contains("AUTO_INCREMENT"));
    session.run("INSERT INTO ai_to_ar (v) VALUES (2)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT COUNT(*) FROM ai_to_ar")[0][0],
        "2"
    );
    let converted = rows(&mut session, "SELECT id FROM ai_to_ar WHERE v=2")[0][0]
        .parse::<i64>()
        .unwrap();
    assert!(
        converted & ((1_i64 << 58) - 1) > 1,
        "the migrated random counter must stay above the consumed auto-increment id"
    );
}
