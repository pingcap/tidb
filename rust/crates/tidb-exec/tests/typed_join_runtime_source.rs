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

//! Live COM_QUERY coverage for planner-bound typed join conditions.

use tidb_datatype::Datum;
use tidb_exec::{Cluster, Outcome, ResultSet};

fn done(session: &mut tidb_exec::Session, sql: &str) {
    assert_eq!(session.execute_sql(sql), Ok(Outcome::Done), "SQL: {sql}");
}

fn rows(session: &mut tidb_exec::Session, sql: &str, expected: Vec<Vec<Datum>>) {
    assert_eq!(
        session.execute_sql(sql),
        Ok(Outcome::Rows(ResultSet {
            rows: expected,
            ordered: sql.to_ascii_lowercase().contains("order by"),
        })),
        "SQL: {sql}"
    );
}

#[test]
fn com_query_inner_and_left_join_consume_typed_residuals() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    done(&mut session, "create table typed_left (id int, v int)");
    done(&mut session, "create table typed_right (id int, v int)");
    done(
        &mut session,
        "insert into typed_left values (1, 9), (2, 2), (3, null)",
    );
    done(
        &mut session,
        "insert into typed_right values (1, 4), (2, 8), (3, 1)",
    );

    rows(
        &mut session,
        "select l.id, r.v from typed_left l join typed_right r on l.id = r.id and l.v > r.v order by l.id",
        vec![vec![Datum::Int(1), Datum::Int(4)]],
    );
    rows(
        &mut session,
        "select l.id, r.v from typed_left l join typed_right r on l.id = r.id and l.v between r.v and 10 order by l.id",
        vec![vec![Datum::Int(1), Datum::Int(4)]],
    );
    rows(
        &mut session,
        "select l.id, r.v from typed_left l left join typed_right r on l.id = r.id and l.v > r.v order by l.id",
        vec![
            vec![Datum::Int(1), Datum::Int(4)],
            vec![Datum::Int(2), Datum::Null],
            vec![Datum::Int(3), Datum::Null],
        ],
    );
}

#[test]
fn null_safe_equality_and_using_share_the_typed_truth_path() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    done(&mut session, "create table null_left (id int, l int)");
    done(&mut session, "create table null_right (id int, r int)");
    done(
        &mut session,
        "insert into null_left values (null, 1), (2, 2), (5, 5)",
    );
    done(
        &mut session,
        "insert into null_right values (null, 3), (2, 4)",
    );

    rows(
        &mut session,
        "select l.l, r.r from null_left l join null_right r on l.id <=> r.id order by l.l",
        vec![
            vec![Datum::Int(1), Datum::Int(3)],
            vec![Datum::Int(2), Datum::Int(4)],
        ],
    );
    rows(
        &mut session,
        "select * from null_left join null_right using (id) order by l",
        vec![vec![Datum::Int(2), Datum::Int(2), Datum::Int(4)]],
    );
    rows(
        &mut session,
        "select l.id, r.id, l.*, r.* from null_left l left join null_right r using (id) order by l.l",
        vec![
            vec![
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Int(1),
                Datum::Null,
                Datum::Null,
            ],
            vec![
                Datum::Int(2),
                Datum::Int(2),
                Datum::Int(2),
                Datum::Int(2),
                Datum::Int(2),
                Datum::Int(4),
            ],
            vec![
                Datum::Int(5),
                Datum::Null,
                Datum::Int(5),
                Datum::Int(5),
                Datum::Null,
                Datum::Null,
            ],
        ],
    );
}

#[test]
fn right_and_natural_join_reach_the_shared_session_runtime() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    done(&mut session, "create table right_left (id int, v int)");
    done(&mut session, "create table right_outer (id int, v int)");
    done(
        &mut session,
        "insert into right_left values (1, 10), (2, null), (3, 30)",
    );
    done(
        &mut session,
        "insert into right_outer values (2, 5), (1, 5), (4, 40)",
    );

    // The executor emits the preserved right/outer relation scan order. ON
    // UNKNOWN for id=2 is a nonmatch and therefore produces the
    // same left NULL extension as FALSE or a missing key.
    rows(
        &mut session,
        "select l.id, l.v, r.id from right_left l right join right_outer r on l.id = r.id and l.v > r.v",
        vec![
            vec![Datum::Null, Datum::Null, Datum::Int(2)],
            vec![Datum::Int(1), Datum::Int(10), Datum::Int(1)],
            vec![Datum::Null, Datum::Null, Datum::Int(4)],
        ],
    );

    done(
        &mut session,
        "create table natural_left (z int, id int, left_only int)",
    );
    done(
        &mut session,
        "create table natural_outer (id int, z int, right_only int)",
    );
    done(&mut session, "insert into natural_left values (10, 1, 100)");
    done(
        &mut session,
        "insert into natural_outer values (1, 10, 200), (2, 20, 300)",
    );

    // RIGHT USING/NATURAL common columns follow original right (outer)
    // declaration order: id,z, then right remainder, then left remainder.
    for sql in [
        "select * from natural_left l right join natural_outer r using (z, id)",
        "select * from natural_left l natural right join natural_outer r",
    ] {
        rows(
            &mut session,
            sql,
            vec![
                vec![
                    Datum::Int(1),
                    Datum::Int(10),
                    Datum::Int(200),
                    Datum::Int(100),
                ],
                vec![Datum::Int(2), Datum::Int(20), Datum::Int(300), Datum::Null],
            ],
        );
    }

    done(&mut session, "create table natural_a (a int)");
    done(&mut session, "create table natural_b (b int)");
    done(&mut session, "insert into natural_a values (1), (2)");
    done(&mut session, "insert into natural_b values (3), (4)");
    rows(
        &mut session,
        "select * from natural_a natural join natural_b",
        vec![
            vec![Datum::Int(1), Datum::Int(3)],
            vec![Datum::Int(1), Datum::Int(4)],
            vec![Datum::Int(2), Datum::Int(3)],
            vec![Datum::Int(2), Datum::Int(4)],
        ],
    );
}
