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

//! Direct-Go ranking/distribution vectors through the live SQL consumer.

use tidb_datatype::Datum;
use tidb_exec::{Database, Outcome, ResultSet};

fn execute(database: &mut Database, sql: &str) -> Result<Outcome, tidb_exec::ExecError> {
    database.run(&tidb_parser::parse(sql).expect("ranking SQL parses"))
}

fn done(database: &mut Database, sql: &str) {
    assert_eq!(execute(database, sql), Ok(Outcome::Done), "SQL: {sql}");
}

fn rows(database: &mut Database, sql: &str, expected: Vec<Vec<Datum>>) {
    assert_eq!(
        execute(database, sql),
        Ok(Outcome::Rows(ResultSet {
            rows: expected,
            ordered: sql.to_ascii_lowercase().contains("order by"),
        })),
        "SQL: {sql}"
    );
}

fn fixture() -> Database {
    let mut database = Database::new();
    done(
        &mut database,
        "create table ranking_live (id int, p int, k int)",
    );
    done(
        &mut database,
        "insert into ranking_live values (1,1,10),(2,1,20),(3,1,20),(4,1,30),(5,2,7)",
    );
    database
}

#[test]
fn canonical_peer_geometry_drives_all_live_ranking_functions() {
    let mut database = fixture();
    rows(
        &mut database,
        "select id, row_number() over (partition by p order by k), rank() over (partition by p order by k), dense_rank() over (partition by p order by k), percent_rank() over (partition by p order by k), cume_dist() over (partition by p order by k) from ranking_live order by id",
        vec![
            vec![Datum::Int(1), Datum::Int(1), Datum::Int(1), Datum::Int(1), Datum::Real(0.0), Datum::Real(0.25)],
            vec![Datum::Int(2), Datum::Int(2), Datum::Int(2), Datum::Int(2), Datum::Real(1.0 / 3.0), Datum::Real(0.75)],
            vec![Datum::Int(3), Datum::Int(3), Datum::Int(2), Datum::Int(2), Datum::Real(1.0 / 3.0), Datum::Real(0.75)],
            vec![Datum::Int(4), Datum::Int(4), Datum::Int(4), Datum::Int(3), Datum::Real(1.0), Datum::Real(1.0)],
            vec![Datum::Int(5), Datum::Int(1), Datum::Int(1), Datum::Int(1), Datum::Real(0.0), Datum::Real(1.0)],
        ],
    );

    // With no ORDER BY every partition row is one peer group. Physical row
    // numbers remain stable, while all peer-aware functions share 1/1/0/1.
    rows(
        &mut database,
        "select id, row_number() over (partition by p), rank() over (partition by p), dense_rank() over (partition by p), percent_rank() over (partition by p), cume_dist() over (partition by p) from ranking_live order by id",
        vec![
            vec![Datum::Int(1), Datum::Int(1), Datum::Int(1), Datum::Int(1), Datum::Real(0.0), Datum::Real(1.0)],
            vec![Datum::Int(2), Datum::Int(2), Datum::Int(1), Datum::Int(1), Datum::Real(0.0), Datum::Real(1.0)],
            vec![Datum::Int(3), Datum::Int(3), Datum::Int(1), Datum::Int(1), Datum::Real(0.0), Datum::Real(1.0)],
            vec![Datum::Int(4), Datum::Int(4), Datum::Int(1), Datum::Int(1), Datum::Real(0.0), Datum::Real(1.0)],
            vec![Datum::Int(5), Datum::Int(1), Datum::Int(1), Datum::Int(1), Datum::Real(0.0), Datum::Real(1.0)],
        ],
    );
}

#[test]
fn ntile_live_runtime_uses_one_source_bucket_cursor_per_partition() {
    let mut database = fixture();
    // The owning Go TestWindowFunctions vector has no ORDER BY comparer.
    // Preserve stable partition scan order in that exact path.
    rows(
        &mut database,
        "select id, ntile(3) over (partition by p) from ranking_live order by id",
        vec![
            vec![Datum::Int(1), Datum::UInt(1)],
            vec![Datum::Int(2), Datum::UInt(1)],
            vec![Datum::Int(3), Datum::UInt(2)],
            vec![Datum::Int(4), Datum::UInt(3)],
            vec![Datum::Int(5), Datum::UInt(1)],
        ],
    );
    rows(
        &mut database,
        "select id, ntile(3) over (partition by p order by k) from ranking_live order by id",
        vec![
            vec![Datum::Int(1), Datum::UInt(1)],
            vec![Datum::Int(2), Datum::UInt(1)],
            vec![Datum::Int(3), Datum::UInt(2)],
            vec![Datum::Int(4), Datum::UInt(3)],
            vec![Datum::Int(5), Datum::UInt(1)],
        ],
    );
    rows(
        &mut database,
        "select id, ntile(5) over (partition by p order by k) from ranking_live order by id",
        vec![
            vec![Datum::Int(1), Datum::UInt(1)],
            vec![Datum::Int(2), Datum::UInt(2)],
            vec![Datum::Int(3), Datum::UInt(3)],
            vec![Datum::Int(4), Datum::UInt(4)],
            vec![Datum::Int(5), Datum::UInt(1)],
        ],
    );
    // The AST retains integer digits, so the descriptor accepts the same
    // positive uint64 constant domain as Go's GetUint64FromConstant rather
    // than narrowing the bucket count to i64.
    rows(
        &mut database,
        "select id, ntile(18446744073709551615) over (partition by p order by k) from ranking_live order by id",
        vec![
            vec![Datum::Int(1), Datum::UInt(1)],
            vec![Datum::Int(2), Datum::UInt(2)],
            vec![Datum::Int(3), Datum::UInt(3)],
            vec![Datum::Int(4), Datum::UInt(4)],
            vec![Datum::Int(5), Datum::UInt(1)],
        ],
    );
    // TRUE is an Int64-valued Constant in Go's expression layer, so
    // GetUint64FromConstant resolves it to one at descriptor time.
    rows(
        &mut database,
        "select id, ntile(true) over (partition by p order by k) from ranking_live order by id",
        vec![
            vec![Datum::Int(1), Datum::UInt(1)],
            vec![Datum::Int(2), Datum::UInt(1)],
            vec![Datum::Int(3), Datum::UInt(1)],
            vec![Datum::Int(4), Datum::UInt(1)],
            vec![Datum::Int(5), Datum::UInt(1)],
        ],
    );
    rows(
        &mut database,
        "select id, ntile(null) over (partition by p order by k) from ranking_live order by id",
        vec![
            vec![Datum::Int(1), Datum::Null],
            vec![Datum::Int(2), Datum::Null],
            vec![Datum::Int(3), Datum::Null],
            vec![Datum::Int(4), Datum::Null],
            vec![Datum::Int(5), Datum::Null],
        ],
    );
}

#[test]
fn ntile_rejects_nonpositive_and_row_dependent_arguments() {
    let mut database = fixture();
    for sql in [
        "select ntile(0) over (order by id) from ranking_live",
        "select ntile(-1) over (order by id) from ranking_live",
        "select ntile(false) over (order by id) from ranking_live",
        "select ntile(k) over (order by id) from ranking_live",
    ] {
        assert!(
            execute(&mut database, sql).is_err(),
            "NTILE descriptor boundary accepted: {sql}"
        );
    }
}
