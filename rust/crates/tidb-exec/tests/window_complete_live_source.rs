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

//! Consolidated live vectors for the canonical physical-partition runtime.

use tidb_datatype::Datum;
use tidb_exec::{Database, Outcome, ResultSet};

fn execute(database: &mut Database, sql: &str) -> Result<Outcome, tidb_exec::ExecError> {
    database.run(&tidb_parser::parse(sql).expect("window SQL parses"))
}

fn done(database: &mut Database, sql: &str) {
    assert_eq!(execute(database, sql), Ok(Outcome::Done), "SQL: {sql}");
}

#[test]
fn all_physical_partition_consumers_share_order_peers_and_reset_boundary() {
    let mut database = Database::new();
    done(
        &mut database,
        "create table window_complete (id int, p int, k int)",
    );
    done(
        &mut database,
        "insert into window_complete values (1,1,10),(2,1,20),(3,1,20),(4,1,30),(5,2,7)",
    );

    let sql = "select id, row_number() over (partition by p order by k), rank() over (partition by p order by k), dense_rank() over (partition by p order by k), percent_rank() over (partition by p order by k), cume_dist() over (partition by p order by k), ntile(3) over (partition by p order by k), lag(id) over (partition by p order by k), lead(id) over (partition by p order by k) from window_complete order by id";
    assert_eq!(
        execute(&mut database, sql),
        Ok(Outcome::Rows(ResultSet {
            rows: vec![
                vec![
                    Datum::Int(1),
                    Datum::Int(1),
                    Datum::Int(1),
                    Datum::Int(1),
                    Datum::Real(0.0),
                    Datum::Real(0.25),
                    Datum::UInt(1),
                    Datum::Null,
                    Datum::Int(2),
                ],
                vec![
                    Datum::Int(2),
                    Datum::Int(2),
                    Datum::Int(2),
                    Datum::Int(2),
                    Datum::Real(1.0 / 3.0),
                    Datum::Real(0.75),
                    Datum::UInt(1),
                    Datum::Int(1),
                    Datum::Int(3),
                ],
                vec![
                    Datum::Int(3),
                    Datum::Int(3),
                    Datum::Int(2),
                    Datum::Int(2),
                    Datum::Real(1.0 / 3.0),
                    Datum::Real(0.75),
                    Datum::UInt(2),
                    Datum::Int(2),
                    Datum::Int(4),
                ],
                vec![
                    Datum::Int(4),
                    Datum::Int(4),
                    Datum::Int(4),
                    Datum::Int(3),
                    Datum::Real(1.0),
                    Datum::Real(1.0),
                    Datum::UInt(3),
                    Datum::Int(3),
                    Datum::Null,
                ],
                vec![
                    Datum::Int(5),
                    Datum::Int(1),
                    Datum::Int(1),
                    Datum::Int(1),
                    Datum::Real(0.0),
                    Datum::Real(1.0),
                    Datum::UInt(1),
                    Datum::Null,
                    Datum::Null,
                ],
            ],
            ordered: true,
        })),
        "SQL: {sql}"
    );
}
