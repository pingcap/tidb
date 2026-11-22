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


//! Go `pkg/executor` cases the executor's in-memory tier used to carry, over
//! the cop-backed TestKit stack (X11): `TestInsertIgnore`'s surviving rows and
//! `join_pkg_test.go`'s equi-join row parity, alone and under an apply.

use super::sql_kit::*;
use super::*;

#[test]
fn insert_ignore_keeps_the_nonduplicate_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 820);
    exec_all(&mut tk, &[
        "create table t (id int primary key, c int unique key)",
        "insert into t values (1, 2)",
        "insert ignore into t values (1, 3), (2, 3)",
    ]);
    assert_eq!(q(&mut tk, "select id, c from t order by id"), [["1", "2"], ["2", "3"]]);
}

#[test]
fn join_exec_inner_equi_rows_source() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 821);
    for rows_count in [3i64, 1024, 4096] {
        exec_all(&mut tk, &["drop table if exists tl, tr", "create table tl(a bigint, b double)", "create table tr(a bigint, b double)"]);
        let values = (0..rows_count).map(|i| format!("({i}, {i}.0)")).collect::<Vec<_>>().join(",");
        rows(&mut tk, &format!("insert into tl values {values}"));
        rows(&mut tk, &format!("insert into tr values {values}"));
        let result = rows(&mut tk, "select tl.a, tl.b, tr.a, tr.b from tl, tr where tl.a = tr.a and tl.b = tr.b order by tl.a");
        assert_eq!(result.len(), rows_count as usize);
        for (key, row) in result.iter().enumerate() {
            let key = key as i64;
            assert_eq!(row, &vec![Datum::Int(key), Datum::Real(key as f64), Datum::Int(key), Datum::Real(key as f64)]);
        }
    }
}

#[test]
fn join_under_apply_repeated_inner_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 822);
    exec_all(&mut tk, &[
        "create table t_outer(a int)",
        "create table tl(a bigint, b double)",
        "create table tr(a bigint, b double)",
        "insert into t_outer values (1),(2),(3)",
        "insert into tl values (0,0),(1,1),(2,2)",
        "insert into tr values (0,0),(1,1),(2,2)",
    ]);
    assert_eq!(
        q(&mut tk, "select t_outer.a, (select count(*) from tl, tr where tl.a = tr.a and tl.b = tr.b and tl.a < t_outer.a) from t_outer order by t_outer.a"),
        [["1", "1"], ["2", "2"], ["3", "3"]]
    );
}
