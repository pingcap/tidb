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

//! Multi-valued indexes (`CAST(... AS ... ARRAY)` key parts): Go's DDL
//! admission, `castJSONAsArrayFunctionSig`, and the write path's
//! `index.getIndexedValue` expansion. Every expected answer is a Go oracle
//! run on this branch, recorded in `rust/docs/multi-valued-index-execplan.md`.

use crate::tests_support::row_text;
use crate::Session;

fn error_text(session: &mut Session, sql: &str) -> String {
    session
        .run(sql)
        .expect_err("the statement must fail")
        .to_mysql_error()
        .message
}

/// The stored entry count of one index of `test.<table>`.
fn index_entry_count(session: &mut Session, table: &str, index: &str) -> usize {
    session
        .with_catalog_mut(|catalog| {
            let Some(tidb_executor::TableEntry::Kv(stored)) = catalog.table_mut_in("test", table)
            else {
                panic!("{table} is not stored as bytes");
            };
            let stored = std::sync::Arc::make_mut(stored);
            let index = stored
                .index_list_for_check()
                .into_iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(index))
                .expect("the index exists");
            Ok(stored
                .index_entries_for_check(index.id)
                .expect("entries readable")
                .len())
        })
        .unwrap()
}

/// `[1,2,2]` files two keys, NULL one, `[]` none and the scalar `7` one (Go
/// casts it to `[7]`): Go `getIndexedValue`'s four cases. Before the port
/// the DDL itself was refused, and every later statement on the table failed
/// with "doesn't exist".
#[test]
fn a_multi_valued_index_files_one_key_per_distinct_element() {
    let mut session = Session::new();
    session
        .run("create table t(a int, j json, index kj((cast(j as signed array))))")
        .unwrap();
    assert_eq!(
        row_text(session.run("show create table t"))[0][1],
        "CREATE TABLE `t` (\n  `a` int DEFAULT NULL,\n  `j` json DEFAULT NULL,\n  \
         KEY `kj` ((cast(`j` as signed array)))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 \
         COLLATE=utf8mb4_bin"
    );
    session
        .run("insert into t values (1, '[1,2,2]'), (2, null), (3, '[]'), (4, '[3]'), (5, '7')")
        .unwrap();
    assert_eq!(index_entry_count(&mut session, "t", "kj"), 2 + 1 + 0 + 1 + 1);
    assert_eq!(
        row_text(session.run("select a from t where 2 member of (j)")),
        vec![vec!["1"]]
    );
    assert_eq!(
        row_text(session.run("select a from t where json_overlaps(j, '[1,3]') order by a")),
        vec![vec!["1"], vec!["4"]]
    );
    session.run("admin check table t").unwrap();

    // An UPDATE removes every old key and files every new one; a DELETE
    // removes them all.
    session.run("update t set j = '[1,9,1]' where a = 1").unwrap();
    assert_eq!(index_entry_count(&mut session, "t", "kj"), 2 + 1 + 0 + 1 + 1);
    session.run("delete from t where a = 4").unwrap();
    assert_eq!(index_entry_count(&mut session, "t", "kj"), 2 + 1 + 0 + 1);
    session.run("admin check table t").unwrap();

    // ADD INDEX backfills the same expansion.
    session
        .run("alter table t add index k2((cast(j as unsigned array)))")
        .unwrap();
    assert_eq!(index_entry_count(&mut session, "t", "k2"), 2 + 1 + 0 + 1);
    session.run("admin check table t").unwrap();
}

/// A unique multi-valued index rejects a shared ELEMENT, and Go's message
/// carries the whole array (`addIndices` formats the row's values before
/// expansion).
#[test]
fn a_unique_multi_valued_index_rejects_a_shared_element() {
    let mut session = Session::new();
    session
        .run("create table t4(j json, unique index k((cast(j->'$.a' as char(10) array))))")
        .unwrap();
    assert_eq!(
        error_text(
            &mut session,
            r#"insert into t4 values ('{"a":["x","y"]}'), ('{"a":["y"]}')"#
        ),
        r#"Duplicate entry '["y"]' for key 't4.k'"#
    );
    session
        .run(r#"insert into t4 values ('{"a":["x","y"]}'), ('{"a":["z"]}')"#)
        .unwrap();
    assert_eq!(index_entry_count(&mut session, "t4", "k"), 3);
}

/// Go `completeError`: a failed element conversion during INSERT names the
/// index (3903, 3907); the object refusal and the cast outside an index are
/// Go's 1235 texts.
#[test]
fn an_unconvertible_element_names_its_index() {
    let mut session = Session::new();
    session
        .run("create table t(a int, j json, index kj((cast(j as signed array))))")
        .unwrap();
    assert_eq!(
        error_text(&mut session, r#"insert into t values (6, '["x"]')"#),
        "Invalid JSON value for CAST for expression index 'kj'"
    );
    assert_eq!(
        error_text(&mut session, r#"insert into t values (6, '{"a":1}')"#),
        "This version of TiDB doesn't yet support 'CAST-ing JSON OBJECT type to array'"
    );
    assert_eq!(
        error_text(&mut session, "select cast('[1,2]' as signed array)"),
        "This version of TiDB doesn't yet support 'Use of CAST( .. AS .. ARRAY) outside of \
         functional index in CREATE(non-SELECT)/ALTER TABLE or in general expressions'"
    );
    session
        .run("create table t4(j json, index k((cast(j as char(10) array))))")
        .unwrap();
    assert_eq!(
        error_text(&mut session, r#"insert into t4 values ('["abcdefghijklmn"]')"#),
        "Data too long for expression index 'k'"
    );
}

/// Go `buildIndexColumns`, `CheckPKOnGeneratedColumn` and
/// `castAsArrayFunctionClass` refusals.
#[test]
fn multi_valued_key_parts_follow_go_admission_rules() {
    let mut session = Session::new();
    assert_eq!(
        error_text(
            &mut session,
            "create table t2(j json, index k((cast(j as signed array)), \
             (cast(j as unsigned array))))"
        ),
        "This version of TiDB doesn't yet support 'more than one multi-valued key part per index'"
    );
    assert_eq!(
        error_text(
            &mut session,
            "create table t3(j json, primary key((cast(j as signed array))))"
        ),
        "The primary key cannot be an expression index"
    );
    assert_eq!(
        error_text(
            &mut session,
            "create table t6(j json, index k((cast(j as json array))))"
        ),
        "This version of TiDB doesn't yet support 'CAST-ing data to array of json BINARY'"
    );
    assert_eq!(
        error_text(
            &mut session,
            "create table t7(j json, k json as (cast(j as signed array)))"
        ),
        "This version of TiDB doesn't yet support 'Use of CAST( .. AS .. ARRAY) outside of \
         functional index in CREATE(non-SELECT)/ALTER TABLE or in general expressions'"
    );
}
