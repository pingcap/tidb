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

//! Go `executor.createColumnarIndex`'s checks before the job is built
//! (`checkTableTypeForColumnarIndex`), for `ALTER TABLE ... ADD VECTOR
//! INDEX` and `CREATE VECTOR INDEX`. Every answer below was captured from
//! gorun on a store without TiFlash.

use super::Session;

fn error_of(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session.run(sql).expect_err(sql).to_mysql_error();
    (error.code, error.message)
}

#[test]
fn a_columnar_index_is_checked_against_its_table_like_go() {
    let mut session = Session::new();
    session.run("create table t(a int, v vector(3))").unwrap();
    let no_replica = (
        8200,
        "Unsupported add columnar index: columnar replica must exist to create vector index, columnar index or fulltext index"
            .to_owned(),
    );
    assert_eq!(error_of(&mut session, "alter table t add vector index idx((vec_cosine_distance(v)))"), no_replica);
    assert_eq!(
        error_of(&mut session, "create vector index idx2 on t ((vec_l2_distance(v))) using hnsw"),
        no_replica
    );
    session.run("create table p(a int, v vector(3)) partition by hash(a) partitions 2").unwrap();
    assert_eq!(
        error_of(&mut session, "alter table p add vector index idx((vec_cosine_distance(v)))"),
        (8200, "Unsupported add columnar index: unsupported partition table".to_owned())
    );
    session
        .run("create global temporary table g(a int, v vector(3)) on commit delete rows")
        .unwrap();
    let temporary = (8006, "`columnar index` is unsupported on temporary tables.".to_owned());
    assert_eq!(error_of(&mut session, "alter table g add vector index idx((vec_cosine_distance(v)))"), temporary);
    assert_eq!(
        error_of(&mut session, "create vector index idx on g ((vec_cosine_distance(v))) using hnsw"),
        temporary
    );
    session.run("create table c(a int, v vector(3))").unwrap();
    session.run("alter table c cache").unwrap();
    assert_eq!(
        error_of(&mut session, "alter table c add vector index idx((vec_cosine_distance(v)))"),
        (8242, "'Alter Table' is unsupported on cache tables.".to_owned())
    );
    assert_eq!(
        error_of(&mut session, "create vector index idx on c ((vec_cosine_distance(v))) using hnsw"),
        (8242, "'create columnar index' is unsupported on cache tables.".to_owned())
    );
    // Go `checkFullTextSupportedInStarter`, after the table is found.
    session.run("create table m(c text)").unwrap();
    assert_eq!(
        error_of(&mut session, "create fulltext index fi on m (c)"),
        (8200, "FULLTEXT index is only supported in starter deployment mode".to_owned())
    );
    let missing = (1146, "Table 'test.nosuch' doesn't exist".to_owned());
    assert_eq!(error_of(&mut session, "create fulltext index fi on nosuch (c)"), missing);
    assert_eq!(
        error_of(&mut session, "alter table nosuch add vector index idx((vec_cosine_distance(v)))"),
        missing
    );
    assert_eq!(
        error_of(&mut session, "create vector index idx on nosuch ((vec_cosine_distance(v))) using hnsw"),
        missing
    );
}
