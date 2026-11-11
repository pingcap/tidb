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

struct FailpointGuard(&'static str);

impl Drop for FailpointGuard {
    fn drop(&mut self) {
        fail::remove(self.0);
    }
}

fn enable(name: &'static str, action: &str) -> FailpointGuard {
    fail::cfg(name, action).expect("enable failpoint");
    FailpointGuard(name)
}

fn mock_tiflash_session() -> Session {
    let session = Session::new_with_mock_tiflash(2);
    tidb_domain_infosync::tiflash_manager::set_mock_tiflash(std::sync::Arc::new(
        tidb_domain_infosync::tiflash_manager::MockTiFlash::new(),
    ));
    session
}

fn exec(session: &mut Session, sql: &str) {
    if let Err(error) = session.run(sql) {
        panic!("{sql}: {:?}", error.to_mysql_error());
    }
}

#[track_caller]
fn must_contain_err_msg(session: &mut Session, sql: &str, expected: &str) {
    let (code, message) = error_of(session, sql);
    assert!(message.contains(expected), "{sql}: {code} {message}");
}

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    crate::tests_support::row_text(session.run(sql))
}

/// The last history job as `type|state|row count`.
fn last_job(session: &mut Session) -> String {
    let row = rows(session, "admin show ddl jobs 1").remove(0);
    [3, 11, 7].map(|column| row[column].clone()).join("|")
}

fn index_names(session: &mut Session, table: &str) -> Vec<String> {
    rows(session, &format!("show index from {table}"))
        .into_iter()
        .map(|row| format!("{}|{}", row[2], row[10]))
        .collect()
}

/// Go `TestAddVectorIndexSimple` (`pkg/ddl/index_modify_test.go`).
#[test]
fn add_vector_index_simple_like_go() {
    let mut session = mock_tiflash_session();
    let session = &mut session;
    // test for errors
    // for partition table
    exec(
        session,
        "create table pt(a int, b vector, c int) PARTITION BY RANGE ( a ) (PARTITION p0 VALUES LESS THAN (6), PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (21))",
    );
    must_contain_err_msg(
        session,
        "alter table pt add vector index idx((vec_cosine_distance(b))) USING HNSW;",
        "Unsupported add columnar index: unsupported partition table",
    );
    // for TiFlash replica
    exec(session, "create table t (a int, b vector, c vector(3), d vector(4));");
    must_contain_err_msg(
        session,
        "alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING HNSW COMMENT 'b comment';",
        "columnar replica must exist",
    );
    exec(session, "alter table t set tiflash replica 2 location labels 'a','b';");
    assert_eq!(
        error_of(session, "alter table t add key idx(a) USING HNSW;"),
        (8200, "'USING HNSW' can be only used for VECTOR INDEX".to_owned())
    );
    // for a wrong column
    assert_eq!(
        error_of(session, "alter table t add vector index ((vec_cosine_distance(n))) USING HNSW;"),
        (1054, "Unknown column 'n' in 't'".to_owned())
    );
    // for wrong functions
    assert_eq!(
        error_of(session, "alter table t add vector index ((vec_cosine_distance(a))) USING HNSW;").0,
        8200
    );
    must_contain_err_msg(
        session,
        "alter table t add vector index ((vec_cosine_distance(a,'[1,2.1,3.3]'))) USING HNSW;",
        "Unsupported add vector index: only support vector type, but this is type: int",
    );
    for sql in [
        "alter table t add vector index ((vec_l1_distance(b))) USING HNSW;",
        "alter table t add vector index ((vec_negative_inner_product(b))) USING HNSW;",
        "alter table t add vector index ((lower(b))) USING HNSW;",
    ] {
        assert_eq!(error_of(session, sql).0, 8200, "{sql}");
    }
    // for duplicated index name
    exec(session, "alter table t add key idx(a);");
    assert_eq!(
        error_of(session, "alter table t add vector index idx((vec_cosine_distance(c))) USING HNSW;").0,
        1061
    );
    // for duplicated column
    let _mock = enable("MockCheckColumnarIndexProcess", "return(1)");
    must_contain_err_msg(
        session,
        "alter table t add vector index vecIdx((vec_cosine_distance(b))) USING HNSW;",
        "add vector index can only be defined on fixed-dimension vector columns",
    );
    exec(session, "alter table t add vector index vecIdx((vec_cosine_distance(c))) USING HNSW;");
    assert_eq!(
        error_of(session, "alter table t add vector index vecIdx1((vec_cosine_distance(c))) USING HNSW;").0,
        1061
    );
    exec(session, "alter table t add vector index vecIdx1((vec_cosine_distance(d))) USING HNSW;");
    exec(session, "alter table t add vector index vecIdx2((vec_l2_distance(c))) USING HNSW;");
    exec(session, "alter table t drop index vecIdx2;");
    // for "if not exists"
    exec(session, "alter table t add vector index if not exists idx((vec_l2_distance(c))) USING HNSW;");
    assert_eq!(
        rows(session, "show warnings"),
        [["Note", "1061", "Duplicate key name 'idx'"]]
    );
    assert_eq!(
        error_of(session, "alter table t add vector index if not exists idx((vec_cosine_distance(c))) USING HNSW;"),
        (1061, "vector index vecIdx function vec_cosine_distance already exist on column c".to_owned())
    );

    // normal test cases
    exec(session, "drop table if exists t;");
    exec(session, "create table t (a int, b vector(3));");
    exec(session, "alter table t set tiflash replica 2 location labels 'a','b';");
    exec(session, "insert into t values (1, '[1,2.1,3.3]');");
    assert!(index_names(session, "t").is_empty());
    exec(
        session,
        "alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING HNSW COMMENT 'b comment';",
    );
    assert_eq!(index_names(session, "t"), ["idx|VECTOR"]);
    // test row count
    assert_eq!(last_job(session), "add columnar index|synced|1");
    assert_eq!(rows(session, "select * from t;"), [["1", "[1,2.1,3.3]"]]);
    exec(session, "admin check table t");
    exec(session, "admin check index t idx");
    must_contain_err_msg(
        session,
        "admin cleanup index t idx",
        "columnar index `idx` is not supported for cleanup index",
    );
    assert_eq!(
        rows(session, "show create table t")[0][1],
        "CREATE TABLE `t` (\n  `a` int DEFAULT NULL,\n  `b` vector(3) DEFAULT NULL,\n  VECTOR INDEX `idx`((VEC_COSINE_DISTANCE(`b`))) COMMENT 'b comment'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    // test multi-schema change for unsupported operations
    must_contain_err_msg(
        session,
        "alter table t drop column b;",
        "can't drop column b with Columnar Index covered now",
    );
    must_contain_err_msg(
        session,
        "alter table t add index idx2(a), add vector index idx3((vec_l2_distance(b))) USING HNSW COMMENT 'b comment'",
        "Unsupported multi schema change for add columnar index",
    );

    // test alter index visibility
    assert_eq!(
        error_of(session, "alter table t alter index idx invisible"),
        (8200, "INVISIBLE can not be used in VECTOR INDEX".to_owned())
    );
    assert_eq!(
        rows(
            session,
            "select distinct index_name, is_visible from information_schema.statistics where table_schema = 'test' and table_name = 't' order by index_name"
        ),
        [["idx", "YES"]]
    );
    exec(session, "alter table t alter index idx visible");

    // test modify/change column with a vector index
    assert_eq!(
        error_of(session, "alter table t modify column b vector(2)"),
        (8200, "Unsupported modify column: columnar indexes on the column".to_owned())
    );
    exec(session, "alter table t modify column b vector(3) not null");

    // test rename index
    exec(session, "alter table t rename index idx to vecIdx");
    assert_eq!(index_names(session, "t"), ["vecIdx|VECTOR"]);

    // test drop a vector index
    exec(session, "alter table t drop index vecIdx;");
    assert!(index_names(session, "t").is_empty());
    assert_eq!(rows(session, "select count(*) from mysql.gc_delete_range"), [["0"]]);
    assert_eq!(rows(session, "select * from t;"), [["1", "[1,2.1,3.3]"]]);

    // test create a vector index with same name
    exec(
        session,
        "create vector index idx on t ((VEC_COSINE_DISTANCE(b))) USING HNSW COMMENT 'b comment';",
    );
    assert_eq!(index_names(session, "t"), ["idx|VECTOR"]);
    assert_eq!(
        rows(session, "show create table t")[0][1],
        "CREATE TABLE `t` (\n  `a` int DEFAULT NULL,\n  `b` vector(3) NOT NULL,\n  VECTOR INDEX `idx`((VEC_COSINE_DISTANCE(`b`))) COMMENT 'b comment'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    // test multi-schema change for dropping indexes
    exec(session, "alter table t add index idx2(a)");
    exec(session, "alter table t drop index idx, drop index idx2");
    assert!(index_names(session, "t").is_empty());
    exec(session, "admin check table t");

    // test anonymous index
    exec(session, "alter table t add vector index ((vec_l2_distance(b))) USING HNSW;");
    assert_eq!(index_names(session, "t"), ["vector_index|VECTOR"]);
    exec(session, "alter table t add key vector_index_2(a);");
    exec(session, "alter table t add vector index ((VEC_COSINE_DISTANCE(b))) USING HNSW;");
    assert_eq!(
        index_names(session, "t"),
        ["vector_index|VECTOR", "vector_index_2|BTREE", "vector_index_3|VECTOR"]
    );
}

/// Go `TestAddColumnarIndexSimple` (`pkg/ddl/index_modify_test.go`).
#[test]
fn add_columnar_index_simple_like_go() {
    let mut session = mock_tiflash_session();
    let session = &mut session;
    exec(
        session,
        "create table pt(a int, b vector, c int) PARTITION BY RANGE ( a ) (PARTITION p0 VALUES LESS THAN (6), PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (21))",
    );
    must_contain_err_msg(
        session,
        "alter table pt add columnar index idx(c) USING INVERTED;",
        "Unsupported add columnar index: unsupported partition table",
    );
    // for TiFlash replica
    exec(session, "create table t (a int, b vector(4), c int, d char(4));");
    must_contain_err_msg(
        session,
        "alter table t add columnar index idx(a) USING INVERTED COMMENT 'b comment';",
        "columnar replica must exist",
    );
    exec(session, "alter table t set tiflash replica 2 location labels 'a','b';");
    assert_eq!(
        error_of(session, "alter table t add key idx(d) USING INVERTED;"),
        (8200, "'USING INVERTED' can be only used for COLUMNAR INDEX".to_owned())
    );
    // for a wrong column
    assert_eq!(
        error_of(session, "alter table t add columnar index (n) USING INVERTED;"),
        (1054, "Unknown column 'n' in 't'".to_owned())
    );
    // for wrong data type
    for sql in [
        "alter table t add columnar index (b) USING INVERTED;",
        "alter table t add columnar index (d) USING INVERTED;",
    ] {
        must_contain_err_msg(session, sql, "only support integer type, but this is type");
    }
    // for duplicated index name
    exec(session, "alter table t add key idx(a);");
    assert_eq!(error_of(session, "alter table t add columnar index idx(c) USING INVERTED;").0, 1061);
    // for duplicated column
    let _mock = enable("MockCheckColumnarIndexProcess", "return(1)");
    exec(session, "alter table t add columnar index vecIdx(c) USING INVERTED;");
    assert_eq!(error_of(session, "alter table t add columnar index vecIdx1(c) USING INVERTED;").0, 1061);
    // for "if not exists"
    exec(session, "alter table t drop index vecIdx");
    exec(session, "alter table t add columnar index if not exists idx(c) USING INVERTED;");
    assert_eq!(
        rows(session, "show warnings"),
        [["Note", "1061", "Duplicate key name 'idx'"]]
    );
    assert_eq!(
        error_of(session, "alter table t add columnar index if not exists idx(c) USING INVERTED;"),
        (1061, "inverted columnar index  already exist on column c".to_owned())
    );

    // normal test cases
    exec(session, "drop table if exists t;");
    exec(session, "create table t (a int, b int);");
    exec(session, "alter table t set tiflash replica 2 location labels 'a','b';");
    exec(session, "insert into t values (1, 2);");
    exec(session, "alter table t add columnar index idx(a) USING INVERTED COMMENT 'a comment';");
    assert_eq!(index_names(session, "t"), ["idx|INVERTED"]);
    // test row count
    assert_eq!(last_job(session), "add columnar index|synced|1");
    assert_eq!(rows(session, "select * from t;"), [["1", "2"]]);
    exec(session, "admin check table t");
    exec(session, "admin check index t idx");
    must_contain_err_msg(
        session,
        "admin cleanup index t idx",
        "columnar index `idx` is not supported for cleanup index",
    );
    assert_eq!(
        rows(session, "show create table t")[0][1],
        "CREATE TABLE `t` (\n  `a` int DEFAULT NULL,\n  `b` int DEFAULT NULL,\n  COLUMNAR INDEX `idx`(`a`) USING INVERTED COMMENT 'a comment'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
    must_contain_err_msg(
        session,
        "alter table t drop column a;",
        "can't drop column a with Columnar Index covered now",
    );
    must_contain_err_msg(
        session,
        "alter table t add index idx2(a), add columnar index idx3(b) USING INVERTED COMMENT 'b comment'",
        "Unsupported multi schema change for add columnar index",
    );

    // test alter index visibility
    assert_eq!(
        error_of(session, "alter table t alter index idx invisible"),
        (8200, "INVISIBLE can not be used in INVERTED INDEX".to_owned())
    );
    assert_eq!(
        rows(
            session,
            "select distinct index_name, is_visible from information_schema.statistics where table_schema = 'test' and table_name = 't' order by index_name"
        ),
        [["idx", "YES"]]
    );
    exec(session, "alter table t alter index idx visible");

    // test modify/change column with a columnar index
    assert_eq!(
        error_of(session, "alter table t modify column a smallint"),
        (8200, "Unsupported modify column: columnar indexes on the column".to_owned())
    );
    exec(session, "alter table t modify column a int not null");

    // test rename index
    exec(session, "alter table t rename index idx to colIdx");
    assert_eq!(index_names(session, "t"), ["colIdx|INVERTED"]);

    // test drop a columnar index
    exec(session, "alter table t drop index colIdx;");
    assert!(index_names(session, "t").is_empty());
    assert_eq!(rows(session, "select count(*) from mysql.gc_delete_range"), [["0"]]);
    assert_eq!(rows(session, "select * from t;"), [["1", "2"]]);
    let bare = "CREATE TABLE `t` (\n  `a` int NOT NULL,\n  `b` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";
    assert_eq!(rows(session, "show create table t")[0][1], bare);

    // test create a columnar index with same name
    exec(session, "create columnar index idx on t (b) USING INVERTED COMMENT 'b comment';");
    assert_eq!(index_names(session, "t"), ["idx|INVERTED"]);
    assert_eq!(rows(session, "select * from t;"), [["1", "2"]]);
    assert_eq!(
        rows(session, "show create table t")[0][1],
        "CREATE TABLE `t` (\n  `a` int NOT NULL,\n  `b` int DEFAULT NULL,\n  COLUMNAR INDEX `idx`(`b`) USING INVERTED COMMENT 'b comment'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    // test multi-schema change for dropping indexes
    exec(session, "alter table t add index idx2(a)");
    exec(session, "alter table t drop index idx, drop index idx2");
    assert_eq!(rows(session, "show create table t")[0][1], bare);
    assert_eq!(rows(session, "select * from t;"), [["1", "2"]]);
    exec(session, "admin check table t");

    // test anonymous index
    exec(session, "alter table t add columnar index (b) USING INVERTED;");
    assert_eq!(index_names(session, "t"), ["b|INVERTED"]);
    exec(session, "alter table t add key a(a);");
    exec(session, "alter table t add columnar index (a) USING INVERTED;");
    assert_eq!(index_names(session, "t"), ["b|INVERTED", "a|BTREE", "a_2|INVERTED"]);
}

/// Go `testAddColumnarIndexRollback` (`pkg/ddl/index_modify_test.go`): no
/// mock TiFlash, a job cancelled at its second `WriteReorganization` step
/// (Go hooks `afterWaitSchemaSynced`; the step's other end,
/// `beforeRunOneJobStep`, is the hook here), a failed build and a finished
/// one.
#[test]
fn add_vector_index_rollback_like_go() {
    let mut session = Session::new_with_mock_tiflash(2);
    let session = &mut session;
    exec(session, "create table t1 (c1 int, b vector, c vector(3), unique key(c1));");
    exec(session, "alter table t1 set tiflash replica 2 location labels 'a','b';");
    exec(
        session,
        "insert into t1 values (1, '[1,6.6]', '[1,8.88,9.99]'), (2, '[2,6.6]', '[2,8.88,9.99]'), (3, '[3,6.6]', '[3,8.88,9.99]'), (4, '[4,6.6]', '[4,8.88,9.99]')",
    );
    exec(session, "set global tidb_ddl_error_count_limit = 5");
    let add = "alter table t1 add vector index v_idx((VEC_COSINE_DISTANCE(c))) USING HNSW";
    // Case1: add a vector index but tiflash is not accessible.
    assert_eq!(
        error_of(session, add),
        (1105, "DDL job rollback, error msg: MockTiFlash is not accessible".to_owned())
    );
    assert_eq!(last_job(session), "add columnar index|rollback done|0");
    assert_eq!(index_names(session, "t1"), ["c1|BTREE"]);

    // Case2: do 'admin cancel ddl job to rollback job.
    tidb_domain_infosync::tiflash_manager::set_mock_tiflash(std::sync::Arc::new(
        tidb_domain_infosync::tiflash_manager::MockTiFlash::new(),
    ));
    {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex};
        let _mock = enable("MockCheckColumnarIndexProcess", "return(0)");
        let domain = crate::domain::mock_store::domain_of(session).expect("a mock store session");
        let times = AtomicUsize::new(1);
        let cancelled = Arc::new(Mutex::new(Vec::new()));
        let seen = cancelled.clone();
        tidb_ddl::failpoint_call::enable_call(
            "beforeRunOneJobStep",
            Arc::new(move |job: &mut dyn std::any::Any| {
                let job = job.downcast_ref::<tidb_model::Job>().expect("the step's job");
                if job.schema_state != tidb_model::SchemaState::WRITE_REORGANIZATION {
                    return;
                }
                if times.fetch_add(1, Ordering::SeqCst) == 2 {
                    let mut tk1 = domain.new_session().expect("a second session");
                    let result = rows(&mut tk1, &format!("admin cancel ddl jobs {}", job.id));
                    seen.lock().unwrap().push(result[0][1].clone());
                }
            }),
        );
        let error = error_of(session, add);
        tidb_ddl::failpoint_call::disable("beforeRunOneJobStep");
        assert_eq!(error, (8214, "Cancelled DDL job".to_owned()));
        assert_eq!(*cancelled.lock().unwrap(), ["successful"]);
        assert_eq!(rows(session, "select count(1) from t1;"), [["4"]]);
        assert_eq!(last_job(session), "add columnar index|rollback done|0");
        assert_eq!(index_names(session, "t1"), ["c1|BTREE"]);
    }

    // Case3: add a vector index normally, but tiflash is failed to backfill.
    {
        let _mock = enable("MockCheckColumnarIndexProcess", "return(-1)");
        assert_eq!(
            error_of(session, add),
            (9014, "TiFlash backfill index failed: mock a check error".to_owned())
        );
        assert_eq!(last_job(session), "add columnar index|rollback done|0");
        assert_eq!(index_names(session, "t1"), ["c1|BTREE"]);
    }

    // Case4: add a vector index normally, tiflash is backfilling.
    let _mock = enable("MockCheckColumnarIndexProcess", "return(4)");
    exec(session, add);
    assert_eq!(last_job(session), "add columnar index|synced|4");
    assert_eq!(index_names(session, "t1"), ["c1|BTREE", "v_idx|VECTOR"]);
}

/// The stored entry count of one index of `test.<table>`.
fn index_entry_count(session: &mut Session, table: &str, index: &str) -> usize {
    let mut store = session.transaction_store().expect("a node session has a store");
    session
        .with_committed_catalog_mut(|catalog| {
            let Some(tidb_infoschema::TableEntry::Kv(stored)) = catalog.table_mut_in("test", table)
            else {
                panic!("{table} is not stored as bytes");
            };
            let stored = std::sync::Arc::make_mut(stored);
            let index = stored
                .index_list_for_check()
                .into_iter()
                .find(|candidate| candidate.read().name.original().eq_ignore_ascii_case(index))
                .expect("the index exists");
            let id = index.read().id;
            Ok(stored.index_entries_for_check(&mut *store, id).expect("entries readable").len())
        })
        .unwrap()
}

/// Go's `addIndices`, `rebuildUpdateRecordIndices` and `removeRowIndices`
/// (`pkg/table/tables/tables.go`) skip `IsColumnarIndex`: a row written after
/// the index exists files entries for the row-store index only.
#[test]
fn a_row_write_files_no_columnar_index_entry_like_go() {
    let mut session = mock_tiflash_session();
    let session = &mut session;
    exec(session, "create table t (a int, b vector(3), c int, key k(c));");
    exec(session, "alter table t set tiflash replica 2 location labels 'a','b';");
    let _mock = enable("MockCheckColumnarIndexProcess", "return(1)");
    exec(session, "alter table t add vector index vi((vec_cosine_distance(b))) USING HNSW;");
    exec(session, "alter table t add columnar index ci(a) USING INVERTED;");
    exec(session, "insert into t values (1, '[1,2,3]', 1), (2, '[4,5,6]', 2), (3, null, 3);");
    exec(session, "update t set a = a + 10, b = '[7,8,9]', c = c + 10 where c < 3;");
    exec(session, "delete from t where c = 3;");
    assert_eq!(index_entry_count(session, "t", "k"), 2);
    assert_eq!(index_entry_count(session, "t", "vi"), 0);
    assert_eq!(index_entry_count(session, "t", "ci"), 0);
    exec(session, "admin check table t;");
    assert_eq!(rows(session, "select a, c from t order by a"), [["11", "11"], ["12", "12"]]);
}
