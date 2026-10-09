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

//! Go `pkg/session/nontransactional.go`, as `session/nontransactional.test`
//! records it: `BATCH ... LIMIT n` splits a DML into jobs over shard-column
//! ranges. The statement was refused before.

use crate::tests_support::row_text;
use crate::Session;

fn session_with_rows(rows: i64) -> Session {
    let mut session = Session::new();
    session.run("create table t(a int, b int, key(a))").unwrap();
    for a in 0..rows {
        session
            .run(&format!("insert into t values ({a}, {})", a * 2))
            .unwrap();
    }
    session
}

fn error_of(session: &mut Session, sql: &str) -> String {
    session.run(sql).unwrap_err().to_string()
}

/// One job per `LIMIT` shard values; every job is its own statement.
#[test]
fn batch_dml_runs_one_statement_per_shard_range() {
    let mut session = session_with_rows(10);
    session
        .run("create table t1(a int, b int, unique key(a))")
        .unwrap();
    assert_eq!(
        row_text(session.run("batch on a limit 3 insert into t1 select * from t")),
        vec![vec!["4", "all succeeded"]]
    );
    assert_eq!(
        row_text(session.run("select count(*), sum(b) from t1")),
        vec![vec!["10", "90"]]
    );
    assert_eq!(
        row_text(session.run("batch on a limit 3 update t set b = b + 1 where a < 5")),
        vec![vec!["2", "all succeeded"]]
    );
    assert_eq!(
        row_text(session.run("batch on a limit 4 delete from t where b > 5")),
        vec![vec!["2", "all succeeded"]]
    );
    assert_eq!(
        row_text(session.run("select a, b from t order by a")),
        vec![vec!["0", "1"], vec!["1", "3"], vec!["2", "5"]]
    );
}

/// `DRY RUN` shows the first and last split statements, `DRY RUN QUERY` the
/// shard SELECT, both restored with the schema Go's preprocessor fills in.
#[test]
fn dry_run_shows_the_split_statements_and_the_shard_query() {
    let mut session = session_with_rows(10);
    assert_eq!(
        row_text(session.run("batch on a limit 3 dry run update t set b = b + 42")),
        vec![
            vec!["UPDATE `test`.`t` SET `b`=(`b` + 42) WHERE `a` BETWEEN 0 AND 2"],
            vec!["UPDATE `test`.`t` SET `b`=(`b` + 42) WHERE `a` BETWEEN 9 AND 9"],
        ]
    );
    assert_eq!(
        row_text(session.run("batch on a limit 3 dry run query delete from t where b > 1")),
        vec![vec![
            "SELECT `a` FROM `test`.`t` WHERE (`b` > 1) ORDER BY IF(ISNULL(`a`),0,1),`a`"
        ]]
    );
    // Without `ON`, a heap table shards on its `_tidb_rowid`, qualified as
    // the table is written.
    assert_eq!(
        row_text(session.run("batch limit 5 dry run delete from t")),
        vec![
            vec!["DELETE FROM `test`.`t` WHERE `test`.`t`.`_tidb_rowid` BETWEEN 1 AND 5"],
            vec!["DELETE FROM `test`.`t` WHERE `test`.`t`.`_tidb_rowid` BETWEEN 6 AND 10"],
        ]
    );
}

/// NULL shard values sort first and join the first job's range.
#[test]
fn null_shard_values_are_read_by_the_first_job() {
    let mut session = session_with_rows(3);
    session.run("insert into t values (null, 7)").unwrap();
    assert_eq!(
        row_text(session.run("batch on a limit 2 dry run delete from t")),
        vec![
            vec!["DELETE FROM `test`.`t` WHERE ((`a` <= 0) OR `a` IS NULL)"],
            vec!["DELETE FROM `test`.`t` WHERE `a` BETWEEN 1 AND 2"],
        ]
    );
    assert_eq!(
        row_text(session.run("batch on a limit 2 delete from t")),
        vec![vec!["2", "all succeeded"]]
    );
    assert_eq!(
        row_text(session.run("select count(*) from t")),
        vec![vec!["0"]]
    );
}

/// Go `checkConstraint`, `selectShardColumn` and `checkUpdateShardColumn`.
#[test]
fn batch_dml_refuses_what_go_refuses() {
    let mut session = session_with_rows(3);
    assert_eq!(
        error_of(&mut session, "batch on b limit 1 delete from t"),
        "Non-transactional DML, shard column b is not indexed"
    );
    assert_eq!(
        error_of(&mut session, "batch on a limit 1 update t set a = a + 1"),
        "Non-transactional DML, shard column cannot be updated"
    );
    assert_eq!(
        error_of(&mut session, "batch on a limit 1 delete from t order by a"),
        "Non-transactional statements don't support order by"
    );
    session.run("begin").unwrap();
    assert_eq!(
        error_of(&mut session, "batch on a limit 1 delete from t"),
        "non-transactional DML can only run in auto-commit mode. auto-commit:true, inTxn:true"
    );
    session.run("rollback").unwrap();
    session
        .run("create table c(a int, b int, c int, primary key(a, b) clustered, key(c))")
        .unwrap();
    assert_eq!(
        error_of(&mut session, "batch limit 1 delete from c"),
        "Non-transactional DML, the clustered index contains multiple columns. Please specify a shard column"
    );
}

/// Go applies `@@tidb_read_staleness` to a SELECT only; on this store, whose
/// min safe ts is current, the read is the current read. Writes and DDL
/// under it had been refused.
#[test]
fn read_staleness_leaves_writes_and_current_reads_alone() {
    let mut session = Session::new();
    session.run("set @@tidb_read_staleness = -100").unwrap();
    session.run("create table t(a int, b int, key(a))").unwrap();
    session.run("insert into t values (1, 2), (3, 4)").unwrap();
    assert_eq!(
        row_text(session.run("batch on a limit 1 update t set b = b + 1")),
        vec![vec!["2", "all succeeded"]]
    );
    assert_eq!(
        row_text(session.run("select a, b from t order by a")),
        vec![vec!["1", "3"], vec!["3", "5"]]
    );
}

/// Go `FindFieldName` matches `db.alias.col` through a table alias, in an
/// UPDATE's SET list as in its WHERE.
#[test]
fn an_update_assignment_names_a_column_by_database_and_alias() {
    let mut session = Session::new();
    session.run("create table t(id int, v int)").unwrap();
    session.run("insert into t values (1, 1)").unwrap();
    session
        .run("update t as t1 set v = test.t1.id + 10")
        .unwrap();
    assert_eq!(row_text(session.run("select v from t")), vec![vec!["11"]]);
    assert!(session.run("update t as t1 set v = other.t1.id").is_err());
}
