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

//! `tidb_max_keys_read` and `tidb_keys_examined`, as
//! `executor/max_keys_read.test` records them: every coprocessor read of a
//! SELECT adds its processed keys to one statement counter, which fails the
//! statement with 8274 once past the limit and joins the session's
//! `tidb_keys_examined`.

use crate::tests_support::row_text;
use crate::Session;

fn session_with_rows() -> Session {
    let mut session = Session::new();
    session
        .run("create table t_mrs (id int primary key auto_increment, val int, extra int)")
        .unwrap();
    let values = (1..=20)
        .map(|value| format!("({value}, 100)"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("insert into t_mrs (val, extra) values {values}"))
        .unwrap();
    session
}

fn error_code(session: &mut Session, sql: &str) -> u16 {
    session.run(sql).unwrap_err().to_mysql_error().code
}

fn keys_examined(session: &mut Session) -> String {
    row_text(session.run("show status like 'tidb_keys_examined'"))[0][1].clone()
}

#[test]
fn a_select_past_the_limit_fails_and_dml_is_exempt() {
    let mut session = session_with_rows();
    session
        .run("set @@session.tidb_max_keys_read = 100")
        .unwrap();
    assert_eq!(
        row_text(session.run("select count(*) from t_mrs")),
        [["20"]]
    );
    assert_eq!(
        error_code(
            &mut session,
            "select /*+ SET_VAR(tidb_max_keys_read=2) */ * from t_mrs"
        ),
        8274
    );
    // The aggregation counts the keys it scanned, not the rows it returns.
    assert_eq!(
        error_code(
            &mut session,
            "select /*+ SET_VAR(tidb_max_keys_read=5) */ count(*) from t_mrs"
        ),
        8274
    );
    session.run("create index idx_val on t_mrs (val)").unwrap();
    // The index side and the table side share one budget: 20 + 20 > 25.
    assert_eq!(
        error_code(
            &mut session,
            "select /*+ SET_VAR(tidb_max_keys_read=25), USE_INDEX(t_mrs, idx_val) */ extra from t_mrs where val >= 1"
        ),
        8274
    );
    assert_eq!(
        row_text(session.run(
            "select /*+ SET_VAR(tidb_max_keys_read=50), USE_INDEX(t_mrs, idx_val) */ sum(extra) from t_mrs where val >= 1"
        )),
        [["2000"]]
    );
    session.run("set @@session.tidb_max_keys_read = 1").unwrap();
    session.run("update t_mrs set val = val + 1").unwrap();
    session.run("delete from t_mrs where id = 20").unwrap();
}

#[test]
fn keys_examined_accumulates_until_flush_status() {
    let mut session = session_with_rows();
    session.run("flush status").unwrap();
    session.run("select count(*) from t_mrs").unwrap();
    assert_eq!(keys_examined(&mut session), "20");
    session.run("select count(*) from t_mrs").unwrap();
    assert_eq!(keys_examined(&mut session), "40");
    session.run("flush status").unwrap();
    assert_eq!(keys_examined(&mut session), "0");
    // A covering IndexReader reads only its index entries.
    session.run("create index idx_val on t_mrs (val)").unwrap();
    session.run("flush status").unwrap();
    session.run("select count(*) from t_mrs").unwrap();
    assert_eq!(keys_examined(&mut session), "20");
    assert!(row_text(session.run("show global status like 'tidb_keys_examined'")).is_empty());
}
