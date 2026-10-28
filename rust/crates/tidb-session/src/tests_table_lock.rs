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

//! Go's `LOCK TABLES` between two sessions of one domain, as the
//! integration recordings capture it under `enable-table-lock = true`
//! (ddl/db_table, executor/executor, executor/foreign_key).

use tidb_config::config_tree::config::{get_global_config, store_global_config, update_global};

use crate::Session;

#[test]
fn lock_tables_follow_go() {
    let previous = get_global_config();
    update_global(|config| config.enable_table_lock = true);
    let mut s1 = Session::new();
    s1.set_connection_id(1);
    let mut s2 = Session::with_catalog(crate::SharedCatalog::clone(&s1.catalog));
    s2.set_connection_id(2);
    let run = |session: &mut Session, sql: &str| {
        session
            .run(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    };
    let error = |session: &mut Session, sql: &str| {
        let error = session.run(sql).expect_err(sql).to_mysql_error();
        (error.code, error.message)
    };
    run(&mut s1, "create table t1 (id int primary key, c int)");
    run(
        &mut s1,
        "create table t2 (id int key, foreign key fk (id) references t1(id) on delete cascade)",
    );
    run(&mut s1, "insert into t1 values (1, 1), (2, 2)");
    run(&mut s1, "insert into t2 values (1), (2)");

    run(&mut s1, "lock table t1 write");
    run(&mut s1, "update t1 set c = 3 where id = 1");
    let (code, message) = error(&mut s2, "select * from t1");
    assert_eq!(code, 8020);
    assert!(
        message.starts_with("Table 't1' was locked in WRITE by server: ")
            && message.ends_with("_session: 1"),
        "{message}"
    );
    // A table nobody locked stays open (lock.Checker returns nil when
    // `Lock` is nil), but a session holding locks creates nothing.
    run(&mut s1, "select * from t2");
    assert_eq!(
        error(&mut s1, "create table t3 (a int)"),
        (
            1100,
            "Table 't3' was not locked with LOCK TABLES".to_owned()
        )
    );

    // Re-locking releases the session's earlier locks first.
    run(&mut s1, "lock table t1 read");
    assert_eq!(
        error(&mut s1, "update t1 set c = 4 where id = 1"),
        (
            1099,
            "Table 't1' was locked with a READ lock and can't be updated".to_owned()
        )
    );
    run(&mut s2, "select * from t1");
    assert_eq!(error(&mut s2, "lock table t1 write").0, 8020);
    run(&mut s2, "lock table t1 read");
    run(&mut s2, "unlock tables");
    assert_eq!(
        error(&mut s1, "lock table t1 read, t1 write"),
        (1066, "Not unique table/alias: 't1'".to_owned())
    );

    // A cascade checks the child table's locks (OptimizeForForeignKeyCascade).
    run(&mut s1, "unlock tables");
    run(&mut s1, "lock table t2 read");
    let (code, message) = error(&mut s2, "delete from t1 where id = 2");
    assert_eq!(code, 8020);
    assert!(
        message.starts_with("Table 't2' was locked in READ by server: "),
        "{message}"
    );

    // Go `CleanupTableLock` drops every holder's lock, and only the cleaning
    // session's own map loses the table.
    run(&mut s1, "unlock tables");
    run(&mut s1, "lock table t2 write");
    run(&mut s2, "admin cleanup table lock t2");
    run(&mut s2, "select * from t2");
    run(&mut s1, "unlock tables");

    // Go's session.Close releases the session's locks.
    run(&mut s1, "lock table t2 read");
    drop(s1);
    run(&mut s2, "delete from t1 where id = 2");
    run(&mut s2, "drop table t2");
    run(&mut s2, "lock tables t1 write");
    // TRUNCATE moves the lock to the new table; DROP releases it.
    run(&mut s2, "truncate table t1");
    run(&mut s2, "insert into t1 values (5, 5)");
    run(&mut s2, "drop table t1");
    run(&mut s2, "create table t4 (a int)");
    store_global_config((*previous).clone());
}
