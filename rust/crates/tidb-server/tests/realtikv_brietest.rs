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

//! Go `tests/realtikvtest/brietest`, over [`crate::realtikvtest`]:
//! `backup_restore_test.go` and `brie_test.go`. Go resets the BRIE queue
//! (`ResetGlobalBRIEQueueForTest`) so a test's jobs number from 1; each test
//! here runs in its own process, where the queue starts empty.
//!
//! The backups go to a `local://` directory, which TiKV writes: the cluster
//! must run on this machine, as a playground does.

use std::time::{Duration, Instant};

/// Go `makeTempDirForBackup`: a fresh directory under `BRIETEST_TMPDIR` or
/// the system's temporary directory, removed when the test ends.
fn make_temp_dir_for_backup() -> tempfile::TempDir {
    let base = std::env::var_os("BRIETEST_TMPDIR").map_or_else(std::env::temp_dir, std::path::PathBuf::from);
    std::fs::create_dir_all(&base).unwrap();
    tempfile::Builder::new().prefix("briesql-").tempdir_in(base).unwrap()
}

/// The directory as a SQL string literal's body.
fn sql_path(dir: &tempfile::TempDir) -> String {
    dir.path().display().to_string().replace('\'', "''")
}

/// Go `require.Eventually(check, timeout, tick)`.
fn eventually(timeout: Duration, tick: Duration, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    loop {
        if check() {
            return;
        }
        assert!(Instant::now() < deadline, "condition never satisfied");
        std::thread::sleep(tick);
    }
}

/// Five tables of 100 rows each in `test`.
fn create_five_tables(tk: &mut crate::realtikvtest::Tk) {
    tk.must_exec("use test;");
    for i in 0..5 {
        tk.must_exec(&format!("create table foo{i}(pk int primary key auto_increment, v varchar(255));"));
        insert_100(tk, i);
    }
}

fn insert_100(tk: &mut crate::realtikvtest::Tk, i: usize) {
    let values = vec!["('hello, world')"; 100].join(",");
    tk.must_exec(&format!("insert into foo{i}(v) values {values};"));
}

fn drop_five_tables(tk: &mut crate::realtikvtest::Tk) {
    for i in 0..5 {
        tk.must_exec(&format!("drop table foo{i};"));
    }
}

/// Go `TestBackupAndRestore`.
#[test]
fn test_backup_and_restore() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists br");
    tk.must_exec("drop database if exists br02");
    tk.must_exec("create database if not exists br");
    tk.must_exec("use br");
    tk.must_exec("create table t1(v int)");
    tk.must_exec("insert into t1 values (1)");
    tk.must_exec("insert into t1 values (2)");
    tk.must_exec("insert into t1 values (3)");
    tk.check("select count(*) from t1", &["3"]);

    tk.must_exec("create database if not exists br02");
    tk.must_exec("use br02");
    tk.must_exec("create table t1(v int)");

    let tmp = make_temp_dir_for_backup();
    let dir = format!("{}/bk1", sql_path(&tmp));
    tk.must_query(&format!("backup database br to 'local://{dir}'"));

    tk.must_exec("drop database br");
    tk.must_exec("drop database br02");

    tk.must_query(&format!("restore database * from 'local://{dir}'"));
    tk.must_exec("use br");
    tk.check("select count(*) from t1", &["3"]);
    tk.must_exec("drop database br");
}

/// Go `TestRestoreMultiTables`: a restore creates the tables in
/// `create tables` jobs, which together name every table once.
#[test]
fn test_restore_multi_tables() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists br");
    tk.must_exec("create database if not exists br");
    tk.must_exec("use br");
    let table_num = 100;
    let mut names = std::collections::BTreeSet::new();
    for i in 0..table_num {
        tk.must_exec(&format!("create table table_{i} (a int primary key, b json, c varchar(20))"));
        tk.must_exec(&format!(r#"insert into table_{i} values (1, '{{"a": 1, "b": 2}}', '123')"#));
        tk.check(&format!("select count(*) from table_{i}"), &["1"]);
        names.insert(format!("table_{i}"));
    }

    let tmp = make_temp_dir_for_backup();
    let dir = format!("{}/bk1", sql_path(&tmp));
    tk.must_query(&format!("backup database br to 'local://{dir}'"));
    tk.must_exec("drop database br");
    tk.must_query(&format!("restore database * from 'local://{dir}'"));
    tk.must_exec("use br");
    let mut count = 0;
    for row in tk.must_query("admin show ddl jobs where JOB_TYPE = 'create tables'") {
        let tables = &row[2];
        assert_ne!(tables, "");
        for table in tables.split(',') {
            assert!(names.contains(table), "{table} is not a restored table");
            count += 1;
        }
    }
    assert_eq!(count, table_num);
    for i in 0..table_num {
        tk.check(&format!("select count(*) from table_{i}"), &["1"]);
    }
    tk.must_exec("drop database br");
}

/// Go `TestShowBackupQuery`.
#[test]
fn test_show_backup_query() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let tmp = make_temp_dir_for_backup();
    let sql_tmp = sql_path(&tmp);
    tk.must_exec("use test;");
    tk.must_exec("drop table if exists foo;");
    tk.must_exec("create table foo(pk int primary key auto_increment, v varchar(255));");
    let values = vec!["('hello, world')"; 100].join(",");
    tk.must_exec(&format!("insert into foo(v) values {values};"));
    let backup_query = format!("BACKUP DATABASE * TO 'local://{sql_tmp}'");
    tk.must_query(&backup_query);
    // Go: "we assume a auto-increamental ID here".
    let rows = tk.must_query("show br job query 1;");
    assert!(rows.iter().flatten().any(|cell| cell.contains(&backup_query)), "{rows:?}");

    tk.must_exec("drop table foo;");
    let restore_query = format!("RESTORE TABLE `test`.`foo` FROM 'local://{sql_tmp}'");
    tk.must_query(&restore_query);
    let rows = tk.must_query("show br job query 2;");
    tk.must_exec("drop table foo;");
    assert!(rows.iter().flatten().any(|cell| cell.contains(&restore_query)), "{rows:?}");
}

/// Go `TestShowBackupQueryRedact`: a running job's query shows its storage
/// URL without the credentials.
#[test]
fn test_show_backup_query_redact() {
    let _block = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/block-on-brie", "return");
    let store = realtikv!();
    let mut tk = store.new_tk();
    let mut background = store.new_tk();
    std::thread::scope(|scope| {
        let backup = scope.spawn(move || {
            background.exec_to_err(
                "backup database * to 's3://nonexist/real?endpoint=http://127.0.0.1&access-key=notleaked&secret-access-key=notleaked'",
            )
        });
        eventually(Duration::from_secs(5), Duration::from_secs(1), || {
            let rows = tk.must_query("show br job query 1;");
            let Some(item) = rows.first().map(|row| row[0].clone()) else {
                return false;
            };
            assert!(!item.contains("secret-access-key"), "The secret key not redacted: {item:?}");
            assert!(item.contains("BACKUP DATABASE * TO 's3://nonexist/real'"), "{item}");
            true
        });
        tk.must_exec("cancel br job 1;");
        // The background job returns.
        backup.join().unwrap();
    });
}

/// Go `TestCancel`: `CANCEL BR JOB` of a queued job warns nothing and the
/// job ends with an error.
#[test]
fn test_cancel() {
    let _block = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/block-on-brie", "return");
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test;");
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let mut background = store.new_tk();
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let err = background.exec("backup database * to 'noop://'");
            assert!(err.is_err(), "{err:?}");
            done_tx.send(()).unwrap();
        });
        // Go compares the statement's warning count before and after the
        // cancel: a job not yet registered makes `CANCEL BR JOB` warn.
        eventually(Duration::from_secs(5), Duration::from_secs(1), || {
            tk.must_exec("cancel br job 1;");
            tk.must_query("show warnings").is_empty()
        });
        assert!(
            done_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "the backup job doesn't be canceled"
        );
    });
}

/// Go `TestExistedTables`: restoring over existing tables fails with "table
/// already exists", for a full and a database backup.
#[test]
fn test_existed_tables() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let tmp = make_temp_dir_for_backup();
    let sql_tmp = sql_path(&tmp);
    tk.must_exec("drop database if exists test;");
    tk.must_exec("create database test;");
    create_five_tables(&mut tk);

    let within = |what: &str, started: Instant| {
        assert!(started.elapsed() < Duration::from_secs(20), "{what} operation exceeded");
    };
    let started = Instant::now();
    tk.must_query(&format!("BACKUP DATABASE * TO 'local://{sql_tmp}/full'"));
    within("Backup", started);
    let started = Instant::now();
    let err = tk.exec_to_err(&format!("RESTORE DATABASE * FROM 'local://{sql_tmp}/full'"));
    within("Restore", started);
    assert!(err.contains("table already exists"), "{err}");

    let started = Instant::now();
    tk.must_query(&format!("BACKUP DATABASE test TO 'local://{sql_tmp}/db'"));
    within("Backup", started);
    let started = Instant::now();
    let err = tk.exec_to_err(&format!("Restore DATABASE test FROM 'local://{sql_tmp}/db'"));
    within("Restore", started);
    assert!(err.contains("table already exists"), "{err}");

    drop_five_tables(&mut tk);
}

/// Go `TestExistedTablesOfIncremental*`: a full backup, rows written, an
/// incremental backup from the full one's BackupTS, the tables dropped, then
/// the full and the incremental restore.
fn existed_tables_of_incremental(backup_scope: &str, restore_scope: &str) {
    let store = realtikv!();
    let mut tk = store.new_tk();
    let tmp = make_temp_dir_for_backup();
    let sql_tmp = sql_path(&tmp);
    tk.must_exec("drop database if exists test;");
    tk.must_exec("create database test;");
    create_five_tables(&mut tk);

    let rows = tk.must_query(&format!("BACKUP DATABASE {backup_scope} TO 'local://{sql_tmp}/full'"));
    let backup_ts = rows[0][2].clone();
    for i in 0..5 {
        insert_100(&mut tk, i);
    }
    tk.must_query(&format!(
        "BACKUP DATABASE {backup_scope} TO 'local://{sql_tmp}/incremental' last_backup={backup_ts}"
    ));
    drop_five_tables(&mut tk);

    tk.must_query(&format!("RESTORE DATABASE {restore_scope} FROM 'local://{sql_tmp}/full'"));
    tk.must_query(&format!("RESTORE DATABASE {restore_scope} FROM 'local://{sql_tmp}/incremental'"));
    // The incremental restore brings the rows written after the full backup.
    for i in 0..5 {
        tk.check(&format!("select count(*) from test.foo{i}"), &["200"]);
    }
    drop_five_tables(&mut tk);
}

/// Go `TestExistedTablesOfIncremental`: full `*`, incremental `*`, restore
/// `*` twice.
#[test]
fn test_existed_tables_of_incremental() {
    existed_tables_of_incremental("*", "*");
}

/// Go `TestExistedTablesOfIncremental_1`: backups of `*`, restores of `test`.
#[test]
fn test_existed_tables_of_incremental_1() {
    existed_tables_of_incremental("*", "test");
}

/// Go `TestExistedTablesOfIncremental_2`: backups of `test`, restores of `*`.
#[test]
fn test_existed_tables_of_incremental_2() {
    existed_tables_of_incremental("test", "*");
}
