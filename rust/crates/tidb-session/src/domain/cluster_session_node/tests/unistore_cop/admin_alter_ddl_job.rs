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

//! Go `pkg/ddl/db_test.go`'s `TestAdminAlterDDLJob*`: `ADMIN ALTER DDL JOBS`
//! over mock rows of `mysql.tidb_ddl_job`.

use super::*;
use tidb_model::{ActionType, DDLReorgMeta, GoShared, Job};

/// Go `insertMockJob2Table`.
fn insert_mock_job_2_table(tk: &mut Session, job: &mut Job) {
    let b = job.encode(false).unwrap();
    rows(
        tk,
        &format!(
            "insert into mysql.tidb_ddl_job(job_id, job_meta) values({}, x'{}');",
            job.id,
            b.iter().map(|byte| format!("{byte:02x}")).collect::<String>()
        ),
    );
}

/// Go `getJobMetaByID`.
fn get_job_meta_by_id(tk: &mut Session, job_id: i64) -> Job {
    let res = rows(tk, &format!("select job_meta from mysql.tidb_ddl_job where job_id = {job_id}"));
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].len(), 1);
    let mut job = Job::default();
    job.decode(res[0][0].go_bytes()).unwrap();
    job
}

/// Go `deleteJobMetaByID`.
fn delete_job_meta_by_id(tk: &mut Session, job_id: i64) {
    rows(tk, &format!("delete from mysql.tidb_ddl_job where job_id = {job_id}"));
}

fn reorg_meta(concurrency: i64, batch_size: i64) -> DDLReorgMeta {
    let meta = DDLReorgMeta::default();
    meta.set_concurrency(concurrency);
    meta.set_batch_size(batch_size);
    meta
}

fn defaults() -> tidb_model::DDLReorgProcessDefaults {
    tidb_model::DDLReorgProcessDefaults::new(|| 4, || 256)
}

/// Go `TestAdminAlterDDLJobUpdateSysTable`.
#[test]
fn test_admin_alter_ddl_job_update_sys_table() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(2950)).unwrap();
    rows(&mut tk, "use test");
    rows(&mut tk, "create table t (a int);");
    for use_cloud_storage in [true, false] {
        let mut meta = reorg_meta(4, 128);
        meta.use_cloud_storage = use_cloud_storage;
        let mut job = Job::default();
    job.id = 1;
    job.type_ = ActionType::ACTION_ADD_INDEX;
        job.reorg_meta = Some(GoShared::new(meta));
        insert_mock_job_2_table(&mut tk, &mut job);
        rows(&mut tk, &format!("admin alter ddl jobs {} thread = 8;", job.id));
        let j = get_job_meta_by_id(&mut tk, job.id);
        assert_eq!(j.reorg_meta.as_ref().unwrap().read().get_concurrency(defaults()), 8);

        rows(&mut tk, &format!("admin alter ddl jobs {} batch_size = 256;", job.id));
        let j = get_job_meta_by_id(&mut tk, job.id);
        assert_eq!(j.reorg_meta.as_ref().unwrap().read().get_batch_size(defaults()), 256);

        rows(&mut tk, &format!("admin alter ddl jobs {} thread = 16, batch_size = 512;", job.id));
        let j = get_job_meta_by_id(&mut tk, job.id);
        assert_eq!(j.reorg_meta.as_ref().unwrap().read().get_concurrency(defaults()), 16);
        assert_eq!(j.reorg_meta.as_ref().unwrap().read().get_batch_size(defaults()), 512);
        delete_job_meta_by_id(&mut tk, job.id);
    }
}

/// Go `TestAdminAlterDDLJobUnsupportedCases`.
#[test]
fn test_admin_alter_ddl_job_unsupported_cases() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(2951)).unwrap();
    rows(&mut tk, "use test");
    rows(&mut tk, "create table t (a int);");
    let must_get_err_msg = |tk: &mut Session, sql: &str, msg: &str| {
        assert_eq!(super::query_error(tk, sql).1, msg, "{sql}");
    };
    // invalid config value
    for (sql, msg) in [
        ("admin alter ddl jobs 1 thread = 0;", "the value 0 for thread is out of range [1, 256]"),
        ("admin alter ddl jobs 1 thread = 257;", "the value 257 for thread is out of range [1, 256]"),
        ("admin alter ddl jobs 1 thread = 10.5;", "the value for thread is invalid, only integer is allowed"),
        ("admin alter ddl jobs 1 thread = '16';", "the value for thread is invalid, only integer is allowed"),
        ("admin alter ddl jobs 1 thread = '';", "the value for thread is invalid, only integer is allowed"),
        ("admin alter ddl jobs 1 batch_size = 31;", "the value 31 for batch_size is out of range [32, 10240]"),
        ("admin alter ddl jobs 1 batch_size = 10241;", "the value 10241 for batch_size is out of range [32, 10240]"),
        ("admin alter ddl jobs 1 batch_size = 321.3;", "the value for batch_size is invalid, only integer is allowed"),
        ("admin alter ddl jobs 1 batch_size = '512';", "the value for batch_size is invalid, only integer is allowed"),
        ("admin alter ddl jobs 1 batch_size = '';", "the value for batch_size is invalid, only integer is allowed"),
        ("admin alter ddl jobs 1 max_write_speed = '2PiB';", "the value 2251799813685248 for max_write_speed is out of range [0, 1125899906842624]"),
        ("admin alter ddl jobs 1 max_write_speed = -1;", "the value -1 for max_write_speed is out of range [0, 1125899906842624]"),
        ("admin alter ddl jobs 1 max_write_speed = 1.23;", "the value 1.23 for max_write_speed is invalid"),
        ("admin alter ddl jobs 1 max_write_speed = 'MiB';", "parse max_write_speed value error: invalid size: 'MiB'"),
        ("admin alter ddl jobs 1 max_write_speed = 'asd';", "parse max_write_speed value error: invalid size: 'asd'"),
        ("admin alter ddl jobs 1 max_write_speed = '';", "parse max_write_speed value error: invalid size: ''"),
        ("admin alter ddl jobs 1 max_write_speed = '20xl';", "parse max_write_speed value error: invalid suffix: 'xl'"),
        ("admin alter ddl jobs 1 max_write_speed = 1.2.3;", "You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 46 near \".3;\" "),
        ("admin alter ddl jobs 1 max_write_speed = 20+30;", "You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 44 near \"+30;\" "),
        ("admin alter ddl jobs 1 max_write_speed = rand();", "You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 45 near \"rand();\" "),
        // testkit probe of Go: the rest of the literal kinds.
        ("admin alter ddl jobs 1 thread = 1e2", "the value for thread is invalid, only integer is allowed"),
        ("admin alter ddl jobs 1 thread = null", "the value for thread is invalid, only integer is allowed"),
        ("admin alter ddl jobs 1 max_write_speed = null", "parse max_write_speed value error: invalid size: ''"),
        ("admin alter ddl jobs 1 max_write_speed = 0x10", "parse max_write_speed value error: invalid size: '\u{10}'"),
        ("admin alter ddl jobs 1 foo = 1", "unsupported admin alter ddl jobs config: foo"),
        ("admin alter ddl jobs 1 thread = 8, foo = 1", "unsupported admin alter ddl jobs config: foo"),
        ("admin alter ddl jobs 1 thread = 0, batch_size = 1", "the value 0 for thread is out of range [1, 256]"),
    ] {
        must_get_err_msg(&mut tk, sql, msg);
    }
    // valid config value
    for sql in [
        "admin alter ddl jobs 1 thread = 16;",
        "admin alter ddl jobs 1 batch_size = 64;",
        "admin alter ddl jobs 1 max_write_speed = '0';",
        "admin alter ddl jobs 1 max_write_speed = '64';",
        "admin alter ddl jobs 1 max_write_speed = '2KB';",
        "admin alter ddl jobs 1 max_write_speed = '3MiB';",
        "admin alter ddl jobs 1 max_write_speed = '4 gb';",
        "admin alter ddl jobs 1 max_write_speed = 1;",
        "admin alter ddl jobs 1 max_write_speed = '1.23';",
        // Go's `int32(batchSize)` keeps 32 of 4294967328; `true` is 1.
        "admin alter ddl jobs 1 batch_size = 4294967328",
        "admin alter ddl jobs 1 thread = true",
        // invalid job id
        "admin alter ddl jobs 1 thread = 8;",
    ] {
        assert_eq!(super::query_error(&mut tk, sql), (1105, "ddl job 1 is not running".to_owned()), "{sql}");
    }

    let mut job = Job::default();
    job.id = 1;
    job.type_ = ActionType::ACTION_ADD_COLUMN;
    insert_mock_job_2_table(&mut tk, &mut job);
    // unsupported job type
    must_get_err_msg(
        &mut tk,
        &format!("admin alter ddl jobs {} thread = 8;", job.id),
        "unsupported DDL operation: add column. Supported DDL operations are: ADD INDEX, MODIFY COLUMN, and ALTER TABLE REORGANIZE PARTITION",
    );
    delete_job_meta_by_id(&mut tk, 1);
}

/// Go `TestAdminAlterDDLJobCommitFailed`.
#[test]
fn test_admin_alter_ddl_job_commit_failed() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(2952)).unwrap();
    rows(&mut tk, "use test");
    rows(&mut tk, "create table t (a int);");
    let _fp = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/mockAlterDDLJobCommitFailed",
        "return(true)",
    );
    let mut job = Job::default();
    job.id = 1;
    job.type_ = ActionType::ACTION_ADD_INDEX;
    job.reorg_meta = Some(GoShared::new(reorg_meta(4, 128)));
    insert_mock_job_2_table(&mut tk, &mut job);
    assert_eq!(
        super::query_error(&mut tk, &format!("admin alter ddl jobs {} thread = 8, batch_size = 256;", job.id)).1,
        "mock commit failed on admin alter ddl jobs"
    );
    let j = get_job_meta_by_id(&mut tk, job.id);
    let (got, want) = (j.reorg_meta.unwrap(), job.reorg_meta.unwrap());
    assert_eq!(got.read().get_concurrency(defaults()), want.read().get_concurrency(defaults()));
    assert_eq!(got.read().get_batch_size(defaults()), want.read().get_batch_size(defaults()));
    assert_eq!(got.read().get_max_write_speed(), want.read().get_max_write_speed());
    delete_job_meta_by_id(&mut tk, job.id);
}
