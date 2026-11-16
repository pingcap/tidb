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

//! Go `tests/realtikvtest/addindextest`, over [`crate::realtikv_testutils`].

use crate::realtikv_testutils::{self as testutils, TestType};

/// Go `FullMode` (`-full-mode`): `TIDB_REALTIKV_FULL_MODE` set.
fn full_mode() -> bool {
    std::env::var_os("TIDB_REALTIKV_FULL_MODE").is_some()
}

/// Go `enableFastAddIndexFailpoints`, until the guards drop.
fn enable_fast_add_index_failpoints() -> [tidb_failpoint::Guard; 2] {
    // by split all the time, we can nearly avoid all region job re-queues caused
    // by EpochNotMatch or other errors, and can make those cases faster.
    // we also reduce the retry backoff to make the test faster when region job
    // is re-queued, as CI ENV might be slow and cause more re-queues.
    [
        tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/forceSplitRegion", "return(true)"),
        tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/adjustRegionJobRetryBackoff",
            |backoff| *backoff.downcast_mut::<std::time::Duration>().unwrap() = std::time::Duration::from_secs(1),
        ),
    ]
}

/// Go `testutils.InitTest` (and its variants), or skip the test.
macro_rules! suite {
    ($init:expr) => {
        match $init {
            Some(ctx) => ctx,
            None => {
                eprintln!("TIDB_REALTIKV_PD is unset: realtikv test skipped");
                return;
            }
        }
    };
}

// Go `add_index_test.go`.

/// Go `TestCreateNonUniqueIndex`.
#[test]
fn test_create_non_unique_index() {
    let _fp = enable_fast_add_index_failpoints();
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let col_ids: [&[usize]; 3] = [&[1, 4, 7, 10, 13, 16, 19, 22, 25], &[2, 5, 8, 11, 14, 17, 20, 23, 26], &[3, 6, 9, 12, 15, 18, 21, 24, 27]];
    let mut ctx = suite!(testutils::init_test());
    ctx.test_one_col_frame(&col_ids, testutils::add_index_non_unique);
}

/// Go `TestCreateUniqueIndex`.
#[test]
fn test_create_unique_index() {
    let _fp = enable_fast_add_index_failpoints();
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let col_ids: [&[usize]; 3] = [&[1, 6, 7, 8, 11, 13, 15, 16, 18, 19, 22, 26], &[2, 9, 11, 17], &[3, 12, 25]];
    let mut ctx = suite!(testutils::init_test());
    ctx.test_one_col_frame(&col_ids, testutils::add_index_unique);
}

/// Go `TestCreatePrimaryKey`.
#[test]
fn test_create_primary_key() {
    let _fp = enable_fast_add_index_failpoints();
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let mut ctx = suite!(testutils::init_test());
    ctx.test_one_index_frame(0, testutils::add_index_pk);
}

/// Go `TestCreateGenColIndex`.
#[test]
fn test_create_gen_col_index() {
    let _fp = enable_fast_add_index_failpoints();
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let mut ctx = suite!(testutils::init_test());
    ctx.test_one_index_frame(29, testutils::add_index_gen_col);
}

/// Go `TestCreateMultiColsIndex`.
#[test]
fn test_create_multi_cols_index() {
    let _fp = enable_fast_add_index_failpoints();
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let (coli_ids, colj_ids): ([&[usize]; 3], [&[usize]; 3]) = if full_mode() {
        ([&[1, 4, 7, 10, 13], &[2, 5, 8, 11], &[3, 6, 9, 12, 15]], [&[16, 19, 22, 25], &[14, 17, 20, 23, 26], &[18, 21, 24, 27]])
    } else {
        ([&[1, 4, 7], &[2, 5], &[3, 6, 9]], [&[16, 19], &[14, 17, 20], &[18, 21]])
    };
    let mut ctx = suite!(testutils::init_test());
    ctx.test_two_cols_frame(&coli_ids, &colj_ids, testutils::add_index_multi_cols);
}

/// Go `TestAddForeignKeyWithAutoCreateIndex`.
#[test]
fn test_add_foreign_key_with_auto_create_index() {
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists fk_index;");
    tk.must_exec("create database fk_index;");
    tk.must_exec("use fk_index;");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=1;");
    tk.must_exec("create table employee (id bigint auto_increment key, pid bigint)");
    tk.must_exec("insert into employee (id) values (1),(2),(3),(4),(5),(6),(7),(8)");
    for _ in 0..14 {
        tk.must_exec("insert into employee (pid) select pid from employee");
    }
    tk.must_exec("update employee set pid=id-1 where id>1");
    assert_eq!(tk.must_query("select count(*) from employee"), vec![vec!["131072".to_owned()]]);
    tk.must_exec("alter table employee add foreign key fk_1(pid) references employee(id)");
    tk.must_exec("alter table employee drop foreign key fk_1");
    tk.must_exec("alter table employee drop index fk_1");
    tk.must_exec("update employee set pid=0 where id=1");
    assert_eq!(
        tk.exec_to_err("alter table employee add foreign key fk_1(pid) references employee(id)"),
        "[ddl:1452]Cannot add or update a child row: a foreign key constraint fails (`fk_index`.`employee`, CONSTRAINT `fk_1` FOREIGN KEY (`pid`) REFERENCES `employee` (`id`))"
    );
    tk.must_exec("insert into employee (pid) select pid from employee");
    tk.must_exec("update employee set pid=id");
    tk.must_exec("alter table employee add foreign key fk_1(pid) references employee(id)");
}

/// Go `TestIssue51162`.
#[test]
fn test_issue51162() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("set global tidb_enable_fast_table_check=0");
    tk.must_exec(
        "CREATE TABLE tl (
	 col_42 json NOT NULL,
	 col_43 tinyint(1) DEFAULT NULL,
	 col_44 char(168) CHARACTER SET gbk COLLATE gbk_bin DEFAULT NULL,
	 col_45 json DEFAULT NULL,
	 col_46 text COLLATE utf8mb4_unicode_ci NOT NULL,
	 col_47 char(43) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'xW2YNb99pse4)',
	 col_48 time NOT NULL DEFAULT '12:31:25',
	 PRIMARY KEY (col_47,col_46(2)) /*T![clustered_index] CLUSTERED */
	  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;",
    );
    tk.must_exec(
        "INSERT INTO tl VALUES
	('[\\\"1\\\"]',0,'1','[1]','Wxup81','1','10:14:20');",
    );
    tk.must_exec("alter table tl add index idx_16(`col_48`,(cast(`col_45` as signed array)),`col_46`(5));");
    tk.must_exec("admin check table tl");
}

/// Go `TestAddUKWithSmallIntHandles`.
#[test]
fn test_add_uk_with_small_int_handles() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists small;");
    tk.must_exec("create database small;");
    tk.must_exec("use small;");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=1;");
    tk.must_exec("create table t (a bigint, b int, primary key (a) clustered)");
    tk.must_exec("insert into t values (-9223372036854775808, 1),(-9223372036854775807, 1)");
    let err = tk.exec_to_err("alter table t add unique index uk(b)");
    assert!(err.contains("Duplicate entry '1' for key 't.uk'"), "{err}");
}

/// Go `TestAddUniqueDuplicateIndexes`.
#[test]
fn test_add_unique_duplicate_indexes() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=1;");
    tk.must_exec(
        "create table t(a int DEFAULT '-13202', b varchar(221) NOT NULL DEFAULT 'duplicatevalue', c int NOT NULL DEFAULT '0');",
    );

    let tk1 = std::sync::Arc::new(std::sync::Mutex::new(store.new_tk()));
    tk1.lock().unwrap().must_exec("use test");
    let _ = tk1.lock().unwrap().exec("INSERT INTO t VALUES (-18585,'duplicatevalue',0);");

    // Go's `assert` records a failure without stopping the goroutine.
    let errors = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let _schema_synced = {
        let (tk1, errors) = (std::sync::Arc::clone(&tk1), std::sync::Arc::clone(&errors));
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", move |job| {
            if job.downcast_ref::<tidb_model::Job>().unwrap().schema_state == tidb_model::SchemaState::DELETE_ONLY {
                let mut tk1 = tk1.lock().unwrap();
                if let Err(err) = tk1.exec("delete from t where c = 0;") {
                    errors.lock().unwrap().push(err);
                }
                if let Err(err) = tk1.exec("insert INTO t VALUES (-18585,'duplicatevalue',1);") {
                    errors.lock().unwrap().push(err);
                }
            }
        })
    };

    let tk3 = std::sync::Arc::new(std::sync::Mutex::new(store.new_tk()));
    tk3.lock().unwrap().must_exec("use test");
    let rows = |tk: &mut crate::realtikvtest::Tk| -> Vec<String> {
        tk.must_query("select * from t;").into_iter().map(|row| row.join(" ")).collect()
    };

    let before_ingest_once = std::sync::Once::new();
    let _before_ingest = {
        let tk3 = std::sync::Arc::clone(&tk3);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/ingest/beforeBackendIngest", move |_| {
            before_ingest_once.call_once(|| {
                let mut tk3 = tk3.lock().unwrap();
                tk3.must_exec("replace INTO t VALUES (-18585,'duplicatevalue',4);");
                assert_eq!(rows(&mut tk3), ["-18585 duplicatevalue 1", "-18585 duplicatevalue 4"]);
            });
        })
    };
    let before_merge_once = std::sync::Once::new();
    let _before_merge = {
        let tk3 = std::sync::Arc::clone(&tk3);
        tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeBackfillMerge", move |_| {
            before_merge_once.call_once(|| {
                let mut tk3 = tk3.lock().unwrap();
                assert_eq!(rows(&mut tk3), ["-18585 duplicatevalue 1", "-18585 duplicatevalue 4"]);
                tk3.must_exec("replace into t values (-18585,'duplicatevalue',0);");
            });
        })
    };

    tk.must_exec("alter table t add unique index idx(b);");
    tk.must_exec("admin check table t;");
    assert!(errors.lock().unwrap().is_empty(), "{:?}", errors.lock().unwrap());
}

/// Go `TestAddIndexOnGB18030Bin`.
#[test]
fn test_add_index_on_gb18030_bin() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("use test");
    tk.must_exec(
        "CREATE TABLE t (
		a varchar(198) COLLATE gb18030_bin NOT NULL,
		b varchar(178) COLLATE gb18030_bin NOT NULL,
		PRIMARY KEY (b,a),
		KEY k1 (b,a),
		KEY k2 (b)
	) ENGINE=InnoDB DEFAULT CHARSET=gb18030 COLLATE=gb18030_bin;",
    );
    tk.must_exec("insert into t values ('a', 'b');");
    tk.must_exec("admin check table t;");
}

// Go `concurrent_ddl_test.go`.

fn concurrent_ddl(col_i_ids: &[&[usize]], col_j_ids: &[&[usize]], t_type: TestType) {
    let _fp = enable_fast_add_index_failpoints();
    let _restore = tidb_dxf_testutil::reduce_check_interval_for_test();
    let mut ctx = suite!(testutils::init_concurrent_ddl_test(col_i_ids, col_j_ids, t_type));
    ctx.comp_start();
    ctx.comp_stop().unwrap();
}

/// Go `TestConcurrentDDLCreateNonUniqueIndex`.
#[test]
fn test_concurrent_ddl_create_non_unique_index() {
    concurrent_ddl(&[&[1, 4, 7, 10, 13], &[14, 17, 20, 23, 26], &[3, 6, 9, 21, 24]], &[], TestType::NonUnique);
}

/// Go `TestConcurrentDDLCreateUniqueIndex`.
#[test]
fn test_concurrent_ddl_create_unique_index() {
    concurrent_ddl(&[&[1, 6, 11, 13], &[2, 11, 17], &[3, 19, 25]], &[], TestType::Unique);
}

/// Go `TestConcurrentDDLCreatePrimaryKey`.
#[test]
fn test_concurrent_ddl_create_primary_key() {
    concurrent_ddl(&[], &[], TestType::Pk);
}

/// Go `TestConcurrentDDLCreateGenColIndex`.
#[test]
fn test_concurrent_ddl_create_gen_col_index() {
    concurrent_ddl(&[], &[], TestType::GenIndex);
}

/// Go `TestConcurrentDDLCreateMultiColsIndex`.
#[test]
fn test_concurrent_ddl_create_multi_cols_index() {
    concurrent_ddl(&[&[7], &[11], &[14]], &[&[16], &[23], &[19]], TestType::MultiCols);
}

// Go `multi_schema_change_test.go`.

fn multi_schema_change_ctx() -> Option<testutils::SuiteContext> {
    let mut ctx = testutils::init_comp_ctx()?;
    ctx.set_multi_schema_change(true);
    Some(ctx)
}

/// Go `TestMultiSchemaChangeCreateNonUniqueIndex`.
#[test]
fn test_multi_schema_change_create_non_unique_index() {
    let _fp = enable_fast_add_index_failpoints();
    let mut ctx = suite!(multi_schema_change_ctx());
    ctx.test_one_col_frame(&[&[1, 4, 7], &[2, 5, 8], &[3, 6, 9]], testutils::add_index_non_unique);
}

/// Go `TestMultiSchemaChangeCreateUniqueIndex`.
#[test]
fn test_multi_schema_change_create_unique_index() {
    let _fp = enable_fast_add_index_failpoints();
    let mut ctx = suite!(multi_schema_change_ctx());
    ctx.test_one_col_frame(&[&[1, 6, 8], &[2, 19], &[11]], testutils::add_index_unique);
}

/// Go `TestMultiSchemaChangeCreatePrimaryKey`.
#[test]
fn test_multi_schema_change_create_primary_key() {
    let _fp = enable_fast_add_index_failpoints();
    let mut ctx = suite!(multi_schema_change_ctx());
    ctx.test_one_index_frame(0, testutils::add_index_pk);
}

/// Go `TestMultiSchemaChangeCreateGenColIndex`.
#[test]
fn test_multi_schema_change_create_gen_col_index() {
    let _fp = enable_fast_add_index_failpoints();
    let mut ctx = suite!(multi_schema_change_ctx());
    ctx.test_one_index_frame(29, testutils::add_index_gen_col);
}

/// Go `TestMultiSchemaChangeMultiColsIndex`.
#[test]
fn test_multi_schema_change_multi_cols_index() {
    let _fp = enable_fast_add_index_failpoints();
    let mut ctx = suite!(multi_schema_change_ctx());
    ctx.test_two_cols_frame(&[&[1], &[2], &[3]], &[&[16], &[14], &[18]], testutils::add_index_multi_cols);
}

// Go `pitr_test.go`.

fn pitr_ctx() -> Option<testutils::SuiteContext> {
    let mut ctx = testutils::init_comp_ctx()?;
    ctx.comp_ctx.as_mut().unwrap().is_pitr = true;
    Some(ctx)
}

/// Go `TestPiTRCreateNonUniqueIndex`.
#[test]
fn test_pitr_create_non_unique_index() {
    let _fp = enable_fast_add_index_failpoints();
    let mut ctx = suite!(pitr_ctx());
    ctx.test_one_col_frame(&[&[1, 4, 7], &[2, 5, 8], &[3, 6, 9]], testutils::add_index_non_unique);
}

/// Go `TestPiTRCreateUniqueIndex`.
#[test]
fn test_pitr_create_unique_index() {
    let _fp = enable_fast_add_index_failpoints();
    let mut ctx = suite!(pitr_ctx());
    ctx.test_one_col_frame(&[&[1, 6], &[11], &[19]], testutils::add_index_unique);
}

/// Go `TestPiTRCreatePrimaryKey`.
#[test]
fn test_pitr_create_primary_key() {
    let _fp = enable_fast_add_index_failpoints();
    let mut ctx = suite!(pitr_ctx());
    ctx.test_one_index_frame(0, testutils::add_index_pk);
}

/// Go `TestPiTRCreateGenColIndex`.
#[test]
fn test_pitr_create_gen_col_index() {
    let _fp = enable_fast_add_index_failpoints();
    let mut ctx = suite!(pitr_ctx());
    ctx.test_one_index_frame(29, testutils::add_index_gen_col);
}

/// Go `TestPiTRCreateMultiColsIndex`.
#[test]
fn test_pitr_create_multi_cols_index() {
    let _fp = enable_fast_add_index_failpoints();
    let mut ctx = suite!(pitr_ctx());
    ctx.test_two_cols_frame(&[&[1], &[8], &[11]], &[&[16], &[23], &[27]], testutils::add_index_multi_cols);
}

// Go `failpoints_test.go`: full mode only.

/// Go `TestFailpointsCreateNonUniqueIndex`.
#[test]
fn test_failpoints_create_non_unique_index() {
    let _fp = enable_fast_add_index_failpoints();
    if !full_mode() {
        return;
    }
    let mut ctx = suite!(testutils::init_test_failpoint());
    ctx.test_one_col_frame(
        &[&[1, 4, 7, 10, 13, 16, 19, 22, 25], &[2, 5, 8, 11, 14, 17, 20, 23, 26], &[3, 6, 9, 12, 15, 18, 21, 24, 27]],
        testutils::add_index_non_unique,
    );
}

/// Go `TestFailpointsCreateUniqueIndex`.
#[test]
fn test_failpoints_create_unique_index() {
    let _fp = enable_fast_add_index_failpoints();
    if !full_mode() {
        return;
    }
    let mut ctx = suite!(testutils::init_test_failpoint());
    ctx.test_one_col_frame(&[&[1, 6, 7, 8, 11, 13, 15, 16, 18, 19, 22, 26], &[2, 9, 11, 17], &[3, 12, 25]], testutils::add_index_unique);
}

/// Go `TestFailpointsCreatePrimaryKeyFailpoints`.
#[test]
fn test_failpoints_create_primary_key_failpoints() {
    if !full_mode() {
        return;
    }
    let _fp = enable_fast_add_index_failpoints();
    let mut ctx = suite!(testutils::init_test());
    ctx.test_one_index_frame(0, testutils::add_index_pk);
}

/// Go `TestFailpointsCreateGenColIndex`.
#[test]
fn test_failpoints_create_gen_col_index() {
    if !full_mode() {
        return;
    }
    let _fp = enable_fast_add_index_failpoints();
    let mut ctx = suite!(testutils::init_test_failpoint());
    ctx.test_one_index_frame(29, testutils::add_index_gen_col);
}

/// Go `TestFailpointsCreateMultiColsIndex`.
#[test]
fn test_failpoints_create_multi_cols_index() {
    let _fp = enable_fast_add_index_failpoints();
    if !full_mode() {
        return;
    }
    let mut ctx = suite!(testutils::init_test_failpoint());
    ctx.test_two_cols_frame(
        &[&[1, 4, 7], &[2, 5, 8], &[3, 6, 9]],
        &[&[16, 19, 22], &[14, 17, 20], &[18, 21, 24]],
        testutils::add_index_multi_cols,
    );
}
