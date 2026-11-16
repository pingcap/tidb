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

//! Go `tests/realtikvtest/testutils`: the add-index suites' tables, their
//! concurrent DML workload, the add-index frames and the compatibility
//! (concurrent DDL, multi-schema change) executors.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::realtikvtest::{RealTiKV, Tk};

const TABLE_NUM: usize = 3;
const NON_PART_TAB_NUM: usize = 1;

/// Go `skippedErrorCode`.
const SKIPPED_ERROR_CODE: [&str; 3] = [
    "8028",     // ErrInfoSchemaChanged
    "9007",     // ErrWriteConflict
    "global:2", // execution result undetermined
];

/// Go `workload`'s state (the package's `wCtx`): what the insert, update and
/// delete workers write.
#[derive(Default)]
struct WorkloadState {
    table_name: String,
    col_id: Vec<usize>,
    insert_id: i64,
    date: String,
}

/// The `SuiteContext` state Go's workload and executor goroutines read.
pub(crate) struct Shared {
    is_unique: AtomicBool,
    is_pk: AtomicBool,
    row_num: usize,
    /// Go `ctx.ctx`: the first workload `stop` cancels it, and nothing
    /// renews it.
    cancelled: AtomicBool,
    /// Go `CompCtx.IsMultiSchemaChange` (false without a `CompCtx`).
    is_multi_schema_change: AtomicBool,
    workload: Mutex<WorkloadState>,
}

impl Shared {
    fn done(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    fn is_unique(&self) -> bool {
        self.is_unique.load(Ordering::SeqCst)
    }

    fn is_pk(&self) -> bool {
        self.is_pk.load(Ordering::SeqCst)
    }
}

/// Go `SuiteContext`.
pub(crate) struct SuiteContext {
    pub(crate) store: RealTiKV,
    pub(crate) tk: Tk,
    pub(crate) shared: Arc<Shared>,
    table_num: usize,
    /// Go `ctx.workload != nil` (`initWorkloadParams`).
    has_workload: bool,
    workers: Vec<std::thread::JoinHandle<Result<(), String>>>,
    is_failpoints_test: bool,
    pub(crate) comp_ctx: Option<CompatibilityContext>,
}

/// Go `InitTest`, `None` when no cluster is configured.
pub(crate) fn init_test() -> Option<SuiteContext> {
    let store = RealTiKV::setup()?;
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists addindex;");
    tk.must_exec("create database addindex;");
    tk.must_exec("use addindex;");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");

    create_table(&mut tk);
    insert_rows(&mut tk);
    // Go `newSuiteContext` and `initWorkloadParams`.
    Some(SuiteContext {
        store,
        tk,
        shared: Arc::new(Shared {
            is_unique: AtomicBool::new(false),
            is_pk: AtomicBool::new(false),
            row_num: 64,
            cancelled: AtomicBool::new(false),
            is_multi_schema_change: AtomicBool::new(false),
            workload: Mutex::new(WorkloadState::default()),
        }),
        table_num: 3,
        has_workload: true,
        workers: Vec::new(),
        is_failpoints_test: false,
        comp_ctx: None,
    })
}

/// Go `InitTestFailpoint`.
pub(crate) fn init_test_failpoint() -> Option<SuiteContext> {
    let mut ctx = init_test()?;
    ctx.is_failpoints_test = true;
    Some(ctx)
}

const COLUMNS: &str = "c0 int, c1 bit(8), c2 boolean, c3 tinyint default 3, c4 smallint not null, c5 mediumint,\
c6 int, c7 bigint, c8 float, c9 double, c10 decimal(13,7), c11 date, c12 time, c13 datetime,\
c14 timestamp, c15 year, c16 char(10), c17 varchar(10), c18 text, c19 tinytext, c20 mediumtext,\
c21 longtext, c22 binary(20), c23 varbinary(30), c24 blob, c25 tinyblob, c26 MEDIUMBLOB, c27 LONGBLOB,\
c28 json, c29 INT AS (JSON_EXTRACT(c28, '$.population'))";

/// Go `genTableStr`.
fn gen_table_str(table_name: &str) -> String {
    format!("create table addindex.{table_name} ({COLUMNS})")
}

/// Go `genPartTableStr`.
fn gen_part_table_str() -> Vec<String> {
    let num = NON_PART_TAB_NUM;
    vec![
        // Range table def
        format!(
            "CREATE TABLE addindex.t{num} ({COLUMNS}) PARTITION BY RANGE (`c0`) (PARTITION `p0` VALUES LESS THAN (10), \
             PARTITION `p1` VALUES LESS THAN (20), PARTITION `p2` VALUES LESS THAN (30), PARTITION `p3` VALUES LESS THAN (40), \
             PARTITION `p4` VALUES LESS THAN (50), PARTITION `p5` VALUES LESS THAN (60), PARTITION `p6` VALUES LESS THAN (70), \
             PARTITION `p7` VALUES LESS THAN (80), PARTITION `p8` VALUES LESS THAN MAXVALUE)"
        ),
        // Hash part table
        format!("CREATE TABLE addindex.t{} ({COLUMNS}) PARTITION BY HASH (c0) PARTITIONS 4", num + 1),
    ]
}

/// Go `createTable`.
fn create_table(tk: &mut Tk) {
    for i in 0..NON_PART_TAB_NUM {
        tk.must_exec(&gen_table_str(&format!("t{i}")));
    }
    for table_def in gen_part_table_str() {
        tk.must_exec(&table_def);
    }
}

/// Go `insertRows`'s values, verbatim.
const VALUES: [&str; 64] = [
    " (1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 1111.1111, '2001-01-01', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 100}')",
    " (2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0, 1112.1111, '2001-01-02', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 101}')",
    " (3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0, 1113.1111, '2001-01-03', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')",
    " (4, 4, 4, 4, 4, 4, 4, 4, 4.0, 4.0, 1114.1111, '2001-01-04', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 103}')",
    " (5, 5, 1, 1, 1, 1, 5, 1, 1.0, 1.0, 1111.1111, '2001-01-05', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'eeee', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 104}')",
    " (6, 2, 2, 2, 2, 2, 6, 2, 2.0, 2.0, 1112.1111, '2001-01-06', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'ffff', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 105}')",
    " (7, 3, 3, 3, 3, 3, 7, 3, 3.0, 3.0, 1113.1111, '2001-01-07', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'gggg', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 106}')",
    " (8, 4, 4, 4, 4, 4, 8, 4, 4.0, 4.0, 1114.1111, '2001-01-08', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'hhhh', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 107}')",
    " (9, 1, 1, 1, 1, 1, 9, 1, 1.0, 1.0, 1111.1111, '2001-01-09', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'iiii', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 100}')",
    " (10, 2, 2, 2, 2, 2, 10, 2, 2.0, 2.0, 1112.1111, '2001-01-10', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'jjjj', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 101}')",
    " (11, 3, 3, 3, 3, 3, 11, 3, 3.0, 3.0, 1113.1111, '2001-01-11', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'kkkk', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')",
    " (12, 4, 4, 4, 4, 4, 12, 4, 4.0, 4.0, 1114.1111, '2001-01-12', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'llll', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 103}')",
    " (13, 5, 1, 1, 1, 1, 13, 1, 1.0, 1.0, 1111.1111, '2001-01-13', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'mmmm', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 104}')",
    " (14, 2, 2, 2, 2, 2, 14, 2, 2.0, 2.0, 1112.1111, '2001-01-14', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'nnnn', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 105}')",
    " (15, 3, 3, 3, 3, 3, 15, 3, 3.0, 3.0, 1113.1111, '2001-01-15', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'oooo', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 106}')",
    " (16, 4, 4, 4, 4, 4, 16, 4, 4.0, 4.0, 1114.1111, '2001-01-16', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'pppp', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 107}')",
    " (17, 1, 1, 1, 1, 1, 17, 1, 1.0, 1.0, 1111.1111, '2001-01-17', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'qqqq', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 100}')",
    " (18, 2, 2, 2, 2, 2, 18, 2, 2.0, 2.0, 1112.1111, '2001-01-18', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'rrrr', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 101}')",
    " (19, 3, 3, 3, 3, 3, 19, 3, 3.0, 3.0, 1113.1111, '2001-01-19', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'ssss', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')",
    " (20, 4, 4, 4, 4, 4, 20, 4, 4.0, 4.0, 1114.1111, '2001-01-20', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'tttt', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 103}')",
    " (21, 5, 1, 1, 1, 1, 21, 1, 1.0, 1.0, 1111.1111, '2001-01-21', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'uuuu', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 104}')",
    " (22, 2, 2, 2, 2, 2, 22, 2, 2.0, 2.0, 1112.1111, '2001-01-22', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'vvvv', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 105}')",
    " (23, 3, 3, 3, 3, 3, 23, 3, 3.0, 3.0, 1113.1111, '2001-01-23', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'wwww', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 106}')",
    " (24, 4, 4, 4, 4, 4, 24, 4, 4.0, 4.0, 1114.1111, '2001-01-24', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'xxxx', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 107}')",
    " (25, 1, 1, 1, 1, 1, 25, 1, 1.0, 1.0, 1111.1111, '2001-01-25', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'yyyy', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 100}')",
    " (26, 2, 2, 2, 2, 2, 26, 2, 2.0, 2.0, 1112.1111, '2001-01-26', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'zzzz', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 101}')",
    " (27, 3, 3, 3, 3, 3, 27, 3, 3.0, 3.0, 1113.1111, '2001-01-27', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaab', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')",
    " (28, 4, 4, 4, 4, 4, 28, 4, 4.0, 4.0, 1114.1111, '2001-01-28', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aaac', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 103}')",
    " (29, 5, 1, 1, 1, 1, 29, 1, 1.0, 1.0, 1111.1111, '2001-01-29', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaad', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 104}')",
    " (30, 2, 2, 2, 2, 2, 30, 2, 2.0, 2.0, 1112.1111, '2001-01-30', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aaae', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 105}')",
    " (31, 3, 3, 3, 3, 3, 31, 3, 3.0, 3.0, 1113.1111, '2001-01-31', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaaf', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 106}')",
    " (32, 4, 4, 4, 4, 4, 32, 4, 4.0, 4.0, 1114.1111, '2001-02-01', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aaag', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 107}')",
    " (33, 1, 1, 1, 1, 1, 33, 1, 1.0, 1.0, 1111.1111, '2001-02-02', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaah', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 100}')",
    " (34, 2, 2, 2, 2, 2, 34, 2, 2.0, 2.0, 1112.1111, '2001-02-03', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aaai', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 101}')",
    " (35, 3, 3, 3, 3, 3, 35, 3, 3.0, 3.0, 1113.1111, '2001-02-05', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaaj', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')",
    " (36, 4, 4, 4, 4, 4, 36, 4, 4.0, 4.0, 1114.1111, '2001-02-04', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aaak', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 103}')",
    " (37, 5, 1, 1, 1, 1, 37, 1, 1.0, 1.0, 1111.1111, '2001-02-06', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaal', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 104}')",
    " (38, 2, 2, 2, 2, 2, 38, 2, 2.0, 2.0, 1112.1111, '2001-02-07', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aaam', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 105}')",
    " (39, 3, 3, 3, 3, 3, 39, 3, 3.0, 3.0, 1113.1111, '2001-02-08', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaan', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 106}')",
    " (40, 4, 4, 4, 4, 4, 40, 4, 4.0, 4.0, 1114.1111, '2001-02-09', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aaao', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 107}')",
    " (41, 1, 1, 1, 1, 1, 41, 1, 1.0, 1.0, 1111.1111, '2001-02-10', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaap', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 100}')",
    " (42, 2, 2, 2, 2, 2, 42, 2, 2.0, 2.0, 1112.1111, '2001-02-11', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aaaq', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 101}')",
    " (43, 3, 3, 3, 3, 3, 43, 3, 3.0, 3.0, 1113.1111, '2001-02-12', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaar', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')",
    " (44, 4, 4, 4, 4, 4, 44, 4, 4.0, 4.0, 1114.1111, '2001-02-13', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aaas', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 103}')",
    " (45, 5, 1, 1, 1, 1, 45, 1, 1.0, 1.0, 1111.1111, '2001-02-14', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaat', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 104}')",
    " (46, 2, 2, 2, 2, 2, 46, 2, 2.0, 2.0, 1112.1111, '2001-02-15', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aaau', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 105}')",
    " (47, 3, 3, 3, 3, 3, 47, 3, 3.0, 3.0, 1113.1111, '2001-02-16', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaav', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 106}')",
    " (48, 4, 4, 4, 4, 4, 48, 4, 4.0, 4.0, 1114.1111, '2001-02-17', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aaaw', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 107}')",
    " (49, 1, 1, 1, 1, 1, 49, 1, 1.0, 1.0, 1111.1111, '2001-02-18', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaax', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 100}')",
    " (50, 2, 2, 2, 2, 2, 50, 2, 2.0, 2.0, 1112.1111, '2001-02-19', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aaay', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 101}')",
    " (51, 3, 3, 3, 3, 3, 51, 3, 3.0, 3.0, 1113.1111, '2001-02-20', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaaz', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')",
    " (52, 4, 4, 4, 4, 4, 52, 4, 4.0, 4.0, 1114.1111, '2001-02-21', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aaba', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 103}')",
    " (53, 5, 1, 1, 1, 1, 53, 1, 1.0, 1.0, 1111.1111, '2001-02-22', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaca', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 104}')",
    " (54, 2, 2, 2, 2, 2, 54, 2, 2.0, 2.0, 1112.1111, '2001-02-23', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aada', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 105}')",
    " (55, 3, 3, 3, 3, 3, 55, 3, 3.0, 3.0, 1113.1111, '2001-02-24', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaea', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 106}')",
    " (56, 4, 4, 4, 4, 4, 56, 4, 4.0, 4.0, 1114.1111, '2001-02-25', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aafa', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 107}')",
    " (57, 1, 1, 1, 1, 1, 57, 1, 1.0, 1.0, 1111.1111, '2001-02-26', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaga', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 100}')",
    " (58, 2, 2, 2, 2, 2, 58, 2, 2.0, 2.0, 1112.1111, '2001-02-27', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aaha', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 101}')",
    " (59, 3, 3, 3, 3, 3, 59, 3, 3.0, 3.0, 1113.1111, '2001-02-28', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaia', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')",
    " (60, 4, 4, 4, 4, 4, 60, 4, 4.0, 4.0, 1114.1111, '2001-03-01', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aaja', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 103}')",
    " (61, 5, 1, 1, 1, 1, 61, 1, 1.0, 1.0, 1111.1111, '2001-03-02', '11:11:11', '2001-01-01 11:11:11', '2001-01-01 11:11:11.123456', 1999, 'aaaa', 'aaaa', 'aaaa', 'aaka', 'aaaa','aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', '{\"name\": \"Beijing\", \"population\": 104}')",
    " (62, 2, 2, 2, 2, 2, 62, 2, 2.0, 2.0, 1112.1111, '2001-03-03', '11:11:12', '2001-01-02 11:11:12', '2001-01-02 11:11:12.123456', 2000, 'bbbb', 'bbbb', 'bbbb', 'aala', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', 'bbbb', '{\"name\": \"Beijing\", \"population\": 105}')",
    " (63, 3, 3, 3, 3, 3, 63, 3, 3.0, 3.0, 1113.1111, '2001-03-04', '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aama', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 106}')",
    " (64, 4, 4, 4, 4, 4, 64, 4, 4.0, 4.0, 1114.1111, '2001-03-05', '11:11:14', '2001-01-04 11:11:14', '2001-01-04 11:11:12.123456', 2002, 'dddd', 'dddd', 'dddd', 'aana', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', 'dddd', '{\"name\": \"Beijing\", \"population\": 107}')",
];

/// Go `insertRows`.
fn insert_rows(tk: &mut Tk) {
    for i in 0..TABLE_NUM {
        let ins_str = format!(
            "insert into addindex.t{i} (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28) values"
        );
        for value in VALUES {
            tk.must_exec(&format!("{ins_str}{value}"));
        }
    }
}

/// Go `createIndexOneCol`; `tk` is the context's, or under concurrent DDL
/// the table's executor's.
fn create_index_one_col(shared: &Shared, tk: &mut Tk, table_id: usize, mut col_id: usize) -> Result<(), String> {
    let (is_pk, is_unique) = (shared.is_pk(), shared.is_unique());
    let add_index_str = if is_pk {
        " add primary key idx"
    } else if is_unique {
        " add unique index idx"
    } else {
        " add index idx"
    };
    let length = if is_unique && col_id == 19 { 16 } else { 4 };
    let mut ddl_str = String::new();
    if !(is_pk || is_unique) || table_id == 0 || (is_pk && table_id > 0) {
        if (18..29).contains(&col_id) {
            ddl_str = format!("alter table addindex.t{table_id}{add_index_str}{col_id}(c{col_id}({length}))");
        } else {
            ddl_str = format!("alter table addindex.t{table_id}{add_index_str}{col_id}(c{col_id})");
        }
    } else if is_unique && table_id > 0 {
        if (18..29).contains(&col_id) {
            ddl_str = format!("alter table addindex.t{table_id}{add_index_str}{col_id}(c0, c{col_id}({length}))");
        } else {
            ddl_str = format!("alter table addindex.t{table_id}{add_index_str}{col_id}(c0, c{col_id})");
        }
    }
    if shared.is_multi_schema_change.load(Ordering::SeqCst) {
        col_id += 60;
        ddl_str += &format!(" , add column c{col_id} int;");
    }
    eprintln!("[add index test] createIndexOneCol sql={ddl_str}");
    let result = tk.exec(&ddl_str).map(drop);
    if let Err(err) = &result {
        if is_unique || is_pk {
            assert!(err.contains("Duplicate entry"), "{err}");
        } else {
            panic!("{ddl_str}: {err}");
        }
    }
    result
}

/// Go `createIndexTwoCols`.
fn create_index_two_cols(
    shared: &Shared,
    tk: &mut Tk,
    table_id: usize,
    index_id: usize,
    mut col_id1: usize,
    col_id2: usize,
) -> Result<(), String> {
    let add_index_str = if shared.is_pk() {
        " add primary key idx"
    } else if shared.is_unique() {
        " add unique index idx"
    } else {
        " add index idx"
    };
    let col_str = |col_id: usize| {
        if (18..29).contains(&col_id) {
            format!("{col_id}(4)")
        } else {
            col_id.to_string()
        }
    };
    let mut ddl_str = format!(
        "alter table addindex.t{table_id}{add_index_str}{index_id}(c{}, c{})",
        col_str(col_id1),
        col_str(col_id2)
    );
    if shared.is_multi_schema_change.load(Ordering::SeqCst) {
        col_id1 += 60;
        ddl_str += &format!(" , add column c{col_id1} varchar(10);");
    }
    eprintln!("[add index test] createIndexTwoCols sql={ddl_str}");
    let result = tk.exec(&ddl_str).map(drop);
    if let Err(err) = &result {
        panic!("add index failed: {ddl_str}: {err}");
    }
    result
}

/// Go `checkResult`.
fn check_result(tk: &mut Tk, table_name: &str, index_id: usize) {
    let admin_check_sql = format!("admin check index {table_name} idx{index_id}");
    if let Err(err) = tk.exec(&admin_check_sql) {
        panic!("checkResult {admin_check_sql}: {err}");
    }
    assert_eq!(tk.affected_rows(), 0);
    let drop_sql = format!("alter table {table_name} drop index idx{index_id}");
    if let Err(err) = tk.exec(&drop_sql) {
        panic!("drop index failed {drop_sql}: {err}");
    }
}

/// Go `checkTableResult`.
fn check_table_result(tk: &mut Tk, table_name: &str) {
    let admin_check_sql = format!("admin check table {table_name}");
    if let Err(err) = tk.exec(&admin_check_sql) {
        panic!("checkTableResult {admin_check_sql}: {err}");
    }
    assert_eq!(tk.affected_rows(), 0);
}

/// An add-index function of one column (Go `func(*SuiteContext, int, string, int) error`).
pub(crate) type OneColFn = fn(&Shared, &mut Tk, usize, &str, usize) -> Result<(), String>;

/// An add-index function of two columns.
pub(crate) type TwoColsFn = fn(&Shared, &mut Tk, usize, &str, usize, usize, usize) -> Result<(), String>;

impl SuiteContext {
    /// Go `TestOneColFrame`.
    pub(crate) fn test_one_col_frame(&mut self, col_ids: &[&[usize]], f: OneColFn) {
        for table_id in 0..self.table_num {
            let table_name = format!("addindex.t{table_id}");
            for &i in col_ids[table_id] {
                if self.has_workload {
                    self.workload_start(&[table_id, i]);
                }
                let fail_sync = self.is_failpoints_test.then(|| std::thread::spawn(move || use_failpoints(i)));
                let err = f(&self.shared, &mut self.tk, table_id, &table_name, i);
                if let Err(err) = &err {
                    if self.shared.is_unique() || self.shared.is_pk() {
                        assert!(err.contains("Duplicate entry"), "{err}");
                    } else {
                        panic!("add index failed: {err}");
                    }
                }
                if self.has_workload {
                    let _ = self.workload_stop();
                }
                if let Some(fail_sync) = fail_sync {
                    fail_sync.join().unwrap();
                }
                if err.is_ok() {
                    check_result(&mut self.tk, &table_name, i);
                }
            }
        }
    }

    /// Go `TestTwoColsFrame`.
    pub(crate) fn test_two_cols_frame(&mut self, i_ids: &[&[usize]], j_ids: &[&[usize]], f: TwoColsFn) {
        for table_id in 0..self.table_num {
            let table_name = format!("addindex.t{table_id}");
            let mut index_id = 0;
            for &i in i_ids[table_id] {
                for &j in j_ids[table_id] {
                    if self.has_workload {
                        self.workload_start(&[table_id, i, j]);
                    }
                    let fail_sync = self.is_failpoints_test.then(|| std::thread::spawn(move || use_failpoints(i)));
                    let err = f(&self.shared, &mut self.tk, table_id, &table_name, index_id, i, j);
                    if let Err(err) = &err {
                        panic!("add index failed: {err}");
                    }
                    if self.has_workload {
                        // Stop workload
                        let _ = self.workload_stop();
                    }
                    if let Some(fail_sync) = fail_sync {
                        fail_sync.join().unwrap();
                    }
                    if err.is_ok() && i != j {
                        check_result(&mut self.tk, &table_name, index_id);
                    }
                    index_id += 1;
                }
            }
        }
    }

    /// Go `TestOneIndexFrame`.
    pub(crate) fn test_one_index_frame(&mut self, col_id: usize, f: OneColFn) {
        for table_id in 0..self.table_num {
            let table_name = format!("addindex.t{table_id}");
            if self.has_workload {
                self.workload_start(&[table_id, col_id]);
            }
            let fail_sync = self.is_failpoints_test.then(|| std::thread::spawn(move || use_failpoints(table_id)));
            let err = f(&self.shared, &mut self.tk, table_id, &table_name, col_id);
            if let Err(err) = &err {
                panic!("add index failed: {err}");
            }
            if self.has_workload {
                let _ = self.workload_stop();
            }
            if let Some(fail_sync) = fail_sync {
                fail_sync.join().unwrap();
            }
            if err.is_ok() {
                if self.shared.is_pk() {
                    check_table_result(&mut self.tk, &table_name);
                } else {
                    check_result(&mut self.tk, &table_name, col_id);
                }
            }
        }
    }
}

/// Go `AddIndexNonUnique`.
pub(crate) fn add_index_non_unique(shared: &Shared, tk: &mut Tk, table_id: usize, _: &str, index_id: usize) -> Result<(), String> {
    shared.is_pk.store(false, Ordering::SeqCst);
    shared.is_unique.store(false, Ordering::SeqCst);
    create_index_one_col(shared, tk, table_id, index_id)
}

/// Go `AddIndexUnique`.
pub(crate) fn add_index_unique(shared: &Shared, tk: &mut Tk, table_id: usize, table_name: &str, index_id: usize) -> Result<(), String> {
    shared.is_pk.store(false, Ordering::SeqCst);
    shared.is_unique.store(true, Ordering::SeqCst);
    let err = create_index_one_col(shared, tk, table_id, index_id);
    if index_id == 0 || index_id == 6 || index_id == 11 || index_id == 19 || table_id > 0 {
        match &err {
            Err(err) => panic!("add index failed: {err}"),
            Ok(()) => eprintln!("[add index test] add index success table name={table_name} index ID={index_id}"),
        }
    } else if let Err(err) = &err {
        assert!(err.contains("1062"), "{err}");
    }
    err
}

/// Go `AddIndexPK`.
pub(crate) fn add_index_pk(shared: &Shared, tk: &mut Tk, table_id: usize, _: &str, _: usize) -> Result<(), String> {
    shared.is_pk.store(true, Ordering::SeqCst);
    shared.is_unique.store(false, Ordering::SeqCst);
    create_index_one_col(shared, tk, table_id, 0)
}

/// Go `AddIndexGenCol`.
pub(crate) fn add_index_gen_col(shared: &Shared, tk: &mut Tk, table_id: usize, _: &str, _: usize) -> Result<(), String> {
    shared.is_pk.store(false, Ordering::SeqCst);
    shared.is_unique.store(false, Ordering::SeqCst);
    create_index_one_col(shared, tk, table_id, 29)
}

/// Go `AddIndexMultiCols`.
pub(crate) fn add_index_multi_cols(
    shared: &Shared,
    tk: &mut Tk,
    table_id: usize,
    _: &str,
    index_id: usize,
    col_id1: usize,
    col_id2: usize,
) -> Result<(), String> {
    shared.is_pk.store(false, Ordering::SeqCst);
    shared.is_unique.store(false, Ordering::SeqCst);
    if col_id1 != col_id2 {
        let err = create_index_two_cols(shared, tk, table_id, index_id, col_id1, col_id2);
        if let Err(err) = &err {
            panic!("add index failed: {err}");
        }
        return err;
    }
    Ok(())
}

/// Go `failpoints`.
const FAILPOINTS: [(&str, &str); 7] = [
    ("github.com/pingcap/tidb/pkg/ddl/mockHighLoadForAddIndex", "return"),
    ("github.com/pingcap/tidb/pkg/ddl/mockBackfillRunErr", "1*return"),
    ("github.com/pingcap/tidb/pkg/ddl/mockBackfillSlow", "return"),
    ("github.com/pingcap/tidb/pkg/ddl/MockCaseWhenParseFailure", "return(true)"),
    ("github.com/pingcap/tidb/pkg/ddl/mockHighLoadForMergeIndex", "return"),
    ("github.com/pingcap/tidb/pkg/ddl/mockMergeRunErr", "1*return"),
    ("github.com/pingcap/tidb/pkg/ddl/mockMergeSlow", "return"),
];

/// Go `useFailpoints`: one failpoint, chosen by `failpos`, for ten seconds.
fn use_failpoints(failpos: usize) {
    let (failpath, in_term) = FAILPOINTS[failpos % 7];
    tidb_failpoint::enable(failpath, in_term).unwrap();
    eprintln!("[add index test] enable failpoints {failpath}");
    std::thread::sleep(std::time::Duration::from_secs(10));
    tidb_failpoint::disable(failpath);
    eprintln!("[add index test] disable failpoints {failpath}");
}

// Go `workload.go`.

impl SuiteContext {
    /// Go `workload.start`.
    fn workload_start(&mut self, col_ids: &[usize]) {
        // Go `initWorkLoadContext`.
        if col_ids.len() >= 2 {
            let mut w = self.shared.workload.lock().unwrap();
            w.table_name = format!("t{}", col_ids[0]);
            w.col_id = col_ids[1..].to_vec();
            // set started insert id to 10000
            w.insert_id = 10000;
            w.date = "2008-02-02".to_owned();
        }
        self.workers.clear();
        for load_type in 0..3 {
            // Go `ctx.getTestKit()`: the pool makes a new connection, here on
            // the test's thread, as the store is not shared across threads.
            let tk = self.store.new_tk();
            let shared = Arc::clone(&self.shared);
            self.workers.push(std::thread::spawn(move || worker_run(&shared, tk, load_type)));
        }
    }

    /// Go `workload.stop`.
    fn workload_stop(&mut self) -> Result<(), String> {
        self.shared.cancelled.store(true, Ordering::SeqCst);
        for worker in self.workers.drain(..) {
            if let Err(err) = worker.join().unwrap() {
                panic!("workload: {err}");
            }
        }
        Ok(())
    }
}

/// Go `worker.run`; `load_type` 0 is insert, 1 update, 2 delete.
fn worker_run(shared: &Shared, mut tk: Tk, load_type: usize) -> Result<(), String> {
    tk.must_exec("use addindex");
    tk.must_exec("set @@tidb_general_log = 1;");
    let mut err = Ok(());
    loop {
        if shared.done() {
            break;
        }
        err = match load_type {
            0 => insert_worker(shared, &mut tk),
            1 => update_worker(shared, &mut tk),
            _ => delete_worker(shared, &mut tk),
        };
        if err.is_err() {
            break;
        }
    }
    err
}

/// Go `isSkippedError`.
fn is_skipped_error(err: &Result<Vec<Vec<String>>, String>, shared: &Shared) -> bool {
    let Err(err) = err else { return true };
    if (shared.is_pk() || shared.is_unique()) && err.find("1062").is_some_and(|pos| pos > 0) {
        return true;
    }
    SKIPPED_ERROR_CODE.iter().any(|code| err.contains(code))
}

/// Go `insertStr`.
fn insert_str(table_name: &str, id: i64, date: &str) -> String {
    format!(
        "insert into addindex.{table_name}(c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28) values({id},3, 3, 3, 3, 3, {id}, 3, 3.0, 3.0, 1113.1111, adddate('{date}', {}), '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', 'aaaa{id}', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{{\"name\": \"Beijing\", \"population\": 102}}')",
        id - 10000
    )
}

/// Go `insertWorker`.
fn insert_worker(shared: &Shared, tk: &mut Tk) -> Result<(), String> {
    let ins_str = {
        let w = shared.workload.lock().unwrap();
        insert_str(&w.table_name, w.insert_id, &w.date)
    };
    let rs = tk.exec(&ins_str);
    if !is_skipped_error(&rs, shared) {
        eprintln!("[add index test] workload insert failed sql={ins_str}");
        return rs.map(drop);
    }
    shared.workload.lock().unwrap().insert_id += 1;
    std::thread::sleep(std::time::Duration::from_millis(10));
    Ok(())
}

/// Go `updateStr`.
fn update_str(shared: &Shared, table_name: &str, col_id: &[usize]) -> String {
    let id = rand::random_range(0..=shared.row_num);
    let mut update_str = String::new();
    for (i, &col) in col_id.iter().enumerate() {
        let col_new_value = gen_colval(col);
        if col_new_value.is_empty() {
            return String::new();
        }
        if i == 0 {
            update_str = format!(" set c{col}={col_new_value}");
        } else {
            update_str = format!("{update_str}, c{col}={col_new_value}");
        }
    }
    format!("update addindex.{table_name}{update_str} where c0={id}")
}

/// Go `updateWorker`.
fn update_worker(shared: &Shared, tk: &mut Tk) -> Result<(), String> {
    let up_str = {
        let w = shared.workload.lock().unwrap();
        update_str(shared, &w.table_name, &w.col_id)
    };
    let rs = tk.exec(&up_str);
    if !is_skipped_error(&rs, shared) {
        eprintln!("[add index test] workload update failed sql={up_str}");
        return rs.map(drop);
    }
    std::thread::sleep(std::time::Duration::from_millis(10));
    Ok(())
}

/// Go `deleteWorker`.
fn delete_worker(shared: &Shared, tk: &mut Tk) -> Result<(), String> {
    let id = rand::random_range(0..=shared.row_num);
    let del_str = format!("delete from addindex.{} where c0 ={id}", shared.workload.lock().unwrap().table_name);
    let rs = tk.exec(&del_str);
    if !is_skipped_error(&rs, shared) {
        eprintln!("[add index test] workload delete failed sql={del_str}");
        return rs.map(drop);
    }
    std::thread::sleep(std::time::Duration::from_millis(10));
    Ok(())
}

/// Go `genColval`.
fn gen_colval(col_id: usize) -> String {
    match col_id {
        1 | 2 | 3 | 4 | 5 | 7 => format!("c{col_id} + 1"),
        6 => format!("c{col_id} - 64"),
        // Go `strconv.FormatFloat(f, 10, 16, 32)`: 10 is no format verb, so
        // Go renders `%` and the verb byte.
        8 | 9 | 10 => "%\n".to_owned(),
        11 => "adddate(c11, 90)".to_owned(),
        // Go `time.Now().String()`, unquoted.
        12..=15 => chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.9f %z %Z").to_string(),
        17 => chrono::Local::now().format("%Y").to_string(),
        19 => "c19 + c0".to_owned(),
        18 | 20..=27 => "ABCDEEEF".to_owned(),
        28 => "json_object('name', 'NanJing', 'population', 2566)".to_owned(),
        _ => String::new(),
    }
}

// Go `compatibility.go`.

/// Go `testType`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum TestType {
    NonUnique,
    Unique,
    Pk,
    GenIndex,
    MultiCols,
}

/// Go `CompatibilityContext`.
pub(crate) struct CompatibilityContext {
    pub(crate) is_multi_schema_change: bool,
    pub(crate) is_concurrent_ddl: bool,
    pub(crate) is_pitr: bool,
    executor: Vec<std::thread::JoinHandle<Result<(), String>>>,
    col_i_ids: Vec<Vec<usize>>,
    col_j_ids: Vec<Vec<usize>>,
    t_type: TestType,
}

/// Go `InitCompCtx`.
pub(crate) fn init_comp_ctx() -> Option<SuiteContext> {
    let mut ctx = init_test()?;
    init_comp_ctx_params(&mut ctx);
    Some(ctx)
}

/// Go `InitCompCtxParams`.
pub(crate) fn init_comp_ctx_params(ctx: &mut SuiteContext) {
    ctx.comp_ctx = Some(CompatibilityContext {
        is_multi_schema_change: false,
        is_concurrent_ddl: false,
        is_pitr: false,
        executor: Vec::new(),
        col_i_ids: Vec::new(),
        col_j_ids: Vec::new(),
        t_type: TestType::NonUnique,
    });
    ctx.shared.is_multi_schema_change.store(false, Ordering::SeqCst);
}

impl SuiteContext {
    /// Sets Go `CompCtx.IsMultiSchemaChange`.
    pub(crate) fn set_multi_schema_change(&mut self, on: bool) {
        self.comp_ctx.as_mut().expect("a compatibility context").is_multi_schema_change = on;
        self.shared.is_multi_schema_change.store(on, Ordering::SeqCst);
    }
}

/// Go `InitConcurrentDDLTest`.
pub(crate) fn init_concurrent_ddl_test(col_i_ids: &[&[usize]], col_j_ids: &[&[usize]], t_type: TestType) -> Option<SuiteContext> {
    let mut ctx = init_comp_ctx()?;
    let comp = ctx.comp_ctx.as_mut().unwrap();
    comp.is_concurrent_ddl = true;
    comp.t_type = t_type;
    comp.col_i_ids = col_i_ids.iter().map(|ids| ids.to_vec()).collect();
    comp.col_j_ids = col_j_ids.iter().map(|ids| ids.to_vec()).collect();
    Some(ctx)
}

impl SuiteContext {
    /// Go `CompatibilityContext.Start`.
    pub(crate) fn comp_start(&mut self) {
        let mut executors = Vec::with_capacity(3);
        for id in 0..3 {
            let tk = self.store.new_tk();
            let shared = Arc::clone(&self.shared);
            let comp = self.comp_ctx.as_ref().unwrap();
            let (t_type, col_i_ids, col_j_ids) = (comp.t_type, comp.col_i_ids.clone(), comp.col_j_ids.clone());
            executors.push(std::thread::spawn(move || executor_run(&shared, tk, id, t_type, &col_i_ids, &col_j_ids)));
        }
        self.comp_ctx.as_mut().unwrap().executor = executors;
    }

    /// Go `CompatibilityContext.Stop`.
    pub(crate) fn comp_stop(&mut self) -> Result<(), String> {
        for executor in self.comp_ctx.as_mut().unwrap().executor.drain(..) {
            match executor.join() {
                Ok(Err(err)) => panic!("{err}"),
                Ok(Ok(())) => {}
                Err(panic) => std::panic::resume_unwind(panic),
            }
        }
        Ok(())
    }
}

/// Go `executor.run`: executor `id` adds indexes on table `id` on its own
/// connection.
fn executor_run(
    shared: &Shared,
    mut tk: Tk,
    id: usize,
    t_type: TestType,
    col_i_ids: &[Vec<usize>],
    col_j_ids: &[Vec<usize>],
) -> Result<(), String> {
    match t_type {
        TestType::NonUnique => test_one_col_frame_para(shared, &mut tk, id, col_i_ids, add_index_non_unique),
        TestType::Unique => test_one_col_frame_para(shared, &mut tk, id, col_i_ids, add_index_unique),
        TestType::Pk => test_one_index_frame_para(shared, &mut tk, id, 0, add_index_pk),
        TestType::GenIndex => test_one_index_frame_para(shared, &mut tk, id, 29, add_index_gen_col),
        TestType::MultiCols => test_two_cols_frame_para(shared, &mut tk, id, col_i_ids, col_j_ids, add_index_multi_cols),
    }
}

/// Go `testOneColFramePara`.
fn test_one_col_frame_para(shared: &Shared, tk: &mut Tk, table_id: usize, col_ids: &[Vec<usize>], f: OneColFn) -> Result<(), String> {
    let table_name = format!("addindex.t{table_id}");
    for &i in &col_ids[table_id] {
        if let Err(err) = f(shared, tk, table_id, &table_name, i) {
            if shared.is_unique() || shared.is_pk() {
                assert!(err.contains("Duplicate entry"), "{err}");
                continue;
            }
            panic!("add index failed: {err}");
        }
        check_result(tk, &table_name, i);
    }
    Ok(())
}

/// Go `testTwoColsFramePara`.
fn test_two_cols_frame_para(
    shared: &Shared,
    tk: &mut Tk,
    table_id: usize,
    i_ids: &[Vec<usize>],
    j_ids: &[Vec<usize>],
    f: TwoColsFn,
) -> Result<(), String> {
    let table_name = format!("addindex.t{table_id}");
    let mut index_id = 0;
    for &i in &i_ids[table_id] {
        for &j in &j_ids[table_id] {
            if let Err(err) = f(shared, tk, table_id, &table_name, index_id, i, j) {
                panic!("add index failed: {err}");
            }
            if i != j {
                check_result(tk, &table_name, index_id);
            }
            index_id += 1;
        }
    }
    Ok(())
}

/// Go `testOneIndexFramePara`.
fn test_one_index_frame_para(shared: &Shared, tk: &mut Tk, table_id: usize, col_id: usize, f: OneColFn) -> Result<(), String> {
    let table_name = format!("addindex.t{table_id}");
    if let Err(err) = f(shared, tk, table_id, &table_name, col_id) {
        panic!("add index failed: {err}");
    }
    if shared.is_pk() {
        check_table_result(tk, &table_name);
    } else {
        check_result(tk, &table_name, col_id);
    }
    Ok(())
}
