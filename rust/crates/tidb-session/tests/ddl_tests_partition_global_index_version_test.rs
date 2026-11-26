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

//! Go `pkg/ddl/tests/partition/global_index_version_test.go`.

use crate::support::testkit::{enable_failpoint, TestKit};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Go `TestGlobalIndexTruncatePartitionMVIndexKeepTakeoverEntry` (#71286):
/// while a partition is truncated, a concurrent write may take over an
/// element key the truncated partition owned, and the cleanup must keep it;
/// an element nobody took over is still cleaned. The decision is per element.
#[test]
fn test_global_index_truncate_partition_mv_index_keep_takeover_entry() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "CREATE TABLE t (
		p INT PRIMARY KEY,
		j JSON,
		UNIQUE KEY m ((CAST(j AS UNSIGNED ARRAY))) GLOBAL
	) PARTITION BY RANGE (p) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (20),
		PARTITION p2 VALUES LESS THAN MAXVALUE
	)",
    );
    tk.must_exec("INSERT INTO t VALUES (1, '[100]')"); // p0, only here to give p0 a row to clean first
    tk.must_exec("INSERT INTO t VALUES (11, '[7, 8]')"); // p1, owns elements 7 and 8

    let mut tk1 = tk.new_tk();
    tk1.must_exec("use test");
    // `MockDMLExecution` runs between the cleanup of p0 and the cleanup of
    // p1, the window in which a real concurrent write can take over element
    // 7. Element 8 is left alone so that the cleanup still has to remove it.
    let hook_ran = Arc::new(AtomicBool::new(false));
    let ran = Arc::clone(&hook_ran);
    let tk1 = Mutex::new(tk1);
    *tidb_ddl::index::MOCK_DML_EXECUTION.lock().unwrap() = Some(Box::new(move || {
        ran.store(true, Ordering::SeqCst);
        tk1.lock().unwrap().must_exec("INSERT INTO t VALUES (21, '[7]')");
    }));
    struct ClearHook;
    impl Drop for ClearHook {
        fn drop(&mut self) {
            *tidb_ddl::index::MOCK_DML_EXECUTION.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        }
    }
    let clear = ClearHook;
    let failpoint = enable_failpoint("github.com/pingcap/tidb/pkg/ddl/mockDMLExecution", "1*return(true)->return(false)");
    tk.must_exec("ALTER TABLE t TRUNCATE PARTITION p0, p1");
    drop(failpoint);
    drop(clear);
    assert!(hook_ran.load(Ordering::SeqCst), "mockDMLExecution never fired, the test premise is broken");

    // The row inserted during the DDL must keep its global index entry.
    tk.must_query("SELECT p FROM t ORDER BY p").check(["21"]);
    tk.must_exec("ADMIN CHECK TABLE t");
    tk.must_query("SELECT p FROM t WHERE 7 MEMBER OF (j)").check(["21"]);

    // The UNIQUE constraint must still hold for the taken-over element.
    let error = tk.exec_to_err("INSERT INTO t VALUES (22, '[7]')").expect("a duplicate element");
    assert!(error.message.contains("Duplicate entry"), "{error:?}");

    // Element 8 was owned only by the truncated partition, so it must be
    // reusable.
    tk.must_exec("INSERT INTO t VALUES (23, '[8]')");
    tk.must_query("SELECT p FROM t WHERE 8 MEMBER OF (j)").check(["23"]);
    tk.must_exec("ADMIN CHECK TABLE t");

    // Empty arrays must not shift the record-to-index mapping. A composite MV
    // index alongside another global index, including NULL and repeated
    // elements, whose expansion must match ordinary index writes.
    tk.must_exec(
        "CREATE TABLE t_cleanup (
		p INT PRIMARY KEY, c INT, j JSON,
		UNIQUE KEY m (c, (CAST(j AS UNSIGNED ARRAY))) GLOBAL,
		UNIQUE KEY g (c) GLOBAL
	) PARTITION BY RANGE (p) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN MAXVALUE
	)",
    );
    tk.must_exec("INSERT INTO t_cleanup VALUES (1, 1, '[]'), (2, 2, NULL), (3, 3, '[7, 7, 8]'), (11, 4, '[9]')");
    tk.must_exec("ALTER TABLE t_cleanup TRUNCATE PARTITION p0");
    tk.must_exec("ADMIN CHECK TABLE t_cleanup");
    tk.must_query("SELECT p FROM t_cleanup USE INDEX(g)").check(["11"]);
    tk.must_exec("INSERT INTO t_cleanup VALUES (12, 1, '[]'), (13, 2, NULL), (14, 3, '[7, 8]')");
    tk.must_query("SELECT p FROM t_cleanup WHERE c = 3 AND 7 MEMBER OF (j)").check(["14"]);
    tk.must_exec("ADMIN CHECK TABLE t_cleanup");
}
