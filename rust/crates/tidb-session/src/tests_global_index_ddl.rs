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


//! A global index read while a partition DDL runs: Go
//! `PhysicalIndexScan.AddSelectionConditionForGlobalIndex` keeps the
//! partitions `IDsInDDLToIgnore` names out of a full read.

use std::any::Any;
use std::sync::{Arc, Mutex};

use tidb_model::Job;

use super::Session;

fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    crate::tests_support::row_text(session.run(sql))
        .into_iter()
        .map(|row| row.join(" "))
        .collect()
}

fn partition_id(session: &mut Session, name: &str) -> String {
    rows(
        session,
        &format!(
            "select tidb_partition_id from information_schema.partitions \
             where table_schema = 'test' and table_name = 't' and partition_name = '{name}'"
        ),
    )
    .remove(0)
}

/// A Go testkit capture (`zzprobe/gidxddl`, failpoints on): EXPLAIN from a
/// second session at each `afterWaitSchemaSynced` of `alter table t truncate
/// partition p0` and `alter table t drop partition p2`, over
/// `create table t (a int, b int, unique key ub (b) global) partition by
/// range(a) (partition p0 values less than (2), partition p1 values less than
/// (3), partition p2 values less than (10))`.
#[test]
fn a_global_index_read_hides_the_partitions_a_ddl_removes_like_go() {
    let domain = crate::domain::mock_store::MockStoreDomain::create().expect("boot the mock store");
    let mut tk = domain.new_session().expect("a session");
    for sql in [
        "use test",
        "create table t (a int, b int, unique key ub (b) global) partition by range(a) \
         (partition p0 values less than (2), partition p1 values less than (3), partition p2 values less than (10))",
        "insert into t values (1,1),(2,2),(3,3)",
    ] {
        tk.run(sql).unwrap();
    }
    let old_p0 = partition_id(&mut tk, "p0");
    let p1 = partition_id(&mut tk, "p1");
    let p2 = partition_id(&mut tk, "p2");
    let seen: Arc<Mutex<Vec<(String, Vec<String>)>>> = Arc::default();
    let record = Arc::clone(&seen);
    let hook_domain = Arc::clone(&domain);
    let guard = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced",
        move |job: &mut dyn Any| {
            let job = job.downcast_mut::<Job>().expect("the step's job");
            if !matches!(
                job.type_,
                tidb_model::ActionType::ACTION_TRUNCATE_TABLE_PARTITION | tidb_model::ActionType::ACTION_DROP_TABLE_PARTITION
            ) {
                return;
            }
            let mut tk2 = hook_domain.new_session().expect("a second session");
            tk2.run("use test").unwrap();
            let mut explained = rows(&mut tk2, "explain format='brief' select b from t use index(ub) where b > 0");
            explained.extend(rows(&mut tk2, "explain format='brief' select b from t partition(p1) use index(ub) where b > 0"));
            record.lock().unwrap().push((format!("{} {}", job.type_, job.schema_state), explained));
        },
    );
    tk.run("alter table t truncate partition p0").unwrap();
    let new_p0 = partition_id(&mut tk, "p0");
    tk.run("alter table t drop partition p2").unwrap();
    drop(guard);

    let scan = "  └─IndexRangeScan 3333.33 cop[tikv] table:t, index:ub(b) range:(0,+inf], keep order:false, stats:pseudo";
    let full = |hidden: Option<&str>| match hidden {
        Some(ids) => vec![
            "IndexReader 3333.33 root partition:all index:Selection".to_owned(),
            format!("└─Selection 3333.33 cop[tikv]  not(in(_tidb_tid, {ids}))"),
            scan.to_owned(),
        ],
        None => vec![
            "IndexReader 3333.33 root partition:all index:IndexRangeScan".to_owned(),
            scan.trim_start_matches("  ").to_owned(),
        ],
    };
    let only_p1 = vec![
        "IndexReader 3333.33 root partition:p1 index:Selection".to_owned(),
        format!("└─Selection 3333.33 cop[tikv]  in(_tidb_tid, {p1})"),
        scan.to_owned(),
    ];
    let with_p1 = |mut rows: Vec<String>| {
        rows.extend(only_p1.iter().cloned());
        rows
    };
    let pruned = vec![
        "IndexReader 3333.33 root partition:p0,p1 index:Selection".to_owned(),
        format!("└─Selection 3333.33 cop[tikv]  in(_tidb_tid, {new_p0}, {p1})"),
        scan.to_owned(),
    ];
    let truncate = "truncate partition";
    let drop_partition = "drop partition";
    let expected = vec![
        (format!("{truncate} write only"), with_p1(full(Some(&new_p0)))),
        (format!("{truncate} delete only"), with_p1(full(Some(&old_p0)))),
        (format!("{truncate} delete reorganization"), with_p1(full(Some(&old_p0)))),
        (format!("{truncate} delete reorganization"), with_p1(full(Some(&old_p0)))),
        (format!("{truncate} none"), with_p1(full(None))),
        (format!("{truncate} none"), with_p1(full(None))),
        (format!("{drop_partition} write only"), with_p1(pruned)),
        (format!("{drop_partition} delete only"), with_p1(full(Some(&p2)))),
        (format!("{drop_partition} delete reorganization"), with_p1(full(Some(&p2)))),
    ];
    let seen = seen.lock().unwrap();
    for (index, (state, rows)) in expected.iter().enumerate() {
        assert_eq!(seen.get(index), Some(&(state.clone(), rows.clone())), "step {index}");
    }
}
