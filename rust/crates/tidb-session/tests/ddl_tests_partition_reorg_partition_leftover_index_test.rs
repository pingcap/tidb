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

//! Go `pkg/ddl/tests/partition/reorg_partition_leftover_index_test.go`.
//! `TestReorgPartitionRollbackResumeAfterOwnerChange` is not ported: the test
//! kit has no seam to resign the DDL owner between two job steps.

use crate::support::testkit::TestKit;
use std::sync::{Arc, Mutex};
use tidb_infoschema::TableEntry;
use tidb_model::{ActionType, SchemaState};

const LEFTOVER_INDEX_TABLE_SQL: &str = "create table t (id bigint not null, a int, s int, ct int not null,
	unique key uk_id (id, ct),
	key idx_l (s),
	key idx_g (a, s, ct) global)
	partition by range (ct) (
	partition p0 values less than (10),
	partition p1 values less than (20),
	partition pMax values less than (maxvalue))";

/// Go `errno.ErrCancelledDDLJob`.
const ERR_CANCELLED_DDL_JOB: u16 = 8214;

fn indices(tk: &TestKit) -> Vec<(String, SchemaState)> {
    match tk.get_table_by_name("test", "t") {
        TableEntry::Kv(table) => table
            .meta()
            .read()
            .indices
            .iter_deref()
            .map(|index| (index.read().name.original().to_owned(), index.read().state))
            .collect(),
        _ => unreachable!("t is a kv table"),
    }
}

/// Go `countUpdateMutations`: the keys one UPDATE stages, `txn.Len()` as
/// `TIDB_TRX.MEM_BUFFER_KEYS` reports it.
fn count_update_mutations(tk: &mut TestKit) -> String {
    tk.must_exec("begin pessimistic");
    tk.must_exec("update t set a = a + 1 where id = 3");
    let keys = tk
        .must_query("select MEM_BUFFER_KEYS from information_schema.TIDB_TRX where SESSION_ID = connection_id()")
        .rows()[0][0]
        .clone();
    tk.must_exec("rollback");
    keys
}

/// Go `TestReorgPartitionCancelNonUniqueGlobalIndex`: a cancelled partition
/// reorganization removes every index copy it made, unique or not, after one
/// schema version in delete only, from any state it is cancelled in.
#[test]
fn test_reorg_partition_cancel_non_unique_global_index() {
    let mut tk = TestKit::new();
    tk.must_exec(LEFTOVER_INDEX_TABLE_SQL);
    tk.must_exec("insert into t values (1, 0, 1, 5), (2, 0, 1, 15), (3, 0, 1, 25), (4, 0, 1, 35)");
    let base_indices = indices(&tk).len();
    let base_mutations = count_update_mutations(&mut tk);

    let cancel_state = Arc::new(Mutex::new(SchemaState::NONE));
    let last_rollback_states = Arc::new(Mutex::new(Vec::<SchemaState>::new()));
    let observer = Mutex::new(tk.new_tk());
    let canceller = Mutex::new(tk.new_tk());
    let (cancel_at, rollback_states) = (Arc::clone(&cancel_state), Arc::clone(&last_rollback_states));
    let _hook = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", move |arg| {
        let job = arg.downcast_mut::<tidb_model::Job>().unwrap();
        if job.type_ != ActionType::ACTION_REORGANIZE_PARTITION && job.type_ != ActionType::ACTION_ALTER_TABLE_PARTITIONING {
            return;
        }
        if job.is_rollingback() {
            let states = indices(&observer.lock().unwrap())
                .into_iter()
                .map(|(_, state)| state)
                .filter(|state| *state != SchemaState::PUBLIC)
                .collect();
            *rollback_states.lock().unwrap() = states;
            return;
        }
        let mut cancel_at = cancel_at.lock().unwrap();
        if *cancel_at == SchemaState::NONE || job.schema_state != *cancel_at {
            return;
        }
        *cancel_at = SchemaState::NONE;
        canceller.lock().unwrap().must_exec(&format!("admin cancel ddl jobs {}", job.id));
    });

    for ddl in [
        "alter table t reorganize partition pMax into (partition p2 values less than (30), partition pMax values less than (maxvalue))",
        "alter table t partition by range (ct) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than (30), partition pMax values less than (maxvalue))",
    ] {
        for state in [
            SchemaState::DELETE_ONLY,
            SchemaState::WRITE_ONLY,
            SchemaState::WRITE_REORGANIZATION,
            SchemaState::DELETE_REORGANIZATION,
        ] {
            *cancel_state.lock().unwrap() = state;
            last_rollback_states.lock().unwrap().clear();
            tk.must_get_err_code(ddl, ERR_CANCELLED_DDL_JOB);
            // The copies must be delete only in the step that removes them.
            let states = last_rollback_states.lock().unwrap().clone();
            assert!(!states.is_empty(), "{ddl}, cancel at {state}");
            for s in &states {
                assert_eq!(*s, SchemaState::DELETE_ONLY, "{ddl}, cancel at {state}");
            }
            let after = indices(&tk);
            assert_eq!(after.len(), base_indices, "{ddl}, cancel at {state}: {after:?}");
            for (name, index_state) in &after {
                assert_eq!(*index_state, SchemaState::PUBLIC, "{ddl}, cancel at {state}, index {name}");
            }
            assert_eq!(count_update_mutations(&mut tk), base_mutations, "{ddl}, cancel at {state}");
            tk.must_exec("admin check table t");
        }
    }
}

/// Go `TestReorgPartitionGlobalIndexCopySharedColumns`: removing the copy of
/// an expression index keeps the hidden column the original still uses, and
/// the column flag the other index on `a` gives.
#[test]
fn test_reorg_partition_global_index_copy_shared_columns() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "create table t (id bigint not null, a int, ct int not null,
		key idx_a (a) global,
		key idx_e ((a + 1)) global)
		partition by range (ct) (
		partition p0 values less than (10),
		partition pMax values less than (maxvalue))",
    );
    tk.must_exec("insert into t values (1, 1, 5), (2, 2, 15), (3, 3, 25)");
    let check = |tk: &mut TestKit| {
        tk.must_exec("admin check table t");
        tk.must_query("show columns from t where Field = 'a'").check_at(&[3], ["MUL"]);
        tk.must_query("select count(*) from t use index(idx_e) where a + 1 > 2").check(["2"]);
    };

    let cancel_at_write_reorg = Mutex::new(true);
    let canceller = Mutex::new(tk.new_tk());
    let _hook = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", move |arg| {
        let job = arg.downcast_mut::<tidb_model::Job>().unwrap();
        let mut cancel = cancel_at_write_reorg.lock().unwrap();
        if !*cancel || job.type_ != ActionType::ACTION_REORGANIZE_PARTITION || job.schema_state != SchemaState::WRITE_REORGANIZATION {
            return;
        }
        *cancel = false;
        canceller.lock().unwrap().must_exec(&format!("admin cancel ddl jobs {}", job.id));
    });
    let ddl = "alter table t reorganize partition pMax into (partition p1 values less than (20), partition pMax values less than (maxvalue))";
    tk.must_get_err_code(ddl, ERR_CANCELLED_DDL_JOB);
    check(&mut tk);
    tk.must_exec(ddl);
    check(&mut tk);
}
