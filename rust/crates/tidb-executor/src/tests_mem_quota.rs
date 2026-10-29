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

use tidb_util::sqlkiller::memory_exceed_for_query;
use tidb_vardef::OomAction;
use tidb_kv::mem_quota::*;
use std::sync::Arc;
use tidb_util::memory::Tracker;
use tidb_util::spill_storage::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};
use tidb_kv::mem_quota::KillStop;

#[derive(Default)]
struct NoopMemStateRecorder;

impl tidb_util::memory::RecordMemState for NoopMemStateRecorder {
    fn load(&self) -> Result<Option<tidb_util::memory::RuntimeMemStateV1>, String> {
        Ok(None)
    }

    fn store(&self, _: &tidb_util::memory::RuntimeMemStateV1) -> Result<(), String> {
        Ok(())
    }
}

fn test_mem_arbitrator() -> Arc<tidb_util::memory::MemArbitrator> {
    let arbitrator =
        tidb_util::memory::MemArbitrator::new(1024, 4, 3, 0, Box::new(NoopMemStateRecorder));
    assert!(arbitrator.auto_run(
        tidb_util::memory::MemArbitratorActions::default(),
        tidb_util::memory::DEF_AWAIT_FREE_POOL_ALLOC_ALIGN_SIZE,
        4,
        tidb_util::memory::DEF_TASK_TICK_DUR,
    ));
    arbitrator.set_work_mode(tidb_util::memory::ArbitratorWorkMode::Standard);
    arbitrator
}

fn test_spill_storage(name: &str) -> Arc<SpillStorage> {
    Arc::new(
        SpillStorage::open(SpillStorageSpec {
            path: std::env::temp_dir()
                .join(format!("tidb-executor-{name}-spill-{}", std::process::id())),
            quota_bytes: -1,
            encryption: SpillEncryptionMethod::Plaintext,
        })
        .unwrap(),
    )
}

#[test]
fn a_statement_root_registers_and_releases_its_global_arbitrator_pool() {
    let arbitrator = test_mem_arbitrator();
    let session = SessionMemory::new(4096, OomAction::Cancel, 97)
        .with_mem_arbitrator(Arc::clone(&arbitrator));
    let statement = session.statement();
    let operator = statement.operator_tracker(3);

    // `1024 / 1000 == 1`, so this crosses the source small-budget
    // threshold and must promote the statement into a root pool.
    operator.consume(2);
    let pool = arbitrator
        .find_root_pool(97)
        .entry
        .expect("statement must reach the process arbitrator");
    assert!(pool.pool().capacity() > 0);

    statement.finish_statement();
    assert_eq!(pool.pool().capacity(), 0);
    assert!(arbitrator.stop());
}

#[test]
fn nolimit_skips_global_memory_arbitration_but_reserve_promotes_immediately() {
    let arbitrator = test_mem_arbitrator();
    let session = SessionMemory::new(4096, OomAction::Cancel, 98)
        .with_mem_arbitrator(Arc::clone(&arbitrator));

    let bypass = session.statement_with_arbitration(None, 0, "");
    bypass.operator_tracker(3).consume(8);
    assert!(arbitrator.find_root_pool(98).entry.is_none());
    bypass.finish_statement();

    let reserved = session.statement_with_arbitration(Some(true), 64, "");
    let pool = arbitrator
        .find_root_pool(98)
        .entry
        .expect("a reserved statement starts in a root pool");
    assert!(pool.pool().capacity() >= 64);
    reserved.finish_statement();
    assert!(arbitrator.stop());
}

#[test]
fn the_shipped_default_is_cancel_at_one_gibibyte() {
    // Captured: `select @@tidb_mem_quota_query` -> 1073741824, and
    // `DefTiDBMemOOMAction` -> CANCEL.
    assert_eq!(OomAction::default(), OomAction::Cancel);
    assert_eq!(StatementMemory::default().quota(), 1_073_741_824);
}

#[test]
fn oom_action_parses_both_enum_values_and_defaults_to_protecting() {
    assert_eq!(OomAction::parse("CANCEL"), OomAction::Cancel);
    assert_eq!(OomAction::parse("cancel"), OomAction::Cancel);
    assert_eq!(OomAction::parse("LOG"), OomAction::Log);
    assert_eq!(OomAction::parse("log"), OomAction::Log);
    assert_eq!(OomAction::parse(""), OomAction::Cancel);
}

#[test]
fn the_quota_lives_on_the_session_root_and_operators_bubble_into_it() {
    let mem = StatementMemory::new(4096, OomAction::Cancel, 7);
    let op = mem.operator_tracker(3);
    assert_eq!(op.get_bytes_limit(), -1);
    assert_eq!(mem.stmt_tracker().get_bytes_limit(), -1);
    assert_eq!(mem.quota(), 4096);

    op.consume(1000);
    assert_eq!(mem.bytes_consumed(), 1000);
    assert!(mem.check().is_ok());
}

#[test]
fn cancel_raises_8175_once_the_operator_crosses_the_quota() {
    let mem = StatementMemory::new(4096, OomAction::Cancel, 7);
    let op = mem.operator_tracker(3);
    op.consume(4095);
    assert!(mem.check().is_ok(), "still inside the quota");
    op.consume(1);
    match mem.check() {
        Err(KillStop::MemoryExceedForQuery { conn_id }) => assert_eq!(conn_id, 7),
        other => panic!("expected 8175, got {other:?}"),
    }
}

#[test]
fn memory_error_preserves_the_unsigned_connection_id() {
    let error = memory_exceed_for_query(u64::MAX);
    assert_eq!(error.code, 8175);
    assert!(
        error.message.ends_with("[conn=18446744073709551615]"),
        "{}",
        error.message
    );
}

#[test]
fn a_session_gives_each_statement_fresh_cancellation_state() {
    let session = SessionMemory::new(1, OomAction::Cancel, 7);

    let first = session.statement();
    let first_op = first.operator_tracker(3);
    first_op.consume(1);
    assert!(matches!(
        first.check(),
        Err(KillStop::MemoryExceedForQuery { conn_id: 7 })
    ));
    first.finish_statement();
    assert_eq!(session.bytes_consumed(), 0);

    let second = session.statement();
    assert!(
        second.check().is_ok(),
        "the previous kill is statement-local"
    );
    let second_op = second.operator_tracker(3);
    second_op.consume(1);
    assert!(matches!(
        second.check(),
        Err(KillStop::MemoryExceedForQuery { conn_id: 7 })
    ));
}

#[test]
fn session_tracker_defers_kill_to_the_typed_statement_boundary() {
    let session = SessionMemory::new(4096, OomAction::Cancel, 7);
    let cancellation = session.begin_query_cancellation();
    let statement = session.statement();
    cancellation.cancel();

    statement.operator_tracker(3).consume(1);
    match statement.check() {
        Err(KillStop::Killed(error)) => {
            assert_eq!(error.code, 1317);
            assert_eq!(error.state, "70100");
        }
        other => panic!("expected typed query interruption, got {other:?}"),
    }
}

#[test]
fn closing_a_retained_cursor_cannot_remove_the_next_statement_action() {
    let session = SessionMemory::new(1, OomAction::Cancel, 7);
    let cursor = Tracker::new(tidb_util::memory::LABEL_FOR_CURSOR_FETCH, -1);
    let first = session.statement();
    cursor.attach_to(first.session_tracker());
    cursor.detach();
    first.finish_statement();

    let second = session.statement();
    let op = second.operator_tracker(3);
    op.consume(1);
    assert!(matches!(
        second.check(),
        Err(KillStop::MemoryExceedForQuery { conn_id: 7 })
    ));
}

#[test]
fn retained_cursor_bytes_count_against_the_next_statement() {
    let session = SessionMemory::new(10, OomAction::Cancel, 7);
    let first = session.statement();
    let cursor = Tracker::new(tidb_util::memory::LABEL_FOR_CURSOR_FETCH, -1);
    cursor.attach_to(first.session_tracker());
    cursor.consume(6);
    first.finish_statement();
    assert_eq!(session.bytes_consumed(), 6);

    let second = session.statement();
    let op = second.operator_tracker(3);
    op.consume(4);
    assert!(matches!(
        second.check(),
        Err(KillStop::MemoryExceedForQuery { conn_id: 7 })
    ));
    cursor.detach();
    assert_eq!(session.bytes_consumed(), 4);
}

#[test]
fn final_statement_handle_drop_detaches_its_accounting_tree() {
    let session = SessionMemory::new(10, OomAction::Cancel, 7);
    let statement = session.statement();
    let clone = statement.clone();
    let op = statement.operator_tracker(3);
    op.consume(4);
    drop(statement);
    assert_eq!(session.bytes_consumed(), 4, "one statement handle remains");
    drop(clone);
    assert_eq!(
        session.bytes_consumed(),
        0,
        "final handle detaches the tree"
    );

    let next = session.statement();
    assert!(next.check().is_ok());
    drop(op);

    // A worker may override its configuration, but Go's retained
    // statement context keeps the same accounting lifetime.
    let context = crate::StmtContext::for_query_with_memory(next);
    let worker = context.clone().with_resource_group_name("worker");
    let op = context.statement_memory().operator_tracker(3);
    op.consume(4);
    drop(context);
    assert_eq!(session.bytes_consumed(), 4);
    drop(worker);
    assert_eq!(session.bytes_consumed(), 0);
    drop(op);
}

#[test]
fn log_never_fails_the_statement_however_far_it_overruns() {
    let mem = StatementMemory::new(4096, OomAction::Log, 7);
    let op = mem.operator_tracker(3);
    op.consume(1 << 30);
    assert!(mem.check().is_ok());
    assert_eq!(mem.bytes_consumed(), 1 << 30);
}

#[test]
fn a_non_positive_quota_is_go_s_no_limit() {
    let mem = StatementMemory::new(-1, OomAction::Cancel, 0);
    let op = mem.operator_tracker(3);
    op.consume(1 << 40);
    assert!(mem.check().is_ok());
}

#[test]
fn statement_disk_usage_reaches_the_one_startup_global_tracker() {
    let storage = test_spill_storage("global-hierarchy");
    let mem = StatementMemory::new(4096, OomAction::Cancel, 7)
        .with_spill_storage(Arc::clone(&storage));
    let operator = mem.operator_disk_tracker(42);

    operator.consume(64);
    assert_eq!(mem.lifetime.disk_stmt.bytes_consumed(), 64);
    assert_eq!(mem.disk_session.bytes_consumed(), 64);
    assert_eq!(storage.global_tracker().bytes_consumed(), 64);

    operator.consume(-64);
    assert_eq!(storage.global_tracker().bytes_consumed(), 0);
    let path = storage.path().to_owned();
    drop(operator);
    drop(mem);
    drop(storage);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn disabling_tmp_storage_detaches_the_session_from_global_disk_quota() {
    let storage = test_spill_storage("disabled-hierarchy");
    let mem = StatementMemory::new(4096, OomAction::Cancel, 7)
        .with_spill_storage(Arc::clone(&storage))
        .with_tmp_storage_on_oom(false);
    let operator = mem.operator_disk_tracker(42);

    operator.consume(64);
    assert_eq!(mem.lifetime.disk_stmt.bytes_consumed(), 64);
    assert_eq!(mem.disk_session.bytes_consumed(), 64);
    assert_eq!(storage.global_tracker().bytes_consumed(), 0);
    let path = storage.path().to_owned();
    drop(operator);
    drop(mem);
    drop(storage);
    std::fs::remove_dir_all(path).unwrap();
}
