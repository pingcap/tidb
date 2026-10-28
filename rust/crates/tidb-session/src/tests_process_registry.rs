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


//! The process registry's tests that run over a session.

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use std::sync::Arc;

use tidb_executor::process::*;
use tidb_util::memoryusagealarm::SessionManager;

use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct CountingTarget {
    queries: AtomicUsize,
    connections: AtomicUsize,
}

impl ProcessKillTarget for CountingTarget {
    fn cancel_query(&self) {
        self.queries.fetch_add(1, Ordering::AcqRel);
    }
    fn kill_connection(&self) {
        self.connections.fetch_add(1, Ordering::AcqRel);
    }
}

#[test]
fn internal_process_batch_shares_entries_without_exposing_idle_pool_sessions() {
    let registry = ProcessRegistry::default();
    let mut session = crate::Session::new();
    let guard = registry.register_internal(71, "test".into());
    let observer = guard.transaction_observer();
    observer.activated(123);
    session.attach_process(71, guard);
    assert_eq!(registry.internal_session_start_ts(), vec![123]);
    assert!(registry.snapshot().is_empty());
    assert!(registry.transaction_snapshot().is_empty());
    let process = session.system_process().unwrap();
    registry.track_system_process(901, process.clone()).unwrap();
    registry.track_system_process(901, process).unwrap();
    let _statement = session.retain_process_statement("ANALYZE TABLE test.t");
    assert_eq!(
        registry.snapshot()[0].info.as_deref(),
        Some("ANALYZE TABLE test.t")
    );
    assert_eq!(registry.snapshot()[0].id, 901);
    assert_eq!(registry.get_process_info(901).unwrap().id, 901);
    assert_eq!(registry.show_process_list().len(), 1);
    assert!(registry.transaction_snapshot().is_empty());
    tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST.tracker(901);
    let excluded = registry.internal_session_start_ts();
    tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST.untracker(901);
    assert!(excluded.is_empty());
    registry.untrack_system_process(901);
    assert!(registry.snapshot().is_empty());
    assert_eq!(registry.internal_session_start_ts(), vec![123]);
    drop(session);
    assert!(registry.internal_session_start_ts().is_empty());
}

#[test]
fn internal_process_batch_kills_both_modes_without_poisoning_reused_sessions() {
    let registry = ProcessRegistry::default();
    let mut session = crate::Session::new();
    session.attach_process(72, registry.register_internal(72, String::new()));
    for query in [true, false] {
        registry
            .track_system_process(902, session.system_process().unwrap())
            .unwrap();
        let memory = session.routed_statement_memory();
        assert!(memory.sql_killer().handle_signal().is_none());
        assert!(registry.kill(902, query));
        assert!(memory.sql_killer().handle_signal().is_some());
        registry.untrack_system_process(902);
        assert!(!registry.kill(902, query));
    }
    let _fresh_command = session.begin_query_cancellation();
    let memory = session.routed_statement_memory();
    assert!(memory.sql_killer().handle_signal().is_none());
}

#[test]
fn internal_process_batch_rejects_conflicting_ids_and_retires_dropped_sessions() {
    let registry = ProcessRegistry::default();
    let mut first = crate::Session::new();
    let mut second = crate::Session::new();
    // Internal membership follows session identity, not a possibly reused connection ID.
    first.attach_process(1, registry.register_internal(1, String::new()));
    second.attach_process(1, registry.register_internal(1, String::new()));
    first.transaction_observer().unwrap().activated(10);
    second.transaction_observer().unwrap().activated(20);
    registry
        .track_system_process(903, first.system_process().unwrap())
        .unwrap();
    assert_eq!(
        registry
            .track_system_process(903, second.system_process().unwrap())
            .unwrap_err(),
        "The ID is in use: 903"
    );
    drop(first);
    assert!(registry.snapshot().is_empty());
    assert_eq!(registry.internal_session_start_ts(), vec![20]);
    registry
        .track_system_process(903, second.system_process().unwrap())
        .unwrap();
    drop(second);
    assert!(registry.snapshot().is_empty());
    assert!(registry.internal_session_start_ts().is_empty());
}

#[test]
fn registered_connection_leaves_the_list_with_its_guard() {
    let registry = ProcessRegistry::default();
    let guard = registry.register(
        7,
        "alice".to_owned(),
        "127.0.0.1:1".to_owned(),
        "test".to_owned(),
        None,
    );
    let rows = registry.snapshot();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 7);
    assert_eq!(rows[0].command, "Sleep");
    assert_eq!(rows[0].info, None);
    drop(guard);
    assert!(registry.snapshot().is_empty());
}

#[test]
fn running_statement_becomes_info_and_is_cleared_again() {
    let registry = ProcessRegistry::default();
    let guard = registry.register(1, String::new(), String::new(), "test".to_owned(), None);
    registry.statement_started(1, "select 1", "autocommit");
    let rows = registry.snapshot();
    assert_eq!(rows[0].info.as_deref(), Some("select 1"));
    assert_eq!(rows[0].command, "Query");
    registry.statement_finished(1, "mysql", "autocommit");
    let rows = registry.snapshot();
    assert_eq!(rows[0].info, None);
    assert_eq!(rows[0].db, "mysql");

    {
        let _statement = guard.statement_started("select 2", "test", "autocommit");
        assert_eq!(registry.snapshot()[0].info.as_deref(), Some("select 2"));
        // The ordinary session publishes and finishes the same statement
        // while the server still owns its result set. Go keeps the
        // command visible until `writeResultSet` returns.
        registry.statement_started(1, "select 2", "autocommit");
        registry.statement_finished(1, "test", "autocommit");
        assert_eq!(registry.snapshot()[0].info.as_deref(), Some("select 2"));
    }
    assert_eq!(registry.snapshot()[0].info, None);
}

#[test]
fn prepared_statement_publishes_its_prepare_time_digest() {
    let registry = ProcessRegistry::default();
    let guard = registry.register(7, String::new(), String::new(), "test".to_owned(), None);
    registry.transaction_started(7, 42);
    // Go's EXECUTE installs the digest fixed at PREPARE
    // (`InitSQLDigest`); the text is not normalized again, so the
    // published digest is exactly the caller's.
    let prepared_digest = tidb_parser::normalize_digest("select c from t where id = ?")
        .1
        .to_string();
    {
        let _statement = guard.statement_started_with_digest(
            "select c from t where id = ?",
            Some(&prepared_digest),
            "test",
            "autocommit",
        );
        let transactions = registry.transaction_snapshot();
        assert_eq!(
            transactions[0].current_sql_digest.as_deref(),
            Some(prepared_digest.as_str())
        );
    }
    // A statement without a known digest still derives it from the text.
    {
        let _statement = guard.statement_started("select 1", "test", "autocommit");
        let derived = tidb_parser::normalize_digest("select 1").1.to_string();
        assert_eq!(
            registry.transaction_snapshot()[0]
                .current_sql_digest
                .as_deref(),
            Some(derived.as_str())
        );
    }
}

#[test]
fn kill_reaches_the_target_and_unknown_ids_report_missing() {
    let registry = ProcessRegistry::default();
    let target = Arc::new(CountingTarget::default());
    let _guard = registry.register(
        3,
        String::new(),
        String::new(),
        String::new(),
        Some(target.clone()),
    );
    assert!(registry.kill(3, true));
    assert_eq!(target.queries.load(Ordering::Acquire), 1);
    assert_eq!(target.connections.load(Ordering::Acquire), 0);
    assert!(registry.kill(3, false));
    assert_eq!(target.connections.load(Ordering::Acquire), 1);
    assert!(!registry.kill(999, false));
}

#[test]
fn show_truncates_info_at_a_hundred_runes_and_full_does_not() {
    let long = "x".repeat(150);
    assert_eq!(truncate_process_info(&long, false).len(), 100);
    assert_eq!(truncate_process_info(&long, true).len(), 150);
    let wide = "é".repeat(150);
    assert_eq!(truncate_process_info(&wide, false).chars().count(), 100);
}
