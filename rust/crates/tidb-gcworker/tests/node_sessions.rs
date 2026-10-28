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

//! The node's GC sessions (Go `session.CreateSession` for the GC worker)
//! against an embedded store.

use std::sync::Arc;

use tidb_gcworker::node::NodeGcSessions;
use tidb_gcworker::seams::{DelRangeTask, GcSessionFactory};
use tidb_session::domain::cluster_session_node::DomainSessions as _;
use tidb_session::domain::configured_user_store::ConfiguredUserStore;
use tidb_session::domain::node_config::{NodeConfig, NodeOptions};
use tidb_session::domain::unistore_node::{unistore_cluster_session_stack, UnistoreClusterStack};

fn unistore_stack() -> UnistoreClusterStack {
    let config = NodeConfig::with_defaults(
        |config| {
            config.store = tidb_config::store::StoreType("unistore".to_owned());
            config.port = 0;
        },
        NodeOptions {
            auth_file: Some("/dev/null".to_owned()),
            ..NodeOptions::default()
        },
    )
    .expect("node config");
    let users = Arc::new(
        ConfiguredUserStore::parse("root\t%\tmysql_native_password\t\n")
            .expect("configured user store"),
    );
    unistore_cluster_session_stack(&config, &users, None, None).expect("unistore stack")
}

/// The GC worker's sessions run Go's statements against the node: the
/// `mysql.tidb` load and upsert (`loadValueFromSysTable`,
/// `saveValueToSysTable`) and `pkg/ddl/util`'s delete-range helpers
/// (`LoadDeleteRanges`, `CompleteDeleteRange`, `LoadDoneDeleteRanges`,
/// `DeleteDoneRecord`, `IsRaftKv2`), as Go's `TestDeleteRangesFailure` sets
/// the tables up.
#[test]
fn gc_worker_sessions_run_gos_system_table_statements() {
    let stack = unistore_stack();
    let sessions = NodeGcSessions::new(
        &stack.factory,
        Arc::new(|_| Ok(None)),
    );
    let mut session = sessions.create_session();

    // A missing row loads as nothing; the upsert inserts, then updates.
    assert_eq!(None, session.load_value("tikv_gc_test_key").unwrap());
    session
        .save_value("tikv_gc_test_key", "v1", "the comment")
        .unwrap();
    assert_eq!(
        Some("v1".to_owned()),
        session.load_value("tikv_gc_test_key").unwrap()
    );
    session
        .save_value("tikv_gc_test_key", "v2", "the comment")
        .unwrap();
    assert_eq!(
        Some("v2".to_owned()),
        session.load_value("tikv_gc_test_key").unwrap()
    );

    let mut sql = stack.factory.open_storage_session(1).expect("session");
    sql.node_conn()
        .execute_internal(
            "INSERT INTO mysql.gc_delete_range VALUES \
         (1, 2, '31', '32', 10), (3, 4, '33', '34', 10), (5, 6, '35', '36', 30)",
    )
    .unwrap();
    let task = |job_id, element_id, start: &[u8], end: &[u8]| DelRangeTask {
        job_id,
        element_id,
        start_key: start.to_vec(),
        end_key: end.to_vec(),
    };
    // `ts < safe point`, the keys hex-decoded.
    let ranges = session.load_delete_ranges(false, 20).unwrap();
    assert_eq!(vec![task(1, 2, b"1", b"2"), task(3, 4, b"3", b"4")], ranges);

    // Completed with a done record, the row moves to the done table.
    session.complete_delete_range(&ranges[0], true).unwrap();
    assert_eq!(
        vec![task(3, 4, b"3", b"4")],
        session.load_delete_ranges(false, 20).unwrap()
    );
    assert_eq!(
        vec![task(1, 2, b"1", b"2")],
        session.load_delete_ranges(true, 20).unwrap()
    );
    // raft-kv2 records no done row.
    session.complete_delete_range(&ranges[1], false).unwrap();
    assert!(session.load_delete_ranges(false, 20).unwrap().is_empty());
    assert_eq!(1, session.load_delete_ranges(true, 20).unwrap().len());

    session.delete_done_record(&ranges[0]).unwrap();
    assert!(session.load_delete_ranges(true, 20).unwrap().is_empty());

    // The embedded store reports no TiKV `storage.engine`.
    assert!(!session.is_raft_kv2().unwrap());
    assert!(session.history_job(1).unwrap().is_none());
}
