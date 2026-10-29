// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The repository's sysvars on a node: `SET GLOBAL` runs the `SetGlobal`
//! hooks `worker.go`'s `init` registers.

use std::sync::Arc;

use tidb_session::domain::cluster_session_node::DomainSessions as _;
use tidb_session::domain::configured_user_store::ConfiguredUserStore;
use tidb_domain_core::node_config::{NodeConfig, NodeOptions};
use tidb_session::domain::unistore_node::{unistore_cluster_session_stack, UnistoreClusterStack};
use tidb_session::{Session, StmtResult};

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

fn scalar_text(session: &mut Session, sql: &str) -> Option<String> {
    match session.run(sql).unwrap() {
        StmtResult::Rows(rows) => match &rows[0][0] {
            tidb_datatype::Datum::Null => None,
            tidb_datatype::Datum::Int(value) => Some(value.to_string()),
            tidb_datatype::Datum::UInt(value) => Some(value.to_string()),
            tidb_datatype::Datum::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
            tidb_datatype::Datum::String(value) => value.as_utf8().ok().map(str::to_owned),
            other => panic!("unexpected value {other:?}"),
        },
        other => panic!("expected rows, got {other:?}"),
    }
}

// Transcreated from pinned Go `pkg/util/workloadrepo.TestSettingSQLVariables`.
#[test]
fn test_setting_sql_variables() {
    let stack = unistore_stack();
    let worker = tidb_workloadrepo::Worker::new(None, None, None, "worker");
    tidb_workloadrepo::node::register_sysvar_hooks(&stack.factory.global_vars(), &worker);
    let mut session = stack.factory.open_storage_session(1).expect("session");

    for (statement, expected) in [
        (
            "SET GLOBAL tidb_workload_repository_active_sampling_interval = -1",
            "0",
        ),
        (
            "SET GLOBAL tidb_workload_repository_snapshot_interval = 899",
            "900",
        ),
        (
            "SET GLOBAL tidb_workload_repository_retention_days = -1",
            "0",
        ),
        (
            "SET GLOBAL tidb_workload_repository_active_sampling_interval = 601",
            "600",
        ),
        (
            "SET GLOBAL tidb_workload_repository_snapshot_interval = 7201",
            "7200",
        ),
        (
            "SET GLOBAL tidb_workload_repository_retention_days = 366",
            "365",
        ),
    ] {
        session.run(statement).unwrap();
        let name = statement
            .split_ascii_whitespace()
            .nth(2)
            .expect("SET GLOBAL variable name");
        assert_eq!(
            scalar_text(&mut session, &format!("SELECT @@global.{name}")),
            Some(expected.to_owned())
        );
    }

    for name in [
        "tidb_workload_repository_active_sampling_interval",
        "tidb_workload_repository_snapshot_interval",
        "tidb_workload_repository_retention_days",
    ] {
        assert!(session
            .run(&format!("SET GLOBAL {name} = 'invalid'"))
            .is_err());
    }

    session
        .run("SET GLOBAL tidb_workload_repository_dest = 'table'")
        .unwrap();
    assert!(worker.enabled());
    session
        .run("SET GLOBAL tidb_workload_repository_dest = ''")
        .unwrap();
    assert!(!worker.enabled());
    assert!(session
        .run("SET GLOBAL tidb_workload_repository_dest = 'invalid'")
        .is_err());
}
