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

//! Go `checkPartitionReplica`: ADD PARTITION of a table whose TiFlash
//! replica is available waits in the replica-only state for each new
//! partition's regions to get a TiFlash peer.

use super::*;

struct FailpointGuard(&'static [&'static str]);

impl Drop for FailpointGuard {
    fn drop(&mut self) {
        for name in self.0 {
            fail::remove(name);
        }
    }
}

/// A partitioned `ddltiflash` with an available TiFlash replica of
/// `replica_count`; answers its table id.
fn available_replica_table(
    stack: &UnistoreClusterStack,
    session: &mut Session,
    replica_count: u64,
) -> i64 {
    rows(session, "CREATE DATABASE IF NOT EXISTS test");
    rows(session, "use test");
    rows(
        session,
        "create table ddltiflash(z int) PARTITION BY RANGE(z) (PARTITION p0 VALUES LESS THAN (10))",
    );
    rows(session, &format!("alter table ddltiflash set tiflash replica {replica_count}"));
    let table_id: i64 = displayed(rows(
        session,
        "select tidb_table_id from information_schema.tables where table_name = 'ddltiflash'",
    ))[0][0]
        .parse()
        .unwrap();
    stack
        .factory
        .ddl
        .execute(
            &tidb_ddl::cluster_ddl::DdlStatement::UpdateTiFlashReplicaStatus {
                table_id,
                available: true,
            },
            &tidb_sessionctx::StmtContext::for_query(),
        )
        .expect("the replica turns available");
    table_id
}

fn partition_names(session: &mut Session) -> Vec<Vec<String>> {
    displayed(rows(
        session,
        "select partition_name from information_schema.partitions where table_name = 'ddltiflash' order by partition_ordinal_position",
    ))
}

pub(super) fn tiflash_stack(nodes: usize) -> UnistoreClusterStack {
    // Go's tests never split a table (`ddl.EnableSplitTableRegion` stays 0
    // outside tidb-server's main), so the mock TiFlash peer of the store's
    // one region covers every new partition.
    let mut global = (*tidb_config::config_tree::config::get_global_config()).clone();
    global.split_table = false;
    tidb_config::config_tree::config::store_global_config(global);
    let config = tidb_domain_core::node_config::NodeConfig::with_defaults(
        |config| {
            config.store = tidb_config::store::StoreType("unistore".to_owned());
            config.port = 0;
        },
        tidb_domain_core::node_config::NodeOptions {
            auth_file: Some("/dev/null".to_owned()),
            ..tidb_domain_core::node_config::NodeOptions::default()
        },
    )
    .expect("node config");
    let users = Arc::new(
        ConfiguredUserStore::parse(&format!("root\t%\tmysql_native_password\t{ABC_HASH}\n"))
            .expect("configured user store"),
    );
    crate::domain::unistore_node::unistore_cluster_session_stack_from(&config, &users, None, None, None, nodes)
        .expect("unistore stack")
}

/// Go `TestTiFlashAvailableAfterAddPartition`: the new partition's regions
/// carry the mock TiFlash store's peer, so the partition is added, joins
/// the available partitions and reports full progress.
#[test]
fn add_partition_keeps_an_available_tiflash_replica_available_like_go() {
    let stack = tiflash_stack(1);
    let mut session = stack.factory.open_session(session_context(2101)).unwrap();
    available_replica_table(&stack, &mut session, 1);
    // A replica that never completes fails the job at its first retry
    // instead of the default 512.
    rows(&mut session, "set @@global.tidb_ddl_error_count_limit = 1");

    rows(&mut session, "ALTER TABLE ddltiflash ADD PARTITION (PARTITION pn VALUES LESS THAN (20))");
    assert_eq!(partition_names(&mut session), [["p0"], ["pn"]]);
    let pn: i64 = displayed(rows(
        &mut session,
        "select tidb_partition_id from information_schema.partitions where table_name = 'ddltiflash' and partition_name = 'pn'",
    ))[0][0]
        .parse()
        .unwrap();
    assert_eq!(
        tidb_domain_infosync::tiflash_manager::get_tiflash_progress_from_cache(pn),
        Some(1.0)
    );
    // `information_schema.tiflash_replica` is read on the DDL owner's status
    // port, which this in-process node does not serve; the replica info is
    // what that reader would return.
    let (_, info) = stack
        .factory
        .catalog_snapshot()
        .table_info_by_name("test", "ddltiflash")
        .unwrap();
    let info = info.read();
    assert!(info.tiflash_replica.as_ref().unwrap().read().available);
}

/// Go `checkPartitionReplica` refuses a replica count above the TiFlash
/// stores PD knows, and the ADD PARTITION rolls back.
#[test]
fn add_partition_rolls_back_when_tiflash_stores_are_too_few_like_go() {
    // Go `TestSetTableFlashReplica`'s failpoint lets the replica be set
    // without TiFlash stores.
    fail::cfg("mockTiFlashStoreCount", "return(true)").expect("enable failpoint");
    let _guard = FailpointGuard(&["mockTiFlashStoreCount"]);
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2102)).unwrap();
    available_replica_table(&stack, &mut session, 1);
    // A refusal that turned into a replica wait fails at its first retry
    // instead of the default 512.
    rows(&mut session, "set @@global.tidb_ddl_error_count_limit = 1");

    let error = session
        .node_conn()
        .execute("ALTER TABLE ddltiflash ADD PARTITION (PARTITION pn VALUES LESS THAN (20))")
        .err()
        .expect("the partition is refused");
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            1105,
            "[ddl] the tiflash replica count: 1 should be less than the total tiflash server count: 0"
        )
    );
    assert_eq!(partition_names(&mut session), [["p0"]]);
}

/// Go `checkPartitionReplica`'s `mockWaitTiFlashReplica`: a partition whose
/// replica never completes keeps the job retrying until the error count
/// limit cancels it, and the adding partition is rolled back.
#[test]
fn add_partition_waits_for_the_tiflash_replica_until_the_error_limit_like_go() {
    fail::cfg("mockWaitTiFlashReplica", "return(true)").expect("enable failpoint");
    let _guard = FailpointGuard(&["mockWaitTiFlashReplica"]);
    let stack = tiflash_stack(1);
    let mut session = stack.factory.open_session(session_context(2103)).unwrap();
    available_replica_table(&stack, &mut session, 1);
    rows(&mut session, "set @@global.tidb_ddl_error_count_limit = 1");

    let error = session
        .node_conn()
        .execute("ALTER TABLE ddltiflash ADD PARTITION (PARTITION pn VALUES LESS THAN (20))")
        .err()
        .expect("the wait ends the job");
    assert_eq!(
        (error.code, error.message.as_str()),
        (1105, "DDL job rollback, error msg: [ddl] add partition wait for tiflash replica to complete")
    );
    assert_eq!(partition_names(&mut session), [["p0"]]);
}
