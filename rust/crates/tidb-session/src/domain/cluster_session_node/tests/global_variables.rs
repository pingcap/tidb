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

//! `SET GLOBAL` errors that must retain their commit disposition through the
//! cluster sysvar seam and the MySQL packet writer.

use super::super::*;
use super::mock_cluster::MockTransactions;
use super::mock_seams::MockAnalyze;
use super::node_fixture::*;
use tidb_domain_core::cluster_sysvar_seam::{ClusterSysvarWriter, PendingSysvarChange};
use std::sync::Arc;
use tidb_datatype::Datum;
use crate::GlobalSysvars;
use tidb_txnkv::retry::RegionBackoffKind;
use tidb_txnkv::transaction::{OptimisticCommitOutcome, OptimisticTransactionReceipt, RolledBackTransaction, TransactionCause};

struct UndeterminedSysvarWriter(GlobalSysvars);

impl ClusterSysvarWriter for UndeterminedSysvarWriter {
    fn begin(&self) -> Result<Box<dyn PendingSysvarChange>, String> {
        Ok(Box::new(UndeterminedSysvarChange(self.0.clone())))
    }
}

struct UndeterminedSysvarChange(GlobalSysvars);

impl PendingSysvarChange for UndeterminedSysvarChange {
    fn table(&self) -> GlobalSysvars {
        self.0.clone()
    }

    fn commit(self: Box<Self>) -> Result<Vec<String>, tidb_store_driver_error::sql_query_error::SqlQueryError> {
        Err(undetermined_cluster_commit_error("sysvar change"))
    }
}

struct BackoffSysvarWriter(GlobalSysvars);

impl ClusterSysvarWriter for BackoffSysvarWriter {
    fn begin(&self) -> Result<Box<dyn PendingSysvarChange>, String> {
        Ok(Box::new(BackoffSysvarChange(
            GlobalSysvars::from_cluster_rows(self.0.overrides()),
        )))
    }
}

struct BackoffSysvarChange(GlobalSysvars);

impl PendingSysvarChange for BackoffSysvarChange {
    fn table(&self) -> GlobalSysvars {
        self.0.clone()
    }

    fn commit(self: Box<Self>) -> Result<Vec<String>, tidb_store_driver_error::sql_query_error::SqlQueryError> {
        let outcome = OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
            receipt: OptimisticTransactionReceipt::new(1, 2, b"key".to_vec(), 1),
            cause: TransactionCause::BackoffExhausted {
                kind: RegionBackoffKind::MaxTimestampNotSynced,
                detail: "maxTimestampNotSynced backoffer exhausted".to_owned(),
            },
        });
        Err(
            tidb_store_driver_error::pessimistic_lock_error::cluster_commit_error(&outcome, "sysvar change")
                .expect("an exhausted max-ts backoff cannot answer success"),
        )
    }
}

fn factory_with_globals(node: &MockNode, globals: GlobalSysvars) -> Domain {
    let domain = Domain::new(
        Arc::new(MockTransactions(Arc::clone(&node.cluster))),
        Arc::clone(&node.ddl) as Arc<dyn ClusterDdl>,
        Arc::clone(&node.accounts) as Arc<dyn ClusterAccountWriter>,
        Arc::clone(&node.sysvars) as Arc<dyn ClusterSysvarWriter>,
        Arc::new(super::mock_seams::MockStatsLock)
            as Arc<dyn tidb_stats_handle_lockstats::cluster_stats_lock_seam::ClusterStatsLock>,
        Arc::clone(&node.catalog),
        node.accounts.live.clone(),
        globals,
        Arc::new(
            SharedStats::new(tidb_stats_handle_syncload::stats_watch::StatsSnapshot::new())
                .expect("statistics cache"),
        ),
        Arc::new(tidb_table::table_from_meta::LocalTableAutoIds::default()),
        crate::domain::cluster_session_node::open_pool_session,
    );
    crate::domain::cluster_session_node::NodeServices::new(Arc::new(MockAnalyze)).bind(&domain);
    domain
}

#[test]
fn factory_rejects_invalid_persisted_fix_control_without_process_leak() {
    let node = MockNode::start();
    let invalid_globals = GlobalSysvars::from_cluster_rows([(
        "tidb_opt_fix_control".to_owned(),
        "invalid".to_owned(),
    )]);
    let invalid_factory = factory_with_globals(&node, invalid_globals).shared();
    let Err(error) = invalid_factory.open_session(session_context(51)) else {
        panic!("an invalid persisted fix-control row must refuse the cluster session");
    };
    assert_eq!(error.code, 1105);
    assert_eq!(error.state, *b"HY000");
    assert_eq!(
        error.message,
        "invalid fix control: expected colon not found"
    );
    assert!(
        invalid_factory.processes().snapshot().is_empty(),
        "a rejected cluster session must release its process registration"
    );

    let valid_globals = GlobalSysvars::from_cluster_rows([(
        "tidb_opt_fix_control".to_owned(),
        "52592:ON".to_owned(),
    )]);
    let valid_factory = factory_with_globals(&node, valid_globals).shared();
    let session = valid_factory
        .open_session(session_context(52))
        .expect("a valid persisted fix-control row opens normally");
    assert_eq!(valid_factory.processes().snapshot().len(), 1);
    drop(session);
    assert!(valid_factory.processes().snapshot().is_empty());
}

#[test]
fn global_sysvar_commit_keeps_an_undetermined_verdict_connection_fatal() {
    let node = MockNode::start();
    let writer = Arc::new(UndeterminedSysvarWriter(node.sysvars.stored.clone()));
    let mut session = open_session_on_with_sysvars(&node, writer);
    let query_error = session
        .node_conn()
        .execute_write("SET GLOBAL autocommit = OFF")
        .expect_err("SET GLOBAL cannot answer success after losing its commit response");
    assert_undetermined_closes_without_packet(&query_error);
}

#[test]
fn global_sysvar_commit_keeps_a_backoff_driver_error_coded_on_the_wire() {
    let node = MockNode::start();
    let stored_before = node.sysvars.stored.get("autocommit").unwrap();
    let live_before = node.sysvars.live.get("autocommit").unwrap();
    let writer = Arc::new(BackoffSysvarWriter(node.sysvars.stored.clone()));
    let mut session = open_session_on_with_sysvars(&node, writer);
    let query_error = session
        .node_conn()
        .execute_write("SET GLOBAL autocommit = OFF")
        .expect_err("SET GLOBAL cannot answer success after a rolled-back commit");
    assert_eq!(
        query_error.code,
        tidb_errno::errcode::ErrTiKVMaxTimestampNotSynced
    );
    assert_query_error_packet(
        &query_error,
        tidb_errno::errcode::ErrTiKVMaxTimestampNotSynced,
        "TiKV max timestamp is not synced",
    );
    assert_eq!(
        node.sysvars.stored.get("autocommit").unwrap(),
        stored_before
    );
    assert_eq!(node.sysvars.live.get("autocommit").unwrap(), live_before);
}

#[test]
fn prepared_system_variable_scope_errors_survive_cluster_metadata_probe() {
    let (mut session, _node) = open_session();
    for (sql, code, message) in [
        (
            "SELECT @@session.ddl_slow_threshold",
            1238,
            "Variable 'ddl_slow_threshold' is a GLOBAL variable",
        ),
        (
            "SELECT @@session.tidb_redact_log",
            1193,
            "Unknown system variable 'tidb_redact_log'",
        ),
    ] {
        let Err(query_error) = session.node_conn().prepare_general(sql) else {
            panic!("{sql} must fail during PREPARE");
        };
        assert_eq!(query_error.code, code, "{sql}");
        assert_eq!(query_error.state, *b"HY000", "{sql}");
        assert_eq!(query_error.message, message, "{sql}");
        assert_query_error_packet(&query_error, code, message);
    }
}

#[test]
fn session_plan_cache_instance_flush_uses_factory_lifetime() {
    use tidb_datatype::Datum;

    let node = MockNode::start();
    let factory = factory_with_globals(&node, GlobalSysvars::from_cluster_rows([])).shared();
    let other_factory = factory_with_globals(&node, GlobalSysvars::from_cluster_rows([])).shared();
    let mut first = factory.open_session(session_context(61)).unwrap();
    let mut peer = factory.open_session(session_context(62)).unwrap();
    let mut other = other_factory.open_session(session_context(63)).unwrap();
    for session in [&mut first, &mut peer, &mut other] {
        session.node_conn().select_database("app").unwrap();
    }
    first
        .node_conn()
        .execute_write("INSERT INTO t VALUES (1,10)")
        .unwrap();
    for session in [&mut first, &mut peer, &mut other] {
        session.node_conn().execute_write("SET @id=1").unwrap();
        session
            .node_conn()
            .execute_write("PREPARE p FROM 'SELECT v FROM t WHERE id=?'")
            .unwrap();
        assert_eq!(
            rows(session, "EXECUTE p USING @id"),
            vec![vec![Datum::Int(10)]]
        );
        assert_eq!(
            rows(session, "SELECT @@last_plan_from_cache"),
            vec![vec![Datum::Int(0)]]
        );
        assert_eq!(
            rows(session, "EXECUTE p USING @id"),
            vec![vec![Datum::Int(10)]]
        );
        assert_eq!(
            rows(session, "SELECT @@last_plan_from_cache"),
            vec![vec![Datum::Int(1)]]
        );
    }
    first
        .node_conn()
        .execute_write("ADMIN FLUSH INSTANCE PLAN_CACHE")
        .unwrap()
        .expect("flush answers with an OK packet");
    for (session, hit) in [(&mut first, 0), (&mut peer, 0), (&mut other, 1)] {
        assert_eq!(
            rows(session, "EXECUTE p USING @id"),
            vec![vec![Datum::Int(10)]]
        );
        assert_eq!(
            rows(session, "SELECT @@last_plan_from_cache"),
            vec![vec![Datum::Int(hit)]]
        );
    }
}

#[test]
fn global_config_factory_shares_the_owner_across_scratch_sysvar_statements() {
    use tidb_domain_globalconfigsync::GlobalConfigSyncer;
    use tidb_proto::pdpb::GlobalConfigItem;
    let node = MockNode::start();
    let (syncer, notifications) = GlobalConfigSyncer::new(None);
    let factory = factory_with_globals(&node, node.sysvars.live.clone())
        .with_global_config_syncer(Arc::clone(&syncer))
        .shared();
    let mut first = factory.open_session(session_context(61)).unwrap();
    let mut second = factory.open_session(session_context(62)).unwrap();
    first
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_top_sql=1")
        .unwrap();
    second
        .node_conn()
        .execute_write("SET GLOBAL tidb_source_id=2")
        .unwrap();
    assert!(syncer.notify(GlobalConfigItem {
        name: "barrier".into(),
        ..Default::default()
    }));
    let mut actual = Vec::new();
    while let Some(item) = notifications.recv() {
        if item.name == "barrier" {
            break;
        }
        actual.push((item.name, item.value));
    }
    assert_eq!(
        actual,
        vec![
            ("enable_resource_metering".into(), "true".into()),
            ("source_id".into(), "2".into())
        ]
    );
}

#[test]
fn instance_plan_cache_shares_factory_schema_versions_across_rebuilt_catalogs() {
    let node = MockNode::start();
    let factory = factory_with_globals(
        &node,
        GlobalSysvars::from_cluster_rows([(
            "tidb_enable_instance_plan_cache".to_owned(),
            "ON".to_owned(),
        )]),
    )
    .shared();
    let mut first = factory.open_session(session_context(71)).unwrap();
    let mut peer = factory.open_session(session_context(72)).unwrap();
    for session in [&mut first, &mut peer] {
        session.node_conn().select_database("app").unwrap();
    }
    first
        .node_conn()
        .execute_write("INSERT INTO t VALUES (1,10),(2,20)")
        .unwrap();
    for session in [&mut first, &mut peer] {
        session
            .node_conn()
            .execute_write("PREPARE p FROM 'SELECT v FROM t WHERE id=?'")
            .unwrap();
        session.node_conn().execute_write("SET @id=1").unwrap();
    }
    assert_eq!(
        rows(&mut first, "EXECUTE p USING @id"),
        vec![vec![Datum::Int(10)]]
    );
    assert_eq!(
        rows(&mut first, "SELECT @@last_plan_from_cache"),
        vec![vec![Datum::Int(0)]]
    );
    peer.node_conn().execute_write("SET @id=2").unwrap();
    assert_eq!(
        rows(&mut peer, "EXECUTE p USING @id"),
        vec![vec![Datum::Int(20)]]
    );
    assert_eq!(
        rows(&mut peer, "SELECT @@last_plan_from_cache"),
        vec![vec![Datum::Int(1)]]
    );
    first
        .node_conn()
        .execute_write("ADMIN FLUSH INSTANCE PLAN_CACHE")
        .unwrap();
    assert_eq!(
        rows(&mut peer, "EXECUTE p USING @id"),
        vec![vec![Datum::Int(20)]]
    );
    assert_eq!(
        rows(&mut peer, "SELECT @@last_plan_from_cache"),
        vec![vec![Datum::Int(1)]]
    );
}

/// Go `buildSet` plans every value of one SET -- user variable, system
/// variable, `NAMES` -- and evaluates subqueries against the statement's
/// snapshot, whatever kinds the statement mixes (captured from Go:
/// `2|2|latin1`).
#[test]
fn mixed_set_reads_its_subqueries_through_the_node() {
    let mut session = crate::Session::new_on_mock_store();
    for sql in [
        "create database set_mixed",
        "use set_mixed",
        "create table t (a int)",
        "insert into t values (1), (2)",
        "set @a = (select count(*) from t), \
         @@session.div_precision_increment = (select max(a) from t), names latin1",
    ] {
        session
            .run(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    let crate::StmtResult::Rows(rows) = session
        .run("select @a, @@div_precision_increment, @@character_set_client")
        .expect("read the assignments")
    else {
        panic!("SELECT answers rows");
    };
    let rendered: Vec<String> = rows[0]
        .iter()
        .map(|datum| match datum {
            Datum::Int(value) => value.to_string(),
            Datum::UInt(value) => value.to_string(),
            Datum::String(value) => String::from_utf8_lossy(&value.bytes()).into_owned(),
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => format!("{other:?}"),
        })
        .collect();
    assert_eq!(rendered, ["2", "2", "latin1"]);
}
