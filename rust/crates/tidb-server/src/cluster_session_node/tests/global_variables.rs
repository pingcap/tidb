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

use super::node_fixture::*;
use crate::cluster_sysvar_seam::{ClusterSysvarWriter, PendingSysvarChange};
use crate::sql_node::QuerySession;
use std::sync::Arc;
use tidb_session::vars::GlobalSysvars;
use tidb_txnkv::region::RegionBackoffKind;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticTransactionReceipt, RolledBackTransaction, TransactionCause,
};

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

    fn commit(self: Box<Self>) -> Result<Vec<String>, crate::sql_node::SqlQueryError> {
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

    fn commit(self: Box<Self>) -> Result<Vec<String>, crate::sql_node::SqlQueryError> {
        let outcome = OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
            receipt: OptimisticTransactionReceipt::new(1, 2, b"key".to_vec(), 1),
            cause: TransactionCause::BackoffExhausted {
                kind: RegionBackoffKind::MaxTimestampNotSynced,
                detail: "maxTimestampNotSynced backoffer exhausted".to_owned(),
            },
        });
        Err(
            crate::sql_node::cluster_commit_error(&outcome, "sysvar change")
                .expect("an exhausted max-ts backoff cannot answer success"),
        )
    }
}

#[test]
fn global_sysvar_commit_keeps_an_undetermined_verdict_connection_fatal() {
    let node = MockNode::start();
    let writer = Arc::new(UndeterminedSysvarWriter(node.sysvars.stored.clone()));
    let mut session = open_session_on_with_sysvars(&node, writer);
    let query_error = session
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
        .execute_write("SET GLOBAL autocommit = OFF")
        .expect_err("SET GLOBAL cannot answer success after a rolled-back commit");
    assert_eq!(
        query_error.code,
        tidb_error::tidb::errcode::ErrTiKVMaxTimestampNotSynced
    );
    assert_query_error_packet(
        &query_error,
        tidb_error::tidb::errcode::ErrTiKVMaxTimestampNotSynced,
        "TiKV max timestamp is not synced",
    );
    assert_eq!(
        node.sysvars.stored.get("autocommit").unwrap(),
        stored_before
    );
    assert_eq!(node.sysvars.live.get("autocommit").unwrap(), live_before);
}
