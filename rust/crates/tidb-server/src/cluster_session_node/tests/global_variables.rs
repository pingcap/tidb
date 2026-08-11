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
