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

//! Reading `mysql.tidb_mdl_info` — the rows a Go DDL owner writes for the
//! jobs whose schema version it is waiting on.
//!
//! Go's non-owner reads this table after every reload
//! (`refreshMDLCheckTableInfo`, `pkg/infoschema/issyncer/syncer.go`): each
//! row is one running DDL job — `job_id`, the schema `version` that job
//! published, and the `table_ids` it touches — and the owner's
//! `WaitVersionSynced` holds the job until every registered node
//! acknowledges that version. This module is the read half of that
//! acknowledgement on the Rust node; the decision and the etcd write live
//! with the node, which owns the etcd client and the session registry.
//!
//! The table is read the way the account and statistics loaders read their
//! `mysql.*` tables: one read-only transaction, the [`SystemTableView`]
//! projection, no SQL session. `job_id` is the clustered integer handle, so
//! it decodes from the record key; `version` comes from the row value.

use std::time::Duration;

use crate::mysql_system_tables::{scan_system_table, SystemRow, SystemTableError, SystemTableView};
use crate::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_txnkv::transaction::{
    RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient, StoreWriteLoader,
};

use crate::cluster_catalog::ClusterCatalog;

/// One running DDL job the owner is waiting on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MdlJob {
    /// Go `mysql.tidb_mdl_info.job_id`.
    pub job_id: i64,
    /// The schema version the job published — what the ack must report.
    pub version: i64,
}

/// Reads every `mysql.tidb_mdl_info` row at one fresh timestamp.
///
/// `catalog` locates the table; it is the node's already-loaded catalog, so
/// no meta walk happens here — only the table's own record scan. A cluster
/// whose bootstrap predates the table (or a unistore without Go's bootstrap)
/// answers an empty list rather than an error: no table means no Go owner
/// writing rows, and therefore nothing to acknowledge.
pub fn load_mdl_jobs<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    catalog: &ClusterCatalog,
) -> Result<Vec<MdlJob>, SystemTableError> {
    let view = match SystemTableView::locate(catalog, "tidb_mdl_info", &["job_id", "version"]) {
        Ok(view) => view,
        Err(SystemTableError::Missing { .. }) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    let loaded = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        scan_system_table(&mut snapshot, &view)
    };
    transaction
        .finish_without_writes()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    let pairs = loaded?;
    let mut jobs = Vec::with_capacity(pairs.len());
    for (key, value) in &pairs {
        let row = SystemRow::parse(&view, key, value)?;
        let (Some(job_id), Some(version)) = (row.i64("job_id")?, row.i64("version")?) else {
            // A row missing either column is not one this node can ack;
            // skipping it leaves the owner waiting on the nodes that can
            // read it, never acking a version this node did not see.
            continue;
        };
        jobs.push(MdlJob { job_id, version });
    }
    Ok(jobs)
}
