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
use tidb_datatype::Datum;
use tidb_txnkv::transaction::{
    RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient, StoreWriteLoader,
};

use crate::cluster_catalog::{ClusterCatalog, MetaPairs, MetaSnapshot};

/// One running DDL job the owner is waiting on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MdlJob {
    /// Go `mysql.tidb_mdl_info.job_id`.
    pub job_id: i64,
    /// The schema version the job published — what the ack must report.
    pub version: i64,
    /// The tables the job changes — `mysql.tidb_mdl_info.table_ids`,
    /// comma-joined int64s, decoded exactly as Go `util.Str2Int64Map`
    /// (`pkg/util/util.go:68-76`): split on `,`, `ParseInt` each piece, and
    /// an unparsable piece contributes 0 rather than failing the row.
    pub table_ids: Vec<i64>,
}

/// Go `util.Str2Int64Map`'s decode, list-shaped.
fn str_to_int64s(text: &str) -> Vec<i64> {
    text.split(',')
        .map(|piece| piece.parse::<i64>().unwrap_or(0))
        .collect()
}

/// Reads every `mysql.tidb_mdl_info` row at one fresh timestamp.
///
/// `catalog` locates the table; it is the node's already-loaded catalog, so
/// no meta walk happens here — only the table's own record scan. A catalog
/// that predates the table returns a retryable missing-table error; the caller
/// refreshes the catalog and retries, matching Go's SQL behavior.
pub fn load_mdl_jobs<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    catalog: &ClusterCatalog,
    min_job_id: i64,
) -> Result<Vec<MdlJob>, SystemTableError> {
    let view = match SystemTableView::locate(
        catalog,
        "tidb_mdl_info",
        &["job_id", "version", "table_ids"],
    ) {
        Ok(view) => view,
        // Go's restricted SQL query would fail if this system table were
        // absent. Treat that as a retryable catalog race instead of silently
        // caching an empty MDL set and leaving the owner waiting forever.
        Err(error @ SystemTableError::Missing { .. }) => return Err(error),
        Err(error) => return Err(error),
    };
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    let loaded: Result<MetaPairs, SystemTableError> = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        // Go executes the restricted SQL query against a fresh session. Keep
        // the same result semantics by scanning the system-table record
        // prefix and applying both predicates after decoding. This avoids
        // depending on a native range seek whose region behavior differs
        // between TiDB nodes while the MDL row is being published.
        let mut pairs = scan_system_table(&mut snapshot, &view)?;
        // The Go SQL planner's lower bound is the current minimum job ID.
        // If a region scan races publication of that row, use the equivalent
        // clustered-key point read before treating the table as empty.
        if pairs.is_empty() && min_job_id > 0 {
            let key = view.record_prefix(&[Datum::Int(min_job_id)])?;
            if let Some(value) = snapshot.get(&key)? {
                pairs.push((key, value));
            }
        }
        Ok(pairs)
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
        if job_id < min_job_id || version > catalog.schema_version {
            continue;
        }
        // Decoded exactly as Go: `Str2Int64Map` on the stored text, where an
        // empty string yields `{0}` (ParseInt("") errors into 0), and table
        // id 0 matches no real table -- so a job with no listed tables gates
        // on nothing, which is Go's behaviour verbatim.
        let table_ids = str_to_int64s(&row.text("table_ids")?.unwrap_or_default());
        jobs.push(MdlJob {
            job_id,
            version,
            table_ids,
        });
    }
    Ok(jobs)
}
