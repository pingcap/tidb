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
//! The rows come from Go's statement on a system session, as Go's syncer
//! reads them; the caller supplies the session's executor.

use tidb_datatype::Datum;
use tidb_infoschema::SessionExtendedInfoSchema;

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

/// Go `Syncer.refreshMDLCheckTableInfo`: the jobs at or above the current
/// minimum job ID whose version this node's `catalog` has loaded, read by
/// Go's statement through `execute` (a fresh system session's executor).
///
/// # Errors
///
/// The statement's error.
pub fn refresh_mdl_check_table_info(
    execute: &mut dyn FnMut(&str) -> Result<Vec<Vec<Datum>>, String>,
    catalog: &SessionExtendedInfoSchema,
    min_job_id: i64,
) -> Result<Vec<MdlJob>, String> {
    let sql = format!(
        "select job_id, version, table_ids from mysql.tidb_mdl_info\n\t\twhere job_id >= {min_job_id} and version <= {}",
        catalog.schema_meta_version()
    );
    let rows = execute(&sql)?;
    Ok(rows
        .iter()
        .map(|row| MdlJob {
            job_id: row[0].get_int64(),
            version: row[1].get_int64(),
            // Go `util.Str2Int64Map(rows[i].GetString(2))`: an empty string
            // yields `{0}` (ParseInt("") errors into 0), and table id 0
            // matches no real table.
            table_ids: str_to_int64s(&String::from_utf8_lossy(row[2].go_bytes())),
        })
        .collect())
}
