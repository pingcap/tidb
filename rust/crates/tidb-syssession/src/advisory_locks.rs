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

//! Finding a stored `mysql` system table by name in a snapshot's schema
//! (Go's `GET_LOCK` itself is the session's internal INSERT into
//! `mysql.advisory_locks`, `pkg/session/advisory_locks.go`).

use std::fmt;

use tidb_infoschema::cluster_catalog::{ClusterCatalogError, MetaSnapshot};
use tidb_meta::{key, value};
use tidb_model::table_info::TableInfo;

use crate::mysql_system_tables::SYSTEM_DB;

/// Why a stored system table could not be read.
#[derive(Clone, Debug)]
pub enum SystemTableLookupError {
    /// The snapshot read failed.
    Snapshot(ClusterCatalogError),
    /// A stored key or value did not decode, or the table is not as expected.
    Encode(String),
}

impl fmt::Display for SystemTableLookupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot(error) => write!(formatter, "{error}"),
            Self::Encode(detail) => write!(formatter, "bootstrap encode failed: {detail}"),
        }
    }
}

impl std::error::Error for SystemTableLookupError {}

impl From<ClusterCatalogError> for SystemTableLookupError {
    fn from(error: ClusterCatalogError) -> Self {
        Self::Snapshot(error)
    }
}

/// Reads one stored `mysql.<name>` table by name at one snapshot, the way Go's
/// restricted SQL resolves a system table through the info schema: the layout
/// a bootstrap chose (reserved ids under the next-gen kernel, ordinary ones
/// under the classic kernel) is not the reader's business. `None` when the
/// snapshot holds no such table.
pub fn find_system_table<S: MetaSnapshot>(
    snapshot: &mut S,
    table_name: &str,
) -> Result<Option<TableInfo>, SystemTableLookupError> {
    for (raw_key, stored) in snapshot.scan_prefix(&key::databases_kv_prefix())? {
        let (_, field) = tidb_tablecodec::table_key::decode_meta_key(&raw_key)
            .map_err(|error| SystemTableLookupError::Encode(format!("DBs hash key: {error}")))?;
        if !key::has_prefix(key::DB_PREFIX, &field) {
            continue;
        }
        let database = value::parse_db_info(&stored)
            .map_err(|error| SystemTableLookupError::Encode(format!("DBInfo: {error}")))?;
        if database.name.lowercase() != SYSTEM_DB {
            continue;
        }
        return Ok(
            tidb_infoschema::cluster_catalog::load_database_tables(snapshot, database.id)?
                .into_iter()
                .find(|table| table.name.lowercase() == table_name),
        );
    }
    Ok(None)
}

