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

//! `mysql.advisory_locks` as Go's `GET_LOCK` reaches it: the system session
//! inserts the lock's row (Go `pkg/session/advisory_locks.go`), so the key it
//! pessimistically locks is that table's, found by name in the stored schema.

use std::fmt;

use tidb_codec::Encoder;
use tidb_datatype::{new_collation_enabled, Datum};
use tidb_infoschema::cluster_catalog::{ClusterCatalogError, MetaSnapshot};
use tidb_meta::{key, value};
use tidb_model::table_info::TableInfo;
use tidb_tablecodec::{generate_index_key, TableInfo as CodecTableInfo};

use crate::mysql_system_tables::SYSTEM_DB;
use crate::system_row_write::codec_table_info;

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
        let (_, field) = tidb_meta::structure::decode_hash_data_key(&raw_key)
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

/// What encoding the key Go's `GET_LOCK` locks takes, located by name in the
/// schema (Go inserts through the info schema's `mysql.advisory_locks`,
/// whatever ids the bootstrap gave it and whether its primary key is a
/// clustered common handle, whose key is the record's, or not, whose key is
/// the unique primary index entry's).
#[derive(Clone, Debug)]
pub struct AdvisoryLockTable {
    info: TableInfo,
    codec_info: CodecTableInfo,
    primary_position: usize,
    table_id: i64,
}

impl AdvisoryLockTable {
    /// Locates `mysql.advisory_locks` at one snapshot.
    pub fn locate<S: MetaSnapshot>(snapshot: &mut S) -> Result<Self, SystemTableLookupError> {
        let info = find_system_table(snapshot, "advisory_locks")?.ok_or_else(|| {
            SystemTableLookupError::Encode("mysql.advisory_locks does not exist".to_owned())
        })?;
        let primary_position = info
            .indices
            .iter_deref()
            .position(|index| index.read().primary)
            .ok_or_else(|| {
                SystemTableLookupError::Encode("advisory_locks primary index is absent".to_owned())
            })?;
        Ok(Self {
            codec_info: codec_table_info(&info),
            primary_position,
            table_id: info.id,
            info,
        })
    }

    /// Encodes the unique-primary-index key Go's INSERT into
    /// `mysql.advisory_locks` pessimistically locks.
    pub fn key(&self, lock_name: &str) -> Result<Vec<u8>, SystemTableLookupError> {
        if self.info.is_common_handle {
            // A clustered primary key owns no index entry: the insert locks
            // the record key.
            let column = self
                .info
                .find_public_column_by_name("lock_name")
                .ok_or_else(|| {
                    SystemTableLookupError::Encode("advisory_locks has no lock_name column".to_owned())
                })?;
            let mut values = crate::system_row_write::RowValues::new();
            values.insert(column.read().id, Datum::new_string(lock_name));
            return crate::system_row_write::clustered_record_key(&self.info, &values)
                .map_err(|error| SystemTableLookupError::Encode(error.to_string()));
        }
        let primary = &self.codec_info.indices[self.primary_position];
        let mut values = vec![Datum::new_string(lock_name)];
        let (key, distinct) = generate_index_key(
            Encoder::new(new_collation_enabled()),
            None,
            &self.codec_info,
            primary,
            self.table_id,
            &mut values,
            None,
        )
        .map_err(|error| SystemTableLookupError::Encode(error.to_string()))?;
        if !distinct {
            return Err(SystemTableLookupError::Encode(
                "advisory_locks primary key encoded as non-distinct".to_owned(),
            ));
        }
        Ok(key)
    }
}
