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

//! LogicalMemTable identity from
//! `pkg/planner/core/operator/logicalop/logical_mem_table.go` and its generated
//! Hash64/Equals implementation.
//!
//! The source identity hashes the MemTableScan tag, output schema, lower-case
//! DBName, and optional TableInfo ID. This leaf preserves that field order over
//! normalized adapters; Extractor/Columns/QueryTimeRange, infoschema behavior,
//! plan context, and runtime memtable execution remain explicit external
//! boundaries.

use crate::hash_equaler::{new_hash_equaler, Hasher, NIL_FLAG, NOT_NIL_FLAG};

/// Normalized output-column identity used by LogicalMemTable.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MemTableColumnIdentity {
    id: i64,
    unique_id: i64,
    index: i64,
    type_fingerprint: Option<u64>,
}

impl MemTableColumnIdentity {
    /// Creates a column identity without a caller-supplied type fingerprint.
    #[must_use]
    pub const fn new(id: i64, unique_id: i64, index: i64) -> Self {
        Self {
            id,
            unique_id,
            index,
            type_fingerprint: None,
        }
    }

    /// Creates a column identity with a normalized type fingerprint.
    #[must_use]
    pub const fn with_type_fingerprint(
        id: i64,
        unique_id: i64,
        index: i64,
        type_fingerprint: u64,
    ) -> Self {
        Self {
            id,
            unique_id,
            index,
            type_fingerprint: Some(type_fingerprint),
        }
    }
}

/// Minimal LogicalMemTable identity and generated Hash64/Equals fields.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LogicalMemTableIdentity {
    schema: Option<Vec<MemTableColumnIdentity>>,
    db_name: String,
    table_info_id: Option<i64>,
}

impl LogicalMemTableIdentity {
    /// Creates an identity, normalizing DBName with source case-folding.
    #[must_use]
    pub fn new(
        schema: Option<Vec<MemTableColumnIdentity>>,
        db_name: impl AsRef<str>,
        table_info_id: Option<i64>,
    ) -> Self {
        Self {
            schema,
            db_name: db_name.as_ref().to_lowercase(),
            table_info_id,
        }
    }

    /// Returns the optional output schema.
    #[must_use]
    pub fn schema(&self) -> Option<&[MemTableColumnIdentity]> {
        self.schema.as_deref()
    }

    /// Returns the normalized DBName used by CIStr Hash64/Equals.
    #[must_use]
    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    /// Returns the optional TableInfo ID.
    #[must_use]
    pub const fn table_info_id(&self) -> Option<i64> {
        self.table_info_id
    }

    /// Computes generated Hash64 in source field order.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_string("MemTableScan");
        hash_schema(&mut hasher, self.schema.as_deref());
        hasher.hash_string(&self.db_name);
        match self.table_info_id {
            Some(id) => {
                hasher.hash_byte(NOT_NIL_FLAG);
                hasher.hash_int64(id);
            }
            None => hasher.hash_byte(NIL_FLAG),
        }
        hasher.sum64()
    }

    /// Compares generated Hash64/Equals identity fields.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

fn hash_schema(hasher: &mut impl Hasher, schema: Option<&[MemTableColumnIdentity]>) {
    match schema {
        Some(columns) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            for column in columns {
                hash_column(hasher, column);
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_column(hasher: &mut impl Hasher, column: &MemTableColumnIdentity) {
    match column.type_fingerprint {
        Some(fingerprint) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_uint64(fingerprint);
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
    hasher.hash_int64(column.id);
    hasher.hash_int64(column.unique_id);
    hasher.hash_int(column.index);
}
