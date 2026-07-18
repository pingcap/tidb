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

//! LogicalShowDDLJobs identity from
//! `pkg/planner/core/operator/logicalop/logical_show_ddl_jobs.go` and its
//! generated Hash64/Equals implementation.
//!
//! The source identity hashes the ShowDDLJobs plan tag and the embedded
//! LogicalSchemaProducer output schema. This leaf preserves that field order
//! over a normalized column adapter; the untagged JobNumber field, DDL stats,
//! plan context, and runtime SHOW behavior remain explicit external boundaries.

use crate::hash_equaler::{new_hash_equaler, Hasher, NIL_FLAG, NOT_NIL_FLAG};

/// Normalized output-column identity used by LogicalShowDDLJobs.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ShowDDLJobsColumnIdentity {
    id: i64,
    unique_id: i64,
    index: i64,
    type_fingerprint: Option<u64>,
}

impl ShowDDLJobsColumnIdentity {
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

/// Minimal LogicalShowDDLJobs identity and output schema.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LogicalShowDDLJobsIdentity {
    schema: Option<Vec<ShowDDLJobsColumnIdentity>>,
}

impl LogicalShowDDLJobsIdentity {
    /// Creates an identity from an optional LogicalSchemaProducer schema.
    #[must_use]
    pub fn new(schema: Option<Vec<ShowDDLJobsColumnIdentity>>) -> Self {
        Self { schema }
    }

    /// Returns the optional output schema.
    #[must_use]
    pub fn schema(&self) -> Option<&[ShowDDLJobsColumnIdentity]> {
        self.schema.as_deref()
    }

    /// Computes generated Hash64 in source field order.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_string("ShowDDLJobs");
        match &self.schema {
            Some(columns) => {
                hasher.hash_byte(NOT_NIL_FLAG);
                for column in columns {
                    hash_column(&mut hasher, column);
                }
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

fn hash_column(hasher: &mut impl Hasher, column: &ShowDDLJobsColumnIdentity) {
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
