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

//! LogicalTableDual identity from
//! `pkg/planner/core/operator/logicalop/logical_table_dual.go` and its
//! generated Hash64/Equals implementation.
//!
//! The source identity hashes the TableDual plan tag, the produced schema, and
//! RowCount, and compares the same fields.  This leaf preserves that ordering
//! over a typed schema/column adapter and keeps the source `rowcount:` explain
//! text; full FieldType/collation/VirtualExpr hashing and logical-plan runtime
//! behavior remain external.

use crate::hash_equaler::{new_hash_equaler, Hasher, NIL_FLAG, NOT_NIL_FLAG};

/// Source-shaped column identity used by a TableDual output schema.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ColumnIdentity {
    id: i64,
    unique_id: i64,
    index: i64,
    type_fingerprint: Option<u64>,
}

impl ColumnIdentity {
    /// Creates a column identity with no caller-supplied type fingerprint.
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

/// Minimal LogicalTableDual identity and output schema.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LogicalTableDualIdentity {
    schema: Option<Vec<ColumnIdentity>>,
    row_count: i64,
}

impl LogicalTableDualIdentity {
    /// Creates a TableDual identity from an optional output schema and row
    /// count.
    #[must_use]
    pub fn new(schema: Option<Vec<ColumnIdentity>>, row_count: i64) -> Self {
        Self { schema, row_count }
    }

    /// Returns the optional output schema.
    #[must_use]
    pub fn schema(&self) -> Option<&[ColumnIdentity]> {
        self.schema.as_deref()
    }

    /// Returns the source row count.
    #[must_use]
    pub const fn row_count(&self) -> i64 {
        self.row_count
    }

    /// Returns source-shaped `ExplainInfo` text.
    #[must_use]
    pub fn explain_info(&self) -> String {
        format!("rowcount:{}", self.row_count)
    }

    /// Computes the generated Hash64 digest in source field order.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_string("TableDual");
        match &self.schema {
            Some(columns) => {
                hasher.hash_byte(NOT_NIL_FLAG);
                for column in columns {
                    hash_column(&mut hasher, column);
                }
            }
            None => hasher.hash_byte(NIL_FLAG),
        }
        hasher.hash_int64(self.row_count);
        hasher.sum64()
    }

    /// Compares the generated Hash64/Equals identity fields.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

fn hash_column(hasher: &mut impl Hasher, column: &ColumnIdentity) {
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
