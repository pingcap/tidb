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

//! LogicalSchemaProducer identity from
//! `pkg/planner/core/operator/logicalop/logical_schema_producer.go`.
//!
//! The source Hash64/Equals implementation uses only the optional ordered
//! output schema: a nil/present marker, each expression-column identity, and
//! schema length/order for equality. This leaf preserves that contract over a
//! normalized column adapter; schema propagation, names, BaseLogicalPlan and
//! children, full FieldType/collation/VirtualExpr metadata, and DataSource
//! integration remain explicit external boundaries.

use crate::hash_equaler::{new_hash_equaler, Hasher, NIL_FLAG, NOT_NIL_FLAG};

/// Normalized expression-column identity used by a logical output schema.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SchemaColumnIdentity {
    id: i64,
    unique_id: i64,
    index: i64,
    type_fingerprint: Option<u64>,
}

impl SchemaColumnIdentity {
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

/// Minimal LogicalSchemaProducer identity and optional ordered schema.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LogicalSchemaProducerIdentity {
    schema: Option<Vec<SchemaColumnIdentity>>,
}

impl LogicalSchemaProducerIdentity {
    /// Creates an identity from an optional output schema.
    #[must_use]
    pub fn new(schema: Option<Vec<SchemaColumnIdentity>>) -> Self {
        Self { schema }
    }

    /// Returns the optional output schema.
    #[must_use]
    pub fn schema(&self) -> Option<&[SchemaColumnIdentity]> {
        self.schema.as_deref()
    }

    /// Computes the source Hash64 schema framing in field order.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = new_hash_equaler();
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

    /// Compares source schema nil/presence, length, order, and column fields.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

fn hash_column(hasher: &mut impl Hasher, column: &SchemaColumnIdentity) {
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
