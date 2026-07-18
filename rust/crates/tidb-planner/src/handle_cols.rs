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

//! Handle-column identity from `pkg/planner/util/handle_cols.go`.
//!
//! This leaf ports the source Hash64/Equals contracts for CommonHandleCols
//! and IntHandleCols. TableInfo/IndexInfo are represented by caller-owned IDs,
//! and expression columns by normalized identity adapters; handle encoding,
//! row/datums, index truncation, metadata cloning, compare/collation behavior,
//! schema index resolution, and runtime storage integration remain explicit
//! external boundaries.

use crate::hash_equaler::{new_hash_equaler, Hasher, NIL_FLAG, NOT_NIL_FLAG};

/// Normalized column identity used by handle-column adapters.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct HandleColumnIdentity {
    id: i64,
    unique_id: i64,
    index: i64,
    type_fingerprint: Option<u64>,
}

impl HandleColumnIdentity {
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

    /// Creates a column identity with normalized type metadata.
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

/// Normalized TableInfo/IndexInfo identity. The Go metadata structs are much
/// larger; this adapter keeps the source identity key and leaves the rest to
/// the catalog/model seam.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct HandleMetadataIdentity {
    id: i64,
}

impl HandleMetadataIdentity {
    /// Creates a normalized table/index metadata identity.
    #[must_use]
    pub const fn new(id: i64) -> Self {
        Self { id }
    }
}

/// Source-shaped CommonHandleCols Hash64/Equals identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CommonHandleIdentity {
    table_info: Option<HandleMetadataIdentity>,
    index_info: Option<HandleMetadataIdentity>,
    columns: Option<Vec<HandleColumnIdentity>>,
}

impl CommonHandleIdentity {
    /// Creates a normalized CommonHandleCols identity.
    #[must_use]
    pub fn new(
        table_info: Option<HandleMetadataIdentity>,
        index_info: Option<HandleMetadataIdentity>,
        columns: Option<Vec<HandleColumnIdentity>>,
    ) -> Self {
        Self {
            table_info,
            index_info,
            columns,
        }
    }

    /// Computes source CommonHandleCols Hash64 field order.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = new_hash_equaler();
        hash_metadata(&mut hasher, self.table_info);
        hash_metadata(&mut hasher, self.index_info);
        hash_columns(&mut hasher, self.columns.as_deref());
        hasher.sum64()
    }

    /// Compares source CommonHandleCols Equals fields.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self == other
    }

    /// Clones the identity while retaining nil-versus-empty option state.
    #[must_use]
    pub fn clone_identity(&self) -> Self {
        self.clone()
    }
}

/// Source-shaped IntHandleCols Hash64/Equals identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct IntHandleIdentity {
    column: Option<HandleColumnIdentity>,
}

impl IntHandleIdentity {
    /// Creates a normalized IntHandleCols identity.
    #[must_use]
    pub fn new(column: Option<HandleColumnIdentity>) -> Self {
        Self { column }
    }

    /// Computes source IntHandleCols Hash64 field order.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = new_hash_equaler();
        match &self.column {
            Some(column) => {
                hasher.hash_byte(NOT_NIL_FLAG);
                hash_column(&mut hasher, column);
            }
            None => hasher.hash_byte(NIL_FLAG),
        }
        hasher.sum64()
    }

    /// Compares source IntHandleCols Equals fields.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

fn hash_metadata(hasher: &mut impl Hasher, metadata: Option<HandleMetadataIdentity>) {
    match metadata {
        Some(metadata) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_int64(metadata.id);
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_columns(hasher: &mut impl Hasher, columns: Option<&[HandleColumnIdentity]>) {
    match columns {
        Some(columns) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_int(columns.len() as i64);
            for column in columns {
                hash_column(hasher, column);
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_column(hasher: &mut impl Hasher, column: &HandleColumnIdentity) {
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

#[cfg(test)]
mod tests {
    use super::{
        CommonHandleIdentity, HandleColumnIdentity, HandleMetadataIdentity, IntHandleIdentity,
    };

    fn column(unique_id: i64) -> HandleColumnIdentity {
        // The Go anchor uses ID=0, Index=0, and differs columns by UniqueID.
        HandleColumnIdentity::with_type_fingerprint(0, unique_id, 0, 1)
    }

    fn common(
        table_id: i64,
        index_id: i64,
        columns: Vec<HandleColumnIdentity>,
    ) -> CommonHandleIdentity {
        CommonHandleIdentity::new(
            Some(HandleMetadataIdentity::new(table_id)),
            Some(HandleMetadataIdentity::new(index_id)),
            Some(columns),
        )
    }

    fn assert_common_differs(first: &CommonHandleIdentity, second: &CommonHandleIdentity) {
        assert_ne!(first.hash64(), second.hash64());
        assert!(!first.equals(second));
    }

    #[test]
    fn source_test_common_handle_matching_hash_and_identity() {
        let first = common(1, 1, vec![column(1), column(2)]);
        let second = common(1, 1, vec![column(1), column(2)]);

        assert_eq!(first.hash64(), second.hash64());
        assert!(first.equals(&second));
    }

    #[test]
    fn source_test_common_handle_metadata_changes_hash_and_equality() {
        let first = common(1, 1, vec![column(1), column(2)]);

        let second = common(2, 1, vec![column(1), column(2)]);
        assert_common_differs(&first, &second);

        let second = common(1, 2, vec![column(1), column(2)]);
        assert_common_differs(&first, &second);
    }

    #[test]
    fn source_test_common_handle_column_order_and_identity_change_hash() {
        let first = common(1, 1, vec![column(1), column(2)]);

        let second = common(1, 1, vec![column(2), column(2)]);
        assert_common_differs(&first, &second);

        let second = common(1, 1, vec![column(2), column(1)]);
        assert_common_differs(&first, &second);

        let second = common(1, 1, vec![column(1), column(2)]);
        assert_eq!(first.hash64(), second.hash64());
        assert!(first.equals(&second));
    }

    #[test]
    fn source_test_common_handle_nil_and_empty_option_framing() {
        let nil_columns = CommonHandleIdentity::new(
            Some(HandleMetadataIdentity::new(1)),
            Some(HandleMetadataIdentity::new(1)),
            None,
        );
        let empty_columns = CommonHandleIdentity::new(
            Some(HandleMetadataIdentity::new(1)),
            Some(HandleMetadataIdentity::new(1)),
            Some(Vec::new()),
        );

        assert_ne!(nil_columns.hash64(), empty_columns.hash64());
        assert!(!nil_columns.equals(&empty_columns));

        let cloned = nil_columns.clone_identity();
        assert_eq!(nil_columns.hash64(), cloned.hash64());
        assert!(nil_columns.equals(&cloned));
    }

    #[test]
    fn source_test_int_handle_matching_and_column_change() {
        let first = IntHandleIdentity::new(Some(column(1)));
        let second = IntHandleIdentity::new(Some(column(1)));
        assert_eq!(first.hash64(), second.hash64());
        assert!(first.equals(&second));

        let different = IntHandleIdentity::new(Some(column(2)));
        assert_ne!(first.hash64(), different.hash64());
        assert!(!first.equals(&different));
    }

    #[test]
    fn source_test_int_handle_nil_and_present_framing() {
        let nil = IntHandleIdentity::new(None);
        let present = IntHandleIdentity::new(Some(column(1)));
        assert_ne!(nil.hash64(), present.hash64());
        assert!(!nil.equals(&present));
    }
}
