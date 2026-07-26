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

//! `pkg/expression/schema.go`: the `Schema`, the ordered set of output columns
//! that every plan node exposes.
//!
//! DIVERGENCE: Go's `KeyInfo` is `[]*Column` sharing the pointers held in
//! `Schema.Columns`; this port stores owned [`Column`] copies in keys (values,
//! not aliases). All identity operations compare `UniqueID`, so behavior is
//! preserved; only pointer aliasing is not.
//!
//! DEFERRED (need `EvalContext`/`ScalarFunction`, or are analysis helpers to be
//! ported with their consumers): `IsUnique`, `ExtractColGroups`, `GetUsedList`,
//! `ExprFromSchema`/`ExprReferenceSchema`, `String`, and `MemoryUsage`.

use crate::column::Column;

/// `model.ExtraHandleID` (`pkg/meta/model/table.go`): the id of the implicit
/// `_tidb_rowid` handle column. Inlined to avoid depending on `tidb-model`.
const EXTRA_HANDLE_ID: i64 = -1;

/// Go `KeyInfo`: a candidate key, i.e. the columns that make a row unique.
pub type KeyInfo = Vec<Column>;

/// Go `Schema`: the ordered output columns of a plan node plus its known keys.
#[derive(Clone, Debug, Default)]
pub struct Schema {
    /// Go `Columns`: the ordered output columns.
    pub columns: Vec<Column>,
    /// Go `PKOrUK`: primary key or not-null unique keys.
    pub pk_or_uk: Vec<KeyInfo>,
    /// Go `NullableUK`: unique keys that allow null values.
    pub nullable_uk: Vec<KeyInfo>,
}

impl Schema {
    /// Go `NewSchema`: a schema made of the given columns (no keys).
    #[must_use]
    pub fn new(columns: Vec<Column>) -> Self {
        Schema {
            columns,
            pk_or_uk: Vec::new(),
            nullable_uk: Vec::new(),
        }
    }

    /// Go `Len`: the number of columns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Whether the schema has no columns.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Go `Append`: append columns.
    pub fn append(&mut self, cols: impl IntoIterator<Item = Column>) {
        self.columns.extend(cols);
    }

    /// Go `SetKeys`: set `PKOrUK`.
    pub fn set_keys(&mut self, keys: Vec<KeyInfo>) {
        self.pk_or_uk = keys;
    }

    /// Go `SetUniqueKeys`: set `NullableUK`.
    pub fn set_unique_keys(&mut self, keys: Vec<KeyInfo>) {
        self.nullable_uk = keys;
    }

    /// Go `ColumnIndex`: the position of a column by `UniqueID`, or `-1`.
    ///
    /// A prefix column is only a fallback: a full (non-prefix) column with the
    /// same `UniqueID` is preferred, since a clustered-index table can list the
    /// same column both as an index-key prefix and as the handle.
    #[must_use]
    pub fn column_index(&self, col: &Column) -> isize {
        let mut backup_idx: isize = -1;
        for (i, c) in self.columns.iter().enumerate() {
            if c.unique_id == col.unique_id {
                backup_idx = i as isize;
                if c.is_prefix {
                    continue;
                }
                return i as isize;
            }
        }
        backup_idx
    }

    /// Go `Contains`: whether the schema contains the column (by `UniqueID`).
    #[must_use]
    pub fn contains(&self, col: &Column) -> bool {
        self.column_index(col) != -1
    }

    /// Go `RetrieveColumn`: the schema's own column matching `col`, if present.
    #[must_use]
    pub fn retrieve_column(&self, col: &Column) -> Option<&Column> {
        let idx = self.column_index(col);
        if idx != -1 {
            Some(&self.columns[idx as usize])
        } else {
            None
        }
    }

    /// Go `ColumnsIndices`: the position of every column, or `None` if any is
    /// missing.
    #[must_use]
    pub fn columns_indices(&self, cols: &[Column]) -> Option<Vec<usize>> {
        let mut ret = Vec::with_capacity(cols.len());
        for col in cols {
            let pos = self.column_index(col);
            if pos == -1 {
                return None;
            }
            ret.push(pos as usize);
        }
        Some(ret)
    }

    /// Go `ColumnsByIndices`: the columns at the given offsets. Callers must
    /// provide valid offsets.
    #[must_use]
    pub fn columns_by_indices(&self, offsets: &[usize]) -> Vec<Column> {
        offsets.iter().map(|&o| self.columns[o].clone()).collect()
    }

    /// Go `GetExtraHandleColumn`: the trailing `_tidb_rowid` handle column, if
    /// the schema ends with one (checking the last two positions).
    #[must_use]
    pub fn get_extra_handle_column(&self) -> Option<&Column> {
        let n = self.columns.len();
        if n > 0 && self.columns[n - 1].id == EXTRA_HANDLE_ID {
            Some(&self.columns[n - 1])
        } else if n > 1 && self.columns[n - 2].id == EXTRA_HANDLE_ID {
            Some(&self.columns[n - 2])
        } else {
            None
        }
    }

    /// Go `Schema.Equal`: same length and column-for-column identity.
    #[must_use]
    pub fn equal(&self, other: &Schema) -> bool {
        if self.columns.len() != other.columns.len() {
            return false;
        }
        self.columns
            .iter()
            .zip(&other.columns)
            .all(|(a, b)| a.unique_id == b.unique_id)
    }
}

/// Go `MergeSchema`: concatenate two schemas' columns (keys are recomputed
/// elsewhere). Cloned so the result owns its columns.
#[must_use]
pub fn merge_schema(l_schema: Option<&Schema>, r_schema: Option<&Schema>) -> Option<Schema> {
    match (l_schema, r_schema) {
        (None, None) => None,
        (None, Some(r)) => Some(r.clone()),
        (Some(l), None) => Some(l.clone()),
        (Some(l), Some(r)) => {
            let mut columns = l.columns.clone();
            columns.extend(r.columns.iter().cloned());
            Some(Schema::new(columns))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode};

    fn col(unique_id: i64) -> Column {
        Column::new(unique_id, FieldType::new(FieldTypeCode::Long))
    }

    fn prefix_col(unique_id: i64) -> Column {
        let mut c = col(unique_id);
        c.is_prefix = true;
        c
    }

    #[test]
    fn index_contains_retrieve() {
        let s = Schema::new(vec![col(10), col(20), col(30)]);
        assert_eq!(s.len(), 3);
        assert!(!s.is_empty());
        assert_eq!(s.column_index(&col(20)), 1);
        assert_eq!(s.column_index(&col(99)), -1);
        assert!(s.contains(&col(30)));
        assert!(!s.contains(&col(99)));
        assert_eq!(s.retrieve_column(&col(10)).unwrap().unique_id, 10);
        assert!(s.retrieve_column(&col(99)).is_none());
    }

    #[test]
    fn column_index_prefers_full_over_prefix() {
        // A prefix column appears before the full column with the same id.
        let s = Schema::new(vec![prefix_col(5), col(5)]);
        assert_eq!(s.column_index(&col(5)), 1);
        // Only a prefix present -> fall back to it.
        let s2 = Schema::new(vec![col(1), prefix_col(5)]);
        assert_eq!(s2.column_index(&col(5)), 1);
    }

    #[test]
    fn columns_indices_all_or_nothing() {
        let s = Schema::new(vec![col(10), col(20), col(30)]);
        assert_eq!(s.columns_indices(&[col(30), col(10)]), Some(vec![2, 0]));
        assert_eq!(s.columns_indices(&[col(30), col(99)]), None);
        assert_eq!(s.columns_by_indices(&[2, 0]).len(), 2);
    }

    #[test]
    fn extra_handle_column_detection() {
        let mut handle = col(7);
        handle.id = EXTRA_HANDLE_ID;
        // last position
        let s = Schema::new(vec![col(1), handle.clone()]);
        assert!(s.get_extra_handle_column().is_some());
        // second-to-last position
        let s2 = Schema::new(vec![col(1), handle.clone(), col(2)]);
        assert!(s2.get_extra_handle_column().is_some());
        // absent
        let s3 = Schema::new(vec![col(1), col(2)]);
        assert!(s3.get_extra_handle_column().is_none());
    }

    #[test]
    fn equal_and_merge() {
        let a = Schema::new(vec![col(1), col(2)]);
        let b = Schema::new(vec![col(1), col(2)]);
        let c = Schema::new(vec![col(1), col(3)]);
        assert!(a.equal(&b));
        assert!(!a.equal(&c));

        let merged = merge_schema(Some(&a), Some(&c)).unwrap();
        assert_eq!(merged.len(), 4);
        assert!(merge_schema(None, None).is_none());
        assert_eq!(merge_schema(None, Some(&a)).unwrap().len(), 2);
    }

    #[test]
    fn set_keys() {
        let mut s = Schema::new(vec![col(1), col(2)]);
        s.set_keys(vec![vec![col(1)]]);
        s.set_unique_keys(vec![vec![col(2)]]);
        assert_eq!(s.pk_or_uk.len(), 1);
        assert_eq!(s.nullable_uk.len(), 1);
        // Clone is a deep copy.
        let s2 = s.clone();
        assert_eq!(s2.pk_or_uk[0][0].unique_id, 1);
    }
}
