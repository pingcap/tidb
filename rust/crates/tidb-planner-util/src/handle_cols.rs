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

use tidb_planner_cascades_base::{new_hash_equaler, Hasher, NIL_FLAG, NOT_NIL_FLAG};

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

    /// Go `Column.UniqueID`, which is how a handle identity is matched back to
    /// the [`Column`](tidb_expr::column::Column) a plan's schema carries.
    #[must_use]
    pub const fn unique_id(&self) -> i64 {
        self.unique_id
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

    /// The unique ids of this common handle's columns, in handle order.
    #[must_use]
    pub fn column_unique_ids(&self) -> Vec<i64> {
        self.columns.as_ref().map_or_else(Vec::new, |columns| {
            columns
                .iter()
                .map(HandleColumnIdentity::unique_id)
                .collect()
        })
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

    /// The unique id of this int handle's column, or nothing when it has none.
    #[must_use]
    pub fn column_unique_ids(&self) -> Vec<i64> {
        self.column
            .as_ref()
            .map(HandleColumnIdentity::unique_id)
            .into_iter()
            .collect()
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

/// Go `HandleCols`' execution half (`pkg/planner/util/handle_cols.go`): the
/// handle columns of a table read, and how a row's handle is built from
/// them.
#[derive(Clone, Debug)]
pub enum HandleCols {
    /// Go `IntHandleCols`.
    Int(IntHandleCols),
    /// Go `CommonHandleCols`.
    Common(CommonHandleCols),
}

/// Go `IntHandleCols`.
#[derive(Clone, Debug)]
pub struct IntHandleCols {
    /// Go `col`.
    pub col: tidb_expr::column::Column,
}

/// Go `CommonHandleCols`.
#[derive(Clone, Debug)]
pub struct CommonHandleCols {
    /// Go `tblInfo`.
    pub tbl_info: tidb_model::GoShared<tidb_model::TableInfo>,
    /// Go `idxInfo`: the clustered primary key.
    pub idx_info: tidb_model::GoShared<tidb_model::IndexInfo>,
    /// Go `columns`.
    pub columns: Vec<tidb_expr::column::Column>,
}

/// The error Go's `sc.HandleError(err)` passes on from encoding a handle.
pub type HandleColsError = tidb_codec::CodecError;

impl CommonHandleCols {
    /// Go `NewCommonHandleCols`: the primary key's columns among
    /// `table_columns`, one per key column, by the key column's offset.
    #[must_use]
    pub fn new(
        tbl_info: tidb_model::GoShared<tidb_model::TableInfo>,
        idx_info: tidb_model::GoShared<tidb_model::IndexInfo>,
        table_columns: &[tidb_expr::column::Column],
    ) -> Self {
        let columns = idx_info
            .read()
            .columns
            .iter_deref()
            .map(|ic| table_columns[ic.read().offset as usize].clone())
            .collect();
        Self {
            tbl_info,
            idx_info,
            columns,
        }
    }

    /// Go `buildHandleByDatumsBuffer`: `TruncateIndexValues` over the primary
    /// key, `codec.EncodeKey`, `kv.NewCommonHandle`.
    fn build_handle_by_datums_buffer(
        &self,
        zone: &tidb_datatype::SessionTimeZone,
        mut datum_buf: Vec<tidb_datatype::Datum>,
    ) -> Result<tidb_txnkv::Handle, HandleColsError> {
        let tbl_info = self.tbl_info.read();
        let idx_info = self.idx_info.read();
        let table_columns: Vec<_> = tbl_info.columns.iter_deref().collect();
        for (datum, ic) in datum_buf.iter_mut().zip(idx_info.columns.iter_deref()) {
            let ic = ic.read();
            tidb_tablecodec::cut_datum_by_prefix_len(
                datum,
                ic.length,
                &table_columns[ic.offset as usize].read().field_type,
            );
        }
        let handle_bytes = tidb_codec::encode_key_in_timezone(zone, &datum_buf)?;
        Ok(tidb_txnkv::Handle::Common(tidb_txnkv::CommonHandle::new(handle_bytes)?))
    }

    fn ret_type(&self, i: usize) -> tidb_datatype::FieldType {
        self.columns[i]
            .ret_type
            .clone()
            .unwrap_or_else(|| tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Null))
    }
}

impl HandleCols {
    /// Go `BuildHandle`: the handle of a row of the table read's schema.
    ///
    /// # Errors
    /// The handle does not encode.
    pub fn build_handle(
        &self,
        zone: &tidb_datatype::SessionTimeZone,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<tidb_txnkv::Handle, HandleColsError> {
        match self {
            Self::Int(ib) => Ok(int_handle(row.get_int64(ib.col.index as usize))),
            Self::Common(cb) => {
                let datum_buf = cb
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, col)| row.get_datum(col.index as usize, &cb.ret_type(i)))
                    .collect();
                cb.build_handle_by_datums_buffer(zone, datum_buf)
            }
        }
    }

    /// Go `BuildHandleFromIndexRow`: the handle an index row carries last.
    ///
    /// # Errors
    /// The handle does not encode.
    pub fn build_handle_from_index_row(
        &self,
        zone: &tidb_datatype::SessionTimeZone,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<tidb_txnkv::Handle, HandleColsError> {
        match self {
            Self::Int(_) => Ok(int_handle(row.get_int64(row.len() - 1))),
            Self::Common(cb) => {
                let n = cb.columns.len();
                let datum_buf =
                    (0..n).map(|i| row.get_datum(row.len() - n + i, &cb.ret_type(i))).collect();
                cb.build_handle_by_datums_buffer(zone, datum_buf)
            }
        }
    }

    /// Go `BuildPartitionHandleFromIndexRow`: the handle an index row carries
    /// before its last column, the physical table id.
    ///
    /// # Errors
    /// The handle does not encode.
    pub fn build_partition_handle_from_index_row(
        &self,
        zone: &tidb_datatype::SessionTimeZone,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<tidb_txnkv::Handle, HandleColsError> {
        let handle = match self {
            Self::Int(_) => int_handle(row.get_int64(row.len() - 2)),
            Self::Common(cb) => {
                let n = cb.columns.len();
                let datum_buf =
                    (0..n).map(|i| row.get_datum(row.len() - 1 - n + i, &cb.ret_type(i))).collect();
                cb.build_handle_by_datums_buffer(zone, datum_buf)?
            }
        };
        Ok(tidb_txnkv::Handle::Partition(tidb_txnkv::PartitionHandle::new(
            row.get_int64(row.len() - 1),
            Some(handle),
        )))
    }

    /// Go `BuildHandleByDatums`.
    ///
    /// # Errors
    /// The handle does not encode.
    pub fn build_handle_by_datums(
        &self,
        zone: &tidb_datatype::SessionTimeZone,
        row: &[tidb_datatype::Datum],
    ) -> Result<tidb_txnkv::Handle, HandleColsError> {
        match self {
            Self::Int(ib) => Ok(int_handle(row[ib.col.index as usize].get_int64())),
            Self::Common(cb) => {
                let datum_buf = cb.columns.iter().map(|col| row[col.index as usize].clone()).collect();
                cb.build_handle_by_datums_buffer(zone, datum_buf)
            }
        }
    }

    /// Go `Compare(a, b, ctors, tpCtx)`: two rows ordered by the handle
    /// columns. As Go, an integer handle compares under the row's collator
    /// at its offset, a common handle under `ctors[i]` for its `i`-th
    /// column.
    ///
    /// # Errors
    /// Two values do not compare.
    pub fn compare(
        &self,
        a: &[tidb_datatype::Datum],
        b: &[tidb_datatype::Datum],
        ctors: &[tidb_charset::Collation],
    ) -> Result<std::cmp::Ordering, String> {
        let compare = |offset: usize, collator: tidb_charset::Collation| {
            a[offset].compare(&b[offset], collator).map_err(|error| format!("{error:?}"))
        };
        match self {
            Self::Int(ib) => compare(ib.col.index as usize, ctors[ib.col.index as usize]),
            Self::Common(cb) => {
                for (i, col) in cb.columns.iter().enumerate() {
                    let cmp = compare(col.index as usize, ctors[i])?;
                    if cmp != std::cmp::Ordering::Equal {
                        return Ok(cmp);
                    }
                }
                Ok(std::cmp::Ordering::Equal)
            }
        }
    }

    /// Go `IsInt`.
    #[must_use]
    pub fn is_int(&self) -> bool {
        matches!(self, Self::Int(_))
    }

    /// Go `NumCols`.
    #[must_use]
    pub fn num_cols(&self) -> usize {
        match self {
            Self::Int(_) => 1,
            Self::Common(cb) => cb.columns.len(),
        }
    }

    /// Go `MemoryUsage`: an integer handle's column, or
    /// `emptyCommonHandleColsSize` (two pointers and a slice) and the common
    /// handle's column pointers and columns.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        match self {
            Self::Int(ib) => ib.col.memory_usage(),
            Self::Common(cb) => {
                const EMPTY_COMMON_HANDLE_COLS_SIZE: i64 = 8 + 8 + 24;
                EMPTY_COMMON_HANDLE_COLS_SIZE
                    + cb.columns.capacity() as i64 * 8
                    + cb.columns.iter().map(tidb_expr::column::Column::memory_usage).sum::<i64>()
            }
        }
    }

    /// Go `GetCol`.
    #[must_use]
    pub fn get_col(&self, idx: usize) -> &tidb_expr::column::Column {
        match self {
            Self::Int(ib) => &ib.col,
            Self::Common(cb) => &cb.columns[idx],
        }
    }

    /// Go `GetFieldsTypes`: the integer handle's `LONGLONG`, or the common
    /// handle columns' types.
    #[must_use]
    pub fn get_fields_types(&self) -> Vec<tidb_datatype::FieldType> {
        match self {
            Self::Int(_) => vec![tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong)],
            Self::Common(cb) => (0..cb.columns.len()).map(|i| cb.ret_type(i)).collect(),
        }
    }

    /// Go `ResolveIndices`: the columns' offsets in `schema`.
    ///
    /// # Errors
    /// A handle column is absent from `schema`.
    pub fn resolve_indices(&self, schema: &tidb_expr::schema::Schema) -> Result<Self, String> {
        let resolve = |col: &tidb_expr::column::Column| {
            let index = schema.column_index(col);
            if index < 0 {
                return Err(format!("Can't find column {} in schema", col.unique_id));
            }
            let mut col = col.clone();
            col.index = index as i64;
            Ok(col)
        };
        Ok(match self {
            Self::Int(ib) => Self::Int(IntHandleCols { col: resolve(&ib.col)? }),
            Self::Common(cb) => Self::Common(CommonHandleCols {
                tbl_info: cb.tbl_info.clone(),
                idx_info: cb.idx_info.clone(),
                columns: cb.columns.iter().map(resolve).collect::<Result<_, _>>()?,
            }),
        })
    }
}

fn int_handle(value: i64) -> tidb_txnkv::Handle {
    tidb_txnkv::Handle::Int(tidb_txnkv::IntHandle::new(value))
}
