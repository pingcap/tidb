//! The part of Go `pkg/lightning/common/util.go` the encoder reads.

use tidb_model::{ColumnInfo, GoShared, TableInfo};

/// Go `EncodeIntRowID`: `codec.EncodeComparableVarint(nil, rowID)`.
#[must_use]
pub fn encode_int_row_id(row_id: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    tidb_codec::encode_comparable_varint(&mut out, row_id);
    out
}

/// Go `TableHasAutoRowID`: the table's rows live under `_tidb_rowid`.
#[must_use]
pub fn table_has_auto_row_id(info: &TableInfo) -> bool {
    !info.pk_is_handle && !info.is_common_handle
}

/// Go `TableHasAutoID`.
#[must_use]
pub fn table_has_auto_id(info: &TableInfo) -> bool {
    table_has_auto_row_id(info)
        || info.get_auto_increment_col_info().is_some()
        || info.contains_auto_random_bits()
}

/// Go `GetAutoRandomColumn`: the integer primary key, or the first column
/// of a clustered primary key, of a table with AUTO_RANDOM bits.
#[must_use]
pub fn get_auto_random_column(info: &TableInfo) -> Option<GoShared<ColumnInfo>> {
    if !info.contains_auto_random_bits() {
        return None;
    }
    if info.pk_is_handle {
        return info.get_pk_col_info();
    }
    if info.is_common_handle {
        let pk = info.indices.iter_deref().find(|index| index.read().primary)?;
        let offset = pk.read().columns.get(0)?.read().offset;
        return usize::try_from(offset).ok().and_then(|offset| info.columns.get(offset));
    }
    None
}
