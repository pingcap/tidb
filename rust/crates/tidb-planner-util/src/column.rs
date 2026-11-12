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

//! Go `pkg/planner/util/column.go`: index columns resolved to expression
//! columns.

use tidb_expr::column::Column;
use tidb_model::{ColumnInfo, GoShared, IndexColumn, IndexInfo};

/// Go `types.UnspecifiedLength`.
const UNSPECIFIED_LENGTH: i64 = tidb_datatype::UNSPECIFIED_LENGTH;

/// Go `indexCol2Col`: the column of `cols` whose `colInfos` entry names the
/// index column, a copy flagged `IsPrefix` when the index holds only a
/// prefix of it.
fn index_col_2_col(col_infos: &[GoShared<ColumnInfo>], cols: &[Column], col: &IndexColumn) -> Option<Column> {
    for (i, info) in col_infos.iter().enumerate() {
        let info = info.read();
        if info.name.lowercase() == col.name.lowercase() {
            if col.length > 0 && info.field_type.flen() > col.length {
                let mut c = cols[i].clone();
                c.is_prefix = true;
                return Some(c);
            }
            return Some(cols[i].clone());
        }
    }
    None
}

/// Go `indexInfo2ColsFlags`.
#[derive(Clone, Copy)]
struct IndexInfo2ColsFlags {
    extract_prefix_cols: bool,
    extract_full_cols: bool,
}

/// Go `indexInfo2ColsImpl`.
#[allow(clippy::type_complexity)]
fn index_info_2_cols_impl(
    col_infos: &[GoShared<ColumnInfo>],
    cols: &[Column],
    index: &IndexInfo,
    flags: IndexInfo2ColsFlags,
) -> (Vec<Column>, Vec<i64>, Vec<Option<Column>>, Vec<i64>) {
    let mut prefix_cols = Vec::new();
    let mut prefix_lens = Vec::new();
    let mut full_cols = Vec::new();
    let mut full_lens = Vec::new();
    let mut prefix_complete = false;

    for c in index.columns.iter_deref() {
        let c = c.read();
        let Some(col) = index_col_2_col(col_infos, cols, &c) else {
            prefix_complete = true;
            if !flags.extract_full_cols {
                return (prefix_cols, prefix_lens, full_cols, full_lens);
            }
            full_cols.push(None);
            full_lens.push(UNSPECIFIED_LENGTH);
            continue;
        };

        // We use `types.UnspecifiedLength` to specifically indicate that a column does not
        // have a prefix length (see the `hasPrefix` function in util/ranger, for example).
        // In other words, `length` is only used for indexed columns with a prefix length
        // (as long as it is less than the full length).
        let mut length = c.length;
        if length != UNSPECIFIED_LENGTH && Some(length) == col.ret_type.as_ref().map(|ty| ty.flen()) {
            length = UNSPECIFIED_LENGTH;
        }
        if flags.extract_prefix_cols && !prefix_complete {
            prefix_cols.push(col.clone());
            prefix_lens.push(length);
        }
        if flags.extract_full_cols {
            full_cols.push(Some(col));
            full_lens.push(length);
        }
    }
    (prefix_cols, prefix_lens, full_cols, full_lens)
}

/// Go `IndexInfo2PrefixCols`: the index's leading columns that `cols`
/// holds, up to the first it does not, with their lengths.
#[must_use]
pub fn index_info_2_prefix_cols(
    col_infos: &[GoShared<ColumnInfo>],
    cols: &[Column],
    index: &IndexInfo,
) -> (Vec<Column>, Vec<i64>) {
    let flags = IndexInfo2ColsFlags { extract_prefix_cols: true, extract_full_cols: false };
    let (prefix_cols, prefix_lens, _, _) = index_info_2_cols_impl(col_infos, cols, index, flags);
    (prefix_cols, prefix_lens)
}

/// Go `IndexInfo2FullCols`: every index column, `None` where `cols` does
/// not hold it, with their lengths.
#[must_use]
pub fn index_info_2_full_cols(
    col_infos: &[GoShared<ColumnInfo>],
    cols: &[Column],
    index: &IndexInfo,
) -> (Vec<Option<Column>>, Vec<i64>) {
    let flags = IndexInfo2ColsFlags { extract_prefix_cols: false, extract_full_cols: true };
    let (_, _, full_cols, full_lens) = index_info_2_cols_impl(col_infos, cols, index, flags);
    (full_cols, full_lens)
}

/// Go `IndexInfo2Cols`: [`index_info_2_prefix_cols`] and
/// [`index_info_2_full_cols`] together.
#[must_use]
#[allow(clippy::type_complexity)]
pub fn index_info_2_cols(
    col_infos: &[GoShared<ColumnInfo>],
    cols: &[Column],
    index: &IndexInfo,
) -> (Vec<Column>, Vec<i64>, Vec<Option<Column>>, Vec<i64>) {
    let flags = IndexInfo2ColsFlags { extract_prefix_cols: true, extract_full_cols: true };
    index_info_2_cols_impl(col_infos, cols, index, flags)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::expression::Expression;
    use tidb_model::GoSharedPointerSlice;

    /// Go `TestIndexInfo2Cols`.
    #[test]
    fn test_index_info_2_cols() {
        let column = |id: i64| {
            let mut column = Column::new(id, FieldType::new(FieldTypeCode::LongLong));
            column.id = id;
            column
        };
        let (col0, col1, col2) = (column(0), column(1), column(2));
        let col_info = |id: i64| GoShared::new(ColumnInfo { id, name: CiString::new(&id.to_string()), ..Default::default() });
        let (col_info0, col_info1, col_info2) = (col_info(0), col_info(1), col_info(2));
        let index_col = |name: &str| GoShared::new(IndexColumn { name: CiString::new(name), ..Default::default() });
        let index_info = IndexInfo { columns: GoSharedPointerSlice::from_handles(vec![Some(index_col("0")), Some(index_col("1")), Some(index_col("2"))]), ..Default::default() };

        let (res_cols, lengths) = index_info_2_prefix_cols(&[col_info0.clone()], &[col0.clone()], &index_info);
        assert_eq!(res_cols.len(), 1);
        assert_eq!(lengths.len(), 1);
        assert!(res_cols[0].equal_column(&Expression::Column(col0.clone())));

        let (res_cols, lengths) = index_info_2_prefix_cols(&[col_info1.clone()], &[col1.clone()], &index_info);
        assert_eq!(res_cols.len(), 0);
        assert_eq!(lengths.len(), 0);

        let (res_cols, lengths) =
            index_info_2_prefix_cols(&[col_info0.clone(), col_info1], &[col0.clone(), col1.clone()], &index_info);
        assert_eq!(res_cols.len(), 2);
        assert_eq!(lengths.len(), 2);
        assert!(res_cols[0].equal_column(&Expression::Column(col0.clone())));
        assert!(res_cols[1].equal_column(&Expression::Column(col1.clone())));

        // If col1 has been pruned, the prefix columns should just be [col0]
        let cols = [col0.clone(), col2.clone()];
        let col_infos = [col_info0, col_info2];
        let (prefix_cols, prefix_lens, full_cols, full_lens) = index_info_2_cols(&col_infos, &cols, &index_info);
        let (res_cols, lengths) = index_info_2_prefix_cols(&col_infos, &cols, &index_info);
        let ids = |cols: &[Column]| cols.iter().map(|col| col.unique_id).collect::<Vec<_>>();
        let full_ids = |cols: &[Option<Column>]| cols.iter().map(|col| col.as_ref().map(|col| col.unique_id)).collect::<Vec<_>>();
        assert_eq!(ids(&res_cols), ids(&prefix_cols));
        assert_eq!(lengths, prefix_lens);
        assert_eq!(res_cols.len(), 1);
        assert_eq!(lengths.len(), 1);
        assert!(res_cols[0].equal_column(&Expression::Column(col0.clone())));

        // If col1 has been pruned, the full columns should be [col0, nil, col2]
        let (res_cols, lengths) = index_info_2_full_cols(&col_infos, &cols, &index_info);
        assert_eq!(full_ids(&res_cols), full_ids(&full_cols));
        assert_eq!(lengths, full_lens);
        assert_eq!(res_cols.len(), 3);
        assert_eq!(lengths.len(), 3);
        assert!(res_cols[0].as_ref().unwrap().equal_column(&Expression::Column(col0.clone())));
        assert!(res_cols[1].is_none());
        assert!(res_cols[2].as_ref().unwrap().equal_column(&Expression::Column(col2.clone())));
    }
}
