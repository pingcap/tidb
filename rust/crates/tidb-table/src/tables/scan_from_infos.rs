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

//! Go `pkg/table/tables/tables.go`'s primary-key lookups and the table scan
//! executors built from the table's metadata (`BuildTableScanFromInfos`,
//! `BuildPartitionTableScanFromInfos`), which the planner's `ToPB` sends.

use tidb_model::{ColumnInfo, GoShared, IndexInfo, TableInfo};
use tidb_proto::tipb;

/// Go `FindPrimaryIndex`: the first index flagged primary, or none.
#[must_use]
pub fn find_primary_index(tbl_info: &TableInfo) -> Option<GoShared<IndexInfo>> {
    tbl_info.indices.iter_deref().find(|idx| idx.read().primary)
}

/// Go `TryGetCommonPkColumnIds`: the IDs of the primary key's columns when
/// the table has a common handle.
#[must_use]
pub fn try_get_common_pk_column_ids(tbl: &TableInfo) -> Vec<i64> {
    if !tbl.is_common_handle {
        return Vec::new();
    }
    let pk_idx = find_primary_index(tbl).expect("a common-handle table has a primary index");
    let pk_idx = pk_idx.read();
    pk_idx
        .columns
        .iter_deref()
        .map(|idx_col| {
            let offset = idx_col.read().offset as usize;
            tbl.columns.get(offset).expect("index column offset in range").read().id
        })
        .collect()
}

/// Go `PrimaryPrefixColumnIDs`: the primary key's columns indexed by a
/// prefix shorter than the column.
#[must_use]
pub fn primary_prefix_column_ids(tbl: &TableInfo) -> Vec<i64> {
    let mut prefix_cols = Vec::new();
    for idx in tbl.indices.iter_deref() {
        let idx = idx.read();
        if !idx.primary {
            continue;
        }
        for col in idx.columns.iter_deref() {
            let col = col.read();
            let column = tbl.columns.get(col.offset as usize).expect("index column offset in range");
            let column = column.read();
            if col.length > 0 && column.field_type.flen() > col.length {
                prefix_cols.push(column.id);
            }
        }
    }
    prefix_cols
}

/// Go `BuildTableScanFromInfos`: the `tipb.TableScan` of `column_infos`.
#[must_use]
pub fn build_table_scan_from_infos(
    table_info: &TableInfo,
    column_infos: &[GoShared<ColumnInfo>],
    is_tiflash_store: bool,
) -> tipb::TableScan {
    let pk_col_ids = try_get_common_pk_column_ids(table_info);
    let mut ts_exec = tipb::TableScan {
        table_id: Some(table_info.id),
        columns: tidb_util::misc::columns_to_proto(column_infos, table_info.pk_is_handle, false, is_tiflash_store),
        primary_column_ids: pk_col_ids,
        ..tipb::TableScan::default()
    };
    if table_info.is_common_handle {
        ts_exec.primary_prefix_column_ids = primary_prefix_column_ids(table_info);
    }
    ts_exec
}

/// Go `BuildPartitionTableScanFromInfos`: the `tipb.PartitionTableScan` of
/// `column_infos`. Currently it's only used for TiFlash.
#[must_use]
pub fn build_partition_table_scan_from_infos(
    table_info: &TableInfo,
    column_infos: &[GoShared<ColumnInfo>],
    fast_scan: bool,
) -> tipb::PartitionTableScan {
    let pk_col_ids = try_get_common_pk_column_ids(table_info);
    let mut ts_exec = tipb::PartitionTableScan {
        table_id: Some(table_info.id),
        columns: tidb_util::misc::columns_to_proto(column_infos, table_info.pk_is_handle, false, true),
        primary_column_ids: pk_col_ids,
        is_fast_scan: Some(fast_scan),
        ..tipb::PartitionTableScan::default()
    };
    if table_info.is_common_handle {
        ts_exec.primary_prefix_column_ids = primary_prefix_column_ids(table_info);
    }
    ts_exec
}

#[cfg(test)]
mod tests {
    use tidb_datatype::{set_new_collation_enabled_for_test, FieldType, FieldTypeCode};
    use tidb_model::{ColumnInfo, GoShared, TableInfo};
    use tidb_mysql::types::GeneratedColumnFlag;

    use super::build_partition_table_scan_from_infos;

    /// Go `TestGeneratedColumnFlagForTiFlash`'s
    /// `BuildPartitionTableScanFromInfos` half: the regression of issue
    /// 59831, where the partition scan passed `isTiFlashStore=false`.
    #[test]
    fn test_generated_column_flag_for_tiflash_partition_scan() {
        set_new_collation_enabled_for_test(false);
        let mut tp = FieldType::new(FieldTypeCode::from_mysql_type(15));
        tp.set_collation_name("utf8_bin");
        let gen_col = ColumnInfo {
            id: 1,
            field_type: tp.clone(),
            generated_expr_string: "lower(c1)".to_owned(),
            generated_stored: false,
            ..ColumnInfo::default()
        };
        let normal_col = ColumnInfo { id: 2, field_type: tp, ..ColumnInfo::default() };
        let cols = vec![GoShared::new(gen_col), GoShared::new(normal_col)];
        let table_info = TableInfo {
            id: 1,
            columns: tidb_model::GoSharedPointerSlice::from_handles(cols.iter().cloned().map(Some).collect()),
            ..TableInfo::default()
        };

        let gen_flag = GeneratedColumnFlag as i32;
        let pts_exec = build_partition_table_scan_from_infos(&table_info, &cols, false);
        assert_ne!(pts_exec.columns[0].flag.unwrap() & gen_flag, 0);
        assert_eq!(pts_exec.columns[1].flag.unwrap() & gen_flag, 0);
    }
}
