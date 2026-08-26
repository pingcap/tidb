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

//! Ports of the remaining `pkg/meta/model` unit tests from Go master that are
//! not yet covered by the inline test modules (`bdr_test.go`,
//! `placement_test.go`, and `table_mode_test.go` are already ported inline in
//! `bdr.rs`, `placement.rs`, and `table_mode.rs`).
//!
//! This batch covers `column_test.go`, `index_test.go`, and `table_test.go`.

use crate::column::ColumnInfo;
use crate::db::DBInfo;
use crate::go_any::{ColumnDefaultValue, GoAny};
use crate::go_runtime::{GoShared, GoSharedPointerSlice};
use crate::index::{
    find_index_by_columns_for_foreign_key, get_global_index_v1_supported,
    is_index_prefix_covered, is_index_prefix_covered_for_foreign_key, IndexColumn, IndexInfo,
};
use crate::partition::PartitionInfo;
use crate::table::{FKInfo, SequenceInfo, TTLInfo, DEFAULT_TTL_JOB_INTERVAL};
use crate::table_info::TableInfo;
use tidb_ast::{CiString, IndexType, PartitionType};
use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};

const HOUR_NANOS: i64 = 3_600_000_000_000;

/// Go `newColumnForTest` (`table_test.go:50` region, also used by
/// `index_test.go`).
fn new_column_for_test(id: i64, offset: i64) -> ColumnInfo {
    ColumnInfo {
        id,
        name: CiString::new(format!("c_{id}")),
        offset,
        ..Default::default()
    }
}

/// Go `newIndexForTest`.
fn new_index_for_test(id: i64, cols: &[ColumnInfo]) -> IndexInfo {
    let idx_cols: Vec<IndexColumn> = cols
        .iter()
        .map(|c| IndexColumn {
            name: c.name.clone(),
            offset: c.offset,
            ..Default::default()
        })
        .collect();
    IndexInfo {
        id,
        name: CiString::new(format!("i_{id}")),
        columns: idx_cols.into(),
        ..Default::default()
    }
}

// Go TestDefaultValue (`column_test.go:28`): plain/BIT default values through
// SetDefaultValue/GetDefaultValue plus the JSON round-trip consistency matrix.
#[test]
fn column_default_value() {
    let src_col = ColumnInfo {
        id: 1,
        ..Default::default()
    };
    let rand_plain_str = ColumnDefaultValue::str("random_plain_string");

    // oldPlainCol.
    let mut old_plain_col = src_col.clone_like_go();
    old_plain_col.name = CiString::new("oldPlainCol");
    old_plain_col.field_type = FieldType::new(FieldTypeCode::Long);
    old_plain_col.default_value = rand_plain_str.clone().into();
    old_plain_col.origin_default_value = rand_plain_str.clone().into();

    // newPlainCol.
    let mut new_plain_col = src_col.clone_like_go();
    new_plain_col.name = CiString::new("newPlainCol");
    new_plain_col.field_type = FieldType::new(FieldTypeCode::Long);
    new_plain_col
        .set_default_value(ColumnDefaultValue::Int(1))
        .expect("plain columns accept any value");
    assert_eq!(
        new_plain_col.get_default_value(),
        Some(ColumnDefaultValue::Int(1))
    );
    new_plain_col
        .set_default_value(rand_plain_str.clone())
        .unwrap();
    assert_eq!(new_plain_col.get_default_value(), Some(rand_plain_str));

    // A BIT default of raw bytes (Go string([]byte{25, 185})).
    let rand_bit_str = ColumnDefaultValue::string_bytes(vec![25, 185]);

    // oldBitCol.
    let mut old_bit_col = src_col.clone_like_go();
    old_bit_col.name = CiString::new("oldBitCol");
    old_bit_col.field_type = FieldType::new(FieldTypeCode::Bit);
    old_bit_col.default_value = rand_bit_str.clone().into();
    old_bit_col.origin_default_value = rand_bit_str.clone().into();

    // newBitCol.
    let mut new_bit_col = src_col.clone_like_go();
    new_bit_col.name = CiString::new("newBitCol");
    new_bit_col.field_type = FieldType::new(FieldTypeCode::Bit);
    // Only string type is allowed in a BIT column.
    let err = new_bit_col
        .set_default_value(ColumnDefaultValue::Int(1))
        .unwrap_err();
    assert!(err.to_string().contains("Invalid default value"));
    assert_eq!(
        new_bit_col.get_default_value(),
        Some(ColumnDefaultValue::Int(1))
    );
    new_bit_col.set_default_value(rand_bit_str.clone()).unwrap();
    assert_eq!(new_bit_col.get_default_value(), Some(rand_bit_str));

    // nullBitCol.
    let mut null_bit_col = src_col.clone_like_go();
    null_bit_col.name = CiString::new("nullBitCol");
    null_bit_col.field_type = FieldType::new(FieldTypeCode::Bit);
    null_bit_col
        .set_origin_default_value(GoAny::nil())
        .unwrap();
    assert!(null_bit_col.get_origin_default_value().is_nil());

    struct Case {
        name: &'static str,
        column: ColumnInfo,
        is_consistent: bool,
    }
    let cases = vec![
        Case {
            name: "oldPlainCol",
            column: old_plain_col,
            is_consistent: true,
        },
        Case {
            name: "oldBitCol",
            column: old_bit_col,
            // The raw BIT bytes do not survive a JSON round trip (invalid
            // UTF-8 becomes U+FFFD), exactly as in Go.
            is_consistent: false,
        },
        Case {
            name: "newPlainCol",
            column: new_plain_col,
            is_consistent: true,
        },
        Case {
            name: "newBitCol",
            column: new_bit_col,
            is_consistent: true,
        },
        Case {
            name: "nullBitCol",
            column: null_bit_col,
            is_consistent: true,
        },
    ];
    for tc in cases {
        let comment = format!("{} assertion failed", tc.name);
        let bytes = serde_json::to_string(&tc.column).expect(&comment);
        let new_col: ColumnInfo = serde_json::from_str(&bytes).expect(&comment);
        if tc.is_consistent {
            assert_eq!(
                tc.column.get_default_value(),
                new_col.get_default_value(),
                "{comment}"
            );
            assert_eq!(
                tc.column.get_origin_default_value(),
                new_col.get_origin_default_value(),
                "{comment}"
            );
        } else {
            assert_ne!(
                tc.column.get_default_value(),
                new_col.get_default_value(),
                "{comment}"
            );
            assert_ne!(
                tc.column.get_origin_default_value(),
                new_col.get_origin_default_value(),
                "{comment}"
            );
        }
    }

    let extra_phys_tbl_id_col = ColumnInfo::new_extra_phys_tbl_id_col_info();
    assert_eq!(
        extra_phys_tbl_id_col.get_flag(),
        u64::from(FieldTypeFlags::NOT_NULL)
    );
    assert_eq!(extra_phys_tbl_id_col.get_type(), FieldTypeCode::LongLong);
}

// Go TestIsIndexPrefixCovered (`index_test.go:47`): prefix coverage over
// leading columns plus the foreign-key-safe partial-index variants.
#[test]
fn index_is_index_prefix_covered() {
    let c0 = new_column_for_test(0, 0);
    let c1 = new_column_for_test(1, 1);
    let c2 = new_column_for_test(2, 2);
    let c3 = new_column_for_test(3, 3);
    let c4 = new_column_for_test(4, 4);

    let i0 = new_index_for_test(0, &[c0.clone(), c1.clone(), c2.clone()]);
    let i1 = new_index_for_test(1, &[c4.clone(), c2.clone()]);

    let tbl = TableInfo {
        id: 1,
        name: CiString::new("t"),
        columns: vec![c0, c1, c2, c3, c4].into(),
        indices: vec![i0, i1].into(),
        ..Default::default()
    };
    let index = |id: i64| tbl.indices.iter_deref().find(|i| i.read().id == id).unwrap();

    fn names(values: &[&str]) -> Vec<CiString> {
        values.iter().map(|v| CiString::new(*v)).collect()
    }

    let i0h = index(0);
    assert!(is_index_prefix_covered(&tbl, &i0h.read(), &names(&["c_0"])));
    assert!(is_index_prefix_covered(
        &tbl,
        &i0h.read(),
        &names(&["c_0", "c_1", "c_2"])
    ));
    assert!(!is_index_prefix_covered(
        &tbl,
        &i0h.read(),
        &names(&["c_1"])
    ));
    assert!(!is_index_prefix_covered(
        &tbl,
        &i0h.read(),
        &names(&["c_2"])
    ));
    assert!(!is_index_prefix_covered(
        &tbl,
        &i0h.read(),
        &names(&["c_1", "c_2"])
    ));
    assert!(!is_index_prefix_covered(
        &tbl,
        &i0h.read(),
        &names(&["c_0", "c_2"])
    ));

    let i1h = index(1);
    assert!(is_index_prefix_covered(&tbl, &i1h.read(), &names(&["c_4"])));
    assert!(is_index_prefix_covered(
        &tbl,
        &i1h.read(),
        &names(&["c_4", "c_2"])
    ));
    assert!(!is_index_prefix_covered(
        &tbl,
        &i0h.read(),
        &names(&["c_2"])
    ));

    let mut safe_partial = new_index_for_test(2, &[new_column_for_test(0, 0), new_column_for_test(1, 1)]);
    safe_partial.condition_expr_string = "`c_1` is not null".to_owned();
    assert!(is_index_prefix_covered_for_foreign_key(
        &tbl,
        &safe_partial,
        &names(&["c_0", "c_1"])
    ));

    let mut safe_partial_on_first_fk_col =
        new_index_for_test(3, &[new_column_for_test(0, 0), new_column_for_test(1, 1)]);
    safe_partial_on_first_fk_col.condition_expr_string = "`c_0` is not null".to_owned();
    assert!(is_index_prefix_covered_for_foreign_key(
        &tbl,
        &safe_partial_on_first_fk_col,
        &names(&["c_0", "c_1"])
    ));

    let mut unsafe_partial_on_non_fk_col =
        new_index_for_test(4, &[new_column_for_test(0, 0), new_column_for_test(1, 1)]);
    unsafe_partial_on_non_fk_col.condition_expr_string = "`c_2` is not null".to_owned();
    assert!(!is_index_prefix_covered_for_foreign_key(
        &tbl,
        &unsafe_partial_on_non_fk_col,
        &names(&["c_0", "c_1"])
    ));

    let mut unsafe_partial_is_null =
        new_index_for_test(5, &[new_column_for_test(0, 0)]);
    unsafe_partial_is_null.condition_expr_string = "`c_0` is null".to_owned();
    assert!(!is_index_prefix_covered_for_foreign_key(
        &tbl,
        &unsafe_partial_is_null,
        &names(&["c_0"])
    ));

    let mut unsafe_partial_binary_condition =
        new_index_for_test(6, &[new_column_for_test(0, 0)]);
    unsafe_partial_binary_condition.condition_expr_string = "`c_0` > 0".to_owned();
    assert!(!is_index_prefix_covered_for_foreign_key(
        &tbl,
        &unsafe_partial_binary_condition,
        &names(&["c_0"])
    ));

    let mut bad_condition = new_index_for_test(7, &[new_column_for_test(0, 0)]);
    bad_condition.condition_expr_string = "`c_0` is".to_owned();
    assert!(!is_index_prefix_covered_for_foreign_key(
        &tbl,
        &bad_condition,
        &names(&["c_0"])
    ));

    // require.Same: FindIndexByColumnsForForeignKey returns the very index
    // handle for the safe partial index.
    let indices: GoSharedPointerSlice<IndexInfo> = GoSharedPointerSlice::from_handles(vec![
        Some(GoShared::new(unsafe_partial_on_non_fk_col)),
        Some(GoShared::new(safe_partial)),
    ]);
    let safe_handle = indices.get(1).unwrap();
    let found = find_index_by_columns_for_foreign_key(
        &tbl,
        &indices,
        &names(&["c_0", "c_1"]),
    )
    .expect("the safe partial index must be found");
    assert!(found.ptr_eq(&safe_handle));
}

// Go TestGlobalIndexV1SupportedForNextGen (`index_test.go:101`). The Go test
// asserts only when built with the `nextgen` build tag; this workspace gate
// compiles the Classic kernel (no nextgen feature), so the guarded assertion
// is inert exactly like Go's Classic build.
#[test]
fn index_global_index_v1_supported_for_next_gen() {
    if cfg!(feature = "nextgen") {
        assert!(get_global_index_v1_supported());
    }
}

/// Go `checkOffsets` (`table_test.go:36`).
fn check_offsets(tbl: &TableInfo, ids: &[i64]) {
    assert_eq!(ids.len(), tbl.columns.len());
    for (i, expected_id) in ids.iter().enumerate() {
        let col = tbl.columns.get(i).expect("column slot");
        let col = col.read();
        assert_eq!(col.name.lowercase(), format!("c_{expected_id}"));
        assert_eq!(col.offset, i as i64);
    }
    for col in tbl.columns.iter_deref() {
        let col = col.read();
        for idx in tbl.indices.iter_deref() {
            let idx = idx.read();
            for idx_col in idx.columns.iter_deref() {
                let idx_col = idx_col.read();
                if col.name.original() != idx_col.name.original() {
                    continue;
                }
                // Columns with the same name should have the same offset.
                assert_eq!(col.offset, idx_col.offset);
            }
        }
    }
}

// Go TestMoveColumnInfo (`table_test.go:89`): MoveColumnInfo reorders both the
// column slice and every index-column offset.
#[test]
fn table_move_column_info() {
    let c0 = new_column_for_test(0, 0);
    let c1 = new_column_for_test(1, 1);
    let c2 = new_column_for_test(2, 2);
    let c3 = new_column_for_test(3, 3);
    let c4 = new_column_for_test(4, 4);

    let i0 = new_index_for_test(0, &[c0.clone(), c1.clone(), c2.clone(), c3.clone(), c4.clone()]);
    let i1 = new_index_for_test(1, &[c4.clone(), c2.clone()]);
    let i2 = new_index_for_test(2, &[c0.clone(), c4.clone()]);
    let i3 = new_index_for_test(3, &[c1.clone(), c2.clone(), c3.clone()]);
    let i4 = new_index_for_test(4, &[c3.clone(), c2.clone(), c1.clone()]);

    let mut tbl = TableInfo {
        id: 1,
        name: CiString::new("t"),
        columns: vec![c0, c1, c2, c3, c4].into(),
        indices: vec![i0, i1, i2, i3, i4].into(),
        ..Default::default()
    };

    // Original offsets: [0, 1, 2, 3, 4]
    tbl.move_column_info(4, 0);
    check_offsets(&tbl, &[4, 0, 1, 2, 3]);
    tbl.move_column_info(2, 3);
    check_offsets(&tbl, &[4, 0, 2, 1, 3]);
    tbl.move_column_info(3, 2);
    check_offsets(&tbl, &[4, 0, 1, 2, 3]);
    tbl.move_column_info(0, 4);
    check_offsets(&tbl, &[0, 1, 2, 3, 4]);
    tbl.move_column_info(2, 2);
    check_offsets(&tbl, &[0, 1, 2, 3, 4]);
    tbl.move_column_info(0, 0);
    check_offsets(&tbl, &[0, 1, 2, 3, 4]);
    tbl.move_column_info(1, 4);
    check_offsets(&tbl, &[0, 2, 3, 4, 1]);
    tbl.move_column_info(3, 0);
    check_offsets(&tbl, &[4, 0, 2, 3, 1]);
}

// Go TestModelBasic (`table_test.go:194` region): basic accessors over
// TableInfo/DBInfo/ColumnInfo/IndexInfo/FKInfo/SequenceInfo and corner cases.
#[test]
fn table_model_basic() {
    let mut column = ColumnInfo {
        id: 1,
        name: CiString::new("c"),
        offset: 0,
        default_value: ColumnDefaultValue::Int(0).into(),
        field_type: FieldType::new(FieldTypeCode::Unspecified),
        hidden: true,
        ..Default::default()
    };
    column.add_flag(u64::from(FieldTypeFlags::PRI_KEY));

    let index = IndexInfo {
        name: CiString::new("key"),
        table: CiString::new("t"),
        columns: vec![IndexColumn {
            name: CiString::new("c"),
            offset: 0,
            length: 10,
            ..Default::default()
        }]
        .into(),
        unique: true,
        primary: true,
        ..Default::default()
    };

    let fk = FKInfo {
        ref_cols: vec![CiString::new("a")].into(),
        cols: vec![CiString::new("a")].into(),
        ..Default::default()
    };

    let seq = SequenceInfo {
        increment: 1,
        min_value: 1,
        max_value: 100,
        ..Default::default()
    };

    let table = TableInfo {
        id: 1,
        name: CiString::new("t"),
        charset: "utf8".to_owned(),
        collate: "utf8_bin".to_owned(),
        columns: vec![column].into(),
        indices: vec![index].into(),
        foreign_keys: vec![fk].into(),
        pk_is_handle: true,
        ..Default::default()
    };

    let table2 = TableInfo {
        id: 2,
        name: CiString::new("s"),
        sequence: Some(GoShared::new(seq)),
        ..Default::default()
    };

    let db_info = DBInfo {
        id: 1,
        name: CiString::new("test"),
        charset: "utf8".to_owned(),
        collate: "utf8_bin".to_owned(),
        deprecated_tables: GoSharedPointerSlice::from_handles(vec![Some(GoShared::new(
            table.clone_like_go(),
        ))]),
        ..Default::default()
    };

    let n = db_info.clone_like_go();
    assert_eq!(
        serde_json::to_string(&db_info).unwrap(),
        serde_json::to_string(&n).unwrap()
    );

    let pk_name = table.get_pk_name();
    assert_eq!(pk_name, CiString::new("c"));
    let new_column = table.get_pk_col_info();
    let new_column = new_column.expect("PKIsHandle tables expose their PK column");
    assert!(new_column.read().hidden);
    assert!(new_column.ptr_eq(&table.columns.get(0).unwrap()));
    let in_idx = table.column_is_in_index(Some(&table.columns.get(0).unwrap().read()));
    assert!(in_idx);
    assert_eq!(IndexType::BTREE.sql(), "BTREE");
    assert_eq!(IndexType::HASH.sql(), "HASH");
    assert_eq!(IndexType(100_000).sql(), "");
    let has = table.indices.get(0).unwrap().read().has_prefix_index();
    assert!(has);
    assert_eq!(
        table.get_update_time(),
        crate::go_runtime::GoTime::from_tso(table.update_ts)
    );
    assert!(table2.is_sequence());
    assert!(!table2.is_base_table());

    // Corner cases.
    let mut pk_col = table.columns.get(0).unwrap().read().clone_like_go();
    pk_col.toggle_flag(u64::from(FieldTypeFlags::PRI_KEY));
    table.columns.get(0).unwrap().write().del_flag(u64::from(
        FieldTypeFlags::PRI_KEY,
    ));
    let pk_name = table.get_pk_name();
    assert_eq!(pk_name, CiString::new(""));
    assert!(table.get_pk_col_info().is_none());
    let an_col = ColumnInfo {
        name: CiString::new("d"),
        ..Default::default()
    };
    let ex_idx = table.column_is_in_index(Some(&an_col));
    assert!(!ex_idx);
    let an_index = IndexInfo {
        columns: Vec::<IndexColumn>::new().into(),
        ..Default::default()
    };
    assert!(!an_index.has_prefix_index());

    let extra_pk = ColumnInfo::new_extra_handle_col_info();
    assert_eq!(
        extra_pk.get_flag(),
        u64::from(FieldTypeFlags::NOT_NULL | FieldTypeFlags::PRI_KEY)
    );
    assert_eq!(extra_pk.get_charset(), "binary");
    assert_eq!(extra_pk.get_collate(), "binary");
}

// Go TestTTLInfoClone (`table_test.go`): cloning TTLInfo deep-copies the
// mutable string fields.
#[test]
fn table_ttl_info_clone() {
    let ttl_info = TTLInfo {
        column_name: CiString::new("test"),
        interval_expr_str: "test_expr".to_owned(),
        interval_time_unit: 5,
        enable: true,
        ..Default::default()
    };

    let mut cloned_ttl_info = ttl_info.clone();
    cloned_ttl_info.column_name = CiString::new("test_2");
    cloned_ttl_info.interval_expr_str = "test_expr_2".to_owned();
    cloned_ttl_info.interval_time_unit = 9;
    cloned_ttl_info.enable = false;

    assert_eq!(ttl_info.column_name.original(), "test");
    assert_eq!(ttl_info.interval_expr_str, "test_expr");
    assert_eq!(ttl_info.interval_time_unit, 5);
    assert!(ttl_info.enable);
}

// Go TestTTLJobInterval: empty persisted intervals fall back to one hour and
// explicit values parse verbatim.
#[test]
fn table_ttl_job_interval() {
    let ttl_info = TTLInfo::default();
    let interval = ttl_info.get_job_interval().unwrap();
    assert_eq!(interval, HOUR_NANOS);

    let ttl_info = TTLInfo {
        job_interval: "200h".to_owned(),
        ..Default::default()
    };
    let interval = ttl_info.get_job_interval().unwrap();
    assert_eq!(interval, HOUR_NANOS * 200);
}

// Go TestClearReorgIntermediateInfo: the reorg scratch fields reset to their
// zero values (DDLColumns back to nil).
#[test]
fn clear_reorg_intermediate_info() {
    let mut pt_info = PartitionInfo::default();
    pt_info.ddl_type = PartitionType::HASH;
    pt_info.ddl_expr = "Test DDL Expr".to_owned();
    pt_info.new_table_id = 1111;

    pt_info.clear_reorg_intermediate_info();
    assert_eq!(pt_info.ddl_type, PartitionType::NONE);
    assert_eq!(pt_info.ddl_expr, "");
    assert!(!pt_info.ddl_columns.is_allocated());
    assert_eq!(pt_info.new_table_id, 0);
}

// Go TestTTLDefaultJobInterval: the package-level default job intervals parse
// to 24h and 1h respectively.
#[test]
fn ttl_default_job_interval() {
    let d = tidb_parser::parse_config_duration(DEFAULT_TTL_JOB_INTERVAL).unwrap();
    assert_eq!(d, HOUR_NANOS * 24);
    let d = tidb_parser::parse_config_duration(crate::table::OLD_DEFAULT_TTL_JOB_INTERVAL).unwrap();
    assert_eq!(d, HOUR_NANOS);
}
