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

use tidb_kv::staged_writes::{StagedKey, StagedWrites};
use super::*;
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_tablecodec::table_key::{get_table_handle_key_range, RecordHandle};
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::schema::Schema;

fn long() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

#[test]
fn auto_id_owner_separates_hidden_handles_from_single_point_values() {
    let mut table = test_table();
    table.set_auto_increment_offset(0);
    table.init_auto_id_cache(1);
    for expected in 1..=3 {
        assert_eq!(table.auto_id().alloc(1, 1).unwrap(), expected);
        let handle = table
            .handle_of_row(
                &[Datum::Int(expected as i64), Datum::Null],
                &SessionTimeZone::utc(),
                0,
            )
            .unwrap();
        assert_eq!(handle, TableHandle::Int(expected as i64));
    }
}

#[test]
fn auto_id_owner_explicit_hidden_handle_does_not_rebase_auto_increment() {
    let mut table = test_table();
    table.set_auto_increment_offset(0);
    table.init_auto_id_cache(1);
    table
        .insert_row_with_row_id(
            &[Datum::Int(1), Datum::Null],
            Some(700),
            0,
            &crate::StmtContext::for_dml(false, false, false),
        )
        .unwrap();
    assert_eq!(table.auto_id().alloc(1, 1).unwrap(), 1);
}

#[test]
fn auto_id_owner_show_lists_both_separate_counters() {
    let mut table = test_table();
    table.set_auto_increment_offset(0);
    table.init_auto_id_cache(1);
    let rows = table.next_global_row_ids().unwrap();
    assert_eq!(
        rows.iter()
            .map(|(name, _, kind)| (name.as_str(), *kind))
            .collect::<Vec<_>>(),
        vec![("_tidb_rowid", "_TIDB_ROWID"), ("a", "AUTO_INCREMENT")]
    );
}

fn varstr() -> FieldType {
    FieldType::new(FieldTypeCode::VarString)
}

fn test_table() -> KvTable {
    KvTable::new(
        42,
        vec![
            KvColumn {
                name: "a".to_owned(),
                id: 1,
                field_type: long(),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                default_value: None,
                // A column present at CREATE TABLE has no pre-existing rows.
                origin_default: None,
                comment: String::new(),
                generated: None,
            },
            KvColumn {
                name: "s".to_owned(),
                id: 2,
                field_type: varstr(),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                default_value: None,
                // A column present at CREATE TABLE has no pre-existing rows.
                origin_default: None,
                comment: String::new(),
                generated: None,
            },
        ],
    )
}

#[test]
fn writable_check_constraints_guard_insert_and_update() {
    let mut table = test_table();
    let expression = tidb_model::generated_expr::parse_expression("a > 0")
        .expect("persisted CHECK expression parses");
    table
        .add_check_constraint(
            "positive_a",
            &expression,
            &tidb_datatype::SessionTimeZone::utc(),
        )
        .expect("persisted CHECK expression builds");

    let handle = table
        .insert_row(&[Datum::Int(1), Datum::Null], &tidb_exprctx::NoColumns)
        .expect("TRUE passes CHECK");
    table
        .insert_row(&[Datum::Null, Datum::Null], &tidb_exprctx::NoColumns)
        .expect("UNKNOWN passes CHECK");
    assert!(matches!(
        table.insert_row(&[Datum::Int(-1), Datum::Null], &tidb_exprctx::NoColumns),
        Err(KvTableError::Mysql(error))
            if error.equal(&tidb_table::table_errors::ERR_CHECK_CONSTRAINT_VIOLATED)
                && error.message == "Check constraint 'positive_a' is violated."
    ));
    assert!(matches!(
        table.update_row_with_context(
            &handle,
            &[Datum::Int(-1), Datum::Null],
            &crate::StmtContext::for_dml(false, false, false),
        ),
        Err(KvTableError::Mysql(error))
            if error.equal(&tidb_table::table_errors::ERR_CHECK_CONSTRAINT_VIOLATED)
                && error.message == "Check constraint 'positive_a' is violated."
    ));
    assert_eq!(
        table
            .scan_rows_with_context(&RowDecodeContext::for_test_query_utc())
            .expect("scan preserved rows")[0][0],
        Datum::Int(1),
        "failed UPDATE must leave the original row"
    );

    let wire = crate::DriverError::from(
        tidb_table::table_errors::ERR_CHECK_CONSTRAINT_VIOLATED
            .fast_generate_by_args(&["positive_a".into()]),
    )
    .to_mysql_error();
    assert_eq!(
        wire.code,
        tidb_errno::errcode::ErrCheckConstraintViolated
    );
    assert_eq!(wire.state, *b"HY000");
    assert_eq!(wire.message, "Check constraint 'positive_a' is violated.");
}

#[test]
fn test_options() {
    // Direct port of pkg/table/table_test.go::TestOptions. Rust builders
    // replace Go's variadic functional options, but the zero values and
    // every conversion retain the same observable fields.
    let add = AddRecordOptions::new();
    assert!(add.context().is_none());
    assert!(!add.is_update());
    assert!(!add.generate_record_id());
    assert_eq!(add.reserve_auto_id(), 0);
    let create = add.create_index_options();
    assert!(create.context().is_none());
    assert!(!create.ignore_assertion());
    assert!(!create.from_backfill());

    let context: std::rc::Rc<dyn tidb_tblctx::MutateContext> =
        std::rc::Rc::new(crate::StmtContext::default());
    let add = AddRecordOptions::new()
        .with_context(context.clone())
        .for_update()
        .with_reserve_auto_id_hint(12);
    assert!(add.context().is_some());
    assert!(add.is_update());
    assert!(add.generate_record_id());
    assert_eq!(add.reserve_auto_id(), 12);
    assert!(std::rc::Rc::ptr_eq(
        add.create_index_options().context().unwrap(),
        &context
    ));

    let update = UpdateRecordOptions::new();
    assert!(update.context().is_none());
    assert!(!update.skip_write_untouched_indices());
    let generated = update.add_record_options();
    assert!(generated.is_update());
    assert!(generated.generate_record_id());
    let kept = update.add_record_options_keep_record_id();
    assert!(kept.is_update());
    assert!(!kept.generate_record_id());
    assert!(update.create_index_options().context().is_none());

    let update = UpdateRecordOptions::new().with_context(context.clone());
    assert!(std::rc::Rc::ptr_eq(update.context().unwrap(), &context));
    assert!(std::rc::Rc::ptr_eq(
        update.add_record_options().context().unwrap(),
        &context
    ));
    assert!(std::rc::Rc::ptr_eq(
        update
            .add_record_options_keep_record_id()
            .context()
            .unwrap(),
        &context
    ));
    assert!(std::rc::Rc::ptr_eq(
        update.create_index_options().context().unwrap(),
        &context
    ));

    let create = CreateIndexOptions::new();
    assert!(create.context().is_none());
    assert!(!create.ignore_assertion());
    assert!(!create.from_backfill());
    let create = create
        .with_context(context.clone())
        .with_ignore_assertion()
        .with_backfill_source();
    assert!(std::rc::Rc::ptr_eq(create.context().unwrap(), &context));
    assert!(create.ignore_assertion());
    assert!(create.from_backfill());

    // The source context remains live through each of those same handles.
    context.append_warning_parts(1105, "source context marker");
    assert_eq!(create.context().unwrap().warning_count(), 1);
    assert_eq!(
        add.create_index_options()
            .context()
            .unwrap()
            .warning_count(),
        1
    );
    assert_eq!(
        update
            .add_record_options()
            .context()
            .unwrap()
            .warning_count(),
        1
    );
}

#[test]
fn test_dedup_index_columns_4_test() {
    // Direct port of
    // pkg/table/tables/index_test.go::TestDedupIndexColumns4Test.
    let all_columns = (0..100)
        .map(|offset| {
            tidb_model::GoShared::new(tidb_model::IndexColumn {
                name: tidb_ast::CiString::new(format!("c{offset}")),
                offset,
                ..Default::default()
            })
        })
        .collect::<Vec<_>>();
    let mut columns = all_columns.clone();
    for index in 0..100 {
        columns.push(all_columns[(index * 37) % all_columns.len()].clone());
    }

    let result = dedup_index_columns(columns);
    assert_eq!(result.len(), all_columns.len());
    for (actual, expected) in result.iter().zip(&all_columns) {
        assert!(actual.ptr_eq(expected));
    }
}

#[test]
fn test_extract_columns_from_condition() {
    // Direct port of
    // pkg/table/tables/index_test.go::TestExtractColumnsFromCondition.
    let column = |name: &str, offset, generated: &str, stored| tidb_model::ColumnInfo {
        name: tidb_ast::CiString::new(name),
        offset,
        state: tidb_model::SchemaState::PUBLIC,
        generated_expr_string: generated.to_owned(),
        generated_stored: stored,
        ..Default::default()
    };
    let table = tidb_model::TableInfo {
        name: tidb_ast::CiString::new("test_table"),
        columns: vec![
            column("c1", 0, "", false),
            column("c2", 1, "", false),
            column("c3", 2, "c1 + c2", false),
            column("c4", 3, "c1 + c2", true),
        ]
        .into(),
        ..Default::default()
    };
    let cases = [
        (
            "c1 AND c2",
            vec![("c1", 0), ("c2", 1)],
            vec![("c1", 0), ("c2", 1)],
        ),
        ("c1 > 100", vec![("c1", 0)], vec![("c1", 0)]),
        (
            "c3 > 50",
            vec![("c3", 2)],
            vec![("c1", 0), ("c2", 1), ("c3", 2)],
        ),
        ("c4 > 50", vec![("c4", 3)], vec![("c4", 3)]),
    ];

    for (condition, expected, expected_with_virtual) in cases {
        let index = tidb_model::IndexInfo {
            condition_expr_string: condition.to_owned(),
            ..Default::default()
        };
        for (include_virtual, expected) in [
            (false, expected.as_slice()),
            (true, expected_with_virtual.as_slice()),
        ] {
            let actual =
                extract_columns_from_index_condition(&index, &table, include_virtual).unwrap();
            let actual = actual
                .iter()
                .map(|column| {
                    let column = column.read();
                    (column.name.original().to_owned(), column.offset)
                })
                .collect::<Vec<_>>();
            let expected = expected
                .iter()
                .map(|(name, offset)| ((*name).to_owned(), *offset))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "condition {condition}");
        }
    }
}

/// Go delete-range cleanup uses key ownership, even if the base row is
/// absent or its generated value can no longer be evaluated.
/// The planner reads a table's Go `TableInfo` (`KvTable::meta`), so every
/// column and index mutator keeps it as Go's DDL leaves it: an added column
/// is appended behind a hidden one unless placed (`LocateOffsetToMove`), a
/// dropped column leaves and renumbers the offsets (`MoveColumnInfo`), an
/// index names its columns, a dropped index leaves, and a clustered handle
/// is the PRIMARY KEY over `PriKeyFlag` columns.
#[test]
fn column_and_index_mutators_keep_the_table_info_in_step() {
    let column = |name: &str, id: i64| KvColumn {
        name: name.to_owned(),
        id,
        field_type: long(),
        column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
        default_value: None,
        origin_default: None,
        comment: String::new(),
        generated: None,
    };
    let layout = |table: &KvTable| {
        table
            .meta()
            .read()
            .columns
            .iter_deref()
            .map(|info| {
                let info = info.read();
                (info.name.original().to_owned(), info.offset, info.hidden)
            })
            .collect::<Vec<_>>()
    };
    let mut table = test_table();
    table.add_hidden_column(column("_V$_i_0", 3));
    table.add_column(3, column("z", 4));
    table.add_column(1, column("y", 5));
    assert_eq!(
        layout(&table),
        [
            ("a".to_owned(), 0, false),
            ("y".to_owned(), 1, false),
            ("s".to_owned(), 2, false),
            ("_V$_i_0".to_owned(), 3, true),
            ("z".to_owned(), 4, false),
        ]
    );
    assert_eq!(table.visible_offsets(), [0, 1, 2, 4]);
    assert!(table.is_hidden(3));

    table.add_index(
        KvIndex {
            id: 9,
            name: "iz".to_owned(),
            comment: String::new(),
            unique: false,
            column_offsets: vec![4],
            prefix_lengths: vec![],
            visible: false,
            global: false,
            global_index_version: 0,
            clustered_primary: false,
            primary: false,
        },
        false,
    );
    table.drop_column(0);
    assert_eq!(
        layout(&table),
        [
            ("y".to_owned(), 0, false),
            ("s".to_owned(), 1, false),
            ("_V$_i_0".to_owned(), 2, true),
            ("z".to_owned(), 3, false),
        ]
    );
    {
        let meta = table.meta();
        let meta = meta.read();
        let index = meta.find_index_by_id(9).expect("the index is recorded");
        let index = index.read();
        assert!(index.invisible);
        let part = index.columns.get(0).unwrap();
        assert_eq!((part.read().name.original(), part.read().offset), ("z", 3));
    }
    assert!(table.drop_index("iz").unwrap());
    assert!(table.meta().read().find_index_by_id(9).is_none());

    table.set_common_handle_offsets(vec![1]);
    let meta = table.meta();
    let meta = meta.read();
    assert!(meta.is_common_handle);
    let primary = meta.get_primary_key().expect("the clustered PRIMARY KEY");
    let part = primary.read().columns.get(0).unwrap();
    assert_eq!(part.read().name.original(), "s");
    let s = meta.columns.get(1).unwrap();
    assert!(s.read().field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY));
}

#[test]
fn drop_index_removes_orphan_entries_without_decoding_rows() {
    let mut table = KvTable::new(42, Vec::new());
    table.add_index(
        KvIndex {
            id: 1,
            name: "retired".to_owned(),
            comment: String::new(),
            unique: false,
            column_offsets: vec![],
            prefix_lengths: vec![],
            visible: true,
            global: false,
            global_index_version: 0,
            clustered_primary: false,
            primary: false,
        },
        false,
    );
    let retired = Key::from_bytes(tidb_tablecodec::table_key::encode_index_seek_key(
        42, 1, b"orphan",
    ));
    let retained = Key::from_bytes(tidb_tablecodec::table_key::encode_index_seek_key(
        42,
        2,
        b"neighbor",
    ));
    table.store_mut().set(retired.clone(), vec![1]).unwrap();
    table.store_mut().set(retained.clone(), vec![2]).unwrap();
    assert!(table.drop_index("retired").unwrap());
    assert!(matches!(
        table.store_mut().get(&retired),
        Err(StorageError::NotFound)
    ));
    assert_eq!(table.store_mut().get(&retained).unwrap(), vec![2]);
}

#[test]
fn drop_index_covers_all_partition_ranges_and_global_ownership() {
    use tidb_table::partition_routing::{PartitionDef, PartitionKind, PartitionSpec};
    for global in [false, true] {
        let mut table = KvTable::new(42, Vec::new());
        *table.partition_mut() = Some(Box::new(PartitionSpec {
            kind: PartitionKind::Hash,
            expr_text: "0".to_owned(),
            expr: Expression::Constant(tidb_expr::constant::Constant::default()),
            dependencies: vec![],
            definitions: vec![
                PartitionDef {
                    id: 43,
                    ..Default::default()
                },
                PartitionDef {
                    id: 44,
                    ..Default::default()
                },
            ],
            overlapping_dropping_partition_indices: vec![],
            is_empty_columns: false,
        }));
        table.restrict_read_to_partitions(&[43]);
        table.add_index(
            KvIndex {
                id: 1,
                name: "retired".to_owned(),
                comment: String::new(),
                unique: false,
                column_offsets: vec![],
                prefix_lengths: vec![],
                visible: true,
                global,
                global_index_version: 0,
                clustered_primary: false,
                primary: false,
            },
            false,
        );
        let keys: Vec<_> = [42, 43, 44, 45]
            .into_iter()
            .map(|id| {
                Key::from_bytes(tidb_tablecodec::table_key::encode_index_seek_key(
                    id, 1, b"orphan",
                ))
            })
            .collect();
        for key in &keys {
            table.store_mut().set(key.clone(), vec![1]).unwrap();
        }
        assert!(table.drop_index("retired").unwrap());
        for (id, key) in [42, 43, 44, 45].into_iter().zip(&keys) {
            let removed = if global {
                id == 42
            } else {
                id == 43 || id == 44
            };
            assert_eq!(
                matches!(table.store_mut().get(key), Err(StorageError::NotFound)),
                removed,
                "global={global}, table={id}"
            );
        }
        assert!(!table.drop_index("retired").unwrap());
    }
}

#[test]
fn update_rewrites_only_index_entries_whose_bytes_change() {
    // Go `pkg/table/tables/tables.go ::
    // rebuildUpdateRecordIndices` skips an index whose columns are all
    // untouched, and `pkg/executor/write.go :: updateRecord` arms that
    // skip (`SkipWriteUntouchedIndices`) for autocommit updates. An
    // update that leaves every indexed value alone must therefore leave
    // the entry byte-for-byte as it was (and stage no index write at
    // all), while an update that changes an indexed column must move the
    // entry to its new key.
    let mut table = KvTable::new(
        42,
        vec![
            KvColumn {
                name: "a".to_owned(),
                id: 1,
                field_type: long(),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                default_value: None,
                origin_default: None,
                comment: String::new(),
                generated: None,
            },
            KvColumn {
                name: "s".to_owned(),
                id: 2,
                field_type: varstr(),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                default_value: None,
                origin_default: None,
                comment: String::new(),
                generated: None,
            },
            KvColumn {
                name: "x".to_owned(),
                id: 3,
                field_type: long(),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                default_value: None,
                origin_default: None,
                comment: String::new(),
                generated: None,
            },
        ],
    );
    table.set_pk_handle_offset(0);
    table.add_index(
        KvIndex {
            id: 1,
            name: "s".to_owned(),
            comment: String::new(),
            unique: false,
            column_offsets: vec![1],
            prefix_lengths: vec![tidb_datatype::UNSPECIFIED_LENGTH],
            visible: true,
            global: false,
            global_index_version: 0,
            clustered_primary: false,
            primary: false,
        },
        false,
    );
    table
        .insert_row(
            &[Datum::Int(1), Datum::Bytes(b"s0".to_vec()), Datum::Int(10)],
            &tidb_exprctx::NoColumns,
        )
        .unwrap();
    let handle = TableHandle::Int(1);
    let before = table.index_entries_for_check(1).unwrap();
    assert_eq!(before.len(), 1);

    // Update a NON-indexed column only: the index entry must survive
    // byte-identical.
    table
        .update_row_with_old_context(
            &handle,
            Some(&[Datum::Int(1), Datum::Bytes(b"s0".to_vec()), Datum::Int(10)]),
            &[Datum::Int(1), Datum::Bytes(b"s0".to_vec()), Datum::Int(11)],
            &crate::StmtContext::for_dml(false, false, false),
        )
        .unwrap();
    let after_untouched = table.index_entries_for_check(1).unwrap();
    assert_eq!(after_untouched, before, "untouched index stayed as it was");
    let rows = table
        .scan_rows_with_handles_with_context(&RowDecodeContext::for_test_query_utc())
        .unwrap();
    assert_eq!(rows[0].1[2], Datum::Int(11), "the row itself did update");

    // Update the INDEXED column: the entry moves to the new key.
    table
        .update_row_with_old_context(
            &handle,
            Some(&[Datum::Int(1), Datum::Bytes(b"s0".to_vec()), Datum::Int(11)]),
            &[Datum::Int(1), Datum::Bytes(b"s9".to_vec()), Datum::Int(11)],
            &crate::StmtContext::for_dml(false, false, false),
        )
        .unwrap();
    let after_touched = table.index_entries_for_check(1).unwrap();
    assert_eq!(
        after_touched.len(),
        1,
        "exactly one entry, moved not copied"
    );
    assert_ne!(after_touched, before, "entry follows the new indexed value");
}

#[test]
fn row_conflicts_only_include_matching_partial_indexes() {
    let mut table = test_table();
    table.set_name("partial_conflicts");
    table.set_pk_handle_offset(0);
    table.add_index(
        KvIndex {
            id: 1,
            name: "unique_s".to_owned(),
            comment: String::new(),
            unique: true,
            column_offsets: vec![1],
            prefix_lengths: vec![-1],
            visible: true,
            global: false,
            global_index_version: 0,
            clustered_primary: false,
            primary: false,
        },
        false,
    );
    let condition = tidb_model::generated_expr::parse_expression("a > 0").unwrap();
    table
        .add_partial_index_condition(1, "unique_s", &condition, &SessionTimeZone::utc(), b'\\')
        .unwrap();
    table
        .insert_row(
            &[Datum::Int(1), Datum::Bytes(b"same".to_vec())],
            &tidb_exprctx::NoColumns,
        )
        .unwrap();
    assert!(table
        .row_conflicts(
            &[Datum::Int(-1), Datum::Bytes(b"same".to_vec())],
            &tidb_exprctx::NoColumns
        )
        .unwrap()
        .is_empty());
    let conflicts = table
        .row_conflicts(
            &[Datum::Int(2), Datum::Bytes(b"same".to_vec())],
            &tidb_exprctx::NoColumns,
        )
        .unwrap();
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].handle, TableHandle::Int(1));
    assert!(matches!(&conflicts[0].error, KvTableError::Mysql(error)
        if error.equal(&tidb_kv::kv_errors::ERR_KEY_EXISTS)
            && error.message == "Duplicate entry 'same' for key 'partial_conflicts.unique_s'"));
}

/// Go `GenTempIdxKeyByState` with `index.create`/`index.Delete`: a
/// non-public index is written by its backfill state -- the index key
/// itself when inapplicable, only the temporary index while a merge
/// backfill runs, both once it merges -- and a delete-only index is never
/// written by an insert. A delete appends a delete element to the
/// temporary index.
#[test]
fn a_nonpublic_index_is_written_by_its_backfill_state() {
    use tidb_model::{BackfillState, SchemaState};
    for (state, backfill, index_written, temp_version) in [
        (
            SchemaState::WRITE_ONLY,
            BackfillState::INAPPLICABLE,
            true,
            None,
        ),
        (
            SchemaState::WRITE_REORGANIZATION,
            BackfillState::RUNNING,
            false,
            Some(b'b'),
        ),
        (
            SchemaState::DELETE_ONLY,
            BackfillState::RUNNING,
            false,
            None,
        ),
        (
            SchemaState::WRITE_REORGANIZATION,
            BackfillState::MERGING,
            true,
            Some(b'm'),
        ),
    ] {
        let mut table = test_table();
        let index = KvIndex {
            id: 1,
            name: "ua".to_owned(),
            comment: String::new(),
            unique: true,
            column_offsets: vec![0],
            prefix_lengths: vec![tidb_datatype::UNSPECIFIED_LENGTH],
            visible: true,
            global: false,
            global_index_version: 0,
            clustered_primary: false,
            primary: false,
        };
        table.add_nonpublic_index(index.clone(), state, backfill);
        let row = [Datum::Int(7), Datum::Null];
        table.insert_row(&row, &tidb_exprctx::NoColumns).unwrap();
        // No reader sees an index a DDL has not made public.
        assert!(table.index_entries_for_check(1).is_err());
        let handle = table
            .scan_rows_with_handles_with_context(&RowDecodeContext::for_test_query_utc())
            .unwrap()[0]
            .0
            .clone();
        let zone = tidb_datatype::SessionTimeZone::utc();
        let key = table
            .index_entry_keys(&index, &row, &handle, table.table_id(), &zone)
            .unwrap()
            .remove(0)
            .key;
        let mut temp_key = key.clone();
        tidb_tablecodec::index_key_to_temp_index_key(&mut temp_key).unwrap();
        assert_eq!(
            table.stored_raw_value(&key).is_some(),
            index_written,
            "{state:?} {backfill:?}"
        );
        let history = table
            .stored_raw_value(&temp_key)
            .map(|raw| tidb_tablecodec::decode_temp_index_value(&raw).unwrap());
        assert_eq!(
            history
                .as_ref()
                .map(|history| (history.len(), history[0].key_version)),
            temp_version.map(|version| (1, version)),
            "{state:?} {backfill:?}"
        );
        table
            .delete_row_with_context(&handle, &crate::StmtContext::for_dml(false, false, false))
            .unwrap();
        assert!(
            table.stored_raw_value(&key).is_none(),
            "{state:?} {backfill:?}"
        );
        if backfill != BackfillState::INAPPLICABLE {
            let history = tidb_tablecodec::decode_temp_index_value(
                &table.stored_raw_value(&temp_key).unwrap(),
            )
            .unwrap();
            assert!(history.last().unwrap().delete, "{state:?} {backfill:?}");
        }
    }
}

#[test]
fn test_unique_index_multiple_null_entries() {
    // Direct port of
    // pkg/table/tables/tables_test.go::TestUniqueIndexMultipleNullEntries.
    // NULL-bearing UNIQUE keys are non-distinct and carry the row handle,
    // so two NULLs must create two entries without a duplicate error.
    let mut table = test_table();
    table.set_pk_handle_offset(0);
    table.add_index(
        KvIndex {
            id: 1,
            name: "s".to_owned(),
            comment: String::new(),
            unique: true,
            column_offsets: vec![1],
            prefix_lengths: vec![tidb_datatype::UNSPECIFIED_LENGTH],
            visible: true,
            global: false,
            global_index_version: 0,
            clustered_primary: false,
            primary: false,
        },
        false,
    );

    table
        .insert_row(&[Datum::Int(1), Datum::Null], &tidb_exprctx::NoColumns)
        .unwrap();
    table
        .insert_row(&[Datum::Int(2), Datum::Null], &tidb_exprctx::NoColumns)
        .unwrap();
    assert_eq!(
        table
            .scan_rows_with_context(&RowDecodeContext::for_test_query_utc())
            .unwrap()
            .len(),
        2
    );
    assert_eq!(table.index_entries_for_check(1).unwrap().len(), 2);
}

#[test]
fn test_table_from_meta_with_collate_uses_fixed_mode() {
    // Direct port of pkg/table/tables/tables_test.go::
    // TestTableFromMetaWithCollateUsesFixedMode. The table captures the
    // supplied persisted mode instead of consulting process-global state.
    for use_new_collation in [false, true] {
        let table = KvTable::with_storage_and_collation(
            1,
            vec![KvColumn {
                name: "a".to_owned(),
                id: 1,
                field_type: varstr(),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                default_value: None,
                origin_default: None,
                comment: String::new(),
                generated: None,
            }],
            Box::new(MemTableStorage::new()),
            use_new_collation,
        );
        assert_eq!(table.use_new_collation(), use_new_collation);
    }
}

#[test]
fn test_iter_records() {
    // Direct port of pkg/table/tables/tables_test.go::TestIterRecords.
    // Record iteration crosses the signed-handle ordering boundary and
    // preserves NULL in a non-handle column.
    let mut table = KvTable::new(
        42,
        ["a", "b"]
            .into_iter()
            .enumerate()
            .map(|(offset, name)| KvColumn {
                name: name.to_owned(),
                id: offset as i64 + 1,
                field_type: long(),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                default_value: None,
                origin_default: None,
                comment: String::new(),
                generated: None,
            })
            .collect(),
    );
    table.set_pk_handle_offset(0);
    table
        .insert_row(&[Datum::Int(-1), Datum::Int(2)], &tidb_exprctx::NoColumns)
        .unwrap();
    table
        .insert_row(&[Datum::Int(2), Datum::Null], &tidb_exprctx::NoColumns)
        .unwrap();

    let rows = table
        .scan_rows_with_handles_with_context(&RowDecodeContext::for_test_query_utc())
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        (TableHandle::Int(-1), vec![Datum::Int(-1), Datum::Int(2)])
    );
    assert_eq!(
        rows[1],
        (TableHandle::Int(2), vec![Datum::Int(2), Datum::Null])
    );
}

/// The scan bound must cover the whole table and nothing beyond it: the
/// codec's handle range is inclusive at the top while the iterator's upper
/// bound is exclusive, so the largest handle must still be returned and a
/// neighbouring table's rows must not be.
/// The handle a scan reports must be the handle the codec wrote, so an
/// UPDATE/DELETE addresses the row it read.
#[test]
fn scan_reports_the_handles_the_key_codec_wrote() {
    let mut t = test_table();
    let mut handles = Vec::new();
    for i in 0..3 {
        handles.push(
            t.insert_row(
                &[Datum::Int(i * 10), Datum::Bytes(b"x".to_vec())],
                &tidb_exprctx::NoColumns,
            )
            .unwrap(),
        );
    }
    let scanned: Vec<TableHandle> = t
        .scan_rows_with_handles_with_context(&RowDecodeContext::for_test_query_utc())
        .unwrap()
        .into_iter()
        .map(|(handle, _)| handle)
        .collect();
    assert_eq!(scanned, handles);

    // A row written under an explicit handle round-trips too.
    t.update_row_with_context(
        &handles[1],
        &[Datum::Int(99), Datum::Bytes(b"y".to_vec())],
        &crate::StmtContext::for_dml(false, false, false),
    )
    .unwrap();
    let rows = t
        .scan_rows_with_handles_with_context(&RowDecodeContext::for_test_query_utc())
        .unwrap();
    assert_eq!(rows.len(), 3, "update replaced in place, it did not append");
    assert_eq!(rows[1].0, handles[1]);
    assert_eq!(rows[1].1[0], Datum::Int(99));

    t.delete_row_with_context(
        &handles[0],
        &crate::StmtContext::for_dml(false, false, false),
    )
    .unwrap();
    let after: Vec<TableHandle> = t
        .scan_rows_with_handles_with_context(&RowDecodeContext::for_test_query_utc())
        .unwrap()
        .into_iter()
        .map(|(handle, _)| handle)
        .collect();
    assert_eq!(after, vec![handles[1].clone(), handles[2].clone()]);
}

#[test]
fn scan_covers_the_whole_table_and_stops_at_its_range() {
    let mut t = test_table();
    for i in 0..3 {
        t.insert_row(
            &[Datum::Int(i), Datum::Bytes(b"x".to_vec())],
            &tidb_exprctx::NoColumns,
        )
        .unwrap();
    }
    // A row of the next table id, written into the same storage layout.
    let mut neighbour = KvTable::new(t.table_id() + 1, t.columns().to_vec());
    neighbour
        .insert_row(
            &[Datum::Int(99), Datum::Bytes(b"y".to_vec())],
            &tidb_exprctx::NoColumns,
        )
        .unwrap();

    let rows = t
        .scan_rows_with_context(&RowDecodeContext::for_test_query_utc())
        .unwrap();
    assert_eq!(
        rows.len(),
        3,
        "every handle including the largest is scanned"
    );
    assert_eq!(rows[2][0], Datum::Int(2));
    assert_eq!(
        neighbour
            .scan_rows_with_context(&RowDecodeContext::for_test_query_utc())
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn insert_encodes_real_bytes_and_scan_decodes() {
    let mut t = test_table();
    let mut s1 = Datum::Null;
    s1.set_bytes(b"hello".to_vec());
    t.insert_row(&[Datum::Int(7), s1.clone()], &tidb_exprctx::NoColumns)
        .unwrap();
    t.insert_row(&[Datum::Int(8), Datum::Null], &tidb_exprctx::NoColumns)
        .unwrap();
    assert_eq!(t.len(), 2);

    let rows = t
        .scan_rows_with_context(&RowDecodeContext::for_test_query_utc())
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Datum::Int(7));
    assert_eq!(rows[1][0], Datum::Int(8));
    assert_eq!(rows[1][1], Datum::Null);
    // The string round-trips through the v2 row format.
    match &rows[0][1] {
        Datum::Bytes(b) => assert_eq!(b.as_slice(), b"hello"),
        Datum::String(s) => assert_eq!(s.bytes(), b"hello"),
        other => panic!("unexpected decoded string datum {other:?}"),
    }
}

#[test]
fn table_scan_exec_emits_chunks() {
    let mut t = test_table();
    let mut s = Datum::Null;
    s.set_bytes(b"x".to_vec());
    t.insert_row(&[Datum::Int(1), s], &tidb_exprctx::NoColumns)
        .unwrap();

    let mut out_cols = Vec::new();
    for (i, ft) in [long(), varstr()].into_iter().enumerate() {
        let mut c = Column::new((i + 1) as i64, ft);
        c.index = i as i64;
        out_cols.push(c);
    }
    let mut scan = TableScanExec::new_with_context(
        ExecutorMeta::new(Schema::new(out_cols), 0, 4, 1024),
        t,
        RowDecodeContext::for_test_query_utc(),
        tidb_distsql::remote_scan::PushdownStatementContext::default(),
    );
    scan.open().unwrap();
    let mut req = scan.new_chunk();
    scan.next(&mut req).unwrap();
    assert_eq!(req.num_rows(), 1);
    assert_eq!(req.get_row(0).get_int64(0), 1);
    assert_eq!(req.get_row(0).get_bytes(1), b"x");
    // EOF afterwards.
    scan.next(&mut req).unwrap();
    assert_eq!(req.num_rows(), 0);
}

#[test]
fn record_keys_are_the_real_format() {
    // t{tid}_r + memcomparable handle: 19 bytes, 't' prefix.
    let key = encode_row_key_with_handle(42, &RecordHandle::Int(1));
    assert_eq!(key[0], b't');
    assert!(key.len() > 10);
    // Keys sort by handle within the table range.
    let k2 = encode_row_key_with_handle(42, &RecordHandle::Int(2));
    assert!(key < k2);
    let (low, high) = get_table_handle_key_range(42);
    assert!(low < key && key < high);
}

/// A clustered common handle whose codec encoding is SHORTER than nine
/// bytes is zero-padded by the record-key codec (Go `kv.NewCommonHandle`,
/// `tablecodec.EncodeRowKeyWithHandle`), so a write that files its index
/// entries under the RAW encoding files them under a key the delete path
/// -- which reads its handle back out of the record key -- can never
/// name. `DECIMAL` is the shortest such encoding this tier can build:
/// `5` is four bytes.
#[test]
fn a_short_common_handle_indexes_under_the_key_the_delete_path_computes() {
    let mut decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
    decimal_type.set_flen(4);
    decimal_type.set_decimal(0);
    let mut t = KvTable::new(
        77,
        vec![
            KvColumn {
                name: "pk".to_owned(),
                id: 1,
                field_type: decimal_type,
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                default_value: None,
                origin_default: None,
                comment: String::new(),
                generated: None,
            },
            KvColumn {
                name: "v".to_owned(),
                id: 2,
                field_type: long(),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                default_value: None,
                origin_default: None,
                comment: String::new(),
                generated: None,
            },
        ],
    );
    t.set_common_handle_offsets(vec![0]);
    t.add_index(
        KvIndex {
            id: 1,
            name: "idx".to_owned(),
            comment: String::new(),
            unique: false,
            column_offsets: vec![1],
            prefix_lengths: vec![tidb_datatype::UNSPECIFIED_LENGTH],
            visible: true,
            global: false,
            global_index_version: 0,
            clustered_primary: false,
            primary: false,
        },
        false,
    );
    let written = t
        .insert_row(
            &[
                Datum::Decimal(tidb_datatype::Decimal::from_int(5)),
                Datum::Int(42),
            ],
            &tidb_exprctx::NoColumns,
        )
        .unwrap();
    // The handle a WRITE derives from the row and the handle a READ
    // derives from the record key must be the same handle, or every
    // index entry the write filed is unreachable.
    let scanned = t
        .scan_rows_with_handles_with_context(&RowDecodeContext::for_test_query_utc())
        .unwrap();
    assert_eq!(scanned.len(), 1);
    assert_eq!(scanned[0].0, written, "the write and the read disagree");

    t.delete_row_with_context(
        &scanned[0].0,
        &crate::StmtContext::for_dml(false, false, false),
    )
    .unwrap();
    assert_eq!(
        t.stored_keys().unwrap(),
        Vec::<Vec<u8>>::new(),
        "the delete left orphaned index entries behind"
    );
}

/// [`StagedWrites`]'s own behavior must not change when its per-key
/// storage moves from a heap `Vec<u8>` to an inline [`StagedKey`]:
/// `note`/`has_dirty_content`/`contains` still answer exactly as they did
/// before, for a key that fits inline and one that spills to the heap.
#[test]
fn staged_writes_answers_the_same_regardless_of_key_width() {
    let staged = StagedWrites::default();
    let short_key = encode_row_key_with_handle(1, &RecordHandle::Int(7));
    let long_key = vec![b'x'; 64];

    assert!(!staged.has_dirty_content(1));
    assert!(!staged.contains(1, &short_key));

    staged.note(1, &short_key);
    staged.note(1, &long_key);

    assert!(staged.has_dirty_content(1));
    assert!(
        !staged.has_dirty_content(2),
        "a different table stays clean"
    );
    assert!(staged.contains(1, &short_key));
    assert!(staged.contains(1, &long_key));
    assert!(
        !staged.contains(1, &[b'y'; 64]),
        "a key that was never staged must not read back as staged"
    );
    assert!(!staged.contains(2, &short_key), "table id is not shared");
}

/// The inline capacity this optimization relies on: a plain
/// integer-handle record key must stay inline (no heap allocation), or
/// the whole point of the change is lost. A wider key (a long
/// common-handle or index entry) is still correct, just heap-backed like
/// the `Vec<u8>` this replaces.
#[test]
fn a_plain_integer_handle_key_stays_inline() {
    let key = encode_row_key_with_handle(1, &RecordHandle::Int(7));
    let staged_key = StagedKey::from_slice(&key);
    assert!(
        !staged_key.spilled(),
        "a {}-byte integer-handle record key must not heap-allocate",
        key.len()
    );
    assert_eq!(staged_key.as_slice(), key.as_slice());

    let wide_key = vec![b'x'; 64];
    assert!(
        StagedKey::from_slice(&wide_key).spilled(),
        "a key wider than the inline capacity must still be stored correctly, on the heap"
    );
}
