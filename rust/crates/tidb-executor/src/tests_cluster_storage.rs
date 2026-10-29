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

//! The cluster storage seam (`tidb_kv::cluster_storage`) under the tables
//! the executor builds over it.

    use crate::KvTableDdl;
use crate::KvTableReader;
use tidb_kv::cluster_storage::*;
use tidb_kv::cluster_storage_test_util::*;
use tidb_kv::storage::{StorageError, StorageIterator, TableStorage};
use std::sync::{Arc, Mutex};
use tidb_txnkv::Key;
    fn mutation_batch_table(store: ClusterTableStorage) -> tidb_table::kv_table::KvTable {
        use tidb_table::kv_table::KvTable;
        use tidb_table::kv_table::KvIndex;
        use tidb_table::kv_table::KvColumn;
        use tidb_datatype::{FieldType, FieldTypeCode};
        let mut table = KvTable::with_storage(
            42,
            ["id", "v"]
                .iter()
                .enumerate()
                .map(|(i, name)| KvColumn {
                    name: name.to_string(),
                    id: i as i64 + 1,
                    field_type: FieldType::new(FieldTypeCode::LongLong),
                    column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                    default_value: None,
                    origin_default: None,
                    comment: String::new(),
                    generated: None,
                })
                .collect(),
            Box::new(store),
        );
        table.set_pk_handle_offset(0);
        table.add_index(
            KvIndex {
                id: 1,
                name: "u".into(),
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
        table
    }

    #[test]
    fn table_mutation_batch_pessimistic_update_defers_destination_check() {
        use tidb_table::kv_table::TableHandle;
        use tidb_datatype::Datum;
        for (pessimistic, ignore, expected_reads) in
            [(true, false, 0), (false, false, 1), (true, true, 1)]
        {
            let (store, snapshot, buffer) = storage(&[]);
            let mut table = mutation_batch_table(store);
            let old = [Datum::Int(1), Datum::Int(10)];
            let new = [Datum::Int(1), Datum::Int(20)];
            let ctx = crate::StmtContext::for_dml(false, true, ignore)
                .with_pessimistic_transaction(pessimistic);
            table
                .insert_row_with_row_id_checked(&old, None, 0, &ctx, false)
                .unwrap();
            for key in buffer.staged_keys() {
                snapshot
                    .lock()
                    .unwrap()
                    .data
                    .insert(key.clone().into_bytes(), buffer.get(&key).unwrap().unwrap());
            }
            buffer.reset();
            snapshot.lock().unwrap().gets.clear();
            table
                .update_row_with_old_context(&TableHandle::Int(1), Some(&old), &new, &ctx)
                .unwrap();
            assert_eq!(
                snapshot.lock().unwrap().gets.len(),
                expected_reads,
                "pessimistic={pessimistic}, ignore={ignore}"
            );
            let new_index = buffer
                .staged_keys()
                .into_iter()
                .find(|key| key.as_bytes()[10] == b'i' && buffer.get(key).unwrap().is_some())
                .unwrap();
            let flags = buffer.get_flags(new_index.as_bytes()).unwrap();
            assert_eq!(flags.has_presume_key_not_exists(), pessimistic && !ignore);
            assert!(flags.has_assert_not_exist());
        }
    }

    #[test]
    fn table_mutation_batch_moved_primary_defers_check_with_duplicate_hint() {
        use tidb_table::kv_table::TableHandle;
        use tidb_datatype::Datum;
        let (store, snapshot, buffer) = storage(&[]);
        let mut table = mutation_batch_table(store);
        let old = [Datum::Int(1), Datum::Null];
        let new = [Datum::Int(2), Datum::Null];
        let ctx = crate::StmtContext::default().with_pessimistic_transaction(true);
        table
            .insert_row_with_row_id_checked(&old, None, 0, &ctx, false)
            .unwrap();
        for key in buffer.staged_keys() {
            snapshot
                .lock()
                .unwrap()
                .data
                .insert(key.clone().into_bytes(), buffer.get(&key).unwrap().unwrap());
        }
        buffer.reset();
        snapshot.lock().unwrap().gets.clear();
        table
            .update_row_with_old_context(&TableHandle::Int(1), Some(&old), &new, &ctx)
            .unwrap();
        assert!(snapshot.lock().unwrap().gets.is_empty());
        let key = Key::from_bytes(tidb_codec::table_key::encode_row_key_with_handle(
            42,
            &tidb_codec::table_key::RecordHandle::Int(2),
        ));
        assert!(buffer.get_flags(key.as_bytes()).unwrap().has_presume_key_not_exists());
        assert_eq!(
            buffer.duplicate_key_hint_for(key.as_bytes()).unwrap().value,
            "2"
        );
    }

    #[test]
    fn table_mutation_batch_partition_move_preserves_repeated_hidden_handle() {
        use tidb_table::kv_table::TableHandle;
        use tidb_table::kv_table::KvTable;
        use tidb_table::kv_table::KvColumn;
        use tidb_table::partition_routing::{PartitionDef, PartitionKind, PartitionSpec};
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::expression::{Column, Expression};
        let (store, _, buffer) = storage(&[]);
        let mut table = KvTable::with_storage(
            42,
            ["v", "payload"]
                .iter()
                .enumerate()
                .map(|(i, name)| KvColumn {
                    name: name.to_string(),
                    id: i as i64 + 1,
                    field_type: FieldType::new(FieldTypeCode::LongLong),
                    column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                    default_value: None,
                    origin_default: None,
                    comment: String::new(),
                    generated: None,
                })
                .collect(),
            Box::new(store),
        );
        let mut column = Column::new(1, FieldType::new(FieldTypeCode::LongLong));
        column.index = 0;
        table.set_partition(PartitionSpec {
            kind: PartitionKind::Hash,
            expr_text: "v".into(),
            expr: Expression::Column(column),
            dependencies: vec!["v".into()],
            definitions: [43, 44]
                .into_iter()
                .map(|id| PartitionDef {
                    id,
                    name: id.to_string(),
                    less_than: vec![],
                    in_values: vec![],
                    comment: String::new(),
                    placement_policy: None,
                    storage_class: Default::default(),
                })
                .collect(),
            overlapping_dropping_partition_indices: vec![],
            is_empty_columns: false,
        });
        let ctx = crate::StmtContext::default();
        let first = [Datum::Int(0), Datum::Int(10)];
        let second = [Datum::Int(1), Datum::Int(20)];
        for row in [&first, &second] {
            table
                .insert_row_with_row_id_checked(row, Some(1), 0, &ctx, false)
                .unwrap();
        }
        let key = |id, handle| {
            Key::from_bytes(tidb_codec::table_key::encode_row_key_with_handle(
                id,
                &tidb_codec::table_key::RecordHandle::Int(handle),
            ))
        };
        let original_first = buffer.get(&key(43, 1));
        let new = [Datum::Int(0), Datum::Int(20)];
        table
            .update_row_with_old_context(&TableHandle::Int(1), Some(&second), &new, &ctx)
            .unwrap();
        assert_eq!(buffer.get(&key(43, 1)), original_first);
        assert_eq!(buffer.get(&key(44, 1)), Some(None));
        assert!(buffer
            .staged_keys()
            .iter()
            .any(|k| tidb_codec::decode_table_id(k.as_bytes()) == 43
                && *k != key(43, 1)
                && buffer.get(k).unwrap().is_some()));
    }

    #[test]
    fn table_writes_preserve_go_assertions_through_the_native_buffer() {
        use tidb_table::kv_table::TableHandle;
        use tidb_table::kv_table::KvTable;
        use tidb_table::kv_table::KvIndex;
        use tidb_table::kv_table::KvColumn;
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};

        fn flags(buffer: &MutationBuffer, key: &Key) -> tikv_client::kv::KeyFlags {
            buffer.get_flags(key.as_bytes()).unwrap()
        }
        fn publish(buffer: &MutationBuffer, snapshot: &Mutex<MockSnapshot>) {
            let mut snapshot = snapshot.lock().unwrap();
            for key in buffer.staged_keys() {
                match buffer.get(&key).unwrap() {
                    Some(value) => {
                        snapshot.data.insert(key.into_bytes(), value);
                    }
                    None => {
                        snapshot.data.remove(key.as_bytes());
                    }
                }
            }
            buffer.reset();
        }

        for (lazy, pessimistic) in [(false, false), (false, true), (true, false), (true, true)] {
            let (store, snapshot, buffer) = storage(&[]);
            let mut table = KvTable::with_storage(
                42,
                vec![KvColumn {
                    name: "a".to_owned(),
                    id: 1,
                    field_type: FieldType::new(FieldTypeCode::LongLong),
                    column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                    default_value: None,
                    origin_default: None,
                    comment: String::new(),
                    generated: None,
                }],
                Box::new(store),
            );
            for (id, unique) in [(1, true), (2, false)] {
                table.add_index(
                    KvIndex {
                        id,
                        name: format!("idx{id}"),
                        comment: String::new(),
                        unique,
                        column_offsets: vec![0],
                        prefix_lengths: vec![-1],
                        visible: true,
                        global: false,
                        global_index_version: 0,
                        clustered_primary: false,
                        primary: false,
                    },
                    false,
                );
            }
            let ctx = crate::StmtContext::default().with_pessimistic_transaction(pessimistic);
            let row = [Datum::Int(7)];
            let handle = TableHandle::Int(1);
            table
                .insert_row_with_row_id_checked(&row, Some(1), 0, &ctx, lazy)
                .unwrap();
            let keys = buffer.staged_keys();
            assert_eq!(keys.len(), 3);
            for key in &keys {
                let flag = flags(&buffer, key);
                if lazy && !pessimistic {
                    assert!(
                        flag.has_assert_unknown(),
                        "lazy optimistic key {key:?}: {flag:?}"
                    );
                } else {
                    assert!(
                        flag.has_assert_not_exist(),
                        "eager/pessimistic key {key:?}: {flag:?}"
                    );
                }
            }

            let checkpoint = buffer.checkpoint();
            table
                .delete_row_with_old_context(&handle, &row, &ctx)
                .unwrap();
            for key in &keys {
                let flag = flags(&buffer, key);
                assert_eq!(buffer.get(key), Some(None));
                assert_eq!(flag.has_assert_unknown(), lazy && !pessimistic);
                assert_eq!(flag.has_assert_not_exist(), !(lazy && !pessimistic));
            }
            buffer.restore(checkpoint);
            buffer.release(checkpoint);
            for key in &keys {
                assert!(buffer.get(key).unwrap().is_some());
            }
            publish(&buffer, &snapshot);

            let updated = [Datum::Int(8)];
            table
                .update_row_with_old_context(&handle, Some(&row), &updated, &ctx)
                .unwrap();
            assert_eq!(buffer.staged_keys().len(), 5);
            for key in buffer.staged_keys() {
                let flag = flags(&buffer, &key);
                if keys.contains(&key) {
                    assert!(
                        flag.has_assert_exist(),
                        "updated/deleted key {key:?}: {flag:?}"
                    );
                } else {
                    assert!(
                        flag.has_assert_not_exist(),
                        "new index key {key:?}: {flag:?}"
                    );
                }
            }
            publish(&buffer, &snapshot);
            table
                .delete_row_with_context(&handle, &ctx)
                .unwrap();
            assert_eq!(buffer.staged_keys().len(), 3);
            for key in buffer.staged_keys() {
                assert!(flags(&buffer, &key).has_assert_exist());
                assert_eq!(buffer.get(&key), Some(None));
            }
            // A reinsert over a local deletion retains the first assertion.
            table
                .insert_row_with_row_id_checked(&updated, Some(1), 0, &ctx, true)
                .unwrap();
            for key in buffer.staged_keys() {
                assert!(flags(&buffer, &key).has_assert_exist());
                assert!(!flags(&buffer, &key).has_presume_key_not_exists());
            }

            // A local tombstone with no prior assertion is different from a
            // local miss. Unique keys use NotExist even for a lazy insert.
            let non_unique = table.index_list_for_check().remove(1);
            let non_unique_key = Key::from_bytes(
                table
                    .index_keys_for_check(
                        &non_unique,
                        &updated,
                        &handle,
                        table.table_id(),
                        &ctx.session_zone(),
                    )
                    .unwrap()
                    .remove(0)
                    .0,
            );
            let keys = buffer.staged_keys();
            buffer.reset();
            for key in keys {
                buffer.delete(key).unwrap();
            }
            table
                .insert_row_with_row_id_checked(&updated, Some(1), 0, &ctx, true)
                .unwrap();
            for key in buffer.staged_keys() {
                let flag = flags(&buffer, &key);
                if key == non_unique_key && !pessimistic {
                    assert!(flag.has_assert_unknown());
                } else {
                    assert!(flag.has_assert_not_exist());
                }
                assert!(!flag.has_presume_key_not_exists());
            }

            // Moving a clustered handle deletes existing keys and inserts
            // new keys; it cannot assert existence for the new record key.
            buffer.reset();
            snapshot.lock().unwrap().data.clear();
            table.set_pk_handle_offset(0);
            table
                .insert_row_with_row_id_checked(&row, None, 0, &ctx, false)
                .unwrap();
            publish(&buffer, &snapshot);
            table
                .update_row_with_old_context(&TableHandle::Int(7), Some(&row), &updated, &ctx)
                .unwrap();
            assert_eq!(buffer.staged_keys().len(), 6);
            for key in buffer.staged_keys() {
                if buffer.get(&key).unwrap().is_none() {
                    assert!(flags(&buffer, &key).has_assert_exist());
                } else {
                    assert!(flags(&buffer, &key).has_assert_not_exist());
                }
            }
        }
    }

    #[test]
    fn insert_dup_check_is_in_place_eagerly_and_local_only_lazily() {
        use tidb_table::kv_table::KvTable;
        use tidb_table::kv_table::KvIndex;
        use tidb_table::kv_table::KvColumn;
        use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};

        let record_key = Key::from_bytes(encode_row_key_with_handle(42, &RecordHandle::Int(1)));
        let mut snapshot = MockSnapshot {
            ..MockSnapshot::default()
        };
        snapshot
            .data
            .insert(record_key.as_bytes().to_vec(), b"committed row".to_vec());
        let snapshot = std::sync::Arc::new(std::sync::Mutex::new(snapshot));
        let buffer = MutationBuffer::new();
        let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&snapshot) as _;
        let mut table = KvTable::with_storage(
            42,
            vec![KvColumn {
                name: "a".to_owned(),
                id: 1,
                field_type: FieldType::new(FieldTypeCode::LongLong),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                default_value: None,
                origin_default: None,
                comment: String::new(),
                generated: None,
            }],
            Box::new(ClusterTableStorage::new(buffer.clone(), handle)),
        );
        let ctx = crate::StmtContext::default();
        let row = [Datum::Int(7)];
        table.add_index(
            KvIndex {
                id: 1,
                name: "unique_a".to_owned(),
                comment: String::new(),
                unique: true,
                column_offsets: vec![0],
                prefix_lengths: vec![-1],
                visible: true,
                global: false,
                global_index_version: 0,
                clustered_primary: false,
                primary: false,
            },
            false,
        );

        // In place: the committed duplicate is reported at statement time,
        // exactly Go's eager `txn.Get` arm finding the key.
        let error = table
            .insert_row_with_row_id_checked(&row, Some(1), 0, &ctx, false)
            .unwrap_err();
        assert!(matches!(
            error,
            tidb_table::kv_table::KvTableError::Mysql(ref error)
                if error.equal(&tidb_error::catalog::kv::ERR_KEY_EXISTS)
        ));
        assert!(buffer.take_presume_not_exists().is_empty());

        // Lazy: the same statement reads nothing from the cluster, succeeds,
        // and stages the row presumed absent for the commit to verify.
        let error_count = snapshot.lock().unwrap().gets.len();
        table
            .insert_row_with_row_id_checked(&row, Some(1), 0, &ctx, true)
            .unwrap();
        assert_eq!(snapshot.lock().unwrap().gets.len(), error_count);
        let marks = buffer.take_presume_not_exists();
        assert!(marks.contains(&record_key));
        assert_eq!(
            marks.len(),
            2,
            "record and unique index share the lazy policy"
        );
        let index_key = marks
            .iter()
            .find(|key| tidb_codec::table_key::is_index_key(key.as_bytes()))
            .unwrap()
            .clone();

        let checkpoint = buffer.checkpoint();
        assert!(
            matches!(
                table.insert_row_with_row_id_checked(&row, Some(2), 0, &ctx, true),
                Err(tidb_table::kv_table::KvTableError::Mysql(error))
                    if error.equal(&tidb_error::catalog::kv::ERR_KEY_EXISTS)
                        && error.message.starts_with("Duplicate entry '7' for key")
            ),
            "a duplicate staged by this transaction is checked locally"
        );
        buffer.restore(checkpoint);
        buffer.delete(index_key.clone()).unwrap();
        table
            .insert_row_with_row_id_checked(&row, Some(2), 0, &ctx, true)
            .unwrap();
        assert!(
            !buffer.take_presume_not_exists().contains(&index_key),
            "replacing a local tombstone does not add an absence assertion"
        );
        assert_eq!(snapshot.lock().unwrap().gets.len(), error_count);

        buffer.reset();
        snapshot.lock().unwrap().fail_with =
            Some(StorageError::Backend("index read failed".to_owned()));
        let error = table
            .insert_row_with_row_id_checked_without_primary_duplicate_check(
                &[Datum::Int(8)],
                Some(3),
                0,
                &ctx,
                false,
            )
            .expect_err("a failed eager index read is not an absent key");
        assert!(
            matches!(error, tidb_table::kv_table::KvTableError::Storage(StorageError::Backend(detail)) if detail.contains("index read failed"))
        );
        assert!(
            buffer.is_empty(),
            "read failure must not stage the index or record"
        );
    }

    #[test]
    fn unique_index_read_failures_are_not_absence() {
        use tidb_table::kv_table::TableHandle;
        use tidb_table::kv_table::KvTable;
        use tidb_table::kv_table::KvIndex;
        use tidb_table::kv_table::KvColumn;
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};

        let mut failures = Vec::new();
        for operation in [
            "insert",
            "update",
            "partial_update",
            "backfill",
            "conflicts",
        ] {
            for error in [
                StorageError::Backend("index unavailable".to_owned()),
                StorageError::Retryable("region changed".to_owned()),
                StorageError::Sql(crate::MysqlError::from_terror(&tidb_error::catalog::kv::ERR_WRITE_CONFLICT.generate_with_stack("write conflict"))),
            ] {
                let (store, snapshot, buffer) = storage(&[]);
                let mut table = KvTable::with_storage(
                    42,
                    ["id", "v"]
                        .into_iter()
                        .enumerate()
                        .map(|(i, name)| KvColumn {
                            name: name.to_owned(),
                            id: i as i64 + 1,
                            field_type: FieldType::new(FieldTypeCode::LongLong),
                            column_info_version:
                                tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                            default_value: None,
                            origin_default: None,
                            comment: String::new(),
                            generated: None,
                        })
                        .collect(),
                    Box::new(store),
                );
                table.set_name("index_errors");
                table.set_pk_handle_offset(0);
                let index = KvIndex {
                    id: 1,
                    name: "unique_v".to_owned(),
                    comment: String::new(),
                    unique: true,
                    column_offsets: vec![1],
                    prefix_lengths: vec![-1],
                    visible: true,
                    global: false,
                    global_index_version: 0,
                    clustered_primary: false,
                    primary: false,
                };
                let ctx = crate::StmtContext::default();
                if operation != "backfill" {
                    table.add_index(index.clone(), false);
                }
                if operation == "partial_update" {
                    let condition = tidb_model::generated_expr::parse_expression("v > 0").unwrap();
                    table
                        .add_partial_index_condition(
                            1,
                            "unique_v",
                            &condition,
                            &ctx.session_zone(),
                            b'\\',
                        )
                        .unwrap();
                }
                let old = [Datum::Int(1), Datum::Int(-1)];
                table.insert_row(&old, &ctx).unwrap();
                table
                    .insert_row(&[Datum::Int(2), Datum::Int(2)], &ctx)
                    .unwrap();
                for (key, value) in buffer.snapshot() {
                    snapshot
                        .lock()
                        .unwrap()
                        .data
                        .insert(key.into_bytes(), value.unwrap());
                }
                buffer.reset();
                let before = table.indexes().len();
                // Keep an earlier statement's write outside this checkpoint.
                buffer.set(key(b"earlier"), b"keep".to_vec()).unwrap();
                let checkpoint = buffer.checkpoint();
                snapshot.lock().unwrap().index_gets = 0;
                snapshot.lock().unwrap().fail_index_get =
                    Some((if operation == "backfill" { 2 } else { 1 }, error.clone()));
                let candidate = [Datum::Int(3), Datum::Int(3)];
                let result = match operation {
                    "insert" => table.insert_row(&candidate, &ctx).map(|_| ()),
                    "update" | "partial_update" => table.update_row_with_old_context(
                        &TableHandle::Int(1),
                        Some(&old),
                        &[Datum::Int(1), Datum::Int(3)],
                        &ctx,
                    ),
                    "backfill" => table.create_index_with_context(index, &ctx),
                    "conflicts" => table.row_conflicts(&candidate, &ctx).map(|_| ()),
                    _ => unreachable!(),
                };
                match result {
                    Ok(()) => failures.push(format!("{operation}: swallowed {error:?}")),
                    Err(actual) => match (actual, &error) {
                        (tidb_table::kv_table::KvTableError::Storage(actual), expected)
                            if actual == *expected => {}
                        (actual, expected) => {
                            failures.push(format!("{operation}: {actual:?}, expected {expected:?}"))
                        }
                    },
                }
                buffer.restore(checkpoint);
                assert_eq!(
                    buffer.snapshot(),
                    [(key(b"earlier"), Some(b"keep".to_vec()))]
                );
                if table.indexes().len() != before {
                    failures.push(format!("{operation}: failed backfill published its index"));
                }
                assert_eq!(
                    table
                        .get_row_by_handle(&TableHandle::Int(1), &ctx.session_zone())
                        .unwrap(),
                    Some(old.to_vec())
                );
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
