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

//! Go-parity pins for `pkg/meta/model` unit tests (batch b007), read from
//! `origin/master`. The production port lives in the `tidb-model` crate; these
//! integration tests exercise its public API from the owning crate named by
//! the batch manifest (`tidb-metadef`). Go tests already pinned verbatim by
//! `#[cfg(test)]` modules inside `tidb-model` are listed (with their Rust
//! counterparts) in `rust/testport/receipts/b007.md`.

use tidb_ast::CiString;
use tidb_model::{
    find_index_by_columns_for_foreign_key, get_job_ver_in_use, is_index_prefix_covered,
    is_index_prefix_covered_for_foreign_key, ts_convert_2_time, ColumnInfo, DBInfo, DDLReorgMeta,
    GoShared, GoSharedPointerSlice, HistoryInfo, IndexColumn, IndexInfo, Job, TableInfo,
    TruncateTableArgs, TTLInfo, DEFAULT_TTL_JOB_INTERVAL, OLD_DEFAULT_TTL_JOB_INTERVAL,
};

fn new_column_for_test(id: i64, offset: i64) -> GoShared<ColumnInfo> {
    GoShared::new(ColumnInfo {
        id,
        name: CiString::new(format!("c_{id}")),
        offset,
        ..Default::default()
    })
}

fn new_index_for_test(id: i64, cols: &[GoShared<ColumnInfo>]) -> IndexInfo {
    IndexInfo {
        id,
        name: CiString::new(format!("i_{id}")),
        columns: cols
            .iter()
            .map(|c| IndexColumn {
                offset: c.read().offset,
                name: c.read().name.clone(),
                ..Default::default()
            })
            .collect::<Vec<_>>()
            .into(),
        ..Default::default()
    }
}

/// Fresh column handles per index, mirroring Go's fresh `*ColumnInfo` args in
/// the FK partial-index cases.
fn col(id: i64) -> GoShared<ColumnInfo> {
    new_column_for_test(id, id)
}

/// Go `pkg/meta/model/index_test.go :: TestIsIndexPrefixCovered`.
#[test]
fn is_index_prefix_covered_prefix_matrix_and_fk_partial_conditions() {
    let c0 = new_column_for_test(0, 0);
    let c1 = new_column_for_test(1, 1);
    let c2 = new_column_for_test(2, 2);
    let c3 = new_column_for_test(3, 3);
    let c4 = new_column_for_test(4, 4);

    let tbl = TableInfo {
        id: 1,
        name: CiString::new("t"),
        columns: GoSharedPointerSlice::from_handles(vec![
            Some(c0),
            Some(c1),
            Some(c2),
            Some(c3),
            Some(c4),
        ]),
        indices: GoSharedPointerSlice::from_handles(vec![
            Some(GoShared::new(new_index_for_test(
                0,
                // c0/c1/c2 handles moved above; rebuild lookups by offset.
                &(0..3).map(col).collect::<Vec<_>>(),
            ))),
            Some(GoShared::new(new_index_for_test(
                1,
                &[col(4), col(2)],
            ))),
        ]),
        ..Default::default()
    };
    let i0 = tbl.indices.get(0).unwrap();
    let i1 = tbl.indices.get(1).unwrap();

    assert!(is_index_prefix_covered(&tbl, &i0.read(), &[CiString::new("c_0")]));
    assert!(is_index_prefix_covered(
        &tbl,
        &i0.read(),
        &[CiString::new("c_0"), CiString::new("c_1"), CiString::new("c_2")]
    ));
    assert!(!is_index_prefix_covered(&tbl, &i0.read(), &[CiString::new("c_1")]));
    assert!(!is_index_prefix_covered(&tbl, &i0.read(), &[CiString::new("c_2")]));
    assert!(!is_index_prefix_covered(
        &tbl,
        &i0.read(),
        &[CiString::new("c_1"), CiString::new("c_2")]
    ));
    assert!(!is_index_prefix_covered(
        &tbl,
        &i0.read(),
        &[CiString::new("c_0"), CiString::new("c_2")]
    ));

    assert!(is_index_prefix_covered(&tbl, &i1.read(), &[CiString::new("c_4")]));
    assert!(is_index_prefix_covered(
        &tbl,
        &i1.read(),
        &[CiString::new("c_4"), CiString::new("c_2")]
    ));
    assert!(!is_index_prefix_covered(&tbl, &i0.read(), &[CiString::new("c_2")]));

    // Partial-index FK coverage: only an "IS NOT NULL" condition over the FK
    // column prefix keeps an index usable for a foreign key.
    let mut safe_partial = new_index_for_test(2, &[col(0), col(1)]);
    safe_partial.condition_expr_string = "`c_1` is not null".into();
    assert!(is_index_prefix_covered_for_foreign_key(
        &tbl,
        &safe_partial,
        &[CiString::new("c_0"), CiString::new("c_1")]
    ));

    let mut safe_partial_on_first_fk_col = new_index_for_test(3, &[col(0), col(1)]);
    safe_partial_on_first_fk_col.condition_expr_string = "`c_0` is not null".into();
    assert!(is_index_prefix_covered_for_foreign_key(
        &tbl,
        &safe_partial_on_first_fk_col,
        &[CiString::new("c_0"), CiString::new("c_1")]
    ));

    let mut unsafe_partial_on_non_fk_col = new_index_for_test(4, &[col(0), col(1)]);
    unsafe_partial_on_non_fk_col.condition_expr_string = "`c_2` is not null".into();
    assert!(!is_index_prefix_covered_for_foreign_key(
        &tbl,
        &unsafe_partial_on_non_fk_col,
        &[CiString::new("c_0"), CiString::new("c_1")]
    ));

    let mut unsafe_partial_is_null = new_index_for_test(5, &[col(0)]);
    unsafe_partial_is_null.condition_expr_string = "`c_0` is null".into();
    assert!(!is_index_prefix_covered_for_foreign_key(
        &tbl,
        &unsafe_partial_is_null,
        &[CiString::new("c_0")]
    ));

    let mut unsafe_partial_binary_condition = new_index_for_test(6, &[col(0)]);
    unsafe_partial_binary_condition.condition_expr_string = "`c_0` > 0".into();
    assert!(!is_index_prefix_covered_for_foreign_key(
        &tbl,
        &unsafe_partial_binary_condition,
        &[CiString::new("c_0")]
    ));

    let mut bad_condition = new_index_for_test(7, &[col(0)]);
    bad_condition.condition_expr_string = "`c_0` is".into();
    assert!(!is_index_prefix_covered_for_foreign_key(
        &tbl,
        &bad_condition,
        &[CiString::new("c_0")]
    ));

    // require.Same: FindIndexByColumnsForForeignKey must return the exact
    // source index handle (pointer identity), not a copy.
    let safe_handle = GoShared::new(safe_partial);
    let found = find_index_by_columns_for_foreign_key(
        &tbl,
        &GoSharedPointerSlice::from_handles(vec![
            Some(GoShared::new(unsafe_partial_on_non_fk_col)),
            Some(safe_handle.clone()),
        ]),
        &[CiString::new("c_0"), CiString::new("c_1")],
    )
    .expect("source index found");
    assert!(found.ptr_eq(&safe_handle));
}

/// Go `pkg/meta/model/index_test.go :: TestGlobalIndexV1SupportedForNextGen`.
#[test]
fn global_index_v1_supported_for_next_gen() {
    if tidb_config::kerneltype::is_next_gen() {
        assert!(tidb_model::index::get_global_index_v1_supported());
    }
}

/// Go `pkg/meta/model/job_test.go :: TestJobVerInUse`.
#[test]
fn job_ver_in_use_matches_kernel_type() {
    if tidb_config::kerneltype::is_classic() {
        assert_eq!(get_job_ver_in_use(), tidb_model::JobVersion::V1);
    } else {
        assert_eq!(get_job_ver_in_use(), tidb_model::JobVersion::V2);
    }
}

/// Go `pkg/meta/model/table_test.go :: TestModelBasic`.
///
/// The Go test's `ast.IndexType.String()` assertions belong to
/// `pkg/parser/ast` and are out of scope for this batch's package.
#[test]
fn model_basic_accessors_flags_and_corner_cases() {
    let column = GoShared::new(ColumnInfo {
        id: 1,
        name: CiString::new("c"),
        offset: 0,
        hidden: true,
        field_type: tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Long),
        ..Default::default()
    });
    column.write().add_flag(tidb_mysql::PriKeyFlag as u64);

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

    let seq = GoShared::new(tidb_model::table::SequenceInfo {
        start: 1,
        min_value: 1,
        max_value: 100,
        ..Default::default()
    });

    let table = GoShared::new(TableInfo {
        id: 1,
        name: CiString::new("t"),
        charset: "utf8".into(),
        collate: "utf8_bin".into(),
        columns: GoSharedPointerSlice::from_handles(vec![Some(column.clone())]),
        indices: GoSharedPointerSlice::from_handles(vec![Some(GoShared::new(index))]),
        pk_is_handle: true,
        ..Default::default()
    });

    let table2 = TableInfo {
        id: 2,
        name: CiString::new("s"),
        sequence: Some(seq),
        ..Default::default()
    };

    let mut db_info = DBInfo {
        id: 1,
        name: CiString::new("test"),
        charset: "utf8".into(),
        collate: "utf8_bin".into(),
        ..Default::default()
    };
    db_info.deprecated_tables = GoSharedPointerSlice::from_handles(vec![Some(table.clone())]);

    // require.Equal(dbInfo, dbInfo.Clone())
    let cloned = db_info.clone_like_go();
    assert_eq!(format!("{cloned:?}"), format!("{db_info:?}"));

    let table_read = table.read();
    assert_eq!(table_read.get_pk_name().original(), "c");
    let new_column = table_read.get_pk_col_info().expect("pk column");
    assert!(new_column.read().hidden);
    assert!(new_column.ptr_eq(&column));
    assert!(table_read.column_is_in_index(Some(&column.read())));
    assert!(table_read.indices.get(0).unwrap().read().has_prefix_index());
    assert_eq!(
        ts_convert_2_time(table_read.update_ts).unix_millis(),
        table_read.get_update_time().unix_millis()
    );
    drop(table_read);

    assert!(table2.is_sequence());
    assert!(!table2.is_base_table());

    // Corner cases: toggling the PK flag hides the pk column again.
    column.write().toggle_flag(tidb_mysql::PriKeyFlag as u64);
    let table_read = table.read();
    assert_eq!(table_read.get_pk_name().original(), "");
    assert!(table_read.get_pk_col_info().is_none());
    let an_col = ColumnInfo {
        name: CiString::new("d"),
        ..Default::default()
    };
    assert!(!table_read.column_is_in_index(Some(&an_col)));
    drop(table_read);

    let an_index = IndexInfo {
        columns: vec![].into(),
        ..Default::default()
    };
    assert!(!an_index.has_prefix_index());

    // NewExtraHandleColInfo carries NotNull|PriKey flags and the binary
    // charset/collation.
    let extra_pk = ColumnInfo::new_extra_handle_col_info();
    assert_eq!(
        extra_pk.get_flag(),
        (tidb_mysql::NotNullFlag | tidb_mysql::PriKeyFlag) as u64
    );
    assert_eq!(extra_pk.get_charset(), "binary");
    assert_eq!(extra_pk.get_collate(), "binary");
}

/// Go `pkg/meta/model/table_test.go :: TestTTLInfoClone`.
#[test]
fn ttl_info_clone_leaves_source_untouched() {
    let ttl_info = TTLInfo {
        column_name: CiString::new("test"),
        interval_expr_str: "test_expr".into(),
        interval_time_unit: 5,
        enable: true,
        ..Default::default()
    };

    let mut cloned = ttl_info.clone();
    cloned.column_name = CiString::new("test_2");
    cloned.interval_expr_str = "test_expr_2".into();
    cloned.interval_time_unit = 9;
    cloned.enable = false;

    assert_eq!(ttl_info.column_name.original(), "test");
    assert_eq!(ttl_info.interval_expr_str, "test_expr");
    assert_eq!(ttl_info.interval_time_unit, 5);
    assert!(ttl_info.enable);
}

/// Go `pkg/meta/model/table_test.go :: TestTTLJobInterval` and
/// `TestTTLDefaultJobInterval`.
#[test]
fn ttl_job_intervals_default_old_and_custom_values() {
    // Empty JobInterval falls back to OldDefaultTTLJobInterval ("1h").
    assert_eq!(
        TTLInfo::default().get_job_interval().unwrap(),
        3_600_000_000_000
    );

    // The package-level defaults parse to their documented durations.
    assert_eq!(
        TTLInfo {
            job_interval: DEFAULT_TTL_JOB_INTERVAL.to_owned(),
            ..Default::default()
        }
        .get_job_interval()
        .unwrap(),
        86_400_000_000_000
    );
    assert_eq!(
        TTLInfo {
            job_interval: OLD_DEFAULT_TTL_JOB_INTERVAL.to_owned(),
            ..Default::default()
        }
        .get_job_interval()
        .unwrap(),
        3_600_000_000_000
    );

    assert_eq!(
        TTLInfo {
            job_interval: "200h".to_owned(),
            ..Default::default()
        }
        .get_job_interval()
        .unwrap(),
        720_000_000_000_000
    );
}

/// Go `pkg/meta/model/job_test.go :: TestJobCodec`.
///
/// Deviation: Go fills the job with `&RenameTableArgs{...}`; the Rust port has
/// not wired `RenameTableArgs` into the typed `JobArgs` receiver codec yet, so
/// this port uses `TruncateTableArgs{FKCheck: true}` — go-parity-gap:
/// RenameTableArgs lacks a Rust JobArgs impl. Every other assertion follows
/// the Go test one-for-one.
#[test]
fn job_codec_round_trips_binlog_reorg_meta_and_resume_reason() {
    let kv_disk_full = tidb_model::job::JOB_RESUME_REASON_KV_DISK_FULL;
    // Job hides its mutex and runtime args slots, so build through Default
    // and public-field assignment instead of a struct literal.
    let mut job = Job::default();
    job.version = tidb_model::JobVersion::V1;
    job.id = 1;
    job.table_id = 2;
    job.schema_id = 1;
    job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
    job.reorg_meta = Some(GoShared::new(DDLReorgMeta::default()));
    // TimeZoneLocation hides its cached-resolution slot, so go through the
    // persisted JSON shape.
    let location = GoShared::new(
        serde_json::from_str::<tidb_model::TimeZoneLocation>(r#"{"name":"UTC","offset":0}"#)
            .expect("parse time zone location"),
    );
    job.reorg_meta.as_ref().unwrap().write().location = Some(location);
    job.fill_args(Some(GoShared::new(TruncateTableArgs {
        fk_check: tidb_model::GoField::new(true),
        ..Default::default()
    })));
    job.binlog_info
        .as_mut()
        .unwrap()
        .write()
        .add_db_info(123, Some(GoShared::new(DBInfo {
            id: 1,
            name: CiString::new("test_history_db"),
            ..Default::default()
        })));
    job.binlog_info
        .as_mut()
        .unwrap()
        .write()
        .add_table_info(123, Some(GoShared::new(TableInfo {
            id: 1,
            name: CiString::new("test_history_tbl"),
            ..Default::default()
        })));
    job.set_resume_reason(kv_disk_full);

    assert!(!job.is_cancelled());
    let b = job.encode(false).expect("encode job");
    let mut new_job = Job::default();
    new_job.decode(&b).expect("decode job");
    let history = new_job.binlog_info.as_ref().unwrap().read();
    assert_eq!(history.schema_version, 123);
    assert_eq!(history.db_info.as_ref().unwrap().read().id, 1);
    assert_eq!(
        history.db_info.as_ref().unwrap().read().name.original(),
        "test_history_db"
    );
    assert_eq!(
        history.table_info.as_ref().unwrap().read().name.original(),
        "test_history_tbl"
    );
    drop(history);
    assert!(!new_job.to_string().is_empty());
    let location = new_job.reorg_meta.as_ref().unwrap().read().location.clone();
    let location = location.expect("decoded reorg meta location");
    assert_eq!(location.read().name, "UTC");
    assert_eq!(location.read().offset, 0);
    assert!(new_job.has_resume_reason(kv_disk_full));

    // Binlog info cleaned before Encode(true): nothing survives the round
    // trip.
    job.binlog_info.as_mut().unwrap().write().clean();
    let b1 = job.encode(true).expect("encode cleaned job");
    let mut new_job = Job::default();
    new_job.decode(&b1).expect("decode cleaned job");
    let history = new_job.binlog_info.as_ref().unwrap().read();
    assert!(history.db_info.is_none());
    assert!(history.table_info.is_none());
    drop(history);
    assert!(!new_job.to_string().is_empty());

    job.state = tidb_model::JobState::DONE;
    assert!(job.is_done());
    assert!(job.is_finished());
    assert!(!job.is_running());
    assert!(!job.is_synced());
    assert!(!job.is_rollback_done());
    job.set_row_count(3);
    assert_eq!(job.get_row_count(), 3);
}

/// Go `pkg/meta/model/job_test.go :: TestString`: the ActionMap-driven
/// `ActionType.String()` renderings pinned by the upstream table.
#[test]
fn action_type_string_names_match_go_action_map() {
    use tidb_model::ActionType;
    let acts = [
        (ActionType::ACTION_NONE, "none"),
        (ActionType::ACTION_ADD_FOREIGN_KEY, "add foreign key"),
        (ActionType::ACTION_DROP_FOREIGN_KEY, "drop foreign key"),
        (ActionType::ACTION_TRUNCATE_TABLE, "truncate table"),
        (ActionType::ACTION_MODIFY_COLUMN, "modify column"),
        (ActionType::ACTION_RENAME_TABLE, "rename table"),
        (ActionType::ACTION_RENAME_TABLES, "rename tables"),
        (ActionType::ACTION_SET_DEFAULT_VALUE, "set default value"),
        (ActionType::ACTION_CREATE_SCHEMA, "create schema"),
        (ActionType::ACTION_DROP_SCHEMA, "drop schema"),
        (ActionType::ACTION_CREATE_TABLE, "create table"),
        (ActionType::ACTION_DROP_TABLE, "drop table"),
        (ActionType::ACTION_ADD_INDEX, "add index"),
        (ActionType::ACTION_DROP_INDEX, "drop index"),
        (ActionType::ACTION_ADD_COLUMN, "add column"),
        (ActionType::ACTION_DROP_COLUMN, "drop column"),
        (
            ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE,
            "modify schema charset and collate",
        ),
        (ActionType::ACTION_ALTER_TABLE_PLACEMENT, "alter table placement"),
        (
            ActionType::ACTION_ALTER_TABLE_PARTITION_PLACEMENT,
            "alter table partition placement",
        ),
        (ActionType::ACTION_ALTER_NO_CACHE_TABLE, "alter table nocache"),
        (ActionType::ACTION_ALTER_TABLE_AFFINITY, "alter table affinity"),
        (
            ActionType::ACTION_ALTER_TABLE_SOFT_DELETE_INFO,
            "alter soft delete info",
        ),
        (
            ActionType::ACTION_MODIFY_SCHEMA_SOFT_DELETE_AND_ACTIVE_ACTIVE,
            "modify schema soft delete and active active",
        ),
    ];
    for (action, expected) in acts {
        assert_eq!(action.to_string(), expected);
    }
}
