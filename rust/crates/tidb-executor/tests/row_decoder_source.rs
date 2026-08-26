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

//! Material contracts from `pkg/util/rowDecoder`.

use tidb_datatype::{
    CoreTime, Datum, Decimal, FieldType, FieldTypeCode, FieldTypeFlags, MySqlDuration,
    SessionTimeZone, Time, TimeType,
};
use tidb_executor::{
    admin_check::check_table,
    analyze::{kv::analyze_kv_table, AnalyzeOptions},
    GeneratedColumnSelection, IndexRange, KvColumn, KvIndex, KvTable, RowDecodeContext, RowDecoder,
    StmtContext, TableHandle,
};

fn column(id: i64, name: &str, field_type: FieldType) -> KvColumn {
    KvColumn {
        name: name.to_owned(),
        id,
        field_type,
        column_info_version: 1,
        comment: String::new(),
        generated: None,
        default_value: None,
        origin_default: None,
    }
}

fn parse_generated_expr(text: &str) -> tidb_ast::Expr {
    let sql = format!("create table t (x int as ({text}))");
    let tidb_ast::Stmt::Ddl(ddl) = tidb_parser::parse(&sql).unwrap() else {
        unreachable!("a CREATE TABLE parses as DDL")
    };
    let tidb_ast::DdlStmt::CreateTable(create) = &*ddl else {
        unreachable!("the DDL is CREATE TABLE")
    };
    create.columns[0]
        .options
        .iter()
        .find_map(|option| match option {
            tidb_ast::ColumnOption::Generated { expression, .. } => Some(expression.clone()),
            _ => None,
        })
        .expect("the column carries a generated expression")
}

fn generated_column(
    text: &str,
    stored: bool,
    preceding: &[KvColumn],
    zone: &SessionTimeZone,
) -> tidb_executor::generated_column::GeneratedColumn {
    let names = preceding
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let types = preceding
        .iter()
        .map(|column| column.field_type.clone())
        .collect::<Vec<_>>();
    tidb_executor::generated_column::build_added_generated_column(
        "generated",
        &parse_generated_expr(text),
        stored,
        &names,
        &types,
        zone,
    )
    .unwrap()
}

fn query_context(zone: &SessionTimeZone) -> RowDecodeContext {
    RowDecodeContext::for_query(&StmtContext::for_query().with_time_zone(zone.clone()))
}

fn encode(ids: &[i64], values: &[Datum], new_format: bool, zone: &SessionTimeZone) -> Vec<u8> {
    tidb_tablecodec::encode_table_row(Some(zone), values, ids, new_format, None).unwrap()
}

fn source_columns(zone: &SessionTimeZone, unsigned_handle: bool) -> Vec<KvColumn> {
    let mut columns = vec![
        column(1, "c1", FieldType::new(FieldTypeCode::LongLong)),
        column(2, "c2", FieldType::new(FieldTypeCode::Varchar)),
        column(3, "c3", FieldType::new(FieldTypeCode::NewDecimal)),
        column(4, "c4", FieldType::new(FieldTypeCode::LongLong)),
        column(5, "c5", FieldType::new(FieldTypeCode::LongLong)),
    ];
    columns[4].origin_default = Some(Datum::Int(2));
    let mut generated = column(6, "c6", FieldType::new(FieldTypeCode::LongLong));
    generated.generated = Some(generated_column("c4 + c5", false, &columns, zone));
    columns.push(generated);
    let mut handle_type = FieldType::new(FieldTypeCode::LongLong);
    if unsigned_handle {
        handle_type.add_flags(FieldTypeFlags::UNSIGNED);
    }
    columns.push(column(7, "c7", handle_type));
    columns
}

#[test]
fn row_decoder_matches_defaults_generated_values_and_integer_handles() {
    let zone = SessionTimeZone::utc();
    let stored_ids = [1, 2, 3, 4];
    let stored_values = [
        Datum::Int(100),
        Datum::new_bytes(b"abc".to_vec()),
        Datum::Decimal(Decimal::from_int(1)),
        Datum::Int(8),
    ];

    for new_format in [false, true] {
        let bytes = encode(&stored_ids, &stored_values, new_format, &zone);
        let columns = source_columns(&zone, false);
        let decoder = RowDecoder::new(
            columns.clone(),
            Some(6),
            Vec::new(),
            GeneratedColumnSelection::All,
            query_context(&zone),
        )
        .unwrap();
        let decoded = decoder
            .decode_and_eval(&TableHandle::Int(11), &bytes)
            .unwrap();

        assert_eq!(
            decoded.values(),
            &[
                Datum::Int(100),
                Datum::new_string(b"abc".to_vec()),
                Datum::Decimal(Decimal::from_int(1)),
                Datum::Int(8),
                Datum::Int(2),
                Datum::Int(10),
                Datum::Int(11),
            ]
        );
        assert_eq!(decoded.by_id().get(&6), Some(&Datum::Int(10)));
        assert_eq!(decoded.by_id().get(&7), Some(&Datum::Int(11)));
        assert!(
            !decoded.by_id().contains_key(&5),
            "a default fills CurrentRow but is not added to Go's decode map"
        );

        let no_generated = RowDecoder::new(
            columns,
            Some(6),
            Vec::new(),
            GeneratedColumnSelection::None,
            query_context(&zone),
        )
        .unwrap()
        .decode_and_eval(&TableHandle::Int(11), &bytes)
        .unwrap();
        assert_eq!(no_generated.values()[5], Datum::Null);
        assert!(!no_generated.by_id().contains_key(&6));
    }

    let bytes = encode(&stored_ids, &stored_values, true, &zone);
    let unsigned = RowDecoder::new(
        source_columns(&zone, true),
        Some(6),
        Vec::new(),
        GeneratedColumnSelection::All,
        query_context(&zone),
    )
    .unwrap()
    .decode_and_eval(&TableHandle::Int(-1), &bytes)
    .unwrap();
    assert_eq!(unsigned.values()[6], Datum::UInt(u64::MAX));
}

#[test]
fn row_decoder_matches_temporal_defaults_and_null_rows() {
    let zone = SessionTimeZone::utc();
    let duration_type = FieldType::new(FieldTypeCode::Duration)
        .with_flen(10)
        .with_decimal(0);
    let mut columns = vec![
        column(
            1,
            "ts",
            FieldType::new(FieldTypeCode::Timestamp)
                .with_flen(19)
                .with_decimal(0),
        ),
        column(2, "duration", duration_type),
    ];
    columns[1].origin_default = Some(Datum::new_string("02:00:02"));
    let mut generated = column(
        3,
        "result",
        FieldType::new(FieldTypeCode::Timestamp)
            .with_flen(19)
            .with_decimal(0),
    );
    generated.generated = Some(generated_column("ts + duration", false, &columns, &zone));
    columns.push(generated);

    let timestamp = Time::new(
        CoreTime::from_date(2019, 1, 1, 8, 1, 1, 0),
        TimeType::Timestamp,
        0,
    )
    .unwrap();
    let expected = timestamp
        .add_duration(
            MySqlDuration::from_nanoseconds((2 * 60 * 60 + 2) * 1_000_000_000, 0).unwrap(),
        )
        .unwrap();

    for new_format in [false, true] {
        let decoder = RowDecoder::new(
            columns.clone(),
            None,
            Vec::new(),
            GeneratedColumnSelection::All,
            query_context(&zone),
        )
        .unwrap();
        let bytes = encode(&[1], &[Datum::Time(timestamp)], new_format, &zone);
        let decoded = decoder
            .decode_and_eval(&TableHandle::Int(1), &bytes)
            .unwrap();
        assert_eq!(
            decoded.values(),
            &[
                Datum::Time(timestamp),
                Datum::Duration(
                    MySqlDuration::from_nanoseconds((2 * 60 * 60 + 2) * 1_000_000_000, 0).unwrap(),
                ),
                Datum::Time(expected),
            ]
        );

        let nulls = encode(&[1, 2], &[Datum::Null, Datum::Null], new_format, &zone);
        let decoded = decoder
            .decode_and_eval(&TableHandle::Int(2), &nulls)
            .unwrap();
        assert_eq!(decoded.values(), &[Datum::Null, Datum::Null, Datum::Null]);
    }
}

#[test]
fn row_decoder_restores_every_common_handle_column() {
    let zone = SessionTimeZone::utc();
    let columns = vec![
        column(1, "c1", FieldType::new(FieldTypeCode::LongLong)),
        column(
            2,
            "c2",
            FieldType::new(FieldTypeCode::String).with_collation_name("utf8mb4_bin"),
        ),
        column(3, "c3", FieldType::new(FieldTypeCode::NewDecimal)),
    ];
    let handle =
        tidb_codec::encode_key(&[Datum::Int(100), Datum::new_bytes(b"abc".to_vec())]).unwrap();
    let bytes = encode(&[3], &[Datum::Decimal(Decimal::from_int(1))], true, &zone);
    let decoded = RowDecoder::new(
        columns,
        None,
        vec![0, 1],
        GeneratedColumnSelection::None,
        query_context(&zone),
    )
    .unwrap()
    .decode_and_eval(&TableHandle::Common(handle), &bytes)
    .unwrap();

    assert_eq!(decoded.values()[0], Datum::Int(100));
    assert_eq!(decoded.values()[1].go_bytes(), b"abc");
    assert_eq!(decoded.values()[2], Datum::Decimal(Decimal::from_int(1)));
    assert_eq!(decoded.by_id().get(&1), Some(&Datum::Int(100)));
    assert_eq!(decoded.by_id().get(&2).unwrap().go_bytes(), b"abc");
}

#[test]
fn restored_common_handle_value_wins_over_its_lossy_sort_key() {
    let zone = SessionTimeZone::utc();
    let restored_char =
        FieldType::new(FieldTypeCode::String).with_collation_name("utf8mb4_general_ci");
    assert!(restored_char.need_restored_data());
    let columns = vec![
        column(1, "a", restored_char),
        column(2, "payload", FieldType::new(FieldTypeCode::LongLong)),
    ];

    // A new-collation common handle keeps the collation sort key in the row
    // key. The original bytes are stored in the row value because that sort
    // key is lossy (case folding is visible here as the upper-case weights).
    let handle =
        tidb_codec::encode_key(&[Datum::new_bytes(vec![0, b'A', 0, b'B', 0, b'C'])]).unwrap();
    let bytes = encode(
        &[1, 2],
        &[Datum::new_string("abc"), Datum::Int(9)],
        true,
        &zone,
    );
    let decoded = RowDecoder::new(
        columns.clone(),
        None,
        vec![0],
        GeneratedColumnSelection::None,
        query_context(&zone),
    )
    .unwrap()
    .decode_and_eval(&TableHandle::Common(handle), &bytes)
    .unwrap();

    assert_eq!(decoded.values()[0].go_bytes(), b"abc");
    assert_eq!(decoded.by_id().get(&1).unwrap().go_bytes(), b"abc");
    assert_eq!(decoded.values()[1], Datum::Int(9));

    let statement = StmtContext::for_query().with_time_zone(zone.clone());
    let mut table = KvTable::new(43, columns.clone());
    table.set_common_handle_offsets(vec![0]);
    table
        .insert_row(&[Datum::new_string("abc"), Datum::Int(9)], &statement)
        .unwrap();
    let (_, stored) = table
        .row_cursor_with_context(&RowDecodeContext::for_query(&statement))
        .unwrap()
        .next_row()
        .unwrap()
        .expect("stored common-handle row");
    assert_eq!(stored[0].go_bytes(), b"abc");
    assert_eq!(stored[1], Datum::Int(9));

    let mut old_collation_table = KvTable::new(44, columns).with_new_collation_mode(false);
    old_collation_table.set_common_handle_offsets(vec![0]);
    old_collation_table
        .insert_row(&[Datum::new_string("abc"), Datum::Int(10)], &statement)
        .unwrap();
    let (_, stored) = old_collation_table
        .row_cursor_with_context(&RowDecodeContext::for_query(&statement))
        .unwrap()
        .next_row()
        .unwrap()
        .expect("old-collation common-handle row");
    assert_eq!(stored[0].go_bytes(), b"abc");
    assert_eq!(stored[1], Datum::Int(10));
}

#[test]
fn projected_row_decoder_keeps_common_handle_component_positions() {
    let zone = SessionTimeZone::utc();
    let columns = vec![
        column(1, "c1", FieldType::new(FieldTypeCode::LongLong)),
        column(
            2,
            "c2",
            FieldType::new(FieldTypeCode::String).with_collation_name("utf8mb4_bin"),
        ),
        column(3, "payload", FieldType::new(FieldTypeCode::LongLong)),
    ];
    let handle =
        tidb_codec::encode_key(&[Datum::Int(100), Datum::new_bytes(b"abc".to_vec())]).unwrap();
    let bytes = encode(&[3], &[Datum::Int(9)], true, &zone);

    let decoded = RowDecoder::projected(
        columns.clone(),
        None,
        vec![0, 1],
        GeneratedColumnSelection::None,
        &[1],
        query_context(&zone),
    )
    .unwrap()
    .decode_and_eval(&TableHandle::Common(handle), &bytes)
    .unwrap();

    assert_eq!(decoded.values()[0], Datum::Null);
    assert_eq!(decoded.values()[1].go_bytes(), b"abc");
    assert_eq!(decoded.values()[2], Datum::Null);
    assert!(!decoded.by_id().contains_key(&1));
    assert_eq!(decoded.by_id().get(&2).unwrap().go_bytes(), b"abc");
    assert!(!decoded.by_id().contains_key(&3));

    let statement = StmtContext::for_query().with_time_zone(zone.clone());
    let mut table = KvTable::new(42, columns);
    table.set_common_handle_offsets(vec![0, 1]);
    table
        .insert_row(
            &[Datum::Int(100), Datum::new_string("abc"), Datum::Int(9)],
            &statement,
        )
        .unwrap();
    let mut cursor = table
        .row_cursor_projected_with_context(
            Some(&[1]),
            None,
            &RowDecodeContext::for_query(&statement),
        )
        .unwrap();
    let (_, projected) = cursor
        .next_row()
        .unwrap()
        .expect("stored common-handle row");
    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].go_bytes(), b"abc");
    assert!(cursor.next_row().unwrap().is_none());
}

#[test]
fn split_phase_defers_changing_and_generated_columns() {
    let zone = SessionTimeZone::utc();
    let mut columns = vec![
        column(1, "a", FieldType::new(FieldTypeCode::LongLong)),
        column(2, "a_changing", FieldType::new(FieldTypeCode::LongLong)),
    ];
    let mut generated = column(3, "c", FieldType::new(FieldTypeCode::LongLong));
    generated.generated = Some(generated_column("a_changing + 1", false, &columns, &zone));
    columns.push(generated);
    let mut ordinary_default = column(4, "d", FieldType::new(FieldTypeCode::LongLong));
    ordinary_default.origin_default = Some(Datum::Int(7));
    columns.push(ordinary_default);
    let decoder = RowDecoder::new(
        columns,
        None,
        Vec::new(),
        GeneratedColumnSelection::All,
        RowDecodeContext::for_ddl(&StmtContext::for_query().with_time_zone(zone.clone())),
    )
    .unwrap()
    .with_changing_column(1, 0)
    .unwrap();
    let bytes = encode(&[1], &[Datum::Int(5)], true, &zone);

    let full = decoder
        .decode_and_eval(&TableHandle::Int(9), &bytes)
        .unwrap();
    assert_eq!(
        full.values(),
        &[Datum::Int(5), Datum::Int(5), Datum::Int(6), Datum::Int(7)]
    );
    assert!(!full.by_id().contains_key(&2));
    assert_eq!(full.by_id().get(&3), Some(&Datum::Int(6)));
    assert!(!full.by_id().contains_key(&4));

    let mut split = decoder
        .decode_existing(&TableHandle::Int(9), &bytes)
        .unwrap();
    assert_eq!(
        split.values(),
        &[Datum::Int(5), Datum::Null, Datum::Null, Datum::Int(7)]
    );
    assert!(!split.by_id().contains_key(&2));
    assert!(!split.by_id().contains_key(&3));
    assert_eq!(split.by_id().get(&4), Some(&Datum::Int(7)));
    decoder
        .set_column_value(&mut split, 1, Datum::Int(5))
        .unwrap();
    decoder.eval_remaining(&mut split).unwrap();
    assert_eq!(split.values()[2], Datum::Int(6));
    assert_eq!(split.by_id().get(&3), Some(&Datum::Int(6)));
}

#[test]
fn full_decode_recomputes_stored_generated_values_and_ignores_cast_truncation() {
    let zone = SessionTimeZone::utc();
    let mut columns = vec![column(1, "a", FieldType::new(FieldTypeCode::Varchar))];
    let mut generated = column(2, "b", FieldType::new(FieldTypeCode::LongLong));
    generated.generated = Some(generated_column("a", true, &columns, &zone));
    columns.push(generated);
    let bytes = encode(
        &[1, 2],
        &[Datum::new_bytes(b"12x".to_vec()), Datum::Int(999)],
        true,
        &zone,
    );
    let statement = StmtContext::for_query().with_time_zone(zone.clone());

    let recomputed = RowDecoder::new(
        columns.clone(),
        None,
        Vec::new(),
        GeneratedColumnSelection::All,
        RowDecodeContext::for_query(&statement),
    )
    .unwrap()
    .decode_and_eval(&TableHandle::Int(1), &bytes)
    .unwrap();
    assert_eq!(recomputed.values()[1], Datum::Int(12));
    assert_eq!(recomputed.by_id().get(&2), Some(&Datum::Int(12)));
    assert_eq!(statement.warning_count(), 0);

    let stored = RowDecoder::new(
        columns,
        None,
        Vec::new(),
        GeneratedColumnSelection::Virtual,
        RowDecodeContext::for_query(&statement),
    )
    .unwrap()
    .decode_and_eval(&TableHandle::Int(1), &bytes)
    .unwrap();
    assert_eq!(stored.values()[1], Datum::Int(999));
}

#[test]
fn changing_column_cast_uses_the_statement_error_level() {
    let zone = SessionTimeZone::utc();
    let columns = vec![
        column(1, "a", FieldType::new(FieldTypeCode::LongLong)),
        column(2, "a_changing", FieldType::new(FieldTypeCode::Tiny)),
    ];
    let bytes = encode(&[1], &[Datum::Int(300)], true, &zone);

    let permissive = StmtContext::for_dml(true, false, false).with_time_zone(zone.clone());
    let decoded = RowDecoder::new(
        columns.clone(),
        None,
        Vec::new(),
        GeneratedColumnSelection::None,
        RowDecodeContext::for_write(&permissive),
    )
    .unwrap()
    .with_changing_column(1, 0)
    .unwrap()
    .decode_and_eval(&TableHandle::Int(1), &bytes)
    .unwrap();
    assert_eq!(decoded.values()[1], Datum::Int(127));
    assert_eq!(permissive.warning_count(), 1);

    let strict = StmtContext::for_dml(true, true, false).with_time_zone(zone.clone());
    let error = RowDecoder::new(
        columns,
        None,
        Vec::new(),
        GeneratedColumnSelection::None,
        RowDecodeContext::for_write(&strict),
    )
    .unwrap()
    .with_changing_column(1, 0)
    .unwrap()
    .decode_and_eval(&TableHandle::Int(1), &bytes)
    .unwrap_err();
    assert!(format!("{error:?}").contains("DataOutOfRange"));
}

#[test]
fn projection_decodes_generated_dependencies_but_rejects_bad_offsets() {
    let zone = SessionTimeZone::utc();
    let mut columns = source_columns(&zone, false);
    let mut unrelated = column(8, "unrelated", FieldType::new(FieldTypeCode::LongLong));
    unrelated.generated = Some(generated_column("100 / c5", false, &columns, &zone));
    columns.push(unrelated);
    let statement = StmtContext::for_query().with_time_zone(zone.clone());
    let decoder = RowDecoder::projected(
        columns.clone(),
        Some(6),
        Vec::new(),
        GeneratedColumnSelection::All,
        &[5],
        RowDecodeContext::for_query(&statement),
    )
    .unwrap();
    let bytes = encode(&[4, 5], &[Datum::Int(8), Datum::Int(0)], true, &zone);
    let decoded = decoder
        .decode_and_eval(&TableHandle::Int(4), &bytes)
        .unwrap();
    assert_eq!(decoded.values()[3], Datum::Int(8));
    assert_eq!(decoded.values()[4], Datum::Int(0));
    assert_eq!(decoded.values()[5], Datum::Int(8));
    assert_eq!(decoded.values()[0], Datum::Null);
    assert_eq!(decoded.values()[6], Datum::Null);
    assert_eq!(decoded.values()[7], Datum::Null);
    assert!(!decoded.by_id().contains_key(&8));
    assert_eq!(statement.warning_count(), 0);

    assert!(RowDecoder::projected(
        columns,
        Some(6),
        Vec::new(),
        GeneratedColumnSelection::All,
        &[99],
        query_context(&zone),
    )
    .is_err());
}

#[test]
fn live_consumers_share_full_and_projected_decoder_semantics() {
    let zone = SessionTimeZone::utc();
    let statement = StmtContext::for_query().with_time_zone(zone.clone());
    let mut columns = vec![column(1, "a", FieldType::new(FieldTypeCode::LongLong))];
    let mut generated = column(2, "b", FieldType::new(FieldTypeCode::LongLong));
    generated.generated = Some(generated_column("a + 1", true, &columns, &zone));
    columns.push(generated);
    let mut table = KvTable::new(42, columns);
    let handle = table
        .insert_row(&[Datum::Int(2), Datum::Null], &statement)
        .unwrap();

    // Stored reads preserve the value written under the old expression.
    let rewritten = generated_column("a + 2", true, &table.columns[..1], &zone);
    table.columns_mut()[1].generated = Some(rewritten);
    let read = table
        .get_row_by_handle_with_context(&handle, &RowDecodeContext::for_query(&statement))
        .unwrap()
        .unwrap();
    assert_eq!(read, [Datum::Int(2), Datum::Int(3)]);

    // DDL backfill and ADMIN use the full map and therefore recompute the
    // stored generated column from the current expression.
    let index = KvIndex {
        id: 7,
        name: "b".to_owned(),
        comment: String::new(),
        unique: false,
        column_offsets: vec![1],
        prefix_lengths: vec![-1],
        visible: true,
        global: false,
        clustered_primary: false,
    };
    table.create_index_with_context(index, &statement).unwrap();
    let handles = table
        .scan_index_range(
            7,
            &IndexRange {
                low: vec![Datum::Int(4)],
                high: vec![Datum::Int(4)],
                low_exclusive: false,
                high_exclusive: false,
            },
            &zone,
        )
        .unwrap();
    assert_eq!(handles, std::slice::from_ref(&handle));
    assert_eq!(
        check_table(&mut table, None, &RowDecodeContext::for_query(&statement)).unwrap(),
        1
    );

    let statistics = analyze_kv_table(
        &mut table,
        &AnalyzeOptions {
            num_topn: 0,
            ..AnalyzeOptions::default()
        },
        None,
        &statement,
    )
    .unwrap();
    assert_eq!(statistics.row_count, 1);
    assert!(statistics.columns.contains_key(&1));
    assert!(statistics.columns.contains_key(&2));
}

#[test]
fn a_missing_not_null_column_reports_the_source_error() {
    let zone = SessionTimeZone::utc();
    let not_null = FieldType::new(FieldTypeCode::LongLong).with_flags(FieldTypeFlags::NOT_NULL);
    let decoder = RowDecoder::new(
        vec![column(1, "a", not_null)],
        None,
        Vec::new(),
        GeneratedColumnSelection::None,
        query_context(&zone),
    )
    .unwrap();
    let bytes = encode(&[], &[], true, &zone);
    let error = decoder
        .decode_and_eval(&TableHandle::Int(1), &bytes)
        .unwrap_err();
    assert!(format!("{error:?}").contains("Miss column"));
}
