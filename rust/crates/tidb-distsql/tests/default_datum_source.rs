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

//! Source checks for typed scalar materialization from a `TypeDefault` chunk.

use prost::Message;
use tidb_codec::{encode_value, encode_value_in_timezone};
use tidb_datatype::{
    parse_datetime, Datum, FieldType, FieldTypeCode, MySqlDuration, SessionTimeZone, TimeType,
};
use tidb_distsql::{decode_chunk, ResponseChannel, WarningCollector};
use tidb_proto::{Chunk, EncodeType};
use tidb_proto::tipb::SelectResponse;

#[test]
fn default_chunk_materializes_codec_decode_one_scalar_rows() {
    // Go sources: pkg/distsql/select_result.go::readRowsData and
    // pkg/util/codec/codec_test.go::TestDecodeOneToChunk. Rows have no
    // RowMeta; field metadata determines each decoded value.
    let field_types = [
        FieldType::new(FieldTypeCode::Varchar),
        FieldType::new(FieldTypeCode::Long),
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::Double),
    ];
    let mut rows_data = encode_value(&[
        Datum::Null,
        Datum::new_int(1),
        Datum::new_uint(300),
        Datum::new_real(1.25),
    ])
    .unwrap();
    rows_data.extend_from_slice(
        &encode_value(&[
            Datum::new_bytes(b"abc"),
            Datum::new_int(-7),
            Datum::Null,
            Datum::new_real(2.5),
        ])
        .unwrap(),
    );

    let chunk = Chunk {
        rows_data: Some(rows_data),
        rows_meta: Vec::new(),
    };
    let raw = decode_chunk(&chunk, EncodeType::TypeDefault).unwrap();
    let rows = raw.decode_default_datums(&field_types).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Datum::Null);
    assert_eq!(rows[0][1], Datum::new_int(1));
    assert_eq!(rows[0][2], Datum::new_uint(300));
    assert_eq!(rows[0][3], Datum::new_real(1.25));
    assert_eq!(rows[1][0], Datum::new_bytes(b"abc"));
    assert_eq!(rows[1][1], Datum::new_int(-7));
    assert_eq!(rows[1][2], Datum::Null);
    assert_eq!(rows[1][3], Datum::new_real(2.5));
}

#[test]
fn default_chunk_materializes_duration_with_field_fsp() {
    let duration = Datum::new_duration(MySqlDuration::from_nanoseconds(1_230_000_000, 6).unwrap());
    let chunk = Chunk {
        rows_data: Some(encode_value(std::slice::from_ref(&duration)).unwrap()),
        rows_meta: Vec::new(),
    };
    let raw = decode_chunk(&chunk, EncodeType::TypeDefault).unwrap();
    assert_eq!(
        raw.decode_default_datums(&[FieldType::new(FieldTypeCode::Duration).with_decimal(2)])
            .unwrap(),
        vec![vec![Datum::new_duration(
            MySqlDuration::from_nanoseconds(1_230_000_000, 2).unwrap()
        )]]
    );
}

#[test]
fn select_iterator_uses_the_statement_zone_for_default_timestamps() {
    let session_zone = SessionTimeZone::Fixed {
        name: "+08:00".to_owned(),
        offset_secs: 8 * 60 * 60,
    };
    let mut timestamp = parse_datetime("2026-08-12 14:30:00", &session_zone, true, false)
        .unwrap()
        .time;
    timestamp.set_kind(TimeType::Timestamp);
    let rows_data =
        encode_value_in_timezone(&session_zone, &[Datum::new_time(timestamp)]).unwrap();
    let response = SelectResponse {
        encode_type: Some(EncodeType::TypeDefault as i32),
        chunks: vec![Chunk {
            rows_data: Some(rows_data),
            rows_meta: Vec::new(),
        }],
        ..SelectResponse::default()
    };
    let mut source = ResponseChannel::new();
    source.push_result(response.encode_to_vec()).unwrap();
    source.finish().unwrap();
    let mut iter = source.into_select_iter_in_timezone(
        vec![FieldType::new(FieldTypeCode::Timestamp)],
        Vec::new(),
        session_zone,
        WarningCollector::new(),
    );

    assert_eq!(iter.next_row().unwrap().unwrap().row, vec![Datum::new_time(timestamp)]);
    assert!(iter.next_row().unwrap().is_none());
}
