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

//! Ports of `pkg/util/chunk/mutrow_test.go`.

use tidb_datatype::{
    BinaryJSON, CoreTime, Datum, FieldType, FieldTypeCode, MySqlDuration, Time, TimeType,
};

use crate::chunk::Chunk;
use crate::mutrow::MutRow;

/// Go's `newAllTypes` subset exercised here: every kind whose zero value this
/// port can construct without a collation registry.
fn all_types() -> Vec<FieldType> {
    vec![
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::Float),
        FieldType::new(FieldTypeCode::NewDecimal),
        FieldType::new(FieldTypeCode::Double),
        FieldType::new(FieldTypeCode::Datetime),
    ]
}

fn zero_time() -> Time {
    Time::new(CoreTime::default(), TimeType::DateTime, 0).unwrap()
}

/// Go `TestMutRow` (mutrow_test.go): a MutRow built from types reads back as
/// each type's zero value, and SetValues/SetDatums/SetRow overwrite cells.
#[test]
fn mut_row() {
    let field_types = all_types();
    let mut_row = MutRow::from_types(&field_types);

    // Every cell reads back as the type's zero value.
    {
        let row = mut_row.to_row();
        assert_eq!(row.get_int64(0), 0);
        assert_eq!(row.get_string(1), "");
        assert_eq!(row.get_float32(2), 0.0);
        // Go compares through `Datum.Compare`, so any representation of
        // numeric zero matches.
        let zero = row.get_my_decimal(3).to_string_bytes();
        assert_eq!(zero, b"0");
        assert_eq!(row.get_float64(4), 0.0);
        assert_eq!(row.get_time(5), zero_time());
        for i in 0..row.len() {
            assert!(!row.is_null(i));
        }
    }

    // MutRowFromValues("abc", 123) then SetValues / SetDatums.
    let mut mut_row = MutRow::from_datums(&[
        Datum::String(string_datum("abc")),
        Datum::Int(123),
    ]);
    assert_eq!(mut_row.to_row().get_string(0), "abc");
    assert_eq!(mut_row.to_row().get_int64(1), 123);

    mut_row.set_values(&[Datum::String(string_datum("abcd")), Datum::Int(456)]);
    {
        let row = mut_row.to_row();
        assert_eq!(row.get_string(0), "abcd");
        assert!(!row.is_null(0));
        assert_eq!(row.get_int64(1), 456);
        assert!(!row.is_null(1));
    }

    mut_row.set_datums(&[Datum::String(string_datum("defgh")), Datum::Int(33)]);
    {
        let row = mut_row.to_row();
        assert_eq!(row.get_string(0), "defgh");
        assert!(!row.is_null(0));
        assert_eq!(row.get_int64(1), 33);
        assert!(!row.is_null(1));
    }

    // SetRow propagates NULLs: ("foobar", nil) then (nil, 111).
    mut_row.set_row(
        MutRow::from_datums(&[Datum::String(string_datum("foobar")), Datum::Null]).to_row(),
    );
    {
        let row = mut_row.to_row();
        assert!(!row.is_null(0));
        assert!(row.is_null(1));
    }

    let n_mut_row = MutRow::from_datums(&[Datum::Null, Datum::Int(111)]);
    let n_row = n_mut_row.to_row();
    assert!(n_row.is_null(0));
    assert!(!n_row.is_null(1));
    mut_row.set_row(n_row);
    drop(n_mut_row);
    {
        let row = mut_row.to_row();
        assert!(row.is_null(0));
        assert!(!row.is_null(1));
    }

    // JSON and time cells round-trip.
    let json = BinaryJSON::parse("true").unwrap();
    let time = Time::new(
        CoreTime::default(),
        TimeType::DateTime,
        6, // Go `types.MaxFsp`
    )
    .unwrap();
    let mut mut_row = MutRow::from_datums(&[Datum::Json(json.clone()), Datum::Time(time)]);
    {
        let row = mut_row.to_row();
        assert_eq!(row.get_json(0), json);
        assert_eq!(row.get_time(1), time);
    }

    // SetValue/SetDatum of a duration match a chunk that appended the same
    // duration (Go compares raw column bytes; this port compares through the
    // typed getters, which read those same bytes).
    let dur = MySqlDuration::new(1, 23, 45, 0, 0).unwrap();
    let duration_fields = [FieldType::new(FieldTypeCode::Duration)];
    let mut chk = Chunk::new(&duration_fields, 1, 1);
    chk.append_duration(0, dur);
    let mut mut_row = MutRow::from_types(&duration_fields);
    mut_row.set_value(0, &Datum::Duration(dur));
    assert_eq!(
        mut_row.to_row().get_duration(0, 0),
        chk.get_row(0).get_duration(0, 0)
    );
    mut_row.set_datum(0, &Datum::Duration(dur));
    assert_eq!(
        mut_row.to_row().get_duration(0, 0),
        chk.get_row(0).get_duration(0, 0)
    );
}

// Local shim keeping the test body close to the Go source names.
use tidb_datatype::MyDecimal;

fn string_datum(text: &str) -> tidb_datatype::StringDatum {
    tidb_datatype::StringDatum::new(text.as_bytes().to_vec(), tidb_datatype::Collation::Binary)
}

/// Go `TestIssue29947` (mutrow_test.go): setting a NULL datum on every column
/// of a fresh MutRow leaves each cell NULL. The byte-level "data/elemBuf
/// unchanged" assertions are internal to Go's Column; the observable contract
/// -- NULL cells that no longer carry values -- is checked here.
#[test]
fn issue_29947_set_null_datum_on_every_type() {
    let field_types = all_types();
    let mut mut_row = MutRow::from_types(&field_types);
    for i in 0..field_types.len() {
        mut_row.set_datum(i, &Datum::Null);
        let row = mut_row.to_row();
        assert!(row.is_null(i), "column {i} must be NULL after SetDatum(nil)");
        // Variable-length columns keep empty cells (Go: every offset stays 0);
        // fixed columns' untouched buffers are internal to both languages.
        if !matches!(
            field_types[i].code(),
            FieldTypeCode::VarString | FieldTypeCode::Json
        ) {
            continue;
        }
        assert_eq!(row.get_raw_len(i), 0, "column {i} must have an empty cell");
    }
}

/// Go `TestMutRowShallowCopyPartialRow` (mutrow_test.go): ShallowCopyPartialRow
/// copies row 0's cells into the MutRow and later appends to the source chunk
/// do not disturb the copied snapshot.
#[test]
fn mut_row_shallow_copy_partial_row() {
    let col_types = vec![
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::Timestamp),
    ];

    let mut mut_row = MutRow::from_types(&col_types);
    let mut row_chunk = {
        let fields = col_types.clone();
        let mut chunk = Chunk::new(&fields, 8, 8);
        chunk.append_string(0, "abc");
        chunk.append_int64(1, 123);
        chunk.append_time(2, zero_time());
        chunk
    };
    mut_row.shallow_copy_partial_row(0, &mut row_chunk, 0);
    {
        let copied = mut_row.to_row();
        let source = row_chunk.get_row(0);
        assert_eq!(copied.get_string(0), source.get_string(0));
        assert_eq!(copied.get_int64(1), source.get_int64(1));
        assert_eq!(copied.get_time(2), source.get_time(2));
    }

    // Append further rows to the source chunk after resetting it. In Go the
    // shallow copy shares `col.data`, so the reset+append rewrites row 0's
    // bytes and the MutRow observes the new values through the shared buffer.
    // This port snapshots into shared `SharedBytes` storage with copy-on-write
    // promotion; whether the overwrite is observed depends on the same backing
    // reuse, so pin whichever side the port realizes -- divergence from Go is
    // flagged below.
    let d_string = Datum::String(string_datum("dfg"));
    let d_int = Datum::Int(567);
    let d_time = Time::new(CoreTime::default(), TimeType::Timestamp, 6).unwrap();
    {
        let copied_before = mut_row.to_row().get_time(2);
        row_chunk.reset();
        row_chunk.append_datum(0, &d_string);
        row_chunk.append_datum(1, &d_int);
        row_chunk.append_datum(2, &Datum::Time(d_time));

        let copied = mut_row.to_row();
        let source = row_chunk.get_row(0);
        if copied.get_string(0) == "abc" {
            // go-parity-gap: ShallowCopyPartialRow snapshotted instead of
            // sharing the source column's backing store, so a later
            // reset+append on the source does not rewrite the MutRow cells.
            panic!(
                "mutrow did not observe the source overwrite (snapshot time was {copied_before:?})"
            );
        }
        assert_eq!(copied.get_time(2), d_time);
        assert_eq!(copied.get_string(0), source.get_string(0));
        assert_eq!(copied.get_int64(1), source.get_int64(1));
        assert_eq!(copied.get_time(2), source.get_time(2));
    }
}
