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

//! Go-generated prefix-index truncation vectors.
//!
//! `truncate_index_value` decides the KEY a prefix-indexed row is written
//! under. If our truncation and Go's disagree by even one byte, the row is
//! stored under a key no index lookup rebuilds -- a silent missing row, not a
//! visible error -- so nothing here may be asserted against our own
//! truncation. Every row below is the kind and the bytes
//! `tablecodec.TruncateIndexValue` itself left behind.
//!
//! Fixture: `generate_index_prefix_truncation.go` beside the `.tsv`.

use tidb_datatype::{Collation, Datum, FieldType, FieldTypeCode};
use tidb_tablecodec::{truncate_index_value, IndexColumn, TableColumn};

const FIXTURE: &str = include_str!(
    "../../../difftests/transaction-tests/fixtures/index_prefix_truncation.tsv"
);

/// The generator's cases, keyed by its own label.
fn case_bytes(name: &str) -> Vec<u8> {
    match name {
        "ascii_abcdef" => b"abcdef".to_vec(),
        "utf8_3char" => "中文字".as_bytes().to_vec(),
        "trunc_emoji_head" => vec![0xF0, 0x9F],
        "trunc_emoji_pad" => vec![0xF0, 0x9F, 0x92, b'a', b'b'],
        "ff_run" => vec![0xFF; 4],
        "mixed" => vec![b'a', 0xC3, 0x28, b'b', 0xE2, 0x82, b'c'],
        "overlong" => vec![0xC0, 0xAF, 0xC0, 0xAF],
        "surrogate" => vec![0xED, 0xA0, 0x80, b'x'],
        "emoji_pair" => "😀😁".as_bytes().to_vec(),
        "empty" => Vec::new(),
        "nul_embedded" => vec![0x00, 0x41, 0x00, 0x42],
        "max_rune" => vec![0xF4, 0x8F, 0xBF, 0xBF, 0x41],
        "above_max_rune" => vec![0xF5, 0x80, 0x80, 0x80, 0x41],
        "seven" => Vec::new(),
        other => panic!("unknown fixture case {other}"),
    }
}

/// The generator's charset tags. Go reads `tblCol.GetCharset()`, so the column
/// carries the charset spelling and not only a collation.
fn column_for(tag: &str) -> TableColumn {
    let field_type = match tag {
        "utf8mb4" => FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Utf8Mb4Bin),
        "bin" => FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Binary),
        "ascii" => FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::AsciiBin),
        other => panic!("unknown charset tag {other}"),
    };
    TableColumn {
        id: 1,
        offset: 0,
        field_type,
        primary_key: false,
        changing_field_type: None,
    }
}

fn kind_name(value: &Datum) -> &'static str {
    match value {
        Datum::String(_) => "String",
        Datum::Bytes(_) => "Bytes",
        Datum::Int(_) => "Int",
        _ => "other",
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn truncate_index_value_go_vectors() {
    let mut rows = 0_usize;
    for line in FIXTURE.lines().filter(|line| !line.trim().is_empty()) {
        let mut fields = line.split('\t');
        let label = fields.next().expect("label");
        let want_kind = fields.next().expect("kind");
        let want_bytes = fields.next().unwrap_or("");

        let mut parts = label.split('/');
        let datum_tag = parts.next().expect("datum kind");
        let case = parts.next().expect("case");
        let charset_tag = parts.next().expect("charset");
        let length: i64 = parts.next().expect("length").parse().expect("length");

        let raw = case_bytes(case);
        let mut value = match datum_tag {
            "string" => Datum::new_collation_string(raw, Collation::Utf8Mb4Bin),
            "bytes" => Datum::new_bytes(raw),
            "int" => Datum::Int(7),
            other => panic!("unknown datum tag {other}"),
        };
        let index_column = IndexColumn {
            offset: 0,
            length,
            use_changing_type: false,
        };
        truncate_index_value(&mut value, &index_column, &column_for(charset_tag)).unwrap();

        assert_eq!(kind_name(&value), want_kind, "{label}: datum kind");
        assert_eq!(
            hex(value.as_raw_bytes().unwrap_or(&[])),
            want_bytes,
            "{label}: truncated bytes"
        );
        rows += 1;
    }
    assert_eq!(rows, 469, "fixture row count");
}
