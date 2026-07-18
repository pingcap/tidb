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

#![allow(missing_docs)]

use tidb_protocol::{
    dump_column, dump_column_with_default, ColumnDefault, ColumnInfo, BINARY_DEFAULT_COLLATION_ID,
    BINARY_FLAG, DEFAULT_COLLATION_ID, ENUM_FLAG, MAX_LONG_BLOB_WIDTH, SET_FLAG, TYPE_LONG_BLOB,
    TYPE_NEW_DATE, TYPE_TIDB_VECTOR_FLOAT32,
};

fn source_column() -> ColumnInfo {
    ColumnInfo {
        schema: "schema".to_owned(),
        table: "table".to_owned(),
        org_table: "org_table".to_owned(),
        name: "name".to_owned(),
        org_name: "org_name".to_owned(),
        column_length: 17,
        charset: BINARY_DEFAULT_COLLATION_ID,
        flag: 0,
        decimal: 3,
        type_code: TYPE_NEW_DATE,
        default_value: None,
    }
}

fn read_lenenc<'a>(packet: &'a [u8], offset: &mut usize) -> &'a [u8] {
    let marker = packet[*offset];
    *offset += 1;
    let length = match marker {
        0..=250 => marker as usize,
        0xfc => {
            let end = *offset + 2;
            let value = u16::from_le_bytes(packet[*offset..end].try_into().unwrap()) as usize;
            *offset = end;
            value
        }
        0xfd => {
            let end = *offset + 3;
            let value = (packet[*offset] as usize)
                | ((packet[*offset + 1] as usize) << 8)
                | ((packet[*offset + 2] as usize) << 16);
            *offset = end;
            value
        }
        0xfe => {
            let end = *offset + 8;
            let value = u64::from_le_bytes(packet[*offset..end].try_into().unwrap()) as usize;
            *offset = end;
            value
        }
        0xfb => panic!("NULL is not a metadata string"),
        _ => panic!("invalid length-encoded marker {marker:#x}"),
    };
    let end = *offset + length;
    let value = &packet[*offset..end];
    *offset = end;
    value
}

fn skip_metadata_prefix(packet: &[u8]) -> usize {
    let mut offset = 0;
    assert_eq!(read_lenenc(packet, &mut offset), b"def");
    for _ in 0..5 {
        read_lenenc(packet, &mut offset);
    }
    offset
}

#[test]
fn metadata_limits_only_display_and_original_column_names() {
    let mut column = source_column();
    column.schema = "s".repeat(300);
    column.table = "t".repeat(300);
    column.org_table = "o".repeat(300);
    column.name = "n".repeat(300);
    column.org_name = "r".repeat(300);

    let mut packet = Vec::new();
    dump_column(&mut packet, &column);
    let mut offset = 0;
    assert_eq!(read_lenenc(&packet, &mut offset), b"def");
    assert_eq!(read_lenenc(&packet, &mut offset), column.schema.as_bytes());
    assert_eq!(read_lenenc(&packet, &mut offset), column.table.as_bytes());
    assert_eq!(
        read_lenenc(&packet, &mut offset),
        column.org_table.as_bytes()
    );
    assert_eq!(
        read_lenenc(&packet, &mut offset),
        &column.name.as_bytes()[..256]
    );
    assert_eq!(
        read_lenenc(&packet, &mut offset),
        &column.org_name.as_bytes()[..256]
    );
    assert_eq!(packet[offset], 0x0c);
}

#[test]
fn metadata_name_limit_never_splits_utf8_or_panics() {
    let mut column = source_column();
    // 86 Euro signs occupy 258 bytes.  The source's 256-byte alias limit
    // lands inside the final code point; the Rust String boundary keeps the
    // largest valid prefix instead of slicing through UTF-8.
    column.name = "€".repeat(86);

    let mut packet = Vec::new();
    dump_column(&mut packet, &column);
    let mut offset = 0;
    for _ in 0..4 {
        read_lenenc(&packet, &mut offset);
    }
    let name = read_lenenc(&packet, &mut offset);
    assert_eq!(name, "€".repeat(85).as_bytes());
    assert_eq!(name.len(), 255);
    assert!(std::str::from_utf8(name).is_ok());
}

#[test]
fn vector_float32_metadata_projection_matches_source() {
    let mut column = source_column();
    column.type_code = TYPE_TIDB_VECTOR_FLOAT32;
    column.charset = BINARY_DEFAULT_COLLATION_ID;
    column.column_length = 19;
    column.flag = BINARY_FLAG | ENUM_FLAG | SET_FLAG;
    column.decimal = 9;

    let mut packet = Vec::new();
    dump_column(&mut packet, &column);
    let mut offset = skip_metadata_prefix(&packet);
    assert_eq!(packet[offset], 0x0c);
    offset += 1;
    assert_eq!(
        u16::from_le_bytes(packet[offset..offset + 2].try_into().unwrap()),
        DEFAULT_COLLATION_ID
    );
    offset += 2;
    assert_eq!(
        u32::from_le_bytes(packet[offset..offset + 4].try_into().unwrap()),
        MAX_LONG_BLOB_WIDTH
    );
    offset += 4;
    assert_eq!(packet[offset], TYPE_LONG_BLOB);
    offset += 1;
    assert_eq!(
        u16::from_le_bytes(packet[offset..offset + 2].try_into().unwrap()),
        ENUM_FLAG | SET_FLAG
    );
    offset += 2;
    assert_eq!(packet[offset], column.decimal);
    assert_eq!(&packet[offset + 1..offset + 3], &[0, 0]);
}

#[test]
fn default_markers_are_all_encoded_as_protocol_null() {
    for default in [
        None,
        Some(ColumnDefault::Null),
        Some(ColumnDefault::CurrentTimestamp),
        Some(ColumnDefault::CurrentDate),
    ] {
        let mut column = source_column();
        column.default_value = default;
        let mut packet = Vec::new();
        dump_column_with_default(&mut packet, &column);
        let mut offset = skip_metadata_prefix(&packet);
        offset += 13;
        assert_eq!(&packet[offset..], &[0xfb]);
    }
}

#[test]
fn byte_and_text_defaults_keep_length_encoded_payload_bytes() {
    for (default, expected) in [
        (ColumnDefault::Bytes(vec![0, 0xff]), vec![0, 0xff]),
        (ColumnDefault::Text("test".to_owned()), b"test".to_vec()),
    ] {
        let mut column = source_column();
        column.default_value = Some(default);
        let mut packet = Vec::new();
        dump_column_with_default(&mut packet, &column);
        let mut offset = skip_metadata_prefix(&packet);
        offset += 13;
        assert_eq!(read_lenenc(&packet, &mut offset), expected);
        assert_eq!(offset, packet.len());
    }
}
