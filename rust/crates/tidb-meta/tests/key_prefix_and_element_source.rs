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

//! `pkg/meta/meta_test.go`'s key-format tables, ported whole.
//!
//! The byte-level side of `pkg/meta`'s keys is already pinned by
//! `tests/go_vectors.rs`, against hex captured from Go. What Go's per-prefix
//! tests add on top is the ROUND TRIP -- `Is*Key` then `Parse*Key` -- and that
//! is what this file ports, over every prefix the crate defines rather than
//! only the five Go happens to have a test for.

use tidb_meta::key;

// --- `TestDBKey` (`:662`), `TestTableKey` (`:672`), `TestAutoTableIDKey`
// (`:682`), `TestAutoRandomTableIDKey` (`:692`), `TestSequenceKey` (`:753`) ---
//
// One shape repeated: build the field for `id = 10`, assert the matching
// `Is*Key` accepts it, parse the id back.

#[test]
fn every_prefixed_field_round_trips_and_rejects_its_neighbours() {
    type Build = fn(i64) -> Vec<u8>;
    type Parse = fn(&[u8]) -> Result<i64, tidb_meta::MetaError>;
    let table: &[(&str, Build, Parse)] = &[
        (key::DB_PREFIX, key::db_key, key::parse_db_key),
        (key::TABLE_PREFIX, key::table_key, key::parse_table_key),
        (
            key::AUTO_TABLE_ID_PREFIX,
            key::auto_table_id_key,
            key::parse_auto_table_id_key,
        ),
        (
            key::AUTO_INCREMENT_ID_PREFIX,
            key::auto_increment_id_key,
            key::parse_auto_increment_id_key,
        ),
        (
            key::AUTO_RANDOM_ID_PREFIX,
            key::auto_random_table_id_key,
            key::parse_auto_random_table_id_key,
        ),
        (
            key::SEQUENCE_PREFIX,
            key::sequence_key,
            key::parse_sequence_key,
        ),
    ];

    for (prefix, build, parse) in table {
        // Go's row: `id = 10`.
        let field = build(10);
        assert_eq!(field, format!("{prefix}:10").as_bytes(), "prefix {prefix}");
        assert!(key::has_prefix(prefix, &field), "prefix {prefix}");
        assert_eq!(parse(&field).unwrap(), 10, "prefix {prefix}");

        // The ids the `int64` field admits at its edges.
        for id in [0_i64, -1, i64::MIN, i64::MAX] {
            let field = build(id);
            assert_eq!(parse(&field).unwrap(), id, "prefix {prefix} id {id}");
        }

        // No prefix accepts another's field. `TID:` versus `Table:` is the
        // pair that matters -- both start with `T`, and a `Table:` parse that
        // accepted `TID:10` would read an allocator value as a `TableInfo`.
        for (other, other_build, _) in table {
            if other == prefix {
                continue;
            }
            let foreign = other_build(10);
            assert!(
                !key::has_prefix(prefix, &foreign),
                "`{prefix}` must not claim `{}`",
                String::from_utf8_lossy(&foreign)
            );
            assert!(
                parse(&foreign).is_err(),
                "`{prefix}` must not parse `{}`",
                String::from_utf8_lossy(&foreign)
            );
        }

        // Malformed shapes: no separator, no id, a non-numeric id, and a
        // trailing space -- Go's `strconv.ParseInt` refuses each.
        for malformed in [
            (*prefix).to_owned(),
            format!("{prefix}:"),
            format!("{prefix}:x"),
            format!("{prefix}:1 "),
        ] {
            assert!(
                parse(malformed.as_bytes()).is_err(),
                "prefix {prefix} accepted `{malformed}`"
            );
        }
    }
}

#[test]
fn key_parse_failures_preserve_go_error_class_message_and_partial_value() {
    use tidb_meta::error::IntegerParseFailure;
    use tidb_meta::MetaError;

    assert_eq!(
        key::parse_db_key(b"DB").unwrap_err().to_string(),
        "[meta:1300]fail to parse dbKey"
    );
    assert_eq!(
        key::parse_auto_table_id_key(b"DB:1")
            .unwrap_err()
            .to_string(),
        "[meta:1300]fail to parse autoTableKey"
    );

    let db_syntax = key::parse_db_key(b"DB:\xff").unwrap_err();
    assert_eq!(
        db_syntax.to_string(),
        "strconv.Atoi: parsing \"\\xff\": invalid syntax"
    );
    assert_eq!(
        db_syntax,
        MetaError::InvalidFieldInteger {
            value: b"\xff".to_vec(),
            partial: 0,
            failure: IntegerParseFailure::Syntax,
            traced: true,
        }
    );

    // ParseTableKey's source check is deliberately only `Table`, not
    // `Table:`. `Tablex` therefore reaches Atoi and returns a numeric syntax
    // error even though IsTableKey rejects it.
    assert!(!key::is_table_key(b"Tablex"));
    assert_eq!(
        key::parse_table_key(b"Tablex").unwrap_err().to_string(),
        "strconv.Atoi: parsing \"Tablex\": invalid syntax"
    );

    let range = key::parse_db_key(b"DB:9223372036854775808").unwrap_err();
    assert_eq!(
        range,
        MetaError::InvalidFieldInteger {
            value: b"9223372036854775808".to_vec(),
            partial: i64::MAX,
            failure: IntegerParseFailure::Range,
            traced: true,
        }
    );
    assert_eq!(
        range.to_string(),
        "strconv.Atoi: parsing \"9223372036854775808\": value out of range"
    );
    let negative_range = key::parse_sequence_key(b"SID:-9223372036854775809").unwrap_err();
    assert_eq!(
        negative_range,
        MetaError::InvalidFieldInteger {
            value: b"-9223372036854775809".to_vec(),
            partial: i64::MIN,
            failure: IntegerParseFailure::Range,
            traced: true,
        }
    );

    // AutoTableID returns the bare NumError in Go rather than tracing it.
    assert!(matches!(
        key::parse_auto_table_id_key(b"TID:x"),
        Err(MetaError::InvalidFieldInteger { traced: false, .. })
    ));
}

// --- `TestElement` -- `pkg/meta/meta_test.go:558`, every row ----------------

#[test]
fn element_round_trips_and_reports_gos_two_failures() {
    use tidb_meta::element::{Element, ElementError, ElementKind};

    // Go's two passing rows: `meta.IndexElementKey` and
    // `meta.ColumnElementKey`, each with `ID: 123`.
    for kind in [ElementKind::Index, ElementKind::Column] {
        let element = kind.element(123);
        let encoded = element.encode();
        assert_eq!(encoded.len(), 13);
        assert_eq!(&encoded[..5], kind.type_key());
        assert_eq!(&encoded[5..], &123_u64.to_be_bytes());
        assert_eq!(Element::decode(&encoded).unwrap(), element);
        assert_eq!(
            element.to_string(),
            format!(
                "ID:123,TypeKey:{}",
                String::from_utf8_lossy(kind.type_key())
            )
        );
    }
    // Go reads the id as a `uint64` and casts, so a negative id survives.
    let negative = ElementKind::Column.element(-1);
    assert_eq!(Element::decode(&negative.encode()).unwrap(), negative);

    // Go's first failing row: `TypeKey: []byte("_col")`. Go's `EncodeElement`
    // copies it into a ZEROED 13-byte buffer, so the stored prefix is
    // `_col` plus a NUL -- which is why the message quotes the NUL.
    let invalid_short = Element {
        id: 123,
        type_key: b"_col".to_vec(),
    };
    assert_eq!(invalid_short.string_bytes(), b"ID:123,TypeKey:_col");
    let truncated = invalid_short.encode();
    let error = Element::decode(&truncated).unwrap_err();
    assert_eq!(error, ElementError::Prefix(b"_col\x00".to_vec()));
    assert_eq!(
        error.to_string(),
        "invalid encoded element key prefix \"_col\\x00\""
    );

    // Go's second failing row: `TypeKey: []byte("inexistent")`, whose copy
    // truncates to five bytes, and whose message quotes `key[:5]`.
    let foreign = Element {
        id: 123,
        type_key: b"inexistent".to_vec(),
    }
    .encode();
    assert_eq!(
        Element::decode(&foreign).unwrap_err().to_string(),
        "invalid encoded element key prefix \"inexi\""
    );

    // Go String preserves arbitrary string bytes even though Rust's Display
    // cannot. The byte receipt is the lossless equivalent.
    assert_eq!(
        Element {
            id: i64::MIN,
            type_key: b"x\xff".to_vec(),
        }
        .string_bytes(),
        b"ID:-9223372036854775808,TypeKey:x\xff"
    );

    // Go's two short-buffer rows.
    assert_eq!(
        Element::decode(b"_col").unwrap_err().to_string(),
        "invalid encoded element \"_col\" length 4"
    );
    assert_eq!(
        Element::decode(b"_col_").unwrap_err().to_string(),
        "invalid encoded element \"_col_\" length 5"
    );
    // The length check is `< 13`, so twelve bytes is short and thirteen is
    // exactly enough.
    assert!(Element::decode(b"_col_\x00\x00\x00\x00\x00\x00\x00").is_err());
    assert!(Element::decode(b"_col_\x00\x00\x00\x00\x00\x00\x00\x00").is_ok());
}
