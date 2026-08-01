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

//! Cutting a value down to an index key part's declared prefix: Go
//! `pkg/tablecodec/tablecodec.go`'s `TruncateIndexValue` and
//! `pkg/util/ranger/ranger.go`'s `CutDatumByPrefixLen` / `ReachPrefixLen`.
//!
//! # Why the three live together
//!
//! They are the same cut asked three ways. The WRITE path asks "what does the
//! entry hold" ([`cut_index_value`]); the RANGE path asks "did the cut change
//! anything" ([`cut_datum_by_prefix_len`], because a cut endpoint must become
//! inclusive) and "is this value exactly the prefix" ([`reaches_prefix_len`],
//! because `s > 'abc'` on `KEY (s(3))` must still read the `'abc'` entries
//! that stand for `'abcdef'`). Go writes the rule twice and the two copies
//! agree; here it is written once, so a range can never seek a key shape the
//! writer never stores.
//!
//! # The unit is characters, except when it is bytes
//!
//! Go counts BYTES for `binary` and `ascii` columns and RUNES for every other
//! charset -- the declared `(3)` on a `varchar(20) charset utf8mb4` is three
//! CHARACTERS. Which one applies is a property of the COLUMN's charset, not
//! of the datum, which is why every entry point takes the column's
//! [`FieldType`].
//!
//! Note this is a different unit from the one
//! [`crate::ddl::index_prefix::MAX_INDEX_LENGTH`] is checked in: the DDL
//! limit counts bytes (characters times the charset's maximum width), and the
//! cut counts characters. Both are Go's.

use tidb_datatype::{Datum, FieldType};

/// Go `types.UnspecifiedLength`, re-exported at the tier that cuts so a
/// caller never has to reach into the DDL module for the sentinel.
pub(crate) use crate::ddl::index_prefix::UNSPECIFIED_LENGTH;

/// Whether this column's declared prefix counts bytes rather than characters
/// (Go's `charset.CharsetBin`/`CharsetASCII` arm).
fn counts_bytes(field_type: &FieldType) -> bool {
    matches!(field_type.charset_name(), "binary" | "ascii")
}

/// The string payload a cut applies to, or `None` for a datum Go leaves
/// alone: Go cuts only `KindString` and `KindBytes`.
fn cuttable(value: &Datum) -> Option<&[u8]> {
    match value {
        Datum::String(string) => Some(string.bytes()),
        Datum::Bytes(bytes) => Some(bytes),
        _ => None,
    }
}

/// Go `CutDatumByPrefixLen`: cuts `value` in place, reporting whether it
/// actually removed anything.
///
/// The report is what the ranger needs: an endpoint that was cut no longer
/// stands for one value but for every value sharing that prefix, so an
/// exclusive endpoint has to become inclusive or the rows behind it go
/// missing.
pub(crate) fn cut_datum_by_prefix_len(
    value: &mut Datum,
    length: i64,
    field_type: &FieldType,
) -> bool {
    if length == UNSPECIFIED_LENGTH {
        return false;
    }
    let Some(bytes) = cuttable(value) else {
        return false;
    };
    let Ok(length) = usize::try_from(length) else {
        return false;
    };
    if counts_bytes(field_type) {
        if bytes.len() <= length {
            return false;
        }
        let cut = bytes[..length].to_vec();
        match value {
            Datum::Bytes(target) => *target = cut,
            Datum::String(string) => {
                *value = Datum::new_collation_string(cut, string.collation());
            }
            _ => unreachable!("cuttable() admitted only String and Bytes"),
        }
        return true;
    }
    // Every other charset is stored as UTF-8, so a character is a rune. Go
    // counts runes over the raw bytes; invalid UTF-8 decodes to replacement
    // characters there, and `chars()` over a lossy decode counts the same.
    let text = String::from_utf8_lossy(bytes);
    if text.chars().count() <= length {
        return false;
    }
    let cut: String = text.chars().take(length).collect();
    // Go's rune arm calls `SetString` whatever the original kind was, so a
    // `KindBytes` datum on a rune-counted column comes back as a string.
    let collation = match value {
        Datum::String(string) => string.collation(),
        _ => tidb_datatype::Collation::Binary,
    };
    *value = Datum::new_collation_string(cut.into_bytes(), collation);
    true
}

/// Go `TruncateIndexValue`: the value an index entry stores for one key part.
///
/// This is [`cut_datum_by_prefix_len`] without the report, which is the shape
/// the write path wants.
pub(crate) fn cut_index_value(value: &mut Datum, length: i64, field_type: &FieldType) {
    cut_datum_by_prefix_len(value, length, field_type);
}

/// Go `ReachPrefixLen`: the value is exactly as long as the declared prefix,
/// so the entry it maps to also stands for every longer value with the same
/// leading characters.
pub(crate) fn reaches_prefix_len(value: &Datum, length: i64, field_type: &FieldType) -> bool {
    if length == UNSPECIFIED_LENGTH {
        return false;
    }
    let Some(bytes) = cuttable(value) else {
        return false;
    };
    let Ok(length) = usize::try_from(length) else {
        return false;
    };
    if counts_bytes(field_type) {
        return bytes.len() == length;
    }
    String::from_utf8_lossy(bytes).chars().count() == length
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::FieldTypeCode;

    fn varchar(charset: &str) -> FieldType {
        let mut field_type = FieldType::new(FieldTypeCode::Varchar);
        field_type.set_flen(20);
        field_type.set_charset_name(charset);
        field_type
    }

    fn text(value: &Datum) -> String {
        String::from_utf8_lossy(cuttable(value).expect("a string datum")).into_owned()
    }

    /// The truncation case the whole feature exists to get right: the entry
    /// holds `'abc'`, and a reader that trusted it would answer `'abc'` where
    /// the row says `'abcdef'`.
    #[test]
    fn a_longer_value_is_cut_to_the_declared_length() {
        let mut value = Datum::new_string("abcdef");
        assert!(cut_datum_by_prefix_len(&mut value, 3, &varchar("utf8mb4")));
        assert_eq!(text(&value), "abc");
    }

    /// Two rows that share a prefix and differ after it collapse onto ONE
    /// index key. This is why a prefix index cannot be covering and why a
    /// UNIQUE one rejects the second row.
    #[test]
    fn values_sharing_a_prefix_cut_to_the_same_key() {
        let mut first = Datum::new_string("abcdef");
        let mut second = Datum::new_string("abcxyz");
        cut_index_value(&mut first, 3, &varchar("utf8mb4"));
        cut_index_value(&mut second, 3, &varchar("utf8mb4"));
        assert_eq!(text(&first), text(&second));
    }

    /// A value no longer than the prefix is stored whole, and reports no cut
    /// -- an endpoint built from it keeps its exclusiveness.
    #[test]
    fn a_shorter_value_is_left_alone() {
        let mut value = Datum::new_string("ab");
        assert!(!cut_datum_by_prefix_len(&mut value, 3, &varchar("utf8mb4")));
        assert_eq!(text(&value), "ab");
        assert!(!reaches_prefix_len(&value, 3, &varchar("utf8mb4")));
    }

    /// Exactly the prefix length: nothing is cut, but the value REACHES the
    /// prefix, which is the second reason Go drops an endpoint's
    /// exclusiveness (`s > 'abc'` must still read the `'abc'` entries that
    /// stand for `'abcdef'`).
    #[test]
    fn a_value_of_exactly_the_prefix_length_reaches_it_without_being_cut() {
        let value = Datum::new_string("abc");
        let mut copy = value.clone();
        assert!(!cut_datum_by_prefix_len(&mut copy, 3, &varchar("utf8mb4")));
        assert!(reaches_prefix_len(&value, 3, &varchar("utf8mb4")));
    }

    /// Go counts CHARACTERS for a rune-based charset: `(3)` over
    /// `utf8mb4` keeps three code points, nine bytes, not three bytes.
    /// Captured from real TiDB: `select a from c where a = '世界你好啊'`
    /// over `key idx(a(3))` returns the whole `'世界你好啊'`, and
    /// `admin check table c` passes -- both of which need the entry to hold
    /// `'世界你'`.
    #[test]
    fn a_rune_charset_counts_characters() {
        let mut value = Datum::new_string("世界你好啊");
        assert!(cut_datum_by_prefix_len(&mut value, 3, &varchar("utf8mb4")));
        assert_eq!(text(&value), "世界你");
        assert_eq!(cuttable(&value).unwrap().len(), 9);
        assert!(reaches_prefix_len(&value, 3, &varchar("utf8mb4")));
    }

    /// `binary` and `ascii` count BYTES. Captured: `varbinary(20) key
    /// idx(a(3))` holding `'abcdef'` reads back whole and admin-checks clean.
    #[test]
    fn a_binary_column_counts_bytes() {
        let binary = varchar("binary");
        let mut value = Datum::new_bytes(b"\xe4\xb8\x96\xe7\x95\x8c".to_vec());
        assert!(cut_datum_by_prefix_len(&mut value, 3, &binary));
        assert_eq!(cuttable(&value).unwrap(), b"\xe4\xb8\x96");
        // A byte datum stays a byte datum on the byte-counted arm.
        assert!(matches!(value, Datum::Bytes(_)));
    }

    /// No declared prefix is the ordinary index: nothing is ever cut, and no
    /// value reaches a length that does not exist.
    #[test]
    fn an_unspecified_length_never_cuts() {
        let mut value = Datum::new_string("abcdef");
        assert!(!cut_datum_by_prefix_len(
            &mut value,
            UNSPECIFIED_LENGTH,
            &varchar("utf8mb4")
        ));
        assert_eq!(text(&value), "abcdef");
        assert!(!reaches_prefix_len(
            &value,
            UNSPECIFIED_LENGTH,
            &varchar("utf8mb4")
        ));
    }

    /// Go cuts only `KindString` and `KindBytes`; a NULL or a number is left
    /// exactly as it is, which is what keeps a NULL key part NULL and a
    /// range sentinel a sentinel.
    #[test]
    fn a_non_string_datum_is_never_cut() {
        for mut value in [Datum::Null, Datum::Int(1234), Datum::MaxValue] {
            let before = value.clone();
            assert!(!cut_datum_by_prefix_len(&mut value, 3, &varchar("utf8mb4")));
            assert_eq!(value, before);
            assert!(!reaches_prefix_len(&value, 3, &varchar("utf8mb4")));
        }
    }
}
