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

//! The endpoints a `LIKE` puts on an index column: Go
//! `pkg/util/ranger/points.go`'s `newBuildFromPatternLike`.
//!
//! # Why this is its own module
//!
//! Every other condition shape bounds the column with the VALUES the user
//! wrote, and the key codec collates them on their way into a key. A `LIKE`
//! cannot: its upper bound is not a value anyone could have written, it is
//! the smallest WEIGHT STRING above every weight string that starts with the
//! pattern's literal prefix. So this is the one builder that leaves the text
//! space and does its arithmetic in the collated one, and the one whose
//! endpoints come back as [`Datum::Bytes`] rather than as strings.

use super::{cut_prefix_for_points, not_null_full_range, Point, RangeColumn};
use tidb_datatype::Datum;

/// Go `builder.newBuildFromPatternLike`: the literal prefix before the first
/// wildcard bounds the scan below, and the next weight string above that
/// prefix bounds it from above.
/// Returns the points and whether this arm already cut the prefix and moved
/// them into the sort key -- Go's `newBuildFromPatternLike` does that itself
/// in exactly one of its five return cases, and leaves the rest to the shared
/// tail. Reporting the whole function as self-finished skipped the conversion
/// for the exact-match case, which then printed its raw text where Go prints
/// a weight string.
pub(super) fn points_from_like(
    pattern: &str,
    escape: u8,
    collation: tidb_datatype::Collation,
    column: &RangeColumn,
    convert_to_sort_key: bool,
) -> (Vec<Point>, bool) {
    let string = |bytes: Vec<u8>| Datum::new_collation_string(bytes, collation);
    // Go's "non-exceptional return case 1": the shared tail converts it.
    if pattern.is_empty() {
        let empty = string(Vec::new());
        return (
            vec![Point::start(empty.clone(), false), Point::end(empty, false)],
            false,
        );
    }
    let bytes = pattern.as_bytes();
    let mut low = Vec::with_capacity(bytes.len());
    let mut exact = true;
    let mut exclude = false;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == escape {
            i += 1;
            low.push(if i < bytes.len() { bytes[i] } else { escape });
            i += 1;
            continue;
        }
        if bytes[i] == b'%' {
            exact = false;
            break;
        }
        if bytes[i] == b'_' {
            // Go `points.go:775-788`: `_` matches exactly one character, so
            // the prefix itself is longer than the match and the low bound is
            // EXCLUSIVE -- but only under a NON-PAD-SPACE collation. Under a
            // PAD SPACE one the stored index key has its trailing spaces
            // trimmed, so `'abc'` and `'abc   '` are the same key and
            // excluding the bound would MISS `'abc   '`, a wrong answer rather
            // than a wider scan.
            //
            // `IsPadSpaceCollation` (`collate.go:363`) is a three-name
            // exception list, and `binary` is one of the three -- so a
            // `VARBINARY` key really does take the exclusive bound. Captured:
            //
            // ```text
            // create table b(a varbinary(20), key(a));
            // explain select * from b where a like 'abc_%'
            //   IndexRangeScan  range:("abc","abd")     <- low EXCLUSIVE
            // create table c(a varchar(20), key(a));
            // explain select * from c where a like 'abc_%'
            //   IndexRangeScan  range:["abc","abd")     <- low inclusive
            // ```
            exclude = !is_pad_space(collation);
            exact = false;
            break;
        }
        low.push(bytes[i]);
        i += 1;
    }
    // Go's case 2. No literal characters before the wildcard: nothing to bound
    // the scan. Returned as-is -- there is no text in it to convert.
    if low.is_empty() {
        return (not_null_full_range(), true);
    }
    // Go's case 3: a pattern with no wildcard at all is an equality on the
    // pattern text, and the shared tail cuts and converts it exactly as Go's
    // `cutPrefixForPoints` + `convertPointsToSortKeyInPlace` pair does here.
    if exact {
        let value = string(low);
        return (
            vec![Point::start(value.clone(), false), Point::end(value, false)],
            false,
        );
    }
    // Go's case 4-1. The upper bound below is the incremented SORT KEY of the
    // prefix, so it only bounds a scan whose keys are sort keys. An entry
    // point that reads raw values instead -- partition pruning -- would be
    // comparing a weight string against the stored text, so Go declines to
    // build a wildcard range at all unless the collation is one where the two
    // coincide.
    if !convert_to_sort_key && !tidb_datatype::is_bin_collation(collation.name()) {
        return (not_null_full_range(), true);
    }
    // Go cuts the START point before deriving the upper bound from it
    // (`newBuildFromPatternLike`'s case 4-2 calls `cutPrefixForPoints` on the
    // start alone, then takes `PrefixNext` of the CUT value). Deriving the
    // bound first and cutting both would collapse `LIKE 'abcd%'` on
    // `KEY (a(3))` onto the empty `["abc","abc")`; Go prints `["abc","abd")`.
    let mut start = Point::start(string(low), exclude);
    cut_prefix_for_points(std::slice::from_mut(&mut start), column);
    let low = match &start.value {
        Datum::String(text) => text.bytes().to_vec(),
        Datum::Bytes(bytes) => bytes.clone(),
        _ => Vec::new(),
    };
    // Go derives the upper bound in the COLLATED space, not the raw one:
    // `sortKeyWithoutTrim` is `KeyWithoutTrimRightSpace(low)` and the bound is
    // that byte string with its last non-`0xff` byte incremented
    // (`points.go:844-859`). Incrementing the RAW bytes instead and letting
    // the key codec collate afterwards is a different number, because a
    // collator is not monotone byte-by-byte: `LIKE 'abé%'` over
    // `utf8mb4_general_ci` raw-increments to `'abê'`, whose weight string is
    // the SAME as `'abé'`'s, so the range collapses to empty and the matching
    // rows go missing. Captured from real TiDB:
    //
    // ```text
    // explain select a from ci where a like 'abé%'
    //   IndexRangeScan  range:["\x00A\x00B\x00E","\x00A\x00B\x00F")
    // select a from ci where a like 'abé%'   -> abéxx
    // ```
    //
    // For a `_bin` or `binary` collation the weight string IS the raw value,
    // so this is byte-for-byte what the raw increment produced before.
    let high = increment_sort_key(&low, collation);
    // The lower bound moves into the same space, as Go's `startPoint` does.
    // Its ENCODED key is unchanged either way -- the codec collates a text
    // endpoint on its way into the key -- but the two bounds of one range have
    // to be comparable to each other, and this is the range whose printed text
    // Go shows as a weight string.
    //
    // Go trims the trailing spaces here, and only here, for a PAD SPACE
    // collation (`shouldTrimTrailingSpace`): the stored index key has them
    // trimmed, so `LIKE 'abc  %'` must start at `'abc'` or it steps past the
    // `'abc  '` entry. The upper bound above deliberately keeps them, because
    // it is the bound of the UNTRIMMED prefix.
    start.value = Datum::Bytes(if is_pad_space(collation) {
        collation.key(&low)
    } else {
        collation.key_without_trim_right_space(&low)
    });
    // Go's case 4-2, the one arm that cuts and converts its own points: the
    // two bounds take DIFFERENT conversions (the start trims trailing spaces
    // under a PAD SPACE collation, the bound it is derived from does not), so
    // the shared tail must not touch them again.
    (vec![start, Point::end(high, true)], true)
}

/// Go `collate.IsPadSpaceCollation`, over the [`tidb_datatype::Collation`]
/// this tier carries. Stated as Go's three-name EXCEPTION list, so a collation
/// added later defaults to PAD SPACE -- the half that reads too much rather
/// than too little.
fn is_pad_space(collation: tidb_datatype::Collation) -> bool {
    !matches!(
        collation,
        tidb_datatype::Collation::Binary
            | tidb_datatype::Collation::Utf8Mb40900AiCi
            | tidb_datatype::Collation::Utf8Mb40900Bin
    )
}

/// Go's upper bound for a `LIKE` prefix (`newBuildFromPatternLike` case 4-2):
/// the low value's sort key WITHOUT the PAD SPACE trim, with its last
/// non-`0xff` byte incremented and every trailing `0xff` zeroed.
///
/// The bound keeps the sort key's LENGTH, which is what makes it the smallest
/// weight string above every weight string that starts with this prefix. A
/// value whose sort key is all `0xff` has nothing above it inside that length,
/// and Go answers `MaxValue` there rather than lengthening the key.
///
/// The result is a `Datum::Bytes` because it is a weight string, not text:
/// the key codec must write it as-is rather than collate it a second time,
/// which is the same reason Go's `endPoint.value.SetBytes` leaves `KindBytes`.
fn increment_sort_key(low: &[u8], collation: tidb_datatype::Collation) -> Datum {
    let mut key = collation.key_without_trim_right_space(low);
    for i in (0..key.len()).rev() {
        key[i] = key[i].wrapping_add(1);
        if key[i] != 0 {
            return Datum::Bytes(key);
        }
    }
    Datum::MaxValue
}

#[cfg(test)]
mod tests {
    use super::super::tests::derive_typed;
    use tidb_datatype::FieldType;

    /// The corpus table's string column, `s varchar(255)`, with one collation
    /// swapped in -- which is the whole variable these rows measure.
    fn column(collation: &str) -> FieldType {
        let mut field_type = FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        field_type.set_flen(255);
        field_type.set_charset_name(if collation == "binary" {
            "binary"
        } else {
            "utf8mb4"
        });
        field_type.set_collation_name(collation);
        field_type
    }

    /// A `LIKE`'s bounds are WEIGHT STRINGS, and the corpus that says so.
    ///
    /// Go derives the upper bound by incrementing the low value's SORT KEY
    /// (`points.go`'s `sortKeyWithoutTrim`), not its raw bytes. A collator is
    /// not monotone byte-by-byte, so the two are different numbers -- and on a
    /// case-insensitive collation the raw increment can land BELOW the lower
    /// bound, which empties the range and silently loses every matching row.
    ///
    /// Every `range:` cell here was captured from real TiDB over
    ///
    /// ```sql
    /// create table ci (a varchar(50) collate utf8mb4_general_ci, key idx(a));
    /// create table bn (a varchar(50) collate utf8mb4_bin, key idx(a));
    /// create table b0 (a varbinary(50), key idx(a));
    /// ```
    ///
    /// The `_bin` and `binary` rows are the CONTROL: there the weight string
    /// is the raw value, so they pin the bytes this derivation already
    /// produced and would catch a "sort key" that quietly changed them.
    const GO_LIKE_SORT_KEY_RANGE: &[(&str, &str, &str)] = &[
        // `é` and `ê` share one weight, so the raw increment `'abê'` collates
        // to exactly the lower bound and the range comes out EMPTY.
        (
            "utf8mb4_general_ci",
            "a like 'abé%'",
            "[\"\\x00A\\x00B\\x00E\",\"\\x00A\\x00B\\x00F\")",
        ),
        // The same failure without a multi-byte character: the byte after
        // '`' is 'a', which folds DOWN to the weight of 'A'.
        (
            "utf8mb4_general_ci",
            "a like 'ab`%'",
            "[\"\\x00A\\x00B\\x00`\",\"\\x00A\\x00B\\x00a\")",
        ),
        // The other direction, where the raw increment merely read too wide:
        // 'z'+1 is '{' raw, but the weight after 'Z' is '['.
        (
            "utf8mb4_general_ci",
            "a like 'abz%'",
            "[\"\\x00A\\x00B\\x00Z\",\"\\x00A\\x00B\\x00[\")",
        ),
        // A `_` under a PAD SPACE collation keeps its lower bound INCLUSIVE.
        (
            "utf8mb4_general_ci",
            "a like 'ab_%'",
            "[\"\\x00A\\x00B\",\"\\x00A\\x00C\")",
        ),
        // PAD SPACE, trailing spaces: the LOW bound is trimmed because the
        // stored key is, and the HIGH bound is not because it bounds the
        // untrimmed prefix. Starting at `"abc  "` would step past the `'abc '`
        // entries entirely.
        ("utf8mb4_bin", "a like 'abc  %'", "[\"abc\",\"abc !\")"),
        // `binary` is not a PAD SPACE collation, so nothing is trimmed.
        ("binary", "a like 'abc  %'", "[\"abc  \",\"abc !\")"),
        // The control: a `_bin` collation's weight string is its raw value.
        ("utf8mb4_bin", "a like 'abc%'", "[\"abc\",\"abd\")"),
        ("binary", "a like 'abc%'", "[\"abc\",\"abd\")"),
    ];

    #[test]
    fn a_like_bound_is_the_incremented_sort_key_not_the_incremented_bytes() {
        let mut mismatches = Vec::new();
        for (collation, where_sql, expected) in GO_LIKE_SORT_KEY_RANGE {
            let got = derive_typed(&[("a", column(collation))], where_sql);
            if got != *expected {
                mismatches.push(format!(
                    "  {collation:<20} {where_sql:<20} want={expected:<40} got={got}"
                ));
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} of {} Go rows diverge:\n{}",
            mismatches.len(),
            GO_LIKE_SORT_KEY_RANGE.len(),
            mismatches.join("\n")
        );
    }
}
