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

//! The whole case table of `TestCompareString`
//! (`pkg/expression/collation_test.go:809`).
//!
//! Go asserts `types.CompareString(a, b, collation)` -- equal or not equal --
//! for four collations across the cases that separate them: ASCII case folding,
//! accent folding, the astral plane (where `utf8_general_ci` maps everything
//! outside the BMP to one weight but `utf8mb4_0900_ai_ci` does not), PAD SPACE
//! versus NO PAD, and the German sharp s and the ae ligature, whose expansion
//! to two letters is exactly what separates `general_ci` from `unicode_ci`.
//! `Collator::compare` is the same seam here.
//!
//! Every row is a load-bearing distinction: getting one wrong silently changes
//! which rows a `WHERE name = ?` matches and how `ORDER BY name` orders them.

use std::cmp::Ordering;
use tidb_datatype::get_collator_with_mode;

/// `get_collator_with_mode(true, ..)` rather than `get_collator`, which reads
/// the process-wide new-collation switch: Go's suite has new collations enabled,
/// and naming the mode keeps these rows independent of what another test in the
/// same process last stored in that global.
fn compare(left: &str, right: &str, collation: &str) -> Ordering {
    get_collator_with_mode(true, collation).compare(left.as_bytes(), right.as_bytes())
}

fn assert_eq_under(collation: &str, pairs: &[(&str, &str)]) {
    for (left, right) in pairs {
        assert_eq!(
            compare(left, right, collation),
            Ordering::Equal,
            "{left:?} vs {right:?} under {collation} must compare equal"
        );
    }
}

fn assert_ne_under(collation: &str, pairs: &[(&str, &str)]) {
    for (left, right) in pairs {
        assert_ne!(
            compare(left, right, collation),
            Ordering::Equal,
            "{left:?} vs {right:?} under {collation} must NOT compare equal"
        );
    }
}

/// `utf8_general_ci`: case- and accent-insensitive, PAD SPACE, one weight for
/// every astral character, and no expansion -- `ß` equals `s`, not `ss`.
#[test]
fn compare_string_source_utf8_general_ci() {
    assert_eq_under(
        "utf8_general_ci",
        &[
            ("a", "A"),
            ("À", "A"),
            ("😜", "😃"),
            ("a ", "a  "),
            ("ß", "s"),
        ],
    );
    assert_ne_under("utf8_general_ci", &[("ß", "ss")]);
}

/// `utf8_unicode_ci`: the UCA table, so `ß` EXPANDS to `ss` -- the opposite of
/// `general_ci` on both sharp-s rows.
#[test]
fn compare_string_source_utf8_unicode_ci() {
    assert_eq_under(
        "utf8_unicode_ci",
        &[
            ("a", "A"),
            ("À", "A"),
            ("😜", "😃"),
            ("a ", "a  "),
            ("ß", "ss"),
        ],
    );
    assert_ne_under("utf8_unicode_ci", &[("ß", "s")]);
}

/// `utf8mb4_0900_ai_ci`: accent-insensitive like the others, but NO PAD (so a
/// trailing space counts) and real astral weights (so two different emoji
/// differ), and it expands both `ß` and `æ`.
#[test]
fn compare_string_source_utf8mb4_0900_ai_ci() {
    assert_eq_under(
        "utf8mb4_0900_ai_ci",
        &[("a", "A"), ("À", "A"), ("ß", "ss"), ("æ", "ae")],
    );
    assert_ne_under(
        "utf8mb4_0900_ai_ci",
        &[
            ("😜", "😃"),
            ("a ", "a  "),
            ("ß", "s"),
            ("\u{FFFFE}", "\u{FFFFF}"),
        ],
    );
}

/// `binary`: byte identity, so every distinction above is preserved, including
/// the trailing space.
#[test]
fn compare_string_source_binary() {
    assert_ne_under(
        "binary",
        &[("a", "A"), ("À", "A"), ("😜", "😃"), ("a ", "a  ")],
    );
}

/// The tail of `TestCompareString` (`:840`), which builds a two-column chunk of
/// the four `utf8_general_ci`-equal pairs and calls
/// `CompareStringWithCollationInfo` over it. That helper is the column-vs-column
/// form of the same comparison, and Go's loop reads row 0 four times -- so the
/// property it actually asserts is that the collation-aware comparison of two
/// STRING COLUMNS agrees with the scalar one. Ported through the operator seam
/// two string columns reach here.
#[test]
fn compare_string_source_column_pairs_agree_with_scalar() {
    for (left, right) in [("a", "A"), ("À", "A"), ("😜", "😃"), ("a ", "a  ")] {
        assert_eq!(
            crate::ops::eval_binary_full(
                tidb_ast::BinaryOp::Eq,
                crate::Datum::new_string(left),
                crate::Datum::new_string(right),
                4,
                tidb_datatype::Collation::Utf8GeneralCi,
                crate::ops::Operands::LITERALS,
                &crate::context::NoColumns,
            ),
            Ok(crate::Datum::Int(1)),
            "{left:?} = {right:?} under utf8_general_ci"
        );
    }
}
