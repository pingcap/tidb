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
// See the License for the specific language governing permissions and
// limitations under the License.

//! The binary-vs-UTF-8 signature pairs of `pkg/expression/builtin_string.go`,
//! checked against answers captured from real TiDB.
//!
//! Every case uses `aéb`: FOUR bytes and THREE characters, so a byte answer
//! and a character answer can never coincide — the trap a CJK-only fixture
//! (three bytes per character) silently passes with the wrong signature.
//!
//! Captured with `go run ./rust/difftests/gorun` on this tree, e.g.
//!
//! ```text
//! select substring('aéb', 2, 2), substring(cast('aéb' as binary), 2, 2);
//! RS:éb|é
//! select char_length('aéb'), char_length(cast('aéb' as binary));
//! RS:3|4
//! select reverse('aéb'), hex(reverse(cast('aéb' as binary)));
//! RS:béa|62A9C361
//! ```

use super::e;

/// One captured `(expression, TiDB answer)` pair per signature, in both the
/// character and the binary spelling of the same call.
#[track_caller]
fn captured(cases: &[(&str, &str)]) {
    for (expression, expected) in cases {
        assert_eq!(&e(expression), expected, "{expression}");
    }
}

#[test]
fn substring_selects_bytes_for_a_binary_argument() {
    captured(&[
        // builtinSubstring2ArgsUTF8Sig / builtinSubstring2ArgsSig
        ("hex(substring('aéb', 2))", "STR:C3A962"),
        ("hex(substring(cast('aéb' as binary), 2))", "STR:C3A962"),
        ("hex(substring('aéb', -2))", "STR:C3A962"),
        ("hex(substring(cast('aéb' as binary), -2))", "STR:A962"),
        // builtinSubstring3ArgsUTF8Sig / builtinSubstring3ArgsSig
        ("hex(substring('aéb', 2, 2))", "STR:C3A962"),
        ("hex(substring(cast('aéb' as binary), 2, 2))", "STR:C3A9"),
        ("hex(substr('aéb', 1, 2))", "STR:61C3A9"),
        ("hex(substr(cast('aéb' as binary), 1, 2))", "STR:61C3"),
        // Out-of-range and zero positions are the empty string either way.
        ("hex(substring(cast('aéb' as binary), 0, 2))", "STR:"),
        ("hex(substring(cast('aéb' as binary), 9, 2))", "STR:"),
        ("hex(substring(cast('aéb' as binary), 2, 0))", "STR:"),
        ("hex(substring(cast('aéb' as binary), 2, -1))", "STR:"),
    ]);
}

#[test]
fn left_right_and_char_length_select_bytes_for_a_binary_argument() {
    captured(&[
        ("hex(left('aéb', 2))", "STR:61C3A9"),
        ("hex(left(cast('aéb' as binary), 2))", "STR:61C3"),
        ("hex(right('aéb', 2))", "STR:C3A962"),
        ("hex(right(cast('aéb' as binary), 2))", "STR:A962"),
        ("char_length('aéb')", "INT:3"),
        ("char_length(cast('aéb' as binary))", "INT:4"),
        // LENGTH has ONE Go signature and counts bytes for both.
        ("length('aéb')", "INT:4"),
        ("length(cast('aéb' as binary))", "INT:4"),
    ]);
}

#[test]
fn reverse_selects_byte_order_for_a_binary_argument() {
    captured(&[
        // builtinReverseUTF8Sig reverses runes, keeping `é` intact.
        ("hex(reverse('aéb'))", "STR:62C3A961"),
        // builtinReverseSig reverses BYTES: `C3 A9` comes back as `A9 C3`.
        ("hex(reverse(cast('aéb' as binary)))", "STR:62A9C361"),
    ]);
}

#[test]
fn insert_selects_bytes_when_either_string_argument_is_binary() {
    captured(&[
        ("hex(insert('aébcd', 2, 2, 'X'))", "STR:61586364"),
        (
            "hex(insert(cast('aébcd' as binary), 2, 2, 'X'))",
            "STR:6158626364",
        ),
        (
            "hex(insert('aébcd', 2, 2, cast('X' as binary)))",
            "STR:6158626364",
        ),
        // pos out of range returns the source unchanged, in either signature.
        (
            "hex(insert(cast('aébcd' as binary), 0, 2, 'X'))",
            "STR:61C3A9626364",
        ),
    ]);
}

#[test]
fn locate_and_instr_report_byte_offsets_for_a_binary_argument() {
    captured(&[
        ("instr('aéb', 'b')", "INT:3"),
        ("instr(cast('aéb' as binary), 'b')", "INT:4"),
        ("locate('b', 'aéb')", "INT:3"),
        ("locate(cast('b' as binary), 'aéb')", "INT:4"),
        ("locate('é', 'aéb')", "INT:2"),
        (
            "locate(cast('é' as binary), cast('aéb' as binary))",
            "INT:2",
        ),
        // An empty needle matches at 1 and a missing one is 0 either way.
        ("locate(cast('' as binary), 'aéb')", "INT:1"),
        ("locate(cast('z' as binary), 'aéb')", "INT:0"),
    ]);
}

/// The three-argument pair, `builtinLocate3ArgsSig` /
/// `builtinLocate3ArgsUTF8Sig`: `pos` itself is counted in the signature's
/// units, so a binary search may start INSIDE a multi-byte character.
#[test]
fn locate_with_a_start_position_counts_pos_in_the_same_units() {
    captured(&[
        ("locate('b', 'aéb', 1)", "INT:3"),
        ("locate(cast('b' as binary), 'aéb', 1)", "INT:4"),
        ("locate('b', 'aéb', 3)", "INT:3"),
        ("locate(cast('b' as binary), 'aéb', 3)", "INT:4"),
        ("locate(cast('b' as binary), 'aéb', 4)", "INT:4"),
        ("locate(cast('b' as binary), 'aéb', 5)", "INT:0"),
        ("locate('é', 'aébé', 3)", "INT:4"),
        ("locate(cast('é' as binary), 'aébé', 3)", "INT:5"),
        // An empty needle answers `pos` itself; a missing one and an
        // out-of-range `pos` are both 0.
        ("locate(cast('' as binary), 'aéb', 2)", "INT:2"),
        ("locate('', 'aéb', 2)", "INT:2"),
        ("locate(cast('z' as binary), 'aéb', 1)", "INT:0"),
        ("locate('b', 'aéb', 0)", "INT:0"),
    ]);
}

#[test]
fn utf8_and_case_insensitive_signatures_are_untouched() {
    // The seam must not leak into the character signatures: these are the
    // same answers TiDB gives, with no binary argument anywhere.
    captured(&[
        ("hex(substring('中文测试', 2, 2))", "STR:E69687E6B58B"),
        ("hex(left('中文测试', 2))", "STR:E4B8ADE69687"),
        ("hex(reverse('中文测试'))", "STR:E8AF95E6B58BE69687E4B8AD"),
        ("char_length('中文测试')", "INT:4"),
    ]);
}

/// The folding collations still reach `builtinLocate2ArgsUTF8Sig`'s collator
/// path, and only a `binary` derivation switches to the byte signature.
///
/// These go through `string_fn::locate` directly because the collation comes
/// from the chunk evaluator's own derivation pass (`ScalarFunction::eval`);
/// the AST helper above has no derivation and always reads `utf8mb4_bin`.
/// Captured from TiDB:
///
/// ```text
/// select instr('ABC' collate utf8mb4_general_ci, 'b'),
///        instr('ABC' collate utf8mb4_bin, 'b'),
///        locate('É' collate utf8mb4_general_ci, 'aéb');
/// RS:2|0|2
/// ```
#[test]
fn only_a_binary_derivation_switches_locate_to_bytes() {
    use crate::string_fn::locate;
    use tidb_datatype::{Collation, Datum};

    let ci = Collation::Utf8Mb4GeneralCi;
    let bin = Collation::Utf8Mb4Bin;
    let needle = Datum::new_string("b".to_string());
    let haystack = Datum::new_string("ABC".to_string());
    assert_eq!(
        locate(&needle, &haystack, ci).unwrap(),
        Datum::Int(2),
        "INSTR folds under utf8mb4_general_ci"
    );
    assert_eq!(
        locate(&needle, &haystack, bin).unwrap(),
        Datum::Int(0),
        "INSTR does not fold under utf8mb4_bin"
    );
    assert_eq!(
        locate(
            &Datum::new_string("É".to_string()),
            &Datum::new_string("aéb".to_string()),
            ci
        )
        .unwrap(),
        Datum::Int(2),
        "a folded multi-byte match still reports a CHARACTER index"
    );
    // The same haystack under a binary derivation reports the BYTE index of
    // the unfolded needle.
    assert_eq!(
        locate(
            &Datum::new_string("b".to_string()),
            &Datum::new_string("aéb".to_string()),
            Collation::Binary
        )
        .unwrap(),
        Datum::Int(4)
    );
}
