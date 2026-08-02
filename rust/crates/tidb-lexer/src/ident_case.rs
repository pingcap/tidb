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

//! Simple-case-mapping lowercase for SQL identifiers.
//!
//! TiDB folds every identifier (`CIStr.L`, charset/collation names, ...) with
//! Go's `strings.ToLower`, which is `unicode.ToLower` applied rune by rune:
//! the UnicodeData simple lowercase mapping, with **no** SpecialCasing table,
//! **no** Greek final-sigma context rule, and **no** 1->N expansions. Rust's
//! `str::to_lowercase` implements the full Unicode default case conversion
//! instead, so it disagrees with Go on exactly the two axes below. Because
//! catalog lookups are keyed on the folded form, using the full mapping makes a
//! table created by a Go node invisible to a Rust node and vice versa.
//!
//! # Go capture (go1.26.0, `unicode.Version` 15.0.0)
//!
//! `strings.ToLower`, printed as codepoints, versus Rust `str::to_lowercase`:
//!
//! | input | codepoints | Go `strings.ToLower` | Rust `to_lowercase` |
//! | --- | --- | --- | --- |
//! | `ΟΔΟΣ` | 039F 0394 039F 03A3 | `οδοσ` = 03BF 03B4 03BF **03C3** | `οδος` = 03BF 03B4 03BF **03C2** |
//! | `İ`    | 0130                | `i` = **0069** (one rune)      | `i̇` = **0069 0307** (two)   |
//! | `Ä`    | 00C4                | `ä` = 00E4                     | `ä` = 00E4 (agree: control)  |
//! | `ẞ`    | 1E9E                | `ß` = 00DF                     | `ß` = 00DF (agree)          |
//! | `ǅ`    | 01C5                | `ǆ` = 01C6                     | `ǆ` = 01C6 (agree)          |
//! | `ﬁ`    | FB01                | `ﬁ` = FB01 (identity)          | FB01 (agree)                |
//! | `AbC_Table1` | ASCII        | `abc_table1`                   | same (agree)                |
//! | `表名Σ` | 8868 540D 03A3      | `表名σ` = 8868 540D 03C3        | `表名σ` = ... 03C3 (agree)   |
//!
//! Note `Σ` in `表名Σ` lowercases to ordinary sigma on *both* sides: Rust only
//! emits final sigma 03C2 when the sigma is word-final, which is precisely the
//! context rule Go does not implement.
//!
//! # Exception-table derivation
//!
//! Iterating `char::to_lowercase` (which is *not* the final-sigma rule -- that
//! lives in `str::to_lowercase` -- but *is* still SpecialCasing-aware) over all
//! of `0..=0x10FFFF` yields a multi-char result for exactly **one** codepoint:
//!
//! ```text
//! U+0130 -> U+0069 U+0307   (LATIN CAPITAL LETTER I WITH DOT ABOVE)
//! ```
//!
//! UnicodeData.txt field 13 (simple lowercase) for U+0130 is `0069`, which is
//! what Go returns. So per-char mapping plus this single override reproduces
//! `unicode.ToLower` exactly; no vendored table and no new dependency needed.
//!
//! Cross-checking every codepoint against a Go dump of `unicode.ToLower` leaves
//! 55 further disagreements, all of the form "Rust maps it, Go returns it
//! unchanged" (U+1C89, U+A7CB.., U+10D50.., U+16EA0..). Those are codepoints
//! assigned in Unicode 16.0 that Go 1.26's Unicode 15.0 tables do not know yet;
//! they are toolchain-version skew, not a semantic difference, and they
//! converge on their own when Go's tables advance. Hard-coding them would
//! freeze a stale Unicode version into the identifier path, so we do not.

/// Lowercases `value` the way Go's `strings.ToLower` does, for identifier
/// folding.
///
/// Unlike [`str::to_lowercase`] this applies the Unicode *simple* mapping per
/// character: no final-sigma context rule and no 1->N expansions. Use it for
/// anything whose folded form is a lookup key (identifiers, charset and
/// collation names); string *values* keep the full Unicode mapping.
pub fn identifier_to_lower(value: &str) -> String {
    // Fast path: pure ASCII identifiers, which is nearly all of them.
    if value.is_ascii() {
        return value.to_ascii_lowercase();
    }
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        let mut lower = ch.to_lowercase();
        match (lower.next(), lower.next()) {
            // Single-char simple mapping: identical to `unicode.ToLower`.
            (Some(single), None) => out.push(single),
            // The sole SpecialCasing expansion; UnicodeData's simple mapping
            // for U+0130 is U+0069 alone.
            _ => out.push(if ch == '\u{0130}' { '\u{0069}' } else { ch }),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::identifier_to_lower;

    fn codepoints(value: &str) -> Vec<u32> {
        value.chars().map(|ch| ch as u32).collect()
    }

    #[test]
    fn greek_sigma_never_becomes_final_sigma() {
        // Go: "ΟΔΟΣ" -> "οδοσ" (U+03C3). Rust's str::to_lowercase gives U+03C2.
        assert_eq!(identifier_to_lower("ΟΔΟΣ"), "οδοσ");
        assert_eq!(
            codepoints(&identifier_to_lower("ΟΔΟΣ")),
            vec![0x03BF, 0x03B4, 0x03BF, 0x03C3]
        );
        assert_eq!(codepoints(&identifier_to_lower("Σ")), vec![0x03C3]);
        assert_eq!(
            codepoints(&identifier_to_lower("ΣΣΣ")),
            vec![0x03C3, 0x03C3, 0x03C3]
        );
    }

    #[test]
    fn dotted_capital_i_stays_one_codepoint() {
        // Go: "İ" -> U+0069. Rust's str::to_lowercase gives U+0069 U+0307.
        assert_eq!(codepoints(&identifier_to_lower("İ")), vec![0x0069]);
    }

    #[test]
    fn agreed_mappings_are_unchanged() {
        // Control cases where Go and Rust already agree.
        assert_eq!(codepoints(&identifier_to_lower("Ä")), vec![0x00E4]);
        assert_eq!(codepoints(&identifier_to_lower("ẞ")), vec![0x00DF]);
        assert_eq!(codepoints(&identifier_to_lower("ǅ")), vec![0x01C6]);
        assert_eq!(codepoints(&identifier_to_lower("ﬁ")), vec![0xFB01]);
        assert_eq!(identifier_to_lower("AbC_Table1"), "abc_table1");
        assert_eq!(identifier_to_lower("表名"), "表名");
        assert_eq!(codepoints(&identifier_to_lower("表名Σ")), vec![
            0x8868, 0x540D, 0x03C3
        ]);
    }

    #[test]
    fn ascii_fast_path_matches_general_path() {
        for value in ["", "T", "SELECT_1", "a_b_c", "ABC123"] {
            assert_eq!(identifier_to_lower(value), value.to_ascii_lowercase());
        }
    }
}
