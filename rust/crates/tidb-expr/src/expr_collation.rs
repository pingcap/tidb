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

//! Collation-derivation support from `pkg/expression/collation.go`: the
//! `Coercibility` and `Repertoire` enums, the `collationInfo` state embedded in
//! every expression, the `ExprCollation` result, and the self-contained charset
//! helpers.
//!
//! SEED of `pkg/expression` (documented, not the whole file): these are the
//! parts that need no decision on how the `Expression` node hierarchy is
//! represented. DEFERRED to when that hierarchy lands: `deriveCoercibilityFor*`,
//! `deriveCollation`, `inferCollation`, `CheckAndDeriveCollationFromExprs`,
//! `safeConvert`, `fixStringTypeForMaxLength`, and `illegalMixCollationErr` (all
//! take `Expression`/`BuildContext`/`EvalContext`), and `collationInfo.Hash64`
//! (Go uses `pkg/planner/cascades/base.Hasher`, which sits above this crate --
//! porting it here would invert the layering).

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

// Charset / collation name constants, mirroring `pkg/parser/charset`. Kept as
// local literals because `tidb-datatype` exposes these names only as enum arms,
// not string constants.
const CHARSET_UTF8: &str = "utf8";
const CHARSET_UTF8MB4: &str = "utf8mb4";
const CHARSET_GBK: &str = "gbk";
const COLLATION_ASCII: &str = "ascii_bin";
const COLLATION_LATIN1: &str = "latin1_bin";
const COLLATION_UTF8: &str = "utf8_bin";
const COLLATION_UTF8MB4: &str = "utf8mb4_bin";
const COLLATION_GBK_BIN: &str = "gbk_bin";
const COLLATION_UTF8MB4_0900_BIN: &str = "utf8mb4_0900_bin";

/// Go `Coercibility` (an `int32`): how strongly a value's collation is fixed,
/// used by MySQL's collation-aggregation rules. Lower binds more strongly.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Coercibility(pub i32);

impl Coercibility {
    /// From an explicit `COLLATE` clause (Go `CoercibilityExplicit`, the zero value).
    pub const EXPLICIT: Coercibility = Coercibility(0);
    /// From concatenating two strings with different collations (Go `CoercibilityNone`).
    pub const NONE: Coercibility = Coercibility(1);
    /// From a column, routine parameter, local variable, or `cast()` (Go `CoercibilityImplicit`).
    pub const IMPLICIT: Coercibility = Coercibility(2);
    /// From a system constant such as `USER()`/`VERSION()` (Go `CoercibilitySysconst`).
    pub const SYSCONST: Coercibility = Coercibility(3);
    /// From a literal (Go `CoercibilityCoercible`).
    pub const COERCIBLE: Coercibility = Coercibility(4);
    /// From a numeric or temporal value (Go `CoercibilityNumeric`).
    pub const NUMERIC: Coercibility = Coercibility(5);
    /// From `NULL` or an expression derived from `NULL` (Go `CoercibilityIgnorable`).
    pub const IGNORABLE: Coercibility = Coercibility(6);

    /// Go `coerString[c]`: the display name used in collation-mismatch errors.
    /// Returns `None` for out-of-range values (Go would panic on the slice index).
    #[must_use]
    pub fn name(self) -> Option<&'static str> {
        const COER_STRING: [&str; 7] = [
            "EXPLICIT",
            "NONE",
            "IMPLICIT",
            "SYSCONST",
            "COERCIBLE",
            "NUMERIC",
            "IGNORABLE",
        ];
        usize::try_from(self.0)
            .ok()
            .and_then(|i| COER_STRING.get(i).copied())
    }
}

/// Go `Repertoire` (an `int`): the character repertoire of a string value.
/// The `Default` is `Repertoire(0)`, matching Go's zero value (not `ASCII`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Repertoire(pub i32);

impl Repertoire {
    /// Pure ASCII, U+0000..U+007F (Go `ASCII`).
    pub const ASCII: Repertoire = Repertoire(0x01);
    /// Extended characters, U+0080..U+FFFF (Go `EXTENDED` = `ASCII << 1`).
    pub const EXTENDED: Repertoire = Repertoire(0x01 << 1);
    /// `ASCII | EXTENDED` (Go `UNICODE`).
    pub const UNICODE: Repertoire = Repertoire(0x01 | (0x01 << 1));
}

/// Go `ExprCollation`: the derived collation of an expression.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExprCollation {
    /// The coercibility.
    pub coer: Coercibility,
    /// The repertoire.
    pub repe: Repertoire,
    /// The character set name.
    pub charset: String,
    /// The collation name.
    pub collation: String,
}

/// Go `collationInfo`: the collation state embedded in every expression node.
///
/// `coer`/`coer_init` are accessed atomically in Go (`goatomic` on `coer`,
/// `atomic.Bool` for `coer_init`) so the derived coercibility can be filled in
/// lazily under concurrency; this mirrors that with atomics.
#[derive(Debug, Default)]
pub struct CollationInfo {
    coer: AtomicI32,
    coer_init: AtomicBool,
    repertoire: Repertoire,
    charset: String,
    collation: String,
    is_explicit_charset: bool,
}

impl CollationInfo {
    /// Go `HasCoercibility`: whether the coercibility has been initialized.
    #[must_use]
    pub fn has_coercibility(&self) -> bool {
        self.coer_init.load(Ordering::SeqCst)
    }

    /// Go `Coercibility`: the (atomically loaded) coercibility value.
    #[must_use]
    pub fn coercibility(&self) -> Coercibility {
        Coercibility(self.coer.load(Ordering::SeqCst))
    }

    /// Go `SetCoercibility`: stores the coercibility and marks it initialized.
    pub fn set_coercibility(&self, val: Coercibility) {
        self.coer.store(val.0, Ordering::SeqCst);
        self.coer_init.store(true, Ordering::SeqCst);
    }

    /// Go `Repertoire`.
    #[must_use]
    pub fn repertoire(&self) -> Repertoire {
        self.repertoire
    }

    /// Go `SetRepertoire`.
    pub fn set_repertoire(&mut self, r: Repertoire) {
        self.repertoire = r;
    }

    /// Go `SetCharsetAndCollation`.
    pub fn set_charset_and_collation(&mut self, chs: &str, coll: &str) {
        self.charset = chs.to_owned();
        self.collation = coll.to_owned();
    }

    /// Go `CharsetAndCollation`.
    #[must_use]
    pub fn charset_and_collation(&self) -> (&str, &str) {
        (&self.charset, &self.collation)
    }

    /// Go `IsExplicitCharset`.
    #[must_use]
    pub fn is_explicit_charset(&self) -> bool {
        self.is_explicit_charset
    }

    /// Go `SetExplicitCharset`.
    pub fn set_explicit_charset(&mut self, explicit: bool) {
        self.is_explicit_charset = explicit;
    }
}

impl Clone for CollationInfo {
    fn clone(&self) -> Self {
        // Go clones a Column by value copy (`*col`), which copies the atomics'
        // current contents; reproduce that by loading and re-storing.
        CollationInfo {
            coer: AtomicI32::new(self.coer.load(Ordering::SeqCst)),
            coer_init: AtomicBool::new(self.coer_init.load(Ordering::SeqCst)),
            repertoire: self.repertoire,
            charset: self.charset.clone(),
            collation: self.collation.clone(),
            is_explicit_charset: self.is_explicit_charset,
        }
    }
}

impl PartialEq for CollationInfo {
    /// Go `collationInfo.Equals`: field-wise equality (atomics loaded).
    fn eq(&self, other: &Self) -> bool {
        self.coer.load(Ordering::SeqCst) == other.coer.load(Ordering::SeqCst)
            && self.coer_init.load(Ordering::SeqCst) == other.coer_init.load(Ordering::SeqCst)
            && self.repertoire == other.repertoire
            && self.charset == other.charset
            && self.collation == other.collation
            && self.is_explicit_charset == other.is_explicit_charset
    }
}

impl Eq for CollationInfo {}

/// Go `isUnicodeCollation`: whether the charset is a Unicode charset.
#[must_use]
pub fn is_unicode_collation(ch: &str) -> bool {
    ch == CHARSET_UTF8 || ch == CHARSET_UTF8MB4
}

/// Go `isBinCollation`: whether a collation has `_bin` semantics for
/// coercibility derivation.
///
/// This is DIFFERENT from `collate.IsBinCollation` (a storage-level
/// "sortkey == raw data" test); see the Go comment. `gbk_bin` is listed here
/// but is not sortkey-identity, and `binary` is the reverse.
#[must_use]
pub fn is_bin_collation(collate: &str) -> bool {
    collate == COLLATION_ASCII
        || collate == COLLATION_LATIN1
        || collate == COLLATION_UTF8
        || collate == COLLATION_UTF8MB4
        || collate == COLLATION_GBK_BIN
        || collate == COLLATION_UTF8MB4_0900_BIN
}

/// Go `getBinCollation`: the binary collation for a charset.
///
/// Go logs an error and returns `utf8mb4_bin` for an unexpected charset ("never
/// reachable"); the fallback return value is preserved here.
#[must_use]
pub fn get_bin_collation(cs: &str) -> &'static str {
    match cs {
        _ if cs == CHARSET_UTF8 => COLLATION_UTF8,
        _ if cs == CHARSET_UTF8MB4 => COLLATION_UTF8MB4,
        _ if cs == CHARSET_GBK => COLLATION_GBK_BIN,
        // Unreachable in practice; Go logs and returns the utf8mb4 fallback.
        _ => COLLATION_UTF8MB4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repertoire_bit_values() {
        assert_eq!(Repertoire::ASCII, Repertoire(1));
        assert_eq!(Repertoire::EXTENDED, Repertoire(2));
        assert_eq!(Repertoire::UNICODE, Repertoire(3));
        assert_eq!(Repertoire::default(), Repertoire(0));
    }

    #[test]
    fn coercibility_names() {
        assert_eq!(Coercibility::EXPLICIT.name(), Some("EXPLICIT"));
        assert_eq!(Coercibility::IGNORABLE.name(), Some("IGNORABLE"));
        assert_eq!(Coercibility(7).name(), None);
        assert_eq!(Coercibility(-1).name(), None);
        assert!(Coercibility::EXPLICIT < Coercibility::NONE);
    }

    #[test]
    fn collation_info_coercibility_lazy_init() {
        let ci = CollationInfo::default();
        assert!(!ci.has_coercibility());
        assert_eq!(ci.coercibility(), Coercibility::EXPLICIT); // zero value
        ci.set_coercibility(Coercibility::IMPLICIT);
        assert!(ci.has_coercibility());
        assert_eq!(ci.coercibility(), Coercibility::IMPLICIT);
    }

    #[test]
    fn collation_info_clone_and_eq() {
        let mut a = CollationInfo::default();
        a.set_coercibility(Coercibility::NUMERIC);
        a.set_charset_and_collation("utf8mb4", "utf8mb4_bin");
        a.set_repertoire(Repertoire::UNICODE);
        a.set_explicit_charset(true);
        let b = a.clone();
        assert_eq!(a, b);
        assert_eq!(b.charset_and_collation(), ("utf8mb4", "utf8mb4_bin"));
        assert!(b.has_coercibility());

        let mut c = a.clone();
        c.set_repertoire(Repertoire::ASCII);
        assert_ne!(a, c);
    }

    #[test]
    fn charset_helpers() {
        assert!(is_unicode_collation("utf8"));
        assert!(is_unicode_collation("utf8mb4"));
        assert!(!is_unicode_collation("gbk"));

        assert!(is_bin_collation("gbk_bin"));
        assert!(is_bin_collation("utf8mb4_0900_bin"));
        assert!(!is_bin_collation("utf8mb4_general_ci"));

        assert_eq!(get_bin_collation("utf8"), "utf8_bin");
        assert_eq!(get_bin_collation("utf8mb4"), "utf8mb4_bin");
        assert_eq!(get_bin_collation("gbk"), "gbk_bin");
        assert_eq!(get_bin_collation("weird"), "utf8mb4_bin"); // fallback
    }
}
