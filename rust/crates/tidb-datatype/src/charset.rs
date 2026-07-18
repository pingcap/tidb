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

use std::fmt;

/// A character set registered by TiDB's charset package.
///
/// The registry is intentionally explicit while the scalar domain is being
/// ported. Additions must come with their row from
/// `pkg/parser/charset/charset.go`; this is not a free-form session string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Charset {
    /// MySQL's byte-oriented pseudo-character-set.
    Binary,
    /// MySQL's seven-bit ASCII character set.
    Ascii,
    /// MySQL's single-byte Latin-1 character set.
    Latin1,
    /// MySQL's legacy three-byte UTF-8 character set.
    Utf8,
    /// MySQL's four-byte UTF-8 character set.
    Utf8Mb4,
}

impl Charset {
    /// Returns the canonical TiDB registry name.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Binary => "binary",
            Self::Ascii => "ascii",
            Self::Latin1 => "latin1",
            Self::Utf8 => "utf8",
            Self::Utf8Mb4 => "utf8mb4",
        }
    }

    /// Resolves a Go charset name, including the legacy `utf8mb3` alias.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "binary" => Some(Self::Binary),
            "ascii" => Some(Self::Ascii),
            "latin1" => Some(Self::Latin1),
            "utf8" | "utf8mb3" => Some(Self::Utf8),
            "utf8mb4" => Some(Self::Utf8Mb4),
            _ => None,
        }
    }

    /// Returns the registry default rather than duplicating the relationship
    /// in callers.
    pub fn default_collation(self) -> Collation {
        COLLATIONS
            .iter()
            .find(|entry| entry.charset == self && entry.is_default)
            .expect("every registered charset has one default collation")
            .collation
    }
}

impl fmt::Display for Charset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A collation registered by TiDB's charset package.
///
/// Keeping this as an enum makes an unregistered collation unrepresentable.
/// Its character set is always derived through the crate-owned registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Collation {
    /// The default collation for [`Charset::Binary`].
    Binary,
    /// The default collation for [`Charset::Ascii`].
    AsciiBin,
    /// The default collation for [`Charset::Latin1`].
    Latin1Bin,
    /// TiDB's default collation for [`Charset::Utf8`].
    Utf8Bin,
    /// MySQL's legacy UTF-8 general case-insensitive collation.
    Utf8GeneralCi,
    /// MySQL's UCA 4.0 UTF-8 Unicode collation.
    Utf8UnicodeCi,
    /// TiDB's default collation for [`Charset::Utf8Mb4`].
    Utf8Mb4Bin,
    /// The utf8mb4 alias of [`Self::Utf8GeneralCi`].
    Utf8Mb4GeneralCi,
    /// The utf8mb4 alias of [`Self::Utf8UnicodeCi`].
    Utf8Mb4UnicodeCi,
}

impl Collation {
    /// TiDB's default collation (`mysql.DefaultCollationName`).
    pub const DEFAULT: Self = Self::Utf8Mb4Bin;

    fn registry_entry(self) -> &'static CollationEntry {
        COLLATIONS
            .iter()
            .find(|entry| entry.collation == self)
            .expect("every Collation variant is registered")
    }

    /// Returns the canonical TiDB registry name.
    pub fn name(self) -> &'static str {
        self.registry_entry().name
    }

    /// Returns the character set registered for this collation.
    pub fn charset(self) -> Charset {
        self.registry_entry().charset
    }

    /// Mirrors `charset.GetCollationByName`: registered names are matched
    /// without ASCII case sensitivity and unknown names are rejected.
    pub fn from_name(name: &str) -> Option<Self> {
        let lower = name.to_ascii_lowercase();
        let name = match lower.as_str() {
            "utf8mb3_bin" => "utf8_bin",
            "utf8mb3_general_ci" => "utf8_general_ci",
            "utf8mb3_unicode_ci" => "utf8_unicode_ci",
            other => other,
        };
        COLLATIONS
            .iter()
            .find(|entry| entry.name.eq_ignore_ascii_case(name))
            .map(|entry| entry.collation)
    }
}

impl fmt::Display for Collation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Debug)]
struct CollationEntry {
    collation: Collation,
    name: &'static str,
    charset: Charset,
    is_default: bool,
}

/// Source: `pkg/parser/charset/charset.go` (`CharacterSetInfos`, default
/// collation constants, and the binary/bin/general-CI/Unicode-CI registry rows
/// for utf8 and utf8mb4).
const COLLATIONS: &[CollationEntry] = &[
    CollationEntry {
        collation: Collation::Utf8Bin,
        name: "utf8_bin",
        charset: Charset::Utf8,
        is_default: true,
    },
    CollationEntry {
        collation: Collation::Utf8GeneralCi,
        name: "utf8_general_ci",
        charset: Charset::Utf8,
        is_default: false,
    },
    CollationEntry {
        collation: Collation::Utf8UnicodeCi,
        name: "utf8_unicode_ci",
        charset: Charset::Utf8,
        is_default: false,
    },
    CollationEntry {
        collation: Collation::Utf8Mb4Bin,
        name: "utf8mb4_bin",
        charset: Charset::Utf8Mb4,
        is_default: true,
    },
    CollationEntry {
        collation: Collation::Utf8Mb4GeneralCi,
        name: "utf8mb4_general_ci",
        charset: Charset::Utf8Mb4,
        is_default: false,
    },
    CollationEntry {
        collation: Collation::Utf8Mb4UnicodeCi,
        name: "utf8mb4_unicode_ci",
        charset: Charset::Utf8Mb4,
        is_default: false,
    },
    CollationEntry {
        collation: Collation::Binary,
        name: "binary",
        charset: Charset::Binary,
        is_default: true,
    },
    CollationEntry {
        collation: Collation::AsciiBin,
        name: "ascii_bin",
        charset: Charset::Ascii,
        is_default: true,
    },
    CollationEntry {
        collation: Collation::Latin1Bin,
        name: "latin1_bin",
        charset: Charset::Latin1,
        is_default: true,
    },
];

#[cfg(test)]
mod tests {
    use super::{Charset, Collation};

    /// Vectors from `pkg/parser/charset/charset_test.go`:
    /// `TestGetDefaultCollation` and `TestGetCollationByName`.
    #[test]
    fn go_registry_vectors_preserve_collation_charset_relation() {
        assert_eq!(Charset::Utf8Mb4.default_collation(), Collation::Utf8Mb4Bin);
        assert_eq!(Charset::Utf8.default_collation(), Collation::Utf8Bin);
        assert_eq!(Charset::Binary.default_collation(), Collation::Binary);
        assert_eq!(Charset::Ascii.default_collation(), Collation::AsciiBin);
        assert_eq!(Charset::Latin1.default_collation(), Collation::Latin1Bin);
        assert_eq!(Collation::Utf8Mb4Bin.charset(), Charset::Utf8Mb4);
        assert_eq!(Collation::Binary.charset(), Charset::Binary);
        assert_eq!(Collation::AsciiBin.charset(), Charset::Ascii);
        assert_eq!(Collation::Latin1Bin.charset(), Charset::Latin1);
        assert_eq!(Collation::Utf8GeneralCi.charset(), Charset::Utf8);
        assert_eq!(Collation::Utf8Mb4UnicodeCi.charset(), Charset::Utf8Mb4);
        assert_eq!(
            Collation::from_name("UTF8MB4_BIN"),
            Some(Collation::Utf8Mb4Bin)
        );
        assert_eq!(Collation::from_name("non_exist"), None);
        assert_eq!(
            Collation::from_name("UTF8_UNICODE_CI"),
            Some(Collation::Utf8UnicodeCi)
        );
        assert_eq!(
            Collation::from_name("utf8mb3_unicode_ci"),
            Some(Collation::Utf8UnicodeCi)
        );
    }

    fn valid_charset_and_collation(charset: &str, collation: &str) -> bool {
        let charset = if charset.is_empty() {
            Charset::Utf8
        } else if let Some(charset) = Charset::from_name(charset) {
            charset
        } else {
            return false;
        };
        collation.is_empty()
            || Collation::from_name(collation)
                .is_some_and(|collation| collation.charset() == charset)
    }

    #[test]
    fn go_valid_charset_vectors() {
        for (charset, collation, expected) in [
            ("utf8", "utf8_general_ci", true),
            ("", "utf8_general_ci", true),
            ("utf8mb4", "utf8mb4_bin", true),
            ("latin1", "latin1_bin", true),
            ("utf8", "utf8_invalid_ci", false),
            ("utf16", "utf16_bin", false),
            ("gb2312", "gb2312_chinese_ci", false),
            ("UTF8", "UTF8_BIN", true),
            ("UTF8MB4", "UTF8MB4_general_ci", true),
            ("utf8mb3", "utf8mb3_unicode_ci", true),
        ] {
            assert_eq!(
                valid_charset_and_collation(charset, collation),
                expected,
                "{charset}/{collation}"
            );
        }
    }

    #[test]
    fn go_charset_description_and_legacy_defaults() {
        for (input, canonical) in [
            ("utf8", "utf8"),
            ("UTF8", "utf8"),
            ("utf8mb3", "utf8"),
            ("utf8mb4", "utf8mb4"),
            ("ascii", "ascii"),
            ("binary", "binary"),
            ("latin1", "latin1"),
        ] {
            assert_eq!(
                Charset::from_name(input).map(Charset::name),
                Some(canonical)
            );
        }
        assert_eq!(Charset::from_name("invalid_cs"), None);
        assert_eq!(Charset::from_name(""), None);
        assert_eq!(
            Charset::from_name("utf8mb3").map(Charset::default_collation),
            Some(Collation::Utf8Bin)
        );
    }
}
