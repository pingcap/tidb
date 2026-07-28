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

//! Complete registry from `pkg/parser/charset/charset.go`.

use std::collections::HashMap;
use std::fmt;
use std::sync::{OnceLock, RwLock};

#[cfg(test)]
pub(crate) static REGISTRY_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Trailing spaces are insignificant.
pub const PAD_SPACE: &str = "PAD SPACE";
/// Trailing spaces are significant.
pub const PAD_NONE: &str = "NO PAD";

/// A charset fully supported by TiDB's parser charset package.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Charset {
    /// MySQL's byte-oriented pseudo-character-set.
    Binary,
    /// Seven-bit ASCII.
    Ascii,
    /// TiDB's backward-compatible Latin-1 behavior.
    Latin1,
    /// Legacy three-byte UTF-8.
    Utf8,
    /// Four-byte UTF-8.
    Utf8Mb4,
    /// GBK/CP936.
    Gbk,
    /// GB18030-2022.
    Gb18030,
}

impl Charset {
    /// Returns the canonical registry name.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Binary => "binary",
            Self::Ascii => "ascii",
            Self::Latin1 => "latin1",
            Self::Utf8 => "utf8",
            Self::Utf8Mb4 => "utf8mb4",
            Self::Gbk => "gbk",
            Self::Gb18030 => "gb18030",
        }
    }

    /// Resolves a supported name, including the `utf8mb3` alias.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "binary" => Some(Self::Binary),
            "ascii" => Some(Self::Ascii),
            "latin1" => Some(Self::Latin1),
            "utf8" | "utf8mb3" => Some(Self::Utf8),
            "utf8mb4" => Some(Self::Utf8Mb4),
            "gbk" => Some(Self::Gbk),
            "gb18030" => Some(Self::Gb18030),
            _ => None,
        }
    }

    /// Returns the charset's maximum bytes per character, which is what turns
    /// a column's character length into its `CHARACTER_OCTET_LENGTH`.
    pub const fn maxlen(self) -> i64 {
        match self {
            Self::Binary | Self::Ascii | Self::Latin1 => 1,
            Self::Gbk => 2,
            Self::Utf8 => 3,
            Self::Utf8Mb4 | Self::Gb18030 => 4,
        }
    }

    /// Returns the parser package's default collation.
    pub const fn default_collation(self) -> Collation {
        match self {
            Self::Binary => Collation::Binary,
            Self::Ascii => Collation::AsciiBin,
            Self::Latin1 => Collation::Latin1Bin,
            Self::Utf8 => Collation::Utf8Bin,
            Self::Utf8Mb4 => Collation::Utf8Mb4Bin,
            // TiDB forces the Unicode/legacy charsets to their `_bin`
            // collation, but leaves the Chinese ones at the registry default
            // in `pkg/parser/charset/charset.go` (`charsetInfos`). Captured:
            // `CREATE TABLE g1(a VARCHAR(20) CHARSET gbk)` then
            // `SELECT COLLATION(a) FROM g1` answers `gbk_chinese_ci`.
            Self::Gbk => Collation::GbkChineseCi,
            Self::Gb18030 => Collation::Gb18030ChineseCi,
        }
    }
}

impl fmt::Display for Charset {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

/// Collations represented directly in shared typed metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Collation {
    /// Binary collation.
    Binary,
    /// ASCII binary collation.
    AsciiBin,
    /// Latin-1 binary collation.
    Latin1Bin,
    /// UTF-8 binary collation.
    Utf8Bin,
    /// UTF-8 general CI collation.
    Utf8GeneralCi,
    /// UTF-8 Unicode CI collation.
    Utf8UnicodeCi,
    /// UTF8MB4 binary collation.
    Utf8Mb4Bin,
    /// UTF8MB4 general CI collation.
    Utf8Mb4GeneralCi,
    /// UTF8MB4 Unicode CI collation.
    Utf8Mb4UnicodeCi,
    /// UTF8MB4 Unicode 9.0 accent-insensitive/case-insensitive collation.
    Utf8Mb40900AiCi,
    /// UTF8MB4 Unicode 9.0 binary collation.
    Utf8Mb40900Bin,
    /// TiDB's reserved pinyin collation stub.
    Utf8Mb4ZhPinyinTiDbAsCs,
    /// GBK binary collation.
    GbkBin,
    /// GBK Chinese case-insensitive collation.
    GbkChineseCi,
    /// GB18030 binary collation.
    Gb18030Bin,
    /// GB18030 Chinese case-insensitive collation.
    Gb18030ChineseCi,
}

impl Collation {
    /// TiDB's default collation.
    pub const DEFAULT: Self = Self::Utf8Mb4Bin;

    /// Returns the source parser registry ID.
    pub const fn id(self) -> i32 {
        match self {
            Self::Binary => 63,
            Self::AsciiBin => 65,
            Self::Latin1Bin => 47,
            Self::Utf8Bin => 83,
            Self::Utf8GeneralCi => 33,
            Self::Utf8UnicodeCi => 192,
            Self::Utf8Mb4Bin => 46,
            Self::Utf8Mb4GeneralCi => 45,
            Self::Utf8Mb4UnicodeCi => 224,
            Self::Utf8Mb40900AiCi => 255,
            Self::Utf8Mb40900Bin => 309,
            Self::Utf8Mb4ZhPinyinTiDbAsCs => 2048,
            Self::GbkBin => 87,
            Self::GbkChineseCi => 28,
            Self::Gb18030Bin => 249,
            Self::Gb18030ChineseCi => 248,
        }
    }

    /// Returns the canonical name.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Binary => "binary",
            Self::AsciiBin => "ascii_bin",
            Self::Latin1Bin => "latin1_bin",
            Self::Utf8Bin => "utf8_bin",
            Self::Utf8GeneralCi => "utf8_general_ci",
            Self::Utf8UnicodeCi => "utf8_unicode_ci",
            Self::Utf8Mb4Bin => "utf8mb4_bin",
            Self::Utf8Mb4GeneralCi => "utf8mb4_general_ci",
            Self::Utf8Mb4UnicodeCi => "utf8mb4_unicode_ci",
            Self::Utf8Mb40900AiCi => "utf8mb4_0900_ai_ci",
            Self::Utf8Mb40900Bin => "utf8mb4_0900_bin",
            Self::Utf8Mb4ZhPinyinTiDbAsCs => "utf8mb4_zh_pinyin_tidb_as_cs",
            Self::GbkBin => "gbk_bin",
            Self::GbkChineseCi => "gbk_chinese_ci",
            Self::Gb18030Bin => "gb18030_bin",
            Self::Gb18030ChineseCi => "gb18030_chinese_ci",
        }
    }

    /// Returns the owning supported charset.
    pub const fn charset(self) -> Charset {
        match self {
            Self::Binary => Charset::Binary,
            Self::AsciiBin => Charset::Ascii,
            Self::Latin1Bin => Charset::Latin1,
            Self::Utf8Bin | Self::Utf8GeneralCi | Self::Utf8UnicodeCi => Charset::Utf8,
            Self::Utf8Mb4Bin
            | Self::Utf8Mb4GeneralCi
            | Self::Utf8Mb4UnicodeCi
            | Self::Utf8Mb40900AiCi
            | Self::Utf8Mb40900Bin
            | Self::Utf8Mb4ZhPinyinTiDbAsCs => Charset::Utf8Mb4,
            Self::GbkBin | Self::GbkChineseCi => Charset::Gbk,
            Self::Gb18030Bin | Self::Gb18030ChineseCi => Charset::Gb18030,
        }
    }

    /// Resolves a typed collation, including UTF8MB3 aliases.
    pub fn from_name(name: &str) -> Option<Self> {
        match utf8_alias(&name.to_ascii_lowercase()) {
            "binary" => Some(Self::Binary),
            "ascii_bin" => Some(Self::AsciiBin),
            "latin1_bin" => Some(Self::Latin1Bin),
            "utf8_bin" => Some(Self::Utf8Bin),
            "utf8_general_ci" => Some(Self::Utf8GeneralCi),
            "utf8_unicode_ci" => Some(Self::Utf8UnicodeCi),
            "utf8mb4_bin" => Some(Self::Utf8Mb4Bin),
            "utf8mb4_general_ci" => Some(Self::Utf8Mb4GeneralCi),
            "utf8mb4_unicode_ci" => Some(Self::Utf8Mb4UnicodeCi),
            "utf8mb4_0900_ai_ci" => Some(Self::Utf8Mb40900AiCi),
            "utf8mb4_0900_bin" => Some(Self::Utf8Mb40900Bin),
            "utf8mb4_zh_pinyin_tidb_as_cs" => Some(Self::Utf8Mb4ZhPinyinTiDbAsCs),
            "gbk_bin" => Some(Self::GbkBin),
            "gbk_chinese_ci" => Some(Self::GbkChineseCi),
            "gb18030_bin" => Some(Self::Gb18030Bin),
            "gb18030_chinese_ci" => Some(Self::Gb18030ChineseCi),
            _ => None,
        }
    }
}

impl fmt::Display for Collation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

/// Full charset metadata, including custom parser registrations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CharsetInfo {
    /// Canonical name.
    pub name: String,
    /// Default collation name.
    pub default_collation: String,
    /// Collations keyed by canonical name.
    pub collations: HashMap<String, CollationInfo>,
    /// Human-readable MySQL description.
    pub description: String,
    /// Maximum encoded bytes per character.
    pub maxlen: usize,
}

/// Full collation metadata from MySQL's registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollationInfo {
    /// Numeric collation ID.
    pub id: i32,
    /// Owning charset name.
    pub charset_name: String,
    /// Canonical collation name.
    pub name: String,
    /// Whether this row is the source table's default.
    pub is_default: bool,
    /// Source sort length.
    pub sortlen: usize,
    /// `PAD SPACE` or `NO PAD`.
    pub pad_attribute: String,
}

/// Registry lookup error with source-compatible text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CharsetError {
    /// Name exists in MySQL's table but TiDB does not support it.
    UnsupportedCharset(String),
    /// Name is absent from MySQL's table.
    UnknownCharset(String),
    /// Collation name is unknown.
    UnknownCollation(String),
    /// Collation ID is unknown.
    UnknownCollationId(i32),
}

impl fmt::Display for CharsetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedCharset(name) => write!(formatter, "Unsupported charset {name}"),
            Self::UnknownCharset(name) => write!(formatter, "Unknown charset {name}"),
            Self::UnknownCollation(name) => {
                write!(formatter, "[ddl:1273]Unknown collation: '{name}'")
            }
            Self::UnknownCollationId(id) => write!(formatter, "Unknown collation id {id}"),
        }
    }
}

impl std::error::Error for CharsetError {}

pub(crate) struct StaticCharsetInfo {
    name: &'static str,
    default_collation: &'static str,
    description: &'static str,
    maxlen: usize,
}

pub(crate) struct StaticCollationInfo {
    id: i32,
    charset: &'static str,
    name: &'static str,
    is_default: bool,
    sortlen: usize,
    pad_space: bool,
}

#[allow(dead_code)]
pub(crate) struct CaseRange {
    pub(crate) lo: u32,
    pub(crate) hi: u32,
    pub(crate) upper: i32,
    pub(crate) lower: i32,
    pub(crate) title: i32,
}

include!("charset_data.rs");

struct Registry {
    supported: HashMap<String, CharsetInfo>,
    known: HashMap<String, CharsetInfo>,
    collations_by_id: HashMap<i32, CollationInfo>,
    collations_by_name: HashMap<String, CollationInfo>,
    supported_collations: Vec<CollationInfo>,
}

fn blank_charset(name: &str, default: &str, description: &str, maxlen: usize) -> CharsetInfo {
    CharsetInfo {
        name: name.to_owned(),
        default_collation: default.to_owned(),
        collations: HashMap::new(),
        description: description.to_owned(),
        maxlen,
    }
}

impl Registry {
    fn source() -> Self {
        let known = KNOWN_CHARSETS
            .iter()
            .map(|row| {
                (
                    row.name.to_owned(),
                    blank_charset(row.name, row.default_collation, row.description, row.maxlen),
                )
            })
            .collect();
        let mut registry = Self {
            supported: HashMap::new(),
            known,
            collations_by_id: HashMap::new(),
            collations_by_name: HashMap::new(),
            supported_collations: Vec::new(),
        };
        for (name, default, description, maxlen) in [
            ("utf8", "utf8_bin", "UTF-8 Unicode", 3),
            ("utf8mb4", "utf8mb4_bin", "UTF-8 Unicode", 4),
            ("ascii", "ascii_bin", "US ASCII", 1),
            ("latin1", "latin1_bin", "Latin1", 1),
            ("binary", "binary", "binary", 1),
            ("gbk", "gbk_bin", "Chinese Internal Code Specification", 2),
            (
                "gb18030",
                "gb18030_bin",
                "China National Standard GB18030",
                4,
            ),
        ] {
            registry.supported.insert(
                name.to_owned(),
                blank_charset(name, default, description, maxlen),
            );
        }
        for row in ALL_COLLATIONS {
            registry.add_collation(CollationInfo {
                id: row.id,
                charset_name: row.charset.to_owned(),
                name: row.name.to_owned(),
                is_default: row.is_default,
                sortlen: row.sortlen,
                pad_attribute: if row.pad_space { PAD_SPACE } else { PAD_NONE }.to_owned(),
            });
        }
        registry
    }

    fn add_collation(&mut self, collation: CollationInfo) {
        self.collations_by_id
            .insert(collation.id, collation.clone());
        self.collations_by_name
            .insert(collation.name.clone(), collation.clone());
        if matches!(
            collation.name.as_str(),
            "utf8_bin"
                | "utf8mb4_bin"
                | "ascii_bin"
                | "latin1_bin"
                | "binary"
                | "gbk_bin"
                | "gb18030_bin"
        ) {
            self.supported_collations.push(collation.clone());
        }
        if let Some(charset) = self.supported.get_mut(&collation.charset_name) {
            charset
                .collations
                .insert(collation.name.clone(), collation.clone());
        }
        if let Some(charset) = self.known.get_mut(&collation.charset_name) {
            charset.collations.insert(collation.name.clone(), collation);
        }
    }
}

fn registry() -> &'static RwLock<Registry> {
    static REGISTRY: OnceLock<RwLock<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(Registry::source()))
}

pub(crate) fn set_new_collation_defaults(enabled: bool) {
    let mut guard = registry().write().expect("charset registry lock poisoned");
    for (charset_name, binary, chinese_ci) in [
        ("gbk", "gbk_bin", "gbk_chinese_ci"),
        ("gb18030", "gb18030_bin", "gb18030_chinese_ci"),
    ] {
        let default = if enabled { chinese_ci } else { binary };
        if let Some(charset) = guard.supported.get_mut(charset_name) {
            charset.default_collation = default.to_owned();
            for collation in charset.collations.values_mut() {
                collation.is_default = collation.name == default;
            }
        }
        if let Some(charset) = guard.known.get_mut(charset_name) {
            charset.default_collation = default.to_owned();
            for collation in charset.collations.values_mut() {
                collation.is_default = collation.name == default;
            }
        }
        for collation in guard.collations_by_name.values_mut() {
            if collation.charset_name == charset_name {
                collation.is_default = collation.name == default;
            }
        }
        for collation in guard.collations_by_id.values_mut() {
            if collation.charset_name == charset_name {
                collation.is_default = collation.name == default;
            }
        }
        for collation in &mut guard.supported_collations {
            if collation.charset_name == charset_name {
                collation.is_default = collation.name == default;
            }
        }
    }
}

fn utf8_alias(name: &str) -> &str {
    match name {
        "utf8mb3_bin" => "utf8_bin",
        "utf8mb3_unicode_ci" => "utf8_unicode_ci",
        "utf8mb3_general_ci" => "utf8_general_ci",
        _ => name,
    }
}

/// Returns all supported charsets sorted by name.
pub fn get_supported_charsets() -> Vec<CharsetInfo> {
    let guard = registry().read().expect("charset registry lock poisoned");
    let mut result: Vec<_> = guard.supported.values().cloned().collect();
    result.sort_by(|left, right| left.name.cmp(&right.name));
    result
}

/// Returns the source-ordered supported collation list.
pub fn get_supported_collations() -> Vec<CollationInfo> {
    registry()
        .read()
        .expect("charset registry lock poisoned")
        .supported_collations
        .clone()
}

/// Checks a supported charset/collation pair.
pub fn valid_charset_and_collation(charset: &str, collation: &str) -> bool {
    let charset = if charset.is_empty() || charset.eq_ignore_ascii_case("utf8mb3") {
        "utf8"
    } else {
        charset
    };
    let Ok(info) = get_charset_info(charset) else {
        return false;
    };
    collation.is_empty()
        || info
            .collations
            .contains_key(utf8_alias(&collation.to_ascii_lowercase()))
}

/// Returns the legacy parser default; GBK and GB18030 are deliberately absent.
pub fn get_default_collation_legacy(charset: &str) -> Result<String, CharsetError> {
    let lower = charset.to_ascii_lowercase();
    match lower.as_str() {
        "utf8mb3" => get_default_collation("utf8"),
        "utf8" | "utf8mb4" | "ascii" | "latin1" | "binary" => get_default_collation(&lower),
        _ => Err(CharsetError::UnknownCharset(charset.to_owned())),
    }
}

/// Returns a supported charset's default collation.
pub fn get_default_collation(charset: &str) -> Result<String, CharsetError> {
    Ok(get_charset_info(charset)?.default_collation)
}

/// Returns TiDB's default charset and collation.
pub const fn get_default_charset_and_collate() -> (&'static str, &'static str) {
    ("utf8mb4", "utf8mb4_bin")
}

/// Looks up supported charset information and distinguishes unsupported names.
pub fn get_charset_info(charset: &str) -> Result<CharsetInfo, CharsetError> {
    let canonical = if charset.eq_ignore_ascii_case("utf8mb3") {
        "utf8".to_owned()
    } else {
        charset.to_ascii_lowercase()
    };
    let guard = registry().read().expect("charset registry lock poisoned");
    if let Some(info) = guard.supported.get(&canonical) {
        return Ok(info.clone());
    }
    if guard.known.contains_key(&canonical) {
        return Err(CharsetError::UnsupportedCharset(charset.to_owned()));
    }
    Err(CharsetError::UnknownCharset(charset.to_owned()))
}

/// Looks up a collation ID, returning TiDB defaults alongside an error when unknown.
pub fn get_charset_info_by_id(id: i32) -> (String, String, Option<CharsetError>) {
    if id == 46 {
        return ("utf8mb4".to_owned(), "utf8mb4_bin".to_owned(), None);
    }
    if let Some(collation) = registry()
        .read()
        .expect("charset registry lock poisoned")
        .collations_by_id
        .get(&id)
        .cloned()
    {
        return (collation.charset_name, collation.name, None);
    }
    (
        "utf8mb4".to_owned(),
        "utf8mb4_bin".to_owned(),
        Some(CharsetError::UnknownCollationId(id)),
    )
}

/// Looks up any registered collation name.
pub fn get_collation_by_name(name: &str) -> Result<CollationInfo, CharsetError> {
    registry()
        .read()
        .expect("charset registry lock poisoned")
        .collations_by_name
        .get(utf8_alias(&name.to_ascii_lowercase()))
        .cloned()
        .ok_or_else(|| CharsetError::UnknownCollation(name.to_owned()))
}

/// Looks up any registered collation ID.
pub fn get_collation_by_id(id: i32) -> Result<CollationInfo, CharsetError> {
    registry()
        .read()
        .expect("charset registry lock poisoned")
        .collations_by_id
        .get(&id)
        .cloned()
        .ok_or(CharsetError::UnknownCollationId(id))
}

/// Adds a custom supported charset.
pub fn add_charset(charset: CharsetInfo) {
    registry()
        .write()
        .expect("charset registry lock poisoned")
        .supported
        .insert(charset.name.clone(), charset);
}

/// Removes a custom supported charset.
pub fn remove_charset(name: &str) {
    let mut guard = registry().write().expect("charset registry lock poisoned");
    guard.supported.remove(name);
    // Preserve the source implementation's name comparison exactly.
    guard.supported_collations.retain(|row| row.name != name);
}

/// Adds a collation to every applicable registry view.
pub fn add_collation(collation: CollationInfo) {
    registry()
        .write()
        .expect("charset registry lock poisoned")
        .add_collation(collation);
}

/// Appends a collation to the supported list.
pub fn add_supported_collation(collation: CollationInfo) {
    registry()
        .write()
        .expect("charset registry lock poisoned")
        .supported_collations
        .push(collation);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_registry_vectors() {
        let _guard = REGISTRY_TEST_LOCK
            .lock()
            .expect("charset test lock poisoned");
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
            assert_eq!(valid_charset_and_collation(charset, collation), expected);
        }
        assert_eq!(get_default_collation("utf8").unwrap(), "utf8_bin");
        assert_eq!(get_default_collation("gbk").unwrap(), "gbk_bin");
        assert_eq!(get_charset_info("utf8mb3").unwrap().name, "utf8");
        assert_eq!(
            get_collation_by_name("non_exist").unwrap_err().to_string(),
            "[ddl:1273]Unknown collation: 'non_exist'"
        );
        for row in ALL_COLLATIONS {
            assert_eq!(get_collation_by_name(row.name).unwrap().id, row.id);
        }
    }

    #[test]
    fn source_custom_charset_mutation() {
        let _guard = REGISTRY_TEST_LOCK
            .lock()
            .expect("charset test lock poisoned");
        add_charset(blank_charset("custom", "custom_collation", "Custom", 4));
        add_collation(CollationInfo {
            id: 99_999,
            charset_name: "custom".to_owned(),
            name: "custom_collation".to_owned(),
            is_default: true,
            sortlen: 8,
            pad_attribute: PAD_NONE.to_owned(),
        });
        assert!(valid_charset_and_collation("custom", "custom_collation"));
        assert_eq!(get_collation_by_id(99_999).unwrap().sortlen, 8);
        remove_charset("custom");
        assert!(get_charset_info("custom").is_err());
    }
}
