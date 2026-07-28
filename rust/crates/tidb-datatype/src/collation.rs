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

//! Compare and key authority for the collations needed by the scalar domain.
//!
//! The generated images are exact little-endian conversions of TiDB's Go
//! tables. Runtime keys remain the source-defined big-endian weight stream.

use std::cmp::Ordering;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use crate::charset::{
    get_collation_by_id as charset_collation_by_id,
    get_collation_by_name as charset_collation_by_name,
    get_supported_collations as charset_supported_collations, set_new_collation_defaults,
};
use crate::{CharsetError, Collation, CollationInfo, Encoding, TransformOp};

static NEW_COLLATION_ENABLED: AtomicBool = AtomicBool::new(true);

/// Source `DefaultLen`, used when a string datum has no known length.
pub const DEFAULT_LEN: usize = 0;

/// Errors owned by the collation package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollationError {
    /// Parser charset-registry lookup failure.
    Registry(CharsetError),
    /// The registry knows the name but new-collation mode has no implementation.
    UnsupportedCollation(String),
}

impl fmt::Display for CollationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registry(error) => error.fmt(formatter),
            Self::UnsupportedCollation(name) => write!(
                formatter,
                "[ddl:1273]Unsupported collation when new collation is enabled: '{name}'"
            ),
        }
    }
}

impl std::error::Error for CollationError {}

impl From<CharsetError> for CollationError {
    fn from(error: CharsetError) -> Self {
        Self::Registry(error)
    }
}

/// Runtime collator resolution. `DerivedBinary` is the legacy mode authority
/// used for every collation name when new collations are disabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Collator {
    /// A concrete new-collation implementation.
    New(Collation),
    /// Legacy byte comparison with rune-oriented wildcard matching.
    DerivedBinary,
}

impl Collator {
    /// Compares source Go-string bytes.
    pub fn compare(self, left: &[u8], right: &[u8]) -> Ordering {
        match self {
            Self::New(collation) => collation.compare(left, right),
            Self::DerivedBinary => left.cmp(right),
        }
    }

    /// Returns the source sort key.
    pub fn key(self, value: &[u8]) -> Vec<u8> {
        match self {
            Self::New(collation) => collation.key(value),
            Self::DerivedBinary => value.to_vec(),
        }
    }

    /// Rust-owned equivalent of Go's allocation-aware `ImmutableKey`.
    pub fn immutable_key(self, value: &[u8]) -> Vec<u8> {
        self.key(value)
    }

    /// Returns the key without PAD SPACE preprocessing.
    pub fn key_without_trim_right_space(self, value: &[u8]) -> Vec<u8> {
        match self {
            Self::New(collation) => collation.key_without_trim_right_space(value),
            Self::DerivedBinary => value.to_vec(),
        }
    }

    /// Returns the source upper bound for a key.
    pub fn max_key_len(self, value: &[u8]) -> usize {
        match self {
            Self::New(collation) => collation.max_key_len(value),
            Self::DerivedBinary => value.len(),
        }
    }

    /// Compiles a wildcard pattern with this collator's equality semantics.
    pub fn pattern(self, pattern: &str, escape: u8) -> WildcardPattern {
        match self {
            Self::DerivedBinary => WildcardPattern::unicode(pattern, escape, PatternMatcher::Exact),
            Self::New(collation) => collation.pattern(pattern, escape),
        }
    }

    /// Whether the source implementation can use the raw input as its key.
    pub const fn can_use_raw_mem_as_key(self) -> bool {
        matches!(self, Self::DerivedBinary | Self::New(Collation::Binary))
    }
}

/// Returns whether new collations are enabled.
pub fn new_collation_enabled() -> bool {
    NEW_COLLATION_ENABLED.load(AtomicOrdering::SeqCst)
}

/// Source test/configuration switch. Callers must serialize changes.
pub fn set_new_collation_enabled(enabled: bool) {
    set_new_collation_defaults(enabled);
    NEW_COLLATION_ENABLED.store(enabled, AtomicOrdering::SeqCst);
}

/// Checks source-compatible collation-name equivalence.
pub fn compatible_collate(left: &str, right: &str) -> bool {
    const GENERAL: [&str; 2] = ["utf8mb4_general_ci", "utf8_general_ci"];
    const BINARY: [&str; 3] = ["utf8mb4_bin", "utf8_bin", "latin1_bin"];
    const UNICODE: [&str; 2] = ["utf8mb4_unicode_ci", "utf8_unicode_ci"];
    left == right
        || [GENERAL.as_slice(), BINARY.as_slice(), UNICODE.as_slice()]
            .into_iter()
            .any(|class| class.contains(&left) && class.contains(&right))
}

/// Rewrites a protocol collation ID when new collations are enabled.
pub fn rewrite_new_collation_id_if_needed(id: i32) -> i32 {
    if new_collation_enabled() && id >= 0 {
        id.wrapping_neg()
    } else {
        id
    }
}

/// Restores a protocol collation ID when new collations are enabled.
pub fn restore_collation_id_if_needed(id: i32) -> i32 {
    if new_collation_enabled() && id <= 0 {
        id.wrapping_neg()
    } else {
        id
    }
}

/// Resolves a name, falling back exactly as the Go package does.
pub fn get_collator(name: &str) -> Collator {
    get_collator_with_mode(new_collation_enabled(), name)
}

/// Resolves a name using an explicit collation mode.
pub fn get_collator_with_mode(use_new_collation: bool, name: &str) -> Collator {
    if !use_new_collation {
        return Collator::DerivedBinary;
    }
    Collator::New(Collation::from_name(name).unwrap_or(Collation::Utf8Mb4Bin))
}

/// Returns the legacy binary collator.
pub const fn get_binary_collator() -> Collator {
    Collator::DerivedBinary
}

/// Returns `n` copies of the singleton-compatible legacy binary collator.
pub fn get_binary_collator_slice(length: usize) -> Vec<Collator> {
    vec![Collator::DerivedBinary; length]
}

/// Resolves a numeric ID, falling back exactly as the Go package does.
pub fn get_collator_by_id(id: i32) -> Collator {
    if !new_collation_enabled() {
        return Collator::DerivedBinary;
    }
    let collation = charset_collation_by_id(id)
        .ok()
        .and_then(|row| Collation::from_name(&row.name))
        .unwrap_or(Collation::Utf8Mb4Bin);
    Collator::New(collation)
}

/// Resolves an ID to its name, with TiDB's default fallback.
pub fn collation_id_to_name(id: i32) -> String {
    charset_collation_by_id(id)
        .map(|row| row.name)
        .unwrap_or_else(|_| Collation::DEFAULT.name().to_owned())
}

/// Resolves a name to its ID, with TiDB's default fallback.
pub fn collation_name_to_id(name: &str) -> i32 {
    charset_collation_by_name(name).map_or(46, |row| row.id)
}

/// Checks both registry existence and new-collation implementation support.
pub fn get_supported_collation_by_name(name: &str) -> Result<CollationInfo, CollationError> {
    let row = charset_collation_by_name(name)?;
    if new_collation_enabled() && Collation::from_name(&row.name).is_none() {
        return Err(CollationError::UnsupportedCollation(name.to_owned()));
    }
    Ok(row)
}

/// Substitutes the default for a missing or currently unsupported collation.
pub fn substitute_missing_collation_to_default(name: &str) -> String {
    get_supported_collation_by_name(name)
        .map(|row| row.name)
        .unwrap_or_else(|_| Collation::DEFAULT.name().to_owned())
}

/// Returns the collations exposed by the active mode.
pub fn supported_collations() -> Vec<CollationInfo> {
    if !new_collation_enabled() {
        return charset_supported_collations();
    }
    let mut rows: Vec<_> = [
        Collation::Binary,
        Collation::AsciiBin,
        Collation::Latin1Bin,
        Collation::Utf8Bin,
        Collation::Utf8GeneralCi,
        Collation::Utf8UnicodeCi,
        Collation::Utf8Mb4Bin,
        Collation::Utf8Mb4GeneralCi,
        Collation::Utf8Mb4UnicodeCi,
        Collation::Utf8Mb40900AiCi,
        Collation::Utf8Mb40900Bin,
        Collation::GbkBin,
        Collation::GbkChineseCi,
        Collation::Gb18030Bin,
        Collation::Gb18030ChineseCi,
    ]
    .into_iter()
    .map(|collation| {
        charset_collation_by_name(collation.name())
            .expect("implemented collation must exist in parser registry")
    })
    .collect();
    rows.sort_by(|left, right| left.name.cmp(&right.name));
    rows
}

/// Whether this is a default UTF8MB4 collation accepted by TiDB migration.
pub fn is_default_collation_for_utf8mb4(name: &str) -> bool {
    matches!(
        name,
        "utf8mb4_bin" | "utf8mb4_general_ci" | "utf8mb4_0900_ai_ci"
    )
}

/// Whether this collation is case-insensitive.
pub fn is_ci_collation(name: &str) -> bool {
    matches!(
        name,
        "utf8_general_ci"
            | "utf8mb4_general_ci"
            | "utf8_unicode_ci"
            | "utf8mb4_unicode_ci"
            | "gbk_chinese_ci"
            | "utf8mb4_0900_ai_ci"
            | "gb18030_chinese_ci"
    )
}

/// Converts a CI collation to the corresponding binary collation.
pub fn binary_collation_name(name: &str) -> &str {
    match name {
        "utf8_general_ci" | "utf8_unicode_ci" => "utf8_bin",
        "utf8mb4_general_ci" | "utf8mb4_unicode_ci" | "utf8mb4_0900_ai_ci" => "utf8mb4_bin",
        "gbk_chinese_ci" => "gbk_bin",
        "gb18030_chinese_ci" => "gb18030_bin",
        _ => name,
    }
}

/// Converts a name to its binary counterpart and resolves that collator.
pub fn binary_collator(name: &str) -> Collator {
    get_collator(binary_collation_name(name))
}

/// Whether a storage sort key is byte-identical to its raw input.
pub fn is_bin_collation(name: &str) -> bool {
    matches!(
        name,
        "ascii_bin" | "latin1_bin" | "utf8_bin" | "utf8mb4_bin" | "binary" | "utf8mb4_0900_bin"
    )
}

/// Whether the collation uses PAD SPACE semantics.
pub fn is_pad_space_collation(name: &str) -> bool {
    !matches!(name, "binary" | "utf8mb4_0900_ai_ci" | "utf8mb4_0900_bin")
}

/// Converts a name to its protocol ID.
pub fn collation_to_proto(name: &str) -> i32 {
    rewrite_new_collation_id_if_needed(collation_name_to_id(name))
}

/// Converts a protocol ID to its collation name.
pub fn proto_to_collation(id: i32) -> String {
    collation_id_to_name(restore_collation_id_if_needed(id))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PatternType {
    Match,
    One,
    Any,
}

#[derive(Debug, Clone, Copy)]
enum PatternMatcher {
    Exact,
    GeneralCi,
    Unicode0400,
    Unicode0900,
    GbkChineseCi,
    Gb18030ChineseCi,
}

/// A compiled collation-aware SQL LIKE wildcard pattern.
#[derive(Debug, Clone)]
pub struct WildcardPattern {
    pattern: Vec<u32>,
    types: Vec<PatternType>,
    binary: bool,
    matcher: PatternMatcher,
}

impl WildcardPattern {
    fn binary(pattern: &str, escape: u8) -> Self {
        let units: Vec<_> = pattern.as_bytes().iter().copied().map(u32::from).collect();
        let (pattern, types) = compile_pattern(units, u32::from(escape));
        Self {
            pattern,
            types,
            binary: true,
            matcher: PatternMatcher::Exact,
        }
    }

    fn unicode(pattern: &str, escape: u8, matcher: PatternMatcher) -> Self {
        let units: Vec<_> = pattern.chars().map(|character| character as u32).collect();
        let (pattern, types) = compile_pattern(units, u32::from(escape));
        Self {
            pattern,
            types,
            binary: false,
            matcher,
        }
    }

    /// Matches arbitrary Go-string bytes against the compiled pattern.
    pub fn is_match(&self, value: &[u8]) -> bool {
        let chars = if self.binary {
            value.iter().copied().map(u32::from).collect()
        } else {
            go_runes(value)
        };
        wildcard_match(&chars, &self.pattern, &self.types, |left, right| {
            self.matcher.matches(left, right)
        })
    }
}

impl PatternMatcher {
    fn matches(self, left: u32, right: u32) -> bool {
        match self {
            Self::Exact => left == right,
            Self::GeneralCi => general_weight(left) == general_weight(right),
            Self::Unicode0400 => {
                if left > 0xFFFF || right > 0xFFFF {
                    return left == right;
                }
                let left_weight = uca_weight(left);
                let right_weight = uca_weight(right);
                left_weight == right_weight && (left_weight.0 != 0xFFFD || left == right)
            }
            Self::Unicode0900 => uca_0900_weight(left) == uca_0900_weight(right),
            Self::GbkChineseCi => gbk_chinese_ci_weight(left) == gbk_chinese_ci_weight(right),
            Self::Gb18030ChineseCi => {
                gb18030_chinese_ci_weight(left) == gb18030_chinese_ci_weight(right)
            }
        }
    }
}

fn compile_pattern(units: Vec<u32>, escape: u32) -> (Vec<u32>, Vec<PatternType>) {
    let mut pattern = Vec::with_capacity(units.len());
    let mut types = Vec::with_capacity(units.len());
    let mut index = 0;
    while index < units.len() {
        let mut unit = units[index];
        let pattern_type = match unit {
            value if value == escape => {
                if index + 1 < units.len() {
                    index += 1;
                    unit = units[index];
                }
                PatternType::Match
            }
            value if value == u32::from(b'_') => {
                if types.last() == Some(&PatternType::Any) {
                    *pattern.last_mut().expect("Any has a pattern unit") = u32::from(b'_');
                    *types.last_mut().expect("Any has a pattern type") = PatternType::One;
                    unit = u32::from(b'%');
                    PatternType::Any
                } else {
                    PatternType::One
                }
            }
            value if value == u32::from(b'%') => {
                if types.last() == Some(&PatternType::Any) {
                    index += 1;
                    continue;
                }
                PatternType::Any
            }
            _ => PatternType::Match,
        };
        pattern.push(unit);
        types.push(pattern_type);
        index += 1;
    }
    (pattern, types)
}

fn wildcard_match(
    chars: &[u32],
    pattern: &[u32],
    types: &[PatternType],
    matcher: impl Fn(u32, u32) -> bool,
) -> bool {
    let (mut char_index, mut pattern_index) = (0, 0);
    let (mut next_char_index, mut next_pattern_index) = (0, 0);
    while pattern_index < pattern.len() || char_index < chars.len() {
        if pattern_index < pattern.len() {
            match types[pattern_index] {
                PatternType::Match
                    if char_index < chars.len()
                        && matcher(chars[char_index], pattern[pattern_index]) =>
                {
                    pattern_index += 1;
                    char_index += 1;
                    continue;
                }
                PatternType::One if char_index < chars.len() => {
                    pattern_index += 1;
                    char_index += 1;
                    continue;
                }
                PatternType::Any => {
                    next_pattern_index = pattern_index;
                    next_char_index = char_index + 1;
                    pattern_index += 1;
                    continue;
                }
                PatternType::Match | PatternType::One => {}
            }
        }
        if next_char_index > 0 && next_char_index <= chars.len() {
            pattern_index = next_pattern_index;
            char_index = next_char_index;
            continue;
        }
        return false;
    }
    true
}

fn go_runes(value: &[u8]) -> Vec<u32> {
    let mut result = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        match decode_rune(&value[index..]) {
            Ok((codepoint, width)) => {
                result.push(codepoint);
                index += width;
            }
            Err(()) => {
                result.push(0xFFFD);
                index += 1;
            }
        }
    }
    result
}

const GENERAL_CI: &[u8; 65_536 * 2] = include_bytes!("collation_data/general_ci_u16_le.bin");
const UNICODE_0400: &[u8; 65_536 * 8] = include_bytes!("collation_data/unicode_0400_u64_le.bin");
const UNICODE_0400_LONG: &[u8; 22 * 20] =
    include_bytes!("collation_data/unicode_0400_long_u64_le.bin");
const UNICODE_0900: &[u8; 183_969 * 8] = include_bytes!("collation_data/unicode_0900_u64_le.bin");
const UNICODE_0900_LONG: &[u8; 27 * 20] =
    include_bytes!("collation_data/unicode_0900_long_u64_le.bin");
const GBK_CHINESE_CI: &[u8; 65_536 * 2] =
    include_bytes!("collation_data/gbk_chinese_ci_u16_le.bin");
const GB18030_CHINESE_CI: &[u8; 0x11_0000 * 4] =
    include_bytes!("collation_data/gb18030_chinese_ci_u32_le.bin");

impl Collation {
    /// Compiles this collation's wildcard matcher.
    pub fn pattern(self, pattern: &str, escape: u8) -> WildcardPattern {
        match self {
            Self::Binary | Self::GbkBin | Self::Gb18030Bin => {
                WildcardPattern::binary(pattern, escape)
            }
            Self::AsciiBin
            | Self::Latin1Bin
            | Self::Utf8Bin
            | Self::Utf8Mb4Bin
            | Self::Utf8Mb40900Bin => {
                WildcardPattern::unicode(pattern, escape, PatternMatcher::Exact)
            }
            Self::Utf8GeneralCi | Self::Utf8Mb4GeneralCi => {
                WildcardPattern::unicode(pattern, escape, PatternMatcher::GeneralCi)
            }
            Self::Utf8UnicodeCi | Self::Utf8Mb4UnicodeCi => {
                WildcardPattern::unicode(pattern, escape, PatternMatcher::Unicode0400)
            }
            Self::Utf8Mb40900AiCi => {
                WildcardPattern::unicode(pattern, escape, PatternMatcher::Unicode0900)
            }
            Self::GbkChineseCi => {
                WildcardPattern::unicode(pattern, escape, PatternMatcher::GbkChineseCi)
            }
            Self::Gb18030ChineseCi => {
                WildcardPattern::unicode(pattern, escape, PatternMatcher::Gb18030ChineseCi)
            }
            Self::Utf8Mb4ZhPinyinTiDbAsCs => {
                panic!("utf8mb4_zh_pinyin_tidb_as_cs is not implemented")
            }
        }
    }

    /// Compares arbitrary Go-string bytes using TiDB's source semantics.
    pub fn compare(self, left: &[u8], right: &[u8]) -> Ordering {
        match self {
            Self::Binary | Self::AsciiBin | Self::Latin1Bin | Self::Utf8Mb40900Bin => {
                left.cmp(right)
            }
            Self::Utf8Bin | Self::Utf8Mb4Bin => {
                trim_trailing_spaces(left).cmp(trim_trailing_spaces(right))
            }
            Self::GbkBin => encoded_binary_compare(Encoding::Gbk, left, right),
            Self::Gb18030Bin => encoded_binary_compare(Encoding::Gb18030, left, right),
            Self::Utf8GeneralCi | Self::Utf8Mb4GeneralCi => general_ci_compare(left, right),
            Self::Utf8UnicodeCi | Self::Utf8Mb4UnicodeCi => unicode_0400_compare(left, right),
            Self::Utf8Mb40900AiCi => unicode_0900_compare(left, right),
            Self::GbkChineseCi => chinese_ci_compare(left, right, gbk_chinese_ci_weight),
            Self::Gb18030ChineseCi => chinese_ci_compare(left, right, gb18030_chinese_ci_weight),
            Self::Utf8Mb4ZhPinyinTiDbAsCs => {
                panic!("utf8mb4_zh_pinyin_tidb_as_cs is not implemented")
            }
        }
    }

    /// Returns TiDB's sort key for arbitrary Go-string bytes.
    pub fn key(self, value: &[u8]) -> Vec<u8> {
        match self {
            Self::Binary | Self::AsciiBin | Self::Latin1Bin | Self::Utf8Mb40900Bin => {
                value.to_vec()
            }
            Self::Utf8Bin | Self::Utf8Mb4Bin => trim_trailing_spaces(value).to_vec(),
            Self::GbkBin => encoded_binary_key(Encoding::Gbk, value, true),
            Self::Gb18030Bin => encoded_binary_key(Encoding::Gb18030, value, true),
            Self::Utf8GeneralCi | Self::Utf8Mb4GeneralCi => general_ci_key(value, true),
            Self::Utf8UnicodeCi | Self::Utf8Mb4UnicodeCi => unicode_0400_key(value, true),
            Self::Utf8Mb40900AiCi => unicode_0900_key(value),
            Self::GbkChineseCi => chinese_ci_key(value, true, gbk_chinese_ci_weight),
            Self::Gb18030ChineseCi => chinese_ci_key(value, true, gb18030_chinese_ci_weight),
            Self::Utf8Mb4ZhPinyinTiDbAsCs => {
                panic!("utf8mb4_zh_pinyin_tidb_as_cs is not implemented")
            }
        }
    }

    /// Rust-owned equivalent of Go's allocation-aware `ImmutableKey`.
    pub fn immutable_key(self, value: &[u8]) -> Vec<u8> {
        self.key(value)
    }

    /// Returns the source key without the collation's PAD SPACE preprocessing.
    pub fn key_without_trim_right_space(self, value: &[u8]) -> Vec<u8> {
        match self {
            Self::Binary
            | Self::AsciiBin
            | Self::Latin1Bin
            | Self::Utf8Bin
            | Self::Utf8Mb4Bin
            | Self::Utf8Mb40900Bin => value.to_vec(),
            Self::GbkBin => encoded_binary_key(Encoding::Gbk, value, false),
            Self::Gb18030Bin => encoded_binary_key(Encoding::Gb18030, value, false),
            Self::Utf8GeneralCi | Self::Utf8Mb4GeneralCi => general_ci_key(value, false),
            Self::Utf8UnicodeCi | Self::Utf8Mb4UnicodeCi => unicode_0400_key(value, false),
            Self::Utf8Mb40900AiCi => unicode_0900_key(value),
            Self::GbkChineseCi => chinese_ci_key(value, false, gbk_chinese_ci_weight),
            Self::Gb18030ChineseCi => chinese_ci_key(value, false, gb18030_chinese_ci_weight),
            Self::Utf8Mb4ZhPinyinTiDbAsCs => {
                panic!("utf8mb4_zh_pinyin_tidb_as_cs is not implemented")
            }
        }
    }

    /// Returns the exact upper bound exposed by the corresponding Go collator.
    pub fn max_key_len(self, value: &[u8]) -> usize {
        match self {
            Self::Binary
            | Self::AsciiBin
            | Self::Latin1Bin
            | Self::Utf8Bin
            | Self::Utf8Mb4Bin
            | Self::Utf8Mb40900Bin => value.len(),
            Self::GbkBin | Self::GbkChineseCi => go_rune_count(value) * 2,
            Self::Gb18030Bin | Self::Gb18030ChineseCi => go_rune_count(value) * 4,
            Self::Utf8GeneralCi | Self::Utf8Mb4GeneralCi => go_rune_count(value) * 2,
            Self::Utf8UnicodeCi | Self::Utf8Mb4UnicodeCi | Self::Utf8Mb40900AiCi => {
                go_rune_count(value) * 16
            }
            Self::Utf8Mb4ZhPinyinTiDbAsCs => {
                panic!("utf8mb4_zh_pinyin_tidb_as_cs is not implemented")
            }
        }
    }
}

fn encoded_binary_key(encoding: Encoding, value: &[u8], trim: bool) -> Vec<u8> {
    let value = if trim {
        trim_trailing_spaces(value)
    } else {
        value
    };
    encoding
        .transform(value, TransformOp::ENCODE_REPLACE)
        .into_parts()
        .0
}

fn encoded_binary_compare(encoding: Encoding, left: &[u8], right: &[u8]) -> Ordering {
    encoded_binary_key(encoding, left, true).cmp(&encoded_binary_key(encoding, right, true))
}

fn trim_trailing_spaces(mut value: &[u8]) -> &[u8] {
    while value.last() == Some(&b' ') {
        value = &value[..value.len() - 1];
    }
    value
}

pub(crate) fn decode_rune(value: &[u8]) -> Result<(u32, usize), ()> {
    let first = *value.first().ok_or(())?;
    if first < 0x80 {
        return Ok((u32::from(first), 1));
    }
    let width = match first {
        0xC2..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF4 => 4,
        _ => return Err(()),
    };
    let bytes = value.get(..width).ok_or(())?;
    let character = std::str::from_utf8(bytes)
        .map_err(|_| ())?
        .chars()
        .next()
        .ok_or(())?;
    Ok((character as u32, width))
}

pub(crate) fn go_rune_count(value: &[u8]) -> usize {
    let mut index = 0;
    let mut count = 0;
    while index < value.len() {
        index += rune_width(&value[index..]);
        count += 1;
    }
    count
}

/// The width in bytes of the rune starting at `value`, Go's `DecodeRune`
/// convention: a byte that starts no valid sequence is one RuneError one byte
/// wide, never an error.
pub(crate) fn rune_width(value: &[u8]) -> usize {
    decode_rune(value).map_or(1, |(_, width)| width)
}

fn general_weight(codepoint: u32) -> u16 {
    let codepoint = codepoint as usize;
    if codepoint > 0xFFFF {
        return 0xFFFD;
    }
    let offset = codepoint * 2;
    u16::from_le_bytes([GENERAL_CI[offset], GENERAL_CI[offset + 1]])
}

fn general_ci_compare(left: &[u8], right: &[u8]) -> Ordering {
    let (left, right) = (trim_trailing_spaces(left), trim_trailing_spaces(right));
    let (mut left_index, mut right_index) = (0, 0);
    while left_index < left.len() && right_index < right.len() {
        let (left_rune, left_width) = match decode_rune(&left[left_index..]) {
            Ok(decoded) => decoded,
            Err(()) => return Ordering::Equal,
        };
        let (right_rune, right_width) = match decode_rune(&right[right_index..]) {
            Ok(decoded) => decoded,
            Err(()) => return Ordering::Equal,
        };
        left_index += left_width;
        right_index += right_width;
        let ordering = general_weight(left_rune).cmp(&general_weight(right_rune));
        if !ordering.is_eq() {
            return ordering;
        }
    }
    (left.len() - left_index).cmp(&(right.len() - right_index))
}

fn general_ci_key(value: &[u8], trim: bool) -> Vec<u8> {
    let value = if trim {
        trim_trailing_spaces(value)
    } else {
        value
    };
    let mut key = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        let (codepoint, width) = match decode_rune(&value[index..]) {
            Ok(decoded) => decoded,
            Err(()) => break,
        };
        index += width;
        key.extend_from_slice(&general_weight(codepoint).to_be_bytes());
    }
    key
}

fn chinese_ci_compare(left: &[u8], right: &[u8], weight: fn(u32) -> u32) -> Ordering {
    let (left, right) = (trim_trailing_spaces(left), trim_trailing_spaces(right));
    let (mut left_index, mut right_index) = (0, 0);
    while left_index < left.len() && right_index < right.len() {
        let (left_rune, left_width) = match decode_rune(&left[left_index..]) {
            Ok(decoded) => decoded,
            Err(()) => return Ordering::Equal,
        };
        let (right_rune, right_width) = match decode_rune(&right[right_index..]) {
            Ok(decoded) => decoded,
            Err(()) => return Ordering::Equal,
        };
        left_index += left_width;
        right_index += right_width;
        let ordering = weight(left_rune).cmp(&weight(right_rune));
        if !ordering.is_eq() {
            return ordering;
        }
    }
    (left.len() - left_index).cmp(&(right.len() - right_index))
}

fn chinese_ci_key(value: &[u8], trim: bool, weight: fn(u32) -> u32) -> Vec<u8> {
    let value = if trim {
        trim_trailing_spaces(value)
    } else {
        value
    };
    let mut key = Vec::with_capacity(value.len() * 2);
    let mut index = 0;
    while index < value.len() {
        let (codepoint, width) = match decode_rune(&value[index..]) {
            Ok(decoded) => decoded,
            Err(()) => break,
        };
        index += width;
        let bytes = weight(codepoint).to_be_bytes();
        let first = bytes.iter().position(|byte| *byte != 0).unwrap_or(3);
        key.extend_from_slice(&bytes[first..]);
    }
    key
}

fn gbk_chinese_ci_weight(codepoint: u32) -> u32 {
    let Some(offset) = usize::try_from(codepoint)
        .ok()
        .filter(|codepoint| *codepoint <= 0xFFFF)
        .map(|codepoint| codepoint * 2)
    else {
        return 0x3F;
    };
    u32::from(u16::from_le_bytes([
        GBK_CHINESE_CI[offset],
        GBK_CHINESE_CI[offset + 1],
    ]))
}

fn gb18030_chinese_ci_weight(codepoint: u32) -> u32 {
    let Some(offset) = usize::try_from(codepoint)
        .ok()
        .filter(|codepoint| *codepoint <= 0x10_FFFF)
        .map(|codepoint| codepoint * 4)
    else {
        return 0x3F;
    };
    u32::from_le_bytes(
        GB18030_CHINESE_CI[offset..offset + 4]
            .try_into()
            .expect("fixed GB18030 weight width"),
    )
}

fn uca_weight(codepoint: u32) -> (u64, u64) {
    let codepoint = codepoint as usize;
    if codepoint > 0xFFFF {
        return (0xFFFD, 0);
    }
    let offset = codepoint * 8;
    let first = u64::from_le_bytes(
        UNICODE_0400[offset..offset + 8]
            .try_into()
            .expect("fixed UCA table width"),
    );
    if first != 0xFFFD {
        return (first, 0);
    }
    long_uca_weight(codepoint as u32)
        .expect("generated UCA 4.0 long-rune marker must have an expansion record")
}

fn uca_0900_weight(codepoint: u32) -> (u64, u64) {
    let Ok(index) = usize::try_from(codepoint) else {
        return (0xFFFD, 0);
    };
    if index > 183_969 {
        return (
            u64::from(codepoint >> 15) + 0xFBC0 + (u64::from((codepoint & 0x7FFF) | 0x8000) << 16),
            0,
        );
    }
    if index == 183_969 {
        panic!("source UCA 9.0 table index is out of range");
    }
    let offset = index * 8;
    let first = u64::from_le_bytes(
        UNICODE_0900[offset..offset + 8]
            .try_into()
            .expect("fixed UCA 9.0 table width"),
    );
    if first != 0xFFFD || (0xD800..=0xDFFF).contains(&codepoint) {
        return (first, 0);
    }
    long_weight(UNICODE_0900_LONG, 27, codepoint)
        .expect("generated UCA 9.0 long-rune marker must have an expansion record")
}

fn long_uca_weight(codepoint: u32) -> Option<(u64, u64)> {
    long_weight(UNICODE_0400_LONG, 22, codepoint)
}

fn long_weight(table: &[u8], row_count: usize, codepoint: u32) -> Option<(u64, u64)> {
    let rune_at = |index: usize| {
        let offset = index * 20;
        u32::from_le_bytes(
            table[offset..offset + 4]
                .try_into()
                .expect("fixed long-rune record width"),
        )
    };
    let (mut low, mut high) = (0_usize, row_count);
    while low < high {
        let middle = low + (high - low) / 2;
        match rune_at(middle).cmp(&codepoint) {
            Ordering::Less => low = middle + 1,
            Ordering::Greater => high = middle,
            Ordering::Equal => {
                low = middle;
                break;
            }
        }
    }
    (low < row_count && rune_at(low) == codepoint).then(|| {
        let index = low;
        let offset = index * 20 + 4;
        let first = u64::from_le_bytes(
            table[offset..offset + 8]
                .try_into()
                .expect("fixed long-rune first weight"),
        );
        let second = u64::from_le_bytes(
            table[offset + 8..offset + 16]
                .try_into()
                .expect("fixed long-rune second weight"),
        );
        (first, second)
    })
}

struct UcaCursor<'a> {
    bytes: &'a [u8],
    byte_index: usize,
    pending: [u16; 8],
    pending_index: usize,
    pending_len: usize,
    weight: fn(u32) -> (u64, u64),
}

impl<'a> UcaCursor<'a> {
    fn new(bytes: &'a [u8], weight: fn(u32) -> (u64, u64)) -> Self {
        Self {
            bytes,
            byte_index: 0,
            pending: [0; 8],
            pending_index: 0,
            pending_len: 0,
            weight,
        }
    }

    fn next_weight(&mut self) -> Result<Option<u16>, ()> {
        loop {
            if self.pending_index < self.pending_len {
                let weight = self.pending[self.pending_index];
                self.pending_index += 1;
                return Ok(Some(weight));
            }
            if self.byte_index == self.bytes.len() {
                return Ok(None);
            }
            let (codepoint, width) = decode_rune(&self.bytes[self.byte_index..])?;
            self.byte_index += width;
            self.pending_index = 0;
            self.pending_len = 0;
            let (first, second) = (self.weight)(codepoint);
            self.append_packed(first);
            self.append_packed(second);
        }
    }

    fn append_packed(&mut self, mut packed: u64) {
        while packed != 0 {
            self.pending[self.pending_len] = packed as u16;
            self.pending_len += 1;
            packed >>= 16;
        }
    }
}

fn unicode_0400_compare(left: &[u8], right: &[u8]) -> Ordering {
    weighted_compare(
        trim_trailing_spaces(left),
        trim_trailing_spaces(right),
        uca_weight,
    )
}

fn unicode_0900_compare(left: &[u8], right: &[u8]) -> Ordering {
    weighted_compare(left, right, uca_0900_weight)
}

fn weighted_compare(left: &[u8], right: &[u8], weight: fn(u32) -> (u64, u64)) -> Ordering {
    let mut left = UcaCursor::new(left, weight);
    let mut right = UcaCursor::new(right, weight);
    loop {
        let left_weight = match left.next_weight() {
            Ok(weight) => weight,
            Err(()) => return Ordering::Equal,
        };
        let right_weight = match right.next_weight() {
            Ok(weight) => weight,
            Err(()) => return Ordering::Equal,
        };
        match (left_weight, right_weight) {
            (Some(left), Some(right)) => {
                let ordering = left.cmp(&right);
                if !ordering.is_eq() {
                    return ordering;
                }
            }
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
        }
    }
}

fn unicode_0400_key(value: &[u8], trim: bool) -> Vec<u8> {
    let value = if trim {
        trim_trailing_spaces(value)
    } else {
        value
    };
    weighted_key(value, uca_weight)
}

fn unicode_0900_key(value: &[u8]) -> Vec<u8> {
    weighted_key(value, uca_0900_weight)
}

fn weighted_key(value: &[u8], weight: fn(u32) -> (u64, u64)) -> Vec<u8> {
    let mut key = Vec::with_capacity(value.len() * 2);
    let mut cursor = UcaCursor::new(value, weight);
    while let Ok(Some(weight)) = cursor.next_weight() {
        key.extend_from_slice(&weight.to_be_bytes());
    }
    key
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use sha2::{Digest, Sha256};

    use super::{
        Collation, GB18030_CHINESE_CI, GBK_CHINESE_CI, GENERAL_CI, UNICODE_0400, UNICODE_0400_LONG,
        UNICODE_0900, UNICODE_0900_LONG,
    };

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    #[test]
    fn generated_images_have_source_pinned_lengths_and_hashes() {
        assert_eq!(GENERAL_CI.len(), 131_072);
        assert_eq!(UNICODE_0400.len(), 524_288);
        assert_eq!(UNICODE_0400_LONG.len(), 440);
        assert_eq!(UNICODE_0900.len(), 1_471_752);
        assert_eq!(UNICODE_0900_LONG.len(), 540);
        assert_eq!(GBK_CHINESE_CI.len(), 131_072);
        assert_eq!(GB18030_CHINESE_CI.len(), 4_456_448);
        assert_eq!(
            digest(GENERAL_CI),
            "787ea411c0600e485ae7dd52ce4b609848b5b832c179f2aed6deaf1e3a173d61"
        );
        assert_eq!(
            digest(UNICODE_0400),
            "87fbb2751d6afe9ff48b4f19136204846e778dd88a1ba8ef8b2d5398354852b6"
        );
        assert_eq!(
            digest(UNICODE_0400_LONG),
            "fc2ea60aa8caa70d615fcdffaf1d8e1d3d2438eae11847d719266be88bb5d776"
        );
        assert_eq!(
            digest(UNICODE_0900),
            "5ff4831e13e7485cff183e4e9971fd17e2719da0d675f8b38db8f02e89aaee7b"
        );
        assert_eq!(
            digest(UNICODE_0900_LONG),
            "8329421bd84ef04ad3ff5650e6b946d2cb22934d1fded231b7938bb094155c6f"
        );
        assert_eq!(
            digest(GBK_CHINESE_CI),
            "f6f63c33fa57eeaffa5d46841694adab58bd9cddfac3f92389dec4564a6036d6"
        );
        assert_eq!(
            digest(GB18030_CHINESE_CI),
            "64faeaa726d3555479fa98b7d61add86bbdcb659235da3ffacbbae4fb45d340d"
        );
    }

    /// The UCA 4.0 half of `TestAllItemInLongRUneMapIsUnique`.
    #[test]
    fn all_uca_0400_long_rune_weights_are_unique() {
        let rows: Vec<_> = UNICODE_0400_LONG
            .chunks_exact(20)
            .map(|row| (&row[4..12], &row[12..20]))
            .collect();
        assert_eq!(rows.len(), 22);
        assert_eq!(rows.iter().copied().collect::<HashSet<_>>().len(), 22);
    }

    /// The UCA 9.0 half of `TestAllItemInLongRUneMapIsUnique`.
    #[test]
    fn all_uca_0900_long_rune_weights_are_unique() {
        let rows: Vec<_> = UNICODE_0900_LONG
            .chunks_exact(20)
            .map(|row| (&row[4..12], &row[12..20]))
            .collect();
        assert_eq!(rows.len(), 27);
        assert_eq!(rows.iter().copied().collect::<HashSet<_>>().len(), 27);
    }

    /// `TestHangulJamoHasOnlyOneWeight`.
    #[test]
    fn uca_0900_hangul_jamo_has_only_one_weight() {
        for codepoint in 0x1100..0x11FF {
            let offset = codepoint * 8;
            let weight = u64::from_le_bytes(
                UNICODE_0900[offset..offset + 8]
                    .try_into()
                    .expect("fixed UCA 9.0 weight"),
            );
            assert_eq!(weight & 0xFFFF_FFFF_FFFF_0000, 0);
        }
    }

    /// `TestFirstIsNotZero`.
    #[test]
    fn every_uca_0900_long_weight_starts_nonzero() {
        for row in UNICODE_0900_LONG.chunks_exact(20) {
            assert_ne!(
                u64::from_le_bytes(row[4..12].try_into().expect("first long weight")),
                0
            );
        }
    }

    #[test]
    fn every_uca_0400_long_marker_has_exactly_one_expansion() {
        let markers: HashSet<_> = UNICODE_0400
            .chunks_exact(8)
            .enumerate()
            .filter_map(|(codepoint, bytes)| {
                (u64::from_le_bytes(bytes.try_into().expect("fixed UCA weight")) == 0xFFFD)
                    .then_some(codepoint as u32)
            })
            .collect();
        let expansions: HashSet<_> = UNICODE_0400_LONG
            .chunks_exact(20)
            .map(|row| u32::from_le_bytes(row[..4].try_into().expect("fixed long-rune key")))
            .collect();
        assert_eq!(markers.len(), 22);
        assert_eq!(markers, expansions);
    }

    #[test]
    fn max_key_lengths_and_without_trim_follow_go_collators() {
        assert_eq!(Collation::Binary.max_key_len(b"a "), 2);
        assert_eq!(Collation::Utf8GeneralCi.max_key_len("😜".as_bytes()), 2);
        assert_eq!(Collation::Utf8UnicodeCi.max_key_len("😜".as_bytes()), 16);
        assert_eq!(Collation::Utf8GeneralCi.max_key_len(&[0xFF]), 2);
        assert_eq!(Collation::Utf8UnicodeCi.max_key_len(&[0xFF]), 16);
        assert_eq!(Collation::Utf8Mb4Bin.key(b"a "), b"a");
        assert_eq!(
            Collation::Utf8Mb4Bin.key_without_trim_right_space(b"a "),
            b"a "
        );
        assert_eq!(
            Collation::Utf8GeneralCi.key_without_trim_right_space(b"a "),
            [0, 0x41, 0, 0x20]
        );
    }
}
