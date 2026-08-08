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

use std::collections::HashSet;
use std::error::Error;
use std::fmt;

use crate::{Collator, GoString, GoStringSource};

/// MySQL ENUM's canonical name and one-based numeric index.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MysqlEnum {
    name: GoString,
    value: u64,
}

impl MysqlEnum {
    /// Creates the exact source name/value pair.
    pub fn new(name: impl Into<GoString>, value: u64) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }

    /// Returns the canonical element spelling.
    pub fn name(&self) -> &GoString {
        &self.name
    }

    /// Returns the canonical name bytes without UTF-8 normalization.
    pub fn name_bytes(&self) -> &[u8] {
        self.name.as_bytes()
    }

    /// Returns the one-based element index, or zero for the Go error sentinel.
    pub const fn value(&self) -> u64 {
        self.value
    }

    /// Mirrors `Enum.ToNumber`.
    pub fn to_number(&self) -> f64 {
        self.value as f64
    }

    /// Mirrors the explicit deep-copy method at the Go ownership boundary.
    pub fn copy(&self) -> Self {
        Self {
            name: self.name.deep_copy(),
            value: self.value,
        }
    }
}

impl fmt::Display for MysqlEnum {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Rust formatting is necessarily UTF-8. Byte-sensitive source paths
        // use `name_bytes` instead of this diagnostic projection.
        self.name.fmt(formatter)
    }
}

/// Exact source context plus TiDB's typed truncation identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumParseError {
    context: GoString,
}

impl EnumParseError {
    /// TiDB dbterror class owning `ErrTruncated`.
    pub const fn class(&self) -> &'static str {
        "types"
    }

    /// MySQL `WarnDataTruncated`.
    pub const fn mysql_code(&self) -> u16 {
        1265
    }

    /// The exact context passed to `errors.Wrap` by `enum.go`.
    pub fn context(&self) -> &GoString {
        &self.context
    }

    /// Returns the exact context bytes, including invalid UTF-8.
    pub fn context_bytes(&self) -> &[u8] {
        self.context.as_bytes()
    }

    /// Returns the exact wrapped Go error bytes. Rust [`fmt::Display`] can
    /// represent only UTF-8 and is therefore a lossy convenience when the
    /// source context contains invalid bytes.
    pub fn message_bytes(&self) -> Vec<u8> {
        let mut message = self.context.as_bytes().to_vec();
        message.extend_from_slice(b": ");
        message.extend_from_slice((&*crate::ERR_TRUNCATED).to_string().as_bytes());
        message
    }

    /// Go returns `Enum{}` together with this error.
    pub fn returned_value(&self) -> MysqlEnum {
        MysqlEnum::default()
    }
}

impl fmt::Display for EnumParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.context, &*crate::ERR_TRUNCATED)
    }
}

impl Error for EnumParseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&*crate::ERR_TRUNCATED)
    }
}

/// Parses an ENUM as a collation-aware name first, then as Go's base-0 u64.
pub fn parse_enum<E, N, C>(
    elements: &[E],
    name: &N,
    collation: C,
) -> Result<MysqlEnum, EnumParseError>
where
    E: GoStringSource,
    N: GoStringSource + ?Sized,
    C: Into<Collator> + Copy,
{
    if let Ok(value) = parse_enum_name(elements, name, collation) {
        return Ok(value);
    }
    if let Some(number) = parse_go_uint64_base_zero(name.as_go_bytes()) {
        return parse_enum_value(elements, number);
    }
    Err(enum_item_error(elements, name))
}

/// Parses an ENUM name and returns the first collation-equal declaration.
pub fn parse_enum_name<E, N, C>(
    elements: &[E],
    name: &N,
    collation: C,
) -> Result<MysqlEnum, EnumParseError>
where
    E: GoStringSource,
    N: GoStringSource + ?Sized,
    C: Into<Collator>,
{
    let collator = collation.into();
    elements
        .iter()
        .enumerate()
        .find(|(_, element)| {
            collator
                .compare(element.as_go_bytes(), name.as_go_bytes())
                .is_eq()
        })
        .map(|(index, element)| MysqlEnum {
            name: element.to_go_string(),
            value: index as u64 + 1,
        })
        .ok_or_else(|| enum_item_error(elements, name))
}

/// Parses an ENUM's one-based numeric index.
pub fn parse_enum_value<E>(elements: &[E], number: u64) -> Result<MysqlEnum, EnumParseError>
where
    E: GoStringSource,
{
    if number == 0 || number > elements.len() as u64 {
        return Err(EnumParseError {
            context: GoString::from(format!(
                "convert to MySQL enum failed: number {number} overflow enum boundary [1, {}]",
                elements.len()
            )),
        });
    }
    Ok(MysqlEnum {
        name: elements[number as usize - 1].to_go_string(),
        value: number,
    })
}

fn enum_item_error<E, N>(elements: &[E], name: &N) -> EnumParseError
where
    E: GoStringSource,
    N: GoStringSource + ?Sized,
{
    let mut context = b"convert to MySQL enum failed: item ".to_vec();
    context.extend_from_slice(name.as_go_bytes());
    context.extend_from_slice(b" is not in enum ");
    context.extend_from_slice(&format_elements(elements));
    EnumParseError {
        context: GoString::from(context),
    }
}

/// MySQL SET's canonical declaration-ordered name and bit mask.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MysqlSet {
    name: GoString,
    value: u64,
}

impl MysqlSet {
    /// Creates the exact source name/value pair.
    pub fn new(name: impl Into<GoString>, value: u64) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }

    /// Returns the canonical comma-separated name.
    pub fn name(&self) -> &GoString {
        &self.name
    }

    /// Returns the canonical comma-separated name bytes.
    pub fn name_bytes(&self) -> &[u8] {
        self.name.as_bytes()
    }

    /// Returns the source bit mask.
    pub const fn value(&self) -> u64 {
        self.value
    }

    /// Mirrors `Set.ToNumber`.
    pub fn to_number(&self) -> f64 {
        self.value as f64
    }

    /// Mirrors the explicit deep-copy method at the Go ownership boundary.
    pub fn copy(&self) -> Self {
        Self {
            name: self.name.deep_copy(),
            value: self.value,
        }
    }
}

impl fmt::Display for MysqlSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Rust formatting is necessarily UTF-8. Byte-sensitive source paths
        // use `name_bytes` instead of this diagnostic projection.
        self.name.fmt(formatter)
    }
}

/// Exact SET parse failures returned before Go's native out-of-bounds panic
/// when `ParseSetValue` traverses more than 64 elements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetParseError {
    /// One or more comma-separated names did not match an element.
    UnknownItem(GoString),
    /// Bits remained after consuming every declared element.
    InvalidNumber(GoString),
}

impl SetParseError {
    /// Returns the exact Go error bytes, including invalid UTF-8.
    pub fn message_bytes(&self) -> &[u8] {
        match self {
            Self::UnknownItem(message) | Self::InvalidNumber(message) => message.as_bytes(),
        }
    }
}

impl fmt::Display for SetParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownItem(message) | Self::InvalidNumber(message) => message.fmt(formatter),
        }
    }
}

impl Error for SetParseError {}

/// Parses a SET as names first, then as Go's base-0 u64.
pub fn parse_set<E, N, C>(elements: &[E], name: &N, collation: C) -> Result<MysqlSet, SetParseError>
where
    E: GoStringSource,
    N: GoStringSource + ?Sized,
    C: Into<Collator> + Copy,
{
    if let Ok(value) = parse_set_name(elements, name, collation) {
        return Ok(value);
    }
    if let Some(number) = parse_go_uint64_base_zero(name.as_go_bytes()) {
        return parse_set_value(elements, number);
    }
    Err(set_item_error(elements, name))
}

/// Parses, deduplicates, and canonicalizes SET names by collation key.
pub fn parse_set_name<E, N, C>(
    elements: &[E],
    name: &N,
    collation: C,
) -> Result<MysqlSet, SetParseError>
where
    E: GoStringSource,
    N: GoStringSource + ?Sized,
    C: Into<Collator>,
{
    if name.as_go_bytes().is_empty() {
        return Ok(MysqlSet::default());
    }

    let collator = collation.into();
    let mut marked: HashSet<Vec<u8>> = name
        .as_go_bytes()
        .split(|byte| *byte == b',')
        .map(|part| collator.key(part))
        .collect();
    let mut items: Vec<&E> = Vec::with_capacity(marked.len());
    let mut value = 0_u64;
    for (index, element) in elements.iter().enumerate() {
        if marked.remove(&collator.key(element.as_go_bytes())) {
            value |= u32::try_from(index)
                .ok()
                .and_then(|shift| 1_u64.checked_shl(shift))
                .unwrap_or(0);
            items.push(element);
        }
    }
    if !marked.is_empty() {
        return Err(set_item_error(elements, name));
    }
    Ok(MysqlSet {
        name: join_strings(&items, b','),
        value,
    })
}

/// Parses a SET bit mask and returns names in declaration order.
pub fn parse_set_value<E>(elements: &[E], number: u64) -> Result<MysqlSet, SetParseError>
where
    E: GoStringSource,
{
    if number == 0 {
        return Ok(MysqlSet::default());
    }
    let value = number;
    let mut remaining = number;
    let mut items: Vec<&E> = Vec::new();
    for (index, element) in elements.iter().enumerate() {
        // Go indexes its fixed `[64]uint64` tables before testing the bit;
        // every nonzero call with a 65th element therefore panics at index 64.
        let bit = 1_u64
            .checked_shl(u32::try_from(index).expect("SET index exceeds u32"))
            .expect("ParseSetValue index out of range");
        if remaining & bit != 0 {
            items.push(element);
            remaining &= !bit;
        }
    }
    if remaining != 0 {
        let mut message = format!("invalid number {remaining} for Set ").into_bytes();
        message.extend_from_slice(&format_elements(elements));
        return Err(SetParseError::InvalidNumber(GoString::from(message)));
    }
    Ok(MysqlSet {
        name: join_strings(&items, b','),
        value,
    })
}

fn set_item_error<E, N>(elements: &[E], name: &N) -> SetParseError
where
    E: GoStringSource,
    N: GoStringSource + ?Sized,
{
    let mut message = b"item ".to_vec();
    message.extend_from_slice(name.as_go_bytes());
    message.extend_from_slice(b" is not in Set ");
    message.extend_from_slice(&format_elements(elements));
    SetParseError::UnknownItem(GoString::from(message))
}

fn format_elements<E: GoStringSource>(elements: &[E]) -> Vec<u8> {
    let mut output = Vec::new();
    output.push(b'[');
    for (index, element) in elements.iter().enumerate() {
        if index != 0 {
            output.push(b' ');
        }
        output.extend_from_slice(element.as_go_bytes());
    }
    output.push(b']');
    output
}

fn join_strings<E: GoStringSource>(items: &[&E], separator: u8) -> GoString {
    match items {
        [] => return GoString::default(),
        [item] => return item.to_go_string(),
        _ => {}
    }
    let capacity = items
        .iter()
        .map(|item| item.as_go_bytes().len())
        .sum::<usize>()
        .saturating_add(items.len().saturating_sub(1));
    let mut output = Vec::with_capacity(capacity);
    for (index, item) in items.iter().enumerate() {
        if index != 0 {
            output.push(separator);
        }
        output.extend_from_slice(item.as_go_bytes());
    }
    GoString::from(output)
}

/// Exact unsigned subset of Go 1.25 `strconv.ParseUint(s, 0, 64)`.
fn parse_go_uint64_base_zero(source: &[u8]) -> Option<u64> {
    if source.is_empty() || matches!(source[0], b'+' | b'-') {
        return None;
    }
    let (base, digits) = if source[0] == b'0' {
        if source.len() >= 3 {
            match source[1].to_ascii_lowercase() {
                b'b' => (2_u64, &source[2..]),
                b'o' => (8, &source[2..]),
                b'x' => (16, &source[2..]),
                _ => (8, &source[1..]),
            }
        } else {
            (8, &source[1..])
        }
    } else {
        (10, source)
    };

    let mut number = 0_u64;
    let mut saw_underscore = false;
    for byte in digits.iter().copied() {
        if byte == b'_' {
            saw_underscore = true;
            continue;
        }
        let digit = match byte {
            b'0'..=b'9' => u64::from(byte - b'0'),
            b'a'..=b'z' => u64::from(byte - b'a' + 10),
            b'A'..=b'Z' => u64::from(byte - b'A' + 10),
            _ => return None,
        };
        if digit >= base {
            return None;
        }
        number = number.checked_mul(base)?.checked_add(digit)?;
    }
    if saw_underscore && !go_underscore_ok(source) {
        return None;
    }
    Some(number)
}

fn go_underscore_ok(source: &[u8]) -> bool {
    let bytes = source;
    let mut index = 0;
    let mut previous = b'^';
    let mut hexadecimal = false;
    if bytes.len() >= 2
        && bytes[0] == b'0'
        && matches!(bytes[1].to_ascii_lowercase(), b'b' | b'o' | b'x')
    {
        index = 2;
        previous = b'0';
        hexadecimal = bytes[1].eq_ignore_ascii_case(&b'x');
    }
    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_digit()
            || (hexadecimal && matches!(byte.to_ascii_lowercase(), b'a'..=b'f'))
        {
            previous = b'0';
        } else if byte == b'_' {
            if previous != b'0' {
                return false;
            }
            previous = b'_';
        } else {
            if previous == b'_' {
                return false;
            }
            previous = b'!';
        }
        index += 1;
    }
    previous != b'_'
}

#[cfg(test)]
mod tests {
    use super::parse_go_uint64_base_zero;

    #[test]
    fn go_base_zero_unsigned_fallback_is_source_exact() {
        let valid = [
            ("0", 0),
            ("1", 1),
            ("077", 63),
            ("0o77", 63),
            ("0b11", 3),
            ("0xFF", 255),
            ("0x_FF", 255),
            ("1_000", 1000),
            ("18446744073709551615", u64::MAX),
        ];
        for (source, expected) in valid {
            assert_eq!(
                parse_go_uint64_base_zero(source.as_bytes()),
                Some(expected),
                "{source}"
            );
        }
        for source in [
            "",
            "+1",
            "-1",
            "09",
            "0x",
            "_1",
            "1_",
            "1__0",
            "18446744073709551616",
        ] {
            assert_eq!(
                parse_go_uint64_base_zero(source.as_bytes()),
                None,
                "{source}"
            );
        }
    }
}
