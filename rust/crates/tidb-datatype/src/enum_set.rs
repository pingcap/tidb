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

use crate::Collation;

/// MySQL ENUM's canonical name and one-based numeric index.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MysqlEnum {
    name: String,
    value: u64,
}

impl MysqlEnum {
    /// Creates the exact source name/value pair.
    pub fn new(name: impl Into<String>, value: u64) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }

    /// Returns the canonical element spelling.
    pub fn name(&self) -> &str {
        &self.name
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
        self.clone()
    }
}

impl fmt::Display for MysqlEnum {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.name)
    }
}

/// Exact source context plus TiDB's typed truncation identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumParseError {
    context: String,
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
    pub fn context(&self) -> &str {
        &self.context
    }

    /// Go returns `Enum{}` together with this error.
    pub fn returned_value(&self) -> MysqlEnum {
        MysqlEnum::default()
    }
}

impl fmt::Display for EnumParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.context)
    }
}

impl Error for EnumParseError {}

/// Parses an ENUM as a collation-aware name first, then as Go's base-0 u64.
pub fn parse_enum(
    elements: &[impl AsRef<str>],
    name: &str,
    collation: Collation,
) -> Result<MysqlEnum, EnumParseError> {
    if let Ok(value) = parse_enum_name(elements, name, collation) {
        return Ok(value);
    }
    if let Some(number) = parse_go_uint64_base_zero(name) {
        return parse_enum_value(elements, number);
    }
    Err(enum_item_error(elements, name))
}

/// Parses an ENUM name and returns the first collation-equal declaration.
pub fn parse_enum_name(
    elements: &[impl AsRef<str>],
    name: &str,
    collation: Collation,
) -> Result<MysqlEnum, EnumParseError> {
    elements
        .iter()
        .enumerate()
        .find(|(_, element)| {
            collation
                .compare(element.as_ref().as_bytes(), name.as_bytes())
                .is_eq()
        })
        .map(|(index, element)| MysqlEnum {
            name: element.as_ref().to_owned(),
            value: index as u64 + 1,
        })
        .ok_or_else(|| enum_item_error(elements, name))
}

/// Parses an ENUM's one-based numeric index.
pub fn parse_enum_value(
    elements: &[impl AsRef<str>],
    number: u64,
) -> Result<MysqlEnum, EnumParseError> {
    if number == 0 || number > elements.len() as u64 {
        return Err(EnumParseError {
            context: format!(
                "convert to MySQL enum failed: number {number} overflow enum boundary [1, {}]",
                elements.len()
            ),
        });
    }
    Ok(MysqlEnum {
        name: elements[number as usize - 1].as_ref().to_owned(),
        value: number,
    })
}

fn enum_item_error(elements: &[impl AsRef<str>], name: &str) -> EnumParseError {
    EnumParseError {
        context: format!(
            "convert to MySQL enum failed: item {name} is not in enum {}",
            format_elements(elements)
        ),
    }
}

/// MySQL SET's canonical declaration-ordered name and bit mask.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MysqlSet {
    name: String,
    value: u64,
}

impl MysqlSet {
    /// Creates the exact source name/value pair.
    pub fn new(name: impl Into<String>, value: u64) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }

    /// Returns the canonical comma-separated name.
    pub fn name(&self) -> &str {
        &self.name
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
        self.clone()
    }
}

impl fmt::Display for MysqlSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.name)
    }
}

/// Exact SET parse failures, including a typed replacement for Go's
/// out-of-bounds panic when `ParseSetValue` receives more than 64 elements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetParseError {
    /// One or more comma-separated names did not match an element.
    UnknownItem(String),
    /// Bits remained after consuming every declared element.
    InvalidNumber(String),
    /// MySQL SET cannot represent more than 64 bit positions.
    TooManyElements(usize),
}

impl fmt::Display for SetParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownItem(message) | Self::InvalidNumber(message) => {
                formatter.write_str(message)
            }
            Self::TooManyElements(count) => write!(
                formatter,
                "cannot parse Set value with {count} elements; MySQL Set supports at most 64"
            ),
        }
    }
}

impl Error for SetParseError {}

/// Parses a SET as names first, then as Go's base-0 u64.
pub fn parse_set(
    elements: &[impl AsRef<str>],
    name: &str,
    collation: Collation,
) -> Result<MysqlSet, SetParseError> {
    if let Ok(value) = parse_set_name(elements, name, collation) {
        return Ok(value);
    }
    if let Some(number) = parse_go_uint64_base_zero(name) {
        return parse_set_value(elements, number);
    }
    Err(set_item_error(elements, name))
}

/// Parses, deduplicates, and canonicalizes SET names by collation key.
pub fn parse_set_name(
    elements: &[impl AsRef<str>],
    name: &str,
    collation: Collation,
) -> Result<MysqlSet, SetParseError> {
    if name.is_empty() {
        return Ok(MysqlSet::default());
    }

    let mut marked: HashSet<Vec<u8>> = name
        .split(',')
        .map(|part| collation.key(part.as_bytes()))
        .collect();
    let mut items = Vec::with_capacity(marked.len());
    let mut value = 0_u64;
    for (index, element) in elements.iter().enumerate() {
        if marked.remove(&collation.key(element.as_ref().as_bytes())) {
            value |= 1_u64.checked_shl(index as u32).unwrap_or(0);
            items.push(element.as_ref());
        }
    }
    if !marked.is_empty() {
        return Err(set_item_error(elements, name));
    }
    Ok(MysqlSet {
        name: items.join(","),
        value,
    })
}

/// Parses a SET bit mask and returns names in declaration order.
pub fn parse_set_value(
    elements: &[impl AsRef<str>],
    number: u64,
) -> Result<MysqlSet, SetParseError> {
    if number == 0 {
        return Ok(MysqlSet::default());
    }
    if elements.len() > 64 {
        return Err(SetParseError::TooManyElements(elements.len()));
    }

    let value = number;
    let mut remaining = number;
    let mut items = Vec::new();
    for (index, element) in elements.iter().enumerate() {
        let bit = 1_u64 << index;
        if remaining & bit != 0 {
            items.push(element.as_ref());
            remaining &= !bit;
        }
    }
    if remaining != 0 {
        return Err(SetParseError::InvalidNumber(format!(
            "invalid number {remaining} for Set {}",
            format_elements(elements)
        )));
    }
    Ok(MysqlSet {
        name: items.join(","),
        value,
    })
}

fn set_item_error(elements: &[impl AsRef<str>], name: &str) -> SetParseError {
    SetParseError::UnknownItem(format!(
        "item {name} is not in Set {}",
        format_elements(elements)
    ))
}

fn format_elements(elements: &[impl AsRef<str>]) -> String {
    format!(
        "[{}]",
        elements
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>()
            .join(" ")
    )
}

/// Exact unsigned subset of Go 1.26 `strconv.ParseUint(s, 0, 64)`.
fn parse_go_uint64_base_zero(source: &str) -> Option<u64> {
    if source.is_empty() || source.starts_with(['+', '-']) {
        return None;
    }
    let bytes = source.as_bytes();
    let (base, digits) = if bytes[0] == b'0' {
        if bytes.len() >= 3 {
            match bytes[1].to_ascii_lowercase() {
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
    for byte in digits.bytes() {
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

fn go_underscore_ok(source: &str) -> bool {
    let bytes = source.as_bytes();
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
                parse_go_uint64_base_zero(source),
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
            assert_eq!(parse_go_uint64_base_zero(source), None, "{source}");
        }
    }
}
